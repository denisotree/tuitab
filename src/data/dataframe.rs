use crate::data::aggregator::AggregatorKind;
use crate::data::column::ColumnMeta;
use crate::types::ColumnType;
use chrono::{NaiveDate, NaiveDateTime};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use unicode_width::UnicodeWidthStr;

/// Columnar in-memory data store powered by Polars.
#[derive(Clone, Serialize, Deserialize)]
pub struct DataFrame {
    pub df: polars::prelude::DataFrame,
    pub columns: Vec<ColumnMeta>,
    /// Mapping from display position → physical row index (enables sort/filter without moving data)
    pub row_order: Arc<Vec<usize>>,
    /// Original load order — used to reset sort and re-apply filter from scratch
    pub original_order: Arc<Vec<usize>>,
    /// Physical row indices selected by the user
    pub selected_rows: HashSet<usize>,
    /// True if any cell has been edited by the user since last save
    pub modified: bool,
    /// Cached aggregate computations to avoid O(N) recalculation every frame
    #[serde(skip)]
    pub aggregates_cache: Option<Vec<Vec<(AggregatorKind, String)>>>,
}

impl DataFrame {
    /// Create an empty DataFrame (placeholder when data is swapped to disk).
    pub fn empty() -> Self {
        Self {
            df: polars::prelude::DataFrame::empty(),
            columns: Vec::new(),
            row_order: Arc::new(Vec::new()),
            original_order: Arc::new(Vec::new()),
            selected_rows: HashSet::new(),
            modified: false,
            aggregates_cache: None,
        }
    }

    /// Helper to reliably format AnyValue into String.
    fn anyvalue_to_string(val: &AnyValue) -> String {
        match val {
            AnyValue::Null => String::new(),
            AnyValue::String(s) => s.to_string(),
            AnyValue::StringOwned(s) => s.to_string(),
            AnyValue::Boolean(b) => (if *b { "true" } else { "false" }).to_string(),
            AnyValue::Int32(i) => i.to_string(),
            AnyValue::Int64(i) => i.to_string(),
            AnyValue::UInt32(i) => i.to_string(),
            AnyValue::UInt64(i) => i.to_string(),
            AnyValue::Float32(f) => f.to_string(),
            AnyValue::Float64(f) => f.to_string(),
            AnyValue::Datetime(_v, tu, tz) => {
                if let Ok(s) = polars::prelude::Series::from_any_values_and_dtype(
                    "".into(),
                    std::slice::from_ref(val),
                    &polars::prelude::DataType::Datetime(*tu, (*tz).cloned()),
                    true,
                ) {
                    if let Ok(cast_s) = s.cast(&polars::prelude::DataType::String) {
                        if let Ok(ca) = cast_s.str() {
                            if let Some(res) = ca.get(0) {
                                return res.to_string();
                            }
                        }
                    }
                }
                let mut s = format!("{}", val);
                if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                    s = s[1..s.len() - 1].to_string();
                }
                s
            }
            _ => {
                let mut s = format!("{}", val);
                if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
                    s = s[1..s.len() - 1].to_string();
                }
                s
            }
        }
    }

    /// Get value by physical row and column index.
    pub fn get_physical(&self, physical_row: usize, col: usize) -> String {
        if col >= self.df.width() || physical_row >= self.df.height() {
            return String::new();
        }
        let series = &self.df.columns()[col];
        if let Ok(any_val) = series.get(physical_row) {
            Self::anyvalue_to_string(&any_val)
        } else {
            String::new()
        }
    }

    /// Return the cell value at (`display_row`, `col`) honouring the current
    /// `row_order` indirection so that sort and filter are transparent to callers.
    pub fn get_val(&self, display_row: usize, col: usize) -> AnyValue<'_> {
        if col >= self.df.width() || display_row >= self.row_order.len() {
            return AnyValue::Null;
        }
        let physical_row = self.row_order[display_row];
        let series = &self.df.columns()[col];
        series.get(physical_row).unwrap_or(AnyValue::Null)
    }

    /// Format a Polars [`AnyValue`] to a display [`String`].
    ///
    /// Public thin wrapper around the private `anyvalue_to_string` helper.
    /// Datetime values are rendered using Polars's own string cast to preserve
    /// timezone and precision information.
    pub fn anyvalue_to_string_fmt(val: &AnyValue) -> String {
        Self::anyvalue_to_string(val)
    }

    /// Format a cell value applying the column's type (Percentage, Currency, Float precision).
    /// Returns the same string a user sees on screen.
    pub fn format_display(&self, physical_row: usize, col: usize) -> String {
        let raw = self.get_physical(physical_row, col);
        if raw.is_empty() || col >= self.columns.len() {
            return raw;
        }
        let meta = &self.columns[col];
        let p = meta.precision as usize;
        match meta.col_type {
            ColumnType::Percentage => {
                if let Ok(f) = raw.parse::<f64>() {
                    return format!("{:.*}%", p, f * 100.0);
                }
            }
            ColumnType::Currency => {
                if let Ok(f) = raw.parse::<f64>() {
                    let sym = meta.currency.map(|k| k.symbol()).unwrap_or("$");
                    let prefix = meta.currency.map(|k| k.is_prefix()).unwrap_or(true);
                    if f < 0.0 {
                        let abs_f = f.abs();
                        return if prefix {
                            format!("({}{:.*})", sym, p, abs_f)
                        } else {
                            format!("({:.*}{})", p, abs_f, sym)
                        };
                    } else {
                        return if prefix {
                            format!("{}{:.*}", sym, p, f)
                        } else {
                            format!("{:.*}{}", p, f, sym)
                        };
                    }
                }
            }
            ColumnType::Float => {
                if let Ok(f) = raw.parse::<f64>() {
                    return format!("{:.*}", p, f);
                }
            }
            _ => {}
        }
        raw
    }

    /// Build a Polars DataFrame where Percentage / Currency / Float columns are
    /// replaced by their display strings, respecting the current row_order.
    /// Used by save functions so exported files match what the user sees on screen.
    pub fn to_display_polars_df(&self) -> polars::prelude::DataFrame {
        use polars::prelude::{Column, Series};
        let nrows = self.row_order.len();
        let cols: Vec<Column> = self
            .columns
            .iter()
            .enumerate()
            .map(|(col_idx, meta)| {
                let needs_fmt = matches!(
                    meta.col_type,
                    ColumnType::Percentage | ColumnType::Currency | ColumnType::Float
                );
                if needs_fmt {
                    let values: Vec<String> = (0..nrows)
                        .map(|r| self.format_display(self.row_order[r], col_idx))
                        .collect();
                    Series::new(meta.name.clone().into(), values).into()
                } else {
                    // Keep original series but reorder to match row_order
                    let orig = &self.df.columns()[col_idx];
                    if nrows == self.df.height() && self.row_order == self.original_order {
                        orig.clone()
                    } else {
                        let idx: Vec<u32> = self.row_order.iter().map(|&i| i as u32).collect();
                        let idx_ca = polars::prelude::IdxCa::new("".into(), idx);
                        orig.take(&idx_ca).unwrap_or_else(|_| orig.clone())
                    }
                }
            })
            .collect();
        polars::prelude::DataFrame::new_infer_height(cols).unwrap_or_else(|_| self.df.clone())
    }

    /// Overwrite the cell at physical (`physical_row`, `col`) with `value`.
    ///
    /// Percentage and currency columns receive special pre-processing before the
    /// value is written back into the Polars series.  Marks the frame as
    /// `modified` and clears the aggregates cache.
    pub fn set_cell(
        &mut self,
        physical_row: usize,
        col: usize,
        value: String,
    ) -> Result<(), String> {
        if col >= self.df.width() || physical_row >= self.df.height() {
            return Err("Out of bounds".into());
        }
        let series = &self.df.columns()[col];
        let series_name = series.name().clone();

        // Cast to string eagerly (handles Int/Date/Float natively via Polars)
        let string_series = series
            .cast(&polars::prelude::DataType::String)
            .map_err(|e| e.to_string())?;
        let str_ca = string_series.str().map_err(|e| e.to_string())?;

        let mut parsed_val = value.clone();
        if col < self.columns.len() {
            match self.columns[col].col_type {
                ColumnType::Percentage => {
                    let s = parsed_val.trim().replace('%', "");
                    if let Ok(f) = s.parse::<f64>() {
                        parsed_val = (f / 100.0).to_string();
                    }
                }
                ColumnType::Currency => {
                    let s = parsed_val.trim();
                    // Dirty float parsing: keep only digits, '.', and '-'
                    let cleaned: String = s
                        .chars()
                        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                        .collect();
                    if let Ok(f) = cleaned.parse::<f64>() {
                        parsed_val = f.to_string();
                    }
                }
                _ => {}
            }
        }

        let mut builder =
            polars::prelude::StringChunkedBuilder::new(series_name.clone(), str_ca.len());
        for (i, opt_s) in str_ca.into_iter().enumerate() {
            if i == physical_row {
                builder.append_value(&parsed_val);
            } else {
                builder.append_option(opt_s);
            }
        }

        let new_series = builder.finish().into_series();
        let final_series = new_series.cast(series.dtype()).unwrap_or(new_series);
        self.df
            .with_column(final_series.into())
            .map_err(|e| e.to_string())?;
        self.modified = true;
        self.aggregates_cache = None;
        Ok(())
    }

    /// Number of rows currently visible (after any active filter).
    pub fn visible_row_count(&self) -> usize {
        self.row_order.len()
    }

    /// Number of columns.
    pub fn col_count(&self) -> usize {
        self.columns.len()
    }

    /// Cast column `col_idx` to `col_type`.
    /// Returns true if the string series contains at least one value ending with `%`.
    fn series_looks_like_percent(series: &polars::prelude::Column) -> bool {
        if let Ok(str_ca) = series.str() {
            str_ca
                .into_iter()
                .flatten()
                .any(|s| s.trim().ends_with('%'))
        } else {
            false
        }
    }

    ///
    /// Updates both the Polars `Series` dtype and the corresponding [`crate::data::column::ColumnMeta`].
    /// Returns an error string if the Polars cast fails (e.g. non-numeric string → integer).
    pub fn set_column_type(&mut self, col_idx: usize, col_type: ColumnType) -> Result<(), String> {
        if col_idx >= self.columns.len() {
            return Err("Column out of bounds".into());
        }
        if self.columns[col_idx].col_type == col_type {
            return Ok(());
        }

        let target_dtype = match col_type {
            ColumnType::Integer => polars::prelude::DataType::Int64,
            ColumnType::Float | ColumnType::Percentage | ColumnType::Currency => {
                polars::prelude::DataType::Float64
            }
            ColumnType::Boolean => polars::prelude::DataType::Boolean,
            ColumnType::Date => polars::prelude::DataType::Date,
            ColumnType::Datetime => {
                polars::prelude::DataType::Datetime(polars::datatypes::TimeUnit::Microseconds, None)
            }
            _ => polars::prelude::DataType::String,
        };

        let series = &self.df.columns()[col_idx];
        let src_dtype = series.dtype().clone();

        let new_col = if target_dtype == polars::prelude::DataType::Boolean
            && src_dtype == polars::prelude::DataType::String
        {
            Self::col_bool_from_str(series)?
        } else if col_type == ColumnType::Currency && src_dtype == polars::prelude::DataType::String
        {
            Self::col_currency_from_str(series)?
        } else if col_type == ColumnType::Date && src_dtype == polars::prelude::DataType::String {
            Self::col_date_from_str(series)?
        } else if col_type == ColumnType::Datetime && src_dtype == polars::prelude::DataType::String
        {
            Self::col_datetime_from_str(series)?
        } else if col_type == ColumnType::Date
            && src_dtype == polars::prelude::DataType::Datetime(TimeUnit::Microseconds, None)
        {
            // Save backup strings so Datetime can be restored later
            if let Ok(str_series) = series.cast(&polars::prelude::DataType::String) {
                if let Ok(str_ca) = str_series.str() {
                    let backup: Vec<Option<String>> = str_ca
                        .into_iter()
                        .map(|s| s.map(|x| x.to_string()))
                        .collect();
                    self.columns[col_idx].backup_datetime_str = Some(backup);
                }
            }
            self.df.columns()[col_idx]
                .strict_cast(&target_dtype)
                .map_err(|e| format!("Cannot cast to {:?}. Error: {}", target_dtype, e))?
        } else if col_type == ColumnType::Datetime && src_dtype == polars::prelude::DataType::Date {
            // Restore from backup strings if available, otherwise plain cast
            if let Some(backup) = &self.columns[col_idx].backup_datetime_str {
                Self::col_datetime_from_backup(self.df.columns()[col_idx].name().clone(), backup)?
            } else {
                self.df.columns()[col_idx]
                    .strict_cast(&target_dtype)
                    .map_err(|e| format!("Cannot cast to {:?}. Error: {}", target_dtype, e))?
            }
        } else if matches!(
            col_type,
            ColumnType::Float | ColumnType::Percentage | ColumnType::Integer
        ) && src_dtype == polars::prelude::DataType::String
            && Self::series_looks_like_percent(series)
        {
            Self::col_from_percent_str(series, col_type)?
        } else {
            series
                .strict_cast(&target_dtype)
                .map_err(|e| format!("Cannot cast to {:?}. Error: {}", target_dtype, e))?
        };

        self.df.with_column(new_col).map_err(|e| e.to_string())?;
        self.columns[col_idx].col_type = col_type;
        if matches!(
            col_type,
            ColumnType::Float | ColumnType::Percentage | ColumnType::Currency
        ) && self.columns[col_idx].precision < 2
        {
            self.columns[col_idx].precision = 2;
        }
        self.aggregates_cache = None;
        self.modified = true;
        Ok(())
    }

    fn col_bool_from_str(
        series: &polars::prelude::Column,
    ) -> Result<polars::prelude::Column, String> {
        let str_ca = series.str().map_err(|e| e.to_string())?;
        let mut builder =
            polars::prelude::BooleanChunkedBuilder::new(series.name().clone(), str_ca.len());
        for opt_s in str_ca.into_iter() {
            if let Some(s) = opt_s {
                let lower = s.trim().to_lowercase();
                if lower == "true" || lower == "1" || lower == "yes" {
                    builder.append_value(true);
                } else if lower.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(false);
                }
            } else {
                builder.append_null();
            }
        }
        Ok(polars::prelude::Column::from(
            builder.finish().into_series(),
        ))
    }

    fn col_currency_from_str(
        series: &polars::prelude::Column,
    ) -> Result<polars::prelude::Column, String> {
        let str_ca = series.str().map_err(|e| e.to_string())?;
        let vals: Vec<Option<f64>> = str_ca
            .into_iter()
            .map(|opt_s| {
                opt_s.and_then(|s| {
                    let cleaned: String = s
                        .chars()
                        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                        .collect();
                    cleaned.parse::<f64>().ok()
                })
            })
            .collect();
        Ok(polars::prelude::Column::from(Series::new(
            series.name().clone(),
            vals,
        )))
    }

    fn col_date_from_str(
        series: &polars::prelude::Column,
    ) -> Result<polars::prelude::Column, String> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let str_ca = series.str().map_err(|e| e.to_string())?;
        let days: Vec<Option<i32>> = str_ca
            .into_iter()
            .map(|opt_s| {
                let s = opt_s?.trim();
                if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    return Some((d - epoch).num_days() as i32);
                }
                for fmt in crate::data::DATETIME_FORMATS {
                    if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                        return Some((dt.date() - epoch).num_days() as i32);
                    }
                }
                None
            })
            .collect();
        Ok(Column::from(
            Series::new(series.name().clone(), days)
                .strict_cast(&polars::prelude::DataType::Date)
                .map_err(|e| format!("Cannot cast to Date. Error: {}", e))?,
        ))
    }

    fn col_datetime_from_str(
        series: &polars::prelude::Column,
    ) -> Result<polars::prelude::Column, String> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let str_ca = series.str().map_err(|e| e.to_string())?;
        let micros: Vec<Option<i64>> = str_ca
            .into_iter()
            .map(|opt_s| {
                let s = opt_s?.trim();
                for fmt in crate::data::DATETIME_FORMATS {
                    if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                        let diff = dt - epoch;
                        return diff
                            .num_microseconds()
                            .or_else(|| diff.num_seconds().checked_mul(1_000_000));
                    }
                }
                if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let diff = d.and_hms_opt(0, 0, 0).unwrap() - epoch;
                    return diff
                        .num_microseconds()
                        .or_else(|| diff.num_seconds().checked_mul(1_000_000));
                }
                None
            })
            .collect();
        Ok(Column::from(
            Series::new(series.name().clone(), micros)
                .strict_cast(&polars::prelude::DataType::Datetime(
                    TimeUnit::Microseconds,
                    None,
                ))
                .map_err(|e| format!("Cannot cast to Datetime. Error: {}", e))?,
        ))
    }

    fn col_datetime_from_backup(
        name: polars::prelude::PlSmallStr,
        backup: &[Option<String>],
    ) -> Result<polars::prelude::Column, String> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let micros: Vec<Option<i64>> = backup
            .iter()
            .map(|opt_s| {
                opt_s.as_ref().and_then(|s| {
                    for fmt in crate::data::DATETIME_FORMATS {
                        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                            let diff = dt - epoch;
                            return diff
                                .num_microseconds()
                                .or_else(|| diff.num_seconds().checked_mul(1_000_000));
                        }
                    }
                    None
                })
            })
            .collect();
        Ok(Column::from(
            Series::new(name, micros)
                .strict_cast(&polars::prelude::DataType::Datetime(
                    TimeUnit::Microseconds,
                    None,
                ))
                .map_err(|e| format!("Cannot restore Datetime from backup. Error: {}", e))?,
        ))
    }

    fn col_from_percent_str(
        series: &polars::prelude::Column,
        col_type: ColumnType,
    ) -> Result<polars::prelude::Column, String> {
        let str_ca = series.str().map_err(|e| e.to_string())?;
        match col_type {
            ColumnType::Percentage => {
                let vals: Vec<Option<f64>> = str_ca
                    .into_iter()
                    .map(|opt| {
                        opt.and_then(|s| s.trim().trim_end_matches('%').parse::<f64>().ok())
                            .map(|f| f / 100.0)
                    })
                    .collect();
                Ok(Column::from(Series::new(series.name().clone(), vals)))
            }
            ColumnType::Float => {
                let vals: Vec<Option<f64>> = str_ca
                    .into_iter()
                    .map(|opt| opt.and_then(|s| s.trim().trim_end_matches('%').parse::<f64>().ok()))
                    .collect();
                Ok(Column::from(Series::new(series.name().clone(), vals)))
            }
            ColumnType::Integer => {
                let vals: Vec<Option<i64>> = str_ca
                    .into_iter()
                    .map(|opt| {
                        opt.and_then(|s| s.trim().trim_end_matches('%').parse::<f64>().ok())
                            .map(|f| f.round() as i64)
                    })
                    .collect();
                Ok(Column::from(
                    Series::new(series.name().clone(), vals)
                        .strict_cast(&polars::prelude::DataType::Int64)
                        .map_err(|e| e.to_string())?,
                ))
            }
            _ => unreachable!(),
        }
    }

    // ── Column Operations (Phase 21) ──────────────────────────────────────────

    /// Pin or unpin column `col_idx`.
    ///
    /// Pinned columns are physically moved to the front of the frame (just after
    /// any already-pinned columns).  Unpinned columns are moved to the end of
    /// the pinned block.  Returns the new physical index of the column.
    pub fn toggle_pin_column(&mut self, col_idx: usize) -> Result<usize, String> {
        if col_idx >= self.columns.len() {
            return Err("Out of bounds".into());
        }

        let is_pinned = self.columns[col_idx].pinned;
        self.columns[col_idx].pinned = !is_pinned;

        let target_idx = if !is_pinned {
            // Pinning: remember position among unpinned columns, then move after pinned block
            let restore_pos = (0..col_idx).filter(|&i| !self.columns[i].pinned).count();
            self.columns[col_idx].pin_restore_pos = Some(restore_pos);

            let mut insert_pos = 0;
            for i in 0..self.columns.len() {
                if i == col_idx {
                    continue;
                }
                if self.columns[i].pinned {
                    insert_pos += 1;
                } else {
                    break;
                }
            }
            insert_pos
        } else {
            // Unpinning: restore to saved position among unpinned columns
            let num_pinned: usize = (0..self.columns.len())
                .filter(|&i| i != col_idx && self.columns[i].pinned)
                .count();
            let num_unpinned: usize = (0..self.columns.len())
                .filter(|&i| i != col_idx && !self.columns[i].pinned)
                .count();
            let restore_pos = self.columns[col_idx]
                .pin_restore_pos
                .unwrap_or(num_unpinned)
                .min(num_unpinned);
            self.columns[col_idx].pin_restore_pos = None;
            num_pinned + restore_pos
        };

        // Physically move the column to target_idx using swap_columns
        let mut current = col_idx;
        if current > target_idx {
            while current > target_idx {
                self.swap_columns(current - 1, current)?;
                current -= 1;
            }
        } else if current < target_idx {
            while current < target_idx {
                self.swap_columns(current, current + 1)?;
                current += 1;
            }
        }
        Ok(target_idx)
    }

    /// Rename column at `col_idx` to `new_name`.
    ///
    /// Updates both the Polars schema and the [`crate::data::column::ColumnMeta`] name.
    pub fn rename_column(&mut self, col_idx: usize, new_name: &str) -> Result<(), String> {
        if col_idx >= self.columns.len() {
            return Err("Column index out of bounds".to_string());
        }
        let old_name = self.columns[col_idx].name.clone();
        self.df
            .rename(&old_name, new_name.into())
            .map_err(|e| e.to_string())?;
        self.columns[col_idx].name = new_name.to_string();
        self.modified = true;
        self.aggregates_cache = None;
        Ok(())
    }

    /// Remove column at `col_idx` from the Polars frame and the `columns` metadata.
    pub fn drop_column(&mut self, col_idx: usize) -> Result<(), String> {
        if col_idx >= self.columns.len() {
            return Err("Column index out of bounds".to_string());
        }
        let name = self.columns[col_idx].name.clone();
        self.df = self.df.drop(&name).map_err(|e| e.to_string())?;
        self.columns.remove(col_idx);
        self.modified = true;
        self.aggregates_cache = None;
        Ok(())
    }

    /// Insert a new empty string column named `name` at position `col_idx`,
    /// shifting all columns at `col_idx` and beyond one position to the right.
    pub fn insert_empty_column(&mut self, col_idx: usize, name: &str) -> Result<(), String> {
        if self.columns.iter().any(|c| c.name == name) {
            return Err("Column name already exists".to_string());
        }
        let height = self.df.height();
        let empty_col: Vec<String> = vec![String::new(); height];
        let empty_series = Series::new(name.into(), &empty_col);

        self.df
            .with_column(empty_series.into())
            .map_err(|e| e.to_string())?;

        let mut meta = ColumnMeta::new(name.to_string());
        meta.col_type = ColumnType::String;
        self.columns.push(meta);

        // Move to the requested position
        let last_idx = self.columns.len() - 1;
        for i in (col_idx..last_idx).rev() {
            self.swap_columns(i, i + 1)?;
        }

        self.calc_widths(40, 1000);
        self.modified = true;
        self.aggregates_cache = None;
        Ok(())
    }

    /// Swap the positions of columns `col1` and `col2` in both the Polars schema
    /// and the `columns` metadata vector.
    pub fn swap_columns(&mut self, col1: usize, col2: usize) -> Result<(), String> {
        if col1 >= self.columns.len() || col2 >= self.columns.len() {
            return Err("Column index out of bounds".to_string());
        }
        if col1 == col2 {
            return Ok(());
        }

        let mut names: Vec<String> = self.columns.iter().map(|c| c.name.clone()).collect();
        names.swap(col1, col2);

        self.df = self.df.select(names).map_err(|e| e.to_string())?;
        self.columns.swap(col1, col2);

        self.modified = true;
        self.aggregates_cache = None;
        Ok(())
    }

    // ── Aggregators ────────────────────────────────────────────────────────────

    // ── Aggregators ────────────────────────────────────────────────────────────

    /// Compute all active aggregators for every column.
    ///
    /// Results are cached in `aggregates_cache` so subsequent calls within the same
    /// frame are free.  The cache is invalidated whenever the data changes
    /// (cell edit, sort, filter, column mutation).
    /// Returns a column-indexed list of `(AggregatorKind, display_string)` pairs.
    pub fn compute_aggregates(&mut self) -> Vec<Vec<(AggregatorKind, String)>> {
        if let Some(ref cache) = self.aggregates_cache {
            return cache.clone();
        }

        let mut computed = vec![Vec::new(); self.columns.len()];
        let mut has_aggs = false;
        let mut exprs = Vec::new();

        for (col_idx, col_meta) in self.columns.iter().enumerate() {
            if !col_meta.aggregators.is_empty() {
                has_aggs = true;
                for (agg_idx, agg) in col_meta.aggregators.iter().enumerate() {
                    if let Some(expr) = agg.to_expr(&col_meta.name) {
                        let alias = format!("agg_{}_{}", col_idx, agg_idx);
                        exprs.push(expr.alias(&alias));
                    }
                }
            }
        }

        if !has_aggs {
            self.aggregates_cache = Some(computed.clone());
            return computed;
        }

        // Native polars evaluation for supported expressions
        let mut native_results = std::collections::HashMap::new();
        if !exprs.is_empty() {
            let indices = polars::prelude::IdxCa::new(
                "".into(),
                self.row_order
                    .iter()
                    .map(|&i| i as polars::prelude::IdxSize)
                    .collect::<Vec<_>>(),
            );
            let visible_df = if self.row_order.len() != self.df.height()
                || self
                    .row_order
                    .iter()
                    .zip(0..self.df.height())
                    .any(|(&a, b)| a != b)
            {
                self.df.take(&indices).unwrap_or_else(|_| self.df.clone())
            } else {
                self.df.clone()
            };

            if let Ok(result_df) = visible_df.lazy().select(exprs).collect() {
                for (col_idx, col_meta) in self.columns.iter().enumerate() {
                    for (agg_idx, _agg) in col_meta.aggregators.iter().enumerate() {
                        let alias = format!("agg_{}_{}", col_idx, agg_idx);
                        if let Ok(series) = result_df.column(&alias) {
                            if let Ok(val) = series.get(0) {
                                native_results.insert(alias, Self::anyvalue_to_string(&val));
                            }
                        }
                    }
                }
            }
        }

        // Combine native results with string-fallback for missing entries
        for (col_idx, col_meta) in self.columns.iter().enumerate() {
            if col_meta.aggregators.is_empty() {
                continue;
            }

            let mut col_aggs = Vec::new();
            // Lazily collected string values — only when a native result is absent
            let mut fallback_values: Option<Vec<String>> = None;

            for (agg_idx, agg) in col_meta.aggregators.iter().enumerate() {
                if !agg.is_compatible(col_meta.col_type) {
                    continue;
                }

                let alias = format!("agg_{}_{}", col_idx, agg_idx);
                let result_str = if let Some(native_val) = native_results.get(&alias) {
                    // Format native numbers with column precision
                    if let Ok(f) = native_val.parse::<f64>() {
                        crate::data::aggregator::format_numeric(
                            f,
                            col_meta.col_type,
                            col_meta.precision,
                            col_meta.currency,
                        )
                    } else {
                        native_val.clone()
                    }
                } else {
                    // Native result absent (no expr or polars path failed): use string fallback
                    let values = fallback_values.get_or_insert_with(|| {
                        self.row_order
                            .iter()
                            .map(|&row_idx| {
                                let series = &self.df.columns()[col_idx];
                                if let Ok(v) = series.get(row_idx) {
                                    Self::anyvalue_to_string(&v)
                                } else {
                                    String::new()
                                }
                            })
                            .collect()
                    });
                    agg.compute(
                        values,
                        col_meta.col_type,
                        col_meta.precision,
                        col_meta.currency,
                    )
                };

                col_aggs.push((*agg, result_str));
            }
            computed[col_idx] = col_aggs;
        }

        self.aggregates_cache = Some(computed.clone());
        computed
    }

    /// Append `agg` to the active aggregators on column `col_idx`.
    ///
    /// Returns an error if the aggregator is not compatible with the column type
    /// (e.g. `Sum` on a string column).
    pub fn add_aggregator(
        &mut self,
        col_idx: usize,
        agg: AggregatorKind,
    ) -> Result<(), &'static str> {
        if col_idx < self.columns.len() {
            let col = &mut self.columns[col_idx];
            if !agg.is_compatible(col.col_type) {
                return Err("Aggregator not compatible with column type (use 't' to change type)");
            }
            if !col.aggregators.contains(&agg) {
                col.aggregators.push(agg);
                self.aggregates_cache = None;
            }
        }
        Ok(())
    }

    pub fn clear_aggregators(&mut self, col_idx: usize) {
        if col_idx < self.columns.len() && !self.columns[col_idx].aggregators.is_empty() {
            self.columns[col_idx].aggregators.clear();
            self.aggregates_cache = None;
        }
    }

    // ── Computed columns ──────────────────────────────────────────────────────

    /// Evaluate `expr` against every row and append the result as a new column named `name`,
    /// inserted immediately after `insert_after_col`.
    ///
    /// Tries the Polars lazy API first for best performance; falls back to a
    /// row-by-row interpreter if the expression contains unsupported constructs.
    pub fn add_computed_column(
        &mut self,
        name: &str,
        expr: &crate::data::expression::Expr,
        insert_after_col: usize,
    ) -> Result<(), String> {
        // Fast path: Try using Polars Lazy API
        if let Ok(polars_expr) = expr.to_polars_expr() {
            match self
                .df
                .clone()
                .lazy()
                .with_column(polars_expr.alias(name))
                .collect()
            {
                Ok(df) => {
                    self.df = df;

                    let mut dtype = self.df.column(name).unwrap().dtype().clone();
                    if let polars::prelude::DataType::Duration(tu) = dtype {
                        if let Ok(series) = self.df.column(name) {
                            let divisor = match tu {
                                polars::datatypes::TimeUnit::Nanoseconds => 1_000_000_000.0,
                                polars::datatypes::TimeUnit::Microseconds => 1_000_000.0,
                                polars::datatypes::TimeUnit::Milliseconds => 1000.0,
                            };
                            if let Ok(int_series) = series.cast(&polars::prelude::DataType::Int64) {
                                if let Ok(float_series) =
                                    int_series.cast(&polars::prelude::DataType::Float64)
                                {
                                    if let Ok(f64_ca) = float_series.f64() {
                                        let new_series_arr = f64_ca.apply_values(|v| v / divisor);
                                        let new_series =
                                            new_series_arr.into_series().with_name(name.into());
                                        let _ = self.df.replace(name, new_series.into());
                                        dtype = polars::prelude::DataType::Float64;
                                    }
                                }
                            }
                        }
                    }

                    let mut meta = ColumnMeta::new(name.to_string());
                    // Map the polars data type to our ColumnType if possible
                    meta.col_type = match dtype {
                        polars::prelude::DataType::Int8
                        | polars::prelude::DataType::Int16
                        | polars::prelude::DataType::Int32
                        | polars::prelude::DataType::Int64
                        | polars::prelude::DataType::UInt8
                        | polars::prelude::DataType::UInt16
                        | polars::prelude::DataType::UInt32
                        | polars::prelude::DataType::UInt64 => ColumnType::Integer,
                        polars::prelude::DataType::Float32 | polars::prelude::DataType::Float64 => {
                            ColumnType::Float
                        }
                        polars::prelude::DataType::Date => ColumnType::Date,
                        polars::prelude::DataType::Datetime(_, _) => ColumnType::Datetime,
                        _ => ColumnType::String,
                    };
                    meta.expression = Some(expr.clone());
                    self.columns.push(meta);
                    self.aggregates_cache = None;
                    self.calc_widths(40, 1000);

                    // Move to the requested position
                    let target_idx = insert_after_col + 1;
                    let mut curr_idx = self.columns.len() - 1;
                    while curr_idx > target_idx {
                        self.swap_columns(curr_idx - 1, curr_idx)?;
                        curr_idx -= 1;
                    }
                    return Ok(());
                }
                Err(_) => {
                    // Fast path failed (e.g. column not found in lazy API), fall through to slow path
                }
            }
        }

        // Slow path: Fallback to manual execution for unsupported AST nodes
        let col_lookup: std::collections::HashMap<&str, usize> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.as_str(), i))
            .collect();

        let total_rows = self.df.height();
        let mut new_col = Vec::with_capacity(total_rows);

        for physical_idx in 0..total_rows {
            let val = expr.eval(physical_idx, &col_lookup, self);
            match val {
                crate::data::expression::Value::Number(n) => {
                    if n.is_nan() {
                        new_col.push("—".to_string());
                    } else if n.fract() == 0.0 {
                        new_col.push(format!("{}", n as i64));
                    } else {
                        new_col.push(format!("{:.2}", n));
                    }
                }
                v => new_col.push(v.to_string()),
            }
        }

        let new_series = Series::new(name.into(), &new_col);

        // Try casting to Float64 if all values are numbers (slow path consistency)
        let final_series = new_series
            .cast(&polars::prelude::DataType::Float64)
            .unwrap_or(new_series);

        self.df = self
            .df
            .clone()
            .lazy()
            .with_column(polars::lazy::dsl::lit(final_series).alias(name))
            .collect()
            .map_err(|e| e.to_string())?;

        let mut meta = ColumnMeta::new(name.to_string());
        meta.col_type = match self.df.column(name).unwrap().dtype() {
            polars::prelude::DataType::Float64 => ColumnType::Float,
            _ => ColumnType::String,
        };
        meta.expression = Some(expr.clone());
        self.columns.push(meta);
        self.aggregates_cache = None;
        self.calc_widths(40, 1000);

        // Move to the requested position
        let target_idx = insert_after_col + 1;
        let mut curr_idx = self.columns.len() - 1;
        while curr_idx > target_idx {
            self.swap_columns(curr_idx - 1, curr_idx)?;
            curr_idx -= 1;
        }

        Ok(())
    }

    // ── Layout ─────────────────────────────────────────────────────────────

    /// Calculate column display widths.
    ///
    /// For numeric / date / boolean Polars dtypes uses a pre-computed fixed
    /// maximum width (zero allocations).  For string-like columns iterates
    /// over the raw `&str` slices in the ChunkedArray — no `series.get()` per
    /// row, no `String` allocation per cell.
    pub fn calc_widths(&mut self, max_width: u16, sample_size: usize) {
        for col_idx in 0..self.columns.len() {
            self.calc_column_width(col_idx, max_width, sample_size);
        }
    }

    /// Calculate the display width for a single column based on actual formatted values.
    pub fn calc_column_width(&mut self, col_idx: usize, max_width: u16, sample_size: usize) {
        if col_idx >= self.df.width() || col_idx >= self.columns.len() {
            return;
        }

        let total_rows = self.df.height();
        let sample_end = sample_size.min(total_rows);

        let max_val_width: u16 = (0..sample_end)
            .map(|physical_row| {
                let text = self.format_display(physical_row, col_idx);
                UnicodeWidthStr::width(text.as_str()) as u16
            })
            .max()
            .unwrap_or(0);

        let col_meta = &mut self.columns[col_idx];
        let header_w = UnicodeWidthStr::width(col_meta.name.as_str()) as u16 + 2;
        let actual_min = col_meta.min_width.max(header_w);
        col_meta.width = actual_min.max(max_val_width).min(max_width);
    }

    // ── Vectorized helpers ────────────────────────────────────────────────────

    /// Return a Polars [`polars::prelude::DataFrame`] containing only the currently visible
    /// (filtered) rows in display order.
    ///
    /// Avoids row-by-row access by using Polars `take()` with an index array.
    /// Used for clipboard copy and file export.
    pub fn get_visible_df(&self) -> Result<polars::prelude::DataFrame, String> {
        // Fast path: full DataFrame is already the visible set
        if self.row_order.len() == self.df.height()
            && self
                .row_order
                .iter()
                .zip(0..self.df.height())
                .all(|(&a, b)| a == b)
        {
            return Ok(self.df.clone());
        }

        let indices = polars::prelude::IdxCa::new(
            "".into(),
            self.row_order
                .iter()
                .map(|&i| i as polars::prelude::IdxSize)
                .collect::<Vec<_>>(),
        );
        self.df.take(&indices).map_err(|e| e.to_string())
    }

    /// Find **display-row** indices where `col_idx` matches `pattern` (regex).
    /// Uses Polars vectorized `str().contains()` — no row-by-row String allocation.
    pub fn find_matching_rows(&self, col_idx: usize, pattern: &str) -> Vec<usize> {
        if col_idx >= self.df.width() {
            return Vec::new();
        }

        let visible = match self.get_visible_df() {
            Ok(df) => df,
            Err(_) => return Vec::new(),
        };

        let col_name = &self.columns[col_idx].name;

        // Cast to String so we can use str().contains() on any dtype
        let str_col = match visible
            .column(col_name)
            .and_then(|c| c.cast(&polars::prelude::DataType::String))
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let str_ca = match str_col.as_materialized_series().str() {
            Ok(ca) => ca.clone(),
            Err(_) => return Vec::new(),
        };

        match str_ca.contains(pattern, false) {
            Ok(mask) => mask
                .into_iter()
                .enumerate()
                .filter_map(|(i, opt_b)| {
                    if opt_b.unwrap_or(false) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Find **display-row** indices where `col_idx` equals `target` exactly.
    /// Returns physical row indices (via `row_order`) for insertion into `selected_rows`.
    pub fn find_rows_by_value(&self, col_idx: usize, target: &str) -> Vec<usize> {
        if col_idx >= self.df.width() {
            return Vec::new();
        }

        let visible = match self.get_visible_df() {
            Ok(df) => df,
            Err(_) => return Vec::new(),
        };

        let col_name = &self.columns[col_idx].name;
        let str_col = match visible
            .column(col_name)
            .and_then(|c| c.cast(&polars::prelude::DataType::String))
        {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        let str_ca = match str_col.as_materialized_series().str() {
            Ok(ca) => ca.clone(),
            Err(_) => return Vec::new(),
        };

        str_ca
            .into_iter()
            .enumerate()
            .filter_map(
                |(i, opt_s)| {
                    if opt_s == Some(target) {
                        Some(i)
                    } else {
                        None
                    }
                },
            )
            .collect()
    }

    /// Returns display indices of rows where the numeric value in `col_idx` is in `[lo, hi)`.
    pub fn find_rows_in_range(&self, col_idx: usize, lo: f64, hi: f64) -> Vec<usize> {
        if col_idx >= self.df.width() {
            return Vec::new();
        }
        (0..self.visible_row_count())
            .filter(|&display_idx| {
                let val_str = Self::anyvalue_to_string_fmt(&self.get_val(display_idx, col_idx));
                if let Ok(v) = val_str.parse::<f64>() {
                    v >= lo && v < hi
                } else {
                    false
                }
            })
            .collect()
    }

    // ── Frequency table builders ──────────────────────────────────────────────

    /// Build a single-column frequency table using Polars `group_by` + `agg`.
    /// `aggregated_cols` is a list of `(col_idx, aggregators)` for per-group aggs.
    /// Returns `(polars::DataFrame, Vec<ColumnMeta>)` ready for a new Sheet.
    pub fn build_frequency_table(
        &self,
        col_idx: usize,
        aggregated_cols: &[(usize, Vec<crate::data::aggregator::AggregatorKind>)],
    ) -> Result<
        (
            polars::prelude::DataFrame,
            Vec<crate::data::column::ColumnMeta>,
        ),
        String,
    > {
        use polars::prelude::*;

        let col_name = self.columns[col_idx].name.clone();
        let visible = self.get_visible_df()?;

        // ── Base agg: count per group ──────────────────────────────────────
        let mut agg_exprs: Vec<Expr> = vec![col(&col_name).count().alias("Count")];

        // ── Per-column aggregators ─────────────────────────────────────────
        let mut extra_metas: Vec<crate::data::column::ColumnMeta> = Vec::new();
        for &(agg_col_idx, ref aggregators) in aggregated_cols {
            if agg_col_idx == col_idx {
                continue; // skip the grouping column itself
            }
            let agg_col_name = self.columns[agg_col_idx].name.clone();
            let src_meta = &self.columns[agg_col_idx];
            for agg_kind in aggregators {
                if let Some(expr) = agg_kind.to_expr(&agg_col_name) {
                    let alias_name = format!("{}:{}", agg_col_name, agg_kind.name());
                    agg_exprs.push(expr.alias(&alias_name));
                    let mut meta = crate::data::column::ColumnMeta::new(alias_name);
                    if agg_kind.preserves_col_type() {
                        meta.col_type = src_meta.col_type;
                        meta.currency = src_meta.currency;
                        meta.precision = src_meta.precision;
                    } else {
                        meta.col_type = crate::types::ColumnType::Integer;
                    }
                    extra_metas.push(meta);
                }
            }
        }

        // ── Run group_by ───────────────────────────────────────────────────
        let grouped = visible
            .lazy()
            .group_by([col(&col_name)])
            .agg(agg_exprs)
            .sort(
                ["Count"],
                SortMultipleOptions::new().with_order_descending_multi([true]),
            )
            .collect()
            .map_err(|e| format!("group_by error: {}", e))?;

        // ── Build Pct and Bar columns ──────────────────────────────────────
        let count_col = grouped.column("Count").map_err(|e| e.to_string())?;
        let total: f64 = count_col.as_materialized_series().sum::<u64>().unwrap_or(1) as f64;
        let max_count: usize = count_col
            .as_materialized_series()
            .max_reduce()
            .map_err(|e| e.to_string())?
            .value()
            .try_extract::<u64>()
            .unwrap_or(1) as usize;

        const BAR_WIDTH: usize = 20;
        let mut pct_values: Vec<f64> = Vec::with_capacity(grouped.height());
        let mut bar_values: Vec<String> = Vec::with_capacity(grouped.height());

        for i in 0..grouped.height() {
            let c = count_col
                .as_materialized_series()
                .get(i)
                .ok()
                .and_then(|v| v.try_extract::<u64>().ok())
                .unwrap_or(0) as usize;
            pct_values.push(c as f64 / total.max(1.0));
            bar_values.push(crate::app::build_bar(c, max_count, BAR_WIDTH));
        }

        // ── Assemble final DataFrame ───────────────────────────────────────
        let mut final_df = grouped.clone();
        final_df
            .with_column(Series::new("Pct".into(), &pct_values).into())
            .map_err(|e| e.to_string())?;
        final_df
            .with_column(Series::new("Bar".into(), &bar_values).into())
            .map_err(|e| e.to_string())?;

        // ── Build ColumnMeta list matching the DataFrame column order ──────
        //
        // group_by result column order:  Value, Count, [extra aggs...]
        // We append:                     Pct, Bar
        let mut columns: Vec<crate::data::column::ColumnMeta> = Vec::new();

        let mut val_meta = self.columns[col_idx].clone();
        val_meta.aggregators.clear();
        columns.push(val_meta);

        let mut count_meta = crate::data::column::ColumnMeta::new("Count".to_string());
        count_meta.col_type = crate::types::ColumnType::Integer;
        columns.push(count_meta);

        for meta in extra_metas {
            columns.push(meta);
        }

        let mut pct_meta = crate::data::column::ColumnMeta::new("Pct".to_string());
        pct_meta.col_type = ColumnType::Percentage;
        pct_meta.precision = 1;
        columns.push(pct_meta);
        columns.push(crate::data::column::ColumnMeta::new("Bar".to_string()));

        Ok((final_df, columns))
    }

    /// Build a multi-column frequency table (group by all pinned columns).
    pub fn build_multi_frequency_table(
        &self,
        group_col_indices: &[usize],
        aggregated_cols: &[(usize, Vec<crate::data::aggregator::AggregatorKind>)],
    ) -> Result<
        (
            polars::prelude::DataFrame,
            Vec<crate::data::column::ColumnMeta>,
        ),
        String,
    > {
        use polars::prelude::*;

        if group_col_indices.is_empty() {
            return Err("No columns specified".to_string());
        }

        let group_names: Vec<String> = group_col_indices
            .iter()
            .map(|&i| self.columns[i].name.clone())
            .collect();

        let visible = self.get_visible_df()?;

        // Use first group column as the "count source" — count() on any col gives row count
        let count_source = group_names[0].clone();
        let mut agg_exprs: Vec<Expr> = vec![col(&count_source).count().alias("Count")];

        // ── Per-column aggregators ─────────────────────────────────────────
        let mut extra_metas: Vec<crate::data::column::ColumnMeta> = Vec::new();
        let group_indices_set: std::collections::HashSet<usize> =
            group_col_indices.iter().cloned().collect();

        for &(agg_col_idx, ref aggregators) in aggregated_cols {
            if group_indices_set.contains(&agg_col_idx) {
                continue; // skip columns that are part of the grouping
            }
            let agg_col_name = self.columns[agg_col_idx].name.clone();
            let src_meta = &self.columns[agg_col_idx];
            for agg_kind in aggregators {
                if let Some(expr) = agg_kind.to_expr(&agg_col_name) {
                    let alias_name = format!("{}:{}", agg_col_name, agg_kind.name());
                    agg_exprs.push(expr.alias(&alias_name));
                    let mut meta = crate::data::column::ColumnMeta::new(alias_name);
                    if agg_kind.preserves_col_type() {
                        meta.col_type = src_meta.col_type;
                        meta.currency = src_meta.currency;
                        meta.precision = src_meta.precision;
                    } else {
                        meta.col_type = crate::types::ColumnType::Integer;
                    }
                    extra_metas.push(meta);
                }
            }
        }

        let group_exprs: Vec<Expr> = group_names.iter().map(col).collect();

        let grouped = visible
            .lazy()
            .group_by(group_exprs)
            .agg(agg_exprs)
            .sort(
                ["Count"],
                SortMultipleOptions::new().with_order_descending_multi([true]),
            )
            .collect()
            .map_err(|e| format!("multi group_by error: {}", e))?;

        // ── Pct and Bar ───────────────────────────────────────────────────
        let count_col = grouped.column("Count").map_err(|e| e.to_string())?;
        let total: f64 = count_col.as_materialized_series().sum::<u64>().unwrap_or(1) as f64;
        let max_count: usize = count_col
            .as_materialized_series()
            .max_reduce()
            .map_err(|e| e.to_string())?
            .value()
            .try_extract::<u64>()
            .unwrap_or(1) as usize;

        const BAR_WIDTH: usize = 20;
        let mut pct_values: Vec<f64> = Vec::with_capacity(grouped.height());
        let mut bar_values: Vec<String> = Vec::with_capacity(grouped.height());

        for i in 0..grouped.height() {
            let c = count_col
                .as_materialized_series()
                .get(i)
                .ok()
                .and_then(|v| v.try_extract::<u64>().ok())
                .unwrap_or(0) as usize;
            pct_values.push(c as f64 / total.max(1.0));
            bar_values.push(crate::app::build_bar(c, max_count, BAR_WIDTH));
        }

        let mut final_df = grouped.clone();
        final_df
            .with_column(Series::new("Pct".into(), &pct_values).into())
            .map_err(|e| e.to_string())?;
        final_df
            .with_column(Series::new("Bar".into(), &bar_values).into())
            .map_err(|e| e.to_string())?;

        // ── ColumnMeta ────────────────────────────────────────────────────
        let mut columns: Vec<crate::data::column::ColumnMeta> = Vec::new();

        for &idx in group_col_indices {
            let mut meta = self.columns[idx].clone();
            meta.aggregators.clear();
            meta.pinned = true;
            columns.push(meta);
        }

        let mut count_meta = crate::data::column::ColumnMeta::new("Count".to_string());
        count_meta.col_type = crate::types::ColumnType::Integer;
        columns.push(count_meta);

        for meta in extra_metas {
            columns.push(meta);
        }

        let mut pct_meta = crate::data::column::ColumnMeta::new("Pct".to_string());
        pct_meta.col_type = ColumnType::Percentage;
        pct_meta.precision = 1;
        columns.push(pct_meta);
        columns.push(crate::data::column::ColumnMeta::new("Bar".to_string()));

        Ok((final_df, columns))
    }

    /// Create a Pivot Table.
    pub fn create_pivot_table(
        &self,
        row_index_cols: &[String],
        pivot_col: &str,
        formula: &crate::data::expression::Expr,
    ) -> Result<
        (
            polars::prelude::DataFrame,
            Vec<crate::data::column::ColumnMeta>,
        ),
        String,
    > {
        use polars::prelude::*;
        use std::sync::Arc;

        let visible = self.get_visible_df()?;

        // 1. Group by [index_cols + pivot_col] and aggregate using formula
        let mut group_by_cols = row_index_cols.to_vec();
        if !group_by_cols.contains(&pivot_col.to_string()) {
            group_by_cols.push(pivot_col.to_string());
        }

        let polars_formula = formula.to_polars_expr()?;

        let grouped = visible
            .lazy()
            .group_by(group_by_cols.iter().map(col).collect::<Vec<_>>())
            .agg([polars_formula.alias("pivot_value")])
            .collect()
            .map_err(|e| format!("Pivot grouping error: {}", e))?;

        // 2. Pivot the result using LazyFrame::pivot (polars ≥0.53 API)
        // on_columns must be a DataFrame of unique pivot column values
        let pivot_series = grouped
            .column(pivot_col)
            .map_err(|e| e.to_string())?
            .as_materialized_series()
            .clone();
        let unique_vals = pivot_series.unique_stable().map_err(|e| e.to_string())?;
        let on_columns_df = Arc::new(
            DataFrame::new_infer_height(vec![unique_vals.into()]).map_err(|e| e.to_string())?,
        );

        let index_names: Arc<[PlSmallStr]> = row_index_cols
            .iter()
            .map(|s| PlSmallStr::from(s.as_str()))
            .collect::<Vec<_>>()
            .into();

        let pivoted = grouped
            .clone()
            .lazy()
            .pivot(
                Selector::ByName {
                    names: Arc::from([PlSmallStr::from(pivot_col)]),
                    strict: true,
                },
                on_columns_df,
                Selector::ByName {
                    names: index_names,
                    strict: true,
                },
                Selector::ByName {
                    names: Arc::from([PlSmallStr::from_static("pivot_value")]),
                    strict: true,
                },
                element().first(),
                true,
                "_".into(),
            )
            .collect()
            .map_err(|e| format!("Pivot error: {}", e))?;

        // 3. Build ColumnMeta
        // Infer the display type for value columns from the formula.
        // Simple aggregations inherit the source column's type; compound expressions → Float.
        let (value_col_type, value_precision, value_currency) = match formula {
            crate::data::expression::Expr::FunctionCall { name, args } => {
                let src = args.first().and_then(|a| {
                    if let crate::data::expression::Expr::ColumnRef(n) = a {
                        self.columns.iter().find(|c| &c.name == n)
                    } else {
                        None
                    }
                });
                match name.as_str() {
                    "count" => (ColumnType::Integer, 0u8, None),
                    "sum" | "min" | "max" | "mean" | "median" => match src {
                        Some(m) => (m.col_type, m.precision, m.currency),
                        None => (ColumnType::Float, 2, None),
                    },
                    _ => (ColumnType::Float, 2, None),
                }
            }
            _ => (ColumnType::Float, 2, None),
        };

        let mut columns = Vec::new();
        for i in 0..pivoted.width() {
            let series = &pivoted.columns()[i];
            let name = series.name().to_string();

            // Index columns: clone source meta to preserve col_type, currency, precision
            if row_index_cols.contains(&name) {
                let mut meta = self
                    .columns
                    .iter()
                    .find(|c| c.name == name)
                    .cloned()
                    .unwrap_or_else(|| crate::data::column::ColumnMeta::new(name));
                meta.aggregators.clear();
                meta.pinned = true;
                columns.push(meta);
                continue;
            }

            // Value columns: use inferred formula result type
            let mut meta = crate::data::column::ColumnMeta::new(name);
            meta.col_type = value_col_type;
            meta.precision = value_precision;
            meta.currency = value_currency;
            columns.push(meta);
        }

        Ok((pivoted, columns))
    }
}
