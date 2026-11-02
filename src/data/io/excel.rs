use crate::data::dataframe::DataFrame;
use crate::data::io::wrap_polars_df;
use color_eyre::{eyre::eyre, Result};
use polars::prelude::*;
use std::path::Path;

pub(super) fn load_excel(path: &Path) -> Result<DataFrame> {
    use calamine::{open_workbook_auto, Reader};

    let mut workbook = open_workbook_auto(path)?;
    let sheet_names = workbook.sheet_names().to_owned();
    if sheet_names.is_empty() {
        return Err(eyre!("Excel file is empty"));
    }
    let range = workbook
        .worksheet_range(&sheet_names[0])
        .map_err(|e| eyre!("Cannot read first sheet: {e}"))?;

    parse_excel_range(range)
}

pub fn load_excel_sheet_by_name(path: &Path, sheet_name: &str) -> Result<DataFrame> {
    use calamine::{open_workbook_auto, Reader};

    let mut workbook = open_workbook_auto(path)?;
    let range = workbook
        .worksheet_range(sheet_name)
        .map_err(|e| eyre!("Sheet '{}' not found: {e}", sheet_name))?;

    parse_excel_range(range)
}

pub fn load_excel_overview(path: &Path) -> Result<DataFrame> {
    let names = excel_sheet_names(path)?;
    if names.is_empty() {
        return Err(eyre!("Excel file has no sheets"));
    }
    let pdf =
        polars::prelude::DataFrame::new_infer_height(vec![
            Series::new("Sheet".into(), &names).into()
        ])?;
    let mut df = wrap_polars_df(pdf)?;
    if !df.columns.is_empty() {
        df.columns[0].width = 40;
    }
    Ok(df)
}

pub fn excel_sheet_names(path: &Path) -> Result<Vec<String>> {
    use calamine::{open_workbook_auto, Reader};
    let workbook = open_workbook_auto(path)?;
    Ok(workbook.sheet_names().to_owned())
}

/// Every sheet with how much is in it: name, data rows, columns.
///
/// A listing that answers `rows: null, columns: 0` costs the caller a second call per
/// sheet purely to learn what it is looking at.  The first row is the header, so it is
/// not counted; a sheet with nothing in it at all answers zero and zero.
pub fn excel_sheet_sizes(path: &Path) -> Result<Vec<(String, usize, usize)>> {
    use calamine::{open_workbook_auto, Reader};
    let mut workbook = open_workbook_auto(path)?;
    let names = workbook.sheet_names().to_owned();
    Ok(names
        .into_iter()
        .map(|name| {
            let (rows, cols) = workbook
                .worksheet_range(&name)
                .map(|r| r.get_size())
                .unwrap_or((0, 0));
            (name, rows.saturating_sub(1), cols)
        })
        .collect())
}

pub(super) fn save_xlsx(df: &DataFrame, path: &Path) -> Result<()> {
    use rust_xlsxwriter::{Format, Workbook};

    let ordered_df = df
        .get_visible_df()
        .map_err(|e| color_eyre::eyre::eyre!(e))?;
    let col_names: Vec<String> = ordered_df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut workbook = Workbook::new();
    let sheet = workbook.add_worksheet();
    let header_fmt = Format::new().set_bold();

    for (ci, name) in col_names.iter().enumerate() {
        sheet
            .write_string_with_format(0, ci as u16, name, &header_fmt)
            .map_err(|e| eyre!("{}", e))?;
    }

    let nrows = ordered_df.height();
    let ncols = col_names.len();
    for row_idx in 0..nrows {
        for ci in 0..ncols {
            let series = &ordered_df.columns()[ci];
            // A missing value is a blank cell.  It used to be written as the word
            // "null", which is a perfectly good label for a spreadsheet to hold and
            // turned the whole column back into text when the file was read again.
            if matches!(
                series.get(row_idx),
                Ok(polars::prelude::AnyValue::Null) | Err(_)
            ) {
                continue;
            }
            let cell_text = series
                .get(row_idx)
                .map(|v| {
                    let s = format!("{}", v);
                    if s.starts_with('"') && s.ends_with('"') {
                        s[1..s.len() - 1].to_string()
                    } else {
                        s
                    }
                })
                .unwrap_or_default();
            if let Ok(n) = cell_text.parse::<f64>() {
                sheet
                    .write_number((row_idx + 1) as u32, ci as u16, n)
                    .map_err(|e| eyre!("{}", e))?;
            } else {
                sheet
                    .write_string((row_idx + 1) as u32, ci as u16, &cell_text)
                    .map_err(|e| eyre!("{}", e))?;
            }
        }
    }

    workbook.save(path).map_err(|e| eyre!("{}", e))?;
    Ok(())
}

/// A sheet column as the type its values actually are.
///
/// Every cell arrives here as text — calamine renders a number, a date and a label
/// alike — and a column left as text is a column no aggregate will touch: `sum` over a
/// spreadsheet's money column used to answer "the column is string". So the whole column
/// is offered to Int64 first and Float64 second, and keeps the text only when some cell
/// is not a number. An empty cell is absent, not the empty string, which is the reason
/// the cast has anything to bite on: `""` is not a number and would sink the column.
fn typed_series(name: &str, values: Vec<String>) -> Series {
    let cells: Vec<Option<String>> = values
        .into_iter()
        .map(|v| {
            let t = v.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        })
        .collect();
    let text = Series::new(name.into(), &cells);
    // A padded number is not a number: `01234` is a postcode, an account, a product
    // code, and casting it would quietly hand back `1234`.  Nothing that reaches a
    // spreadsheet cell as an actual number is written that way, so one such value is
    // enough to say the column is text.
    let padded = |s: &String| {
        let b = s.as_bytes();
        b.len() > 1 && b[0] == b'0' && b[1].is_ascii_digit()
    };
    if cells.iter().flatten().any(padded) {
        return text;
    }
    for target in [DataType::Int64, DataType::Float64] {
        if let Ok(cast) = text.strict_cast(&target) {
            return cast;
        }
    }
    text
}

/// One cell as text.
///
/// A date in a spreadsheet is a number with a format on it, and calamine renders it as
/// that number: the day tuitab showed as `46051` was the 29th of January 2026. What a
/// spreadsheet holds is a date, so it is written the way every other reader here writes
/// one — `2026-01-29`, or with the time when there is one — which is also what the CSV
/// loader leaves in a date column.
fn cell_text(cell: &calamine::Data) -> String {
    use calamine::{Data, DataType};
    match cell {
        Data::DateTime(_) | Data::DateTimeIso(_) => match cell.as_datetime() {
            Some(dt) if dt.time() == chrono::NaiveTime::MIN => dt.date().to_string(),
            Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            // A duration, or a serial the calendar cannot place — left as it came.
            None => cell.to_string(),
        },
        other => other.to_string(),
    }
}

fn parse_excel_range(range: calamine::Range<calamine::Data>) -> Result<DataFrame> {
    let all_rows: Vec<Vec<String>> = range
        .rows()
        .map(|row| row.iter().map(cell_text).collect())
        .collect();

    let mut iter = all_rows.into_iter();
    let header_row = iter
        .next()
        .ok_or_else(|| eyre!("Excel sheet has no headers"))?;

    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let headers: Vec<String> = header_row
        .into_iter()
        .enumerate()
        .map(|(i, h)| {
            let base = if h.is_empty() {
                format!("column_{}", i + 1)
            } else {
                h
            };
            let count = seen.entry(base.clone()).or_insert(0);
            *count += 1;
            if *count == 1 {
                base
            } else {
                format!("{}_{}", base, count)
            }
        })
        .collect();

    let col_count = headers.len();
    let mut cols_data: Vec<Vec<String>> = vec![Vec::new(); col_count];

    for row in iter {
        for (i, cell) in row.into_iter().enumerate() {
            if i < col_count {
                cols_data[i].push(cell);
            }
        }
    }

    let mut series_vec = Vec::new();
    for (i, col_data) in cols_data.into_iter().enumerate() {
        series_vec.push(typed_series(headers[i].as_str(), col_data).into());
    }

    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)?;
    wrap_polars_df(pdf)
}
