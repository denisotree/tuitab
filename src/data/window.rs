//! Window functions — a value computed per row from the rows around it.
//!
//! # Row order is the window's order
//!
//! `cum_sum`, `lag`, `lead` and `row_number` answer "relative to what came
//! before", so what came before has to be settled first. By default these read
//! the frame **in its current order**, which is the order the file happened to
//! be written unless something sorted it.
//!
//! That contract only holds because a sort materialises its result. A sort that
//! merely reordered a view would leave the window looking at the untouched
//! frame underneath and quietly computing the wrong thing — the same class of
//! mistake as an aggregate ignoring a filter.
//!
//! [`Spec::order_by`] is the other way round: it orders the rows *for the
//! window only* and hands back the answer in the table's own order. A running
//! total by date on a table the user wants left alone is the case it exists
//! for — SQL's `OVER (PARTITION BY … ORDER BY …)`. Ties keep their relative
//! order and nulls sort last, so the same question gives the same answer twice.
//!
//! # Partitions
//!
//! `over` restarts the window for each distinct combination of those columns:
//! a running total per region, a rank within each department. Without it the
//! window spans the whole table.

use crate::data::column::ColumnMeta;
use crate::data::dataframe::DataFrame;
use crate::types::ColumnType;
use polars::prelude::*;

/// What to compute over the window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowFn {
    /// Position within the partition, starting at 1.
    RowNumber,
    /// Rank by value; ties share a rank and leave a gap after them.
    Rank,
    /// Rank by value; ties share a rank and the next value follows immediately.
    DenseRank,
    /// Running total in the current row order.
    CumSum,
    /// The value `offset` rows earlier.
    Lag,
    /// The value `offset` rows later.
    Lead,
    /// The partition's sum, repeated on every row of it.
    Sum,
    Avg,
    Min,
    Max,
    Count,
    /// The row's share of its partition's total.
    PctOfTotal,
    /// The partition's median, repeated on every row of it.
    Median,
    /// The partition's median absolute deviation from its median.
    Mad,
}

impl WindowFn {
    pub fn parse(name: &str) -> Result<Self, String> {
        Ok(match name {
            "row_number" => Self::RowNumber,
            "rank" => Self::Rank,
            "dense_rank" => Self::DenseRank,
            "cum_sum" => Self::CumSum,
            "lag" => Self::Lag,
            "lead" => Self::Lead,
            "sum" => Self::Sum,
            "avg" => Self::Avg,
            "min" => Self::Min,
            "max" => Self::Max,
            "count" => Self::Count,
            "pct_of_total" => Self::PctOfTotal,
            "median" => Self::Median,
            "mad" => Self::Mad,
            other => {
                return Err(format!(
                    "Unknown window function '{}'. Available: row_number, rank, dense_rank, \
                     cum_sum, lag, lead, sum, avg, min, max, count, pct_of_total, median, mad",
                    other
                ))
            }
        })
    }

    /// Every function, in the order a picker should list them.
    pub fn all() -> &'static [WindowFn] {
        &[
            Self::RowNumber,
            Self::Rank,
            Self::DenseRank,
            Self::CumSum,
            Self::Lag,
            Self::Lead,
            Self::Sum,
            Self::Avg,
            Self::Min,
            Self::Max,
            Self::Count,
            Self::PctOfTotal,
            Self::Median,
            Self::Mad,
        ]
    }

    /// One line for a picker.
    pub fn describe(self) -> &'static str {
        match self {
            Self::RowNumber => "position in the group",
            Self::Rank => "rank by value, ties share and leave a gap",
            Self::DenseRank => "rank by value, ties share, no gap",
            Self::CumSum => "running total in the current order",
            Self::Lag => "the previous row's value",
            Self::Lead => "the next row's value",
            Self::Sum => "the group's total on every row",
            Self::Avg => "the group's mean on every row",
            Self::Min => "the group's smallest on every row",
            Self::Max => "the group's largest on every row",
            Self::Count => "how many rows in the group",
            Self::PctOfTotal => "this row's share of the group",
            Self::Median => "the group's median on every row",
            Self::Mad => "the group's median absolute deviation on every row",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::RowNumber => "row_number",
            Self::Rank => "rank",
            Self::DenseRank => "dense_rank",
            Self::CumSum => "cum_sum",
            Self::Lag => "lag",
            Self::Lead => "lead",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Count => "count",
            Self::PctOfTotal => "pct_of_total",
            Self::Median => "median",
            Self::Mad => "mad",
        }
    }

    /// Whether the function reads the rows in order, so that [`Spec::order_by`]
    /// changes the answer.
    ///
    /// A running total, a neighbour and a position all mean "relative to what
    /// came before". The aggregates and the ranks do not: a sum is a sum
    /// whatever order it is added in, and a rank orders by the value it reads.
    pub fn uses_order_by(self) -> bool {
        matches!(
            self,
            Self::RowNumber | Self::CumSum | Self::Lag | Self::Lead
        )
    }

    /// Whether [`Spec::desc`] changes the answer.
    ///
    /// Two different meanings, one field, because a function is never both: for
    /// the ranks it is which end ranks first, and for the ordered functions it
    /// is which way [`Spec::order_by`] runs. Everything else is an aggregate
    /// over a set, where direction means nothing.
    pub fn uses_direction(self) -> bool {
        self.uses_order_by() || matches!(self, Self::Rank | Self::DenseRank)
    }

    /// Whether the function reads values from a column, or only positions.
    ///
    /// `row_number` counts rows and does not care what is in them.
    fn needs_a_column(self) -> bool {
        self != Self::RowNumber
    }

    /// Whether it only makes sense over numbers.
    fn needs_numbers(self) -> bool {
        matches!(
            self,
            Self::CumSum | Self::Sum | Self::Avg | Self::PctOfTotal | Self::Median | Self::Mad
        )
    }

    /// The type of the column it produces.
    ///
    /// Only `pct_of_total` is a share. An average is in the source column's
    /// units — typing it as a percentage renders an average salary of 80428.79
    /// as "8042878.57%".
    fn output_type(self, source: Option<ColumnType>) -> ColumnType {
        match self {
            Self::RowNumber | Self::Rank | Self::DenseRank | Self::Count => ColumnType::Integer,
            Self::PctOfTotal => ColumnType::Percentage,
            // An average of integers is fractional; everything else that
            // carries a value forward keeps the source column's type.
            Self::Avg => match source {
                Some(ColumnType::Integer) | None => ColumnType::Float,
                Some(other) => other,
            },
            _ => source.unwrap_or(ColumnType::Float),
        }
    }
}

/// One window computation.
pub struct Spec {
    pub function: WindowFn,
    /// The column to read. Ignored by `row_number`.
    pub col: Option<String>,
    /// Columns whose distinct combinations restart the window.
    pub over: Vec<String>,
    /// Order the rows by these columns *for the window only*, handing the
    /// answer back in the table's own order.
    ///
    /// Empty means "read the frame as it stands", which is what the ranks and
    /// the partition aggregates want — only [`WindowFn::uses_order_by`]
    /// functions read this.
    pub order_by: Vec<String>,
    /// Name for the new column; defaults to `col:fn` or the function's name.
    pub as_name: Option<String>,
    /// `rank` descending, so the largest value ranks first.
    pub desc: bool,
    /// How far `lag` and `lead` reach.
    pub offset: i64,
}

impl Spec {
    fn output_name(&self) -> String {
        if let Some(name) = &self.as_name {
            return name.clone();
        }
        match &self.col {
            Some(col) => format!("{}:{}", col, self.function.name()),
            None => self.function.name().to_string(),
        }
    }
}

/// Add a window column to the visible rows.
///
/// The result is a fresh frame: the window is computed over the rows as they
/// are currently ordered and filtered, not over everything the file holds.
pub fn add_window_column(df: &DataFrame, spec: &Spec) -> Result<DataFrame, String> {
    let source_meta = match &spec.col {
        Some(name) => Some(df.columns[df.column_index(name)?].clone()),
        None => {
            if spec.function.needs_a_column() {
                return Err(format!("'{}' needs a column to read", spec.function.name()));
            }
            None
        }
    };

    if spec.function.needs_numbers() {
        if let Some(meta) = &source_meta {
            let numeric = matches!(
                meta.col_type,
                ColumnType::Integer
                    | ColumnType::Float
                    | ColumnType::Percentage
                    | ColumnType::Currency
            );
            if !numeric {
                return Err(format!(
                    "Cannot compute {} over '{}': the column is {}, and {} needs a numeric one",
                    spec.function.name(),
                    meta.name,
                    meta.col_type.name(),
                    spec.function.name()
                ));
            }
        }
    }

    for name in &spec.over {
        df.column_index(name)?;
    }
    for name in &spec.order_by {
        df.column_index(name)?;
    }

    // Silently ignoring it would answer a different question than the one
    // asked: in SQL `RANK() OVER (ORDER BY x)` ranks *by* x, so the request is
    // a reasonable one to make and a bad one to drop on the floor.
    if !spec.order_by.is_empty() && !spec.function.uses_order_by() {
        return Err(format!(
            "'{}' does not read the rows in order, so order_by would change nothing — \
             drop it, or use one of row_number, cum_sum, lag, lead",
            spec.function.name()
        ));
    }

    // `with_column` replaces a column of the same name, but the metadata below
    // is appended either way — leaving one more header than there are cells in
    // a row, and a reader whose columns no longer line up with its values.
    let name = spec.output_name();
    if df.column_index(&name).is_ok() {
        return Err(format!(
            "'{}' already exists — give the new column a different name with 'as'",
            name
        ));
    }

    let target = spec
        .col
        .as_deref()
        .map(crate::data::column_expr)
        .unwrap_or_else(|| lit(1));

    let computed = match spec.function {
        // Counting positions needs an expression of the frame's length that is
        // never null: `cum_count` skips nulls, and a literal stays length-1 —
        // it broadcasts to the first row of each partition and leaves the rest
        // at zero. `is_null()` on any column is exactly that: one boolean per
        // row, none of them missing, whatever the column holds.
        WindowFn::RowNumber => {
            let anchor = df
                .columns
                .first()
                .ok_or("row_number needs a table with at least one column")?;
            crate::data::column_expr(&anchor.name)
                .is_null()
                .cum_count(false)
        }
        WindowFn::Rank => target.clone().rank(
            RankOptions {
                method: RankMethod::Min,
                descending: spec.desc,
            },
            None,
        ),
        WindowFn::DenseRank => target.clone().rank(
            RankOptions {
                method: RankMethod::Dense,
                descending: spec.desc,
            },
            None,
        ),
        WindowFn::CumSum => target.clone().cast(DataType::Float64).cum_sum(false),
        WindowFn::Lag => target.clone().shift(lit(spec.offset)),
        WindowFn::Lead => target.clone().shift(lit(-spec.offset)),
        WindowFn::Sum => target.clone().sum(),
        WindowFn::Avg => target.clone().mean(),
        WindowFn::Min => target.clone().min(),
        WindowFn::Max => target.clone().max(),
        WindowFn::Count => target.clone().count(),
        WindowFn::PctOfTotal => {
            target.clone().cast(DataType::Float64) / target.clone().sum().cast(DataType::Float64)
        }
        WindowFn::Median => target.clone().cast(DataType::Float64).median(),
        WindowFn::Mad => {
            let x = target.clone().cast(DataType::Float64);
            (x.clone() - x.median()).abs().median()
        }
    };

    let windowed = if spec.over.is_empty() {
        computed
    } else {
        let partitions: Vec<Expr> = spec
            .over
            .iter()
            .map(|s| crate::data::column_expr(s))
            .collect();
        computed.over(partitions)
    }
    .alias(name.as_str());

    let visible = df.get_visible_df()?;
    let mut out = if spec.order_by.is_empty() {
        visible
            .lazy()
            .with_column(windowed)
            .collect()
            .map_err(|e| format!("window function failed: {}", e))?
    } else {
        // Compute on rows put in the asked-for order, then undo the move. The
        // marker rides along through both sorts and carries every row back to
        // where the table has it, so the answer arrives ordered by date while
        // the table stays as the user left it.
        let mut marker = "__tuitab_window_pos".to_string();
        while visible.column(&marker).is_ok() {
            marker.push('_');
        }

        let mut staged = visible;
        let positions: Vec<u32> = (0..staged.height() as u32).collect();
        staged
            .with_column(Series::new(marker.as_str().into(), positions).into())
            .map_err(|e| e.to_string())?;

        // `maintain_order` so two rows sharing a date always split the running
        // total the same way, and nulls last so a missing date cannot land at
        // the front and claim everything came after it.
        let ordered = staged
            .sort(
                spec.order_by.clone(),
                SortMultipleOptions::new()
                    .with_order_descending(spec.desc)
                    .with_nulls_last(true)
                    .with_maintain_order(true),
            )
            .map_err(|e| format!("ordering the window failed: {}", e))?;

        ordered
            .lazy()
            .with_column(windowed)
            .collect()
            .map_err(|e| format!("window function failed: {}", e))?
            .sort([marker.as_str()], SortMultipleOptions::new())
            .map_err(|e| format!("restoring the row order failed: {}", e))?
    };

    // Beside the column it describes, not at the far right — on a wide table
    // the answer would otherwise land off-screen.
    let mut metas = df.columns.clone();
    let insert_at = spec
        .col
        .as_deref()
        .and_then(|c| df.column_index(c).ok())
        .map(|i| i + 1)
        .unwrap_or(metas.len());

    let mut meta = ColumnMeta::new(name.clone());
    meta.col_type = spec
        .function
        .output_type(source_meta.as_ref().map(|m| m.col_type));
    if matches!(meta.col_type, ColumnType::Percentage) {
        meta.precision = 2;
    } else if let Some(source) = &source_meta {
        meta.currency = source.currency;
        meta.precision = source.precision;
    }
    metas.insert(insert_at, meta);

    // `with_column` appended it; reorder the frame to match the metadata.
    let order: Vec<String> = metas.iter().map(|m| m.name.clone()).collect();
    out = out.select(order).map_err(|e| e.to_string())?;

    let mut result = DataFrame::from_parts(out, metas);

    // A window adds a column; it does not reload the table. `from_parts` starts
    // a derived table from scratch — identity row order, nothing selected,
    // unmodified — which is right for a group-by and wrong here: rebasing
    // `original_order` onto the sorted rows meant `r` could no longer restore
    // the file's own order, and the selection vanished under the user.
    //
    // Carrying the old values across directly does not work either. The window
    // is computed over the *visible* frame, so row `j` of the result is what
    // used to be physical row `row_order[j]` — every stored index refers to the
    // old numbering and has to be translated to the new one.
    let mut new_index = vec![usize::MAX; df.df.height()];
    for (position, &physical) in df.row_order.iter().enumerate() {
        new_index[physical] = position;
    }
    let translate = |physical: usize| {
        new_index
            .get(physical)
            .copied()
            .filter(|i| *i != usize::MAX)
    };

    result.original_order = std::sync::Arc::new(
        df.original_order
            .iter()
            .filter_map(|p| translate(*p))
            .collect(),
    );
    result.selected_rows = df
        .selected_rows
        .iter()
        .filter_map(|p| translate(*p))
        .collect();
    result.modified = df.modified;
    result.transposed = df.transposed;

    result.calc_widths(40, 1000);
    Ok(result)
}
