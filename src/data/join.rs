use crate::data::dataframe::DataFrame;
use crate::data::io::wrap_polars_df;
use crate::types::ColumnType;
use color_eyre::eyre::eyre;
use color_eyre::Result;
use polars::prelude::{
    col, lit, when, DataFrameJoinOps, DataType, Expr, IntoLazy, JoinArgs, JoinCoalesce,
    JoinType as PolarsJoinType, SortMultipleOptions, TimeUnit, NULL,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
    Anti,
    Semi,
    Diff,
}

impl JoinType {
    pub fn all() -> &'static [JoinType] {
        &[
            Self::Inner,
            Self::Left,
            Self::Right,
            Self::Outer,
            Self::Anti,
            Self::Semi,
            Self::Diff,
        ]
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Inner => "INNER  (only matching rows)",
            Self::Left => "LEFT   (all left rows)",
            Self::Right => "RIGHT  (all right rows)",
            Self::Outer => "OUTER  (all rows from both)",
            Self::Anti => "ANTI   (left rows missing in right)",
            Self::Semi => "SEMI   (left rows found in right)",
            Self::Diff => "DIFF   (compare rows, like git diff)",
        }
    }

    /// What goes between the two table names in the result's title.
    pub fn sql_name(self) -> &'static str {
        match self {
            Self::Anti => "ANTI JOIN",
            Self::Semi => "SEMI JOIN",
            Self::Diff => "DIFF",
            _ => "JOIN",
        }
    }
}

/// Key dtypes that can meet in a join, each with the dtype both sides are cast to.
///
/// Polars refuses i32 against i64, and the user cannot fix that: both columns
/// show as `integer`, so retyping one is a no-op.  Within a family the cast is
/// ours to make; across families (integer against float, string against date)
/// the choice of how values correspond is the user's.
fn family(dtype: &DataType) -> DataType {
    match dtype {
        // ponytail: UInt64 above i64::MAX becomes null on the cast; a u64 key column is rare.
        d if d.is_integer() => DataType::Int64,
        d if d.is_float() => DataType::Float64,
        DataType::Datetime(_, _) => DataType::Datetime(TimeUnit::Microseconds, None),
        d => d.clone(),
    }
}

fn type_name(df: &DataFrame, col: &str) -> String {
    df.columns
        .iter()
        .find(|c| c.name == col)
        .map(|c| c.col_type.name().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Why `left_key` and `right_key` cannot be joined on, or `None` when they can.
///
/// A missing column is not a type conflict — the join itself reports that.
pub fn key_type_mismatch(
    left: &DataFrame,
    left_key: &str,
    right: &DataFrame,
    right_key: &str,
) -> Option<String> {
    let l = left.df.column(left_key).ok()?.dtype();
    let r = right.df.column(right_key).ok()?.dtype();
    if family(l) == family(r) {
        return None;
    }
    let (ln, rn) = (type_name(left, left_key), type_name(right, right_key));
    Some(if left_key == right_key {
        format!("key \"{left_key}\" is {ln} on the left but {rn} on the right")
    } else {
        format!("left key \"{left_key}\" is {ln} but right key \"{right_key}\" is {rn}")
    })
}

/// Both frames with their key columns ready to meet: refused when a pair's types
/// cannot, cast to one dtype when they differ only in width.
fn aligned_frames(
    left: &DataFrame,
    right: &DataFrame,
    left_keys: &[String],
    right_keys: &[String],
    verb: &str,
) -> Result<(polars::prelude::DataFrame, polars::prelude::DataFrame)> {
    for (lk, rk) in left_keys.iter().zip(right_keys) {
        if let Some(why) = key_type_mismatch(left, lk, right, rk) {
            return Err(eyre!(
                "cannot {verb}: {why}. Key types must match — change one column's type and {verb} again"
            ));
        }
    }

    let mut left_df = left.df.clone();
    let mut right_df = right.df.clone();
    for (lk, rk) in left_keys.iter().zip(right_keys) {
        let (Ok(l), Ok(r)) = (left_df.column(lk), right_df.column(rk)) else {
            continue; // the join names the missing column
        };
        if l.dtype() != r.dtype() {
            let target = family(l.dtype());
            let l = l.cast(&target)?;
            let r = r.cast(&target)?;
            left_df.with_column(l)?;
            right_df.with_column(r)?;
        }
    }
    Ok((left_df, right_df))
}

pub fn join_dataframes(
    left: &DataFrame,
    right: &DataFrame,
    left_keys: &[String],
    right_keys: &[String],
    join_type: JoinType,
) -> Result<DataFrame> {
    if join_type == JoinType::Diff {
        return diff_dataframes(left, right, left_keys, right_keys);
    }
    let (left_df, right_df) = aligned_frames(left, right, left_keys, right_keys, "join")?;

    let left_key_strs: Vec<&str> = left_keys.iter().map(|s| s.as_str()).collect();
    let right_key_strs: Vec<&str> = right_keys.iter().map(|s| s.as_str()).collect();

    let mut args = JoinArgs::new(match join_type {
        JoinType::Inner => PolarsJoinType::Inner,
        JoinType::Left => PolarsJoinType::Left,
        JoinType::Right => PolarsJoinType::Right,
        JoinType::Outer => PolarsJoinType::Full,
        JoinType::Anti => PolarsJoinType::Anti,
        JoinType::Semi | JoinType::Diff => PolarsJoinType::Semi,
    });
    // ANTI and SEMI answer "is this row in the other table", where a NULL key is a
    // value like any other.  The rest keep SQL's NULL <> NULL.
    args.nulls_equal = matches!(join_type, JoinType::Anti | JoinType::Semi);

    let result = left_df.join(&right_df, &left_key_strs, &right_key_strs, args, None)?;

    wrap_polars_df(result)
}

/// The status column of a DIFF result.
pub const DIFF_COL: &str = "_diff";
/// Suffix of the column holding the right table's value of a compared column.
pub const RIGHT_SUFFIX: &str = "_right";

const LEFT_ROW: &str = "__tuitab_diff_left_row";
const RIGHT_ROW: &str = "__tuitab_diff_right_row";

/// Row counts per DIFF status, in the order `=`, `~`, `-`, `+`.
pub fn diff_counts(df: &DataFrame) -> [usize; 4] {
    let mut counts = [0; 4];
    if let Ok(status) = df.df.column(DIFF_COL).and_then(|c| c.str().cloned()) {
        for s in status.into_iter().flatten() {
            if let Some(i) = ["=", "~", "-", "+"].iter().position(|k| *k == s) {
                counts[i] += 1;
            }
        }
    }
    counts
}

fn refuse_repeated_keys(
    df: &polars::prelude::DataFrame,
    keys: &[String],
    side: &str,
) -> Result<()> {
    let repeated = df
        .clone()
        .lazy()
        .group_by(keys.iter().map(|k| col(k.as_str())).collect::<Vec<_>>())
        .agg([polars::prelude::len().alias("n")])
        .filter(col("n").gt(lit(1)))
        .collect()?;
    if repeated.height() > 0 {
        return Err(eyre!(
            "cannot diff: key ({}) repeats in the {side} table ({} of its values {}). \
             Add columns to the key so each row is unique",
            keys.join(", "),
            repeated.height(),
            if repeated.height() == 1 {
                "occurs more than once"
            } else {
                "occur more than once"
            }
        ));
    }
    Ok(())
}

/// The type a column is stored as, without the display flavour on top.
fn storage(t: ColumnType) -> ColumnType {
    match t {
        ColumnType::Percentage | ColumnType::Currency => ColumnType::Float,
        ColumnType::FileSize => ColumnType::Integer,
        t => t,
    }
}

/// The left table compared with the right one row by row, like `git diff`.
///
/// The keys say which rows are the same row; every other column both tables have
/// is compared.  `_diff` holds `=` (same), `~` (changed), `-` (only on the left) or
/// `+` (only on the right).  A compared column's right-hand value sits in
/// `<col>_right`, filled on `~` rows only.  Left rows keep their order; added rows
/// follow.  NULL equals NULL, in keys and in cells.
pub fn diff_dataframes(
    left: &DataFrame,
    right: &DataFrame,
    left_keys: &[String],
    right_keys: &[String],
) -> Result<DataFrame> {
    for side in [left, right] {
        if side.columns.iter().any(|c| c.name == DIFF_COL) {
            return Err(eyre!(
                "cannot diff: a table already has a column named {DIFF_COL}; rename it first"
            ));
        }
    }
    let (left_df, right_df) = aligned_frames(left, right, left_keys, right_keys, "diff")?;
    refuse_repeated_keys(&left_df, left_keys, "left")?;
    refuse_repeated_keys(&right_df, right_keys, "right")?;

    let right_names: Vec<String> = right_df
        .get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect();
    // Compared: non-key columns on both sides.  A right key column is coalesced
    // away by the join, so it has no `_right` twin to compare with.
    let compared: Vec<(String, bool)> = left_df
        .columns()
        .iter()
        .filter(|c| !left_keys.iter().any(|k| k == c.name().as_str()))
        .filter(|c| {
            right_names.iter().any(|r| r == c.name().as_str())
                && !right_keys.iter().any(|k| k == c.name().as_str())
        })
        .map(|c| {
            let same_type = right_df.column(c.name()).map(|r| r.dtype() == c.dtype());
            (c.name().to_string(), same_type.unwrap_or(false))
        })
        .collect();

    let left_df = left_df.with_row_index(LEFT_ROW.into(), None)?;
    let right_df = right_df.with_row_index(RIGHT_ROW.into(), None)?;
    let mut args = JoinArgs::new(PolarsJoinType::Full).with_coalesce(JoinCoalesce::CoalesceColumns);
    args.nulls_equal = true;
    let left_on: Vec<Expr> = left_keys.iter().map(|k| col(k.as_str())).collect();
    let right_on: Vec<Expr> = right_keys.iter().map(|k| col(k.as_str())).collect();

    let right_col = |c: &str| col(format!("{c}{RIGHT_SUFFIX}"));
    let changed = compared.iter().fold(lit(false), |acc, (c, same_type)| {
        let (l, r) = if *same_type {
            (col(c.as_str()), right_col(c))
        } else {
            // ponytail: values of different types are compared as text.
            (
                col(c.as_str()).cast(DataType::String),
                right_col(c).cast(DataType::String),
            )
        };
        acc.or(l.neq_missing(r))
    });
    let status = when(col(LEFT_ROW).is_null())
        .then(lit("+"))
        .when(col(RIGHT_ROW).is_null())
        .then(lit("-"))
        .when(changed)
        .then(lit("~"))
        .otherwise(lit("="))
        .alias(DIFF_COL);

    let is = |s: &str| col(DIFF_COL).eq(lit(s));
    let mut reshaped = Vec::new();
    for (c, same_type) in &compared {
        if *same_type {
            // An added row reads like any other: its values in the table's own columns.
            reshaped.push(
                when(is("+"))
                    .then(right_col(c))
                    .otherwise(col(c.as_str()))
                    .alias(c.as_str()),
            );
            reshaped.push(
                when(is("~"))
                    .then(right_col(c))
                    .otherwise(lit(NULL))
                    .alias(format!("{c}{RIGHT_SUFFIX}")),
            );
        } else {
            // Nowhere to move a value of another type: an added row keeps it on the right.
            reshaped.push(
                when(is("~").or(is("+")))
                    .then(right_col(c))
                    .otherwise(lit(NULL))
                    .alias(format!("{c}{RIGHT_SUFFIX}")),
            );
        }
    }

    let joined = left_df
        .lazy()
        .join(right_df.lazy(), left_on, right_on, args)
        .with_column(status)
        .with_columns(reshaped)
        .sort(
            [LEFT_ROW, RIGHT_ROW],
            SortMultipleOptions::default().with_nulls_last(true),
        )
        .collect()?;

    let order: Vec<String> = std::iter::once(DIFF_COL.to_string())
        .chain(
            joined
                .get_column_names()
                .iter()
                .map(|n| n.to_string())
                .filter(|n| n != DIFF_COL && n != LEFT_ROW && n != RIGHT_ROW),
        )
        .collect();
    let result = joined.select(order)?;

    let mut out = wrap_polars_df(result)?;
    // Keep each column's own type label (currency, percentage…) where it survived.
    for meta in out.columns.iter_mut() {
        let base = meta.name.strip_suffix(RIGHT_SUFFIX).unwrap_or(&meta.name);
        if let Some(src) = left.columns.iter().find(|c| c.name == base) {
            if storage(meta.col_type) == storage(src.col_type) {
                meta.col_type = src.col_type;
                meta.currency = src.currency;
                meta.precision = src.precision;
            }
        }
    }
    if let Some(first) = out.columns.first_mut() {
        first.pinned = true;
    }
    Ok(out)
}
