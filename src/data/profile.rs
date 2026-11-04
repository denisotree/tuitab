//! What a column's values say about its type — the rules every reader applies, written
//! once as expressions, so one pass answers them over a loaded frame and over a scan of
//! a file larger than memory alike.

use crate::data::column::EpochUnit;
use crate::data::column_expr;
use polars::prelude::*;

/// What a CSV writes for "no value" without saying so: `undefined` from a Grafana
/// export, `null`, `NaN`, `None`, `N/A`.  Not `NA` — it is Namibia's country code.
pub const NULL_TOKENS: &[&str] = &["undefined", "null", "NULL", "NaN", "None", "N/A", "n/a"];

fn trimmed(name: &str) -> Expr {
    column_expr(name).str().strip_chars(lit(Null {}))
}

fn is_token(x: Expr) -> Expr {
    x.is_in(
        lit(Series::new("tokens".into(), NULL_TOKENS)).implode(),
        false,
    )
}

/// A text column's value trimmed, with the null tokens and the empty string as NULL.
fn cleaned(name: &str) -> Expr {
    let t = trimmed(name);
    when(is_token(t.clone()).or(t.clone().eq(lit(""))))
        .then(lit(Null {}))
        .otherwise(t)
}

/// The statistics that settle which type the text `x` takes, named `{tag}…`.
fn type_stats(x: Expr, tag: &str) -> Vec<Expr> {
    let present = || x.clone().is_not_null();
    let failing = |dtype: DataType| {
        present()
            .and(x.clone().cast(dtype).is_null())
            .sum()
            .cast(DataType::UInt64)
    };
    vec![
        x.clone()
            .str()
            .contains(lit("^-?0[0-9]"), true)
            .any(true)
            .alias(format!("{tag}padded")),
        failing(DataType::Int64).alias(format!("{tag}int")),
        failing(DataType::Float64).alias(format!("{tag}float")),
        present()
            .sum()
            .cast(DataType::UInt64)
            .alias(format!("{tag}present")),
        present()
            .and(
                x.clone()
                    .eq(lit("true"))
                    .or(x.clone().eq(lit("false")))
                    .not(),
            )
            .sum()
            .cast(DataType::UInt64)
            .alias(format!("{tag}notbool")),
    ]
}

fn stat<'a>(stats: &'a polars::prelude::DataFrame, key: &str) -> Option<AnyValue<'a>> {
    stats.column(key).ok().and_then(|c| c.get(0).ok())
}

fn count(stats: &polars::prelude::DataFrame, key: &str) -> u64 {
    stat(stats, key)
        .and_then(|v| v.extract::<u64>())
        .unwrap_or(0)
}

fn truthy(stats: &polars::prelude::DataFrame, key: &str) -> bool {
    matches!(stat(stats, key), Some(AnyValue::Boolean(true)))
}

/// The narrowest type every value fits: Int64, then Float64, then — when asked —
/// Boolean (`true`/`false`); text otherwise.
///
/// A zero-padded number is not a number: `01234` is a postcode, an account, a product
/// code, and casting it would quietly hand back `1234`, so one such value keeps the
/// whole column text.
fn settled(stats: &polars::prelude::DataFrame, tag: &str, booleans: bool) -> DataType {
    // No values is no evidence of a number.
    if count(stats, &format!("{tag}present")) == 0 || truthy(stats, &format!("{tag}padded")) {
        DataType::String
    } else if count(stats, &format!("{tag}int")) == 0 {
        DataType::Int64
    } else if count(stats, &format!("{tag}float")) == 0 {
        DataType::Float64
    } else if booleans
        && count(stats, &format!("{tag}present")) > 0
        && count(stats, &format!("{tag}notbool")) == 0
    {
        DataType::Boolean
    } else {
        DataType::String
    }
}

/// The type the text `x` takes over the rows of `lf` — one pass.
pub fn text_type(lf: &LazyFrame, x: Expr, booleans: bool) -> PolarsResult<DataType> {
    let x_ = column_expr("__text");
    let stats = crate::data::collect(
        lf.clone()
            .select([x.alias("__text")])
            .select(type_stats(x_, "")),
    )?;
    Ok(settled(&stats, "", booleans))
}

/// The text `x` as `dtype`, a type [`text_type`] settled on.
pub fn as_type(x: Expr, dtype: &DataType) -> Expr {
    match dtype {
        DataType::Boolean => when(x.clone().is_null())
            .then(lit(Null {}).cast(DataType::Boolean))
            .otherwise(x.eq(lit("true"))),
        DataType::String => x,
        other => x.cast(other.clone()),
    }
}

/// A text series as the type its values settle on.
pub fn typed_text(text: &Series, booleans: bool) -> Series {
    if text.dtype() != &DataType::String {
        return text.clone();
    }
    let name = text.name().to_string();
    let lf = text.clone().into_frame().lazy();
    let x = column_expr(&name);
    text_type(&lf, x.clone(), booleans)
        .and_then(|dtype| {
            crate::data::collect(lf.select([as_type(x, &dtype).alias(name.as_str())]))
        })
        .ok()
        .and_then(|df| {
            df.columns()
                .first()
                .map(|c| c.as_materialized_series().clone())
        })
        .unwrap_or_else(|| text.clone())
}

/// The text columns holding a null token whose every other value is a number, and
/// the number type each becomes.  A column whose other values are text keeps every
/// value as written: a name `None` stays `None`.
pub fn token_casts(lf: &LazyFrame, schema: &Schema) -> PolarsResult<Vec<(String, DataType)>> {
    let text: Vec<String> = schema
        .iter()
        .filter(|(_, t)| **t == DataType::String)
        .map(|(n, _)| n.to_string())
        .collect();
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let mut exprs = Vec::new();
    for (i, name) in text.iter().enumerate() {
        exprs.push(is_token(trimmed(name)).any(true).alias(format!("t{i}")));
        exprs.extend(type_stats(cleaned(name), &format!("{i}_")));
    }
    let stats = crate::data::collect(lf.clone().select(exprs))?;
    Ok(text
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            truthy(&stats, &format!("t{i}")) && count(&stats, &format!("{i}_present")) > 0
        })
        .filter_map(|(i, name)| match settled(&stats, &format!("{i}_"), false) {
            DataType::String => None,
            dtype => Some((name.clone(), dtype)),
        })
        .collect())
}

/// Integer columns named like a time whose every value is an epoch in one unit.
pub fn epoch_columns(lf: &LazyFrame, schema: &Schema) -> PolarsResult<Vec<(String, EpochUnit)>> {
    let candidates: Vec<String> = schema
        .iter()
        .filter(|(n, t)| t.is_integer() && crate::data::column::names_a_time(n))
        .map(|(n, _)| n.to_string())
        .collect();
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let mut exprs = Vec::new();
    for (i, name) in candidates.iter().enumerate() {
        let c = column_expr(name).cast(DataType::Int64);
        exprs.push(c.clone().min().alias(format!("lo{i}")));
        exprs.push(c.max().alias(format!("hi{i}")));
    }
    let stats = crate::data::collect(lf.clone().select(exprs))?;
    let get = |key: String| stat(&stats, &key).and_then(|v| v.extract::<i64>());
    Ok(candidates
        .iter()
        .enumerate()
        .filter_map(|(i, name)| {
            let unit = EpochUnit::of_range(get(format!("lo{i}"))?, get(format!("hi{i}"))?)?;
            Some((name.clone(), unit))
        })
        .collect())
}

/// `lf` with NaN read as missing in its float columns: Polars reads the `NaN` of a CSV
/// as a number before any token is looked at, and one NaN makes every sum and average
/// NaN.  It is a null token like the others.
pub fn without_nan(lf: LazyFrame, schema: &Schema) -> LazyFrame {
    let floats: Vec<Expr> = schema
        .iter()
        .filter(|(_, t)| t.is_float())
        .map(|(n, _)| column_expr(n).fill_nan(lit(Null {})).alias(n.as_str()))
        .collect();
    if floats.is_empty() {
        lf
    } else {
        lf.with_columns(floats)
    }
}

/// `lf` with the casts [`token_casts`] found.
pub fn with_casts(lf: LazyFrame, casts: &[(String, DataType)]) -> LazyFrame {
    if casts.is_empty() {
        return lf;
    }
    lf.with_columns(
        casts
            .iter()
            .map(|(name, dtype)| cleaned(name).cast(dtype.clone()).alias(name.as_str()))
            .collect::<Vec<_>>(),
    )
}

/// `lf` with the epoch columns [`epoch_columns`] found as datetimes.
pub fn with_epochs(lf: LazyFrame, epochs: &[(String, EpochUnit)]) -> LazyFrame {
    if epochs.is_empty() {
        return lf;
    }
    lf.with_columns(
        epochs
            .iter()
            .map(|(name, unit)| {
                crate::data::expression::epoch_to_datetime(column_expr(name), *unit, true)
                    .alias(name.as_str())
            })
            .collect::<Vec<_>>(),
    )
}

/// Datetime columns with a time zone, the zone set aside: the same instants as UTC
/// without one.  Polars without its zone database cannot show, compare or format a
/// zoned value — some of it panics — and UTC is what tuitab shows anyway.  The
/// expressions that strip them, and each column's zone.
pub fn zone_free(schema: &Schema) -> (Vec<Expr>, Vec<(String, String)>) {
    schema
        .iter()
        .filter_map(|(n, t)| match t {
            DataType::Datetime(tu, Some(tz)) => Some((
                column_expr(n)
                    .to_physical()
                    .cast(DataType::Datetime(*tu, None))
                    .alias(n.as_str()),
                (n.to_string(), tz.to_string()),
            )),
            _ => None,
        })
        .unzip()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed(values: &[Option<&str>]) -> DataType {
        let s = Series::new("x".into(), values);
        typed_text(&s, true).dtype().clone()
    }

    /// The narrowest type every value fits, and no number out of no values.
    #[test]
    fn text_settles_on_the_narrowest_type() {
        assert_eq!(typed(&[Some("1"), Some("-2")]), DataType::Int64);
        assert_eq!(typed(&[Some("1"), Some("0.5")]), DataType::Float64);
        assert_eq!(
            typed(&[Some("true"), Some("false"), None]),
            DataType::Boolean
        );
        assert_eq!(typed(&[Some("1"), Some("a")]), DataType::String);
        assert_eq!(
            typed(&[None, None]),
            DataType::String,
            "no values, no number"
        );
    }

    /// A zero-padded number is a code: one keeps the column text.  Zero itself, and a
    /// fraction below one, are numbers.
    #[test]
    fn a_padded_number_keeps_its_column_text() {
        assert_eq!(typed(&[Some("01234"), Some("5")]), DataType::String);
        assert_eq!(typed(&[Some("-01"), Some("5")]), DataType::String);
        assert_eq!(typed(&[Some("0"), Some("5")]), DataType::Int64);
        assert_eq!(typed(&[Some("0.5"), Some("5")]), DataType::Float64);
    }

    /// A column of nothing but tokens is no column of numbers.
    #[test]
    fn a_column_of_only_tokens_is_left_as_text() {
        let df = df!("a" => ["undefined", "NaN"], "b" => ["1", "undefined"]).unwrap();
        let casts = token_casts(&df.clone().lazy(), df.schema()).unwrap();
        assert_eq!(casts, vec![("b".to_string(), DataType::Int64)]);
    }

    /// Epochs in each unit up to their last ten-digit, thirteen-digit … value.
    #[test]
    fn epochs_are_recognised_to_the_edges_of_their_units() {
        let df = df!(
            "ts" => [1_000_000_000i64, 9_999_999_999],
            "seen_at" => [1_000_000_000_000i64, 9_999_999_999_999],
            "time" => [5i64, 1_700_000_000]
        )
        .unwrap();
        let mut found = epoch_columns(&df.clone().lazy(), df.schema()).unwrap();
        found.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            found,
            vec![
                ("seen_at".to_string(), EpochUnit::Millis),
                ("ts".to_string(), EpochUnit::Seconds),
            ]
        );
    }
}
