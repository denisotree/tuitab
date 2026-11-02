//! Turning a [`DataFrame`] into the JSON the model reads.
//!
//! Two rules drive this module.
//!
//! **Raw values, not display values.**  Rendering goes through
//! [`DataFrame::get_visible_df`], the typed frame — never the strings the screen
//! shows for Float, Currency and Percentage columns (`$1,234.50`), which would be
//! exactly wrong for JSON a model computes over.  Percentages therefore arrive as
//! fractions: `0.42`, not `"42%"`.  Files are written from the same typed frame.
//!
//! **Bounded output.**  A model that asks for a million rows should get an
//! answer, not a blown context window.  Rows are capped by count and by bytes,
//! and the response always says how many rows existed so a truncated answer is
//! never mistaken for a complete one.

use crate::data::dataframe::DataFrame;
use polars::prelude::AnyValue;
use serde_json::{json, Map, Value};

/// Rows returned when the caller does not say.
pub const DEFAULT_LIMIT: usize = 100;
/// Ceiling on the serialised row payload.  Rows stop being added once crossing
/// it, whatever `limit` said.
pub const MAX_BYTES: usize = 256 * 1024;

/// Convert one cell to JSON, keeping numbers as numbers.
fn cell(value: &AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => json!(b),
        AnyValue::Int8(i) => json!(i),
        AnyValue::Int16(i) => json!(i),
        AnyValue::Int32(i) => json!(i),
        AnyValue::Int64(i) => json!(i),
        AnyValue::UInt8(i) => json!(i),
        AnyValue::UInt16(i) => json!(i),
        AnyValue::UInt32(i) => json!(i),
        AnyValue::UInt64(i) => json!(i),
        // JSON has no NaN or infinity; emitting null keeps the document valid
        // and is honest about the value being absent rather than zero.
        AnyValue::Float32(f) => json!(f.is_finite().then_some(*f)),
        AnyValue::Float64(f) => json!(f.is_finite().then_some(*f)),
        AnyValue::String(s) => json!(s),
        AnyValue::StringOwned(s) => json!(s.as_str()),
        // Dates, datetimes, categoricals and the nested types all render
        // through tuitab's own formatter so they match what the TUI shows.
        other => json!(DataFrame::anyvalue_to_string_fmt(other)),
    }
}

/// Column descriptors: name plus the type tuitab inferred.
pub fn columns_json(df: &DataFrame) -> Vec<Value> {
    df.columns
        .iter()
        .map(|c| json!({"name": c.name, "type": c.col_type.name()}))
        .collect()
}

/// Render up to `limit` rows.
///
/// Returns the rows and whether anything was left out, so callers can decide
/// what to say about it.
fn rows_json(df: &DataFrame, limit: usize) -> Result<(Vec<Value>, bool), String> {
    let visible = df.get_visible_df()?;
    let height = visible.height();
    let series: Vec<_> = visible.columns().iter().collect();

    let mut rows = Vec::new();
    let mut bytes = 0usize;

    for r in 0..height.min(limit) {
        let row: Vec<Value> = series
            .iter()
            .map(|s| cell(&s.get(r).unwrap_or(AnyValue::Null)))
            .collect();

        bytes += serde_json::to_string(&row).map(|s| s.len()).unwrap_or(0);
        rows.push(Value::Array(row));

        if bytes >= MAX_BYTES {
            break;
        }
    }

    let truncated = rows.len() < height;
    Ok((rows, truncated))
}

/// The standard table payload every tool returns.
pub fn table(df: &DataFrame, limit: usize) -> Result<Value, String> {
    table_of(df, limit, df.row_order.len())
}

/// [`table`] for the first rows of a longer result — a streamed pipeline collects only
/// what it shows, and counts the rest.
pub fn table_of(df: &DataFrame, limit: usize, row_count: usize) -> Result<Value, String> {
    let (rows, _) = rows_json(df, limit)?;
    let truncated = rows.len() < row_count;

    let mut out = Map::new();
    out.insert("columns".into(), Value::Array(columns_json(df)));
    out.insert("rows".into(), Value::Array(rows.clone()));
    out.insert("row_count".into(), json!(row_count));
    out.insert("returned".into(), json!(rows.len()));
    out.insert("truncated".into(), json!(truncated));
    if truncated {
        out.insert(
            "note".into(),
            json!(format!(
                "Showing {} of {} rows. Raise 'output.limit', narrow the pipeline, \
                 or set 'output.path' to write the full result to a file.",
                rows.len(),
                row_count
            )),
        );
    }
    Ok(Value::Object(out))
}
