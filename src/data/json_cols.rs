//! JSON held in a text column: the types its values carry, and one column per key.

use crate::data::dataframe::DataFrame;
use polars::prelude::*;
use rayon::prelude::*;
use serde_json::Value as Json;

/// A text column offered to the narrowest type that holds every value: Int64, then
/// Float64, then — when asked — Boolean (`true`/`false`); text otherwise.
///
/// A zero-padded number is not a number: `01234` is a postcode, an account, a product
/// code, and casting it would quietly hand back `1234`, so one such value keeps the
/// whole column text.
pub fn type_from_text(text: &Series, booleans: bool) -> Series {
    let Ok(ca) = text.str() else {
        return text.clone();
    };
    let padded = ca.into_iter().flatten().any(|s| {
        let b = s.as_bytes();
        let digits = b.strip_prefix(b"-").unwrap_or(b);
        digits.len() > 1 && digits[0] == b'0' && digits[1].is_ascii_digit()
    });
    if padded {
        return text.clone();
    }
    for target in [DataType::Int64, DataType::Float64] {
        if let Ok(cast) = text.strict_cast(&target) {
            return cast;
        }
    }
    if booleans
        && ca.null_count() < ca.len()
        && ca
            .into_iter()
            .flatten()
            .all(|s| s == "true" || s == "false")
    {
        let flags: BooleanChunked = ca.into_iter().map(|v| v.map(|s| s == "true")).collect();
        return flags.with_name(text.name().clone()).into_series();
    }
    text.clone()
}

/// The subset of JSONPath `json_get` reads: `$`, then `.key`, `["key"]`, `['key']` or
/// `[index]`, any number of times.  Wildcards, recursive descent and filters are not
/// paths to one value, so they are refused rather than half-supported.
pub fn path_steps(path: &str) -> Result<Vec<Step>, String> {
    let bad = || {
        format!(
            "json_get(): {:?} is not a path. Write $ then .key, [\"key\"] or [index], as \
             \"$.geo.city\" or \"$.items[0]\"",
            path
        )
    };
    let rest = path.strip_prefix('$').ok_or_else(bad)?;
    let mut steps = Vec::new();
    let mut s = rest;
    while !s.is_empty() {
        if let Some(after) = s.strip_prefix('.') {
            let end = after.find(['.', '[']).unwrap_or(after.len());
            let key = &after[..end];
            if key.is_empty()
                || !key
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
            {
                return Err(bad());
            }
            steps.push(Step::Key(key.to_string()));
            s = &after[end..];
        } else if let Some(after) = s.strip_prefix('[') {
            let end = after.find(']').ok_or_else(bad)?;
            let inner = &after[..end];
            let quoted = |q: char| inner.strip_prefix(q).and_then(|i| i.strip_suffix(q));
            if let Some(key) = quoted('"').or_else(|| quoted('\'')) {
                steps.push(Step::Key(key.to_string()));
            } else if !inner.is_empty() && inner.bytes().all(|b| b.is_ascii_digit()) {
                steps.push(Step::Index(inner.parse().map_err(|_| bad())?));
            } else {
                return Err(bad());
            }
            s = &after[end + 1..];
        } else {
            return Err(bad());
        }
    }
    Ok(steps)
}

#[derive(Clone, Debug)]
pub enum Step {
    Key(String),
    Index(usize),
}

/// What `json_get` finds at `steps` in one cell, as the text a JSON scalar reads as —
/// a string's own text, a number or boolean as written, an object or array as compact
/// JSON — or `None` when there is nothing there.
pub fn get_text(cell: &str, steps: &[Step]) -> Option<String> {
    let doc: Json = serde_json::from_str(cell).ok()?;
    let mut at = &doc;
    for step in steps {
        at = match step {
            Step::Key(k) => at.get(k)?,
            Step::Index(i) => at.get(*i)?,
        };
    }
    scalar_text(at)
}

fn scalar_text(v: &Json) -> Option<String> {
    match v {
        Json::Null => None,
        Json::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// One column per top-level key of the JSON objects in `col`, named `prefix + key`,
/// inserted after `col`.  Every cell is parsed once, in parallel; the keys are every
/// key any row has, in the order they first appear.  A column keeps the JSON type its
/// values share — whole numbers, numbers, booleans, text — and a nested object or
/// array is its compact JSON text.  A cell that is not a JSON object contributes NULLs.
pub fn expand_json(df: &DataFrame, col: &str, prefix: &str) -> Result<DataFrame, String> {
    let col_idx = df.column_index(col)?;
    // The physical frame, every row — a sheet showing a filtered view keeps its hidden
    // rows, and the row order stays the sheet's own.
    let text = df.df.columns()[col_idx]
        .as_materialized_series()
        .cast(&DataType::String)
        .map_err(|e| e.to_string())?;
    let cells: Vec<Option<&str>> = text.str().map_err(|e| e.to_string())?.into_iter().collect();

    let parsed: Vec<Option<serde_json::Map<String, Json>>> = cells
        .par_iter()
        .map(
            |c| match c.and_then(|c| serde_json::from_str::<Json>(c).ok()) {
                Some(Json::Object(map)) => Some(map),
                _ => None,
            },
        )
        .collect();

    let mut keys: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for map in parsed.iter().flatten() {
        for k in map.keys() {
            if seen.insert(k.as_str()) {
                keys.push(k.clone());
            }
        }
    }
    if keys.is_empty() {
        return Err(format!("'{}' holds no JSON objects to expand", col));
    }
    for k in &keys {
        let name = format!("{}{}", prefix, k);
        if df.column_index(&name).is_ok() {
            return Err(format!(
                "expanding '{}' would make a column '{}', and the table has one — choose \
                 another prefix",
                col, name
            ));
        }
    }

    let new_columns: Vec<Series> = keys
        .par_iter()
        .map(|k| {
            let values: Vec<Option<&Json>> = parsed
                .iter()
                .map(|m| m.as_ref().and_then(|m| m.get(k)).filter(|v| !v.is_null()))
                .collect();
            typed_series(&format!("{}{}", prefix, k), &values)
        })
        .collect();

    let mut out = df.clone();
    for (offset, s) in new_columns.into_iter().enumerate() {
        let at = col_idx + 1 + offset;
        let mut meta = crate::data::column::ColumnMeta::new(s.name().to_string());
        meta.col_type = crate::data::io::column_type_of(s.dtype());
        out.df
            .insert_column(at, s.into_column())
            .map_err(|e| e.to_string())?;
        out.columns.insert(at, meta);
    }
    out.aggregates_cache = None;
    out.calc_widths(40, 1000);
    Ok(out)
}

/// A column from JSON values by the type they share.
fn typed_series(name: &str, values: &[Option<&Json>]) -> Series {
    let all = |f: fn(&Json) -> bool| values.iter().flatten().all(|v| f(v));
    if values.iter().all(Option::is_none) {
        return Series::new(
            name.into(),
            values.iter().map(|_| None::<String>).collect::<Vec<_>>(),
        );
    }
    if all(|v| v.as_i64().is_some()) {
        let ints: Vec<Option<i64>> = values.iter().map(|v| v.and_then(Json::as_i64)).collect();
        return Series::new(name.into(), ints);
    }
    if all(Json::is_number) {
        let floats: Vec<Option<f64>> = values.iter().map(|v| v.and_then(Json::as_f64)).collect();
        return Series::new(name.into(), floats);
    }
    if all(Json::is_boolean) {
        let flags: Vec<Option<bool>> = values.iter().map(|v| v.and_then(Json::as_bool)).collect();
        return Series::new(name.into(), flags);
    }
    let texts: Vec<Option<String>> = values.iter().map(|v| v.and_then(scalar_text)).collect();
    Series::new(name.into(), texts)
}
