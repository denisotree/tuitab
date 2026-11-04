//! Per-column statistical profile — what the `I` key shows, as a function.
//!
//! Lives here rather than in `App` because it is arithmetic over a
//! [`DataFrame`] and nothing else: the TUI wraps the result in a sheet, the MCP
//! server renders it as JSON, and neither needs the other.
//!
//! Two things about the numbers are worth knowing before relying on them.
//!
//! * `nulls` counts **empty strings**, and `count` counts non-empty ones.  At
//!   this layer there is no null concept distinct from an empty cell.
//! * `stdev` is the **population** standard deviation (divides by `n`), while
//!   [`crate::data::aggregator::AggregatorKind::Stdev`] in the footer is the
//!   sample one (divides by `n-1`).  Same word, two numbers.  Callers that show
//!   this to someone who cannot see the source should say which one it is.

use crate::data::column::ColumnMeta;
use crate::data::dataframe::DataFrame;
use indexmap::IndexMap;
use std::collections::HashSet;

/// Metric rows, in output order.
pub const METRICS: [&str; 16] = [
    "type", "count", "nulls", "unique", "min", "max", "mean", "median", "mode", "stdev", "range",
    "q5", "q25", "q50", "q75", "q95",
];

fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let k = (sorted.len() as f64 - 1.0) * q;
    let f = k.floor() as usize;
    let c = k.ceil() as usize;
    if f == c {
        sorted[f]
    } else {
        sorted[f] * (c as f64 - k) + sorted[c] * (k - f as f64)
    }
}

/// The 16 metric values for one column, in [`METRICS`] order.
fn column_metrics(df: &DataFrame, col: usize) -> Vec<String> {
    let meta = &df.columns[col];

    let mut non_empty: Vec<String> = Vec::new();
    let mut nulls = 0usize;
    let mut unique_set = HashSet::new();

    for row in 0..df.visible_row_count() {
        let physical = df.row_order[row];
        let val = df.get_physical(physical, col);
        if val.is_empty() {
            nulls += 1;
        } else {
            unique_set.insert(val.clone());
            non_empty.push(val);
        }
    }

    let is_numeric = meta.col_type.is_numeric();

    let nums: Vec<f64> = if is_numeric {
        non_empty
            .iter()
            .filter_map(|v| v.parse::<f64>().ok())
            .collect()
    } else {
        Vec::new()
    };

    // Most frequent value, ties broken by first appearance.
    //
    // The tie rule matters: every value in a column of unique names ties at a
    // count of one, and the previous implementation picked whichever the
    // HashMap happened to yield first — an answer that changed between runs,
    // since Rust seeds its hasher randomly per process.  IndexMap keeps
    // insertion order, and folding rather than `max_by_key` keeps the first of
    // an equal run instead of the last.
    let mode = || -> String {
        let mut freq: IndexMap<&str, usize> = IndexMap::new();
        for v in &non_empty {
            *freq.entry(v.as_str()).or_insert(0) += 1;
        }
        freq.iter()
            .fold(
                None,
                |best: Option<(&str, usize)>, (value, count)| match best {
                    Some((_, best_count)) if best_count >= *count => best,
                    _ => Some((value, *count)),
                },
            )
            .map(|(value, _)| value.to_string())
            .unwrap_or_default()
    };

    let p = meta.precision as usize;
    let blank = String::new;

    let (min_s, max_s, mean_s, median_s, mode_s, stdev_s, range_s, q5, q25, q50, q75, q95) =
        if !nums.is_empty() {
            let mut sorted = nums.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let n = sorted.len() as f64;
            let mean = sorted.iter().sum::<f64>() / n;
            let median = if sorted.len().is_multiple_of(2) {
                (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
            } else {
                sorted[sorted.len() / 2]
            };
            // Population variance: divides by n, not n-1.  See the module docs.
            let stdev = (sorted.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n).sqrt();
            let range = sorted[sorted.len() - 1] - sorted[0];
            (
                format!("{:.*}", p, sorted[0]),
                format!("{:.*}", p, sorted[sorted.len() - 1]),
                format!("{:.*}", p, mean),
                format!("{:.*}", p, median),
                mode(),
                format!("{:.*}", p, stdev),
                format!("{:.*}", p, range),
                format!("{:.*}", p, quantile(&sorted, 0.05)),
                format!("{:.*}", p, quantile(&sorted, 0.25)),
                format!("{:.*}", p, quantile(&sorted, 0.50)),
                format!("{:.*}", p, quantile(&sorted, 0.75)),
                format!("{:.*}", p, quantile(&sorted, 0.95)),
            )
        } else if !non_empty.is_empty() {
            // Non-numeric: min/max are lexicographic, and range shows both ends.
            let min_s = non_empty.iter().min().cloned().unwrap_or_default();
            let max_s = non_empty.iter().max().cloned().unwrap_or_default();
            let range_s = format!("{} → {}", min_s, max_s);
            (
                min_s,
                max_s,
                blank(),
                blank(),
                mode(),
                blank(),
                range_s,
                blank(),
                blank(),
                blank(),
                blank(),
                blank(),
            )
        } else {
            (
                blank(),
                blank(),
                blank(),
                blank(),
                blank(),
                blank(),
                blank(),
                blank(),
                blank(),
                blank(),
                blank(),
                blank(),
            )
        };

    vec![
        format!("{:?}", meta.col_type),
        non_empty.len().to_string(),
        nulls.to_string(),
        unique_set.len().to_string(),
        min_s,
        max_s,
        mean_s,
        median_s,
        mode_s,
        stdev_s,
        range_s,
        q5,
        q25,
        q50,
        q75,
        q95,
    ]
}

/// Profile every column of `df`.
///
/// The result is itself a table: a pinned `metric` column naming each row, then
/// one column per source column.  Values are strings because a column of mixed
/// metrics has no single type.
pub fn describe(df: &DataFrame) -> DataFrame {
    use polars::prelude::{Column, NamedFrom, Series};

    let mut series_vec: Vec<Column> = vec![Series::new(
        "metric".into(),
        &METRICS.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    )
    .into()];

    for col in 0..df.columns.len() {
        let values = column_metrics(df, col);
        series_vec.push(Series::new(df.columns[col].name.clone().into(), &values).into());
    }

    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)
        .unwrap_or_else(|_| polars::prelude::DataFrame::empty());

    let mut columns: Vec<ColumnMeta> = std::iter::once("metric".to_string())
        .chain(df.columns.iter().map(|c| c.name.clone()))
        .map(ColumnMeta::new)
        .collect();
    // Keep the metric names visible while scrolling sideways.
    columns[0].pinned = true;

    let mut out = DataFrame::from_parts(pdf, columns);
    out.calc_widths(40, 500);
    out
}
