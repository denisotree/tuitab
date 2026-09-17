//! The pipeline of operations a model sends to `tuitab_query`.
//!
//! Each operation maps onto a function that already exists in [`crate::data`],
//! so the arithmetic here is tuitab's, not a second implementation of it.  The
//! only genuinely new compute is `filter`.
//!
//! # How operations compose
//!
//! **Every operation returns a materialised frame** whose `row_order` is the
//! identity.  One rule, no exceptions.
//!
//! The cheap-looking alternative is to let row-affecting operations rewrite
//! `row_order` and leave the frame alone — a permutation instead of a copy.
//! That is what this module used to do, and it was wrong twice over:
//!
//! * [`DataFrame::add_computed_column`] evaluates over the whole physical
//!   frame, so `sum(x)` in a `compute` after a `filter` divided by the total of
//!   every row in the file, dropped ones included. A share-of-total came back
//!   confidently wrong.
//! * Window functions read the frame in physical order, so a `sort` that only
//!   permuted `row_order` left a running total accumulating in load order.
//!
//! Both are the same mistake: a downstream operation reaching past the view to
//! the data underneath. An invariant with an exception is one an author has to
//! remember; this one costs a `take()` per operation and nothing to remember.

use crate::data::aggregator::AggregatorKind;
use crate::data::dataframe::DataFrame;
use crate::data::expression::{Expr as TuiExpr, Value as ExprValue};
use crate::data::filter::{Clause, Operand, PredOp, Predicate};
use crate::data::group::AggSpec;
use crate::data::join::{join_dataframes, JoinType};
use serde_json::Value;
use std::sync::Arc;

// ── Operation model ─────────────────────────────────────────────────────────

pub enum Op {
    Filter(Vec<Clause>),
    Select(Vec<String>),
    /// One or more sort keys, the first most significant.
    Sort(Vec<SortKey>),
    Compute {
        name: String,
        expr: String,
    },
    GroupBy {
        by: Vec<String>,
        agg: Vec<AggSpec>,
    },
    Frequency {
        by: Vec<String>,
        agg: Vec<AggSpec>,
    },
    Pivot {
        index: Vec<String>,
        on: String,
        formula: String,
    },
    /// Aggregate every visible row as one group — a grand total.
    Aggregate(Vec<AggSpec>),
    /// Keep one row per distinct combination of `by`.
    Dedup {
        by: Vec<String>,
        keep: String,
        /// Tiebreaker column for `min` / `max`.
        on: Option<String>,
        /// Seed for `random`, so the same answer can be had twice.
        seed: Option<u64>,
    },
    /// Keep only rows whose `by` values appear more than once.
    Duplicates {
        by: Vec<String>,
    },
    /// Add a column computed from the rows around each row.
    Window(crate::data::window::Spec),
    /// Keep `n` rows chosen at random.
    Sample {
        n: usize,
        seed: Option<u64>,
    },
    /// Stand the table on end, or one row of it.
    Transpose {
        /// A display row to transpose on its own; the whole table when absent.
        row: Option<usize>,
    },
    Join(JoinSpec),
    Limit(usize),
}

pub struct SortKey {
    pub col: String,
    pub desc: bool,
}

pub struct JoinSpec {
    pub source: super::source::Source,
    pub left_on: Vec<String>,
    pub right_on: Vec<String>,
    pub how: JoinType,
}

// ── Parsing ─────────────────────────────────────────────────────────────────

/// Parse the `ops` array.  Each entry is a single-key object naming the
/// operation, e.g. `{"sort": {"col": "amount", "desc": true}}`.
pub fn parse_ops(value: &Value) -> Result<Vec<Op>, String> {
    let array = value
        .as_array()
        .ok_or_else(|| "'ops' must be an array of operations".to_string())?;

    array
        .iter()
        .enumerate()
        .map(|(i, v)| parse_op(v).map_err(|e| format!("ops[{}]: {}", i, e)))
        .collect()
}

/// The keys each operation understands, for the ones whose body is a record.
///
/// A key that is not here was a mistake, and dropping it in silence sends the caller
/// looking in the wrong place: `aggregate` written for `agg` was ignored, and the
/// failure arrived a step later as "grouping needs at least one aggregate" — the one
/// thing the caller had in fact supplied.  `filter`, `select` and `aggregate` take
/// arrays and `limit` a number, so they have nothing to check.
fn known_keys(op: &str) -> Option<&'static [&'static str]> {
    Some(match op {
        "compute" => &["name", "expr"],
        "group_by" | "frequency" => &["by", "agg"],
        "pivot" => &["index", "on", "formula"],
        "dedup" => &["by", "keep", "on", "seed"],
        "duplicates" => &["by"],
        "window" => &["fn", "col", "over", "order_by", "as", "desc", "offset"],
        "sample" => &["n", "seed"],
        "transpose" => &["row"],
        "join" => &["source", "path", "left_on", "right_on", "how"],
        "sort" => &["col", "desc", "by"],
        _ => return None,
    })
}

/// How close two key names are, for "did you mean" — a plain edit distance, capped.
fn close_enough(a: &str, b: &str) -> bool {
    // `aggregate` for `agg` is the mistake that prompted this, and no edit distance
    // small enough to be useful reaches across six characters — but one name being the
    // start of the other is as good a signal as they come.
    if a.starts_with(b) || b.starts_with(a) {
        return true;
    }
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    if a.len().abs_diff(b.len()) > 3 {
        return false;
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut row = vec![i + 1];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            row.push((row[j] + 1).min(prev[j + 1] + 1).min(prev[j] + cost));
        }
        prev = row;
    }
    prev[b.len()] <= 3
}

fn check_keys(op: &str, body: &Value) -> Result<(), String> {
    let Some(allowed) = known_keys(op) else {
        return Ok(());
    };
    if body.is_array() {
        return Err(format!(
            "'{}' takes an object like {{\"{}\": ...}}, not a list",
            op, allowed[0]
        ));
    }
    let Some(obj) = body.as_object() else {
        return Ok(());
    };
    for key in obj.keys() {
        if allowed.contains(&key.as_str()) {
            continue;
        }
        let suggestion = allowed
            .iter()
            .find(|a| close_enough(key, a))
            .map(|a| format!(" (did you mean '{}'?)", a))
            .unwrap_or_default();
        return Err(format!(
            "'{}' has no field '{}'{}. It takes: {}",
            op,
            key,
            suggestion,
            allowed.join(", ")
        ));
    }
    Ok(())
}

fn parse_op(value: &Value) -> Result<Op, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "an operation must be an object like {\"limit\": 10}".to_string())?;

    if obj.len() != 1 {
        return Err(format!(
            "an operation must have exactly one key naming it, found {}",
            obj.len()
        ));
    }

    let (name, body) = obj.iter().next().expect("checked len == 1");
    check_keys(name, body)?;

    match name.as_str() {
        "filter" => Ok(Op::Filter(parse_predicates(body)?)),
        "select" => Ok(Op::Select(string_array(body, "select")?)),
        "sort" => Ok(Op::Sort(parse_sort_keys(body)?)),
        "compute" => Ok(Op::Compute {
            name: required_str(body, "name")?,
            expr: required_str(body, "expr")?,
        }),
        "group_by" => Ok(Op::GroupBy {
            by: string_array(body.get("by").unwrap_or(&Value::Null), "by")?,
            agg: parse_aggs(body.get("agg"))?,
        }),
        "frequency" => Ok(Op::Frequency {
            by: string_array(body.get("by").unwrap_or(&Value::Null), "by")?,
            agg: parse_aggs(body.get("agg"))?,
        }),
        "pivot" => Ok(Op::Pivot {
            index: string_array(body.get("index").unwrap_or(&Value::Null), "index")?,
            on: required_str(body, "on")?,
            formula: required_str(body, "formula")?,
        }),
        "aggregate" => Ok(Op::Aggregate(parse_aggs(Some(body))?)),
        "dedup" => Ok(Op::Dedup {
            by: string_array(body.get("by").unwrap_or(&Value::Null), "by")?,
            keep: body
                .get("keep")
                .and_then(Value::as_str)
                .unwrap_or("first")
                .to_string(),
            on: body.get("on").and_then(Value::as_str).map(str::to_string),
            seed: body.get("seed").and_then(Value::as_u64),
        }),
        "duplicates" => Ok(Op::Duplicates {
            by: match body.get("by") {
                Some(v) if !v.is_null() => string_array(v, "by")?,
                _ => Vec::new(),
            },
        }),
        "window" => Ok(Op::Window(crate::data::window::Spec {
            function: crate::data::window::WindowFn::parse(&required_str(body, "fn")?)?,
            col: body.get("col").and_then(Value::as_str).map(str::to_string),
            over: match body.get("over") {
                Some(v) if !v.is_null() => string_array(v, "over")?,
                _ => Vec::new(),
            },
            order_by: match body.get("order_by") {
                Some(v) if !v.is_null() => string_array(v, "order_by")?,
                _ => Vec::new(),
            },
            as_name: body.get("as").and_then(Value::as_str).map(str::to_string),
            desc: body.get("desc").and_then(Value::as_bool).unwrap_or(false),
            offset: body.get("offset").and_then(Value::as_i64).unwrap_or(1),
        })),
        "sample" => Ok(Op::Sample {
            n: body
                .get("n")
                .and_then(Value::as_u64)
                .map(|n| n as usize)
                .ok_or("'sample' needs 'n', how many rows to keep")?,
            seed: body.get("seed").and_then(Value::as_u64),
        }),
        "transpose" => Ok(Op::Transpose {
            row: body.get("row").and_then(Value::as_u64).map(|n| n as usize),
        }),
        "join" => Ok(Op::Join(parse_join(body)?)),
        "limit" => body
            .as_u64()
            .or_else(|| body.get("n").and_then(Value::as_u64))
            .map(|n| Op::Limit(n as usize))
            .ok_or_else(|| "'limit' takes a number".to_string()),
        other => Err(format!(
            "Unknown operation '{}'. Available: filter, select, sort, compute, \
             group_by, aggregate, frequency, pivot, join, dedup, duplicates, \
             sample, window, transpose, limit",
            other
        )),
    }
}

/// Read sort keys from either shape.
///
/// `{"col": "amount", "desc": true}` is the common single-key case and stays;
/// `{"by": [{"col": ..., "desc": ...}, ...]}` gives a compound sort. Two `sort`
/// operations in sequence are *not* an equivalent of the latter — see
/// [`DataFrame::sort_by_keys`].
fn parse_sort_keys(body: &Value) -> Result<Vec<SortKey>, String> {
    let read = |item: &Value| -> Result<SortKey, String> {
        // A bare column name is accepted for the ascending case.
        if let Some(col) = item.as_str() {
            return Ok(SortKey {
                col: col.to_string(),
                desc: false,
            });
        }
        Ok(SortKey {
            col: required_str(item, "col")?,
            desc: item.get("desc").and_then(Value::as_bool).unwrap_or(false),
        })
    };

    match body.get("by") {
        Some(Value::Array(items)) if !items.is_empty() => items.iter().map(read).collect(),
        Some(Value::Array(_)) => Err("'by' needs at least one sort key".to_string()),
        Some(other) => read(other).map(|k| vec![k]),
        None => read(body).map(|k| vec![k]),
    }
}

fn required_str(body: &Value, key: &str) -> Result<String, String> {
    body.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("missing required string field '{}'", key))
}

fn string_array(value: &Value, what: &str) -> Result<Vec<String>, String> {
    // A single name is accepted where a list is expected — models write both.
    if let Some(s) = value.as_str() {
        return Ok(vec![s.to_string()]);
    }
    let array = value
        .as_array()
        .ok_or_else(|| format!("'{}' must be a column name or a list of them", what))?;
    array
        .iter()
        .map(|v| {
            v.as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("'{}' entries must be strings", what))
        })
        .collect()
}

/// Translate a JSON value into the operand a predicate compares against.
///
/// `{"col": "cost"}` names another column, so `revenue > cost` is expressible
/// without first computing a difference. Anything else is a constant.
fn parse_operand(value: &Value, op: PredOp) -> Result<Operand, String> {
    if let Some(name) = value.get("col").and_then(Value::as_str) {
        return Ok(Operand::Column(name.to_string()));
    }

    let scalar = |v: &Value| -> Result<ExprValue, String> {
        Ok(match v {
            Value::Number(n) => ExprValue::Number(n.as_f64().unwrap_or(f64::NAN)),
            Value::String(s) => ExprValue::String(s.clone()),
            Value::Bool(b) => ExprValue::Boolean(*b),
            Value::Null => ExprValue::Null,
            other => return Err(format!("cannot compare against {}", other)),
        })
    };

    // `in`, `not_in` and `between` want a list; everything else a single value.
    if matches!(op, PredOp::In | PredOp::NotIn | PredOp::Between) {
        let items = value
            .as_array()
            .ok_or_else(|| format!("'{:?}' takes an array of values", op).to_lowercase())?;
        return items
            .iter()
            .map(scalar)
            .collect::<Result<_, _>>()
            .map(Operand::List);
    }

    scalar(value).map(Operand::Literal)
}

fn parse_predicate(item: &Value) -> Result<Predicate, String> {
    let op = PredOp::parse(&required_str(item, "op")?)?;
    Ok(Predicate {
        col: required_str(item, "col")?,
        op,
        value: parse_operand(item.get("value").unwrap_or(&Value::Null), op)?,
    })
}

/// Parse a filter: a list whose entries are predicates or `any_of` groups.
///
/// Entries are joined with AND, predicates inside an `any_of` with OR — one
/// level of nesting, which covers `(a OR b) AND c` and stops short of being a
/// query language.
pub fn parse_predicates(value: &Value) -> Result<Vec<Clause>, String> {
    // A lone predicate object is accepted as a one-element list.
    let items: Vec<&Value> = match value {
        Value::Array(a) => a.iter().collect(),
        Value::Object(_) => vec![value],
        _ => return Err("'filter' takes a predicate or a list of them".to_string()),
    };

    items
        .into_iter()
        .map(|item| match item.get("any_of") {
            Some(Value::Array(group)) => group
                .iter()
                .map(parse_predicate)
                .collect::<Result<Vec<_>, _>>()
                .map(Clause::AnyOf),
            Some(_) => Err("'any_of' takes an array of predicates".to_string()),
            None => parse_predicate(item).map(Clause::One),
        })
        .collect()
}

fn parse_aggs(value: Option<&Value>) -> Result<Vec<AggSpec>, String> {
    let value = match value {
        Some(Value::Null) | None => return Ok(Vec::new()),
        Some(v) => v,
    };
    let array = value
        .as_array()
        .ok_or_else(|| "'agg' must be a list of {col, fn} objects".to_string())?;

    array
        .iter()
        .map(|item| {
            let col = required_str(item, "col")?;
            let name = required_str(item, "fn")?;
            let kind = AggregatorKind::all()
                .iter()
                .find(|k| k.name() == name)
                .copied()
                .ok_or_else(|| {
                    format!(
                        "Unknown aggregate '{}'. Available: count, distinct, sum, avg, \
                         min, max, median, stdev, p5, p25, p50, p75, p95",
                        name
                    )
                })?;
            Ok(AggSpec { col, kind })
        })
        .collect()
}

fn parse_join(body: &Value) -> Result<JoinSpec, String> {
    let source_value = body
        .get("source")
        .or_else(|| body.get("path"))
        .ok_or_else(|| "'join' requires a 'source'".to_string())?;
    let source = super::source::Source::from_json(source_value)?;

    let left_on = string_array(body.get("left_on").unwrap_or(&Value::Null), "left_on")?;
    // A single `on` covers the common case of identically named keys.
    let right_on = match body.get("right_on") {
        Some(v) if !v.is_null() => string_array(v, "right_on")?,
        _ => left_on.clone(),
    };

    if left_on.len() != right_on.len() {
        return Err(format!(
            "join needs the same number of keys on both sides, got {} and {}",
            left_on.len(),
            right_on.len()
        ));
    }
    if left_on.is_empty() {
        return Err("'join' requires at least one key in 'left_on'".to_string());
    }

    let how = match body.get("how").and_then(Value::as_str).unwrap_or("inner") {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "right" => JoinType::Right,
        "outer" | "full" => JoinType::Outer,
        "anti" => JoinType::Anti,
        "semi" => JoinType::Semi,
        "diff" => JoinType::Diff,
        other => {
            return Err(format!(
                "Unknown join type '{}'. Available: inner, left, right, outer, anti, semi, diff",
                other
            ))
        }
    };

    Ok(JoinSpec {
        source,
        left_on,
        right_on,
        how,
    })
}

// ── Application ─────────────────────────────────────────────────────────────

/// Seeds drawn for operations the caller left unseeded.
///
/// Reported back so a random result can be had again — a sample nobody can
/// reproduce is a poor thing to quote at someone.
#[derive(Default)]
pub struct Seeds(pub Vec<(usize, u64)>);

pub fn apply_all(df: DataFrame, ops: &[Op]) -> Result<DataFrame, String> {
    apply_all_reporting_seeds(df, ops).map(|(df, _)| df)
}

pub fn apply_all_reporting_seeds(
    mut df: DataFrame,
    ops: &[Op],
) -> Result<(DataFrame, Seeds), String> {
    let mut seeds = Seeds::default();
    for (i, op) in ops.iter().enumerate() {
        // Draw here rather than inside `apply`, so the value can be reported.
        let drawn = match op {
            Op::Sample { seed: None, .. } => Some(crate::data::dedup::random_seed()),
            Op::Dedup {
                keep, seed: None, ..
            } if keep == "random" => Some(crate::data::dedup::random_seed()),
            _ => None,
        };
        if let Some(seed) = drawn {
            seeds.0.push((i, seed));
        }
        df = apply(df, op, drawn).map_err(|e| format!("ops[{}]: {}", i, e))?;
    }
    Ok((df, seeds))
}

fn apply(mut df: DataFrame, op: &Op, drawn_seed: Option<u64>) -> Result<DataFrame, String> {
    match op {
        Op::Filter(clauses) => {
            df.row_order = Arc::new(crate::data::filter::matching_rows(&df, clauses)?);
            materialize(df)
        }
        Op::Limit(n) => {
            let mut order = (*df.row_order).clone();
            order.truncate(*n);
            df.row_order = Arc::new(order);
            materialize(df)
        }
        Op::Sort(keys) => {
            let resolved: Vec<(usize, bool)> = keys
                .iter()
                .map(|k| df.column_index(&k.col).map(|i| (i, k.desc)))
                .collect::<Result<_, _>>()?;
            df.sort_by_keys(&resolved)?;
            materialize(df)
        }
        Op::Select(names) => df.select_columns(names),
        Op::Compute { name, expr } => {
            let parsed =
                TuiExpr::parse(expr).map_err(|e| format!("in expression '{}': {}", expr, e))?;
            let last = df.columns.len().saturating_sub(1);
            df.add_computed_column(name, &parsed, last)?;
            Ok(df)
        }
        Op::GroupBy { by, agg } => crate::data::group::group_by(&df, by, agg),
        Op::Aggregate(agg) => crate::data::group::total(&df, agg),
        Op::Dedup { by, keep, on, seed } => {
            let keys: Vec<usize> = by
                .iter()
                .map(|n| df.column_index(n))
                .collect::<Result<_, _>>()?;
            let tiebreaker = on.as_deref().map(|n| df.column_index(n)).transpose()?;
            let rule = crate::data::dedup::Keep::parse(keep, seed.or(drawn_seed), tiebreaker)?;
            df.row_order = Arc::new(crate::data::dedup::deduplicate(&df, &keys, rule)?);
            materialize(df)
        }
        Op::Duplicates { by } => {
            let keys: Vec<usize> = by
                .iter()
                .map(|n| df.column_index(n))
                .collect::<Result<_, _>>()?;
            let duplicates: std::collections::HashSet<usize> =
                crate::data::dedup::duplicate_rows(&df, &keys)
                    .into_iter()
                    .collect();
            let kept: Vec<usize> = df
                .row_order
                .iter()
                .copied()
                .filter(|r| duplicates.contains(r))
                .collect();
            df.row_order = Arc::new(kept);
            materialize(df)
        }
        Op::Window(spec) => crate::data::window::add_window_column(&df, spec),
        Op::Sample { n, seed } => {
            let seed = seed.or(drawn_seed).unwrap_or(0);
            df.row_order = Arc::new(crate::data::dedup::sample_rows(&df, *n, seed));
            materialize(df)
        }
        Op::Transpose { row } => match row {
            Some(display) => {
                let physical = df.row_order.get(*display).copied().ok_or_else(|| {
                    format!(
                        "row {} is out of range; {} rows are visible",
                        display,
                        df.row_order.len()
                    )
                })?;
                crate::data::transpose::transpose_row(&df, physical)
            }
            None => crate::data::transpose::transpose_table(&df),
        },
        Op::Frequency { by, agg } => crate::data::group::frequency(&df, by, agg),
        Op::Pivot { index, on, formula } => pivot(&df, index, on, formula),
        Op::Join(spec) => {
            let right = super::source::load_once(&spec.source)?;
            join_dataframes(&df, &right, &spec.left_on, &spec.right_on, spec.how)
                .map_err(|e| e.to_string())
        }
    }
}

/// Collapse `row_order` into the frame, so the next operation sees the rows it
/// is supposed to see, in the order it is supposed to see them.
///
/// Cheap when nothing changed: [`DataFrame::get_visible_df`] returns a clone of
/// the frame — Arc-backed, so O(columns) — whenever `row_order` is already the
/// identity.
fn materialize(df: DataFrame) -> Result<DataFrame, String> {
    let visible = df.get_visible_df()?;
    Ok(DataFrame::from_parts(visible, df.columns))
}

// ── pivot ───────────────────────────────────────────────────────────────────

fn pivot(df: &DataFrame, index: &[String], on: &str, formula: &str) -> Result<DataFrame, String> {
    if index.is_empty() {
        return Err("'pivot' requires at least one column in 'index'".to_string());
    }
    for name in index {
        df.column_index(name)?;
    }
    df.column_index(on)?;

    let expr =
        TuiExpr::parse(formula).map_err(|e| format!("in pivot formula '{}': {}", formula, e))?;

    let (pdf, metas) = df.create_pivot_table(index, on, &expr)?;
    Ok(DataFrame::from_parts(pdf, metas))
}
