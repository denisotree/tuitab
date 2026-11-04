//! Selecting rows, shared by the TUI and the MCP server.
//!
//! The two surfaces describe a filter differently — the TUI takes a typed
//! expression, the server takes structured predicates — but they must *mean*
//! the same thing, so both funnel into one [`crate::data::expression::Expr`]
//! and one evaluator. Before this module they had nothing in common, which is
//! how the server ended up with a predicate language the TUI could not express
//! and the TUI kept a fallback the server never inherited.
//!
//! # Which evaluator runs
//!
//! [`Expr`] has two: `to_polars_expr` lowers to a vectorised Polars expression,
//! and `eval` walks the tree per row. Neither covers everything the other does
//! — Polars has the aggregates, the interpreter has the date and string
//! functions — so *which one runs* would otherwise decide which expressions
//! work.
//!
//! The rule that removes the question: **structured predicates never touch the
//! interpreter.** Everything [`Predicate`] can express (comparisons, `in`,
//! `contains`, `and`/`or`/`not`) lowers to Polars by construction, so
//! [`select_rows`] can insist on the fast path for them. The interpreter is
//! reached only from free text, where a user may legitimately write
//! `year(hire_date) > 2020`.

use crate::data::dataframe::DataFrame;
use crate::data::expression::{Expr, Op, Value};
use crate::types::ColumnType;
use std::collections::HashMap;

/// How a predicate compares a column against something.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PredOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    In,
    NotIn,
    Contains,
    Between,
    IsEmpty,
    NotEmpty,
}

impl PredOp {
    pub fn parse(s: &str) -> Result<Self, String> {
        Ok(match s {
            "eq" => Self::Eq,
            "ne" => Self::Ne,
            "gt" => Self::Gt,
            "ge" => Self::Ge,
            "lt" => Self::Lt,
            "le" => Self::Le,
            "in" => Self::In,
            "not_in" => Self::NotIn,
            "contains" => Self::Contains,
            "between" => Self::Between,
            "is_empty" => Self::IsEmpty,
            "not_empty" => Self::NotEmpty,
            other => {
                return Err(format!(
                    "Unknown filter operator '{}'. Available: eq, ne, gt, ge, lt, le, \
                     in, not_in, contains, between, is_empty, not_empty",
                    other
                ))
            }
        })
    }
}

/// What a predicate compares its column against.
#[derive(Clone, Debug)]
pub enum Operand {
    /// A constant.
    Literal(Value),
    /// Another column of the same row, so `revenue > cost` is expressible.
    Column(String),
    /// Several constants, for `in` / `not_in` and the two ends of `between`.
    List(Vec<Value>),
}

/// One comparison against one column.
#[derive(Clone, Debug)]
pub struct Predicate {
    pub col: String,
    pub op: PredOp,
    pub value: Operand,
}

/// An element of a filter: a single predicate, or a group of them joined by OR.
///
/// One level of nesting, deliberately. `(a OR b) AND c` covers what people
/// actually write; an arbitrary tree would be a query language, and the point
/// of structured predicates is that they are checkable, not that they are
/// expressive.
#[derive(Clone, Debug)]
pub enum Clause {
    One(Predicate),
    AnyOf(Vec<Predicate>),
    /// Rows where the inner clause is not true — one level, around a predicate or an
    /// `any_of`.  A row whose column is NULL is kept: `not` and the filter it negates
    /// together cover every row.
    Not(Box<Clause>),
}

impl Clause {
    /// The predicates this clause reads, for checking their columns up front.
    pub fn predicates(&self) -> &[Predicate] {
        match self {
            Clause::One(p) => std::slice::from_ref(p),
            Clause::AnyOf(list) => list,
            Clause::Not(inner) => inner.predicates(),
        }
    }
}

// ── Compiling predicates to expressions ─────────────────────────────────────

fn binop(op: Op, left: Expr, right: Expr) -> Expr {
    Expr::BinOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// An "empty" cell: missing, or the empty string.
///
/// Both halves are needed and neither is optional. Polars reads a blank CSV
/// field as null, but a blank in a JSON string or a cell a user cleared by hand
/// is an empty string — a predicate that caught only one of the two would leave
/// rows matching neither `is_empty` nor `not_empty`.
///
/// The emptiness test goes through [`Expr::IsNull`] rather than a comparison
/// against null, which is null and therefore never true; and the empty-string
/// half compares the column *as text*, because `"" == 5` is a type error rather
/// than a false.
fn is_empty_expr(col: &str) -> Expr {
    let column = || Expr::ColumnRef(col.to_string());
    binop(
        Op::Or,
        Expr::IsNull(Box::new(column())),
        binop(
            Op::Eq,
            Expr::FunctionCall {
                name: "text".to_string(),
                args: vec![column()],
            },
            Expr::Literal(Value::String(String::new())),
        ),
    )
}

/// How a column wants its literals shaped.
///
/// The predicate carries JSON, which knows strings and numbers; the column
/// knows what it holds. Reconciling the two at compile time is what the old
/// MCP-only path did and what the move to a shared expression tree dropped.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// Compare as written.
    AsIs,
    /// Numeric column: a quoted number is the number it spells.
    Numeric,
    /// Date or datetime against something that is not a moment — a pattern, a word:
    /// compare the ISO text of both sides, the text the column shows.
    IsoText,
    /// Date or datetime against literals that name moments, or another date column:
    /// compare as moments.  The column's text carries its unit's fraction
    /// (`11:10:00.000`), so an equal instant written `11:10:00` never matched as text.
    Moment,
}

fn shape_for(df: &DataFrame, col: &str) -> Result<Shape, String> {
    Ok(match df.columns[df.column_index(col)?].col_type {
        t if t.is_numeric() => Shape::Numeric,
        ColumnType::Date | ColumnType::Datetime => Shape::IsoText,
        _ => Shape::AsIs,
    })
}

/// Reshape one literal to suit the column it is compared against.
fn fit(value: &Value, shape: Shape) -> Result<Value, String> {
    Ok(match (shape, value) {
        // `"30"` where `30` was meant is a slip worth absorbing; `"a lot"` is
        // not, and stays a type error rather than a guess.
        (Shape::Numeric, Value::String(s)) => Value::Number(
            s.trim()
                .parse::<f64>()
                .map_err(|_| format!("'{}' is not a number, and the column holds numbers", s))?,
        ),
        (Shape::IsoText, other) => Value::String(other.to_string()),
        (Shape::Moment, v) => moment(v).unwrap_or_else(|| v.clone()),
        _ => value.clone(),
    })
}

/// A literal a date or datetime column can be compared with as a moment: a date, or a
/// date and time to the minute, second or fraction.  `None` keeps the text comparison.
fn moment(value: &Value) -> Option<Value> {
    let Value::String(s) = value else {
        return None;
    };
    let s = s.trim();
    crate::data::expression::parse_temporal(s).or_else(|| {
        ["%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"]
            .iter()
            .find_map(|f| chrono::NaiveDateTime::parse_from_str(s, f).ok())
            .map(Value::Datetime)
    })
}

/// A column rendered as text, for [`Shape::IsoText`].
fn as_text(col: &str) -> Expr {
    Expr::FunctionCall {
        name: "text".to_string(),
        args: vec![Expr::ColumnRef(col.to_string())],
    }
}

impl Predicate {
    /// Whether this predicate on a date or datetime column orders or matches moments:
    /// a comparison whose literals all name one, or against another such column.
    fn compares_moments(&self, df: &DataFrame) -> Result<bool, String> {
        let ordering = matches!(
            self.op,
            PredOp::Eq
                | PredOp::Ne
                | PredOp::Gt
                | PredOp::Ge
                | PredOp::Lt
                | PredOp::Le
                | PredOp::Between
                | PredOp::In
                | PredOp::NotIn
        );
        Ok(ordering
            && match &self.value {
                Operand::Literal(v) => moment(v).is_some(),
                Operand::List(items) => {
                    !items.is_empty() && items.iter().all(|v| moment(v).is_some())
                }
                Operand::Column(other) => matches!(
                    df.columns[df.column_index(other)?].col_type,
                    ColumnType::Date | ColumnType::Datetime
                ),
            })
    }

    /// Turn this predicate into an expression over the frame's columns.
    pub fn to_expr(&self, df: &DataFrame) -> Result<Expr, String> {
        let shape = match shape_for(df, &self.col)? {
            Shape::IsoText if self.compares_moments(df)? => Shape::Moment,
            shape => shape,
        };
        let side = |name: &str| {
            if shape == Shape::IsoText {
                as_text(name)
            } else {
                Expr::ColumnRef(name.to_string())
            }
        };
        let column = side(&self.col);

        let operand = |value: &Operand| -> Result<Expr, String> {
            match value {
                Operand::Literal(v) => Ok(Expr::Literal(fit(v, shape)?)),
                Operand::Column(name) => Ok(side(name)),
                Operand::List(_) => {
                    Err("this operator takes a single value, not a list".to_string())
                }
            }
        };

        let simple = |op: Op| -> Result<Expr, String> {
            Ok(binop(op, column.clone(), operand(&self.value)?))
        };

        match self.op {
            PredOp::Eq => simple(Op::Eq),
            PredOp::Ne => simple(Op::NotEq),
            PredOp::Gt => simple(Op::Gt),
            PredOp::Ge => simple(Op::Geq),
            PredOp::Lt => simple(Op::Lt),
            PredOp::Le => simple(Op::Leq),
            PredOp::IsEmpty => Ok(is_empty_expr(&self.col)),
            PredOp::NotEmpty => Ok(Expr::Not(Box::new(is_empty_expr(&self.col)))),
            PredOp::Contains => {
                let pattern = match &self.value {
                    Operand::Literal(Value::String(s)) => s.clone(),
                    Operand::Literal(other) => other.to_string(),
                    _ => return Err("'contains' takes a regex string".to_string()),
                };
                Ok(Expr::FunctionCall {
                    name: "contains".to_string(),
                    args: vec![column, Expr::Literal(Value::String(pattern))],
                })
            }
            PredOp::Between => match &self.value {
                Operand::List(bounds) if bounds.len() == 2 => Ok(binop(
                    Op::And,
                    binop(
                        Op::Geq,
                        column.clone(),
                        Expr::Literal(fit(&bounds[0], shape)?),
                    ),
                    binop(Op::Leq, column, Expr::Literal(fit(&bounds[1], shape)?)),
                )),
                _ => Err("'between' takes a two-element [low, high] array".to_string()),
            },
            PredOp::In | PredOp::NotIn => match &self.value {
                Operand::List(items) => {
                    let list = Expr::InList {
                        left: Box::new(column),
                        list: items
                            .iter()
                            .map(|v| fit(v, shape).map(Expr::Literal))
                            .collect::<Result<_, _>>()?,
                    };
                    Ok(if self.op == PredOp::In {
                        list
                    } else {
                        Expr::Not(Box::new(list))
                    })
                }
                _ => Err("'in' takes an array of values".to_string()),
            },
        }
    }
}

/// Combine clauses: predicates within a group with OR, groups with AND.
///
/// An empty filter is `None` rather than a tautology, so callers can tell
/// "everything" from "a condition that happens to match everything".
pub fn clauses_to_expr(df: &DataFrame, clauses: &[Clause]) -> Result<Option<Expr>, String> {
    let mut combined: Option<Expr> = None;

    for clause in clauses {
        let expr = clause_to_expr(df, clause)?;
        combined = Some(match combined {
            Some(acc) => binop(Op::And, acc, expr),
            None => expr,
        });
    }

    Ok(combined)
}

fn clause_to_expr(df: &DataFrame, clause: &Clause) -> Result<Expr, String> {
    Ok(match clause {
        Clause::One(p) => p.to_expr(df)?,
        Clause::AnyOf(predicates) => {
            let mut any: Option<Expr> = None;
            for p in predicates {
                let e = p.to_expr(df)?;
                any = Some(match any {
                    Some(acc) => binop(Op::Or, acc, e),
                    None => e,
                });
            }
            // An empty `any_of` matches nothing, which is the honest answer
            // to "any of no things".
            any.unwrap_or(Expr::Literal(Value::Boolean(false)))
        }
        Clause::Not(inner) => {
            // NOT x OR x IS NULL: a NULL verdict — the column was empty — counts as
            // "not true" rather than staying NULL and dropping the row.
            let e = clause_to_expr(df, inner)?;
            binop(
                Op::Or,
                Expr::Not(Box::new(e.clone())),
                Expr::IsNull(Box::new(e)),
            )
        }
    })
}

// ── Evaluating ──────────────────────────────────────────────────────────────

/// Whether [`select_rows`] may fall back to the per-row interpreter.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Fallback {
    /// Free text: try Polars, then the interpreter for what it cannot lower.
    Allowed,
    /// Structured predicates: Polars or an error, never a second opinion.
    Forbidden,
}

/// Display-row indices where `expr` holds.
///
/// The fast path is a vectorised Polars mask. The fallback runs only when
/// Polars **cannot evaluate** the expression — not when it evaluates it and
/// finds nothing.
///
/// That distinction was a real bug: the previous version in `App` fell back
/// whenever the result was empty, so a predicate that legitimately matched no
/// rows was silently re-run through a different evaluator with different
/// semantics. "No rows matched" is an answer, not a failure.
pub fn select_rows(df: &DataFrame, expr: &Expr, fallback: Fallback) -> Result<Vec<usize>, String> {
    use polars::prelude::*;

    let visible = df.get_visible_df()?;
    let height = visible.height();

    // Why the error is carried rather than discarded: a filter can be refused
    // for five different reasons — an expression Polars cannot lower, a type
    // mismatch, a bad regex, a mixed `in` list, a non-boolean result — and a
    // model told only "it did not work" has nothing to correct.
    let refusal = match expr.to_polars_expr_with(Some(visible.schema().as_ref())) {
        Ok(polars_expr) => match visible
            .clone()
            .lazy()
            .select([polars_expr.alias("__match")])
            .collect()
        {
            Ok(mask_df) => {
                let mask = mask_df
                    .column("__match")
                    .map_err(|e| e.to_string())?
                    .as_materialized_series()
                    .bool()
                    .map_err(|_| {
                        "this filter produced values rather than yes-or-no answers".to_string()
                    })?
                    .clone();

                // A condition about the table as a whole — `mean(age) > 30` —
                // lowers to one value, and it is true of every row or of none.
                // Enumerating it as if it were the mask answered about row zero
                // and called it the answer for the table.
                if mask.len() == 1 {
                    return Ok(if mask.get(0).unwrap_or(false) {
                        (0..height).collect()
                    } else {
                        Vec::new()
                    });
                }
                if mask.len() != height {
                    return Err(format!(
                        "this filter produced {} verdicts for {} rows",
                        mask.len(),
                        height
                    ));
                }

                return Ok(mask
                    .into_iter()
                    .enumerate()
                    .filter(|(_, hit)| hit.unwrap_or(false))
                    .map(|(i, _)| i)
                    .collect());
            }
            Err(e) => e.to_string(),
        },
        Err(e) => e,
    };

    if fallback == Fallback::Forbidden {
        return Err(refusal);
    }

    let lookup: HashMap<&str, usize> = df
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.as_str(), i))
        .collect();

    Ok((0..df.visible_row_count())
        .filter(|i| {
            expr.eval(df.row_order[*i], &lookup, df)
                .as_bool()
                .unwrap_or(false)
        })
        .collect())
}

/// Apply a structured filter, returning the surviving **physical** row indices
/// in display order — ready to become a new `row_order`.
pub fn matching_rows(df: &DataFrame, clauses: &[Clause]) -> Result<Vec<usize>, String> {
    for clause in clauses {
        for p in clause.predicates() {
            df.column_index(&p.col)?;
            if let Operand::Column(other) = &p.value {
                df.column_index(other)?;
            }
        }
    }

    let expr = match clauses_to_expr(df, clauses)? {
        Some(e) => e,
        None => return Ok((*df.row_order).clone()),
    };

    let display = select_rows(df, &expr, Fallback::Forbidden)?;
    Ok(display.into_iter().map(|i| df.row_order[i]).collect())
}
