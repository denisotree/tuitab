use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A runtime value produced by evaluating an [`Expr`] against a data row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Number(f64),
    String(String),
    Boolean(bool),
    Date(NaiveDate),
    Datetime(NaiveDateTime),
    Null,
}

impl Value {
    /// Coerce to `f64`.  Strings are parsed; booleans map to `1.0`/`0.0`;
    /// `Null`, `Date`, and `Datetime` return `None`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Number(n) => Some(*n),
            Value::String(s) => s.parse().ok(),
            Value::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    /// The boolean value.  Text `true`/`false` counts, as text that reads as a number
    /// counts for [`Value::as_f64`]: a text column of them is still a condition.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Number(n) => write!(f, "{}", n),
            Value::String(s) => write!(f, "{}", s),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Date(d) => write!(f, "{}", d),
            Value::Datetime(dt) => write!(f, "{}", dt),
            Value::Null => write!(f, "Null"),
        }
    }
}

/// Simple expression AST for computed columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    /// Reference to a column by name — resolved at evaluation time.
    ColumnRef(String),
    /// Literal value
    Literal(Value),
    /// Binary operation: left op right.
    BinOp {
        op: Op,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// A built-in function call, e.g. `upper(name)` or `round(price, 2)`.
    FunctionCall { name: String, args: Vec<Expr> },
    /// Ternary conditional: `if cond then then_branch else else_branch`.
    If {
        cond: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Box<Expr>,
    },
    /// Membership test: `left in (v1, v2, ...)`.  Evaluates to a boolean.
    InList { left: Box<Expr>, list: Vec<Expr> },
    /// Logical negation: `not cond`.
    Not(Box<Expr>),
    /// Whether a value is missing.
    ///
    /// Not expressible as `x == null`: in three-valued logic that comparison is
    /// itself null, never true, so a filter built on it matches nothing.
    IsNull(Box<Expr>),
}

/// Binary operator for [`Expr::BinOp`] nodes in the expression AST.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Op {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    Gt,
    Leq,
    Geq,
    /// Logical conjunction — binds tighter than `Or`, looser than a comparison.
    And,
    /// Logical disjunction — the loosest binding of all.
    Or,
}

// ── Parser ──────────────────────────────────────────────────────────────────────
// Recursive descent parser, loosest binding first.
// Grammar:
//   expr       → or
//   or         → and ('or' and)*
//   and        → not ('and' not)*
//   not        → 'not' not | comparison
//   comparison → term_add (('<'|'>'|'=='|'!='|'<='|'>=') term_add | 'in' '(' list ')')*
//   term_add   → term_mul (('+' | '-') term_mul)*
//   term_mul   → factor (('*' | '/') factor)*
//   factor     → NUMBER | STRING | COLUMN_NAME | call | '(' expr ')'

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Number(f64),
    StringLit(String),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
    Comma,
    Eq,
    NotEq,
    Lt,
    Gt,
    Leq,
    Geq,
    In,
    And,
    Or,
    Not,
}

impl Expr {
    /// Parse an expression string into an AST.
    pub fn parse(input: &str) -> Result<Expr, String> {
        let tokens = tokenize(input)?;
        if tokens.is_empty() {
            return Err("Empty expression".to_string());
        }
        let mut parser = Parser { tokens, pos: 0 };
        let expr = parser.parse_expr()?;
        if parser.pos < parser.tokens.len() {
            return Err(format!("Unexpected token at position {}", parser.pos));
        }
        Ok(expr)
    }

    /// Refuse text that arithmetic can only turn into NULL.
    ///
    /// `"3" * 1` is a real conversion and stays; `"К выплате" * 1` is someone reaching
    /// for a column and getting a string literal, which Polars answers with a column of
    /// nulls and no error at all — the worst thing a query engine can do.  Only the
    /// arithmetic operators are checked: a string against `==` or `in` is the ordinary
    /// way to compare, whatever the columns happen to be called.
    pub fn check_text_arithmetic(&self, is_column: &dyn Fn(&str) -> bool) -> Result<(), String> {
        let complain = |s: &String| -> Result<(), String> {
            if s.parse::<f64>().is_ok() {
                return Ok(());
            }
            Err(if is_column(s) {
                format!(
                    "'{}' in double quotes is a text literal, not the column of that \
                     name — arithmetic on it can only produce NULL. Write it in \
                     backticks: `{}`",
                    s, s
                )
            } else {
                format!(
                    "'{}' is text and cannot be part of a calculation. If a column is \
                     meant, write its name in backticks: `{}`",
                    s, s
                )
            })
        };
        match self {
            Expr::BinOp { op, left, right } => {
                if matches!(op, Op::Add | Op::Sub | Op::Mul | Op::Div) {
                    for side in [left, right] {
                        if let Expr::Literal(Value::String(s)) = side.as_ref() {
                            complain(s)?;
                        }
                    }
                }
                left.check_text_arithmetic(is_column)?;
                right.check_text_arithmetic(is_column)
            }
            Expr::FunctionCall { args, .. } => args
                .iter()
                .try_for_each(|a| a.check_text_arithmetic(is_column)),
            Expr::If {
                cond,
                then_branch,
                else_branch,
            } => {
                cond.check_text_arithmetic(is_column)?;
                then_branch.check_text_arithmetic(is_column)?;
                else_branch.check_text_arithmetic(is_column)
            }
            Expr::InList { left, list } => {
                left.check_text_arithmetic(is_column)?;
                list.iter()
                    .try_for_each(|e| e.check_text_arithmetic(is_column))
            }
            Expr::Not(inner) | Expr::IsNull(inner) => inner.check_text_arithmetic(is_column),
            Expr::ColumnRef(_) | Expr::Literal(_) => Ok(()),
        }
    }

    /// Try to translate AST into Polars lazy expression.
    /// Returns Err if the operation is not supported (triggers fallback execution).
    pub fn to_polars_expr(&self) -> Result<polars::lazy::dsl::Expr, String> {
        match self {
            Expr::ColumnRef(name) => Ok(crate::data::column_expr(name.as_str())),
            Expr::Literal(val) => match val {
                // An integral value is emitted as an integer. JSON has a single
                // number type, so an id arrives here as `f64`; comparing it
                // against an Int64 column promotes the column to float, and
                // above 2^53 neighbouring ids stop being distinguishable.
                Value::Number(n) if n.fract() == 0.0 && n.abs() <= i64::MAX as f64 => {
                    Ok(polars::lazy::dsl::lit(*n as i64))
                }
                Value::Number(n) => Ok(polars::lazy::dsl::lit(*n)),
                Value::String(s) => Ok(polars::lazy::dsl::lit(s.clone())),
                Value::Boolean(b) => Ok(polars::lazy::dsl::lit(*b)),
                Value::Date(d) => Ok(polars::lazy::dsl::lit(*d)),
                Value::Datetime(dt) => Ok(polars::lazy::dsl::lit(*dt)),
                Value::Null => Ok(polars::lazy::dsl::lit(polars::prelude::Null {})),
            },
            Expr::BinOp { op, left, right } => {
                let l = left.to_polars_expr()?;
                let r = right.to_polars_expr()?;
                Ok(match op {
                    Op::Add => l + r,
                    Op::Sub => l - r,
                    Op::Mul => l * r,
                    Op::Div => {
                        l.cast(polars::prelude::DataType::Float64)
                            / r.cast(polars::prelude::DataType::Float64)
                    }
                    Op::Eq => l.eq(r),
                    Op::NotEq => l.neq(r),
                    Op::Lt => l.lt(r),
                    Op::Gt => l.gt(r),
                    Op::Leq => l.lt_eq(r),
                    Op::Geq => l.gt_eq(r),
                    Op::And => l.and(r),
                    Op::Or => l.or(r),
                })
            }
            Expr::Not(inner) => Ok(inner.to_polars_expr()?.not()),
            Expr::IsNull(inner) => Ok(inner.to_polars_expr()?.is_null()),
            Expr::InList { left, list } => {
                let l = left.to_polars_expr()?;
                let mut polars_list = Vec::new();
                for item in list {
                    // Only support literals in Polars is_in for now
                    if let Expr::Literal(v) = item {
                        match v {
                            Value::Number(n) => {
                                polars_list.push(polars::prelude::AnyValue::Float64(*n))
                            }
                            Value::String(s) => polars_list
                                .push(polars::prelude::AnyValue::StringOwned(s.clone().into())),
                            _ => {
                                return Err(
                                    "Only numbers and strings supported in IN list for Polars"
                                        .to_string(),
                                )
                            }
                        }
                    } else {
                        return Err("Only literals supported in IN list for Polars".to_string());
                    }
                }
                let s = polars::prelude::Series::from_any_values("".into(), &polars_list, true)
                    .map_err(|e| e.to_string())?;
                Ok(l.is_in(polars::lazy::dsl::lit(s), false))
            }
            Expr::If {
                cond,
                then_branch,
                else_branch,
            } => {
                let c = cond.to_polars_expr()?;
                let t = then_branch.to_polars_expr()?;
                let e = else_branch.to_polars_expr()?;
                Ok(polars::lazy::dsl::when(c).then(t).otherwise(e))
            }
            Expr::FunctionCall { name, args } => match name.as_str() {
                "len" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr()?
                    .str()
                    .len_chars()
                    .cast(polars::prelude::DataType::Int64)),
                "sum" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr()?
                    .sum()
                    .cast(polars::prelude::DataType::Float64)),
                "count" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr()?
                    .count()
                    .cast(polars::prelude::DataType::Float64)),
                "mean" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr()?
                    .mean()
                    .cast(polars::prelude::DataType::Float64)),
                "max" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr()?
                    .max()
                    .cast(polars::prelude::DataType::Float64)),
                "min" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr()?
                    .min()
                    .cast(polars::prelude::DataType::Float64)),
                // Casting to text so a value of any type can be compared with
                // a string — polars refuses `numeric == ""` outright.
                "text" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr()?
                    .cast(polars::prelude::DataType::String)),
                // The pattern has to be a literal: a per-row regex would mean
                // recompiling it for every row, and no caller wants that.
                "contains" if args.len() == 2 => match &args[1] {
                    Expr::Literal(Value::String(pattern)) => Ok(args[0]
                        .to_polars_expr()?
                        .cast(polars::prelude::DataType::String)
                        .str()
                        .contains(polars::lazy::dsl::lit(pattern.clone()), true)),
                    _ => {
                        Err("contains() needs a literal pattern as its second argument".to_string())
                    }
                },
                "year" | "month" | "day" | "hour" | "minute" | "today" | "now" | "date_format" => {
                    Err(format!("Function '{}' requires slow-path evaluation", name))
                }
                _ => Err(format!(
                    "Function '{}' not supported in fast evaluation mode",
                    name
                )),
            },
        }
    }

    /// Evaluate the expression for a specific row.
    pub fn eval(
        &self,
        row_idx: usize,
        col_lookup: &HashMap<&str, usize>,
        df: &crate::data::dataframe::DataFrame,
    ) -> Value {
        match self {
            Expr::Literal(v) => v.clone(),
            Expr::Not(inner) => match inner.eval(row_idx, col_lookup, df).as_bool() {
                Some(b) => Value::Boolean(!b),
                None => Value::Null,
            },
            Expr::IsNull(inner) => {
                Value::Boolean(inner.eval(row_idx, col_lookup, df) == Value::Null)
            }
            // A text column is text: read by its look, `007` became 7 and `nan` NaN.
            Expr::ColumnRef(name) => match col_lookup.get(name.as_str()) {
                Some(&col_idx) if df.is_null_physical(row_idx, col_idx) => Value::Null,
                Some(&col_idx)
                    if df
                        .df
                        .columns()
                        .get(col_idx)
                        .is_some_and(|c| c.dtype() == &polars::prelude::DataType::String) =>
                {
                    Value::String(df.get_physical(row_idx, col_idx))
                }
                Some(&col_idx) => infer(&df.get_physical(row_idx, col_idx)),
                None => Value::Null,
            },
            Expr::BinOp { op, left, right } => {
                let l = left.eval(row_idx, col_lookup, df);
                let r = right.eval(row_idx, col_lookup, df);

                // 1. String Concatenation overloaded to +
                if matches!(op, Op::Add) {
                    if let (Value::String(s1), Value::String(s2)) = (&l, &r) {
                        return Value::String(format!("{}{}", s1, s2));
                    }
                }

                // Text that reads as a date takes part in date arithmetic as one.
                let (l, r) = if matches!(op, Op::Add | Op::Sub) {
                    (as_temporal(l), as_temporal(r))
                } else {
                    (l, r)
                };

                // 2. Date Math
                match op {
                    Op::Add => {
                        if let (Value::Date(d), Value::Number(days)) = (&l, &r) {
                            return add_days(*d, *days);
                        }
                        if let (Value::Number(days), Value::Date(d)) = (&l, &r) {
                            return add_days(*d, *days);
                        }
                        if let (Value::Datetime(dt), Value::Number(secs)) = (&l, &r) {
                            return add_seconds(*dt, *secs);
                        }
                        if let (Value::Number(secs), Value::Datetime(dt)) = (&l, &r) {
                            return add_seconds(*dt, *secs);
                        }
                    }
                    Op::Sub => {
                        if let (Value::Date(d), Value::Number(days)) = (&l, &r) {
                            return add_days(*d, -*days);
                        }
                        if let (Value::Date(d1), Value::Date(d2)) = (&l, &r) {
                            return Value::Number((*d1 - *d2).num_days() as f64);
                        }
                        if let (Value::Datetime(dt), Value::Number(secs)) = (&l, &r) {
                            return add_seconds(*dt, -*secs);
                        }
                        if let (Value::Datetime(dt1), Value::Datetime(dt2)) = (&l, &r) {
                            return Value::Number((*dt1 - *dt2).num_seconds() as f64);
                        }
                    }
                    _ => {}
                }

                // 3. Comparisons
                match op {
                    Op::Eq => {
                        return Value::Boolean(values_equal(&l, &r));
                    }
                    Op::NotEq => {
                        return Value::Boolean(!values_equal(&l, &r));
                    }
                    Op::Lt => return compare_ordered(&l, &r, std::cmp::Ordering::Less, false),
                    Op::Gt => return compare_ordered(&l, &r, std::cmp::Ordering::Greater, false),
                    Op::Leq => return compare_ordered(&l, &r, std::cmp::Ordering::Less, true),
                    Op::Geq => return compare_ordered(&l, &r, std::cmp::Ordering::Greater, true),
                    // Only a genuine boolean counts. A non-boolean operand makes
                    // the whole clause Null rather than quietly reading as false,
                    // so `not (x and y)` cannot turn a broken comparison into a
                    // confident answer.
                    Op::And => {
                        return match (l.as_bool(), r.as_bool()) {
                            (Some(a), Some(b)) => Value::Boolean(a && b),
                            _ => Value::Null,
                        }
                    }
                    Op::Or => {
                        return match (l.as_bool(), r.as_bool()) {
                            (Some(a), Some(b)) => Value::Boolean(a || b),
                            _ => Value::Null,
                        }
                    }
                    _ => {}
                }

                // 4. Numeric Math
                if let (Some(n1), Some(n2)) = (l.as_f64(), r.as_f64()) {
                    match op {
                        Op::Add => Value::Number(n1 + n2),
                        Op::Sub => Value::Number(n1 - n2),
                        Op::Mul => Value::Number(n1 * n2),
                        Op::Div => {
                            if n2 == 0.0 {
                                Value::Null
                            } else {
                                Value::Number(n1 / n2)
                            }
                        }
                        _ => Value::Null,
                    }
                } else if op == &Op::Sub {
                    if let (Value::Date(d1), Value::Date(d2)) = (&l, &r) {
                        return Value::Number((*d1 - *d2).num_days() as f64);
                    }
                    if let (Value::Datetime(dt1), Value::Datetime(dt2)) = (&l, &r) {
                        return Value::Number((*dt1 - *dt2).num_seconds() as f64);
                    }
                    Value::Null
                } else {
                    Value::Null
                }
            }
            Expr::InList { left, list } => {
                let l = left.eval(row_idx, col_lookup, df);
                for item in list {
                    if values_equal(&l, &item.eval(row_idx, col_lookup, df)) {
                        return Value::Boolean(true);
                    }
                }
                Value::Boolean(false)
            }
            Expr::FunctionCall { name, args } => {
                let mut evaluated_args: Vec<Value> = args
                    .iter()
                    .map(|a| a.eval(row_idx, col_lookup, df))
                    .collect();
                // A date read from a text column.
                if matches!(
                    name.as_str(),
                    "year" | "month" | "day" | "hour" | "minute" | "date_format"
                ) {
                    if let Some(first) = evaluated_args.first_mut() {
                        *first = as_temporal(std::mem::replace(first, Value::Null));
                    }
                }

                match name.as_str() {
                    "text" if evaluated_args.len() == 1 => match &evaluated_args[0] {
                        Value::Null => Value::Null,
                        other => Value::String(other.to_string()),
                    },
                    // A missing value matches nothing; as text it was `Null`.
                    "contains" if evaluated_args[0] == Value::Null => Value::Null,
                    "contains" if evaluated_args.len() == 2 => {
                        match regex::Regex::new(&evaluated_args[1].to_string()) {
                            Ok(re) => Value::Boolean(re.is_match(&evaluated_args[0].to_string())),
                            // A malformed pattern is Null, not false — a bad
                            // regex is not the same answer as "no match".
                            Err(_) => Value::Null,
                        }
                    }
                    "concat" => {
                        let result: String = evaluated_args
                            .iter()
                            .map(|v| match v {
                                Value::String(s) => s.clone(),
                                Value::Number(n) => n.to_string(),
                                Value::Boolean(b) => b.to_string(),
                                Value::Date(d) => d.to_string(),
                                Value::Datetime(dt) => dt.to_string(),
                                Value::Null => "".to_string(),
                            })
                            .collect();
                        Value::String(result)
                    }
                    "split" => {
                        // Returns first part temporarily (to keep Value simple)
                        if evaluated_args.len() == 2 {
                            if let (Some(s), Value::String(delim)) =
                                (text_of(&evaluated_args[0]), &evaluated_args[1])
                            {
                                return Value::String(
                                    s.split(delim).next().unwrap_or("").to_string(),
                                );
                            }
                        }
                        Value::Null
                    }
                    "substring" => {
                        if evaluated_args.len() == 3 {
                            if let (Some(s), Value::Number(start), Value::Number(len)) = (
                                text_of(&evaluated_args[0]),
                                &evaluated_args[1],
                                &evaluated_args[2],
                            ) {
                                let st = *start as usize;
                                let ln = *len as usize;
                                let chars: String = s.chars().skip(st).take(ln).collect();
                                return Value::String(chars);
                            }
                        }
                        Value::Null
                    }
                    "len" => {
                        if evaluated_args.len() == 1 {
                            return match text_of(&evaluated_args[0]) {
                                Some(s) => Value::Number(s.chars().count() as f64),
                                None => Value::Null,
                            };
                        }
                        Value::Null
                    }
                    "if" => {
                        // Expecting 3 arguments
                        if args.len() == 3 {
                            let cond = args[0].eval(row_idx, col_lookup, df);
                            if let Some(b) = cond.as_bool() {
                                if b {
                                    return args[1].eval(row_idx, col_lookup, df);
                                } else {
                                    return args[2].eval(row_idx, col_lookup, df);
                                }
                            }
                        }
                        Value::Null
                    }
                    "year" => {
                        if evaluated_args.len() == 1 {
                            match &evaluated_args[0] {
                                Value::Date(d) => return Value::Number(d.year() as f64),
                                Value::Datetime(dt) => return Value::Number(dt.year() as f64),
                                _ => return Value::Null,
                            }
                        }
                        Value::Null
                    }
                    "month" => {
                        if evaluated_args.len() == 1 {
                            match &evaluated_args[0] {
                                Value::Date(d) => return Value::Number(d.month() as f64),
                                Value::Datetime(dt) => return Value::Number(dt.month() as f64),
                                _ => return Value::Null,
                            }
                        }
                        Value::Null
                    }
                    "day" => {
                        if evaluated_args.len() == 1 {
                            match &evaluated_args[0] {
                                Value::Date(d) => return Value::Number(d.day() as f64),
                                Value::Datetime(dt) => return Value::Number(dt.day() as f64),
                                _ => return Value::Null,
                            }
                        }
                        Value::Null
                    }
                    "hour" => {
                        if evaluated_args.len() == 1 {
                            match &evaluated_args[0] {
                                Value::Datetime(dt) => return Value::Number(dt.hour() as f64),
                                _ => return Value::Null,
                            }
                        }
                        Value::Null
                    }
                    "minute" => {
                        if evaluated_args.len() == 1 {
                            match &evaluated_args[0] {
                                Value::Datetime(dt) => return Value::Number(dt.minute() as f64),
                                _ => return Value::Null,
                            }
                        }
                        Value::Null
                    }
                    "today" => Value::Date(chrono::Local::now().naive_local().date()),
                    "now" => Value::Datetime(chrono::Local::now().naive_local()),
                    "date_format" => {
                        if evaluated_args.len() == 2 {
                            if let (Value::String(fmt), v) =
                                (&evaluated_args[1], &evaluated_args[0])
                            {
                                // `to_string` panics on a pattern chrono cannot format.
                                use std::fmt::Write;
                                let mut out = String::new();
                                let written = match v {
                                    Value::Date(d) => write!(out, "{}", d.format(fmt)),
                                    Value::Datetime(dt) => write!(out, "{}", dt.format(fmt)),
                                    _ => return Value::Null,
                                };
                                return match written {
                                    Ok(()) => Value::String(out),
                                    Err(_) => Value::Null,
                                };
                            }
                        }
                        Value::Null
                    }
                    "date" => {
                        if evaluated_args.len() == 1 {
                            match &evaluated_args[0] {
                                Value::Datetime(dt) => return Value::Date(dt.date()),
                                Value::Date(d) => return Value::Date(*d),
                                Value::String(s) => {
                                    // Try parsing as Date directly
                                    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                                    {
                                        return Value::Date(d);
                                    }
                                    // Try parsing as Datetime, extract date
                                    for fmt in crate::data::DATETIME_FORMATS {
                                        if let Ok(dt) =
                                            chrono::NaiveDateTime::parse_from_str(s, fmt)
                                        {
                                            return Value::Date(dt.date());
                                        }
                                    }
                                }
                                _ => return Value::Null,
                            }
                        }
                        Value::Null
                    }
                    _ => Value::Null,
                }
            }
            Expr::If {
                cond,
                then_branch,
                else_branch,
            } => {
                let c = cond.eval(row_idx, col_lookup, df);
                if c.as_bool() == Some(true) {
                    then_branch.eval(row_idx, col_lookup, df)
                } else {
                    else_branch.eval(row_idx, col_lookup, df)
                }
            }
        }
    }
}

/// A value as the string functions see it.  A cell is typed by how it looks —
/// `2026-01-01` arrives as a date — so a function over text takes any value.
fn text_of(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        v => Some(v.to_string()),
    }
}

/// What a cell's text reads as: a number, a boolean, a date or a datetime, NULL when
/// empty, text otherwise.
fn infer(text: &str) -> Value {
    if let Ok(n) = text.parse::<f64>() {
        Value::Number(n)
    } else if let Ok(b) = text.parse::<bool>() {
        Value::Boolean(b)
    } else if let Some(t) = parse_temporal(text) {
        t
    } else if text.is_empty() {
        Value::Null
    } else {
        Value::String(text.to_string())
    }
}

/// `date + days`, or NULL past the calendar's range, where chrono panics.
fn add_days(d: NaiveDate, days: f64) -> Value {
    chrono::TimeDelta::try_days(days as i64)
        .and_then(|delta| d.checked_add_signed(delta))
        .map_or(Value::Null, Value::Date)
}

/// `datetime + seconds`, or NULL past the calendar's range, where chrono panics.
fn add_seconds(dt: NaiveDateTime, secs: f64) -> Value {
    chrono::TimeDelta::try_seconds(secs as i64)
        .and_then(|delta| dt.checked_add_signed(delta))
        .map_or(Value::Null, Value::Datetime)
}

/// Text that reads as a date or a datetime, as one; any other value unchanged.  A
/// text column stays text until an operation needs a date from it.
fn as_temporal(v: Value) -> Value {
    if let Value::String(s) = &v {
        if let Some(t) = parse_temporal(s) {
            return t;
        }
    }
    v
}

/// `==` for the interpreter.  Text meets a number as a number when it reads as one
/// (`"007" == 7`), and a date as a date; everything else compares as it is.
fn values_equal(l: &Value, r: &Value) -> bool {
    match (l, r) {
        (Value::String(s), Value::Number(n)) | (Value::Number(n), Value::String(s)) => {
            s.parse::<f64>().is_ok_and(|x| x == *n)
        }
        (Value::String(_), Value::Date(_) | Value::Datetime(_))
        | (Value::Date(_) | Value::Datetime(_), Value::String(_)) => {
            as_temporal(l.clone()) == as_temporal(r.clone())
        }
        _ => l == r,
    }
}

/// A date (`YYYY-MM-DD`) or a datetime in one of the forms a cell takes.
fn parse_temporal(text: &str) -> Option<Value> {
    if let Ok(d) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Some(Value::Date(d));
    }
    for fmt in ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%dT%H:%M:%S%.f%#z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(text, fmt) {
            return Some(Value::Datetime(dt.naive_local()));
        }
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(text, fmt) {
            return Some(Value::Datetime(dt));
        }
    }
    None
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let tok = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(tok)
        } else {
            None
        }
    }

    /// expr → or
    fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_or()
    }

    /// or → and ('or' and)*
    fn parse_or(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and()?;
        while let Some(Token::Or) = self.peek() {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinOp {
                op: Op::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// and → not ('and' not)*
    fn parse_and(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_not()?;
        while let Some(Token::And) = self.peek() {
            self.advance();
            let right = self.parse_not()?;
            left = Expr::BinOp {
                op: Op::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// not → 'not' not | comparison
    fn parse_not(&mut self) -> Result<Expr, String> {
        if let Some(Token::Not) = self.peek() {
            self.advance();
            return Ok(Expr::Not(Box::new(self.parse_not()?)));
        }
        self.parse_comparison()
    }

    /// comparison → term_add (('<' | '>' | '==' | '!=' | '<=' | '>=') term_add | 'in' '(' list ')')*
    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_term_add()?;
        loop {
            match self.peek() {
                Some(Token::Eq) => {
                    self.advance();
                    let right = self.parse_term_add()?;
                    left = Expr::BinOp {
                        op: Op::Eq,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                Some(Token::NotEq) => {
                    self.advance();
                    let right = self.parse_term_add()?;
                    left = Expr::BinOp {
                        op: Op::NotEq,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                Some(Token::Lt) => {
                    self.advance();
                    let right = self.parse_term_add()?;
                    left = Expr::BinOp {
                        op: Op::Lt,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                Some(Token::Gt) => {
                    self.advance();
                    let right = self.parse_term_add()?;
                    left = Expr::BinOp {
                        op: Op::Gt,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                Some(Token::Leq) => {
                    self.advance();
                    let right = self.parse_term_add()?;
                    left = Expr::BinOp {
                        op: Op::Leq,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                Some(Token::Geq) => {
                    self.advance();
                    let right = self.parse_term_add()?;
                    left = Expr::BinOp {
                        op: Op::Geq,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                Some(Token::In) => {
                    self.advance();
                    if let Some(Token::LParen) = self.peek() {
                        self.advance();
                        let mut list = Vec::new();
                        if let Some(Token::RParen) = self.peek() {
                            self.advance();
                        } else {
                            loop {
                                list.push(self.parse_expr()?);
                                match self.peek() {
                                    Some(Token::Comma) => {
                                        self.advance();
                                    }
                                    Some(Token::RParen) => {
                                        self.advance();
                                        break;
                                    }
                                    tok => {
                                        return Err(format!("Expected ',' or ')', got {:?}", tok))
                                    }
                                }
                            }
                        }
                        left = Expr::InList {
                            left: Box::new(left),
                            list,
                        };
                    } else {
                        return Err("Expected '(' after 'in'".to_string());
                    }
                }
                _ => break,
            }
        }
        Ok(left)
    }

    /// term_add → term_mul (('+' | '-') term_mul)*
    fn parse_term_add(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_term_mul()?;
        loop {
            match self.peek() {
                Some(Token::Plus) => {
                    self.advance();
                    let right = self.parse_term_mul()?;
                    left = Expr::BinOp {
                        op: Op::Add,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                Some(Token::Minus) => {
                    self.advance();
                    let right = self.parse_term_mul()?;
                    left = Expr::BinOp {
                        op: Op::Sub,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                _ => break,
            }
        }
        Ok(left)
    }

    /// term_mul → factor (('*' | '/') factor)*
    fn parse_term_mul(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_factor()?;
        loop {
            match self.peek() {
                Some(Token::Star) => {
                    self.advance();
                    let right = self.parse_factor()?;
                    left = Expr::BinOp {
                        op: Op::Mul,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                Some(Token::Slash) => {
                    self.advance();
                    let right = self.parse_factor()?;
                    left = Expr::BinOp {
                        op: Op::Div,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                _ => break,
            }
        }
        Ok(left)
    }

    /// factor → NUMBER | STRING | COLUMN_NAME | FUNCTION_CALL | '(' expr ')'
    fn parse_factor(&mut self) -> Result<Expr, String> {
        // Unary minus: `-x` is `0 - x`, which is what the user wrote down anyway, and
        // saves the AST a variant no evaluator would treat differently.
        if let Some(Token::Minus) = self.peek() {
            self.advance();
            let operand = self.parse_factor()?;
            return Ok(Expr::BinOp {
                op: Op::Sub,
                left: Box::new(Expr::Literal(Value::Number(0.0))),
                right: Box::new(operand),
            });
        }
        match self.advance() {
            Some(Token::Number(n)) => Ok(Expr::Literal(Value::Number(n))),
            Some(Token::StringLit(s)) => Ok(Expr::Literal(Value::String(s))),
            Some(Token::Ident(name)) => {
                if let Some(Token::LParen) = self.peek() {
                    // It's a function call or if condition
                    self.advance(); // consume '('
                    let mut args = Vec::new();
                    if let Some(Token::RParen) = self.peek() {
                        self.advance(); // consume ')'
                    } else {
                        loop {
                            args.push(self.parse_expr()?);
                            match self.peek() {
                                Some(Token::Comma) => {
                                    self.advance(); // consume ','
                                }
                                Some(Token::RParen) => {
                                    self.advance();
                                    break;
                                }
                                tok => return Err(format!("Expected ',' or ')', got {:?}", tok)),
                            }
                        }
                    }
                    if name == "if" && args.len() == 3 {
                        let mut args_iter = args.into_iter();
                        Ok(Expr::If {
                            cond: Box::new(args_iter.next().unwrap()),
                            then_branch: Box::new(args_iter.next().unwrap()),
                            else_branch: Box::new(args_iter.next().unwrap()),
                        })
                    } else {
                        Ok(Expr::FunctionCall { name, args })
                    }
                } else {
                    Ok(Expr::ColumnRef(name))
                }
            }
            Some(Token::LParen) => {
                let expr = self.parse_expr()?;
                match self.advance() {
                    Some(Token::RParen) => Ok(expr),
                    _ => Err("Expected closing parenthesis ')'".to_string()),
                }
            }
            Some(tok) => Err(format!("Unexpected token: {:?}", tok)),
            None => Err("Unexpected end of expression".to_string()),
        }
    }
}

// ── Tokenizer ───────────────────────────────────────────────────────────────────

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' => {
                i += 1;
            }
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Leq);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Geq);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            '=' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Eq);
                    i += 2;
                } else {
                    return Err(
                        "Expected '==' for equality, but got '='. Assignments are not supported."
                            .to_string(),
                    );
                }
            }
            '!' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::NotEq);
                    i += 2;
                } else {
                    return Err("Expected '!=' for inequality, but got '!'".to_string());
                }
            }
            // A column whose name has a space in it — or a dash, or anything else the
            // bare identifier rule below would stop at — is written in backticks.
            // Double quotes cannot serve: they are a string literal, and a table with a
            // column called "К выплате" would have no way to say which was meant.
            '`' => {
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '`' {
                    i += 1;
                }
                if i >= chars.len() {
                    return Err("Unterminated `column name`".to_string());
                }
                let name: String = chars[start..i].iter().collect();
                if name.is_empty() {
                    return Err("Empty column name in backticks".to_string());
                }
                tokens.push(Token::Ident(name));
                i += 1; // consume closing backtick
            }
            '"' | '\'' => {
                let quote = chars[i];
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != quote {
                    i += 1;
                }
                if i < chars.len() {
                    let s: String = chars[start..i].iter().collect();
                    tokens.push(Token::StringLit(s));
                    i += 1; // consume closing quote
                } else {
                    return Err("Unterminated string literal".to_string());
                }
            }
            c if c.is_ascii_digit() || c == '.' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let num_str: String = chars[start..i].iter().collect();
                let num: f64 = num_str
                    .parse()
                    .map_err(|_| format!("Invalid number: '{}'", num_str))?;
                tokens.push(Token::Number(num));
            }
            c if c.is_alphanumeric() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let name: String = chars[start..i].iter().collect();
                match name.to_lowercase().as_str() {
                    "in" => tokens.push(Token::In),
                    "and" => tokens.push(Token::And),
                    "or" => tokens.push(Token::Or),
                    "not" => tokens.push(Token::Not),
                    _ => tokens.push(Token::Ident(name)),
                }
            }
            c => {
                return Err(format!("Unexpected character: '{}'", c));
            }
        }
    }

    Ok(tokens)
}

/// Returns `Value::Boolean(l op r)` for ordered comparison types.
/// `target` is `Ordering::Less` for `<`/`<=` and `Ordering::Greater` for `>`/`>=`.
/// `allow_equal` adds equality to the comparison (i.e. `<=` vs `<`).
fn compare_ordered(l: &Value, r: &Value, target: std::cmp::Ordering, allow_equal: bool) -> Value {
    let matches = |ord: std::cmp::Ordering| {
        ord == target || (allow_equal && ord == std::cmp::Ordering::Equal)
    };
    if let (Some(n1), Some(n2)) = (l.as_f64(), r.as_f64()) {
        return Value::Boolean(matches(
            n1.partial_cmp(&n2).unwrap_or(std::cmp::Ordering::Equal),
        ));
    }
    // Text against a date compares as a date when it reads as one.
    let (l, r) = match (l, r) {
        (Value::String(_), Value::Date(_) | Value::Datetime(_))
        | (Value::Date(_) | Value::Datetime(_), Value::String(_)) => {
            (as_temporal(l.clone()), as_temporal(r.clone()))
        }
        _ => (l.clone(), r.clone()),
    };
    match (&l, &r) {
        (Value::String(s1), Value::String(s2)) => Value::Boolean(matches(s1.cmp(s2))),
        (Value::Date(d1), Value::Date(d2)) => Value::Boolean(matches(d1.cmp(d2))),
        (Value::Datetime(dt1), Value::Datetime(dt2)) => Value::Boolean(matches(dt1.cmp(dt2))),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{NamedFrom, Series};

    fn mock_df(data: Vec<Vec<String>>, names: Vec<&str>) -> crate::data::dataframe::DataFrame {
        let mut series_vec = Vec::new();
        for (i, col_data) in data.into_iter().enumerate() {
            series_vec.push(Series::new(names[i].into(), &col_data).into());
        }
        let df = polars::prelude::DataFrame::new_infer_height(series_vec).unwrap();
        let mut tui_df = crate::data::dataframe::DataFrame::empty();
        tui_df.df = df;
        tui_df
    }

    fn mock_int_df(data: Vec<Vec<i64>>, names: Vec<&str>) -> crate::data::dataframe::DataFrame {
        let series_vec = data
            .into_iter()
            .enumerate()
            .map(|(i, col_data)| Series::new(names[i].into(), &col_data).into())
            .collect();
        let mut tui_df = crate::data::dataframe::DataFrame::empty();
        tui_df.df = polars::prelude::DataFrame::new_infer_height(series_vec).unwrap();
        tui_df
    }

    #[test]
    fn test_parse_simple_add() {
        let expr = Expr::parse("a+b").unwrap();
        let data = vec![vec![10, 20], vec![3, 7]];
        let lookup: HashMap<&str, usize> = [("a", 0), ("b", 1)].into_iter().collect();
        let df = mock_int_df(data, vec!["a", "b"]);
        assert_eq!(expr.eval(0, &lookup, &df), Value::Number(13.0));
        assert_eq!(expr.eval(1, &lookup, &df), Value::Number(27.0));
    }

    #[test]
    fn test_parse_multiply_literal() {
        let expr = Expr::parse("age*2").unwrap();
        let data = vec![vec!["25".to_string(), "30".to_string()]];
        let lookup: HashMap<&str, usize> = [("age", 0)].into_iter().collect();
        let df = mock_df(data, vec!["age"]);
        assert_eq!(expr.eval(0, &lookup, &df), Value::Number(50.0));
        assert_eq!(expr.eval(1, &lookup, &df), Value::Number(60.0));
    }

    #[test]
    fn test_parse_parentheses() {
        let expr = Expr::parse("(a+b)*c").unwrap();
        let data = vec![vec![2], vec![3], vec![4]];
        let lookup: HashMap<&str, usize> = [("a", 0), ("b", 1), ("c", 2)].into_iter().collect();
        let df = mock_int_df(data, vec!["a", "b", "c"]);
        assert_eq!(expr.eval(0, &lookup, &df), Value::Number(20.0));
    }

    #[test]
    fn test_division_by_zero() {
        let expr = Expr::parse("a/b").unwrap();
        let data = vec![vec!["10".to_string()], vec!["0".to_string()]];
        let lookup: HashMap<&str, usize> = [("a", 0), ("b", 1)].into_iter().collect();
        let df = mock_df(data, vec!["a", "b"]);
        assert_eq!(expr.eval(0, &lookup, &df), Value::Null);
    }

    #[test]
    fn test_logical_ops() {
        let expr = Expr::parse("a > b").unwrap();
        let expr_eq = Expr::parse("a == b").unwrap();
        let expr_neq = Expr::parse("a != b").unwrap();

        assert!(matches!(expr, Expr::BinOp { op: Op::Gt, .. }));
        assert!(matches!(expr_eq, Expr::BinOp { op: Op::Eq, .. }));
        assert!(matches!(expr_neq, Expr::BinOp { op: Op::NotEq, .. }));
    }

    #[test]
    fn test_string_functions() {
        let expr = Expr::parse("concat('hello', \" world\")").unwrap();
        let lookup: HashMap<&str, usize> = HashMap::new();
        let df = mock_df(vec![], vec![]);
        assert_eq!(
            expr.eval(0, &lookup, &df),
            Value::String("hello world".to_string())
        );
    }

    #[test]
    fn test_if_condition() {
        let expr = Expr::parse("if(1 > 0, 'yes', 'no')").unwrap();
        let lookup: HashMap<&str, usize> = HashMap::new();
        let df = mock_df(vec![], vec![]);
        assert_eq!(expr.eval(0, &lookup, &df), Value::String("yes".to_string()));

        let expr2 = Expr::parse("if(0 > 1, 'yes', 'no')").unwrap();
        assert_eq!(expr2.eval(0, &lookup, &df), Value::String("no".to_string()));
    }

    #[test]
    fn test_invalid_expression() {
        assert!(Expr::parse("").is_err());
        assert!(Expr::parse("(a+b").is_err());
        assert!(Expr::parse("a++b").is_err());
        assert!(Expr::parse("a = b").is_err()); // Ensure assignment fails nicely
    }

    // ── Boolean combination ────────────────────────────────────────────────

    /// Read the boolean an expression evaluates to for one row of a frame,
    /// through the Polars path rather than the interpreter.
    fn polars_bool(expr: &str, df: &crate::data::dataframe::DataFrame, row: usize) -> Option<bool> {
        // `polars::prelude::*` brings its own `Expr`, so name ours explicitly.
        use polars::prelude::IntoLazy;
        let parsed = super::Expr::parse(expr).expect("expression must parse");
        let polars_expr = parsed.to_polars_expr().expect("must lower to Polars");
        let out = df
            .df
            .clone()
            .lazy()
            .select([polars_expr.alias("m")])
            .collect()
            .expect("must evaluate");
        out.column("m").unwrap().bool().unwrap().get(row)
    }

    fn sample() -> crate::data::dataframe::DataFrame {
        crate::data::io::load_file(std::path::Path::new("test_data/sample.csv"), None).unwrap()
    }

    #[test]
    fn and_or_not_parse_and_lower_to_polars() {
        let df = sample();
        // Row 0 is Alice Johnson, 30, Engineering.
        assert_eq!(
            polars_bool("department == \"Engineering\" and age > 25", &df, 0),
            Some(true)
        );
        assert_eq!(
            polars_bool("department == \"HR\" or age > 25", &df, 0),
            Some(true)
        );
        assert_eq!(
            polars_bool("not (department == \"Engineering\")", &df, 0),
            Some(false)
        );
    }

    /// `or` must bind looser than `and`, so `a and b or c` reads as
    /// `(a and b) or c` — the conventional reading, and the one a user writing
    /// a filter expects.
    #[test]
    fn or_binds_looser_than_and() {
        let df = sample();
        // Row 0: Engineering, 30. The `and` clause is false (age is not > 40),
        // the trailing `or` clause is true. Under the wrong precedence —
        // a and (b or c) — this would be true as well, so pick a case that
        // separates them: false and false or true.
        assert_eq!(
            polars_bool(
                "department == \"HR\" and age > 40 or department == \"Engineering\"",
                &df,
                0
            ),
            Some(true)
        );
        // a and (b or c) would give true here; (a and b) or c gives false.
        assert_eq!(
            polars_bool("department == \"HR\" and age > 40 or age > 100", &df, 0),
            Some(false)
        );
    }

    #[test]
    fn contains_matches_a_regex_against_a_column() {
        let df = sample();
        assert_eq!(
            polars_bool("contains(name, \"^Alice\")", &df, 0),
            Some(true)
        );
        assert_eq!(polars_bool("contains(name, \"^Bob\")", &df, 0), Some(false));
    }

    /// The interpreter has to agree with the Polars path, since the TUI falls
    /// back to it for expressions Polars cannot lower.
    #[test]
    fn the_interpreter_agrees_on_boolean_combination() {
        let df = sample();
        let lookup: std::collections::HashMap<&str, usize> = df
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.as_str(), i))
            .collect();

        for (source, expected) in [
            ("department == \"Engineering\" and age > 25", true),
            ("department == \"HR\" or age > 25", true),
            ("not (department == \"Engineering\")", false),
            ("department == \"HR\" and age > 40 or age > 100", false),
            ("contains(name, \"^Alice\")", true),
        ] {
            let parsed = Expr::parse(source).expect(source);
            assert_eq!(
                parsed.eval(0, &lookup, &df).as_bool(),
                Some(expected),
                "interpreter disagrees on: {}",
                source
            );
        }
    }

    /// A non-boolean operand yields Null rather than reading as false — so a
    /// broken comparison cannot be negated into a confident answer.
    #[test]
    fn a_non_boolean_operand_makes_the_clause_null() {
        let df = sample();
        let lookup: std::collections::HashMap<&str, usize> = df
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.as_str(), i))
            .collect();

        let parsed = Expr::parse("age and department").unwrap();
        assert_eq!(parsed.eval(0, &lookup, &df), Value::Null);

        let negated = Expr::parse("not age").unwrap();
        assert_eq!(negated.eval(0, &lookup, &df), Value::Null);
    }

    #[test]
    fn a_column_name_with_a_space_is_written_in_backticks() {
        let expr = Expr::parse("`К выплате` * 1").unwrap();
        match &expr {
            Expr::BinOp { left, .. } => match left.as_ref() {
                Expr::ColumnRef(name) => assert_eq!(name, "К выплате"),
                other => panic!("{:?}", other),
            },
            other => panic!("{:?}", other),
        }
        assert!(Expr::parse("`unterminated * 1").is_err());
        assert!(Expr::parse("`` * 1").is_err());
    }

    #[test]
    fn a_leading_minus_negates() {
        // `-x` parses as `0 - x`; the shape is what the evaluator already handles.
        match Expr::parse("-quantity").unwrap() {
            Expr::BinOp {
                op: Op::Sub, left, ..
            } => match left.as_ref() {
                Expr::Literal(Value::Number(n)) => assert_eq!(*n, 0.0),
                other => panic!("{:?}", other),
            },
            other => panic!("{:?}", other),
        }
        assert!(Expr::parse("if(a > 1, b, -b)").is_ok());
        assert!(Expr::parse("3 - -2").is_ok());
    }

    #[test]
    fn text_in_arithmetic_is_refused_unless_it_is_a_number() {
        let is_col = |s: &str| s == "К выплате";
        // The conversion idiom stays.
        assert!(Expr::parse("\"3\" * 1")
            .unwrap()
            .check_text_arithmetic(&is_col)
            .is_ok());
        // Comparing against text is ordinary, whatever the columns are called.
        assert!(Expr::parse("kind == \"К выплате\"")
            .unwrap()
            .check_text_arithmetic(&is_col)
            .is_ok());
        // Reaching for a column and getting a string literal is not.
        let err = Expr::parse("\"К выплате\" * 1")
            .unwrap()
            .check_text_arithmetic(&is_col)
            .unwrap_err();
        assert!(err.contains("backticks"), "{}", err);
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| s.to_string()).collect()
    }

    fn codes() -> (
        crate::data::dataframe::DataFrame,
        HashMap<&'static str, usize>,
    ) {
        let df = mock_df(
            vec![
                strings(&["007", "nan", "1e3", "A12"]),
                strings(&["2026-01-15", "2026-01-15 10:30:00", "x", "y"]),
            ],
            vec!["sku", "when"],
        );
        (df, [("sku", 0), ("when", 1)].into_iter().collect())
    }

    /// Reading a text cell by its look turned `007` into 7 and `nan` into NaN.
    #[test]
    fn a_text_column_stays_text() {
        let (df, lookup) = codes();
        let at = |e: &str, row| Expr::parse(e).unwrap().eval(row, &lookup, &df);

        assert_eq!(at("concat(sku, '|')", 0), Value::String("007|".into()));
        assert_eq!(at("concat(sku, '|')", 2), Value::String("1e3|".into()));
        assert_eq!(at("substring(sku, 0, 2)", 1), Value::String("na".into()));
        // Two texts add up to one text, as in Polars.
        assert_eq!(at("sku + sku", 0), Value::String("007007".into()));
        // Arithmetic still reads a number out of text.
        assert_eq!(at("sku * 2", 0), Value::Number(14.0));
        // Date functions and date arithmetic still read a date out of text.
        assert_eq!(at("year(when)", 0), Value::Number(2026.0));
        assert_eq!(at("hour(when)", 1), Value::Number(10.0));
        assert_eq!(
            at("date_format(when, '%Y-%m')", 0),
            Value::String("2026-01".into())
        );
        assert_eq!(
            at("when - 1", 0),
            Value::Date(NaiveDate::from_ymd_opt(2026, 1, 14).unwrap())
        );
    }

    #[test]
    fn text_meets_numbers_and_dates_on_their_terms() {
        let (df, lookup) = codes();
        let at = |e: &str, row| Expr::parse(e).unwrap().eval(row, &lookup, &df);

        assert_eq!(at("sku == 7", 0), Value::Boolean(true));
        assert_eq!(at("sku != 7", 0), Value::Boolean(false));
        assert_eq!(at("sku in (7, 8)", 0), Value::Boolean(true));
        assert_eq!(at("sku == 7", 3), Value::Boolean(false));
        assert_eq!(at("sku > 9", 2), Value::Boolean(true));
        // Text that is not a number has no order against one.
        assert_eq!(at("sku > 9", 3), Value::Null);
    }

    #[test]
    fn a_date_compares_with_text_that_reads_as_one() {
        let d = Series::new("d".into(), [NaiveDate::from_ymd_opt(2026, 1, 15)]);
        let mut df = crate::data::dataframe::DataFrame::empty();
        df.df = polars::prelude::DataFrame::new_infer_height(vec![d.into()]).unwrap();
        let lookup: HashMap<&str, usize> = [("d", 0)].into_iter().collect();
        let at = |e: &str| Expr::parse(e).unwrap().eval(0, &lookup, &df);

        assert_eq!(at("d > '2026-01-01'"), Value::Boolean(true));
        assert_eq!(at("d == '2026-01-15'"), Value::Boolean(true));
        assert_eq!(at("d < 'not a date'"), Value::Null);
    }

    /// A text column of `true`/`false` (SQLite TEXT, JSON strings, a CSV column with
    /// other values mixed in) still works as a condition.
    #[test]
    fn text_true_and_false_still_work_as_conditions() {
        let df = mock_df(vec![strings(&["true", "false", "maybe"])], vec!["active"]);
        let lookup: HashMap<&str, usize> = [("active", 0)].into_iter().collect();
        let at = |e: &str, row| Expr::parse(e).unwrap().eval(row, &lookup, &df);

        assert_eq!(at("if(active, 1, 0)", 0), Value::Number(1.0));
        assert_eq!(at("if(active, 1, 0)", 1), Value::Number(0.0));
        assert_eq!(at("not active", 1), Value::Boolean(true));
        assert_eq!(at("active and 1 > 0", 0), Value::Boolean(true));
        assert_eq!(at("not active", 2), Value::Null);
    }

    /// A missing value matches no pattern: read as text it was `Null`, which `^N` found.
    #[test]
    fn contains_on_a_missing_value_is_null() {
        let name = Series::new("name".into(), [Some("Nina"), None]);
        let mut df = crate::data::dataframe::DataFrame::empty();
        df.df = polars::prelude::DataFrame::new_infer_height(vec![name.into()]).unwrap();
        let lookup: HashMap<&str, usize> = [("name", 0)].into_iter().collect();
        let at = |e: &str, row| Expr::parse(e).unwrap().eval(row, &lookup, &df);

        assert_eq!(at("contains(name, '^N')", 0), Value::Boolean(true));
        assert_eq!(at("contains(name, '^N')", 1), Value::Null);
    }

    /// Past the calendar's range there is no answer.  chrono panics there, and the
    /// TUI has no boundary to catch it: the whole app went down.
    #[test]
    fn date_arithmetic_out_of_range_is_null() {
        let (df, lookup) = codes();
        let at = |e: &str, row| Expr::parse(e).unwrap().eval(row, &lookup, &df);

        assert_eq!(at("when + 1000000000", 0), Value::Null);
        assert_eq!(at("when - 1000000000", 0), Value::Null);
        assert_eq!(at("when + 100000000000000000000", 1), Value::Null);
    }

    /// A pattern chrono cannot format has no answer; `to_string` panicked on it.
    #[test]
    fn date_format_with_a_bad_pattern_is_null() {
        let (df, lookup) = codes();
        let at = |e: &str, row| Expr::parse(e).unwrap().eval(row, &lookup, &df);

        assert_eq!(at("date_format(when, '%Q')", 0), Value::Null);
    }
}
