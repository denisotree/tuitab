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

/// Every function the expression language knows, with how many arguments it takes
/// (fewest, most).  Checked when an expression is parsed: a name missing from here used
/// to evaluate to a column of NULLs with no error, and a wrong count could panic.
pub const FUNCTIONS: &[(&str, usize, usize)] = &[
    ("concat", 1, usize::MAX),
    ("split", 2, 2),
    ("substring", 3, 3),
    ("len", 1, 1),
    ("contains", 2, 2),
    ("text", 1, 1),
    ("if", 3, 3),
    ("sum", 1, 1),
    ("count", 1, 1),
    ("mean", 1, 1),
    ("min", 1, 1),
    ("max", 1, 1),
    ("year", 1, 1),
    ("month", 1, 1),
    ("day", 1, 1),
    ("hour", 1, 1),
    ("minute", 1, 1),
    ("date", 1, 1),
    ("date_format", 2, 2),
    ("today", 0, 0),
    ("now", 0, 0),
    ("abs", 1, 1),
    ("round", 1, 2),
    ("floor", 1, 1),
    ("ceil", 1, 1),
    ("sqrt", 1, 1),
    ("exp", 1, 1),
    ("ln", 1, 1),
    ("log10", 1, 1),
    ("log", 2, 2),
    ("regexp_replace", 3, 3),
    ("regexp_extract", 2, 3),
    ("from_epoch", 2, 2),
    ("time_bucket", 2, 2),
    ("json_get", 2, 2),
];

/// `json_get`'s path: a literal in quotes that [`crate::data::json_cols::path_steps`]
/// accepts.
fn json_path_of(arg: &Expr) -> Result<(String, Vec<crate::data::json_cols::Step>), String> {
    match arg {
        Expr::Literal(Value::String(p)) => {
            crate::data::json_cols::path_steps(p).map(|steps| (p.clone(), steps))
        }
        _ => Err("json_get() needs its path in quotes, as \"$.country_code\"".to_string()),
    }
}

/// Which kind of time a sub-expression produces, when the frame's types say.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Temporal {
    Date,
    Datetime,
}

/// Whether a sub-expression is text on both paths alike — a text column, a string
/// literal, or a function that returns text — so a text function over it gives the same
/// characters vectorised as row by row.  (A number turned into text is formatted
/// differently by the two, so those stay row by row.)
fn is_text(e: &Expr, schema: Option<&polars::prelude::Schema>) -> bool {
    match e {
        Expr::Literal(Value::String(_)) => true,
        Expr::ColumnRef(name) => matches!(
            schema.and_then(|s| s.get(name.as_str())),
            Some(polars::prelude::DataType::String)
        ),
        Expr::FunctionCall { name, .. } => matches!(
            name.as_str(),
            "regexp_replace" | "regexp_extract" | "concat" | "substring" | "split"
        ),
        _ => false,
    }
}

/// A value as the text the row-by-row path makes of it.  A datetime cast to text
/// carries its unit's fraction (`11:10:00.000`); chrono's `%.f` shows one only when
/// there is one, as the table does.
fn as_text(
    e: &Expr,
    schema: Option<&polars::prelude::Schema>,
) -> Result<polars::lazy::dsl::Expr, String> {
    use polars::prelude::DataType;
    let x = e.to_polars_expr_with(schema)?;
    let float = matches!(e, Expr::ColumnRef(name)
        if matches!(schema.and_then(|s| s.get(name.as_str())), Some(DataType::Float32 | DataType::Float64)));
    Ok(match temporal_kind(e, schema) {
        Some(Temporal::Datetime) => x.dt().to_string("%Y-%m-%d %H:%M:%S%.f"),
        // A whole float without its `.0`, as the row-by-row path writes one.
        _ if float => polars::lazy::dsl::when(
            x.clone().eq(x.clone().floor()).and(
                x.clone()
                    .abs()
                    .lt(polars::lazy::dsl::lit(9_007_199_254_740_992.0)),
            ),
        )
        .then(x.clone().cast(DataType::Int64).cast(DataType::String))
        .otherwise(x.cast(DataType::String)),
        _ => x.cast(DataType::String),
    })
}

/// Whether a text function can be vectorised over `e`: text, or a date or datetime,
/// which [`as_text`] writes as the row-by-row path does.  A number is written
/// differently by the two engines, so a regex over one runs row by row.
fn reads_as_text(e: &Expr, schema: Option<&polars::prelude::Schema>) -> bool {
    is_text(e, schema) || temporal_kind(e, schema).is_some()
}

/// A whole-number literal at least zero, for `substring`'s start and length.
fn count_literal(e: &Expr) -> Option<u64> {
    match e {
        Expr::Literal(Value::Number(n)) if *n >= 0.0 && n.is_finite() => Some(*n as u64),
        _ => None,
    }
}

fn temporal_kind(e: &Expr, schema: Option<&polars::prelude::Schema>) -> Option<Temporal> {
    use polars::prelude::DataType;
    match e {
        Expr::ColumnRef(name) => match schema?.get(name.as_str())? {
            DataType::Date => Some(Temporal::Date),
            DataType::Datetime(_, _) => Some(Temporal::Datetime),
            _ => None,
        },
        Expr::FunctionCall { name, args } => match name.as_str() {
            "from_epoch" => Some(Temporal::Datetime),
            "date" if temporal_kind(args.first()?, schema).is_some() => Some(Temporal::Date),
            "time_bucket" => temporal_kind(args.first()?, schema),
            _ => None,
        },
        _ => None,
    }
}

/// `from_epoch`'s unit: `"s"`, `"ms"`, `"us"` or `"ns"`, in quotes.
fn epoch_unit_of(arg: &Expr) -> Result<crate::data::column::EpochUnit, String> {
    match arg {
        Expr::Literal(Value::String(u)) => {
            crate::data::column::EpochUnit::parse(u).ok_or_else(|| {
                format!(
                    "from_epoch(): unit is \"s\", \"ms\", \"us\" or \"ns\", got {:?}",
                    u
                )
            })
        }
        _ => Err(
            "from_epoch() needs its unit in quotes: \"s\", \"ms\", \"us\" or \"ns\"".to_string(),
        ),
    }
}

/// `time_bucket`'s width: a whole number of seconds, minutes, hours or days in quotes,
/// as `"10m"`, `"1h"`, `"1d"`, and no wider than a datetime spans.  Returned in the
/// form Polars reads.
fn bucket_of(arg: &Expr) -> Result<String, String> {
    match arg {
        Expr::Literal(Value::String(w)) if w.is_ascii() && bucket_nanos(w).is_some() => {
            Ok(w.clone())
        }
        _ => Err(
            "time_bucket() takes a width in quotes: a whole number of s, m, h or d, as \"10m\" or \"1h\", \
             no wider than about 290 years"
                .to_string(),
        ),
    }
}

/// The bucket width in nanoseconds; `None` for a width that is not one, or does not
/// fit a datetime.
fn bucket_nanos(w: &str) -> Option<i64> {
    let (n, unit) = w.split_at(w.len().checked_sub(1)?);
    if n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: i64 = n.parse().ok().filter(|v| *v > 0)?;
    n.checked_mul(match unit {
        "s" => 1_000_000_000,
        "m" => 60_000_000_000,
        "h" => 3_600_000_000_000,
        "d" => 86_400_000_000_000,
        _ => return None,
    })
}

/// `integral`: `x` is whole numbers already, scaled and cast in integers — a float
/// holds only 15–16 digits, and a nanosecond epoch has 19.
/// The type a Polars expression comes out as over a table of `schema` — run on one
/// with no rows, so nothing is read.  `None` when it cannot run on those types.
pub(crate) fn output_type(
    e: &polars::lazy::dsl::Expr,
    schema: &polars::prelude::Schema,
) -> Option<polars::prelude::DataType> {
    let out =
        polars::prelude::IntoLazy::lazy(polars::prelude::DataFrame::empty_with_schema(schema))
            .select([e.clone().alias("__out")])
            .collect()
            .ok()?;
    out.columns().first().map(|c| c.dtype().clone())
}

pub(crate) fn epoch_to_datetime(
    x: polars::lazy::dsl::Expr,
    unit: crate::data::column::EpochUnit,
    integral: bool,
) -> polars::lazy::dsl::Expr {
    use crate::data::column::EpochUnit;
    use polars::prelude::{DataType, RoundMode, TimeUnit};
    let whole = |e: polars::lazy::dsl::Expr| {
        if integral {
            e.cast(DataType::Int64)
        } else {
            e.cast(DataType::Float64)
                .round(0, RoundMode::HalfAwayFromZero)
                .cast(DataType::Int64)
        }
    };
    let x = if integral { x.cast(DataType::Int64) } else { x };
    // A count of ticks past what a datetime can hold is NULL — beyond it Polars cannot
    // show the value, and panics trying.
    let held = |ticks: polars::lazy::dsl::Expr, per_second: i64| {
        let most = chrono::DateTime::<chrono::Utc>::MAX_UTC.timestamp() * per_second;
        polars::lazy::dsl::when(ticks.clone().abs().lt_eq(polars::lazy::dsl::lit(most)))
            .then(ticks)
            .otherwise(polars::lazy::dsl::lit(polars::prelude::Null {}).cast(DataType::Int64))
    };
    match unit {
        EpochUnit::Seconds if integral => (held(x, 1) * polars::lazy::dsl::lit(1000i64))
            .cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
        EpochUnit::Seconds => held(whole(x * polars::lazy::dsl::lit(1000.0)), 1000)
            .cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
        EpochUnit::Millis => {
            held(whole(x), 1000).cast(DataType::Datetime(TimeUnit::Milliseconds, None))
        }
        EpochUnit::Micros => {
            held(whole(x), 1_000_000).cast(DataType::Datetime(TimeUnit::Microseconds, None))
        }
        EpochUnit::Nanos => whole(x).cast(DataType::Datetime(TimeUnit::Nanoseconds, None)),
    }
}

/// The pattern argument of a regex function: a text literal that compiles.
fn regex_literal(name: &str, arg: &Expr) -> Result<regex::Regex, String> {
    let Expr::Literal(Value::String(pattern)) = arg else {
        return Err(format!(
            "{}() needs the pattern as a text literal in quotes",
            name
        ));
    };
    regex::Regex::new(pattern).map_err(|e| format!("{}(): bad pattern: {}", name, e))
}

/// `regexp_replace`'s replacement with `\1` turned into `${1}`, the form the regex
/// engine (and Polars, which uses it) reads; `$1` and `${name}` pass through.
fn replacement_of(name: &str, arg: &Expr) -> Result<String, String> {
    let Expr::Literal(Value::String(rep)) = arg else {
        return Err(format!(
            "{}() needs the replacement as a text literal in quotes",
            name
        ));
    };
    static BACKREF: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let backref = BACKREF.get_or_init(|| regex::Regex::new(r"\\(\d+)").unwrap());
    Ok(backref.replace_all(rep, "$${$1}").into_owned())
}

/// `regexp_extract`'s group: a whole-number literal the pattern actually has.
fn group_of(args: &[Expr], re: &regex::Regex) -> Result<usize, String> {
    let group = match args.get(2) {
        None => 0,
        Some(Expr::Literal(Value::Number(n))) if n.fract() == 0.0 && *n >= 0.0 => *n as usize,
        Some(_) => {
            return Err(
                "regexp_extract()'s group is a whole number literal, 0 for the whole match"
                    .to_string(),
            )
        }
    };
    if group >= re.captures_len() {
        return Err(format!(
            "regexp_extract(): the pattern has {} group(s), so there is no group {}",
            re.captures_len() - 1,
            group
        ));
    }
    Ok(group)
}

/// `round()`'s places: a whole-number literal, as in `tuitab_calc`.
fn round_places(args: &[Expr]) -> Result<i32, String> {
    match args.get(1) {
        None => Ok(0),
        Some(Expr::Literal(Value::Number(n))) if n.fract() == 0.0 && n.abs() <= 28.0 => {
            Ok(*n as i32)
        }
        Some(_) => {
            Err("round()'s second argument is a whole number of places, -28 to 28".to_string())
        }
    }
}

/// Half away from zero, as a person rounds and as `tuitab_calc` does.
fn round_half_away(x: f64, places: i32) -> f64 {
    let scale = 10f64.powi(places);
    (x * scale).round() / scale
}

/// A result outside the function's domain — NaN, or an infinity — is NULL, not a number.
fn finite_or_null(e: polars::lazy::dsl::Expr) -> polars::lazy::dsl::Expr {
    use polars::lazy::dsl::{lit, when};
    when(e.clone().is_finite())
        .then(e)
        .otherwise(lit(polars::prelude::Null {}))
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
        expr.check_functions()?;
        Ok(expr)
    }

    /// Refuse a function the language does not have, or one called with the wrong
    /// number of arguments — before anything is evaluated.
    fn check_functions(&self) -> Result<(), String> {
        match self {
            Expr::FunctionCall { name, args } => {
                let Some(&(_, fewest, most)) = FUNCTIONS.iter().find(|(n, _, _)| n == name) else {
                    let names: Vec<&str> = FUNCTIONS.iter().map(|(n, _, _)| *n).collect();
                    return Err(format!(
                        "Unknown function '{}'. Available: {}",
                        name,
                        names.join(", ")
                    ));
                };
                if args.len() < fewest || args.len() > most {
                    let wanted = if fewest == most {
                        format!("{}", fewest)
                    } else if most == usize::MAX {
                        format!("at least {}", fewest)
                    } else {
                        format!("{} to {}", fewest, most)
                    };
                    return Err(format!(
                        "'{}' takes {} argument{}, got {}",
                        name,
                        wanted,
                        if fewest == 1 && most == 1 { "" } else { "s" },
                        args.len()
                    ));
                }
                match name.as_str() {
                    "regexp_replace" => {
                        regex_literal(name, &args[1])?;
                        replacement_of(name, &args[2])?;
                    }
                    "regexp_extract" => {
                        let re = regex_literal(name, &args[1])?;
                        group_of(args, &re)?;
                    }
                    "from_epoch" => {
                        epoch_unit_of(&args[1])?;
                    }
                    "contains" if matches!(args.get(1), Some(Expr::Literal(Value::String(_)))) => {
                        regex_literal(name, &args[1])?;
                    }
                    "time_bucket" => {
                        bucket_of(&args[1])?;
                    }
                    "json_get" => {
                        json_path_of(&args[1])?;
                    }
                    "round" => {
                        round_places(args)?;
                    }
                    // A pattern chrono cannot read is refused here rather than answered
                    // with NULLs on one path and an error on the other.
                    "date_format" => {
                        if let Some(Expr::Literal(Value::String(fmt))) = args.get(1) {
                            use chrono::format::{Item, StrftimeItems};
                            if StrftimeItems::new(fmt).any(|i| matches!(i, Item::Error)) {
                                return Err(format!(
                                    "date_format(): '{}' holds a directive that is not one; \
                                     see the strftime directives, as %Y-%m-%d %H:%M",
                                    fmt
                                ));
                            }
                        }
                    }
                    _ => {}
                }
                args.iter().try_for_each(|a| a.check_functions())
            }
            Expr::BinOp { left, right, .. } => {
                left.check_functions()?;
                right.check_functions()
            }
            Expr::If {
                cond,
                then_branch,
                else_branch,
            } => {
                cond.check_functions()?;
                then_branch.check_functions()?;
                else_branch.check_functions()
            }
            Expr::InList { left, list } => {
                left.check_functions()?;
                list.iter().try_for_each(|e| e.check_functions())
            }
            Expr::Not(inner) | Expr::IsNull(inner) => inner.check_functions(),
            Expr::ColumnRef(_) | Expr::Literal(_) => Ok(()),
        }
    }

    /// Refuse text that arithmetic can only turn into NULL.
    ///
    /// `"3" * 1` is a real conversion and stays; `"К выплате" * 1` is someone reaching
    /// for a column and getting a string literal, which Polars answers with a column of
    /// nulls and no error at all — the worst thing a query engine can do.  Only the
    /// arithmetic operators are checked: a string against `==` or `in` is the ordinary
    /// way to compare, whatever the columns happen to be called.
    /// Every column the expression reads, by name.
    pub fn columns(&self) -> Vec<&str> {
        let mut found = Vec::new();
        self.collect_columns(&mut found);
        found
    }

    fn collect_columns<'a>(&'a self, found: &mut Vec<&'a str>) {
        match self {
            Expr::ColumnRef(name) => found.push(name),
            Expr::Literal(_) => {}
            Expr::BinOp { left, right, .. } => {
                left.collect_columns(found);
                right.collect_columns(found);
            }
            Expr::FunctionCall { args, .. } => args.iter().for_each(|a| a.collect_columns(found)),
            Expr::If {
                cond,
                then_branch,
                else_branch,
            } => {
                cond.collect_columns(found);
                then_branch.collect_columns(found);
                else_branch.collect_columns(found);
            }
            Expr::InList { left, list } => {
                left.collect_columns(found);
                list.iter().for_each(|e| e.collect_columns(found));
            }
            Expr::Not(e) | Expr::IsNull(e) => e.collect_columns(found),
        }
    }

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

    /// The expression as a Polars one; `Err` when it is not supported there, which
    /// sends the caller to the row-by-row path.  The column types of the frame let the
    /// date functions lower too: `hour(ts)` on a datetime column is vectorised, while
    /// on text it still parses row by row.
    pub fn to_polars_expr_with(
        &self,
        schema: Option<&polars::prelude::Schema>,
    ) -> Result<polars::lazy::dsl::Expr, String> {
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
                // In microseconds: a nanosecond literal, Polars' own, cannot hold a moment
                // past 2262 or before 1677, and an SCD's `9999-12-31` is one.
                Value::Datetime(dt) => Ok(polars::lazy::dsl::lit(dt.and_utc().timestamp_micros())
                    .cast(polars::prelude::DataType::Datetime(
                        polars::prelude::TimeUnit::Microseconds,
                        None,
                    ))),
                Value::Null => Ok(polars::lazy::dsl::lit(polars::prelude::Null {})),
            },
            Expr::BinOp { op, left, right } => {
                let l = left.to_polars_expr_with(schema)?;
                let r = right.to_polars_expr_with(schema)?;
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
            Expr::Not(inner) => Ok(inner.to_polars_expr_with(schema)?.not()),
            Expr::IsNull(inner) => Ok(inner.to_polars_expr_with(schema)?.is_null()),
            // Dates and datetimes as equalities, each lowered against the column's
            // own unit: an IN list as a Series holds numbers and text.
            Expr::InList { left, list }
                if list
                    .iter()
                    .any(|e| matches!(e, Expr::Literal(Value::Date(_) | Value::Datetime(_)))) =>
            {
                list.iter()
                    .map(|item| {
                        Expr::BinOp {
                            op: Op::Eq,
                            left: left.clone(),
                            right: Box::new(item.clone()),
                        }
                        .to_polars_expr_with(schema)
                    })
                    .reduce(|a, b| Ok(a?.or(b?)))
                    .unwrap_or_else(|| Ok(polars::lazy::dsl::lit(false)))
            }
            Expr::InList { left, list } => {
                let l = left.to_polars_expr_with(schema)?;
                // Whole numbers as integers, as a literal on its own is: an integer
                // column compared as floats loses its neighbours past 2^53.
                let whole = list.iter().all(|e| match e {
                    Expr::Literal(Value::Number(n)) => {
                        n.fract() == 0.0 && n.abs() <= i64::MAX as f64
                    }
                    _ => true,
                });
                let mut polars_list = Vec::new();
                for item in list {
                    // Only support literals in Polars is_in for now
                    if let Expr::Literal(v) = item {
                        match v {
                            Value::Number(n) if whole => {
                                polars_list.push(polars::prelude::AnyValue::Int64(*n as i64))
                            }
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
                Ok(l.is_in(polars::lazy::dsl::lit(s).implode(), false))
            }
            Expr::If {
                cond,
                then_branch,
                else_branch,
            } => {
                let c = cond.to_polars_expr_with(schema)?;
                let t = then_branch.to_polars_expr_with(schema)?;
                let e = else_branch.to_polars_expr_with(schema)?;
                Ok(polars::lazy::dsl::when(c).then(t).otherwise(e))
            }
            Expr::FunctionCall { name, args } => match name.as_str() {
                "len" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr_with(schema)?
                    .str()
                    .len_chars()
                    .cast(polars::prelude::DataType::Int64)),
                "sum" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr_with(schema)?
                    .sum()
                    .cast(polars::prelude::DataType::Float64)),
                "count" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr_with(schema)?
                    .count()
                    .cast(polars::prelude::DataType::Float64)),
                "mean" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr_with(schema)?
                    .mean()
                    .cast(polars::prelude::DataType::Float64)),
                "max" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr_with(schema)?
                    .max()
                    .cast(polars::prelude::DataType::Float64)),
                "min" if args.len() == 1 => Ok(args[0]
                    .to_polars_expr_with(schema)?
                    .min()
                    .cast(polars::prelude::DataType::Float64)),
                // Casting to text so a value of any type can be compared with
                // a string — polars refuses `numeric == ""` outright.
                "text" if args.len() == 1 => as_text(&args[0], schema),
                // The pattern has to be a literal: a per-row regex would mean
                // recompiling it for every row, and no caller wants that.
                "contains" if args.len() == 2 => match &args[1] {
                    Expr::Literal(Value::String(pattern)) => Ok(as_text(&args[0], schema)?
                        .str()
                        .contains(polars::lazy::dsl::lit(pattern.clone()), true)),
                    _ => {
                        Err("contains() needs a literal pattern as its second argument".to_string())
                    }
                },
                "abs" | "round" | "floor" | "ceil" | "sqrt" | "exp" | "ln" | "log10" | "log" => {
                    let x = args[0]
                        .to_polars_expr_with(schema)?
                        .cast(polars::prelude::DataType::Float64);
                    let e = std::f64::consts::E;
                    Ok(finite_or_null(match name.as_str() {
                        "abs" => x.abs(),
                        "round" => {
                            let places = round_places(args)?;
                            let mode = polars::prelude::RoundMode::HalfAwayFromZero;
                            if places >= 0 {
                                // Polars' own kernel: no division the optimiser could turn
                                // into a multiplication by 0.1 in one engine and not another.
                                x.round(places as u32, mode)
                            } else {
                                let scale = polars::lazy::dsl::lit(10f64.powi(places));
                                (x * scale.clone()).round(0, mode) / scale
                            }
                        }
                        "floor" => x.floor(),
                        "ceil" => x.ceil(),
                        "sqrt" => x.sqrt(),
                        "exp" => x.exp(),
                        "ln" => x.log(polars::lazy::dsl::lit(e)),
                        "log10" => x.log(polars::lazy::dsl::lit(10.0)),
                        _ => {
                            let base = args[1]
                                .to_polars_expr_with(schema)?
                                .cast(polars::prelude::DataType::Float64);
                            x.log(polars::lazy::dsl::lit(e)) / base.log(polars::lazy::dsl::lit(e))
                        }
                    }))
                }
                "regexp_replace" if reads_as_text(&args[0], schema) => {
                    let pattern = regex_literal(name, &args[1])?;
                    let rep = replacement_of(name, &args[2])?;
                    Ok(as_text(&args[0], schema)?.str().replace_all(
                        polars::lazy::dsl::lit(pattern.as_str().to_string()),
                        polars::lazy::dsl::lit(rep),
                        false,
                    ))
                }
                "regexp_extract" if reads_as_text(&args[0], schema) => {
                    let pattern = regex_literal(name, &args[1])?;
                    let group = group_of(args, &pattern)?;
                    Ok(as_text(&args[0], schema)?
                        .str()
                        .extract(polars::lazy::dsl::lit(pattern.as_str().to_string()), group))
                }
                "substring" if is_text(&args[0], schema) => {
                    match (count_literal(&args[1]), count_literal(&args[2])) {
                        (Some(start), Some(len)) => {
                            Ok(args[0].to_polars_expr_with(schema)?.str().slice(
                                polars::lazy::dsl::lit(start as i64),
                                polars::lazy::dsl::lit(len),
                            ))
                        }
                        _ => Err(
                            "substring() with a computed start or length runs row by row".into(),
                        ),
                    }
                }
                "concat" if args.iter().all(|a| is_text(a, schema)) => {
                    let parts = args
                        .iter()
                        .map(|a| {
                            a.to_polars_expr_with(schema)
                                .map(|e| e.fill_null(polars::lazy::dsl::lit("")))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(polars::lazy::dsl::concat_str(parts, "", false))
                }
                "split" if is_text(&args[0], schema) => match &args[1] {
                    Expr::Literal(Value::String(delim)) => Ok(args[0]
                        .to_polars_expr_with(schema)?
                        .str()
                        .split(polars::lazy::dsl::lit(delim.clone()))
                        .list()
                        .first()),
                    _ => Err("split() with a computed delimiter runs row by row".into()),
                },
                "date" if temporal_kind(&args[0], schema).is_some() => {
                    Ok(args[0].to_polars_expr_with(schema)?.dt().date())
                }
                "json_get" if is_text(&args[0], schema) => {
                    let (path, _) = json_path_of(&args[1])?;
                    Ok(args[0]
                        .to_polars_expr_with(schema)?
                        .str()
                        .json_path_match(polars::lazy::dsl::lit(path)))
                }
                "from_epoch" => {
                    let unit = epoch_unit_of(&args[1])?;
                    let x = args[0].to_polars_expr_with(schema)?;
                    // Whole numbers stay in integers: what the argument is, asked of
                    // a frame with the columns and no rows.
                    let integral = schema
                        .and_then(|s| output_type(&x, s))
                        .is_some_and(|t| t.is_integer());
                    Ok(epoch_to_datetime(x, unit, integral))
                }
                "time_bucket" if temporal_kind(&args[0], schema).is_some() => {
                    let every = bucket_of(&args[1])?;
                    let dt = args[0].to_polars_expr_with(schema)?;
                    Ok(dt.dt().truncate(polars::lazy::dsl::lit(every)))
                }
                "year" | "month" | "day" | "hour" | "minute" | "date_format"
                    if temporal_kind(&args[0], schema).is_some() =>
                {
                    let kind = temporal_kind(&args[0], schema).unwrap_or(Temporal::Date);
                    let d = args[0].to_polars_expr_with(schema)?;
                    let int = polars::prelude::DataType::Int64;
                    match name.as_str() {
                        "year" => Ok(d.dt().year().cast(int)),
                        "month" => Ok(d.dt().month().cast(int)),
                        "day" => Ok(d.dt().day().cast(int)),
                        // A date has no time of day; the row path answers NULL for it.
                        "hour" | "minute" if kind == Temporal::Date => {
                            Err(format!("{}() of a date is NULL row by row", name))
                        }
                        "hour" => Ok(d.dt().hour().cast(int)),
                        "minute" => Ok(d.dt().minute().cast(int)),
                        _ => match &args[1] {
                            Expr::Literal(Value::String(fmt)) => Ok(d.dt().strftime(fmt)),
                            _ => Err("date_format() needs its pattern in quotes".to_string()),
                        },
                    }
                }
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
                    // SQL's three-valued logic, as Polars has it: false decides an
                    // `and` and true an `or` whatever the other side is.
                    Op::And => {
                        return match (l.as_bool(), r.as_bool()) {
                            (Some(false), _) | (_, Some(false)) => Value::Boolean(false),
                            (Some(true), Some(true)) => Value::Boolean(true),
                            _ => Value::Null,
                        }
                    }
                    Op::Or => {
                        return match (l.as_bool(), r.as_bool()) {
                            (Some(true), _) | (_, Some(true)) => Value::Boolean(true),
                            (Some(false), Some(false)) => Value::Boolean(false),
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
                // An unknown value is in no list, nor out of one.
                if matches!(l, Value::Null) {
                    return Value::Null;
                }
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
                        match cached_regex(&evaluated_args[1].to_string()) {
                            Some(re) => Value::Boolean(re.is_match(&evaluated_args[0].to_string())),
                            // A malformed pattern is Null, not false — a bad
                            // regex is not the same answer as "no match".
                            None => Value::Null,
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
                    "abs" | "round" | "floor" | "ceil" | "sqrt" | "exp" | "ln" | "log10"
                    | "log" => {
                        let Some(x) = evaluated_args[0].as_f64() else {
                            return Value::Null;
                        };
                        let out = match name.as_str() {
                            "abs" => x.abs(),
                            "round" => match round_places(args) {
                                Ok(places) => round_half_away(x, places),
                                Err(_) => return Value::Null,
                            },
                            "floor" => x.floor(),
                            "ceil" => x.ceil(),
                            "sqrt" => x.sqrt(),
                            "exp" => x.exp(),
                            "ln" => x.ln(),
                            "log10" => x.log10(),
                            _ => match evaluated_args[1].as_f64() {
                                Some(base) => x.ln() / base.ln(),
                                None => return Value::Null,
                            },
                        };
                        if out.is_finite() {
                            Value::Number(out)
                        } else {
                            Value::Null
                        }
                    }
                    "regexp_replace" | "regexp_extract" => {
                        let Some(text) = text_of(&evaluated_args[0]) else {
                            return Value::Null;
                        };
                        // Parsing already checked the literal compiles; this reuses the
                        // compiled pattern rather than building it again for every row.
                        let Expr::Literal(Value::String(pattern)) = &args[1] else {
                            return Value::Null;
                        };
                        let Some(re) = cached_regex(pattern) else {
                            return Value::Null;
                        };
                        if name == "regexp_replace" {
                            match replacement_of(name, &args[2]) {
                                Ok(rep) => {
                                    Value::String(re.replace_all(&text, rep.as_str()).into_owned())
                                }
                                Err(_) => Value::Null,
                            }
                        } else {
                            let Ok(group) = group_of(args, &re) else {
                                return Value::Null;
                            };
                            re.captures(&text)
                                .and_then(|c| c.get(group))
                                .map_or(Value::Null, |m| Value::String(m.as_str().to_string()))
                        }
                    }
                    "json_get" => {
                        let (Some(cell), Ok((_, steps))) =
                            (text_of(&evaluated_args[0]), json_path_of(&args[1]))
                        else {
                            return Value::Null;
                        };
                        crate::data::json_cols::get_text(&cell, &steps)
                            .map_or(Value::Null, Value::String)
                    }
                    "from_epoch" => {
                        let (Some(x), Ok(unit)) =
                            (evaluated_args[0].as_f64(), epoch_unit_of(&args[1]))
                        else {
                            return Value::Null;
                        };
                        // As Polars does it: round to the unit (to milliseconds for
                        // seconds), then scale in integers, so 13-digit milliseconds do
                        // not lose their last digits to a float.
                        use crate::data::column::EpochUnit;
                        let (count, scale) = match unit {
                            EpochUnit::Seconds => ((x * 1000.0).round(), 1_000_000i64),
                            other => (x.round(), other.nanos()),
                        };
                        if !count.is_finite() || count.abs() >= i64::MAX as f64 {
                            return Value::Null;
                        }
                        // In i128 nanoseconds: an i64 of them ends in 2262.
                        from_nanos(count as i128 * scale as i128)
                    }
                    "time_bucket" => {
                        let Some(step) = bucket_of(&args[1]).ok().and_then(|w| bucket_nanos(&w))
                        else {
                            return Value::Null;
                        };
                        const DAY: i64 = 86_400_000_000_000;
                        match &evaluated_args[0] {
                            Value::Datetime(dt) => {
                                let at = dt.and_utc();
                                let ns = at.timestamp() as i128 * 1_000_000_000
                                    + at.timestamp_subsec_nanos() as i128;
                                from_nanos(ns - ns.rem_euclid(step as i128))
                            }
                            // A date is a whole day: a width of whole days buckets it, a
                            // narrower one leaves it as it is — as Polars does.
                            Value::Date(d) if step % DAY == 0 => {
                                let days = step / DAY;
                                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                let n = (*d - epoch).num_days();
                                Value::Date(epoch + chrono::Duration::days(n - n.rem_euclid(days)))
                            }
                            Value::Date(d) => Value::Date(*d),
                            _ => Value::Null,
                        }
                    }
                    // UTC, as every datetime column is held and shown.
                    "today" => Value::Date(chrono::Utc::now().naive_utc().date()),
                    "now" => Value::Datetime(chrono::Utc::now().naive_utc()),
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
        (Value::Date(_), Value::Datetime(_)) | (Value::Datetime(_), Value::Date(_)) => {
            at_midnight(l) == at_midnight(r)
        }
        _ => l == r,
    }
}

/// The moment `ns` nanoseconds from the epoch, or NULL past what a datetime holds.
fn from_nanos(ns: i128) -> Value {
    let secs = ns.div_euclid(1_000_000_000);
    let sub = ns.rem_euclid(1_000_000_000) as u32;
    i64::try_from(secs)
        .ok()
        .and_then(|s| chrono::DateTime::from_timestamp(s, sub))
        .map_or(Value::Null, |dt| Value::Datetime(dt.naive_utc()))
}

/// A date as the moment it starts, so it compares with a datetime as Polars compares
/// them; anything else as it is.
fn at_midnight(v: &Value) -> Value {
    match v {
        Value::Date(d) => Value::Datetime(d.and_hms_opt(0, 0, 0).expect("midnight exists")),
        other => other.clone(),
    }
}

type Compiled = Option<std::sync::Arc<regex::Regex>>;
static REGEXES: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<String, Compiled>>> =
    std::sync::OnceLock::new();
/// Patterns kept: one read from a column is a pattern per distinct cell.
const REGEXES_KEPT: usize = 256;

/// A pattern compiled once, however many rows ask for it.  The row-by-row path used to
/// compile the same regex again on every row.  `None` for a pattern that does not
/// compile.  Shared, not cloned: a cloned `Regex` starts its match caches over.
fn cached_regex(pattern: &str) -> Option<std::sync::Arc<regex::Regex>> {
    use std::collections::HashMap;
    use std::sync::Arc;
    let mut cache = REGEXES
        .get_or_init(|| std::sync::Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(hit) = cache.get(pattern) {
        return hit.clone();
    }
    // ponytail: starts over when full rather than evicting the oldest.
    if cache.len() >= REGEXES_KEPT {
        cache.clear();
    }
    let compiled = regex::Regex::new(pattern).ok().map(Arc::new);
    cache.insert(pattern.to_string(), compiled.clone());
    compiled
}

/// A date (`YYYY-MM-DD`) or a datetime in one of the forms a cell takes.
pub(crate) fn parse_temporal(text: &str) -> Option<Value> {
    if let Ok(d) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Some(Value::Date(d));
    }
    for fmt in ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%dT%H:%M:%S%.f%#z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(text, fmt) {
            // The instant, in UTC: the columns it is compared with are held that way.
            return Some(Value::Datetime(dt.naive_utc()));
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
        // saves the AST a variant no evaluator would treat differently.  A negative
        // number is a literal of its own: `round(x, -2)` needs to see one.
        if let Some(Token::Minus) = self.peek() {
            self.advance();
            let operand = self.parse_factor()?;
            if let Expr::Literal(Value::Number(n)) = operand {
                return Ok(Expr::Literal(Value::Number(-n)));
            }
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
        (Value::Date(_), Value::Datetime(_)) | (Value::Datetime(_), Value::Date(_)) => {
            (at_midnight(l), at_midnight(r))
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

    /// `contains(text, pattern_col)` compiles a pattern per distinct cell; a server
    /// running for days keeps a bounded number of them.
    #[test]
    fn compiled_patterns_are_bounded() {
        for i in 0..REGEXES_KEPT * 3 {
            cached_regex(&format!("cell-{}", i));
        }
        let kept = REGEXES.get().unwrap().lock().unwrap().len();
        assert!(kept <= REGEXES_KEPT, "{} patterns kept", kept);
    }

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
        let polars_expr = parsed
            .to_polars_expr_with(None)
            .expect("must lower to Polars");
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
    fn date_format_with_a_bad_pattern_is_refused() {
        assert!(Expr::parse("date_format(when, '%Q')").is_err());
    }

    /// A misspelt function used to evaluate to a column of NULLs with no error at all.
    #[test]
    fn an_unknown_function_is_named_with_the_available_ones() {
        let err = Expr::parse("regex_replace(a, \"x\", \"y\")").unwrap_err();
        assert!(err.contains("Unknown function 'regex_replace'"), "{}", err);
        assert!(err.contains("contains"), "{}", err);
    }

    /// `contains()` with no arguments panicked in the row-by-row evaluator.
    #[test]
    fn a_function_with_the_wrong_number_of_arguments_is_refused() {
        for e in [
            "contains()",
            "contains(a)",
            "substring(a)",
            "len(a, b)",
            "today(1)",
        ] {
            let err = Expr::parse(e).unwrap_err();
            assert!(err.contains("takes"), "{}: {}", e, err);
        }
        for e in ["concat(a)", "concat(a, b, c, d)", "today()", "if(a, 1, 2)"] {
            assert!(Expr::parse(e).is_ok(), "{}", e);
        }
    }

    /// A function name inside a nested expression is checked too.
    #[test]
    fn an_unknown_function_deep_inside_is_found() {
        assert!(Expr::parse("1 + if(a > 1, lenn(b), 0)").is_err());
        assert!(Expr::parse("x in (1, 2) and not frob(y)").is_err());
    }

    /// One number column `v`, and a way to evaluate an expression over it on both paths:
    /// Polars (the one 35M rows go through) and the row-by-row interpreter.
    fn numbers(
        values: &[f64],
    ) -> (
        crate::data::dataframe::DataFrame,
        HashMap<&'static str, usize>,
    ) {
        let df =
            polars::prelude::DataFrame::new_infer_height(vec![
                Series::new("v".into(), values).into()
            ])
            .unwrap();
        let mut tui_df = crate::data::dataframe::DataFrame::empty();
        tui_df.df = df;
        (tui_df, [("v", 0)].into_iter().collect())
    }

    fn on_both_paths(expr: &str, values: &[f64]) -> (Vec<Option<f64>>, Vec<Option<f64>>) {
        use polars::prelude::IntoLazy;
        let (df, lookup) = numbers(values);
        let parsed = super::Expr::parse(expr).expect("expression must parse");
        let fast = df
            .df
            .clone()
            .lazy()
            .select([parsed
                // With the frame's schema, as every real caller lowers: the forms
                // that depend on a column's type are the ones to hold to the row path.
                .to_polars_expr_with(Some(df.df.schema().as_ref()))
                .expect("must lower to Polars")
                .cast(polars::prelude::DataType::Float64)
                .alias("m")])
            .collect()
            .expect("must evaluate");
        let fast: Vec<Option<f64>> = fast
            .column("m")
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .collect();
        let slow: Vec<Option<f64>> = (0..values.len())
            .map(|row| match parsed.eval(row, &lookup, &df) {
                Value::Number(n) => Some(n),
                Value::Null => None,
                other => panic!("{} gave {:?}", expr, other),
            })
            .collect();
        (fast, slow)
    }

    /// A frame of the given columns, and its name lookup, for the parity checks.
    fn frame_of(
        columns: Vec<Series>,
    ) -> (
        crate::data::dataframe::DataFrame,
        HashMap<&'static str, usize>,
    ) {
        let names: Vec<&'static str> = columns
            .iter()
            .map(|c| &*Box::leak(c.name().to_string().into_boxed_str()))
            .collect();
        let pdf = polars::prelude::DataFrame::new_infer_height(
            columns.into_iter().map(|c| c.into()).collect(),
        )
        .unwrap();
        let df = crate::data::io::wrap_polars_df(pdf).unwrap();
        (
            df,
            names.into_iter().enumerate().map(|(i, n)| (n, i)).collect(),
        )
    }

    /// NULL in `and`, `or` and `in` follows SQL's three-valued logic on both paths: a
    /// filter keeps the same rows whichever engine answers it.
    #[test]
    fn null_logic_agrees_on_both_paths() {
        let (df, lookup) = frame_of(vec![
            Series::new("x".into(), [None, Some(5.0)]),
            Series::new("y".into(), [0.0, 1.0]),
        ]);
        for expr in [
            "x > 1 or y == 0",
            "x > 1 and y == 1",
            "not (x > 1 and y == 1)",
            "x > 1 and y == 5",
            "not (x in (5, 7))",
            "x in (5, 7)",
        ] {
            let (fast, slow) = shown_on_both_paths(expr, &df, &lookup);
            assert_eq!(fast, slow, "{}", expr);
        }
    }

    /// An IN list of whole numbers compares an integer column as integers, as `==` does.
    #[test]
    fn an_in_list_of_whole_numbers_compares_as_integers() {
        let (df, lookup) = frame_of(vec![Series::new(
            "id".into(),
            [9_007_199_254_740_992i64, 9_007_199_254_740_993],
        )]);
        let (in_list, _) = shown_on_both_paths("id in (9007199254740992)", &df, &lookup);
        let (eq, _) = shown_on_both_paths("id == 9007199254740992", &df, &lookup);
        assert_eq!(in_list, eq);
        assert_eq!(in_list, ["true", "false"]);
    }

    /// A datetime compared with a date — `ts < today()` — is an answer on the row path.
    #[test]
    fn a_datetime_compares_with_a_date_row_by_row() {
        let ts = Series::new("ts".into(), [1_700_000_000_000i64, 1_800_000_000_000])
            .cast(&polars::prelude::DataType::Datetime(
                polars::prelude::TimeUnit::Milliseconds,
                None,
            ))
            .unwrap();
        let (df, lookup) = frame_of(vec![ts]);
        let cutoff = Expr::parse("ts < date(\"2025-01-01\")").unwrap();
        let rows: Vec<Value> = (0..2).map(|r| cutoff.eval(r, &lookup, &df)).collect();
        assert_eq!(rows, [Value::Boolean(true), Value::Boolean(false)]);
        let same = Expr::parse("ts == date(\"2023-11-14\")").unwrap();
        assert_eq!(same.eval(0, &lookup, &df), Value::Boolean(false));
    }

    /// `from_epoch` past 2262 and `time_bucket` over a date give the same answer row by
    /// row as vectorised.
    #[test]
    fn epochs_and_buckets_agree_outside_the_nanosecond_range() {
        let (df, lookup) = frame_of(vec![Series::new(
            "e".into(),
            [9_999_999_999i64, 1_700_000_000],
        )]);
        for expr in [
            "from_epoch(e, \"s\")",
            "time_bucket(from_epoch(e, \"s\"), \"1h\")",
        ] {
            let (fast, slow) = shown_on_both_paths(expr, &df, &lookup);
            assert_eq!(fast, slow, "{}", expr);
        }
        let d = Series::new("d".into(), [19_000i32])
            .cast(&polars::prelude::DataType::Date)
            .unwrap();
        let (df, lookup) = frame_of(vec![d]);
        let (fast, slow) = shown_on_both_paths("time_bucket(d, \"1h\")", &df, &lookup);
        assert_eq!(fast, slow);
    }

    /// A number or a datetime read as text reads alike on both paths: a regex over a
    /// number runs row by row, over a datetime vectorised on the text the table shows,
    /// and `text()` of a whole float is `2`, as row by row.
    #[test]
    fn text_of_numbers_and_datetimes_agrees_on_both_paths() {
        let t = Series::new("t".into(), [1_700_000_000_000i64, 1_700_000_000_500])
            .cast(&polars::prelude::DataType::Datetime(
                polars::prelude::TimeUnit::Milliseconds,
                None,
            ))
            .unwrap();
        let (df, lookup) = frame_of(vec![Series::new("price".into(), [2.0, 2.5]), t]);
        for expr in [
            "text(price)",
            "contains(price, \"[.]0\")",
            "regexp_extract(t, \"[.]([0-9]+)\", 1)",
            "regexp_replace(t, \"[0-9]\", \"x\")",
        ] {
            let (fast, slow) = shown_on_both_paths(expr, &df, &lookup);
            assert_eq!(fast, slow, "{}", expr);
        }
        for expr in [
            "regexp_extract(price, \"[.]([0-9]+)\", 1)",
            "regexp_replace(price, \"0\", \"x\")",
            "json_get(price, \"$.a\")",
        ] {
            let lowered = Expr::parse(expr)
                .unwrap()
                .to_polars_expr_with(Some(df.df.schema().as_ref()));
            assert!(lowered.is_err(), "{} lowered over a number", expr);
        }
    }

    /// An epoch past what a datetime can hold is NULL on both paths, not a panic.
    #[test]
    fn an_epoch_past_any_datetime_is_null() {
        let (df, lookup) = frame_of(vec![Series::new("e".into(), [9_000_000_000_000_000i64, 1])]);
        for expr in [
            "from_epoch(e, \"s\")",
            "from_epoch(e, \"ms\")",
            "from_epoch(e * 1000, \"us\")",
            "from_epoch(e / 3, \"s\")",
        ] {
            let (fast, slow) = shown_on_both_paths(expr, &df, &lookup);
            assert_eq!(fast, slow, "{}", expr);
            assert_eq!(fast[0], "", "{}", expr);
        }
    }

    /// A moment written with an offset is that instant in UTC, as every datetime
    /// column is held.
    #[test]
    fn an_offset_literal_is_read_as_utc() {
        let want = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(7, 0, 0)
            .unwrap();
        for text in ["2024-01-01T10:00:00+03:00", "2024-01-01 10:00:00+03:00"] {
            assert_eq!(
                parse_temporal(text),
                Some(Value::Datetime(want)),
                "{}",
                text
            );
        }
    }

    /// Places that are not a whole number from -28 to 28 are refused when the
    /// expression is read, not answered with a column of NULLs.
    #[test]
    fn round_refuses_places_it_cannot_honour() {
        for bad in ["round(v, 1.5)", "round(v, 40)", "round(v, v)"] {
            assert!(Expr::parse(bad).is_err(), "{} parsed", bad);
        }
        assert!(Expr::parse("round(v, -2)").is_ok());
    }

    #[test]
    fn math_functions_agree_on_both_paths() {
        let cases: &[(&str, &[f64], &[f64])] = &[
            ("abs(v)", &[-2.5, 3.0], &[2.5, 3.0]),
            ("round(v, 2)", &[2.345, -2.345], &[2.35, -2.35]),
            // Half away from zero, as tuitab_calc rounds.
            ("round(v)", &[2.5, -2.5, 1.4], &[3.0, -3.0, 1.0]),
            ("round(v, -2)", &[1234.5678, -150.0], &[1200.0, -200.0]),
            ("floor(v)", &[-1.5, 1.5], &[-2.0, 1.0]),
            ("ceil(v)", &[1.2, -1.2], &[2.0, -1.0]),
            ("sqrt(v)", &[9.0], &[3.0]),
            ("exp(v)", &[0.0], &[1.0]),
            ("ln(v)", &[1.0], &[0.0]),
            ("log10(v)", &[100.0], &[2.0]),
            ("log(v, 2)", &[8.0], &[3.0]),
            ("round(abs(v - 10) / 4, 2)", &[3.0], &[1.75]),
        ];
        for (expr, input, want) in cases {
            let (fast, slow) = on_both_paths(expr, input);
            for (i, w) in want.iter().enumerate() {
                for (path, got) in [("polars", fast[i]), ("rows", slow[i])] {
                    let got = got
                        .unwrap_or_else(|| panic!("{} on {} gave NULL ({})", expr, input[i], path));
                    assert!(
                        (got - w).abs() < 1e-9,
                        "{} on {}: {} gave {}, want {}",
                        expr,
                        input[i],
                        path,
                        got,
                        w
                    );
                }
            }
        }
    }

    #[test]
    fn math_outside_its_domain_is_null() {
        for expr in ["sqrt(v)", "ln(v)", "log10(v)", "log(v, 2)"] {
            let (fast, slow) = on_both_paths(expr, &[-1.0, 0.0]);
            assert_eq!(fast[0], None, "{} polars", expr);
            assert_eq!(slow[0], None, "{} rows", expr);
        }
        for expr in ["ln(v)", "log10(v)"] {
            let (fast, slow) = on_both_paths(expr, &[0.0]);
            assert_eq!((fast[0], slow[0]), (None, None), "{}", expr);
        }
    }

    /// A text column `s`, evaluated on both paths and read back as text.
    fn texts_on_both_paths(
        expr: &str,
        values: &[Option<&str>],
    ) -> (Vec<Option<String>>, Vec<Option<String>>) {
        use polars::prelude::IntoLazy;
        let df =
            polars::prelude::DataFrame::new_infer_height(vec![
                Series::new("s".into(), values).into()
            ])
            .unwrap();
        let mut tui_df = crate::data::dataframe::DataFrame::empty();
        tui_df.df = df;
        let lookup: HashMap<&str, usize> = [("s", 0)].into_iter().collect();
        let parsed = super::Expr::parse(expr).expect("expression must parse");
        let fast = tui_df
            .df
            .clone()
            .lazy()
            .select([parsed
                .to_polars_expr_with(Some(tui_df.df.schema().as_ref()))
                .expect("must lower to Polars")
                .alias("m")])
            .collect()
            .expect("must evaluate");
        let fast: Vec<Option<String>> = fast
            .column("m")
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .map(|v| v.map(str::to_string))
            .collect();
        let slow: Vec<Option<String>> = (0..values.len())
            .map(|row| match parsed.eval(row, &lookup, &tui_df) {
                Value::String(t) => Some(t),
                Value::Null => None,
                other => panic!("{} gave {:?}", expr, other),
            })
            .collect();
        (fast, slow)
    }

    fn agree(expr: &str, values: &[Option<&str>], want: &[Option<&str>]) {
        let (fast, slow) = texts_on_both_paths(expr, values);
        let want: Vec<Option<String>> = want.iter().map(|w| w.map(str::to_string)).collect();
        assert_eq!(fast, want, "{} on polars", expr);
        assert_eq!(slow, want, "{} row by row", expr);
    }

    #[test]
    fn regexp_replace_replaces_every_match() {
        agree(
            "regexp_replace(s, \"[0-9]+\", \"N\")",
            &[Some("a1b22"), Some("none"), None],
            &[Some("aNbN"), Some("none"), None],
        );
        agree(
            "regexp_replace(s, \"(?i)x\", \"-\")",
            &[Some("xXx")],
            &[Some("---")],
        );
    }

    #[test]
    fn regexp_replace_backrefs() {
        agree(
            "regexp_replace(s, \"(a)(b)\", \"$2$1\")",
            &[Some("ab")],
            &[Some("ba")],
        );
        agree(
            "regexp_replace(s, \"(a)(b)\", \"\\2\\1\")",
            &[Some("ab")],
            &[Some("ba")],
        );
        agree(
            "regexp_replace(s, \"(?P<w>[a-z]+)\", \"<${w}>\")",
            &[Some("hi 1")],
            &[Some("<hi> 1")],
        );
    }

    #[test]
    fn regexp_extract_takes_a_group_and_null_when_absent() {
        agree(
            "regexp_extract(s, \"x=([0-9]+)\", 1)",
            &[Some("a x=42 b"), Some("nothing"), None],
            &[Some("42"), None, None],
        );
        agree(
            "regexp_extract(s, \"x=[0-9]+\")",
            &[Some("a x=42 b")],
            &[Some("x=42")],
        );
        agree(
            "regexp_extract(s, \"(?i)https?://([A-Za-z0-9.-]+)\", 1)",
            &[Some("see HTTPS://Geo.Example/x?y=1")],
            &[Some("Geo.Example")],
        );
    }

    /// A pattern that cannot work is refused when the expression is read: lowering to
    /// Polars failing would otherwise fall back to the row-by-row path, which answers
    /// NULL for every row.
    #[test]
    fn a_non_literal_or_broken_pattern_is_an_error() {
        for e in [
            "regexp_replace(s, s, \"x\")",
            "regexp_replace(s, \"x\", s)",
            "regexp_replace(s, \"(\", \"x\")",
            "regexp_extract(s, \"[\", 0)",
            "regexp_extract(s, s, 0)",
            "regexp_extract(s, \"(a)\", 2)",
            "regexp_extract(s, \"(a)\", -1)",
            "regexp_extract(s, \"(a)\", 0.5)",
        ] {
            assert!(super::Expr::parse(e).is_err(), "{} should be refused", e);
        }
    }

    /// Evaluate on both paths and read every result back as the text a cell shows.
    fn shown_on_both_paths(
        expr: &str,
        df: &crate::data::dataframe::DataFrame,
        lookup: &HashMap<&str, usize>,
    ) -> (Vec<String>, Vec<String>) {
        use polars::prelude::IntoLazy;
        let parsed = super::Expr::parse(expr).expect("expression must parse");
        let lowered = parsed
            .to_polars_expr_with(Some(df.df.schema().as_ref()))
            .unwrap_or_else(|e| panic!("{} must lower to Polars: {}", expr, e));
        let fast = df
            .df
            .clone()
            .lazy()
            .select([lowered.alias("m")])
            .collect()
            .expect("must evaluate");
        let col = fast.column("m").unwrap().as_materialized_series().clone();
        let fast: Vec<String> = (0..col.len())
            .map(|i| match col.get(i).unwrap() {
                d @ polars::prelude::AnyValue::Datetime(..) => {
                    crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(&d)
                }
                polars::prelude::AnyValue::Null => String::new(),
                polars::prelude::AnyValue::String(t) => t.to_string(),
                polars::prelude::AnyValue::StringOwned(t) => t.to_string(),
                other => other.to_string(),
            })
            .collect();
        let slow: Vec<String> = (0..df.df.height())
            .map(|row| match parsed.eval(row, lookup, df) {
                Value::Null => String::new(),
                Value::Number(n) if n.fract() == 0.0 => format!("{}", n as i64),
                other => other.to_string(),
            })
            .collect();
        (fast, slow)
    }

    fn epochs(
        ms: &[i64],
    ) -> (
        crate::data::dataframe::DataFrame,
        HashMap<&'static str, usize>,
    ) {
        let df =
            polars::prelude::DataFrame::new_infer_height(vec![Series::new("t".into(), ms).into()])
                .unwrap();
        let mut tui_df = crate::data::dataframe::DataFrame::empty();
        tui_df.df = df;
        (tui_df, [("t", 0)].into_iter().collect())
    }

    #[test]
    fn from_epoch_and_time_bucket() {
        let (df, lookup) = epochs(&[1787569800000, 1787571412345]);
        let cases: &[(&str, [&str; 2])] = &[
            (
                "from_epoch(t, \"ms\")",
                ["2026-08-24 11:10:00", "2026-08-24 11:36:52.345"],
            ),
            (
                "from_epoch(t / 1000, \"s\")",
                ["2026-08-24 11:10:00", "2026-08-24 11:36:52.345"],
            ),
            (
                "time_bucket(from_epoch(t, \"ms\"), \"1h\")",
                ["2026-08-24 11:00:00", "2026-08-24 11:00:00"],
            ),
            (
                "time_bucket(from_epoch(t, \"ms\"), \"10m\")",
                ["2026-08-24 11:10:00", "2026-08-24 11:30:00"],
            ),
            (
                "time_bucket(from_epoch(t, \"ms\"), \"1d\")",
                ["2026-08-24 00:00:00", "2026-08-24 00:00:00"],
            ),
            ("hour(from_epoch(t, \"ms\"))", ["11", "11"]),
            ("minute(from_epoch(t, \"ms\"))", ["10", "36"]),
            ("year(from_epoch(t, \"ms\"))", ["2026", "2026"]),
            (
                "date_format(from_epoch(t, \"ms\"), \"%H:%M\")",
                ["11:10", "11:36"],
            ),
        ];
        for (expr, want) in cases {
            let (fast, slow) = shown_on_both_paths(expr, &df, &lookup);
            assert_eq!(fast, want.to_vec(), "{} on polars", expr);
            assert_eq!(slow, want.to_vec(), "{} row by row", expr);
        }
    }

    #[test]
    fn time_functions_refuse_what_they_cannot_read() {
        for e in [
            "from_epoch(t, \"minutes\")",
            "from_epoch(t, u)",
            "time_bucket(t, \"1mo\")",
            "time_bucket(t, \"often\")",
            "time_bucket(t, u)",
        ] {
            assert!(super::Expr::parse(e).is_err(), "{} should be refused", e);
        }
    }

    /// The text functions run vectorised on text — a log template over 35 million
    /// rows took twelve minutes row by row.
    #[test]
    fn text_functions_lower_to_polars_and_agree() {
        let schema =
            polars::prelude::Schema::from_iter([("s".into(), polars::prelude::DataType::String)]);
        for e in [
            "substring(s, 0, 3)",
            "substring(regexp_replace(s, \"[0-9]+\", \"N\"), 2, 5)",
            "concat(s, \"-\", s)",
            "split(s, \" \")",
        ] {
            assert!(
                super::Expr::parse(e)
                    .unwrap()
                    .to_polars_expr_with(Some(&schema))
                    .is_ok(),
                "{} should lower",
                e
            );
        }
        agree(
            "substring(s, 1, 3)",
            &[Some("abcdef"), Some("ab"), Some(""), None],
            &[Some("bcd"), Some("b"), Some(""), None],
        );
        agree("substring(s, 0, 160)", &[Some("é1ü")], &[Some("é1ü")]);
        agree(
            "concat(s, \"-\", s)",
            &[Some("a"), None],
            &[Some("a-a"), Some("-")],
        );
        agree(
            "split(s, \" \")",
            &[Some("Rest error: x"), Some("one"), Some(""), None],
            &[Some("Rest"), Some("one"), Some(""), None],
        );
    }

    #[test]
    fn date_of_a_datetime_lowers_to_polars() {
        let (df, lookup) = epochs(&[1787569800000, 1787656200000]);
        let (fast, slow) = shown_on_both_paths("date(from_epoch(t, \"ms\"))", &df, &lookup);
        assert_eq!(fast, ["2026-08-24", "2026-08-25"]);
        assert_eq!(slow, ["2026-08-24", "2026-08-25"]);
    }
}
