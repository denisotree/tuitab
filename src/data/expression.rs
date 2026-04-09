use chrono::{Datelike, Timelike, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Number(n) => Some(*n),
            Value::String(s) => s.parse().ok(),
            Value::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
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
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    If {
        cond: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Box<Expr>,
    },
    InList {
        left: Box<Expr>,
        list: Vec<Expr>,
    },
}

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
}

// ── Parser ──────────────────────────────────────────────────────────────────────
// Recursive descent parser.
// Grammar:
//   expr     → term (('+' | '-') term)*
//   term     → factor (('*' | '/') factor)*
//   factor   → NUMBER | COLUMN_NAME | '(' expr ')'

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

    /// Try to translate AST into Polars lazy expression.
    /// Returns Err if the operation is not supported (triggers fallback execution).
    pub fn to_polars_expr(&self) -> Result<polars::lazy::dsl::Expr, String> {
        match self {
            Expr::ColumnRef(name) => Ok(polars::lazy::dsl::col(name.as_str())),
            Expr::Literal(val) => match val {
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
                    Op::Div => l.cast(polars::prelude::DataType::Float64) / r.cast(polars::prelude::DataType::Float64),
                    Op::Eq => l.eq(r),
                    Op::NotEq => l.neq(r),
                    Op::Lt => l.lt(r),
                    Op::Gt => l.gt(r),
                    Op::Leq => l.lt_eq(r),
                    Op::Geq => l.gt_eq(r),
                })
            }
            Expr::InList { left, list } => {
                let l = left.to_polars_expr()?;
                let mut polars_list = Vec::new();
                for item in list {
                    // Only support literals in Polars is_in for now
                    if let Expr::Literal(v) = item {
                        match v {
                            Value::Number(n) => polars_list.push(polars::prelude::AnyValue::Float64(*n)),
                            Value::String(s) => polars_list.push(polars::prelude::AnyValue::StringOwned(s.clone().into())),
                            _ => return Err("Only numbers and strings supported in IN list for Polars".to_string()),
                        }
                    } else {
                        return Err("Only literals supported in IN list for Polars".to_string());
                    }
                }
                let s = polars::prelude::Series::from_any_values("".into(), &polars_list, true).map_err(|e| e.to_string())?;
                Ok(l.is_in(polars::lazy::dsl::lit(s)))
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
                    .cast(polars::prelude::DataType::Float64)),
                "sum" if args.len() == 1 => Ok(args[0].to_polars_expr()?.sum().cast(polars::prelude::DataType::Float64)),
                "count" if args.len() == 1 => Ok(args[0].to_polars_expr()?.count().cast(polars::prelude::DataType::Float64)),
                "mean" if args.len() == 1 => Ok(args[0].to_polars_expr()?.mean().cast(polars::prelude::DataType::Float64)),
                "max" if args.len() == 1 => Ok(args[0].to_polars_expr()?.max().cast(polars::prelude::DataType::Float64)),
                "min" if args.len() == 1 => Ok(args[0].to_polars_expr()?.min().cast(polars::prelude::DataType::Float64)),
                "year" | "month" | "day" | "hour" | "minute" | "today" | "now" | "date_format" => Err(format!(
                    "Function '{}' requires slow-path evaluation",
                    name
                )),
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
            Expr::ColumnRef(name) => {
                if let Some(&col_idx) = col_lookup.get(name.as_str()) {
                    let cell_text = df.get_physical(row_idx, col_idx);
                    if let Ok(n) = cell_text.parse::<f64>() {
                        Value::Number(n)
                    } else if let Ok(b) = cell_text.parse::<bool>() {
                        Value::Boolean(b)
                    } else if let Ok(d) = NaiveDate::parse_from_str(&cell_text, "%Y-%m-%d") {
                        Value::Date(d)
                    } else if let Ok(dt) = chrono::DateTime::parse_from_str(&cell_text, "%Y-%m-%d %H:%M:%S%.f%#z") {
                        Value::Datetime(dt.naive_local())
                    } else if let Ok(dt) = chrono::DateTime::parse_from_str(&cell_text, "%Y-%m-%dT%H:%M:%S%.f%#z") {
                        Value::Datetime(dt.naive_local())
                    } else if let Ok(dt) = NaiveDateTime::parse_from_str(&cell_text, "%Y-%m-%d %H:%M:%S%.f") {
                        Value::Datetime(dt)
                    } else if let Ok(dt) = NaiveDateTime::parse_from_str(&cell_text, "%Y-%m-%dT%H:%M:%S%.f") {
                        Value::Datetime(dt)
                    } else if let Ok(dt) = NaiveDateTime::parse_from_str(&cell_text, "%Y-%m-%d %H:%M:%S") {
                        Value::Datetime(dt)
                    } else if let Ok(dt) = NaiveDateTime::parse_from_str(&cell_text, "%Y-%m-%dT%H:%M:%S") {
                        Value::Datetime(dt)
                    } else if cell_text.is_empty() {
                        Value::Null
                    } else {
                        Value::String(cell_text.clone())
                    }
                } else {
                    Value::Null
                }
            }
            Expr::BinOp { op, left, right } => {
                let l = left.eval(row_idx, col_lookup, df);
                let r = right.eval(row_idx, col_lookup, df);

                // 1. String Concatenation overloaded to +
                if matches!(op, Op::Add) {
                    if let (Value::String(s1), Value::String(s2)) = (&l, &r) {
                        return Value::String(format!("{}{}", s1, s2));
                    }
                }

                // 2. Date Math
                match op {
                    Op::Add => {
                        if let (Value::Date(d), Value::Number(days)) = (&l, &r) {
                            return Value::Date(*d + chrono::Duration::days(*days as i64));
                        }
                        if let (Value::Number(days), Value::Date(d)) = (&l, &r) {
                            return Value::Date(*d + chrono::Duration::days(*days as i64));
                        }
                        if let (Value::Datetime(dt), Value::Number(secs)) = (&l, &r) {
                            return Value::Datetime(*dt + chrono::Duration::seconds(*secs as i64));
                        }
                        if let (Value::Number(secs), Value::Datetime(dt)) = (&l, &r) {
                            return Value::Datetime(*dt + chrono::Duration::seconds(*secs as i64));
                        }
                    }
                    Op::Sub => {
                        if let (Value::Date(d), Value::Number(days)) = (&l, &r) {
                            return Value::Date(*d - chrono::Duration::days(*days as i64));
                        }
                        if let (Value::Date(d1), Value::Date(d2)) = (&l, &r) {
                            return Value::Number((*d1 - *d2).num_days() as f64);
                        }
                        if let (Value::Datetime(dt), Value::Number(secs)) = (&l, &r) {
                            return Value::Datetime(*dt - chrono::Duration::seconds(*secs as i64));
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
                        return Value::Boolean(l == r);
                    }
                    Op::NotEq => {
                        return Value::Boolean(l != r);
                    }
                    Op::Lt => {
                        if let (Some(n1), Some(n2)) = (l.as_f64(), r.as_f64()) {
                            return Value::Boolean(n1 < n2);
                        }
                        if let (Value::String(s1), Value::String(s2)) = (&l, &r) {
                            return Value::Boolean(s1 < s2);
                        }
                        if let (Value::Date(d1), Value::Date(d2)) = (&l, &r) {
                            return Value::Boolean(d1 < d2);
                        }
                        if let (Value::Datetime(dt1), Value::Datetime(dt2)) = (&l, &r) {
                            return Value::Boolean(dt1 < dt2);
                        }
                        return Value::Null;
                    }
                    Op::Gt => {
                        if let (Some(n1), Some(n2)) = (l.as_f64(), r.as_f64()) {
                            return Value::Boolean(n1 > n2);
                        }
                        if let (Value::String(s1), Value::String(s2)) = (&l, &r) {
                            return Value::Boolean(s1 > s2);
                        }
                        if let (Value::Date(d1), Value::Date(d2)) = (&l, &r) {
                            return Value::Boolean(d1 > d2);
                        }
                        if let (Value::Datetime(dt1), Value::Datetime(dt2)) = (&l, &r) {
                            return Value::Boolean(dt1 > dt2);
                        }
                        return Value::Null;
                    }
                    Op::Leq => {
                        if let (Some(n1), Some(n2)) = (l.as_f64(), r.as_f64()) {
                            return Value::Boolean(n1 <= n2);
                        }
                        if let (Value::String(s1), Value::String(s2)) = (&l, &r) {
                            return Value::Boolean(s1 <= s2);
                        }
                        if let (Value::Date(d1), Value::Date(d2)) = (&l, &r) {
                            return Value::Boolean(d1 <= d2);
                        }
                        if let (Value::Datetime(dt1), Value::Datetime(dt2)) = (&l, &r) {
                            return Value::Boolean(dt1 <= dt2);
                        }
                        return Value::Null;
                    }
                    Op::Geq => {
                        if let (Some(n1), Some(n2)) = (l.as_f64(), r.as_f64()) {
                            return Value::Boolean(n1 >= n2);
                        }
                        if let (Value::String(s1), Value::String(s2)) = (&l, &r) {
                            return Value::Boolean(s1 >= s2);
                        }
                        if let (Value::Date(d1), Value::Date(d2)) = (&l, &r) {
                            return Value::Boolean(d1 >= d2);
                        }
                        if let (Value::Datetime(dt1), Value::Datetime(dt2)) = (&l, &r) {
                            return Value::Boolean(dt1 >= dt2);
                        }
                        return Value::Null;
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
                    if l == item.eval(row_idx, col_lookup, df) {
                        return Value::Boolean(true);
                    }
                }
                Value::Boolean(false)
            }
            Expr::FunctionCall { name, args } => {
                let evaluated_args: Vec<Value> = args
                    .iter()
                    .map(|a| a.eval(row_idx, col_lookup, df))
                    .collect();

                match name.as_str() {
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
                            if let (Value::String(s), Value::String(delim)) =
                                (&evaluated_args[0], &evaluated_args[1])
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
                            if let (Value::String(s), Value::Number(start), Value::Number(len)) =
                                (&evaluated_args[0], &evaluated_args[1], &evaluated_args[2])
                            {
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
                            match &evaluated_args[0] {
                                Value::String(s) => return Value::Number(s.chars().count() as f64),
                                _ => return Value::Null,
                            }
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
                    "today" => {
                        Value::Date(chrono::Local::now().naive_local().date())
                    }
                    "now" => {
                        Value::Datetime(chrono::Local::now().naive_local())
                    }
                    "date_format" => {
                        if evaluated_args.len() == 2 {
                            if let (Value::String(fmt), v) = (&evaluated_args[1], &evaluated_args[0]) {
                                match v {
                                    Value::Date(d) => return Value::String(d.format(fmt).to_string()),
                                    Value::Datetime(dt) => return Value::String(dt.format(fmt).to_string()),
                                    _ => return Value::Null,
                                }
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

    /// expr → comparison
    fn parse_expr(&mut self) -> Result<Expr, String> {
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
                                    tok => return Err(format!("Expected ',' or ')', got {:?}", tok)),
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
                if name.to_lowercase() == "in" {
                    tokens.push(Token::In);
                } else {
                    tokens.push(Token::Ident(name));
                }
            }
            c => {
                return Err(format!("Unexpected character: '{}'", c));
            }
        }
    }

    Ok(tokens)
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
        let df = polars::prelude::DataFrame::new(series_vec).unwrap();
        let mut tui_df = crate::data::dataframe::DataFrame::empty();
        tui_df.df = df;
        tui_df
    }

    #[test]
    fn test_parse_simple_add() {
        let expr = Expr::parse("a+b").unwrap();
        let data = vec![
            vec!["10".to_string(), "20".to_string()],
            vec!["3".to_string(), "7".to_string()],
        ];
        let lookup: HashMap<&str, usize> = [("a", 0), ("b", 1)].into_iter().collect();
        let df = mock_df(data, vec!["a", "b"]);
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
        let data = vec![
            vec!["2".to_string()],
            vec!["3".to_string()],
            vec!["4".to_string()],
        ];
        let lookup: HashMap<&str, usize> = [("a", 0), ("b", 1), ("c", 2)].into_iter().collect();
        let df = mock_df(data, vec!["a", "b", "c"]);
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
}
