//! `tuitab_calc`: the arithmetic a model would otherwise do in its head.
//!
//! Not every question is about a table, and a model multiplying three numbers from
//! the conversation is exactly as unreliable as one summing a column by eye.  This
//! tool takes expressions as text and computes them here.
//!
//! **Decimal, not float.**  Numbers are [`Decimal`]s: 28 significant digits, and
//! `+ - * / %` with no binary rounding — `0.1 + 0.2` is `0.3`, and a loan payment
//! does not drift by a fraction of a cent.  What a decimal cannot carry falls back to
//! `f64` and says so: every value comes back with a `precision` of
//!
//! - `exact` — no digit was lost;
//! - `rounded` — a decimal rounded to 28 significant digits (`1/3`, `sqrt(2)`, `pi`);
//! - `approximate` — computed in floating point, good to about 15 digits: logarithms,
//!   trigonometry, fractional powers, IRR, and anything out of decimal range.
//!
//! **Steps.**  A calculation is a list of named steps, each able to use the ones
//! before it.  The model reads every intermediate value back instead of copying
//! numbers between calls, which is where digits get dropped.

use rust_decimal::prelude::{FromPrimitive, MathematicalOps, ToPrimitive};
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Most steps one call may hold.
const MAX_STEPS: usize = 200;
/// Longest expression accepted, in bytes.
const MAX_EXPR_LEN: usize = 10_000;
/// Largest `n` for `factorial(n)`; the result is out of float range long before.
const MAX_FACTORIAL: i64 = 10_000;

// ── numbers ─────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Precision {
    Exact,
    Rounded,
    Approximate,
}

impl Precision {
    fn name(self) -> &'static str {
        match self {
            Precision::Exact => "exact",
            Precision::Rounded => "rounded",
            Precision::Approximate => "approximate",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Repr {
    Dec(Decimal),
    Float(f64),
}

#[derive(Clone, Copy, Debug)]
struct Num {
    repr: Repr,
    precision: Precision,
}

type Res<T> = Result<T, String>;

impl Num {
    fn dec(d: Decimal, precision: Precision) -> Num {
        Num {
            repr: Repr::Dec(d.normalize()),
            precision,
        }
    }

    fn exact(d: Decimal) -> Num {
        Num::dec(d, Precision::Exact)
    }

    /// A float result.  Infinity and NaN are errors: a model handed `inf` reports it.
    fn float(x: f64) -> Res<Num> {
        if !x.is_finite() {
            return Err("the result is not a finite number (out of range or undefined)".into());
        }
        // A float that is a small whole number or a short decimal reads as a decimal,
        // but it is still only as good as the float it came from.
        Ok(Num {
            repr: Repr::Float(x),
            precision: Precision::Approximate,
        })
    }

    fn f64(self) -> f64 {
        match self.repr {
            Repr::Dec(d) => d.to_f64().unwrap_or(f64::NAN),
            Repr::Float(x) => x,
        }
    }

    fn with(self, precision: Precision) -> Num {
        Num {
            precision: self.precision.max(precision),
            ..self
        }
    }

    fn is_zero(self) -> bool {
        match self.repr {
            Repr::Dec(d) => d.is_zero(),
            Repr::Float(x) => x == 0.0,
        }
    }

    /// The value as an `i64` when it is a whole number.
    fn integer(self) -> Option<i64> {
        match self.repr {
            Repr::Dec(d) if d.fract().is_zero() => d.to_i64(),
            Repr::Float(x) if x.fract() == 0.0 && x.abs() < 9.0e15 => Some(x as i64),
            _ => None,
        }
    }

    fn render(self) -> String {
        match self.repr {
            Repr::Dec(d) => d.normalize().to_string(),
            Repr::Float(x) => render_float(x),
        }
    }
}

/// A float shown to the 15 significant digits it can vouch for, so `sin(pi/6)` reads
/// `0.5` rather than `0.49999999999999994`.
fn render_float(x: f64) -> String {
    let r: f64 = format!("{:.14e}", x).parse().unwrap_or(x);
    if r != 0.0 && !(1e-7..1e21).contains(&r.abs()) {
        format!("{:e}", r)
    } else {
        format!("{}", r)
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Arith {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

/// One arithmetic step, in decimal when both sides are decimal and it fits, in float
/// otherwise.  A decimal result that lost digits is marked `rounded`.
fn arith(a: Num, b: Num, op: Arith) -> Res<Num> {
    if matches!(op, Arith::Div | Arith::Rem) && b.is_zero() {
        return Err("division by zero".into());
    }
    let precision = a.precision.max(b.precision);
    if let (Repr::Dec(x), Repr::Dec(y)) = (a.repr, b.repr) {
        let checked = match op {
            Arith::Add => x.checked_add(y).map(|r| (r, r.checked_sub(y) == Some(x))),
            Arith::Sub => x.checked_sub(y).map(|r| (r, r.checked_add(y) == Some(x))),
            // A product needs exactly the digits of both factors; fewer means rounding.
            Arith::Mul => x
                .checked_mul(y)
                .map(|r| (r, r.scale() == x.scale() + y.scale())),
            // Exact only if multiplying back needs no rounding either: a rounded
            // quotient times the divisor can round straight back to the dividend.
            Arith::Div => x.checked_div(y).map(|r| {
                let r = r.normalize();
                let back = r.checked_mul(y);
                let exact = back.is_some_and(|m| m == x && m.scale() == r.scale() + y.scale());
                (r, exact)
            }),
            Arith::Rem => x.checked_rem(y).map(|r| (r, true)),
        };
        // A product or quotient of non-zero values that rounds to zero has underflowed
        // the decimal's 28 places: the float still has the magnitude.
        let underflow = |r: Decimal| {
            r.is_zero() && !x.is_zero() && matches!(op, Arith::Mul | Arith::Div) && !y.is_zero()
        };
        if let Some((r, exact)) = checked {
            if !underflow(r) {
                let p = if exact {
                    precision
                } else {
                    precision.max(Precision::Rounded)
                };
                return Ok(Num::dec(r, p));
            }
        }
    }
    let (x, y) = (a.f64(), b.f64());
    Num::float(match op {
        Arith::Add => x + y,
        Arith::Sub => x - y,
        Arith::Mul => x * y,
        Arith::Div => x / y,
        Arith::Rem => x % y,
    })
}

fn add(a: Num, b: Num) -> Res<Num> {
    arith(a, b, Arith::Add)
}
fn sub(a: Num, b: Num) -> Res<Num> {
    arith(a, b, Arith::Sub)
}
fn mul(a: Num, b: Num) -> Res<Num> {
    arith(a, b, Arith::Mul)
}
fn div(a: Num, b: Num) -> Res<Num> {
    arith(a, b, Arith::Div)
}

fn int(n: i64) -> Num {
    Num::exact(Decimal::from(n))
}

fn cmp(a: Num, b: Num) -> Ordering {
    match (a.repr, b.repr) {
        (Repr::Dec(x), Repr::Dec(y)) => x.cmp(&y),
        _ => a.f64().total_cmp(&b.f64()),
    }
}

/// `base ^ exp`: repeated multiplication for a whole exponent, so `1.07^10` stays a
/// decimal; a fractional exponent can only be approximated.
fn pow(base: Num, exp: Num) -> Res<Num> {
    if let Some(n) = exp.integer().filter(|n| n.unsigned_abs() <= 1_000_000) {
        let mut result = int(1).with(exp.precision);
        let mut square = base;
        let mut k = n.unsigned_abs();
        while k > 0 {
            if k & 1 == 1 {
                result = mul(result, square)?;
            }
            k >>= 1;
            if k > 0 {
                square = mul(square, square)?;
            }
        }
        return if n < 0 {
            div(int(1), result)
        } else {
            Ok(result)
        };
    }
    let (b, e) = (base.f64(), exp.f64());
    if b < 0.0 {
        return Err("a negative number has no real fractional power".into());
    }
    Num::float(b.powf(e))
}

/// A decimal square root is `exact` only when squaring it back gives the input with
/// no digit rounded away — `sqrt(2)` squared rounds to `2`, and is not exact.
fn sqrt(x: Num) -> Res<Num> {
    if x.f64() < 0.0 {
        return Err("sqrt() of a negative number".into());
    }
    if let Repr::Dec(d) = x.repr {
        if let Some(r) = d.sqrt() {
            let root = Num::dec(r, x.precision);
            let square = mul(root, root)?;
            let exact = square.precision == x.precision && cmp(square, x) == Ordering::Equal;
            return Ok(if exact {
                root
            } else {
                root.with(Precision::Rounded)
            });
        }
    }
    float_fn(x, f64::sqrt)
}

fn float_fn(x: Num, f: fn(f64) -> f64) -> Res<Num> {
    Num::float(f(x.f64()))
}

// ── values ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum Val {
    Num(Num),
    Bool(bool),
    List(Vec<Num>),
}

impl Val {
    fn json(&self) -> Value {
        match self {
            Val::Num(n) => json!({"value": n.render(), "precision": n.precision.name()}),
            Val::Bool(b) => json!({"value": b, "precision": "exact"}),
            Val::List(items) => {
                let worst = items
                    .iter()
                    .map(|n| n.precision)
                    .max()
                    .unwrap_or(Precision::Exact);
                json!({
                    "value": items.iter().map(|n| n.render()).collect::<Vec<_>>(),
                    "precision": worst.name()
                })
            }
        }
    }
}

// ── tokens ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Num(String),
    Ident(String),
    Sym(&'static str),
}

fn tokenize(src: &str) -> Res<Vec<Tok>> {
    let chars: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_digit()
            || (c == '.' && chars.get(i + 1).is_some_and(char::is_ascii_digit))
        {
            let start = i;
            while i < chars.len()
                && (chars[i].is_ascii_digit() || chars[i] == '.' || chars[i] == '_')
            {
                i += 1;
            }
            if i < chars.len() && (chars[i] == 'e' || chars[i] == 'E') {
                let mut j = i + 1;
                if j < chars.len() && (chars[j] == '+' || chars[j] == '-') {
                    j += 1;
                }
                if j < chars.len() && chars[j].is_ascii_digit() {
                    i = j;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                }
            }
            let text: String = chars[start..i].iter().filter(|c| **c != '_').collect();
            out.push(Tok::Num(text));
        } else if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            out.push(Tok::Ident(chars[start..i].iter().collect()));
        } else {
            let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
            let sym = match two.as_str() {
                "==" | "!=" | "<=" | ">=" | "**" | "&&" | "||" => {
                    i += 2;
                    match two.as_str() {
                        "==" => "==",
                        "!=" => "!=",
                        "<=" => "<=",
                        ">=" => ">=",
                        "**" => "^",
                        "&&" => "and",
                        _ => "or",
                    }
                }
                _ => {
                    i += 1;
                    match c {
                        '+' => "+",
                        '-' | '−' => "-",
                        '*' | '×' => "*",
                        '/' | '÷' => "/",
                        '%' => "%",
                        '^' => "^",
                        '(' => "(",
                        ')' => ")",
                        '[' => "[",
                        ']' => "]",
                        ',' => ",",
                        '<' => "<",
                        '>' => ">",
                        '!' => "not",
                        '=' => return Err("'=' is not an operator here: compare with '=='".into()),
                        other => return Err(format!("unexpected character '{}'", other)),
                    }
                }
            };
            out.push(Tok::Sym(sym));
        }
    }
    Ok(out)
}

/// A number literal as exactly as it can be held.
fn parse_number(text: &str) -> Res<Num> {
    let has_exp = text.contains(['e', 'E']);
    let exact = if has_exp {
        Decimal::from_scientific(text).ok()
    } else {
        Decimal::from_str_exact(text).ok()
    };
    if let Some(d) = exact {
        return Ok(Num::exact(d));
    }
    // Too many digits for 28 places, or too large for a decimal at all.
    if !has_exp {
        if let Ok(d) = text.parse::<Decimal>() {
            return Ok(Num::dec(d, Precision::Rounded));
        }
    }
    let x: f64 = text
        .parse()
        .map_err(|_| format!("'{}' is not a number", text))?;
    Num::float(x)
}

// ── syntax tree ─────────────────────────────────────────────────────────────

#[derive(Debug)]
enum Node {
    Lit(Num),
    Bool(bool),
    Name(String),
    List(Vec<Node>),
    Neg(Box<Node>),
    Not(Box<Node>),
    Bin(&'static str, Box<Node>, Box<Node>),
    Call(String, Vec<Node>),
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek_sym(&self, sym: &str) -> bool {
        match self.toks.get(self.pos) {
            Some(Tok::Sym(s)) => *s == sym,
            Some(Tok::Ident(word)) => word == sym && matches!(sym, "and" | "or" | "not"),
            _ => false,
        }
    }

    fn eat(&mut self, sym: &str) -> bool {
        let hit = self.peek_sym(sym);
        if hit {
            self.pos += 1;
        }
        hit
    }

    fn expect(&mut self, sym: &str) -> Res<()> {
        if self.eat(sym) {
            Ok(())
        } else {
            Err(format!("expected '{}' {}", sym, self.here()))
        }
    }

    fn here(&self) -> String {
        match self.toks.get(self.pos) {
            None => "at the end of the expression".into(),
            Some(Tok::Num(n)) => format!("before '{}'", n),
            Some(Tok::Ident(n)) => format!("before '{}'", n),
            Some(Tok::Sym(s)) => format!("before '{}'", s),
        }
    }

    fn or(&mut self) -> Res<Node> {
        let mut left = self.and()?;
        while self.eat("or") {
            left = Node::Bin("or", Box::new(left), Box::new(self.and()?));
        }
        Ok(left)
    }

    fn and(&mut self) -> Res<Node> {
        let mut left = self.not()?;
        while self.eat("and") {
            left = Node::Bin("and", Box::new(left), Box::new(self.not()?));
        }
        Ok(left)
    }

    fn not(&mut self) -> Res<Node> {
        if self.eat("not") {
            return Ok(Node::Not(Box::new(self.not()?)));
        }
        self.comparison()
    }

    fn comparison(&mut self) -> Res<Node> {
        let left = self.additive()?;
        for op in ["==", "!=", "<=", ">=", "<", ">"] {
            if self.eat(op) {
                return Ok(Node::Bin(op, Box::new(left), Box::new(self.additive()?)));
            }
        }
        Ok(left)
    }

    fn additive(&mut self) -> Res<Node> {
        let mut left = self.multiplicative()?;
        loop {
            let op = if self.eat("+") {
                "+"
            } else if self.eat("-") {
                "-"
            } else {
                return Ok(left);
            };
            left = Node::Bin(op, Box::new(left), Box::new(self.multiplicative()?));
        }
    }

    fn multiplicative(&mut self) -> Res<Node> {
        let mut left = self.unary()?;
        loop {
            let op = if self.eat("*") {
                "*"
            } else if self.eat("/") {
                "/"
            } else if self.eat("%") {
                "%"
            } else {
                return Ok(left);
            };
            left = Node::Bin(op, Box::new(left), Box::new(self.unary()?));
        }
    }

    /// Unary minus binds looser than `^`: `-2^2` is `-4`, as on paper.
    fn unary(&mut self) -> Res<Node> {
        if self.eat("-") {
            return Ok(Node::Neg(Box::new(self.unary()?)));
        }
        if self.eat("+") {
            return self.unary();
        }
        self.power()
    }

    /// Right-associative: `2^3^2` is `2^9`.
    fn power(&mut self) -> Res<Node> {
        let base = self.primary()?;
        if self.eat("^") {
            return Ok(Node::Bin("^", Box::new(base), Box::new(self.unary()?)));
        }
        Ok(base)
    }

    fn primary(&mut self) -> Res<Node> {
        let tok = self
            .toks
            .get(self.pos)
            .cloned()
            .ok_or_else(|| "the expression ends too early".to_string())?;
        self.pos += 1;
        match tok {
            Tok::Num(text) => Ok(Node::Lit(parse_number(&text)?)),
            Tok::Ident(word) => match word.as_str() {
                "true" => Ok(Node::Bool(true)),
                "false" => Ok(Node::Bool(false)),
                _ if self.eat("(") => {
                    let args = self.items(")")?;
                    Ok(Node::Call(word.to_lowercase(), args))
                }
                _ => Ok(Node::Name(word)),
            },
            Tok::Sym("(") => {
                let inner = self.or()?;
                self.expect(")")?;
                Ok(inner)
            }
            Tok::Sym("[") => Ok(Node::List(self.items("]")?)),
            Tok::Sym(s) => {
                self.pos -= 1;
                Err(format!("unexpected '{}' {}", s, self.here()))
            }
        }
    }

    fn items(&mut self, close: &str) -> Res<Vec<Node>> {
        let mut items = Vec::new();
        if self.eat(close) {
            return Ok(items);
        }
        loop {
            items.push(self.or()?);
            if self.eat(close) {
                return Ok(items);
            }
            self.expect(",")?;
        }
    }
}

fn parse(src: &str) -> Res<Node> {
    let mut parser = Parser {
        toks: tokenize(src)?,
        pos: 0,
    };
    let node = parser.or()?;
    if parser.pos < parser.toks.len() {
        return Err(format!("unexpected input {}", parser.here()));
    }
    Ok(node)
}

// ── evaluation ──────────────────────────────────────────────────────────────

const FUNCTIONS: &[&str] = &[
    "if",
    "abs",
    "round",
    "floor",
    "ceil",
    "trunc",
    "sqrt",
    "exp",
    "ln",
    "log10",
    "log",
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    "radians",
    "degrees",
    "min",
    "max",
    "factorial",
    "gcd",
    "lcm",
    "sum",
    "product",
    "count",
    "mean",
    "avg",
    "median",
    "variance",
    "var_p",
    "stdev",
    "stdev_p",
    "percentile",
    "pmt",
    "fv",
    "pv",
    "npv",
    "irr",
    "erf",
    "erfc",
    "norm_cdf",
    "norm_inv",
    "holm",
    "t_cdf",
    "t_inv",
    "chi2_cdf",
    "chi2_inv",
    "poisson_cdf",
    "binom_cdf",
    "bh",
];

const CONSTANTS: &[&str] = &["pi", "e"];

struct Env<'a> {
    names: &'a HashMap<String, Val>,
}

impl Env<'_> {
    fn eval(&self, node: &Node) -> Res<Val> {
        match node {
            Node::Lit(n) => Ok(Val::Num(*n)),
            Node::Bool(b) => Ok(Val::Bool(*b)),
            Node::Name(name) => self.name(name),
            Node::List(items) => {
                let mut out = Vec::new();
                for item in items {
                    match self.eval(item)? {
                        Val::Num(n) => out.push(n),
                        Val::List(inner) => out.extend(inner),
                        Val::Bool(_) => return Err("a list holds numbers, not true/false".into()),
                    }
                }
                Ok(Val::List(out))
            }
            Node::Neg(inner) => Ok(Val::Num(sub(int(0), self.num(inner)?)?)),
            Node::Not(inner) => Ok(Val::Bool(!self.boolean(inner)?)),
            Node::Bin(op, l, r) => self.binary(op, l, r),
            Node::Call(name, args) => self.call(name, args),
        }
    }

    fn name(&self, name: &str) -> Res<Val> {
        if let Some(v) = self.names.get(name) {
            return Ok(v.clone());
        }
        match name {
            "pi" => Ok(Val::Num(Num::dec(Decimal::PI, Precision::Rounded))),
            "e" => Ok(Val::Num(Num::dec(Decimal::E, Precision::Rounded))),
            _ if FUNCTIONS.contains(&name) => {
                Err(format!("'{}' is a function: call it as {}(…)", name, name))
            }
            _ => {
                let mut known: Vec<&str> = self.names.keys().map(String::as_str).collect();
                known.sort_unstable();
                Err(if known.is_empty() {
                    format!("unknown name '{}' — no earlier step has that name", name)
                } else {
                    format!(
                        "unknown name '{}' — earlier steps: {}",
                        name,
                        known.join(", ")
                    )
                })
            }
        }
    }

    fn num(&self, node: &Node) -> Res<Num> {
        match self.eval(node)? {
            Val::Num(n) => Ok(n),
            Val::Bool(_) => Err("expected a number, got true/false".into()),
            Val::List(_) => Err("expected a number, got a list".into()),
        }
    }

    fn boolean(&self, node: &Node) -> Res<bool> {
        match self.eval(node)? {
            Val::Bool(b) => Ok(b),
            _ => Err("expected a condition (true/false), got a number".into()),
        }
    }

    fn binary(&self, op: &str, l: &Node, r: &Node) -> Res<Val> {
        match op {
            "and" => Ok(Val::Bool(self.boolean(l)? && self.boolean(r)?)),
            "or" => Ok(Val::Bool(self.boolean(l)? || self.boolean(r)?)),
            "==" | "!=" | "<" | ">" | "<=" | ">=" => {
                let ord = cmp(self.num(l)?, self.num(r)?);
                Ok(Val::Bool(match op {
                    "==" => ord == Ordering::Equal,
                    "!=" => ord != Ordering::Equal,
                    "<" => ord == Ordering::Less,
                    ">" => ord == Ordering::Greater,
                    "<=" => ord != Ordering::Greater,
                    _ => ord != Ordering::Less,
                }))
            }
            _ => {
                let (a, b) = (self.num(l)?, self.num(r)?);
                Ok(Val::Num(match op {
                    "+" => add(a, b)?,
                    "-" => sub(a, b)?,
                    "*" => mul(a, b)?,
                    "/" => div(a, b)?,
                    "%" => arith(a, b, Arith::Rem)?,
                    _ => pow(a, b)?,
                }))
            }
        }
    }

    /// Every argument's numbers, with lists spread out: `sum(1, [2, 3])` is 6.
    fn numbers(&self, args: &[Node]) -> Res<Vec<Num>> {
        let mut out = Vec::new();
        for arg in args {
            match self.eval(arg)? {
                Val::Num(n) => out.push(n),
                Val::List(items) => out.extend(items),
                Val::Bool(_) => return Err("expected numbers, got true/false".into()),
            }
        }
        Ok(out)
    }

    fn call(&self, name: &str, args: &[Node]) -> Res<Val> {
        let arity = |min: usize, max: usize| -> Res<()> {
            if args.len() < min || args.len() > max {
                let want = if min == max {
                    min.to_string()
                } else {
                    format!("{} to {}", min, max)
                };
                Err(format!(
                    "{}() takes {} argument(s), got {}",
                    name,
                    want,
                    args.len()
                ))
            } else {
                Ok(())
            }
        };
        let arg = |i: usize| self.num(&args[i]);
        let opt = |i: usize, default: i64| -> Res<Num> {
            if i < args.len() {
                self.num(&args[i])
            } else {
                Ok(int(default))
            }
        };
        let n = |v: Num| Ok(Val::Num(v));

        match name {
            "if" => {
                arity(3, 3)?;
                if self.boolean(&args[0])? {
                    self.eval(&args[1])
                } else {
                    self.eval(&args[2])
                }
            }
            "abs" | "floor" | "ceil" | "trunc" => {
                arity(1, 1)?;
                let x = arg(0)?;
                n(match x.repr {
                    Repr::Dec(d) => Num::dec(
                        match name {
                            "abs" => d.abs(),
                            "floor" => d.floor(),
                            "ceil" => d.ceil(),
                            _ => d.trunc(),
                        },
                        x.precision,
                    ),
                    Repr::Float(f) => Num::float(match name {
                        "abs" => f.abs(),
                        "floor" => f.floor(),
                        "ceil" => f.ceil(),
                        _ => f.trunc(),
                    })?,
                })
            }
            "round" => {
                arity(1, 2)?;
                let x = arg(0)?;
                let places = opt(1, 0)?
                    .integer()
                    .filter(|p| p.abs() <= 28)
                    .ok_or("round()'s second argument is a whole number of places, -28 to 28")?;
                // Half away from zero, as a person rounds and as spreadsheets do.
                let scale = pow(int(10), int(places.abs()))?;
                let shifted = if places >= 0 {
                    mul(x, scale)?
                } else {
                    div(x, scale)?
                };
                let rounded = match shifted.repr {
                    Repr::Dec(d) => Num::dec(
                        d.round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero),
                        shifted.precision,
                    ),
                    Repr::Float(f) => Num::float(f.round())?,
                };
                n(if places >= 0 {
                    div(rounded, scale)?
                } else {
                    mul(rounded, scale)?
                })
            }
            "sqrt" => {
                arity(1, 1)?;
                n(sqrt(arg(0)?)?)
            }
            "exp" => {
                arity(1, 1)?;
                n(float_fn(arg(0)?, f64::exp)?)
            }
            "ln" | "log10" => {
                arity(1, 1)?;
                let x = arg(0)?;
                if x.f64() <= 0.0 {
                    return Err(format!("{}() needs a positive number", name));
                }
                n(float_fn(
                    x,
                    if name == "ln" { f64::ln } else { f64::log10 },
                )?)
            }
            "log" => {
                arity(1, 2)?;
                let x = arg(0)?.f64();
                let base = opt(1, 10)?.f64();
                if x <= 0.0 || base <= 0.0 || base == 1.0 {
                    return Err(
                        "log() needs a positive number and a positive base other than 1".into(),
                    );
                }
                n(Num::float(x.ln() / base.ln())?)
            }
            "sin" | "cos" | "tan" | "asin" | "acos" | "atan" => {
                arity(1, 1)?;
                let f: fn(f64) -> f64 = match name {
                    "sin" => f64::sin,
                    "cos" => f64::cos,
                    "tan" => f64::tan,
                    "asin" => f64::asin,
                    "acos" => f64::acos,
                    _ => f64::atan,
                };
                n(float_fn(arg(0)?, f)?)
            }
            "radians" | "degrees" => {
                arity(1, 1)?;
                let pi = Num::dec(Decimal::PI, Precision::Rounded);
                let x = arg(0)?;
                n(if name == "radians" {
                    div(mul(x, pi)?, int(180))?
                } else {
                    div(mul(x, int(180))?, pi)?
                })
            }
            "min" | "max" => {
                let xs = self.numbers(args)?;
                let want = if name == "min" {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
                let best = xs
                    .into_iter()
                    .reduce(|a, b| if cmp(b, a) == want { b } else { a })
                    .ok_or_else(|| format!("{}() needs at least one number", name))?;
                n(best)
            }
            "factorial" => {
                arity(1, 1)?;
                let k = arg(0)?
                    .integer()
                    .filter(|k| (0..=MAX_FACTORIAL).contains(k))
                    .ok_or_else(|| {
                        format!(
                            "factorial() takes a whole number from 0 to {}",
                            MAX_FACTORIAL
                        )
                    })?;
                let mut acc = int(1);
                for i in 2..=k {
                    acc = mul(acc, int(i))?;
                }
                n(acc)
            }
            "gcd" | "lcm" => {
                let xs = self.numbers(args)?;
                if xs.len() < 2 {
                    return Err(format!("{}() needs at least two whole numbers", name));
                }
                let mut acc: u128 = 0;
                for (i, x) in xs.iter().enumerate() {
                    let v = x
                        .integer()
                        .ok_or_else(|| format!("{}() takes whole numbers", name))?
                        .unsigned_abs() as u128;
                    acc = if i == 0 {
                        v
                    } else if name == "gcd" {
                        gcd(acc, v)
                    } else if acc == 0 || v == 0 {
                        0
                    } else {
                        (acc / gcd(acc, v))
                            .checked_mul(v)
                            .ok_or("lcm() result is too large")?
                    };
                }
                let d = Decimal::from_u128(acc).ok_or("the result is too large")?;
                n(Num::exact(d))
            }
            "sum" | "product" | "count" | "mean" | "avg" | "median" | "variance" | "var_p"
            | "stdev" | "stdev_p" => n(stat(name, &self.numbers(args)?)?),
            "percentile" => {
                arity(2, 2)?;
                let xs = self.numbers(&args[..1])?;
                n(percentile(&xs, arg(1)?)?)
            }
            "pmt" | "fv" | "pv" => {
                arity(3, 5)?;
                n(annuity(
                    name,
                    arg(0)?,
                    arg(1)?,
                    arg(2)?,
                    opt(3, 0)?,
                    opt(4, 0)?,
                )?)
            }
            "npv" => {
                if args.len() < 2 {
                    return Err("npv() takes a rate and then the cash flows".into());
                }
                let rate = arg(0)?;
                let one_plus = add(int(1), rate)?;
                let mut total = int(0);
                for (i, cash) in self.numbers(&args[1..])?.into_iter().enumerate() {
                    total = add(total, div(cash, pow(one_plus, int(i as i64 + 1))?)?)?;
                }
                n(total)
            }
            "t_cdf" | "t_inv" | "chi2_cdf" | "chi2_inv" | "poisson_cdf" => {
                arity(2, 2)?;
                let (a, b) = (arg(0)?.f64(), arg(1)?.f64());
                use super::special;
                n(Num::float(match name {
                    "t_cdf" => special::t_cdf(a, b)?,
                    "t_inv" => special::t_inv(a, b)?,
                    "chi2_cdf" => special::chi2_cdf(a, b)?,
                    "chi2_inv" => special::chi2_inv(a, b)?,
                    _ => special::poisson_cdf(a, b)?,
                })?)
            }
            "binom_cdf" => {
                arity(3, 3)?;
                let p = super::special::binom_cdf(arg(0)?.f64(), arg(1)?.f64(), arg(2)?.f64())?;
                n(Num::float(p)?)
            }
            "holm" | "bh" => Ok(Val::List(adjust_p_values(name, &self.numbers(args)?)?)),
            "erf" | "erfc" | "norm_cdf" => {
                arity(1, 1)?;
                let f: fn(f64) -> f64 = match name {
                    "erf" => libm::erf,
                    "erfc" => libm::erfc,
                    // Through erfc, so a far tail keeps its digits instead of 1 - 0.99999….
                    _ => |x| 0.5 * libm::erfc(-x / std::f64::consts::SQRT_2),
                };
                n(float_fn(arg(0)?, f)?)
            }
            "norm_inv" => {
                arity(1, 1)?;
                let p = arg(0)?.f64();
                if !(p > 0.0 && p < 1.0) {
                    return Err("norm_inv() takes a probability strictly between 0 and 1".into());
                }
                n(Num::float(norm_inv(p))?)
            }
            "irr" => {
                arity(1, 2)?;
                let flows: Vec<f64> = self.numbers(&args[..1])?.iter().map(|x| x.f64()).collect();
                let guess = if args.len() > 1 { arg(1)?.f64() } else { 0.1 };
                n(Num::float(irr(&flows, guess)?)?)
            }
            _ => Err(format!(
                "unknown function '{}'. Available: {}",
                name,
                FUNCTIONS.join(", ")
            )),
        }
    }
}

/// The standard normal quantile: Acklam's rational approximation, polished by two
/// Halley steps against `erfc`, which brings it to double precision.
fn norm_inv(p: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969683028665376e1,
        2.209460984245205e2,
        -2.759285104469687e2,
        1.38357751867269e2,
        -3.066479806614716e1,
        2.506628277459239e0,
    ];
    const B: [f64; 5] = [
        -5.447609879822406e1,
        1.615858368580409e2,
        -1.556989798598866e2,
        6.680131188771972e1,
        -1.328068155288572e1,
    ];
    const C: [f64; 6] = [
        -7.784894002430293e-3,
        -3.223964580411365e-1,
        -2.400758277161838e0,
        -2.549732539343734e0,
        4.374664141464968e0,
        2.938163982698783e0,
    ];
    const D: [f64; 4] = [
        7.784695709041462e-3,
        3.224671290700398e-1,
        2.445134137142996e0,
        3.754408661907416e0,
    ];
    let tail = |q: f64| {
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    };
    let mut x = if p < 0.02425 {
        tail((-2.0 * p.ln()).sqrt())
    } else if p > 1.0 - 0.02425 {
        -tail((-2.0 * (1.0 - p).ln()).sqrt())
    } else {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    };
    for _ in 0..2 {
        let e = 0.5 * libm::erfc(-x / std::f64::consts::SQRT_2) - p;
        let u = e * (2.0 * std::f64::consts::PI).sqrt() * (x * x / 2.0).exp();
        x -= u / (1.0 + x * u / 2.0);
    }
    x
}

fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Multiple-comparison adjusted p-values, returned in the order given.
///
/// `holm` controls the family-wise error rate (Holm–Bonferroni step-down); `bh` the
/// false discovery rate (Benjamini–Hochberg step-up), as R's `p.adjust` defines them.
fn adjust_p_values(name: &str, ps: &[Num]) -> Res<Vec<Num>> {
    if ps.is_empty() {
        return Err(format!("{}() needs at least one p-value", name));
    }
    if ps
        .iter()
        .any(|p| cmp(*p, int(0)) == Ordering::Less || cmp(*p, int(1)) == Ordering::Greater)
    {
        return Err(format!("{}() takes p-values from 0 to 1", name));
    }
    let m = ps.len();
    let mut order: Vec<usize> = (0..m).collect();
    order.sort_by(|a, b| cmp(ps[*a], ps[*b]));

    let one = int(1);
    let at_most_one = |x: Num| {
        if cmp(x, one) == Ordering::Greater {
            one
        } else {
            x
        }
    };
    let mut adjusted = vec![one; m];
    if name == "holm" {
        // Smallest first: (m - rank) * p, never below the adjusted value before it.
        let mut running = int(0);
        for (rank, &i) in order.iter().enumerate() {
            let scaled = mul(int((m - rank) as i64), ps[i])?;
            if cmp(scaled, running) == Ordering::Greater {
                running = scaled;
            }
            adjusted[i] = at_most_one(running);
        }
    } else {
        // Largest first: m / rank * p, never above the adjusted value after it.
        let mut running = one;
        for (rank, &i) in order.iter().enumerate().rev() {
            let scaled = div(mul(int(m as i64), ps[i])?, int(rank as i64 + 1))?;
            if cmp(scaled, running) == Ordering::Less {
                running = scaled;
            }
            adjusted[i] = at_most_one(running);
        }
    }
    Ok(adjusted)
}

fn stat(name: &str, xs: &[Num]) -> Res<Num> {
    let count = int(xs.len() as i64);
    if name == "count" {
        return Ok(count);
    }
    if xs.is_empty() {
        return Err(format!("{}() needs at least one number", name));
    }
    let sum = xs.iter().try_fold(int(0), |acc, x| add(acc, *x))?;
    match name {
        "sum" => Ok(sum),
        "product" => xs.iter().try_fold(int(1), |acc, x| mul(acc, *x)),
        "mean" | "avg" => div(sum, count),
        "median" => {
            let mut sorted = xs.to_vec();
            sorted.sort_by(|a, b| cmp(*a, *b));
            let mid = sorted.len() / 2;
            if sorted.len() % 2 == 1 {
                Ok(sorted[mid])
            } else {
                div(add(sorted[mid - 1], sorted[mid])?, int(2))
            }
        }
        _ => {
            let sample = matches!(name, "variance" | "stdev");
            if sample && xs.len() < 2 {
                return Err(format!(
                    "{}() is the sample statistic and needs at least two numbers; {}_p is the population one",
                    name,
                    if name == "variance" { "var" } else { "stdev" }
                ));
            }
            let mean = div(sum, count)?;
            let squares = xs.iter().try_fold(int(0), |acc, x| {
                let d = sub(*x, mean)?;
                add(acc, mul(d, d)?)
            })?;
            let denominator = if sample { sub(count, int(1))? } else { count };
            let variance = div(squares, denominator)?;
            if name.starts_with("var") {
                Ok(variance)
            } else {
                sqrt(variance)
            }
        }
    }
}

/// The inclusive percentile spreadsheets call `PERCENTILE.INC`: `p` from 0 to 1, with
/// linear interpolation between the two nearest ranks.
fn percentile(xs: &[Num], p: Num) -> Res<Num> {
    if xs.is_empty() {
        return Err("percentile() needs at least one number".into());
    }
    if cmp(p, int(0)) == Ordering::Less || cmp(p, int(1)) == Ordering::Greater {
        return Err("percentile()'s second argument is a fraction from 0 to 1, e.g. 0.95".into());
    }
    let mut sorted = xs.to_vec();
    sorted.sort_by(|a, b| cmp(*a, *b));
    let rank = mul(p, int(sorted.len() as i64 - 1))?;
    let lower = match rank.repr {
        Repr::Dec(d) => d.floor().to_usize().unwrap_or(0),
        Repr::Float(f) => f.floor() as usize,
    };
    if lower + 1 >= sorted.len() {
        return Ok(sorted[sorted.len() - 1]);
    }
    let fraction = sub(rank, int(lower as i64))?;
    let step = sub(sorted[lower + 1], sorted[lower])?;
    add(sorted[lower], mul(step, fraction)?)
}

/// `pmt`, `fv` and `pv` with the spreadsheet signatures and sign convention: money
/// paid out is negative.  `type` 1 means payments at the start of each period.
fn annuity(name: &str, rate: Num, nper: Num, third: Num, fourth: Num, due: Num) -> Res<Num> {
    let due = match due.integer() {
        Some(0) => int(0),
        Some(1) => int(1),
        _ => return Err(format!("{}()'s last argument (type) is 0 or 1", name)),
    };
    let neg = |x: Num| sub(int(0), x);
    if rate.is_zero() {
        return match name {
            // pmt(rate, nper, pv, fv)
            "pmt" => neg(div(add(third, fourth)?, nper)?),
            // fv(rate, nper, pmt, pv)
            "fv" => neg(add(fourth, mul(third, nper)?)?),
            // pv(rate, nper, pmt, fv)
            _ => neg(add(fourth, mul(third, nper)?)?),
        };
    }
    let growth = pow(add(int(1), rate)?, nper)?;
    let timing = add(int(1), mul(rate, due)?)?;
    let annuity_factor = div(sub(growth, int(1))?, rate)?;
    match name {
        "pmt" => {
            let (pv, fv) = (third, fourth);
            neg(div(
                add(fv, mul(pv, growth)?)?,
                mul(timing, annuity_factor)?,
            )?)
        }
        "fv" => {
            let (pmt, pv) = (third, fourth);
            neg(add(
                mul(pv, growth)?,
                mul(mul(pmt, timing)?, annuity_factor)?,
            )?)
        }
        _ => {
            let (pmt, fv) = (third, fourth);
            neg(div(
                add(fv, mul(mul(pmt, timing)?, annuity_factor)?)?,
                growth,
            )?)
        }
    }
}

/// The rate at which the cash flows' present value is zero: Newton's method from the
/// guess, then bisection when Newton wanders off.
fn irr(flows: &[f64], guess: f64) -> Res<f64> {
    let has_both_signs = flows.iter().any(|x| *x > 0.0) && flows.iter().any(|x| *x < 0.0);
    if flows.len() < 2 || !has_both_signs {
        return Err(
            "irr() needs cash flows with at least one negative and one positive value".into(),
        );
    }
    let npv = |r: f64| -> f64 {
        flows
            .iter()
            .enumerate()
            .map(|(i, c)| c / (1.0 + r).powi(i as i32))
            .sum()
    };
    let slope = |r: f64| -> f64 {
        flows
            .iter()
            .enumerate()
            .skip(1)
            .map(|(i, c)| -(i as f64) * c / (1.0 + r).powi(i as i32 + 1))
            .sum()
    };

    let mut r = guess;
    for _ in 0..100 {
        let (v, d) = (npv(r), slope(r));
        if !v.is_finite() || !d.is_finite() || d == 0.0 {
            break;
        }
        let next = r - v / d;
        if next <= -1.0 {
            break;
        }
        if (next - r).abs() < 1e-12 {
            return Ok(next);
        }
        r = next;
    }

    let (mut lo, mut hi) = (-0.999_999, 1.0);
    while npv(lo).signum() == npv(hi).signum() && hi < 1e6 {
        hi *= 10.0;
    }
    if npv(lo).signum() == npv(hi).signum() {
        return Err("irr() found no rate where the cash flows balance".into());
    }
    for _ in 0..300 {
        let mid = (lo + hi) / 2.0;
        if npv(lo).signum() == npv(mid).signum() {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok((lo + hi) / 2.0)
}

// ── the tool ────────────────────────────────────────────────────────────────

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_alphabetic() || c == '_')
        && chars.all(|c| c.is_alphanumeric() || c == '_')
}

/// Run a `tuitab_calc` call: `{"steps": [{"name", "expr"}, …]}`, or `{"expr": …}` for one.
pub fn run(args: &Value) -> Result<Value, String> {
    let steps: Vec<(Option<String>, String)> = if let Some(expr) = args.get("expr") {
        let expr = expr.as_str().ok_or("'expr' must be a string")?.to_string();
        vec![(None, expr)]
    } else {
        let list = args
            .get("steps")
            .and_then(Value::as_array)
            .ok_or("pass 'steps' — a list of {\"name\", \"expr\"} — or a single 'expr'")?;
        list.iter()
            .enumerate()
            .map(|(i, step)| match step {
                Value::String(expr) => Ok((None, expr.clone())),
                Value::Object(obj) => {
                    let expr = obj
                        .get("expr")
                        .and_then(Value::as_str)
                        .ok_or_else(|| format!("steps[{}] has no 'expr' string", i))?;
                    let name = match obj.get("name") {
                        None | Some(Value::Null) => None,
                        Some(Value::String(s)) => Some(s.clone()),
                        Some(_) => return Err(format!("steps[{}].name must be a string", i)),
                    };
                    Ok((name, expr.to_string()))
                }
                _ => Err(format!("steps[{}] must be an object with 'expr'", i)),
            })
            .collect::<Result<_, String>>()?
    };
    if steps.is_empty() {
        return Err("'steps' is empty".into());
    }
    if steps.len() > MAX_STEPS {
        return Err(format!("at most {} steps per call", MAX_STEPS));
    }

    let mut names: HashMap<String, Val> = HashMap::new();
    let mut results = Vec::new();
    let mut last = Value::Null;
    for (i, (name, expr)) in steps.iter().enumerate() {
        let label = name.clone().unwrap_or_else(|| format!("steps[{}]", i));
        let fail = |why: String| format!("{} ({}): {}", label, expr, why);
        if expr.len() > MAX_EXPR_LEN {
            return Err(fail(format!("longer than {} characters", MAX_EXPR_LEN)));
        }
        if let Some(name) = name {
            if !is_identifier(name) {
                return Err(fail(
                    "a step name is letters, digits and '_', not starting with a digit".into(),
                ));
            }
            let reserved = FUNCTIONS.contains(&name.as_str())
                || CONSTANTS.contains(&name.as_str())
                || ["and", "or", "not", "true", "false"].contains(&name.as_str());
            if reserved {
                return Err(fail(format!(
                    "'{}' is a built-in name; choose another",
                    name
                )));
            }
            if names.contains_key(name) {
                return Err(fail(format!("a step named '{}' already exists", name)));
            }
        }

        let node = parse(expr).map_err(fail)?;
        let value = Env { names: &names }.eval(&node).map_err(fail)?;
        let mut entry = value.json();
        if let Some(name) = name {
            entry["name"] = json!(name);
            names.insert(name.clone(), value);
        }
        entry["expr"] = json!(expr);
        last = entry.clone();
        results.push(entry);
    }

    Ok(json!({"steps": results, "result": last}))
}
