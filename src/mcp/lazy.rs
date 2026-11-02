//! Pipelines over files larger than memory: the file is scanned, the leading
//! operations become one Polars plan with projection and predicate pushdown, and the
//! plan runs on the streaming engine.  Only an answer — a few rows, a grouped table — or
//! what an operation that needs the whole table asks for is ever collected.
//!
//! Every answer is the in-memory path's answer.  The column types, names and order of
//! each step come from running that path's own operation on an empty table of the same
//! columns (the "shell"), and the grouping operations run the very same function over
//! the scan instead of over a loaded table.

use super::pipeline::Op;
use super::source::Source;
use crate::data::column::EpochUnit;
use crate::data::dataframe::DataFrame;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

/// A source as a plan, and an empty table of its columns carrying what tuitab knows
/// about each — types, epoch units.
#[derive(Clone)]
pub struct Scanned {
    /// How a read error names the file, as the in-memory path does.
    reading: String,
    pub lf: LazyFrame,
    pub shell: DataFrame,
    /// Columns holding nested JSON, which the in-memory path shows as a compact
    /// summary; they are summarised once collected, and an operation that reads one
    /// is run in memory.
    nested: Vec<String>,
}

/// What running a pipeline came to.
pub enum Outcome {
    /// The whole pipeline ran as a plan: the first `limit` rows, and how many there are.
    Head { df: DataFrame, row_count: usize },
    /// `ops[next..]` are left for the in-memory path, over `df`.
    Collected { df: DataFrame, next: usize },
}

/// The source as a plan, when it is one file (or a pattern of them) in a format Polars
/// scans.  `None` sends the caller to the in-memory path.
pub fn scan(src: &Source) -> Result<Option<Scanned>, String> {
    if src.container.is_some() {
        return Ok(None);
    }
    let text = src.path.to_string_lossy().to_string();
    let pattern = text.contains(['*', '?', '[']);
    if !pattern {
        if !src.path.is_file() {
            return Ok(None);
        }
        // A database under any name, and a file whose name says nothing, are the
        // in-memory path's to read.
        if crate::data::io::db_write::kind_of_file(&src.path).is_some()
            || (src.format.is_none() && src.path.extension().is_none())
        {
            return Ok(None);
        }
    }
    let ext = super::source::extension_of(src);
    let lines = matches!(ext.as_str(), "jsonl" | "ndjson" | "ldjson");
    let table = matches!(
        ext.as_str(),
        "csv" | "tsv" | "parquet" | "arrow" | "feather" | "ipc"
    );
    if !lines && !table {
        return Ok(None);
    }
    let first = if pattern {
        let files = match crate::data::io::glob_matches(&text) {
            Ok(files) if !files.is_empty() => files,
            _ => return Ok(None),
        };
        // A pattern reads files that hold one table.  One whose columns differ is the
        // in-memory path's to name, as it always has.
        let names = |file: &Path| -> Option<Vec<String>> {
            let one = PlRefPath::try_from_path(file).ok()?;
            let mut lf = match ext.as_str() {
                "csv" | "tsv" => LazyCsvReader::new(one)
                    .with_separator(
                        src.delimiter
                            .unwrap_or_else(|| crate::data::loader::detect_delimiter(file)),
                    )
                    .with_has_header(true)
                    .with_infer_schema_length(Some(10))
                    .finish()
                    .ok()?,
                "parquet" => LazyFrame::scan_parquet(one, ScanArgsParquet::default()).ok()?,
                "arrow" | "feather" | "ipc" => {
                    LazyFrame::scan_ipc(one, Default::default(), Default::default()).ok()?
                }
                _ => LazyJsonLineReader::new(one).finish().ok()?,
            };
            let schema = lf.collect_schema().ok()?;
            Some(schema.iter_names().map(|n| n.to_string()).collect())
        };
        let head = names(&files[0]);
        if head.is_none() || files[1..].iter().any(|f| names(f) != head) {
            return Ok(None);
        }
        files[0].clone()
    } else {
        src.path.clone()
    };

    // A read error names the file, the way the in-memory path's does.
    let reading = match src.format.as_deref() {
        Some(declared) if crate::data::doc::Format::from_name(declared).is_none() => {
            format!("Could not read {} as {}", src.path.display(), ext)
        }
        _ => format!("Could not read {}", src.path.display()),
    };
    let named = |e: String| format!("{}: {}", reading, e);
    let path = PlRefPath::try_from_path(Path::new(&text)).map_err(|e| e.to_string())?;
    let key = profile_key(src, &first, pattern);
    // A CSV's and a JSONL file's types are worked out from every row, as the in-memory
    // readers do — one full pass, paid once per version of the file.
    let inferred = |lf: LazyFrame| -> Result<Schema, String> {
        let mut lf = lf;
        Ok((*lf.collect_schema().map_err(|e| e.to_string())?).clone())
    };
    let mut lf = match ext.as_str() {
        "csv" | "tsv" => {
            let schema = cached(&SCHEMAS, &key, || {
                inferred(scan_csv(src, path.clone(), &first, None)?)
            })
            .map_err(named)?;
            scan_csv(src, path, &first, Some(schema)).map_err(named)?
        }
        "parquet" => LazyFrame::scan_parquet(path, ScanArgsParquet::default())
            .map_err(|e| named(e.to_string()))?,
        "arrow" | "feather" | "ipc" => {
            LazyFrame::scan_ipc(path, Default::default(), Default::default())
                .map_err(|e| named(e.to_string()))?
        }
        _ => {
            let schema = cached(&SCHEMAS, &key, || {
                inferred(
                    LazyJsonLineReader::new(path.clone())
                        .with_infer_schema_length(None)
                        .finish()
                        .map_err(|e| e.to_string())?,
                )
            })
            .map_err(named)?;
            LazyJsonLineReader::new(path)
                .with_schema(Some(std::sync::Arc::new(schema)))
                .finish()
                .map_err(|e| named(e.to_string()))?
        }
    };

    let mut schema = (*lf.collect_schema().map_err(|e| named(e.to_string()))?).clone();

    // The in-memory CSV reader turns `undefined` and friends into NULL in a column
    // that is otherwise numbers; the scan does the same, from one profiling pass.
    if matches!(ext.as_str(), "csv" | "tsv") {
        let casts = cached(&TOKEN_CASTS, &key, || token_casts(&lf, &schema)).map_err(named)?;
        if !casts.is_empty() {
            lf = lf.with_columns(
                casts
                    .iter()
                    .map(|(name, dtype)| token_cast_expr(name, dtype))
                    .collect::<Vec<_>>(),
            );
            schema = (*lf.collect_schema().map_err(|e| named(e.to_string()))?).clone();
        }
    }

    // Epoch columns of a table file, as the in-memory loader shows them.
    let mut epochs: Vec<(String, EpochUnit)> = Vec::new();
    if table && src.detect_epoch {
        epochs = cached(&EPOCHS, &key, || epoch_columns(&lf, &schema)).map_err(named)?;
        if !epochs.is_empty() {
            lf = lf.with_columns(
                epochs
                    .iter()
                    .map(|(name, unit)| {
                        crate::data::expression::epoch_to_datetime(col(name.as_str()), *unit)
                            .alias(name.as_str())
                    })
                    .collect::<Vec<_>>(),
            );
            schema = (*lf.collect_schema().map_err(|e| named(e.to_string()))?).clone();
        }
    }

    let nested: Vec<String> = schema
        .iter()
        .filter(|(_, t)| matches!(t, DataType::Struct(_) | DataType::List(_)))
        .map(|(n, _)| n.to_string())
        .collect();
    // In the shell a nested column is the text the in-memory path makes of it.
    let mut shell_schema = schema.clone();
    for name in &nested {
        shell_schema.with_column(name.as_str().into(), DataType::String);
    }
    let empty = polars::prelude::DataFrame::empty_with_schema(&shell_schema);
    let mut shell = crate::data::io::wrap_polars_df(empty).map_err(|e| e.to_string())?;
    for (name, unit) in &epochs {
        if let Ok(i) = shell.column_index(name) {
            shell.columns[i].epoch_unit = Some(*unit);
        }
    }
    Ok(Some(Scanned {
        reading,
        lf,
        shell,
        nested,
    }))
}

fn scan_csv(
    src: &Source,
    path: PlRefPath,
    first: &Path,
    schema: Option<Schema>,
) -> Result<LazyFrame, String> {
    let opts = src.load_opts();
    let sep = opts
        .delimiter
        .unwrap_or_else(|| crate::data::loader::detect_delimiter(first));
    let mut reader = LazyCsvReader::new(path)
        .with_separator(sep)
        .with_has_header(true)
        .with_infer_schema_length(None);
    if let Some(schema) = schema {
        reader = reader.with_schema(Some(std::sync::Arc::new(schema)));
    }
    if !opts.null_values.is_empty() {
        reader = reader.with_null_values(Some(NullValues::AllColumns(
            opts.null_values.iter().map(|v| v.as_str().into()).collect(),
        )));
    }
    reader.finish().map_err(|e| e.to_string())
}

// ── Profiling, cached per file version ──────────────────────────────────────────────

type Key = String;

/// Per file version: what one profiling pass found.
type Store<T> = OnceLock<Mutex<HashMap<Key, T>>>;

static SCHEMAS: Store<Schema> = OnceLock::new();
static TOKEN_CASTS: Store<Vec<(String, DataType)>> = OnceLock::new();
static EPOCHS: Store<Vec<(String, EpochUnit)>> = OnceLock::new();

/// Which file, read how, at which version — a pattern is never cached, its files can
/// change one by one.
fn profile_key(src: &Source, first: &Path, pattern: bool) -> Option<Key> {
    if pattern {
        return None;
    }
    let meta = std::fs::metadata(first).ok()?;
    let stamp = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos();
    Some(format!(
        "{}|{}|{}|{:?}",
        first.display(),
        stamp,
        meta.len(),
        src.load_opts()
    ))
}

fn cached<T: Clone>(
    store: &'static Store<T>,
    key: &Option<Key>,
    compute: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    let store = store.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(k) = key {
        if let Some(hit) = store.lock().unwrap_or_else(|e| e.into_inner()).get(k) {
            return Ok(hit.clone());
        }
    }
    let value = compute()?;
    if let Some(k) = key {
        store
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(k.clone(), value.clone());
    }
    Ok(value)
}

/// What the in-memory reader reads as "no value" in a column of numbers.
const NULL_TOKENS: &[&str] = &["undefined", "null", "NULL", "NaN", "None", "N/A", "n/a"];

fn tokens_series() -> Series {
    Series::new("tokens".into(), NULL_TOKENS)
}

/// A text column's value with the null tokens and the empty string as NULL.
fn cleaned(name: &str) -> Expr {
    let trimmed = col(name).str().strip_chars(lit(polars::prelude::Null {}));
    when(
        trimmed
            .clone()
            .is_in(lit(tokens_series()), false)
            .or(trimmed.clone().eq(lit(""))),
    )
    .then(lit(polars::prelude::Null {}))
    .otherwise(trimmed)
}

fn token_cast_expr(name: &str, dtype: &DataType) -> Expr {
    cleaned(name).cast(dtype.clone()).alias(name)
}

/// The text columns holding a null token whose every other value is a number, and
/// the number type each becomes — one streaming pass, the in-memory rule.
fn token_casts(lf: &LazyFrame, schema: &Schema) -> Result<Vec<(String, DataType)>, String> {
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
        let trimmed = col(name.as_str())
            .str()
            .strip_chars(lit(polars::prelude::Null {}));
        let c = cleaned(name);
        exprs.push(
            trimmed
                .clone()
                .is_in(lit(tokens_series()), false)
                .any(true)
                .alias(format!("t{}", i)),
        );
        exprs.push(c.clone().is_not_null().sum().alias(format!("n{}", i)));
        for (tag, dtype) in [("i", DataType::Int64), ("f", DataType::Float64)] {
            exprs.push(
                (c.clone().is_not_null().and(c.clone().cast(dtype).is_null()))
                    .sum()
                    .alias(format!("{}{}", tag, i)),
            );
        }
        exprs.push(
            c.clone()
                .str()
                .contains(lit("^-?0[0-9]"), true)
                .any(true)
                .alias(format!("p{}", i)),
        );
    }
    let stats = crate::data::collect(lf.clone().select(exprs)).map_err(|e| e.to_string())?;
    let get = |key: String| stats.column(&key).ok().and_then(|c| c.get(0).ok());
    let truthy = |v: Option<AnyValue>| matches!(v, Some(AnyValue::Boolean(true)));
    let count = |v: Option<AnyValue>| v.and_then(|v| v.extract::<u64>()).unwrap_or(0);
    let mut casts = Vec::new();
    for (i, name) in text.iter().enumerate() {
        if !truthy(get(format!("t{}", i)))
            || count(get(format!("n{}", i))) == 0
            || truthy(get(format!("p{}", i)))
        {
            continue;
        }
        if count(get(format!("i{}", i))) == 0 {
            casts.push((name.clone(), DataType::Int64));
        } else if count(get(format!("f{}", i))) == 0 {
            casts.push((name.clone(), DataType::Float64));
        }
    }
    Ok(casts)
}

/// Integer columns named like a time whose every value is an epoch in one unit.
fn epoch_columns(lf: &LazyFrame, schema: &Schema) -> Result<Vec<(String, EpochUnit)>, String> {
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
        let c = col(name.as_str()).cast(DataType::Int64);
        exprs.push(c.clone().min().alias(format!("lo{}", i)));
        exprs.push(c.max().alias(format!("hi{}", i)));
    }
    let stats = crate::data::collect(lf.clone().select(exprs)).map_err(|e| e.to_string())?;
    let get = |key: String| {
        stats
            .column(&key)
            .ok()
            .and_then(|c| c.get(0).ok())
            .and_then(|v| v.extract::<i64>())
    };
    Ok(candidates
        .iter()
        .enumerate()
        .filter_map(|(i, name)| {
            let unit = EpochUnit::of_range(get(format!("lo{}", i))?, get(format!("hi{}", i))?)?;
            Some((name.clone(), unit))
        })
        .collect())
}

// ── Running a pipeline ──────────────────────────────────────────────────────────────

/// Run `ops` over the scan.  `limit` is how many rows the caller will show when the
/// whole pipeline is a plan; `whole` asks for every row (a file is being written);
/// `budget` is how many bytes may be collected.
pub fn run(
    scanned: &Scanned,
    ops: &[Op],
    limit: usize,
    whole: bool,
    budget: u64,
) -> Result<Outcome, String> {
    let mut lf = scanned.lf.clone();
    let mut shell = scanned.shell.clone();
    let mut i = 0;
    while i < ops.len() {
        let op = &ops[i];
        // Only a step that can join the plan is tried on the empty table; the others —
        // dedup, a join, an expansion that needs the values — run in memory over the
        // collected rows, exactly as they always have.
        let streams = matches!(
            op,
            Op::Filter(_)
                | Op::Select(_)
                | Op::Compute { .. }
                | Op::Sort(_)
                | Op::Limit(_)
                | Op::Unpivot(_)
                | Op::GroupBy { .. }
                | Op::Aggregate(_)
                | Op::Frequency { .. }
        );
        if !streams || reads_nested(op, &scanned.nested) {
            break;
        }
        // The in-memory operation on the empty table first: its errors are the ones
        // the caller would have had, and its table says what the step produces.
        let next = super::pipeline::apply_op(shell.clone(), op)
            .map_err(|e| format!("ops[{}]: {}", i, super::pipeline::without_polars_hints(&e)))?;
        match op {
            // The grouping steps run the in-memory path's own function over the scan;
            // what they produce is small, and the rest of the pipeline runs on it.
            Op::GroupBy { by, agg } => {
                let df = crate::data::group::group_by_from(&shell, lf, by, agg).map_err(|e| {
                    format!("ops[{}]: {}", i, super::pipeline::without_polars_hints(&e))
                })?;
                return Ok(Outcome::Collected { df, next: i + 1 });
            }
            Op::Aggregate(agg) => {
                let df = crate::data::group::total_from(&shell, lf, agg).map_err(|e| {
                    format!("ops[{}]: {}", i, super::pipeline::without_polars_hints(&e))
                })?;
                return Ok(Outcome::Collected { df, next: i + 1 });
            }
            Op::Frequency { by, agg } => {
                let df = crate::data::group::frequency_from(&shell, lf, by, agg).map_err(|e| {
                    format!("ops[{}]: {}", i, super::pipeline::without_polars_hints(&e))
                })?;
                return Ok(Outcome::Collected { df, next: i + 1 });
            }
            _ => match lower(lf.clone(), &shell, op)? {
                Some(lowered) => lf = lowered,
                None => break,
            },
        }
        shell = next;
        i += 1;
    }

    // The step's columns, in the step's order and types.
    let lf = conform(lf, &shell, &scanned.nested);
    if i == ops.len() && !whole {
        let counted = crate::data::collect(lf.clone().select([len().alias("n")]))
            .map_err(|e| format!("{}: {}", scanned.reading, e))?;
        let row_count = counted
            .column("n")
            .ok()
            .and_then(|c| c.get(0).ok())
            .and_then(|v| v.extract::<u64>())
            .unwrap_or(0) as usize;
        let head = crate::data::collect(lf.limit(limit as IdxSize))
            .map_err(|e| format!("{}: {}", scanned.reading, e))?;
        let df = finish(head, &shell, &scanned.nested)?;
        return Ok(Outcome::Head { df, row_count });
    }

    guard(
        &lf,
        ops.get(i).map_or("writing the result", op_name),
        budget,
    )
    .map_err(|e| match e {
        Guard::Refused(why) => why,
        Guard::Read(why) => format!("{}: {}", scanned.reading, why),
    })?;
    let all = crate::data::collect(lf).map_err(|e| format!("{}: {}", scanned.reading, e))?;
    let df = finish(all, &shell, &scanned.nested)?;
    Ok(Outcome::Collected { df, next: i })
}

/// One step as part of the plan, or `None` when it needs the whole table in memory.
fn lower(lf: LazyFrame, shell: &DataFrame, op: &Op) -> Result<Option<LazyFrame>, String> {
    let schema = shell.df.schema().clone();
    Ok(Some(match op {
        Op::Filter(clauses) => {
            let Some(expr) = crate::data::filter::clauses_to_expr(shell, clauses)? else {
                return Ok(Some(lf));
            };
            match expr.to_polars_expr_with(Some(schema.as_ref())) {
                Ok(mask) => lf.filter(mask.fill_null(lit(false))),
                Err(_) => return Ok(None),
            }
        }
        Op::Select(names) => lf.select(names.iter().map(|n| col(n.as_str())).collect::<Vec<_>>()),
        Op::Compute { name, expr } => {
            let Ok(parsed) = crate::data::expression::Expr::parse(expr) else {
                return Ok(None);
            };
            // json_get's type depends on every value, and a date difference is turned
            // into seconds after the fact: both are the in-memory path's to finish.
            if matches!(&parsed, crate::data::expression::Expr::FunctionCall { name, .. } if name == "json_get")
            {
                return Ok(None);
            }
            match parsed.to_polars_expr_with(Some(schema.as_ref())) {
                Ok(e) => {
                    let out = lf.with_column(e.alias(name.as_str()));
                    let mut probe = out.clone();
                    match probe.collect_schema() {
                        Ok(s) if !matches!(s.get(name.as_str()), Some(DataType::Duration(_))) => {
                            out
                        }
                        _ => return Ok(None),
                    }
                }
                Err(_) => return Ok(None),
            }
        }
        Op::Sort(keys) => {
            let names: Vec<PlSmallStr> = keys.iter().map(|k| k.col.as_str().into()).collect();
            let descending: Vec<bool> = keys.iter().map(|k| k.desc).collect();
            lf.sort(
                names,
                SortMultipleOptions::new()
                    .with_order_descending_multi(descending)
                    .with_nulls_last(true)
                    .with_maintain_order(true),
            )
        }
        Op::Limit(n) => lf.limit(*n as IdxSize),
        Op::Unpivot(spec) => crate::data::unpivot::unpivot_lazy(lf, schema.as_ref(), spec)?,
        _ => return Ok(None),
    }))
}

/// Whether a step reads a nested column — those it can only read as the in-memory
/// path's text.
fn reads_nested(op: &Op, nested: &[String]) -> bool {
    if nested.is_empty() {
        return false;
    }
    fn named<'a>(mut names: impl Iterator<Item = &'a String>, nested: &[String]) -> bool {
        names.any(|n| nested.contains(n))
    }
    match op {
        Op::Select(_) | Op::Limit(_) => false,
        Op::Filter(clauses) => named(
            clauses.iter().flat_map(|c| c.predicates()).map(|p| &p.col),
            nested,
        ),
        Op::Sort(keys) => named(keys.iter().map(|k| &k.col), nested),
        Op::GroupBy { by, agg } | Op::Frequency { by, agg } => {
            named(by.iter(), nested)
                || named(agg.iter().map(|a| &a.col), nested)
                || named(
                    agg.iter()
                        .flat_map(|a| a.where_.iter())
                        .flat_map(|c| c.predicates())
                        .map(|p| &p.col),
                    nested,
                )
        }
        Op::Aggregate(agg) => {
            named(agg.iter().map(|a| &a.col), nested)
                || named(
                    agg.iter()
                        .flat_map(|a| a.where_.iter())
                        .flat_map(|c| c.predicates())
                        .map(|p| &p.col),
                    nested,
                )
        }
        // An expression or anything else may read any column.
        _ => true,
    }
}

/// The plan's columns as the step's table has them: its order, its types.
fn conform(lf: LazyFrame, shell: &DataFrame, nested: &[String]) -> LazyFrame {
    let exprs: Vec<Expr> = shell
        .df
        .schema()
        .iter()
        .map(|(name, dtype)| {
            if nested.iter().any(|n| n == name.as_str()) {
                col(name.as_str())
            } else {
                col(name.as_str()).cast(dtype.clone())
            }
        })
        .collect();
    lf.select(exprs)
}

/// A collected frame as a table: nested values as the in-memory path's compact text,
/// the step's column descriptions.
fn finish(
    mut pdf: polars::prelude::DataFrame,
    shell: &DataFrame,
    nested: &[String],
) -> Result<DataFrame, String> {
    for name in nested {
        if let Ok(column) = pdf.column(name) {
            let series = column.as_materialized_series();
            let texts: Vec<Option<String>> = (0..series.len())
                .map(|r| series.get(r).ok().and_then(|v| compact(&v)))
                .collect();
            let replaced = Series::new(name.as_str().into(), texts);
            pdf.replace(name, replaced.into())
                .map_err(|e| e.to_string())?;
        }
    }
    let mut df = DataFrame::from_parts(pdf, shell.columns.clone());
    df.calc_widths(40, 1000);
    Ok(df)
}

/// A nested value the way a JSON document's table shows it: `{2} lat=1.5 city=X`,
/// `[2] a; b`.  A field Polars holds as NULL — absent from that row — is left out.
fn compact(v: &AnyValue) -> Option<String> {
    let mut out = String::new();
    write_compact(v, &mut out);
    if matches!(v, AnyValue::Null) {
        None
    } else {
        Some(out)
    }
}

fn write_compact(v: &AnyValue, out: &mut String) {
    match v {
        AnyValue::Null => {}
        AnyValue::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        AnyValue::String(s) => out.push_str(s),
        AnyValue::StringOwned(s) => out.push_str(s),
        AnyValue::Float64(f) => out.push_str(&crate::data::doc::fmt_float(*f)),
        AnyValue::Float32(f) => out.push_str(&crate::data::doc::fmt_float(*f as f64)),
        AnyValue::List(items) => {
            out.push_str(&format!("[{}]", items.len()));
            for (i, item) in items.iter().enumerate() {
                out.push_str(if i == 0 { " " } else { "; " });
                write_compact(&item, out);
            }
        }
        AnyValue::Struct(_, _, fields) => {
            let values: Vec<AnyValue> = v._iter_struct_av().collect();
            let present: Vec<(&Field, &AnyValue)> = fields
                .iter()
                .zip(values.iter())
                .filter(|(_, v)| !matches!(v, AnyValue::Null))
                .collect();
            out.push_str(&format!("{{{}}}", present.len()));
            for (field, value) in present {
                out.push(' ');
                out.push_str(field.name());
                out.push('=');
                write_compact(value, out);
            }
        }
        other => out.push_str(&other.to_string()),
    }
}

/// Refuse to collect what will not fit, saying how big it is and what streams.
enum Guard {
    Refused(String),
    Read(String),
}

fn guard(lf: &LazyFrame, step: &str, budget: u64) -> Result<(), Guard> {
    let counted = crate::data::collect(lf.clone().select([len().alias("n")]))
        .map_err(|e| Guard::Read(e.to_string()))?;
    let rows = counted
        .column("n")
        .ok()
        .and_then(|c| c.get(0).ok())
        .and_then(|v| v.extract::<u64>())
        .unwrap_or(0);
    let sample =
        crate::data::collect(lf.clone().limit(1000)).map_err(|e| Guard::Read(e.to_string()))?;
    let mut row_bytes: u64 = 0;
    for column in sample.columns() {
        row_bytes += match column.dtype() {
            DataType::String => {
                let total: u64 = column
                    .str()
                    .map(|ca| ca.into_iter().flatten().map(|s| s.len() as u64).sum())
                    .unwrap_or(0);
                total / sample.height().max(1) as u64 + 16
            }
            DataType::Struct(_) | DataType::List(_) => 64,
            DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
            DataType::Int16 | DataType::UInt16 => 2,
            DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date => 4,
            _ => 8,
        };
    }
    let need = rows.saturating_mul(row_bytes);
    if need <= budget {
        return Ok(());
    }
    Err(Guard::Refused(format!(
        "'{}' needs the whole table in memory: {} rows, about {} — more than this server may \
         hold ({}). What streams without holding the rows: filter, select, compute, sort, \
         limit, unpivot, group_by, aggregate, frequency. Narrow the rows or columns first, \
         or put a group_by before it.",
        step,
        rows,
        human(need),
        human(budget)
    )))
}

fn op_name(op: &Op) -> &'static str {
    match op {
        Op::Filter(_) => "filter",
        Op::Select(_) => "select",
        Op::Sort(_) => "sort",
        Op::Compute { .. } => "compute",
        Op::GroupBy { .. } => "group_by",
        Op::Frequency { .. } => "frequency",
        Op::Pivot { .. } => "pivot",
        Op::Aggregate(_) => "aggregate",
        Op::Dedup { .. } => "dedup",
        Op::Duplicates { .. } => "duplicates",
        Op::Window(_) => "window",
        Op::Sample { .. } => "sample",
        Op::Transpose { .. } => "transpose",
        Op::Join(_) => "join",
        Op::Limit(_) => "limit",
        Op::ExpandJson { .. } => "expand_json",
        Op::Unpivot(_) => "unpivot",
    }
}

fn human(bytes: u64) -> String {
    let b = bytes as f64;
    if b >= 1e9 {
        format!("{:.1} GB", b / 1e9)
    } else if b >= 1e6 {
        format!("{:.1} MB", b / 1e6)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Half the machine's physical memory: what a pipeline may collect by default.
pub fn default_budget() -> u64 {
    #[cfg(unix)]
    {
        // SAFETY: sysconf reads a system constant and has no preconditions.
        let (pages, size) = unsafe {
            (
                libc::sysconf(libc::_SC_PHYS_PAGES),
                libc::sysconf(libc::_SC_PAGESIZE),
            )
        };
        if pages > 0 && size > 0 {
            return (pages as u64).saturating_mul(size as u64) / 2;
        }
    }
    8 * 1024 * 1024 * 1024
}
