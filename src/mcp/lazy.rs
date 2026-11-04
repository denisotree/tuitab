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
    /// JSON Lines columns holding nested values, which the in-memory path holds as
    /// their JSON text; they are written out once collected, and an operation that
    /// reads one is run in memory.
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
    let pattern = crate::data::io::is_pattern(&src.path);
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
    // A `.json` is scanned when it holds JSON Lines; whether it does is its first file's to say.
    let named_json = ext == "json" && src.format.is_none();
    let lines = named_json || matches!(ext.as_str(), "jsonl" | "ndjson" | "ldjson");
    let table = matches!(
        ext.as_str(),
        "csv" | "tsv" | "parquet" | "arrow" | "feather" | "ipc"
    );
    if !lines && !table {
        return Ok(None);
    }
    let files = if pattern {
        match crate::data::io::glob_matches(&text) {
            Ok(files) if !files.is_empty() => files,
            _ => return Ok(None),
        }
    } else {
        vec![src.path.clone()]
    };
    if named_json
        && crate::data::doc::Format::of_file(&files[0]) != Some(crate::data::doc::Format::Jsonl)
    {
        return Ok(None);
    }
    // One scan reads every file with one separator; files that differ are each read by
    // their own in memory.
    if pattern && matches!(ext.as_str(), "csv" | "tsv") {
        let sep = |f: &Path| crate::data::loader::separator(&src.opts, &ext, f);
        let first_sep = sep(&files[0]);
        if files.iter().any(|f| sep(f) != first_sep) {
            return Ok(None);
        }
    }
    let first = files[0].clone();
    let key = profile_key(src, &files);
    // A pattern's schema, when its files agree on one: the CSV scan takes it rather
    // than inferring it a second time.
    let pattern_schema = if pattern {
        // A pattern reads files that hold one table.  One whose columns or their types
        // differ — each typed from all its rows, as the in-memory reader types it — is
        // the in-memory path's to name, as it always has.
        let schema_of = |file: &Path| -> Option<Schema> {
            let one = PlRefPath::try_from_path(file).ok()?;
            let mut lf = match ext.as_str() {
                "csv" | "tsv" => scan_csv(src, one, file, None).ok()?,
                "parquet" => LazyFrame::scan_parquet(one, ScanArgsParquet::default()).ok()?,
                "arrow" | "feather" | "ipc" => {
                    LazyFrame::scan_ipc(one, Default::default(), Default::default()).ok()?
                }
                _ => LazyJsonLineReader::new(one)
                    .with_infer_schema_length(None)
                    .finish()
                    .ok()?,
            };
            Some((*lf.collect_schema().ok()?).clone())
        };
        let agreed = cached(&PATTERN_SCHEMAS, &key, || {
            use rayon::prelude::*;
            let schemas: Vec<Option<Schema>> = files
                .par_iter()
                .map(|f| {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| schema_of(f)))
                        .ok()
                        .flatten()
                })
                .collect();
            let head = schemas[0].clone();
            Ok(head.filter(|_| schemas[1..].iter().all(|s| *s == schemas[0])))
        })?;
        let Some(schema) = agreed else {
            return Ok(None);
        };
        Some(schema)
    } else {
        None
    };

    // A read error names the file, the way the in-memory path's does.
    let reading = match src.format.as_deref() {
        Some(declared) if crate::data::doc::Format::from_name(declared).is_none() => {
            format!("Could not read {} as {}", src.path.display(), ext)
        }
        _ => format!("Could not read {}", src.path.display()),
    };
    let named = |e: String| format!("{}: {}", reading, e);
    // The JSON Lines scan always expands its path: a real file whose name holds `[1]`
    // is named with those characters escaped, so it matches only itself.
    let scanned_as = if lines && !pattern {
        literal_glob(&text)
    } else {
        text.clone()
    };
    let path = PlRefPath::try_from_path(Path::new(&scanned_as)).map_err(|e| e.to_string())?;
    // A CSV's and a JSONL file's types are worked out from every row, as the in-memory
    // readers do — one full pass, paid once per version of the file.
    let inferred = |lf: LazyFrame| -> Result<Schema, String> {
        let mut lf = lf;
        Ok((*lf.collect_schema().map_err(|e| e.to_string())?).clone())
    };
    let mut lf = match ext.as_str() {
        "csv" | "tsv" => {
            let schema = match pattern_schema {
                Some(schema) => schema,
                None => cached(&SCHEMAS, &key, || {
                    inferred(scan_csv(src, path.clone(), &first, None)?)
                })
                .map_err(named)?,
            };
            scan_csv(src, path, &first, Some(schema)).map_err(named)?
        }
        "parquet" => LazyFrame::scan_parquet(
            path,
            ScanArgsParquet {
                glob: pattern,
                ..Default::default()
            },
        )
        .map_err(|e| named(e.to_string()))?,
        "arrow" | "feather" | "ipc" => LazyFrame::scan_ipc(
            path,
            Default::default(),
            UnifiedScanArgs {
                glob: pattern,
                ..Default::default()
            },
        )
        .map_err(|e| named(e.to_string()))?,
        _ => {
            // Lines whose fields change shape, arrays per line, numbers past 64 bits:
            // the scan cannot type them (and Polars may panic trying), the in-memory
            // reader can.  The answer is remembered, so the passes are paid once.
            let known = pattern_schema;
            let schema = cached(&LINE_SCHEMAS, &key, || {
                let p = path.clone();
                Ok(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                        let schema = match known {
                            Some(schema) => schema,
                            None => {
                                let lf = LazyJsonLineReader::new(p.clone())
                                    .with_infer_schema_length(None)
                                    .finish()
                                    .ok()?;
                                inferred(lf).ok()?
                            }
                        };
                        // A type that fits every line is not yet a type every value
                        // decodes into: a field that is a number here and an object there
                        // types as the object, then fails.  Decode every column once.
                        let decoded = LazyJsonLineReader::new(p)
                            .with_schema(Some(std::sync::Arc::new(schema.clone())))
                            .finish()
                            .ok()?
                            .select([all().as_expr().null_count()]);
                        crate::data::collect(decoded).ok()?;
                        // And a type is not yet the in-memory reader's: a number past 64
                        // bits is text to Polars and a float to the reader.  The head of
                        // each file, typed both ways, has to agree.
                        if !files.iter().all(|f| head_types_agree(f, &schema)) {
                            return None;
                        }
                        Some(schema)
                    }))
                    .ok()
                    .flatten(),
                )
            })
            .map_err(named)?;
            let Some(schema) = schema else {
                return Ok(None);
            };
            LazyJsonLineReader::new(path)
                .with_schema(Some(std::sync::Arc::new(schema)))
                .finish()
                .map_err(|e| named(e.to_string()))?
        }
    };

    let mut schema = (*lf.collect_schema().map_err(|e| named(e.to_string()))?).clone();

    // A DECIMAL column as the in-memory loader holds it: a float.
    let decimals: Vec<Expr> = schema
        .iter()
        .filter(|(_, t)| matches!(t, DataType::Decimal(..)))
        .map(|(n, _)| col(n.as_str()).cast(DataType::Float64))
        .collect();
    if !decimals.is_empty() {
        lf = lf.with_columns(decimals);
        schema = (*lf.collect_schema().map_err(|e| named(e.to_string()))?).clone();
    }

    // A zoned datetime as the in-memory loader holds it: UTC, the zone set aside.
    let (unzoned, zones) = crate::data::profile::zone_free(&schema);
    if !unzoned.is_empty() {
        lf = lf.with_columns(unzoned);
        schema = (*lf.collect_schema().map_err(|e| named(e.to_string()))?).clone();
    }

    // The in-memory CSV reader turns `undefined` and friends into NULL in a column
    // that is otherwise numbers, and NaN into NULL in a float one; the scan does the
    // same, from one profiling pass.
    if matches!(ext.as_str(), "csv" | "tsv") {
        lf = crate::data::profile::without_nan(lf, &schema);
        let casts = cached(&TOKEN_CASTS, &key, || {
            crate::data::profile::token_casts(&lf, &schema).map_err(|e| e.to_string())
        })
        .map_err(named)?;
        if !casts.is_empty() {
            lf = crate::data::profile::with_casts(lf, &casts);
            schema = (*lf.collect_schema().map_err(|e| named(e.to_string()))?).clone();
        }
    }

    // Epoch columns of a table file, as the in-memory loader shows them.
    let mut epochs: Vec<(String, EpochUnit)> = Vec::new();
    if table && src.opts.detect_epoch {
        epochs = cached(&EPOCHS, &key, || {
            crate::data::profile::epoch_columns(&lf, &schema).map_err(|e| e.to_string())
        })
        .map_err(named)?;
        if !epochs.is_empty() {
            lf = crate::data::profile::with_epochs(lf, &epochs);
            schema = (*lf.collect_schema().map_err(|e| named(e.to_string()))?).clone();
        }
    }

    // A nested JSON Lines value is its JSON text, as the in-memory reader holds it; a
    // Parquet or Arrow struct or list keeps its own type, on both paths alike.
    let nested: Vec<String> = if lines {
        schema
            .iter()
            .filter(|(_, t)| matches!(t, DataType::Struct(_) | DataType::List(_)))
            .map(|(n, _)| n.to_string())
            .collect()
    } else {
        Vec::new()
    };
    let mut shell_schema = schema.clone();
    for name in &nested {
        shell_schema.with_column(name.as_str().into(), DataType::String);
    }
    let empty = polars::prelude::DataFrame::empty_with_schema(&shell_schema);
    let mut shell = crate::data::io::wrap_polars_df(empty).map_err(|e| e.to_string())?;
    for name in &nested {
        if let Ok(i) = shell.column_index(name) {
            shell.columns[i].json = true;
        }
    }
    for (name, unit) in &epochs {
        if let Ok(i) = shell.column_index(name) {
            shell.columns[i].epoch_unit = Some(*unit);
        }
    }
    for (name, zone) in zones {
        if let Ok(i) = shell.column_index(&name) {
            shell.columns[i].time_zone = Some(zone);
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
    let sep = crate::data::loader::separator(&src.opts, &super::source::extension_of(src), first);
    let mut reader = crate::data::loader::csv_reader(path, sep, &src.opts);
    if let Some(schema) = schema {
        reader = reader.with_schema(Some(std::sync::Arc::new(schema)));
    }
    reader.finish().map_err(|e| e.to_string())
}

// ── Profiling, cached per file version ──────────────────────────────────────────────

type Key = String;

/// Per file version: what one profiling pass found.
type Store<T> = OnceLock<Mutex<HashMap<Key, T>>>;

/// Profiles kept per store; past that the store starts over.
const PROFILES_KEPT: usize = 64;

static SCHEMAS: Store<Schema> = OnceLock::new();
static PATTERN_SCHEMAS: Store<Option<Schema>> = OnceLock::new();
static LINE_SCHEMAS: Store<Option<Schema>> = OnceLock::new();
static TOKEN_CASTS: Store<Vec<(String, DataType)>> = OnceLock::new();
static EPOCHS: Store<Vec<(String, EpochUnit)>> = OnceLock::new();

/// Whether the first lines of a JSON Lines file get, from the in-memory reader, the
/// types `schema` gives them — its nested and empty columns aside, which the two hold
/// in their own ways.
fn head_types_agree(file: &Path, schema: &Schema) -> bool {
    use std::io::Read;
    let mut head = Vec::new();
    if std::fs::File::open(file)
        .and_then(|f| f.take(1 << 20).read_to_end(&mut head))
        .is_err()
    {
        return false;
    }
    if let Some(end) = head.iter().rposition(|&b| b == b'\n') {
        head.truncate(end + 1);
    }
    let Ok(text) = std::str::from_utf8(&head) else {
        return false;
    };
    let Ok(table) = crate::data::doc::jsonl_records(text, 0)
        .map_err(|e| e.to_string())
        .and_then(|rows| crate::data::view::records_table(&rows).map_err(|e| e.to_string()))
    else {
        return false;
    };
    table
        .columns
        .iter()
        .zip(table.df.columns())
        .all(|(meta, column)| {
            if meta.json || column.null_count() == column.len() {
                return true;
            }
            match (schema.get(meta.name.as_str()), column.dtype()) {
                (Some(DataType::Struct(_) | DataType::List(_)), _) => true,
                // Whole numbers at the head, a decimal further on: the whole file is
                // floats to the in-memory reader as well.
                (Some(DataType::Float64), DataType::Int64) => true,
                (Some(dtype), head) => dtype == head,
                (None, _) => true,
            }
        })
}

/// A file's name as a glob pattern that matches only that file.
fn literal_glob(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '[' | ']' | '*' | '?' => format!("[{}]", c),
            c => c.to_string(),
        })
        .collect()
}

/// Which files, read how, at which version — every file a pattern matches, so one
/// added, removed or rewritten is a new profile.
fn profile_key(src: &Source, files: &[std::path::PathBuf]) -> Option<Key> {
    use std::fmt::Write;
    let mut key = format!("{}|{:?}", super::source::extension_of(src), src.opts);
    for file in files {
        let meta = std::fs::metadata(file).ok()?;
        let stamp = meta
            .modified()
            .ok()?
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_nanos();
        write!(key, "|{}|{}|{}", file.display(), stamp, meta.len()).ok()?;
    }
    Some(key)
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
        let mut kept = store.lock().unwrap_or_else(|e| e.into_inner());
        // ponytail: starts over when full rather than evicting the oldest; an LRU if
        // a server ever cycles through more files than this.
        if kept.len() >= PROFILES_KEPT {
            kept.clear();
        }
        kept.insert(k.clone(), value.clone());
    }
    Ok(value)
}

// ── Running a pipeline ──────────────────────────────────────────────────────────────

/// Run `ops` over the scan.  `limit` is how many rows the caller will show when the
/// whole pipeline is a plan; `whole` asks for every row, naming what wants them (a
/// file being written, a describe) for the guard to say; `budget` is how many bytes
/// may be collected.
pub fn run(
    scanned: &Scanned,
    ops: &[Op],
    limit: usize,
    whole: Option<&str>,
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
        let failed =
            |e: String| format!("ops[{}]: {}", i, super::pipeline::without_polars_hints(&e));
        // The grouping steps run the in-memory path's own function over the scan;
        // what they produce is small, and the rest of the pipeline runs on it.
        let grouped = match op {
            Op::GroupBy { by, agg } => Some(crate::data::group::group_by_from(
                &shell,
                lf.clone(),
                by,
                agg,
            )),
            Op::Aggregate(agg) => Some(crate::data::group::total_from(&shell, lf.clone(), agg)),
            Op::Frequency { by, agg } => Some(crate::data::group::frequency_from(
                &shell,
                lf.clone(),
                by,
                agg,
            )),
            _ => None,
        };
        if let Some(grouped) = grouped {
            return Ok(Outcome::Collected {
                df: grouped.map_err(failed)?,
                next: i + 1,
            });
        }
        // The in-memory operation on the empty table first: its errors are the ones
        // the caller would have had, and its table says what the step produces.
        let next = super::pipeline::apply_op(shell.clone(), op).map_err(failed)?;
        let Some((lowered, retyped)) = lower(lf.clone(), &shell, op)? else {
            break;
        };
        lf = lowered;
        shell = next;
        // json_get's column took its type from the values, which the empty table has
        // none of.
        if let Some((name, dtype)) = retyped {
            retype_shell(&mut shell, &name, &dtype)?;
        }
        i += 1;
    }

    // The step's columns, in the step's order and types.
    let lf = conform(lf, &shell, &scanned.nested);
    if i == ops.len() && whole.is_none() {
        let read = |e: PolarsError| format!("{}: {}", scanned.reading, e);
        // A long head is a result like any other, held to the budget.
        if limit > 1000 {
            let head = lf
                .clone()
                .limit(IdxSize::try_from(limit).unwrap_or(IdxSize::MAX));
            guard(&head, "output.limit", budget).map_err(|e| match e {
                Guard::Refused(why) => why,
                Guard::Read(why) => format!("{}: {}", scanned.reading, why),
            })?;
        }
        let head = crate::data::collect(
            lf.clone()
                .limit(IdxSize::try_from(limit).unwrap_or(IdxSize::MAX)),
        )
        .map_err(read)?;
        // Fewer rows than were asked for are all the rows there are.
        let row_count = if head.height() < limit {
            head.height()
        } else {
            row_count(&lf).map_err(read)? as usize
        };
        let df = finish(head, &shell, &scanned.nested)?;
        return Ok(Outcome::Head { df, row_count });
    }

    guard(
        &lf,
        ops.get(i)
            .map_or(whole.unwrap_or("collecting the result"), |op| op_name(op)),
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

/// A column of the shell given the type the plan found for it.
fn retype_shell(shell: &mut DataFrame, name: &str, dtype: &DataType) -> Result<(), String> {
    let idx = shell.column_index(name)?;
    let empty = Series::new_empty(name.into(), dtype);
    shell
        .df
        .replace(name, empty.into())
        .map_err(|e| e.to_string())?;
    shell.columns[idx].col_type = crate::data::io::column_type_of(dtype);
    Ok(())
}

/// A step as part of the plan, and the column it gave a type of its own, if any.
type Lowered = (LazyFrame, Option<(String, DataType)>);

/// One step as part of the plan — with the column it retyped, if any — or `None` when
/// it needs the whole table in memory.
fn lower(lf: LazyFrame, shell: &DataFrame, op: &Op) -> Result<Option<Lowered>, String> {
    let schema = shell.df.schema().clone();
    Ok(Some((
        match op {
            Op::Filter(clauses) => {
                let Some(expr) = crate::data::filter::clauses_to_expr(shell, clauses)? else {
                    return Ok(Some((lf, None)));
                };
                match expr.to_polars_expr_with(Some(schema.as_ref())) {
                    Ok(mask) => lf.filter(mask.fill_null(lit(false))),
                    Err(_) => return Ok(None),
                }
            }
            Op::Select(names) => {
                lf.select(names.iter().map(|n| col(n.as_str())).collect::<Vec<_>>())
            }
            Op::Compute { name, expr } => {
                let Ok(parsed) = crate::data::expression::Expr::parse(expr) else {
                    return Ok(None);
                };
                // json_get's type depends on every value: profile the rows the plan has
                // reached, then cast to what the in-memory path would have made of them.
                if matches!(&parsed, crate::data::expression::Expr::FunctionCall { name, .. } if name == "json_get")
                {
                    let Ok(text) = parsed.to_polars_expr_with(Some(schema.as_ref())) else {
                        return Ok(None);
                    };
                    let dtype = crate::data::profile::text_type(&lf, text.clone(), true)
                        .map_err(|e| e.to_string())?;
                    let typed = crate::data::profile::as_type(text, &dtype);
                    return Ok(Some((
                        lf.with_column(typed.alias(name.as_str())),
                        Some((name.clone(), dtype)),
                    )));
                }
                // A date difference is turned into seconds after the fact, and a form
                // Polars rejects for these types (`len` of a number) is computed row by
                // row: both the in-memory path's to finish.  Run on no rows, the form
                // meets its types without reading the file.
                let Ok(e) = parsed.to_polars_expr_with(Some(schema.as_ref())) else {
                    return Ok(None);
                };
                match crate::data::expression::output_type(&e, schema.as_ref()) {
                    Some(t) if !matches!(t, DataType::Duration(_)) => {
                        lf.with_column(e.alias(name.as_str()))
                    }
                    _ => return Ok(None),
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
            Op::Limit(n) => lf.limit(IdxSize::try_from(*n).unwrap_or(IdxSize::MAX)),
            Op::Unpivot(spec) => crate::data::unpivot::unpivot_lazy(lf, schema.as_ref(), spec)?,
            _ => return Ok(None),
        },
        None,
    )))
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
        Op::Filter(clauses) => named(predicate_columns(clauses.iter()), nested),
        Op::Sort(keys) => named(keys.iter().map(|k| &k.col), nested),
        Op::GroupBy { by, agg } | Op::Frequency { by, agg } => {
            named(by.iter(), nested) || named(agg_columns(agg), nested)
        }
        Op::Aggregate(agg) => named(agg_columns(agg), nested),
        // The columns the expression names; one it cannot read is no reason to leave.
        Op::Compute { expr, .. } => crate::data::expression::Expr::parse(expr).map_or(true, |e| {
            e.columns().iter().any(|c| nested.iter().any(|n| n == c))
        }),
        Op::Unpivot(spec) => match &spec.columns {
            Some(columns) => named(spec.index.iter().chain(columns), nested),
            None => true,
        },
        // Anything else may read any column.
        _ => true,
    }
}

/// Every column aggregates read: their own, and those their conditions test.
fn agg_columns(agg: &[crate::data::group::AggSpec]) -> impl Iterator<Item = &String> {
    agg.iter()
        .map(|a| &a.col)
        .chain(predicate_columns(agg.iter().flat_map(|a| a.where_.iter())))
}

/// The columns predicates read: each one's own, and one it is compared with.
fn predicate_columns<'a>(
    clauses: impl Iterator<Item = &'a crate::data::filter::Clause>,
) -> impl Iterator<Item = &'a String> {
    clauses.flat_map(|c| c.predicates()).flat_map(|p| {
        let other = match &p.value {
            crate::data::filter::Operand::Column(c) => Some(c),
            _ => None,
        };
        std::iter::once(&p.col).chain(other)
    })
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

/// A collected frame as a table: nested values as their JSON text, the step's column
/// descriptions.
fn finish(
    mut pdf: polars::prelude::DataFrame,
    shell: &DataFrame,
    nested: &[String],
) -> Result<DataFrame, String> {
    for name in nested {
        if let Ok(column) = pdf.column(name) {
            let series = column.as_materialized_series();
            let texts: Vec<Option<String>> = (0..series.len())
                .map(|r| {
                    series
                        .get(r)
                        .ok()
                        .filter(|v| !matches!(v, AnyValue::Null))
                        .map(|v| json_of(&v).to_string())
                })
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

/// A nested value as JSON, as the in-memory reader writes a record's value
/// (`Node::json_text`).  Polars holds a field a line lacks as NULL, so a NULL field is
/// left out — and an object's fields come in the order of the whole file's.
fn json_of(v: &AnyValue) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        AnyValue::Null => J::Null,
        AnyValue::Boolean(b) => J::Bool(*b),
        AnyValue::String(s) => J::from(*s),
        AnyValue::StringOwned(s) => J::from(s.as_str()),
        AnyValue::Float64(f) => J::from(*f),
        AnyValue::Float32(f) => J::from(*f as f64),
        AnyValue::List(items) => J::Array(items.iter().map(|i| json_of(&i)).collect()),
        AnyValue::Struct(_, _, fields) => J::Object(
            fields
                .iter()
                .zip(v._iter_struct_av())
                .filter(|(_, v)| !matches!(v, AnyValue::Null))
                .map(|(f, v)| (f.name().to_string(), json_of(&v)))
                .collect(),
        ),
        other => match other.extract::<i64>() {
            Some(i) if other.dtype().is_integer() => J::from(i),
            _ => J::from(other.to_string()),
        },
    }
}

/// Refuse to collect what will not fit, saying how big it is and what streams.
enum Guard {
    Refused(String),
    Read(String),
}

/// How many rows the plan yields — one streaming pass.
fn row_count(lf: &LazyFrame) -> PolarsResult<u64> {
    let counted = crate::data::collect(lf.clone().select([len().alias("n")]))?;
    Ok(counted
        .column("n")
        .ok()
        .and_then(|c| c.get(0).ok())
        .and_then(|v| v.extract::<u64>())
        .unwrap_or(0))
}

fn guard(lf: &LazyFrame, step: &str, budget: u64) -> Result<(), Guard> {
    let read = |e: PolarsError| Guard::Read(e.to_string());
    let sample = crate::data::collect(lf.clone().limit(1000)).map_err(read)?;
    // A sample short of its thousand rows is the whole result.
    let rows = if sample.height() < 1000 {
        sample.height() as u64
    } else {
        row_count(lf).map_err(read)?
    };
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
        crate::data::io::format_file_size_pub(need),
        crate::data::io::format_file_size_pub(budget)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A long-running server reading many files keeps a bounded number of profiles.
    #[test]
    fn profiles_are_bounded() {
        static STORE: Store<usize> = OnceLock::new();
        for i in 0..PROFILES_KEPT * 3 {
            cached(&STORE, &Some(format!("file-{}", i)), || Ok(i)).unwrap();
        }
        let kept = STORE.get().unwrap().lock().unwrap().len();
        assert!(kept <= PROFILES_KEPT, "{} profiles kept", kept);
    }
}
