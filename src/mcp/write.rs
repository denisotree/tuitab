//! Changing a database table from the server, in two steps.
//!
//! The terminal guards a write with a popup showing every statement and waiting for a
//! keypress.  A model cannot be shown a popup, so it gets the same guarantee in a shape
//! it can honour: [`plan`] works out exactly what would run and returns it without
//! touching the file, and [`apply`] executes that plan and nothing else.  Neither tool
//! exists unless the human who started the server passed `--mcp-write`.
//!
//! Everything under the handshake is the terminal's own machinery — the frame is loaded
//! with its `TableSource`, mutated with the same `DataFrame` methods a keypress uses,
//! and handed to `build_plan`/`apply`.  There is no second engine and no second set of
//! refusals, so a change the terminal would not make is not one the server can make
//! either.

use super::{render, source, Server};
use crate::data::dataframe::{DataFrame, NULL_INPUT};
use crate::data::io::db_write::{self, TableSource, WritePlan};
use crate::mcp::tools::CallError;
use serde_json::{json, Value};

/// Statements shown back in phase one, and the most a caller may ask for.  The plan
/// holds every value twice over — once bound, once spelled out for reading — so a change
/// over a large table would otherwise answer with megabytes of SQL.  The ceiling is what
/// keeps that true when the caller does the asking.
const DEFAULT_SHOWN: usize = 20;
const MAX_SHOWN: usize = 200;

/// Rows a single call may insert.
const MAX_INSERT: usize = 1000;

/// A plan waiting for its second call.
pub struct Pending {
    pub id: String,
    pub work: Work,
    pub container: String,
}

/// What a waiting plan would do.
///
/// One slot holds both kinds, because the invariant is about the conversation and not
/// about the target: exactly one plan is applicable, whichever tool made it.
pub enum Work {
    /// Statements against a table — a change, or the wholesale replacement of one.
    Table { plan: WritePlan, src: TableSource },
    /// A file that would be overwritten by a computed result.  The rows are held here
    /// because they are the plan: there is no SQL to keep instead.
    File {
        path: std::path::PathBuf,
        df: DataFrame,
    },
}

/// What the caller asked to change.
enum Change {
    Set(serde_json::Map<String, Value>),
    Delete,
    Insert(Vec<Value>),
    Alter(Value),
}

fn one_change(args: &Value) -> Result<Change, CallError> {
    let mut found = Vec::new();
    if args.get("set").is_some() {
        found.push("set");
    }
    if args.get("delete").and_then(Value::as_bool) == Some(true) {
        found.push("delete");
    }
    if args.get("insert").is_some() {
        found.push("insert");
    }
    if args.get("alter").is_some() {
        found.push("alter");
    }
    match found.as_slice() {
        [] => Err(CallError::Failed(
            "Give one of 'set', 'delete', 'insert' or 'alter'.".to_string(),
        )),
        ["set"] => args
            .get("set")
            .and_then(Value::as_object)
            .cloned()
            .map(Change::Set)
            .ok_or_else(|| CallError::Failed("'set' takes an object of column to value".into())),
        ["delete"] => Ok(Change::Delete),
        ["insert"] => args
            .get("insert")
            .and_then(Value::as_array)
            .cloned()
            .map(Change::Insert)
            .ok_or_else(|| CallError::Failed("'insert' takes an array of row objects".into())),
        ["alter"] => Ok(Change::Alter(
            args.get("alter").cloned().unwrap_or_default(),
        )),
        many => Err(CallError::Failed(format!(
            "Give one change per call, not {}.",
            many.join(" and ")
        ))),
    }
}

/// A JSON value as the cell text the frame stores.  `null` is the one that matters:
/// it becomes the sentinel `set_cell` already reads as SQL NULL.
fn cell_text(v: &Value) -> String {
    match v {
        Value::Null => NULL_INPUT.to_string(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn column_index(df: &DataFrame, name: &str) -> Result<usize, CallError> {
    df.column_index(name).map_err(CallError::Failed)
}

/// Work out what a change would do, and say so without doing it.
pub fn plan(server: &mut Server, args: &Value) -> Result<Value, CallError> {
    // Taken first, and unconditionally: a second call means the model has moved on, and
    // leaving the old plan applicable through a *failed* second call is how
    // `tuitab_write_apply` ends up running something nobody is looking at any more.
    let displaced = server.pending.take().map(|p| p.id);
    let src_arg = super::tools::source_arg(args)?;
    if !db_write::is_db_path(&src_arg.path) {
        return Err(CallError::Failed(
            "tuitab_write changes a table in a database. Give a .sqlite or .duckdb source."
                .to_string(),
        ));
    }
    let Some(container) = src_arg.container.clone() else {
        return Err(CallError::Failed(
            "Give the table to change as 'container'. tuitab_inspect lists them.".to_string(),
        ));
    };

    // Loaded fresh, never from the cache: the cache holds no `TableSource`, and a
    // snapshot reused across two writes in one session would trip the drift check on
    // the model's own first write.
    let (mut df, source) = source::load_db_table(&src_arg.path, &container)?;
    let Some(source) = source else {
        return Err(CallError::Failed(format!(
            "'{}' is a view, or has no row identity of its own, so it cannot be written to. \
             Views can be read but not changed.",
            container
        )));
    };

    let change = one_change(args)?;
    let clauses = super::tools::where_arg(args)?;
    // Emptying a table is not something a missing argument should be able to ask for,
    // and unlike a replaced table it does not announce itself in the plan as a DROP.
    if matches!(change, Change::Delete) && clauses.is_empty() {
        return Err(CallError::Failed(
            "'delete' needs a 'where'. Deleting every row is not something this tool will \
             plan for you."
                .to_string(),
        ));
    }
    let matched = if clauses.is_empty() {
        (0..df.df.height()).collect::<Vec<_>>()
    } else {
        crate::data::filter::matching_rows(&df, &clauses).map_err(CallError::Failed)?
    };

    // The rows as they stand, captured before anything is changed.  A model reading
    // these can see a mis-aimed 'where' for what it is; rowids in the SQL cannot be
    // checked against anything.
    let preview_rows = args
        .get("preview_rows")
        .and_then(Value::as_u64)
        .unwrap_or(10)
        .min(100) as usize;
    let mut before = df.clone();
    before.row_order = std::sync::Arc::new(matched.clone());
    let affected = render::table(&before, preview_rows).map_err(CallError::Failed)?;

    let wanted_schema = matches!(change, Change::Alter(_));
    apply_change(&mut df, &source, change, &matched)?;

    // Verbatim, here and in `apply`: these are tuitab's own refusals, written to be
    // read by a person, and wrapping them would bury the sentence that matters.
    let plan = db_write::build_plan(&source, &df).map_err(|e| CallError::Failed(e.to_string()))?;
    if !wanted_schema && plan.schema > 0 {
        return Err(CallError::Failed(
            "That change would alter the table's shape, which was not what was asked for."
                .to_string(),
        ));
    }
    if plan.is_empty() {
        return Ok(json!({
            "summary": "no change",
            "rows_matched": matched.len(),
            "statements": [],
            "warnings": plan.warnings,
            "note": note_with_displaced(
                "Nothing would change, so there is no plan to apply.",
                displaced.as_deref(),
            ),
        }));
    }

    server.plan_seq += 1;
    let id = format!("write-{}", server.plan_seq);
    let show = args
        .get("show_statements")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_SHOWN as u64)
        .clamp(1, MAX_SHOWN as u64) as usize;
    let shown: Vec<&str> = plan
        .stmts
        .iter()
        .take(show)
        // Past `DISPLAY_CAP` the plan keeps no readable text at all, so an empty one is
        // a statement that exists and cannot be shown, not a statement that is blank.
        .filter(|s| !s.display.is_empty())
        .map(|s| s.display.as_str())
        .collect();
    let out = json!({
        "plan_id": id,
        "summary": plan.summary(),
        "schema": plan.schema,
        "updates": plan.updates,
        "inserts": plan.inserts,
        "deletes": plan.deletes,
        "rows_matched": matched.len(),
        "statements": shown,
        "statements_total": plan.stmts.len(),
        "statements_not_shown": plan.stmts.len().saturating_sub(shown.len()),
        "warnings": plan.warnings,
        "affected_rows": affected,
        "note": note_with_displaced(
            &format!(
                "Nothing has been written. Read these statements back to the user, then \
                 call tuitab_write_apply with plan_id '{}' to run exactly them.",
                id
            ),
            displaced.as_deref(),
        ),
    });
    server.pending = Some(Pending {
        id,
        work: Work::Table { plan, src: source },
        container,
    });
    Ok(out)
}

/// Plan the replacement of a whole table by a query's result, and write nothing.
///
/// `output.overwrite` destroys more than any `set` ever does — the previous table
/// entire, and with it the indexes, triggers and views that hung off it — so it goes
/// through the same handshake: the statements exist, the caller reads them back, and
/// `tuitab_write_apply` runs precisely them.  There is no human at the keyboard to
/// catch a model that answered a refusal by re-sending it with the flag set; a second
/// deliberate call is the only thing that does not depend on the model behaving.
pub fn plan_replacement(
    server: &mut Server,
    df: &DataFrame,
    path: &std::path::Path,
    table: &str,
) -> Result<Value, CallError> {
    let displaced = server.pending.take().map(|p| p.id);
    let kind = db_write::kind_for_path(path);
    // What is about to be lost, read before anything is planned: the count is the whole
    // point of the sentence the model has to relay.
    let losing = crate::data::io::db_containers(path)
        .ok()
        .and_then(|cs| cs.into_iter().find(|c| c.name == table))
        .and_then(|c| c.rows);
    let (plan, src) = db_write::create_plan(kind, path, table, df)
        .map_err(|e| CallError::Failed(e.to_string()))?;

    server.plan_seq += 1;
    let id = format!("write-{}", server.plan_seq);
    let shown: Vec<&str> = plan
        .stmts
        .iter()
        .take(DEFAULT_SHOWN)
        .filter(|s| !s.display.is_empty())
        .map(|s| s.display.as_str())
        .collect();
    let out = json!({
        "plan_id": id,
        "summary": plan.summary(),
        "replaces": {
            "table": table,
            "path": path.to_string_lossy(),
            "rows_now": losing,
            "rows_after": df.row_order.len(),
        },
        "schema": plan.schema,
        "inserts": plan.inserts,
        "statements": shown,
        "statements_total": plan.stmts.len(),
        "statements_not_shown": plan.stmts.len().saturating_sub(shown.len()),
        "warnings": plan.warnings,
        "note": note_with_displaced(
            &format!(
                "Nothing has been written. Replacing '{}' drops the table that is there \
                 now. Tell the user what it holds, then call tuitab_write_apply with \
                 plan_id '{}' to run exactly these statements.",
                table, id
            ),
            displaced.as_deref(),
        ),
    });
    server.pending = Some(Pending {
        id,
        work: Work::Table { plan, src },
        container: table.to_string(),
    });
    Ok(out)
}

/// Plan the overwriting of a file that already exists, and write nothing.
///
/// A report somebody has is no less theirs than a table, and `output.overwrite` used to
/// replace one in a single call — the file is gone before anybody could be told what it
/// was.  Same handshake, same reason: with no person at the keyboard, a second
/// deliberate call is the only gate a model cannot talk itself through.
pub fn plan_file_overwrite(
    server: &mut Server,
    df: &DataFrame,
    path: &std::path::Path,
    sheet: &str,
) -> Result<Value, CallError> {
    let displaced = server.pending.take().map(|p| p.id);
    let meta = std::fs::metadata(path).ok();
    let bytes = meta.as_ref().map(|m| m.len());
    let modified = meta
        .as_ref()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs());

    server.plan_seq += 1;
    let id = format!("write-{}", server.plan_seq);
    let out = json!({
        "plan_id": id,
        "summary": format!("overwrite {}", path.display()),
        "replaces": {
            "path": path.to_string_lossy(),
            "bytes_now": bytes,
            "modified_unix": modified,
            "rows_after": df.row_order.len(),
        },
        "columns": render::columns_json(df),
        "note": note_with_displaced(
            &format!(
                "Nothing has been written. {} exists and would be replaced entirely. \
                 Tell the user what is about to be overwritten, then call \
                 tuitab_write_apply with plan_id '{}' to write it.",
                path.display(),
                id
            ),
            displaced.as_deref(),
        ),
    });
    server.pending = Some(Pending {
        id,
        work: Work::File {
            path: path.to_path_buf(),
            df: df.clone(),
        },
        container: sheet.to_string(),
    });
    Ok(out)
}

/// Say that an earlier plan is gone, when there was one.
///
/// A model that planned twice has to know the first plan is no longer applicable, or it
/// will read a refusal from `tuitab_write_apply` as a bug rather than as the answer.
fn note_with_displaced(note: &str, displaced: Option<&str>) -> String {
    match displaced {
        Some(id) => format!(
            "{} Plan '{}' was replaced and can no longer be applied.",
            note, id
        ),
        None => note.to_string(),
    }
}

/// Turn the requested change into frame mutations, which `build_plan` then reads.
fn apply_change(
    df: &mut DataFrame,
    src: &TableSource,
    change: Change,
    matched: &[usize],
) -> Result<(), CallError> {
    match change {
        Change::Set(values) => {
            if values.is_empty() {
                return Err(CallError::Failed("'set' needs at least one column".into()));
            }
            let rows: std::collections::HashSet<usize> = matched.iter().copied().collect();
            for (name, value) in values {
                let col = column_index(df, &name)?;
                df.set_cells_bulk(&rows, col, cell_text(&value))
                    .map_err(CallError::Failed)?;
            }
        }
        Change::Delete => df.record_deleted_rows(matched.to_vec()),
        Change::Insert(rows) => {
            if rows.is_empty() {
                return Err(CallError::Failed("'insert' needs at least one row".into()));
            }
            if rows.len() > MAX_INSERT {
                return Err(CallError::Failed(format!(
                    "'insert' takes at most {} rows per call; you gave {}.",
                    MAX_INSERT,
                    rows.len()
                )));
            }
            insert_rows(df, src, &rows)?;
        }
        Change::Alter(spec) => alter(df, &spec)?,
    }
    Ok(())
}

/// Append the given rows in one go.
///
/// Building the whole block and stacking it once, rather than a row at a time through
/// `insert_empty_row`, because the latter rebuilds every column per call.
fn insert_rows(df: &mut DataFrame, src: &TableSource, rows: &[Value]) -> Result<(), CallError> {
    use polars::prelude::{Column, NamedFrom, Series};

    // A column the table insists on and does not default would fail inside the
    // transaction; naming it here is a sentence instead of an engine error.
    for col in &src.columns {
        if col.notnull && col.default_sql.is_none() && !col.generated {
            let missing = rows
                .iter()
                .any(|r| r.get(&col.name).map(|v| v.is_null()).unwrap_or(true));
            if missing {
                return Err(CallError::Failed(format!(
                    "Column '{}' is NOT NULL and has no default, so every new row must give \
                     it a value.",
                    col.name
                )));
            }
        }
    }

    let mut series_vec: Vec<Column> = Vec::with_capacity(df.columns.len());
    for (i, meta) in df.columns.iter().enumerate() {
        let values: Vec<Option<String>> = rows
            .iter()
            .map(|r| match r.get(&meta.name) {
                // A column left out is NULL, not the column's DEFAULT: the INSERT names
                // every column, so a default never gets the chance to apply.
                None | Some(Value::Null) => None,
                Some(v) => Some(cell_text(v)),
            })
            .collect();
        let series = Series::new(meta.name.as_str().into(), values);
        let target = df.df.columns()[i].dtype();
        let series = series.strict_cast(target).map_err(|_| {
            CallError::Failed(format!(
                "Column '{}' holds {}, and one of the values given is not",
                meta.name, target
            ))
        })?;
        series_vec.push(series.into());
    }

    let block = polars::prelude::DataFrame::new(rows.len(), series_vec)
        .map_err(|e| CallError::Failed(format!("Could not build the rows to insert: {}", e)))?;
    let first = df.df.height();
    df.df
        .vstack_mut(&block)
        .map_err(|e| CallError::Failed(format!("Could not add the rows to the table: {}", e)))?;
    for i in 0..rows.len() {
        std::sync::Arc::make_mut(&mut df.row_order).push(first + i);
        std::sync::Arc::make_mut(&mut df.original_order).push(first + i);
    }
    df.record_added_rows(rows.len());
    df.modified = true;
    Ok(())
}

/// Add, drop and rename columns, through the same frame operations the terminal uses.
fn alter(df: &mut DataFrame, spec: &Value) -> Result<(), CallError> {
    use crate::types::ColumnType;

    // Anything not read below would leave the plan empty and the answer "no change",
    // which reads as "the table already looks like that" rather than "that word means
    // nothing here".  A typo has to fail loudly or it fails silently.
    let Some(keys) = spec.as_object() else {
        return Err(CallError::Failed(
            "'alter' takes an object with 'add', 'drop' or 'rename'.".to_string(),
        ));
    };
    if keys.is_empty() {
        return Err(CallError::Failed(
            "'alter' needs one of 'add', 'drop' or 'rename'.".to_string(),
        ));
    }
    for key in keys.keys() {
        if !matches!(key.as_str(), "add" | "drop" | "rename") {
            return Err(CallError::Failed(format!(
                "'{}' is not something alter does. It takes 'add', 'drop' and 'rename'; \
                 changing an existing column's type and reordering columns are only in \
                 the terminal.",
                key
            )));
        }
    }

    if let Some(renames) = spec.get("rename").and_then(Value::as_object) {
        for (from, to) in renames {
            let col = column_index(df, from)?;
            let to = to
                .as_str()
                .ok_or_else(|| CallError::Failed("'rename' maps old name to new name".into()))?;
            df.rename_column(col, to).map_err(CallError::Failed)?;
        }
    }
    if let Some(drops) = spec.get("drop").and_then(Value::as_array) {
        for name in drops {
            let name = name
                .as_str()
                .ok_or_else(|| CallError::Failed("'drop' takes column names".into()))?;
            let col = column_index(df, name)?;
            df.drop_column(col).map_err(CallError::Failed)?;
        }
    }
    if let Some(adds) = spec.get("add").and_then(Value::as_array) {
        for add in adds {
            let name = add
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| CallError::Failed("every 'add' needs a 'name'".into()))?;
            let type_name = add
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("string")
                .to_ascii_lowercase();
            let col_type = *ColumnType::all()
                .iter()
                .find(|t| t.name() == type_name)
                .ok_or_else(|| {
                    CallError::Failed(format!(
                        "'{}' is not a column type. Use string, integer, float, boolean, date \
                         or datetime.",
                        type_name
                    ))
                })?;
            // Percentage, currency and file size are ways of *showing* a number — a
            // percentage is stored divided by 100 — so they are not types a table can
            // be given.
            if matches!(
                col_type,
                ColumnType::Percentage | ColumnType::Currency | ColumnType::FileSize
            ) {
                return Err(CallError::Failed(format!(
                    "'{}' is a display format, not a storage type.",
                    type_name
                )));
            }
            let at = df.col_count();
            df.insert_empty_column(at, name)
                .map_err(CallError::Failed)?;
            if col_type != ColumnType::String {
                // `insert_empty_column` fills with empty strings, which is right for a
                // column someone is about to type into and wrong for one the database
                // is about to create: a new column holds nothing, and nothing is NULL.
                let col = column_index(df, name)?;
                let dtype = polars_dtype(col_type);
                let nulls = polars::prelude::Series::full_null(name.into(), df.df.height(), &dtype);
                df.df.with_column(nulls.into()).map_err(|e| {
                    CallError::Failed(format!("Could not add the column '{}': {}", name, e))
                })?;
                df.columns[col].col_type = col_type;
            }
        }
    }
    Ok(())
}

/// The polars dtype behind a column type, for a column being created empty.
fn polars_dtype(t: crate::types::ColumnType) -> polars::prelude::DataType {
    use crate::types::ColumnType as C;
    use polars::prelude::DataType as D;
    match t {
        C::Integer | C::FileSize => D::Int64,
        C::Float | C::Percentage | C::Currency => D::Float64,
        C::Boolean => D::Boolean,
        _ => D::String,
    }
}

/// Run a plan made by [`plan`], and nothing else.
pub fn apply(server: &mut Server, args: &Value) -> Result<Value, CallError> {
    let wanted = args
        .get("plan_id")
        .and_then(Value::as_str)
        .ok_or_else(|| CallError::Failed("tuitab_write_apply needs a 'plan_id'".to_string()))?;

    // Taken before the write: a plan that failed is invalid by definition — after a
    // drift error the data it was made against is gone — so it must not be retryable.
    let pending = match server.pending.take() {
        Some(p) if p.id == wanted => p,
        Some(other) => {
            let id = other.id.clone();
            server.pending = Some(other);
            return Err(CallError::Failed(format!(
                "No plan '{}'. The plan waiting to be applied is '{}'.",
                wanted, id
            )));
        }
        None => {
            // A plan this server once handed out is a different situation from one it
            // never made: it was superseded by a later plan or spent by a write, and
            // "call tuitab_write to make one" reads as if it had never existed.  The id
            // is a counter, so no bookkeeping is needed to tell the two apart.
            let issued = wanted
                .strip_prefix("write-")
                .and_then(|n| n.parse::<u64>().ok())
                .is_some_and(|n| (1..=server.plan_seq).contains(&n));
            return Err(CallError::Failed(if issued {
                format!(
                    "Plan '{}' is no longer valid: a later plan or a write that has \
                     already run replaced it. Call tuitab_write again for a plan against \
                     the table as it stands now.",
                    wanted
                )
            } else {
                format!(
                    "No plan '{}' is waiting. Call tuitab_write to make one.",
                    wanted
                )
            }));
        }
    };

    let out = match &pending.work {
        Work::Table { plan, src } => {
            db_write::apply(src, plan).map_err(|e| CallError::Failed(e.to_string()))?;
            json!({
                "applied": true,
                "path": src.db_path.to_string_lossy(),
                "container": pending.container,
                "summary": plan.summary(),
                "schema": plan.schema,
                "updates": plan.updates,
                "inserts": plan.inserts,
                "deletes": plan.deletes,
            })
        }
        Work::File { path, df } => {
            crate::data::io::save_file_as(
                df,
                None,
                path,
                crate::data::io::doc_io::Shape::Records,
                &pending.container,
            )
            .map_err(|e| CallError::Failed(format!("Could not write {}: {}", path.display(), e)))?;
            json!({
                "applied": true,
                "written": path.to_string_lossy(),
                "row_count": df.row_order.len(),
                "summary": format!("overwrote {}", path.display()),
            })
        }
    };
    // The cached frame predates the write; leaving it would let the next inspect answer
    // with values that are no longer there.
    server.cache.clear();

    Ok(out)
}
