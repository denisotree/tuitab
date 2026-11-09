use crate::data::dataframe::DataFrame;
use crate::data::io::db_write::{DbColumn, DbKind, DbRows, DeclType, TableSource};
use crate::data::io::{wrap_polars_df, ContainerInfo};
use color_eyre::{eyre::eyre, Result};
use polars::prelude::*;
use std::path::Path;

/// Double any `"` so an identifier can go inside `"…"`.
pub(crate) fn quote_ident(name: &str) -> String {
    name.replace('"', "\"\"")
}

/// Read one value the way the loader does: `None` for NULL, and the exact string the
/// rest of tuitab will hold.
///
/// The drift check has to go through this same function.  `CAST(x AS TEXT)` inside
/// SQLite is *not* equivalent: SQLite renders a REAL `1.0` as `"1.0"` while Rust's
/// `f64::to_string` gives `"1"`, so comparing across the two reports drift on every
/// float column of every table.
pub(crate) fn value_to_opt_string(val: rusqlite::types::Value) -> Option<String> {
    use rusqlite::types::Value;
    match val {
        Value::Null => None,
        Value::Integer(i) => Some(i.to_string()),
        Value::Real(f) => Some(f.to_string()),
        Value::Text(s) => Some(s),
        // The length rather than a bare marker: the drift check compares these strings,
        // so carrying the size makes it notice a blob swapped for one of a different
        // size, for free.  A replacement of exactly the same length still slips past —
        // reading the bytes to compare them is not worth what a table of images costs.
        Value::Blob(b) => Some(format!("[BLOB {} bytes]", b.len())),
    }
}

/// Every table and view of the database, with what the catalogue knows about each.
///
/// One query answers both surfaces: the overview sheet below and the MCP container
/// listing.  Views are included — reading one is reading a table.
pub fn sqlite_containers(path: &Path) -> Result<Vec<ContainerInfo>> {
    let conn = crate::data::io::db_write::open_sqlite(path)?;
    let mut stmt = conn.prepare(
        "SELECT name, type, sql FROM sqlite_master \
         WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;

    let mut out = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let view = row.get::<_, String>(1)? == "view";
        let sql: Option<String> = row.get(2)?;

        let mut ps = conn.prepare(&format!("PRAGMA table_info(\"{}\")", quote_ident(&name)))?;
        let mut pr = ps.query([])?;
        let mut columns = 0usize;
        while pr.next()?.is_some() {
            columns += 1;
        }

        let rows_count = if view {
            None
        } else {
            conn.query_row(
                &format!("SELECT COUNT(*) FROM \"{}\"", quote_ident(&name)),
                [],
                |r| r.get::<_, i64>(0),
            )
            .ok()
        };

        out.push(ContainerInfo {
            name,
            view,
            rows: rows_count,
            columns,
            sql,
        });
    }
    Ok(out)
}

pub fn load_sqlite_overview(path: &Path) -> Result<DataFrame> {
    let containers = sqlite_containers(path)?;
    if containers.is_empty() {
        return Err(eyre!("No tables found in SQLite database"));
    }

    let table_names: Vec<String> = containers.iter().map(|c| c.name.clone()).collect();
    let kinds: Vec<&str> = containers
        .iter()
        .map(|c| if c.view { "view" } else { "table" })
        .collect();
    let row_counts: Vec<String> = containers
        .iter()
        .map(|c| c.rows.map(|n| n.to_string()).unwrap_or_default())
        .collect();
    let col_counts: Vec<String> = containers.iter().map(|c| c.columns.to_string()).collect();
    let sql_defs: Vec<String> = containers
        .iter()
        .map(|c| c.sql.clone().unwrap_or_default())
        .collect();

    let series_vec = vec![
        Series::new("Table".into(), &table_names).into(),
        Series::new("Kind".into(), &kinds).into(),
        Series::new("Rows".into(), &row_counts).into(),
        Series::new("Columns".into(), &col_counts).into(),
        Series::new("SQL".into(), &sql_defs).into(),
    ];

    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)?;
    let mut df = wrap_polars_df(pdf)?;

    if df.columns.len() == 5 {
        df.columns[0].width = 30;
        df.columns[1].width = 6;
        df.columns[2].width = 10;
        df.columns[3].width = 10;
        df.columns[4].width = 60;
    }

    Ok(df)
}

pub fn load_sqlite_table_by_name(path: &Path, table_name: &str) -> Result<DataFrame> {
    load_sqlite_table_full(path, table_name).map(|(df, _)| df)
}

/// Load a table together with everything needed to write edits back into it.
///
/// The source comes back `None` when the table cannot be addressed row by row — a
/// `WITHOUT ROWID` table, or anything else whose `rowid` the engine refuses.  Such a
/// sheet still opens; it just cannot be saved in place.
pub fn load_sqlite_table_full(
    path: &Path,
    table_name: &str,
) -> Result<(DataFrame, Option<TableSource>)> {
    let conn = crate::data::io::db_write::open_sqlite(path)?;
    // Not `unwrap_or_default()`: an empty catalogue makes `describe_frame` invent a
    // plain, unconstrained text column for every column in the table, which silently
    // switches off the NOT NULL and DEFAULT checks, the generated-column skip and the
    // declared typing.  A pragma that fails is a real error — and so is one that comes
    // back empty for something that then reads as a table with columns, which is the
    // shape the invented metadata actually took.
    let meta = sqlite_columns(&conn, table_name)?;
    let described = |df: &DataFrame| -> Result<Vec<DbColumn>> {
        if meta.is_empty() && !df.columns.is_empty() {
            return Err(eyre!(
                "The catalogue says nothing about '{}', so its columns cannot be \
                 described. Reopen the database.",
                table_name
            ));
        }
        Ok(describe_frame(df, &meta))
    };

    // rowid first — it addresses a row without relying on a key the user can edit.
    match read_sqlite_table(&conn, table_name, true) {
        Ok((mut df, ids)) => {
            let columns = described(&df)?;
            // Values arrive as text from both engines; give the frame the types the
            // table declares, or a numeric filter has nothing to compare numerically.
            // Before the snapshot, so `original` and the live frame agree on dtype.
            crate::data::io::db_write::apply_declared_types(&mut df, &columns);
            let source = TableSource {
                kind: DbKind::Sqlite,
                db_path: path.to_path_buf(),
                table: table_name.to_string(),
                key_col: "rowid".to_string(),
                columns,
                original: df.df.clone(),
            };
            // Which database column each sheet column *is*.  Renaming leaves this
            // alone, so a later save can tell "renamed" from "dropped and added".
            for c in &mut df.columns {
                c.db_origin = Some(c.name.clone());
            }
            df.db_rows = Some(DbRows::new(ids));
            Ok((df, Some(source)))
        }
        // A view, or a WITHOUT ROWID table: readable, just not addressable row by row.
        // It still gets its declared types — a filter must not depend on which path the
        // same column was read through.
        //
        // Only that one error: a locked file or a revoked permission would otherwise
        // come back as a read-only sheet, and the model would be told the table is a
        // view.  Anything that is not "there is no rowid here" goes up as itself.
        Err(e) if is_missing_rowid(&e) => {
            let (mut df, _) = read_sqlite_table(&conn, table_name, false)?;
            let columns = described(&df)?;
            crate::data::io::db_write::apply_declared_types(&mut df, &columns);
            Ok((df, None))
        }
        Err(e) => Err(e),
    }
}

/// Whether the engine refused the query because this object has no `rowid`.
///
/// A view and a `WITHOUT ROWID` table both answer `no such column: rowid`; every other
/// failure means something else went wrong and must not be read as "read-only".  The
/// whole phrase, not the bare word: a table called `rowid_map` that is not there answers
/// `no such table: rowid_map`, and reading that as "this is a view" is exactly the
/// confusion this narrowing exists to remove.
fn is_missing_rowid(err: &color_eyre::Report) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("no such column: rowid")
}

/// Pair each column of the frame with what the catalogue says about it, inventing a
/// plain text column for anything the catalogue does not describe.
fn describe_frame(df: &DataFrame, meta: &[DbColumn]) -> Vec<DbColumn> {
    df.columns
        .iter()
        .map(|c| {
            meta.iter()
                .find(|m| m.name == c.name)
                .cloned()
                .unwrap_or_else(|| DbColumn {
                    name: c.name.clone(),
                    decl_raw: String::new(),
                    decl: DeclType::Text,
                    notnull: false,
                    pk: false,
                    default_sql: None,
                    generated: false,
                })
        })
        .collect()
}

/// Column metadata from `PRAGMA table_xinfo` — `xinfo` rather than `info` because it
/// is the one that marks `GENERATED ALWAYS` columns (`hidden` 2 or 3), which can be
/// read but never written.
fn sqlite_columns(conn: &rusqlite::Connection, table_name: &str) -> Result<Vec<DbColumn>> {
    let mut stmt = conn.prepare(&format!(
        "PRAGMA table_xinfo(\"{}\")",
        quote_ident(table_name)
    ))?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        let decl_raw: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
        let hidden: i64 = row.get::<_, Option<i64>>(6)?.unwrap_or(0);
        out.push(DbColumn {
            decl: DeclType::from_sql(&decl_raw),
            name,
            decl_raw,
            notnull: row.get::<_, Option<i64>>(3)?.unwrap_or(0) != 0,
            pk: row.get::<_, Option<i64>>(5)?.unwrap_or(0) != 0,
            default_sql: row.get::<_, Option<String>>(4)?,
            generated: hidden == 2 || hidden == 3,
        });
    }
    Ok(out)
}

pub fn sqlite_table_names(path: &Path) -> Result<Vec<String>> {
    Ok(sqlite_containers(path)?
        .into_iter()
        .map(|c| c.name)
        .collect())
}

/// Read a whole table.
///
/// With `with_rowid` the query asks for the row identifier as an extra leading column
/// under an alias, so a user column genuinely named `rowid` cannot collide with it.
/// That column never enters the frame — there is nothing to hide in the UI.
fn read_sqlite_table(
    conn: &rusqlite::Connection,
    table_name: &str,
    with_rowid: bool,
) -> Result<(DataFrame, Vec<Option<i64>>)> {
    let table = quote_ident(table_name);
    let query = if with_rowid {
        format!("SELECT rowid AS _tt_rid, * FROM \"{}\"", table)
    } else {
        format!("SELECT * FROM \"{}\"", table)
    };
    let mut stmt = conn.prepare(&query)?;
    let skip = usize::from(with_rowid);
    let column_names: Vec<String> = stmt
        .column_names()
        .skip(skip)
        .map(|s| s.to_string())
        .collect();

    let mut cols_data: Vec<Vec<Option<String>>> = vec![Vec::new(); column_names.len()];
    let mut ids: Vec<Option<i64>> = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        if with_rowid {
            ids.push(Some(row.get(0)?));
        }
        for (col_idx, col_vec) in cols_data.iter_mut().enumerate() {
            col_vec.push(value_to_opt_string(row.get(col_idx + skip)?));
        }
    }

    let mut series_vec = Vec::new();
    for (i, col_data) in cols_data.into_iter().enumerate() {
        series_vec.push(Series::new(column_names[i].as_str().into(), col_data).into());
    }

    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)?;
    wrap_polars_df(pdf).map(|df| (df, ids))
}
