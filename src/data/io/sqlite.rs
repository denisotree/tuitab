use crate::data::dataframe::DataFrame;
use crate::data::io::wrap_polars_df;
use color_eyre::{eyre::eyre, Result};
use polars::prelude::*;
use std::path::Path;

pub fn load_sqlite_overview(path: &Path) -> Result<DataFrame> {
    use rusqlite::Connection;
    let conn = Connection::open(path)?;

    let mut stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;

    let mut table_names: Vec<String> = Vec::new();
    let mut row_counts: Vec<String> = Vec::new();
    let mut col_counts: Vec<String> = Vec::new();
    let mut sql_defs: Vec<String> = Vec::new();

    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let sql: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();

        let col_count: usize = {
            let pragma = format!("PRAGMA table_info(\"{}\")", name);
            let mut ps = conn.prepare(&pragma)?;
            let mut pr = ps.query([])?;
            let mut n = 0usize;
            while pr.next()?.is_some() {
                n += 1;
            }
            n
        };

        let row_count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM \"{}\"", name), [], |r| {
                r.get(0)
            })
            .unwrap_or(0);

        table_names.push(name);
        row_counts.push(row_count.to_string());
        col_counts.push(col_count.to_string());
        sql_defs.push(sql);
    }

    if table_names.is_empty() {
        return Err(eyre!("No tables found in SQLite database"));
    }

    let series_vec = vec![
        Series::new("Table".into(), &table_names).into(),
        Series::new("Rows".into(), &row_counts).into(),
        Series::new("Columns".into(), &col_counts).into(),
        Series::new("SQL".into(), &sql_defs).into(),
    ];

    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)?;
    let mut df = wrap_polars_df(pdf)?;

    if df.columns.len() == 4 {
        df.columns[0].width = 30;
        df.columns[1].width = 10;
        df.columns[2].width = 10;
        df.columns[3].width = 60;
    }

    Ok(df)
}

pub fn load_sqlite_table_by_name(path: &Path, table_name: &str) -> Result<DataFrame> {
    use rusqlite::Connection;
    let conn = Connection::open(path)?;
    load_sqlite_table(&conn, table_name)
}

pub fn sqlite_table_names(path: &Path) -> Result<Vec<String>> {
    use rusqlite::Connection;
    let conn = Connection::open(path)?;
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let mut names = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        names.push(name);
    }
    Ok(names)
}

pub(super) fn save_sqlite(df: &DataFrame, path: &Path) -> Result<()> {
    use rusqlite::Connection;
    let conn = Connection::open(path)?;

    let ordered_df = df.to_display_polars_df();
    let col_names: Vec<String> = ordered_df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    conn.execute_batch("DROP TABLE IF EXISTS data;")?;
    let col_defs: String = col_names
        .iter()
        .map(|n| format!("\"{n}\" TEXT"))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!("CREATE TABLE data ({});", col_defs))?;

    let placeholders: String = col_names.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let insert_sql = format!(
        "INSERT INTO data ({}) VALUES ({})",
        col_names
            .iter()
            .map(|n| format!("\"{n}\""))
            .collect::<Vec<_>>()
            .join(", "),
        placeholders,
    );

    let mut stmt = conn.prepare(&insert_sql)?;
    let nrows = ordered_df.height();
    let ncols = col_names.len();

    for row_idx in 0..nrows {
        let row_vals: Vec<String> = (0..ncols)
            .map(|ci| {
                let series = &ordered_df.columns()[ci];
                series
                    .get(row_idx)
                    .map(|v| {
                        let s = format!("{}", v);
                        if s.starts_with('"') && s.ends_with('"') {
                            s[1..s.len() - 1].to_string()
                        } else {
                            s
                        }
                    })
                    .unwrap_or_default()
            })
            .collect();
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            row_vals.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        stmt.execute(rusqlite::params_from_iter(params_refs.iter().copied()))?;
    }
    Ok(())
}

fn load_sqlite_table(conn: &rusqlite::Connection, table_name: &str) -> Result<DataFrame> {
    let query = format!("SELECT * FROM \"{}\"", table_name);
    let mut stmt = conn.prepare(&query)?;
    let column_names: Vec<String> = stmt
        .column_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    let col_count = column_names.len();

    let mut cols_data: Vec<Vec<String>> = vec![Vec::new(); col_count];
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        for (col_idx, col_vec) in cols_data.iter_mut().enumerate() {
            let val: rusqlite::types::Value = row.get(col_idx)?;
            let str_val = match val {
                rusqlite::types::Value::Null => String::new(),
                rusqlite::types::Value::Integer(i) => i.to_string(),
                rusqlite::types::Value::Real(f) => f.to_string(),
                rusqlite::types::Value::Text(s) => s,
                rusqlite::types::Value::Blob(_) => "[BLOB]".to_string(),
            };
            col_vec.push(str_val);
        }
    }

    let mut series_vec = Vec::new();
    for (i, col_data) in cols_data.into_iter().enumerate() {
        series_vec.push(Series::new(column_names[i].as_str().into(), &col_data).into());
    }

    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)?;
    wrap_polars_df(pdf)
}
