use crate::data::dataframe::DataFrame;
use crate::data::io::wrap_polars_df;
use color_eyre::{eyre::eyre, Result};
use polars::prelude::*;
use std::path::Path;

pub fn load_duckdb_overview(path: &Path) -> Result<DataFrame> {
    use duckdb::Connection;
    let conn = Connection::open(path)?;

    let mut stmt = conn.prepare(
        "SELECT table_name \
         FROM information_schema.tables \
         WHERE table_schema = 'main' AND table_type = 'BASE TABLE' \
         ORDER BY table_name",
    )?;

    let mut table_names: Vec<String> = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        table_names.push(name);
    }

    let mut row_counts: Vec<String> = Vec::new();
    let mut col_counts: Vec<String> = Vec::new();

    for name in &table_names {
        let row_count: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM \"{}\"", name.replace('"', "\"\"")),
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        row_counts.push(row_count.to_string());

        let col_count: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM information_schema.columns \
                     WHERE table_schema = 'main' AND table_name = '{}'",
                    name.replace('\'', "''")
                ),
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        col_counts.push(col_count.to_string());
    }

    if table_names.is_empty() {
        return Err(eyre!("No tables found in DuckDB database"));
    }

    let series_vec = vec![
        Series::new("Table".into(), &table_names).into(),
        Series::new("Rows".into(), &row_counts).into(),
        Series::new("Columns".into(), &col_counts).into(),
    ];
    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)?;
    let mut df = wrap_polars_df(pdf)?;
    if df.columns.len() == 3 {
        df.columns[0].width = 40;
        df.columns[1].width = 12;
        df.columns[2].width = 12;
    }
    Ok(df)
}

pub fn load_duckdb_table_by_name(path: &Path, table_name: &str) -> Result<DataFrame> {
    use duckdb::Connection;
    let conn = Connection::open(path)?;

    let safe_table = table_name.replace('\'', "''");
    let mut col_stmt = conn.prepare(&format!(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = 'main' AND table_name = '{}' \
         ORDER BY ordinal_position",
        safe_table
    ))?;
    let mut col_rows = col_stmt.query([])?;
    let mut col_names: Vec<String> = Vec::new();
    while let Some(row) = col_rows.next()? {
        let name: String = row.get(0)?;
        col_names.push(name);
    }
    if col_names.is_empty() {
        return Err(eyre!("No columns found in table: {}", table_name));
    }

    let safe_tbl = table_name.replace('"', "\"\"");
    let casts: Vec<String> = col_names
        .iter()
        .map(|c| format!("CAST(\"{}\" AS VARCHAR)", c.replace('"', "\"\"")))
        .collect();
    let query = format!("SELECT {} FROM \"{}\"", casts.join(", "), safe_tbl);

    let mut stmt = conn.prepare(&query)?;
    let col_count = col_names.len();
    let mut cols_data: Vec<Vec<String>> = vec![Vec::new(); col_count];
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        for (ci, col_vec) in cols_data.iter_mut().enumerate() {
            let val: Option<String> = row.get(ci)?;
            col_vec.push(val.unwrap_or_default());
        }
    }

    let mut series_vec = Vec::new();
    for (i, col_data) in cols_data.into_iter().enumerate() {
        series_vec.push(Series::new(col_names[i].as_str().into(), &col_data).into());
    }
    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)?;
    wrap_polars_df(pdf)
}

pub fn duckdb_table_names(path: &Path) -> Result<Vec<String>> {
    use duckdb::Connection;
    let conn = Connection::open(path)?;
    let mut stmt = conn.prepare(
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'main' AND table_type = 'BASE TABLE' \
         ORDER BY table_name",
    )?;
    let mut names = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        names.push(name);
    }
    Ok(names)
}
