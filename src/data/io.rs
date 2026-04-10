use crate::data::column::ColumnMeta;
use crate::data::dataframe::DataFrame;
use crate::types::ColumnType;
use color_eyre::{eyre::eyre, Result};
use polars::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

/// Load file into DataFrame based on extension.
pub fn load_file(path: &Path, delimiter: Option<u8>) -> Result<DataFrame> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("csv")
        .to_lowercase();

    match ext.as_str() {
        "csv" | "tsv" => crate::data::loader::load_csv(path, delimiter),
        "txt" => load_txt(path),
        "json" => load_json(path),
        "parquet" => load_parquet(path),
        "xlsx" | "xls" => load_excel(path),
        "db" | "sqlite" | "sqlite3" => load_sqlite_overview(path),
        _ => Err(eyre!("Unsupported file format: .{}", ext)),
    }
}

/// Save DataFrame to file based on extension.
pub fn save_file(df: &DataFrame, path: &Path) -> Result<()> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("csv")
        .to_lowercase();

    match ext.as_str() {
        "csv" => save_csv_polars(df, path, b','),
        "tsv" => save_csv_polars(df, path, b'\t'),
        "json" => save_json_polars(df, path),
        "parquet" => save_parquet_polars(df, path),
        "db" | "sqlite" | "sqlite3" => save_sqlite(df, path),
        "xlsx" | "xls" => save_xlsx(df, path),
        _ => Err(eyre!("Unsupported save format: .{}", ext)),
    }
}

/// Load typed data from stdin by buffering it to a temporary file.
pub fn load_from_stdin_typed(data_type: &str, delimiter: Option<u8>) -> Result<DataFrame> {
    use std::io::{Read, Write};
    use tempfile::NamedTempFile;

    // Read all of stdin into a buffer
    let mut buf = Vec::new();
    std::io::stdin().read_to_end(&mut buf)?;

    // Write to a temporary file
    let mut temp_file = NamedTempFile::new()?;
    temp_file.write_all(&buf)?;
    let temp_path = temp_file.path().to_path_buf();

    // Parse the temporary file based on the explicitly provided type
    let pdf = match data_type.to_lowercase().as_str() {
        "csv" | "txt" => {
            let sep = delimiter.unwrap_or(b',');
            polars::prelude::CsvReadOptions::default()
                .with_has_header(true)
                .map_parse_options(|o| o.with_separator(sep))
                .try_into_reader_with_file_path(Some(temp_path))?
                .finish()?
        }
        "tsv" => polars::prelude::CsvReadOptions::default()
            .with_has_header(true)
            .map_parse_options(|o| o.with_separator(b'\t'))
            .try_into_reader_with_file_path(Some(temp_path))?
            .finish()?,
        "json" => {
            let file = File::open(temp_path)?;
            JsonReader::new(file).finish()?
        }
        _ => return Err(eyre!("Unsupported stdin data type: {}", data_type)),
    };

    // Explicitly keep temporary file alive until parsing is fully done
    drop(temp_file);

    wrap_polars_df(pdf)
}

fn load_json(path: &Path) -> Result<DataFrame> {
    let file = File::open(path)?;
    let pdf = JsonReader::new(file).finish()?;
    wrap_polars_df(pdf)
}

fn load_parquet(path: &Path) -> Result<DataFrame> {
    let file = File::open(path)?;
    let pdf = ParquetReader::new(file).finish()?;
    wrap_polars_df(pdf)
}

fn load_excel(path: &Path) -> Result<DataFrame> {
    use calamine::{open_workbook_auto, Reader};

    let mut workbook = open_workbook_auto(path)?;
    let sheet_names = workbook.sheet_names().to_owned();
    if sheet_names.is_empty() {
        return Err(eyre!("Excel file is empty"));
    }
    let first_sheet = &sheet_names[0];

    let range = workbook
        .worksheet_range(first_sheet)
        .ok_or_else(|| eyre!("Cannot read first sheet"))??;

    let mut rows = range.rows();
    let header_row = rows
        .next()
        .ok_or_else(|| eyre!("Excel sheet has no headers"))?;

    let headers: Vec<String> = header_row.iter().map(|c| c.to_string()).collect();
    let col_count = headers.len();

    // We will collect strings for each column to build Polars series
    // In a production app, we would infer types directly from Calamine cells.
    let mut cols_data: Vec<Vec<String>> = vec![Vec::new(); col_count];

    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < col_count {
                cols_data[i].push(cell.to_string());
            }
        }
    }

    let mut series_vec = Vec::new();
    for (i, col_data) in cols_data.into_iter().enumerate() {
        series_vec.push(Series::new(headers[i].as_str().into(), &col_data).into());
    }

    let pdf = polars::prelude::DataFrame::new(series_vec)?;
    wrap_polars_df(pdf)
}

/// Load a plain-text file where each line becomes one row in a single "Line" column.
fn load_txt(path: &Path) -> Result<DataFrame> {
    use std::io::{BufRead, BufReader};

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().map(|l| l.unwrap_or_default()).collect();

    let series = Series::new("Line".into(), &lines);
    let pdf = polars::prelude::DataFrame::new(vec![series.into()])?;
    let row_count = pdf.height();

    let mut col_meta = ColumnMeta::new("Line".to_string());
    col_meta.col_type = ColumnType::String;
    col_meta.width = 60;

    let row_order: Vec<usize> = (0..row_count).collect();
    let original_order = row_order.clone();

    Ok(DataFrame {
        df: pdf,
        columns: vec![col_meta],
        row_order: std::sync::Arc::new(row_order),
        original_order: std::sync::Arc::new(original_order),
        selected_rows: HashSet::new(),
        modified: false,
        aggregates_cache: None,
    })
}

/// Load the content of a single SQLite table by name.
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

    let pdf = polars::prelude::DataFrame::new(series_vec)?;
    wrap_polars_df(pdf)
}

/// Load a SQLite database as a table-browser overview.
/// Returns a DataFrame with columns: Table, Rows, Columns, SQL.
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

        // Count columns via PRAGMA
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

        // Count rows
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

    let pdf = polars::prelude::DataFrame::new(series_vec)?;
    let mut df = wrap_polars_df(pdf)?;

    if df.columns.len() == 4 {
        df.columns[0].width = 30; // Table
        df.columns[1].width = 10; // Rows
        df.columns[2].width = 10; // Columns
        df.columns[3].width = 60; // SQL
    }

    Ok(df)
}

/// Load a specific table from a SQLite file by table name.
pub fn load_sqlite_table_by_name(path: &Path, table_name: &str) -> Result<DataFrame> {
    use rusqlite::Connection;
    let conn = Connection::open(path)?;
    load_sqlite_table(&conn, table_name)
}

fn wrap_polars_df(pdf: polars::prelude::DataFrame) -> Result<DataFrame> {
    let col_count = pdf.width();
    let row_count = pdf.height();
    let mut columns = Vec::with_capacity(col_count);

    for series in pdf.get_columns() {
        let name = series.name().to_string();
        let mut col_meta = ColumnMeta::new(name);

        col_meta.col_type = match series.dtype() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => ColumnType::Integer,
            DataType::Float32 | DataType::Float64 => ColumnType::Float,
            DataType::Date => ColumnType::Date,
            DataType::Datetime(_, _) => ColumnType::Datetime,
            _ => ColumnType::String,
        };

        columns.push(col_meta);
    }

    let row_order: Vec<usize> = (0..row_count).collect();
    let original_order = row_order.clone();

    let mut df = DataFrame {
        df: pdf,
        columns,
        row_order: std::sync::Arc::new(row_order),
        original_order: std::sync::Arc::new(original_order),
        selected_rows: HashSet::new(),
        modified: false,
        aggregates_cache: None,
    };
    df.calc_widths(40, 1000);
    Ok(df)
}

fn save_csv_polars(df: &DataFrame, path: &Path, delimiter: u8) -> Result<()> {
    let mut out_df = if df.row_order.len() != df.df.height() || df.row_order != df.original_order {
        let indices = IdxCa::new(
            "".into(),
            df.row_order.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        df.df.take(&indices)?
    } else {
        df.df.clone()
    };

    let mut file = File::create(path)?;
    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(delimiter)
        .finish(&mut out_df)?;
    Ok(())
}

fn save_json_polars(df: &DataFrame, path: &Path) -> Result<()> {
    let mut out_df = if df.row_order.len() != df.df.height() || df.row_order != df.original_order {
        let indices = IdxCa::new(
            "".into(),
            df.row_order.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        df.df.take(&indices)?
    } else {
        df.df.clone()
    };

    let mut file = File::create(path)?;
    JsonWriter::new(&mut file).finish(&mut out_df)?;
    Ok(())
}

fn save_parquet_polars(df: &DataFrame, path: &Path) -> Result<()> {
    let mut out_df = if df.row_order.len() != df.df.height() || df.row_order != df.original_order {
        let indices = IdxCa::new(
            "".into(),
            df.row_order.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        df.df.take(&indices)?
    } else {
        df.df.clone()
    };

    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(&mut out_df)?;
    Ok(())
}

/// Save DataFrame to a SQLite file (overwrites / creates the table 'data').
fn save_sqlite(df: &DataFrame, path: &Path) -> Result<()> {
    use rusqlite::Connection;
    let conn = Connection::open(path)?;

    // Determine column info from visible rows
    let ordered_df = if df.row_order.len() != df.df.height() || df.row_order != df.original_order {
        let indices = IdxCa::new(
            "".into(),
            df.row_order.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        df.df.take(&indices)?
    } else {
        df.df.clone()
    };

    let col_names: Vec<String> = ordered_df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    // Drop and recreate the table
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
                let series = &ordered_df.get_columns()[ci];
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

/// Save DataFrame to an XLSX file.
fn save_xlsx(df: &DataFrame, path: &Path) -> Result<()> {
    use rust_xlsxwriter::{Format, Workbook};

    let ordered_df = if df.row_order.len() != df.df.height() || df.row_order != df.original_order {
        let indices = IdxCa::new(
            "".into(),
            df.row_order.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        df.df.take(&indices)?
    } else {
        df.df.clone()
    };

    let col_names: Vec<String> = ordered_df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    let mut workbook = Workbook::new();
    let sheet = workbook.add_worksheet();
    let header_fmt = Format::new().set_bold();

    // Write headers
    for (ci, name) in col_names.iter().enumerate() {
        sheet
            .write_string_with_format(0, ci as u16, name, &header_fmt)
            .map_err(|e| eyre!("{}", e))?;
    }

    // Write data rows
    let nrows = ordered_df.height();
    let ncols = col_names.len();
    for row_idx in 0..nrows {
        for ci in 0..ncols {
            let series = &ordered_df.get_columns()[ci];
            let cell_text = series
                .get(row_idx)
                .map(|v| {
                    let s = format!("{}", v);
                    if s.starts_with('"') && s.ends_with('"') {
                        s[1..s.len() - 1].to_string()
                    } else {
                        s
                    }
                })
                .unwrap_or_default();
            // Try to write as number, fall back to string
            if let Ok(n) = cell_text.parse::<f64>() {
                sheet
                    .write_number((row_idx + 1) as u32, ci as u16, n)
                    .map_err(|e| eyre!("{}", e))?;
            } else {
                sheet
                    .write_string((row_idx + 1) as u32, ci as u16, &cell_text)
                    .map_err(|e| eyre!("{}", e))?;
            }
        }
    }

    workbook.save(path).map_err(|e| eyre!("{}", e))?;
    Ok(())
}

/// Format byte count as human-readable string (like `ls -lh`).
fn format_file_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
pub fn format_file_size_pub(bytes: u64) -> String {
    format_file_size(bytes)
}

/// Load a directory listing into a DataFrame.
pub fn load_directory(dir: &Path) -> Result<DataFrame> {
    use chrono::{DateTime, Local};
    use std::fs;

    let mut names = Vec::new();
    let mut is_dirs = Vec::new();
    let mut sizes: Vec<String> = Vec::new();
    let mut modifieds = Vec::new();
    let mut is_supported = Vec::new();

    let supported_exts: HashSet<&str> = [
        "csv", "tsv", "txt", "json", "parquet", "xlsx", "xls", "db", "sqlite", "sqlite3",
    ]
    .iter()
    .cloned()
    .collect();

    if dir.is_dir() {
        let mut entries: Vec<_> = fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();

        // Sort: directories first, then alphabetically by name
        entries.sort_by(|a, b| {
            let a_dir = a.file_type().map(|t| t.is_dir()).unwrap_or(false);
            let b_dir = b.file_type().map(|t| t.is_dir()).unwrap_or(false);
            match (a_dir, b_dir) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => a
                    .file_name()
                    .to_ascii_lowercase()
                    .cmp(&b.file_name().to_ascii_lowercase()),
            }
        });

        for entry in entries {
            let meta = entry.metadata()?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();

            // Skip hidden files/directories (starting with .)
            if name.starts_with('.') {
                continue;
            }

            let is_dir = meta.is_dir();

            let size = if is_dir {
                "-".to_string()
            } else {
                format_file_size(meta.len())
            };

            let mod_time = meta
                .modified()
                .ok()
                .map(|t| {
                    let dt: DateTime<Local> = t.into();
                    dt.format("%Y-%m-%d %H:%M:%S").to_string()
                })
                .unwrap_or_else(|| "".to_string());

            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_lowercase();
            let supported = if is_dir {
                false
            } else {
                supported_exts.contains(ext.as_str())
            };

            names.push(name);
            is_dirs.push(is_dir);
            sizes.push(size);
            modifieds.push(mod_time);
            is_supported.push(supported);
        }
    } else {
        return Err(eyre!("Path is not a directory: {}", dir.display()));
    }

    // Convert Vec<Option<u64>> to Series, we can use an f64 or sortable integer
    // Let's create Series
    let series_vec = vec![
        Series::new("Name".into(), &names).into(),
        Series::new("Is Directory".into(), &is_dirs).into(),
        Series::new("Size".into(), &sizes).into(),
        Series::new("Modified".into(), &modifieds).into(),
        Series::new("Supported".into(), &is_supported).into(),
    ];

    let pdf = polars::prelude::DataFrame::new(series_vec)?;

    // Create our DataFrame wrapper and mark the types properly
    let mut df = wrap_polars_df(pdf)?;

    // Force specific column types for UI formatting later
    if df.columns.len() == 5 {
        df.columns[0].col_type = ColumnType::String;
        df.columns[1].col_type = ColumnType::Boolean;
        df.columns[2].col_type = ColumnType::String;
        df.columns[3].col_type = ColumnType::Datetime;
        df.columns[4].col_type = ColumnType::Boolean;

        df.columns[0].width = 40;
        df.columns[1].width = 15;
        df.columns[2].width = 10;
        df.columns[3].width = 20;
        df.columns[4].width = 10;
    }

    Ok(df)
}
