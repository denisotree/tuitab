use crate::data::dataframe::DataFrame;
use crate::data::io::wrap_polars_df;
use crate::types::ColumnType;
use color_eyre::{eyre::eyre, Result};
use polars::prelude::*;
use std::collections::HashSet;
use std::path::Path;

pub fn load_directory(dir: &Path) -> Result<DataFrame> {
    use chrono::{DateTime, Local};
    use std::fs;

    if !dir.is_dir() {
        return Err(eyre!("Path is not a directory: {}", dir.display()));
    }

    let mut names = Vec::new();
    let mut is_dirs = Vec::new();
    let mut sizes: Vec<String> = Vec::new();
    let mut modifieds = Vec::new();
    let mut is_supported = Vec::new();

    let supported_exts: HashSet<&str> = [
        "csv", "tsv", "txt", "json", "parquet", "xlsx", "xls", "db", "sqlite", "sqlite3",
        "duckdb", "ddb",
    ]
    .iter()
    .cloned()
    .collect();

    let mut entries: Vec<_> = fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
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
            .unwrap_or_default();
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        let supported = !is_dir && supported_exts.contains(ext.as_str());

        names.push(name);
        is_dirs.push(is_dir);
        sizes.push(size);
        modifieds.push(mod_time);
        is_supported.push(supported);
    }

    let series_vec = vec![
        Series::new("Name".into(), &names).into(),
        Series::new("Is Directory".into(), &is_dirs).into(),
        Series::new("Size".into(), &sizes).into(),
        Series::new("Modified".into(), &modifieds).into(),
        Series::new("Supported".into(), &is_supported).into(),
    ];

    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)?;
    let mut df = wrap_polars_df(pdf)?;
    apply_directory_column_types(&mut df);
    Ok(df)
}

pub fn load_files_list(
    paths: &[std::path::PathBuf],
) -> Result<(DataFrame, Vec<std::path::PathBuf>)> {
    use chrono::{DateTime, Local};

    let supported_exts: HashSet<&str> = [
        "csv", "tsv", "txt", "json", "parquet", "xlsx", "xls", "db", "sqlite", "sqlite3",
        "duckdb", "ddb",
    ]
    .iter()
    .cloned()
    .collect();

    let mut names = Vec::new();
    let mut is_dirs = Vec::new();
    let mut sizes: Vec<String> = Vec::new();
    let mut modifieds = Vec::new();
    let mut is_supported = Vec::new();
    let mut abs_paths = Vec::new();

    for p in paths {
        let abs = p.canonicalize().unwrap_or_else(|_| p.to_path_buf());
        let meta =
            std::fs::metadata(&abs).map_err(|e| eyre!("Cannot read '{}': {}", abs.display(), e))?;

        let is_dir = meta.is_dir();
        let name = abs
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| abs.to_string_lossy().to_string());
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
            .unwrap_or_default();
        let ext = abs
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        let supported = if is_dir {
            true
        } else {
            supported_exts.contains(ext.as_str())
        };

        names.push(name);
        is_dirs.push(is_dir);
        sizes.push(size);
        modifieds.push(mod_time);
        is_supported.push(supported);
        abs_paths.push(abs);
    }

    let series_vec = vec![
        Series::new("Name".into(), &names).into(),
        Series::new("Is Directory".into(), &is_dirs).into(),
        Series::new("Size".into(), &sizes).into(),
        Series::new("Modified".into(), &modifieds).into(),
        Series::new("Supported".into(), &is_supported).into(),
    ];
    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)?;
    let mut df = wrap_polars_df(pdf)?;
    apply_directory_column_types(&mut df);
    Ok((df, abs_paths))
}

fn apply_directory_column_types(df: &mut DataFrame) {
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
}

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
