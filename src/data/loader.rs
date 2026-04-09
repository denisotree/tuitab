use crate::data::column::ColumnMeta;
use crate::data::dataframe::DataFrame;
use crate::types::ColumnType;
use color_eyre::Result;
use polars::prelude::*;
use std::collections::HashSet;
use std::io::Read;
use std::path::Path;

/// Load a CSV or TSV file into a DataFrame.
/// Delimiter is auto-detected from the first line if not supplied.
pub fn load_csv(path: &Path, delimiter: Option<u8>) -> Result<DataFrame> {
    let delim = delimiter.unwrap_or_else(|| detect_delimiter(path));

    let lf = LazyCsvReader::new(path)
        .with_separator(delim)
        .with_has_header(true)
        .with_infer_schema_length(None)
        .finish()?;

    let pdf = lf.collect()?;

    let col_count = pdf.width();
    let row_count = pdf.height();

    let mut columns = Vec::with_capacity(col_count);

    for series in pdf.get_columns() {
        let name = series.name().to_string();
        let mut col_meta = ColumnMeta::new(name);

        // Map Polars DataType → our ColumnType
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
        row_order: Arc::new(row_order),
        original_order: Arc::new(original_order),
        selected_rows: HashSet::new(),
        modified: false,
        aggregates_cache: None,
    };

    df.calc_widths(40, 1000);

    Ok(df)
}

/// Detect the most likely delimiter by reading the first 8 KB of the file.
/// Counts occurrences of common delimiters per line and picks the most consistent one.
fn detect_delimiter(path: &Path) -> u8 {
    let candidates: &[u8] = &[b',', b'\t', b'|', b';'];
    let default = b',';

    let Ok(mut file) = std::fs::File::open(path) else {
        return default;
    };

    let mut buf = [0u8; 8192];
    let Ok(n) = file.read(&mut buf) else {
        return default;
    };

    let content = &buf[..n];
    // Split into lines
    let lines: Vec<&[u8]> = content.split(|&b| b == b'\n').take(10).collect();

    let mut best_delim = default;
    let mut best_score = 0usize;

    'outer: for &delim in candidates {
        // Count occurrences per line
        let counts: Vec<usize> = lines
            .iter()
            .filter(|l| !l.is_empty())
            .map(|l| l.iter().filter(|&&b| b == delim).count())
            .collect();

        if counts.is_empty() || counts[0] == 0 {
            continue 'outer;
        }

        // Score = first-line count × consistency (how many lines have the same count)
        let first_count = counts[0];
        let consistent = counts.iter().filter(|&&c| c == first_count).count();
        let score = first_count * consistent;

        if score > best_score {
            best_score = score;
            best_delim = delim;
        }
    }

    best_delim
}
