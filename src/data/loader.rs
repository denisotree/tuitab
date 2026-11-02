use crate::data::dataframe::DataFrame;
use color_eyre::Result;
use polars::prelude::*;
use std::io::Read;
use std::path::Path;

/// Load a CSV or TSV file into a DataFrame.
/// Delimiter is auto-detected from the first line if not supplied.
pub fn load_csv(path: &Path, delimiter: Option<u8>) -> Result<DataFrame> {
    load_csv_opts(path, &crate::data::io::LoadOpts::with_delimiter(delimiter))
}

pub fn load_csv_opts(path: &Path, opts: &crate::data::io::LoadOpts) -> Result<DataFrame> {
    let delim = opts.delimiter.unwrap_or_else(|| detect_delimiter(path));

    let mut reader = LazyCsvReader::new(
        PlRefPath::try_from_path(path).map_err(|e| color_eyre::eyre::eyre!("{}", e))?,
    )
    .with_separator(delim)
    .with_has_header(true)
    .with_infer_schema_length(None);
    if !opts.null_values.is_empty() {
        reader = reader.with_null_values(Some(NullValues::AllColumns(
            opts.null_values.iter().map(|v| v.as_str().into()).collect(),
        )));
    }
    let lf = reader.finish()?;

    let pdf = lf.collect()?;
    crate::data::io::wrap_polars_df(pdf)
}

/// Detect the most likely delimiter by reading the first 8 KB of the file.
/// Counts occurrences of common delimiters per line and picks the most consistent one.
pub(crate) fn detect_delimiter(path: &Path) -> u8 {
    let candidates: &[u8] = b",\t|;";
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
