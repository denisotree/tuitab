use crate::data::dataframe::DataFrame;
use color_eyre::Result;
use polars::prelude::*;
use std::io::Read;
use std::path::Path;

/// Load a CSV or TSV file into a DataFrame.
/// Delimiter is auto-detected from the first line if not supplied.
pub fn load_csv(path: &Path, delimiter: Option<u8>) -> Result<DataFrame> {
    load_csv_as(
        path,
        &crate::data::io::LoadOpts::with_delimiter(delimiter),
        "csv",
    )
}

/// Load a file read as `ext` (`csv` or `tsv`).
pub(crate) fn load_csv_as(
    path: &Path,
    opts: &crate::data::io::LoadOpts,
    ext: &str,
) -> Result<DataFrame> {
    let file = PlRefPath::try_from_path(path).map_err(|e| color_eyre::eyre::eyre!("{}", e))?;
    let pdf = csv_reader(file, separator(opts, ext, path), opts)
        .finish()?
        .collect()?;
    crate::data::io::wrap_polars_df(pdf)
}

/// The separator a file is read with: the one given, a tab for a tsv — sniffing a file
/// whose text holds commas would split it on them — or what its first lines suggest.
pub(crate) fn separator(opts: &crate::data::io::LoadOpts, ext: &str, file: &Path) -> u8 {
    opts.delimiter
        .or((ext == "tsv").then_some(b'\t'))
        .unwrap_or_else(|| detect_delimiter(file))
}

/// How every path reads a CSV: a header, each column typed from all its rows, and
/// the text the caller named read as NULL.
pub(crate) fn csv_reader(
    path: PlRefPath,
    sep: u8,
    opts: &crate::data::io::LoadOpts,
) -> LazyCsvReader {
    // A pattern is expanded; a real file whose name holds `[1]` is not.
    let glob = crate::data::io::is_pattern(std::path::Path::new(path.as_str()));
    let reader = LazyCsvReader::new(path)
        .with_glob(glob)
        .with_separator(sep)
        .with_has_header(true)
        .with_infer_schema_length(None);
    if opts.null_values.is_empty() {
        return reader;
    }
    reader.with_null_values(Some(NullValues::AllColumns(
        opts.null_values.iter().map(|v| v.as_str().into()).collect(),
    )))
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
