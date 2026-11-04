use crate::data::dataframe::DataFrame;
use color_eyre::Result;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

/// Write a DataFrame to a binary file on disk (swap-out for memory savings), streamed:
/// a sheet of gigabytes must not need as much again for its bytes on the way out.
pub fn swap_out(df: &DataFrame, path: &Path) -> Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    bincode::serialize_into(&mut out, df).map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    out.flush()?;
    Ok(())
}

/// Read a DataFrame back from its swap file (swap-in), streamed the same way.
pub fn swap_in(path: &Path) -> Result<DataFrame> {
    let input = BufReader::new(File::open(path)?);
    bincode::deserialize_from(input).map_err(|e| color_eyre::eyre::eyre!(e.to_string()))
}
