use crate::data::dataframe::DataFrame;
use bincode;
use color_eyre::Result;
use std::fs;
use std::path::Path;

/// Serialize a DataFrame to a binary file on disk (swap-out for memory savings).
pub fn swap_out(df: &DataFrame, path: &Path) -> Result<()> {
    let bytes = bincode::serialize(df).map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    fs::write(path, bytes)?;
    Ok(())
}

/// Deserialize a DataFrame from a binary swap file (swap-in).
pub fn swap_in(path: &Path) -> Result<DataFrame> {
    let bytes = fs::read(path)?;
    let df: DataFrame =
        bincode::deserialize(&bytes).map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
    Ok(df)
}
