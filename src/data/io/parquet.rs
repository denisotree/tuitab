use crate::data::dataframe::DataFrame;
use crate::data::io::wrap_polars_df;
use color_eyre::Result;
use polars::prelude::*;
use std::fs::File;
use std::path::Path;

pub(super) fn load_parquet(path: &Path) -> Result<DataFrame> {
    let file = File::open(path)?;
    let pdf = ParquetReader::new(file).finish()?;
    wrap_polars_df(pdf)
}

pub(super) fn save_parquet(df: &DataFrame, path: &Path) -> Result<()> {
    let mut out_df = df.to_display_polars_df();
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(&mut out_df)?;
    Ok(())
}
