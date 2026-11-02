//! Arrow IPC, also known as Feather v2 — the on-disk form of the columnar layout
//! polars already uses in memory, so it round-trips faster than anything else here.

use crate::data::dataframe::DataFrame;
use crate::data::io::wrap_polars_df;
use color_eyre::Result;
use polars::prelude::*;
use std::fs::File;
use std::path::Path;

pub(super) fn load_arrow(path: &Path) -> Result<DataFrame> {
    let file = File::open(path)?;
    let pdf = IpcReader::new(file).finish()?;
    wrap_polars_df(pdf)
}

pub(super) fn save_arrow(df: &DataFrame, path: &Path) -> Result<()> {
    let mut out_df = super::with_zones(df)?;
    let mut file = File::create(path)?;
    IpcWriter::new(&mut file).finish(&mut out_df)?;
    Ok(())
}
