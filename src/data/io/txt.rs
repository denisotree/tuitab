use crate::data::column::ColumnMeta;
use crate::data::dataframe::DataFrame;
use crate::types::ColumnType;
use color_eyre::Result;
use polars::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

pub(super) fn load_txt(path: &Path) -> Result<DataFrame> {
    use std::io::{BufRead, BufReader};

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().map(|l| l.unwrap_or_default()).collect();

    let series = Series::new("Line".into(), &lines);
    let pdf = polars::prelude::DataFrame::new_infer_height(vec![series.into()])?;
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
