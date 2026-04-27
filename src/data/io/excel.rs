use crate::data::dataframe::DataFrame;
use crate::data::io::wrap_polars_df;
use color_eyre::{eyre::eyre, Result};
use polars::prelude::*;
use std::path::Path;

pub(super) fn load_excel(path: &Path) -> Result<DataFrame> {
    use calamine::{open_workbook_auto, Reader};

    let mut workbook = open_workbook_auto(path)?;
    let sheet_names = workbook.sheet_names().to_owned();
    if sheet_names.is_empty() {
        return Err(eyre!("Excel file is empty"));
    }
    let range = workbook
        .worksheet_range(&sheet_names[0])
        .ok_or_else(|| eyre!("Cannot read first sheet"))??;

    parse_excel_range(range)
}

pub fn load_excel_sheet_by_name(path: &Path, sheet_name: &str) -> Result<DataFrame> {
    use calamine::{open_workbook_auto, Reader};

    let mut workbook = open_workbook_auto(path)?;
    let range = workbook
        .worksheet_range(sheet_name)
        .ok_or_else(|| eyre!("Sheet '{}' not found", sheet_name))??;

    parse_excel_range(range)
}

pub fn load_excel_overview(path: &Path) -> Result<DataFrame> {
    let names = excel_sheet_names(path)?;
    if names.is_empty() {
        return Err(eyre!("Excel file has no sheets"));
    }
    let pdf =
        polars::prelude::DataFrame::new_infer_height(vec![
            Series::new("Sheet".into(), &names).into()
        ])?;
    let mut df = wrap_polars_df(pdf)?;
    if !df.columns.is_empty() {
        df.columns[0].width = 40;
    }
    Ok(df)
}

pub fn excel_sheet_names(path: &Path) -> Result<Vec<String>> {
    use calamine::{open_workbook_auto, Reader};
    let workbook = open_workbook_auto(path)?;
    Ok(workbook.sheet_names().to_owned())
}

pub(super) fn save_xlsx(df: &DataFrame, path: &Path) -> Result<()> {
    use rust_xlsxwriter::{Format, Workbook};

    let ordered_df = df.to_display_polars_df();
    let col_names: Vec<String> = ordered_df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut workbook = Workbook::new();
    let sheet = workbook.add_worksheet();
    let header_fmt = Format::new().set_bold();

    for (ci, name) in col_names.iter().enumerate() {
        sheet
            .write_string_with_format(0, ci as u16, name, &header_fmt)
            .map_err(|e| eyre!("{}", e))?;
    }

    let nrows = ordered_df.height();
    let ncols = col_names.len();
    for row_idx in 0..nrows {
        for ci in 0..ncols {
            let series = &ordered_df.columns()[ci];
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

fn parse_excel_range(range: calamine::Range<calamine::DataType>) -> Result<DataFrame> {
    let all_rows: Vec<Vec<String>> = range
        .rows()
        .map(|row| row.iter().map(|c| c.to_string()).collect())
        .collect();

    let mut iter = all_rows.into_iter();
    let header_row = iter.next().ok_or_else(|| eyre!("Excel sheet has no headers"))?;

    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let headers: Vec<String> = header_row
        .into_iter()
        .enumerate()
        .map(|(i, h)| {
            let base = if h.is_empty() {
                format!("column_{}", i + 1)
            } else {
                h
            };
            let count = seen.entry(base.clone()).or_insert(0);
            *count += 1;
            if *count == 1 {
                base
            } else {
                format!("{}_{}", base, count)
            }
        })
        .collect();

    let col_count = headers.len();
    let mut cols_data: Vec<Vec<String>> = vec![Vec::new(); col_count];

    for row in iter {
        for (i, cell) in row.into_iter().enumerate() {
            if i < col_count {
                cols_data[i].push(cell);
            }
        }
    }

    let mut series_vec = Vec::new();
    for (i, col_data) in cols_data.into_iter().enumerate() {
        series_vec.push(Series::new(headers[i].as_str().into(), &col_data).into());
    }

    let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)?;
    wrap_polars_df(pdf)
}
