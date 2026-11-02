//! What a spreadsheet's cells are once they are in a table.
//!
//! Every cell reaches tuitab as text, and a column left as text is one no aggregate
//! will touch — `sum` over a money column used to answer "the column is string", which
//! made an .xlsx source useless for the arithmetic the tool exists to do.

use std::path::{Path, PathBuf};
use tuitab::data::io::{load_file, save_file_as};
use tuitab::types::ColumnType;

fn tmp(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("excel-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    let _ = std::fs::remove_file(&path);
    path
}

/// Written through tuitab's own writer, which puts numbers in as numbers — the same
/// thing Excel does, and what the reader has to make sense of.
fn round_trip(case: &str, csv: &str) -> tuitab::data::dataframe::DataFrame {
    let src = tmp(&format!("{}.csv", case));
    std::fs::write(&src, csv).unwrap();
    let df = load_file(&src, None).unwrap();
    let xlsx = tmp(&format!("{}.xlsx", case));
    save_file_as(
        &df,
        None,
        &xlsx,
        tuitab::data::io::doc_io::Shape::Records,
        "Sheet1",
    )
    .unwrap();
    load_file(&xlsx, None).unwrap()
}

#[test]
fn numeric_cells_come_back_as_numbers() {
    let df = round_trip(
        "numbers",
        "id,price,label\n\
         1,7.1138211382113825,alpha\n\
         2,0.6,beta\n\
         3,4,gamma\n",
    );

    let types: Vec<ColumnType> = df.columns.iter().map(|c| c.col_type).collect();
    assert_eq!(
        types,
        vec![ColumnType::Integer, ColumnType::Float, ColumnType::String],
        "columns: {:?}",
        df.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );

    // Whole numbers stay whole: an id must not come back as 1.00.
    assert_eq!(df.format_display(0, 0), "1");
}

#[test]
fn a_column_with_one_word_in_it_stays_text() {
    // A real word: `n/a` is now a gap in a CSV's number column, read as NULL before
    // the workbook is written.
    let df = round_trip("mixed", "qty\n1\n2\nmany\n");
    assert_eq!(df.columns[0].col_type, ColumnType::String);
    assert_eq!(df.format_display(2, 0), "many");
}

#[test]
fn an_empty_cell_is_missing_rather_than_text() {
    let df = round_trip("blank", "qty,note\n1,a\n,b\n3,c\n");
    assert_eq!(
        df.columns[0].col_type,
        ColumnType::Integer,
        "a blank must not sink the column back to text"
    );
    assert!(df.is_null_physical(1, 0), "the blank cell is NULL");
}

#[test]
fn a_padded_number_stays_the_text_it_is() {
    // 01234 is a postcode, not 1234.  Written as a text cell, which is the only way a
    // spreadsheet can hold it — the CSV path loses the padding long before this.
    use rust_xlsxwriter::Workbook;
    let path = tmp("padded.xlsx");
    let mut wb = Workbook::new();
    {
        let sheet = wb.add_worksheet();
        sheet.write_string(0, 0, "code").unwrap();
        sheet.write_string(0, 1, "qty").unwrap();
        for (row, (code, qty)) in [("01234", 1.0), ("00777", 2.0), ("10000", 3.0)]
            .into_iter()
            .enumerate()
        {
            sheet.write_string(row as u32 + 1, 0, code).unwrap();
            sheet.write_number(row as u32 + 1, 1, qty).unwrap();
        }
    }
    wb.save(&path).unwrap();

    let df = load_file(&path, None).unwrap();
    assert_eq!(df.columns[0].col_type, ColumnType::String);
    assert_eq!(df.format_display(0, 0), "01234");
    assert_eq!(
        df.columns[1].col_type,
        ColumnType::Integer,
        "its neighbour is still a number"
    );
}

#[test]
fn a_date_cell_is_a_date_and_not_its_serial_number() {
    // A date in a spreadsheet is a number with a format on it. Read as the number, the
    // 29th of January 2026 arrives as 46051 — which is what tuitab used to answer.
    use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};
    let path = tmp("dates.xlsx");
    let mut wb = Workbook::new();
    {
        let day = Format::new().set_num_format("yyyy-mm-dd");
        let moment = Format::new().set_num_format("yyyy-mm-dd hh:mm:ss");
        let sheet = wb.add_worksheet();
        sheet.write_string(0, 0, "day").unwrap();
        sheet.write_string(0, 1, "moment").unwrap();
        sheet
            .write_with_format(1, 0, ExcelDateTime::from_ymd(2026, 1, 29).unwrap(), &day)
            .unwrap();
        sheet
            .write_with_format(
                1,
                1,
                ExcelDateTime::from_ymd(2026, 1, 29)
                    .unwrap()
                    .and_hms(14, 30, 0)
                    .unwrap(),
                &moment,
            )
            .unwrap();
    }
    wb.save(&path).unwrap();

    let df = load_file(&path, None).unwrap();
    assert_eq!(df.get_physical(0, 0), "2026-01-29");
    assert_eq!(df.get_physical(0, 1), "2026-01-29 14:30:00");
}
