use polars::prelude::NamedFrom;
use std::path::Path;
use tuitab::data::dataframe::DataFrame;
use tuitab::data::loader::load_csv;
use tuitab::types::ColumnType;

fn sample_path() -> &'static Path {
    Path::new("test_data/sample.csv")
}

#[test]
fn test_load_csv_row_and_col_count() {
    let df = load_csv(sample_path(), None).expect("Failed to load sample.csv");
    assert_eq!(df.visible_row_count(), 20, "Expected 20 data rows");
    assert_eq!(df.col_count(), 5, "Expected 5 columns");
}

#[test]
fn test_column_names() {
    let df = load_csv(sample_path(), None).expect("Failed to load sample.csv");
    let names: Vec<&str> = df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name", "age", "salary", "department"]);
}

#[test]
fn test_type_inference() {
    let df = load_csv(sample_path(), None).expect("Failed to load sample.csv");
    // id column (integers)
    assert_eq!(df.columns[0].col_type, ColumnType::Integer);
    // name column (strings)
    assert_eq!(df.columns[1].col_type, ColumnType::String);
    // age column (integers)
    assert_eq!(df.columns[2].col_type, ColumnType::Integer);
    // salary column (floats)
    assert_eq!(df.columns[3].col_type, ColumnType::Float);
    // department column (strings)
    assert_eq!(df.columns[4].col_type, ColumnType::String);
}

#[test]
fn test_get_value() {
    let df = load_csv(sample_path(), None).expect("Failed to load sample.csv");
    // First row: id=1, name="Alice Johnson"
    assert_eq!(DataFrame::anyvalue_to_string_fmt(&df.get_val(0, 0)), "1");
    assert_eq!(
        DataFrame::anyvalue_to_string_fmt(&df.get_val(0, 1)),
        "Alice Johnson"
    );
}

#[test]
fn test_currency_dirty_float_parsing() {
    use polars::prelude::Series;
    use tuitab::data::column::ColumnMeta;

    let mut df = DataFrame::empty();
    let series = Series::new(
        "Price".into(),
        &["$1,234.56", "€-50.00", "100.00₽", " (10.5) ", "invalid"],
    );
    df.df = polars::prelude::DataFrame::new(vec![series.into()]).unwrap();
    df.columns = vec![ColumnMeta::new("Price".to_string())];

    // Set type to Currency, which should trigger dirty float parsing
    df.set_column_type(0, ColumnType::Currency).unwrap();

    // Check parsed values
    let s = &df.df.get_columns()[0];
    let ca = s.f64().unwrap();

    assert_eq!(ca.get(0), Some(1234.56));
    assert_eq!(ca.get(1), Some(-50.0));
    assert_eq!(ca.get(2), Some(100.0));
    assert_eq!(ca.get(3), Some(10.5));
    assert_eq!(ca.get(4), None); // invalid -> null
}

#[test]
fn test_expression_after_rename() {
    use polars::prelude::Series;
    use tuitab::data::column::ColumnMeta;
    use tuitab::data::expression::Expr;

    let mut df = DataFrame::empty();
    let s1 = Series::new("old_sum".into(), &[10.0, 20.0]);
    let s2 = Series::new("old_count".into(), &[2.0, 4.0]);
    df.df = polars::prelude::DataFrame::new(vec![s1.into(), s2.into()]).unwrap();
    df.columns = vec![
        ColumnMeta::new("old_sum".to_string()),
        ColumnMeta::new("old_count".to_string()),
    ];
    df.columns[0].col_type = ColumnType::Float;
    df.columns[1].col_type = ColumnType::Float;

    // Rename columns
    df.rename_column(0, "_sum_").unwrap();
    df.rename_column(1, "_count_").unwrap();

    // Try to add computed column
    let expr = Expr::parse("_sum_ / _count_").unwrap();
    df.add_computed_column("result", &expr, 1).unwrap();

    let s = df.df.column("result").unwrap();
    let ca = s.f64().unwrap();
    assert_eq!(ca.get(0), Some(5.0));
    assert_eq!(ca.get(1), Some(5.0));
}
