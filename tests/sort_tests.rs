use std::path::Path;
use tuitab::data::dataframe::DataFrame;
use tuitab::data::loader::load_csv;

fn load_sample() -> DataFrame {
    load_csv(Path::new("test_data/sample.csv"), None).expect("Failed to load sample.csv")
}

#[test]
fn test_sort_integer_ascending() {
    let mut df = load_sample();
    df.sort_by(0, false); // sort by 'id' ascending
    let values: Vec<String> = (0..df.visible_row_count())
        .map(|i| DataFrame::anyvalue_to_string_fmt(&df.get_val(i, 0)))
        .collect();
    let ids: Vec<i64> = values.iter().map(|s| s.parse::<i64>().unwrap()).collect();
    let mut expected = ids.clone();
    expected.sort();
    assert_eq!(ids, expected, "id column should be sorted ascending");
}

#[test]
fn test_sort_integer_descending() {
    let mut df = load_sample();
    df.sort_by(2, true); // sort by 'age' descending
    let first_age: i64 = DataFrame::anyvalue_to_string_fmt(&df.get_val(0, 2)).parse::<i64>().unwrap();
    let last_age: i64 = DataFrame::anyvalue_to_string_fmt(&df.get_val(df.visible_row_count() - 1, 2)).parse::<i64>().unwrap();
    assert!(
        first_age >= last_age,
        "First age ({}) should be >= last age ({}) in descending sort",
        first_age,
        last_age
    );
}

#[test]
fn test_sort_float() {
    let mut df = load_sample();
    df.sort_by(3, false); // sort by 'salary' ascending
    let values: Vec<f64> = (0..df.visible_row_count())
        .map(|i| DataFrame::anyvalue_to_string_fmt(&df.get_val(i, 3)).parse::<f64>().unwrap())
        .collect();
    for w in values.windows(2) {
        assert!(w[0] <= w[1], "salary values should be non-decreasing");
    }
}

#[test]
fn test_reset_sort() {
    let mut df = load_sample();
    let original_first = DataFrame::anyvalue_to_string_fmt(&df.get_val(0, 0));
    df.sort_by(2, true); // sort by age desc
    df.reset_sort();
    assert_eq!(
        DataFrame::anyvalue_to_string_fmt(&df.get_val(0, 0)),
        original_first,
        "After reset, first row should be back to original"
    );
}
