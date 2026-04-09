use std::path::Path;
use tuitab::data::dataframe::DataFrame;
use tuitab::data::loader::load_csv;

fn load_sample() -> DataFrame {
    load_csv(Path::new("test_data/sample.csv"), None).expect("Failed to load sample.csv")
}

fn build_freq_data(df: &DataFrame, col: usize) -> Vec<(String, usize)> {
    let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for row_idx in 0..df.visible_row_count() {
        let val = DataFrame::anyvalue_to_string_fmt(&df.get_val(row_idx, col));
        *counts.entry(val).or_insert(0) += 1;
    }
    let mut freq: Vec<(String, usize)> = counts
        .into_iter()
        .map(|(k, v)| (k, v))
        .collect();
    freq.sort_by(|a, b| b.1.cmp(&a.1));
    freq
}

#[test]
fn test_frequency_departments() {
    let df = load_sample();
    let freq = build_freq_data(&df, 4); // department column

    // There should be 4 unique departments
    assert_eq!(freq.len(), 4, "Expected 4 unique departments");
}

#[test]
fn test_frequency_total_count() {
    let df = load_sample();
    let freq = build_freq_data(&df, 4);

    let total: usize = freq.iter().map(|(_, c)| *c).sum();
    assert_eq!(
        total,
        df.visible_row_count(),
        "Sum of frequency counts should equal total visible rows"
    );
}

#[test]
fn test_frequency_sorted_descending() {
    let df = load_sample();
    let freq = build_freq_data(&df, 4);

    for w in freq.windows(2) {
        assert!(
            w[0].1 >= w[1].1,
            "Frequency table should be sorted by count descending"
        );
    }
}

#[test]
fn test_frequency_engineering_most_common() {
    let df = load_sample();
    let freq = build_freq_data(&df, 4);

    // Engineering has 6 employees in the sample
    assert_eq!(
        freq[0].0, "Engineering",
        "Engineering should be the most frequent department"
    );
}
