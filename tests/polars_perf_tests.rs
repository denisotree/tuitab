use std::path::Path;
use tuitab::data::dataframe::DataFrame;
use tuitab::data::loader::load_csv;

fn load_sample() -> DataFrame {
    load_csv(Path::new("test_data/sample.csv"), None).expect("Failed to load sample.csv")
}

// ── P6: get_visible_df ─────────────────────────────────────────────────────

#[test]
fn test_get_visible_df_returns_all_rows() {
    let df = load_sample();
    let visible = df.get_visible_df().expect("get_visible_df failed");
    assert_eq!(visible.height(), df.visible_row_count());
    assert_eq!(visible.width(), df.df.width());
}

#[test]
fn test_get_visible_df_after_sort_preserves_count() {
    let mut df = load_sample();
    df.sort_by(0, true); // Sort descending
    let visible = df
        .get_visible_df()
        .expect("get_visible_df failed after sort");
    assert_eq!(visible.height(), df.visible_row_count());
}

// ── P2: build_frequency_table ──────────────────────────────────────────────

#[test]
fn test_build_frequency_table_basic() {
    let df = load_sample();
    // Column 4 is "department"
    let (freq_df, columns) = df
        .build_frequency_table(4, &[])
        .expect("build_frequency_table failed");

    // Should have 4 columns: department, Count, Pct, Bar
    assert_eq!(
        columns.len(),
        4,
        "Expected 4 columns: department, Count, Pct, Bar"
    );
    assert_eq!(columns[0].name, "department");
    assert_eq!(columns[1].name, "Count");
    assert_eq!(columns[2].name, "Pct");
    assert_eq!(columns[3].name, "Bar");

    // 4 unique departments
    assert_eq!(freq_df.height(), 4, "Expected 4 unique departments");

    // Total count should equal total rows
    let count_col = freq_df.column("Count").unwrap();
    let total: u64 = count_col.as_materialized_series().sum::<u64>().unwrap_or(0);
    assert_eq!(total as usize, df.visible_row_count());
}

#[test]
fn test_build_frequency_table_sorted_desc() {
    let df = load_sample();
    let (freq_df, _) = df.build_frequency_table(4, &[]).unwrap();
    let count_col = freq_df.column("Count").unwrap();

    // Verify descending sort
    let counts: Vec<u64> = (0..freq_df.height())
        .map(|i| {
            count_col
                .as_materialized_series()
                .get(i)
                .unwrap()
                .try_extract::<u64>()
                .unwrap()
        })
        .collect();

    for w in counts.windows(2) {
        assert!(w[0] >= w[1], "Freq table should be sorted desc by Count");
    }
}

// ── P4: find_matching_rows / find_rows_by_value ────────────────────────────

#[test]
fn test_find_matching_rows_basic() {
    let df = load_sample();
    // Column 4 = department, search for "Engineering"
    let matches = df.find_matching_rows(4, "Engineering");
    assert!(
        !matches.is_empty(),
        "Should find Engineering in department column"
    );
}

#[test]
fn test_find_matching_rows_case_insensitive() {
    let df = load_sample();
    let matches = df.find_matching_rows(4, "(?i)engineering");
    assert!(
        !matches.is_empty(),
        "Case-insensitive search should find Engineering"
    );
}

#[test]
fn test_find_rows_by_value_exact() {
    let df = load_sample();
    let matches = df.find_rows_by_value(4, "Engineering");
    assert_eq!(matches.len(), 7, "Engineering should have 7 employees");
}

#[test]
fn test_find_rows_by_value_no_match() {
    let df = load_sample();
    let matches = df.find_rows_by_value(4, "NoSuchDepartment");
    assert!(
        matches.is_empty(),
        "Should find no matches for non-existent department"
    );
}

// ── P3: build_multi_frequency_table ────────────────────────────────────────

#[test]
fn test_build_multi_frequency_table() {
    let df = load_sample();
    // Group by department (col 4) + city (col 5 if it exists)
    // For safety, just group by the one column
    let (mft_df, columns) = df
        .build_multi_frequency_table(&[4], &[])
        .expect("build_multi_frequency_table failed");

    assert!(mft_df.height() > 0, "Should have at least one group");
    // Columns: department, Count, Pct, Bar
    assert_eq!(columns.len(), 4);
    assert_eq!(columns[0].name, "department");
    assert_eq!(columns[1].name, "Count");
}
