#[cfg(test)]
mod tests {
    use crate::data::io::format_file_size_pub;

    #[test]
    fn test_format_file_size() {
        assert_eq!(format_file_size_pub(0), "0 B");
        assert_eq!(format_file_size_pub(512), "512 B");
        assert_eq!(format_file_size_pub(1024), "1.0 KB");
        assert_eq!(format_file_size_pub(1536), "1.5 KB");
        assert_eq!(format_file_size_pub(1024 * 1024), "1.0 MB");
        assert_eq!(format_file_size_pub(1024 * 1024 * 1024), "1.0 GB");
    }

    fn make_test_df() -> crate::data::dataframe::DataFrame {
        use crate::data::column::ColumnMeta;
        use crate::data::dataframe::DataFrame;
        use crate::types::ColumnType;
        use std::collections::HashSet;
        use std::sync::Arc;

        let pdf = polars::prelude::DataFrame::new(
            5,
            vec![
                polars::prelude::Column::new("cat".into(), vec!["A", "A", "B", "B", "B"]),
                polars::prelude::Column::new("val".into(), vec![10i64, 20, 30, 40, 50]),
            ],
        )
        .unwrap();

        let row_count = pdf.height();
        let mut columns = Vec::new();
        let mut cm0 = ColumnMeta::new("cat".to_string());
        cm0.col_type = ColumnType::String;
        columns.push(cm0);
        let mut cm1 = ColumnMeta::new("val".to_string());
        cm1.col_type = ColumnType::Integer;
        columns.push(cm1);

        let row_order: Vec<usize> = (0..row_count).collect();
        let original_order = row_order.clone();
        DataFrame {
            df: pdf,
            columns,
            row_order: Arc::new(row_order),
            original_order: Arc::new(original_order),
            selected_rows: HashSet::new(),
            modified: false,
            aggregates_cache: None,
        }
    }

    #[test]
    fn test_avg_median_aggregators() {
        use crate::data::aggregator::AggregatorKind;

        let mut tdf = make_test_df();

        // Add Avg and Median aggregators to column 1 (val, Integer)
        tdf.add_aggregator(1, AggregatorKind::Avg).unwrap();
        tdf.add_aggregator(1, AggregatorKind::Median).unwrap();

        let aggs = tdf.compute_aggregates();
        assert_eq!(aggs.len(), 2);
        // aggs[1] should have (Avg, non-empty) and (Median, non-empty)
        let col_aggs = &aggs[1];
        assert!(
            !col_aggs.is_empty(),
            "Expected aggregators for column 1 but got none"
        );
        let avg_entry = col_aggs.iter().find(|(k, _)| *k == AggregatorKind::Avg);
        let med_entry = col_aggs.iter().find(|(k, _)| *k == AggregatorKind::Median);
        assert!(avg_entry.is_some(), "Avg aggregator result missing");
        assert!(med_entry.is_some(), "Median aggregator result missing");
        let (_, avg_val) = avg_entry.unwrap();
        let (_, med_val) = med_entry.unwrap();
        assert!(
            !avg_val.is_empty(),
            "Avg value is empty; expected '30' or similar"
        );
        assert!(
            !med_val.is_empty(),
            "Median value is empty; expected '30' or similar"
        );
        // mean of [10,20,30,40,50] = 30.0
        assert!(avg_val.contains("30"), "Avg should be 30, got: {}", avg_val);
        // median of [10,20,30,40,50] = 30.0
        assert!(
            med_val.contains("30"),
            "Median should be 30, got: {}",
            med_val
        );
    }

    #[test]
    fn test_freq_table_avg_median() {
        use crate::data::aggregator::AggregatorKind;

        let mut tdf = make_test_df();
        tdf.add_aggregator(1, AggregatorKind::Avg).unwrap();
        tdf.add_aggregator(1, AggregatorKind::Median).unwrap();

        let aggregated_cols: Vec<(usize, Vec<AggregatorKind>)> = tdf
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.aggregators.is_empty())
            .map(|(i, c)| (i, c.aggregators.clone()))
            .collect();

        let result = tdf.build_frequency_table(0, &aggregated_cols);
        assert!(
            result.is_ok(),
            "build_frequency_table failed: {:?}",
            result.err()
        );

        let (freq_df, _cols) = result.unwrap();
        // Should have: cat, Count, val:avg, val:median, Pct, Bar
        assert!(
            freq_df.width() >= 4,
            "Expected at least 4 columns, got {}",
            freq_df.width()
        );

        let col_names_raw = freq_df.get_column_names();
        let col_names: Vec<&str> = col_names_raw.iter().map(|s| s.as_str()).collect();
        assert!(
            col_names.contains(&"val:avg"),
            "val:avg column missing, got: {:?}",
            col_names
        );
        assert!(
            col_names.contains(&"val:median"),
            "val:median column missing, got: {:?}",
            col_names
        );
    }

    #[test]
    fn test_col_replace_literal() {
        let mut tdf = make_test_df();
        tdf.col_replace(0, "A", "X", true).unwrap();
        let series = tdf
            .df
            .column("cat")
            .unwrap()
            .as_materialized_series()
            .clone();
        let values: Vec<String> = series
            .str()
            .unwrap()
            .iter()
            .map(|o| o.unwrap_or("").to_string())
            .collect();
        assert_eq!(values, vec!["X", "X", "B", "B", "B"]);
    }

    #[test]
    fn test_col_replace_regex() {
        let mut tdf = make_test_df();
        // [A-B] regex hits everything; "A" → "X", "B" → "X"
        tdf.col_replace(0, "[AB]", "X", false).unwrap();
        let series = tdf
            .df
            .column("cat")
            .unwrap()
            .as_materialized_series()
            .clone();
        let values: Vec<String> = series
            .str()
            .unwrap()
            .iter()
            .map(|o| o.unwrap_or("").to_string())
            .collect();
        assert_eq!(values, vec!["X", "X", "X", "X", "X"]);
    }

    #[test]
    fn test_col_split_basic() {
        use crate::data::column::ColumnMeta;
        use crate::data::dataframe::DataFrame;
        use crate::types::ColumnType;
        use std::collections::HashSet;
        use std::sync::Arc;

        let pdf = polars::prelude::DataFrame::new(
            3,
            vec![polars::prelude::Column::new(
                "path".into(),
                vec!["a/b/c", "x/y", "1/2/3/4"],
            )],
        )
        .unwrap();
        let mut cm = ColumnMeta::new("path".to_string());
        cm.col_type = ColumnType::String;
        let row_order: Vec<usize> = (0..3).collect();
        let mut tdf = DataFrame {
            df: pdf,
            columns: vec![cm],
            row_order: Arc::new(row_order.clone()),
            original_order: Arc::new(row_order),
            selected_rows: HashSet::new(),
            modified: false,
            aggregates_cache: None,
        };

        let n = tdf.col_split(0, "/").unwrap();
        assert_eq!(n, 4);
        let names: Vec<String> = tdf.columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(names, vec!["path", "path.1", "path.2", "path.3", "path.4"]);

        // Check that values landed in the right columns
        let p1: Vec<String> = tdf
            .df
            .column("path.1")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .iter()
            .map(|o| o.unwrap_or("").to_string())
            .collect();
        assert_eq!(p1, vec!["a", "x", "1"]);

        let p4: Vec<Option<String>> = tdf
            .df
            .column("path.4")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .iter()
            .map(|o| o.map(String::from))
            .collect();
        // First two rows have <4 parts → null; third has "4"
        assert_eq!(p4, vec![None, None, Some("4".to_string())]);
    }

    #[test]
    fn test_col_split_name_collision() {
        use crate::data::column::ColumnMeta;
        use crate::data::dataframe::DataFrame;
        use crate::types::ColumnType;
        use std::collections::HashSet;
        use std::sync::Arc;

        // Pre-existing "path.1" column should force unique-suffix on the new parts.
        let pdf = polars::prelude::DataFrame::new(
            2,
            vec![
                polars::prelude::Column::new("path".into(), vec!["a/b", "c/d"]),
                polars::prelude::Column::new("path.1".into(), vec!["pre1", "pre2"]),
            ],
        )
        .unwrap();
        let mut cm0 = ColumnMeta::new("path".to_string());
        cm0.col_type = ColumnType::String;
        let mut cm1 = ColumnMeta::new("path.1".to_string());
        cm1.col_type = ColumnType::String;
        let row_order: Vec<usize> = (0..2).collect();
        let mut tdf = DataFrame {
            df: pdf,
            columns: vec![cm0, cm1],
            row_order: Arc::new(row_order.clone()),
            original_order: Arc::new(row_order),
            selected_rows: HashSet::new(),
            modified: false,
            aggregates_cache: None,
        };

        let n = tdf.col_split(0, "/").unwrap();
        assert_eq!(n, 2);
        let names: Vec<String> = tdf.columns.iter().map(|c| c.name.clone()).collect();
        // "path.1" is taken → suffix becomes "path.1_2"; "path.2" is free
        assert_eq!(names, vec!["path", "path.1_2", "path.2", "path.1"]);

        // Verify the original "path.1" wasn't overwritten
        let preserved: Vec<String> = tdf
            .df
            .column("path.1")
            .unwrap()
            .as_materialized_series()
            .str()
            .unwrap()
            .iter()
            .map(|o| o.unwrap_or("").to_string())
            .collect();
        assert_eq!(preserved, vec!["pre1", "pre2"]);
    }

    #[test]
    fn test_filesize_format_display() {
        use crate::data::column::ColumnMeta;
        use crate::data::dataframe::DataFrame;
        use crate::types::ColumnType;
        use std::collections::HashSet;
        use std::sync::Arc;

        let pdf = polars::prelude::DataFrame::new(
            3,
            vec![polars::prelude::Column::new(
                "size".into(),
                vec![Some(0i64), Some(1024 * 1024), Some(1024 * 1024 * 1024 * 2)],
            )],
        )
        .unwrap();
        let mut cm = ColumnMeta::new("size".to_string());
        cm.col_type = ColumnType::FileSize;
        let row_order: Vec<usize> = (0..3).collect();
        let tdf = DataFrame {
            df: pdf,
            columns: vec![cm],
            row_order: Arc::new(row_order.clone()),
            original_order: Arc::new(row_order),
            selected_rows: HashSet::new(),
            modified: false,
            aggregates_cache: None,
        };

        assert_eq!(tdf.format_display(0, 0), "0 B");
        assert_eq!(tdf.format_display(1, 0), "1.0 MB");
        assert_eq!(tdf.format_display(2, 0), "2.0 GB");
    }

    #[test]
    fn test_set_cells_bulk() {
        use std::collections::HashSet;

        let mut tdf = make_test_df();
        // Bulk-set rows 1 and 3 of "cat" column to "Z"
        let selected: HashSet<usize> = [1usize, 3].iter().copied().collect();
        let n = tdf.set_cells_bulk(&selected, 0, "Z".to_string()).unwrap();
        assert_eq!(n, 2);

        let series = tdf
            .df
            .column("cat")
            .unwrap()
            .as_materialized_series()
            .clone();
        let values: Vec<String> = series
            .str()
            .unwrap()
            .iter()
            .map(|o| o.unwrap_or("").to_string())
            .collect();
        assert_eq!(values, vec!["A", "Z", "B", "Z", "B"]);
    }

    #[test]
    fn test_col_split_non_string_column() {
        // Splitting an Integer column should auto-cast to String first.
        let mut tdf = make_test_df();
        // val column is Integer with values [10, 20, 30, 40, 50] — no delimiter.
        // Use "0" as delimiter — every value contains "0".
        let n = tdf.col_split(1, "0").unwrap();
        assert_eq!(n, 2);
    }
}
