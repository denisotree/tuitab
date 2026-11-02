//! Group-by and grand totals, and the parity between the two surfaces.

use std::path::Path;
use tuitab::data::aggregator::AggregatorKind;
use tuitab::data::dataframe::DataFrame;
use tuitab::data::group::{group_by, total, AggSpec};
use tuitab::data::io::load_file;

fn sample() -> DataFrame {
    load_file(Path::new("test_data/sample.csv"), None).unwrap()
}

fn spec(col: &str, kind: AggregatorKind) -> AggSpec {
    AggSpec::new(col, kind)
}

fn number(df: &DataFrame, row: usize, col: &str) -> f64 {
    let idx = df.column_index(col).unwrap();
    DataFrame::anyvalue_to_string_fmt(&df.get_val(row, idx))
        .parse()
        .unwrap()
}

#[test]
fn a_grand_total_needs_no_grouping_column() {
    let df = sample();
    let out = total(
        &df,
        &[
            spec("salary", AggregatorKind::Sum),
            spec("*", AggregatorKind::Count),
        ],
    )
    .unwrap();

    assert_eq!(out.visible_row_count(), 1, "a total is one row");
    // The twenty salaries add up to 1,624,003.25.
    assert_eq!(number(&out, 0, "salary:sum"), 1_624_003.25);
    assert_eq!(number(&out, 0, "count"), 20.0);
}

#[test]
fn a_total_respects_a_prior_filter() {
    let mut df = sample();
    // The seven Engineering rows.
    df.row_order = std::sync::Arc::new(vec![0, 2, 4, 7, 11, 14, 18]);

    let out = total(&df, &[spec("salary", AggregatorKind::Sum)]).unwrap();
    assert_eq!(number(&out, 0, "salary:sum"), 563_001.5);
}

#[test]
fn grouping_returns_only_the_requested_aggregates() {
    let df = sample();
    let out = group_by(
        &df,
        &["department".to_string()],
        &[spec("salary", AggregatorKind::Sum)],
    )
    .unwrap();

    let names: Vec<&str> = out.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["department", "salary:sum"],
        "no Count, Pct or Bar that nobody asked for"
    );
    // First appearance order: Engineering leads.
    assert_eq!(number(&out, 0, "salary:sum"), 563_001.5);
}

#[test]
fn grouping_without_a_key_or_without_an_aggregate_is_refused() {
    let df = sample();
    assert!(group_by(&df, &[], &[spec("salary", AggregatorKind::Sum)]).is_err());
    assert!(group_by(&df, &["department".to_string()], &[]).is_err());
    assert!(total(&df, &[]).is_err());
}

#[test]
fn an_aggregate_the_column_cannot_carry_is_refused_by_name() {
    let df = sample();
    let message = match total(&df, &[spec("name", AggregatorKind::Sum)]) {
        Err(e) => e,
        Ok(_) => panic!("summing a column of names must be refused"),
    };
    assert!(message.contains("name"), "{}", message);
    assert!(message.contains("string"), "must say why: {}", message);
}

// ── parity ──────────────────────────────────────────────────────────────────

/// `gb` in the terminal and `group_by` over MCP run the same function, so they
/// must land on the same table.
#[test]
fn the_gb_key_produces_the_same_table_as_the_shared_engine() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new_as(Path::new("test_data/sample.csv"), None, None).unwrap();
    {
        let s = app.stack.active_mut();
        let department = s.dataframe.column_index("department").unwrap();
        let salary = s.dataframe.column_index("salary").unwrap();
        s.dataframe.columns[department].pinned = true;
        s.dataframe
            .add_aggregator(salary, AggregatorKind::Sum)
            .unwrap();
    }

    // The action defers one frame through the Calculating overlay.
    app.handle_action(Action::OpenGroupBy);
    app.handle_action(Action::OpenGroupBy);

    let produced = &app.stack.active().dataframe;
    let expected = group_by(
        &sample(),
        &["department".to_string()],
        &[spec("salary", AggregatorKind::Sum)],
    )
    .unwrap();

    assert_eq!(produced.visible_row_count(), expected.visible_row_count());
    assert_eq!(produced.columns.len(), expected.columns.len());
    for row in 0..expected.visible_row_count() {
        for col in 0..expected.columns.len() {
            assert_eq!(
                DataFrame::anyvalue_to_string_fmt(&produced.get_val(row, col)),
                DataFrame::anyvalue_to_string_fmt(&expected.get_val(row, col)),
                "cell ({}, {})",
                row,
                col
            );
        }
    }
}

#[test]
fn grouping_without_pinned_columns_says_what_to_do() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new_as(Path::new("test_data/sample.csv"), None, None).unwrap();
    app.handle_action(Action::OpenGroupBy);
    app.handle_action(Action::OpenGroupBy);

    assert!(app.status_message.contains("Pin"), "{}", app.status_message);
    assert_eq!(app.stack.depth(), 1, "no sheet is pushed on a refusal");
}

// ── frequency ───────────────────────────────────────────────────────────────

fn gaps() -> DataFrame {
    load_file(Path::new("test_data/gaps.csv"), None).unwrap()
}

/// Counting rows means counting all of them. `Expr::count()` skips nulls, so
/// the group of blank cells reported zero rows — and a zero share of the total.
#[test]
fn a_group_of_blank_cells_is_counted() {
    let df = gaps();
    let out = tuitab::data::group::frequency(&df, &["team".to_string()], &[]).unwrap();

    let team = out.column_index("team").unwrap();
    let count = out.column_index("Count").unwrap();

    let blank_row = (0..out.visible_row_count())
        .find(|r| DataFrame::anyvalue_to_string_fmt(&out.get_val(*r, team)).is_empty())
        .expect("the blank team must appear as a group");

    assert_eq!(
        number(&out, blank_row, "Count"),
        2.0,
        "Carol and the fifth row have no team"
    );
    let _ = count;
}

/// The same question must give the same answer twice. `group_by` returns groups
/// in whatever order the hash table produced, and sorting by count alone leaves
/// ties to fall where they may.
#[test]
fn a_frequency_table_is_ordered_the_same_way_every_time() {
    let df = sample();
    let first = tuitab::data::group::frequency(&df, &["department".to_string()], &[]).unwrap();

    for _ in 0..8 {
        let again = tuitab::data::group::frequency(&df, &["department".to_string()], &[]).unwrap();
        for row in 0..first.visible_row_count() {
            assert_eq!(
                DataFrame::anyvalue_to_string_fmt(&first.get_val(row, 0)),
                DataFrame::anyvalue_to_string_fmt(&again.get_val(row, 0)),
                "row {} moved between runs",
                row
            );
        }
    }

    // HR and Marketing both have four. The tie breaks by first appearance in
    // the data — Marketing on row 4, HR on row 7 — which is stable and means
    // something, unlike the hash order it used to take.
    let names: Vec<String> = (0..first.visible_row_count())
        .map(|r| DataFrame::anyvalue_to_string_fmt(&first.get_val(r, 0)))
        .collect();
    assert_eq!(names, vec!["Engineering", "Management", "Marketing", "HR"]);
}

/// `frequency` promises to refuse an aggregate a column cannot carry. It then
/// silently dropped any aggregate naming one of the grouping columns.
#[test]
fn an_aggregate_over_the_grouping_column_is_not_dropped_in_silence() {
    let df = sample();
    let out = tuitab::data::group::frequency(
        &df,
        &["department".to_string()],
        &[spec("department", AggregatorKind::Distinct)],
    );

    match out {
        Err(message) => assert!(message.contains("department"), "{}", message),
        Ok(table) => assert!(
            table.column_index("department:distinct").is_ok(),
            "either compute it or refuse — not neither: {:?}",
            table.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
        ),
    }
}
