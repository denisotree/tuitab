//! Window functions: rank, running totals, neighbours and partition shares.

use std::path::Path;
use std::sync::Arc;
use tuitab::data::dataframe::DataFrame;
use tuitab::data::io::load_file;
use tuitab::data::window::{add_window_column, Spec, WindowFn};

fn sample() -> DataFrame {
    load_file(Path::new("test_data/sample.csv"), None).unwrap()
}

fn spec(function: WindowFn, col: Option<&str>, over: &[&str]) -> Spec {
    Spec {
        function,
        col: col.map(str::to_string),
        over: over.iter().map(|s| s.to_string()).collect(),
        order_by: Vec::new(),
        as_name: None,
        desc: false,
        offset: 1,
    }
}

fn values(df: &DataFrame, column: &str) -> Vec<String> {
    let col = df.column_index(column).unwrap();
    (0..df.visible_row_count())
        .map(|r| DataFrame::anyvalue_to_string_fmt(&df.get_val(r, col)))
        .collect()
}

fn numbers(df: &DataFrame, column: &str) -> Vec<f64> {
    values(df, column)
        .iter()
        .map(|v| v.parse().unwrap_or(f64::NAN))
        .collect()
}

#[test]
fn row_number_counts_within_each_partition() {
    let df = sample();
    let out = add_window_column(&df, &spec(WindowFn::RowNumber, None, &["department"])).unwrap();

    let ranks = numbers(&out, "row_number");
    // Row 0 is the first Engineering row, row 1 the first Management row.
    assert_eq!(ranks[0], 1.0);
    assert_eq!(ranks[1], 1.0);
    // Row 2 (Carol) is the second Engineering row.
    assert_eq!(ranks[2], 2.0);
}

#[test]
fn rank_orders_by_value_inside_the_partition() {
    let df = sample();
    let mut s = spec(WindowFn::Rank, Some("salary"), &["department"]);
    s.desc = true;
    let out = add_window_column(&df, &s).unwrap();

    let ranks = numbers(&out, "salary:rank");
    let salaries = numbers(&out, "salary");
    let departments = values(&out, "department");

    // Henry Moore, 105000, is the best-paid engineer.
    let henry = salaries
        .iter()
        .position(|s| (*s - 105_000.0).abs() < 1e-9)
        .unwrap();
    assert_eq!(departments[henry], "Engineering");
    assert_eq!(ranks[henry], 1.0);

    // Seven engineers, so their ranks run 1 to 7.
    let mut engineering: Vec<f64> = ranks
        .iter()
        .zip(&departments)
        .filter(|(_, d)| *d == "Engineering")
        .map(|(r, _)| *r)
        .collect();
    engineering.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(engineering, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]);
}

/// A running total is only meaningful once the rows are in an order, so this
/// sorts first — which is the contract the module documents.
#[test]
fn a_running_total_follows_the_current_row_order() {
    let mut df = sample();
    let age = df.column_index("age").unwrap();
    df.sort_by(age, false);

    let out = add_window_column(&df, &spec(WindowFn::CumSum, Some("age"), &[])).unwrap();
    let running = numbers(&out, "age:cum_sum");
    let ages = numbers(&out, "age");

    assert_eq!(running[0], ages[0], "the first row totals itself");
    for i in 1..running.len() {
        assert!(
            (running[i] - (running[i - 1] + ages[i])).abs() < 1e-9,
            "row {} breaks the running total",
            i
        );
    }
    // All twenty ages sum to 733.
    assert_eq!(*running.last().unwrap(), 733.0);
}

#[test]
fn a_running_total_restarts_in_each_partition() {
    let df = sample();
    let out = add_window_column(
        &df,
        &spec(WindowFn::CumSum, Some("salary"), &["department"]),
    )
    .unwrap();

    let running = numbers(&out, "salary:cum_sum");
    let salaries = numbers(&out, "salary");
    // Row 0 starts Engineering and row 1 starts Management: each totals itself.
    assert_eq!(running[0], salaries[0]);
    assert_eq!(running[1], salaries[1]);
}

#[test]
fn lag_and_lead_reach_the_neighbouring_rows() {
    let df = sample();

    let lagged = add_window_column(&df, &spec(WindowFn::Lag, Some("age"), &[])).unwrap();
    let ages = numbers(&df, "age");
    assert_eq!(
        values(&lagged, "age:lag")[0],
        "",
        "nothing precedes the first row"
    );
    assert_eq!(numbers(&lagged, "age:lag")[1], ages[0]);

    let led = add_window_column(&df, &spec(WindowFn::Lead, Some("age"), &[])).unwrap();
    assert_eq!(numbers(&led, "age:lead")[0], ages[1]);
    assert_eq!(
        values(&led, "age:lead").last().unwrap(),
        "",
        "nothing follows the last row"
    );
}

#[test]
fn a_partition_share_sums_to_one_within_each_partition() {
    let df = sample();
    let out = add_window_column(
        &df,
        &spec(WindowFn::PctOfTotal, Some("salary"), &["department"]),
    )
    .unwrap();

    let shares = numbers(&out, "salary:pct_of_total");
    let departments = values(&out, "department");

    for wanted in ["Engineering", "Management", "Marketing", "HR"] {
        let total: f64 = shares
            .iter()
            .zip(&departments)
            .filter(|(_, d)| *d == wanted)
            .map(|(s, _)| *s)
            .sum();
        assert!(
            (total - 1.0).abs() < 1e-9,
            "{} shares total {} rather than 1",
            wanted,
            total
        );
    }
}

#[test]
fn a_partition_aggregate_repeats_on_every_row_of_its_group() {
    let df = sample();
    let out =
        add_window_column(&df, &spec(WindowFn::Sum, Some("salary"), &["department"])).unwrap();

    let totals = numbers(&out, "salary:sum");
    let departments = values(&out, "department");
    for (total, department) in totals.iter().zip(&departments) {
        if department == "Engineering" {
            assert_eq!(*total, 563_001.5);
        }
    }
}

#[test]
fn a_window_sees_only_the_visible_rows() {
    let mut df = sample();
    // The seven Engineering rows.
    df.row_order = Arc::new(vec![0, 2, 4, 7, 11, 14, 18]);

    let out = add_window_column(&df, &spec(WindowFn::Sum, Some("salary"), &[])).unwrap();
    assert_eq!(out.visible_row_count(), 7);
    assert_eq!(
        numbers(&out, "salary:sum")[0],
        563_001.5,
        "a filtered-out row must not join the total"
    );
}

#[test]
fn a_numeric_window_over_a_text_column_is_refused() {
    let df = sample();
    let failure = add_window_column(&df, &spec(WindowFn::CumSum, Some("name"), &[]));
    let message = match failure {
        Err(e) => e,
        Ok(_) => panic!("a running total of names must be refused"),
    };
    assert!(message.contains("name"), "{}", message);
    assert!(message.contains("string"), "{}", message);
}

#[test]
fn an_unknown_column_or_partition_is_refused() {
    let df = sample();
    assert!(add_window_column(&df, &spec(WindowFn::Sum, Some("bonus"), &[])).is_err());
    assert!(add_window_column(&df, &spec(WindowFn::Sum, Some("salary"), &["team"])).is_err());
}

// ── parity ──────────────────────────────────────────────────────────────────

/// `zf` in the terminal and `{"window": {"fn": "pct_of_total"}}` over MCP run
/// the same function, so the column they produce must match.
#[test]
fn the_zf_key_produces_the_shared_window() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new_as(Path::new("test_data/sample.csv"), None, None).unwrap();
    let salary = app.stack.active().dataframe.column_index("salary").unwrap();
    app.stack.active_mut().cursor_col = salary;
    app.handle_action(Action::CreatePctColumn);

    let produced = &app.stack.active().dataframe;
    let shares = numbers(produced, "salary_pct_of_total");
    assert_eq!(shares.len(), 20);

    let total: f64 = shares.iter().sum();
    assert!(
        (total - 1.0).abs() < 1e-9,
        "shares of the whole table must total 1, got {}",
        total
    );

    // Cell for cell against the shared engine.
    let expected = add_window_column(
        &sample(),
        &Spec {
            function: WindowFn::PctOfTotal,
            col: Some("salary".to_string()),
            over: Vec::new(),
            order_by: Vec::new(),
            as_name: Some("salary_pct_of_total".to_string()),
            desc: false,
            offset: 1,
        },
    )
    .unwrap();
    assert_eq!(shares, numbers(&expected, "salary_pct_of_total"));
}

/// `zw` must reach every function the MCP server offers — otherwise the two
/// surfaces drift, which is the thing this whole layer exists to prevent.
#[test]
fn the_zw_picker_offers_every_window_function() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new_as(Path::new("test_data/sample.csv"), None, None).unwrap();
    let salary = app.stack.active().dataframe.column_index("salary").unwrap();
    app.stack.active_mut().cursor_col = salary;

    let all = WindowFn::all();
    assert_eq!(
        all.len(),
        14,
        "the picker lists whatever WindowFn::all does"
    );

    // Walk to `cum_sum`, third in the list, and apply it with no partition.
    app.handle_action(Action::OpenWindowFnSelect);
    for _ in 0..3 {
        app.handle_action(Action::WindowFnSelectDown);
    }
    assert_eq!(all[app.window_fn.select_index], WindowFn::CumSum);

    app.handle_action(Action::ApplyWindowFnSelect);
    app.handle_action(Action::ApplyPartitionedPct);

    let produced = &app.stack.active().dataframe;
    let running = numbers(produced, "salary_cum_sum");
    assert_eq!(running.len(), 20);
    assert_eq!(
        *running.last().unwrap(),
        1_624_003.25,
        "the running total ends at the grand total"
    );
}

/// Picking a function and cancelling out must not leave it armed for the next
/// `zF`, which would silently apply the wrong window.
#[test]
fn a_cancelled_picker_does_not_arm_the_next_partitioned_share() {
    use tuitab::types::Action;

    let mut app = tuitab::app::App::new_as(Path::new("test_data/sample.csv"), None, None).unwrap();
    let salary = app.stack.active().dataframe.column_index("salary").unwrap();
    app.stack.active_mut().cursor_col = salary;

    app.handle_action(Action::OpenWindowFnSelect);
    app.handle_action(Action::WindowFnSelectDown);
    app.handle_action(Action::CancelWindowFnSelect);
    assert!(app.pending_window_fn.is_none(), "cancelling must disarm it");

    // zF now behaves as itself.
    app.handle_action(Action::OpenPartitionSelect);
    app.handle_action(Action::ApplyPartitionedPct);
    assert!(
        app.stack
            .active()
            .dataframe
            .column_index("salary_pct_of_total")
            .is_ok(),
        "zF must still produce a share, not a leftover function"
    );
}

/// Only a share is a percentage. An average is in the column's own units —
/// typing it otherwise renders an average salary of 80428.79 as "8042878.57%".
#[test]
fn an_average_is_not_a_percentage() {
    use tuitab::types::ColumnType;

    let df = sample();
    let out =
        add_window_column(&df, &spec(WindowFn::Avg, Some("salary"), &["department"])).unwrap();
    let i = out.column_index("salary:avg").unwrap();
    assert_eq!(out.columns[i].col_type, ColumnType::Float);
    assert!(
        !out.format_display(out.row_order[0], i).contains('%'),
        "an average must not render as a percentage"
    );

    let share = add_window_column(
        &df,
        &spec(WindowFn::PctOfTotal, Some("salary"), &["department"]),
    )
    .unwrap();
    let j = share.column_index("salary:pct_of_total").unwrap();
    assert_eq!(share.columns[j].col_type, ColumnType::Percentage);
}

// ── a new column must not collide with an existing one ──────────────────────

/// `with_column` replaces a column of the same name, but the metadata was
/// pushed regardless — leaving one more header than there are cells in a row.
/// The model then reads a table whose columns do not line up with its values.
#[test]
fn a_window_column_may_not_take_an_existing_name() {
    let df = sample();
    let mut s = spec(WindowFn::Rank, Some("age"), &[]);
    s.as_name = Some("age".to_string());

    let outcome = add_window_column(&df, &s);
    match outcome {
        Err(message) => assert!(message.contains("age"), "say which name: {}", message),
        Ok(out) => panic!(
            "a colliding name must be refused; got {} columns for a frame {} wide",
            out.columns.len(),
            out.df.width()
        ),
    }
}

/// Two windows in a row default to `col:fn`, so asking for the same one twice
/// collides just as surely.
#[test]
fn the_same_window_twice_is_refused_rather_than_desynced() {
    let df = sample();
    let once = add_window_column(&df, &spec(WindowFn::Rank, Some("age"), &[])).unwrap();
    assert_eq!(once.columns.len(), once.df.width());

    assert!(
        add_window_column(&once, &spec(WindowFn::Rank, Some("age"), &[])).is_err(),
        "the second must not silently overwrite the first"
    );
}

/// Whatever a frame comes out of, its metadata and its columns describe the
/// same table.
#[test]
fn metadata_and_columns_stay_in_step() {
    let df = sample();
    for function in [WindowFn::Rank, WindowFn::CumSum, WindowFn::PctOfTotal] {
        let out = add_window_column(&df, &spec(function, Some("salary"), &["department"])).unwrap();
        assert_eq!(
            out.columns.len(),
            out.df.width(),
            "{} left the metadata and the frame disagreeing",
            function.name()
        );
    }
}

// ── ORDER BY inside the window ──────────────────────────────────────────────

/// `growth.csv` is deliberately not in date order, so a running total that
/// reads the file as it stands and one that reads it by date differ on every
/// row.
fn growth() -> DataFrame {
    load_file(Path::new("test_data/growth.csv"), None).unwrap()
}

fn ordered(function: WindowFn, over: &[&str], order: &[&str], desc: bool) -> Spec {
    Spec {
        function,
        col: Some("amount".to_string()),
        over: over.iter().map(|s| s.to_string()).collect(),
        order_by: order.iter().map(|s| s.to_string()).collect(),
        as_name: Some("w".to_string()),
        desc,
        offset: 1,
    }
}

/// The whole point: totalled by date, returned where the rows already are.
#[test]
fn a_running_total_can_be_ordered_without_reordering_the_table() {
    let df = growth();
    let out =
        add_window_column(&df, &ordered(WindowFn::CumSum, &["dept"], &["date"], false)).unwrap();

    // Row 0 is eng/2026-03-01, the last eng date, so it carries all of eng.
    // Row 2 is eng/2026-01-10, the first, so it carries only itself.
    assert_eq!(numbers(&out, "w"), vec![60.0, 7.0, 10.0, 24.0, 30.0, 15.0]);
    assert_eq!(
        values(&out, "date"),
        values(&df, "date"),
        "the table's own order must be untouched"
    );
}

/// Without it, the same request totals in the order the file was written —
/// the behaviour every existing test depends on.
#[test]
fn without_an_order_the_running_total_reads_the_frame_as_it_stands() {
    let df = growth();
    let out = add_window_column(&df, &ordered(WindowFn::CumSum, &["dept"], &[], false)).unwrap();
    assert_eq!(numbers(&out, "w"), vec![30.0, 7.0, 40.0, 16.0, 60.0, 24.0]);
}

/// Descending runs the order the other way: the newest date starts the total.
#[test]
fn an_ordered_window_can_run_backwards() {
    let df = growth();
    let out =
        add_window_column(&df, &ordered(WindowFn::CumSum, &["dept"], &["date"], true)).unwrap();
    assert_eq!(numbers(&out, "w"), vec![30.0, 24.0, 60.0, 9.0, 50.0, 17.0]);
}

/// `lag` is a separate question from `cum_sum`: `shift` inside a partition on a
/// re-ordered frame has to map back the same way.
#[test]
fn lag_reaches_the_previous_row_in_date_order() {
    let df = growth();
    let out = add_window_column(&df, &ordered(WindowFn::Lag, &["dept"], &["date"], false)).unwrap();

    // The first date in each department has nothing before it.
    let got = values(&out, "w");
    assert_eq!(got[2], "", "eng's earliest date has no predecessor");
    assert_eq!(got[1], "", "sales's earliest date has no predecessor");
    assert_eq!(got[0], "20", "eng 03-01 follows eng 02-05");
    assert_eq!(got[4], "10", "eng 02-05 follows eng 01-10");
}

/// And `row_number` numbers by date rather than by file position.
#[test]
fn row_number_counts_in_the_order_it_was_given() {
    let df = growth();
    let mut spec = ordered(WindowFn::RowNumber, &["dept"], &["date"], false);
    spec.col = None;
    let out = add_window_column(&df, &spec).unwrap();
    assert_eq!(numbers(&out, "w"), vec![3.0, 1.0, 1.0, 3.0, 2.0, 2.0]);
}

/// A function that reads no order must not accept one. In SQL
/// `RANK() OVER (ORDER BY x)` ranks *by* x, so the request is a reasonable one
/// to make — and dropping it silently would answer a different question.
#[test]
fn a_function_that_ignores_order_refuses_it_rather_than_dropping_it() {
    let df = growth();
    let err = add_window_column(&df, &ordered(WindowFn::Sum, &[], &["date"], false))
        .err()
        .expect("sum must refuse an order");
    assert!(err.contains("sum"), "the message must name it: {}", err);
    assert!(err.contains("order"), "and say what was wrong: {}", err);

    let err = add_window_column(&df, &ordered(WindowFn::Rank, &[], &["date"], false))
        .err()
        .expect("rank must refuse an order");
    assert!(
        err.contains("rank"),
        "ranks read a value, not an order: {}",
        err
    );
}

/// An unknown order column is caught before anything is computed.
#[test]
fn an_order_column_that_does_not_exist_is_refused() {
    let df = growth();
    let err = add_window_column(&df, &ordered(WindowFn::CumSum, &[], &["nope"], false))
        .err()
        .expect("an unknown column must be refused");
    assert!(err.contains("nope"), "{}", err);
}
