//! ANTI and SEMI joins, NULL keys, and keys whose types cannot meet.

mod keys;

use crossterm::event::KeyCode;
use keys::{key, open, press};
use polars::prelude::{Column, DataType, NamedFrom, Series};
use tuitab::data::column::ColumnMeta;
use tuitab::data::dataframe::DataFrame;
use tuitab::data::join::{join_dataframes, JoinType};
use tuitab::types::{AppMode, ColumnType};

fn frame(cols: Vec<(Series, ColumnType)>) -> DataFrame {
    let metas = cols
        .iter()
        .map(|(s, t)| {
            let mut m = ColumnMeta::new(s.name().to_string());
            m.col_type = *t;
            m
        })
        .collect();
    let cols: Vec<Column> = cols.into_iter().map(|(s, _)| s.into()).collect();
    DataFrame::from_parts(
        polars::prelude::DataFrame::new_infer_height(cols).unwrap(),
        metas,
    )
}

fn ids(values: &[Option<i64>]) -> (Series, ColumnType) {
    (Series::new("id".into(), values), ColumnType::Integer)
}

fn names(values: &[Option<&str>]) -> (Series, ColumnType) {
    (Series::new("name".into(), values), ColumnType::String)
}

fn keys(k: &[&str]) -> Vec<String> {
    k.iter().map(|s| s.to_string()).collect()
}

fn id_column(df: &DataFrame) -> Vec<Option<i64>> {
    df.df
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_iter()
        .collect()
}

#[test]
fn anti_keeps_left_rows_without_a_match_and_only_left_columns() {
    let left = frame(vec![
        ids(&[Some(1), Some(2), Some(3)]),
        names(&[Some("a"), Some("b"), Some("c")]),
    ]);
    let right = frame(vec![ids(&[Some(2)]), names(&[Some("other")])]);

    let out = join_dataframes(
        &left,
        &right,
        &keys(&["id"]),
        &keys(&["id"]),
        JoinType::Anti,
    )
    .unwrap();

    assert_eq!(id_column(&out), vec![Some(1), Some(3)]);
    let cols: Vec<&str> = out.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        cols,
        vec!["id", "name"],
        "no _right columns from an ANTI join"
    );
}

#[test]
fn semi_keeps_left_rows_with_a_match() {
    let left = frame(vec![ids(&[Some(1), Some(2), Some(3)])]);
    let right = frame(vec![ids(&[Some(2), Some(3), Some(3)])]);

    let out = join_dataframes(
        &left,
        &right,
        &keys(&["id"]),
        &keys(&["id"]),
        JoinType::Semi,
    )
    .unwrap();

    assert_eq!(
        id_column(&out),
        vec![Some(2), Some(3)],
        "a repeated match is one row"
    );
}

/// Diffing two tables on every column: a row with an empty cell on both sides is
/// the same row, not a difference.
#[test]
fn anti_treats_null_keys_as_equal() {
    let left = frame(vec![ids(&[Some(1), Some(2)]), names(&[None, Some("b")])]);
    let right = frame(vec![
        ids(&[Some(1), Some(2)]),
        names(&[None, Some("changed")]),
    ]);
    let all = keys(&["id", "name"]);

    let out = join_dataframes(&left, &right, &all, &all, JoinType::Anti).unwrap();
    assert_eq!(id_column(&out), vec![Some(2)]);

    let out = join_dataframes(&left, &right, &all, &all, JoinType::Semi).unwrap();
    assert_eq!(id_column(&out), vec![Some(1)]);
}

#[test]
fn inner_still_does_not_match_null_keys() {
    let left = frame(vec![ids(&[None, Some(1)])]);
    let right = frame(vec![ids(&[None, Some(1)])]);

    let out = join_dataframes(
        &left,
        &right,
        &keys(&["id"]),
        &keys(&["id"]),
        JoinType::Inner,
    )
    .unwrap();

    assert_eq!(id_column(&out), vec![Some(1)]);
}

#[test]
fn keys_of_different_types_are_refused_with_both_types_named() {
    let left = frame(vec![ids(&[Some(1)])]);
    let right = frame(vec![(
        Series::new("id".into(), &[1.0f64]),
        ColumnType::Float,
    )]);

    let Err(err) = join_dataframes(
        &left,
        &right,
        &keys(&["id"]),
        &keys(&["id"]),
        JoinType::Anti,
    ) else {
        panic!("integer against float must be refused");
    };
    let err = err.to_string();

    assert!(err.contains("\"id\""), "{err}");
    assert!(err.contains("integer") && err.contains("float"), "{err}");
}

/// Both sides read as `integer`, so retyping cannot fix this — the join has to.
#[test]
fn integer_keys_of_different_widths_still_join() {
    let left = frame(vec![ids(&[Some(1), Some(2)])]);
    let narrow = Series::new("id".into(), &[2i64])
        .cast(&DataType::Int32)
        .unwrap();
    let right = frame(vec![(narrow, ColumnType::Integer)]);

    let out = join_dataframes(
        &left,
        &right,
        &keys(&["id"]),
        &keys(&["id"]),
        JoinType::Anti,
    )
    .unwrap();

    assert_eq!(id_column(&out), vec![Some(1)]);
}

/// The wizard end to end: ANTI is the fifth type, `a` takes every column as a key,
/// and the right keys follow by name.
#[test]
fn anti_join_of_a_table_with_itself_through_the_wizard() {
    const SAMPLE: &str = "test_data/sample.csv";
    let mut app = open(SAMPLE, "id");

    press(&mut app, "J");
    key(&mut app, KeyCode::Enter); // [Browse file...]
    press(&mut app, SAMPLE);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::JoinSelectType);

    press(&mut app, "jjjj");
    key(&mut app, KeyCode::Enter);
    press(&mut app, "a");
    assert_eq!(app.join.left_keys.len(), 5);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.join.right_keys, app.join.left_keys);
    key(&mut app, KeyCode::Enter);

    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);
    assert!(app.stack.active().title.contains("ANTI JOIN"));
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 0);
}

/// A string key against an integer one stops in the right-key step, says which
/// keys and types clash, and keeps the right table so a retry needs no reload.
#[test]
fn mismatched_key_types_stop_the_wizard_with_a_fix() {
    let mut app = open("test_data/sample.csv", "id");

    press(&mut app, "J");
    key(&mut app, KeyCode::Enter);
    press(&mut app, "test_data/prices.csv");
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter); // INNER
    press(&mut app, "j "); // left key: name
    key(&mut app, KeyCode::Enter);
    press(&mut app, " j "); // right key: id instead of the prefilled name
    key(&mut app, KeyCode::Enter);

    assert_eq!(app.mode, AppMode::JoinSelectRightKeys);
    let msg = &app.status_message;
    assert!(msg.starts_with("JOIN impossible"), "{msg}");
    assert!(
        msg.contains("\"name\"") && msg.contains("string") && msg.contains("integer"),
        "{msg}"
    );
    assert!(app.join.other_df.is_some());
}

// ── DIFF ────────────────────────────────────────────────────────────────────

use tuitab::data::join::diff_dataframes;
use tuitab::data::loader::load_csv;

const MARCH: &str = "test_data/diff_march.csv";
const APRIL: &str = "test_data/diff_april.csv";

fn load(path: &str) -> DataFrame {
    load_csv(std::path::Path::new(path), None).unwrap()
}

fn strings(df: &DataFrame, name: &str) -> Vec<Option<String>> {
    let c = df.df.column(name).unwrap().cast(&DataType::String).unwrap();
    c.str()
        .unwrap()
        .into_iter()
        .map(|v| v.map(str::to_string))
        .collect()
}

fn s(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some(v.to_string())).collect()
}

/// Keyed on every column, a changed row is a removal and an addition, as in git.
#[test]
fn diff_on_every_column_marks_same_removed_and_added() {
    let all = keys(&["id", "name", "city", "amount"]);
    let out = diff_dataframes(&load(MARCH), &load(APRIL), &all, &all).unwrap();

    assert_eq!(out.columns[0].name, "_diff");
    assert!(out.columns[0].pinned);
    assert_eq!(strings(&out, "_diff"), s(&["=", "=", "-", "-", "+", "+"]));
    assert_eq!(strings(&out, "id"), s(&["1", "2", "3", "4", "3", "5"]));
    assert_eq!(
        strings(&out, "amount"),
        s(&["100", "200", "300", "400", "350", "500"])
    );
    assert!(
        out.df.column("amount_right").is_err(),
        "no column is compared when every column is a key"
    );
}

/// Keyed on id, the same row with another amount is one changed row, and the
/// right-hand value sits next to it.  Row 2's empty city on both sides is no change.
#[test]
fn diff_on_a_key_marks_changed_rows_and_keeps_the_right_values() {
    let id = keys(&["id"]);
    let out = diff_dataframes(&load(MARCH), &load(APRIL), &id, &id).unwrap();

    assert_eq!(strings(&out, "_diff"), s(&["=", "=", "~", "-", "+"]));
    assert_eq!(strings(&out, "id"), s(&["1", "2", "3", "4", "5"]));
    let amount_right = strings(&out, "amount_right");
    assert_eq!(amount_right[2], Some("350".to_string()));
    assert!(
        amount_right
            .iter()
            .enumerate()
            .all(|(i, v)| i == 2 || v.is_none()),
        "_right is filled on changed rows only: {amount_right:?}"
    );
    assert_eq!(
        strings(&out, "name")[4],
        Some("Elena".to_string()),
        "an added row's values are in the table's own columns"
    );
}

#[test]
fn a_value_that_became_null_is_a_change() {
    let left = frame(vec![ids(&[Some(1)]), names(&[Some("a")])]);
    let right = frame(vec![ids(&[Some(1)]), names(&[None])]);
    let id = keys(&["id"]);

    let out = diff_dataframes(&left, &right, &id, &id).unwrap();
    assert_eq!(strings(&out, "_diff"), s(&["~"]));
}

#[test]
fn diff_refuses_a_key_that_repeats() {
    let left = frame(vec![ids(&[Some(1), Some(2)])]);
    let right = frame(vec![ids(&[Some(1), Some(1)])]);
    let id = keys(&["id"]);

    let Err(err) = diff_dataframes(&left, &right, &id, &id) else {
        panic!("a repeated key must be refused");
    };
    let err = err.to_string();
    assert!(err.contains("repeats in the right table"), "{err}");
}

#[test]
fn a_column_on_one_side_only_is_kept_but_not_compared() {
    let left = frame(vec![ids(&[Some(1)]), names(&[Some("a")])]);
    let right = frame(vec![
        ids(&[Some(1)]),
        (Series::new("extra".into(), &["x"]), ColumnType::String),
    ]);
    let id = keys(&["id"]);

    let out = diff_dataframes(&left, &right, &id, &id).unwrap();
    assert_eq!(strings(&out, "_diff"), s(&["="]));
    assert_eq!(strings(&out, "extra"), s(&["x"]));
}

#[test]
fn diff_refuses_a_table_that_already_has_a_diff_column() {
    let left = frame(vec![
        ids(&[Some(1)]),
        (Series::new("_diff".into(), &["x"]), ColumnType::String),
    ]);
    let right = frame(vec![ids(&[Some(1)])]);
    let id = keys(&["id"]);

    assert!(diff_dataframes(&left, &right, &id, &id).is_err());
}

fn diff_through_the_wizard() -> tuitab::app::App {
    let mut app = open(MARCH, "id");
    press(&mut app, "J");
    key(&mut app, KeyCode::Enter);
    press(&mut app, APRIL);
    key(&mut app, KeyCode::Enter);
    press(&mut app, "jjjjjj"); // DIFF
    key(&mut app, KeyCode::Enter);
    press(&mut app, " "); // id
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter);
    app
}

#[test]
fn diff_through_the_wizard_reports_the_counts() {
    let app = diff_through_the_wizard();

    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);
    assert!(app.stack.active().title.contains("DIFF"));
    assert_eq!(
        app.status_message,
        "DIFF: 2 same, 1 changed, 1 removed, 1 added"
    );
}

/// The colours are the feature, so read them off a drawn frame.
#[test]
fn diff_rows_are_drawn_in_their_status_colours() {
    use ratatui::{backend::TestBackend, style::Color, Terminal};
    use tuitab::theme::EverforestTheme as T;

    let mut app = diff_through_the_wizard();
    let mut terminal = Terminal::new(TestBackend::new(120, 20)).unwrap();
    terminal.draw(|f| tuitab::ui::render(f, &mut app)).unwrap();
    let buffer = terminal.backend().buffer().clone();

    let find = |text: &str| {
        let cells = buffer.content();
        let n = text.chars().count();
        let at = (0..cells.len().saturating_sub(n))
            .find(|&i| {
                cells[i..i + n]
                    .iter()
                    .map(|c| c.symbol())
                    .collect::<String>()
                    == text
            })
            .unwrap_or_else(|| panic!("{text} not drawn"));
        cells[at].style()
    };

    let fg = |text: &str| find(text).fg.unwrap_or(Color::Reset);
    assert_eq!(fg("Dmitry"), T::RED, "a removed row is red");
    assert_eq!(fg("Elena"), T::GREEN, "an added row is green");
    assert_eq!(fg("Clara"), T::YELLOW, "a changed row is yellow");
    assert_eq!(
        find("300").bg,
        Some(T::YELLOW),
        "the changed cell itself is marked"
    );
    assert_eq!(
        fg("Anna"),
        T::BG_DIM,
        "the cursor row keeps the cursor style"
    );

    let width = buffer.area.width as usize;
    let symbols: Vec<&str> = buffer.content().iter().map(|c| c.symbol()).collect();
    let line = |needle: &str| {
        symbols
            .chunks(width)
            .map(|row| row.concat())
            .find(|row| row.contains(needle))
            .unwrap_or_else(|| panic!("{needle} not drawn"))
    };
    assert!(
        !line("Anna").contains("NULL"),
        "a same row shows no right-hand NULLs"
    );
    let clara = line("Clara");
    assert_eq!(
        clara.matches("Clara").count(),
        1,
        "an unchanged cell of a changed row is not repeated: {clara}"
    );
    assert!(clara.contains("350"), "the changed value is shown: {clara}");
    assert!(
        line("Dmitry").contains("NULL"),
        "a removed row has no right side, and says so"
    );
}
