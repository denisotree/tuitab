//! The new-row form (`O`) — the one insert path that checks a value before storing it.
//!
//! The regression these guard is quiet: `set_cell` answers a value the column cannot
//! hold by turning the whole column into text, so a table loaded as Int64 comes back
//! from a save as strings and nobody is told. The form exists to refuse that value
//! while the user is still looking at it, and these tests fail if it stops refusing —
//! or if the row it does insert lands with the wrong dtype.

mod keys;

use crossterm::event::KeyCode;
use keys::{key, open, press};
use tuitab::app::App;
use tuitab::types::{AppMode, ColumnType};

const TYPED: &str = "test_data/typed.csv";

/// Every one of the nine column types on its own column.
///
/// A CSV load only ever infers Integer, Float and String — Boolean, Date, Datetime,
/// Percentage, Currency and FileSize arrive by assignment (`t`) and nothing else. A
/// fixture that skipped this step would test the three easy types and miss the six
/// that actually go through a cast.
const TYPES: &[(&str, ColumnType)] = &[
    ("text", ColumnType::String),
    ("count", ColumnType::Integer),
    ("ratio", ColumnType::Float),
    ("day", ColumnType::Date),
    ("moment", ColumnType::Datetime),
    ("flag", ColumnType::Boolean),
    ("share", ColumnType::Percentage),
    ("price", ColumnType::Currency),
    ("bytes", ColumnType::FileSize),
];

fn typed_app() -> App {
    let mut app = open(TYPED, "text");
    let df = &mut app.stack.active_mut().dataframe;
    for (name, col_type) in TYPES {
        let i = df.column_index(name).unwrap();
        df.set_column_type(i, *col_type)
            .unwrap_or_else(|e| panic!("cannot type '{}' as {:?}: {}", name, col_type, e));
    }
    app
}

/// The dtype of every column, which is what a later save writes against.
fn dtypes(app: &App) -> Vec<String> {
    app.stack
        .active()
        .dataframe
        .df
        .dtypes()
        .iter()
        .map(|d| d.to_string())
        .collect()
}

/// Type `text` into the focused field and move down to the next one.
fn fill(app: &mut App, text: &str) {
    press(app, text);
    key(app, KeyCode::Down);
}

/// Enter, then Enter again to accept whatever was left blank.
fn confirm(app: &mut App) {
    key(app, KeyCode::Enter);
    key(app, KeyCode::Enter);
}

#[test]
fn o_opens_a_field_for_every_column() {
    let mut app = typed_app();
    let n = app.stack.active().dataframe.columns.len();

    press(&mut app, "O");

    assert_eq!(app.mode, AppMode::RowForm);
    assert_eq!(app.row_form.fields.len(), n);
    assert!(app.row_form.fields.iter().all(|f| f.is_empty()));
}

#[test]
fn a_filled_form_adds_one_row_and_leaves_every_dtype_alone() {
    let mut app = typed_app();
    let before_rows = app.stack.active().dataframe.visible_row_count();
    let before_types = dtypes(&app);

    press(&mut app, "O");
    fill(&mut app, "gamma");
    fill(&mut app, "3");
    fill(&mut app, "3.5");
    fill(&mut app, "2024-03-07");
    fill(&mut app, "2024-03-07 12:00:00");
    fill(&mut app, "yes");
    fill(&mut app, "30%");
    fill(&mut app, "450.25");
    fill(&mut app, "4096");
    key(&mut app, KeyCode::Enter);

    assert_eq!(app.mode, AppMode::Normal, "the form closes after inserting");
    let s = app.stack.active();
    assert_eq!(s.dataframe.visible_row_count(), before_rows + 1);
    assert_eq!(
        dtypes(&app),
        before_types,
        "an insert must not retype a column"
    );

    // The cursor lands on the new row, which is the last one.
    let s = app.stack.active();
    assert_eq!(s.table_state.selected(), Some(before_rows));

    let df = &s.dataframe;
    let row = before_rows; // physical index: the row was appended
    assert_eq!(
        df.get_physical(row, df.column_index("text").unwrap()),
        "gamma"
    );
    assert_eq!(df.get_physical(row, df.column_index("count").unwrap()), "3");
    assert_eq!(
        df.get_physical(row, df.column_index("day").unwrap()),
        "2024-03-07"
    );
    assert_eq!(
        df.get_physical(row, df.column_index("flag").unwrap()),
        "true"
    );
    // A percentage column holds the fraction, so 30% is stored as 0.3.
    assert_eq!(
        df.get_physical(row, df.column_index("share").unwrap()),
        "0.3"
    );
    assert_eq!(
        df.get_physical(row, df.column_index("bytes").unwrap()),
        "4096"
    );
}

#[test]
fn a_value_the_column_cannot_hold_blocks_the_insert() {
    let mut app = typed_app();
    let before = app.stack.active().dataframe.visible_row_count();

    press(&mut app, "O");
    fill(&mut app, "gamma");
    press(&mut app, "abc"); // 'count' is an integer column
    key(&mut app, KeyCode::Enter);

    assert_eq!(app.mode, AppMode::RowForm, "the form stays open");
    assert_eq!(
        app.stack.active().dataframe.visible_row_count(),
        before,
        "nothing may be inserted while a field is wrong"
    );
    assert!(app.row_form.errors[1].is_some(), "the bad field is marked");
    assert_eq!(app.row_form.focus, 1, "and the cursor is put on it");
}

/// The complaint has to arrive while the user is typing, not only at Enter.
#[test]
fn a_field_is_checked_as_it_is_typed() {
    let mut app = typed_app();
    press(&mut app, "O");
    key(&mut app, KeyCode::Down); // onto 'count'

    press(&mut app, "x");
    assert!(app.row_form.errors[1].is_some());

    key(&mut app, KeyCode::Backspace);
    press(&mut app, "7");
    assert!(
        app.row_form.errors[1].is_none(),
        "fixing it clears the mark"
    );
}

#[test]
fn an_empty_field_is_null_rather_than_an_empty_string() {
    let mut app = typed_app();
    let before = app.stack.active().dataframe.visible_row_count();

    press(&mut app, "O");
    confirm(&mut app); // every field left empty

    let s = app.stack.active();
    assert_eq!(s.dataframe.visible_row_count(), before + 1);
    for col in 0..s.dataframe.columns.len() {
        assert!(
            s.dataframe.is_null_physical(before, col),
            "column {} should be NULL, not empty",
            s.dataframe.columns[col].name
        );
    }
}

#[test]
fn undo_takes_the_whole_row_back_at_once() {
    let mut app = typed_app();
    let before = app.stack.active().dataframe.visible_row_count();

    press(&mut app, "O");
    fill(&mut app, "gamma");
    fill(&mut app, "3");
    confirm(&mut app);
    assert_eq!(app.stack.active().dataframe.visible_row_count(), before + 1);

    press(&mut app, "U");
    assert_eq!(
        app.stack.active().dataframe.visible_row_count(),
        before,
        "one insert is one undo, not one per field"
    );
}

#[test]
fn esc_leaves_the_table_untouched() {
    let mut app = typed_app();
    let before = app.stack.active().dataframe.visible_row_count();

    press(&mut app, "O");
    fill(&mut app, "gamma");
    key(&mut app, KeyCode::Esc);

    assert_eq!(app.mode, AppMode::Normal);
    assert_eq!(app.stack.active().dataframe.visible_row_count(), before);
    assert!(app.row_form.fields.is_empty());
}

/// A row with holes in it is probably a row someone is still filling in, so the first
/// Enter says what would happen instead of doing it.
#[test]
fn a_half_filled_form_asks_before_inserting() {
    let mut app = typed_app();
    let before = app.stack.active().dataframe.visible_row_count();

    press(&mut app, "O");
    fill(&mut app, "gamma");
    key(&mut app, KeyCode::Enter);

    assert_eq!(app.mode, AppMode::RowForm, "the form stays open to be read");
    assert_eq!(
        app.stack.active().dataframe.visible_row_count(),
        before,
        "the first Enter must not insert"
    );
    assert!(app.row_form.confirm_empty);
    assert!(
        app.status_message.contains("8 of 9"),
        "the warning must count the blanks: {}",
        app.status_message
    );

    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal);
    assert_eq!(app.stack.active().dataframe.visible_row_count(), before + 1);
}

/// The offer is about the form as it stood; touching it withdraws the offer.
#[test]
fn typing_after_the_warning_asks_again() {
    let mut app = typed_app();
    let before = app.stack.active().dataframe.visible_row_count();

    press(&mut app, "O");
    fill(&mut app, "gamma");
    key(&mut app, KeyCode::Enter);
    assert!(app.row_form.confirm_empty);

    press(&mut app, "5"); // now filling 'count'
    assert!(!app.row_form.confirm_empty, "the offer lapses");

    key(&mut app, KeyCode::Enter);
    assert_eq!(
        app.stack.active().dataframe.visible_row_count(),
        before,
        "so this Enter warns again rather than inserting"
    );
    assert!(app.row_form.confirm_empty);
}

/// A form is where a hand reaches for Tab, and there is nothing here to autocomplete.
#[test]
fn tab_and_shift_tab_walk_the_fields() {
    let mut app = typed_app();
    let last = app.stack.active().dataframe.columns.len() - 1;

    press(&mut app, "O");
    key(&mut app, KeyCode::Tab);
    assert_eq!(app.row_form.focus, 1);

    key(&mut app, KeyCode::BackTab);
    assert_eq!(app.row_form.focus, 0);

    // And they wrap, exactly as the arrows do.
    key(&mut app, KeyCode::BackTab);
    assert_eq!(app.row_form.focus, last);
    key(&mut app, KeyCode::Tab);
    assert_eq!(app.row_form.focus, 0);
}

/// A document sheet is a projection, so a row typed into it would be lost on the next
/// reprojection. `add_empty_row` refuses it, and the form has to refuse it before
/// putting a popup up rather than after the user has filled it in.
#[test]
fn the_form_does_not_open_on_a_document_sheet() {
    let mut app = open("test_data/nested.json", "name");
    press(&mut app, "O");

    assert_ne!(app.mode, AppMode::RowForm);
    assert!(app.row_form.fields.is_empty());
}

/// JSON Lines is a table, not a document: a row goes in like a CSV row.
#[test]
fn the_form_opens_on_json_lines() {
    let mut app = open("test_data/rows.jsonl", "a");
    press(&mut app, "O");
    assert_eq!(app.mode, AppMode::RowForm);
}
