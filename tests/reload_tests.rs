//! `R` re-reads what a sheet is — a table, a view, a workbook sheet, a listing, a file —
//! and refuses on a view built out of another sheet.

mod keys;

use crossterm::event::KeyCode;
use keys::{columns as cols, drill, key};
use std::path::PathBuf;
use tuitab::app::App;
use tuitab::types::Action;

const USERS: &str = "CREATE TABLE users (id INTEGER, name TEXT);
                     INSERT INTO users VALUES (1, 'a'), (2, 'b');";

fn dir() -> PathBuf {
    let d = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("reload-tests");
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn rows(app: &App) -> usize {
    app.stack.active().dataframe.visible_row_count()
}

fn reload(app: &mut App) {
    app.handle_action(Action::ReloadFile);
}

/// A fresh SQLite file with `sql` applied.
fn sqlite(name: &str, sql: &str) -> PathBuf {
    let p = dir().join(name);
    let _ = std::fs::remove_file(&p);
    rusqlite::Connection::open(&p)
        .unwrap()
        .execute_batch(sql)
        .unwrap();
    p
}

/// The derived-view refusal exactly — "Cannot reload inside a node" and "no source
/// path" are other answers, and a test that accepted them would not see this one go.
fn assert_refused(app: &App) {
    assert_eq!(
        app.status_message,
        "Cannot reload this view — go back to the sheet it came from first"
    );
}

#[test]
fn reloading_a_drill_down_refuses_and_keeps_it() {
    let path = dir().join("d.dat");
    std::fs::write(&path, "k,v\na,1\na,2\nb,3\n").unwrap();
    let mut app = App::new_as(&path, None, Some("csv")).unwrap();
    drill(&mut app);
    let before = rows(&app);
    reload(&mut app);
    assert_refused(&app);
    assert_eq!(rows(&app), before);
}

#[test]
fn reloading_a_drill_down_of_a_database_table_refuses() {
    let db = sqlite("drill.sqlite", USERS);
    let mut app = App::new_as(&db, None, None).unwrap();
    key(&mut app, KeyCode::Enter); // listing → users
    drill(&mut app);
    let before = rows(&app);
    reload(&mut app);
    assert_refused(&app);
    assert_eq!(rows(&app), before);
    assert_eq!(cols(&app), ["id", "name"]);
}

#[test]
fn reloading_document_search_results_refuses() {
    let path = dir().join("search.json");
    std::fs::write(
        &path,
        r#"[{"id":1,"meta":{"owner":"alice"}},{"id":2,"meta":{"owner":"bob"}}]"#,
    )
    .unwrap();
    let mut app = App::new_as(&path, None, None).unwrap();
    app.handle_action(Action::StartDocSearch);
    for c in "alice".chars() {
        app.handle_action(Action::SearchInput(c));
    }
    app.handle_action(Action::ApplyDocSearch);
    assert!(
        app.stack.active().doc_hits.is_some(),
        "{}",
        app.status_message
    );
    let before = rows(&app);
    reload(&mut app);
    assert_refused(&app);
    assert_eq!(rows(&app), before);
}

#[test]
fn reloading_a_top_level_scalar_view_refuses() {
    // From the root, `gp port` only moves the cursor — it is the anchor already on
    // screen.  From inside `servers` it pushes a sheet anchored at the root, sharing the
    // tree with the two sheets below it.
    let path = dir().join("goto.json");
    std::fs::write(&path, r#"{"port":1,"servers":[{"host":"a"}]}"#).unwrap();
    let mut app = App::new_as(&path, None, None).unwrap();
    for target in ["servers", "port"] {
        app.handle_action(Action::StartPathGoto);
        app.stack.active_mut().path_input =
            tuitab::ui::text_input::TextInput::with_value(target.into());
        app.handle_action(Action::ApplyPathGoto);
    }
    assert_eq!(app.stack.depth(), 3, "{}", app.status_message);
    assert!(app
        .stack
        .active()
        .doc
        .as_ref()
        .unwrap()
        .view
        .anchor
        .is_empty());
    let before = rows(&app);
    reload(&mut app);
    assert_refused(&app);
    assert_eq!(rows(&app), before);
}

/// Put the cursor on the listing row whose first cell is `name`.
fn select(app: &mut App, name: &str) {
    let df = &app.stack.active().dataframe;
    let row = (0..df.visible_row_count())
        .find(|&r| {
            tuitab::data::dataframe::DataFrame::anyvalue_to_string_fmt(&df.get_val(r, 0)) == name
        })
        .unwrap_or_else(|| panic!("no row {} in {:?}", name, cols(app)));
    app.stack.active_mut().table_state.select(Some(row));
}

/// A workbook of two sheets: `first` holds a column `one`, `second` a column `two`.
fn workbook(name: &str) -> PathBuf {
    use rust_xlsxwriter::Workbook;
    let path = dir().join(name);
    let _ = std::fs::remove_file(&path);
    let mut wb = Workbook::new();
    for (sheet, col) in [("first", "one"), ("second", "two")] {
        let ws = wb.add_worksheet().set_name(sheet).unwrap();
        ws.write_string(0, 0, col).unwrap();
        ws.write_number(1, 0, 1.0).unwrap();
    }
    wb.save(&path).unwrap();
    path
}

fn assert_reloaded(app: &App) {
    assert!(
        app.status_message.starts_with("Reloaded"),
        "{}",
        app.status_message
    );
}

#[test]
fn reloading_a_database_table_rereads_the_table() {
    let db = sqlite("table.sqlite", USERS);
    let mut app = App::new_as(&db, None, None).unwrap();
    key(&mut app, KeyCode::Enter);
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("INSERT INTO users VALUES (3, 'c');")
        .unwrap();
    reload(&mut app);
    assert_reloaded(&app);
    assert_eq!(cols(&app), ["id", "name"]);
    assert_eq!(rows(&app), 3);
    assert!(app.stack.active().table_source.is_some());
}

#[test]
fn reloading_a_view_rereads_the_view() {
    let db = sqlite(
        "view.sqlite",
        "CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1); CREATE VIEW v AS SELECT x FROM t;",
    );
    let mut app = App::new_as(&db, None, None).unwrap();
    select(&mut app, "v");
    key(&mut app, KeyCode::Enter);
    assert!(
        app.stack.active().table_source.is_none(),
        "a view is read-only"
    );
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("INSERT INTO t VALUES (2);")
        .unwrap();
    reload(&mut app);
    assert_reloaded(&app);
    assert_eq!(cols(&app), ["x"]);
    assert_eq!(rows(&app), 2);
}

#[test]
fn reloading_a_duckdb_table_rereads_the_table() {
    let db = dir().join("table.duckdb");
    let _ = std::fs::remove_file(&db);
    duckdb::Connection::open(&db)
        .unwrap()
        .execute_batch(USERS)
        .unwrap();
    let mut app = App::new_as(&db, None, None).unwrap();
    key(&mut app, KeyCode::Enter);
    duckdb::Connection::open(&db)
        .unwrap()
        .execute_batch("INSERT INTO users VALUES (3, 'c');")
        .unwrap();
    reload(&mut app);
    assert_reloaded(&app);
    assert_eq!(cols(&app), ["id", "name"]);
    assert_eq!(rows(&app), 3);
}

#[test]
fn reloading_a_dropped_table_keeps_the_sheet() {
    // A second table keeps the listing non-empty, so re-reading the file as a listing
    // would succeed and this would not pass by accident.
    let db = sqlite(
        "dropped.sqlite",
        &format!("{} CREATE TABLE other (y INTEGER);", USERS),
    );
    let mut app = App::new_as(&db, None, None).unwrap();
    select(&mut app, "users");
    key(&mut app, KeyCode::Enter);
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("DROP TABLE users;")
        .unwrap();
    reload(&mut app);
    assert!(
        app.status_message.starts_with("Reload failed"),
        "{}",
        app.status_message
    );
    assert_eq!(cols(&app), ["id", "name"]);
    assert_eq!(rows(&app), 2);
}

#[test]
fn reloading_an_excel_sheet_rereads_that_sheet() {
    let book = workbook("two.xlsx");
    let mut app = App::new_as(&book, None, None).unwrap();
    select(&mut app, "second");
    key(&mut app, KeyCode::Enter);
    reload(&mut app);
    assert_reloaded(&app);
    assert_eq!(cols(&app), ["two"]);
}

/// An empty directory under `tmp/reload-tests`.
fn fresh_dir(name: &str) -> PathBuf {
    let d = dir().join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn reloading_an_excel_listing_keeps_the_listing() {
    let book = workbook("listing.xlsx");
    let mut app = App::new_as(&book, None, None).unwrap();
    reload(&mut app);
    assert_reloaded(&app);
    assert_eq!(cols(&app), ["Sheet"]);
}

#[test]
fn reloading_a_file_opened_from_the_browser_rereads_it() {
    let base = fresh_dir("browse");
    let file = base.join("a.csv");
    std::fs::write(&file, "k\n1\n").unwrap();
    let mut app = App::new_as(&base, None, None).unwrap();
    app.open_directory_row(); // the only row is under the cursor
    std::fs::write(&file, "k\n1\n2\n").unwrap();
    reload(&mut app);
    assert_reloaded(&app);
    assert_eq!(rows(&app), 2);
}

#[test]
fn reloading_a_file_opened_as_from_the_browser_keeps_the_type() {
    // By its extension `.txt` reads as text, so a lost "open as" shows as other columns.
    let base = fresh_dir("browse-as");
    std::fs::write(base.join("data.txt"), r#"[{"a":1},{"a":2}]"#).unwrap();
    let mut app = App::new_as(&base, None, None).unwrap();
    app.open_directory_row_as(Some(tuitab::data::doc::Format::Json));
    assert_eq!(cols(&app), ["a"]);
    reload(&mut app);
    assert_reloaded(&app);
    assert_eq!(cols(&app), ["a"]);
    assert!(app.stack.active().doc.is_some());
}

#[test]
fn reloading_a_duckdb_view_rereads_the_view() {
    let db = dir().join("view.duckdb");
    let _ = std::fs::remove_file(&db);
    duckdb::Connection::open(&db)
        .unwrap()
        .execute_batch(
            "CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1); CREATE VIEW v AS SELECT x FROM t;",
        )
        .unwrap();
    let mut app = App::new_as(&db, None, None).unwrap();
    select(&mut app, "v");
    key(&mut app, KeyCode::Enter);
    assert!(
        app.stack.active().table_source.is_none(),
        "a view is read-only"
    );
    duckdb::Connection::open(&db)
        .unwrap()
        .execute_batch("INSERT INTO t VALUES (2);")
        .unwrap();
    reload(&mut app);
    assert_reloaded(&app);
    assert_eq!(cols(&app), ["x"]);
    assert_eq!(rows(&app), 2);
}

/// A listing is not a file among siblings — the same rule the background loader keeps.
#[test]
fn reloading_a_browser_file_that_became_a_database_drops_its_siblings() {
    let base = fresh_dir("browse-db");
    let file = base.join("x.csv");
    std::fs::write(&file, "k\n1\n").unwrap();
    let mut app = App::new_as(&base, None, None).unwrap();
    app.open_directory_row();
    assert!(app.stack.active().dir_source_path.is_some());

    std::fs::remove_file(&file).unwrap();
    rusqlite::Connection::open(&file)
        .unwrap()
        .execute_batch(USERS)
        .unwrap();
    reload(&mut app);
    assert_reloaded(&app);
    assert_eq!(
        app.stack.active().sqlite_db_path.as_deref(),
        Some(file.as_path())
    );
    assert!(app.stack.active().dir_source_path.is_none());
}
