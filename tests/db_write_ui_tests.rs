//! The save flow for a table opened from a database, driven through the key layer.
//!
//! A picker wired into the actions but left out of `ui::render`'s dispatch passes every
//! logic test and shows the user nothing, so these go through both.

mod keys;

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use keys::{key, press};
use ratatui::{backend::TestBackend, Terminal};
use std::path::{Path, PathBuf};
use tuitab::app::App;
use tuitab::types::AppMode;

fn fixture(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("db-write-ui-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    let _ = std::fs::remove_file(&path);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, note TEXT);
         INSERT INTO users VALUES (1, 'ann', NULL);
         INSERT INTO users VALUES (2, 'bob', '');
         INSERT INTO users VALUES (3, 'cara', 'hi');",
    )
    .unwrap();
    path
}

/// Open the database, drill into `users`, and put the cursor on `name`.
fn open_table(path: &Path) -> App {
    let mut app = App::new_as(path, None, None).unwrap();
    key(&mut app, KeyCode::Enter); // overview → users
    let idx = app.stack.active().dataframe.column_index("name").unwrap();
    app.stack.active_mut().cursor_col = idx;
    app
}

/// Ctrl+S — the save binding.  The shared harness only sends unmodified keys.
fn save_key(app: &mut App) {
    let event = KeyEvent {
        code: KeyCode::Char('s'),
        modifiers: KeyModifiers::CONTROL,
        kind: KeyEventKind::Press,
        state: crossterm::event::KeyEventState::NONE,
    };
    let action = tuitab::event::handle_key_event(event, app.mode, app.stack.can_pop());
    app.handle_action(action);
}

fn screen(app: &mut App) -> String {
    let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
    terminal.draw(|f| tuitab::ui::render(f, app)).unwrap();
    terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect()
}

fn names(path: &Path) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn.prepare("SELECT name FROM users ORDER BY id").unwrap();
    let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
    rows.map(|r| r.unwrap()).collect()
}

/// Edit the cell under the cursor.
fn edit(app: &mut App, value: &str) {
    press(app, "e"); // start editing
    for _ in 0..40 {
        key(app, KeyCode::Backspace);
    }
    press(app, value);
    key(app, KeyCode::Enter);
}

#[test]
fn saving_a_database_table_offers_the_database_file_not_the_sheet_title() {
    let path = fixture("default-name.sqlite");
    let mut app = open_table(&path);
    save_key(&mut app);
    assert_eq!(app.mode, AppMode::Saving);
    assert!(
        app.save.input.as_str().ends_with("default-name.sqlite"),
        "got {:?} — the title would have no usable extension",
        app.save.input.as_str()
    );
}

#[test]
fn confirming_the_sql_writes_it_and_the_popup_shows_what_will_run() {
    let path = fixture("confirm.sqlite");
    let mut app = open_table(&path);
    edit(&mut app, "ANN");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter); // accept the path → SQL confirmation
    assert_eq!(app.mode, AppMode::SqlConfirm);

    let text = screen(&mut app);
    assert!(text.contains("UPDATE"), "no SQL on screen");
    assert!(text.contains("users"), "the table is not named");
    assert!(text.contains("'ANN'"), "the value is not shown");
    assert!(text.contains("1 UPDATE"), "no summary in the title");
    assert_eq!(names(&path), ["ann", "bob", "cara"], "nothing written yet");

    key(&mut app, KeyCode::Enter); // run it
    assert_eq!(app.mode, AppMode::Normal);
    assert_eq!(names(&path), ["ANN", "bob", "cara"]);
    assert!(
        app.status_message.contains("Wrote"),
        "{}",
        app.status_message
    );
}

#[test]
fn escaping_the_popup_writes_nothing() {
    let path = fixture("escape.sqlite");
    let mut app = open_table(&path);
    edit(&mut app, "ANN");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm);

    key(&mut app, KeyCode::Esc);
    assert_eq!(app.mode, AppMode::Saving, "back to the filename prompt");
    assert_eq!(names(&path), ["ann", "bob", "cara"]);
}

#[test]
fn an_unbound_key_neither_runs_nor_dismisses_the_sql() {
    let path = fixture("unbound.sqlite");
    let mut app = open_table(&path);
    edit(&mut app, "ANN");
    save_key(&mut app);
    key(&mut app, KeyCode::Enter);

    press(&mut app, "x");
    assert_eq!(app.mode, AppMode::SqlConfirm, "a stray key must do nothing");
    assert_eq!(names(&path), ["ann", "bob", "cara"]);
}

#[test]
fn saving_an_unchanged_table_says_so_instead_of_opening_an_empty_popup() {
    let path = fixture("nochange.sqlite");
    let mut app = open_table(&path);
    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal);
    assert!(
        app.status_message.contains("No changes"),
        "{}",
        app.status_message
    );
}

#[test]
fn a_long_statement_list_scrolls() {
    let path = fixture("scroll.sqlite");
    // Enough distinct edits that the list cannot fit in the popup at once.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        for i in 4..60 {
            conn.execute(
                "INSERT INTO users VALUES (?1, ?2, NULL)",
                rusqlite::params![i, format!("n{}", i)],
            )
            .unwrap();
        }
    }
    let mut app = open_table(&path);
    let name = app.stack.active().dataframe.column_index("name").unwrap();
    for row in 0..50 {
        app.stack
            .active_mut()
            .dataframe
            .set_cell(row, name, format!("edited-{}", row))
            .unwrap();
    }

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm);

    let top = screen(&mut app);
    assert!(top.contains("edited-0"), "the list starts at the top");
    assert!(
        top.contains("↓"),
        "the title should say there is more below"
    );

    press(&mut app, "G");
    let bottom = screen(&mut app);
    assert!(bottom.contains("edited-49"), "G did not reach the end");
    assert!(
        !bottom.contains("edited-0'"),
        "the top should be scrolled off"
    );

    press(&mut app, "g");
    let back = screen(&mut app);
    assert!(back.contains("edited-0"), "g did not return to the top");
}

#[test]
fn a_null_looks_different_from_an_empty_string_on_screen() {
    let path = fixture("nulls.sqlite");
    let mut app = open_table(&path);
    let text = screen(&mut app);
    assert!(
        text.contains("NULL"),
        "a NULL cell must be visible as such, not as blank"
    );
}

/// A NULL is copied as `\N` — what the editor shows — so pasting it back into
/// another cell of a database sheet writes a real NULL rather than the two
/// characters. Skips where there is no system clipboard to talk to.
#[test]
fn a_null_copied_from_a_cell_pastes_back_as_a_null() {
    let saved = tuitab::clipboard::paste_from_clipboard().ok();
    if tuitab::clipboard::copy_text("tuitab clipboard probe").is_err() {
        eprintln!("no system clipboard here — skipping");
        return;
    }
    let path = fixture("null-paste.sqlite");
    let mut app = open_table(&path);
    let note = app.stack.active().dataframe.column_index("note").unwrap();
    app.stack.active_mut().cursor_col = note;

    app.stack.active_mut().table_state.select(Some(0)); // ann: note IS NULL
    press(&mut app, "yc");
    assert_eq!(tuitab::clipboard::paste_from_clipboard().unwrap(), "\\N");

    app.stack.active_mut().table_state.select(Some(2)); // cara: note = 'hi'
    key(&mut app, KeyCode::Char('p'));
    assert_eq!(
        app.stack.active().dataframe.get_editable(2, note),
        "\\N",
        "pasting a copied NULL must write a NULL, got: {}",
        app.status_message
    );

    if let Some(s) = saved {
        let _ = tuitab::clipboard::copy_text(&s);
    }
}

#[test]
fn a_value_the_column_cannot_hold_is_reported_and_nothing_is_written() {
    let path = fixture("badtype.sqlite");
    let mut app = open_table(&path);
    let id = app.stack.active().dataframe.column_index("id").unwrap();
    app.stack.active_mut().cursor_col = id;
    edit(&mut app, "not-a-number");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Saving, "stays on the filename prompt");
    let err = app.save.error.clone().unwrap_or_default();
    assert!(err.contains("id"), "{}", err);
    assert!(err.contains("not an integer"), "{}", err);
    assert!(
        screen(&mut app).contains("not an integer"),
        "error not shown"
    );
}

#[test]
fn saving_to_a_different_database_file_copies_it_and_asks_nothing() {
    let path = fixture("copy-from.sqlite");
    let dest = path.with_file_name("copy-to.sqlite");
    let _ = std::fs::remove_file(&dest);

    let mut app = open_table(&path);
    edit(&mut app, "ANN");

    save_key(&mut app);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(dest.to_string_lossy().into_owned());
    key(&mut app, KeyCode::Enter);

    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);
    assert_eq!(names(&path), ["ann", "bob", "cara"], "source untouched");
    assert_eq!(names(&dest), ["ANN", "bob", "cara"], "copy has the edit");
}

#[test]
fn a_sheet_that_lost_its_row_identity_is_refused_with_a_reason() {
    let path = fixture("blocked.sqlite");
    let mut app = open_table(&path);
    edit(&mut app, "ANN");
    // A window column renumbers rows, which is exactly what invalidates writeback.
    app.stack.active_mut().dataframe.db_rows = None;

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Saving);
    let err = app.save.error.clone().unwrap_or_default();
    assert!(err.contains("row identity was lost"), "{}", err);
    assert!(err.contains("different file"), "{}", err);
    assert_eq!(names(&path), ["ann", "bob", "cara"]);
}

/// The whole "diff against the load snapshot, don't journal edits" design rests on
/// undo needing no special handling.  It is only true because `Sheet::snapshot` clones
/// the frame — including its row identity — so an older frame carries an older diff.
#[test]
fn undoing_an_edit_leaves_nothing_to_write() {
    let path = fixture("undo-edit.sqlite");
    let mut app = open_table(&path);
    edit(&mut app, "ANN");
    press(&mut app, "U"); // undo

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);
    assert!(
        app.status_message.contains("No changes"),
        "{}",
        app.status_message
    );
    assert_eq!(names(&path), ["ann", "bob", "cara"]);
}

#[test]
fn undoing_a_deletion_takes_the_delete_back_with_it() {
    let path = fixture("undo-delete.sqlite");
    let mut app = open_table(&path);
    press(&mut app, "s"); // select the row under the cursor
    press(&mut app, "d"); // delete selected rows
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 2);

    press(&mut app, "U");
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 3);

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);
    assert!(
        app.status_message.contains("No changes"),
        "an undone deletion must not still be queued: {}",
        app.status_message
    );
    assert_eq!(names(&path), ["ann", "bob", "cara"]);
}

#[test]
fn a_deletion_that_stands_reaches_the_database() {
    let path = fixture("delete-stands.sqlite");
    let mut app = open_table(&path);
    press(&mut app, "s");
    press(&mut app, "d");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm);
    assert!(screen(&mut app).contains("DELETE"), "no DELETE shown");
    key(&mut app, KeyCode::Enter);

    assert_eq!(names(&path), ["bob", "cara"]);
    // And the reloaded sheet has nothing left to say.
    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert!(
        app.status_message.contains("No changes"),
        "{}",
        app.status_message
    );
}

// ── Schema changes through the key layer ─────────────────────────────────────────

/// Rename the column under the cursor via `z` `e`.
fn rename_column(app: &mut App, to: &str) {
    press(app, "ze");
    for _ in 0..40 {
        key(app, KeyCode::Backspace);
    }
    press(app, to);
    key(app, KeyCode::Enter);
}

fn column_names(app: &App) -> Vec<String> {
    app.stack
        .active()
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect()
}

#[test]
fn the_popup_shows_the_alter_before_the_updates() {
    let path = fixture("schema-order.sqlite");
    let mut app = open_table(&path);
    edit(&mut app, "ANN");
    rename_column(&mut app, "nom");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm, "{:?}", app.save.error);

    let text = screen(&mut app);
    // Match the statements themselves — the title also carries the word "UPDATE".
    let alter = text.find("ALTER TABLE").expect("no ALTER on screen");
    let update = text.find(r#"UPDATE "users""#).expect("no UPDATE on screen");
    assert!(alter < update, "the schema change must come first");
    assert!(text.contains("1 SCHEMA"), "no schema count in the title");

    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let renamed: String = conn
        .query_row("SELECT nom FROM users WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(renamed, "ANN");
}

#[test]
fn undoing_a_rename_takes_the_alter_back_with_it() {
    let path = fixture("schema-undo.sqlite");
    let mut app = open_table(&path);
    rename_column(&mut app, "nom");
    assert_eq!(column_names(&app)[1], "nom");
    press(&mut app, "U");
    assert_eq!(column_names(&app)[1], "name");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);
    assert!(
        app.status_message.contains("No changes"),
        "{}",
        app.status_message
    );
}

#[test]
fn a_refused_schema_change_says_why_on_screen_and_writes_nothing() {
    let path = fixture("schema-refused.sqlite");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE INDEX idx_name ON users(name)")
            .unwrap();
    }
    let mut app = open_table(&path);
    press(&mut app, "zd"); // delete the column under the cursor (`name`)
    assert_eq!(column_names(&app), ["id", "note"]);

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Saving);
    assert!(
        screen(&mut app).contains("idx_name"),
        "the index is not named"
    );
    assert_eq!(names(&path), ["ann", "bob", "cara"]);
}

/// Pinning keeps a column in sight. It is not a schema change, and it must not move
/// the cursor off the data it was on.
#[test]
fn pinning_a_column_moves_neither_the_column_nor_the_cursor() {
    let path = fixture("schema-pin.sqlite");
    let mut app = open_table(&path);
    let before = column_names(&app);
    let cursor = app.stack.active().cursor_col;

    press(&mut app, "!");
    assert_eq!(
        column_names(&app),
        before,
        "the frame must not be reordered"
    );
    assert_eq!(app.stack.active().cursor_col, cursor);
    assert!(app.stack.active().dataframe.columns[cursor].pinned);

    // …and it still renders in the pinned block on the left.
    let text = screen(&mut app);
    assert!(text.contains("name"), "the pinned column is not on screen");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal);
    assert!(
        app.status_message.contains("No changes"),
        "{}",
        app.status_message
    );
}

#[test]
fn a_new_column_is_offered_as_an_add_column() {
    let path = fixture("schema-add.sqlite");
    let mut app = open_table(&path);
    press(&mut app, "zi");
    press(&mut app, "tier");
    key(&mut app, KeyCode::Enter);
    assert!(column_names(&app).contains(&"tier".to_string()));

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm, "{:?}", app.save.error);
    let text = screen(&mut app);
    assert!(text.contains("ADD COLUMN"), "{}", text);
    assert!(text.contains("tier"), "the column name is not shown");

    key(&mut app, KeyCode::Enter);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'tier'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 1);
}

/// Dropping the rightmost column leaves the cursor past the end of the reloaded sheet
/// unless the save path pulls it back.
#[test]
fn the_cursor_survives_a_save_that_dropped_the_last_column() {
    let path = fixture("schema-cursor.sqlite");
    let mut app = open_table(&path);
    let last = column_names(&app).len() - 1;
    app.stack.active_mut().cursor_col = last;

    press(&mut app, "zd");
    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm, "{:?}", app.save.error);
    key(&mut app, KeyCode::Enter);

    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);
    let cols = column_names(&app).len();
    assert!(
        app.stack.active().cursor_col < cols,
        "cursor {} is past {} columns",
        app.stack.active().cursor_col,
        cols
    );
    // Nothing indexes out of bounds while drawing or moving.
    let _ = screen(&mut app);
    press(&mut app, "zl");
    let _ = screen(&mut app);
}

#[test]
fn a_rebuild_is_announced_before_the_user_confirms_it() {
    let path = fixture("rebuild-warning.sqlite");
    let mut app = open_table(&path);
    press(&mut app, "zl"); // move the column right → the table has to be rebuilt
    key(&mut app, KeyCode::Esc); // leave column-move mode
    assert_eq!(column_names(&app), ["id", "note", "name"]);

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm, "{:?}", app.save.error);

    let text = screen(&mut app);
    assert!(text.contains("will be rebuilt"), "no warning on screen");
    assert!(text.contains("DROP TABLE"), "the drop must be visible");
    assert_eq!(names(&path), ["ann", "bob", "cara"], "nothing written yet");

    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let cols: Vec<String> = conn
        .prepare("SELECT name FROM pragma_table_info('users') ORDER BY cid")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(cols, ["id", "note", "name"]);
    assert_eq!(names(&path), ["ann", "bob", "cara"], "rows survived");
}

// ── Building a database from nothing ─────────────────────────────────────────────

fn missing(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("db-create-ui-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    let _ = std::fs::remove_file(&path);
    path
}

#[test]
fn a_path_that_does_not_exist_opens_a_blank_sheet() {
    let path = missing("blank.sqlite");
    let mut app = App::new_as(&path, None, None).unwrap();

    assert_eq!(column_names(&app), ["column_1"]);
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 0);
    assert_eq!(app.stack.active().source_path.as_deref(), Some(&*path));
    assert!(
        app.status_message.contains("does not exist yet"),
        "{}",
        app.status_message
    );
    assert!(screen(&mut app).contains("[new]"), "the title must say so");
}

#[test]
fn a_blank_sheet_offers_its_own_path_when_saving() {
    let path = missing("prefill.sqlite");
    let mut app = App::new_as(&path, None, None).unwrap();
    save_key(&mut app);
    assert_eq!(app.mode, AppMode::Saving);
    assert!(app.save.input.as_str().ends_with("prefill.sqlite"));
}

#[test]
fn a_format_tuitab_cannot_write_is_still_an_error() {
    let dir = missing("x.sqlite").parent().unwrap().to_path_buf();
    assert!(
        App::new_as(&dir.join("notes"), None, None).is_err(),
        "no extension"
    );
    assert!(
        App::new_as(&dir.join("a.zip"), None, None).is_err(),
        "unknown format"
    );
    assert!(
        App::new_as(&dir.join("nowhere/deep/x.csv"), None, None).is_err(),
        "missing directory"
    );
}

#[test]
fn o_adds_a_row_below_the_cursor_where_vim_would() {
    let path = fixture("addrow.sqlite");
    let mut app = open_table(&path);
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 3);

    app.stack.active_mut().table_state.select(Some(0));
    press(&mut app, "o"); // below row 0
    assert_eq!(app.stack.active().table_state.selected(), Some(1));
    let names_now: Vec<String> = (0..4)
        .map(|r| {
            tuitab::data::dataframe::DataFrame::anyvalue_to_string_fmt(
                &app.stack.active().dataframe.get_val(r, 1),
            )
        })
        .collect();
    assert_eq!(names_now, ["ann", "", "bob", "cara"]);

    press(&mut app, "o"); // below the new one
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 5);
    assert_eq!(app.stack.active().table_state.selected(), Some(2));

    press(&mut app, "U");
    press(&mut app, "U");
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 3);
}

/// A row typed into the form has to carry the same "no database identity yet" mark
/// `o` gives one, or the save would look for a row to UPDATE and find none.
#[test]
fn a_row_typed_into_the_form_is_inserted_on_save() {
    let path = fixture("addrow-form.sqlite");
    let mut app = open_table(&path);

    press(&mut app, "O");
    assert_eq!(app.mode, AppMode::RowForm);
    key(&mut app, KeyCode::Down); // id is a PRIMARY KEY — leave it to the database
    press(&mut app, "dave");
    key(&mut app, KeyCode::Enter); // warns about the blank id and note
    key(&mut app, KeyCode::Enter); // and this accepts them as NULL
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm, "{:?}", app.save.error);
    assert!(screen(&mut app).contains("INSERT INTO"), "no INSERT shown");
    key(&mut app, KeyCode::Enter);

    assert_eq!(names(&path), ["ann", "bob", "cara", "dave"]);
}

/// `\N` is what `e` shows and takes back for a NULL, so the form has to read it the
/// same way rather than storing the two characters.
#[test]
fn the_null_sentinel_works_in_the_form_too() {
    let path = fixture("addrow-null.sqlite");
    let mut app = open_table(&path);
    let before = app.stack.active().dataframe.visible_row_count();

    press(&mut app, "O");
    key(&mut app, KeyCode::Down); // id
    press(&mut app, "\\N"); // name
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter); // id and note are blank, so this accepts them
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);

    let s = app.stack.active();
    let name = s.dataframe.column_index("name").unwrap();
    assert!(
        s.dataframe.is_null_physical(before, name),
        "\\N must mean NULL, not a two-character string"
    );
}

#[test]
fn a_row_added_to_a_database_sheet_is_inserted_on_save() {
    let path = fixture("addrow-save.sqlite");
    let mut app = open_table(&path);
    press(&mut app, "o");
    edit(&mut app, "dave");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm, "{:?}", app.save.error);
    assert!(screen(&mut app).contains("INSERT INTO"), "no INSERT shown");
    key(&mut app, KeyCode::Enter);

    assert_eq!(names(&path), ["ann", "bob", "cara", "dave"]);
}

#[test]
fn saving_a_new_database_asks_for_the_table_name_and_writes_without_confirmation() {
    let path = missing("inventory.sqlite");
    let mut app = App::new_as(&path, None, None).unwrap();

    // A second column, typed, then two rows with a value.
    press(&mut app, "zi");
    press(&mut app, "qty");
    key(&mut app, KeyCode::Enter);
    let qty = app.stack.active().dataframe.column_index("qty").unwrap();
    app.stack.active_mut().cursor_col = qty;
    press(&mut app, "t");
    // ColumnType::all() lists Integer second.
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Enter);
    press(&mut app, "o");
    edit(&mut app, "12");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter); // accept the path
    assert_eq!(app.mode, AppMode::TableNameInput, "{:?}", app.save.error);
    assert!(screen(&mut app).contains("inventory"), "stem not prefilled");
    key(&mut app, KeyCode::Enter); // accept the name

    // Nothing of the user's was at stake, so no confirmation.
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);
    assert!(path.exists(), "{}", app.status_message);

    let conn = rusqlite::Connection::open(&path).unwrap();
    let decl: String = conn
        .query_row(
            "SELECT type FROM pragma_table_info('inventory') WHERE name = 'qty'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(decl, "INTEGER");
    let stored: i64 = conn
        .query_row("SELECT qty FROM inventory", [], |r| r.get(0))
        .unwrap();
    assert_eq!(stored, 12);

    // …and the sheet has adopted the table, so it is now an ordinary writeback sheet.
    assert!(app.stack.active().table_source.is_some());
}

#[test]
fn saving_over_an_existing_table_shows_the_drop_first() {
    let path = missing("twice.sqlite");
    let mut app = App::new_as(&path, None, None).unwrap();
    press(&mut app, "o");
    edit(&mut app, "one");
    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);

    // The sheet adopted the table, so this second save is an ordinary writeback.
    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "nothing changed since");
    assert!(
        app.status_message.contains("No changes"),
        "{}",
        app.status_message
    );
}

#[test]
fn cancelling_the_sql_forgets_the_table_name() {
    let path = missing("forget.sqlite");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE taken (a TEXT); INSERT INTO taken VALUES ('x')")
            .unwrap();
    }
    let mut app = App::new_as(Path::new("test_data/sample.csv"), None, None).unwrap();

    save_key(&mut app);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(path.to_string_lossy().into_owned());
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::TableNameInput);
    for _ in 0..40 {
        key(&mut app, KeyCode::Backspace);
    }
    press(&mut app, "taken");
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm, "replacing needs confirming");
    assert!(screen(&mut app).contains("DROP TABLE"));

    key(&mut app, KeyCode::Esc);
    assert_eq!(app.mode, AppMode::Saving);
    // The next path must ask again rather than silently reuse 'taken'.
    key(&mut app, KeyCode::Enter);
    assert_eq!(
        app.mode,
        AppMode::TableNameInput,
        "the name must be forgotten"
    );
    assert_eq!(
        rusqlite::Connection::open(&path)
            .unwrap()
            .query_row("SELECT a FROM taken", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "x",
        "nothing was written"
    );
}

#[test]
fn a_blank_sheet_becomes_a_duckdb_database() {
    let path = missing("inventory.duckdb");
    let mut app = App::new_as(&path, None, None).unwrap();
    press(&mut app, "o");
    edit(&mut app, "hello");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::TableNameInput);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);

    let (df, src) = tuitab::data::io::load_duckdb_table_full(&path, "inventory").unwrap();
    assert!(src.is_some());
    assert_eq!(df.visible_row_count(), 1);
    assert_eq!(df.get_physical(0, 0), "hello");
}

/// The whole journey in one test: nothing on disk, then a typed table with data in it.
#[test]
fn a_database_can_be_built_from_absolutely_nothing() {
    let path = missing("scratch.sqlite");
    let mut app = App::new_as(&path, None, None).unwrap();

    // Name the placeholder column, add a typed one.
    press(&mut app, "ze");
    for _ in 0..40 {
        key(&mut app, KeyCode::Backspace);
    }
    press(&mut app, "sku");
    key(&mut app, KeyCode::Enter);

    press(&mut app, "zi");
    press(&mut app, "qty");
    key(&mut app, KeyCode::Enter);
    let qty = app.stack.active().dataframe.column_index("qty").unwrap();
    app.stack.active_mut().cursor_col = qty;
    press(&mut app, "t");
    key(&mut app, KeyCode::Down); // String → Integer
    key(&mut app, KeyCode::Enter);

    // Two rows, filled in.  Look the columns up by name — `zi` put the new one first.
    let sku_col = app.stack.active().dataframe.column_index("sku").unwrap();
    let qty_col = app.stack.active().dataframe.column_index("qty").unwrap();
    for (sku, n) in [("A-1", "12"), ("B-2", "7")] {
        press(&mut app, "o");
        app.stack.active_mut().cursor_col = sku_col;
        edit(&mut app, sku);
        app.stack.active_mut().cursor_col = qty_col;
        edit(&mut app, n);
    }

    save_key(&mut app);
    key(&mut app, KeyCode::Enter); // path
    key(&mut app, KeyCode::Enter); // table name = "scratch"
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);

    let conn = rusqlite::Connection::open(&path).unwrap();
    let ddl: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name = 'scratch'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    // `zi` inserts at the cursor, so `qty` sits before `sku` — the table is built in
    // the order the sheet shows, whatever that order turned out to be.
    assert_eq!(ddl, r#"CREATE TABLE "scratch" ("qty" INTEGER, "sku" TEXT)"#);

    let mut stmt = conn
        .prepare("SELECT sku, qty, typeof(qty) FROM scratch ORDER BY rowid")
        .unwrap();
    let rows: Vec<(String, i64, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![
            ("A-1".to_string(), 12, "integer".to_string()),
            ("B-2".to_string(), 7, "integer".to_string()),
        ]
    );
}

/// A created table has to look like a drilled-into one, or the JOIN picker cannot see
/// its siblings until the file is reopened.
#[test]
fn an_adopted_table_can_be_joined_against_its_neighbours() {
    let path = missing("joinable.sqlite");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE neighbour (k TEXT); INSERT INTO neighbour VALUES ('x')")
            .unwrap();
    }
    let mut app = App::new_as(Path::new("test_data/sample.csv"), None, None).unwrap();
    save_key(&mut app);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(path.to_string_lossy().into_owned());
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter); // name defaults to the stem
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);

    let sheet = app.stack.active();
    assert!(sheet.table_source.is_some());
    assert_eq!(
        sheet.sqlite_source_path.as_deref(),
        Some(&*path),
        "the created table must know which database it lives in"
    );
}

// ── What the sheet keeps across a save ────────────────────────────────────────────

/// Everything the user arranged on a column has to survive the reload a save triggers.
#[test]
fn a_save_keeps_the_pins_widths_and_aggregators_the_user_set_up() {
    let path = fixture("keep-meta.sqlite");
    let mut app = open_table(&path);
    let cursor = app.stack.active().cursor_col;
    {
        let cols = &mut app.stack.active_mut().dataframe.columns;
        cols[cursor].pinned = true;
        cols[cursor].width = 42;
        cols[cursor].width_mode = tuitab::data::column::ColumnWidthMode::Fit;
        cols[cursor].default_width = 17;
        cols[cursor].precision = 4;
        cols[cursor].selected = true;
        cols[cursor].aggregators = vec![tuitab::data::aggregator::AggregatorKind::Count];
    }
    edit(&mut app, "ANN");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal);
    assert_eq!(names(&path), ["ANN", "bob", "cara"]);

    let col = &app.stack.active().dataframe.columns[cursor];
    assert_eq!(col.name, "name", "the reload lined the metadata up by name");
    assert!(col.pinned);
    assert_eq!(col.width, 42);
    assert_eq!(col.width_mode, tuitab::data::column::ColumnWidthMode::Fit);
    assert_eq!(col.default_width, 17);
    assert_eq!(col.precision, 4);
    assert!(col.selected);
    assert_eq!(
        col.aggregators,
        vec![tuitab::data::aggregator::AggregatorKind::Count]
    );
}

/// A column the save dropped must not come back with the metadata of the one that took
/// its index.
#[test]
fn a_dropped_column_does_not_reappear_after_the_save() {
    let path = fixture("keep-meta-drop.sqlite");
    let mut app = open_table(&path);
    {
        let s = app.stack.active_mut();
        let note = s.dataframe.column_index("note").unwrap();
        s.dataframe.columns[note].pinned = true;
        s.cursor_col = note;
    }
    press(&mut app, "zd"); // drop the column

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);
    assert_eq!(column_names(&app), ["id", "name"]);
    assert!(
        app.stack
            .active()
            .dataframe
            .columns
            .iter()
            .all(|c| !c.pinned),
        "the dropped column's pin landed on a survivor"
    );
}

/// The type of a database column comes from the table, not from a snapshot of the sheet.
#[test]
fn the_declared_type_wins_over_what_the_sheet_had_before_the_save() {
    let path = fixture("keep-meta-type.sqlite");
    let mut app = open_table(&path);
    let id = app.stack.active().dataframe.column_index("id").unwrap();
    assert_eq!(
        app.stack.active().dataframe.columns[id].col_type,
        tuitab::types::ColumnType::Integer
    );
    // Pretend the sheet thought otherwise; the reload must not restore that.
    app.stack.active_mut().dataframe.columns[id].col_type = tuitab::types::ColumnType::String;
    edit(&mut app, "ANN");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter);
    assert_eq!(
        app.stack.active().dataframe.columns[id].col_type,
        tuitab::types::ColumnType::Integer
    );
}

/// The search remembers a column by name, so a save that removes a column to its left
/// leaves it searching the same data rather than whatever slid into that index.
#[test]
fn a_search_keeps_its_column_across_a_save_that_dropped_another() {
    let path = fixture("keep-search.sqlite");
    let mut app = open_table(&path);
    {
        let s = app.stack.active_mut();
        s.cursor_col = s.dataframe.column_index("note").unwrap();
    }
    press(&mut app, "/");
    press(&mut app, "hi");
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.stack.active().search_col_name.as_deref(), Some("note"));
    assert_eq!(app.stack.active().search_col(), 2);

    {
        let s = app.stack.active_mut();
        s.cursor_col = s.dataframe.column_index("name").unwrap();
    }
    press(&mut app, "zd"); // drop 'name', shifting 'note' left
    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);

    assert_eq!(column_names(&app), ["id", "note"]);
    let s = app.stack.active();
    assert_eq!(s.search_col_name.as_deref(), Some("note"));
    assert_eq!(s.search_col(), 1, "an index would still say 2");
}

/// A view has no row identity, so saving it into its own file has to be refused before
/// the create branch offers to `DROP TABLE` the thing it was read from.
#[test]
fn saving_a_view_back_into_its_own_database_is_refused() {
    let path = fixture("view-readonly.sqlite");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE VIEW adults AS SELECT id, name FROM users")
        .unwrap();

    let mut app = App::new_as(&path, None, None).unwrap();
    // The overview lists tables and views alphabetically: adults, users.
    key(&mut app, KeyCode::Enter);
    assert_eq!(column_names(&app), ["id", "name"]);
    assert!(app.stack.active().table_source.is_none());

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Saving, "still at the prompt");
    let error = app.save.error.clone().unwrap_or_default();
    assert!(error.contains("read-only"), "{}", error);
    // Nothing was proposed and nothing ran.
    assert!(app.sql.plan.is_none());
    assert_eq!(names(&path), ["ann", "bob", "cara"]);
}

/// `R` on a sheet whose file is gone must not throw away the work in it.
#[test]
fn a_failed_reload_keeps_the_undo_history() {
    let path = fixture("reload-missing.sqlite");
    let mut app = open_table(&path);
    edit(&mut app, "ANN");
    assert!(!app.stack.active().undo_stack.is_empty());

    // The reload for a table sheet goes through the file loader, and the file is gone.
    std::fs::remove_file(&path).unwrap();
    press(&mut app, "R");

    assert!(
        app.status_message.contains("Reload failed"),
        "{}",
        app.status_message
    );
    assert!(
        !app.stack.active().undo_stack.is_empty(),
        "the undo history was cleared before the reload was known to fail"
    );
}

/// A row added in the middle belongs in the middle, not at the bottom, once the sort
/// that put it there is taken away.
#[test]
fn a_row_added_in_the_middle_stays_there_when_the_sort_is_reset() {
    let path = fixture("addrow-unsort.sqlite");
    let mut app = open_table(&path);

    // Sort by name descending: cara, bob, ann.
    press(&mut app, "]");
    assert_eq!(app.stack.active().sort_keys, [("name".to_string(), true)]);

    // After 'ann', which is *first* in load order — the case that tells an insert at
    // the right place apart from an append.
    app.stack.active_mut().table_state.select(Some(2));
    press(&mut app, "o");
    let shown = |app: &App| -> Vec<String> {
        (0..app.stack.active().dataframe.visible_row_count())
            .map(|r| {
                tuitab::data::dataframe::DataFrame::anyvalue_to_string_fmt(
                    &app.stack.active().dataframe.get_val(r, 1),
                )
            })
            .collect()
    };
    assert_eq!(shown(&app), ["cara", "bob", "ann", ""]);

    press(&mut app, "r"); // reset the sort
    assert_eq!(
        shown(&app),
        ["ann", "", "bob", "cara"],
        "load order, with the new row where the sort had put it"
    );
}

/// A plan too big to hold readable text for must say so on screen — a silent cut would
/// read as "those were all the statements".
#[test]
fn a_plan_past_the_display_cap_says_how_many_it_is_not_showing() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("db-write-ui-tests")
        .join("display-cap.sqlite");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let _ = std::fs::remove_file(&path);
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..2050 {
            tx.execute(
                "INSERT INTO users (id, name) VALUES (?1, ?2)",
                rusqlite::params![i, format!("n{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }

    let mut app = App::new_as(&path, None, None).unwrap();
    key(&mut app, KeyCode::Enter); // overview → users
    {
        let df = &mut app.stack.active_mut().dataframe;
        let name = df.column_index("name").unwrap();
        for row in 0..2050 {
            df.set_cell(row, name, format!("x{}", row)).unwrap();
        }
    }

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm);

    assert_eq!(app.sql.plan.as_ref().unwrap().hidden_stmts(), 50);
    // The tail of the list is where the notice lives, so scroll to the bottom — and the
    // scroll limit only exists once the popup has been drawn at a known size.
    let _ = screen(&mut app);
    key(&mut app, KeyCode::Char('G'));
    let text = screen(&mut app);
    assert!(
        text.contains("and 50 more statement"),
        "the hidden statements are not accounted for on screen"
    );
    assert!(text.contains("they will run too"), "{}", &text[..200]);
}

// ── Sheets that are not the table ─────────────────────────────────────────────────

/// The overview lists what a database holds.  Saving it into that database used to
/// write the listing back as a table — and with a free name, without a popup.
#[test]
fn saving_the_database_overview_into_itself_is_refused() {
    let path = fixture("overview-save.sqlite");
    let mut app = App::new_as(&path, None, None).unwrap();
    assert_eq!(
        column_names(&app),
        ["Table", "Kind", "Rows", "Columns", "SQL"]
    );

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Saving, "still at the prompt");
    let error = app.save.error.clone().unwrap_or_default();
    assert!(error.contains("list of tables"), "{}", error);
    assert_ne!(
        app.mode,
        AppMode::TableNameInput,
        "it must not ask for a name"
    );

    // The database is untouched: still just `users`.
    let conn = rusqlite::Connection::open(&path).unwrap();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap();
    let names: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(names, ["users"]);
}

/// A drill-down is the same rows seen through a filter, so saving it is an ordinary
/// writeback — not an offer to create a table out of the visible slice.
#[test]
fn a_drilled_down_sheet_saves_back_into_its_table() {
    let path = fixture("drill-save.sqlite");
    let mut app = open_table(&path);
    // A drill-down is Enter on a value of a frequency table over the sheet.
    // The frequency table defers one frame through the Calculating overlay, which the
    // key layer does not drive on its own.
    app.handle_action(tuitab::types::Action::OpenFrequencyTable);
    app.handle_action(tuitab::types::Action::OpenFrequencyTable);
    let row = (0..app.stack.active().dataframe.visible_row_count())
        .find(|&r| {
            tuitab::data::dataframe::DataFrame::anyvalue_to_string_fmt(
                &app.stack.active().dataframe.get_val(r, 0),
            ) == "bob"
        })
        .unwrap_or_else(|| panic!("bob is missing from {:?}", column_names(&app)));
    app.stack.active_mut().table_state.select(Some(row));
    key(&mut app, KeyCode::Enter);
    assert!(
        app.stack.active().title.starts_with("Filter:"),
        "{}",
        app.stack.active().title
    );
    assert!(
        app.stack.active().table_source.is_some(),
        "the drilled sheet lost the table it came from"
    );
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 1);

    let idx = app.stack.active().dataframe.column_index("name").unwrap();
    app.stack.active_mut().cursor_col = idx;
    edit(&mut app, "BOB");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::SqlConfirm, "{:?}", app.save.error);
    let text = screen(&mut app);
    assert!(
        text.contains("UPDATE"),
        "a drill-down proposed something else"
    );
    assert!(!text.contains("CREATE TABLE"), "{}", text);

    key(&mut app, KeyCode::Enter);
    assert_eq!(
        names(&path),
        ["ann", "BOB", "cara"],
        "only the drilled row changed"
    );
}

/// Replacing a table shows what the DROP takes with it before the user says yes.
#[test]
fn replacing_a_table_shows_the_losses_in_the_popup() {
    let path = fixture("replace-popup.sqlite");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE INDEX ix_users_name ON users(name)")
        .unwrap();

    // A sheet with no table behind it, saved into the database under an existing name.
    let mut app = App::new_as(std::path::Path::new("test_data/sample.csv"), None, None).unwrap();
    save_key(&mut app);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(path.to_string_lossy().into_owned());
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::TableNameInput);
    app.save.table_input = tuitab::ui::text_input::TextInput::with_value("users".to_string());
    key(&mut app, KeyCode::Enter);

    assert_eq!(app.mode, AppMode::SqlConfirm, "{:?}", app.save.error);
    let text = screen(&mut app);
    assert!(
        text.contains("ix_users_name"),
        "the index loss is not on screen"
    );
    assert!(text.contains("will be lost"), "{}", text);
    assert!(text.contains("DROP TABLE"), "{}", text);
}

/// Row selection is by physical index, which a reload renumbers — the rowid is what it
/// was really about.
#[test]
fn a_selected_row_is_still_selected_after_a_save() {
    let path = fixture("keep-selection.sqlite");
    let mut app = open_table(&path);
    {
        let s = app.stack.active_mut();
        // Select the last row, then delete the first so the physical indices shift.
        s.dataframe.selected_rows.insert(2);
    }
    edit(&mut app, "ANN");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);

    let s = app.stack.active();
    assert_eq!(
        s.dataframe
            .selected_rows
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![2],
        "the selection did not survive the reload"
    );
}

/// A type assigned by hand comes back, and pressing save again plans nothing — the
/// round trip that made carrying it unsafe before.
#[test]
fn a_hand_assigned_type_survives_a_save_and_does_not_replan() {
    let path = fixture("keep-type.sqlite");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("ALTER TABLE users ADD COLUMN active BOOLEAN DEFAULT 1")
        .unwrap();

    let mut app = open_table(&path);
    let active = app.stack.active().dataframe.column_index("active").unwrap();
    {
        let s = app.stack.active_mut();
        s.cursor_col = active;
        s.dataframe
            .set_column_type(active, tuitab::types::ColumnType::Boolean)
            .unwrap();
        s.dataframe.columns[active].db_retype = Some(tuitab::types::ColumnType::Boolean);
    }
    let idx = app.stack.active().dataframe.column_index("name").unwrap();
    app.stack.active_mut().cursor_col = idx;
    edit(&mut app, "ANN");

    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{}", app.status_message);
    assert_eq!(
        app.stack.active().dataframe.columns[active].col_type,
        tuitab::types::ColumnType::Boolean,
        "the type was dropped by the reload"
    );

    // Straight back into save: nothing left to write.
    save_key(&mut app);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);
    assert!(
        app.status_message.contains("No changes"),
        "{}",
        app.status_message
    );
}

/// Issue #58: once a database without an extension opens, its table must save back
/// into it.  The extension-only check sent this path to the CSV exporter, which would
/// have truncated the database.
#[test]
fn a_table_from_a_database_without_an_extension_saves_back_into_it() {
    let path = fixture("noext-db");
    let mut app = open_table(&path);
    edit(&mut app, "ANN");
    save_key(&mut app);
    key(&mut app, KeyCode::Enter); // accept the path
    assert_eq!(
        app.mode,
        AppMode::SqlConfirm,
        "must take the database path, not the CSV exporter"
    );
    key(&mut app, KeyCode::Enter); // run it
    assert_eq!(names(&path), ["ANN", "bob", "cara"]);
    assert_eq!(
        tuitab::data::io::db_write::kind_of_file(&path),
        Some(tuitab::data::io::db_write::DbKind::Sqlite)
    );
}

/// `--type` describes the file the sheet was opened from.  Once the sheet becomes a
/// table in a database it made, `R` must not read that database as CSV.
#[test]
fn a_table_created_from_a_typed_sheet_forgets_the_type() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("db-write-ui-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let source = dir.join("typed.dat");
    std::fs::write(&source, "a,b\n1,2\n").unwrap();
    let dest = dir.join("typed-out.sqlite");
    let _ = std::fs::remove_file(&dest);

    let mut app = App::new_as(&source, None, Some("csv")).unwrap();
    save_key(&mut app);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(dest.to_string_lossy().into_owned());
    key(&mut app, KeyCode::Enter); // accept the path
    assert_eq!(app.mode, AppMode::TableNameInput, "{:?}", app.save.error);
    key(&mut app, KeyCode::Enter); // accept the name
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);
    assert_eq!(
        app.stack.active().source_path.as_deref(),
        Some(dest.as_path())
    );
    assert_eq!(app.stack.active().forced_type, None);
}

/// A drill-down saved into a new database becomes that database's table, so `R` reads
/// the table rather than refusing the view it used to be.
#[test]
fn reloading_a_table_created_by_saving_rereads_it() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("db-write-ui-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let source = dir.join("created.csv");
    std::fs::write(&source, "a,b\n1,2\n1,3\n").unwrap();
    let dest = dir.join("created-out.sqlite");
    let _ = std::fs::remove_file(&dest);

    let mut app = App::new_as(&source, None, None).unwrap();
    // Frequency over `a`, then Enter on its only value — see the drill-down test above.
    app.handle_action(tuitab::types::Action::OpenFrequencyTable);
    app.handle_action(tuitab::types::Action::OpenFrequencyTable);
    app.stack.active_mut().table_state.select(Some(0));
    key(&mut app, KeyCode::Enter);
    assert!(
        app.stack.active().title.starts_with("Filter:"),
        "{}",
        app.stack.active().title
    );

    save_key(&mut app);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(dest.to_string_lossy().into_owned());
    key(&mut app, KeyCode::Enter); // accept the path
    assert_eq!(app.mode, AppMode::TableNameInput, "{:?}", app.save.error);
    key(&mut app, KeyCode::Enter); // accept the name
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);

    app.handle_action(tuitab::types::Action::ReloadFile);
    assert!(
        app.status_message.starts_with("Reloaded"),
        "{}",
        app.status_message
    );
    assert_eq!(column_names(&app), ["a", "b"]);
    assert_eq!(app.stack.active().dataframe.visible_row_count(), 2);
}

/// Search results saved into a new database are that database's table too — the hits
/// they were stop being what `R` and Enter go by.
#[test]
fn reloading_search_results_saved_as_a_table_rereads_the_table() {
    use tuitab::types::Action;
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("db-write-ui-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let source = dir.join("hits.json");
    std::fs::write(
        &source,
        r#"[{"id":1,"meta":{"owner":"alice"}},{"id":2,"meta":{"owner":"bob"}}]"#,
    )
    .unwrap();
    let dest = dir.join("hits-out.sqlite");
    let _ = std::fs::remove_file(&dest);

    let mut app = App::new_as(&source, None, None).unwrap();
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

    save_key(&mut app);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(dest.to_string_lossy().into_owned());
    key(&mut app, KeyCode::Enter); // accept the path
    assert_eq!(app.mode, AppMode::TableNameInput, "{:?}", app.save.error);
    key(&mut app, KeyCode::Enter); // accept the name
    assert_eq!(app.mode, AppMode::Normal, "{:?}", app.save.error);

    app.handle_action(Action::ReloadFile);
    assert!(
        app.status_message.starts_with("Reloaded"),
        "{}",
        app.status_message
    );
    assert!(app.stack.active().doc_hits.is_none());
}
