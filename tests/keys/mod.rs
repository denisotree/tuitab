//! Driving the app the way a person does — through the key layer.
//!
//! Every other test in this suite calls [`App::handle_action`] directly, which
//! skips [`handle_key_event`] entirely. That is where the modal state lives:
//! whether a chord left `ZPrefix`, whether the second key of `z[` is read as a
//! z-command or as a normal one. Two crashes reached the tree through that gap,
//! including one that deletes a column when the user asked to delete rows.
//!
//! So this helper does what the real loop does — `handle_key_event(key, mode,
//! can_pop)` then `handle_action` — and nothing more.

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use tuitab::app::App;
use tuitab::event::handle_key_event;

/// Send one key, exactly as the event loop would.
pub fn key(app: &mut App, code: KeyCode) {
    let event = KeyEvent {
        code,
        modifiers: KeyModifiers::NONE,
        kind: KeyEventKind::Press,
        state: crossterm::event::KeyEventState::NONE,
    };
    let action = handle_key_event(event, app.mode, app.stack.can_pop());
    app.handle_action(action);
}

/// Send a run of character keys, one at a time.
///
/// Written as the user would describe it: `press(&mut app, "z[")`. Each `char`
/// is a separate key press, so chords work the way they do in the terminal.
#[allow(dead_code)] // not every test file in this suite types characters
pub fn press(app: &mut App, keys: &str) {
    for c in keys.chars() {
        key(app, KeyCode::Char(c));
    }
}

/// Open a fixture and put the cursor on a named column.
#[allow(dead_code)] // not every test file in this suite opens a fixture this way
pub fn open(path: &str, cursor_column: &str) -> App {
    let mut app = App::new_as(std::path::Path::new(path), None, None).unwrap();
    let idx = app
        .stack
        .active()
        .dataframe
        .column_index(cursor_column)
        .unwrap();
    app.stack.active_mut().cursor_col = idx;
    app
}

/// Column names of the active sheet.
#[allow(dead_code)] // not every test file in this suite reads columns
pub fn columns(app: &App) -> Vec<String> {
    app.stack
        .active()
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect()
}
