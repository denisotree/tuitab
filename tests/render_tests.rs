//! That a popup reaches the screen.
//!
//! Every other test here drives state: which mode the app is in, what the frame
//! holds. None of them draw anything, so a picker wired into the key layer but
//! left out of `ui::render`'s dispatch passes all of them and shows the user
//! nothing. That is a whole class of bug — "the picker never appeared" — with
//! no coverage at all, so these render into a `TestBackend` and read the text
//! back off the buffer.

mod keys;

use crossterm::event::KeyCode;
use keys::{key, open, press};
use ratatui::{backend::TestBackend, Terminal};

const SAMPLE: &str = "test_data/sample.csv";

/// Draw a frame and return everything on it as text.
fn screen(app: &mut tuitab::app::App) -> String {
    let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
    terminal.draw(|f| tuitab::ui::render(f, app)).unwrap();
    terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect()
}

#[test]
fn the_window_function_picker_reaches_the_screen() {
    let mut app = open(SAMPLE, "salary");
    press(&mut app, "zw");
    let text = screen(&mut app);
    assert!(
        text.contains("pct_of_total"),
        "the function list did not draw"
    );
}

#[test]
fn the_direction_picker_reaches_the_screen() {
    let mut app = open(SAMPLE, "salary");
    press(&mut app, "zw");
    key(&mut app, KeyCode::Down); // rank
    key(&mut app, KeyCode::Enter);

    let text = screen(&mut app);
    assert!(
        text.contains("Ascending") && text.contains("Descending"),
        "the direction picker did not draw"
    );
    assert!(
        text.contains("rank"),
        "it must name the function it is asking about"
    );
}

/// The header is where the sort arrow lives, and it was being truncated away.
#[test]
fn a_sorted_column_shows_its_arrow_on_screen() {
    let mut app = open(SAMPLE, "age");
    press(&mut app, "]");
    assert!(
        screen(&mut app).contains('▼'),
        "no arrow on a sorted column"
    );

    press(&mut app, "[");
    assert!(screen(&mut app).contains('▲'), "no arrow after re-sorting");
}

#[test]
fn the_order_picker_reaches_the_screen() {
    let mut app = open("test_data/growth.csv", "amount");
    press(&mut app, "zw");
    for _ in 0..3 {
        key(&mut app, KeyCode::Down); // cum_sum
    }
    key(&mut app, KeyCode::Enter);

    let text = screen(&mut app);
    assert!(
        text.contains("the table's order"),
        "the order picker did not draw"
    );
    assert!(
        text.contains("date"),
        "it must list the columns to order by"
    );
    assert!(text.contains("cum_sum"), "and name the function it is for");
}

#[test]
fn the_new_row_form_reaches_the_screen() {
    let mut app = open(SAMPLE, "salary");
    press(&mut app, "O");

    let text = screen(&mut app);
    assert!(text.contains("New row"), "the form did not draw");
    for name in ["id", "name", "age", "salary", "department"] {
        assert!(
            text.contains(name),
            "the form must offer a field for {}",
            name
        );
    }

    // A value the column cannot hold is complained about on the screen, not only
    // in the state — this is the whole point of the form.
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Down);
    press(&mut app, "old");
    assert!(
        screen(&mut app).contains("not a whole number"),
        "the reason did not reach the screen"
    );
}

/// A key's explanation follows the key it explains: the window column's note on
/// running totals sits under `zw`, not under whatever line was added after it.
#[test]
fn the_help_keeps_each_note_under_its_key() {
    let mut terminal = Terminal::new(TestBackend::new(100, 400)).unwrap();
    terminal
        .draw(|f| tuitab::ui::popup::render_help_popup(f, f.area(), &mut 0))
        .unwrap();
    let cells: Vec<String> = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol().to_string())
        .collect();
    let rows: Vec<String> = cells.chunks(100).map(|r| r.concat()).collect();
    let at = rows
        .iter()
        .position(|r| r.contains("zw ") && r.contains("Window column"))
        .expect("zw is in the help");
    assert!(
        rows[at + 1].contains("a running total asks"),
        "{}\n{}",
        rows[at],
        rows[at + 1]
    );
}

/// A cell holding several lines — pretty-printed JSON in a log — showed only its
/// first line, `{`. The line breaks fold to spaces so the value reads on one row.
#[test]
fn a_cell_with_line_breaks_shows_past_the_first_line() {
    let mut app = open("test_data/multiline.jsonl", "client");
    let text = screen(&mut app);
    assert!(
        text.contains(r#"{ "country_code": "US", "city_name""#),
        "the cell stopped at its first line:\n{text}"
    );
}

/// Pressing `z` lists every z-command on screen — `zJ` was missing from the old
/// one-line hint, which could not hold them all at a hundred columns anyway.
#[test]
fn the_z_hint_lists_every_z_command_on_screen() {
    let mut app = open(SAMPLE, "salary");
    press(&mut app, "z");
    let text = screen(&mut app);
    let rows: Vec<String> = text
        .chars()
        .collect::<Vec<_>>()
        .chunks(100)
        .map(|r| r.iter().collect())
        .collect();
    for (key, what) in [
        ("J", "One column per key"),
        ("w", "Window column"),
        ("[ ]", "Add another sort key"),
    ] {
        let row = rows.iter().find(|r| r.contains(what));
        assert!(
            row.is_some_and(|r| r.contains(&format!("│{key} "))),
            "{key} {what:?} is not in the z hint:\n{}",
            rows.join("\n")
        );
    }
}

/// The help is longer than a terminal; its end is reachable by scrolling.
#[test]
fn the_help_scrolls_to_its_last_line() {
    let mut app = open(SAMPLE, "salary");
    press(&mut app, "?");
    assert!(
        !screen(&mut app).contains("This help"),
        "the help fits; nothing to scroll"
    );
    key(&mut app, KeyCode::End);
    assert!(
        screen(&mut app).contains("This help"),
        "End did not reach the last line"
    );
    press(&mut app, "g");
    assert!(
        screen(&mut app).contains("── Navigation"),
        "g did not go back to the top"
    );
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.mode, tuitab::types::AppMode::Normal);
}
