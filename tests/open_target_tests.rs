//! What `open_target` decides a path is — the one answer every caller that puts a file
//! on the sheet stack now shares, so the background loader cannot disagree with the
//! foreground one about whether something is a database (#43).

use std::path::{Path, PathBuf};
use tuitab::data::io::open_target;

fn dir() -> PathBuf {
    let d = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("open-target-tests");
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn sqlite_at(name: &str) -> PathBuf {
    let path = dir().join(name);
    let _ = std::fs::remove_file(&path);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TABLE users (id INTEGER, name TEXT); INSERT INTO users VALUES (1,'ann');",
        )
        .unwrap();
    path
}

fn xlsx_at(name: &str, sheets: &[&str]) -> PathBuf {
    use rust_xlsxwriter::Workbook;
    let path = dir().join(name);
    let _ = std::fs::remove_file(&path);
    let mut wb = Workbook::new();
    for s in sheets {
        let ws = wb.add_worksheet().set_name(*s).unwrap();
        ws.write_string(0, 0, "col").unwrap();
        ws.write_number(1, 0, 1.0).unwrap();
    }
    wb.save(&path).unwrap();
    path
}

#[test]
fn a_database_opens_as_a_listing_that_knows_where_it_came_from() {
    let path = sqlite_at("plain.db");
    let opened = open_target(&path, None, None).unwrap();
    assert_eq!(opened.df.columns[0].name, "Table");
    assert_eq!(opened.sqlite_db_path.as_deref(), Some(path.as_path()));
    assert!(opened.duckdb_db_path.is_none() && opened.xlsx_db_path.is_none());
}

#[test]
fn a_workbook_of_several_sheets_opens_as_a_listing() {
    let path = xlsx_at("many.xlsx", &["alpha", "beta"]);
    let opened = open_target(&path, None, None).unwrap();
    assert_eq!(opened.df.columns[0].name, "Sheet");
    assert_eq!(opened.xlsx_db_path.as_deref(), Some(path.as_path()));
}

#[test]
fn a_workbook_of_one_sheet_opens_as_its_rows() {
    let path = xlsx_at("one.xlsx", &["only"]);
    let opened = open_target(&path, None, None).unwrap();
    assert_eq!(opened.df.columns[0].name, "col");
    assert!(
        opened.xlsx_db_path.is_none(),
        "a listing of length one stands between the user and their data"
    );
}

#[test]
fn an_explicit_type_wins_over_the_extension() {
    let path = dir().join("notes.db");
    let _ = std::fs::remove_file(&path);
    std::fs::write(&path, "a: 1\nb: two\n").unwrap();
    let opened = open_target(&path, None, Some("yaml")).unwrap();
    assert!(
        opened.doc.is_some(),
        "--type yaml must parse, not open a db"
    );
    assert!(opened.sqlite_db_path.is_none());
}

#[test]
fn an_explicit_type_beats_an_extension_that_would_otherwise_win() {
    // `.txt` has a loader of its own, so this is the case where the two really compete.
    let path = dir().join("records.txt");
    std::fs::write(
        &path,
        "{\"a\": 1, \"b\": \"two\"}\n{\"a\": 2, \"b\": \"three\"}\n",
    )
    .unwrap();

    let as_named = open_target(&path, None, None).unwrap();
    assert_ne!(
        as_named.df.columns.len(),
        2,
        "without --type this is plain text, one column"
    );

    let forced = open_target(&path, None, Some("jsonl")).unwrap();
    let cols: Vec<&str> = forced.df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["a", "b"]);
}

#[test]
fn a_plain_table_carries_no_container_path() {
    let path = Path::new("test_data/sample.csv");
    let opened = open_target(path, None, None).unwrap();
    assert!(
        opened.sqlite_db_path.is_none()
            && opened.duckdb_db_path.is_none()
            && opened.xlsx_db_path.is_none()
    );
}

// ── A database is what its header says, whatever it is called (#58) ──────────────

#[test]
fn a_database_without_an_extension_opens_as_a_listing() {
    let path = sqlite_at("noext");
    let opened = open_target(&path, None, None).unwrap();
    assert_eq!(opened.df.columns[0].name, "Table");
    assert_eq!(opened.sqlite_db_path.as_deref(), Some(path.as_path()));
}

#[test]
fn a_database_under_a_foreign_extension_opens_as_a_listing() {
    // Audacity's project format, one of the files in the report.
    let path = sqlite_at("project.aup3");
    let opened = open_target(&path, None, None).unwrap();
    assert_eq!(opened.df.columns[0].name, "Table");
    assert_eq!(opened.sqlite_db_path.as_deref(), Some(path.as_path()));
}

#[test]
fn a_database_named_as_json_opens_as_a_listing() {
    // The extension names a format with a reader of its own; the header still wins.
    let path = sqlite_at("looks.json");
    let opened = open_target(&path, None, None).unwrap();
    assert_eq!(opened.df.columns[0].name, "Table");
    assert_eq!(opened.sqlite_db_path.as_deref(), Some(path.as_path()));
}

#[test]
fn a_duckdb_file_without_an_extension_opens_as_a_listing() {
    let path = dir().join("duck-noext");
    let _ = std::fs::remove_file(&path);
    duckdb::Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1);")
        .unwrap();
    let opened = open_target(&path, None, None).unwrap();
    assert_eq!(opened.duckdb_db_path.as_deref(), Some(path.as_path()));
    assert!(opened.sqlite_db_path.is_none());
}

#[test]
fn a_text_file_without_an_extension_still_reads_as_csv() {
    let path = dir().join("plain-noext");
    std::fs::write(&path, "a,b\n1,2\n").unwrap();
    let opened = open_target(&path, None, None).unwrap();
    let cols: Vec<&str> = opened.df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["a", "b"]);
    assert!(opened.sqlite_db_path.is_none());
}

#[test]
fn a_file_too_short_to_carry_a_header_is_not_a_database() {
    let path = dir().join("short-noext");
    std::fs::write(&path, "a\n1\n").unwrap();
    let opened = open_target(&path, None, None).unwrap();
    let cols: Vec<&str> = opened.df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["a"]);
    assert!(opened.sqlite_db_path.is_none());
}

#[test]
fn load_file_reads_a_database_without_an_extension_as_its_listing() {
    // The join picker and the file-list sheet load through here, not `open_target`.
    let path = sqlite_at("noext-direct");
    let df = tuitab::data::io::load_file(&path, None).unwrap();
    assert_eq!(df.columns[0].name, "Table");
}

fn table_names(path: &Path) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap();
    let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
    rows.map(|r| r.unwrap()).collect()
}

#[test]
fn saving_over_an_existing_database_adds_a_table_instead_of_writing_csv() {
    use tuitab::data::io::db_write::{kind_of_file, DbKind};
    let path = sqlite_at("named.csv");
    let df = tuitab::data::io::load_file(Path::new("test_data/sample.csv"), None).unwrap();
    tuitab::data::io::save_file_as(
        &df,
        None,
        &path,
        tuitab::data::io::doc_io::Shape::Records,
        "added",
    )
    .unwrap();
    assert_eq!(
        kind_of_file(&path),
        Some(DbKind::Sqlite),
        "the database was overwritten"
    );
    assert_eq!(table_names(&path), vec!["added", "users"]);
}

#[test]
fn saving_to_a_new_path_without_an_extension_still_writes_csv() {
    let path = dir().join("new-out");
    let _ = std::fs::remove_file(&path);
    let df = tuitab::data::io::load_file(Path::new("test_data/sample.csv"), None).unwrap();
    tuitab::data::io::save_file_as(
        &df,
        None,
        &path,
        tuitab::data::io::doc_io::Shape::Records,
        "data",
    )
    .unwrap();
    assert!(tuitab::data::io::db_write::kind_of_file(&path).is_none());
    let written = std::fs::read_to_string(&path).unwrap();
    let sample = std::fs::read_to_string("test_data/sample.csv").unwrap();
    assert_eq!(written.lines().next(), sample.lines().next());
}

#[cfg(unix)]
#[test]
fn a_fifo_is_not_a_database_and_is_not_opened() {
    // Opening a FIFO with no writer blocks, so the check runs on a thread: a regression
    // fails here instead of hanging the suite.
    let fifo = dir().join("pipe");
    let _ = std::fs::remove_file(&fifo);
    let c = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
    let (tx, rx) = std::sync::mpsc::channel();
    let path = fifo.clone();
    std::thread::spawn(move || {
        let _ = tx.send(tuitab::data::io::db_write::is_db_path(&path));
    });
    let answer = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("is_db_path blocked on a FIFO");
    assert!(!answer);
}

#[test]
fn enter_in_a_directory_listing_opens_a_database_without_an_extension() {
    // The listing's own `Supported` column goes by extension; Enter asks the file.
    let base = dir().join("browse");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let db = base.join("db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("CREATE TABLE users (id INTEGER); INSERT INTO users VALUES (1);")
        .unwrap();

    let mut app = tuitab::app::App::new_as(&base, None, None).unwrap();
    app.open_directory_row(); // the only row is already under the cursor
    assert_eq!(
        app.stack.active().sqlite_db_path.as_deref(),
        Some(db.as_path()),
        "{}",
        app.status_message
    );
}

// ── `--type` reads any format the MCP server can ────────────────────────────────

fn csv_dat(name: &str) -> PathBuf {
    let path = dir().join(name);
    std::fs::write(&path, "a,b\n1,2\n3,4\n").unwrap();
    path
}

#[test]
fn an_explicit_tabular_type_reads_the_file_as_that() {
    let path = csv_dat("export.dat");
    let opened = open_target(&path, None, Some("csv")).unwrap();
    let cols: Vec<&str> = opened.df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["a", "b"]);
    assert_eq!(opened.df.visible_row_count(), 2);
}

#[test]
fn an_explicit_type_is_case_insensitive() {
    let path = csv_dat("upper.dat");
    let opened = open_target(&path, None, Some("CSV")).unwrap();
    let cols: Vec<&str> = opened.df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["a", "b"]);
}

#[test]
fn an_explicit_database_type_opens_a_listing() {
    let path = sqlite_at("forced.bin");
    let opened = open_target(&path, None, Some("sqlite")).unwrap();
    assert_eq!(opened.sqlite_db_path.as_deref(), Some(path.as_path()));
}

#[test]
fn an_explicit_spreadsheet_type_reads_a_workbook_under_another_name() {
    let path = xlsx_at("book.bin", &["only"]);
    let opened = open_target(&path, None, Some("xlsx")).unwrap();
    assert_eq!(opened.df.columns[0].name, "col");
}

#[test]
fn an_unknown_type_is_an_error_that_names_it() {
    let path = csv_dat("unknown.dat");
    let err = open_target(&path, None, Some("foo"))
        .err()
        .expect("--type foo must not be ignored");
    assert!(format!("{err:#}").contains("foo"), "{err:#}");
}

#[test]
fn a_text_file_with_duck_at_offset_eight_is_not_a_database() {
    // `animals,` is eight bytes, so `DUCK` lands exactly where DuckDB keeps its magic.
    // Long enough for the header to be read and compared, with no extension to lean on.
    let path = dir().join("birds-noext");
    std::fs::write(&path, "animals,DUCK,goose\n1,2,3\n").unwrap();
    assert!(tuitab::data::io::db_write::kind_of_file(&path).is_none());
    let opened = open_target(&path, None, None).unwrap();
    let cols: Vec<&str> = opened.df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cols, vec!["animals", "DUCK", "goose"]);
    assert!(opened.duckdb_db_path.is_none());
}

// ── A file too large to load opens its first lines ─────────────────────────────────

mod keys;

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use keys::key;
use tuitab::data::io::{open_target_from, Sample, Sampling};

/// Three records of eight bytes each.
const LINES: &str = "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n";

fn lines_at(name: &str) -> PathBuf {
    let path = dir().join(name);
    std::fs::write(&path, LINES).unwrap();
    path
}

#[test]
fn a_jsonl_past_the_threshold_opens_its_first_whole_lines() {
    let path = lines_at("big.jsonl");
    let opened = open_target_from(
        &path,
        None,
        None,
        Sampling {
            from: 0,
            chunk: 20,
            cap: 16,
        },
    )
    .unwrap();
    assert_eq!(
        opened.df.visible_row_count(),
        2,
        "the third line is cut, so not read"
    );
    let sample = opened.sample.expect("a sampled open says so");
    assert_eq!((sample.read, sample.size), (16, 24));
    assert_eq!(sample.file, path);
    assert!(opened.doc.is_none(), "a table needs no tree");
    assert!(opened.tail.is_some(), "the rest follows in the background");
}

#[test]
fn a_jsonl_under_the_threshold_opens_whole() {
    let path = lines_at("small.jsonl");
    let opened = open_target_from(
        &path,
        None,
        None,
        Sampling {
            from: 25,
            chunk: 20,
            cap: 16,
        },
    )
    .unwrap();
    assert_eq!(opened.df.visible_row_count(), 3);
    assert!(opened.sample.is_none());
}

#[test]
fn a_json_file_holding_lines_is_sampled_too() {
    let path = lines_at("big-lines.json");
    let opened = open_target_from(
        &path,
        None,
        None,
        Sampling {
            from: 0,
            chunk: 20,
            cap: 16,
        },
    )
    .unwrap();
    assert_eq!(opened.df.visible_row_count(), 2);
}

fn sampled_app(name: &str) -> (tuitab::app::App, PathBuf) {
    let path = lines_at(name);
    let mut app = tuitab::app::App::new_as(&path, None, None).unwrap();
    app.stack.active_mut().sample = Some(Sample {
        file: path.clone(),
        read: 16,
        size: 24,
    });
    (app, path)
}

fn save_to(app: &mut tuitab::app::App, path: &Path) {
    let event = KeyEvent {
        code: KeyCode::Char('s'),
        modifiers: KeyModifiers::CONTROL,
        kind: KeyEventKind::Press,
        state: crossterm::event::KeyEventState::NONE,
    };
    let action = tuitab::event::handle_key_event(event, app.mode, app.stack.can_pop());
    app.handle_action(action);
    app.save.input =
        tuitab::ui::text_input::TextInput::with_value(path.to_string_lossy().into_owned());
    key(app, KeyCode::Enter);
}

/// Writing the sample back would cut the file down to it.
#[test]
fn a_sample_is_never_saved_over_its_file() {
    let (mut app, path) = sampled_app("guarded.jsonl");
    save_to(&mut app, &path);
    assert!(app.save.error.is_some(), "the save is refused");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), LINES);

    // Nor is a table made from it.
    let (mut app, path) = sampled_app("guarded-freq.jsonl");
    // The frequency table defers one frame through the Calculating overlay.
    app.handle_action(tuitab::types::Action::OpenFrequencyTable);
    app.handle_action(tuitab::types::Action::OpenFrequencyTable);
    assert!(app.stack.can_pop(), "the frequency table is open");
    assert!(
        app.stack.active().sample.is_some(),
        "a frequency of a sample is one"
    );
    save_to(&mut app, &path);
    assert!(app.save.error.is_some(), "the save is refused");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), LINES);

    // Anywhere else is fine.
    let (mut app, _) = sampled_app("guarded-elsewhere.jsonl");
    let other = dir().join("sample-copy.jsonl");
    let _ = std::fs::remove_file(&other);
    save_to(&mut app, &other);
    assert!(app.save.error.is_none(), "{:?}", app.save.error);
    assert_eq!(std::fs::read_to_string(&other).unwrap().lines().count(), 3);
}

/// The title says the sheet is part of its file.
#[test]
fn a_sample_says_so_in_its_title() {
    let (mut app, _) = sampled_app("titled.jsonl");
    let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(100, 10)).unwrap();
    terminal.draw(|f| tuitab::ui::render(f, &mut app)).unwrap();
    let screen: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    assert!(screen.contains("first 16 B of 24 B"), "{}", screen);
}

// ── JSON Lines is a table: read into columns, no tree kept ─────────────────────────

use tuitab::data::io::{doc_io::Shape, save_file_as};

fn table_of(name: &str, text: &str, sampling: Sampling) -> tuitab::data::dataframe::DataFrame {
    let path = dir().join(name);
    std::fs::write(&path, text).unwrap();
    let opened = open_target_from(&path, None, None, sampling).unwrap();
    assert!(opened.doc.is_none(), "JSON Lines keeps no tree");
    opened.df
}

fn cell(df: &tuitab::data::dataframe::DataFrame, row: usize, name: &str) -> String {
    df.get_physical(row, df.column_index(name).unwrap())
}

fn is_json(df: &tuitab::data::dataframe::DataFrame, name: &str) -> bool {
    df.columns[df.column_index(name).unwrap()].json
}

/// A piece of eight bytes at a time, read to the end before the sheet opens.
const PIECEMEAL: Sampling = Sampling {
    from: u64::MAX,
    chunk: 8,
    cap: u64::MAX,
};

#[test]
fn a_column_of_one_kind_is_typed_and_any_other_holds_json() {
    let df = table_of(
        "kinds.jsonl",
        "{\"n\":1,\"s\":\"x\",\"m\":1,\"o\":{\"k\":1}}\n{\"n\":2,\"s\":\"y\",\"m\":\"two\",\"o\":[1,2]}\n",
        Sampling::default(),
    );
    assert_eq!(
        df.df.column("n").unwrap().dtype(),
        &polars::prelude::DataType::Int64
    );
    assert!(!is_json(&df, "n") && !is_json(&df, "s"));
    assert_eq!(cell(&df, 0, "s"), "x");
    assert!(is_json(&df, "m") && is_json(&df, "o"));
    assert_eq!(
        (cell(&df, 0, "m"), cell(&df, 1, "m")),
        ("1".into(), "\"two\"".into())
    );
    assert_eq!(
        (cell(&df, 0, "o"), cell(&df, 1, "o")),
        ("{\"k\":1}".into(), "[1,2]".into())
    );
}

/// What a JSON Lines sheet holds is what it writes: a long nested value whole, a
/// string that holds JSON as a string, `5` apart from `"5"`.
#[test]
fn a_jsonl_table_saves_back_the_values_it_read() {
    let long: String = (0..60)
        .map(|i| format!("\"key{}\":\"value {}\",", i, i))
        .collect();
    let nested = format!("{{{}\"end\":true}}", long);
    assert!(nested.len() > 512);
    let lines = [
        format!(
            "{{\"id\":1,\"client\":\"{{\\\"country\\\": \\\"US\\\"}}\",\"geo\":{},\"code\":5}}",
            nested
        ),
        "{\"id\":2,\"client\":\"plain\",\"geo\":[1,{\"a\":null}],\"code\":\"5\"}".to_string(),
    ];
    let text = lines.join("\n") + "\n";
    let path = dir().join("fidelity.jsonl");
    std::fs::write(&path, &text).unwrap();
    let df = open_target(&path, None, None).unwrap().df;
    assert!(!is_json(&df, "client"), "a column of strings is strings");

    let saved = dir().join("fidelity-saved.jsonl");
    let _ = std::fs::remove_file(&saved);
    save_file_as(&df, None, &saved, Shape::Records, "t").unwrap();
    let parse = |t: &str| -> Vec<serde_json::Value> {
        t.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    };
    assert_eq!(
        parse(&std::fs::read_to_string(&saved).unwrap()),
        parse(&text)
    );
}

#[test]
fn pieces_of_other_types_meet_in_one_column() {
    // An integer then a float: a number column.
    let df = table_of("int-float.jsonl", "{\"a\":1}\n{\"a\":2.5}\n", PIECEMEAL);
    assert_eq!(
        df.df.column("a").unwrap().dtype(),
        &polars::prelude::DataType::Float64
    );
    assert!(!is_json(&df, "a"));
    // A string then a number: JSON, the string quoted.
    let df = table_of("str-int.jsonl", "{\"b\":\"x\"}\n{\"b\":3}\n", PIECEMEAL);
    assert!(is_json(&df, "b"));
    assert_eq!(
        (cell(&df, 0, "b"), cell(&df, 1, "b")),
        ("\"x\"".into(), "3".into())
    );
    // A field first seen late: empty behind it.
    let df = table_of("late.jsonl", "{\"a\":1}\n{\"c\":7}\n", PIECEMEAL);
    assert_eq!(df.visible_row_count(), 2);
    assert_eq!(
        (cell(&df, 0, "c"), cell(&df, 1, "c")),
        ("".into(), "7".into())
    );
    assert_eq!(
        df.df.column("c").unwrap().dtype(),
        &polars::prelude::DataType::Int64
    );
}

#[test]
fn a_bad_line_names_its_line_across_pieces() {
    let path = dir().join("bad-piece.jsonl");
    std::fs::write(&path, "{\"a\":1}\n{\"a\":2}\n{oops}\n").unwrap();
    let err = open_target_from(&path, None, None, PIECEMEAL)
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("line 3"), "{}", err);
}

/// JSON Lines has one shape, so there is nothing to ask.
#[test]
fn saving_a_table_as_jsonl_asks_no_shape() {
    let (mut app, _) = sampled_app("shapeless.jsonl");
    app.stack.active_mut().sample = None;
    let other = dir().join("shapeless-copy.jsonl");
    let _ = std::fs::remove_file(&other);
    save_to(&mut app, &other);
    assert_ne!(app.mode, tuitab::types::AppMode::SaveShapeSelect);
    assert_eq!(std::fs::read_to_string(&other).unwrap(), LINES);
}

#[test]
fn json_lines_text_is_a_table() {
    let df = tuitab::data::io::jsonl_table("{\"a\":1}\n{\"a\":2}\n").unwrap();
    assert_eq!(df.visible_row_count(), 2);
    assert_eq!(
        df.df.column("a").unwrap().dtype(),
        &polars::prelude::DataType::Int64
    );
}

/// Blank lines between records — or a piece that is nothing else — add no column.
#[test]
fn blank_lines_add_no_column() {
    let df = table_of("blanks.jsonl", "{\"a\":1}\n\n\n{\"a\":2}\n\n", PIECEMEAL);
    let names: Vec<&str> = df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["a"]);
    assert_eq!(df.visible_row_count(), 2);
}

/// Keys a path would quote, empty strings and lines that are not objects come back as
/// they were read.
#[test]
fn a_jsonl_table_saves_back_its_keys_strings_and_bare_lines() {
    let text = "{\"log.level\":\"info\",\"s\":\"\",\"\":1}\n[1,2]\n{\"log.level\":\"warn\",\"s\":\"x\",\"\":2}\n5\n";
    let path = dir().join("keys.jsonl");
    std::fs::write(&path, text).unwrap();
    let df = open_target(&path, None, None).unwrap().df;
    assert!(
        df.column_index("log.level").is_ok(),
        "a key names its column as written"
    );

    let saved = dir().join("keys-saved.jsonl");
    let _ = std::fs::remove_file(&saved);
    save_file_as(&df, None, &saved, Shape::Records, "t").unwrap();
    let parse = |t: &str| -> Vec<serde_json::Value> {
        t.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    };
    assert_eq!(
        parse(&std::fs::read_to_string(&saved).unwrap()),
        parse(text)
    );
}

/// Bare lines in one piece and a field named `value` in another stay apart.
#[test]
fn bare_lines_and_a_value_field_stay_apart_across_pieces() {
    // Twelve bytes: the record is one piece, the bare line the next.
    let df = table_of(
        "bare-value.jsonl",
        "{\"value\":1}\n5\n",
        Sampling {
            chunk: 12,
            ..PIECEMEAL
        },
    );
    assert_eq!(
        df.col_count(),
        2,
        "{:?}",
        df.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
    assert_eq!(cell(&df, 0, "value"), "1");
    assert_eq!(cell(&df, 1, "value"), "");
}

/// Files of a JSON Lines pattern meet as pieces of one file do: the nested field stays
/// JSON, and saving writes it back as an object.
#[test]
fn a_jsonl_pattern_keeps_its_json_columns() {
    let sub = dir().join("jsonl-pattern");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(sub.join("a.jsonl"), "{\"id\":1,\"geo\":{\"lat\":1}}\n").unwrap();
    std::fs::write(
        sub.join("b.jsonl"),
        "{\"id\":2,\"geo\":{\"lat\":2},\"extra\":true}\n",
    )
    .unwrap();
    let app = tuitab::app::App::new_as(&sub.join("*.jsonl"), None, None).unwrap();
    let df = &app.stack.active().dataframe;
    assert!(is_json(df, "geo"));
    assert_eq!(df.visible_row_count(), 2);
    let saved = dir().join("jsonl-pattern-saved.jsonl");
    let _ = std::fs::remove_file(&saved);
    save_file_as(df, None, &saved, Shape::Records, "t").unwrap();
    let first: serde_json::Value = serde_json::from_str(
        std::fs::read_to_string(&saved)
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first["geo"], serde_json::json!({"lat": 1}));
}

/// A byte that is not UTF-8 is refused with its line, as every other reader refuses it
/// — not quietly replaced, which a save would then write.
#[test]
fn a_jsonl_line_that_is_not_utf8_is_refused_by_line() {
    let path = dir().join("latin1.jsonl");
    std::fs::write(&path, b"{\"a\":\"ok\"}\n{\"a\":\"caf\xe9\"}\n").unwrap();
    let err = open_target(&path, None, None).err().unwrap().to_string();
    assert!(err.contains("line 2") && err.contains("UTF-8"), "{}", err);
}

/// Saved onto the file it was read from, a `.json` of JSON Lines stays lines; saved
/// over another `.json`, the sheet is written as JSON, whatever that file held before.
#[test]
fn a_json_file_of_lines_stays_lines_only_when_saved_onto_itself() {
    let path = dir().join("own-lines.json");
    std::fs::write(&path, LINES).unwrap();
    let mut app = tuitab::app::App::new_as(&path, None, None).unwrap();
    save_to(&mut app, &path);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), LINES);

    let other = dir().join("other-lines.json");
    std::fs::write(&other, LINES).unwrap();
    let mut app = tuitab::app::App::new_as(&path, None, None).unwrap();
    save_to(&mut app, &other);
    assert_eq!(
        app.mode,
        tuitab::types::AppMode::SaveShapeSelect,
        "a new JSON asks its shape"
    );
}

/// `head logs.json | tuitab -t json` on JSON Lines content: the same check a `.json`
/// file gets, so it opens as rows rather than failing on "trailing characters".
#[test]
fn stdin_typed_json_holding_json_lines_opens_as_rows() {
    let lines = b"{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n".to_vec();
    let (df, state) = tuitab::data::io::load_from_bytes(lines, "json", None).unwrap();
    assert_eq!(df.df.height(), 2);
    assert!(state.is_none(), "JSON Lines is a table, not a tree");
}
