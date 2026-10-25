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
