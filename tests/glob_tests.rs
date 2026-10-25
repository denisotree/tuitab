//! Patterns, directories and markdown pages — reading many files as one table.
//!
//! The three arrived together because they are one task: a directory of pages is only
//! reachable through a pattern, and a pattern is only worth having if what it matches
//! can be read.

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tuitab::app::App;
use tuitab::mcp::{handle_message, Server};

fn dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("glob-tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write(rel: &str, text: &str) -> PathBuf {
    let path = dir().join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, text).unwrap();
    path
}

fn call(server: &mut Server, name: &str, arguments: Value) -> (bool, Value) {
    let response = handle_message(
        server,
        &json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {"name": name, "arguments": arguments}})
        .to_string(),
    )
    .expect("a request must get a response");
    let result = &response["result"];
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    let is_error = result["isError"].as_bool().unwrap_or(false);
    let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (is_error, body)
}

fn rows(server: &mut Server, path: &str) -> (bool, Value) {
    call(
        server,
        "tuitab_query",
        json!({"source": {"path": path}, "ops": []}),
    )
}

#[test]
fn a_pattern_reads_every_file_it_matches() {
    let base = dir().join("csvs");
    let _ = std::fs::remove_dir_all(&base);
    write("csvs/one.csv", "a,b\n1,x\n2,y\n");
    write("csvs/two.csv", "a,b\n3,z\n");
    write("csvs/sub/three.csv", "a,b\n9,q\n");

    let mut server = Server::new();
    let (err, out) = rows(&mut server, &format!("{}/*.csv", base.display()));
    assert!(!err, "{}", out);
    assert_eq!(out["row_count"], json!(3), "{}", out);

    // `**` reaches the nested one, and the order is the sorted one, twice running.
    let deep = format!("{}/**/*.csv", base.display());
    let (err, out) = rows(&mut server, &deep);
    assert!(!err, "{}", out);
    assert_eq!(out["row_count"], json!(4));
    let (_, again) = rows(&mut server, &deep);
    assert_eq!(out["rows"], again["rows"], "a pattern is not a coin toss");

    // A relative pattern answers the same as the absolute one.
    let (err, relative) = rows(&mut server, "tmp/glob-tests/csvs/**/*.csv");
    assert!(!err, "{}", relative);
    assert_eq!(relative["rows"], out["rows"]);
}

#[test]
fn a_pattern_that_matches_nothing_says_so() {
    let mut server = Server::new();
    let (err, out) = rows(&mut server, "tmp/glob-tests/csvs/nothing-*.csv");
    assert!(err);
    // Not "No such file": the pattern is fine and nothing matched it, and the two call
    // for different next moves.
    assert!(
        out.as_str().unwrap().contains("glob matched no files"),
        "{}",
        out
    );
}

#[test]
fn a_file_with_other_columns_is_named_rather_than_stacked() {
    let base = dir().join("mismatch");
    let _ = std::fs::remove_dir_all(&base);
    write("mismatch/good.csv", "a,b\n1,x\n");
    write("mismatch/odd.csv", "c\n7\n");

    let mut server = Server::new();
    let (err, out) = rows(&mut server, &format!("{}/*.csv", base.display()));
    assert!(err, "{}", out);
    let message = out.as_str().unwrap();
    assert!(message.contains("odd.csv"), "{}", message);
    assert!(message.contains("different columns"), "{}", message);
}

#[test]
fn a_directory_is_a_list_of_its_files() {
    let base = dir().join("mixed");
    let _ = std::fs::remove_dir_all(&base);
    write("mixed/data.csv", "a\n1\n");
    write("mixed/cover.webp", "not an image, near enough");

    // It used to be handed to the CSV reader, which refused a directory holding two
    // extensions and advised a glob pattern the server then would not accept.
    let mut server = Server::new();
    let (err, out) = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": {"path": base.to_string_lossy()}}),
    );
    assert!(!err, "{}", out);
    let names: Vec<&str> = out["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"Name"), "{:?}", names);
    assert_eq!(out["row_count"], json!(2), "{}", out);
}

#[test]
fn a_page_is_a_row_of_its_frontmatter() {
    let base = dir().join("site");
    let _ = std::fs::remove_dir_all(&base);
    write(
        "site/bai-hao/index.md",
        "---\ntitle: Bai Hao\nprice: 12.5\n---\n\nBody one.\n\n---\n\nStill body.\n",
    );
    // A page with a field the first one lacks, and TOML frontmatter besides: a real
    // site has both, and a table of pages that refused them would be no use.
    write(
        "site/shou-mei/index.md",
        "+++\ntitle = \"Shou Mei\"\nprice = 9.0\ndraft = true\n+++\n\nBody two.\n",
    );

    let mut server = Server::new();
    let (err, out) = rows(&mut server, &format!("{}/*/index.md", base.display()));
    assert!(!err, "{}", out);
    assert_eq!(out["row_count"], json!(2), "{}", out);

    let names: Vec<&str> = out["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    // `file` first: with a pattern it is the only thing telling the rows apart.
    assert_eq!(names[0], "file");
    for wanted in ["title", "price", "draft", "body"] {
        assert!(names.contains(&wanted), "{:?} misses {}", names, wanted);
    }

    // The body keeps its own `---`, which is a rule and not the end of anything.
    let first = &out["rows"][0];
    assert!(
        first[names.iter().position(|n| *n == "body").unwrap()]
            .as_str()
            .unwrap()
            .contains("Still body"),
        "{}",
        first
    );
    // The field only the second page has is NULL on the first, not an error.
    assert!(first[names.iter().position(|n| *n == "draft").unwrap()].is_null());

    // And the point of the exercise: arithmetic over a directory of pages.
    let (err, total) = call(
        &mut server,
        "tuitab_query",
        json!({"source": {"path": format!("{}/*/index.md", base.display())},
               "ops": [{"aggregate": [{"col": "price", "fn": "sum"}]}]}),
    );
    assert!(!err, "{}", total);
    assert_eq!(total["rows"][0][0], json!(21.5));
}

// ── The terminal ───────────────────────────────────────────────────────────────
//
// The pattern reading above was the MCP server's alone: `App::new_as` asked
// `Path::exists`, got false for `data/*.csv`, and opened a blank writable sheet
// titled `*.csv` — offering, on Ctrl+S, to create a file with a star in its name.
// These hold the two surfaces to one answer.

#[test]
fn the_terminal_stacks_what_a_pattern_matches() {
    let base = dir().join("cli");
    let _ = std::fs::remove_dir_all(&base);
    write("cli/one.csv", "a,b\n1,x\n2,y\n");
    write("cli/two.csv", "a,b\n3,z\n");

    let pattern = format!("{}/*.csv", base.display());
    let app = App::new_as(Path::new(&pattern), None, None).expect("a pattern opens");

    let sheet = app.stack.active();
    assert_eq!(sheet.dataframe.visible_row_count(), 3);
    assert!(sheet.title.contains("*.csv"), "{}", sheet.title);
    assert!(
        app.status_message.contains("2 files"),
        "the file count is the only evidence the pattern caught what was meant: {}",
        app.status_message
    );

    // A pattern is not a file: no path to reload from, and nothing that would let a
    // save write back to one of the several files on screen.
    assert!(sheet.source_path.is_none(), "{:?}", sheet.source_path);

    // What the user actually sees, not just the field behind it: with no `source_path`
    // the prefill falls back through the hint to the sheet title, and the title is the
    // pattern.  Without the hint this offers to create a file with a `*` in its name.
    let mut app = app;
    app.handle_action(tuitab::types::Action::SaveFile);
    let offered = app.save.input.as_str();
    assert!(
        !offered.contains('*'),
        "Ctrl+S offers a filename with a star in it: {}",
        offered
    );
    assert!(!offered.is_empty(), "Ctrl+S offers nothing to save as");
}

#[test]
fn a_pattern_the_terminal_cannot_match_is_an_error_not_a_blank_sheet() {
    // The reported bug, stated as a test: this used to succeed, handing back an empty
    // one-column sheet and "does not exist yet — Ctrl+S creates it".  The directory has
    // to be there, or `blank_at` would refuse for the wrong reason and the test would
    // pass against the very bug it is here to catch.
    let _ = std::fs::remove_dir_all(dir().join("cli-empty"));
    write("cli-empty/keep.txt", "");
    let pattern = format!("{}/nothing-*.csv", dir().join("cli-empty").display());
    let Err(err) = App::new_as(Path::new(&pattern), None, None) else {
        panic!("a pattern that matches nothing is not a new file to create");
    };
    assert!(err.to_string().contains("glob matched no files"), "{}", err);
}

#[test]
fn the_terminal_names_the_file_with_other_columns() {
    let base = dir().join("cli-mismatch");
    let _ = std::fs::remove_dir_all(&base);
    write("cli-mismatch/good.csv", "a,b\n1,x\n");
    write("cli-mismatch/odd.csv", "c\n7\n");

    let pattern = format!("{}/*.csv", base.display());
    let Err(err) = App::new_as(Path::new(&pattern), None, None) else {
        panic!("columns disagree, so the pattern must not stack them");
    };
    let message = err.to_string();
    assert!(message.contains("odd.csv"), "{}", message);
    assert!(message.contains("different columns"), "{}", message);
}

#[test]
fn the_terminal_unions_markdown_pages() {
    let base = dir().join("cli-site");
    let _ = std::fs::remove_dir_all(&base);
    write("cli-site/one/index.md", "---\ntitle: One\n---\n\nBody.\n");
    // `draft` is the field only the second page carries: a refusal here would mean the
    // terminal stacked instead of unioning.
    write(
        "cli-site/two/index.md",
        "---\ntitle: Two\ndraft: true\n---\n\nBody.\n",
    );

    let pattern = format!("{}/*/index.md", base.display());
    let app = App::new_as(Path::new(&pattern), None, None).expect("pages union");
    let sheet = app.stack.active();
    assert_eq!(sheet.dataframe.visible_row_count(), 2);
    let names: Vec<&str> = sheet
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(names.contains(&"draft"), "{:?}", names);
}

#[test]
fn an_explicit_type_applies_to_every_file_a_pattern_matches() {
    let base = dir().join("cli-typed");
    let _ = std::fs::remove_dir_all(&base);
    write(
        "cli-typed/a.conf",
        "{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n",
    );
    write("cli-typed/b.conf", "{\"a\":3,\"b\":\"z\"}\n");

    let pattern = format!("{}/*.conf", base.display());
    let app = App::new_as(Path::new(&pattern), None, Some("jsonl"))
        .expect("--type reaches every matched file");
    let sheet = app.stack.active();
    assert_eq!(sheet.dataframe.visible_row_count(), 3);
    let names: Vec<&str> = sheet
        .dataframe
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, vec!["a", "b"]);
}

/// `-t md` says the pattern is pages, and pages are records: they need not share fields,
/// the same as without `-t` and as the MCP server's `format: md`.
#[test]
fn a_typed_md_pattern_unions_pages_with_different_fields() {
    // A page left over from an earlier run would change the count.
    let _ = std::fs::remove_dir_all(dir().join("typed-md"));
    write("typed-md/a.md", "---\ntitle: A\n---\nbody a\n");
    write("typed-md/b.md", "---\ntitle: B\ntags: x\n---\nbody b\n");
    let pattern = dir().join("typed-md").join("*.md");
    let app = App::new_as(&pattern, None, Some("md")).unwrap();
    let df = &app.stack.active().dataframe;
    assert_eq!(df.visible_row_count(), 2);
    assert!(df.column_index("title").is_ok() && df.column_index("tags").is_ok());
}
