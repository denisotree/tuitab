//! Wide to long: a column per country becomes a row per country.

use serde_json::{json, Value};
use std::path::PathBuf;
use tuitab::mcp::{handle_message, Server};

/// Written once: the tests read it in parallel, and a rewrite under a reader is an
/// empty file.
fn wide() -> PathBuf {
    static WRITTEN: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    WRITTEN.get_or_init(write_wide).clone()
}

fn write_wide() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("unpivot-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("rps-by-country.csv");
    std::fs::write(
        &path,
        "Time,IN,US,BD\n\
         1,10,20,undefined\n\
         2,11,21,5\n\
         3,12,22,6\n",
    )
    .unwrap();
    path
}

fn call(ops: Value) -> Value {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "tuitab_query",
                   "arguments": {"source": wide().to_string_lossy(), "ops": ops,
                                 "output": {"limit": 1000}}}});
    handle_message(&mut Server::new(), &message.to_string()).unwrap()["result"].clone()
}

fn ok(ops: Value) -> Value {
    let result = call(ops);
    assert_eq!(result["isError"], json!(false), "{}", result);
    result["structuredContent"].clone()
}

fn names(out: &Value) -> Vec<String> {
    out["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn unpivot_star_turns_country_columns_into_rows() {
    let out = ok(json!([
        {"unpivot": {"index": ["Time"], "columns": "*", "name": "country",
                     "value": "rps", "drop_nulls": true}},
        {"frequency": {"by": ["country"]}}
    ]));
    let counts: Vec<(Value, Value)> = out["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r[0].clone(), r[1].clone()))
        .collect();
    assert_eq!(
        counts,
        [
            (json!("IN"), json!(3)),
            (json!("US"), json!(3)),
            (json!("BD"), json!(2))
        ]
    );
}

#[test]
fn unpivot_keeps_nulls_unless_told_and_names_its_columns() {
    let out = ok(json!([{"unpivot": {"index": ["Time"]}}]));
    assert_eq!(names(&out), ["Time", "variable", "value"]);
    assert_eq!(out["row_count"], 9);
    let types: Vec<&str> = out["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["type"].as_str().unwrap())
        .collect();
    assert_eq!(types, ["integer", "string", "integer"]);
}

#[test]
fn unpivot_some_columns() {
    let out = ok(json!([{"unpivot": {"index": ["Time"], "columns": ["US"], "value": "rps"}}]));
    assert_eq!(names(&out), ["Time", "variable", "rps"]);
    assert_eq!(out["row_count"], 3);
}

#[test]
fn unpivot_mixed_text_and_numbers_is_refused() {
    let out = call(json!([
        {"compute": {"name": "label", "expr": "concat('t', Time)"}},
        {"unpivot": {"index": ["Time"], "columns": ["IN", "label"]}}
    ]));
    assert_eq!(out["isError"], json!(true), "{}", out);
    let text = out["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("label") && text.contains("IN"), "{}", text);
}

#[test]
fn unpivot_refuses_an_unknown_column_and_a_name_that_is_taken() {
    for ops in [
        json!([{"unpivot": {"index": ["nope"]}}]),
        json!([{"unpivot": {"index": ["Time"], "name": "Time"}}]),
    ] {
        assert_eq!(call(ops.clone())["isError"], json!(true), "{}", ops);
    }
}

#[test]
fn gw_unpivots_the_unpinned_columns() {
    use tuitab::types::Action;
    let mut app = tuitab::app::App::new_as(&wide(), None, None).unwrap();
    let time = app.stack.active().dataframe.column_index("Time").unwrap();
    app.stack.active_mut().dataframe.columns[time].pinned = true;
    let sheets = app.stack.depth();
    app.handle_action(Action::Unpivot);
    assert_eq!(app.stack.depth(), sheets + 1, "{}", app.status_message);
    let df = &app.stack.active().dataframe;
    let names: Vec<&str> = df.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["Time", "variable", "value"]);
    assert_eq!(df.visible_row_count(), 9);
}

#[test]
fn gw_without_a_pinned_column_says_what_to_do() {
    use tuitab::types::Action;
    let mut app = tuitab::app::App::new_as(&wide(), None, None).unwrap();
    let sheets = app.stack.depth();
    app.handle_action(Action::Unpivot);
    assert_eq!(app.stack.depth(), sheets);
    assert!(app.status_message.contains("Pin"), "{}", app.status_message);
}

/// Columns of kinds that do not mix are named by their kinds.
#[test]
fn unpivot_names_the_kinds_it_cannot_mix() {
    let out = call(json!([
        {"compute": {"name": "when", "expr": "from_epoch(Time, \"s\")"}},
        {"compute": {"name": "big", "expr": "US > 20"}},
        {"compute": {"name": "label", "expr": "concat('t', Time)"}},
        {"unpivot": {"index": ["Time"], "columns": ["when", "big", "label"]}}
    ]));
    assert_eq!(out["isError"], json!(true), "{}", out);
    let text = out["content"][0]["text"].as_str().unwrap();
    for part in ["when is a datetime", "big is true/false", "label is text"] {
        assert!(text.contains(part), "{}: {}", part, text);
    }
}

/// A column cannot be both an index and one of the unpivoted.
#[test]
fn unpivot_refuses_an_index_column_among_the_unpivoted() {
    let out = call(json!([{"unpivot": {"index": ["Time"], "columns": ["Time", "IN"]}}]));
    assert_eq!(out["isError"], json!(true), "{}", out);
    let text = out["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("'Time'") && text.contains("index"),
        "{}",
        text
    );
}

/// Datetimes in different units unpivot into one datetime column.
#[test]
fn unpivot_datetimes_of_different_units() {
    let out = ok(json!([
        {"compute": {"name": "a", "expr": "from_epoch(Time, \"ms\")"}},
        {"compute": {"name": "b", "expr": "from_epoch(Time + 1787569800000000000, \"ns\")"}},
        {"unpivot": {"index": ["Time"], "columns": ["a", "b"]}}
    ]));
    // The nanosecond column keeps its digits next to the millisecond one.
    assert_eq!(
        out["rows"][3][2],
        json!("2026-08-24 11:10:00.000000001"),
        "{}",
        out
    );
    assert_eq!(out["columns"][2]["type"], json!("datetime"), "{}", out);
    assert_eq!(out["row_count"], json!(6), "{}", out);
}

/// JSON a column holds stays JSON through an unpivot and a join: saved, the values come
/// back as the objects and strings they were.
#[test]
fn json_columns_stay_json_through_unpivot_and_join() {
    use tuitab::data::io::jsonl_table;
    let wide = jsonl_table("{\"id\":1,\"a\":{\"x\":1},\"b\":\"s\"}\n").unwrap();
    let long = tuitab::data::unpivot::unpivot(
        &wide,
        &tuitab::data::unpivot::Spec {
            index: vec!["id".into()],
            columns: None,
            name: "key".into(),
            value: "val".into(),
            drop_nulls: false,
        },
    )
    .unwrap();
    let val = long.column_index("val").unwrap();
    assert!(long.columns[val].json);
    let cells: Vec<String> = (0..2).map(|r| long.get_physical(r, val)).collect();
    assert_eq!(cells, ["{\"x\":1}", "\"s\""]);

    let right = jsonl_table("{\"id\":1,\"n\":5}\n").unwrap();
    let joined = tuitab::data::join::join_dataframes(
        &wide,
        &right,
        &["id".to_string()],
        &["id".to_string()],
        tuitab::data::join::JoinType::Left,
    )
    .unwrap();
    assert!(joined.columns[joined.column_index("a").unwrap()].json);
}
