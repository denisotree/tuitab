//! Boolean columns typed as boolean, and frequency tables that count every row.

use serde_json::{json, Value};
use std::path::PathBuf;
use tuitab::mcp::{handle_message, Server};

fn file(name: &str, text: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("boolean-frequency-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

fn call(tool: &str, arguments: Value) -> Value {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                         "params": {"name": tool, "arguments": arguments}});
    let result =
        handle_message(&mut Server::new(), &message.to_string()).unwrap()["result"].clone();
    assert_eq!(result["isError"], json!(false), "{}", result);
    result["structuredContent"].clone()
}

fn type_of(out: &Value, name: &str) -> String {
    out["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == name)
        .unwrap()["type"]
        .as_str()
        .unwrap()
        .to_string()
}

/// A true/false column was reported — and filtered, and aggregated — as text.
#[test]
fn a_boolean_column_is_reported_as_boolean() {
    let csv = file("flags.csv", "id,mobile\n1,true\n2,false\n3,true\n");
    let out = call("tuitab_inspect", json!({"source": csv}));
    assert_eq!(type_of(&out, "mobile"), "boolean", "{}", out);

    let jsonl = file(
        "flags.jsonl",
        "{\"id\":1,\"ok\":true}\n{\"id\":2,\"ok\":false}\n",
    );
    let out = call("tuitab_inspect", json!({"source": jsonl}));
    assert_eq!(type_of(&out, "ok"), "boolean", "{}", out);
}

#[test]
fn a_boolean_column_filters_and_sums() {
    let csv = file("flags2.csv", "id,mobile\n1,true\n2,false\n3,true\n");
    let out = call(
        "tuitab_query",
        json!({"source": csv, "ops": [
            {"filter": {"col": "mobile", "op": "eq", "value": true}},
            {"aggregate": [{"col": "*", "fn": "count"}, {"col": "mobile", "fn": "sum"}]}
        ]}),
    );
    assert_eq!(out["rows"][0][0], json!(2), "{}", out);
    assert_eq!(out["rows"][0][1], json!(2), "{}", out);
}

/// One column counted rows with `len()`, several with `count()` over the first
/// column — which skips that column's NULLs, so a group keyed by a NULL read 0.
#[test]
fn multi_column_frequency_counts_rows_with_a_null_first_key() {
    let csv = file("keys.csv", "a,b\n,x\n,x\n1,y\n");
    let out = call(
        "tuitab_query",
        json!({"source": csv, "ops": [{"frequency": {"by": ["a", "b"]}}]}),
    );
    let rows = out["rows"].as_array().unwrap();
    let null_x = rows
        .iter()
        .find(|r| r[0].is_null() && r[1] == json!("x"))
        .unwrap_or_else(|| panic!("no (NULL, x) group in {}", out));
    let count_at = out["columns"]
        .as_array()
        .unwrap()
        .iter()
        .position(|c| c["name"] == "Count")
        .unwrap();
    assert_eq!(null_x[count_at], json!(2), "{}", out);
}
