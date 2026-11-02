//! Aggregates over the rows a filter picks, side by side in one `group_by`.

use serde_json::{json, Value};
use std::path::PathBuf;
use tuitab::mcp::{handle_message, Server};

fn responses() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("conditional-agg-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("responses.csv");
    std::fs::write(
        &path,
        "day,code,ms\n\
         mon,200,10\n\
         mon,503,900\n\
         mon,503 slow,1200\n\
         mon,500,50\n\
         tue,200,12\n\
         tue,,8\n",
    )
    .unwrap();
    path
}

fn call(ops: Value) -> Value {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "tuitab_query",
                   "arguments": {"source": responses().to_string_lossy(), "ops": ops}}});
    handle_message(&mut Server::new(), &message.to_string()).unwrap()["result"].clone()
}

fn ok(ops: Value) -> Value {
    let result = call(ops);
    assert_eq!(result["isError"], json!(false), "{}", result);
    result["structuredContent"].clone()
}

fn error(ops: Value) -> String {
    let result = call(ops);
    assert_eq!(result["isError"], json!(true), "{}", result);
    result["content"][0]["text"].as_str().unwrap().to_string()
}

fn col(out: &Value, name: &str) -> Vec<Value> {
    let i = out["columns"]
        .as_array()
        .unwrap()
        .iter()
        .position(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("no {} in {}", name, out));
    out["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r[i].clone())
        .collect()
}

#[test]
fn conditional_counts_in_one_group_by() {
    let out = ok(json!([{"group_by": {"by": ["day"], "agg": [
        {"col": "*", "fn": "count"},
        {"col": "*", "fn": "count", "as": "n503",
         "where": [{"col": "code", "op": "contains", "value": "^503"}]},
        {"col": "*", "fn": "count", "as": "n500",
         "where": {"col": "code", "op": "eq", "value": "500"}}
    ]}}]));
    assert_eq!(col(&out, "day"), [json!("mon"), json!("tue")]);
    assert_eq!(col(&out, "count"), [json!(4), json!(2)]);
    assert_eq!(col(&out, "n503"), [json!(2), json!(0)]);
    assert_eq!(col(&out, "n500"), [json!(1), json!(0)]);
}

#[test]
fn sum_with_where() {
    let out = ok(json!([{"aggregate": [
        {"col": "ms", "fn": "sum", "as": "slow_ms",
         "where": [{"col": "ms", "op": "gt", "value": 100}]},
        {"col": "ms", "fn": "max", "as": "fast_max",
         "where": [{"not": {"col": "ms", "op": "gt", "value": 100}}]}
    ]}]));
    assert_eq!(col(&out, "slow_ms"), [json!(2100)]);
    assert_eq!(col(&out, "fast_max"), [json!(50)]);
}

#[test]
fn where_requires_as() {
    let err = error(json!([{"group_by": {"by": ["day"], "agg": [
        {"col": "*", "fn": "count", "where": [{"col": "ms", "op": "gt", "value": 1}]}
    ]}}]));
    assert!(err.contains("'as'"), "{}", err);
}

#[test]
fn frequency_refuses_where() {
    let err = error(json!([{"frequency": {"by": ["day"], "agg": [
        {"col": "ms", "fn": "sum", "as": "x", "where": [{"col": "ms", "op": "gt", "value": 1}]}
    ]}}]));
    assert!(err.contains("frequency"), "{}", err);
}

#[test]
fn an_unknown_key_in_agg_is_named() {
    let err = error(json!([{"group_by": {"by": ["day"], "agg": [
        {"col": "*", "fn": "count", "filter": [{"col": "ms", "op": "gt", "value": 1}]}
    ]}}]));
    assert!(err.contains("'filter'"), "{}", err);
    assert!(err.contains("where"), "{}", err);
}

#[test]
fn a_where_on_a_missing_column_is_named() {
    let err = error(json!([{"group_by": {"by": ["day"], "agg": [
        {"col": "*", "fn": "count", "as": "x", "where": [{"col": "nope", "op": "eq", "value": 1}]}
    ]}}]));
    assert!(err.contains("nope"), "{}", err);
}
