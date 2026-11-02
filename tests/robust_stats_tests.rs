//! The median absolute deviation, and median and MAD as windows: the pieces of a
//! robust z-score per group.

use serde_json::{json, Value};
use std::path::PathBuf;
use tuitab::data::aggregator::AggregatorKind;
use tuitab::mcp::{handle_message, Server};
use tuitab::types::ColumnType;

fn csv(name: &str, text: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("robust-stats-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

fn query(path: &std::path::Path, ops: Value) -> Value {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "tuitab_query",
                   "arguments": {"source": path.to_string_lossy(), "ops": ops}}});
    let response = handle_message(&mut Server::new(), &message.to_string()).unwrap();
    let result = &response["result"];
    assert_eq!(result["isError"], json!(false), "{}", result);
    result["structuredContent"].clone()
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

/// `[1, 2, 3, 4, 100]`: median 3, deviations `[2, 1, 0, 1, 97]`, their median 1 —
/// the outlier moves neither.
#[test]
fn mad_of_a_group() {
    let path = csv("groups.csv", "g,v\na,1\na,2\na,3\na,4\na,100\nb,10\nb,20\n");
    let out = query(
        &path,
        json!([{"group_by": {"by": ["g"], "agg": [
            {"col": "v", "fn": "median"}, {"col": "v", "fn": "mad"}
        ]}}]),
    );
    assert_eq!(col(&out, "v:mad"), [json!(1.0), json!(5.0)]);
    assert_eq!(col(&out, "v:median"), [json!(3.0), json!(15.0)]);
}

#[test]
fn mad_as_a_grand_total() {
    let path = csv("total.csv", "v\n1\n2\n3\n4\n100\n");
    let out = query(&path, json!([{"aggregate": [{"col": "v", "fn": "mad"}]}]));
    assert_eq!(out["rows"][0][0], json!(1.0));
}

#[test]
fn mad_refuses_text() {
    let path = csv("text.csv", "s\na\nb\n");
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "tuitab_query", "arguments": {"source": path.to_string_lossy(),
            "ops": [{"aggregate": [{"col": "s", "fn": "mad"}]}]}}});
    let response = handle_message(&mut Server::new(), &message.to_string()).unwrap();
    assert_eq!(response["result"]["isError"], json!(true));
}

#[test]
fn the_footer_computes_mad() {
    let values: Vec<String> = ["1", "2", "3", "4", "100"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        AggregatorKind::Mad.compute(&values, ColumnType::Integer, 0, None),
        "1"
    );
    assert!(AggregatorKind::all().contains(&AggregatorKind::Mad));
}

/// The robust z-score of the request: per-hour median and MAD as windows, then one
/// `compute`.
#[test]
fn window_median_and_mad_over_a_partition() {
    let path = csv(
        "hours.csv",
        "hour,v\n1,10\n1,12\n1,11\n1,50\n2,100\n2,104\n",
    );
    let out = query(
        &path,
        json!([
            {"window": {"fn": "median", "col": "v", "over": ["hour"], "as": "med"}},
            {"window": {"fn": "mad", "col": "v", "over": ["hour"], "as": "mad"}},
            {"compute": {"name": "z", "expr": "(v - med) / (1.4826 * mad)"}}
        ]),
    );
    assert_eq!(
        col(&out, "med"),
        [
            json!(11.5),
            json!(11.5),
            json!(11.5),
            json!(11.5),
            json!(102.0),
            json!(102.0)
        ]
    );
    // hour 1: |v - 11.5| = 1.5, 0.5, 0.5, 38.5 → MAD 1; hour 2: 2, 2 → MAD 2.
    assert_eq!(
        col(&out, "mad"),
        [
            json!(1.0),
            json!(1.0),
            json!(1.0),
            json!(1.0),
            json!(2.0),
            json!(2.0)
        ]
    );
    let z = col(&out, "z")[3].as_f64().unwrap();
    assert!(z > 4.0, "the outlier is far out: {}", z);
}

#[test]
fn median_and_mad_windows_refuse_an_order() {
    let path = csv("order.csv", "v\n1\n2\n");
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "tuitab_query", "arguments": {"source": path.to_string_lossy(),
            "ops": [{"window": {"fn": "mad", "col": "v", "order_by": ["v"]}}]}}});
    let response = handle_message(&mut Server::new(), &message.to_string()).unwrap();
    assert_eq!(response["result"]["isError"], json!(true));
}
