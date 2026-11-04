//! Tokens that mean "no value" in a CSV, and a source whose keys are checked.

use serde_json::{json, Value};
use std::path::PathBuf;
use tuitab::mcp::{handle_message, Server};

fn csv(name: &str, text: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("null-values-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

fn call(name: &str, arguments: Value) -> Value {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                         "params": {"name": name, "arguments": arguments}});
    let response = handle_message(&mut Server::new(), &message.to_string()).unwrap();
    response["result"].clone()
}

fn ok(name: &str, arguments: Value) -> Value {
    let result = call(name, arguments);
    assert_eq!(result["isError"], json!(false), "{}", result);
    result["structuredContent"].clone()
}

fn types(inspect: &Value) -> Vec<(String, String)> {
    inspect["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["name"].as_str().unwrap().to_string(),
                c["type"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

/// A Grafana export writes `undefined` for a missing point; one of those used to turn
/// the whole column into text, so no aggregate would touch it.
#[test]
fn undefined_does_not_turn_a_numeric_column_into_text() {
    let path = csv("grafana.csv", "PK,IN\n1.5,2\nundefined,3\n4,undefined\n");
    let out = ok("tuitab_inspect", json!({"source": path}));
    assert_eq!(
        types(&out),
        [
            ("PK".into(), "float".into()),
            ("IN".into(), "integer".into())
        ]
    );
    assert_eq!(out["sample_rows"][1][0], Value::Null);
}

#[test]
fn builtin_null_tokens_leave_a_text_column_alone() {
    let path = csv("names.csv", "name,n\nNone,1\nAnn,null\nN/A,3\n");
    let out = ok("tuitab_inspect", json!({"source": path}));
    assert_eq!(
        types(&out),
        [
            ("name".into(), "string".into()),
            ("n".into(), "integer".into())
        ]
    );
    assert_eq!(out["sample_rows"][0][0], json!("None"));
    assert_eq!(out["sample_rows"][2][0], json!("N/A"));
}

/// `NA` is Namibia's country code, so it is not a built-in token.
#[test]
fn na_is_not_a_builtin_token() {
    let path = csv("countries.csv", "code,v\nNA,1\n2,2\n");
    let out = ok("tuitab_inspect", json!({"source": path}));
    assert_eq!(types(&out)[0].1, "string");
}

#[test]
fn null_values_option_applies_to_every_column() {
    let path = csv("dashes.csv", "label,v\n-,1\nb,-\n");
    let out = ok(
        "tuitab_inspect",
        json!({"source": {"path": path, "null_values": ["-"]}}),
    );
    assert_eq!(
        types(&out),
        [
            ("label".into(), "string".into()),
            ("v".into(), "integer".into())
        ]
    );
    assert_eq!(out["sample_rows"][0][0], Value::Null);
    assert_eq!(out["sample_rows"][1][1], Value::Null);
}

#[test]
fn null_values_must_be_a_list_of_text() {
    let path = csv("bad.csv", "a\n1\n");
    let result = call(
        "tuitab_inspect",
        json!({"source": {"path": path, "null_values": "undefined"}}),
    );
    assert_eq!(result["isError"], json!(true));
    assert!(result["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("null_values"));
}

#[test]
fn a_misspelt_source_key_is_named() {
    let path = csv("typo.csv", "a\n1\n");
    let result = call(
        "tuitab_inspect",
        json!({"source": {"path": path, "null_value": ["x"]}}),
    );
    assert_eq!(result["isError"], json!(true), "{}", result);
    let text = result["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("'null_value'"), "{}", text);
    assert!(text.contains("did you mean 'null_values'"), "{}", text);
}

#[test]
fn a_count_over_a_column_with_tokens_counts_the_values() {
    let path = csv("count.csv", "BR\n1\nundefined\n3\n");
    let out = ok(
        "tuitab_query",
        json!({"source": path, "ops": [{"aggregate": [{"col": "BR", "fn": "count"}]}]}),
    );
    assert_eq!(out["rows"][0][0], json!(2));
}

/// CSV from a pipe follows the rules a file does: a null token in a column of numbers
/// is a missing number.
#[test]
fn csv_from_a_pipe_reads_null_tokens_as_a_file_does() {
    let (df, _) =
        tuitab::data::io::load_from_bytes(b"v\n1\nundefined\n3\n".to_vec(), "csv", None).unwrap();
    assert_eq!(
        df.df.column("v").unwrap().dtype(),
        &polars::prelude::DataType::Int64
    );
    assert_eq!(df.df.column("v").unwrap().null_count(), 1);
}
