//! JSON held in a text column: `json_get` in expressions, `expand_json` as an operation
//! and on `zJ`.

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tuitab::mcp::{handle_message, Server};

fn tmp(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("json-column-tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

/// A CSV whose `client` column holds a JSON object per row — written once, since the
/// tests read it in parallel and a rewrite under a reader is an empty file.
fn clients() -> PathBuf {
    static WRITTEN: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    WRITTEN.get_or_init(write_clients).clone()
}

fn write_clients() -> PathBuf {
    let path = tmp("clients.csv");
    std::fs::write(
        &path,
        "id,client\n\
         1,\"{\"\"country_code\"\": \"\"US\"\", \"\"isp_name\"\": \"\"Comcast\"\", \"\"asn\"\": 7922, \"\"geo\"\": {\"\"lat\"\": 1.5}, \"\"zip\"\": \"\"00123\"\", \"\"mobile\"\": false}\"\n\
         2,\"{\"\"country_code\"\": \"\"IN\"\", \"\"isp_name\"\": \"\"Jio\"\", \"\"asn\"\": 55836, \"\"geo\"\": {\"\"lat\"\": 2.5}, \"\"zip\"\": \"\"00456\"\", \"\"mobile\"\": true}\"\n\
         3,not json\n\
         4,\n",
    )
    .unwrap();
    path
}

fn query(ops: Value) -> Value {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "tuitab_query",
                   "arguments": {"source": clients().to_string_lossy(), "ops": ops}}});
    let response = handle_message(&mut Server::new(), &message.to_string()).unwrap();
    let result = &response["result"];
    assert_eq!(result["isError"], json!(false), "{}", result);
    result["structuredContent"].clone()
}

fn query_error(ops: Value) -> String {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "tuitab_query",
                   "arguments": {"source": clients().to_string_lossy(), "ops": ops}}});
    let response = handle_message(&mut Server::new(), &message.to_string()).unwrap();
    let result = &response["result"];
    assert_eq!(result["isError"], json!(true), "{}", result);
    result["content"][0]["text"].as_str().unwrap().to_string()
}

fn column(out: &Value, name: &str) -> (String, Vec<Value>) {
    let cols = out["columns"].as_array().unwrap();
    let i = cols
        .iter()
        .position(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("no column {} in {}", name, out));
    let values = out["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r[i].clone())
        .collect();
    (cols[i]["type"].as_str().unwrap().to_string(), values)
}

#[test]
fn json_get_reads_a_path_and_types_the_column() {
    let out = query(json!([
        {"compute": {"name": "cc", "expr": "json_get(client, \"$.country_code\")"}},
        {"compute": {"name": "asn", "expr": "json_get(client, \"$.asn\")"}},
        {"compute": {"name": "lat", "expr": "json_get(client, \"$.geo.lat\")"}},
        {"compute": {"name": "mobile", "expr": "json_get(client, \"$.mobile\")"}},
        {"compute": {"name": "geo", "expr": "json_get(client, \"$.geo\")"}}
    ]));
    assert_eq!(
        column(&out, "cc"),
        (
            "string".into(),
            vec![json!("US"), json!("IN"), Value::Null, Value::Null]
        )
    );
    assert_eq!(column(&out, "asn").0, "integer");
    assert_eq!(column(&out, "asn").1[1], json!(55836));
    assert_eq!(column(&out, "lat").0, "float");
    assert_eq!(column(&out, "mobile").1[1], json!(true));
    let (_, geo) = column(&out, "geo");
    let parsed: Value = serde_json::from_str(geo[0].as_str().unwrap()).unwrap();
    assert_eq!(parsed, json!({"lat": 1.5}));
}

#[test]
fn json_get_keeps_zero_padded_ids_as_text() {
    let out = query(json!([
        {"compute": {"name": "zip", "expr": "json_get(client, \"$.zip\")"}}
    ]));
    assert_eq!(
        column(&out, "zip"),
        (
            "string".into(),
            vec![json!("00123"), json!("00456"), Value::Null, Value::Null]
        )
    );
}

#[test]
fn json_get_path_syntax_errors_are_errors() {
    for path in [
        "country_code",
        "$..country_code",
        "$.*",
        "$[?(@.a)]",
        "$.a[",
    ] {
        let err = query_error(json!([
            {"compute": {"name": "x", "expr": format!("json_get(client, \"{}\")", path)}}
        ]));
        assert!(err.contains("json_get"), "{}: {}", path, err);
    }
    let err = query_error(json!([
        {"compute": {"name": "x", "expr": "json_get(client, client)"}}
    ]));
    assert!(err.contains("json_get"), "{}", err);
}

#[test]
fn json_get_in_a_filter() {
    let out = query(json!([
        {"filter": {"col": "client", "op": "contains", "value": "country_code"}},
        {"compute": {"name": "cc", "expr": "json_get(client, \"$.country_code\")"}},
        {"filter": {"col": "cc", "op": "eq", "value": "IN"}}
    ]));
    assert_eq!(out["row_count"], 1);
}

#[test]
fn expand_json_adds_one_column_per_top_level_key() {
    let out = query(json!([
        {"expand_json": {"col": "client", "prefix": "client_"}}
    ]));
    let names: Vec<&str> = out["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "id",
            "client",
            "client_country_code",
            "client_isp_name",
            "client_asn",
            "client_geo",
            "client_zip",
            "client_mobile"
        ]
    );
    assert_eq!(column(&out, "client_asn").0, "integer");
    assert_eq!(column(&out, "client_zip").1[0], json!("00123"));
    assert_eq!(column(&out, "client_country_code").1[2], Value::Null);
    let geo: Value =
        serde_json::from_str(column(&out, "client_geo").1[1].as_str().unwrap()).unwrap();
    assert_eq!(geo, json!({"lat": 2.5}));
}

#[test]
fn expand_json_refuses_a_name_that_is_taken() {
    // The second expansion would make the columns the first one made.
    let err = query_error(json!([
        {"expand_json": {"col": "client"}},
        {"expand_json": {"col": "client"}}
    ]));
    assert!(err.contains("'client_country_code'"), "{}", err);
}

#[test]
fn zj_expands_the_current_column() {
    use tuitab::types::Action;
    let mut app = tuitab::app::App::new_as(Path::new(&clients()), None, None).unwrap();
    let col = app.stack.active().dataframe.column_index("client").unwrap();
    app.stack.active_mut().cursor_col = col;
    app.handle_action(Action::ExpandJsonColumn);
    let df = &app.stack.active().dataframe;
    assert!(
        df.column_index("client_country_code").is_ok(),
        "{}",
        app.status_message
    );
    assert_eq!(
        df.get_physical(1, df.column_index("client_isp_name").unwrap()),
        "Jio"
    );
}

/// `expand_json` types a key's values by the rule `json_get` uses — `"42"` and `42`
/// both read as 42 — and keeps a nested value as JSON that saves back as itself.
#[test]
fn expand_json_types_its_values_as_json_get_does() {
    let df = tuitab::data::io::jsonl_table(
        "{\"c\":\"{\\\"code\\\":\\\"42\\\",\\\"x\\\":null,\\\"o\\\":{\\\"a\\\":1}}\"}\n",
    )
    .unwrap();
    let out = tuitab::data::json_cols::expand_json(&df, "c", "c_").unwrap();
    let dtype = |n: &str| out.df.column(n).unwrap().dtype().clone();
    assert_eq!(dtype("c_code"), polars::prelude::DataType::Int64);
    assert_eq!(dtype("c_x"), polars::prelude::DataType::String);
    let o = out.column_index("c_o").unwrap();
    assert!(out.columns[o].json);
    assert_eq!(out.get_physical(0, o), "{\"a\":1}");
}

/// A path that matches nothing is no evidence of numbers.
#[test]
fn json_get_of_a_path_that_matches_nothing_is_text() {
    let out = query(json!([
        {"compute": {"name": "m", "expr": "json_get(client, \"$.missing\")"}}
    ]));
    assert_eq!(column(&out, "m").0, column(&out, "client").0);
}
