//! Integer epoch columns read as datetimes, and written back as the integers they were.

use serde_json::{json, Value};
use std::path::PathBuf;
use tuitab::data::io::{load_file, save_file_as};
use tuitab::mcp::{handle_message, Server};
use tuitab::types::ColumnType;

/// Inside the project, never the system temp dir.
fn tmp(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("epoch-tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn csv(name: &str, text: &str) -> PathBuf {
    let path = tmp(name);
    std::fs::write(&path, text).unwrap();
    path
}

fn call(server: &mut Server, name: &str, arguments: Value) -> Value {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                         "params": {"name": name, "arguments": arguments}});
    let response = handle_message(server, &message.to_string()).unwrap();
    let result = &response["result"];
    assert_eq!(
        result["isError"],
        json!(false),
        "{} failed: {}",
        name,
        result
    );
    result["structuredContent"].clone()
}

fn column<'a>(inspect: &'a Value, name: &str) -> &'a Value {
    inspect["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("no column {} in {}", name, inspect))
}

#[test]
fn epoch_ms_column_named_time_becomes_datetime() {
    let path = csv("grafana.csv", "Time,v\n1787569800000,1\n1787570400000,2\n");
    let out = call(
        &mut Server::new(),
        "tuitab_inspect",
        json!({"source": path}),
    );
    let time = column(&out, "Time");
    assert_eq!(time["type"], "datetime", "{}", out);
    assert!(
        time["note"].as_str().unwrap_or("").contains("epoch ms"),
        "{}",
        out
    );
    assert_eq!(
        out["sample_rows"][0][0],
        json!("2026-08-24 11:10:00"),
        "{}",
        out
    );
    assert_eq!(column(&out, "v")["type"], "integer");
}

#[test]
fn epoch_detection_leaves_other_integers_alone() {
    // The name does not say time.
    let ids = csv("ids.csv", "id\n1787569800000\n1787570400000\n");
    // The name says time, the values are not epochs.
    let small = csv("small_ts.csv", "ts\n1\n2\n3\n");
    // One value outside the range, so not every value is an epoch in the same unit.
    let mixed = csv("mixed_ts.csv", "ts\n1787569800000\n1787569800\n");
    for path in [ids, small, mixed] {
        let df = load_file(&path, None).unwrap();
        assert_eq!(
            df.columns[0].col_type,
            ColumnType::Integer,
            "{}",
            path.display()
        );
        assert!(df.columns[0].epoch_unit.is_none());
    }
}

#[test]
fn seconds_and_microseconds_are_told_apart() {
    let s = csv("s.csv", "created_at\n1787569800\n");
    let us = csv("us.csv", "event_ts\n1787569800000000\n");
    let out = call(&mut Server::new(), "tuitab_inspect", json!({"source": s}));
    assert!(column(&out, "created_at")["note"]
        .as_str()
        .unwrap()
        .contains("epoch s"));
    assert_eq!(out["sample_rows"][0][0], json!("2026-08-24 11:10:00"));
    let out = call(&mut Server::new(), "tuitab_inspect", json!({"source": us}));
    assert!(column(&out, "event_ts")["note"]
        .as_str()
        .unwrap()
        .contains("epoch us"));
}

#[test]
fn detect_epoch_false_keeps_the_integers() {
    let path = csv("off.csv", "Time\n1787569800000\n");
    let out = call(
        &mut Server::new(),
        "tuitab_inspect",
        json!({"source": {"path": path, "detect_epoch": false}}),
    );
    assert_eq!(column(&out, "Time")["type"], "integer", "{}", out);
    assert_eq!(out["sample_rows"][0][0], json!(1787569800000i64));
}

/// The file on disk holds integers; reading them as datetimes must not turn a save
/// into a rewrite of every value.
#[test]
fn saving_a_detected_epoch_column_writes_the_original_integers() {
    let path = csv("roundtrip.csv", "Time,v\n1787569800000,1\n");
    let df = load_file(&path, None).unwrap();
    assert_eq!(df.columns[0].col_type, ColumnType::Datetime);
    let out = tmp("roundtrip-out.csv");
    save_file_as(
        &df,
        None,
        &out,
        tuitab::data::io::doc_io::Shape::Records,
        "t",
    )
    .unwrap();
    let text = std::fs::read_to_string(&out).unwrap();
    assert!(text.contains("1787569800000"), "{}", text);
}

#[test]
fn retyping_a_detected_epoch_column_to_integer_restores_it() {
    let path = csv("retype.csv", "ts\n1787569800\n");
    let mut df = load_file(&path, None).unwrap();
    assert_eq!(df.columns[0].col_type, ColumnType::Datetime);
    df.set_column_type(0, ColumnType::Integer).unwrap();
    assert_eq!(df.columns[0].col_type, ColumnType::Integer);
    assert!(df.columns[0].epoch_unit.is_none());
    assert_eq!(df.get_physical(0, 0), "1787569800");
}

#[test]
fn a_glob_of_epoch_files_keeps_the_detection() {
    csv("glob-a.csv", "Time,v\n1787569800000,1\n");
    csv("glob-b.csv", "Time,v\n1787570400000,2\n");
    let pattern = tmp("glob-*.csv").to_string_lossy().into_owned();
    let out = call(
        &mut Server::new(),
        "tuitab_inspect",
        json!({"source": pattern}),
    );
    assert_eq!(out["row_count"], 2, "{}", out);
    assert!(column(&out, "Time")["note"]
        .as_str()
        .unwrap()
        .contains("epoch ms"));
}

#[test]
fn hour_of_a_detected_column_groups_into_integers() {
    let path = csv(
        "hours.csv",
        "Time\n1787569800000\n1787571600000\n1787573400000\n",
    );
    let out = call(
        &mut Server::new(),
        "tuitab_query",
        json!({"source": path, "ops": [
            {"compute": {"name": "h", "expr": "hour(Time)"}},
            {"group_by": {"by": ["h"], "agg": [{"col": "*", "fn": "count"}]}}
        ]}),
    );
    let h = out["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "h")
        .unwrap();
    assert_eq!(h["type"], "integer", "{}", out);
    assert_eq!(
        out["row_count"], 2,
        "11:10, 11:40 and 12:10 → hours 11 and 12"
    );
}

/// One table read from CSV and from Parquet comes back with the same column types —
/// the CSV loader used to keep a dtype mapping of its own.
#[test]
fn parquet_load_and_csv_load_report_the_same_types() {
    let path = csv(
        "types.csv",
        "n,x,s,flag,created_at\n1,1.5,a,true,1787569800\n2,2.5,b,false,1787570400\n",
    );
    let from_csv = load_file(&path, None).unwrap();
    let pq = tmp("types.parquet");
    save_file_as(
        &from_csv,
        None,
        &pq,
        tuitab::data::io::doc_io::Shape::Records,
        "t",
    )
    .unwrap();
    let from_parquet = load_file(&pq, None).unwrap();
    let types = |df: &tuitab::data::dataframe::DataFrame| -> Vec<ColumnType> {
        df.columns.iter().map(|c| c.col_type).collect()
    };
    assert_eq!(types(&from_csv), types(&from_parquet));
    assert_eq!(from_parquet.columns[4].col_type, ColumnType::Datetime);
}

/// Saving wrote numbers the way the screen shows them — two decimals by default — so
/// 1.23456 went to disk as 1.23, and Parquet got the text "1.23".
#[test]
fn saving_keeps_every_digit_and_the_number_type() {
    let path = csv("digits.csv", "x\n1.23456\n2.5\n");
    let df = load_file(&path, None).unwrap();
    let out = tmp("digits-out.csv");
    save_file_as(
        &df,
        None,
        &out,
        tuitab::data::io::doc_io::Shape::Records,
        "t",
    )
    .unwrap();
    let text = std::fs::read_to_string(&out).unwrap();
    assert!(text.contains("1.23456"), "{}", text);
    let pq = tmp("digits.parquet");
    save_file_as(
        &df,
        None,
        &pq,
        tuitab::data::io::doc_io::Shape::Records,
        "t",
    )
    .unwrap();
    let back = load_file(&pq, None).unwrap();
    assert_eq!(back.columns[0].col_type, ColumnType::Float);
    assert_eq!(back.get_physical(0, 0), "1.23456");
}
