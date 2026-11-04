//! Datetime columns that carry a time zone: shown, compared and computed in UTC, and
//! written back with their zone.

use polars::prelude::*;
use serde_json::{json, Value};
use std::path::PathBuf;
use tuitab::mcp::{handle_message, Server};

fn call(server: &mut Server, tool: &str, arguments: Value) -> Value {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                         "params": {"name": tool, "arguments": arguments}});
    handle_message(server, &message.to_string()).unwrap()["result"].clone()
}

fn dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("tz-tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn zone() -> TimeZone {
    unsafe { TimeZone::from_static("Europe/Moscow") }
}

/// `at` in Europe/Moscow: 2026-08-24 08:10 and 09:10 UTC.
fn zoned() -> PathBuf {
    static ONCE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let path = dir().join("zoned.parquet");
        let at = Int64Chunked::new("at".into(), &[1787559000000000i64, 1787562600000000])
            .into_datetime(TimeUnit::Microseconds, Some(zone()))
            .into_series();
        let v = Series::new("v".into(), [1i64, 2]);
        let mut df = DataFrame::new(2, vec![at.into(), v.into()]).unwrap();
        ParquetWriter::new(std::fs::File::create(&path).unwrap())
            .finish(&mut df)
            .unwrap();
        path
    })
    .clone()
}

fn both(arguments: Value) -> Value {
    let streamed = call(&mut Server::new(), "tuitab_query", arguments.clone());
    let loaded = call(
        &mut Server {
            stream_from: Some(u64::MAX),
            ..Server::new()
        },
        "tuitab_query",
        arguments.clone(),
    );
    assert_eq!(streamed, loaded, "paths differ for {}", arguments);
    streamed
}

#[test]
fn a_zoned_column_filters_on_the_moment_shown() {
    let out = both(json!({"source": zoned(), "ops": [
        {"filter": {"col": "at", "op": "eq", "value": "2026-08-24 08:10:00"}}]}));
    assert_eq!(out["structuredContent"]["row_count"], json!(1), "{}", out);
    let out = both(json!({"source": zoned(), "ops": [
        {"filter": {"col": "at", "op": "gt", "value": "2026-08-24 08:30"}}]}));
    assert_eq!(out["structuredContent"]["row_count"], json!(1), "{}", out);
    let out = both(json!({"source": zoned(), "ops": [
        {"compute": {"name": "h", "expr": "hour(at)"}}]}));
    assert_eq!(out["structuredContent"]["rows"][0][2], json!(8), "{}", out);
}

#[test]
fn a_zoned_column_is_written_with_its_zone() {
    for eager in [false, true] {
        for ext in ["parquet", "arrow"] {
            let out = dir().join(format!("zoned-out-{}.{}", eager, ext));
            let _ = std::fs::remove_file(&out);
            let wrote = call(
                &mut Server {
                    stream_from: Some(if eager { u64::MAX } else { 0 }),
                    ..Server::new()
                },
                "tuitab_query",
                json!({"source": zoned(), "ops": [], "output": {"path": out}}),
            );
            assert_eq!(wrote["isError"], json!(false), "{}", wrote);
            let file = std::fs::File::open(&out).unwrap();
            let back = if ext == "parquet" {
                ParquetReader::new(file).finish().unwrap()
            } else {
                IpcReader::new(file).finish().unwrap()
            };
            assert_eq!(
                back.column("at").unwrap().dtype(),
                &DataType::Datetime(TimeUnit::Microseconds, Some(zone())),
                "{} eager={}",
                ext,
                eager
            );
        }
        let out = dir().join(format!("zoned-out-{}.csv", eager));
        let _ = std::fs::remove_file(&out);
        let wrote = call(
            &mut Server {
                stream_from: Some(if eager { u64::MAX } else { 0 }),
                ..Server::new()
            },
            "tuitab_query",
            json!({"source": zoned(), "ops": [], "output": {"path": out}}),
        );
        assert_eq!(wrote["isError"], json!(false), "{}", wrote);
    }
}

#[test]
fn inspect_names_the_zone() {
    let out = call(
        &mut Server::new(),
        "tuitab_inspect",
        json!({"source": zoned()}),
    );
    let note = out["structuredContent"]["columns"][0]["note"]
        .as_str()
        .unwrap_or("");
    assert!(
        note.contains("Europe/Moscow") && note.contains("UTC"),
        "{}",
        out
    );
}

#[test]
fn a_pattern_of_zoned_files_keeps_the_zone() {
    let a = zoned();
    let b = dir().join("zoned-b.parquet");
    std::fs::copy(&a, &b).unwrap();
    let pattern = dir().join("zoned*.parquet").to_string_lossy().into_owned();
    let out = dir().join("zoned-pattern-out.parquet");
    let _ = std::fs::remove_file(&out);
    let wrote = call(
        &mut Server {
            stream_from: Some(u64::MAX),
            ..Server::new()
        },
        "tuitab_query",
        json!({"source": pattern, "ops": [], "output": {"path": out}}),
    );
    assert_eq!(wrote["isError"], json!(false), "{}", wrote);
    let back = ParquetReader::new(std::fs::File::open(&out).unwrap())
        .finish()
        .unwrap();
    assert_eq!(
        back.column("at").unwrap().dtype(),
        &DataType::Datetime(TimeUnit::Microseconds, Some(zone()))
    );
}
