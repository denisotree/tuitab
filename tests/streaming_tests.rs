//! Pipelines over files larger than memory: scanned and streamed rather than loaded.
//! Every answer must be the one the in-memory path gives.

use serde_json::{json, Value};
use std::path::PathBuf;
use tuitab::mcp::{handle_message, Server};

fn dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("streaming-tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write(name: &str, text: &str) -> PathBuf {
    let path = dir().join(name);
    std::fs::write(&path, text).unwrap();
    path
}

const CSV: &str = "ts,class,code,ms,Time,flag\n\
    2026-08-24 11:10:00,app,200,10,1787569800000,true\n\
    2026-08-24 11:20:00,backend,503,900,1787570400000,false\n\
    2026-08-24 11:30:00,app,503 slow,undefined,1787571000000,true\n\
    2026-08-24 12:10:00,php,500,50,1787573400000,\n\
    2026-08-24 12:40:00,app,,12,1787575200000,false\n";

const JSONL: &str = "{\"class\":\"app\",\"ms\":10,\"geo\":{\"lat\":1.5,\"city\":\"X\"},\"tags\":[\"a\",\"b\"],\"msg\":\"see http://geo.example/x\"}\n\
    {\"class\":\"backend\",\"ms\":900,\"geo\":{\"lat\":2.5},\"tags\":[],\"msg\":\"plain\"}\n\
    {\"class\":\"app\",\"ms\":12.5,\"geo\":null,\"tags\":[\"c\"],\"msg\":\"https://hooks.slack.com/a\"}\n";

fn server(eager_only: bool) -> Server {
    Server {
        eager_only,
        ..Server::new()
    }
}

fn call(server: &mut Server, tool: &str, arguments: Value) -> Value {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                         "params": {"name": tool, "arguments": arguments}});
    handle_message(server, &message.to_string()).unwrap()["result"].clone()
}

/// The two paths, side by side: their answers, success or error, must be equal.
fn agree(tool: &str, arguments: Value) -> Value {
    let streamed = call(&mut server(false), tool, arguments.clone());
    let loaded = call(&mut server(true), tool, arguments.clone());
    assert_eq!(
        streamed, loaded,
        "\nstreamed and loaded answers differ for {}",
        arguments
    );
    streamed
}

fn pipelines() -> Vec<Value> {
    vec![
        json!([]),
        json!([{"filter": {"col": "class", "op": "eq", "value": "app"}}]),
        json!([{"filter": {"not": {"col": "code", "op": "contains", "value": "^5"}}}]),
        json!([{"select": ["class", "ms"]}, {"limit": 2}]),
        json!([{"compute": {"name": "slow", "expr": "ms > 100"}},
               {"compute": {"name": "r", "expr": "round(ms / 3, 1)"}}]),
        json!([{"compute": {"name": "tmpl", "expr": "regexp_replace(code, \"[0-9]+\", \"N\")"}},
               {"frequency": {"by": ["tmpl"]}}]),
        json!([{"sort": {"by": [{"col": "class"}, {"col": "ms", "desc": true}]}}]),
        json!([{"group_by": {"by": ["class"], "agg": [
                {"col": "*", "fn": "count"}, {"col": "ms", "fn": "median"},
                {"col": "*", "fn": "count", "as": "n5",
                 "where": [{"col": "code", "op": "contains", "value": "^5"}]}]}}]),
        json!([{"aggregate": [{"col": "ms", "fn": "sum"}, {"col": "ms", "fn": "mad"}]}]),
        json!([{"frequency": {"by": ["class"]}}]),
        json!([{"frequency": {"by": ["class", "flag"]}}]),
        json!([{"compute": {"name": "h", "expr": "hour(Time)"}},
               {"group_by": {"by": ["h"], "agg": [{"col": "*", "fn": "count"}]}}]),
        json!([{"window": {"fn": "rank", "col": "ms", "over": ["class"]}}]),
        json!([{"unpivot": {"index": ["ts"], "columns": ["ms"], "drop_nulls": true}}]),
        json!([{"filter": {"col": "class", "op": "eq", "value": "app"}},
               {"dedup": {"by": ["class"], "keep": "first"}}]),
        json!([{"filter": {"col": "nope", "op": "eq", "value": 1}}]),
    ]
}

#[test]
fn lazy_and_eager_paths_agree_on_a_csv() {
    let path = write("events.csv", CSV);
    for ops in pipelines() {
        agree("tuitab_query", json!({"source": path, "ops": ops}));
    }
    agree("tuitab_inspect", json!({"source": path}));
    agree(
        "tuitab_describe",
        json!({"source": path, "columns": ["ms"]}),
    );
}

#[test]
fn lazy_and_eager_paths_agree_on_parquet() {
    let csv = write("events-for-parquet.csv", CSV);
    let parquet = dir().join("events.parquet");
    let _ = std::fs::remove_file(&parquet);
    let wrote = call(
        &mut server(true),
        "tuitab_query",
        json!({"source": csv, "ops": [], "output": {"path": parquet}}),
    );
    assert_eq!(wrote["isError"], json!(false), "{}", wrote);
    for ops in pipelines() {
        agree("tuitab_query", json!({"source": parquet, "ops": ops}));
    }
    agree("tuitab_inspect", json!({"source": parquet}));
}

#[test]
fn lazy_and_eager_paths_agree_on_jsonl() {
    let path = write("events.jsonl", JSONL);
    for ops in [
        json!([]),
        json!([{"frequency": {"by": ["class"]}}]),
        json!([{"filter": {"col": "msg", "op": "contains", "value": "https?://"}},
               {"compute": {"name": "host",
                            "expr": "regexp_extract(msg, \"(?i)https?://([A-Za-z0-9.-]+)\", 1)"}},
               {"frequency": {"by": ["host"]}}]),
        json!([{"group_by": {"by": ["class"], "agg": [{"col": "ms", "fn": "sum"}]}}]),
        json!([{"select": ["geo", "tags"]}]),
    ] {
        agree("tuitab_query", json!({"source": path, "ops": ops}));
    }
    agree("tuitab_inspect", json!({"source": path}));
}

#[test]
fn a_glob_of_parquet_is_scanned() {
    let a = write("glob-a.csv", "k,v\nx,1\ny,2\n");
    let b = write("glob-b.csv", "k,v\nx,3\n");
    for (src, name) in [(a, "glob-p-a.parquet"), (b, "glob-p-b.parquet")] {
        let out = dir().join(name);
        let _ = std::fs::remove_file(&out);
        call(
            &mut server(true),
            "tuitab_query",
            json!({"source": src, "ops": [], "output": {"path": out}}),
        );
    }
    let pattern = dir()
        .join("glob-p-*.parquet")
        .to_string_lossy()
        .into_owned();
    let out = agree(
        "tuitab_query",
        json!({"source": pattern, "ops": [
            {"group_by": {"by": ["k"], "agg": [{"col": "v", "fn": "sum"}]}}
        ]}),
    );
    assert_eq!(out["isError"], json!(false), "{}", out);
    assert_eq!(out["structuredContent"]["row_count"], json!(2));
}

/// More rows than memory would hold: refused with what it would take and what does
/// stream, rather than left to run out of memory.
#[test]
fn the_guard_refuses_a_result_too_big_to_collect() {
    let path = write("guard.csv", CSV);
    let mut tight = Server {
        memory_budget: Some(64),
        ..Server::new()
    };
    let out = call(
        &mut tight,
        "tuitab_query",
        json!({"source": path, "ops": [{"dedup": {"by": ["class"], "keep": "first"}}]}),
    );
    assert_eq!(out["isError"], json!(true), "{}", out);
    let text = out["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("dedup") && text.contains("rows"), "{}", text);

    // What streams to a small answer is not held to the budget.
    let out = call(
        &mut tight,
        "tuitab_query",
        json!({"source": path, "ops": [{"frequency": {"by": ["class"]}}]}),
    );
    assert_eq!(out["isError"], json!(false), "{}", out);
}
