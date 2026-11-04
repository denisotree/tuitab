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
        stream_from: Some(if eager_only { u64::MAX } else { 0 }),
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

/// json_get types its column from every value; streamed, it profiles the same rows
/// first and gives the same column.
#[test]
fn json_get_streams_with_the_type_the_loaded_path_gives() {
    let path = write(
        "clients.csv",
        "id,client\n\
         1,\"{\"\"cc\"\": \"\"US\"\", \"\"asn\"\": 7922, \"\"lat\"\": 1.5, \"\"m\"\": true, \"\"zip\"\": \"\"00123\"\"}\"\n\
         2,\"{\"\"cc\"\": \"\"IN\"\", \"\"asn\"\": 55836, \"\"lat\"\": 2, \"\"m\"\": false, \"\"zip\"\": \"\"00456\"\"}\"\n\
         3,not json\n",
    );
    for field in ["cc", "asn", "lat", "m", "zip", "missing"] {
        let ops = json!([
            {"compute": {"name": "x", "expr": format!("json_get(client, \"$.{}\")", field)}},
            {"frequency": {"by": ["x"]}}
        ]);
        agree("tuitab_query", json!({"source": path, "ops": ops}));
        let ops = json!([
            {"compute": {"name": "x", "expr": format!("json_get(client, \"$.{}\")", field)}}
        ]);
        agree("tuitab_query", json!({"source": path, "ops": ops}));
    }
}

/// A frequency over a value read out of JSON streams: it is not held to collecting
/// the table.
#[test]
fn json_get_then_frequency_is_not_held_to_the_budget() {
    let path = write(
        "clients-budget.csv",
        "id,client\n1,\"{\"\"cc\"\": \"\"US\"\"}\"\n2,\"{\"\"cc\"\": \"\"IN\"\"}\"\n3,\"{\"\"cc\"\": \"\"IN\"\"}\"\n",
    );
    let mut tight = Server {
        memory_budget: Some(16),
        ..Server::new()
    };
    let out = call(
        &mut tight,
        "tuitab_query",
        json!({"source": path, "ops": [
            {"compute": {"name": "cc", "expr": "json_get(client, \"$.cc\")"}},
            {"frequency": {"by": ["cc"]}}
        ]}),
    );
    assert_eq!(out["isError"], json!(false), "{}", out);
    assert_eq!(
        out["structuredContent"]["rows"][0],
        json!(["IN", 2, 2.0 / 3.0])
    );
}

/// Nanosecond and microsecond epochs keep every digit when streamed, as loaded, and
/// a file written from the stream holds the integers it was read from.
#[test]
fn fine_epochs_keep_every_digit_streamed() {
    let path = write(
        "fine-epochs.csv",
        "sent_at,recv_at,v\n1787569800123456789,1787569800123457,1\n1787569801987654321,1787569801987655,2\n",
    );
    agree("tuitab_query", json!({"source": path, "ops": []}));
    agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"compute": {"name": "x", "expr": "from_epoch(v * 1000000000 + 1787569800000000000, \"ns\")"}}]}),
    );
    let out = dir().join("fine-epochs-out.csv");
    let _ = std::fs::remove_file(&out);
    let wrote = call(
        &mut server(false),
        "tuitab_query",
        json!({"source": path, "ops": [], "output": {"path": out}}),
    );
    assert_eq!(wrote["isError"], json!(false), "{}", wrote);
    let text = std::fs::read_to_string(&out).unwrap();
    assert!(
        text.contains("1787569800123456789,1787569800123457")
            && text.contains("1787569801987654321,1787569801987655"),
        "{}",
        text
    );
}

#[test]
fn from_epoch_over_an_integer_column_keeps_nanoseconds() {
    let path = write("raw-ns.csv", "raw\n1787569800123456789\n");
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"compute": {"name": "x", "expr": "from_epoch(raw, \"ns\")"}}]}),
    );
    assert_eq!(
        out["structuredContent"]["rows"][0][1],
        json!("2026-08-24 11:10:00.123456789"),
        "{}",
        out
    );
}

/// JSONL that the in-memory reader takes but a scan cannot — a field that is an
/// object in one line and a list or a number in another, an array per line, a number
/// past 64 bits — is read the in-memory way rather than refused.
#[test]
fn jsonl_a_scan_cannot_take_is_read_in_memory() {
    for (name, text) in [
        (
            "mixed-obj-list.jsonl",
            "{\"a\":1,\"g\":{\"x\":1}}\n{\"a\":2,\"g\":[1,2]}\n",
        ),
        (
            "mixed-scalar-obj.jsonl",
            "{\"a\":1,\"g\":5}\n{\"a\":2,\"g\":{\"x\":1}}\n",
        ),
        ("arrays.jsonl", "[1,2]\n[3,4]\n"),
        (
            "huge.jsonl",
            "{\"a\":1,\"n\":123456789012345678901234567890}\n{\"a\":2,\"n\":1}\n",
        ),
    ] {
        let path = write(name, text);
        let out = agree("tuitab_query", json!({"source": path, "ops": []}));
        let loaded = call(
            &mut server(true),
            "tuitab_query",
            json!({"source": path, "ops": []}),
        );
        assert_eq!(out, loaded, "{}", name);
        agree(
            "tuitab_query",
            json!({"source": path, "ops": [{"aggregate": [{"col": "*", "fn": "count"}]}]}),
        );
    }
}

/// A JSONL file the scan takes stays on the scan: held to the budget, not loaded.
#[test]
fn a_jsonl_the_scan_takes_is_still_streamed() {
    let path = write("sentinel.jsonl", JSONL);
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
    assert!(
        out["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("dedup"),
        "{}",
        out
    );
}

/// A `.json` file holding JSON Lines is scanned as the lines it holds, and jq reads it.
#[test]
fn a_json_file_holding_json_lines_is_streamed() {
    let path = write("sentinel-lines.json", JSONL);
    let pattern = dir().join("sentinel-lines*.json");
    for source in [&path, &pattern] {
        let mut tight = Server {
            memory_budget: Some(64),
            ..Server::new()
        };
        let out = call(
            &mut tight,
            "tuitab_query",
            json!({"source": source, "ops": [{"dedup": {"by": ["class"], "keep": "first"}}]}),
        );
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("dedup"),
            "{}",
            out
        );
    }
    let out = call(
        &mut server(true),
        "tuitab_jq",
        json!({"source": path, "program": "length"}),
    );
    assert_ne!(out["isError"], json!(true), "{}", out);
    assert!(
        out["content"][0]["text"].as_str().unwrap().contains('3'),
        "{}",
        out
    );
}

/// A nested JSONL field is its JSON on both paths: `json_get` and `expand_json` read
/// it, and `output.path` writes it back as the object it was.
#[test]
fn a_nested_jsonl_field_is_json_on_both_paths() {
    let text = "{\"id\":1,\"client\":{\"country\":\"RU\",\"n\":2},\"tags\":[\"a\"]}\n\
                {\"id\":2,\"client\":{\"country\":\"DE\",\"n\":3},\"tags\":[]}\n";
    let path = write("nested-fields.jsonl", text);
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"select": ["client", "tags"]}]}),
    );
    assert_eq!(
        out["structuredContent"]["rows"][0],
        json!(["{\"country\":\"RU\",\"n\":2}", "[\"a\"]"])
    );
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [
            {"compute": {"name": "c", "expr": "json_get(client, \"$.country\")"}},
            {"select": ["c"]}]}),
    );
    assert_eq!(out["structuredContent"]["rows"], json!([["RU"], ["DE"]]));
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [
            {"expand_json": {"col": "client", "prefix": "client_"}},
            {"select": ["client_country"]}]}),
    );
    assert_eq!(out["structuredContent"]["rows"], json!([["RU"], ["DE"]]));
    for eager in [true, false] {
        let target = dir().join(format!("nested-fields-out-{}.jsonl", eager));
        let _ = std::fs::remove_file(&target);
        let out = call(
            &mut server(eager),
            "tuitab_query",
            json!({"source": path, "ops": [], "output": {"path": target}}),
        );
        assert_ne!(out["isError"], json!(true), "{}", out);
        let parse = |t: &str| -> Vec<Value> {
            t.lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect()
        };
        assert_eq!(
            parse(&std::fs::read_to_string(&target).unwrap()),
            parse(text)
        );
    }
}

/// A Parquet list or struct reads the same streamed as loaded.
#[test]
fn a_parquet_list_and_struct_read_alike_on_both_paths() {
    use polars::prelude::*;
    let tags = Series::new(
        "tags".into(),
        [
            Series::new("".into(), ["a", "b"]),
            Series::new("".into(), Vec::<&str>::new()),
        ],
    );
    let geo = StructChunked::from_series(
        "geo".into(),
        2,
        [
            Series::new("lat".into(), [1.5, 2.5]),
            Series::new("city".into(), [Some("X"), None]),
        ]
        .iter(),
    )
    .unwrap()
    .into_series();
    let mut df = DataFrame::new(
        2,
        vec![
            Series::new("id".into(), [1i64, 2]).into(),
            tags.into(),
            geo.into(),
        ],
    )
    .unwrap();
    let path = dir().join("nested.parquet");
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    agree("tuitab_query", json!({"source": path, "ops": []}));
    agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"frequency": {"by": ["tags"]}}]}),
    );
}

/// A pattern is profiled once over all its files, on both paths: an epoch column in
/// one file and small integers in the same column of another stack as integers.
#[test]
fn a_pattern_is_profiled_over_all_its_files() {
    write("prof-a.csv", "ts,v\n1700000000000,1.5\n1700000001000,2\n");
    write("prof-b.csv", "ts,v\n5,3.5\n6,4.5\n");
    let pattern = dir().join("prof-*.csv").to_string_lossy().into_owned();
    let out = agree("tuitab_query", json!({"source": pattern, "ops": []}));
    assert_eq!(out["structuredContent"]["row_count"], json!(4), "{}", out);
    write("unit-a.csv", "created_at\n1787569800000\n");
    write("unit-b.csv", "created_at\n1787569800\n");
    let pattern = dir().join("unit-*.csv").to_string_lossy().into_owned();
    agree("tuitab_query", json!({"source": pattern, "ops": []}));
}

/// A moment past 2262 or before 1677 — `9999-12-31`, an SCD's open end — compares,
/// rather than failing the call.
#[test]
fn a_moment_outside_the_nanosecond_range_compares() {
    let path = write("events-far.csv", CSV);
    for (op, value, rows) in [
        ("lt", "2300-01-01 00:00", 5),
        ("gt", "2300-01-01 00:00", 0),
        ("gt", "1600-01-01T00:00", 5),
        ("lt", "9999-12-31", 5),
    ] {
        let out = agree(
            "tuitab_query",
            json!({"source": path, "ops": [{"filter": {"col": "Time", "op": op, "value": value}}]}),
        );
        assert_eq!(
            out["structuredContent"]["row_count"],
            json!(rows),
            "{} {} {}",
            op,
            value,
            out
        );
    }
    // A column held in nanoseconds, as Parquet often holds one.
    use polars::prelude::*;
    let ts = Series::new(
        "ts".into(),
        [1_700_000_000_000_000_000i64, 1_700_000_001_000_000_000],
    )
    .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
    .unwrap();
    let mut df = DataFrame::new(2, vec![ts.into()]).unwrap();
    let path = dir().join("ns.parquet");
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    for (op, value, rows) in [("lt", "2300-01-01 00:00", 2), ("gt", "1600-01-01T00:00", 2)] {
        let out = agree(
            "tuitab_query",
            json!({"source": path, "ops": [{"filter": {"col": "ts", "op": op, "value": value}}]}),
        );
        assert_eq!(
            out["structuredContent"]["row_count"],
            json!(rows),
            "{} {} {}",
            op,
            value,
            out
        );
    }
}

/// A CSV pattern whose files use different separators reads each file by its own.
#[test]
fn a_csv_pattern_reads_each_file_by_its_own_separator() {
    write("sep-a.csv", "k,v\nx,1\n");
    write("sep-b.csv", "k;v\ny;2\n");
    let pattern = dir().join("sep-*.csv").to_string_lossy().into_owned();
    let out = agree("tuitab_query", json!({"source": pattern, "ops": []}));
    assert_eq!(
        out["structuredContent"]["rows"],
        json!([["x", 1], ["y", 2]]),
        "{}",
        out
    );
}

/// A file whose name holds `[`, `]`, `?` or `*` is that file, not a pattern.
#[test]
fn a_file_named_like_a_pattern_is_read_as_itself() {
    let csv: String = std::iter::once("a\n".to_string())
        .chain((0..200).map(|i| format!("{}\n", i)))
        .collect();
    let jsonl: String = (0..200).map(|i| format!("{{\"a\":{}}}\n", i)).collect();
    for (name, text) in [("report[1].csv", csv), ("events[1].jsonl", jsonl)] {
        let path = write(name, &text);
        let out = agree("tuitab_query", json!({"source": path, "ops": []}));
        assert_eq!(out["structuredContent"]["row_count"], json!(200), "{}", out);
        // Scanned, not loaded: only the scan refuses a dedup past the budget.
        let out = call(
            &mut Server {
                memory_budget: Some(64),
                ..Server::new()
            },
            "tuitab_query",
            json!({"source": path, "ops": [{"dedup": {"by": ["a"], "keep": "first"}}]}),
        );
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("dedup"),
            "{} {}",
            name,
            out
        );
    }
}

/// A limit past what a row index holds is no limit at all, not a wrapped-around one.
#[test]
fn a_limit_past_the_row_index_reads_every_row() {
    let path = write("limit-far.csv", CSV);
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"limit": 4294967297u64}], "output": {"limit": 4294967296u64}}),
    );
    assert_eq!(out["structuredContent"]["row_count"], json!(5), "{}", out);
}

/// A step that compares with a nested column reads it as the in-memory path does.
#[test]
fn a_comparison_with_a_nested_column_reads_it_alike() {
    let path = write("nested-cmp.jsonl", JSONL);
    agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"filter": {"col": "msg", "op": "ne", "value": {"col": "geo"}}}]}),
    );
}

/// A compute that reads no nested column streams beside one: a JSON Lines log with an
/// object field still counts in the budget.
#[test]
fn a_compute_beside_a_nested_column_streams() {
    let mut text = String::new();
    for i in 0..2000 {
        text.push_str(&format!("{{\"ms\":{},\"geo\":{{\"lat\":{}}}}}\n", i, i % 7));
    }
    let path = write("nested-compute.jsonl", &text);
    let mut tight = Server {
        memory_budget: Some(4096),
        ..Server::new()
    };
    let out = call(
        &mut tight,
        "tuitab_query",
        json!({"source": path, "ops": [
            {"compute": {"name": "slow", "expr": "ms > 1000"}},
            {"frequency": {"by": ["slow"]}}]}),
    );
    assert_ne!(out["isError"], json!(true), "{}", out);
}

/// A large `output.limit` is held to the budget like any other result.
#[test]
fn a_large_output_limit_is_held_to_the_budget() {
    let text: String = std::iter::once("id,name\n".to_string())
        .chain((0..5000).map(|i| format!("{},row number {}\n", i, i)))
        .collect();
    let path = write("many-rows.csv", &text);
    let out = call(
        &mut Server {
            memory_budget: Some(4096),
            ..Server::new()
        },
        "tuitab_query",
        json!({"source": path, "ops": [], "output": {"limit": 100000}}),
    );
    assert_eq!(out["isError"], json!(true), "{}", out);
}

/// A file past the budget that cannot be scanned is refused with the reason, not read
/// whole into memory.
#[test]
fn a_file_too_large_to_load_that_cannot_be_scanned_is_refused() {
    let mut text = String::new();
    for i in 0..300 {
        if i % 2 == 0 {
            text.push_str(&format!("{{\"g\":{}}}\n", i));
        } else {
            text.push_str(&format!("{{\"g\":{{\"x\":{}}}}}\n", i));
        }
    }
    let path = write("shape-shifting.jsonl", &text);
    let out = call(
        &mut Server {
            memory_budget: Some(1024),
            ..Server::new()
        },
        "tuitab_query",
        json!({"source": path, "ops": []}),
    );
    assert_eq!(out["isError"], json!(true), "{}", out);
    let message = out["content"][0]["text"].as_str().unwrap();
    assert!(message.contains("stream"), "{}", message);
}

/// `NaN` in a CSV is a missing value, as the documented null tokens are: it is not
/// counted, and it does not turn an average into NaN.
#[test]
fn nan_in_a_csv_is_missing_on_both_paths() {
    let path = write("nan.csv", "v,w\n1.5,1\nNaN,NaN\n3.5,3\n");
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"aggregate": [
            {"col": "v", "fn": "avg"}, {"col": "v", "fn": "count"}, {"col": "w", "fn": "sum"}]}]}),
    );
    assert_eq!(
        out["structuredContent"]["rows"],
        json!([[2.5, 2, 4.0]]),
        "{}",
        out
    );
}

/// A `date_format` pattern chrono cannot read is refused when the expression is read —
/// on both paths, and not as a failure to read the file.
#[test]
fn a_bad_date_format_is_refused_alike() {
    let path = write("dates-format.csv", CSV);
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"compute": {"name": "f", "expr": "date_format(ts, \"%Q\")"}}]}),
    );
    assert_eq!(out["isError"], json!(true), "{}", out);
    let message = out["content"][0]["text"].as_str().unwrap();
    assert!(
        message.contains("%Q") && !message.contains("Could not read"),
        "{}",
        message
    );
}

/// A DECIMAL column — DuckDB, Spark and Arrow write them — reads as numbers on both
/// paths.
#[test]
fn a_parquet_decimal_column_reads_as_numbers() {
    use polars::prelude::*;
    let amount = Series::new("amount".into(), [1.25f64, 10.5])
        .cast(&DataType::Decimal(5, 2))
        .unwrap();
    let mut df = DataFrame::new_infer_height(vec![amount.into()]).unwrap();
    let path = dir().join("decimal.parquet");
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"aggregate": [{"col": "amount", "fn": "sum"}]}]}),
    );
    assert_eq!(
        out["structuredContent"]["rows"],
        json!([[11.75]]),
        "{}",
        out
    );
}

/// Whole numbers at the head of a JSON Lines file and a decimal past it are one column
/// of floats on both paths, so the file still streams.
#[test]
fn a_decimal_past_the_head_still_streams() {
    let mut text = "{\"ms\":1}\n".repeat(120_000);
    text.push_str("{\"ms\":1.5}\n");
    assert!(text.len() > 1 << 20);
    let path = write("late-decimal.jsonl", &text);
    let out = call(
        &mut Server {
            memory_budget: Some(4096),
            ..Server::new()
        },
        "tuitab_query",
        json!({"source": path, "ops": [{"dedup": {"by": ["ms"], "keep": "first"}}]}),
    );
    let message = out["content"][0]["text"].as_str().unwrap();
    assert!(
        message.contains("'dedup'"),
        "it did not stream: {}",
        message
    );
}

/// `in` and `not_in` over a datetime column match the moments their literals name,
/// as `eq` does.
#[test]
fn in_over_a_datetime_column_matches_moments() {
    let path = write("events-in.csv", CSV);
    for (op, rows) in [("in", 2), ("not_in", 3)] {
        let out = agree(
            "tuitab_query",
            json!({"source": path, "ops": [{"filter": {"col": "Time", "op": op,
                "value": ["2026-08-24 11:10:00", "2026-08-24 12:10"]}}]}),
        );
        assert_eq!(
            out["structuredContent"]["row_count"],
            json!(rows),
            "{} {}",
            op,
            out
        );
    }
}

/// A step that cannot run answers the same on both paths, and a step's error is not
/// called a read error.
#[test]
fn a_step_that_cannot_run_fails_alike() {
    let path = write("events-errors.csv", CSV);
    for expr in [
        "time_bucket(class, \"1h\")",
        "contains(class, \"[\")",
        "time_bucket(ms, \"1h\")",
        "abs(class)",
        "log(class)",
        "round(class, 1)",
        "hour(class)",
        "class + 1",
        "ms * class",
        "date(class)",
        "json_get(ms, \"$.a\")",
        "regexp_replace(ms, \"1\", \"x\")",
        "substring(ms, 0, 1)",
        "split(ms, \"0\", 0)",
        "concat(ms, class)",
        "len(ms)",
        "from_epoch(class, \"s\")",
    ] {
        let out = agree(
            "tuitab_query",
            json!({"source": path, "ops": [{"compute": {"name": "x", "expr": expr}}]}),
        );
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(!text.contains("Could not read"), "{}: {}", expr, text);
    }
    for filter in [
        json!({"col": "ms", "op": "contains", "value": "^1"}),
        json!({"col": "class", "op": "gt", "value": 5}),
        json!({"col": "ms", "op": "eq", "value": "ten"}),
        json!({"col": "flag", "op": "gt", "value": 0}),
        json!({"col": "Time", "op": "contains", "value": "11:10"}),
        json!({"col": "ms", "op": "gt", "value": {"col": "class"}}),
        json!({"col": "Time", "op": "between", "value": [1, 2]}),
        json!({"col": "ms", "op": "in", "value": ["a", 10]}),
        json!({"col": "class", "op": "contains", "value": "["}),
    ] {
        let out = agree(
            "tuitab_query",
            json!({"source": path, "ops": [{"filter": filter}]}),
        );
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(!text.contains("Could not read"), "{}: {}", filter, text);
    }
    // A pattern taken from a column only fails once a row holds a bad one.
    let path = write(
        "patterns.csv",
        "k,p
abc,b
abc,[
",
    );
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"compute": {"name": "x", "expr": "contains(k, p)"}}]}),
    );
    let text = out["content"][0]["text"].as_str().unwrap();
    assert!(!text.contains("Could not read"), "{}", text);
}

/// Sums and means over a boolean column are numbers, on both paths.
#[test]
fn numeric_aggregates_over_booleans_are_numbers() {
    let path = write("events-bool.csv", CSV);
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"group_by": {"by": ["class"], "agg": [
            {"col": "flag", "fn": "sum"}, {"col": "flag", "fn": "avg"}]}}]}),
    );
    let cols = &out["structuredContent"]["columns"];
    assert_eq!(cols[1]["type"], json!("integer"), "{}", out);
    assert_eq!(cols[2]["type"], json!("float"), "{}", out);
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"aggregate": [{"col": "flag", "fn": "sum"}]}]}),
    );
    assert_eq!(
        out["structuredContent"]["columns"][0]["type"],
        json!("integer"),
        "{}",
        out
    );
    // A window over the same column reads it the same way.
    for (function, kind) in [("sum", "integer"), ("avg", "float"), ("median", "float")] {
        let out = agree(
            "tuitab_query",
            json!({"source": path, "ops": [
                {"window": {"fn": function, "col": "flag", "over": ["class"], "as": "w"}},
                {"select": ["w"]}]}),
        );
        assert_ne!(out["isError"], json!(true), "{} {}", function, out);
        assert_eq!(
            out["structuredContent"]["columns"][0]["type"],
            json!(kind),
            "{} {}",
            function,
            out
        );
    }
}

/// A file is written with the values the rows carry, and the answer does not claim
/// they were formatted.
#[test]
fn a_written_file_is_not_said_to_be_formatted() {
    let path = write("events-note.csv", CSV);
    let out = dir().join("events-note-out.csv");
    let _ = std::fs::remove_file(&out);
    let wrote = call(
        &mut server(true),
        "tuitab_query",
        json!({"source": path, "ops": [], "output": {"path": out}}),
    );
    assert_eq!(wrote["isError"], json!(false), "{}", wrote);
    assert!(!wrote.to_string().contains("formatted"), "{}", wrote);
}

/// `contains` over a datetime column reads the text the column shows.
#[test]
fn contains_over_a_datetime_reads_what_is_shown() {
    let path = write("events-contains-dt.csv", CSV);
    let out = agree(
        "tuitab_query",
        json!({"source": path, "ops": [{"filter": {"col": "Time", "op": "contains", "value": "10:00$"}}]}),
    );
    // 11:10:00 and 12:10:00 — no `.000` after them.
    assert_eq!(out["structuredContent"]["row_count"], json!(2), "{}", out);
}

/// A bucket wider than a datetime can hold is refused, not a crash.
#[test]
fn a_bucket_too_wide_is_refused() {
    let path = write("events-bucket.csv", CSV);
    for col in ["ts", "Time"] {
        let out = agree(
            "tuitab_query",
            json!({"source": path, "ops": [{"compute": {"name": "b",
                "expr": format!("time_bucket({}, \"4000000000d\")", col)}}]}),
        );
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(!text.contains("crashed"), "{}", text);
        assert_eq!(out["isError"], json!(true), "{}", out);
    }
}

/// The same file read as two formats is read as each, not from the other's profile.
#[test]
fn a_file_read_as_two_formats_is_profiled_for_each() {
    let path = write("two-formats.txt", "a\tb,c\n1\t2,3\n");
    let as_csv = agree(
        "tuitab_query",
        json!({"source": {"path": path, "format": "csv"}, "ops": []}),
    );
    let as_tsv = agree(
        "tuitab_query",
        json!({"source": {"path": path, "format": "tsv"}, "ops": []}),
    );
    let names = |out: &Value| -> Vec<String> {
        out["structuredContent"]["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(names(&as_csv), ["a\tb", "c"], "{}", as_csv);
    assert_eq!(names(&as_tsv), ["a", "b,c"], "{}", as_tsv);
}

/// Files of a pattern that disagree on a column's type are named, on both paths, as
/// the in-memory path always has.
#[test]
fn a_csv_pattern_with_types_that_differ_names_the_file() {
    write("mixed-types-a.csv", "k,v\nx,1\n");
    write("mixed-types-b.csv", "k,v\ny,abc\n");
    let pattern = dir()
        .join("mixed-types-*.csv")
        .to_string_lossy()
        .into_owned();
    let out = agree("tuitab_query", json!({"source": pattern, "ops": []}));
    assert_eq!(out["isError"], json!(true), "{}", out);
    assert!(
        out["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("mixed-types-b.csv"),
        "{}",
        out
    );
}

/// The guard names what wanted the rows: a describe or a list is not writing anything.
#[test]
fn the_guard_names_the_tool_that_wanted_the_rows() {
    let path = write("guard-names.csv", CSV);
    let mut tight = Server {
        memory_budget: Some(16),
        ..Server::new()
    };
    let out = call(
        &mut tight,
        "tuitab_describe",
        json!({"source": path, "columns": ["ms"]}),
    );
    let text = out["content"][0]["text"].as_str().unwrap().to_string();
    assert!(
        text.contains("tuitab_describe") && !text.contains("writing"),
        "{}",
        text
    );
    let out = call(
        &mut tight,
        "tuitab_calc",
        json!({"lists": {"m": {"source": path, "col": "ms"}}, "expr": "mean(m)"}),
    );
    let text = out["content"][0]["text"].as_str().unwrap().to_string();
    assert!(
        text.contains("lists") && !text.contains("writing"),
        "{}",
        text
    );
}

#[test]
fn a_jsonl_pattern_streams_like_its_files_loaded() {
    write(
        "lines-a.jsonl",
        "{\"k\":\"x\",\"v\":1}\n{\"k\":\"y\",\"v\":2}\n",
    );
    write("lines-b.jsonl", "{\"k\":\"x\",\"v\":3}\n");
    let pattern = dir().join("lines-*.jsonl").to_string_lossy().into_owned();
    let out = agree(
        "tuitab_query",
        json!({"source": pattern, "ops": [{"group_by": {"by": ["k"], "agg": [{"col": "v", "fn": "sum"}]}}]}),
    );
    assert_eq!(out["structuredContent"]["row_count"], json!(2), "{}", out);
}

/// A pattern's files are typed with the `null_values` the source names, as each file
/// alone is.
#[test]
fn a_csv_pattern_types_its_files_with_the_null_values_given() {
    write("nulls-a.csv", "k,v\nx,1\ny,-\n");
    write("nulls-b.csv", "k,v\nz,3\nw,-\n");
    let pattern = dir().join("nulls-*.csv").to_string_lossy().into_owned();
    let out = agree(
        "tuitab_query",
        json!({"source": {"path": pattern, "null_values": ["-"]},
               "ops": [{"aggregate": [{"col": "v", "fn": "sum"}]}]}),
    );
    assert_eq!(out["isError"], json!(false), "{}", out);
    assert_eq!(out["structuredContent"]["rows"][0][0], json!(4), "{}", out);
}

/// A pattern's profile is kept per version of every file it matches: rewriting one of
/// them is read afresh, by a server that has seen the pattern before.
#[test]
fn a_pattern_is_profiled_again_when_one_of_its_files_changes() {
    write("versions-a.csv", "k,v\nx,1\n");
    write("versions-b.csv", "k,v\ny,2\n");
    let pattern = dir().join("versions-*.csv").to_string_lossy().into_owned();
    let mut s = server(false);
    let args = json!({"source": pattern, "ops": [{"aggregate": [{"col": "v", "fn": "sum"}]}]});
    let out = call(&mut s, "tuitab_query", args.clone());
    assert_eq!(out["structuredContent"]["rows"][0][0], json!(3), "{}", out);
    write("versions-b.csv", "k,v\ny,20\nz,30\n");
    let out = call(&mut s, "tuitab_query", args);
    assert_eq!(out["structuredContent"]["rows"][0][0], json!(51), "{}", out);
}

/// A file far smaller than the memory budget is loaded once and kept, as before
/// streaming: a repeated query answers from memory instead of reading it again.
#[test]
fn a_small_file_is_loaded_and_kept_rather_than_streamed() {
    let path = write("kept.csv", CSV);
    let mut s = Server::new();
    let args = json!({"source": path, "ops": [{"frequency": {"by": ["class"]}}]});
    let out = call(&mut s, "tuitab_query", args);
    assert_eq!(out["isError"], json!(false), "{}", out);
    assert_eq!(s.cache.len(), 1, "the frame is kept");
    // Past a sixteenth of the budget it streams, and nothing is kept.
    let mut tight = Server {
        memory_budget: Some(16 * 8),
        ..Server::new()
    };
    let out = call(
        &mut tight,
        "tuitab_query",
        json!({"source": path, "ops": [{"limit": 1}]}),
    );
    assert_eq!(out["isError"], json!(false), "{}", out);
    assert!(tight.cache.is_empty());
}
