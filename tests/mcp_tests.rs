//! Coverage for the MCP server.
//!
//! Most of these drive `handle_message` directly — it is the whole request path
//! minus the pipe, so a test gets the same answer a client would without paying
//! for a process.  One test at the end does spawn the binary, to check the
//! framing and that nothing leaks onto stdout.

use serde_json::{json, Value};
use std::path::PathBuf;
use tuitab::mcp::{handle_message, Server};

fn tmp(name: &str) -> PathBuf {
    // Not the system temp dir: test output belongs to the project, under the
    // gitignored tmp/ directory.
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("mcp-tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn send(server: &mut Server, message: Value) -> Value {
    handle_message(server, &message.to_string()).expect("a request must get a response")
}

/// Call a tool and return its structured payload, failing on a tool error.
fn call(server: &mut Server, name: &str, arguments: Value) -> Value {
    let response = send(
        server,
        json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
               "params": {"name": name, "arguments": arguments}}),
    );
    let result = response
        .get("result")
        .unwrap_or_else(|| panic!("{} returned a protocol error: {}", name, response));
    assert_eq!(
        result.get("isError"),
        Some(&json!(false)),
        "{} failed: {}",
        name,
        result
    );
    result.get("structuredContent").cloned().unwrap()
}

/// Call a tool expecting it to run and fail, returning the message.
fn call_expecting_failure(server: &mut Server, name: &str, arguments: Value) -> String {
    let response = send(
        server,
        json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
               "params": {"name": name, "arguments": arguments}}),
    );
    let result = response
        .get("result")
        .expect("a tool error is still a result");
    assert_eq!(
        result.get("isError"),
        Some(&json!(true)),
        "expected {} to fail, got {}",
        name,
        result
    );
    result["content"][0]["text"].as_str().unwrap().to_string()
}

fn query(server: &mut Server, ops: Value) -> Value {
    call(
        server,
        "tuitab_query",
        json!({"source": {"path": "test_data/sample.csv"}, "ops": ops}),
    )
}

/// Cells of a result row, keyed by column name.
fn row(result: &Value, index: usize) -> Vec<(String, Value)> {
    let names: Vec<String> = result["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect();
    names
        .into_iter()
        .zip(result["rows"][index].as_array().unwrap().iter().cloned())
        .collect()
}

fn cell(result: &Value, index: usize, column: &str) -> Value {
    row(result, index)
        .into_iter()
        .find(|(name, _)| name == column)
        .unwrap_or_else(|| panic!("no column '{}' in {}", column, result["columns"]))
        .1
}

// ── protocol ────────────────────────────────────────────────────────────────

#[test]
fn initialize_answers_with_the_requested_version_and_the_instructions() {
    let mut server = Server::new();
    let response = send(
        &mut server,
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
               "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                          "clientInfo": {"name": "test", "version": "0"}}}),
    );

    let result = &response["result"];
    assert_eq!(result["protocolVersion"], "2025-06-18");
    assert_eq!(result["serverInfo"]["name"], "tuitab");
    assert!(result["capabilities"]["tools"].is_object());

    let instructions = result["instructions"].as_str().unwrap();
    assert!(instructions.contains("tuitab_inspect"), "{}", instructions);
    assert!(instructions.contains("group_by"), "{}", instructions);
}

#[test]
fn an_unsupported_protocol_version_falls_back_to_one_we_speak() {
    let mut server = Server::new();
    let response = send(
        &mut server,
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize",
               "params": {"protocolVersion": "1.0.0"}}),
    );
    assert_eq!(response["result"]["protocolVersion"], "2025-06-18");
}

#[test]
fn notifications_and_responses_get_no_reply() {
    let mut server = Server::new();
    assert!(handle_message(
        &mut server,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#
    )
    .is_none());
    assert!(handle_message(&mut server, r#"{"jsonrpc":"2.0","id":7,"result":{}}"#).is_none());
}

#[test]
fn malformed_input_is_a_parse_error_rather_than_a_crash() {
    let mut server = Server::new();
    let response = handle_message(&mut server, "not json at all").unwrap();
    assert_eq!(response["error"]["code"], -32700);
}

#[test]
fn tools_list_offers_the_five_tools() {
    let mut server = Server::new();
    let response = send(
        &mut server,
        json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    );

    let names: Vec<&str> = response["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec![
            "tuitab_inspect",
            "tuitab_query",
            "tuitab_describe",
            "tuitab_jq",
            "tuitab_calc"
        ]
    );

    for tool in response["result"]["tools"].as_array().unwrap() {
        assert!(
            tool["inputSchema"]["properties"].is_object(),
            "{}",
            tool["name"]
        );
        assert!(
            !tool["description"].as_str().unwrap().is_empty(),
            "{}",
            tool["name"]
        );
    }
}

#[test]
fn an_unknown_method_is_a_protocol_error() {
    let mut server = Server::new();
    let response = send(
        &mut server,
        json!({"jsonrpc": "2.0", "id": 1, "method": "nope"}),
    );
    assert_eq!(response["error"]["code"], -32601);
}

/// An unknown *tool* breaks the contract, so it is a protocol error — unlike a
/// tool that runs and fails, which comes back as a result the model can read.
#[test]
fn an_unknown_tool_is_a_protocol_error() {
    let mut server = Server::new();
    let response = send(
        &mut server,
        json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
               "params": {"name": "tuitab_nonesuch", "arguments": {}}}),
    );
    assert_eq!(response["error"]["code"], -32602);
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("tuitab_nonesuch"));
}

// ── tuitab_inspect ──────────────────────────────────────────────────────────

#[test]
fn inspect_reports_columns_types_and_a_sample() {
    let mut server = Server::new();
    let result = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": "test_data/sample.csv"}),
    );

    assert_eq!(result["row_count"], 20);
    let columns: Vec<&str> = result["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(columns, vec!["id", "name", "age", "salary", "department"]);

    let types: Vec<&str> = result["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["type"].as_str().unwrap())
        .collect();
    assert_eq!(types[2], "integer", "age");
    assert_eq!(types[4], "string", "department");

    assert_eq!(result["sample_rows"].as_array().unwrap().len(), 5);
}

/// Multi-table formats are the reason `container` exists, and the repository
/// ships no SQLite fixture — so this makes one, which also exercises the saver.
#[test]
fn inspect_lists_tables_before_showing_columns() {
    let mut server = Server::new();
    let db = tmp("sample.sqlite");
    let _ = std::fs::remove_file(&db);

    call(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [],
               "output": {"path": db.to_string_lossy()}}),
    );

    // Without a container, the answer is the list of tables, not rows.
    let listing = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": {"path": db.to_string_lossy()}}),
    );
    let tables: Vec<&str> = listing["containers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        tables,
        vec!["result"],
        "the saver names the table after what it was given"
    );
    // The listing carries what the catalogue knows, not just a name.
    assert_eq!(listing["containers"][0]["kind"], "table");
    assert_eq!(listing["containers"][0]["rows"], 20);
    assert!(listing["containers"][0]["create_sql"]
        .as_str()
        .unwrap()
        .contains("CREATE TABLE"));
    assert!(
        listing.get("columns").is_none(),
        "no columns without a container"
    );
    assert!(listing["note"].as_str().unwrap().contains("container"));

    // With one, it is an ordinary table.
    let table = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": {"path": db.to_string_lossy(), "container": "result"}}),
    );
    assert_eq!(table["row_count"], 20);

    // And it queries like any other source.
    let result = call(
        &mut server,
        "tuitab_query",
        json!({"source": {"path": db.to_string_lossy(), "container": "result"},
               "ops": [{"frequency": {"by": ["department"]}}]}),
    );
    assert_eq!(result["row_count"], 4);

    std::fs::remove_file(&db).unwrap();
}

#[test]
fn inspect_reports_a_missing_file_as_a_tool_error() {
    let mut server = Server::new();
    let message = call_expecting_failure(
        &mut server,
        "tuitab_inspect",
        json!({"source": "test_data/does_not_exist.csv"}),
    );
    assert!(message.contains("No such file"), "{}", message);
}

// ── filtering and sorting ───────────────────────────────────────────────────

#[test]
fn filter_keeps_only_matching_rows() {
    let mut server = Server::new();
    // Seven people are over 40: ages 45, 42, 52, 47, 44, 50 and 41.
    let result = query(
        &mut server,
        json!([{"filter": [{"col": "age", "op": "gt", "value": 40}]}]),
    );
    assert_eq!(result["row_count"], 7);
}

#[test]
fn filter_predicates_combine_with_and() {
    let mut server = Server::new();
    // Engineering ages are 30, 28, 42, 52, 33, 37 and 34 — two are over 40.
    // Seven people company-wide are, so this only passes if both predicates
    // apply rather than the last one winning.
    let result = query(
        &mut server,
        json!([{"filter": [
            {"col": "department", "op": "eq", "value": "Engineering"},
            {"col": "age", "op": "gt", "value": 40}
        ]}]),
    );
    assert_eq!(result["row_count"], 2);
}

/// An aggregate in `compute` must see only the rows that survived `filter`.
///
/// `add_computed_column` evaluates over the whole physical frame
/// (`dataframe.rs:1101`), and `sum(x)` lowers to a broadcast over the entire
/// column (`expression.rs:213`).  Unless a row-dropping operation materialises
/// its result, a share-of-total lands on the wrong denominator — and looks
/// perfectly reasonable while doing it.
#[test]
fn an_aggregate_after_filter_uses_only_the_surviving_rows() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([
            {"filter": [{"col": "department", "op": "eq", "value": "Engineering"}]},
            {"compute": {"name": "share", "expr": "salary / sum(salary)"}}
        ]),
    );

    assert_eq!(result["row_count"], 7, "Engineering has seven people");

    let shares: f64 = (0..7)
        .map(|i| cell(&result, i, "share").as_f64().unwrap())
        .sum();

    // The seven Engineering salaries total 563001.50; the whole file totals
    // 1624003.25.  Dividing by the latter would give 0.3467 — a number that
    // reads like a plausible answer to a different question.
    assert!(
        (shares - 1.0).abs() < 1e-9,
        "shares must total 1.0 within the filtered set, got {}",
        shares
    );
}

#[test]
fn filter_supports_in_between_and_contains() {
    let mut server = Server::new();

    let in_list = query(
        &mut server,
        json!([{"filter": [{"col": "department", "op": "in", "value": ["HR", "Marketing"]}]}]),
    );
    assert_eq!(in_list["row_count"], 8, "4 in HR and 4 in Marketing");

    let between = query(
        &mut server,
        json!([{"filter": [{"col": "age", "op": "between", "value": [30, 35]}]}]),
    );
    assert_eq!(
        between["row_count"], 5,
        "ages 30, 35, 31, 33 and 34, bounds included"
    );

    let contains = query(
        &mut server,
        json!([{"filter": [{"col": "name", "op": "contains", "value": "^A"}]}]),
    );
    assert_eq!(contains["row_count"], 1, "only Alice Johnson starts with A");
}

#[test]
fn any_of_gives_the_filter_an_or() {
    let mut server = Server::new();

    let either = query(
        &mut server,
        json!([{"filter": [{"any_of": [
            {"col": "department", "op": "eq", "value": "HR"},
            {"col": "department", "op": "eq", "value": "Marketing"}
        ]}]}]),
    );
    assert_eq!(either["row_count"], 8, "4 in HR and 4 in Marketing");

    // A group beside a plain predicate reads as (A OR B) AND C.
    let narrowed = query(
        &mut server,
        json!([{"filter": [
            {"any_of": [{"col": "department", "op": "eq", "value": "HR"},
                        {"col": "department", "op": "eq", "value": "Marketing"}]},
            {"col": "age", "op": "gt", "value": 40}
        ]}]),
    );
    assert_eq!(narrowed["row_count"], 1, "only Noah Martin, 44");
}

#[test]
fn a_predicate_can_name_another_column_as_its_value() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([{"filter": [{"col": "salary", "op": "gt", "value": {"col": "age"}}]}]),
    );
    assert_eq!(result["row_count"], 20, "every salary exceeds its age");
}

/// A filter that legitimately matches nothing must return nothing, not fall
/// through to a second evaluator with different semantics.
#[test]
fn a_filter_matching_nothing_returns_nothing() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([{"filter": [{"col": "department", "op": "eq", "value": "Legal"}]}]),
    );
    assert_eq!(result["row_count"], 0);
    assert_eq!(result["returned"], 0);
}

#[test]
fn a_filter_on_an_unknown_column_names_the_ones_that_exist() {
    let mut server = Server::new();
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv",
               "ops": [{"filter": [{"col": "salary_usd", "op": "gt", "value": 1}]}]}),
    );
    assert!(message.contains("salary_usd"), "{}", message);
    assert!(
        message.contains("department"),
        "must list what is available: {}",
        message
    );
}

#[test]
fn sort_orders_rows_and_composes_with_filter() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([
            {"filter": [{"col": "department", "op": "eq", "value": "HR"}]},
            {"sort": {"col": "age", "desc": true}}
        ]),
    );
    // HR ages are 25, 29, 26 and 27.
    assert_eq!(result["row_count"], 4);
    assert_eq!(cell(&result, 0, "age"), json!(29));
    assert_eq!(cell(&result, 3, "age"), json!(25));
}

#[test]
fn sort_takes_several_keys_at_once() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([{"sort": {"by": [{"col": "department"}, {"col": "age", "desc": true}]}}]),
    );

    // Engineering sorts first alphabetically, and holds ages 52, 42, 37, 34,
    // 33, 30, 28 once ordered downwards.
    for (i, age) in [52, 42, 37, 34, 33, 30, 28].iter().enumerate() {
        assert_eq!(cell(&result, i, "department"), json!("Engineering"));
        assert_eq!(cell(&result, i, "age"), json!(age));
    }
}

/// A bare column name and the single-key object form both still work — a model
/// asking one simple question should not have to write a list.
#[test]
fn sort_still_accepts_the_short_forms() {
    let mut server = Server::new();

    let object_form = query(&mut server, json!([{"sort": {"col": "age", "desc": true}}]));
    assert_eq!(cell(&object_form, 0, "age"), json!(52));

    let bare_name = query(&mut server, json!([{"sort": {"by": "age"}}]));
    assert_eq!(cell(&bare_name, 0, "age"), json!(25));
}

// ── aggregation ─────────────────────────────────────────────────────────────

#[test]
fn aggregate_answers_a_grand_total() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([{"aggregate": [{"col": "salary", "fn": "sum"},
                              {"col": "*", "fn": "count"}]}]),
    );

    assert_eq!(result["row_count"], 1, "a total is a single row");
    assert_eq!(cell(&result, 0, "salary:sum"), json!(1_624_003.25));
    assert_eq!(cell(&result, 0, "count"), json!(20));
}

#[test]
fn aggregate_totals_only_what_survived_the_filter() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([
            {"filter": [{"col": "department", "op": "eq", "value": "Engineering"}]},
            {"aggregate": [{"col": "salary", "fn": "sum"}]}
        ]),
    );
    assert_eq!(cell(&result, 0, "salary:sum"), json!(563_001.5));
}

#[test]
fn group_by_returns_exactly_the_requested_aggregates() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([{"group_by": {"by": ["department"],
                             "agg": [{"col": "salary", "fn": "sum"},
                                     {"col": "*", "fn": "count"}]}}]),
    );

    let columns: Vec<&str> = result["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        columns,
        vec!["department", "salary:sum", "count"],
        "no Count, Pct or Bar columns anyone did not ask for"
    );

    // group_by_stable keeps first-appearance order: Engineering, Management,
    // Marketing, HR.
    assert_eq!(cell(&result, 0, "department"), json!("Engineering"));
    assert_eq!(cell(&result, 0, "salary:sum"), json!(563001.5));
    assert_eq!(cell(&result, 0, "count"), json!(7));
    assert_eq!(cell(&result, 1, "department"), json!("Management"));
    assert_eq!(cell(&result, 1, "salary:sum"), json!(480000.5));
}

/// `preserves_col_type` hands an average the source column's type, which is
/// right for TUI formatting and wrong as a label on a fractional number.
#[test]
fn an_average_over_integers_is_reported_as_a_float() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([{"group_by": {"by": ["department"], "agg": [{"col": "age", "fn": "avg"}]}}]),
    );

    let age_avg = result["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "age:avg")
        .unwrap();
    assert_eq!(age_avg["type"], "float");

    // Engineering ages 30, 28, 42, 52, 33, 37, 34 sum to 256 over 7 people.
    assert_eq!(cell(&result, 0, "age:avg"), json!(256.0 / 7.0));
}

#[test]
fn group_by_after_filter_aggregates_only_the_surviving_rows() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([
            {"filter": [{"col": "age", "op": "gt", "value": 40}]},
            {"group_by": {"by": ["department"], "agg": [{"col": "*", "fn": "count"}]}}
        ]),
    );
    // Of the seven over-40s: Management 4 (45, 47, 50, 41), Engineering 2
    // (42, 52), Marketing 1 (44).
    let total: i64 = result["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r[1].as_i64().unwrap())
        .sum();
    assert_eq!(total, 7);
}

/// build_frequency_table silently drops an incompatible aggregator
/// (dataframe.rs:1445).  That would hand the model a short answer with no way
/// to notice, so the MCP layer refuses instead.
#[test]
fn an_aggregate_that_cannot_apply_is_an_error_not_a_silent_omission() {
    let mut server = Server::new();
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv",
               "ops": [{"group_by": {"by": ["department"],
                                     "agg": [{"col": "name", "fn": "sum"}]}}]}),
    );
    assert!(message.contains("name"), "{}", message);
    assert!(message.contains("sum"), "{}", message);
    assert!(message.contains("string"), "must say why: {}", message);
}

#[test]
fn frequency_ranks_by_count_and_carries_a_share_column() {
    let mut server = Server::new();
    let result = query(&mut server, json!([{"frequency": {"by": ["department"]}}]));

    let columns: Vec<&str> = result["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        columns,
        vec!["department", "Count", "Pct"],
        "the ASCII Bar column is dropped"
    );

    assert_eq!(result["row_count"], 4);
    assert_eq!(cell(&result, 0, "department"), json!("Engineering"));
    assert_eq!(cell(&result, 0, "Count"), json!(7));
    // Percentages are fractions, not "35%".
    assert_eq!(cell(&result, 0, "Pct"), json!(0.35));
}

#[test]
fn compute_adds_a_derived_column() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([
            {"compute": {"name": "decade", "expr": "age / 10"}},
            {"filter": [{"col": "department", "op": "eq", "value": "HR"}]},
            {"sort": {"col": "age", "desc": false}}
        ]),
    );
    // The youngest HR person is 25.
    assert_eq!(cell(&result, 0, "decade"), json!(2.5));
}

// ── join ────────────────────────────────────────────────────────────────────

#[test]
fn join_brings_in_columns_from_a_second_file() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([
            {"join": {"source": {"path": "test_data/prices.csv"},
                      "left_on": ["id"], "how": "inner"}},
            {"sort": {"col": "id", "desc": false}}
        ]),
    );
    // prices.csv holds ids 1 and 2 only.
    assert_eq!(result["row_count"], 2);
    assert_eq!(cell(&result, 0, "value"), json!(100));
    assert_eq!(cell(&result, 1, "value"), json!(200));
}

#[test]
fn anti_join_keeps_the_rows_the_second_file_lacks() {
    let mut server = Server::new();
    let result = query(
        &mut server,
        json!([
            {"join": {"source": {"path": "test_data/prices.csv"},
                      "left_on": ["id"], "how": "anti"}}
        ]),
    );
    // sample.csv holds ids 1..=20, prices.csv ids 1 and 2.
    assert_eq!(result["row_count"], 18);
}

#[test]
fn diff_join_marks_each_row() {
    let mut server = Server::new();
    let result = call(
        &mut server,
        "tuitab_query",
        json!({"source": {"path": "test_data/diff_march.csv"}, "ops": [
            {"join": {"source": {"path": "test_data/diff_april.csv"},
                      "left_on": ["id"], "how": "diff"}}
        ]}),
    );
    let status: Vec<Value> = (0..5).map(|i| cell(&result, i, "_diff")).collect();
    assert_eq!(
        status,
        vec![json!("="), json!("="), json!("~"), json!("-"), json!("+")]
    );
    assert_eq!(cell(&result, 2, "amount_right"), json!(350));
}

// ── output shaping ──────────────────────────────────────────────────────────

#[test]
fn a_truncated_result_says_so_and_still_reports_the_real_count() {
    let mut server = Server::new();
    let result = call(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [], "output": {"limit": 2}}),
    );

    assert_eq!(result["returned"], 2);
    assert_eq!(result["row_count"], 20);
    assert_eq!(result["truncated"], true);
    assert!(result["note"].as_str().unwrap().contains("output.path"));
}

#[test]
fn several_pipelines_run_against_one_load() {
    let mut server = Server::new();
    let result = call(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "pipelines": [
            {"name": "headcount", "ops": [{"frequency": {"by": ["department"]}}]},
            {"name": "seniors", "ops": [{"filter": [{"col": "age", "op": "ge", "value": 45}]}]}
        ]}),
    );

    let results = result["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["name"], "headcount");
    assert_eq!(results[0]["row_count"], 4);
    assert_eq!(results[1]["name"], "seniors");
    assert_eq!(results[1]["row_count"], 4, "ages 45, 52, 47 and 50");
}

#[test]
fn output_path_writes_a_file_and_refuses_to_clobber_one() {
    let mut server = Server::new();
    let path = tmp("headcount.csv");
    let _ = std::fs::remove_file(&path);

    let ops = json!([{"frequency": {"by": ["department"]}}]);
    let result = call(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": ops,
               "output": {"path": path.to_string_lossy()}}),
    );

    assert_eq!(result["row_count"], 4);
    let written = std::fs::read_to_string(&path).unwrap();
    assert!(written.contains("Engineering"), "{}", written);

    // A second run must not quietly replace it.
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": ops,
               "output": {"path": path.to_string_lossy()}}),
    );
    assert!(message.contains("already exists"), "{}", message);

    // Nor with overwrite alone: replacing a file the user has is what --mcp-write is
    // for, the same as replacing a table.
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": ops,
               "output": {"path": path.to_string_lossy(), "overwrite": true}}),
    );
    assert!(message.contains("--mcp-write"), "{}", message);

    // With the flag it is planned, and the file only changes on apply.
    let mut on = writable_server();
    let planned = call(
        &mut on,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [{"limit": 1}],
               "output": {"path": path.to_string_lossy(), "overwrite": true}}),
    );
    assert!(
        planned["replaces"]["bytes_now"].as_u64().unwrap() > 0,
        "{}",
        planned
    );
    assert!(
        std::fs::read_to_string(&path)
            .unwrap()
            .contains("Engineering"),
        "a plan writes nothing"
    );

    let id = planned["plan_id"].as_str().unwrap().to_string();
    call(&mut on, "tuitab_write_apply", json!({"plan_id": id}));
    let after = std::fs::read_to_string(&path).unwrap();
    assert!(
        after.starts_with("id,name,age,salary,department"),
        "apply replaced it with the new result: {}",
        after
    );

    std::fs::remove_file(&path).unwrap();
}

// ── describe and jq ─────────────────────────────────────────────────────────

#[test]
fn describe_labels_the_population_standard_deviation_unambiguously() {
    let mut server = Server::new();
    let result = call(
        &mut server,
        "tuitab_describe",
        json!({"source": "test_data/sample.csv", "columns": ["age"]}),
    );

    let metrics: Vec<&str> = result["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r[0].as_str().unwrap())
        .collect();
    assert!(metrics.contains(&"stdev_pop"), "{:?}", metrics);
    assert!(
        !metrics.contains(&"stdev"),
        "the ambiguous label is gone: {:?}",
        metrics
    );

    let mean = result["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r[0] == "mean")
        .unwrap();
    assert_eq!(mean[1], "36.65");
}

#[test]
fn jq_walks_a_nested_document() {
    let mut server = Server::new();
    let result = call(
        &mut server,
        "tuitab_jq",
        json!({"source": "test_data/nested.json", "program": "map(.name)"}),
    );
    assert_eq!(result["result"], json!(["alpha", "beta"]));
}

#[test]
fn jq_on_a_csv_points_at_the_right_tool() {
    let mut server = Server::new();
    let message = call_expecting_failure(
        &mut server,
        "tuitab_jq",
        json!({"source": "test_data/sample.csv", "program": "."}),
    );
    assert!(message.contains("tuitab_query"), "{}", message);
}

// ── the wire ────────────────────────────────────────────────────────────────

/// The one test that pays for a process: it checks the newline framing and that
/// nothing but protocol messages reaches stdout.
#[test]
fn the_binary_speaks_the_protocol_over_stdio() {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new(env!("CARGO_BIN_EXE_tuitab"))
        .arg("--mcp")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let script = [
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}"#,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
        r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"tuitab_inspect","arguments":{"source":"test_data/sample.csv"}}}"#,
    ]
    .join("\n");

    child
        .stdin
        .take()
        .unwrap()
        .write_all(format!("{}\n", script).as_bytes())
        .unwrap();

    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "server exited with {}",
        output.status
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();

    // Three requests, three replies — the notification gets none.
    assert_eq!(lines.len(), 3, "unexpected stdout:\n{}", stdout);

    for line in &lines {
        let value: Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("stdout line is not a message: {} ({})", line, e));
        assert_eq!(value["jsonrpc"], "2.0");
    }

    let ids: Vec<u64> = lines
        .iter()
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["id"]
                .as_u64()
                .unwrap()
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

/// An unseeded random operation must report the seed it drew, or its result is
/// one nobody — including the model quoting it — can reproduce.
#[test]
fn an_unseeded_sample_reports_the_seed_it_used() {
    let mut server = Server::new();
    let first = query(&mut server, json!([{"sample": {"n": 5}}]));

    let seeds = first["seeds"]
        .as_array()
        .expect("the drawn seed must come back");
    assert_eq!(seeds.len(), 1);
    let seed = seeds[0]["seed"].as_u64().unwrap();

    // Feeding it back reproduces the same rows.
    let repeated = query(&mut server, json!([{"sample": {"n": 5, "seed": seed}}]));
    assert_eq!(first["rows"], repeated["rows"]);
}

#[test]
fn a_seeded_operation_reports_no_seed_because_none_was_drawn() {
    let mut server = Server::new();
    let result = query(&mut server, json!([{"sample": {"n": 5, "seed": 3}}]));
    assert!(
        result.get("seeds").is_none(),
        "nothing was drawn for the caller"
    );
}

// ── Databases ─────────────────────────────────────────────────────────────────────

/// A table with the things `create_table` cannot declare — a key, NOT NULL, a DEFAULT —
/// because those are exactly what the metadata tests are about. Plus a view.
fn db_fixture(name: &str) -> PathBuf {
    let path = tmp(name);
    let _ = std::fs::remove_file(&path);
    let ddl = "CREATE TABLE users (
                   id INTEGER PRIMARY KEY,
                   name TEXT NOT NULL,
                   score INTEGER,
                   note TEXT,
                   tier TEXT DEFAULT 'basic');
               INSERT INTO users VALUES (1, 'ann', 20, NULL, 'basic');
               INSERT INTO users VALUES (2, 'bob', 1000, '', 'gold');
               INSERT INTO users VALUES (3, 'cara', 3, 'hi', 'basic');
               CREATE TABLE other (k TEXT);
               INSERT INTO other VALUES ('untouched');
               CREATE VIEW big AS SELECT id, name FROM users WHERE score > 100;";
    if name.ends_with(".duckdb") {
        duckdb::Connection::open(&path)
            .unwrap()
            .execute_batch(ddl)
            .unwrap();
    } else {
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute_batch(ddl)
            .unwrap();
    }
    path
}

fn names_in(path: &PathBuf) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn.prepare("SELECT name FROM users ORDER BY id").unwrap();
    let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
    rows.map(|r| r.unwrap()).collect()
}

fn writable_server() -> Server {
    Server::writable()
}

#[test]
fn inspect_reports_declared_types_keys_and_defaults() {
    let mut server = Server::new();
    let db = db_fixture("meta.sqlite");
    let out = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"}}),
    );

    let cols = out["columns"].as_array().unwrap();
    assert_eq!(cols[0]["name"], "id");
    assert_eq!(cols[0]["declared"], "INTEGER");
    assert_eq!(cols[0]["primary_key"], true);
    assert_eq!(cols[1]["name"], "name");
    assert_eq!(cols[1]["not_null"], true);
    assert_eq!(cols[4]["default"], "'basic'");
    assert_eq!(out["writable"], true);
    assert!(out["create_sql"].as_str().unwrap().contains("CREATE TABLE"));
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn inspect_lists_row_counts_and_views() {
    let mut server = Server::new();
    let db = db_fixture("listing.sqlite");
    let out = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": db.to_string_lossy()}),
    );

    let listed = out["containers"].as_array().unwrap();
    let users = listed.iter().find(|c| c["name"] == "users").unwrap();
    assert_eq!(users["kind"], "table");
    assert_eq!(users["rows"], 3);
    assert_eq!(users["columns"], 5);

    let view = listed.iter().find(|c| c["name"] == "big").unwrap();
    assert_eq!(view["kind"], "view");
    assert_eq!(view["rows"], Value::Null, "a view is not counted");
    assert!(out["note"].as_str().unwrap().contains("view"));
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_view_reads_but_is_not_writable() {
    let mut server = Server::new();
    let db = db_fixture("view.sqlite");
    let out = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": {"path": db.to_string_lossy(), "container": "big"}}),
    );
    assert_eq!(out["row_count"], 1, "the view reads");
    assert_eq!(out["writable"], false);
    assert!(out["note"].as_str().unwrap().contains("view"));
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_database_without_a_container_points_at_container() {
    let mut server = Server::new();
    let db = db_fixture("nocontainer.sqlite");
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": db.to_string_lossy(), "ops": []}),
    );
    assert!(message.contains("pass 'container'"), "{}", message);
    assert!(message.contains("tuitab_inspect"), "{}", message);
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_numeric_filter_over_a_database_compares_numerically() {
    let mut server = Server::new();
    let db = db_fixture("numeric.sqlite");
    let out = call(
        &mut server,
        "tuitab_query",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "ops": [{"filter": [{"col": "score", "op": "gt", "value": 100}]}]}),
    );
    // Lexically "20" and "3" both beat "100"; numerically only 1000 does.
    assert_eq!(out["row_count"], 1);
    std::fs::remove_file(&db).unwrap();
}

// ── output.table ──────────────────────────────────────────────────────────────────

#[test]
fn output_table_names_the_table_and_can_sit_beside_others() {
    let mut server = writable_server();
    let db = db_fixture("output.sqlite");
    call(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [],
               "output": {"path": db.to_string_lossy(), "table": "imported"}}),
    );

    let listing = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": db.to_string_lossy()}),
    );
    let names: Vec<&str> = listing["containers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"imported"), "{:?}", names);
    assert!(
        names.contains(&"users"),
        "the neighbours survive: {:?}",
        names
    );
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn replacing_a_table_needs_overwrite_and_the_message_names_it() {
    let mut server = writable_server();
    let db = db_fixture("replace.sqlite");
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [],
               "output": {"path": db.to_string_lossy(), "table": "users"}}),
    );
    assert!(message.contains("'users' already exists"), "{}", message);
    assert!(message.contains("output.overwrite"), "{}", message);
    assert_eq!(names_in(&db), ["ann", "bob", "cara"], "nothing written");
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn an_invalid_table_name_is_refused() {
    let mut server = Server::new();
    let out = tmp("badname.sqlite");
    let _ = std::fs::remove_file(&out);
    for bad in ["", "sqlite_master"] {
        let message = call_expecting_failure(
            &mut server,
            "tuitab_query",
            json!({"source": "test_data/sample.csv", "ops": [],
                   "output": {"path": out.to_string_lossy(), "table": bad}}),
        );
        assert!(message.contains("output.table"), "{}", message);
    }
    assert!(!out.exists(), "nothing was created");
}

// ── The write tools exist only behind the flag ────────────────────────────────────

#[test]
fn the_write_tools_are_absent_unless_the_server_allows_writing() {
    let mut off = Server::new();
    let listed = send(
        &mut off,
        json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    );
    let names: Vec<&str> = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(!names.contains(&"tuitab_write"), "{:?}", names);

    let mut on = writable_server();
    let listed = send(
        &mut on,
        json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    );
    let names: Vec<&str> = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"tuitab_write"), "{:?}", names);
    assert!(names.contains(&"tuitab_write_apply"), "{:?}", names);
}

#[test]
fn calling_a_write_tool_without_the_flag_names_the_flag() {
    let mut server = Server::new();
    let message =
        call_expecting_failure(&mut server, "tuitab_write", json!({"source": "x.sqlite"}));
    assert!(message.contains("--mcp-write"), "{}", message);
}

#[test]
fn the_instructions_describe_writing_only_when_it_is_allowed() {
    let mut off = Server::new();
    let init = send(
        &mut off,
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
    );
    let text = init["result"]["instructions"].as_str().unwrap();
    assert!(!text.contains("tuitab_write_apply"));

    let mut on = writable_server();
    let init = send(
        &mut on,
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
    );
    let text = init["result"]["instructions"].as_str().unwrap();
    assert!(text.contains("tuitab_write_apply"), "{}", text);
}

// ── Writing, phase one: says what would happen ────────────────────────────────────

#[test]
fn a_planned_change_returns_the_sql_and_writes_nothing() {
    let mut server = writable_server();
    let db = db_fixture("plan.sqlite");
    let out = call(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "set": {"name": "ANN"},
               "where": [{"col": "id", "op": "eq", "value": 1}]}),
    );

    assert_eq!(out["updates"], 1);
    assert_eq!(out["rows_matched"], 1);
    assert!(out["plan_id"].as_str().unwrap().starts_with("write-"));
    let sql = out["statements"][0].as_str().unwrap();
    assert!(
        sql.starts_with("UPDATE \"users\" SET \"name\" = 'ANN'"),
        "{}",
        sql
    );
    // The rows as they stand, so a mis-aimed 'where' is visible rather than inferred.
    assert_eq!(out["affected_rows"]["row_count"], 1);
    assert_eq!(names_in(&db), ["ann", "bob", "cara"], "nothing written yet");
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_change_that_changes_nothing_offers_no_plan() {
    let mut server = writable_server();
    let db = db_fixture("noop.sqlite");
    let out = call(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "set": {"name": "ann"},
               "where": [{"col": "id", "op": "eq", "value": 1}]}),
    );
    assert_eq!(out["summary"], "no change");
    assert!(out.get("plan_id").is_none());
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn phase_one_refuses_what_cannot_work() {
    let mut server = writable_server();
    let db = db_fixture("refuse.sqlite");
    let src = json!({"path": db.to_string_lossy(), "container": "users"});

    let cases: Vec<(Value, &str)> = vec![
        (json!({"source": src, "delete": true}), "needs a 'where'"),
        (
            json!({"source": src, "set": {"name": "x"}, "delete": true}),
            "one change per call",
        ),
        (
            json!({"source": src, "set": {"nope": "x"}}),
            "No column named 'nope'",
        ),
        (
            json!({"source": src, "set": {"score": "not a number"}}),
            "not an integer",
        ),
        (
            json!({"source": {"path": db.to_string_lossy(), "container": "big"}, "set": {"name": "x"}}),
            "view",
        ),
        (
            json!({"source": "test_data/sample.csv", "set": {"name": "x"}}),
            ".sqlite or .duckdb",
        ),
        (
            json!({"source": db.to_string_lossy(), "set": {"name": "x"}}),
            "as 'container'",
        ),
        (json!({"source": src, "insert": [{"score": 1}]}), "NOT NULL"),
    ];
    for (args, expected) in cases {
        let message = call_expecting_failure(&mut server, "tuitab_write", args);
        assert!(
            message.to_lowercase().contains(&expected.to_lowercase()),
            "expected {:?} in {:?}",
            expected,
            message
        );
    }
    assert_eq!(names_in(&db), ["ann", "bob", "cara"]);
    std::fs::remove_file(&db).unwrap();
}

// ── Writing, phase two: runs exactly that plan ────────────────────────────────────

fn plan_then_apply(server: &mut Server, args: Value) -> Value {
    let planned = call(server, "tuitab_write", args);
    let id = planned["plan_id"]
        .as_str()
        .expect("a plan was made")
        .to_string();
    call(server, "tuitab_write_apply", json!({"plan_id": id}))
}

#[test]
fn applying_a_plan_writes_exactly_those_rows() {
    let mut server = writable_server();
    let db = db_fixture("apply.sqlite");
    let out = plan_then_apply(
        &mut server,
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "set": {"name": "ANN"},
               "where": [{"col": "id", "op": "eq", "value": 1}]}),
    );
    assert_eq!(out["applied"], true);
    assert_eq!(names_in(&db), ["ANN", "bob", "cara"]);
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_delete_removes_only_the_matched_rows() {
    let mut server = writable_server();
    let db = db_fixture("delete.sqlite");
    plan_then_apply(
        &mut server,
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "delete": true,
               "where": [{"col": "name", "op": "eq", "value": "bob"}]}),
    );
    assert_eq!(names_in(&db), ["ann", "cara"]);
    let conn = rusqlite::Connection::open(&db).unwrap();
    let other: String = conn
        .query_row("SELECT k FROM other", [], |r| r.get(0))
        .unwrap();
    assert_eq!(other, "untouched", "the neighbouring table is not touched");
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn an_insert_leaves_a_defaulted_column_to_the_schema_and_writes_null_otherwise() {
    let mut server = writable_server();
    let db = db_fixture("insert.sqlite");
    plan_then_apply(
        &mut server,
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "insert": [{"name": "dave", "note": null}]}),
    );

    let conn = rusqlite::Connection::open(&db).unwrap();
    let (score, note, tier): (Option<i64>, Option<String>, Option<String>) = conn
        .query_row(
            "SELECT score, note, tier FROM users WHERE name = 'dave'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(score, None, "no DEFAULT to fall back on, so NULL");
    assert_eq!(note, None, "a JSON null is a real NULL");
    assert_eq!(
        tier.as_deref(),
        Some("basic"),
        "a column with a DEFAULT is left out of the statement so the DEFAULT runs"
    );

    // An explicit null on a defaulted column is a value the row does not have, so the
    // DEFAULT beats it.  Documented, not silent: forcing NULL there means insert, then
    // set.
    plan_then_apply(
        &mut server,
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "insert": [{"name": "erin", "tier": null}]}),
    );
    let tier: Option<String> = conn
        .query_row("SELECT tier FROM users WHERE name = 'erin'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(tier.as_deref(), Some("basic"));
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn alter_adds_drops_and_renames_columns() {
    let mut server = writable_server();
    let db = db_fixture("alter.sqlite");
    plan_then_apply(
        &mut server,
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "alter": {"add": [{"name": "level", "type": "integer"}],
                         "drop": ["note"],
                         "rename": {"tier": "plan"}}}),
    );

    let conn = rusqlite::Connection::open(&db).unwrap();
    let mut stmt = conn
        .prepare("SELECT name, type FROM pragma_table_info('users') ORDER BY cid")
        .unwrap();
    let cols: Vec<(String, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let names: Vec<&str> = cols.iter().map(|(n, _)| n.as_str()).collect();
    assert!(names.contains(&"level"), "{:?}", names);
    assert!(names.contains(&"plan"), "{:?}", names);
    assert!(!names.contains(&"note"), "{:?}", names);
    assert_eq!(
        cols.iter().find(|(n, _)| n == "level").unwrap().1,
        "INTEGER"
    );
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_plan_cannot_be_applied_twice_and_an_unknown_id_is_refused() {
    let mut server = writable_server();
    let db = db_fixture("once.sqlite");
    let planned = call(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "set": {"name": "ANN"}, "where": [{"col": "id", "op": "eq", "value": 1}]}),
    );
    let id = planned["plan_id"].as_str().unwrap().to_string();

    let unknown = call_expecting_failure(
        &mut server,
        "tuitab_write_apply",
        json!({"plan_id": "write-99"}),
    );
    assert!(unknown.contains("write-99"), "{}", unknown);

    call(
        &mut server,
        "tuitab_write_apply",
        json!({"plan_id": id.clone()}),
    );
    let again = call_expecting_failure(&mut server, "tuitab_write_apply", json!({"plan_id": id}));
    // A plan this server did hand out is a different answer from one it never made.
    assert!(again.contains("no longer valid"), "{}", again);
    assert_eq!(
        names_in(&db),
        ["ANN", "bob", "cara"],
        "applied exactly once"
    );
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_second_plan_replaces_the_first() {
    let mut server = writable_server();
    let db = db_fixture("replaceplan.sqlite");
    let src = json!({"path": db.to_string_lossy(), "container": "users"});
    let first = call(
        &mut server,
        "tuitab_write",
        json!({"source": src, "set": {"name": "ONE"}, "where": [{"col": "id", "op": "eq", "value": 1}]}),
    );
    let first_id = first["plan_id"].as_str().unwrap().to_string();
    call(
        &mut server,
        "tuitab_write",
        json!({"source": src, "set": {"name": "TWO"}, "where": [{"col": "id", "op": "eq", "value": 2}]}),
    );

    let stale = call_expecting_failure(
        &mut server,
        "tuitab_write_apply",
        json!({"plan_id": first_id}),
    );
    assert!(stale.contains("waiting to be applied"), "{}", stale);
    assert_eq!(names_in(&db), ["ann", "bob", "cara"]);
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_row_changed_between_the_phases_stops_the_write() {
    let mut server = writable_server();
    let db = db_fixture("drift.sqlite");
    let planned = call(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "set": {"name": "ANN"}, "where": [{"col": "id", "op": "eq", "value": 1}]}),
    );
    let id = planned["plan_id"].as_str().unwrap().to_string();

    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("UPDATE users SET name = 'elsewhere' WHERE id = 1")
        .unwrap();

    let message = call_expecting_failure(&mut server, "tuitab_write_apply", json!({"plan_id": id}));
    assert!(
        message.contains("changed since it was opened"),
        "{}",
        message
    );
    assert_eq!(
        names_in(&db),
        ["elsewhere", "bob", "cara"],
        "nothing of ours was written"
    );
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn two_writes_in_one_session_both_land_and_inspect_sees_them() {
    let mut server = writable_server();
    let db = db_fixture("twice.sqlite");
    let src = json!({"path": db.to_string_lossy(), "container": "users"});

    plan_then_apply(
        &mut server,
        json!({"source": src, "set": {"name": "ONE"}, "where": [{"col": "id", "op": "eq", "value": 1}]}),
    );
    plan_then_apply(
        &mut server,
        json!({"source": src, "set": {"name": "TWO"}, "where": [{"col": "id", "op": "eq", "value": 2}]}),
    );
    assert_eq!(names_in(&db), ["ONE", "TWO", "cara"]);

    // The cache must not answer from a frame that predates the writes.
    let out = call(
        &mut server,
        "tuitab_query",
        json!({"source": src, "ops": [{"filter": [{"col": "name", "op": "eq", "value": "ONE"}]}]}),
    );
    assert_eq!(out["row_count"], 1);
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn duckdb_writes_the_same_way() {
    let mut server = writable_server();
    let db = db_fixture("write.duckdb");
    plan_then_apply(
        &mut server,
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "set": {"name": "ANN"},
               "where": [{"col": "id", "op": "eq", "value": 1}]}),
    );

    let (df, _) = tuitab::data::io::load_duckdb_table_full(&db, "users").unwrap();
    assert_eq!(df.get_physical(0, 1), "ANN");
    std::fs::remove_file(&db).unwrap();
}

/// The same column, read through a table and through a view over it, has to answer a
/// filter the same way. Typing used to land only on the row-addressable path.
#[test]
fn a_filter_over_a_view_compares_the_same_way_as_over_the_table() {
    for name in ["viewfilter.sqlite", "viewfilter.duckdb"] {
        let mut server = Server::new();
        let db = db_fixture(name);
        let via_table = call(
            &mut server,
            "tuitab_query",
            json!({"source": {"path": db.to_string_lossy(), "container": "users"},
                   "ops": [{"filter": [{"col": "score", "op": "gt", "value": 100}]}]}),
        );
        assert_eq!(via_table["row_count"], 1, "{}", name);

        // `big` is a view; its `id` is the same declared INTEGER.
        let via_view = call(
            &mut server,
            "tuitab_query",
            json!({"source": {"path": db.to_string_lossy(), "container": "big"},
                   "ops": [{"filter": [{"col": "id", "op": "gt", "value": 1}]}]}),
        );
        assert_eq!(via_view["row_count"], 1, "{}: the view must type too", name);
        std::fs::remove_file(&db).unwrap();
    }
}

#[test]
fn writing_into_a_database_invalidates_the_cached_frame_of_it() {
    let mut server = writable_server();
    let db = db_fixture("output-cache.sqlite");

    // Read a table first, so the server holds a frame of this file.
    let before = call(
        &mut server,
        "tuitab_query",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"}, "ops": []}),
    );
    assert_eq!(before["row_count"], json!(3));

    // Write a second table into the same file…
    call(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [],
               "output": {"path": db.to_string_lossy(), "table": "imported"}}),
    );

    // …and the very next read must see it rather than the snapshot from before.
    let listing = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": db.to_string_lossy()}),
    );
    let names: Vec<&str> = listing["containers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"imported"), "{:?}", names);
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_duckdb_file_named_db_is_read_as_duckdb() {
    let mut server = Server::new();
    let path = tmp("mcp-disguised.db");
    let _ = std::fs::remove_file(&path);
    {
        let conn = duckdb::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER, name TEXT);
             INSERT INTO users VALUES (1, 'ann'), (2, 'bob');
             CHECKPOINT;",
        )
        .unwrap();
    }

    let listing = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": path.to_string_lossy()}),
    );
    let names: Vec<&str> = listing["containers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["users"]);

    let rows = call(
        &mut server,
        "tuitab_query",
        json!({"source": {"path": path.to_string_lossy(), "container": "users"}, "ops": []}),
    );
    assert_eq!(rows["row_count"], json!(2));
    std::fs::remove_file(&path).unwrap();
}

// ── The write gate on output.path ─────────────────────────────────────────────────

/// The gate is on the table, not the file: a second table can join a database the
/// server itself just made, while replacing one somebody has needs the flag.
#[test]
fn adding_a_table_needs_no_flag_and_replacing_one_does() {
    let mut off = Server::new();

    // A database built up over two writes, with the flag off throughout.
    let fresh = tmp("gate-fresh.sqlite");
    let _ = std::fs::remove_file(&fresh);
    for table in ["first", "second"] {
        call(
            &mut off,
            "tuitab_query",
            json!({"source": "test_data/sample.csv", "ops": [],
                   "output": {"path": fresh.to_string_lossy(), "table": table}}),
        );
    }
    let listing = call(
        &mut off,
        "tuitab_inspect",
        json!({"source": fresh.to_string_lossy()}),
    );
    let names: Vec<&str> = listing["containers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(
        names.contains(&"first") && names.contains(&"second"),
        "both tables must be there: {:?}",
        names
    );

    // A table added beside somebody else's leaves theirs alone.
    let db = db_fixture("gate-existing.sqlite");
    call(
        &mut off,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [],
               "output": {"path": db.to_string_lossy(), "table": "imported"}}),
    );
    assert_eq!(names_in(&db), ["ann", "bob", "cara"], "users untouched");

    // Replacing one is refused in the same breath as the flag that would allow it,
    // and asking for overwrite does not get round it.
    let message = call_expecting_failure(
        &mut off,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [],
               "output": {"path": db.to_string_lossy(), "table": "users", "overwrite": true}}),
    );
    assert!(message.contains("--mcp-write"), "{}", message);
    assert_eq!(names_in(&db), ["ann", "bob", "cara"], "nothing written");

    // And with the flag it is planned, not done: the one destructive thing this path
    // can do goes through the same handshake as any other write.
    let mut on = writable_server();
    let planned = call(
        &mut on,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [],
               "output": {"path": db.to_string_lossy(), "table": "users", "overwrite": true}}),
    );
    assert_eq!(planned["replaces"]["rows_now"], json!(3), "{}", planned);
    assert_eq!(planned["replaces"]["rows_after"], json!(20), "{}", planned);
    assert!(
        planned["statements"][0].as_str().unwrap().contains("DROP"),
        "{}",
        planned
    );
    assert_eq!(
        names_in(&db),
        ["ann", "bob", "cara"],
        "a plan writes nothing"
    );

    let id = planned["plan_id"].as_str().unwrap().to_string();
    call(&mut on, "tuitab_write_apply", json!({"plan_id": id}));
    assert_ne!(
        names_in(&db),
        ["ann", "bob", "cara"],
        "users replaced on apply"
    );
    std::fs::remove_file(&db).unwrap();
    std::fs::remove_file(&fresh).unwrap();
}

#[test]
fn a_query_cannot_write_into_the_database_it_read() {
    let mut server = writable_server();
    let db = db_fixture("gate-samefile.sqlite");
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"}, "ops": [],
               "output": {"path": db.to_string_lossy(), "table": "copy", "overwrite": true}}),
    );
    assert!(message.contains("source this query read"), "{}", message);
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn overwriting_a_view_is_refused_in_words() {
    let mut server = writable_server();
    let db = db_fixture("gate-view.sqlite");
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [],
               "output": {"path": db.to_string_lossy(), "table": "big", "overwrite": true}}),
    );
    assert!(message.contains("is a view"), "{}", message);
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_table_name_with_spaces_around_it_is_trimmed() {
    let mut server = Server::new();
    let out = tmp("trimmed.sqlite");
    let _ = std::fs::remove_file(&out);
    call(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [],
               "output": {"path": out.to_string_lossy(), "table": "  spaced  "}}),
    );
    let conn = rusqlite::Connection::open(&out).unwrap();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .unwrap();
    let names: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(names, ["spaced"]);
    std::fs::remove_file(&out).unwrap();
}

// ── The pending plan ──────────────────────────────────────────────────────────────

#[test]
fn planning_again_retires_the_previous_plan_and_says_so() {
    let mut server = writable_server();
    let db = db_fixture("pending-replace.sqlite");
    let first = call(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "set": {"note": "one"},
               "where": [{"col": "name", "op": "eq", "value": "ann"}]}),
    );
    let first_id = first["plan_id"].as_str().unwrap().to_string();

    let second = call(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "set": {"note": "two"},
               "where": [{"col": "name", "op": "eq", "value": "bob"}]}),
    );
    let note = second["note"].as_str().unwrap();
    assert!(
        note.contains(&first_id),
        "the retired plan is not named: {}",
        note
    );

    let message = call_expecting_failure(
        &mut server,
        "tuitab_write_apply",
        json!({"plan_id": first_id}),
    );
    // A newer plan is waiting, so the refusal names both.
    assert!(message.contains(&first_id), "{}", message);
    assert!(message.contains("waiting to be applied"), "{}", message);
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn a_failed_second_plan_does_not_leave_the_first_applicable() {
    let mut server = writable_server();
    let db = db_fixture("pending-failed.sqlite");
    let first = call(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "set": {"note": "one"},
               "where": [{"col": "name", "op": "eq", "value": "ann"}]}),
    );
    let first_id = first["plan_id"].as_str().unwrap().to_string();

    // A second call that fails must still retire the first: the model has moved on.
    call_expecting_failure(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "set": {"nosuchcolumn": "x"}}),
    );

    let message = call_expecting_failure(
        &mut server,
        "tuitab_write_apply",
        json!({"plan_id": first_id}),
    );
    assert!(message.contains("no longer valid"), "{}", message);
    assert_eq!(names_in(&db), ["ann", "bob", "cara"], "nothing was written");
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn alter_refuses_a_word_it_does_not_know() {
    let mut server = writable_server();
    let db = db_fixture("alter-typo.sqlite");
    for spec in [
        json!({"retype": {"score": "float"}}),
        json!({"reorder": ["name", "id"]}),
        json!(true),
        json!({}),
    ] {
        let message = call_expecting_failure(
            &mut server,
            "tuitab_write",
            json!({"source": {"path": db.to_string_lossy(), "container": "users"},
                   "alter": spec}),
        );
        assert!(
            message.contains("alter") || message.contains("not something alter does"),
            "{} → {}",
            spec,
            message
        );
    }
    std::fs::remove_file(&db).unwrap();
}

// ── What the model is told when something fails ───────────────────────────────────

#[test]
fn an_engine_error_says_what_was_being_done() {
    let mut server = writable_server();
    let db = db_fixture("errors-context.sqlite");

    // A polars failure inside `insert` used to arrive as a bare shape complaint.
    let message = call_expecting_failure(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "insert": [{"name": "eve", "score": "not a number"}]}),
    );
    assert!(!message.is_empty());

    // A load failure names the file.
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": {"path": "test_data/sample.csv", "format": "parquet"}, "ops": []}),
    );
    assert!(message.contains("sample.csv"), "{}", message);

    // A jq failure says it was jq.
    let message = call_expecting_failure(
        &mut server,
        "tuitab_jq",
        json!({"source": "test_data/nested.json", "program": "this is not jq ("}),
    );
    assert!(message.contains("jq"), "{}", message);

    // And a refusal from the write engine still arrives in its own words.
    let message = call_expecting_failure(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "big"},
               "set": {"name": "x"}}),
    );
    assert!(message.contains("view"), "{}", message);
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn an_unwritable_output_is_refused_before_the_pipeline_runs() {
    let mut server = Server::new();
    // The ops name a column that does not exist, so if the pipeline ran first its
    // error would be the one that came back.
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv",
               "ops": [{"sort": {"col": "nosuchcolumn"}}],
               "output": {"path": tmp("out.xyz").to_string_lossy()}}),
    );
    assert!(message.contains("no writer for '.xyz'"), "{}", message);

    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv", "ops": [],
               "output": {"path": tmp("no/such/dir/out.csv").to_string_lossy()}}),
    );
    assert!(message.contains("no directory"), "{}", message);
}

#[test]
fn a_caller_can_ask_for_more_statements_than_the_default() {
    let mut server = writable_server();
    let db = tmp("show-statements.sqlite");
    let _ = std::fs::remove_file(&db);
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..60 {
            tx.execute(
                "INSERT INTO users (id, name) VALUES (?1, ?2)",
                rusqlite::params![i, format!("n{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    // One statement per row: each row gets a distinct value.
    let rows: Vec<serde_json::Value> = (0..60)
        .map(|i| json!({"id": 1000 + i, "name": format!("x{}", i)}))
        .collect();

    let default = call(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "insert": rows}),
    );
    assert_eq!(default["statements"].as_array().unwrap().len(), 20);
    assert_eq!(default["statements_total"], json!(60));
    assert_eq!(default["statements_not_shown"], json!(40));

    let more = call(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "insert": rows, "show_statements": 50}),
    );
    assert_eq!(more["statements"].as_array().unwrap().len(), 50);
    assert_eq!(more["statements_not_shown"], json!(10));

    // Asking for more than the ceiling gets the ceiling, not everything.
    let capped = call(
        &mut server,
        "tuitab_write",
        json!({"source": {"path": db.to_string_lossy(), "container": "users"},
               "insert": rows, "show_statements": 5000}),
    );
    assert_eq!(capped["statements"].as_array().unwrap().len(), 60);
    std::fs::remove_file(&db).unwrap();
}

#[test]
fn two_sources_read_in_turn_do_not_evict_each_other() {
    let mut server = Server::new();
    let a = tmp("cache-a.csv");
    let b = tmp("cache-b.csv");
    std::fs::write(&a, "x\n1\n").unwrap();
    std::fs::write(&b, "y\n2\n").unwrap();

    for _ in 0..3 {
        let ra = call(
            &mut server,
            "tuitab_query",
            json!({"source": a.to_string_lossy(), "ops": []}),
        );
        assert_eq!(ra["columns"][0]["name"], json!("x"));
        let rb = call(
            &mut server,
            "tuitab_query",
            json!({"source": b.to_string_lossy(), "ops": []}),
        );
        assert_eq!(rb["columns"][0]["name"], json!("y"));
    }
    assert_eq!(server.cache.len(), 2, "each source should hold a slot");

    // A change to one of them is still seen, cache or no cache.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    std::fs::write(&a, "x\n1\n2\n3\n").unwrap();
    let ra = call(
        &mut server,
        "tuitab_query",
        json!({"source": a.to_string_lossy(), "ops": []}),
    );
    assert_eq!(ra["row_count"], json!(3));

    std::fs::remove_file(&a).unwrap();
    std::fs::remove_file(&b).unwrap();
}

// ── What the report of 2026-08-12 found ───────────────────────────────────────────

/// A copy is a whole operation: no ops at all is the table as it stands, and used to
/// need a made-up `limit` big enough not to cut anything.
#[test]
fn a_query_with_no_ops_is_the_table_itself() {
    let mut server = Server::new();
    let out = tmp("copy-no-ops.sqlite");
    let _ = std::fs::remove_file(&out);
    let written = call(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv",
               "output": {"path": out.to_string_lossy(), "table": "people"}}),
    );
    assert_eq!(written["row_count"], json!(20));
    // A database is written for the next query, not for a reader, and saying otherwise
    // makes a perfectly good table look unfit to query.
    assert!(
        written.get("note").is_none(),
        "a db write must not claim formatting: {}",
        written
    );
    std::fs::remove_file(&out).unwrap();

    // A file a person opens still says so.
    let csv = tmp("copy-no-ops.csv");
    let written = call(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv",
               "output": {"path": csv.to_string_lossy()}}),
    );
    assert!(written["note"].as_str().unwrap().contains("formatted"));
    std::fs::remove_file(&csv).unwrap();
}

/// One broken question must not throw away the answers beside it — the whole point of
/// asking several in one call is that the file is read once.
#[test]
fn a_failing_pipeline_does_not_take_the_others_with_it() {
    let mut server = Server::new();
    let out = call(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv",
               "pipelines": [
                   {"name": "ok", "ops": [{"frequency": {"by": "department"}}]},
                   {"name": "bad", "ops": [{"group_by": {"by": ["department"]}}]}]}),
    );
    let results = out["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["name"], json!("ok"));
    assert!(results[0]["rows"].as_array().unwrap().len() > 1);
    assert_eq!(results[1]["name"], json!("bad"));
    assert!(
        results[1]["error"].as_str().unwrap().contains("aggregate"),
        "{}",
        results[1]
    );

    // Every question failing is still a failed call.
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv",
               "pipelines": [{"name": "bad", "ops": [{"group_by": {"by": ["department"]}}]}]}),
    );
    assert!(message.contains("aggregate"), "{}", message);
}

/// A key nobody reads is a key the caller got wrong; ignoring it sends them looking in
/// the wrong place when the failure surfaces a step later.
#[test]
fn a_misspelt_field_is_named_rather_than_ignored() {
    let mut server = Server::new();
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv",
               "ops": [{"group_by": {"by": ["department"],
                                     "aggregate": [{"col": "salary", "fn": "sum"}]}}]}),
    );
    assert!(message.contains("'agg'"), "{}", message);

    // And a list where an object belongs is a mistake of shape, not of field.
    let message = call_expecting_failure(
        &mut server,
        "tuitab_query",
        json!({"source": "test_data/sample.csv",
               "ops": [{"compute": [{"name": "x", "expr": "salary * 2"}]}]}),
    );
    assert!(message.contains("not a list"), "{}", message);
}

/// A listing that answers `rows: null, columns: 0` costs a call per sheet to learn what
/// the sheet even is.
#[test]
fn inspecting_a_spreadsheet_gives_the_size_of_every_sheet() {
    let mut server = Server::new();
    let xlsx = tmp("sizes.xlsx");
    let _ = std::fs::remove_file(&xlsx);
    let df =
        tuitab::data::io::load_file(std::path::Path::new("test_data/sample.csv"), None).unwrap();
    tuitab::data::io::save_file_as(
        &df,
        None,
        &xlsx,
        tuitab::data::io::doc_io::Shape::Records,
        "Sheet1",
    )
    .unwrap();

    let listing = call(
        &mut server,
        "tuitab_inspect",
        json!({"source": xlsx.to_string_lossy()}),
    );
    let sheet = &listing["containers"][0];
    assert_eq!(sheet["rows"], json!(20), "{}", listing);
    assert_eq!(sheet["columns"], json!(5), "{}", listing);
    std::fs::remove_file(&xlsx).unwrap();
}
