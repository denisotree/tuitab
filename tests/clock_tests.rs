//! `now()` and `today()` against the clock — in a binary of their own, since the test
//! sets the process's time zone.

use serde_json::{json, Value};
use tuitab::mcp::{handle_message, Server};

/// Every datetime column is held in UTC, so the clock the expressions read is UTC too:
/// off UTC, a local clock would compare an event from an hour ago as one to come.
#[test]
fn now_and_today_read_the_utc_clock() {
    std::env::set_var("TZ", "Pacific/Kiritimati"); // UTC+14
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("clock-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("one.csv");
    std::fs::write(&path, "a\n1\n").unwrap();
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "tuitab_query", "arguments": {"source": path, "ops": [
            {"compute": {"name": "n", "expr": "text(now())"}},
            {"compute": {"name": "d", "expr": "text(today())"}}]}}});
    let out: Value =
        handle_message(&mut Server::new(), &message.to_string()).unwrap()["result"].clone();
    let row = &out["structuredContent"]["rows"][0];
    let utc = chrono::Utc::now().naive_utc();
    let now =
        chrono::NaiveDateTime::parse_from_str(&row[1].as_str().unwrap()[..19], "%Y-%m-%d %H:%M:%S")
            .unwrap_or_else(|e| panic!("{}: {}", e, out));
    assert!(
        (now - utc).num_seconds().abs() < 60,
        "now() {} against UTC {}",
        now,
        utc
    );
    assert_eq!(row[2], json!(utc.date().to_string()), "{}", out);
}
