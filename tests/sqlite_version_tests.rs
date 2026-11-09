//! The SQLite compiled into tuitab (rusqlite's `bundled` feature).

/// SQLite before 3.54.0 can return uninitialised page-cache memory when a database is
/// opened beside a `-wal` file with a different page size (CVE-2026-119375,
/// RUSTSEC-2026-0459). tuitab opens databases the user points it at, so a crafted
/// database and WAL pair could leak process memory into query results.
#[test]
fn the_bundled_sqlite_is_past_the_wal_page_size_leak() {
    assert!(
        rusqlite::version_number() >= 3_054_000,
        "bundled SQLite {} is affected by RUSTSEC-2026-0459",
        rusqlite::version()
    );
}
