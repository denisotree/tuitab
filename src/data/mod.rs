//! Data model and I/O layer for tuitab.
//!
//! This module contains everything needed to load, store, and manipulate tabular data:
//!
//! | Sub-module | Responsibility |
//! |---|---|
//! | [`io`] | Format-aware file loader and saver (CSV, JSON, Parquet, Excel, SQLite, directory) |
//! | [`loader`] | Low-level CSV/TSV reader with auto-delimiter detection |
//! | [`async_loader`] | Background thread loader for files larger than 10 MB |
//! | [`dataframe`] | [`dataframe::DataFrame`] — Polars-backed in-memory store with view state |
//! | [`dedup`] | Finding duplicate rows and keeping one per group, with a seeded random keeper |
//! | [`describe`] | Per-column statistical profile, shared by the `I` key and the MCP server |
//! | [`doc`] | [`doc::Doc`] — document tree shared by JSON/JSONL/YAML/TOML, source of truth for those formats |
//! | [`mod@column`] | [`column::ColumnMeta`] — per-column metadata (type, width, aggregators) |
//! | [`expression`] | Expression AST and recursive-descent parser for computed columns and row filters |
//! | [`filter`] | Row selection shared by the TUI's typed expressions and the MCP server's structured predicates |
//! | [`profile`] | What a column's values say about its type — null tokens, epochs, zones — for a loaded frame and a scan alike |
//! | [`group`] | Group-by and grand totals — exactly the requested aggregates, unlike a frequency table |
//! | [`aggregator`] | [`aggregator::AggregatorKind`] enum and compatibility rules |
//! | [`sort`] | Sort-by-column implementation using Polars `arg_sort` |
//! | [`transpose`] | Standing rows on end — one row, or the whole table, with the inverse built in |
//! | [`typed_value`] | Checking one typed-in value against a column's type, before it is stored |
//! | [`swap`] | Serialize/deserialize a `DataFrame` to disk to free memory when sheets are stacked |
//! | [`query`] | jq programs over a document — the result is just another document |
//! | [`window`] | Window functions — rank, running total, lag/lead, partition shares |
//! | [`view`] | [`view::View`] — projection of a `Doc` subtree into a table, and the cell→node mapping behind editing |

pub mod aggregator;
pub mod async_loader;
pub mod column;
pub mod dataframe;
pub mod dedup;
pub mod describe;
pub mod doc;
pub mod expression;
pub mod filter;
pub mod group;
pub mod io;
pub mod join;
pub mod json_cols;
pub mod loader;
pub mod profile;
pub mod query;
pub mod sort;
pub mod swap;
pub mod transpose;
pub mod typed_value;
pub mod unpivot;
pub mod view;
pub mod window;

/// A Polars column reference that is always a name, never a pattern.
///
/// [`polars::lazy::dsl::col`] treats an argument that starts with `^` and ends
/// with `$` as a regular expression selecting every matching column — a
/// convenience for hand-written queries and a trap for us, because our column
/// names come out of the user's file. A CSV whose header row contains
/// `^total$` would have that column silently select *nothing*, which turns a
/// frequency table into "unable to find column Count" and a pivot into
/// "not found".
///
/// The pattern handling lives in `col()`, not in the expression it builds, so
/// constructing the expression directly keeps the name a name.
pub fn column_expr(name: &str) -> polars::prelude::Expr {
    polars::prelude::Expr::Column(name.into())
}

/// Naive datetime format strings tried when parsing a string to `NaiveDateTime`.
/// Fractional-seconds variants come first so strings with microseconds don't
/// fail the plain `%H:%M:%S` format before reaching the `%.f` one.
pub const DATETIME_FORMATS: &[&str] = &[
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S",
];

/// Run a lazy plan with Polars' streaming engine: a scan of a file larger than memory
/// is read in pieces, and a plan over a table already in memory gives the same answer.
pub fn collect(
    lf: polars::prelude::LazyFrame,
) -> polars::prelude::PolarsResult<polars::prelude::DataFrame> {
    lf.collect_with_engine(polars::prelude::Engine::Streaming)
}
