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
//! | [`mod@column`] | [`column::ColumnMeta`] — per-column metadata (type, width, aggregators) |
//! | [`expression`] | Expression AST and recursive-descent parser for computed columns and row filters |
//! | [`aggregator`] | [`aggregator::AggregatorKind`] enum and compatibility rules |
//! | [`sort`] | Sort-by-column implementation using Polars `arg_sort` |
//! | [`swap`] | Serialize/deserialize a `DataFrame` to disk to free memory when sheets are stacked |

pub mod aggregator;
pub mod async_loader;
pub mod column;
pub mod dataframe;
pub mod expression;
pub mod io;
pub mod loader;
pub mod sort;
pub mod swap;
