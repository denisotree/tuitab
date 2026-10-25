//! Resolving a `source` argument into a loaded [`DataFrame`].
//!
//! Every tool takes the same source shape — a path, plus an optional container
//! for the formats that hold more than one table (Excel sheets, SQLite and
//! DuckDB tables).  Loading goes through [`crate::data::io`], so the MCP layer
//! inherits tuitab's format support and type inference rather than restating it.

use crate::data::dataframe::DataFrame;
use crate::data::doc::{Doc, Format};
use crate::data::io;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// A source as the model described it.
#[derive(Clone, PartialEq, Eq)]
pub struct Source {
    pub path: PathBuf,
    /// Sheet name (Excel) or table name (SQLite/DuckDB).
    pub container: Option<String>,
    pub delimiter: Option<u8>,
    /// Overrides the extension, so a `.conf` file can be read as YAML.
    pub format: Option<String>,
}

impl Source {
    /// Read a source out of tool arguments.  Accepts either the object form or a
    /// bare string path, because a model that only needs a path will write one.
    pub fn from_json(value: &Value) -> Result<Self, String> {
        if let Some(path) = value.as_str() {
            return Ok(Self {
                path: PathBuf::from(path),
                container: None,
                delimiter: None,
                format: None,
            });
        }

        let obj = value.as_object().ok_or_else(|| {
            "'source' must be an object with a 'path', or a path string".to_string()
        })?;

        let path = obj
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| "'source' requires a 'path'".to_string())?;

        let delimiter = match obj.get("delimiter").and_then(Value::as_str) {
            Some(d) => {
                let mut chars = d.chars();
                match (chars.next(), chars.next()) {
                    (Some(c), None) if c.is_ascii() => Some(c as u8),
                    _ => {
                        return Err(format!(
                            "'delimiter' must be a single ASCII character, got {:?}",
                            d
                        ))
                    }
                }
            }
            None => None,
        };

        Ok(Self {
            path: PathBuf::from(path),
            container: obj
                .get("container")
                .and_then(Value::as_str)
                .map(str::to_string),
            delimiter,
            format: obj
                .get("format")
                .and_then(Value::as_str)
                .map(|s| s.to_lowercase()),
        })
    }

    fn extension(&self) -> String {
        self.format.clone().unwrap_or_else(|| {
            self.path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or_default()
                .to_lowercase()
        })
    }
}

/// How many frames the server keeps.  Enough for a comparison between two files and
/// the odd lookup beside it; small enough that the memory is bounded by what a handful
/// of reads cost.
const CACHE_ENTRIES: usize = 4;

/// One cached source.  See [`crate::mcp::Server::cache`].
pub struct Cached {
    source: Source,
    stamp: Stamp,
    df: DataFrame,
}

/// What is compared to decide whether a file is still the file that was read.
///
/// Not just the modification time of the path: both engines commit through a
/// write-ahead log beside the database, and a commit by another process lands there
/// without touching the main file until a checkpoint.  A cache keyed on the main file
/// alone would keep answering with pre-commit rows indefinitely.  Size goes in too — it
/// costs nothing and catches a same-second rewrite.
type Stamp = Vec<(Option<SystemTime>, Option<u64>)>;

fn stamp_of(path: &Path) -> Stamp {
    let one = |p: &Path| match std::fs::metadata(p) {
        Ok(m) => (m.modified().ok(), Some(m.len())),
        Err(_) => (None, None),
    };
    let name = path.to_string_lossy();
    let mut out = vec![one(path)];
    for extra in [
        format!("{}-wal", name),
        format!("{}.wal", name),
        format!("{}-shm", name),
    ] {
        out.push(one(Path::new(&extra)));
    }
    out
}

/// Load `source`, reusing the cached frame when the file has not changed.
pub fn load(server: &mut super::Server, source: &Source) -> Result<DataFrame, String> {
    let stamp = stamp_of(&source.path);
    // A file whose metadata cannot be read is a miss, not a match: two unreadable
    // stamps are equal to each other and say nothing about the contents.
    let readable = stamp[0].0.is_some();

    if readable {
        if let Some(i) = server
            .cache
            .iter()
            .position(|c| c.source == *source && c.stamp == stamp)
        {
            // To the front, so the entry that keeps being asked for is the last to go.
            let hit = server.cache.remove(i);
            let df = hit.df.clone();
            server.cache.insert(0, hit);
            return Ok(df);
        }
    }

    let df = load_once(source)?;
    // A stale entry for the same source would otherwise sit behind the new one and
    // never be reached, holding a frame nobody can get to.
    server.cache.retain(|c| c.source != *source);
    server.cache.insert(
        0,
        Cached {
            source: source.clone(),
            stamp,
            df: df.clone(),
        },
    );
    server.cache.truncate(CACHE_ENTRIES);
    Ok(df)
}

/// Read every file a pattern matches as one table.
///
/// The stacking itself lives in [`crate::data::io::load_pattern`], shared with the
/// terminal — what the MCP server adds is its own idea of opening one file, which
/// carries the `container` and `format` the model asked for.
fn load_pattern(source: &Source) -> Result<DataFrame, String> {
    let pattern = source.path.to_string_lossy().to_string();
    let records = matches!(source.extension().as_str(), "md" | "markdown");
    io::load_pattern(&pattern, records, |p| {
        load_single(&Source {
            path: p.to_path_buf(),
            ..source.clone()
        })
        // Plain `eyre!` on a string is `Report::msg`, so `to_string` below hands back
        // exactly this text.  Nothing between here and there may wrap it.
        .map_err(|e| color_eyre::eyre::eyre!(e))
    })
    .map(|(df, _)| df)
    .map_err(|e| e.to_string())
}

/// Load without consulting or filling the cache.  `join` uses this: its
/// right-hand side would otherwise evict the frame the pipeline is built on.
pub fn load_once(source: &Source) -> Result<DataFrame, String> {
    if io::is_pattern(&source.path) {
        return load_pattern(source);
    }
    // A directory is a list of files, which is what the instructions have always said
    // and what the terminal has always done.  It used to be handed to the CSV reader —
    // the default for a path with no extension — which quietly concatenated a directory
    // of like files and refused one holding a `cover.jpg` beside an `index.md`, with
    // Polars' advice to use a glob pattern that tuitab then would not accept.
    if source.path.is_dir() {
        return io::load_directory(&source.path)
            .map_err(|e| format!("Could not list {}: {}", source.path.display(), e));
    }
    load_single(source)
}

fn load_single(source: &Source) -> Result<DataFrame, String> {
    if !source.path.exists() {
        return Err(format!("No such file: {}", source.path.display()));
    }

    let ext = source.extension();

    if let Some(container) = &source.container {
        return load_container(&source.path, &ext, container);
    }

    // A database with no container is a listing, not data.  Refusing here rather than
    // in each tool covers `query`, `describe` and the right-hand side of `join` at once,
    // and stops a fall-through that handed back raw CREATE statements as rows while the
    // instructions said there was no SQL anywhere.  A declared format decides; without
    // one, the file's own header — a database named `db` or `.aup3` is just as much a
    // listing — and then its extension.
    if crate::data::io::db_write::is_db(&source.path, source.format.as_deref()) {
        let n = io::db_containers(&source.path)
            .map(|c| c.len())
            .unwrap_or(0);
        return Err(format!(
            "'{}' holds {} tables and views; pass 'container' to pick one. \
             tuitab_inspect lists them.",
            source.path.display(),
            n
        ));
    }

    // `load_file_as`'s `forced` parameter only covers the document formats, so a
    // declared tabular format used to be accepted and then ignored — `.csv` read as
    // 'parquet' quietly came back as CSV.  Send those through the reader by name.
    if let Some(declared) = source.format.as_deref() {
        if Format::from_name(declared).is_none() {
            return io::load_tabular(&source.path, source.delimiter, &ext).map_err(|e| {
                format!("Could not read {} as {}: {}", source.path.display(), ext, e)
            });
        }
    }

    let forced = source.format.as_deref().and_then(Format::from_name);
    io::load_file_as(&source.path, source.delimiter, forced)
        .map(|(df, _)| df)
        .map_err(|e| format!("Could not read {}: {}", source.path.display(), e))
}

/// Load a database table or view, keeping the source that describes it.
///
/// The declared types, keys and defaults ride on that source; dropping it — which is
/// what reading through the plain loader does — is why a model used to see every
/// database column as a guess made over text.
pub fn load_db_table(
    path: &Path,
    container: &str,
) -> Result<(DataFrame, Option<io::db_write::TableSource>), String> {
    // The engine comes from the file's header, not from its extension — `.db` names
    // neither engine, and the other extensions are a claim the file itself can settle.
    match crate::data::io::db_write::kind_for_path(path) {
        crate::data::io::db_write::DbKind::DuckDb => io::load_duckdb_table_full(path, container),
        crate::data::io::db_write::DbKind::Sqlite => io::load_sqlite_table_full(path, container),
    }
    .map_err(|e| {
        format!(
            "Could not read '{}' from {}: {}",
            container,
            path.display(),
            e
        )
    })
}

fn load_container(path: &Path, ext: &str, container: &str) -> Result<DataFrame, String> {
    if crate::data::io::db_write::is_db_path(path) {
        return load_db_table(path, container).map(|(df, _)| df);
    }
    match ext {
        "xlsx" | "xls" => io::load_excel_sheet_by_name(path, container).map_err(|e| {
            format!(
                "Could not read sheet '{}' from {}: {}",
                container,
                path.display(),
                e
            )
        }),
        other => Err(format!(
            "'.{}' files hold a single table — drop 'container'",
            other
        )),
    }
}

/// What a file holds, or `None` when the format holds one unnamed table.
///
/// Databases answer with their tables *and* views, each carrying what the catalogue
/// knows; a spreadsheet has nothing beyond a name to say.
pub fn containers(path: &Path, ext: &str) -> Option<Vec<io::ContainerInfo>> {
    if crate::data::io::db_write::is_db_path(path) {
        return io::db_containers(path).ok().filter(|c| !c.is_empty());
    }
    match ext {
        "xlsx" | "xls" => io::excel_sheet_sizes(path).ok().map(|sheets| {
            sheets
                .into_iter()
                .map(|(name, rows, columns)| io::ContainerInfo {
                    name,
                    view: false,
                    rows: Some(rows as i64),
                    columns,
                    sql: None,
                })
                .collect()
        }),
        _ => None,
    }
}

/// The extension a source resolves to, for callers outside this module.
pub fn extension_of(source: &Source) -> String {
    source.extension()
}

/// Load a source as a document tree.  Only the structured formats have one; a
/// CSV has no nesting for jq to walk.
pub fn load_doc(source: &Source) -> Result<Doc, String> {
    let ext = source.extension();
    let format = Format::from_name(&ext).ok_or_else(|| {
        format!(
            "jq needs a JSON, JSONL, YAML or TOML source; '{}' is not one. \
             Use tuitab_query for tabular data.",
            if ext.is_empty() {
                "(no extension)"
            } else {
                &ext
            }
        )
    })?;
    Doc::load(&source.path, format)
        .map_err(|e| format!("Could not read {}: {}", source.path.display(), e))
}
