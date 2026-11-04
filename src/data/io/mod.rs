use crate::data::column::ColumnMeta;
use crate::data::dataframe::DataFrame;
use crate::data::profile;
use crate::types::ColumnType;
use color_eyre::{eyre::eyre, Result};
use polars::prelude::*;
use std::fs::File;
use std::path::Path;

mod arrow;
pub mod db_write;
mod directory;
pub mod doc_io;
mod duckdb;
mod excel;
mod markdown;

mod parquet;
mod sample;
mod sqlite;
mod txt;

pub use directory::{load_directory, load_files_list};
pub use duckdb::{
    duckdb_table_names, load_duckdb_overview, load_duckdb_table_by_name, load_duckdb_table_full,
};
pub use excel::{
    excel_sheet_names, excel_sheet_sizes, load_excel_overview, load_excel_sheet_by_name,
};
pub use sqlite::{
    load_sqlite_overview, load_sqlite_table_by_name, load_sqlite_table_full, sqlite_table_names,
};

pub use directory::format_file_size_pub;
pub use sample::{columns_of, jsonl_table, Growth, Sample, Sampling, Tail};

/// One table or view of a database, as the catalogue describes it.
///
/// The same query answers the terminal's overview sheet and the MCP server's container
/// listing, so the two cannot disagree about what a file holds.
#[derive(Clone, Debug)]
pub struct ContainerInfo {
    pub name: String,
    pub view: bool,
    /// `None` for a view: counting its rows means running it, and listing what a file
    /// holds should not execute somebody's ten-million-row query.
    pub rows: Option<i64>,
    pub columns: usize,
    /// The `CREATE TABLE` / `CREATE VIEW` the database keeps.
    pub sql: Option<String>,
}

/// Tables and views of a database, whichever engine it is.
///
/// The engine comes from the file's header rather than its name — see
/// [`db_write::kind_for_path`].
pub fn db_containers(path: &Path) -> Result<Vec<ContainerInfo>> {
    match db_write::kind_for_path(path) {
        db_write::DbKind::Sqlite => sqlite::sqlite_containers(path),
        db_write::DbKind::DuckDb => duckdb::duckdb_containers(path),
    }
}

pub fn load_file(path: &Path, delimiter: Option<u8>) -> Result<DataFrame> {
    load_file_with_doc(path, delimiter).map(|(df, _)| df)
}

/// Load `path`, additionally returning the document tree when the file is one of the
/// structured formats.  Sheets that keep the [`doc_io::DocState`] can edit and re-save
/// the real structure; callers that drop it get a read-only projection.
///
/// `forced` overrides the extension, which is how `--type` opens a file whose name says
/// nothing useful (`deploy.conf` as YAML).
pub fn load_file_with_doc(
    path: &Path,
    delimiter: Option<u8>,
) -> Result<(DataFrame, Option<doc_io::DocState>)> {
    load_file_as(path, delimiter, None)
}

/// Everything opening a path produces: the table, the document tree when there is one,
/// and — when the table is a listing of what is inside a container — the path that
/// listing came from.  A sheet without the container path renders the same but cannot be
/// drilled into: `Enter` has nothing to tell it that a row names a table (#43).
pub struct Opened {
    pub df: DataFrame,
    pub doc: Option<doc_io::DocState>,
    pub sqlite_db_path: Option<std::path::PathBuf>,
    pub duckdb_db_path: Option<std::path::PathBuf>,
    pub xlsx_db_path: Option<std::path::PathBuf>,
    /// Set when only the head of the file was read.
    pub sample: Option<Sample>,
    /// The rest of the file, arriving in the background.
    pub tail: Option<Tail>,
}

/// Where a file stops being loaded whole: a sixteenth of the memory `budget`.  The MCP
/// server streams past it, the terminal reads a JSON Lines file a piece at a time past
/// it, and a sheet underneath others stays in memory past it rather than being
/// written out — one threshold, so the three cannot drift apart.
pub fn stream_from(budget: u64) -> u64 {
    budget / 16
}

/// Half the machine's physical memory: what may be held in memory by default.
pub fn memory_budget() -> u64 {
    #[cfg(unix)]
    {
        // SAFETY: sysconf reads a system constant and has no preconditions.
        let (pages, size) = unsafe {
            (
                libc::sysconf(libc::_SC_PHYS_PAGES),
                libc::sysconf(libc::_SC_PAGESIZE),
            )
        };
        if pages > 0 && size > 0 {
            return (pages as u64).saturating_mul(size as u64) / 2;
        }
    }
    8 * 1024 * 1024 * 1024
}

impl Default for Opened {
    fn default() -> Self {
        Self {
            df: DataFrame::empty(),
            doc: None,
            sqlite_db_path: None,
            duckdb_db_path: None,
            xlsx_db_path: None,
            sample: None,
            tail: None,
        }
    }
}

/// Open `path` the way the application opens it, rather than the way its bytes read.
///
/// The difference is the containers: a database and a workbook of several sheets open as
/// a listing of what is inside, and that listing is only useful with the path it came
/// from.  Every caller that puts a file on the sheet stack goes through here, so the
/// answer cannot depend on which of them asked — the background loader included, which
/// is where the two used to disagree.
///
/// `forced` wins over all of it: `--type yaml notes.db` means parse it as YAML, not open
/// a database.  It is the format's name as the user gave it, any case — a document
/// format, a database, or any tabular name `load_tabular` knows, the same set the MCP
/// server's `format` takes.
pub fn open_target(path: &Path, delimiter: Option<u8>, forced: Option<&str>) -> Result<Opened> {
    open_target_from(path, delimiter, forced, Sampling::default())
}

/// [`open_target`], sampling as `sampling` says.
pub fn open_target_from(
    path: &Path,
    delimiter: Option<u8>,
    forced: Option<&str>,
    sampling: Sampling,
) -> Result<Opened> {
    open_target_opts(path, &LoadOpts::with_delimiter(delimiter), forced, sampling)
}

/// [`open_target_from`], reading as `opts` says.
pub fn open_target_opts(
    path: &Path,
    opts: &LoadOpts,
    forced: Option<&str>,
    sampling: Sampling,
) -> Result<Opened> {
    let forced = forced.map(str::to_lowercase);
    if let Some(opened) = sample::jsonl_of(path, forced.as_deref(), sampling)? {
        return Ok(opened);
    }
    if let Some(fmt) = forced
        .as_deref()
        .and_then(crate::data::doc::Format::from_name)
    {
        let (df, doc) = load_file_as_opts(path, opts, Some(fmt))?;
        return Ok(Opened {
            df,
            doc,
            ..Default::default()
        });
    }

    if db_write::is_db(path, forced.as_deref()) {
        // Not the extension: `.db` names no engine.  `kind_for_path` reads the header,
        // the same answer the writer uses.
        return Ok(match db_write::kind_for_path(path) {
            db_write::DbKind::Sqlite => Opened {
                df: load_sqlite_overview(path)?,
                sqlite_db_path: Some(path.to_path_buf()),
                ..Default::default()
            },
            db_write::DbKind::DuckDb => Opened {
                df: load_duckdb_overview(path)?,
                duckdb_db_path: Some(path.to_path_buf()),
                ..Default::default()
            },
        });
    }

    let ext = forced.clone().unwrap_or_else(|| {
        path.extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase()
    });
    // One sheet is just a table; the overview would be a list of length one standing
    // between the user and their data.
    if matches!(ext.as_str(), "xlsx" | "xls" | "xlsm" | "xlsb") {
        if let Ok(names) = excel_sheet_names(path) {
            if names.len() > 1 {
                return Ok(Opened {
                    df: load_excel_overview(path)?,
                    xlsx_db_path: Some(path.to_path_buf()),
                    ..Default::default()
                });
            }
        }
    }

    // A named format is read as exactly that, with no sniffing: the user has said what
    // the file is, which is the whole point of `--type csv export.dat`.
    let (df, doc) = if forced.is_some() {
        let df = load_tabular(path, opts, &ext)
            .map_err(|e| eyre!("Could not read {} as {}: {}", path.display(), ext, e))?;
        (df, None)
    } else {
        load_file_as_opts(path, opts, None)?
    };
    Ok(Opened {
        df,
        doc,
        ..Default::default()
    })
}

/// Whether a path is a pattern rather than a name.
///
/// `Path::exists` is false for `db/*.csv` however many files it would match, so a
/// pattern has to be recognised before anything asks the filesystem about it — which is
/// what used to answer "No such file" to the very form tuitab's own error advised.
/// A path that exists is never one: `report[1].csv` is a real name a browser hands
/// out, and it is read as that file.
pub fn is_pattern(path: &Path) -> bool {
    path.to_string_lossy()
        .contains(['*', '?', '[', ']'].as_slice())
        && !path.exists()
}

/// Every file a pattern names, in a fixed order.
pub(crate) fn glob_matches(pattern: &str) -> Result<Vec<std::path::PathBuf>> {
    let mut found: Vec<std::path::PathBuf> = glob::glob(pattern)
        .map_err(|e| eyre!("'{}' is not a usable pattern: {}", pattern, e))?
        .filter_map(std::result::Result::ok)
        .filter(|p| p.is_file())
        .collect();
    // Sorted, so the same pattern gives the same table twice — the filesystem's own
    // order is not one.
    found.sort();
    Ok(found)
}

/// Read every file a pattern matches as one table, and how many files that was.
///
/// The files have to agree on their columns: a table stacked out of frames that do not
/// is not an answer, it is a mess with a row count.  Which file broke the agreement is
/// named, because that is the one to look at.
///
/// `records` unions instead of stacking — a page is a record and records differ — and
/// JSON Lines files meet field by field, as the pieces of one file do.  `ext` is the
/// format the files are read as, and `opts` how; `one` is how the caller opens a single
/// file, with the options it is handed: the MCP server honours the container and format
/// it was asked for, the terminal goes through [`open_target_opts`].
pub fn load_pattern(
    pattern: &str,
    records: bool,
    ext: &str,
    opts: &LoadOpts,
    one: impl Fn(&Path, &LoadOpts) -> Result<DataFrame>,
) -> Result<(DataFrame, usize)> {
    let files = glob_matches(pattern)?;
    let Some((first, rest)) = files.split_first() else {
        // Not "No such file": the pattern is fine, nothing matched it, and those call
        // for different next moves.
        return Err(eyre!("glob matched no files: {}", pattern));
    };
    // Each file read as it is; what the values say about their types is asked of all
    // of them at once, as a scan of the pattern asks it — an epoch column in one file
    // and small integers in another are one column of integers, not two that will not
    // stack.
    let raw = LoadOpts {
        profile: false,
        ..opts.clone()
    };
    let lines = matches!(ext, "jsonl" | "ndjson" | "ldjson")
        || crate::data::doc::Format::of_file(first) == Some(crate::data::doc::Format::Jsonl);
    let head = one(first, &raw)?;
    let profiled = |df: DataFrame| {
        if opts.profile && !lines && !records {
            profile_table(df, ext, opts)
        } else {
            df
        }
    };
    if rest.is_empty() {
        return Ok((profiled(head), 1));
    }

    // JSON Lines files meet as the pieces of one file do: by field, a field one file
    // lacks NULL in it, two kinds of value in one field held as JSON.
    if lines {
        let mut stacked = head;
        for path in rest {
            stacked.push_rows(one(path, &raw)?).map_err(|e| {
                eyre!(
                    "{} could not be stacked onto {}: {}",
                    path.display(),
                    first.display(),
                    e
                )
            })?;
        }
        stacked.calc_widths(40, 1000);
        return Ok((stacked, files.len()));
    }

    // A page is a record, and records differ: one has `tags`, the next has not, and a
    // site where every page carried the same keys would not need a table to check it.
    // So markdown is unioned, missing fields arriving as NULL — the way a list of JSON
    // objects already behaves.  A csv or a parquet is a table, where a column set that
    // does not match means the pattern caught a file it should not have, and saying so
    // is worth more than a sparse frame.
    let names: Vec<&str> = head.columns.iter().map(|c| c.name.as_str()).collect();
    let mut frames = vec![head.df.clone()];
    for path in rest {
        let next = one(path, &raw)?;
        let next_names: Vec<&str> = next.columns.iter().map(|c| c.name.as_str()).collect();
        if !records && next_names != names {
            return Err(eyre!(
                "{} has different columns from {}: [{}] against [{}]. A pattern reads \
                 files that hold the same table.",
                path.display(),
                first.display(),
                next_names.join(", "),
                names.join(", ")
            ));
        }
        frames.push(next.df.clone());
    }

    let combined = if records {
        polars::functions::concat_df_diagonal(&frames).map_err(|e| {
            eyre!(
                "{} matched {} files that could not be read as one table: {}",
                pattern,
                files.len(),
                e
            )
        })?
    } else {
        let mut stacked = frames[0].clone();
        for (frame, path) in frames[1..].iter().zip(rest) {
            stacked.vstack_mut(frame).map_err(|e| {
                eyre!(
                    "{} could not be stacked onto {}: {}",
                    path.display(),
                    first.display(),
                    e
                )
            })?;
        }
        stacked
    };
    let mut df = wrap_polars_df(combined)
        .map_err(|e| eyre!("{} matched {} files: {}", pattern, files.len(), e))?;
    // Zones the files set aside are the stacked frame's too, so it saves back the way
    // a single file does.
    df.inherit_meta(&head.columns, |n| n);
    Ok((profiled(df), files.len()))
}

/// How a file is read: the CSV delimiter, and whether integer epoch columns are shown
/// as datetimes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadOpts {
    pub delimiter: Option<u8>,
    pub detect_epoch: bool,
    /// Text read as NULL in every column of a CSV, before its types are worked out.
    pub null_values: Vec<String>,
    /// Whether the columns' values settle their types — null tokens, epochs.  Off for
    /// the files of a pattern, which are profiled once, stacked.
    pub profile: bool,
}

impl Default for LoadOpts {
    fn default() -> Self {
        Self {
            delimiter: None,
            detect_epoch: true,
            null_values: Vec::new(),
            profile: true,
        }
    }
}

impl LoadOpts {
    pub fn with_delimiter(delimiter: Option<u8>) -> Self {
        Self {
            delimiter,
            ..Self::default()
        }
    }
}

pub fn load_file_as(
    path: &Path,
    delimiter: Option<u8>,
    forced: Option<crate::data::doc::Format>,
) -> Result<(DataFrame, Option<doc_io::DocState>)> {
    load_file_as_opts(path, &LoadOpts::with_delimiter(delimiter), forced)
}

pub fn load_file_as_opts(
    path: &Path,
    opts: &LoadOpts,
    forced: Option<crate::data::doc::Format>,
) -> Result<(DataFrame, Option<doc_io::DocState>)> {
    // The header before the name, and before the sniffer below: a database stored as
    // `db` or `.aup3` would otherwise be read in full by `read_to_string`, then parsed
    // as CSV.
    if forced.is_none() && db_write::kind_of_file(path).is_some() {
        return load_tabular(path, opts, "db").map(|df| (df, None));
    }

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("csv")
        .to_lowercase();

    if let Some(fmt) = forced.or_else(|| crate::data::doc::Format::of_file(path)) {
        // JSON Lines is a table, read as one wherever it is opened: see `sample`.
        if fmt == crate::data::doc::Format::Jsonl {
            return Ok((sample::jsonl_file(path)?, None));
        }
        let (df, state) = doc_io::DocState::open(path, fmt)?;
        return Ok((df, Some(state)));
    }

    // The extension said nothing useful.  Look at the contents before giving up — or,
    // for a file with no extension, before falling back to the CSV default.
    let has_ext = path.extension().is_some();
    let known_tabular = matches!(
        ext.as_str(),
        "csv"
            | "tsv"
            | "txt"
            | "parquet"
            | "arrow"
            | "feather"
            | "ipc"
            | "xlsx"
            | "xls"
            | "db"
            | "sqlite"
            | "sqlite3"
            | "duckdb"
            | "ddb"
            | "md"
            | "markdown"
    );
    if !has_ext || !known_tabular {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Some(fmt) = crate::data::doc::sniff(&text, !has_ext) {
                let mut doc = crate::data::doc::Doc::from_str(&text, fmt)?;
                doc.path = Some(path.to_path_buf());
                let (df, state) = doc_io::DocState::from_doc(doc)?;
                return Ok((df, Some(state)));
            }
        }
    }

    load_tabular(path, opts, &ext).map(|df| (df, None))
}

/// Read a file as `ext` says, whatever it happens to be called.
///
/// A CSV's text column that would be numeric but for [`profile::NULL_TOKENS`] becomes
/// numeric, the tokens NULL.  Integer columns named like a time whose every value is an
/// epoch in one unit become datetimes (UTC); the unit is kept on the column, so a save
/// writes the integers back and retyping to Integer restores them.  Documents — JSON,
/// YAML, TOML, markdown — are not touched, because their table is rebuilt from the tree
/// after every edit and a column would change type under the user.
pub(crate) fn load_tabular(path: &Path, opts: &LoadOpts, ext: &str) -> Result<DataFrame> {
    let df = load_tabular_raw(path, opts, ext)?;
    Ok(if opts.profile {
        profile_table(df, ext, opts)
    } else {
        df
    })
}

/// What a table's values say about its types, applied: null tokens of a CSV cast away,
/// epoch columns shown as datetimes.  The same rules, and the same code, as a scan of a
/// file too large to load.
fn profile_table(df: DataFrame, ext: &str, opts: &LoadOpts) -> DataFrame {
    let tokens = matches!(ext, "csv" | "tsv");
    let epochs = opts.detect_epoch
        && matches!(
            ext,
            "csv" | "tsv" | "parquet" | "arrow" | "feather" | "ipc" | "xlsx" | "xls"
        );
    if !tokens && !epochs {
        return df;
    }
    let profiled = || -> Result<Option<DataFrame>> {
        let mut lf = df.df.clone().lazy();
        let nan = tokens
            && df.df.columns().iter().any(|c| {
                c.dtype().is_float() && c.as_materialized_series().is_nan().is_ok_and(|m| m.any())
            });
        if nan {
            lf = profile::without_nan(lf, df.df.schema());
        }
        let casts = if tokens {
            profile::token_casts(&lf, df.df.schema())?
        } else {
            Vec::new()
        };
        lf = profile::with_casts(lf, &casts);
        let found = if epochs {
            profile::epoch_columns(&lf, &*lf.clone().collect_schema()?)?
        } else {
            Vec::new()
        };
        if !nan && casts.is_empty() && found.is_empty() {
            return Ok(None);
        }
        let pdf = crate::data::collect(profile::with_epochs(lf, &found))?;
        let mut out = wrap_polars_df(pdf)?;
        out.inherit_meta(&df.columns, |n| n);
        for (name, unit) in found {
            if let Ok(i) = out.column_index(&name) {
                out.columns[i].epoch_unit = Some(unit);
            }
        }
        Ok(Some(out))
    };
    // A profile that cannot be taken leaves the columns as they were read.
    profiled().ok().flatten().unwrap_or(df)
}

/// The frame with every detected epoch column back to the integers it was read from —
/// what a save writes, so that opening a file and saving it changes nothing.
pub fn with_epoch_restored(df: &DataFrame) -> std::borrow::Cow<'_, DataFrame> {
    if df.columns.iter().all(|c| c.epoch_unit.is_none()) {
        return std::borrow::Cow::Borrowed(df);
    }
    let mut out = df.clone();
    for idx in 0..out.columns.len() {
        if out.columns[idx].epoch_unit.is_some() {
            let _ = out.set_column_type(idx, ColumnType::Integer);
        }
    }
    std::borrow::Cow::Owned(out)
}

fn load_tabular_raw(path: &Path, opts: &LoadOpts, ext: &str) -> Result<DataFrame> {
    match ext {
        "csv" | "tsv" => crate::data::loader::load_csv_as(path, opts, ext),
        "txt" => txt::load_txt(path),
        "parquet" => parquet::load_parquet(path),
        "arrow" | "feather" | "ipc" => arrow::load_arrow(path),
        // A page is a record: its frontmatter fields, plus the body.
        "md" | "markdown" => markdown::load_markdown(path),
        "xlsx" | "xls" => excel::load_excel(path),
        // Not the extension: `.db` names no engine at all, and a name is only ever a
        // claim.  `kind_for_path` reads the file's own header and falls back to the
        // extension for one that does not exist yet — the same answer the writer uses.
        "db" | "sqlite" | "sqlite3" | "duckdb" | "ddb" => match db_write::kind_for_path(path) {
            db_write::DbKind::DuckDb => duckdb::load_duckdb_overview(path),
            db_write::DbKind::Sqlite => sqlite::load_sqlite_overview(path),
        },
        _ => Err(eyre!("Unsupported file format: .{}", ext)),
    }
}

/// Save to `path`, choosing the writer from its extension.
///
/// For the structured formats the rule is: a sheet that carries a document tree is
/// written by re-serialising that tree, so the original structure survives and
/// converting between formats is just picking a different extension.  A sheet with no
/// tree behind it (CSV, Parquet, SQL, pivot) is first turned into a tree using `shape`.
///
/// For a database target `sheet_name` is the name of the table to create.  The TUI asks
/// for it and never reaches here; the MCP server has nowhere to ask, so it passes its
/// own.
pub fn save_file_as(
    df: &DataFrame,
    doc: Option<&doc_io::DocState>,
    path: &Path,
    shape: doc_io::Shape,
    sheet_name: &str,
) -> Result<()> {
    let format = path
        .extension()
        .and_then(|e| e.to_str())
        .and_then(crate::data::doc::Format::from_ext);
    save_file_in(df, doc, path, format, shape, sheet_name)
}

/// [`save_file_as`] with the document format given rather than read off the extension —
/// a `.json` of JSON Lines saved back onto itself is written as lines.
pub fn save_file_in(
    df: &DataFrame,
    doc: Option<&doc_io::DocState>,
    path: &Path,
    format: Option<crate::data::doc::Format>,
    shape: doc_io::Shape,
    sheet_name: &str,
) -> Result<()> {
    let restored = with_epoch_restored(df);
    let df: &DataFrame = &restored;
    // First, before any writer the name would pick: an existing database gets a table
    // added, never a CSV or JSON written over it.  The engine comes from the file when
    // there is one — the new table has to speak that database's dialect.
    if db_write::is_db_path(path) {
        return db_write::create_table(db_write::kind_for_path(path), path, sheet_name, df);
    }

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("csv")
        .to_lowercase();

    if let Some(fmt) = format {
        let opts = crate::data::doc::SaveOpts::default();
        return match doc {
            Some(state) => state.save_wrapped(path, fmt, &opts, sheet_name),
            None => doc_io::table_to_doc(df, shape, fmt, sheet_name)?.save_as(path, fmt, &opts),
        };
    }

    match ext.as_str() {
        "csv" => save_csv(df, path, b','),
        "tsv" => save_csv(df, path, b'\t'),
        "parquet" => parquet::save_parquet(df, path),
        "arrow" | "feather" | "ipc" => arrow::save_arrow(df, path),
        "xlsx" | "xls" => excel::save_xlsx(df, path),
        _ => Err(eyre!("Unsupported save format: .{}", ext)),
    }
}

/// Whether [`save_file_as`] has a writer for this extension.
///
/// The same list, so a caller can ask *before* doing the work that produces the rows —
/// finding out the target is unwritable after a join and a group-by has run is a waste
/// the answer was always going to be able to prevent.
pub fn writable_ext(ext: &str) -> bool {
    let ext = ext.to_lowercase();
    crate::data::doc::Format::from_ext(&ext).is_some()
        || matches!(
            ext.as_str(),
            "csv"
                | "tsv"
                | "parquet"
                | "arrow"
                | "feather"
                | "ipc"
                | "db"
                | "sqlite"
                | "sqlite3"
                | "duckdb"
                | "ddb"
                | "xlsx"
                | "xls"
        )
}

pub fn load_from_stdin_typed(data_type: &str, delimiter: Option<u8>) -> Result<DataFrame> {
    load_from_stdin_with_doc(data_type, delimiter).map(|(df, _)| df)
}

pub fn load_from_stdin_with_doc(
    data_type: &str,
    delimiter: Option<u8>,
) -> Result<(DataFrame, Option<doc_io::DocState>)> {
    use std::io::Read;
    let mut buf = Vec::new();
    std::io::stdin().read_to_end(&mut buf)?;
    load_from_bytes(buf, data_type, delimiter)
}

/// Data handed over whole — stdin — read as `data_type`, by the rules a file of that
/// type is read by.
pub fn load_from_bytes(
    buf: Vec<u8>,
    data_type: &str,
    delimiter: Option<u8>,
) -> Result<(DataFrame, Option<doc_io::DocState>)> {
    use std::io::Write;

    if let Some(fmt) = crate::data::doc::Format::from_name(data_type) {
        use crate::data::doc::{holds_lines, Format};
        let text = String::from_utf8(buf)?;
        // A table, not a tree: see `sample::jsonl_of`. `-t json` on JSON Lines
        // content gets the check a `.json` file gets.
        if fmt == Format::Jsonl || (fmt == Format::Json && holds_lines(&text)) {
            return Ok((jsonl_table(&text)?, None));
        }
        let (df, state) = doc_io::DocState::from_doc(crate::data::doc::Doc::from_str(&text, fmt)?)?;
        return Ok((df, Some(state)));
    }

    let ext = match data_type.to_lowercase().as_str() {
        "csv" | "txt" => "csv",
        "tsv" => "tsv",
        _ => {
            return Err(eyre!(
                "Unsupported stdin data type: {}. stdin takes csv, tsv, txt, json, jsonl, \
                 yaml or toml.",
                data_type
            ))
        }
    };
    // Through a file, by the reader a file of this type gets: its null tokens, its
    // epochs, its types from every row.
    let mut temp_file = tempfile::NamedTempFile::new()?;
    temp_file.write_all(&buf)?;
    let df = load_tabular(temp_file.path(), &LoadOpts::with_delimiter(delimiter), ext)?;
    Ok((df, None))
}

/// The column type a Polars dtype is shown as.
/// DECIMAL columns — DuckDB, Spark and Arrow write them — as floats, and the places
/// each was written with: every statistic and every writer reads a float.
fn as_floats(pdf: &mut polars::prelude::DataFrame) -> Result<Vec<(String, u8)>> {
    let mut found = Vec::new();
    for i in 0..pdf.width() {
        if let DataType::Decimal(_, scale) = pdf.columns()[i].dtype() {
            let scale = *scale;
            let cast = pdf.columns()[i].cast(&DataType::Float64)?;
            found.push((cast.name().to_string(), scale.min(u8::MAX as usize) as u8));
            pdf.replace_column(i, cast)?;
        }
    }
    Ok(found)
}

pub(crate) fn column_type_of(dtype: &DataType) -> ColumnType {
    match dtype {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => ColumnType::Integer,
        DataType::Float32 | DataType::Float64 => ColumnType::Float,
        DataType::Date => ColumnType::Date,
        DataType::Datetime(_, _) => ColumnType::Datetime,
        DataType::Boolean => ColumnType::Boolean,
        _ => ColumnType::String,
    }
}

pub(crate) fn wrap_polars_df(mut pdf: polars::prelude::DataFrame) -> Result<DataFrame> {
    let zones = without_zones(&mut pdf);
    let decimals = as_floats(&mut pdf)?;
    let col_count = pdf.width();
    let mut columns = Vec::with_capacity(col_count);

    for series in pdf.columns() {
        let name = series.name().to_string();
        let mut col_meta = ColumnMeta::new(name);

        col_meta.col_type = column_type_of(series.dtype());
        if let Some((_, scale)) = decimals.iter().find(|(n, _)| *n == col_meta.name) {
            col_meta.precision = *scale;
        }
        col_meta.time_zone = zones
            .iter()
            .find(|(n, _)| *n == col_meta.name)
            .map(|(_, z)| z.clone());

        columns.push(col_meta);
    }

    let mut df = DataFrame::from_parts(pdf, columns);
    df.calc_widths(40, 1000);
    Ok(df)
}

/// Datetime columns with a time zone as UTC without one (see [`profile::zone_free`]);
/// returns each column's zone.
pub(crate) fn without_zones(pdf: &mut polars::prelude::DataFrame) -> Vec<(String, String)> {
    let (exprs, zones) = profile::zone_free(pdf.schema());
    if !exprs.is_empty() {
        if let Ok(out) = pdf.clone().lazy().with_columns(exprs).collect() {
            *pdf = out;
        }
    }
    zones
}

/// The visible rows with each zoned column's zone put back, for a format that
/// stores one (Parquet, Arrow).
pub(super) fn with_zones(df: &DataFrame) -> Result<polars::prelude::DataFrame> {
    use polars::prelude::{DataType, IntoSeries, TimeZone};
    let mut out = df.get_visible_df().map_err(|e| eyre!(e))?;
    for meta in &df.columns {
        let Some(zone) = &meta.time_zone else {
            continue;
        };
        let Ok(column) = out.column(&meta.name) else {
            continue;
        };
        let DataType::Datetime(tu, None) = column.dtype() else {
            continue;
        };
        let tu = *tu;
        let physical = column.as_materialized_series().to_physical_repr();
        if let Ok(ints) = physical.i64() {
            let zoned = ints
                .clone()
                .into_datetime(tu, TimeZone::opt_try_new(Some(zone.as_str()))?)
                .into_series();
            out.replace(
                &meta.name,
                zoned.with_name(meta.name.as_str().into()).into(),
            )?;
        }
    }
    Ok(out)
}

fn save_csv(df: &DataFrame, path: &Path, delimiter: u8) -> Result<()> {
    let mut out_df = df
        .get_visible_df()
        .map_err(|e| color_eyre::eyre::eyre!(e))?;
    let mut file = File::create(path)?;
    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(delimiter)
        .finish(&mut out_df)?;
    Ok(())
}
