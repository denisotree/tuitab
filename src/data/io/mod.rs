use crate::data::column::ColumnMeta;
use crate::data::dataframe::DataFrame;
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
}

impl Default for Opened {
    fn default() -> Self {
        Self {
            df: DataFrame::empty(),
            doc: None,
            sqlite_db_path: None,
            duckdb_db_path: None,
            xlsx_db_path: None,
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
/// a database.
pub fn open_target(
    path: &Path,
    delimiter: Option<u8>,
    forced: Option<crate::data::doc::Format>,
) -> Result<Opened> {
    if forced.is_some() {
        let (df, doc) = load_file_as(path, delimiter, forced)?;
        return Ok(Opened {
            df,
            doc,
            ..Default::default()
        });
    }

    if db_write::is_db_ext(path) {
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

    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
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

    let (df, doc) = load_file_as(path, delimiter, None)?;
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
/// Ask it only about a path that is not there: `report[1].csv` is a real name a browser
/// hands out, and a file that exists is never a pattern.
pub fn is_pattern(path: &Path) -> bool {
    path.to_string_lossy()
        .contains(['*', '?', '[', ']'].as_slice())
}

/// Every file a pattern names, in a fixed order.
fn glob_matches(pattern: &str) -> Result<Vec<std::path::PathBuf>> {
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
/// `records` unions instead of stacking — a page is a record and records differ.
/// `one` is how the caller opens a single file: the MCP server honours the container
/// and format it was handed, the terminal goes through [`open_target`].  That is the
/// only thing the two surfaces disagree about, so it is the only thing passed in.
pub fn load_pattern(
    pattern: &str,
    records: bool,
    one: impl Fn(&Path) -> Result<DataFrame>,
) -> Result<(DataFrame, usize)> {
    let files = glob_matches(pattern)?;
    let Some((first, rest)) = files.split_first() else {
        // Not "No such file": the pattern is fine, nothing matched it, and those call
        // for different next moves.
        return Err(eyre!("glob matched no files: {}", pattern));
    };

    let head = one(first)?;
    if rest.is_empty() {
        return Ok((head, 1));
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
        let next = one(path)?;
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
    let df = wrap_polars_df(combined)
        .map_err(|e| eyre!("{} matched {} files: {}", pattern, files.len(), e))?;
    Ok((df, files.len()))
}

pub fn load_file_as(
    path: &Path,
    delimiter: Option<u8>,
    forced: Option<crate::data::doc::Format>,
) -> Result<(DataFrame, Option<doc_io::DocState>)> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("csv")
        .to_lowercase();

    if let Some(fmt) = forced.or_else(|| crate::data::doc::Format::from_ext(&ext)) {
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

    load_tabular(path, delimiter, &ext).map(|df| (df, None))
}

/// Read a file as `ext` says, whatever it happens to be called.
pub(crate) fn load_tabular(path: &Path, delimiter: Option<u8>, ext: &str) -> Result<DataFrame> {
    match ext {
        "csv" | "tsv" => crate::data::loader::load_csv(path, delimiter),
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
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("csv")
        .to_lowercase();

    if let Some(fmt) = crate::data::doc::Format::from_ext(&ext) {
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
        // The engine comes from the file when there is one — adding a table to an
        // existing database has to speak that database's dialect, whatever it is called.
        "db" | "sqlite" | "sqlite3" | "duckdb" | "ddb" => {
            db_write::create_table(db_write::kind_for_path(path), path, sheet_name, df)
        }
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
    use std::io::{Read, Write};
    use tempfile::NamedTempFile;

    let mut buf = Vec::new();
    std::io::stdin().read_to_end(&mut buf)?;

    if let Some(fmt) = crate::data::doc::Format::from_name(data_type) {
        let text = String::from_utf8(buf)?;
        let (df, state) = doc_io::DocState::from_doc(crate::data::doc::Doc::from_str(&text, fmt)?)?;
        return Ok((df, Some(state)));
    }

    let mut temp_file = NamedTempFile::new()?;
    temp_file.write_all(&buf)?;
    let temp_path = temp_file.path().to_path_buf();

    let pdf = match data_type.to_lowercase().as_str() {
        "csv" | "txt" => {
            let sep = delimiter.unwrap_or(b',');
            polars::prelude::CsvReadOptions::default()
                .with_has_header(true)
                .map_parse_options(|o| o.with_separator(sep))
                .try_into_reader_with_file_path(Some(temp_path))?
                .finish()?
        }
        "tsv" => polars::prelude::CsvReadOptions::default()
            .with_has_header(true)
            .map_parse_options(|o| o.with_separator(b'\t'))
            .try_into_reader_with_file_path(Some(temp_path))?
            .finish()?,
        _ => return Err(eyre!("Unsupported stdin data type: {}", data_type)),
    };

    drop(temp_file);
    wrap_polars_df(pdf).map(|df| (df, None))
}

/// The column type a Polars dtype is shown as.
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
        _ => ColumnType::String,
    }
}

pub(crate) fn wrap_polars_df(pdf: polars::prelude::DataFrame) -> Result<DataFrame> {
    let col_count = pdf.width();
    let mut columns = Vec::with_capacity(col_count);

    for series in pdf.columns() {
        let name = series.name().to_string();
        let mut col_meta = ColumnMeta::new(name);

        col_meta.col_type = column_type_of(series.dtype());

        columns.push(col_meta);
    }

    let mut df = DataFrame::from_parts(pdf, columns);
    df.calc_widths(40, 1000);
    Ok(df)
}

fn save_csv(df: &DataFrame, path: &Path, delimiter: u8) -> Result<()> {
    let mut out_df = df.to_display_polars_df();
    let mut file = File::create(path)?;
    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(delimiter)
        .finish(&mut out_df)?;
    Ok(())
}
