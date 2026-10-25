//! Writing sheet edits back into the SQLite or DuckDB file they were read from.
//!
//! The table a sheet was loaded from is described by [`TableSource`], captured once at
//! load time: where the file is, which table, how to address a row, what the columns
//! were declared as, and — the load-bearing part — a snapshot of the values as they
//! arrived.  Edits are not journalled; they are *derived* by diffing the live frame
//! against that snapshot.  A polars frame clone shares its column buffers, and every
//! mutation replaces a whole column rather than writing into one, so the snapshot costs
//! nothing until a column is actually edited, and undo/redo needs no special handling —
//! restoring an older frame restores an older diff.
//!
//! Deletions are the exception and are recorded explicitly in [`DbRows::deleted`]: a
//! deleted row only disappears from `row_order`, which is exactly what a drill-down
//! filter does to it as well, so the two are indistinguishable after the fact.

use crate::data::column::ColumnMeta;
use crate::data::dataframe::DataFrame;
use crate::types::ColumnType;
use color_eyre::{eyre::eyre, Result};
use std::path::{Path, PathBuf};

/// Row identifiers per statement.  Keeps a single `UPDATE … WHERE rowid IN (…)` short
/// enough to read in the confirmation popup, and well under SQLite's parameter cap.
const IDS_PER_STMT: usize = 500;

/// How many statements the confirmation popup keeps readable text for.
///
/// Well past what anyone scrolls through, and far short of what a whole-table edit
/// would otherwise hold in memory a second time.
const DISPLAY_CAP: usize = 2000;

/// How long to wait for another writer to let go before giving up.
const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Open SQLite the way every path here wants it: willing to wait its turn.
///
/// Without this, a second writer — another tuitab, the MCP server, `sqlite3` in a
/// terminal — turns a save into a bare "database is locked" the instant the two
/// collide, even though the collision is usually over in milliseconds.
pub(crate) fn open_sqlite(path: &Path) -> Result<rusqlite::Connection> {
    let conn = rusqlite::Connection::open(path)?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    Ok(conn)
}

/// Open DuckDB, saying what it means when it refuses.
///
/// DuckDB has no busy timeout to set: one process holds the file and every other one is
/// turned away outright, with a message about lock files and PIDs.  Waiting is not on
/// offer, so the only thing to improve is the sentence.
pub(crate) fn open_duckdb(path: &Path) -> Result<duckdb::Connection> {
    duckdb::Connection::open(path).map_err(|e| {
        let text = e.to_string();
        if text.contains("Conflicting lock") {
            eyre!(
                "'{}' is open in another program — DuckDB allows one process at a time. \
                 Close it and try again.",
                path.display()
            )
        } else {
            eyre!(text)
        }
    })
}

/// Which engine a table came from.  The SQL the two accept is close enough that only
/// the connection and a couple of queries differ.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DbKind {
    Sqlite,
    DuckDb,
}

impl DbKind {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Sqlite => "SQLite",
            Self::DuckDb => "DuckDB",
        }
    }
}

/// The four shapes a value can be bound as.
///
/// Not a faithful model of either engine's type system — deliberately.  Everything the
/// TUI holds is a string, and the only question at write time is which SQL type to turn
/// that string into so the column keeps storing what it stored before.  Dates,
/// timestamps and blobs bind as text and let the engine do its own coercion, which is
/// what the loader's `CAST(… AS VARCHAR)` already assumes in the other direction.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeclType {
    Int,
    Real,
    Bool,
    Text,
}

impl DeclType {
    /// Classify a declared type name, following SQLite's affinity rules — which
    /// DuckDB's names happen to satisfy too (`UINTEGER`/`HUGEINT` contain `INT`,
    /// `VARCHAR` contains `CHAR`, `DOUBLE` contains `DOUB`).
    pub fn from_sql(decl: &str) -> Self {
        let d = decl.to_ascii_uppercase();
        if d.starts_with("BOOL") {
            Self::Bool
        } else if d.contains("INT") {
            Self::Int
        } else if d.contains("CHAR") || d.contains("CLOB") || d.contains("TEXT") {
            Self::Text
        } else if d.contains("REAL")
            || d.contains("FLOA")
            || d.contains("DOUB")
            || d.contains("DEC")
            || d.contains("NUMERIC")
        {
            Self::Real
        } else {
            Self::Text
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Int => "integer",
            Self::Real => "number",
            Self::Bool => "boolean",
            Self::Text => "text",
        }
    }
}

/// One column of the source table, as the database describes it.
#[derive(Clone, Debug)]
pub struct DbColumn {
    pub name: String,
    /// The declared type verbatim, for error messages.
    pub decl_raw: String,
    pub decl: DeclType,
    pub notnull: bool,
    pub pk: bool,
    /// `DEFAULT …` as the schema spells it, needed verbatim by a table rebuild.
    pub default_sql: Option<String>,
    /// `GENERATED ALWAYS AS …` — cannot be inserted or updated.  SQLite reports it via
    /// `PRAGMA table_xinfo`; DuckDB exposes no flag, so there it stays `false` and the
    /// engine rejects the write at bind time instead (cleanly, before any row moves).
    pub generated: bool,
}

/// Everything needed to write a sheet's edits back into the table it came from.
///
/// Lives on [`crate::sheet::Sheet`] and needs no serde: `SheetStack::swap_out` replaces
/// only the sheet's `dataframe`, leaving the rest of the struct in memory.
#[derive(Clone)]
pub struct TableSource {
    pub kind: DbKind,
    pub db_path: PathBuf,
    pub table: String,
    /// Column used to address a row.  Always `rowid`, in both engines: a table whose
    /// rowid the engine will not give up is handed back read-only rather than addressed
    /// by its primary key, because a key the user can edit would silently retarget every
    /// statement built against it.
    pub key_col: String,
    pub columns: Vec<DbColumn>,
    /// Values exactly as loaded.  Arc-shares its buffers with the live frame until a
    /// column is edited.
    pub original: polars::prelude::DataFrame,
}

impl TableSource {
    /// The same source pointed at a different file — used by the copy-then-apply path.
    pub fn at(&self, db_path: &std::path::Path) -> Self {
        Self {
            db_path: db_path.to_path_buf(),
            ..self.clone()
        }
    }

    pub fn column(&self, name: &str) -> Option<&DbColumn> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// Row identity, carried by the frame itself so that undo, redo and the disk swap all
/// move it around for free.
#[derive(Clone, Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct DbRows {
    /// Key value per *physical* row.  `None` means the row was added after load and
    /// needs an INSERT.  `Option` rather than a sentinel: `-1` is a legal key.
    pub ids: Vec<Option<i64>>,
    /// Keys of rows removed from the sheet, in removal order.
    pub deleted: Vec<i64>,
}

impl DbRows {
    pub fn new(ids: Vec<Option<i64>>) -> Self {
        Self {
            ids,
            deleted: Vec::new(),
        }
    }
}

// ── Values ────────────────────────────────────────────────────────────────────────

/// A value on its way into the database.
#[derive(Clone, PartialEq, Debug)]
pub enum Val {
    Null,
    Int(i64),
    Real(f64),
    Bool(bool),
    Text(String),
}

impl Val {
    /// The value as SQL source, for the confirmation popup only.  Never executed —
    /// execution binds parameters — but produced in the same pass as the placeholder it
    /// stands for, so the two cannot drift apart.
    fn literal(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Int(i) => i.to_string(),
            Self::Real(f) => f.to_string(),
            Self::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Self::Text(s) => format!("'{}'", s.replace('\'', "''")),
        }
    }

    /// Parse the string a cell now holds into the column's declared type.
    ///
    /// An emptied cell is an empty string in a text column and NULL in any other —
    /// `''` is simply not a number, and refusing to save would be worse than the one
    /// reading that is defensible.
    fn parse(raw: Option<&str>, col: &DbColumn) -> Result<Self, String> {
        let Some(s) = raw else {
            return Ok(Self::Null);
        };
        if s.contains('\0') {
            return Err("contains a NUL byte".to_string());
        }
        if matches!(col.decl, DeclType::Text) {
            return Ok(Self::Text(s.to_string()));
        }
        if s.is_empty() {
            return Ok(Self::Null);
        }
        let t = s.trim();
        match col.decl {
            DeclType::Int => t
                .parse::<i64>()
                .map(Self::Int)
                .map_err(|_| format!("'{}' is not an integer ({} column)", s, col.decl_raw)),
            DeclType::Real => t
                .parse::<f64>()
                .map(Self::Real)
                .map_err(|_| format!("'{}' is not a number ({} column)", s, col.decl_raw)),
            DeclType::Bool => match t.to_ascii_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Ok(Self::Bool(true)),
                "false" | "f" | "0" | "no" => Ok(Self::Bool(false)),
                _ => Err(format!(
                    "'{}' is not a boolean ({} column)",
                    s, col.decl_raw
                )),
            },
            DeclType::Text => unreachable!("handled above"),
        }
    }
}

impl rusqlite::ToSql for Val {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, Value as V, ValueRef};
        Ok(match self {
            Self::Null => ToSqlOutput::Borrowed(ValueRef::Null),
            Self::Int(i) => ToSqlOutput::Owned(V::Integer(*i)),
            Self::Real(f) => ToSqlOutput::Owned(V::Real(*f)),
            Self::Bool(b) => ToSqlOutput::Owned(V::Integer(i64::from(*b))),
            Self::Text(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
        })
    }
}

impl duckdb::ToSql for Val {
    fn to_sql(&self) -> duckdb::Result<duckdb::types::ToSqlOutput<'_>> {
        use duckdb::types::{ToSqlOutput, Value as V, ValueRef};
        Ok(match self {
            Self::Null => ToSqlOutput::Borrowed(ValueRef::Null),
            Self::Int(i) => ToSqlOutput::Owned(V::BigInt(*i)),
            Self::Real(f) => ToSqlOutput::Owned(V::Double(*f)),
            Self::Bool(b) => ToSqlOutput::Owned(V::Boolean(*b)),
            Self::Text(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
        })
    }
}

// ── The plan ──────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StmtKind {
    Update,
    Insert,
    Delete,
    /// `ALTER TABLE …` and, later, every statement of a table rebuild.  One variant
    /// rather than several: the popup wants a colour, not a taxonomy.
    Schema,
}

/// One statement, in both the form that executes and the form the user confirms.
#[derive(Clone, Debug)]
pub struct Stmt {
    /// SQL with `?` placeholders — what actually runs.
    pub sql: String,
    pub params: Vec<Val>,
    /// The same SQL with literals inlined — what the popup shows.
    pub display: String,
    pub kind: StmtKind,
}

/// Values a row is expected to still hold in the database, as of load time.
///
/// Every column, not only the ones being changed: a `rowid` can be reused by a
/// different row after a deletion in both engines, and a narrow check could match the
/// wrong row on one column by chance.  Reading the whole row costs nothing at this size
/// and removes the failure mode.
#[derive(Clone, Debug)]
pub struct RowCheck {
    pub id: i64,
    /// One entry per [`TableSource::columns`], in that order.
    pub values: Vec<Option<String>>,
}

#[derive(Clone, Debug, Default)]
pub struct WritePlan {
    pub stmts: Vec<Stmt>,
    pub checks: Vec<RowCheck>,
    /// This plan *creates* the table rather than editing one that already exists, so
    /// the column-shape check before the DDL has nothing to compare against.
    pub create: bool,
    /// The table is dropped and recreated by this plan.  Worth saying out loud in the
    /// popup, and on SQLite it decides the pragmas wrapped around the transaction.
    pub rebuild: bool,
    /// What this plan will destroy that the statements do not spell out — an index the
    /// `DROP TABLE` takes with it, a trigger, a view left pointing at nothing, a column
    /// too display-formatted to write.  Plain sentences: the popup shows them above the
    /// statements as SQL comments, and the status line shows them when a plan has no
    /// statements at all and therefore never reaches the popup.
    pub warnings: Vec<String>,
    pub schema: usize,
    pub updates: usize,
    pub inserts: usize,
    pub deletes: usize,
}

impl WritePlan {
    pub fn is_empty(&self) -> bool {
        self.stmts.is_empty()
    }

    /// Add a statement, keeping its readable form only while the popup can still show it.
    ///
    /// A whole-table edit holds the changed data three times over — once in the grouping
    /// key, once in `params` and once in `display` — plus a full copy of every touched
    /// row in `checks`.  `params` and `checks` are executed and cannot go; `display` is
    /// only ever read by a human, and past this many statements no human is reading it.
    /// The text is still *built* — that string is freed at once — it is keeping every one
    /// of them that would cost a second copy of the table.
    fn push(&mut self, sql: String, display: String, params: Vec<Val>, kind: StmtKind) {
        let display = if self.stmts.len() < DISPLAY_CAP {
            display
        } else {
            String::new()
        };
        self.stmts.push(Stmt {
            sql,
            display,
            params,
            kind,
        });
    }

    /// Statements the popup can only count, because their text was dropped.
    pub fn hidden_stmts(&self) -> usize {
        self.stmts.iter().filter(|s| s.display.is_empty()).count()
    }

    /// "42 UPDATE · 1 DELETE" — the popup title, and the status line after the write.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        for (n, label) in [
            // Schema first: it is the half that changes the table, not the rows.
            (self.schema, "SCHEMA"),
            (self.updates, "UPDATE"),
            (self.inserts, "INSERT"),
            (self.deletes, "DELETE"),
        ] {
            if n > 0 {
                parts.push(format!("{} {}", n, label));
            }
        }
        if parts.is_empty() {
            "no changes".to_string()
        } else {
            parts.join(" · ")
        }
    }
}

/// Append one bound value, writing the placeholder, the readable literal and the
/// parameter in a single step.
///
/// The popup showing something other than what runs is not a bug that can happen here,
/// because both strings are built by the same call.
fn push_val(sql: &mut String, display: &mut String, params: &mut Vec<Val>, v: Val) {
    sql.push('?');
    display.push_str(&v.literal());
    params.push(v);
}

fn quote_ident(name: &str) -> String {
    name.replace('"', "\"\"")
}

/// What a save has to do to the table's *shape*, before it touches a single row.
#[derive(Debug, Default)]
pub struct SchemaPlan {
    /// Load-time names no live column claims any more.
    pub drops: Vec<String>,
    /// (name at load time, name now).
    pub renames: Vec<(String, String)>,
    /// Live column indices that did not exist at load time.
    pub adds: Vec<usize>,
    /// Live column index → the type the user deliberately assigned.
    pub retypes: Vec<(usize, ColumnType)>,
    /// The surviving columns are in a different order than the table has them.
    pub reorder: bool,
}

impl SchemaPlan {
    pub fn is_empty(&self) -> bool {
        self.drops.is_empty()
            && self.renames.is_empty()
            && self.adds.is_empty()
            && self.retypes.is_empty()
            && !self.reorder
    }
}

/// How an engine spells a [`ColumnType`] in a column definition.
///
/// One map, two callers: the `ADD COLUMN` of a schema change and the `CREATE TABLE` of
/// a new table.  Both have to agree, or a column added today would be declared
/// differently from the same column created yesterday.
pub fn declared_type(kind: DbKind, t: ColumnType) -> &'static str {
    match t {
        ColumnType::Integer | ColumnType::FileSize => match kind {
            DbKind::Sqlite => "INTEGER",
            DbKind::DuckDb => "BIGINT",
        },
        ColumnType::Float | ColumnType::Percentage | ColumnType::Currency => match kind {
            DbKind::Sqlite => "REAL",
            DbKind::DuckDb => "DOUBLE",
        },
        ColumnType::Boolean => "BOOLEAN",
        // Dates and times travel as text in both directions: every value tuitab holds is
        // a string, `Val` binds them as text, and the loaders read them back as text, so
        // text round-trips with no cast on either side.
        _ => match kind {
            DbKind::Sqlite => "TEXT",
            DbKind::DuckDb => "VARCHAR",
        },
    }
}

/// Give a freshly loaded frame the types its table declares.
///
/// Every value arrives from both engines as text, so without this a database column is
/// a string column and `score > 100` is a comparison polars refuses outright — numeric
/// filtering over a database simply does not work.
///
/// Two deliberate omissions.  **Boolean stays text**: SQLite has no boolean type and
/// stores a `BOOLEAN` column as the integers 1 and 0, so a polars Boolean column would
/// render `true` where the re-read database says `1`, and every drift check would fire
/// on rows nobody touched.  **A failed cast leaves the column alone**: SQLite lets an
/// `INTEGER` column hold text, and a column that does is still worth reading.
pub fn apply_declared_types(df: &mut DataFrame, columns: &[DbColumn]) {
    use polars::prelude::DataType;
    for (i, col) in columns.iter().enumerate() {
        let (target, as_type) = match col.decl {
            DeclType::Int => (DataType::Int64, ColumnType::Integer),
            DeclType::Real => (DataType::Float64, ColumnType::Float),
            DeclType::Bool | DeclType::Text => continue,
        };
        let Some(column) = df.df.columns().get(i) else {
            continue;
        };
        // Strict: a non-strict cast would turn a value the column cannot hold into a
        // silent NULL, which is data loss dressed up as a type.
        let Ok(cast) = column.as_materialized_series().strict_cast(&target) else {
            continue;
        };
        if df.df.with_column(cast.into()).is_ok() {
            df.columns[i].col_type = as_type;
        }
    }
    df.calc_widths(40, 1000);
}

/// Whether two readings of the same cell mean the same value.
///
/// For a column the table declares as a number this compares the *number*, not its
/// spelling: DuckDB renders a DOUBLE `1.0` as `"1.0"` while Rust's `f64::to_string`
/// gives `"1"`, and both are the same value.  Comparing text would report drift on
/// every whole-numbered double in the table.
fn same_value(a: Option<&str>, b: Option<&str>, col: &DbColumn) -> bool {
    if a == b {
        return true;
    }
    if !matches!(col.decl, DeclType::Int | DeclType::Real) {
        return false;
    }
    match (Val::parse(a, col), Val::parse(b, col)) {
        // NaN is not equal to itself, and DuckDB can store one.
        (Ok(Val::Real(x)), Ok(Val::Real(y))) => x == y || (x.is_nan() && y.is_nan()),
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    }
}

/// Whether a column stores bytes rather than something a person can type.
///
/// SQLite reads one as a `[BLOB n bytes]` marker and DuckDB as escaped text; either way
/// what the frame holds is a *rendering*, and writing that rendering back would replace
/// the bytes with a description of them.
fn is_binary(col: &DbColumn) -> bool {
    col.decl_raw.to_ascii_uppercase().contains("BLOB")
}

/// Whether `t` made this column by parsing text the database still calls text.
///
/// `col_date_from_str` truncates `"2024-01-01 09:30:00"` to the day and turns anything
/// it cannot read into NULL; neither step is reversible, so where it did either the
/// frame no longer holds what the table holds.  The values are a *reading* of the
/// column, the way a BLOB marker is a reading of its bytes.
///
/// Where the declared type itself changed (DuckDB's `ALTER COLUMN … TYPE DATE`, or a
/// rebuild on SQLite) the table becomes the new type and this does not apply.
fn is_parsed_out_of_text(t: ColumnType, live: &DbColumn) -> bool {
    matches!(t, ColumnType::Date | ColumnType::Datetime) && live.decl == DeclType::Text
}

/// Display formats masquerading as storage types.
///
/// A Percentage column stores the value divided by 100 and a Currency column stores a
/// bare `f64` with the symbol stripped, so writing one back would put `0.42` where the
/// user is looking at `42%`.  These are how a column is *shown*, and there is nothing
/// to tell the database about them.
fn is_display_only_type(t: ColumnType) -> bool {
    matches!(
        t,
        ColumnType::Percentage | ColumnType::Currency | ColumnType::FileSize
    )
}

impl TableSource {
    /// Whether this sheet can still be written back, and the row identity if so.
    ///
    /// This is the row-identity and sanity gate.  Changes to the *shape* of the table
    /// are no longer refused here — they become a [`SchemaPlan`].
    ///
    /// Evaluated only when saving — nothing has to be tracked while the user works.
    pub fn writeback_status<'a>(&self, df: &'a DataFrame) -> Result<&'a DbRows, String> {
        let refuse = |why: String| {
            Err(format!(
                "Cannot write back into '{}': {}. Save to a different file instead.",
                self.table, why
            ))
        };

        let Some(rows) = df.db_rows.as_ref() else {
            return refuse(
                "row identity was lost (window column, transpose, pivot, join or group)".into(),
            );
        };
        if rows.ids.len() != df.df.height() {
            return refuse("row identity no longer lines up with the table".into());
        }
        if df.columns.is_empty() {
            return refuse("the sheet has no columns".into());
        }
        // `columns[i].name == df.get_column_names()[i]` is load-bearing everywhere and
        // asserted nowhere.  Now that column identity decides what DDL runs, check it.
        if df.columns.len() != df.df.width() {
            return refuse("the column metadata no longer matches the data".into());
        }
        for (meta, actual) in df.columns.iter().zip(df.df.get_column_names()) {
            if meta.name.as_str() != actual.as_str() {
                return refuse(format!(
                    "column '{}' does not line up with the data behind it",
                    meta.name
                ));
            }
        }

        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for meta in &df.columns {
            if let Some(origin) = meta.db_origin.as_deref() {
                if self.column(origin).is_none() {
                    return refuse(format!("column '{}' is no longer in the table", origin));
                }
                if !seen.insert(origin) {
                    return refuse(format!("two columns both claim to be '{}'", origin));
                }
            }
            // A deliberate retype to a display format would rescale the stored value.
            if let Some(t) = meta.db_retype {
                if is_display_only_type(t) {
                    return refuse(format!(
                        "column '{}' is shown as a {} — that is a display format, not a \
                         storage type (a percentage is stored divided by 100), so writing \
                         it back would silently rescale the column",
                        meta.name,
                        t.name()
                    ));
                }
                if meta
                    .db_origin
                    .as_deref()
                    .and_then(|o| self.column(o))
                    .is_some_and(|c| c.generated)
                {
                    return refuse(format!(
                        "column '{}' is generated by the database and its type is not ours \
                         to change",
                        meta.name
                    ));
                }
            }
        }
        Ok(rows)
    }

    /// Work out what has to happen to the table's shape.
    ///
    /// Reads the origin tags rather than diffing positions: a rename and a
    /// drop-plus-add look identical after the fact, and only the tag knows which one
    /// the user performed.
    pub fn schema_plan(&self, df: &DataFrame) -> SchemaPlan {
        let mut plan = SchemaPlan::default();

        let claimed: std::collections::HashSet<&str> = df
            .columns
            .iter()
            .filter_map(|c| c.db_origin.as_deref())
            .collect();
        for col in &self.columns {
            if !claimed.contains(col.name.as_str()) {
                plan.drops.push(col.name.clone());
            }
        }

        for (i, meta) in df.columns.iter().enumerate() {
            match meta.db_origin.as_deref() {
                None => plan.adds.push(i),
                Some(origin) => {
                    if origin != meta.name {
                        plan.renames.push((origin.to_string(), meta.name.clone()));
                    }
                }
            }
            // A column added in this session has no type to change — it is created with
            // the right one.  And the comparison is between storage classes, not
            // spellings: a column declared `INT` retyped to Integer is already what it
            // is being asked to become, and emitting an ALTER for it would refuse the
            // whole save on SQLite for no reason.
            if let Some(t) = meta.db_retype {
                if meta.db_origin.is_some()
                    && DeclType::from_sql(declared_type(self.kind, t))
                        != DeclType::from_sql(self.decl_of(meta))
                {
                    plan.retypes.push((i, t));
                }
            }
        }

        // Order is compared over the surviving columns only: an added column has no
        // position in the table yet, and a dropped one no longer has one.
        let live: Vec<&str> = df
            .columns
            .iter()
            .filter_map(|c| c.db_origin.as_deref())
            .collect();
        let table: Vec<&str> = self
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .filter(|n| live.contains(n))
            .collect();
        plan.reorder = live != table;

        plan
    }

    /// The declared type the table currently gives this column, if it has one.
    fn decl_of(&self, meta: &ColumnMeta) -> &str {
        meta.db_origin
            .as_deref()
            .and_then(|o| self.column(o))
            .map(|c| c.decl_raw.as_str())
            .unwrap_or_default()
    }

    /// The table as it will be when the row statements run: one entry per live sheet
    /// column, carrying its final name and its final declared type.
    ///
    /// Everything downstream — parsing values, building `SET` clauses, listing the
    /// columns of an INSERT — reads this instead of [`Self::columns`], which is what
    /// keeps the SQL generator free of any name mapping.
    pub fn live_columns(&self, df: &DataFrame) -> Vec<DbColumn> {
        df.columns
            .iter()
            .map(|meta| {
                let retyped = meta.db_retype.filter(|t| !is_display_only_type(*t));
                match meta.db_origin.as_deref().and_then(|o| self.column(o)) {
                    Some(existing) => {
                        let mut c = existing.clone();
                        c.name = meta.name.clone();
                        if let Some(t) = retyped {
                            c.decl_raw = declared_type(self.kind, t).to_string();
                            c.decl = DeclType::from_sql(&c.decl_raw);
                        }
                        c
                    }
                    None => {
                        let decl_raw =
                            declared_type(self.kind, retyped.unwrap_or(meta.col_type)).to_string();
                        DbColumn {
                            name: meta.name.clone(),
                            decl: DeclType::from_sql(&decl_raw),
                            decl_raw,
                            notnull: false,
                            pk: false,
                            default_sql: None,
                            generated: false,
                        }
                    }
                }
            })
            .collect()
    }
}

/// The cell as a nullable string, matching how the loader read it.
fn cell(pdf: &polars::prelude::DataFrame, row: usize, col: usize) -> Option<String> {
    match pdf.columns().get(col)?.get(row) {
        Ok(polars::prelude::AnyValue::Null) | Err(_) => None,
        Ok(v) => Some(DataFrame::anyvalue_to_string_fmt(&v)),
    }
}

/// Work out what changed since load and turn it into statements.
///
/// Nothing is emitted until every changed value has parsed into its column's declared
/// type, so a bad cell stops the save before any SQL exists, let alone runs.
pub fn build_plan(src: &TableSource, df: &DataFrame) -> Result<WritePlan> {
    let rows = src.writeback_status(df).map_err(|e| eyre!(e))?;

    let deleted: std::collections::HashSet<i64> = rows.deleted.iter().copied().collect();
    // Physical row for a key, so a deleted row's original values can still be found —
    // deletion shrinks `row_order`, never the frame itself.
    let mut phys_of_id: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    for (i, id) in rows.ids.iter().enumerate() {
        if let Some(id) = id {
            phys_of_id.insert(*id, i);
        }
    }
    // Display position of a physical row, for error messages that name what the user
    // can actually see.
    let mut display_of_phys: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::new();
    for (d, p) in df.row_order.iter().enumerate() {
        display_of_phys.insert(*p, d + 1);
    }

    // The table as it will be when the row statements run.  Everything below names
    // columns the way they will be named then, so no statement needs a name mapping.
    let live_cols = src.live_columns(df);
    let schema = src.schema_plan(df);

    // Where each surviving column's load-time values are.  By name, not by position:
    // the frame may have been reordered since.
    let snapshot_of: std::collections::HashMap<String, usize> = src
        .original
        .get_column_names()
        .iter()
        .enumerate()
        .map(|(i, n)| (n.to_string(), i))
        .collect();

    /// How a live column has to be compared against the load snapshot.
    enum ColDiff {
        /// Added this session — there is no snapshot, so every row is a change.
        Added,
        /// Ordinary column; the index of its values in the snapshot.
        Plain(usize),
        /// Retyped, so raw strings disagree on formatting alone (`"42.50"` vs `42.5`).
        /// Compare the parsed values instead.
        Retyped(usize),
    }

    // Only columns that can differ are worth scanning row by row; a bulk edit touches
    // one column out of however many the table has.
    let mut diff_cols: Vec<(usize, ColDiff)> = Vec::new();
    let mut unwritable: Vec<String> = Vec::new();
    for (i, meta) in df.columns.iter().enumerate() {
        match meta.db_origin.as_deref() {
            None => diff_cols.push((i, ColDiff::Added)),
            Some(origin) => {
                let Some(&s) = snapshot_of.get(origin) else {
                    continue;
                };
                // A type the user assigned with `t` converts the column, so the frame and
                // the snapshot beside it no longer even share a dtype.  When the
                // conversion throws information away there is nothing to write back;
                // otherwise the two are the same values in different clothes, and the
                // parsed comparison is what sees through that.
                if let Some(t) = meta.db_retype {
                    if is_parsed_out_of_text(t, &live_cols[i]) {
                        // Parsed out of text, so a row that differs is either the parse
                        // throwing something away or an edit — and there is no telling
                        // which.  Unchanged means there is nothing to write either way.
                        let changed = (0..df.df.height())
                            .any(|r| cell(&df.df, r, i) != cell(&src.original, r, s));
                        if changed {
                            unwritable.push(format!(
                                "column '{}' is shown as a {} but stored as text, so it \
                                 will not be written — press t to put it back to edit it",
                                meta.name,
                                t.name()
                            ));
                        }
                        continue;
                    }
                    diff_cols.push((i, ColDiff::Retyped(s)));
                    continue;
                }
                if schema.retypes.iter().any(|(c, _)| *c == i) {
                    diff_cols.push((i, ColDiff::Retyped(s)));
                } else {
                    match (src.original.columns().get(s), df.df.columns().get(i)) {
                        (Some(a), Some(b)) if a.equals_missing(b) => {}
                        _ => diff_cols.push((i, ColDiff::Plain(s))),
                    }
                }
            }
        }
    }

    // Rows sharing an identical set of changes collapse into one statement: that is
    // what turns a bulk edit over 5000 rows into a single readable UPDATE.
    let mut groups: indexmap::IndexMap<Vec<(usize, Option<String>)>, Vec<i64>> =
        indexmap::IndexMap::new();
    let mut updated_ids: Vec<i64> = Vec::new();

    for (phys, id) in rows.ids.iter().enumerate() {
        let Some(id) = *id else { continue };
        if deleted.contains(&id) {
            continue;
        }
        let mut change = Vec::new();
        for (i, how) in &diff_cols {
            let now = cell(&df.df, phys, *i);
            let differs = match how {
                // `ADD COLUMN` leaves every existing row NULL, so only a value worth
                // writing is worth a statement.
                ColDiff::Added => now.is_some(),
                ColDiff::Plain(s) => now != cell(&src.original, phys, *s),
                ColDiff::Retyped(s) => {
                    let before = cell(&src.original, phys, *s);
                    Val::parse(before.as_deref(), &live_cols[*i]).ok()
                        != Val::parse(now.as_deref(), &live_cols[*i]).ok()
                }
            };
            if differs {
                change.push((*i, now));
            }
        }
        if change.is_empty() {
            continue;
        }
        updated_ids.push(id);
        groups.entry(change).or_default().push(id);
    }

    let mut plan = WritePlan {
        warnings: unwritable,
        ..Default::default()
    };
    let table = quote_ident(&src.table);
    let key = quote_ident(&src.key_col);

    // ── Schema, before anything that names a column ───────────────────────────
    push_schema_stmts(src, df, &schema, &live_cols, &mut plan)?;

    // ── UPDATE ────────────────────────────────────────────────────────────────
    for (change, ids) in &groups {
        let mut sets: Vec<(String, Val)> = Vec::with_capacity(change.len());
        for (c, raw) in change {
            let col = &live_cols[*c];
            if col.generated {
                return Err(eyre!(
                    "Column '{}' is generated by the database and cannot be edited",
                    col.name
                ));
            }
            if is_binary(col) {
                return Err(eyre!(
                    "Column '{}' holds binary data: it can be read but not edited. \
                     Undo the change there and the rest of the row still saves",
                    col.name
                ));
            }
            let val = Val::parse(raw.as_deref(), col).map_err(|why| {
                let where_ = ids
                    .first()
                    .and_then(|id| phys_of_id.get(id))
                    .and_then(|p| display_of_phys.get(p))
                    .map(|d| format!("row {}", d))
                    .unwrap_or_else(|| "a row".to_string());
                eyre!("Column '{}', {}: {}", col.name, where_, why)
            })?;
            sets.push((col.name.clone(), val));
        }

        for chunk in ids.chunks(IDS_PER_STMT) {
            let mut sql = format!("UPDATE \"{}\" SET ", table);
            let mut display = sql.clone();
            let mut params: Vec<Val> = Vec::new();
            for (i, (name, val)) in sets.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                    display.push_str(", ");
                }
                let assign = format!("\"{}\" = ", quote_ident(name));
                sql.push_str(&assign);
                display.push_str(&assign);
                push_val(&mut sql, &mut display, &mut params, val.clone());
            }
            write_where(&mut sql, &mut display, &mut params, &key, chunk);
            plan.push(sql, display, params, StmtKind::Update);
        }
    }
    plan.updates = updated_ids.len();

    // ── INSERT ────────────────────────────────────────────────────────────────
    // A generated column has no place in the column list; for SQLite an integer
    // primary key left empty becomes NULL, which is how the engine is asked to assign
    // the next rowid itself.
    let insertable: Vec<(usize, &DbColumn)> = live_cols
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.generated)
        .collect();
    for (phys, id) in rows.ids.iter().enumerate() {
        if id.is_some() {
            continue;
        }
        let mut values: Vec<(&DbColumn, Val)> = Vec::with_capacity(insertable.len());
        for (c, col) in insertable.iter() {
            let raw = cell(&df.df, phys, *c);
            let val = Val::parse(raw.as_deref(), col).map_err(|why| {
                let where_ = display_of_phys
                    .get(&phys)
                    .map(|d| format!("row {}", d))
                    .unwrap_or_else(|| "a new row".to_string());
                eyre!("Column '{}', {}: {}", col.name, where_, why)
            })?;
            // A new row leaves a binary column empty; anything else is the marker text
            // the loader put there, and storing that would be storing a description.
            if is_binary(col) && !matches!(val, Val::Null) {
                return Err(eyre!(
                    "Column '{}' holds binary data: a new row can only leave it empty",
                    col.name
                ));
            }
            values.push((col, val));
        }

        // A column with a DEFAULT and no value is left out of the statement entirely,
        // which is the only way the DEFAULT ever runs: naming it and passing NULL is an
        // instruction to store NULL, and it walks straight past `DEFAULT 'direct'` and
        // `DEFAULT (datetime('now'))` while a CHECK constraint waves it through, NULL
        // satisfying CHECK in SQLite.  Without a DEFAULT there is nothing to fall back
        // on and NULL is written as before.  If that would empty the statement — every
        // column defaulted and nothing given — the full list is kept, an INSERT naming
        // no columns being no INSERT at all.
        let given: Vec<&(&DbColumn, Val)> = values
            .iter()
            .filter(|(col, val)| !(matches!(val, Val::Null) && col.default_sql.is_some()))
            .collect();
        let given = if given.is_empty() {
            values.iter().collect()
        } else {
            given
        };

        let mut sql = format!("INSERT INTO \"{}\" (", table);
        for (i, (col, _)) in given.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("\"{}\"", quote_ident(&col.name)));
        }
        sql.push_str(") VALUES (");
        let mut display = sql.clone();
        let mut params: Vec<Val> = Vec::new();
        for (i, (_, val)) in given.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
                display.push_str(", ");
            }
            push_val(&mut sql, &mut display, &mut params, val.clone());
        }
        sql.push(')');
        display.push(')');
        plan.push(sql, display, params, StmtKind::Insert);
        plan.inserts += 1;
    }

    // ── DELETE ────────────────────────────────────────────────────────────────
    // Last, so that every rowid the UPDATEs address is still valid while they run.
    for chunk in rows.deleted.chunks(IDS_PER_STMT) {
        let mut sql = format!("DELETE FROM \"{}\"", table);
        let mut display = sql.clone();
        let mut params: Vec<Val> = Vec::new();
        write_where(&mut sql, &mut display, &mut params, &key, chunk);
        plan.push(sql, display, params, StmtKind::Delete);
    }
    plan.deletes = rows.deleted.len();

    // ── Reorder, once nothing addresses rows by their old identity ────────────
    push_post_stmts(src, df, &schema, &live_cols, &mut plan)?;

    // ── Drift checks ──────────────────────────────────────────────────────────
    for id in updated_ids.iter().chain(rows.deleted.iter()) {
        let Some(&phys) = phys_of_id.get(id) else {
            continue;
        };
        plan.checks.push(RowCheck {
            id: *id,
            values: (0..src.columns.len())
                .map(|c| cell(&src.original, phys, c))
                .collect(),
        });
    }

    Ok(plan)
}

// ── Creating a table ──────────────────────────────────────────────────────────────
//
// Deliberately *not* built on `build_plan`.  That, and `schema_plan`, `writeback_status`
// and `live_columns`, all work by diffing the live frame against `TableSource::original`
// — a snapshot of the table as it was loaded, which by definition does not exist when
// the table is being made.  What is shared instead: `declared_type`, `column_def`,
// `quote_ident`, `push_val`, `Val::parse`, `cell`, `Stmt`, `WritePlan` and `apply`.

/// Values per statement, so a multi-row `INSERT` stays readable in the popup and well
/// under SQLite's variable cap.
const PARAMS_PER_STMT: usize = 500;

/// The columns a table made from this sheet would have.
///
/// Types come from what the sheet says a column is, through the same map `ADD COLUMN`
/// uses — so a column created today is declared exactly as the same column added
/// tomorrow.  No primary key, no NOT NULL, no DEFAULT: a sheet has no way to express
/// any of them, and inventing an `id INTEGER PRIMARY KEY` would add a column nobody
/// asked for.  Rows are still addressable afterwards because both engines give an
/// ordinary table a `rowid` whether it declares a key or not.
pub fn new_columns(kind: DbKind, df: &DataFrame) -> Result<Vec<DbColumn>> {
    if df.columns.is_empty() {
        return Err(eyre!("This sheet has no columns to make a table from"));
    }
    df.columns
        .iter()
        .enumerate()
        .map(|(i, meta)| {
            if meta.name.trim().is_empty() {
                return Err(eyre!(
                    "Column {} has no name — rename it with 'ze' before creating a table",
                    i + 1
                ));
            }
            // Percentage, Currency and FileSize are *not* refused here, unlike in
            // `writeback_status`.  Writing one back would put 0.42 into a column that
            // has always held 42; creating one puts 0.42 into a column that has never
            // held anything, which is simply the value the sheet stores.
            let decl_raw = declared_type(kind, meta.col_type).to_string();
            Ok(DbColumn {
                name: meta.name.clone(),
                decl: DeclType::from_sql(&decl_raw),
                decl_raw,
                notnull: false,
                pk: false,
                default_sql: None,
                generated: false,
            })
        })
        .collect()
}

/// Whether a name can be a table.
///
/// Shared by the terminal's prompt and the server's `output.table`, so the two cannot
/// disagree about what is allowed.  Anything else is legal because `quote_ident` wraps
/// it.
pub fn validate_table_name(name: &str) -> Result<(), &'static str> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        Err("a table needs a name")
    } else if trimmed.contains('\0') || trimmed.contains('\n') {
        Err("a table name cannot contain a newline")
    } else if trimmed.to_ascii_lowercase().starts_with("sqlite_") {
        Err("names starting with 'sqlite_' are reserved by the engine")
    } else {
        Ok(())
    }
}

/// Whether `path` already holds a table or a view by this name.
///
/// Deliberately not `existing_tables(…).contains(…)`: that goes through the container
/// listing, which runs a `COUNT(*)` over every table in the file — a full scan of the
/// whole database to answer one membership question.
pub fn table_exists(kind: DbKind, path: &Path, name: &str) -> bool {
    if !path.exists() {
        return false;
    }
    match kind {
        DbKind::Sqlite => open_sqlite(path)
            .and_then(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?1",
                )?;
                Ok(stmt.exists([name])?)
            })
            .unwrap_or(false),
        DbKind::DuckDb => open_duckdb(path)
            .and_then(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT 1 FROM duckdb_tables() WHERE table_name = ? \
                     UNION ALL SELECT 1 FROM duckdb_views() WHERE view_name = ?",
                )?;
                let mut rows = stmt.query([name, name])?;
                Ok(rows.next()?.is_some())
            })
            .unwrap_or(false),
    }
}

/// Whether the name belongs to a view rather than a table.
///
/// `DROP TABLE` on a view is a bind-time error inside the transaction, and its text is
/// the engine's, not ours.  Asking first is what turns it into a sentence.
pub fn is_view(kind: DbKind, path: &Path, name: &str) -> bool {
    if !path.exists() {
        return false;
    }
    match kind {
        DbKind::Sqlite => open_sqlite(path)
            .and_then(|conn| {
                let mut stmt =
                    conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'view' AND name = ?1")?;
                Ok(stmt.exists([name])?)
            })
            .unwrap_or(false),
        DbKind::DuckDb => open_duckdb(path)
            .and_then(|conn| {
                let mut stmt = conn.prepare("SELECT 1 FROM duckdb_views() WHERE view_name = ?")?;
                let mut rows = stmt.query([name])?;
                Ok(rows.next()?.is_some())
            })
            .unwrap_or(false),
    }
}

/// Which tables `path` already has, or none if it is not there yet.
pub fn existing_tables(kind: DbKind, path: &Path) -> Vec<String> {
    if !path.exists() {
        return Vec::new();
    }
    match kind {
        DbKind::Sqlite => super::sqlite::sqlite_table_names(path).unwrap_or_default(),
        DbKind::DuckDb => super::duckdb::duckdb_table_names(path).unwrap_or_default(),
    }
}

/// Build `table` in `path` out of `df`.
///
/// Rows are written in the order the sheet shows them — sorted and filtered — because
/// that is what the user is looking at, and it is what saving to CSV already does.
/// (Writeback disagrees: it inserts only physically-new rows regardless of the filter.
/// The two operations mean different things.)
/// What replacing an existing table costs, and the one case where it is refused.
///
/// Creating over a table is `DROP TABLE` + `CREATE TABLE` from what the sheet knows,
/// which is names and types.  Everything else the old table declared — its indexes, its
/// triggers, the views built on it — goes, and unlike the rebuild path (which refuses
/// exactly this) replacing is a deliberate act with a name typed into a prompt.  So the
/// cost is *shown* rather than forbidden.
///
/// The exception is a foreign key pointing *into* the table from elsewhere: that does
/// not cost the user something of theirs, it leaves the database itself inconsistent.
fn preflight_replace(kind: DbKind, path: &Path, table: &str) -> Result<Vec<String>> {
    let mut warnings = Vec::new();
    let refuse = |other: &str| -> Result<Vec<String>> {
        Err(eyre!(
            "Table '{}' has a foreign key into '{}', which replacing it would leave \
             pointing at nothing. Choose another table name.",
            other,
            table
        ))
    };

    match kind {
        DbKind::Sqlite => {
            let conn = open_sqlite(path)?;
            // Auto-indexes have no SQL of their own and come back with the table.
            let mut objs = conn.prepare(
                "SELECT type, name FROM sqlite_master \
                 WHERE tbl_name = ?1 AND sql IS NOT NULL AND type IN ('index', 'trigger') \
                 ORDER BY type, name",
            )?;
            let mut rows = objs.query([table])?;
            while let Some(row) = rows.next()? {
                warnings.push(format!(
                    "{} '{}' will be lost",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?
                ));
            }

            let mut views =
                conn.prepare("SELECT name, sql FROM sqlite_master WHERE type = 'view'")?;
            let mut rows = views.query([])?;
            while let Some(row) = rows.next()? {
                if mentions_word(&row.get::<_, String>(1)?, table) {
                    warnings.push(format!(
                        "view '{}' is built on this table and will break",
                        row.get::<_, String>(0)?
                    ));
                }
            }

            for other in super::sqlite::sqlite_table_names(path)? {
                if other.eq_ignore_ascii_case(table) {
                    continue;
                }
                let mut fks = conn.prepare(&format!(
                    "PRAGMA foreign_key_list(\"{}\")",
                    quote_ident(&other)
                ))?;
                let mut rows = fks.query([])?;
                while let Some(row) = rows.next()? {
                    if row.get::<_, String>(2)?.eq_ignore_ascii_case(table) {
                        return refuse(&other);
                    }
                }
            }
        }
        DbKind::DuckDb => {
            let conn = open_duckdb(path)?;
            let mut idx =
                conn.prepare("SELECT index_name FROM duckdb_indexes() WHERE table_name = ?")?;
            let mut rows = idx.query([table])?;
            while let Some(row) = rows.next()? {
                warnings.push(format!("index '{}' will be lost", row.get::<_, String>(0)?));
            }

            let mut views = conn.prepare("SELECT view_name, sql FROM duckdb_views()")?;
            let mut rows = views.query([])?;
            while let Some(row) = rows.next()? {
                if mentions_word(&row.get::<_, String>(1)?, table) {
                    warnings.push(format!(
                        "view '{}' is built on this table and will break",
                        row.get::<_, String>(0)?
                    ));
                }
            }

            // DuckDB keeps no reverse index of foreign keys, so the constraint text of
            // every *other* table is what says whether one points here.
            let mut fks = conn.prepare(
                "SELECT table_name, constraint_text FROM duckdb_constraints() \
                 WHERE constraint_type = 'FOREIGN KEY'",
            )?;
            let mut rows = fks.query([])?;
            while let Some(row) = rows.next()? {
                let other: String = row.get(0)?;
                if !other.eq_ignore_ascii_case(table)
                    && mentions_word(&row.get::<_, String>(1)?, table)
                {
                    return refuse(&other);
                }
            }
        }
    }
    Ok(warnings)
}

pub fn create_plan(
    kind: DbKind,
    path: &Path,
    table: &str,
    df: &DataFrame,
) -> Result<(WritePlan, TableSource)> {
    let cols = new_columns(kind, df)?;
    let quoted = quote_ident(table);
    let mut plan = WritePlan {
        create: true,
        ..Default::default()
    };
    let push_schema = |sql: String, plan: &mut WritePlan| {
        plan.stmts.push(Stmt {
            display: sql.clone(),
            sql,
            params: Vec::new(),
            kind: StmtKind::Schema,
        });
        plan.schema += 1;
    };

    if table_exists(kind, path, table) {
        plan.warnings = preflight_replace(kind, path, table)?;
        push_schema(format!("DROP TABLE \"{}\"", quoted), &mut plan);
        // Not a fudge: this is what buys the foreign-key pragmas around the SQLite
        // transaction and the dangling-reference check before it commits.
        plan.rebuild = true;
    }
    let defs: Vec<String> = cols.iter().map(|c| column_def(c, false)).collect();
    push_schema(
        format!("CREATE TABLE \"{}\" ({})", quoted, defs.join(", ")),
        &mut plan,
    );

    let names: Vec<String> = cols
        .iter()
        .map(|c| format!("\"{}\"", quote_ident(&c.name)))
        .collect();
    let prefix = format!("INSERT INTO \"{}\" ({}) VALUES ", quoted, names.join(", "));

    // ponytail: `params` holds the whole export a second time, and it has to — those
    // are the values that get written.  (`display` is capped by `WritePlan::push`.)
    // Upgrade path is streaming the inserts outside the plan, at the cost of the popup
    // no longer showing what runs.
    let per_stmt = (PARAMS_PER_STMT / cols.len()).clamp(1, IDS_PER_STMT);
    for chunk in df.row_order.chunks(per_stmt) {
        let mut sql = prefix.clone();
        let mut display = prefix.clone();
        let mut params: Vec<Val> = Vec::with_capacity(chunk.len() * cols.len());
        for (r, &phys) in chunk.iter().enumerate() {
            if r > 0 {
                sql.push_str(", ");
                display.push_str(", ");
            }
            sql.push('(');
            display.push('(');
            for (c, col) in cols.iter().enumerate() {
                if c > 0 {
                    sql.push_str(", ");
                    display.push_str(", ");
                }
                let raw = cell(&df.df, phys, c);
                let val = Val::parse(raw.as_deref(), col).map_err(|why| {
                    let shown = df.row_order.iter().position(|p| *p == phys).unwrap_or(phys) + 1;
                    eyre!("Column '{}', row {}: {}", col.name, shown, why)
                })?;
                push_val(&mut sql, &mut display, &mut params, val);
            }
            sql.push(')');
            display.push(')');
        }
        plan.push(sql, display, params, StmtKind::Insert);
    }
    plan.inserts = df.row_order.len();

    // Only `kind`, `db_path` and `table` are read from this — `apply` routes on them.
    // `original` is never touched: `checks` is empty and `create`
    // skips the shape comparison.
    let source = TableSource {
        kind,
        db_path: path.to_path_buf(),
        table: table.to_string(),
        key_col: "rowid".to_string(),
        columns: cols,
        original: polars::prelude::DataFrame::empty(),
    };
    Ok((plan, source))
}

/// Delete a database file and whatever the engine keeps beside it.
///
/// `Connection::open` makes the file *before* the transaction, so a failed create would
/// otherwise leave a zero-byte database behind — and the next attempt would see it
/// exist, take the table-already-there branch, and behave differently from the first.
pub fn remove_new_file(path: &Path) {
    let _ = std::fs::remove_file(path);
    let name = path.to_string_lossy().into_owned();
    for extra in [
        format!("{}-wal", name),
        format!("{}-shm", name),
        format!("{}.wal", name),
    ] {
        let _ = std::fs::remove_file(extra);
    }
}

/// Create `table` in `path` from `df`, in one transaction, without asking anything.
///
/// The one-shot entry point for callers that have no popup to show — `save_file_as`
/// and the MCP server.
pub fn create_table(kind: DbKind, path: &Path, table: &str, df: &DataFrame) -> Result<()> {
    let existed = path.exists();
    let (plan, src) = create_plan(kind, path, table, df)?;
    let result = apply(&src, &plan);
    if result.is_err() && !existed {
        remove_new_file(path);
    }
    result
}

/// Which engine actually wrote this file, read from its first bytes.
///
/// `.db` says nothing about the format, and both engines stamp themselves: SQLite writes
/// `SQLite format 3\0` at the start, DuckDB writes `DUCK` eight bytes in.  Sixteen bytes
/// settle it — no connection, no listing, and the same answer everywhere it is asked.
/// `None` for a file that is neither, or too short to tell (a database yet to be made).
///
/// Only a regular file is opened.  This is asked of every path now, not only of those
/// named like a database, and reading the head of a pipe (`tuitab <(cmd)`) would eat
/// bytes the real reader needs — or, with no writer on the other end, never return.
pub fn kind_of_file(path: &Path) -> Option<DbKind> {
    use std::io::Read;
    if !std::fs::metadata(path).is_ok_and(|m| m.is_file()) {
        return None;
    }
    let mut head = [0u8; 16];
    let mut file = std::fs::File::open(path).ok()?;
    if file.read_exact(&mut head).is_err() {
        return None;
    }
    // `DUCK` alone is four letters a CSV can hold at offset 8 (`animals,DUCK,…`).  DuckDB
    // follows it with a little-endian u64 version, a small number whose high bytes are
    // zero — and text never carries NULs there.
    if head.starts_with(b"SQLite format 3\0") {
        Some(DbKind::Sqlite)
    } else if &head[8..12] == b"DUCK" && head[14..16] == [0, 0] {
        Some(DbKind::DuckDb)
    } else {
        None
    }
}

/// Which engine a path names: what the file says it is, and for one that does not exist
/// yet, what the extension implies — `.db` is ambiguous and a new one is made in SQLite.
pub fn kind_for_path(path: &Path) -> DbKind {
    if let Some(kind) = kind_of_file(path) {
        return kind;
    }
    match path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "duckdb" | "ddb" => DbKind::DuckDb,
        _ => DbKind::Sqlite,
    }
}

// ── Execution ─────────────────────────────────────────────────────────────────────

/// Whether `path` is a database: what the file's own header says, and for a file that
/// does not exist yet (or is too short to tell), what its extension claims.
///
/// The header comes first because applications store SQLite under names of their own —
/// `.aup3`, `.mmb`, `.gnucash`, or no extension at all — and a save that trusted the
/// name would export over the database instead of writing into it.
pub fn is_db_path(path: &Path) -> bool {
    kind_of_file(path).is_some()
        || is_db_name(
            path.extension()
                .and_then(|e| e.to_str())
                .unwrap_or_default(),
        )
}

/// The same question asked of a format name rather than a path, for the callers that
/// let one be given explicitly.
pub fn is_db_name(ext: &str) -> bool {
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "db" | "sqlite" | "sqlite3" | "duckdb" | "ddb"
    )
}

/// Whether two paths name the same file, resolving symlinks and `..` where possible.
pub fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

/// Run the plan against the source database, all of it or none of it.
///
/// The drift check runs first, inside the same transaction, so a table that changed
/// under the user cannot be half-written before anyone notices.
pub fn apply(src: &TableSource, plan: &WritePlan) -> Result<()> {
    match src.kind {
        DbKind::Sqlite => apply_sqlite(src, plan),
        DbKind::DuckDb => apply_duckdb(src, plan),
    }
}

/// The columns of a drift-check SELECT, in `TableSource::columns` order.
///
/// Each engine renders them exactly the way its loader did — anything else reports
/// drift on rows nobody touched.  SQLite reads native values and stringifies them in
/// Rust (`value_to_opt_string`); DuckDB casts on its own side.
fn check_query(src: &TableSource, n_ids: usize) -> String {
    let cast = |name: &str| match src.kind {
        DbKind::Sqlite => format!("\"{}\"", quote_ident(name)),
        DbKind::DuckDb => format!("CAST(\"{}\" AS VARCHAR)", quote_ident(name)),
    };
    let cols: Vec<String> = src.columns.iter().map(|c| cast(&c.name)).collect();
    let placeholders = vec!["?"; n_ids].join(", ");
    format!(
        "SELECT \"{key}\", {cols} FROM \"{table}\" WHERE \"{key}\" IN ({placeholders})",
        key = quote_ident(&src.key_col),
        cols = cols.join(", "),
        table = quote_ident(&src.table),
    )
}

/// Compare what the database holds now against what it held when the sheet was loaded.
fn compare_drift(
    src: &TableSource,
    checks: &[RowCheck],
    actual: &std::collections::HashMap<i64, Vec<Option<String>>>,
) -> Result<()> {
    let stale = |detail: String| {
        eyre!(
            "{} — '{}' changed since it was opened; something else has written to it. \
             Nothing was written; reopen the table and redo the edit.",
            detail,
            src.table
        )
    };
    for check in checks {
        let Some(now) = actual.get(&check.id) else {
            return Err(stale(format!("Row {} is gone", check.id)));
        };
        for (i, expected) in check.values.iter().enumerate() {
            let Some(col) = src.columns.get(i) else {
                continue;
            };
            let found = now.get(i).and_then(|v| v.as_deref());
            if !same_value(found, expected.as_deref(), col) {
                let name = col.name.as_str();
                // Quoted, not `{:?}` on the Option: `Some("x")` is Rust talking to
                // itself, and this sentence is read by a person.
                let shown = |v: Option<&str>| match v {
                    Some(text) => format!("'{}'", text),
                    None => "NULL".to_string(),
                };
                return Err(stale(format!(
                    "Row {}, column '{}': the database has {}, expected {}",
                    check.id,
                    name,
                    shown(found),
                    shown(expected.as_deref())
                )));
            }
        }
    }
    Ok(())
}

/// The table's column names changed since the sheet was opened.
///
/// The value-level drift check compares cells and would not notice a column appearing
/// or vanishing — which matters now that a plan can name columns that have to exist.
fn check_shape(src: &TableSource, actual: Vec<String>) -> Result<()> {
    let expected: Vec<&str> = src.columns.iter().map(|c| c.name.as_str()).collect();
    if actual != expected {
        return Err(eyre!(
            "The columns of '{}' changed since it was opened — the table now has [{}] \
             where it had [{}]. Nothing was written; reopen the table and redo the edit.",
            src.table,
            actual.join(", "),
            expected.join(", ")
        ));
    }
    Ok(())
}

fn apply_sqlite(src: &TableSource, plan: &WritePlan) -> Result<()> {
    use super::sqlite::value_to_opt_string;
    let mut conn = open_sqlite(&src.db_path)?;

    // Dropping the table would otherwise cascade through anything pointing at it.  This
    // has to be set outside a transaction — inside one it is a no-op — and the check
    // before COMMIT is what makes turning it off safe.
    if plan.rebuild {
        conn.pragma_update(None, "foreign_keys", false)?;
    }
    let result = apply_sqlite_inner(src, plan, &mut conn, value_to_opt_string);
    if plan.rebuild {
        let _ = conn.pragma_update(None, "foreign_keys", true);
    }
    result
}

fn apply_sqlite_inner(
    src: &TableSource,
    plan: &WritePlan,
    conn: &mut rusqlite::Connection,
    value_to_opt_string: fn(rusqlite::types::Value) -> Option<String>,
) -> Result<()> {
    // IMMEDIATE, not the default DEFERRED: a deferred transaction takes its read lock
    // first and asks to upgrade at the first write, and SQLite refuses to *wait* for
    // that upgrade — it returns "database is locked" at once, busy timeout or not,
    // because waiting there could deadlock two readers.  Taking the write lock up front
    // is where the timeout does its job.
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

    if !plan.create {
        let mut stmt = tx.prepare(&format!(
            "PRAGMA table_xinfo(\"{}\")",
            quote_ident(&src.table)
        ))?;
        let mut rows = stmt.query([])?;
        let mut actual = Vec::new();
        while let Some(row) = rows.next()? {
            actual.push(row.get::<_, String>(1)?);
        }
        check_shape(src, actual)?;
    }

    for chunk in plan.checks.chunks(IDS_PER_STMT) {
        let mut stmt = tx.prepare(&check_query(src, chunk.len()))?;
        let mut rows = stmt.query(rusqlite::params_from_iter(chunk.iter().map(|c| c.id)))?;
        let mut actual = std::collections::HashMap::new();
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let mut vals = Vec::with_capacity(src.columns.len());
            for i in 0..src.columns.len() {
                vals.push(value_to_opt_string(row.get(i + 1)?));
            }
            actual.insert(id, vals);
        }
        compare_drift(src, chunk, &actual)?;
    }

    for stmt in &plan.stmts {
        // A `PRAGMA x = y` returns no rows but is not an `execute` either as far as
        // rusqlite is concerned; run every schema statement as a batch.
        if stmt.params.is_empty() {
            tx.execute_batch(&stmt.sql)?;
        } else {
            tx.execute(&stmt.sql, rusqlite::params_from_iter(stmt.params.iter()))?;
        }
    }

    // Turning foreign keys off for the rebuild is only safe if nothing was left
    // dangling by it.
    if plan.rebuild {
        let mut check = tx.prepare("PRAGMA foreign_key_check")?;
        let mut rows = check.query([])?;
        if let Some(row) = rows.next()? {
            return Err(eyre!(
                "Rebuilding '{}' would leave a dangling reference from '{}'. Nothing was \
                 written.",
                src.table,
                row.get::<_, String>(0).unwrap_or_default()
            ));
        }
    }

    tx.commit()?;
    Ok(())
}

fn apply_duckdb(src: &TableSource, plan: &WritePlan) -> Result<()> {
    let mut conn = open_duckdb(&src.db_path)?;
    let tx = conn.transaction()?;

    if !plan.create {
        let mut stmt = tx.prepare(&format!(
            "PRAGMA table_info('{}')",
            src.table.replace('\'', "''")
        ))?;
        let mut rows = stmt.query([])?;
        let mut actual = Vec::new();
        while let Some(row) = rows.next()? {
            actual.push(row.get::<_, String>(1)?);
        }
        check_shape(src, actual)?;
    }

    for chunk in plan.checks.chunks(IDS_PER_STMT) {
        let mut stmt = tx.prepare(&check_query(src, chunk.len()))?;
        let mut rows = stmt.query(duckdb::params_from_iter(chunk.iter().map(|c| c.id)))?;
        let mut actual = std::collections::HashMap::new();
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let mut vals = Vec::with_capacity(src.columns.len());
            for i in 0..src.columns.len() {
                vals.push(row.get::<_, Option<String>>(i + 1)?);
            }
            actual.insert(id, vals);
        }
        compare_drift(src, chunk, &actual)?;
    }

    for stmt in &plan.stmts {
        tx.execute(&stmt.sql, duckdb::params_from_iter(stmt.params.iter()))?;
    }
    tx.commit()?;
    Ok(())
}

/// Copy the whole database to `dest` — every table, index, view and trigger — so that
/// applying the plan to the copy leaves a file of the same shape as the original.
pub fn copy_db(src: &TableSource, dest: &Path) -> Result<()> {
    // This path runs without a confirmation popup, because copying into a new file has
    // nothing of the user's to lose.  That stops being true the moment the destination
    // already exists, so it has to be a new file in fact and not just in intent.
    if dest.exists() {
        return Err(eyre!(
            "{} already exists. Saving a database elsewhere writes a fresh copy of the \
             whole file — remove it first or choose another name.",
            dest.display()
        ));
    }
    match src.kind {
        // The documented way to take a consistent snapshot, WAL and all.
        DbKind::Sqlite => {
            let conn = open_sqlite(&src.db_path)?;
            conn.execute("VACUUM INTO ?1", [dest.to_string_lossy().as_ref()])?;
            Ok(())
        }
        // DuckDB has no VACUUM INTO.  CHECKPOINT folds the WAL into the main file,
        // after which a byte copy is a valid snapshot — and tuitab holds no long-lived
        // connection of its own to race with.
        DbKind::DuckDb => {
            {
                let conn = open_duckdb(&src.db_path)?;
                conn.execute_batch("CHECKPOINT;")?;
            }
            std::fs::copy(&src.db_path, dest)?;
            // A write-ahead log left behind by whatever used to live at this path would
            // be replayed against the copy on the next open.
            let wal = dest.with_extension(format!(
                "{}.wal",
                dest.extension().unwrap_or_default().to_string_lossy()
            ));
            let _ = std::fs::remove_file(wal);
            Ok(())
        }
    }
}

/// Refuse a schema change the engine is going to reject anyway, while there is still a
/// sentence to say about it.
///
/// Opens a read-only connection, so it runs only when there is schema work — every
/// value-level check has already happened by now, and "a bad cell stops the save before
/// any SQL exists" stays true.
fn preflight_schema(src: &TableSource, schema: &SchemaPlan) -> Result<()> {
    if schema.drops.is_empty() {
        return Ok(());
    }
    // The primary key is in hand already; no connection needed to say so.
    for name in &schema.drops {
        if src.column(name).is_some_and(|c| c.pk) {
            return Err(eyre!(
                "'{}' is the primary key of '{}' and cannot be dropped. Save to a \
                 different file instead.",
                name,
                src.table
            ));
        }
    }
    if src.kind != DbKind::Sqlite {
        // DuckDB rejects these at bind time, inside the transaction, which rolls back
        // cleanly — its own message is as good as one we would write.
        return Ok(());
    }

    let conn = open_sqlite(&src.db_path)?;
    let table = quote_ident(&src.table);

    // An index over a column that is going away would go away with it — SQLite refuses,
    // and it is right to.
    let mut list = conn.prepare(&format!("PRAGMA index_list(\"{}\")", table))?;
    let mut indexes = list.query([])?;
    while let Some(row) = indexes.next()? {
        let index: String = row.get(1)?;
        let mut info = conn.prepare(&format!("PRAGMA index_info(\"{}\")", quote_ident(&index)))?;
        let mut cols = info.query([])?;
        while let Some(c) = cols.next()? {
            let col: Option<String> = c.get(2)?;
            if let Some(col) = col {
                if schema.drops.contains(&col) {
                    return Err(eyre!(
                        "'{}' is used by index '{}' and cannot be dropped. Drop the index \
                         first, or save to a different file.",
                        col,
                        index
                    ));
                }
            }
        }
    }

    // A trigger or view that names the column would break the moment it is gone.
    let mut objs = conn.prepare(
        "SELECT type, name, sql FROM sqlite_master \
         WHERE type IN ('view', 'trigger') AND sql IS NOT NULL",
    )?;
    let mut rows = objs.query([])?;
    while let Some(row) = rows.next()? {
        let kind: String = row.get(0)?;
        let name: String = row.get(1)?;
        let sql: String = row.get(2)?;
        for col in &schema.drops {
            if mentions_word(&sql, col) {
                return Err(eyre!(
                    "'{}' is referenced by {} '{}' and cannot be dropped. Save to a \
                     different file instead.",
                    col,
                    kind,
                    name
                ));
            }
        }
    }
    Ok(())
}

/// Whether `sql` uses `word` as an identifier rather than as part of a longer one.
///
/// Deliberately crude and deliberately over-matching: a false positive costs the user a
/// "save to a different file", a false negative costs them a broken trigger.
pub(crate) fn mentions_word(sql: &str, word: &str) -> bool {
    let hay = sql.to_ascii_lowercase();
    let needle = word.to_ascii_lowercase();
    let boundary = |c: Option<char>| !c.is_some_and(|c| c.is_alphanumeric() || c == '_');
    let mut from = 0;
    while let Some(at) = hay[from..].find(&needle) {
        let start = from + at;
        let end = start + needle.len();
        if boundary(hay[..start].chars().next_back()) && boundary(hay[end..].chars().next()) {
            return true;
        }
        from = end;
    }
    false
}

/// The scratch name a column is parked under while a cycle of renames unwinds.
const SWAP_SUFFIX: &str = "__tuitab_swap";

/// The scratch name a rebuild builds under before taking the real one.
const REBUILD_SUFFIX: &str = "__tuitab_rebuild";

/// Whether a change needs the table rebuilt rather than altered.
///
/// Neither engine can reorder columns.  SQLite additionally has no `ALTER COLUMN`, so a
/// type change there is a rebuild too; DuckDB does it natively.
fn needs_rebuild(kind: DbKind, schema: &SchemaPlan) -> bool {
    schema.reorder || (kind == DbKind::Sqlite && !schema.retypes.is_empty())
}

/// One `"name" TYPE [NOT NULL] [DEFAULT …] [PRIMARY KEY]` line of a `CREATE TABLE`.
fn column_def(col: &DbColumn, inline_pk: bool) -> String {
    let mut def = format!("\"{}\" {}", quote_ident(&col.name), col.decl_raw);
    if col.notnull {
        def.push_str(" NOT NULL");
    }
    if let Some(d) = &col.default_sql {
        def.push_str(&format!(" DEFAULT {}", d));
    }
    if inline_pk && col.pk {
        def.push_str(" PRIMARY KEY");
    }
    def
}

/// Refuse to rebuild a table whose definition says more than we can reproduce.
///
/// The `CREATE TABLE` is synthesized from the column pragma, which knows about names,
/// types, NOT NULL, DEFAULT and a single-column primary key — and nothing else.  A
/// CHECK, a UNIQUE, a foreign key or a collation would be silently dropped on the way
/// through, so anything hinting at one is a refusal.  The bias is deliberate: a false
/// refusal costs a "save to a different file", a false pass costs a constraint.
fn preflight_rebuild_sqlite(
    src: &TableSource,
    schema: &SchemaPlan,
    live_cols: &[DbColumn],
) -> Result<()> {
    let conn = open_sqlite(&src.db_path)?;
    let table = quote_ident(&src.table);
    let refuse = |why: String| -> Result<()> {
        Err(eyre!(
            "'{}' has to be rebuilt to do this, and tuitab will not rebuild it: {}. \
             Save to a different file instead.",
            src.table,
            why
        ))
    };

    if live_cols.iter().filter(|c| c.pk).count() > 1 {
        return refuse("its primary key spans several columns".into());
    }
    if live_cols.iter().any(|c| c.generated) {
        return refuse("it has a generated column".into());
    }

    let mut fks = conn.prepare(&format!("PRAGMA foreign_key_list(\"{}\")", table))?;
    if fks.query([])?.next()?.is_some() {
        return refuse("it has a foreign key of its own".into());
    }

    let mut list = conn.prepare(&format!("PRAGMA index_list(\"{}\")", table))?;
    let mut rows = list.query([])?;
    while let Some(row) = rows.next()? {
        // 'c' is a plain CREATE INDEX, which is reproducible; 'u' and 'pk' are
        // constraints written into the table definition, which are not.
        if row.get::<_, String>(3)?.as_str() != "c" {
            return refuse(format!(
                "index '{}' comes from a UNIQUE or PRIMARY KEY constraint in the table \
                 definition",
                row.get::<_, String>(1)?
            ));
        }
    }

    // Everything else lives only in the text of the definition.
    let ddl: String = conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [&src.table],
        |r| r.get(0),
    )?;
    for word in [
        "CHECK",
        "COLLATE",
        "AUTOINCREMENT",
        "WITHOUT ROWID",
        "STRICT",
        "GENERATED",
    ] {
        if mentions_word(&ddl, word) {
            return refuse(format!("its definition uses {}", word));
        }
    }

    // Other tables pointing at this one would be left pointing at nothing.
    for other in super::sqlite::sqlite_table_names(&src.db_path)? {
        if other == src.table {
            continue;
        }
        let mut stmt = conn.prepare(&format!(
            "PRAGMA foreign_key_list(\"{}\")",
            quote_ident(&other)
        ))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            if row.get::<_, String>(2)?.eq_ignore_ascii_case(&src.table) {
                return refuse(format!("table '{}' has a foreign key into it", other));
            }
        }
    }

    // Indexes and triggers are replayed from their stored SQL, which still spells a
    // renamed column the old way.  In the ALTER path SQLite rewrites those references
    // itself; a rebuild has no rename for it to notice, so the replay would fail on a
    // column that no longer exists.
    if !schema.renames.is_empty() {
        let mut objs = conn.prepare(
            "SELECT type, name, sql FROM sqlite_master \
             WHERE tbl_name = ?1 AND sql IS NOT NULL AND type IN ('index', 'trigger')",
        )?;
        let mut rows = objs.query([&src.table])?;
        while let Some(row) = rows.next()? {
            let sql: String = row.get(2)?;
            for (from, _) in &schema.renames {
                if mentions_word(&sql, from) {
                    return refuse(format!(
                        "{} '{}' names the column '{}' that is being renamed",
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        from
                    ));
                }
            }
        }
    }

    // A view built on the table would survive the DROP as a broken object.
    let mut views = conn
        .prepare("SELECT name, sql FROM sqlite_master WHERE type = 'view' AND sql IS NOT NULL")?;
    let mut rows = views.query([])?;
    while let Some(row) = rows.next()? {
        let sql: String = row.get(1)?;
        if mentions_word(&sql, &src.table) {
            return refuse(format!(
                "view '{}' is built on it",
                row.get::<_, String>(0)?
            ));
        }
    }

    if conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
            [&format!("{}{}", src.table, REBUILD_SUFFIX)],
            |r| r.get::<_, i64>(0),
        )
        .unwrap_or(0)
        > 0
    {
        return refuse("a leftover rebuild table from an earlier run is in the way".into());
    }
    Ok(())
}

fn preflight_rebuild_duckdb(src: &TableSource) -> Result<()> {
    let conn = open_duckdb(&src.db_path)?;
    let refuse = |why: String| -> Result<()> {
        Err(eyre!(
            "'{}' has to be rebuilt to reorder its columns, and tuitab will not rebuild \
             it: {}. Save to a different file instead.",
            src.table,
            why
        ))
    };

    let mut stmt = conn.prepare(
        "SELECT constraint_type FROM duckdb_constraints() \
         WHERE table_name = ? AND constraint_type <> 'NOT NULL'",
    )?;
    let mut rows = stmt.query([&src.table])?;
    while let Some(row) = rows.next()? {
        let kind: String = row.get(0)?;
        if kind != "PRIMARY KEY" {
            return refuse(format!("it has a {} constraint", kind));
        }
    }

    let mut idx = conn.prepare("SELECT index_name FROM duckdb_indexes() WHERE table_name = ?")?;
    let mut rows = idx.query([&src.table])?;
    if let Some(row) = rows.next()? {
        return refuse(format!(
            "index '{}' would be lost",
            row.get::<_, String>(0)?
        ));
    }

    // `duckdb_columns` cannot say whether a column is generated — the catalogue view has
    // no such flag and `information_schema` leaves `is_generated` NULL — but the stored
    // DDL spells it out.  Reading it is what makes this preflight as strict as SQLite's.
    let ddl: String = conn.query_row(
        "SELECT sql FROM duckdb_tables() WHERE table_name = ?",
        [&src.table],
        |r| r.get(0),
    )?;
    for word in ["GENERATED", "CHECK", "COLLATE"] {
        if mentions_word(&ddl, word) {
            return refuse(format!("its definition uses {}", word));
        }
    }

    // The three checks SQLite's side has had all along.  Nothing about them is
    // engine-specific; they were simply never written here, so a rebuild that SQLite
    // refuses went through on DuckDB and left the damage behind.
    let mut views = conn.prepare("SELECT view_name, sql FROM duckdb_views()")?;
    let mut rows = views.query([])?;
    while let Some(row) = rows.next()? {
        if mentions_word(&row.get::<_, String>(1)?, &src.table) {
            return refuse(format!(
                "view '{}' is built on it",
                row.get::<_, String>(0)?
            ));
        }
    }

    let mut fks = conn.prepare(
        "SELECT table_name, constraint_text FROM duckdb_constraints() \
         WHERE constraint_type = 'FOREIGN KEY'",
    )?;
    let mut rows = fks.query([])?;
    while let Some(row) = rows.next()? {
        let other: String = row.get(0)?;
        if !other.eq_ignore_ascii_case(&src.table)
            && mentions_word(&row.get::<_, String>(1)?, &src.table)
        {
            return refuse(format!("table '{}' has a foreign key into it", other));
        }
    }

    let scratch = format!("{}{}", src.table, REBUILD_SUFFIX);
    let mut left = conn.prepare("SELECT 1 FROM duckdb_tables() WHERE table_name = ?")?;
    if left.query([&scratch])?.next()?.is_some() {
        return refuse("a leftover rebuild table from an earlier run is in the way".into());
    }
    Ok(())
}

/// Build the table again with the columns the sheet has, in the order it has them.
///
/// The new definition carries everything the column pragma knows; [`preflight_rebuild_sqlite`]
/// has already refused anything it does not know.  Existing rows are copied by name, so
/// the reordering and any type change happen in the copy — and on SQLite the `rowid` is
/// copied explicitly, which keeps every row identity the plan is about to use.
fn push_rebuild_stmts(
    src: &TableSource,
    df: &DataFrame,
    live_cols: &[DbColumn],
    plan: &mut WritePlan,
) -> Result<()> {
    let table = quote_ident(&src.table);
    let scratch = quote_ident(&format!("{}{}", src.table, REBUILD_SUFFIX));
    let mut push = |sql: String| {
        plan.stmts.push(Stmt {
            display: sql.clone(),
            sql,
            params: Vec::new(),
            kind: StmtKind::Schema,
        });
        plan.schema += 1;
    };

    let inline_pk = live_cols.iter().filter(|c| c.pk).count() == 1;
    let defs: Vec<String> = live_cols.iter().map(|c| column_def(c, inline_pk)).collect();
    push(format!(
        "CREATE TABLE \"{}\" ({})",
        scratch,
        defs.join(", ")
    ));

    // What the table being copied *from* calls its columns depends on when this runs.
    //
    // On SQLite the rebuild goes first, so the old table is still the one that was
    // loaded: columns answer to their load-time names, and a column added this session
    // has no data there yet — it is left out of the copy and the row statements, which
    // run afterwards, fill it.
    //
    // On DuckDB the rebuild goes last, after the ALTERs and the row statements have
    // already run, so every column is already present under its final name and already
    // holds its final values.  Copying by load-time names would read a column that was
    // renamed out from under it, and skipping the added ones would drop everything just
    // written into them.
    let post_alter = src.kind == DbKind::DuckDb;
    let mut targets: Vec<String> = Vec::new();
    let mut sources: Vec<String> = Vec::new();
    if src.kind == DbKind::Sqlite {
        targets.push("rowid".to_string());
        sources.push("rowid".to_string());
    }
    for (meta, col) in df.columns.iter().zip(live_cols) {
        let source = match (post_alter, meta.db_origin.as_deref()) {
            (true, _) => col.name.as_str(),
            (false, Some(origin)) => origin,
            (false, None) => continue,
        };
        targets.push(format!("\"{}\"", quote_ident(&col.name)));
        sources.push(format!("\"{}\"", quote_ident(source)));
    }
    let order = if post_alter {
        // DuckDB assigns its own rowids to the copy, so the row order has to be pinned
        // down or the copy is only incidentally in the same order as the original.
        " ORDER BY rowid".to_string()
    } else {
        String::new()
    };
    push(format!(
        "INSERT INTO \"{}\" ({}) SELECT {} FROM \"{}\"{}",
        scratch,
        targets.join(", "),
        sources.join(", "),
        table,
        order
    ));

    push(format!("DROP TABLE \"{}\"", table));
    if src.kind == DbKind::Sqlite {
        // Without this SQLite helpfully rewrites references to the scratch name inside
        // every other object in the schema — which is the opposite of what a rename
        // back to the original name means here.
        push("PRAGMA legacy_alter_table = ON".to_string());
    }
    push(format!(
        "ALTER TABLE \"{}\" RENAME TO \"{}\"",
        scratch, table
    ));
    if src.kind == DbKind::Sqlite {
        push("PRAGMA legacy_alter_table = OFF".to_string());

        // Indexes and triggers went with the old table; replay their own definitions.
        let conn = open_sqlite(&src.db_path)?;
        let mut stmt = conn.prepare(
            "SELECT sql FROM sqlite_master \
             WHERE tbl_name = ?1 AND sql IS NOT NULL AND type IN ('index', 'trigger') \
             ORDER BY type, name",
        )?;
        let mut rows = stmt.query([&src.table])?;
        while let Some(row) = rows.next()? {
            push(row.get::<_, String>(0)?);
        }
    }

    plan.rebuild = true;
    Ok(())
}

/// Turn a [`SchemaPlan`] into statements, in the only order that cannot collide.
///
/// DROP before RENAME, because renaming `b`→`a` needs the old `a` gone first.  RENAME
/// before ADD, because adding a new `x` needs the old `x` renamed away first.  All of
/// it before the row statements, which is legal because `rowid` survives ADD, DROP and
/// RENAME COLUMN in both engines — verified, not assumed — and which is what lets every
/// UPDATE below name columns as they will finally be called.
fn push_schema_stmts(
    src: &TableSource,
    df: &DataFrame,
    schema: &SchemaPlan,
    live_cols: &[DbColumn],
    plan: &mut WritePlan,
) -> Result<()> {
    if schema.is_empty() {
        return Ok(());
    }
    preflight_schema(src, schema)?;

    // A SQLite rebuild expresses every shape change at once — the new table is created
    // with the final columns, in the final order, with the final types — so the ALTERs
    // below would be redundant work on a table that is about to be dropped.  It goes
    // first because it copies rowids explicitly, which keeps the row identity the
    // statements after it depend on.
    if src.kind == DbKind::Sqlite && needs_rebuild(src.kind, schema) {
        preflight_rebuild_sqlite(src, schema, live_cols)?;
        return push_rebuild_stmts(src, df, live_cols, plan);
    }

    let table = quote_ident(&src.table);
    let mut push = |sql: String| {
        plan.stmts.push(Stmt {
            display: sql.clone(),
            sql,
            params: Vec::new(),
            kind: StmtKind::Schema,
        });
        plan.schema += 1;
    };

    for name in &schema.drops {
        push(format!(
            "ALTER TABLE \"{}\" DROP COLUMN \"{}\"",
            table,
            quote_ident(name)
        ));
    }

    // Renames go out greedily: emit any whose target name is free, repeat.  A stall means
    // the names form a cycle — the user swapped a pair round — and a scratch name breaks
    // it: move one column out of the way and every other rename in the cycle has a free
    // target, after which the parked one takes the name it was waiting for.
    let mut pending: Vec<(String, String)> = schema.renames.clone();
    while !pending.is_empty() {
        let occupied: std::collections::HashSet<&str> =
            pending.iter().map(|(from, _)| from.as_str()).collect();
        let ready: Vec<usize> = pending
            .iter()
            .enumerate()
            .filter(|(_, (from, to))| from == to || !occupied.contains(to.as_str()))
            .map(|(i, _)| i)
            .collect();
        if ready.is_empty() {
            let taken: std::collections::HashSet<&str> = src
                .columns
                .iter()
                .chain(live_cols.iter())
                .map(|c| c.name.as_str())
                .collect();
            let from = pending[0].0.clone();
            let mut scratch = format!("{}{}", from, SWAP_SUFFIX);
            for n in 2.. {
                if !taken.contains(scratch.as_str()) {
                    break;
                }
                scratch = format!("{}{}{}", from, SWAP_SUFFIX, n);
            }
            push(format!(
                "ALTER TABLE \"{}\" RENAME COLUMN \"{}\" TO \"{}\"",
                table,
                quote_ident(&from),
                quote_ident(&scratch)
            ));
            pending[0].0 = scratch;
            continue;
        }
        for i in ready.iter().rev() {
            let (from, to) = pending.remove(*i);
            push(format!(
                "ALTER TABLE \"{}\" RENAME COLUMN \"{}\" TO \"{}\"",
                table,
                quote_ident(&from),
                quote_ident(&to)
            ));
        }
    }

    for &i in &schema.adds {
        let col = &live_cols[i];
        push(format!(
            "ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}",
            table,
            quote_ident(&col.name),
            col.decl_raw
        ));
    }

    // DuckDB changes a column's type in place; SQLite reached the rebuild above.
    for &(i, _) in &schema.retypes {
        let col = &live_cols[i];
        push(format!(
            "ALTER TABLE \"{}\" ALTER COLUMN \"{}\" TYPE {}",
            table,
            quote_ident(&col.name),
            col.decl_raw
        ));
    }

    Ok(())
}

/// Statements that have to run *after* the row changes.
///
/// Only DuckDB has any: it cannot reorder columns either, and its rebuild assigns fresh
/// `rowid`s to the copy — so it has to happen once nothing is addressing rows by the
/// old ones any more.
fn push_post_stmts(
    src: &TableSource,
    df: &DataFrame,
    schema: &SchemaPlan,
    live_cols: &[DbColumn],
    plan: &mut WritePlan,
) -> Result<()> {
    if src.kind != DbKind::DuckDb || !needs_rebuild(src.kind, schema) {
        return Ok(());
    }
    preflight_rebuild_duckdb(src)?;
    push_rebuild_stmts(src, df, live_cols, plan)
}

/// `WHERE key = ?` for one row, `WHERE key IN (?, ?, …)` for several.
fn write_where(
    sql: &mut String,
    display: &mut String,
    params: &mut Vec<Val>,
    key: &str,
    ids: &[i64],
) {
    if let [only] = ids {
        sql.push_str(&format!(" WHERE \"{}\" = ", key));
        display.push_str(&format!(" WHERE \"{}\" = ", key));
        push_val(sql, display, params, Val::Int(*only));
    } else {
        sql.push_str(&format!(" WHERE \"{}\" IN (", key));
        display.push_str(&format!(" WHERE \"{}\" IN (", key));
        for (i, id) in ids.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
                display.push_str(", ");
            }
            push_val(sql, display, params, Val::Int(*id));
        }
        sql.push(')');
        display.push(')');
    }
}
