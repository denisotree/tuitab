//! Central application controller.
//!
//! [`App`] owns the [`crate::sheet::SheetStack`] and all transient UI state.
//! The main event loop lives in [`App::run`]:
//!
//! ```text
//! loop {
//!     ui::render(frame, app);          // draw
//!     event → handle_key_event()       // crossterm → semantic Action
//!     app.handle_action(action);       // mutate state
//! }
//! ```
//!
//! [`App::handle_action`] is the large dispatch table that maps every
//! [`crate::types::Action`] variant to the corresponding state mutation.

mod actions;

use crate::app_state::{
    AggregatorState, ChartState, CopyState, DedupTiebreakerState, ExpressionState, JoinState,
    PartitionState, PivotState, SaveState, SqlConfirmState, TypeSelectState,
};
use crate::data::aggregator::AggregatorKind;
use crate::data::async_loader::{self, LoadEvent};
use crate::data::dataframe::DataFrame;
use crate::data::expression::Expr;
use crate::event::handle_key_event;
use crate::sheet::{Sheet, SheetStack};
use crate::types::{Action, AppMode, ColumnType, SheetType};
use crate::ui;
use crate::ui::text_input::TextInput;
use color_eyre::Result;
use crossterm::event::{self, Event};
use ratatui::widgets::ScrollbarState;
use ratatui::DefaultTerminal;
use regex::Regex;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct App {
    pub stack: SheetStack,
    pub mode: AppMode,
    pub status_message: String,
    pub should_quit: bool,
    pub load_receiver: Option<std::sync::mpsc::Receiver<LoadEvent>>,
    pub background_task: Option<(String, usize, usize)>,
    pub spinner_tick: u8,
    pub pending_action: Option<Action>,
    pub open_in_editor_pending: bool,
    /// Set by the table renderer each frame: `Some((shown, full))` when the
    /// current cursor cell's first-line content width (`full`) exceeds the
    /// display width allocated to its column by the viewport (`shown`).
    /// Read by the status bar to show a clip indicator. None when not clipped.
    pub cursor_cell_overflow: Option<(u16, u16)>,

    pub save: SaveState,
    /// The SQL about to be run against a source database, while the user reviews it.
    pub sql: SqlConfirmState,
    pub aggregator: AggregatorState,
    pub window_fn: crate::app_state::WindowFnState,
    /// Which window function the partition picker is collecting columns for.
    pub pending_window_fn: Option<crate::data::window::WindowFn>,
    pub col_op_literal: bool,
    pub type_select: TypeSelectState,
    pub partition: PartitionState,
    pub expression: ExpressionState,
    pub pivot: PivotState,
    pub chart: ChartState,
    pub join: JoinState,
    pub row_form: crate::app_state::RowFormState,
    pub copy: CopyState,
    pub dedup_tiebreaker: DedupTiebreakerState,
}

impl App {
    fn init(
        stack: SheetStack,
        mode: AppMode,
        status_message: String,
        save: SaveState,
        load_receiver: Option<std::sync::mpsc::Receiver<LoadEvent>>,
    ) -> Self {
        Self {
            stack,
            mode,
            status_message,
            should_quit: false,
            load_receiver,
            background_task: None,
            spinner_tick: 0,
            pending_action: None,
            open_in_editor_pending: false,
            cursor_cell_overflow: None,
            save,
            sql: SqlConfirmState::default(),
            aggregator: AggregatorState::default(),
            window_fn: crate::app_state::WindowFnState::default(),
            pending_window_fn: None,
            col_op_literal: true,
            type_select: TypeSelectState::default(),
            partition: PartitionState::default(),
            expression: ExpressionState::default(),
            pivot: PivotState::default(),
            chart: ChartState::default(),
            join: JoinState::default(),
            row_form: crate::app_state::RowFormState::default(),
            copy: CopyState::default(),
            dedup_tiebreaker: DedupTiebreakerState::default(),
        }
    }
}

fn load_join_context_item_df(
    item: &crate::types::JoinContextItem,
) -> color_eyre::Result<(crate::data::dataframe::DataFrame, String)> {
    use crate::types::JoinContextItem;
    match item {
        JoinContextItem::SqliteTable {
            db_path,
            table_name,
        } => crate::data::io::load_sqlite_table_by_name(db_path, table_name)
            .map(|df| (df, table_name.clone())),
        JoinContextItem::DuckdbTable {
            db_path,
            table_name,
        } => crate::data::io::load_duckdb_table_by_name(db_path, table_name)
            .map(|df| (df, table_name.clone())),
        JoinContextItem::DirectoryFile { file_path } => {
            let label = file_path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            crate::data::io::load_file(file_path, None).map(|df| (df, label))
        }
        JoinContextItem::XlsxSheet {
            xlsx_path,
            sheet_name,
        } => crate::data::io::load_excel_sheet_by_name(xlsx_path, sheet_name)
            .map(|df| (df, sheet_name.clone())),
    }
}

impl App {
    /// Construct `App` by loading a file or directory at `path`.
    ///
    /// For CSV/TSV files larger than 10 MB, loading is deferred to a background
    /// thread so the UI can display a spinner while data is streamed in.
    /// `delimiter` overrides auto-detection for CSV/TSV files.
    pub fn new(path: &Path, delimiter: Option<char>) -> Result<Self> {
        App::new_as(path, delimiter, None)
    }

    /// Open a file that is not there yet: one empty column, and the path remembered as
    /// where `Ctrl+S` will write.
    ///
    /// This is how a database gets made from nothing — `tuitab inventory.sqlite` on a
    /// missing file, then columns, rows, and a save.  It refuses two cases, which is
    /// where a mistyped path shows up: a directory that does not exist (nothing could
    /// be saved there anyway), and an extension tuitab cannot write, *including no
    /// extension at all* — `tuitab notes` is far more likely to be a typo than an
    /// intention.
    fn blank_at(path: &Path) -> Result<Self> {
        use crate::data::column::ColumnMeta;
        use color_eyre::eyre::eyre;

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(eyre!("'{}': no such directory", parent.display()));
            }
        }
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default()
            .to_lowercase();
        let writable = crate::data::doc::Format::from_ext(&ext).is_some()
            || matches!(
                ext.as_str(),
                "csv"
                    | "tsv"
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
            );
        if !writable {
            return Err(eyre!(
                "'{}': no such file, and tuitab cannot create a .{} — try .csv, .json, \
                 .parquet, .xlsx, .sqlite or .duckdb",
                path.display(),
                if ext.is_empty() { "…" } else { &ext }
            ));
        }

        let filename = path
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_default();

        // A real one-column frame, not `DataFrame::empty()`: that one is zero columns
        // wide, which would make adding a row and every column operation a special case.
        let pdf = polars::prelude::DataFrame::new(
            0,
            vec![polars::prelude::Column::new(
                "column_1".into(),
                Vec::<String>::new(),
            )],
        )?;
        let mut dataframe =
            DataFrame::from_parts(pdf, vec![ColumnMeta::new("column_1".to_string())]);
        dataframe.calc_widths(40, 1000);

        let mut root_sheet = Sheet::new(filename.clone(), dataframe);
        // `Action::SaveFile` prefills from this, so Ctrl+S already offers the path the
        // user typed on the command line.
        root_sheet.source_path = Some(path.to_path_buf());

        Ok(Self::init(
            SheetStack::new(root_sheet),
            AppMode::Normal,
            format!(
                "{} does not exist yet — Ctrl+S creates it. 'zi' adds a column, 'o' adds a row.",
                filename
            ),
            SaveState {
                input: TextInput::with_value(filename),
                ..Default::default()
            },
            None,
        ))
    }

    /// Open every file a pattern matches as one sheet.
    ///
    /// The same reading the MCP server gives it, through the same code: a table has to
    /// agree on its columns and the file that does not is named, while markdown pages
    /// are records and are unioned.  Only `tuitab 'data/*.csv'` arrives here — unquoted,
    /// the shell expands first and `from_file_list` shows the listing instead.
    fn from_pattern(
        pattern: &Path,
        delimiter: Option<char>,
        forced: Option<crate::data::doc::Format>,
    ) -> Result<Self> {
        let text = pattern.to_string_lossy().into_owned();
        let delim_byte = delimiter.map(|c| c as u8);
        let ext = pattern
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default()
            .to_lowercase();
        // `--type` names a structured format, and those are tables, not bags of records.
        let records = forced.is_none() && matches!(ext.as_str(), "md" | "markdown");

        // ponytail: a pattern loads on the main thread, so no spinner. The async branch
        // in `new_as` takes a single path and returns a single `Opened`; teaching it to
        // stack is its own job, worth doing when a glob over large files starts hurting.
        let (dataframe, files) = crate::data::io::load_pattern(&text, records, |p| {
            crate::data::io::open_target(p, delim_byte, forced).map(|o| o.df)
        })?;
        let row_count = dataframe.visible_row_count();

        let mut root_sheet = Sheet::new(text.clone(), dataframe);
        root_sheet.source_delimiter = delim_byte;
        // No `source_path`: a pattern is not a file.  Leaving it set is how the pattern
        // used to be mistaken for a new file's name — `Ctrl+S` offering to create one
        // with a `*` in it, `[new]` in the title bar, and `r` reloading a single file
        // out of the several on screen.  Reload therefore says it cannot, which is true.
        root_sheet.save_name_hint = Some(if ext.is_empty() {
            "stacked".to_string()
        } else {
            format!("stacked.{}", ext)
        });

        let hint = root_sheet.save_name_hint.clone().unwrap_or_default();
        Ok(Self::init(
            SheetStack::new(root_sheet),
            AppMode::Normal,
            // The file count is the only evidence the pattern caught what was meant.
            format!(
                "Loaded {} rows from {} files matching {}",
                row_count, files, text
            ),
            SaveState {
                input: TextInput::with_value(hint),
                ..Default::default()
            },
            None,
        ))
    }

    /// Open `path`, optionally forcing a structured format instead of trusting the
    /// extension — this is what `--type yaml deploy.conf` does.
    pub fn new_as(
        path: &Path,
        delimiter: Option<char>,
        forced_format: Option<crate::data::doc::Format>,
    ) -> Result<Self> {
        if !path.exists() {
            // Asked only here: `is_pattern` is a test for `* ? [ ]`, and `report[1].csv`
            // is a name a browser hands out.  A file that is there is never a pattern.
            if crate::data::io::is_pattern(path) {
                return App::from_pattern(path, delimiter, forced_format);
            }
            return App::blank_at(path);
        }
        let delim_byte = delimiter.map(|c| c as u8);

        let filename = path
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_default();

        // Phase 10: use async loading for large files
        let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let is_dir = std::fs::metadata(path).map(|m| m.is_dir()).unwrap_or(false);
        const ASYNC_THRESHOLD: u64 = 10 * 1024 * 1024; // 10 MB

        if is_dir {
            // Load directory listing
            let dataframe = crate::data::io::load_directory(path)?;
            let row_count = dataframe.visible_row_count();
            let mut root_sheet = Sheet::new(filename.clone(), dataframe);
            root_sheet.is_dir_sheet = true;
            root_sheet.source_path = Some(path.to_path_buf());
            Ok(Self::init(
                SheetStack::new(root_sheet),
                AppMode::Normal,
                format!("Loaded directory '{}' ({} items)", filename, row_count),
                SaveState {
                    input: TextInput::with_value(filename),
                    ..Default::default()
                },
                None,
            ))
        } else if file_size > ASYNC_THRESHOLD {
            // Size decides where the work happens, never what the file is: the loader
            // opens it exactly as the branch below would, and `poll_async_load` copies
            // the result onto the sheet whole — the tree and the container path
            // included, which is what it used to drop (#43).
            let rx =
                async_loader::load_in_background(path.to_path_buf(), delim_byte, forced_format);
            let placeholder = DataFrame::empty();
            let mut root_sheet = Sheet::new(filename.clone(), placeholder);
            root_sheet.source_path = Some(path.to_path_buf());
            root_sheet.source_delimiter = delim_byte;
            Ok(Self::init(
                SheetStack::new(root_sheet),
                AppMode::Loading,
                format!("Loading {}...", path.display()),
                SaveState {
                    input: TextInput::with_value(filename.clone()),
                    ..Default::default()
                },
                Some(rx),
            ))
        } else {
            let opened = crate::data::io::open_target(path, delim_byte, forced_format)?;

            let row_count = opened.df.visible_row_count();
            let mut root_sheet = Sheet::new(filename.clone(), opened.df);
            root_sheet.doc = opened.doc;
            root_sheet.sqlite_db_path = opened.sqlite_db_path;
            root_sheet.duckdb_db_path = opened.duckdb_db_path;
            root_sheet.xlsx_db_path = opened.xlsx_db_path;
            root_sheet.source_path = Some(path.to_path_buf());
            root_sheet.source_delimiter = delim_byte;
            let status_message = if root_sheet.xlsx_db_path.is_some() {
                format!("Loaded '{}' — {} sheets", filename, row_count)
            } else if root_sheet.sqlite_db_path.is_some() || root_sheet.duckdb_db_path.is_some() {
                format!("Loaded '{}' — {} tables", filename, row_count)
            } else {
                format!("Loaded {} rows", row_count)
            };
            Ok(Self::init(
                SheetStack::new(root_sheet),
                AppMode::Normal,
                status_message,
                SaveState {
                    input: TextInput::with_value(filename),
                    ..Default::default()
                },
                None,
            ))
        }
    }

    /// Construct `App` by reading typed data from stdin.
    ///
    /// `data_type` is `"csv"`, `"tsv"`, `"txt"`, or one of the structured formats
    /// (`"json"`, `"jsonl"`, `"ndjson"`, `"yaml"`, `"toml"`), which arrive with a
    /// document tree attached and are therefore editable and convertible.
    /// `delimiter` overrides auto-detection for CSV/TSV input.
    pub fn from_stdin_typed(data_type: &str, delimiter: Option<char>) -> Result<Self> {
        let delim_byte = delimiter.map(|c| c as u8);
        let (dataframe, doc) = crate::data::io::load_from_stdin_with_doc(data_type, delim_byte)?;
        let row_count = dataframe.visible_row_count();
        let title = "stdin".to_string();
        let mut root_sheet = Sheet::new(title.clone(), dataframe);
        root_sheet.doc = doc;
        Ok(Self::init(
            SheetStack::new(root_sheet),
            AppMode::Normal,
            format!("Loaded {} rows from stdin", row_count),
            SaveState {
                input: TextInput::with_value(title),
                ..Default::default()
            },
            None,
        ))
    }

    /// Construct `App` from an explicit list of files (multi-file CLI argument).
    pub fn from_file_list(paths: Vec<PathBuf>, delimiter: Option<char>) -> Result<Self> {
        let _ = delimiter; // delimiter not applicable to the file-list sheet itself
        let n = paths.len();
        let (dataframe, abs_paths) = crate::data::io::load_files_list(&paths)?;
        let title = format!("Selected files ({})", n);
        let mut root_sheet = Sheet::new(title.clone(), dataframe);
        root_sheet.is_dir_sheet = true;
        root_sheet.explicit_row_paths = Some(abs_paths);
        Ok(Self::init(
            SheetStack::new(root_sheet),
            AppMode::Normal,
            format!("{} files", n),
            SaveState {
                input: TextInput::with_value(title),
                ..Default::default()
            },
            None,
        ))
    }

    // ── Main event loop ────────────────────────────────────────────────────────

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        loop {
            self.poll_async_load();

            // Process pending calculating actions BEFORE drawing
            // so the user sees the result immediately.
            if self.mode == AppMode::Calculating && self.pending_action.is_some() {
                let action = self.pending_action.take().unwrap();
                self.handle_action(action);
                // Don't block — loop back to draw the result
                continue;
            }

            terminal.draw(|f| ui::render(f, self))?;

            let has_bg = self.mode == AppMode::Loading
                || self.mode == AppMode::Calculating
                || self.background_task.is_some();
            if has_bg {
                if crossterm::event::poll(Duration::from_millis(100))? {
                    if let Event::Key(key) = event::read()? {
                        let action = handle_key_event(key, self.mode, self.stack.can_pop());
                        self.handle_action(action);
                    }
                }
                self.spinner_tick = self.spinner_tick.wrapping_add(1);
            } else {
                if let Event::Key(key) = event::read()? {
                    let action = handle_key_event(key, self.mode, self.stack.can_pop());
                    self.handle_action(action);
                }
            }

            if self.open_in_editor_pending {
                self.open_in_editor_pending = false;
                if let Err(e) = self.do_open_in_editor(terminal) {
                    self.status_message = format!("Editor error: {}", e);
                }
            }

            if self.should_quit {
                break;
            }
        }
        Ok(())
    }

    /// Phase 10: check if the background loader has finished.
    fn poll_async_load(&mut self) {
        if let Some(ref rx) = self.load_receiver {
            match rx.try_recv() {
                Ok(LoadEvent::Complete(Ok(opened))) => {
                    let row_count = opened.df.visible_row_count();
                    let xlsx = opened.xlsx_db_path.is_some();
                    let is_container = opened.sqlite_db_path.is_some()
                        || opened.duckdb_db_path.is_some()
                        || opened.xlsx_db_path.is_some();
                    let s = self.stack.active_mut();
                    s.dataframe = opened.df;
                    s.doc = opened.doc;
                    // The container path is what makes the listing openable — a sheet
                    // that arrives without it looks right and does nothing on `Enter`.
                    s.sqlite_db_path = opened.sqlite_db_path;
                    s.duckdb_db_path = opened.duckdb_db_path;
                    s.xlsx_db_path = opened.xlsx_db_path;
                    // A listing is not a file among siblings, so it offers none.
                    if is_container {
                        s.dir_source_path = None;
                    }
                    s.dataframe.calc_widths(40, 1000);
                    s.reapply_sort();
                    let vis = s.dataframe.visible_row_count();
                    s.scroll_state = ScrollbarState::new(vis.saturating_sub(1));
                    s.table_state.select(Some(0));
                    self.mode = AppMode::Normal;
                    // The same three phrasings the foreground open uses — which one a
                    // file gets must not depend on how big it happens to be.
                    let name = s.title.clone();
                    self.status_message = if is_container {
                        let what = if xlsx { "sheets" } else { "tables" };
                        format!("Loaded '{}' — {} {}", name, row_count, what)
                    } else {
                        format!("Loaded {} rows", row_count)
                    };
                    self.load_receiver = None;
                }
                Ok(LoadEvent::Complete(Err(e))) => {
                    self.mode = AppMode::Normal;
                    self.status_message = format!("Load error: {}", e);
                    self.load_receiver = None;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    self.mode = AppMode::Normal;
                    self.status_message = "Load thread disconnected".to_string();
                    self.load_receiver = None;
                }
            }
        }
    }

    fn reload_file(&mut self) {
        let (source_path, source_delimiter) = {
            let s = self.stack.active();
            (s.source_path.clone(), s.source_delimiter)
        };

        let Some(path) = source_path else {
            self.status_message = "Cannot reload: no source path".to_string();
            return;
        };

        // Reloading a dived-into sheet would give it a fresh tree while its parents keep
        // the old one, and the two would silently diverge.  Pop back to the root first.
        if self
            .stack
            .active()
            .doc
            .as_ref()
            .is_some_and(|d| !d.view.anchor.is_empty())
        {
            self.status_message =
                "Cannot reload inside a node — go back to the top sheet first".to_string();
            return;
        }

        let meta = std::fs::metadata(&path);
        let is_dir = meta.as_ref().map(|m| m.is_dir()).unwrap_or(false);
        let file_size = meta.map(|m| m.len()).unwrap_or(0);

        // Nothing is thrown away yet: a reload that fails must leave the sheet exactly as
        // it was.  Each arm below clears the undo history only once it has rows to put in
        // place of the old ones.  The sort order and the search are kept — both resolve
        // by column name, and reloading is not a request to forget what you were looking
        // for.
        let saved_row = self.stack.active().table_state.selected().unwrap_or(0);

        if is_dir {
            match crate::data::io::load_directory(&path) {
                Ok(df) => {
                    let row_count = df.visible_row_count();
                    let s = self.stack.active_mut();
                    s.undo_stack.clear();
                    s.redo_stack.clear();
                    s.dataframe = df;
                    s.dataframe.calc_widths(40, 1000);
                    s.reapply_sort();
                    let vis = s.dataframe.visible_row_count();
                    s.scroll_state = ScrollbarState::new(vis.saturating_sub(1));
                    let clamped = saved_row.min(vis.saturating_sub(1));
                    s.table_state.select(Some(clamped));
                    s.top_row = clamped;
                    s.source_path = Some(path);
                    self.status_message = format!("Reloaded — {} items", row_count);
                }
                Err(e) => self.status_message = format!("Reload failed: {}", e),
            }
        } else if file_size > 10 * 1024 * 1024 {
            {
                let s = self.stack.active_mut();
                s.undo_stack.clear();
                s.redo_stack.clear();
                s.dataframe = DataFrame::empty();
                s.source_path = Some(path.clone());
                s.source_delimiter = source_delimiter;
            }
            // Reload re-reads what the sheet already is, and a sheet built from a
            // forced format keeps its tree — reopening by extension would drop it.
            let forced = self.stack.active().doc.as_ref().map(|d| d.format());
            self.load_receiver = Some(async_loader::load_in_background(
                path,
                source_delimiter,
                forced,
            ));
            self.mode = AppMode::Loading;
            self.status_message = "Reloading...".to_string();
        } else {
            match crate::data::io::load_file_with_doc(&path, source_delimiter) {
                Ok((df, doc)) => {
                    let row_count = df.visible_row_count();
                    let s = self.stack.active_mut();
                    s.undo_stack.clear();
                    s.redo_stack.clear();
                    s.dataframe = df;
                    // The tree must be replaced too: keeping the old one would leave
                    // row paths pointing into a document that no longer matches the
                    // table, and a later save would write the pre-reload contents.
                    s.doc = doc;
                    s.dataframe.calc_widths(40, 1000);
                    s.reapply_sort();
                    let vis = s.dataframe.visible_row_count();
                    s.scroll_state = ScrollbarState::new(vis.saturating_sub(1));
                    let clamped = saved_row.min(vis.saturating_sub(1));
                    s.table_state.select(Some(clamped));
                    s.top_row = clamped;
                    s.source_path = Some(path);
                    s.source_delimiter = source_delimiter;
                    self.mode = AppMode::Normal;
                    self.status_message = format!("Reloaded — {} rows", row_count);
                }
                Err(e) => self.status_message = format!("Reload failed: {}", e),
            }
        }
    }

    // ── Action dispatcher ──────────────────────────────────────────────────────

    /// Dispatch a semantic [`Action`] to mutate application state.
    ///
    /// Called once per key event from [`App::run`].
    /// The action is produced by [`crate::event::handle_key_event`].
    pub fn handle_action(&mut self, action: Action) {
        let action = match self.handle_navigation_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_search_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_expression_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_column_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_join_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_chart_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_aggregator_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_edit_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_type_select_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_clipboard_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_io_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_pivot_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_row_form_action(action) {
            Some(a) => a,
            None => return,
        };
        let action = match self.handle_selection_action(action) {
            Some(a) => a,
            None => return,
        };

        match action {
            Action::Quit => self.pop_sheet(),
            Action::ConfirmQuitYes => {
                self.should_quit = true;
            }
            Action::ConfirmQuitNo => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::PopSheet => self.pop_sheet(),
            Action::Undo => {
                let s = self.stack.active_mut();
                if s.pop_undo() {
                    self.status_message = "Undo successful".to_string();
                } else {
                    self.status_message = "Nothing to undo".to_string();
                }
            }
            Action::Redo => {
                let s = self.stack.active_mut();
                if s.pop_redo() {
                    self.status_message = "Redo successful".to_string();
                } else {
                    self.status_message = "Nothing to redo".to_string();
                }
            }

            // ── Sorting ───────────────────────────────────────────────────────
            Action::SortAscending => self.sort_cursor_column(false, false),
            Action::SortDescending => self.sort_cursor_column(true, false),
            Action::AddSortKeyAscending => self.sort_cursor_column(false, true),
            Action::AddSortKeyDescending => self.sort_cursor_column(true, true),
            Action::OpenRow => {
                let s = self.stack.active();
                let is_freq = matches!(s.sheet_type, SheetType::FrequencyTable { .. });
                let is_pivot = matches!(s.sheet_type, SheetType::PivotTable { .. });
                let is_dir = s.is_dir_sheet;
                let is_sqlite = s.sqlite_db_path.is_some();
                let is_duckdb = s.duckdb_db_path.is_some();
                let is_xlsx = s.xlsx_db_path.is_some();

                if is_freq && self.stack.depth() >= 2 {
                    self.drill_down_freq_value();
                } else if is_pivot && self.stack.depth() >= 2 {
                    self.drill_down_pivot_value();
                } else if is_dir {
                    self.open_directory_row();
                } else if is_sqlite {
                    self.open_sqlite_table_row();
                } else if is_duckdb {
                    self.open_duckdb_table_row();
                } else if is_xlsx {
                    self.open_excel_sheet_row();
                } else if s.doc_hits.is_some() {
                    self.open_search_hit();
                } else if s.doc.is_some() {
                    self.dive_into_node(false);
                } else {
                    // FEATURE F5: Transpose row on Enter if not special sheet
                    self.transpose_row();
                }
            }
            Action::OpenCell => {
                if self.stack.active().doc.is_some() {
                    self.dive_into_node(true);
                } else {
                    self.status_message = "Not a JSON/YAML/TOML sheet".to_string();
                }
            }
            Action::CycleViewMode => self.cycle_view_mode(),
            Action::StartDocSearch => self.start_doc_search(),
            Action::StartPathGoto => self.start_path_goto(),
            Action::StartQuery => self.start_query(),
            Action::ApplyQuery => self.apply_query(),
            Action::CancelQuery => {
                self.mode = AppMode::Normal;
                self.stack.active_mut().query_input.clear();
            }
            Action::QueryInputChar(c) => self.stack.active_mut().query_input.insert_char(c),
            Action::QueryBackspace => self.stack.active_mut().query_input.delete_backward(),
            Action::QueryForwardDelete => self.stack.active_mut().query_input.delete_forward(),
            Action::QueryCursorLeft => self.stack.active_mut().query_input.move_cursor_left(),
            Action::QueryCursorRight => self.stack.active_mut().query_input.move_cursor_right(),
            Action::QueryCursorStart => self.stack.active_mut().query_input.move_cursor_start(),
            Action::QueryCursorEnd => self.stack.active_mut().query_input.move_cursor_end(),
            Action::ApplyPathGoto => self.apply_path_goto(),
            Action::CancelPathGoto => {
                self.mode = AppMode::Normal;
                self.stack.active_mut().path_input.clear();
            }
            Action::PathInputChar(c) => self.stack.active_mut().path_input.insert_char(c),
            Action::PathBackspace => self.stack.active_mut().path_input.delete_backward(),
            Action::PathForwardDelete => self.stack.active_mut().path_input.delete_forward(),
            Action::PathCursorLeft => self.stack.active_mut().path_input.move_cursor_left(),
            Action::PathCursorRight => self.stack.active_mut().path_input.move_cursor_right(),
            Action::PathCursorStart => self.stack.active_mut().path_input.move_cursor_start(),
            Action::PathCursorEnd => self.stack.active_mut().path_input.move_cursor_end(),
            Action::ApplyDocSearch => self.apply_doc_search(),
            Action::CancelDocSearch => {
                self.mode = AppMode::Normal;
                self.stack.active_mut().search_input.clear();
            }
            Action::ExpandColumn => self.expand_column(),
            Action::ContractColumn => self.contract_column(),
            Action::ResetSort => {
                let s = self.stack.active_mut();
                s.push_undo();
                s.dataframe.reset_sort();
                s.sort_keys.clear();
                s.table_state.select(Some(0));
            }
            Action::ReloadFile => self.reload_file(),
            Action::TransposeRow => self.transpose_row(),
            // Transposing replaces the table wholesale, which a document-backed
            // sheet cannot survive: the projection stops matching the document,
            // after which editing refuses for the rest of the session and the
            // result disappears at the next reprojection.
            Action::TransposeTable if self.stack.active().doc.is_some() => {
                self.reject_on_doc_sheet("Transposing");
            }
            Action::TransposeTable => self.transpose_table(),
            Action::DescribeSheet => self.describe_sheet(),

            // ── Frequency table (push new Sheet) ──────────────────────────────
            Action::OpenFrequencyTable => {
                if self.mode == AppMode::Calculating {
                    self.open_frequency_table();
                } else {
                    self.mode = AppMode::Calculating;
                    self.pending_action = Some(Action::OpenFrequencyTable);
                }
            }
            Action::OpenWindowFnSelect => {
                self.window_fn.select_index = 0;
                self.window_fn.desc = false;
                self.window_fn.order_by = None;
                self.window_fn.order_index = 0;
                self.mode = AppMode::WindowFnSelect;
                self.status_message =
                    "Pick a window function (Enter to choose, Esc to cancel)".to_string();
            }
            Action::WindowFnSelectUp => {
                self.window_fn.select_index = self.window_fn.select_index.saturating_sub(1);
            }
            Action::WindowFnSelectDown => {
                let last = crate::data::window::WindowFn::all().len() - 1;
                self.window_fn.select_index = (self.window_fn.select_index + 1).min(last);
            }
            Action::CancelWindowFnSelect => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::ApplyWindowFnSelect => {
                let all = crate::data::window::WindowFn::all();
                let function = all[self.window_fn.select_index.min(all.len() - 1)];
                self.pending_window_fn = Some(function);
                if function.uses_order_by() {
                    // "Relative to what came before" needs to know what before
                    // means. Running totals used to read the file's own order,
                    // so the only way to total by date was to sort the table —
                    // which is a change to the table the user did not ask for.
                    self.mode = AppMode::WindowOrderSelect;
                } else if function.uses_direction() {
                    // A rank has to know which end is first, and the answer is
                    // not guessable from the column: `zw rank` on salary means
                    // top earner for one question and lowest paid for another.
                    self.mode = AppMode::WindowDirSelect;
                } else {
                    // Hand off to the same partition picker `zF` uses, so a
                    // window can be scoped to a group without a second kind of
                    // dialog.
                    self.open_partition_select();
                }
            }
            Action::WindowOrderSelectUp => {
                self.window_fn.order_index = self.window_fn.order_index.saturating_sub(1)
            }
            Action::WindowOrderSelectDown => {
                let last = self.stack.active().dataframe.columns.len();
                self.window_fn.order_index = (self.window_fn.order_index + 1).min(last);
            }
            Action::ApplyWindowOrderSelect => {
                // Row 0 is "the table's order"; column `i` sits at `i + 1`.
                self.window_fn.order_by = self
                    .window_fn
                    .order_index
                    .checked_sub(1)
                    .and_then(|i| self.stack.active().dataframe.columns.get(i))
                    .map(|c| c.name.clone());
                if self.window_fn.order_by.is_some() {
                    self.mode = AppMode::WindowDirSelect;
                } else {
                    // Nothing to run in a direction.
                    self.open_partition_select();
                }
            }
            Action::CancelWindowOrderSelect => {
                self.pending_window_fn = None;
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::WindowDirSelectUp => self.window_fn.desc = false,
            Action::WindowDirSelectDown => self.window_fn.desc = true,
            Action::ApplyWindowDirSelect => self.open_partition_select(),
            Action::CancelWindowDirSelect => {
                // Same discipline as `CancelWindowFnSelect`: a function left
                // armed here turned the next `zF` into a rank.
                self.pending_window_fn = None;
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::OpenGroupBy => {
                if self.mode == AppMode::Calculating {
                    self.open_group_by();
                } else {
                    self.mode = AppMode::Calculating;
                    self.pending_action = Some(Action::OpenGroupBy);
                }
            }
            Action::OpenMultiFrequencyTable => {
                if self.mode == AppMode::Calculating {
                    self.open_multi_frequency_table();
                } else {
                    self.mode = AppMode::Calculating;
                    self.pending_action = Some(Action::OpenMultiFrequencyTable);
                }
            }

            Action::None => {}
            // Navigation, search, expression, column, and join actions are
            // handled by sub-dispatchers above and never reach this match.
            _ => {}
        }
    }

    // ── Column width adjustment ───────────────────────────────────────────────

    fn adjust_column_width(&mut self) {
        use crate::data::column::ColumnWidthMode;
        let s = self.stack.active_mut();
        let col = s.cursor_col;
        if col >= s.dataframe.columns.len() {
            return;
        }
        let col_name = s.dataframe.columns[col].name.clone();
        match s.dataframe.columns[col].width_mode {
            ColumnWidthMode::Default => {
                // Default → Fit: scan all rows for full content width (header width is the floor).
                s.dataframe.calc_column_width(col, u16::MAX, usize::MAX);
                s.dataframe.columns[col].width_mode = ColumnWidthMode::Fit;
                let width = s.dataframe.columns[col].width;
                self.status_message = format!("Column '{}' width: fit ({})", col_name, width);
            }
            ColumnWidthMode::Fit => {
                // Fit → Default: restore load-time width.
                let default_w = s.dataframe.columns[col].default_width;
                if default_w > 0 {
                    s.dataframe.columns[col].width = default_w;
                } else {
                    s.dataframe.calc_column_width(col, 40, 1000);
                }
                s.dataframe.columns[col].width_mode = ColumnWidthMode::Default;
                self.status_message = format!("Column '{}' width: default", col_name);
            }
        }
    }

    fn adjust_all_column_widths(&mut self) {
        use crate::data::column::ColumnWidthMode;
        let s = self.stack.active_mut();
        let all_default = s
            .dataframe
            .columns
            .iter()
            .all(|c| c.width_mode == ColumnWidthMode::Default);
        if all_default {
            // All Default → fit all to full content width.
            s.dataframe.calc_widths(u16::MAX, usize::MAX);
            for col_meta in s.dataframe.columns.iter_mut() {
                col_meta.width_mode = ColumnWidthMode::Fit;
            }
            self.mode = AppMode::Normal;
            self.status_message = "All column widths: fit".to_string();
        } else {
            // Any non-Default → restore all to Default width.
            // For columns whose default_width was never cached, compute it now using
            // the same calc params used at load time.
            for col_meta in s.dataframe.columns.iter_mut() {
                if col_meta.default_width > 0 {
                    col_meta.width = col_meta.default_width;
                }
                col_meta.width_mode = ColumnWidthMode::Default;
            }
            let needs_calc: Vec<usize> = s
                .dataframe
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.default_width == 0)
                .map(|(i, _)| i)
                .collect();
            for idx in needs_calc {
                s.dataframe.calc_column_width(idx, 40, 1000);
            }
            self.mode = AppMode::Normal;
            self.status_message = "All column widths: default".to_string();
        }
    }

    fn adjust_precision(&mut self, delta: i8) {
        let s = self.stack.active_mut();
        s.push_undo();
        let col = s.cursor_col;
        if col < s.dataframe.columns.len() {
            let meta = &mut s.dataframe.columns[col];
            if !matches!(
                meta.col_type,
                crate::types::ColumnType::Float
                    | crate::types::ColumnType::Percentage
                    | crate::types::ColumnType::Currency
            ) {
                self.mode = AppMode::Normal;
                self.status_message =
                    "Precision only applies to Float, Percentage, Currency".to_string();
                return;
            }
            if delta > 0 {
                meta.precision = meta.precision.saturating_add(1).min(6);
            } else {
                meta.precision = meta.precision.saturating_sub(1);
            }
            s.dataframe.modified = true;
            s.dataframe.aggregates_cache = None;
            let p = meta.precision;
            s.dataframe.calc_column_width(col, 100, 1000);
            self.status_message = format!("Precision set to {}", p);
        }
        self.mode = AppMode::Normal;
    }

    /// Share of the column's total, for the whole table (`zf`).
    fn create_pct_column(&mut self) {
        self.add_window(crate::data::window::WindowFn::PctOfTotal, Vec::new());
    }

    fn open_partition_select(&mut self) {
        let s = self.stack.active();
        let col_idx = s.cursor_col;
        if col_idx >= s.dataframe.columns.len() {
            return;
        }

        // Only `zF` — a share of a total — needs a number here. `zw` reaches
        // this picker too, and eight of its twelve functions read no numbers
        // (`row_number` reads no column at all), so judging them by this gate
        // refused most of the feature with a message about percent columns the
        // user never invoked. Those are checked by the window layer, which
        // knows which function was asked for and says so by name.
        if self.pending_window_fn.is_none() {
            let meta = &s.dataframe.columns[col_idx];
            let is_numeric = matches!(
                meta.col_type,
                crate::types::ColumnType::Integer
                    | crate::types::ColumnType::Float
                    | crate::types::ColumnType::Percentage
                    | crate::types::ColumnType::Currency
            );

            if !is_numeric {
                self.mode = AppMode::Normal;
                self.status_message =
                    "Partitioned percent column only works for numeric columns".to_string();
                return;
            }
        }

        self.partition.select_index = 0;
        self.partition.selected.clear();
        self.mode = AppMode::PartitionSelect;
    }

    /// Apply whatever the partition picker was collecting columns for.
    ///
    /// `zF` goes straight to a partitioned share; `zw` sets
    /// [`Self::pending_window_fn`] first, so both arrive here.
    fn apply_partitioned_pct(&mut self) {
        let mut partitions: Vec<String> = self.partition.selected.iter().cloned().collect();
        // Sorted so the generated column name does not depend on click order.
        partitions.sort();
        let function = self
            .pending_window_fn
            .take()
            .unwrap_or(crate::data::window::WindowFn::PctOfTotal);
        self.add_window(function, partitions);
    }

    /// Add a window column over the column under the cursor.
    ///
    /// Same function the MCP server's `window` operation calls, so the two
    /// cannot answer differently.
    fn add_window(&mut self, function: crate::data::window::WindowFn, over: Vec<String>) {
        let s = self.stack.active_mut();
        let col_idx = s.cursor_col;
        if col_idx >= s.dataframe.columns.len() {
            self.mode = AppMode::Normal;
            return;
        }
        let col_name = s.dataframe.columns[col_idx].name.clone();

        let order_by: Vec<String> = match &self.window_fn.order_by {
            Some(name) if function.uses_order_by() => vec![name.clone()],
            _ => Vec::new(),
        };

        // Everything that changes the answer goes in the name, or the second
        // window over one column collides with the first and is refused —
        // a running total by date and one by id are two different columns.
        let desc = function.uses_direction() && self.window_fn.desc;
        let as_name = [
            Some(col_name.as_str()),
            (!over.is_empty()).then_some("by"),
            (!over.is_empty()).then(|| over.join("_")).as_deref(),
            order_by.first().map(|_| "ordered"),
            order_by.first().map(String::as_str),
            Some(function.name()),
            desc.then_some("desc"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("_");

        let spec = crate::data::window::Spec {
            function,
            col: Some(col_name),
            over,
            order_by: order_by.clone(),
            as_name: Some(as_name.clone()),
            desc,
            offset: 1,
        };

        match crate::data::window::add_window_column(&s.dataframe, &spec) {
            Ok(df) => {
                s.push_undo();
                s.dataframe = df;
                self.status_message = format!("Created column '{}'", as_name);
            }
            Err(e) => self.status_message = e,
        }
        self.mode = AppMode::Normal;
    }

    // ── Navigation helpers ─────────────────────────────────────────────────────

    fn move_cursor_down(&mut self) {
        let s = self.stack.active_mut();
        let max = s.dataframe.visible_row_count().saturating_sub(1);
        let cur = s.table_state.selected().unwrap_or(0);
        let next = (cur + 1).min(max);
        s.table_state.select(Some(next));

        // Let UI rendering pull top_row down if cursor exceeds it,
        // but we can enforce a basic scrolling rule here:
        // (the precise area.height is only known in render)
        if next > s.top_row + 50 {
            s.top_row += 1; // Fallback, real adjustment happens in table_view
        }

        s.scroll_state = s.scroll_state.position(next);
    }

    fn move_cursor_up(&mut self) {
        let s = self.stack.active_mut();
        let cur = s.table_state.selected().unwrap_or(0);
        let next = cur.saturating_sub(1);
        s.table_state.select(Some(next));

        if next < s.top_row {
            s.top_row = next;
        }

        s.scroll_state = s.scroll_state.position(next);
    }

    fn move_cursor_right(&mut self) {
        let s = self.stack.active_mut();
        let max = s.dataframe.col_count().saturating_sub(1);
        s.cursor_col = (s.cursor_col + 1).min(max);
        s.table_state.select_column(Some(s.cursor_col));
    }

    fn move_cursor_left(&mut self) {
        let s = self.stack.active_mut();
        s.cursor_col = s.cursor_col.saturating_sub(1);
        s.table_state.select_column(Some(s.cursor_col));
    }

    fn page_down(&mut self) {
        let s = self.stack.active_mut();
        let max = s.dataframe.visible_row_count().saturating_sub(1);
        let cur = s.table_state.selected().unwrap_or(0);
        let next = (cur + 20).min(max);
        s.table_state.select(Some(next));
        s.top_row = (s.top_row + 20).min(max);
        s.scroll_state = s.scroll_state.position(next);
    }

    fn page_up(&mut self) {
        let s = self.stack.active_mut();
        let cur = s.table_state.selected().unwrap_or(0);
        let next = cur.saturating_sub(20);
        s.table_state.select(Some(next));
        s.top_row = s.top_row.saturating_sub(20);
        s.scroll_state = s.scroll_state.position(next);
    }

    // ── Sheet stack ────────────────────────────────────────────────────────────

    fn pop_sheet(&mut self) {
        if self.stack.can_pop() {
            self.stack.pop();
            self.refresh_doc_projection();
            if self.chart.drill_return {
                self.chart.drill_return = false;
                self.mode = AppMode::Chart;
                let s = self.stack.active();
                let col_name = s.dataframe.columns[s.cursor_col].name.clone();
                self.status_message = format!(
                    "Chart: {} — ← → navigate | Enter: drill down | v/q/Esc: exit",
                    col_name
                );
            } else {
                self.mode = AppMode::Normal;
                self.status_message = format!(
                    "Returned to '{}' (depth {})",
                    self.stack.active().title,
                    self.stack.depth()
                );
            }
        } else {
            self.mode = AppMode::ConfirmQuit;
            self.status_message = "Quit? Press 'y' to confirm, 'n' to cancel".to_string();
        }
    }

    // ── Search (/) ─────────────────────────────────────────────────────────────

    fn apply_search(&mut self) {
        let s = self.stack.active_mut();
        let pattern = s.search_input.as_str().to_string();
        let col = s.search_col();
        s.search_input.clear();

        // Validate regex first
        if let Err(e) = Regex::new(&format!("(?i){}", pattern)) {
            self.status_message = format!("Invalid regex: {}", e);
            self.mode = AppMode::Normal;
            return;
        }

        s.search_pattern = Some(pattern.clone());
        if s.dataframe.visible_row_count() == 0 {
            self.status_message = "No data".to_string();
            self.mode = AppMode::Normal;
            return;
        }

        // Vectorized: get all matching display-row indices in one pass
        let pi_pattern = format!("(?i){}", pattern);
        let matches = s.dataframe.find_matching_rows(col, &pi_pattern);

        let start = s.table_state.selected().unwrap_or(0);
        // Pick first match after current position, wrapping around
        let found = matches
            .iter()
            .find(|&&r| r > start)
            .or_else(|| matches.first());

        if let Some(&row) = found {
            s.table_state.select(Some(row));
            s.scroll_state = s.scroll_state.position(row);
            self.status_message = format!("/{}", pattern);
        } else {
            self.status_message = format!("Not found: {}", pattern);
        }
        self.mode = AppMode::Normal;
    }

    fn search_next(&mut self) {
        let s = self.stack.active_mut();
        let pattern = match &s.search_pattern {
            Some(p) => p.clone(),
            None => {
                self.status_message = "No active search (press / first)".to_string();
                return;
            }
        };
        let col = s.search_col();
        if s.dataframe.visible_row_count() == 0 {
            return;
        }

        let pi_pattern = format!("(?i){}", pattern);
        let matches = s.dataframe.find_matching_rows(col, &pi_pattern);
        let start = s.table_state.selected().unwrap_or(0);
        let found = matches
            .iter()
            .find(|&&r| r > start)
            .or_else(|| matches.first());

        if let Some(&row) = found {
            s.table_state.select(Some(row));
            s.scroll_state = s.scroll_state.position(row);
            self.status_message = format!("/{} (next)", pattern);
        } else {
            self.status_message = format!("Not found: {}", pattern);
        }
    }

    fn search_prev(&mut self) {
        let s = self.stack.active_mut();
        let pattern = match &s.search_pattern {
            Some(p) => p.clone(),
            None => {
                self.status_message = "No active search (press / first)".to_string();
                return;
            }
        };
        let col = s.search_col();
        if s.dataframe.visible_row_count() == 0 {
            return;
        }

        let pi_pattern = format!("(?i){}", pattern);
        let matches = s.dataframe.find_matching_rows(col, &pi_pattern);
        let start = s.table_state.selected().unwrap_or(0);
        // Pick last match before current position, wrapping to last match overall
        let found = matches
            .iter()
            .rev()
            .find(|&&r| r < start)
            .or_else(|| matches.last());

        if let Some(&row) = found {
            s.table_state.select(Some(row));
            s.scroll_state = s.scroll_state.position(row);
            self.status_message = format!("/{} (prev)", pattern);
        } else {
            self.status_message = format!("Not found: {}", pattern);
        }
    }

    // ── Select by value (,) ────────────────────────────────────────────────────

    fn select_by_value(&mut self) {
        let s = self.stack.active_mut();
        let display_row = s.table_state.selected().unwrap_or(0);
        if display_row >= s.dataframe.visible_row_count() {
            return;
        }
        let col = s.cursor_col;
        let target = DataFrame::anyvalue_to_string_fmt(&s.dataframe.get_val(display_row, col));

        let matching_display_rows = s.dataframe.find_rows_by_value(col, &target);
        let count = matching_display_rows.len();

        let physical_rows: Vec<usize> = matching_display_rows
            .iter()
            .filter(|&&di| di < s.dataframe.row_order.len())
            .map(|&di| s.dataframe.row_order[di])
            .collect();

        let all_selected = !physical_rows.is_empty()
            && physical_rows
                .iter()
                .all(|idx| s.dataframe.selected_rows.contains(idx));

        if all_selected {
            for idx in &physical_rows {
                s.dataframe.selected_rows.remove(idx);
            }
            self.status_message = format!(
                "Deselected {} rows where {} = '{}'",
                count, s.dataframe.columns[col].name, target
            );
        } else {
            for idx in physical_rows {
                s.dataframe.selected_rows.insert(idx);
            }
            self.status_message = format!(
                "Selected {} rows where {} = '{}'",
                count, s.dataframe.columns[col].name, target
            );
        }
    }

    // ── Select by regex (|) ────────────────────────────────────────────────────

    fn apply_select_by_regex(&mut self) {
        let s = self.stack.active_mut();
        let input = s.select_regex_input.as_str().to_string();
        let col = s.cursor_col;

        if input.starts_with("!=") || input.starts_with("!= ") {
            let expr_str = input.strip_prefix("!= ").unwrap_or(&input[2..]);
            // Free text, so the per-row interpreter is allowed: a user may well
            // write `year(hire_date) > 2020`, which Polars cannot lower.
            match Expr::parse(expr_str).and_then(|expr| {
                crate::data::filter::select_rows(
                    &s.dataframe,
                    &expr,
                    crate::data::filter::Fallback::Allowed,
                )
            }) {
                Ok(selected_indices) => {
                    let count = selected_indices.len();
                    for display_idx in selected_indices {
                        if display_idx < s.dataframe.row_order.len() {
                            s.dataframe
                                .selected_rows
                                .insert(s.dataframe.row_order[display_idx]);
                        }
                    }
                    self.status_message = format!("Selected {} rows by expression", count);
                }
                Err(e) => {
                    self.status_message = format!("Expression error: {}", e);
                }
            }
            s.select_regex_input.clear();
            self.mode = AppMode::Normal;
            return;
        }

        let pattern = input;
        if let Err(e) = Regex::new(&format!("(?i){}", pattern)) {
            self.status_message = format!("Invalid regex: {}", e);
            s.select_regex_input.clear();
            self.mode = AppMode::Normal;
            return;
        }

        // Vectorized regex match via Polars str().contains()
        let pi_pattern = format!("(?i){}", pattern);
        let matching_display_rows = s.dataframe.find_matching_rows(col, &pi_pattern);
        let count = matching_display_rows.len();
        for display_idx in matching_display_rows {
            if display_idx < s.dataframe.row_order.len() {
                s.dataframe
                    .selected_rows
                    .insert(s.dataframe.row_order[display_idx]);
            }
        }
        self.status_message = format!("Selected {} rows matching /{}/", count, pattern);
        s.select_regex_input.clear();
        self.mode = AppMode::Normal;
    }

    // ── Expression / computed column (=) ───────────────────────────────────────

    fn apply_expression(&mut self) {
        let input = self.stack.active_mut().expr_input.as_str().to_string();

        if input.is_empty() {
            self.mode = AppMode::Normal;
            self.status_message.clear();
            return;
        }

        if self.expression.history.last() != Some(&input) {
            self.expression.history.push(input.clone());
        }
        self.expression.history_idx = None;
        self.expression.autocomplete_candidates.clear();

        match Expr::parse(&input) {
            Ok(expr) => {
                let s = self.stack.active_mut();
                s.push_undo();
                let name = format!("={}", input);
                let col = s.cursor_col;
                match s.dataframe.add_computed_column(&name, &expr, col) {
                    Ok(()) => {
                        self.status_message = format!("Added column '{}'", name);
                    }
                    Err(e) => {
                        self.status_message = format!("Expression error: {}", e);
                    }
                }
            }
            Err(e) => {
                self.status_message = format!("Parse error: {}", e);
            }
        }
        self.stack.active_mut().expr_input.clear();
        self.mode = AppMode::Normal;
    }

    fn expr_autocomplete(&mut self) {
        let s = self.stack.active_mut();
        if self.expression.autocomplete_candidates.is_empty() {
            let input_str = s.expr_input.as_str();
            let rpos = input_str.rfind(|c: char| !c.is_alphanumeric() && c != '_');
            let (prefix, word) = if let Some(p) = rpos {
                input_str.split_at(p + 1)
            } else {
                ("", input_str)
            };

            let word_lower = word.to_lowercase();
            let mut prefix_matches = Vec::new();
            let mut contains_matches = Vec::new();
            for col in &s.dataframe.columns {
                let lower = col.name.to_lowercase();
                if lower.starts_with(&word_lower) {
                    prefix_matches.push(col.name.clone());
                } else if lower.contains(&word_lower) {
                    contains_matches.push(col.name.clone());
                }
            }
            prefix_matches.sort();
            contains_matches.sort();
            prefix_matches.extend(contains_matches);
            let matches = prefix_matches;

            if matches.is_empty() {
                return;
            }
            self.expression.autocomplete_candidates = matches;
            self.expression.autocomplete_idx = 0;
            self.expression.autocomplete_prefix = prefix.to_string();
        } else {
            self.expression.autocomplete_idx = (self.expression.autocomplete_idx + 1)
                % self.expression.autocomplete_candidates.len();
        }

        let completion = &self.expression.autocomplete_candidates[self.expression.autocomplete_idx];
        let new_val = format!("{}{}", self.expression.autocomplete_prefix, completion);
        s.expr_input = TextInput::with_value(new_val);
    }

    fn select_regex_autocomplete(&mut self) {
        let s = self.stack.active_mut();
        let input_str = s.select_regex_input.as_str();

        // Autocomplete is only meaningful in expression mode (input starts with !=)
        if !input_str.starts_with("!=") {
            return;
        }

        if self.expression.autocomplete_candidates.is_empty() {
            let rpos = input_str.rfind(|c: char| !c.is_alphanumeric() && c != '_');
            let (prefix, word) = if let Some(p) = rpos {
                input_str.split_at(p + 1)
            } else {
                ("", input_str)
            };

            let word_lower = word.to_lowercase();
            let mut prefix_matches = Vec::new();
            let mut contains_matches = Vec::new();
            for col in &s.dataframe.columns {
                let lower = col.name.to_lowercase();
                if lower.starts_with(&word_lower) {
                    prefix_matches.push(col.name.clone());
                } else if lower.contains(&word_lower) {
                    contains_matches.push(col.name.clone());
                }
            }
            prefix_matches.sort();
            contains_matches.sort();
            prefix_matches.extend(contains_matches);
            let matches = prefix_matches;

            if matches.is_empty() {
                return;
            }
            self.expression.autocomplete_candidates = matches;
            self.expression.autocomplete_idx = 0;
            self.expression.autocomplete_prefix = prefix.to_string();
        } else {
            self.expression.autocomplete_idx = (self.expression.autocomplete_idx + 1)
                % self.expression.autocomplete_candidates.len();
        }

        let completion = &self.expression.autocomplete_candidates[self.expression.autocomplete_idx];
        let new_val = format!("{}{}", self.expression.autocomplete_prefix, completion);
        s.select_regex_input = TextInput::with_value(new_val);
    }

    fn expr_history_prev(&mut self) {
        if self.expression.history.is_empty() {
            return;
        }

        let mut reset_input = false;
        if let Some(mut idx) = self.expression.history_idx {
            if idx > 0 {
                idx -= 1;
                self.expression.history_idx = Some(idx);
                reset_input = true;
            }
        } else {
            self.expression.history_idx = Some(self.expression.history.len() - 1);
            reset_input = true;
        }

        if reset_input {
            let s = self.stack.active_mut();
            if let Some(idx) = self.expression.history_idx {
                s.expr_input = TextInput::with_value(self.expression.history[idx].clone());
            }
        }
    }

    fn expr_history_next(&mut self) {
        if let Some(idx) = self.expression.history_idx {
            let next_idx = idx + 1;
            if next_idx < self.expression.history.len() {
                self.expression.history_idx = Some(next_idx);
                self.stack.active_mut().expr_input =
                    TextInput::with_value(self.expression.history[next_idx].clone());
            } else {
                self.expression.history_idx = None;
                self.stack.active_mut().expr_input.clear();
            }
        }
    }

    // ── Frequency table (push Sheet) ──────────────────────────────────────────

    fn open_frequency_table(&mut self) {
        let s = self.stack.active();
        let col = s.cursor_col;
        let col_name = s.dataframe.columns[col].name.clone();

        // Collect columns that have active aggregators (for per-group aggs)
        let aggregated_cols: Vec<(usize, Vec<AggregatorKind>)> = s
            .dataframe
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.aggregators.is_empty())
            .map(|(i, c)| (i, c.aggregators.clone()))
            .collect();

        match s.dataframe.build_frequency_table(col, &aggregated_cols) {
            Ok((pdf, columns)) => {
                let row_count = pdf.height();
                let mut df = DataFrame::from_parts(pdf, columns);
                // Inherit original column type for Value column
                df.columns[0].col_type = s.dataframe.columns[col].col_type;
                df.columns[1].col_type = ColumnType::Integer;
                df.calc_widths(40, 500);

                let mut freq_sheet = Sheet::new(format!("Freq: {}", col_name), df);
                freq_sheet.sort_keys = vec![("Count".to_string(), true)]; // pre-sorted
                freq_sheet.sheet_type = SheetType::FrequencyTable {
                    group_cols: vec![col_name.clone()],
                };
                self.stack.push(freq_sheet);
                self.mode = AppMode::Normal;
                self.status_message = format!(
                    "Frequency table for '{}' ({} distinct)",
                    col_name, row_count
                );
            }
            Err(e) => {
                self.status_message = format!("Error building frequency table: {}", e);
                self.mode = AppMode::Normal;
            }
        }
    }

    /// Group by the pinned columns, computing the aggregates marked on the
    /// other columns with `+`.
    ///
    /// The sibling of `gF`, and the difference is worth knowing: a frequency
    /// table ranks groups by how many rows fall in each, always carrying a
    /// `Count` and a share. This gives exactly the aggregates asked for, in
    /// the order asked for. Same engine as the MCP server's `group_by`.
    fn open_group_by(&mut self) {
        let s = self.stack.active();

        let by: Vec<String> = s
            .dataframe
            .columns
            .iter()
            .filter(|c| c.pinned)
            .map(|c| c.name.clone())
            .collect();

        if by.is_empty() {
            self.status_message = "Pin the columns to group by with '!' first".to_string();
            self.mode = AppMode::Normal;
            return;
        }

        let agg: Vec<crate::data::group::AggSpec> = s
            .dataframe
            .columns
            .iter()
            .filter(|c| !c.pinned)
            .flat_map(|c| {
                c.aggregators
                    .iter()
                    .map(|kind| crate::data::group::AggSpec {
                        col: c.name.clone(),
                        kind: *kind,
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        if agg.is_empty() {
            self.status_message =
                "Mark at least one column with '+' to say what to aggregate".to_string();
            self.mode = AppMode::Normal;
            return;
        }

        match crate::data::group::group_by(&s.dataframe, &by, &agg) {
            Ok(mut grouped) => {
                let rows = grouped.visible_row_count();
                grouped.calc_widths(40, 1000);
                let title = format!("Group by {}", by.join(", "));
                self.stack.push(crate::sheet::Sheet::new(title, grouped));
                self.status_message = format!("Grouped into {} rows", rows);
            }
            Err(e) => self.status_message = e,
        }
        self.mode = AppMode::Normal;
    }

    fn open_multi_frequency_table(&mut self) {
        let s = self.stack.active();
        let pinned_cols: Vec<usize> = s
            .dataframe
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.pinned)
            .map(|(i, _)| i)
            .collect();

        if pinned_cols.is_empty() {
            self.status_message = "No pinned columns to group by".to_string();
            self.mode = AppMode::Normal;
            return;
        }

        let mut aggregated_cols = Vec::new();
        for (i, c) in s.dataframe.columns.iter().enumerate() {
            if !c.aggregators.is_empty() {
                aggregated_cols.push((i, c.aggregators.clone()));
            }
        }

        match s
            .dataframe
            .build_multi_frequency_table(&pinned_cols, &aggregated_cols)
        {
            Ok((pdf, columns)) => {
                let row_count = pdf.height();
                let mut new_df = crate::data::dataframe::DataFrame::from_parts(pdf, columns);
                new_df.calc_widths(40, 1000);

                let pinned_names: Vec<&str> = pinned_cols
                    .iter()
                    .map(|&c| s.dataframe.columns[c].name.as_str())
                    .collect();
                let title = format!("MultiFreq: {}", pinned_names.join(", "));

                let mut freq_sheet = crate::sheet::Sheet::new(title, new_df);
                freq_sheet.sort_keys = vec![("Count".to_string(), true)];
                freq_sheet.sheet_type = SheetType::FrequencyTable {
                    group_cols: pinned_names.iter().map(|&s| s.to_string()).collect(),
                };
                self.stack.push(freq_sheet);
                self.mode = AppMode::Normal;
                self.status_message = format!("MultiFreq created ({} distinct groups)", row_count);
            }
            Err(e) => {
                self.status_message = format!("Error building multi-freq table: {}", e);
                self.mode = AppMode::Normal;
            }
        }
    }
    pub fn open_directory_row(&mut self) {
        self.open_directory_row_as(None);
    }

    /// Open the selected file from a directory listing.  `forced` overrides both the
    /// extension and content sniffing — the `zo` escape hatch for a file whose name and
    /// contents are both uninformative.
    pub fn open_directory_row_as(&mut self, forced: Option<crate::data::doc::Format>) {
        let s = self.stack.active();
        let df = &s.dataframe;

        // Ensure this is a directory view
        if !s.is_dir_sheet {
            return;
        }
        if df.columns.len() < 5
            || df.columns[0].name != "Name"
            || df.columns[1].name != "Is Directory"
            || df.columns[4].name != "Supported"
        {
            return;
        }

        if let Some(row_idx) = s.table_state.selected() {
            let name_val = df.get_val(row_idx, 0);
            let is_dir_val = df.get_val(row_idx, 1);
            let supported_val = df.get_val(row_idx, 4);

            let name = crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(&name_val);
            let is_dir =
                crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(&is_dir_val) == "true";
            let supported =
                crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(&supported_val) == "true";

            // For synthetic file-list sheets, use the stored absolute path directly.
            // Otherwise prefer the full source_path of the directory sheet, falling
            // back to the title only if neither is available.
            let resolve_base = |s: &crate::sheet::Sheet| -> std::path::PathBuf {
                if let Some(ref p) = s.source_path {
                    p.clone()
                } else if s.title == "." || s.title.is_empty() {
                    std::path::PathBuf::from(".")
                } else {
                    std::path::PathBuf::from(&s.title)
                }
            };
            let target_path = if let Some(ref paths) = s.explicit_row_paths {
                paths
                    .get(row_idx)
                    .cloned()
                    .unwrap_or_else(|| resolve_base(s).join(&name))
            } else {
                resolve_base(s).join(&name)
            };

            if is_dir {
                match crate::data::io::load_directory(&target_path) {
                    Ok(new_df) => {
                        let mut new_sheet = crate::sheet::Sheet::new(
                            target_path.to_string_lossy().into_owned(),
                            new_df,
                        );
                        new_sheet.is_dir_sheet = true;
                        new_sheet.source_path = Some(target_path.clone());
                        self.stack.push(new_sheet);
                    }
                    Err(e) => {
                        self.status_message = format!("Failed to open directory: {}", e);
                    }
                }
            // An explicit "open as" overrides the listing's own idea of what it can
            // handle — saying "this is YAML" is the whole point of the escape hatch.
            } else if supported || forced.is_some() {
                match crate::data::io::open_target(&target_path, None, forced) {
                    Ok(opened) => {
                        let mut new_sheet = crate::sheet::Sheet::new(
                            target_path.to_string_lossy().into_owned(),
                            opened.df,
                        );
                        new_sheet.doc = opened.doc;
                        new_sheet.sqlite_db_path = opened.sqlite_db_path;
                        new_sheet.duckdb_db_path = opened.duckdb_db_path;
                        new_sheet.xlsx_db_path = opened.xlsx_db_path;
                        // Track the parent directory for regular data files so J can offer siblings.
                        if new_sheet.sqlite_db_path.is_none()
                            && new_sheet.duckdb_db_path.is_none()
                            && new_sheet.xlsx_db_path.is_none()
                        {
                            new_sheet.dir_source_path =
                                target_path.parent().map(|p| p.to_path_buf());
                        }
                        self.stack.push(new_sheet);
                    }
                    Err(e) => {
                        self.status_message = format!("Failed to open file: {}", e);
                    }
                }
            } else {
                self.status_message = format!("Unsupported file: {}", name);
            }
        }
    }

    pub fn open_sqlite_table_row(&mut self) {
        let s = self.stack.active();

        let db_path = match &s.sqlite_db_path {
            Some(p) => p.clone(),
            None => return,
        };

        if s.dataframe.columns.is_empty() || s.dataframe.columns[0].name != "Table" {
            return;
        }

        let selected_row = match s.table_state.selected() {
            Some(r) => r,
            None => return,
        };

        let table_name_val = s.dataframe.get_val(selected_row, 0);
        let table_name = crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(&table_name_val);

        if table_name.is_empty() {
            return;
        }

        match crate::data::io::load_sqlite_table_full(&db_path, &table_name) {
            Ok((new_df, source)) => {
                let row_count = new_df.visible_row_count();
                let mut new_sheet = crate::sheet::Sheet::new(
                    format!("{} :: {}", db_path.display(), table_name),
                    new_df,
                );
                new_sheet.sqlite_source_path = Some(db_path.clone());
                // Without this the save dialog falls back to the title and offers
                // "/x/db.sqlite :: users" as the filename, which has no usable
                // extension and fails every time.
                new_sheet.source_path = Some(db_path.clone());
                new_sheet.table_source = source;
                self.stack.push(new_sheet);
                self.status_message = format!("Opened table '{}' ({} rows)", table_name, row_count);
            }
            Err(e) => {
                self.status_message = format!("Failed to open table '{}': {}", table_name, e);
            }
        }
    }

    fn collect_join_context_items(&self) -> Vec<crate::types::JoinContextItem> {
        use crate::types::JoinContextItem;
        let s = self.stack.active();
        let mut items: Vec<JoinContextItem> = Vec::new();

        // SQLite: current sheet is a table (sqlite_source_path) or the overview (sqlite_db_path)
        let sqlite_path = s.sqlite_source_path.as_ref().or(s.sqlite_db_path.as_ref());
        if let Some(path) = sqlite_path {
            let current_table = if s.sqlite_source_path.is_some() {
                s.title
                    .rsplit(" :: ")
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string()
            } else {
                String::new()
            };
            if let Ok(names) = crate::data::io::sqlite_table_names(path) {
                for name in names {
                    if name != current_table {
                        items.push(JoinContextItem::SqliteTable {
                            db_path: path.clone(),
                            table_name: name,
                        });
                    }
                }
            }
        }

        // DuckDB: same pattern
        let duckdb_path = s.duckdb_source_path.as_ref().or(s.duckdb_db_path.as_ref());
        if let Some(path) = duckdb_path {
            let current_table = if s.duckdb_source_path.is_some() {
                s.title
                    .rsplit(" :: ")
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string()
            } else {
                String::new()
            };
            if let Ok(names) = crate::data::io::duckdb_table_names(path) {
                for name in names {
                    if name != current_table {
                        items.push(JoinContextItem::DuckdbTable {
                            db_path: path.clone(),
                            table_name: name,
                        });
                    }
                }
            }
        }

        // xlsx: current sheet is a sheet (xlsx_source_path) or the overview (xlsx_db_path)
        let xlsx_path = s.xlsx_source_path.as_ref().or(s.xlsx_db_path.as_ref());
        if let Some(path) = xlsx_path {
            let current_sheet = if s.xlsx_source_path.is_some() {
                s.title
                    .rsplit(" :: ")
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string()
            } else {
                String::new()
            };
            if let Ok(names) = crate::data::io::excel_sheet_names(path) {
                for name in names {
                    if name != current_sheet {
                        items.push(JoinContextItem::XlsxSheet {
                            xlsx_path: path.clone(),
                            sheet_name: name,
                        });
                    }
                }
            }
        }

        // Directory: current sheet is a file opened from a dir (dir_source_path) or is a dir listing
        let dir_path = s.dir_source_path.as_ref().or(if s.is_dir_sheet {
            s.source_path.as_ref()
        } else {
            None
        });
        // For is_dir_sheet without source_path, use the title as directory path
        let dir_path_owned: Option<std::path::PathBuf> = dir_path.cloned().or_else(|| {
            if s.is_dir_sheet && !s.title.is_empty() {
                Some(std::path::PathBuf::from(&s.title))
            } else {
                None
            }
        });
        if let Some(dir) = dir_path_owned {
            let current_file = if s.dir_source_path.is_some() {
                s.source_path
                    .as_ref()
                    .and_then(|p| p.file_name())
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default()
            } else {
                String::new()
            };
            if let Ok(read_dir) = std::fs::read_dir(&dir) {
                let supported_exts = [
                    "csv", "tsv", "json", "jsonl", "ndjson", "ldjson", "yaml", "yml", "toml",
                    "parquet", "xlsx", "xls", "xlsm", "xlsb", "sqlite", "sqlite3", "db", "duckdb",
                    "ddb", "txt",
                ];
                let mut paths: Vec<std::path::PathBuf> = read_dir
                    .filter_map(|e| e.ok())
                    .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                    .map(|e| e.path())
                    .filter(|p| {
                        let ext = p
                            .extension()
                            .and_then(|e| e.to_str())
                            .unwrap_or("")
                            .to_lowercase();
                        supported_exts.contains(&ext.as_str())
                    })
                    .collect();
                paths.sort();
                for file_path in paths {
                    let fname = file_path
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    if fname != current_file {
                        items.push(JoinContextItem::DirectoryFile { file_path });
                    }
                }
            }
        }

        items
    }

    fn execute_join(&mut self) {
        use crate::data::join::JoinType;
        let join_type = JoinType::all()[self.join.type_index];
        let left_keys = self.join.left_keys.clone();
        let right_keys = self.join.right_keys.clone();
        let other_title = self.join.other_title.clone();

        let left_df = self.stack.active().dataframe.clone();
        let right_df = match self.join.other_df.take() {
            Some(df) => df,
            None => {
                self.status_message = "JOIN: no right-hand table loaded".to_string();
                self.mode = AppMode::Normal;
                return;
            }
        };

        let mismatch = left_keys
            .iter()
            .zip(&right_keys)
            .find_map(|(lk, rk)| crate::data::join::key_type_mismatch(&left_df, lk, &right_df, rk));
        if let Some(why) = mismatch {
            self.join.other_df = Some(right_df);
            self.status_message = format!(
                "{} impossible: {}. Change the column type (t) so both keys match, then run it again",
                join_type.sql_name(),
                why
            );
            self.mode = AppMode::JoinSelectRightKeys;
            return;
        }

        match crate::data::join::join_dataframes(
            &left_df,
            &right_df,
            &left_keys,
            &right_keys,
            join_type,
        ) {
            Ok(result_df) => {
                let row_count = result_df.visible_row_count();
                let left_title = self.stack.active().title.clone();
                let result_title =
                    format!("{} {} {}", left_title, join_type.sql_name(), other_title);
                let new_sheet = crate::sheet::Sheet::new(result_title, result_df);
                self.stack.push(new_sheet);

                // Continue chained join if items are queued
                if !self.join.pending_queue.is_empty() {
                    let next = self.join.pending_queue.remove(0);
                    match load_join_context_item_df(&next) {
                        Ok((df, title)) => {
                            self.join.other_df = Some(df);
                            self.join.other_title = title;
                            self.join.left_keys.clear();
                            self.join.right_keys.clear();
                            self.join.left_key_index = 0;
                            self.join.right_key_index = 0;
                            self.mode = AppMode::JoinSelectType;
                            let remaining = self.join.pending_queue.len();
                            self.status_message = if remaining > 0 {
                                format!("JOIN: {} more table(s) to add — select type", remaining)
                            } else {
                                "JOIN: select join type".to_string()
                            };
                        }
                        Err(e) => {
                            self.join.pending_queue.clear();
                            self.mode = AppMode::Normal;
                            self.status_message = format!(
                                "JOIN result: {} rows (next load failed: {})",
                                row_count, e
                            );
                        }
                    }
                } else {
                    self.mode = AppMode::Normal;
                    self.status_message = match join_type {
                        JoinType::Anti if row_count == 0 => {
                            format!(
                                "ANTI JOIN: every row of {} found in {}",
                                left_title, other_title
                            )
                        }
                        JoinType::Anti => format!(
                            "ANTI JOIN: {} rows of {} not found in {}",
                            row_count, left_title, other_title
                        ),
                        JoinType::Semi => format!(
                            "SEMI JOIN: {} rows of {} found in {}",
                            row_count, left_title, other_title
                        ),
                        JoinType::Diff => {
                            let [same, changed, removed, added] =
                                crate::data::join::diff_counts(&self.stack.active().dataframe);
                            format!(
                                "DIFF: {} same, {} changed, {} removed, {} added",
                                same, changed, removed, added
                            )
                        }
                        _ => format!("JOIN result: {} rows", row_count),
                    };
                }
            }
            Err(e) => {
                self.join.other_df = Some(right_df); // restore so user can try again
                self.status_message = format!("JOIN error: {}", e);
                self.mode = AppMode::JoinSelectRightKeys;
            }
        }
    }

    pub fn open_duckdb_table_row(&mut self) {
        let s = self.stack.active();

        let db_path = match &s.duckdb_db_path {
            Some(p) => p.clone(),
            None => return,
        };

        if s.dataframe.columns.is_empty() || s.dataframe.columns[0].name != "Table" {
            return;
        }

        let selected_row = match s.table_state.selected() {
            Some(r) => r,
            None => return,
        };

        let table_name_val = s.dataframe.get_val(selected_row, 0);
        let table_name = crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(&table_name_val);

        if table_name.is_empty() {
            return;
        }

        match crate::data::io::load_duckdb_table_full(&db_path, &table_name) {
            Ok((new_df, source)) => {
                let row_count = new_df.visible_row_count();
                let mut new_sheet = crate::sheet::Sheet::new(
                    format!("{} :: {}", db_path.display(), table_name),
                    new_df,
                );
                new_sheet.duckdb_source_path = Some(db_path.clone());
                // See open_sqlite_table_row: the save dialog needs a real path.
                new_sheet.source_path = Some(db_path.clone());
                new_sheet.table_source = source;
                self.stack.push(new_sheet);
                self.status_message = format!("Opened table '{}' ({} rows)", table_name, row_count);
            }
            Err(e) => {
                self.status_message = format!("Failed to open table '{}': {}", table_name, e);
            }
        }
    }

    pub fn open_excel_sheet_row(&mut self) {
        let s = self.stack.active();

        let xlsx_path = match &s.xlsx_db_path {
            Some(p) => p.clone(),
            None => return,
        };

        if s.dataframe.columns.is_empty() || s.dataframe.columns[0].name != "Sheet" {
            return;
        }

        let selected_row = match s.table_state.selected() {
            Some(r) => r,
            None => return,
        };

        let sheet_name_val = s.dataframe.get_val(selected_row, 0);
        let sheet_name = crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(&sheet_name_val);

        if sheet_name.is_empty() {
            return;
        }

        match crate::data::io::load_excel_sheet_by_name(&xlsx_path, &sheet_name) {
            Ok(new_df) => {
                let row_count = new_df.visible_row_count();
                let mut new_sheet = crate::sheet::Sheet::new(
                    format!("{} :: {}", xlsx_path.display(), sheet_name),
                    new_df,
                );
                new_sheet.xlsx_source_path = Some(xlsx_path.clone());
                // See open_sqlite_table_row: the save dialog needs a real path.
                new_sheet.source_path = Some(xlsx_path.clone());
                self.stack.push(new_sheet);
                self.status_message = format!("Opened sheet '{}' ({} rows)", sheet_name, row_count);
            }
            Err(e) => {
                self.status_message = format!("Failed to open sheet '{}': {}", sheet_name, e);
            }
        }
    }

    fn drill_down_freq_value(&mut self) {
        let s = self.stack.active();
        let selected_row = s.table_state.selected().unwrap_or(0);
        if selected_row >= s.dataframe.visible_row_count() {
            return;
        }

        let mut key_cols = Vec::new();
        let mut key_values = Vec::new();

        // The key columns in a freq table are those before the "Count" column.
        for (i, col) in s.dataframe.columns.iter().enumerate() {
            if col.name == "Count" {
                break;
            }
            key_cols.push(col.name.clone());
            key_values.push(DataFrame::anyvalue_to_string_fmt(
                &s.dataframe.get_val(selected_row, i),
            ));
        }

        if key_cols.is_empty() {
            return;
        }

        if let Some(mut parent_df) = self.stack.clone_parent_dataframe() {
            // Map key columns to their indices in the parent dataframe
            let mut parent_col_indices = Vec::new();
            for kc in &key_cols {
                if let Some(idx) = parent_df.columns.iter().position(|c| &c.name == kc) {
                    parent_col_indices.push(idx);
                } else {
                    self.status_message = format!("Column {} not found in parent", kc);
                    return;
                }
            }

            // Vectorized intersection of matches
            let mut display_matches: Option<std::collections::HashSet<usize>> = None;
            for (i, &parent_col_idx) in parent_col_indices.iter().enumerate() {
                let matches_for_col = parent_df.find_rows_by_value(parent_col_idx, &key_values[i]);
                if let Some(ref mut current_matches) = display_matches {
                    let new_matches: std::collections::HashSet<usize> =
                        matches_for_col.into_iter().collect();
                    current_matches.retain(|idx| new_matches.contains(idx));
                    if current_matches.is_empty() {
                        break;
                    }
                } else {
                    display_matches = Some(matches_for_col.into_iter().collect());
                }
            }

            let mut matching_indices: Vec<usize> = Vec::new();
            if let Some(matches) = display_matches {
                // Iterate in visible order to preserve parent's sort order
                for display_idx in 0..parent_df.visible_row_count() {
                    if matches.contains(&display_idx) {
                        matching_indices.push(parent_df.row_order[display_idx]);
                    }
                }
            }

            if matching_indices.is_empty() {
                self.status_message = "No matching rows found".to_string();
                return;
            }

            parent_df.row_order = matching_indices.clone().into();
            parent_df.original_order = matching_indices.into(); // Reset sort base to this filtered set
            parent_df.aggregates_cache = None;

            let vals_str = key_values.join(", ");
            let cols_str = key_cols.join(", ");
            let mut sheet =
                crate::sheet::Sheet::new(format!("Filter: {} = {}", cols_str, vals_str), parent_df);
            // From the *parent*, which is whose frame was cloned — the frequency or
            // pivot sheet in between has no table behind it.
            if let Some(parent) = self.stack.parent() {
                sheet.inherit_db_origin(parent);
            }
            self.stack.push(sheet);
            self.status_message = format!("Drilled down into {} = {}", cols_str, vals_str);
        }
    }

    fn drill_down_pivot_value(&mut self) {
        let (index_cols, pivot_col_name) = {
            let s = self.stack.active();
            if let SheetType::PivotTable {
                index_cols,
                pivot_col,
                ..
            } = &s.sheet_type
            {
                (index_cols.clone(), pivot_col.clone())
            } else {
                return;
            }
        };

        let s = self.stack.active();
        let selected_row = s.table_state.selected().unwrap_or(0);
        if selected_row >= s.dataframe.visible_row_count() {
            return;
        }

        let mut key_cols = index_cols.clone();
        let mut key_values = Vec::new();

        // 1. Get values for index columns from the current row
        for name in &index_cols {
            if let Some(idx) = s.dataframe.columns.iter().position(|c| &c.name == name) {
                key_values.push(DataFrame::anyvalue_to_string_fmt(
                    &s.dataframe.get_val(selected_row, idx),
                ));
            }
        }

        // 2. If the cursor is on a value column (not an index column), add the pivot column filter
        let current_col_name = s.dataframe.columns[s.cursor_col].name.clone();
        if !index_cols.contains(&current_col_name) {
            key_cols.push(pivot_col_name.clone());
            key_values.push(current_col_name);
        }

        if let Some(mut parent_df) = self.stack.clone_parent_dataframe() {
            let mut parent_col_indices = Vec::new();
            for kc in &key_cols {
                if let Some(idx) = parent_df.columns.iter().position(|c| &c.name == kc) {
                    parent_col_indices.push(idx);
                } else {
                    self.status_message = format!("Column {} not found in parent", kc);
                    return;
                }
            }

            let mut display_matches: Option<std::collections::HashSet<usize>> = None;
            for (i, &parent_col_idx) in parent_col_indices.iter().enumerate() {
                let matches_for_col = parent_df.find_rows_by_value(parent_col_idx, &key_values[i]);
                if let Some(ref mut current_matches) = display_matches {
                    let new_matches: std::collections::HashSet<usize> =
                        matches_for_col.into_iter().collect();
                    current_matches.retain(|idx| new_matches.contains(idx));
                    if current_matches.is_empty() {
                        break;
                    }
                } else {
                    display_matches = Some(matches_for_col.into_iter().collect());
                }
            }

            let mut matching_indices: Vec<usize> = Vec::new();
            if let Some(matches) = display_matches {
                for display_idx in 0..parent_df.visible_row_count() {
                    if matches.contains(&display_idx) {
                        matching_indices.push(parent_df.row_order[display_idx]);
                    }
                }
            }

            if matching_indices.is_empty() {
                self.status_message = "No matching rows found".to_string();
                return;
            }

            parent_df.row_order = matching_indices.clone().into();
            parent_df.original_order = matching_indices.into();
            parent_df.aggregates_cache = None;

            let vals_str = key_values.join(", ");
            let cols_str = key_cols.join(", ");
            let mut sheet =
                crate::sheet::Sheet::new(format!("Filter: {} = {}", cols_str, vals_str), parent_df);
            // From the *parent*, which is whose frame was cloned — the frequency or
            // pivot sheet in between has no table behind it.
            if let Some(parent) = self.stack.parent() {
                sheet.inherit_db_origin(parent);
            }
            self.stack.push(sheet);
            self.status_message = format!("Drilled down into {} = {}", cols_str, vals_str);
        }
    }

    fn transpose_row(&mut self) {
        let s = self.stack.active();
        let selected_row = s.table_state.selected().unwrap_or(0);
        if selected_row >= s.dataframe.visible_row_count() {
            return;
        }
        let physical_row = s.dataframe.row_order[selected_row];

        match crate::data::transpose::transpose_row(&s.dataframe, physical_row) {
            Ok(df) => {
                let sheet = crate::sheet::Sheet::new(format!("Row {}", physical_row), df);
                self.stack.push(sheet);
                self.status_message = format!("Transposed row {}", physical_row);
            }
            Err(e) => self.status_message = e,
        }
    }

    fn transpose_table(&mut self) {
        let transposed = crate::data::transpose::transpose_table(&self.stack.active().dataframe);

        match transposed {
            Ok(df) => {
                let rows = df.visible_row_count();
                let cols = df.columns.len();
                let s = self.stack.active_mut();
                s.push_undo();
                // Replaces the sheet's data rather than pushing a new sheet, so
                // pressing T again inverts what is on screen.
                s.dataframe = df;
                s.sort_keys.clear();
                s.cursor_col = 0;
                s.top_row = 0;
                s.left_col = 0;
                s.table_state.select(Some(0));
                self.status_message = format!("Transposed: {} rows, {} columns", rows, cols);
            }
            Err(e) => self.status_message = e,
        }
    }

    /// Sort by the column under the cursor.
    ///
    /// `append` is the difference between `[`/`]`, which start a fresh sort, and
    /// `z[`/`z]`, which add a less significant key to the one already running.
    /// Sorting by the same column twice replaces its direction rather than
    /// listing it twice.
    fn sort_cursor_column(&mut self, descending: bool, append: bool) {
        let s = self.stack.active_mut();
        s.push_undo();
        let col = s.cursor_col;

        let name = s.dataframe.columns[col].name.clone();

        if append {
            s.sort_keys.retain(|(n, _)| *n != name);
            s.sort_keys.push((name, descending));
        } else {
            s.sort_keys = vec![(name, descending)];
        }

        let resolved = s.resolved_sort_keys();
        let s = self.stack.active_mut();
        if let Err(e) = s.dataframe.sort_by_keys(&resolved) {
            self.status_message = e;
            self.mode = AppMode::Normal;
            return;
        }
        s.table_state.select(Some(0));

        if s.sort_keys.len() > 1 {
            let described: Vec<String> = s
                .sort_keys
                .iter()
                .map(|(n, d)| format!("{}{}", n, if *d { " ▼" } else { " ▲" }))
                .collect();
            self.status_message = format!("Sorted by {}", described.join(", then "));
        }

        // Every other z-command closes the prefix. Leaving it open made the
        // next keystroke a z-command: `z[` then `d` deleted a column.
        self.mode = AppMode::Normal;
    }

    fn describe_sheet(&mut self) {
        let s = self.stack.active();
        let ncols = s.dataframe.columns.len();
        let df = crate::data::describe::describe(&s.dataframe);

        let sheet_title = s.title.clone();
        let sheet = crate::sheet::Sheet::new(format!("Describe: {}", sheet_title), df);
        self.stack.push(sheet);
        self.status_message = format!("Describe: {} columns", ncols);
    }

    fn do_open_in_editor(&mut self, terminal: &mut DefaultTerminal) -> color_eyre::Result<()> {
        use crossterm::{
            cursor, execute,
            terminal::{
                disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
            },
        };
        use std::io::{self, Write as _};

        // Resolve current cell
        let s = self.stack.active();
        let display_row = match s.table_state.selected() {
            Some(r) => r,
            None => return Ok(()),
        };
        if display_row >= s.dataframe.row_order.len() {
            return Ok(());
        }
        let col = s.cursor_col;
        if col >= s.dataframe.columns.len() {
            return Ok(());
        }
        let physical_row = s.dataframe.row_order[display_row];

        // On a doc-backed sheet the table cell is only a rendering.  Containers are
        // edited as their real serialised subtree — editing the `{2} a=1 b=2` summary as
        // a string would write that summary back into the document as a string.
        let (current_value, suffix, node_path) = match s.doc.as_ref() {
            Some(doc) => match doc.path_of(physical_row, col) {
                Some(path) => {
                    let is_container = doc
                        .node_at(physical_row, col)
                        .map(|n| n.is_container())
                        .unwrap_or(false);
                    if is_container {
                        let text = doc.node_text(&path).unwrap_or_default();
                        (text, format!(".{}", doc.format().name()), Some(path))
                    } else {
                        (
                            s.dataframe.get_physical(physical_row, col),
                            ".txt".to_string(),
                            None,
                        )
                    }
                }
                None => (
                    s.dataframe.get_physical(physical_row, col),
                    ".txt".to_string(),
                    None,
                ),
            },
            None => (
                s.dataframe.get_physical(physical_row, col),
                ".txt".to_string(),
                None,
            ),
        };

        // Write the value to a temp file
        let mut tmp = tempfile::Builder::new().suffix(&suffix).tempfile()?;
        tmp.write_all(current_value.as_bytes())?;
        tmp.flush()?;
        let tmp_path = tmp.path().to_path_buf();

        // Suspend TUI
        disable_raw_mode()?;
        execute!(io::stdout(), LeaveAlternateScreen, cursor::Show)?;

        // Resolve editor command
        let editor = std::env::var("EDITOR")
            .or_else(|_| std::env::var("VISUAL"))
            .unwrap_or_else(|_| "vi".to_string());

        let status = std::process::Command::new(&editor).arg(&tmp_path).status();

        // Resume TUI (always, even on error)
        enable_raw_mode()?;
        execute!(io::stdout(), EnterAlternateScreen, cursor::Hide)?;
        terminal.clear()?;

        match status {
            Ok(exit_status) if exit_status.success() => {
                let new_content = std::fs::read_to_string(&tmp_path).unwrap_or_default();
                // Strip a single trailing newline that editors append
                let new_value = new_content.trim_end_matches(['\n', '\r']).to_string();

                if new_value == current_value {
                    self.status_message = "No changes".to_string();
                } else if let Some(path) = node_path {
                    self.apply_node_from_editor(&path, &new_value, &tmp_path);
                } else {
                    self.apply_cell_from_editor(physical_row, col, new_value);
                }
            }
            Ok(_) => {
                self.status_message = "Editor exited with non-zero status".to_string();
            }
            Err(e) => {
                self.status_message = format!("Failed to launch '{}': {}", editor, e);
            }
        }

        Ok(())
    }

    /// Apply a scalar edited in `$EDITOR`, routing through the document tree when the
    /// sheet has one so the value keeps its type.
    pub(crate) fn apply_cell_from_editor(
        &mut self,
        physical_row: usize,
        col: usize,
        new_value: String,
    ) {
        let s = self.stack.active_mut();
        s.push_undo();

        if !s.doc_mapping_ok() {
            s.undo_stack.pop();
            self.status_message =
                "The table no longer matches the document — reopen the sheet".to_string();
            return;
        }

        let mut value = new_value;
        if let Some(doc) = s.doc.as_mut() {
            match doc.set_cell(physical_row, col, &value) {
                Ok(shown) => value = shown,
                Err(e) => {
                    s.undo_stack.pop();
                    self.status_message = format!("Cell update failed: {}", e);
                    return;
                }
            }
        }
        match s.dataframe.set_cell(physical_row, col, value) {
            Ok(()) => self.status_message = "Cell updated from editor".to_string(),
            Err(e) => self.status_message = format!("Cell update failed: {}", e),
        }
    }

    /// Replace a whole subtree with text edited in `$EDITOR`.  If it does not parse,
    /// nothing is written and the temp file is kept and named in the status line — a
    /// rejected edit must never silently discard what the user typed.
    pub(crate) fn apply_node_from_editor(
        &mut self,
        path: &[crate::data::doc::Seg],
        text: &str,
        tmp_path: &Path,
    ) {
        let s = self.stack.active_mut();
        s.push_undo();
        let Some(doc) = s.doc.as_mut() else { return };

        if let Err(e) = doc.set_node_text(path, text) {
            s.undo_stack.pop();
            let kept = keep_failed_edit(tmp_path);
            self.status_message = match kept {
                Some(p) => format!("Parse error: {} — your text is kept in {}", e, p.display()),
                None => format!("Parse error: {}", e),
            };
            return;
        }
        match doc.reproject() {
            Ok(df) => {
                s.dataframe = df;
                s.reset_view_state();
                self.status_message = "Node updated from editor".to_string();
            }
            Err(e) => self.status_message = format!("Reprojection failed: {}", e),
        }
    }

    // ── Z Prefix (Column Operations) ──────────────────────────────────────────

    /// Run a column operation on a doc-backed sheet.  It changes keys in the tree, and
    /// the table is rebuilt from it, so the cell→node mapping stays true.
    /// `op` gets the cursor column and returns the table, the new cursor column and the
    /// status line.
    fn doc_column_op(
        &mut self,
        mode_after: AppMode,
        op: impl FnOnce(
            &mut crate::data::io::doc_io::DocState,
            usize,
        )
            -> color_eyre::Result<(crate::data::dataframe::DataFrame, usize, String)>,
    ) {
        self.mode = mode_after;
        let s = self.stack.active_mut();
        if !s.doc_mapping_ok() {
            self.status_message =
                "The table no longer matches the document — reopen the sheet".to_string();
            return;
        }
        let col = s.cursor_col;
        s.push_undo();
        let doc = s.doc.as_mut().expect("doc sheet");
        match op(doc, col) {
            Ok((df, at, msg)) => {
                s.dataframe = df;
                s.dataframe.modified = true;
                // The table comes back in the tree's order; a kept sort marker would
                // claim an order the rows no longer have.
                s.sort_keys.clear();
                s.cursor_col = at.min(s.dataframe.col_count().saturating_sub(1));
                s.table_state.select_column(Some(s.cursor_col));
                s.left_col = s.left_col.min(s.cursor_col);
                s.clamp_cursor();
                self.status_message = msg;
            }
            Err(e) => {
                s.undo_stack.pop();
                self.status_message = e.to_string();
            }
        }
    }

    fn apply_rename_column(&mut self) {
        if self.stack.active().doc.is_some() {
            let new_name = self
                .stack
                .active()
                .rename_column_input
                .as_str()
                .trim()
                .to_string();
            self.stack.active_mut().rename_column_input.clear();
            self.doc_column_op(AppMode::Normal, |doc, col| {
                let (df, n) = doc.rename_field(col, &new_name)?;
                Ok((
                    df,
                    col,
                    format!("Renamed key to '{}' in {} records", new_name, n),
                ))
            });
            return;
        }
        let s = self.stack.active_mut();
        let new_name = s.rename_column_input.as_str().trim().to_string();
        let col = s.cursor_col;
        let old_name = s.dataframe.columns[col].name.clone();
        if new_name != old_name && !new_name.is_empty() {
            s.push_undo();
            if let Err(e) = s.dataframe.rename_column(col, &new_name) {
                self.status_message = format!("Rename error: {}", e);
            } else {
                self.status_message = format!("Renamed column '{}' to '{}'", old_name, new_name);
            }
        }
        s.rename_column_input.clear();
        self.mode = AppMode::Normal;
    }

    fn delete_column(&mut self) {
        if self.stack.active().doc.is_some() {
            self.doc_column_op(AppMode::Normal, |doc, col| {
                let (df, n) = doc.delete_field(col)?;
                Ok((df, col, format!("Deleted the key from {} records", n)))
            });
            return;
        }
        let s = self.stack.active_mut();
        let col = s.cursor_col;
        if s.dataframe.col_count() <= 1 {
            self.status_message = "Cannot delete the last column".to_string();
            self.mode = AppMode::Normal;
            return;
        }
        s.push_undo();
        let old_name = s.dataframe.columns[col].name.clone();
        if let Err(e) = s.dataframe.drop_column(col) {
            self.status_message = format!("Delete error: {}", e);
        } else {
            self.status_message = format!("Deleted column '{}'", old_name);
            s.cursor_col = s.cursor_col.min(s.dataframe.col_count().saturating_sub(1));
            s.table_state.select_column(Some(s.cursor_col));
        }
        self.mode = AppMode::Normal;
    }

    fn apply_insert_column(&mut self) {
        if self.stack.active().doc.is_some() {
            let name = self.stack.active().insert_column_input.as_str().to_string();
            self.stack.active_mut().insert_column_input.clear();
            self.doc_column_op(AppMode::Normal, |doc, col| {
                let (df, n, at) = doc.insert_field(col, &name)?;
                let toml = if doc.format() == crate::data::doc::Format::Toml {
                    " — TOML keeps it only once filled"
                } else {
                    ""
                };
                Ok((
                    df,
                    at,
                    format!("Added key '{}' to {} records{}", name, n, toml),
                ))
            });
            return;
        }
        let s = self.stack.active_mut();
        let name = s.insert_column_input.as_str().to_string();
        if !name.is_empty() {
            s.push_undo();
            let col = s.cursor_col;
            if let Err(e) = s.dataframe.insert_empty_column(col, &name) {
                self.status_message = format!("Insert error: {}", e);
            } else {
                self.status_message = format!("Inserted column '{}'", name);
            }
        }
        s.insert_column_input.clear();
        self.mode = AppMode::Normal;
    }

    fn move_col_left(&mut self) {
        if self.stack.active().doc.is_some() {
            self.doc_column_op(AppMode::ColumnMove, |doc, col| {
                let other = col
                    .checked_sub(1)
                    .filter(|o| *o < doc.col_roles.len())
                    .ok_or_else(|| color_eyre::eyre::eyre!("already at the edge"))?;
                let (df, at) = doc.move_field(col, other)?;
                let name = df.columns[at].name.clone();
                Ok((
                    df,
                    at,
                    format!("Move column '{}': ←/→ to reorder, Esc to exit", name),
                ))
            });
            return;
        }
        let s = self.stack.active_mut();
        let col = s.cursor_col;
        if col > 0 {
            s.push_undo();
            if let Err(e) = s.dataframe.swap_columns(col, col - 1) {
                self.status_message = format!("Move error: {}", e);
                self.mode = AppMode::ColumnMove;
                return;
            }
            s.cursor_col -= 1;
            s.table_state.select_column(Some(s.cursor_col));
        }
        let col_name = self.stack.active().dataframe.columns[self.stack.active().cursor_col]
            .name
            .clone();
        self.mode = AppMode::ColumnMove;
        self.status_message = format!("Move column '{}': ←/→ to reorder, Esc to exit", col_name);
    }

    fn move_col_right(&mut self) {
        if self.stack.active().doc.is_some() {
            self.doc_column_op(AppMode::ColumnMove, |doc, col| {
                let other = Some(col + 1)
                    .filter(|o| *o < doc.col_roles.len())
                    .ok_or_else(|| color_eyre::eyre::eyre!("already at the edge"))?;
                let (df, at) = doc.move_field(col, other)?;
                let name = df.columns[at].name.clone();
                Ok((
                    df,
                    at,
                    format!("Move column '{}': ←/→ to reorder, Esc to exit", name),
                ))
            });
            return;
        }
        let s = self.stack.active_mut();
        let col = s.cursor_col;
        if col + 1 < s.dataframe.col_count() {
            s.push_undo();
            if let Err(e) = s.dataframe.swap_columns(col, col + 1) {
                self.status_message = format!("Move error: {}", e);
                self.mode = AppMode::ColumnMove;
                return;
            }
            s.cursor_col += 1;
            s.table_state.select_column(Some(s.cursor_col));
        }
        let col_name = self.stack.active().dataframe.columns[self.stack.active().cursor_col]
            .name
            .clone();
        self.mode = AppMode::ColumnMove;
        self.status_message = format!("Move column '{}': ←/→ to reorder, Esc to exit", col_name);
    }

    fn join_path_autocomplete(&mut self) {
        let input = self.join.path_input.as_str().to_owned();
        let path = std::path::Path::new(&input);
        let (dir, prefix) = if input.ends_with('/') {
            (path, "")
        } else {
            let dir = path.parent().unwrap_or(std::path::Path::new("."));
            let prefix = path.file_name().and_then(|f| f.to_str()).unwrap_or("");
            (dir, prefix)
        };
        let dir_str = if dir == std::path::Path::new("") {
            std::path::Path::new(".")
        } else {
            dir
        };
        let expanded_dir = expand_tilde(dir_str.to_str().unwrap_or("."));
        let full_prefix = input.trim_end_matches(prefix).to_string();
        if self.expression.autocomplete_prefix != full_prefix
            || self.expression.autocomplete_candidates.is_empty()
        {
            self.expression.autocomplete_prefix = full_prefix.clone();
            self.expression.autocomplete_idx = 0;
            let mut candidates: Vec<String> = std::fs::read_dir(&expanded_dir)
                .into_iter()
                .flatten()
                .filter_map(|e| e.ok())
                .map(|e| {
                    let name = e.file_name().to_string_lossy().into_owned();
                    let is_dir = e.file_type().map(|t| t.is_dir()).unwrap_or(false);
                    if is_dir {
                        format!("{}/", name)
                    } else {
                        name
                    }
                })
                .filter(|name| name.starts_with(prefix))
                .collect();
            candidates.sort();
            self.expression.autocomplete_candidates = candidates;
        }
        if self.expression.autocomplete_candidates.is_empty() {
            return;
        }
        let common = longest_common_prefix(&self.expression.autocomplete_candidates);
        let current_suffix = self
            .join
            .path_input
            .as_str()
            .strip_prefix(&self.expression.autocomplete_prefix)
            .unwrap_or("");
        if common.len() > current_suffix.len() {
            let new_value = format!("{}{}", self.expression.autocomplete_prefix, common);
            self.join.path_input = crate::ui::text_input::TextInput::with_value(new_value);
        } else {
            self.expression.autocomplete_idx = (self.expression.autocomplete_idx + 1)
                % self.expression.autocomplete_candidates.len();
            let completion =
                &self.expression.autocomplete_candidates[self.expression.autocomplete_idx];
            let new_value = format!("{}{}", self.expression.autocomplete_prefix, completion);
            self.join.path_input = crate::ui::text_input::TextInput::with_value(new_value);
        }
    }
}

/// Copy a rejected `$EDITOR` buffer somewhere stable before the temp file is dropped,
/// so a parse error never costs the user their typing.  Returns the kept path.
fn keep_failed_edit(tmp_path: &Path) -> Option<std::path::PathBuf> {
    let ext = tmp_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("txt");
    let kept = std::env::temp_dir().join(format!("tuitab-failed-edit.{}", ext));
    std::fs::copy(tmp_path, &kept).ok()?;
    Some(kept)
}

/// Formats offered by `zo` ("open as") on a directory listing, for the case the
/// extension lies and the contents are ambiguous enough that sniffing declines.
pub const OPEN_AS_FORMATS: [crate::data::doc::Format; 4] = [
    crate::data::doc::Format::Json,
    crate::data::doc::Format::Jsonl,
    crate::data::doc::Format::Yaml,
    crate::data::doc::Format::Toml,
];

/// Expand a leading `~` to the user's home directory.
pub fn expand_tilde(input: &str) -> std::path::PathBuf {
    if let Some(rest) = input.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return std::path::PathBuf::from(home).join(rest);
        }
    } else if input == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return std::path::PathBuf::from(home);
        }
    }
    std::path::PathBuf::from(input)
}

fn longest_common_prefix(strs: &[String]) -> String {
    if strs.is_empty() {
        return String::new();
    }
    let first = &strs[0];
    let mut len = first.len();
    for s in &strs[1..] {
        len = len.min(
            first
                .chars()
                .zip(s.chars())
                .take_while(|(a, b)| a == b)
                .count(),
        );
    }
    first[..first
        .char_indices()
        .nth(len)
        .map(|(i, _)| i)
        .unwrap_or(first.len())]
        .to_owned()
}

#[cfg(test)]
mod async_open_tests {
    //! The background loader has to hand back everything the foreground one would.  These
    //! live here because driving it means calling `poll_async_load`, which nothing outside
    //! this module should be able to do.

    use super::*;

    fn dir() -> std::path::PathBuf {
        let d = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join("async-open-tests");
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Run the loader to completion, the way `App::run` would between draws.
    fn settle(app: &mut App) {
        for _ in 0..600 {
            app.poll_async_load();
            if app.mode != AppMode::Loading {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("the background load never finished: {}", app.status_message);
    }

    /// Issue #43: a database past the async threshold came back as a frame with no idea
    /// what it was, so `Enter` on a table transposed the row instead of opening it.
    #[test]
    fn a_database_past_the_threshold_still_drills_into_tables() {
        let path = dir().join("over-threshold.db");
        let _ = std::fs::remove_file(&path);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE event (id INTEGER, data TEXT)")
            .unwrap();
        let blob = "x".repeat(8000);
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..1500 {
            tx.execute("INSERT INTO event VALUES (?1, ?2)", (i, &blob))
                .unwrap();
        }
        tx.commit().unwrap();
        drop(conn);
        assert!(
            std::fs::metadata(&path).unwrap().len() > 10 * 1024 * 1024,
            "the fixture has to cross the threshold to exercise the loader"
        );

        let mut app = App::new_as(&path, None, None).unwrap();
        assert_eq!(
            app.mode,
            AppMode::Loading,
            "big files load in the background"
        );
        settle(&mut app);

        assert_eq!(app.stack.active().dataframe.columns[0].name, "Table");
        app.handle_action(Action::OpenRow);
        assert!(
            app.status_message.contains("Opened table 'event'"),
            "Enter transposed the overview row instead of opening the table: {}",
            app.status_message
        );
        assert_eq!(app.stack.active().dataframe.visible_row_count(), 1500);
    }

    /// The same for a workbook: past the threshold it used to open sheet one and leave
    /// the rest unreachable.
    #[test]
    fn a_workbook_past_the_threshold_opens_the_sheet_list() {
        use rust_xlsxwriter::Workbook;
        let path = dir().join("over-threshold.xlsx");
        if std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) <= 10 * 1024 * 1024 {
            let mut wb = Workbook::new();
            // Random-looking text: a zip full of one repeated string compresses to nothing.
            let mut seed: u64 = 0x2545F4914F6CDD1D;
            for name in ["alpha", "beta"] {
                let ws = wb.add_worksheet().set_name(name).unwrap();
                for r in 0..300_000u32 {
                    let cell: String = (0..24)
                        .map(|_| {
                            seed ^= seed << 13;
                            seed ^= seed >> 7;
                            seed ^= seed << 17;
                            (b'a' + (seed % 26) as u8) as char
                        })
                        .collect();
                    ws.write_string(r, 0, &cell).unwrap();
                }
            }
            wb.save(&path).unwrap();
        }
        assert!(std::fs::metadata(&path).unwrap().len() > 10 * 1024 * 1024);

        let mut app = App::new_as(&path, None, None).unwrap();
        assert_eq!(app.mode, AppMode::Loading);
        settle(&mut app);

        assert_eq!(app.stack.active().dataframe.columns[0].name, "Sheet");
        assert_eq!(app.stack.active().dataframe.visible_row_count(), 2);
        app.handle_action(Action::OpenRow);
        assert!(
            app.status_message.contains("alpha"),
            "the sheet list did not open: {}",
            app.status_message
        );
    }

    /// `--type` has to survive the trip too: the loader used to reopen by extension.
    #[test]
    fn an_explicit_type_survives_the_threshold() {
        let path = dir().join("big.txt");
        let mut out = String::new();
        while out.len() < 11 * 1024 * 1024 {
            out.push_str("{\"a\": 1, \"b\": \"two\"}\n");
        }
        std::fs::write(&path, out).unwrap();

        let mut app = App::new_as(&path, None, Some(crate::data::doc::Format::Jsonl)).unwrap();
        assert_eq!(app.mode, AppMode::Loading);
        settle(&mut app);

        let cols: Vec<String> = app
            .stack
            .active()
            .dataframe
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        assert_eq!(
            cols,
            vec!["a".to_string(), "b".to_string()],
            "--type was dropped and the file was read as plain text"
        );
    }
}
