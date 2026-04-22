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

use crate::clipboard;
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
use polars::prelude::*;
use ratatui::widgets::ScrollbarState;
use ratatui::DefaultTerminal;
use regex::Regex;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub struct App {
    /// Stack of open sheets — last element is the active (displayed) sheet
    pub stack: SheetStack,
    /// Current application mode
    pub mode: AppMode,
    /// Transient message shown in the status bar
    pub status_message: String,

    /// Set to `true` to signal the event loop to exit cleanly.
    pub should_quit: bool,
    /// Async loading receiver (Phase 10) — polled every frame in run()
    pub load_receiver: Option<std::sync::mpsc::Receiver<LoadEvent>>,
    /// Input text for the save popup
    pub saving_input: TextInput,
    /// Error message for the save popup
    pub saving_error: Option<String>,

    /// Cursor index within the aggregator selection popup list.
    pub agg_select_index: usize,
    /// Set of aggregator kinds currently enabled on the cursor column.
    pub agg_selected: HashSet<AggregatorKind>,
    /// Cursor index within the column-type selection popup.
    pub type_select_index: usize,
    /// Selected index in the currency selection popup
    pub currency_select_index: usize,

    /// Cursor index within the partition-column selection popup.
    pub partition_select_index: usize,
    /// Set of column names selected for partitioned sheet creation.
    pub partition_selected: HashSet<String>,

    /// Background task status (Name, Current Step, Total Steps)
    pub background_task: Option<(String, usize, usize)>,
    /// Counter for animating a spinner for indeterminate tasks
    pub spinner_tick: u8,

    // ── Expression History & Autocomplete ─────────────────────────────────────
    /// Saved expression strings for ↑/↓ history navigation in the `=` input.
    pub expr_history: Vec<String>,
    /// Current position in `expr_history` (`None` = not browsing history).
    pub history_idx: Option<usize>,
    /// Column-name candidates for Tab-completion in the expression input.
    pub autocomplete_candidates: Vec<String>,
    /// Current index within `autocomplete_candidates`.
    pub autocomplete_idx: usize,
    /// The prefix string that triggered the current autocomplete session.
    pub autocomplete_prefix: String,

    // ── Pivot Table History ────────────────────────────────────────────────────
    /// Saved pivot formula strings for ↑/↓ history in the `W` input.
    pub pivot_history: Vec<String>,
    /// Current position in `pivot_history` (`None` = not browsing history).
    pub pivot_history_idx: Option<usize>,

    // ── Contextual Chart State ─────────────────────────────────────────────────
    /// Reference (pinned) column index for 2-column contextual charts
    pub chart_ref_col: Option<usize>,
    /// Selected aggregation function for contextual charts
    pub chart_agg: crate::types::ChartAgg,
    /// Current selection index in the ChartAggSelect popup
    pub chart_agg_index: usize,

    /// Delayed action for processing after rendering calculating overlay
    pub pending_action: Option<Action>,

    // ── JOIN wizard state ──────────────────────────────────────────────────────
    pub join_source_index: usize,
    pub join_other_df: Option<DataFrame>,
    pub join_other_title: String,
    pub join_type_index: usize,
    pub join_left_keys: Vec<String>,
    pub join_right_keys: Vec<String>,
    pub join_left_key_index: usize,
    pub join_right_key_index: usize,
    pub join_path_input: TextInput,
    pub join_path_error: Option<String>,
    pub join_context_items: Vec<crate::types::JoinContextItem>,
    pub join_overview_cursor: usize,
    pub join_overview_selected: Vec<usize>,
    pub join_pending_queue: Vec<crate::types::JoinContextItem>,
    pub open_in_editor_pending: bool,
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
            Ok(Self {
                stack: SheetStack::new(root_sheet),
                mode: AppMode::Normal,
                status_message: format!("Loaded directory '{}' ({} items)", filename, row_count),
                should_quit: false,
                load_receiver: None,
                saving_input: TextInput::with_value(filename),
                saving_error: None,
                agg_select_index: 0,
                agg_selected: HashSet::new(),
                type_select_index: 0,
                currency_select_index: 0,
                partition_select_index: 0,
                partition_selected: HashSet::new(),
                background_task: None,
                spinner_tick: 0,
                expr_history: Vec::new(),
                history_idx: None,
                autocomplete_candidates: Vec::new(),
                autocomplete_idx: 0,
                autocomplete_prefix: String::new(),
                pivot_history: Vec::new(),
                pivot_history_idx: None,
                chart_ref_col: None,
                chart_agg: crate::types::ChartAgg::Count,
                chart_agg_index: 0,
                pending_action: None,
                join_source_index: 0,
                join_other_df: None,
                join_other_title: String::new(),
                join_type_index: 0,
                join_left_keys: Vec::new(),
                join_right_keys: Vec::new(),
                join_left_key_index: 0,
                join_right_key_index: 0,
                join_path_input: TextInput::new(),
                join_path_error: None,
                join_context_items: Vec::new(),
                join_overview_cursor: 0,
                join_overview_selected: Vec::new(),
                join_pending_queue: Vec::new(),
                open_in_editor_pending: false,
            })
        } else if file_size > ASYNC_THRESHOLD {
            let rx = async_loader::load_in_background(path.to_path_buf(), delim_byte);
            let placeholder = DataFrame::empty();
            let mut root_sheet = Sheet::new(filename.clone(), placeholder);
            root_sheet.source_path = Some(path.to_path_buf());
            root_sheet.source_delimiter = delim_byte;
            Ok(Self {
                stack: SheetStack::new(root_sheet),
                mode: AppMode::Loading,
                status_message: format!("Loading {}...", path.display()),
                should_quit: false,
                load_receiver: Some(rx),
                saving_input: TextInput::with_value(filename.clone()),
                saving_error: None,
                agg_select_index: 0,
                agg_selected: HashSet::new(),
                type_select_index: 0,
                currency_select_index: 0,
                partition_select_index: 0,
                partition_selected: HashSet::new(),
                background_task: None,
                spinner_tick: 0,
                expr_history: Vec::new(),
                history_idx: None,
                autocomplete_candidates: Vec::new(),
                autocomplete_idx: 0,
                autocomplete_prefix: String::new(),
                pivot_history: Vec::new(),
                pivot_history_idx: None,
                chart_ref_col: None,
                chart_agg: crate::types::ChartAgg::Count,
                chart_agg_index: 0,
                pending_action: None,
                join_source_index: 0,
                join_other_df: None,
                join_other_title: String::new(),
                join_type_index: 0,
                join_left_keys: Vec::new(),
                join_right_keys: Vec::new(),
                join_left_key_index: 0,
                join_right_key_index: 0,
                join_path_input: TextInput::new(),
                join_path_error: None,
                join_context_items: Vec::new(),
                join_overview_cursor: 0,
                join_overview_selected: Vec::new(),
                join_pending_queue: Vec::new(),
                open_in_editor_pending: false,
            })
        } else {
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_lowercase();

            // For multi-sheet xlsx: load sheet overview instead of first sheet
            let (dataframe, xlsx_db) = if matches!(ext.as_str(), "xlsx" | "xls" | "xlsm" | "xlsb") {
                match crate::data::io::excel_sheet_names(path) {
                    Ok(names) if names.len() > 1 => {
                        let df = crate::data::io::load_excel_overview(path)?;
                        (df, Some(path.to_path_buf()))
                    }
                    _ => (crate::data::io::load_file(path, delim_byte)?, None),
                }
            } else {
                (crate::data::io::load_file(path, delim_byte)?, None)
            };

            let row_count = dataframe.visible_row_count();
            let mut root_sheet = Sheet::new(filename.clone(), dataframe);
            if matches!(ext.as_str(), "sqlite" | "sqlite3") {
                root_sheet.sqlite_db_path = Some(path.to_path_buf());
            } else if matches!(ext.as_str(), "duckdb" | "ddb") {
                root_sheet.duckdb_db_path = Some(path.to_path_buf());
            } else if ext == "db" {
                // .db: detect by trying to open as SQLite
                if crate::data::io::load_sqlite_overview(path).is_ok() {
                    root_sheet.sqlite_db_path = Some(path.to_path_buf());
                } else {
                    root_sheet.duckdb_db_path = Some(path.to_path_buf());
                }
            }
            root_sheet.xlsx_db_path = xlsx_db;
            root_sheet.source_path = Some(path.to_path_buf());
            root_sheet.source_delimiter = delim_byte;
            let status_message = if root_sheet.xlsx_db_path.is_some() {
                format!("Loaded '{}' — {} sheets", filename, row_count)
            } else {
                format!("Loaded {} rows", row_count)
            };
            Ok(Self {
                stack: SheetStack::new(root_sheet),
                mode: AppMode::Normal,
                status_message,
                should_quit: false,
                load_receiver: None,
                saving_input: TextInput::with_value(filename),
                saving_error: None,
                agg_select_index: 0,
                agg_selected: HashSet::new(),
                type_select_index: 0,
                currency_select_index: 0,
                partition_select_index: 0,
                partition_selected: HashSet::new(),
                background_task: None,
                spinner_tick: 0,
                expr_history: Vec::new(),
                history_idx: None,
                autocomplete_candidates: Vec::new(),
                autocomplete_idx: 0,
                autocomplete_prefix: String::new(),
                pivot_history: Vec::new(),
                pivot_history_idx: None,
                chart_ref_col: None,
                chart_agg: crate::types::ChartAgg::Count,
                chart_agg_index: 0,
                pending_action: None,
                join_source_index: 0,
                join_other_df: None,
                join_other_title: String::new(),
                join_type_index: 0,
                join_left_keys: Vec::new(),
                join_right_keys: Vec::new(),
                join_left_key_index: 0,
                join_right_key_index: 0,
                join_path_input: TextInput::new(),
                join_path_error: None,
                join_context_items: Vec::new(),
                join_overview_cursor: 0,
                join_overview_selected: Vec::new(),
                join_pending_queue: Vec::new(),
                open_in_editor_pending: false,
            })
        }
    }

    /// Construct `App` by reading typed data from stdin.
    ///
    /// `data_type` must be one of `"csv"`, `"json"`, or `"parquet"`.
    /// `delimiter` overrides auto-detection for CSV/TSV input.
    pub fn from_stdin_typed(data_type: &str, delimiter: Option<char>) -> Result<Self> {
        let delim_byte = delimiter.map(|c| c as u8);
        let dataframe = crate::data::io::load_from_stdin_typed(data_type, delim_byte)?;
        let row_count = dataframe.visible_row_count();
        let title = "stdin".to_string();
        let root_sheet = Sheet::new(title.clone(), dataframe);
        Ok(Self {
            stack: SheetStack::new(root_sheet),
            mode: AppMode::Normal,
            status_message: format!("Loaded {} rows from stdin", row_count),
            should_quit: false,
            load_receiver: None,
            saving_input: TextInput::with_value(title),
            saving_error: None,
            agg_select_index: 0,
            agg_selected: HashSet::new(),
            type_select_index: 0,
            currency_select_index: 0,
            partition_select_index: 0,
            partition_selected: HashSet::new(),
            background_task: None,
            spinner_tick: 0,
            expr_history: Vec::new(),
            history_idx: None,
            autocomplete_candidates: Vec::new(),
            autocomplete_idx: 0,
            autocomplete_prefix: String::new(),
            pivot_history: Vec::new(),
            pivot_history_idx: None,
            chart_ref_col: None,
            chart_agg: crate::types::ChartAgg::Count,
            chart_agg_index: 0,
            pending_action: None,
            join_source_index: 0,
            join_other_df: None,
            join_other_title: String::new(),
            join_type_index: 0,
            join_left_keys: Vec::new(),
            join_right_keys: Vec::new(),
            join_left_key_index: 0,
            join_right_key_index: 0,
            join_path_input: TextInput::new(),
            join_path_error: None,
            join_context_items: Vec::new(),
            join_overview_cursor: 0,
            join_overview_selected: Vec::new(),
            join_pending_queue: Vec::new(),
            open_in_editor_pending: false,
        })
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
        Ok(Self {
            stack: SheetStack::new(root_sheet),
            mode: AppMode::Normal,
            status_message: format!("{} files", n),
            should_quit: false,
            load_receiver: None,
            saving_input: TextInput::with_value(title),
            saving_error: None,
            agg_select_index: 0,
            agg_selected: HashSet::new(),
            type_select_index: 0,
            currency_select_index: 0,
            partition_select_index: 0,
            partition_selected: HashSet::new(),
            background_task: None,
            spinner_tick: 0,
            expr_history: Vec::new(),
            history_idx: None,
            autocomplete_candidates: Vec::new(),
            autocomplete_idx: 0,
            autocomplete_prefix: String::new(),
            pivot_history: Vec::new(),
            pivot_history_idx: None,
            chart_ref_col: None,
            chart_agg: crate::types::ChartAgg::Count,
            chart_agg_index: 0,
            pending_action: None,
            join_source_index: 0,
            join_other_df: None,
            join_other_title: String::new(),
            join_type_index: 0,
            join_left_keys: Vec::new(),
            join_right_keys: Vec::new(),
            join_left_key_index: 0,
            join_right_key_index: 0,
            join_path_input: TextInput::new(),
            join_path_error: None,
            join_context_items: Vec::new(),
            join_overview_cursor: 0,
            join_overview_selected: Vec::new(),
            join_pending_queue: Vec::new(),
            open_in_editor_pending: false,
        })
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
                Ok(LoadEvent::Complete(Ok(dataframe))) => {
                    let row_count = dataframe.visible_row_count();
                    let s = self.stack.active_mut();
                    s.dataframe = dataframe;
                    s.dataframe.calc_widths(40, 1000);
                    let vis = s.dataframe.visible_row_count();
                    s.scroll_state = ScrollbarState::new(vis.saturating_sub(1));
                    s.table_state.select(Some(0));
                    self.mode = AppMode::Normal;
                    self.status_message = format!("Loaded {} rows", row_count);
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

        let meta = std::fs::metadata(&path);
        let is_dir = meta.as_ref().map(|m| m.is_dir()).unwrap_or(false);
        let file_size = meta.map(|m| m.len()).unwrap_or(0);

        let saved_row = {
            let s = self.stack.active_mut();
            s.undo_stack.clear();
            s.sort_col = None;
            s.sort_desc = false;
            s.search_pattern = None;
            s.search_col = None;
            s.dataframe.selected_rows.clear();
            s.table_state.selected().unwrap_or(0)
        };

        if is_dir {
            match crate::data::io::load_directory(&path) {
                Ok(df) => {
                    let row_count = df.visible_row_count();
                    let s = self.stack.active_mut();
                    s.dataframe = df;
                    s.dataframe.calc_widths(40, 1000);
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
                s.dataframe = DataFrame::empty();
                s.source_path = Some(path.clone());
                s.source_delimiter = source_delimiter;
            }
            self.load_receiver = Some(async_loader::load_in_background(path, source_delimiter));
            self.mode = AppMode::Loading;
            self.status_message = "Reloading...".to_string();
        } else {
            match crate::data::io::load_file(&path, source_delimiter) {
                Ok(df) => {
                    let row_count = df.visible_row_count();
                    let s = self.stack.active_mut();
                    s.dataframe = df;
                    s.dataframe.calc_widths(40, 1000);
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

            // ── Navigation ────────────────────────────────────────────────────
            Action::MoveDown => self.move_cursor_down(),
            Action::MoveUp => self.move_cursor_up(),
            Action::MoveLeft => self.move_cursor_left(),
            Action::MoveRight => self.move_cursor_right(),
            Action::PageDown => self.page_down(),
            Action::PageUp => self.page_up(),
            Action::GoTop => {
                let s = self.stack.active_mut();
                s.table_state.select(Some(0));
                s.top_row = 0;
                s.scroll_state = s.scroll_state.position(0);
            }
            Action::GoBottom => {
                let s = self.stack.active_mut();
                let last = s.dataframe.visible_row_count().saturating_sub(1);
                s.table_state.select(Some(last));
                // We'll update top_row precisely inside table_view bounds loop,
                // but setting it to `last` ensures the UI snaps to bottom.
                s.top_row = last;
                s.scroll_state = s.scroll_state.position(last);
            }

            // ── Sorting ───────────────────────────────────────────────────────
            Action::SortAscending => {
                let s = self.stack.active_mut();
                s.push_undo();
                let col = s.cursor_col;
                s.dataframe.sort_by(col, false);
                s.sort_col = Some(col);
                s.sort_desc = false;
                s.table_state.select(Some(0));
            }
            Action::SortDescending => {
                let s = self.stack.active_mut();
                s.push_undo();
                let col = s.cursor_col;
                s.dataframe.sort_by(col, true);
                s.sort_col = Some(col);
                s.sort_desc = true;
                s.table_state.select(Some(0));
            }
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
                } else {
                    // FEATURE F5: Transpose row on Enter if not special sheet
                    self.transpose_row();
                }
            }
            Action::ResetSort => {
                let s = self.stack.active_mut();
                s.push_undo();
                s.dataframe.reset_sort();
                s.sort_col = None;
                s.table_state.select(Some(0));
            }
            Action::ReloadFile => self.reload_file(),
            Action::TransposeRow => self.transpose_row(),
            Action::TransposeTable => self.transpose_table(),
            Action::DescribeSheet => self.describe_sheet(),

            // ── Search (/) ────────────────────────────────────────────────────
            Action::StartSearch => {
                let s = self.stack.active_mut();
                s.search_col = Some(s.cursor_col);
                s.search_input.clear();
                self.mode = AppMode::Searching;
                self.status_message = "Search (regex): ".to_string();
            }
            Action::SearchInput(c) => {
                self.stack.active_mut().search_input.insert_char(c);
            }
            Action::SearchBackspace => {
                self.stack.active_mut().search_input.delete_backward();
            }
            Action::SearchForwardDelete => {
                self.stack.active_mut().search_input.delete_forward();
            }
            Action::SearchCursorLeft => {
                self.stack.active_mut().search_input.move_cursor_left();
            }
            Action::SearchCursorRight => {
                self.stack.active_mut().search_input.move_cursor_right();
            }
            Action::SearchCursorStart => {
                self.stack.active_mut().search_input.move_cursor_start();
            }
            Action::SearchCursorEnd => {
                self.stack.active_mut().search_input.move_cursor_end();
            }
            Action::ApplySearch => self.apply_search(),
            Action::CancelSearch => {
                self.stack.active_mut().search_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::SearchNext => self.search_next(),
            Action::SearchPrev => self.search_prev(),
            Action::ClearSearch => {
                let s = self.stack.active_mut();
                s.search_pattern = None;
                s.search_col = None;
                self.status_message = "Search cleared".to_string();
            }

            // ── Select by value (,) ───────────────────────────────────────────
            Action::SelectByValue => self.select_by_value(),

            // ── Select by regex (|) ───────────────────────────────────────────
            Action::StartSelectByRegex => {
                self.stack.active_mut().select_regex_input.clear();
                self.mode = AppMode::SelectByRegex;
                self.status_message = "Select by regex: ".to_string();
            }
            Action::SelectRegexInput(c) => {
                self.autocomplete_candidates.clear();
                self.stack.active_mut().select_regex_input.insert_char(c);
            }
            Action::SelectRegexBackspace => {
                self.autocomplete_candidates.clear();
                self.stack.active_mut().select_regex_input.delete_backward();
            }
            Action::SelectRegexForwardDelete => {
                self.stack.active_mut().select_regex_input.delete_forward();
            }
            Action::SelectRegexCursorLeft => {
                self.stack
                    .active_mut()
                    .select_regex_input
                    .move_cursor_left();
            }
            Action::SelectRegexCursorRight => {
                self.stack
                    .active_mut()
                    .select_regex_input
                    .move_cursor_right();
            }
            Action::SelectRegexCursorStart => {
                self.stack
                    .active_mut()
                    .select_regex_input
                    .move_cursor_start();
            }
            Action::SelectRegexCursorEnd => {
                self.stack.active_mut().select_regex_input.move_cursor_end();
            }
            Action::ApplySelectByRegex => self.apply_select_by_regex(),
            Action::CancelSelectByRegex => {
                self.autocomplete_candidates.clear();
                self.stack.active_mut().select_regex_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::SelectRegexAutocomplete => self.select_regex_autocomplete(),

            // ── Expression / computed column (=) ──────────────────────────────
            Action::StartExpression => {
                self.stack.active_mut().expr_input.clear();
                self.mode = AppMode::ExpressionInput;
                self.status_message = "Expression: ".to_string();
                self.history_idx = None;
                self.autocomplete_candidates.clear();
            }
            Action::ExpressionInputChar(c) => {
                self.autocomplete_candidates.clear();
                self.stack.active_mut().expr_input.insert_char(c);
            }
            Action::ExpressionBackspace => {
                self.autocomplete_candidates.clear();
                self.stack.active_mut().expr_input.delete_backward();
            }
            Action::ExpressionForwardDelete => {
                self.autocomplete_candidates.clear();
                self.stack.active_mut().expr_input.delete_forward();
            }
            Action::ExpressionCursorLeft => {
                self.stack.active_mut().expr_input.move_cursor_left();
            }
            Action::ExpressionCursorRight => {
                self.stack.active_mut().expr_input.move_cursor_right();
            }
            Action::ExpressionCursorStart => {
                self.stack.active_mut().expr_input.move_cursor_start();
            }
            Action::ExpressionCursorEnd => {
                self.stack.active_mut().expr_input.move_cursor_end();
            }
            Action::ApplyExpression => self.apply_expression(),
            Action::CancelExpression => {
                self.stack.active_mut().expr_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::ExpressionAutocomplete => self.expr_autocomplete(),
            Action::ExpressionHistoryPrev => self.expr_history_prev(),
            Action::ExpressionHistoryNext => self.expr_history_next(),

            // ── Frequency table (push new Sheet) ──────────────────────────────
            Action::OpenFrequencyTable => {
                if self.mode == AppMode::Calculating {
                    self.open_frequency_table();
                } else {
                    self.mode = AppMode::Calculating;
                    self.pending_action = Some(Action::OpenFrequencyTable);
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

            // ── Pivot Table (Shift+W) ─────────────────────────────────────────────────
            Action::OpenPivotTableInput => {
                let s = self.stack.active();
                let has_pinned = s.dataframe.columns.iter().any(|c| c.pinned);
                let on_unpinned = !s.dataframe.columns[s.cursor_col].pinned;

                if has_pinned && on_unpinned {
                    self.mode = AppMode::PivotTableInput;
                    self.status_message =
                        "Enter aggregation formula (e.g. sum(amount) / sum(count))".to_string();
                } else if !has_pinned {
                    self.status_message =
                        "Pivot requires at least one pinned column (!) as row index".to_string();
                } else {
                    self.status_message =
                        "Cursor must be on an unpinned column to pivot".to_string();
                }
            }
            Action::ApplyPivotTable => {
                if self.mode == AppMode::Calculating {
                    self.apply_pivot_table();
                } else {
                    self.mode = AppMode::Calculating;
                    self.pending_action = Some(Action::ApplyPivotTable);
                }
            }
            Action::CancelPivotTable => {
                self.autocomplete_candidates.clear();
                self.pivot_history_idx = None;
                self.mode = AppMode::Normal;
                self.stack.active_mut().pivot_input.clear();
            }
            Action::PivotAutocomplete => self.pivot_autocomplete(),
            Action::PivotHistoryPrev => self.pivot_history_prev(),
            Action::PivotHistoryNext => self.pivot_history_next(),
            Action::PivotInput(c) => {
                self.autocomplete_candidates.clear();
                self.stack.active_mut().pivot_input.insert_char(c);
            }
            Action::PivotBackspace => {
                self.autocomplete_candidates.clear();
                self.stack.active_mut().pivot_input.delete_backward();
            }
            Action::PivotForwardDelete => {
                self.stack.active_mut().pivot_input.delete_forward();
            }
            Action::PivotCursorLeft => {
                self.stack.active_mut().pivot_input.move_cursor_left();
            }
            Action::PivotCursorRight => {
                self.stack.active_mut().pivot_input.move_cursor_right();
            }
            Action::PivotCursorStart => {
                self.stack.active_mut().pivot_input.move_cursor_start();
            }
            Action::PivotCursorEnd => {
                self.stack.active_mut().pivot_input.move_cursor_end();
            }
            Action::DeduplicateByPinned => {
                if self.mode == AppMode::Calculating {
                    self.deduplicate_by_pinned();
                } else {
                    self.mode = AppMode::Calculating;
                    self.pending_action = Some(Action::DeduplicateByPinned);
                }
            }

            // ── Charts ────────────────────────────────────────────────────────
            Action::OpenChart => {
                if self.mode == AppMode::Chart || self.mode == AppMode::ChartAggSelect {
                    self.mode = AppMode::Normal;
                    self.chart_ref_col = None;
                    self.status_message.clear();
                } else {
                    self.open_chart();
                }
            }
            Action::ChartAggSelectUp => {
                if self.chart_agg_index > 0 {
                    self.chart_agg_index -= 1;
                }
            }
            Action::ChartAggSelectDown => {
                let max = crate::types::ChartAgg::all().len() - 1;
                if self.chart_agg_index < max {
                    self.chart_agg_index += 1;
                }
            }
            Action::ApplyChartAgg => {
                self.chart_agg = crate::types::ChartAgg::all()[self.chart_agg_index];
                self.mode = AppMode::Chart;
                let s = self.stack.active();
                let col_name = s.dataframe.columns[s.cursor_col].name.clone();
                self.status_message =
                    format!("Chart: {} — Press 'v', 'q' or Esc to exit", col_name);
            }
            Action::CancelChartAgg => {
                self.mode = AppMode::Normal;
                self.chart_ref_col = None;
                self.status_message.clear();
            }

            // ── Type selection popup (t) ──────────────────────────────────────
            Action::OpenTypeSelect => {
                self.type_select_index = 0;
                let s = self.stack.active();
                let col = s.cursor_col;
                if col < s.dataframe.columns.len() {
                    let current_type = s.dataframe.columns[col].col_type;
                    if let Some(idx) = crate::types::ColumnType::all()
                        .iter()
                        .position(|t| *t == current_type)
                    {
                        self.type_select_index = idx;
                    }
                }
                self.mode = AppMode::TypeSelect;
                self.status_message =
                    "Select column type (↑↓ navigate, Enter apply, Esc cancel)".to_string();
            }
            Action::TypeSelectUp => {
                let n = crate::types::ColumnType::all().len();
                if self.type_select_index > 0 {
                    self.type_select_index -= 1;
                } else {
                    self.type_select_index = n.saturating_sub(1);
                }
            }
            Action::TypeSelectDown => {
                let n = crate::types::ColumnType::all().len();
                if n > 0 {
                    self.type_select_index = (self.type_select_index + 1) % n;
                }
            }
            Action::ApplyTypeSelect => {
                let col_type = crate::types::ColumnType::all()[self.type_select_index];
                if col_type == crate::types::ColumnType::Currency {
                    self.currency_select_index = 0;
                    self.mode = AppMode::CurrencySelect;
                    self.status_message =
                        "Select currency (↑↓ navigate, Enter apply, Esc cancel)".to_string();
                } else {
                    let s = self.stack.active_mut();
                    s.push_undo();
                    let col = s.cursor_col;
                    match s.dataframe.set_column_type(col, col_type) {
                        Ok(_) => {
                            let col_name = self.stack.active().dataframe.columns[col].name.clone();
                            self.mode = AppMode::Normal;
                            self.status_message =
                                format!("Column '{}' set to {:?}", col_name, col_type);
                        }
                        Err(e) => {
                            self.mode = AppMode::Normal;
                            self.status_message = format!("Type error: {}", e);
                        }
                    }
                }
            }
            Action::CancelTypeSelect => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }

            Action::CurrencySelectUp => {
                let n = crate::types::CurrencyKind::all().len();
                if self.currency_select_index > 0 {
                    self.currency_select_index -= 1;
                } else {
                    self.currency_select_index = n.saturating_sub(1);
                }
            }
            Action::CurrencySelectDown => {
                let n = crate::types::CurrencyKind::all().len();
                if n > 0 {
                    self.currency_select_index = (self.currency_select_index + 1) % n;
                }
            }
            Action::ApplyCurrencySelect => {
                let currency = crate::types::CurrencyKind::all()[self.currency_select_index];
                let s = self.stack.active_mut();
                s.push_undo();
                let col = s.cursor_col;
                if col < s.dataframe.columns.len() {
                    match s
                        .dataframe
                        .set_column_type(col, crate::types::ColumnType::Currency)
                    {
                        Ok(_) => {
                            s.dataframe.columns[col].currency = Some(currency);
                            s.dataframe.modified = true;
                            let col_name = s.dataframe.columns[col].name.clone();
                            self.status_message =
                                format!("Column '{}' set to Currency ({:?})", col_name, currency);
                        }
                        Err(e) => {
                            self.status_message = format!("Type error: {}", e);
                        }
                    }
                    self.mode = AppMode::Normal;
                } else {
                    self.mode = AppMode::Normal;
                }
            }
            Action::CancelCurrencySelect => {
                self.mode = AppMode::TypeSelect;
                self.status_message =
                    "Select column type (↑↓ navigate, Enter apply, Esc cancel)".to_string();
            }

            // ── Cell editing ──────────────────────────────────────────────────
            Action::OpenExternalEditor => {
                self.open_in_editor_pending = true;
            }
            Action::StartEdit => self.start_edit(),
            Action::EditInput(c) => {
                self.stack.active_mut().edit_input.insert_char(c);
            }
            Action::EditBackspace => {
                self.stack.active_mut().edit_input.delete_backward();
            }
            Action::EditForwardDelete => {
                self.stack.active_mut().edit_input.delete_forward();
            }
            Action::EditCursorLeft => {
                self.stack.active_mut().edit_input.move_cursor_left();
            }
            Action::EditCursorRight => {
                self.stack.active_mut().edit_input.move_cursor_right();
            }
            Action::EditCursorStart => {
                self.stack.active_mut().edit_input.move_cursor_start();
            }
            Action::EditCursorEnd => {
                self.stack.active_mut().edit_input.move_cursor_end();
            }
            Action::ApplyEdit => self.apply_edit(),
            Action::CancelEdit => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }

            // ── Aggregators ───────────────────────────────────────────────────
            Action::OpenAggregatorSelect => {
                let s = self.stack.active();
                let col = s.cursor_col;
                let col_meta = &s.dataframe.columns[col];

                self.agg_select_index = 0;
                self.agg_selected = col_meta.aggregators.iter().cloned().collect();

                self.mode = AppMode::AggregatorSelect;
                self.status_message = "Space to toggle, Enter to apply, Esc to cancel".to_string();
            }
            Action::ApplyAggregators => {
                if self.mode == AppMode::Calculating {
                    self.apply_aggregators();
                } else {
                    self.mode = AppMode::Calculating;
                    self.pending_action = Some(Action::ApplyAggregators);
                }
            }
            Action::AggregatorSelectUp => {
                if self.agg_select_index > 0 {
                    self.agg_select_index -= 1;
                } else {
                    let max = AggregatorKind::all().len();
                    if max > 0 {
                        self.agg_select_index = max - 1;
                    }
                }
            }
            Action::AggregatorSelectDown => {
                self.agg_select_index += 1;
                if self.agg_select_index >= AggregatorKind::all().len() {
                    self.agg_select_index = 0;
                }
            }
            Action::ToggleAggregatorSelection => {
                let all_aggs = AggregatorKind::all();
                if self.agg_select_index < all_aggs.len() {
                    let agg = all_aggs[self.agg_select_index];
                    if self.agg_selected.contains(&agg) {
                        self.agg_selected.remove(&agg);
                    } else {
                        self.agg_selected.insert(agg);
                    }
                }
            }
            Action::ClearAggregators => {
                let s = self.stack.active_mut();
                let col = s.cursor_col;
                s.dataframe.clear_aggregators(col);
                self.mode = AppMode::Normal;
                self.status_message = "Aggregators cleared".to_string();
            }
            Action::CancelAggregatorSelect => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::QuickAggregate => self.quick_aggregate(),

            // ── Row selection ─────────────────────────────────────────────────
            Action::SelectRow => self.select_row(true),
            Action::UnselectRow => self.select_row(false),
            Action::EnterGPrefix => {
                self.mode = AppMode::GPrefix;
                self.status_message = "g: (g)o top  (s)elect all  (u)nselect all".to_string();
            }
            Action::CancelGPrefix => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::SelectAllRows => {
                let s = self.stack.active_mut();
                for &idx in s.dataframe.row_order.iter() {
                    s.dataframe.selected_rows.insert(idx);
                }
                let count = s.dataframe.selected_rows.len();
                self.mode = AppMode::Normal;
                self.status_message = format!("Selected all {} rows", count);
            }
            Action::UnselectAllRows => {
                self.stack.active_mut().dataframe.selected_rows.clear();
                self.mode = AppMode::Normal;
                self.status_message = "All rows unselected".to_string();
            }

            // ── Clipboard & delete ────────────────────────────────────────────
            Action::CopySelectedRows => self.copy_selected_rows(),
            Action::PasteRows => self.paste_rows(),
            Action::DeleteSelectedRows => self.delete_selected_rows(),

            Action::EnterYPrefix => {
                self.mode = AppMode::YPrefix;
                self.status_message =
                    "y: (y)row  (c)cell  (l)column  (s)selected rows  Esc=cancel".to_string();
            }
            Action::CancelYPrefix => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::CopyCurrentCell => {
                let s = self.stack.active();
                let row = s.table_state.selected().unwrap_or(0);
                let col = s.cursor_col;
                let val = DataFrame::anyvalue_to_string_fmt(&s.dataframe.get_val(row, col));
                match crate::clipboard::copy_text(&val) {
                    Ok(_) => self.status_message = format!("Copied cell value: {}", val),
                    Err(e) => self.status_message = format!("Clipboard error: {}", e),
                }
                self.mode = AppMode::Normal;
            }
            Action::CopyCurrentRow => {
                let s = self.stack.active();
                let row = s.table_state.selected().unwrap_or(0);
                let headers: Vec<&str> = s
                    .dataframe
                    .columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                let row_data: Vec<String> = (0..s.dataframe.col_count())
                    .map(|c| DataFrame::anyvalue_to_string_fmt(&s.dataframe.get_val(row, c)))
                    .collect();
                match crate::clipboard::copy_to_clipboard(&headers, &[row_data]) {
                    Ok(_) => self.status_message = "Copied current row (TSV)".to_string(),
                    Err(e) => self.status_message = format!("Clipboard error: {}", e),
                }
                self.mode = AppMode::Normal;
            }
            Action::CopyCurrentColumn => {
                let s = self.stack.active();
                let col = s.cursor_col;
                let values: Vec<String> = (0..s.dataframe.visible_row_count())
                    .map(|r| DataFrame::anyvalue_to_string_fmt(&s.dataframe.get_val(r, col)))
                    .collect();
                let text = values.join("\n");
                match crate::clipboard::copy_text(&text) {
                    Ok(_) => {
                        self.status_message = format!(
                            "Copied column '{}' ({} values)",
                            s.dataframe.columns[col].name,
                            values.len()
                        )
                    }
                    Err(e) => self.status_message = format!("Clipboard error: {}", e),
                }
                self.mode = AppMode::Normal;
            }

            // ── Table Column settings ─────────────────────────────────────────────────
            Action::TogglePinColumn => {
                let s = self.stack.active_mut();
                s.push_undo();
                let col = s.cursor_col;
                if let Ok(new_col) = s.dataframe.toggle_pin_column(col) {
                    s.cursor_col = new_col;
                    s.table_state.select_column(Some(new_col));
                    let pinned = s.dataframe.columns[new_col].pinned;
                    self.status_message = if pinned {
                        format!("Pinned column '{}'", s.dataframe.columns[new_col].name)
                    } else {
                        format!("Unpinned column '{}'", s.dataframe.columns[new_col].name)
                    };
                }
            }

            // ── Help overlay (?) ──────────────────────────────────────────────
            Action::ShowHelp => {
                self.mode = AppMode::Help;
                self.status_message = "Press Esc or ? to close help".to_string();
            }
            Action::CloseHelp => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }

            // ── Derived sheet ─────────────────────────────────────────────────
            Action::CreateSheetFromSelection => self.create_sheet_from_selection(),

            // ── Save/Export ───────────────────────────────────────────────────
            Action::SaveFile => {
                self.saving_error = None;
                let default_path = self
                    .stack
                    .active()
                    .source_path
                    .as_deref()
                    .and_then(|p| {
                        std::env::current_dir()
                            .ok()
                            .and_then(|cwd| {
                                p.strip_prefix(&cwd)
                                    .ok()
                                    .map(|r| r.to_string_lossy().into_owned())
                            })
                            .or_else(|| Some(p.to_string_lossy().into_owned()))
                    })
                    .unwrap_or_else(|| self.stack.active().title.clone());

                self.autocomplete_candidates.clear();
                self.autocomplete_prefix.clear();
                self.autocomplete_idx = 0;
                self.saving_input = crate::ui::text_input::TextInput::with_value(default_path);
                self.mode = AppMode::Saving;
            }
            Action::SavingInput(c) => {
                self.saving_input.insert_char(c);
            }
            Action::SavingBackspace => {
                self.saving_input.delete_backward();
            }
            Action::SavingForwardDelete => {
                self.saving_input.delete_forward();
            }
            Action::SavingCursorLeft => {
                self.saving_input.move_cursor_left();
            }
            Action::SavingCursorRight => {
                self.saving_input.move_cursor_right();
            }
            Action::SavingCursorStart => {
                self.saving_input.move_cursor_start();
            }
            Action::SavingCursorEnd => {
                self.saving_input.move_cursor_end();
            }
            Action::ApplySave => {
                let path = expand_tilde(self.saving_input.as_str());
                match crate::data::io::save_file(&self.stack.active().dataframe, &path) {
                    Ok(_) => {
                        self.mode = AppMode::Normal;
                        self.status_message =
                            format!("Saved successfully to: {}", self.saving_input.as_str());
                        self.saving_error = None;
                    }
                    Err(e) => {
                        self.saving_error = Some(format!("Error: {}", e));
                    }
                }
            }
            Action::CancelSave => {
                self.mode = AppMode::Normal;
                self.saving_error = None;
            }
            Action::SavingAutocomplete => self.saving_autocomplete(),

            // ── Z Prefix (Column Operations) ──────────────────────────────────
            Action::EnterZPrefix => {
                self.mode = AppMode::ZPrefix;
                self.status_message =
                    "z: (e)dit name  (d)elete  (i)nsert  (s)elect  (u)nselect  (<-/->) move"
                        .to_string();
            }
            Action::SelectColumn => {
                let s = self.stack.active_mut();
                let col = s.cursor_col;
                s.dataframe.columns[col].selected = true;
                let sel_count = s.dataframe.columns.iter().filter(|c| c.selected).count();
                self.status_message = format!("{} column(s) selected", sel_count);
                self.mode = AppMode::ZPrefix;
            }
            Action::UnselectColumn => {
                let s = self.stack.active_mut();
                let col = s.cursor_col;
                s.dataframe.columns[col].selected = false;
                let sel_count = s.dataframe.columns.iter().filter(|c| c.selected).count();
                self.status_message = if sel_count == 0 {
                    "No columns selected".to_string()
                } else {
                    format!("{} column(s) selected", sel_count)
                };
                self.mode = AppMode::ZPrefix;
            }
            Action::CancelZPrefix => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::StartRenameColumn => {
                let s = self.stack.active_mut();
                s.rename_column_input =
                    TextInput::with_value(s.dataframe.columns[s.cursor_col].name.clone());
                self.mode = AppMode::RenamingColumn;
                self.status_message = "Rename column: ".to_string();
            }
            Action::RenameColumnInput(c) => {
                self.stack.active_mut().rename_column_input.insert_char(c);
            }
            Action::RenameColumnBackspace => {
                self.stack
                    .active_mut()
                    .rename_column_input
                    .delete_backward();
            }
            Action::RenameColumnForwardDelete => {
                self.stack.active_mut().rename_column_input.delete_forward();
            }
            Action::RenameColumnCursorLeft => {
                self.stack
                    .active_mut()
                    .rename_column_input
                    .move_cursor_left();
            }
            Action::RenameColumnCursorRight => {
                self.stack
                    .active_mut()
                    .rename_column_input
                    .move_cursor_right();
            }
            Action::RenameColumnCursorStart => {
                self.stack
                    .active_mut()
                    .rename_column_input
                    .move_cursor_start();
            }
            Action::RenameColumnCursorEnd => {
                self.stack
                    .active_mut()
                    .rename_column_input
                    .move_cursor_end();
            }
            Action::ApplyRenameColumn => self.apply_rename_column(),
            Action::CancelRenameColumn => {
                self.stack.active_mut().rename_column_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::DeleteColumn => self.delete_column(),
            Action::StartInsertColumn => {
                self.stack.active_mut().insert_column_input.clear();
                self.mode = AppMode::InsertingColumn;
                self.status_message = "Insert column: ".to_string();
            }
            Action::InsertColumnInput(c) => {
                self.stack.active_mut().insert_column_input.insert_char(c);
            }
            Action::InsertColumnBackspace => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .delete_backward();
            }
            Action::InsertColumnForwardDelete => {
                self.stack.active_mut().insert_column_input.delete_forward();
            }
            Action::InsertColumnCursorLeft => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .move_cursor_left();
            }
            Action::InsertColumnCursorRight => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .move_cursor_right();
            }
            Action::InsertColumnCursorStart => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .move_cursor_start();
            }
            Action::InsertColumnCursorEnd => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .move_cursor_end();
            }
            Action::ApplyInsertColumn => self.apply_insert_column(),
            Action::CancelInsertColumn => {
                self.stack.active_mut().insert_column_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }
            Action::MoveColumnLeft => self.move_col_left(),
            Action::MoveColumnRight => self.move_col_right(),

            Action::AdjustColumnWidth => self.adjust_column_width(),
            Action::AdjustAllColumnWidths => self.adjust_all_column_widths(),

            Action::IncreasePrecision => self.adjust_precision(1),
            Action::DecreasePrecision => self.adjust_precision(-1),

            Action::CreatePctColumn => self.create_pct_column(),
            Action::OpenPartitionSelect => self.open_partition_select(),
            Action::ApplyPartitionedPct => self.apply_partitioned_pct(),
            Action::PartitionSelectUp => {
                if self.partition_select_index > 0 {
                    self.partition_select_index -= 1;
                }
            }
            Action::PartitionSelectDown => {
                let ncols = self.stack.active().dataframe.columns.len();
                if self.partition_select_index + 1 < ncols {
                    self.partition_select_index += 1;
                }
            }
            Action::TogglePartitionSelection => {
                let s = self.stack.active();
                let col_name = s.dataframe.columns[self.partition_select_index]
                    .name
                    .clone();
                if self.partition_selected.contains(&col_name) {
                    self.partition_selected.remove(&col_name);
                } else {
                    self.partition_selected.insert(col_name);
                }
            }
            Action::CancelPartitionSelect => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }

            // ── JOIN wizard ───────────────────────────────────────────────────
            Action::OpenJoin => {
                self.join_other_df = None;
                self.join_other_title.clear();
                self.join_left_keys.clear();
                self.join_right_keys.clear();
                self.join_left_key_index = 0;
                self.join_right_key_index = 0;
                self.join_path_input.clear();
                self.join_path_error = None;
                self.join_pending_queue.clear();

                let s = self.stack.active();
                let is_overview = s.is_dir_sheet
                    || s.sqlite_db_path.is_some()
                    || s.duckdb_db_path.is_some()
                    || s.xlsx_db_path.is_some();

                if is_overview {
                    self.join_context_items = self.collect_join_context_items();
                    if self.join_context_items.is_empty() {
                        self.status_message = "No items available for JOIN".to_string();
                        return;
                    }
                    self.join_overview_cursor = 0;
                    self.join_overview_selected.clear();
                    self.mode = AppMode::JoinOverviewSelect;
                    self.status_message =
                        "JOIN: select items (Space=toggle, Enter=confirm, min 2)".to_string();
                } else {
                    self.join_source_index = 0;
                    self.join_context_items = self.collect_join_context_items();
                    self.mode = AppMode::JoinSelectSource;
                    self.status_message =
                        "JOIN: select source (↑↓ navigate, Enter select)".to_string();
                }
            }

            // ── JOIN overview multi-select ─────────────────────────────────────
            Action::JoinOverviewUp => {
                if self.join_overview_cursor > 0 {
                    self.join_overview_cursor -= 1;
                }
            }
            Action::JoinOverviewDown => {
                if self.join_overview_cursor + 1 < self.join_context_items.len() {
                    self.join_overview_cursor += 1;
                }
            }
            Action::JoinOverviewToggle => {
                let idx = self.join_overview_cursor;
                if let Some(pos) = self.join_overview_selected.iter().position(|&i| i == idx) {
                    self.join_overview_selected.remove(pos);
                } else {
                    self.join_overview_selected.push(idx);
                }
            }
            Action::JoinOverviewApply => {
                if self.join_overview_selected.len() < 2 {
                    self.status_message =
                        "SELECT at least 2 items for JOIN (Space to toggle)".to_string();
                    return;
                }
                let mut sorted_sel = self.join_overview_selected.clone();
                sorted_sel.sort_unstable();
                let items: Vec<crate::types::JoinContextItem> = sorted_sel
                    .iter()
                    .map(|&i| self.join_context_items[i].clone())
                    .collect();

                // Load both LEFT and RIGHT before mutating stack
                let left_result = load_join_context_item_df(&items[0]);
                let right_result = load_join_context_item_df(&items[1]);

                match (left_result, right_result) {
                    (Ok((left_df, left_title)), Ok((right_df, right_title))) => {
                        let left_sheet = crate::sheet::Sheet::new(left_title, left_df);
                        self.stack.push(left_sheet);
                        self.join_other_df = Some(right_df);
                        self.join_other_title = right_title;
                        self.join_pending_queue = items[2..].to_vec();
                        self.join_type_index = 0;
                        self.join_left_keys.clear();
                        self.join_right_keys.clear();
                        self.join_left_key_index = 0;
                        self.join_right_key_index = 0;
                        self.mode = AppMode::JoinSelectType;
                        self.status_message = "JOIN: select join type".to_string();
                    }
                    (Err(e), _) => {
                        self.status_message = format!("Failed to load LEFT table: {}", e);
                    }
                    (_, Err(e)) => {
                        self.status_message = format!("Failed to load RIGHT table: {}", e);
                    }
                }
            }
            Action::JoinOverviewCancel => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }

            Action::JoinSourceUp => {
                if self.join_source_index > 0 {
                    self.join_source_index -= 1;
                }
            }
            Action::JoinSourceDown => {
                let max =
                    self.join_context_items.len() + self.stack.sheet_titles_except_active().len();
                if self.join_source_index < max {
                    self.join_source_index += 1;
                }
            }
            Action::JoinSourceApply => {
                let ctx_count = self.join_context_items.len();
                if self.join_source_index == 0 {
                    // Browse file
                    self.join_path_input.clear();
                    self.join_path_error = None;
                    self.mode = AppMode::JoinInputPath;
                    self.status_message = "Type path to file to join with".to_string();
                } else if self.join_source_index <= ctx_count {
                    // Context item (sibling table / file / sheet)
                    let item = self.join_context_items[self.join_source_index - 1].clone();
                    match load_join_context_item_df(&item) {
                        Ok((df, title)) => {
                            self.join_other_df = Some(df);
                            self.join_other_title = title;
                            self.join_type_index = 0;
                            self.mode = AppMode::JoinSelectType;
                            self.status_message = "JOIN: select join type".to_string();
                        }
                        Err(e) => {
                            self.status_message = format!("Failed to load: {}", e);
                        }
                    }
                } else {
                    // Stack sheet (adjusted offset past context items)
                    let stack_idx = self.join_source_index - 1 - ctx_count;
                    if let Some(df) = self.stack.clone_sheet_dataframe(stack_idx) {
                        let title = self
                            .stack
                            .sheet_titles_except_active()
                            .into_iter()
                            .nth(stack_idx)
                            .unwrap_or_default();
                        self.join_other_df = Some(df);
                        self.join_other_title = title;
                        self.join_type_index = 0;
                        self.mode = AppMode::JoinSelectType;
                        self.status_message = "JOIN: select join type".to_string();
                    } else {
                        self.status_message = "Failed to access sheet".to_string();
                    }
                }
            }
            Action::JoinSourceCancel => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
            }

            // JOIN path input
            Action::JoinPathInput(c) => {
                self.join_path_input.insert_char(c);
            }
            Action::JoinPathBackspace => {
                self.join_path_input.delete_backward();
            }
            Action::JoinPathForwardDelete => {
                self.join_path_input.delete_forward();
            }
            Action::JoinPathCursorLeft => {
                self.join_path_input.move_cursor_left();
            }
            Action::JoinPathCursorRight => {
                self.join_path_input.move_cursor_right();
            }
            Action::JoinPathCursorStart => {
                self.join_path_input.move_cursor_start();
            }
            Action::JoinPathCursorEnd => {
                self.join_path_input.move_cursor_end();
            }
            Action::JoinPathAutocomplete => self.join_path_autocomplete(),
            Action::JoinPathApply => {
                let raw = self.join_path_input.as_str().to_string();
                let path = expand_tilde(&raw);
                match crate::data::io::load_file(&path, None) {
                    Ok(df) => {
                        self.join_other_title = path
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_else(|| raw.clone());
                        self.join_other_df = Some(df);
                        self.join_type_index = 0;
                        self.mode = AppMode::JoinSelectType;
                        self.status_message = "JOIN: select join type".to_string();
                    }
                    Err(e) => {
                        self.join_path_error = Some(format!("{}", e));
                        self.status_message = format!("Error: {}", e);
                    }
                }
            }
            Action::JoinPathCancel => {
                self.mode = AppMode::JoinSelectSource;
                self.join_path_error = None;
                self.status_message = "JOIN: select source".to_string();
            }

            // JOIN type selection
            Action::JoinTypeUp => {
                if self.join_type_index > 0 {
                    self.join_type_index -= 1;
                }
            }
            Action::JoinTypeDown => {
                if self.join_type_index + 1 < crate::data::join::JoinType::all().len() {
                    self.join_type_index += 1;
                }
            }
            Action::JoinTypeApply => {
                self.join_left_keys.clear();
                self.join_left_key_index = 0;
                self.mode = AppMode::JoinSelectLeftKeys;
                self.status_message =
                    "JOIN: select LEFT key columns (Space=toggle, Enter=next)".to_string();
            }
            Action::JoinTypeCancel => {
                self.mode = AppMode::JoinSelectSource;
                self.status_message = "JOIN: select source".to_string();
            }

            // JOIN left key selection
            Action::JoinLeftKeyUp => {
                if self.join_left_key_index > 0 {
                    self.join_left_key_index -= 1;
                }
            }
            Action::JoinLeftKeyDown => {
                let n = self.stack.active().dataframe.columns.len();
                if self.join_left_key_index + 1 < n {
                    self.join_left_key_index += 1;
                }
            }
            Action::JoinLeftKeyToggle => {
                let col_name = self
                    .stack
                    .active()
                    .dataframe
                    .columns
                    .get(self.join_left_key_index)
                    .map(|c| c.name.clone());
                if let Some(name) = col_name {
                    if let Some(pos) = self.join_left_keys.iter().position(|k| k == &name) {
                        self.join_left_keys.remove(pos);
                    } else {
                        self.join_left_keys.push(name);
                    }
                }
            }
            Action::JoinLeftKeyApply => {
                if self.join_left_keys.is_empty() {
                    self.status_message = "Select at least one key column".to_string();
                } else {
                    // Auto-pre-select right keys matching left key names
                    let right_cols: Vec<String> = self
                        .join_other_df
                        .as_ref()
                        .map(|df| df.columns.iter().map(|c| c.name.clone()).collect())
                        .unwrap_or_default();
                    self.join_right_keys = self
                        .join_left_keys
                        .iter()
                        .filter(|lk| right_cols.contains(lk))
                        .cloned()
                        .collect();
                    self.join_right_key_index = 0;
                    self.mode = AppMode::JoinSelectRightKeys;
                    self.status_message =
                        "JOIN: select RIGHT key columns (Space=toggle, Enter=execute)".to_string();
                }
            }
            Action::JoinLeftKeyCancel => {
                self.mode = AppMode::JoinSelectType;
                self.status_message = "JOIN: select join type".to_string();
            }

            // JOIN right key selection
            Action::JoinRightKeyUp => {
                if self.join_right_key_index > 0 {
                    self.join_right_key_index -= 1;
                }
            }
            Action::JoinRightKeyDown => {
                let n = self
                    .join_other_df
                    .as_ref()
                    .map(|df| df.columns.len())
                    .unwrap_or(0);
                if self.join_right_key_index + 1 < n {
                    self.join_right_key_index += 1;
                }
            }
            Action::JoinRightKeyToggle => {
                let col_name = self
                    .join_other_df
                    .as_ref()
                    .and_then(|df| df.columns.get(self.join_right_key_index))
                    .map(|c| c.name.clone());
                if let Some(name) = col_name {
                    if let Some(pos) = self.join_right_keys.iter().position(|k| k == &name) {
                        self.join_right_keys.remove(pos);
                    } else {
                        self.join_right_keys.push(name);
                    }
                }
            }
            Action::JoinRightKeyApply => {
                if self.join_right_keys.len() != self.join_left_keys.len() {
                    self.status_message = format!(
                        "Key count mismatch: {} left vs {} right — must match",
                        self.join_left_keys.len(),
                        self.join_right_keys.len()
                    );
                } else if self.join_right_keys.is_empty() {
                    self.status_message = "Select at least one right key column".to_string();
                } else {
                    self.execute_join();
                }
            }
            Action::JoinRightKeyCancel => {
                self.mode = AppMode::JoinSelectLeftKeys;
                self.status_message = "JOIN: select LEFT key columns".to_string();
            }

            Action::None => {}
        }
    }

    // ── Column width adjustment ───────────────────────────────────────────────

    fn adjust_column_width(&mut self) {
        let s = self.stack.active_mut();
        let col = s.cursor_col;
        if col >= s.dataframe.columns.len() {
            return;
        }
        if s.dataframe.columns[col].width_expanded {
            // Toggle OFF: contract to header width (min_width)
            let header_w = s.dataframe.columns[col].min_width;
            s.dataframe.columns[col].width = header_w;
            s.dataframe.columns[col].width_expanded = false;
            let col_name = s.dataframe.columns[col].name.clone();
            self.status_message = format!("Column '{}' width reset to header", col_name);
        } else {
            // Toggle ON: expand to content width
            s.dataframe.calc_column_width(col, u16::MAX, usize::MAX);
            s.dataframe.columns[col].width_expanded = true;
            let col_name = s.dataframe.columns[col].name.clone();
            let width = s.dataframe.columns[col].width;
            self.status_message = format!("Column '{}' width set to {}", col_name, width);
        }
    }

    fn adjust_all_column_widths(&mut self) {
        let s = self.stack.active_mut();
        let any_expanded = s.dataframe.columns.iter().any(|c| c.width_expanded);
        if any_expanded {
            // Toggle OFF: contract all to header width
            for col_meta in s.dataframe.columns.iter_mut() {
                col_meta.width = col_meta.min_width;
                col_meta.width_expanded = false;
            }
            self.mode = AppMode::Normal;
            self.status_message = "All column widths reset to header".to_string();
        } else {
            // Toggle ON: expand all to content width
            s.dataframe.calc_widths(u16::MAX, usize::MAX);
            for col_meta in s.dataframe.columns.iter_mut() {
                col_meta.width_expanded = true;
            }
            self.mode = AppMode::Normal;
            self.status_message = "All column widths adjusted to content".to_string();
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

    fn create_pct_column(&mut self) {
        let s = self.stack.active_mut();
        let col_idx = s.cursor_col;
        if col_idx >= s.dataframe.columns.len() {
            return;
        }

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
            self.status_message = "Percent column only works for numeric columns".to_string();
            return;
        }

        let col_name = meta.name.clone();
        let new_name = format!("{}_pct", col_name);

        // Expression: col / sum(col)
        let expr_str = format!("{} / sum({})", col_name, col_name);
        if let Ok(expr) = crate::data::expression::Expr::parse(&expr_str) {
            s.push_undo();
            if let Err(e) = s.dataframe.add_computed_column(&new_name, &expr, col_idx) {
                self.status_message = format!("Error: {}", e);
            } else {
                // Set type to Percentage
                if let Some(c) = s.dataframe.columns.iter_mut().find(|c| c.name == new_name) {
                    c.col_type = crate::types::ColumnType::Percentage;
                    c.precision = 2;
                }
                self.status_message = format!("Created column '{}'", new_name);
            }
        }
        self.mode = AppMode::Normal;
    }

    fn open_partition_select(&mut self) {
        let s = self.stack.active();
        let col_idx = s.cursor_col;
        if col_idx >= s.dataframe.columns.len() {
            return;
        }

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

        self.partition_select_index = 0;
        self.partition_selected.clear();
        self.mode = AppMode::PartitionSelect;
    }

    fn apply_partitioned_pct(&mut self) {
        let s = self.stack.active_mut();
        let col_idx = s.cursor_col;
        let col_name = s.dataframe.columns[col_idx].name.clone();

        let mut partition_cols: Vec<String> = self.partition_selected.iter().cloned().collect();
        partition_cols.sort(); // Consistent naming

        let mut new_name = format!("{}_", col_name);
        for pc in &partition_cols {
            new_name.push_str(pc);
            new_name.push('_');
        }
        new_name.push_str("pct");

        // Use Polars Lazy API directly for Window Function
        use polars::prelude::*;
        let target = col(&col_name);
        let partition_exprs: Vec<polars::prelude::Expr> = partition_cols.iter().map(col).collect();

        // Window expression: col / sum(col).over(partition_cols)
        let pct_expr = (target.clone().cast(DataType::Float64)
            / target.sum().over(partition_exprs).cast(DataType::Float64))
        .alias(&new_name);

        s.push_undo();
        match s
            .dataframe
            .df
            .clone()
            .lazy()
            .with_column(pct_expr)
            .collect()
        {
            Ok(new_df) => {
                s.dataframe.df = new_df;
                let mut meta = crate::data::column::ColumnMeta::new(new_name.clone());
                meta.col_type = crate::types::ColumnType::Percentage;
                meta.precision = 2;

                // Find insertion position (after current col)
                let target_idx = col_idx + 1;
                s.dataframe.columns.insert(target_idx, meta);

                // Re-align df columns if necessary (though with_column appends, we might need select to reorder)
                let names: Vec<String> =
                    s.dataframe.columns.iter().map(|c| c.name.clone()).collect();
                if let Ok(reordered_df) = s.dataframe.df.select(names) {
                    s.dataframe.df = reordered_df;
                }

                s.dataframe.calc_column_width(target_idx, 40, 1000);
                self.status_message = format!("Created column '{}'", new_name);
            }
            Err(e) => {
                self.status_message = format!("Polars error: {}", e);
            }
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
            self.mode = AppMode::Normal;
            self.status_message = format!(
                "Returned to '{}' (depth {})",
                self.stack.active().title,
                self.stack.depth()
            );
        } else {
            self.mode = AppMode::ConfirmQuit;
            self.status_message = "Quit? Press 'y' to confirm, 'n' to cancel".to_string();
        }
    }

    // ── Search (/) ─────────────────────────────────────────────────────────────

    fn apply_search(&mut self) {
        let s = self.stack.active_mut();
        let pattern = s.search_input.as_str().to_string();
        let col = s.search_col.unwrap_or(s.cursor_col);
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
        let col = s.search_col.unwrap_or(s.cursor_col);
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
        let col = s.search_col.unwrap_or(s.cursor_col);
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

        // Vectorized equality scan via Polars str() iteration
        let matching_display_rows = s.dataframe.find_rows_by_value(col, &target);
        let count = matching_display_rows.len();
        for display_idx in matching_display_rows {
            if display_idx < s.dataframe.row_order.len() {
                s.dataframe
                    .selected_rows
                    .insert(s.dataframe.row_order[display_idx]);
            }
        }
        self.status_message = format!(
            "Selected {} rows where {} = '{}'",
            count, s.dataframe.columns[col].name, target
        );
    }

    // ── Select by regex (|) ────────────────────────────────────────────────────

    fn apply_select_by_regex(&mut self) {
        let s = self.stack.active_mut();
        let input = s.select_regex_input.as_str().to_string();
        let col = s.cursor_col;

        if input.starts_with("!=") || input.starts_with("!= ") {
            let expr_str = input.strip_prefix("!= ").unwrap_or(&input[2..]);
            match Expr::parse(expr_str) {
                Ok(expr) => {
                    let mut selected_indices = Vec::new();
                    // Fast path: Polars
                    if let Ok(polars_expr) = expr.to_polars_expr() {
                        if let Ok(visible_df) = s.dataframe.get_visible_df() {
                            if let Ok(mask_df) = visible_df
                                .lazy()
                                .select([polars_expr.alias("mask")])
                                .collect()
                            {
                                if let Ok(mask_col) = mask_df.column("mask") {
                                    if let Ok(ca) = mask_col.bool() {
                                        for (i, val) in ca.into_iter().enumerate() {
                                            if val.unwrap_or(false) {
                                                selected_indices.push(i);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if selected_indices.is_empty() {
                        // Slow path: manual evaluation
                        let col_lookup: std::collections::HashMap<&str, usize> = s
                            .dataframe
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| (c.name.as_str(), i))
                            .collect();
                        for i in 0..s.dataframe.visible_row_count() {
                            let physical = s.dataframe.row_order[i];
                            let val = expr.eval(physical, &col_lookup, &s.dataframe);
                            if let Some(true) = val.as_bool() {
                                selected_indices.push(i);
                            }
                        }
                    }

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
        let s = self.stack.active_mut();
        let input = s.expr_input.as_str().to_string();

        if input.is_empty() {
            self.mode = AppMode::Normal;
            self.status_message.clear();
            return;
        }

        if self.expr_history.last() != Some(&input) {
            self.expr_history.push(input.clone());
        }
        self.history_idx = None;
        self.autocomplete_candidates.clear();

        match Expr::parse(&input) {
            Ok(expr) => {
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
        if self.autocomplete_candidates.is_empty() {
            let input_str = s.expr_input.as_str();
            let rpos = input_str.rfind(|c: char| !c.is_alphanumeric() && c != '_');
            let (prefix, word) = if let Some(p) = rpos {
                input_str.split_at(p + 1)
            } else {
                ("", input_str)
            };

            let word_lower = word.to_lowercase();
            // Collect candidates
            let mut matches = Vec::new();
            for col in &s.dataframe.columns {
                let lower = col.name.to_lowercase();
                if lower.starts_with(&word_lower) || lower.contains(&word_lower) {
                    matches.push(col.name.clone());
                }
            }

            if matches.is_empty() {
                return;
            }
            self.autocomplete_candidates = matches;
            self.autocomplete_idx = 0;
            self.autocomplete_prefix = prefix.to_string();
        } else {
            self.autocomplete_idx =
                (self.autocomplete_idx + 1) % self.autocomplete_candidates.len();
        }

        let completion = &self.autocomplete_candidates[self.autocomplete_idx];
        let new_val = format!("{}{}", self.autocomplete_prefix, completion);
        s.expr_input = TextInput::with_value(new_val);
    }

    fn select_regex_autocomplete(&mut self) {
        let s = self.stack.active_mut();
        let input_str = s.select_regex_input.as_str();

        // Autocomplete is only meaningful in expression mode (input starts with !=)
        if !input_str.starts_with("!=") {
            return;
        }

        if self.autocomplete_candidates.is_empty() {
            let rpos = input_str.rfind(|c: char| !c.is_alphanumeric() && c != '_');
            let (prefix, word) = if let Some(p) = rpos {
                input_str.split_at(p + 1)
            } else {
                ("", input_str)
            };

            let word_lower = word.to_lowercase();
            let mut matches = Vec::new();
            for col in &s.dataframe.columns {
                let lower = col.name.to_lowercase();
                if lower.starts_with(&word_lower) || lower.contains(&word_lower) {
                    matches.push(col.name.clone());
                }
            }

            if matches.is_empty() {
                return;
            }
            self.autocomplete_candidates = matches;
            self.autocomplete_idx = 0;
            self.autocomplete_prefix = prefix.to_string();
        } else {
            self.autocomplete_idx =
                (self.autocomplete_idx + 1) % self.autocomplete_candidates.len();
        }

        let completion = &self.autocomplete_candidates[self.autocomplete_idx];
        let new_val = format!("{}{}", self.autocomplete_prefix, completion);
        s.select_regex_input = TextInput::with_value(new_val);
    }

    // ── Pivot autocomplete & history ───────────────────────────────────────────

    fn pivot_autocomplete(&mut self) {
        // Aggregation functions offered as candidates alongside column names
        const AGG_FUNCS: &[&str] = &["sum", "count", "mean", "median", "min", "max"];

        let s = self.stack.active_mut();
        if self.autocomplete_candidates.is_empty() {
            let input_str = s.pivot_input.as_str();
            let rpos = input_str.rfind(|c: char| !c.is_alphanumeric() && c != '_');
            let (prefix, word) = if let Some(p) = rpos {
                input_str.split_at(p + 1)
            } else {
                ("", input_str)
            };

            let word_lower = word.to_lowercase();
            let mut matches = Vec::new();
            // Column names first
            for col in &s.dataframe.columns {
                let lower = col.name.to_lowercase();
                if lower.starts_with(&word_lower) || lower.contains(&word_lower) {
                    matches.push(col.name.clone());
                }
            }
            // Then aggregation function names
            for func in AGG_FUNCS {
                let lower = func.to_lowercase();
                if lower.starts_with(&word_lower) && !matches.iter().any(|m| m == func) {
                    matches.push(func.to_string());
                }
            }

            if matches.is_empty() {
                return;
            }
            self.autocomplete_candidates = matches;
            self.autocomplete_idx = 0;
            self.autocomplete_prefix = prefix.to_string();
        } else {
            self.autocomplete_idx =
                (self.autocomplete_idx + 1) % self.autocomplete_candidates.len();
        }

        let completion = self.autocomplete_candidates[self.autocomplete_idx].clone();
        let new_val = format!("{}{}", self.autocomplete_prefix, completion);
        s.pivot_input = TextInput::with_value(new_val);
    }

    fn pivot_history_prev(&mut self) {
        if self.pivot_history.is_empty() {
            return;
        }
        let new_idx = match self.pivot_history_idx {
            Some(i) if i > 0 => i - 1,
            Some(i) => i,
            None => self.pivot_history.len() - 1,
        };
        self.pivot_history_idx = Some(new_idx);
        let val = self.pivot_history[new_idx].clone();
        self.stack.active_mut().pivot_input = TextInput::with_value(val);
    }

    fn pivot_history_next(&mut self) {
        if let Some(idx) = self.pivot_history_idx {
            if idx + 1 < self.pivot_history.len() {
                let new_idx = idx + 1;
                self.pivot_history_idx = Some(new_idx);
                let val = self.pivot_history[new_idx].clone();
                self.stack.active_mut().pivot_input = TextInput::with_value(val);
            } else {
                self.pivot_history_idx = None;
                self.stack.active_mut().pivot_input = TextInput::new();
            }
        }
    }

    // ── Chart open logic ───────────────────────────────────────────────────────

    fn open_chart(&mut self) {
        use crate::types::{ChartAgg, ColumnType};
        let s = self.stack.active();
        let cur_col = s.cursor_col;
        let cur_type = s.dataframe.columns[cur_col].col_type;

        // Find the first pinned column that is not the cursor column
        let ref_col = s
            .dataframe
            .columns
            .iter()
            .enumerate()
            .find(|(i, c)| c.pinned && *i != cur_col)
            .map(|(i, _)| i);

        let ref_type = ref_col.map(|i| s.dataframe.columns[i].col_type);

        let is_date = |ct: ColumnType| matches!(ct, ColumnType::Date | ColumnType::Datetime);
        let is_numeric = |ct: ColumnType| {
            matches!(
                ct,
                ColumnType::Integer
                    | ColumnType::Float
                    | ColumnType::Percentage
                    | ColumnType::Currency
            )
        };
        let is_categorical = |ct: ColumnType| !is_date(ct) && !is_numeric(ct);

        let col_name = s.dataframe.columns[cur_col].name.clone();

        if let (Some(ref_idx), Some(rtype)) = (ref_col, ref_type) {
            if is_date(rtype) && is_numeric(cur_type) {
                // Date × Numeric → aggregation popup → line chart
                self.chart_ref_col = Some(ref_idx);
                self.chart_agg_index = 0;
                self.mode = AppMode::ChartAggSelect;
                self.status_message =
                    "Select aggregation for line chart (↑↓ navigate, Enter confirm)".to_string();
                return;
            }
            if is_date(rtype) && is_categorical(cur_type) {
                // Date × Categorical → auto count → line chart
                self.chart_ref_col = Some(ref_idx);
                self.chart_agg = ChartAgg::Count;
                self.mode = AppMode::Chart;
                self.status_message =
                    format!("Line chart: count('{}') by date — Esc to exit", col_name);
                return;
            }
            if is_categorical(rtype) && is_numeric(cur_type) {
                // Categorical × Numeric → aggregation popup → bar chart
                self.chart_ref_col = Some(ref_idx);
                self.chart_agg_index = 0;
                self.mode = AppMode::ChartAggSelect;
                self.status_message =
                    "Select aggregation for bar chart (↑↓ navigate, Enter confirm)".to_string();
                return;
            }
        }

        // Fallback: normal single-column chart
        self.chart_ref_col = None;
        self.mode = AppMode::Chart;
        self.status_message = format!("Chart: {} — Press 'v', 'q' or Esc to exit", col_name);
    }

    fn expr_history_prev(&mut self) {
        if self.expr_history.is_empty() {
            return;
        }

        let mut reset_input = false;
        if let Some(mut idx) = self.history_idx {
            if idx > 0 {
                idx -= 1;
                self.history_idx = Some(idx);
                reset_input = true;
            }
        } else {
            self.history_idx = Some(self.expr_history.len() - 1);
            reset_input = true;
        }

        if reset_input {
            let s = self.stack.active_mut();
            if let Some(idx) = self.history_idx {
                s.expr_input = TextInput::with_value(self.expr_history[idx].clone());
            }
        }
    }

    fn expr_history_next(&mut self) {
        if let Some(idx) = self.history_idx {
            let next_idx = idx + 1;
            if next_idx < self.expr_history.len() {
                self.history_idx = Some(next_idx);
                self.stack.active_mut().expr_input =
                    TextInput::with_value(self.expr_history[next_idx].clone());
            } else {
                self.history_idx = None;
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
                let row_order: Vec<usize> = (0..row_count).collect();

                let mut df = DataFrame {
                    df: pdf,
                    columns,
                    row_order: row_order.clone().into(),
                    original_order: row_order.into(),
                    selected_rows: HashSet::new(),
                    modified: false,
                    aggregates_cache: None,
                };
                // Inherit original column type for Value column
                df.columns[0].col_type = s.dataframe.columns[col].col_type;
                df.columns[1].col_type = ColumnType::Integer;
                df.calc_widths(40, 500);

                let mut freq_sheet = Sheet::new(format!("Freq: {}", col_name), df);
                freq_sheet.sort_col = Some(1); // Count column is pre-sorted
                freq_sheet.sort_desc = true;
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
                let row_order: Vec<usize> = (0..row_count).collect();

                let mut new_df = crate::data::dataframe::DataFrame {
                    df: pdf,
                    columns,
                    row_order: row_order.clone().into(),
                    original_order: row_order.into(),
                    selected_rows: std::collections::HashSet::new(),
                    modified: false,
                    aggregates_cache: None,
                };
                new_df.calc_widths(40, 1000);

                let pinned_names: Vec<&str> = pinned_cols
                    .iter()
                    .map(|&c| s.dataframe.columns[c].name.as_str())
                    .collect();
                let title = format!("MultiFreq: {}", pinned_names.join(", "));

                let mut freq_sheet = crate::sheet::Sheet::new(title, new_df);
                freq_sheet.sort_col = Some(pinned_cols.len()); // Count column
                freq_sheet.sort_desc = true;
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

    fn apply_pivot_table(&mut self) {
        let formula_str = self.stack.active().pivot_input.as_str().to_string();
        if formula_str.is_empty() {
            self.mode = AppMode::Normal;
            return;
        }

        // Save to history (deduplicated)
        if self.pivot_history.last() != Some(&formula_str) {
            self.pivot_history.push(formula_str.clone());
        }
        self.pivot_history_idx = None;
        self.autocomplete_candidates.clear();

        let expr = match crate::data::expression::Expr::parse(&formula_str) {
            Ok(e) => e,
            Err(e) => {
                self.status_message = format!("Formula error: {}", e);
                self.mode = AppMode::Normal;
                return;
            }
        };

        let (index_cols, pivot_col) = {
            let s = self.stack.active();
            let index_cols: Vec<String> = s
                .dataframe
                .columns
                .iter()
                .filter(|c| c.pinned)
                .map(|c| c.name.clone())
                .collect();
            let pivot_col = s.dataframe.columns[s.cursor_col].name.clone();
            (index_cols, pivot_col)
        };

        match self
            .stack
            .active()
            .dataframe
            .create_pivot_table(&index_cols, &pivot_col, &expr)
        {
            Ok((pdf, columns)) => {
                let row_count = pdf.height();
                let row_order: Vec<usize> = (0..row_count).collect();

                let mut new_df = crate::data::dataframe::DataFrame {
                    df: pdf,
                    columns,
                    row_order: row_order.clone().into(),
                    original_order: row_order.into(),
                    selected_rows: std::collections::HashSet::new(),
                    modified: false,
                    aggregates_cache: None,
                };
                new_df.calc_widths(40, 1000);

                let mut pivot_sheet = crate::sheet::Sheet::new(
                    format!("Pivot: {} by {}", formula_str, pivot_col),
                    new_df,
                );
                pivot_sheet.sheet_type = SheetType::PivotTable {
                    index_cols,
                    pivot_col,
                    formula: formula_str,
                };
                self.stack.push(pivot_sheet);
                self.mode = AppMode::Normal;
                self.status_message = format!("Pivot table created: {} rows", row_count);
            }
            Err(e) => {
                self.status_message = format!("Pivot error: {}", e);
                self.mode = AppMode::Normal;
            }
        }
        self.stack.active_mut().pivot_input.clear();
    }
    pub fn open_directory_row(&mut self) {
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
            let target_path = if let Some(ref paths) = s.explicit_row_paths {
                paths.get(row_idx).cloned().unwrap_or_else(|| {
                    let current_dir_str = s.title.clone();
                    let base_path = std::path::PathBuf::from(
                        if current_dir_str == "." || current_dir_str.is_empty() {
                            "."
                        } else {
                            &current_dir_str
                        },
                    );
                    base_path.join(&name)
                })
            } else {
                let current_dir_str = s.title.clone();
                let base_path = std::path::PathBuf::from(
                    if current_dir_str == "." || current_dir_str.is_empty() {
                        "."
                    } else {
                        &current_dir_str
                    },
                );
                base_path.join(&name)
            };

            if is_dir {
                match crate::data::io::load_directory(&target_path) {
                    Ok(new_df) => {
                        let mut new_sheet = crate::sheet::Sheet::new(
                            target_path.to_string_lossy().into_owned(),
                            new_df,
                        );
                        new_sheet.is_dir_sheet = true;
                        self.stack.push(new_sheet);
                    }
                    Err(e) => {
                        self.status_message = format!("Failed to open directory: {}", e);
                    }
                }
            } else if supported {
                let target_ext = target_path
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("")
                    .to_lowercase();
                // Returns (df, sqlite_db_path, duckdb_db_path, xlsx_db_path)
                #[allow(clippy::type_complexity)]
                let load_result: Result<
                    (
                        crate::data::dataframe::DataFrame,
                        Option<std::path::PathBuf>,
                        Option<std::path::PathBuf>,
                        Option<std::path::PathBuf>,
                    ),
                    _,
                > = if target_ext == "db" {
                    match crate::data::io::load_sqlite_overview(&target_path) {
                        Ok(df) => Ok((df, Some(target_path.clone()), None, None)),
                        Err(_) => crate::data::io::load_duckdb_overview(&target_path)
                            .map(|df| (df, None, Some(target_path.clone()), None)),
                    }
                } else if matches!(target_ext.as_str(), "sqlite" | "sqlite3") {
                    crate::data::io::load_sqlite_overview(&target_path)
                        .map(|df| (df, Some(target_path.clone()), None, None))
                } else if matches!(target_ext.as_str(), "duckdb" | "ddb") {
                    crate::data::io::load_duckdb_overview(&target_path)
                        .map(|df| (df, None, Some(target_path.clone()), None))
                } else if matches!(target_ext.as_str(), "xlsx" | "xls" | "xlsm" | "xlsb") {
                    match crate::data::io::excel_sheet_names(&target_path) {
                        Ok(names) if names.len() > 1 => {
                            crate::data::io::load_excel_overview(&target_path)
                                .map(|df| (df, None, None, Some(target_path.clone())))
                        }
                        _ => crate::data::io::load_file(&target_path, None)
                            .map(|df| (df, None, None, None)),
                    }
                } else {
                    crate::data::io::load_file(&target_path, None).map(|df| (df, None, None, None))
                };
                match load_result {
                    Ok((new_df, sqlite_path, duckdb_path, xlsx_path)) => {
                        let mut new_sheet = crate::sheet::Sheet::new(
                            target_path.to_string_lossy().into_owned(),
                            new_df,
                        );
                        new_sheet.sqlite_db_path = sqlite_path;
                        new_sheet.duckdb_db_path = duckdb_path;
                        new_sheet.xlsx_db_path = xlsx_path;
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

        match crate::data::io::load_sqlite_table_by_name(&db_path, &table_name) {
            Ok(new_df) => {
                let row_count = new_df.visible_row_count();
                let mut new_sheet = crate::sheet::Sheet::new(
                    format!("{} :: {}", db_path.display(), table_name),
                    new_df,
                );
                new_sheet.sqlite_source_path = Some(db_path.clone());
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
                    "csv", "tsv", "json", "parquet", "xlsx", "xls", "xlsm", "xlsb", "sqlite",
                    "sqlite3", "db", "duckdb", "ddb", "txt",
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
        let join_type = crate::data::join::JoinType::all()[self.join_type_index];
        let left_keys = self.join_left_keys.clone();
        let right_keys = self.join_right_keys.clone();
        let other_title = self.join_other_title.clone();

        let left_df = self.stack.active().dataframe.clone();
        let right_df = match self.join_other_df.take() {
            Some(df) => df,
            None => {
                self.status_message = "JOIN: no right-hand table loaded".to_string();
                self.mode = AppMode::Normal;
                return;
            }
        };

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
                let result_title = format!("{} JOIN {}", left_title, other_title);
                let new_sheet = crate::sheet::Sheet::new(result_title, result_df);
                self.stack.push(new_sheet);

                // Continue chained join if items are queued
                if !self.join_pending_queue.is_empty() {
                    let next = self.join_pending_queue.remove(0);
                    match load_join_context_item_df(&next) {
                        Ok((df, title)) => {
                            self.join_other_df = Some(df);
                            self.join_other_title = title;
                            self.join_left_keys.clear();
                            self.join_right_keys.clear();
                            self.join_left_key_index = 0;
                            self.join_right_key_index = 0;
                            self.mode = AppMode::JoinSelectType;
                            let remaining = self.join_pending_queue.len();
                            self.status_message = if remaining > 0 {
                                format!("JOIN: {} more table(s) to add — select type", remaining)
                            } else {
                                "JOIN: select join type".to_string()
                            };
                        }
                        Err(e) => {
                            self.join_pending_queue.clear();
                            self.mode = AppMode::Normal;
                            self.status_message = format!(
                                "JOIN result: {} rows (next load failed: {})",
                                row_count, e
                            );
                        }
                    }
                } else {
                    self.mode = AppMode::Normal;
                    self.status_message = format!("JOIN result: {} rows", row_count);
                }
            }
            Err(e) => {
                self.join_other_df = Some(right_df); // restore so user can try again
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

        match crate::data::io::load_duckdb_table_by_name(&db_path, &table_name) {
            Ok(new_df) => {
                let row_count = new_df.visible_row_count();
                let mut new_sheet = crate::sheet::Sheet::new(
                    format!("{} :: {}", db_path.display(), table_name),
                    new_df,
                );
                new_sheet.duckdb_source_path = Some(db_path.clone());
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
            let sheet =
                crate::sheet::Sheet::new(format!("Filter: {} = {}", cols_str, vals_str), parent_df);
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
            let sheet =
                crate::sheet::Sheet::new(format!("Filter: {} = {}", cols_str, vals_str), parent_df);
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

        let columns = vec![
            crate::data::column::ColumnMeta::new("Column".to_string()),
            crate::data::column::ColumnMeta::new("Value".to_string()),
        ];

        let mut col_names = Vec::new();
        let mut col_values = Vec::new();

        for i in 0..s.dataframe.columns.len() {
            col_names.push(s.dataframe.columns[i].name.clone());
            col_values.push(s.dataframe.get_physical(physical_row, i).to_string());
        }

        let s1 = polars::prelude::Series::new("Column".into(), &col_names);
        let s2 = polars::prelude::Series::new("Value".into(), &col_values);
        let pdf = polars::prelude::DataFrame::new_infer_height(vec![s1.into(), s2.into()])
            .unwrap_or_else(|_| polars::prelude::DataFrame::empty());

        let row_count = col_names.len();
        let row_order: Vec<usize> = (0..row_count).collect();

        let mut df = crate::data::dataframe::DataFrame {
            df: pdf,
            columns,
            row_order: row_order.clone().into(),
            original_order: row_order.into(),
            selected_rows: std::collections::HashSet::new(),
            modified: false,
            aggregates_cache: None,
        };

        df.calc_widths(40, 500);

        let sheet = crate::sheet::Sheet::new(format!("Row {}", physical_row), df);
        self.stack.push(sheet);
        self.status_message = format!("Transposed row {}", physical_row);
    }

    fn transpose_table(&mut self) {
        // Compute new df in isolated scope so the immutable borrow of active() is released
        let result: Option<(crate::data::dataframe::DataFrame, String)> = {
            let s = self.stack.active();
            let ncols = s.dataframe.columns.len();
            let nrows = s.dataframe.visible_row_count();

            if ncols == 0 || nrows == 0 {
                None
            } else {
                // Detect previously transposed table: first column named "column" and pinned
                let is_transposed =
                    s.dataframe.columns[0].name == "column" && s.dataframe.columns[0].pinned;

                // row_labels  → values in the output "column" column (one per output row)
                // new_col_names → names of the output data columns (one per output col)
                // data_cols_start → first source column index to treat as data
                let (row_labels, new_col_names, data_cols_start): (
                    Vec<String>,
                    Vec<String>,
                    usize,
                ) = if is_transposed {
                    // Inverse transpose:
                    //   new column headers = current "column" column values
                    //   new row labels     = current data column names
                    let row_labels = s.dataframe.columns[1..]
                        .iter()
                        .map(|c| c.name.clone())
                        .collect();
                    let new_col_names = (0..nrows)
                        .map(|r| {
                            let physical = s.dataframe.row_order[r];
                            s.dataframe.get_physical(physical, 0).to_string()
                        })
                        .collect();
                    (row_labels, new_col_names, 1)
                } else {
                    // Normal transpose:
                    //   new column headers = "row_{physical}" for each visible row
                    //   new row labels     = current column names
                    let row_labels = s.dataframe.columns.iter().map(|c| c.name.clone()).collect();
                    let new_col_names = (0..nrows)
                        .map(|r| format!("row_{}", s.dataframe.row_order[r]))
                        .collect();
                    (row_labels, new_col_names, 0)
                };

                let data_ncols = ncols - data_cols_start;

                // row_data[i][r] = value of source column (data_cols_start + i) at display row r
                let row_data: Vec<Vec<String>> = (data_cols_start..ncols)
                    .map(|i| {
                        (0..nrows)
                            .map(|r| {
                                let physical = s.dataframe.row_order[r];
                                s.dataframe.get_physical(physical, i).to_string()
                            })
                            .collect()
                    })
                    .collect();

                // Build Polars DataFrame.
                // On inverse transpose (is_transposed=true): only new_col_names columns, no "column" prefix.
                // On normal transpose: col 0 = "column" with row_labels, col 1+ = new_col_names[j].
                let mut series_vec: Vec<polars::prelude::Column> = Vec::new();

                if !is_transposed {
                    let col_names_series =
                        polars::prelude::Series::new("column".into(), &row_labels);
                    series_vec.push(col_names_series.into());
                }

                for (col_idx, col_name) in new_col_names.iter().enumerate() {
                    let col_vals: Vec<String> = (0..data_ncols)
                        .map(|i| row_data[i][col_idx].clone())
                        .collect();
                    let series = polars::prelude::Series::new(col_name.clone().into(), &col_vals);
                    series_vec.push(series.into());
                }

                let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)
                    .unwrap_or_else(|_| polars::prelude::DataFrame::empty());

                let row_order: Vec<usize> = (0..data_ncols).collect();
                let mut new_columns: Vec<crate::data::column::ColumnMeta> = if is_transposed {
                    new_col_names
                        .iter()
                        .map(|n| crate::data::column::ColumnMeta::new(n.clone()))
                        .collect()
                } else {
                    std::iter::once("column".to_string())
                        .chain(new_col_names.iter().cloned())
                        .map(crate::data::column::ColumnMeta::new)
                        .collect()
                };
                if !is_transposed && !new_columns.is_empty() {
                    new_columns[0].pinned = true;
                }

                let mut df = crate::data::dataframe::DataFrame {
                    df: pdf,
                    columns: new_columns,
                    row_order: row_order.clone().into(),
                    original_order: row_order.into(),
                    selected_rows: std::collections::HashSet::new(),
                    modified: false,
                    aggregates_cache: None,
                };
                df.calc_widths(40, 500);

                let col_count = if is_transposed {
                    new_col_names.len()
                } else {
                    new_col_names.len() + 1
                };
                let status = format!("Transposed: {} rows, {} columns", data_ncols, col_count);
                Some((df, status))
            }
        };

        match result {
            None => {
                self.status_message = "Nothing to transpose".to_string();
            }
            Some((df, status)) => {
                // Replace dataframe in-place (Task 4: no new sheet pushed)
                let s = self.stack.active_mut();
                s.push_undo();
                s.dataframe = df;
                s.sort_col = None;
                s.cursor_col = 0;
                s.top_row = 0;
                s.left_col = 0;
                s.table_state.select(Some(0));
                self.status_message = status;
            }
        }
    }

    fn describe_sheet(&mut self) {
        let s = self.stack.active();
        let ncols = s.dataframe.columns.len();

        // Metric row names
        let metric_names = [
            "type", "count", "nulls", "unique", "min", "max", "mean", "median", "mode", "stdev",
            "range", "q5", "q25", "q50", "q75", "q95",
        ];
        let n_metrics = metric_names.len();

        // For each source column, compute all metrics
        let mut col_values: Vec<Vec<String>> = Vec::with_capacity(ncols);

        let calc_quantile = |sorted: &[f64], q: f64| -> f64 {
            if sorted.is_empty() {
                return 0.0;
            }
            let k = (sorted.len() as f64 - 1.0) * q;
            let f = k.floor() as usize;
            let c = k.ceil() as usize;
            if f == c {
                sorted[f]
            } else {
                sorted[f] * (c as f64 - k) + sorted[c] * (k - f as f64)
            }
        };

        for i in 0..ncols {
            let meta = &s.dataframe.columns[i];

            let mut non_empty: Vec<String> = Vec::new();
            let mut nulls = 0usize;
            let mut unique_set = std::collections::HashSet::new();

            for row in 0..s.dataframe.visible_row_count() {
                let physical = s.dataframe.row_order[row];
                let val = s.dataframe.get_physical(physical, i);
                if val.is_empty() {
                    nulls += 1;
                } else {
                    unique_set.insert(val.clone());
                    non_empty.push(val);
                }
            }

            let is_numeric = matches!(
                meta.col_type,
                crate::types::ColumnType::Integer
                    | crate::types::ColumnType::Float
                    | crate::types::ColumnType::Percentage
                    | crate::types::ColumnType::Currency
            );

            let nums: Vec<f64> = if is_numeric {
                non_empty
                    .iter()
                    .filter_map(|v| v.parse::<f64>().ok())
                    .collect()
            } else {
                Vec::new()
            };

            let p = meta.precision as usize;

            let (
                min_s,
                max_s,
                mean_s,
                median_s,
                mode_s,
                stdev_s,
                range_s,
                q5_s,
                q25_s,
                q50_s,
                q75_s,
                q95_s,
            ) = if !nums.is_empty() {
                let mut sorted = nums.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = sorted.len() as f64;
                let sum: f64 = sorted.iter().sum();
                let mean = sum / n;
                let median = if sorted.len().is_multiple_of(2) {
                    (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
                } else {
                    sorted[sorted.len() / 2]
                };
                let variance = sorted.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
                let stdev = variance.sqrt();
                let range = sorted[sorted.len() - 1] - sorted[0];
                // Mode: most frequent value
                let mode_val = {
                    let mut freq: std::collections::HashMap<String, usize> =
                        std::collections::HashMap::new();
                    for v in &non_empty {
                        *freq.entry(v.clone()).or_insert(0) += 1;
                    }
                    freq.into_iter()
                        .max_by_key(|(_, c)| *c)
                        .map(|(v, _)| v)
                        .unwrap_or_default()
                };
                (
                    format!("{:.*}", p, sorted[0]),
                    format!("{:.*}", p, sorted[sorted.len() - 1]),
                    format!("{:.*}", p, mean),
                    format!("{:.*}", p, median),
                    mode_val,
                    format!("{:.*}", p, stdev),
                    format!("{:.*}", p, range),
                    format!("{:.*}", p, calc_quantile(&sorted, 0.05)),
                    format!("{:.*}", p, calc_quantile(&sorted, 0.25)),
                    format!("{:.*}", p, calc_quantile(&sorted, 0.50)),
                    format!("{:.*}", p, calc_quantile(&sorted, 0.75)),
                    format!("{:.*}", p, calc_quantile(&sorted, 0.95)),
                )
            } else if !non_empty.is_empty() {
                // String: min/max alphabetically, mode = most frequent
                let min_s = non_empty.iter().min().cloned().unwrap_or_default();
                let max_s = non_empty.iter().max().cloned().unwrap_or_default();
                let range_s = format!("{} → {}", min_s, max_s);
                let mode_val = {
                    let mut freq: std::collections::HashMap<String, usize> =
                        std::collections::HashMap::new();
                    for v in &non_empty {
                        *freq.entry(v.clone()).or_insert(0) += 1;
                    }
                    freq.into_iter()
                        .max_by_key(|(_, c)| *c)
                        .map(|(v, _)| v)
                        .unwrap_or_default()
                };
                (
                    min_s,
                    max_s,
                    String::new(),
                    String::new(),
                    mode_val,
                    String::new(),
                    range_s,
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                )
            } else {
                (
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                )
            };

            col_values.push(vec![
                format!("{:?}", meta.col_type), // type
                non_empty.len().to_string(),    // count
                nulls.to_string(),              // nulls
                unique_set.len().to_string(),   // unique
                min_s,
                max_s,
                mean_s,
                median_s,
                mode_s,
                stdev_s,
                range_s,
                q5_s,
                q25_s,
                q50_s,
                q75_s,
                q95_s,
            ]);
        }

        // Build describe DataFrame: col 0 = "metric", col 1..N = source columns
        // First column: metric names (string)
        let metric_col = polars::prelude::Series::new(
            "metric".into(),
            &metric_names
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        );
        let mut series_vec: Vec<polars::prelude::Column> = vec![metric_col.into()];

        for (i, cv) in col_values.iter().enumerate() {
            let col_name = s.dataframe.columns[i].name.clone();
            let series = polars::prelude::Series::new(col_name.into(), cv as &Vec<String>);
            series_vec.push(series.into());
        }

        let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)
            .unwrap_or_else(|_| polars::prelude::DataFrame::empty());

        let row_order: Vec<usize> = (0..n_metrics).collect();
        let mut columns: Vec<crate::data::column::ColumnMeta> =
            std::iter::once("metric".to_string())
                .chain(s.dataframe.columns.iter().map(|c| c.name.clone()))
                .map(crate::data::column::ColumnMeta::new)
                .collect();
        // Pin the "metric" column so it stays visible while scrolling
        columns[0].pinned = true;

        let mut df = crate::data::dataframe::DataFrame {
            df: pdf,
            columns,
            row_order: row_order.clone().into(),
            original_order: row_order.into(),
            selected_rows: std::collections::HashSet::new(),
            modified: false,
            aggregates_cache: None,
        };
        df.calc_widths(40, 500);

        let sheet_title = s.title.clone();
        let sheet = crate::sheet::Sheet::new(format!("Describe: {}", sheet_title), df);
        self.stack.push(sheet);
        self.status_message = format!("Describe: {} columns", ncols);
    }

    fn deduplicate_by_pinned(&mut self) {
        let s = self.stack.active_mut();
        let pinned_cols: Vec<usize> = s
            .dataframe
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.pinned)
            .map(|(i, _)| i)
            .collect();

        if pinned_cols.is_empty() {
            self.mode = AppMode::Normal;
            self.status_message = "No pinned columns to deduplicate by".to_string();
            return;
        }

        s.push_undo();

        let old_count = s.dataframe.visible_row_count();
        let mut seen = std::collections::HashSet::new();
        let mut new_order = Vec::new();

        for &physical_row in s.dataframe.row_order.iter() {
            let key: Vec<String> = pinned_cols
                .iter()
                .map(|&c| s.dataframe.get_physical(physical_row, c).to_string())
                .collect();
            if seen.insert(key) {
                new_order.push(physical_row);
            }
        }

        s.dataframe.row_order = new_order.into();
        s.dataframe.original_order = s.dataframe.row_order.clone();
        s.dataframe.selected_rows.clear();
        s.dataframe.modified = true;
        s.dataframe.aggregates_cache = None;
        s.table_state.select(Some(0));

        let new_count = s.dataframe.visible_row_count();
        self.mode = AppMode::Normal;
        self.status_message = format!("Deduplicated: {} -> {} rows", old_count, new_count);
    }

    fn apply_aggregators(&mut self) {
        let s = self.stack.active_mut();
        let col = s.cursor_col;
        s.dataframe.clear_aggregators(col);

        let mut errs = Vec::new();
        for agg in AggregatorKind::all() {
            if self.agg_selected.contains(agg) {
                if let Err(e) = s.dataframe.add_aggregator(col, *agg) {
                    errs.push(e.to_string());
                }
            }
        }

        s.dataframe.aggregates_cache = None;
        let _ = s.dataframe.compute_aggregates();

        self.mode = AppMode::Normal;
        if errs.is_empty() {
            self.status_message = "Aggregators applied successfully".to_string();
        } else {
            self.status_message = format!("Errors: {}", errs.join(", "));
        }
    }

    // ── Cell editing ───────────────────────────────────────────────────────────

    fn start_edit(&mut self) {
        let s = self.stack.active_mut();
        if let Some(display_row) = s.table_state.selected() {
            if display_row < s.dataframe.visible_row_count() {
                let physical_row = s.dataframe.row_order[display_row];
                let col = s.cursor_col;
                s.edit_input =
                    TextInput::with_value(s.dataframe.get_physical(physical_row, col).to_string());
                s.edit_row = physical_row;
                s.edit_col = col;
                self.mode = AppMode::Editing;
            }
        }
    }

    fn apply_edit(&mut self) {
        let s = self.stack.active_mut();
        s.push_undo();
        let new_value = s.edit_input.as_str().to_string();
        let row = s.edit_row;
        let col = s.edit_col;
        match s.dataframe.set_cell(row, col, new_value.clone()) {
            Ok(_) => {
                self.mode = AppMode::Normal;
                self.status_message = format!("Cell updated: '{}'", new_value);
            }
            Err(e) => {
                self.mode = AppMode::Normal;
                self.status_message = format!("Edit error: '{}'", e);
            }
        }
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
        let current_value = s.dataframe.get_physical(physical_row, col);

        // Write cell value to a temp file
        let mut tmp = tempfile::Builder::new().suffix(".txt").tempfile()?;
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

                if new_value != current_value {
                    let s = self.stack.active_mut();
                    s.push_undo();
                    match s.dataframe.set_cell(physical_row, col, new_value.clone()) {
                        Ok(()) => {
                            self.status_message = "Cell updated from editor".to_string();
                        }
                        Err(e) => {
                            self.status_message = format!("Cell update failed: {}", e);
                        }
                    }
                } else {
                    self.status_message = "No changes".to_string();
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

    // ── Quick aggregate ────────────────────────────────────────────────────────

    fn quick_aggregate(&mut self) {
        let s = self.stack.active();
        let col = s.cursor_col;
        let col_meta = &s.dataframe.columns[col];
        let col_type = col_meta.col_type;
        let values: Vec<String> = (0..s.dataframe.visible_row_count())
            .map(|i| DataFrame::anyvalue_to_string_fmt(&s.dataframe.get_val(i, col)))
            .collect();

        let total = values.len();
        let distinct: std::collections::HashSet<String> = values.iter().cloned().collect();
        let distinct_count = distinct.len();
        let min_val = values.iter().min().unwrap_or(&String::new()).clone();
        let max_val = values.iter().max().unwrap_or(&String::new()).clone();

        let mut msg = format!(
            "{}: count={}  distinct={}  min={}  max={}",
            col_meta.name, total, distinct_count, min_val, max_val
        );

        if matches!(col_type, ColumnType::Integer | ColumnType::Float) {
            let nums: Vec<f64> = values
                .iter()
                .filter_map(|s| s.parse::<f64>().ok())
                .collect();
            if !nums.is_empty() {
                let sum: f64 = nums.iter().sum();
                let avg = sum / nums.len() as f64;
                let _ = write!(msg, "  sum={}  avg={:.2}", sum as i64, avg);
            }
        }

        self.status_message = msg;
    }

    // ── Row selection ──────────────────────────────────────────────────────────

    fn select_row(&mut self, select: bool) {
        let s = self.stack.active_mut();
        if let Some(display_row) = s.table_state.selected() {
            if display_row < s.dataframe.visible_row_count() {
                let physical = s.dataframe.row_order[display_row];
                if select {
                    s.dataframe.selected_rows.insert(physical);
                } else {
                    s.dataframe.selected_rows.remove(&physical);
                }
                let count = s.dataframe.selected_rows.len();
                self.status_message = if select {
                    format!("Row {} selected ({} total)", display_row + 1, count)
                } else {
                    format!("Row {} unselected ({} total)", display_row + 1, count)
                };
            }
        }
        // Advance cursor down after edit
        self.move_cursor_down();
        self.mode = AppMode::Normal;
    }

    // ── Clipboard & delete ─────────────────────────────────────────────────────

    fn copy_selected_rows(&mut self) {
        let s = self.stack.active();
        let df = &s.dataframe;
        if df.selected_rows.is_empty() {
            self.status_message = "No rows selected (use 's' to select)".to_string();
            return;
        }
        let headers: Vec<&str> = df.columns.iter().map(|c| c.name.as_str()).collect();
        let mut rows: Vec<Vec<String>> = df
            .selected_rows
            .iter()
            .map(|&phys| {
                (0..df.col_count())
                    .map(|col| df.get_physical(phys, col))
                    .collect()
            })
            .collect();
        rows.sort(); // stable output order

        let count = rows.len();
        match clipboard::copy_to_clipboard(&headers, &rows) {
            Ok(()) => {
                self.status_message = format!("Copied {} rows to clipboard (TSV)", count);
            }
            Err(e) => {
                self.status_message = format!("Clipboard error: {}", e);
            }
        }
    }

    fn paste_rows(&mut self) {
        match clipboard::paste_from_clipboard() {
            Ok(text) => {
                let s = self.stack.active_mut();
                s.push_undo();
                let df = &mut s.dataframe;
                let col_count = df.col_count();
                if col_count == 0 {
                    return;
                }
                let lines: Vec<&str> = text.lines().collect();
                if lines.is_empty() {
                    self.status_message = "Clipboard is empty".to_string();
                    return;
                }
                // Skip header row if it matches column names
                let start = if lines[0]
                    .split('\t')
                    .zip(df.columns.iter())
                    .all(|(a, b)| a == b.name)
                {
                    1
                } else {
                    0
                };

                let mut series_vec = Vec::new();
                for col in 0..col_count {
                    let mut col_data = Vec::new();
                    for line in &lines[start..] {
                        let fields: Vec<&str> = line.split('\t').collect();
                        let val = fields.get(col).unwrap_or(&"").to_string();
                        col_data.push(val);
                    }
                    let s = polars::prelude::Series::new(
                        df.columns[col].name.clone().into(),
                        &col_data,
                    );
                    series_vec.push(s.into());
                }
                if let Ok(new_df) = polars::prelude::DataFrame::new_infer_height(series_vec) {
                    let original_height = df.df.height();
                    if original_height == 0 {
                        df.df = new_df;
                    } else {
                        let _ = df.df.vstack_mut(&new_df);
                    }
                    let added = lines.len() - start;
                    for i in 0..added {
                        let new_idx = original_height + i;
                        std::sync::Arc::make_mut(&mut df.row_order).push(new_idx);
                        std::sync::Arc::make_mut(&mut df.original_order).push(new_idx);
                    }
                    df.modified = true;
                    df.calc_widths(40, 1000);
                    let vis = df.visible_row_count();
                    s.scroll_state = ScrollbarState::new(vis.saturating_sub(1));
                    self.status_message = format!("Pasted {} rows", added);
                } else {
                    self.status_message = "Failed to create dataframe for paste".to_string();
                }
            }
            Err(e) => {
                self.status_message = format!("Clipboard error: {}", e);
            }
        }
    }

    fn delete_selected_rows(&mut self) {
        let s = self.stack.active_mut();
        let count = s.dataframe.selected_rows.len();
        if count == 0 {
            self.status_message = "No rows selected to delete".to_string();
            return;
        }
        s.push_undo();
        std::sync::Arc::make_mut(&mut s.dataframe.row_order)
            .retain(|idx| !s.dataframe.selected_rows.contains(idx));
        std::sync::Arc::make_mut(&mut s.dataframe.original_order)
            .retain(|idx| !s.dataframe.selected_rows.contains(idx));
        s.dataframe.selected_rows.clear();
        s.dataframe.modified = true;

        let vis = s.dataframe.visible_row_count();
        s.scroll_state = ScrollbarState::new(vis.saturating_sub(1));
        let sel = s
            .table_state
            .selected()
            .unwrap_or(0)
            .min(vis.saturating_sub(1));
        s.table_state.select(Some(sel));
        self.status_message = format!("Deleted {} rows", count);
    }

    // ── Derived sheet ──────────────────────────────────────────────────────────

    fn create_sheet_from_selection(&mut self) {
        let s = self.stack.active();
        let df = &s.dataframe;

        let has_selected_rows = !df.selected_rows.is_empty();
        let selected_col_indices: Vec<usize> = df
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.selected)
            .map(|(i, _)| i)
            .collect();
        let has_selected_cols = !selected_col_indices.is_empty();

        if !has_selected_rows && !has_selected_cols {
            self.status_message =
                "No rows or columns selected (use 's' to select rows, 'zs' for columns)"
                    .to_string();
            return;
        }

        // Determine which physical rows to include
        let selected_physical: Vec<usize> = if has_selected_rows {
            let sel = &df.selected_rows;
            df.row_order
                .iter()
                .filter(|&&i| sel.contains(&i))
                .copied()
                .collect()
        } else {
            // All rows in display order
            df.row_order.iter().copied().collect()
        };

        // Determine which columns to include
        let col_indices: Vec<usize> = if has_selected_cols {
            selected_col_indices
        } else {
            (0..df.col_count()).collect()
        };

        let mut series_vec = Vec::new();
        let mut new_columns = Vec::new();
        for &col in &col_indices {
            let col_meta = df.columns[col].clone();
            let mut col_data = Vec::with_capacity(selected_physical.len());
            for &phys_idx in &selected_physical {
                col_data.push(df.get_physical(phys_idx, col));
            }
            let series = polars::prelude::Series::new(col_meta.name.clone().into(), &col_data);
            series_vec.push(series.into());
            new_columns.push(col_meta);
        }

        let pdf = polars::prelude::DataFrame::new_infer_height(series_vec)
            .unwrap_or_else(|_| polars::prelude::DataFrame::empty());

        let row_count = selected_physical.len();
        let row_order: Vec<usize> = (0..row_count).collect();

        let title = match (has_selected_rows, has_selected_cols) {
            (true, true) => format!(
                "{} [{}rows, {}cols]",
                s.title,
                selected_physical.len(),
                col_indices.len()
            ),
            (true, false) => format!("{} [{}sel]", s.title, selected_physical.len()),
            (false, true) => format!("{} [{}cols]", s.title, col_indices.len()),
            (false, false) => unreachable!(),
        };

        let mut new_df = DataFrame {
            df: pdf,
            columns: new_columns,
            row_order: row_order.clone().into(),
            original_order: row_order.into(),
            selected_rows: HashSet::new(),
            modified: false,
            aggregates_cache: None,
        };
        new_df.calc_widths(40, 1000);

        let status = match (has_selected_rows, has_selected_cols) {
            (true, true) => format!(
                "Created sheet from {} rows × {} columns",
                selected_physical.len(),
                col_indices.len()
            ),
            (true, false) => format!(
                "Created sheet from {} selected rows",
                selected_physical.len()
            ),
            (false, true) => format!("Created sheet from {} selected columns", col_indices.len()),
            (false, false) => unreachable!(),
        };

        let derived = Sheet::new(title, new_df);
        self.stack.push(derived);
        self.status_message = status;
    }

    // ── Z Prefix (Column Operations) ──────────────────────────────────────────

    fn apply_rename_column(&mut self) {
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
        let s = self.stack.active_mut();
        let col = s.cursor_col;
        if col > 0 {
            s.push_undo();
            if let Err(e) = s.dataframe.swap_columns(col, col - 1) {
                self.status_message = format!("Move error: {}", e);
            } else {
                s.cursor_col -= 1;
                s.table_state.select_column(Some(s.cursor_col));
                self.status_message = "Moved column left".to_string();
            }
        }
        self.mode = AppMode::ZPrefix; // Stay in ZPrefix mode
    }

    fn move_col_right(&mut self) {
        let s = self.stack.active_mut();
        let col = s.cursor_col;
        if col + 1 < s.dataframe.col_count() {
            s.push_undo();
            if let Err(e) = s.dataframe.swap_columns(col, col + 1) {
                self.status_message = format!("Move error: {}", e);
            } else {
                s.cursor_col += 1;
                s.table_state.select_column(Some(s.cursor_col));
                self.status_message = "Moved column right".to_string();
            }
        }
        self.mode = AppMode::ZPrefix; // Stay in ZPrefix mode
    }

    fn saving_autocomplete(&mut self) {
        let input = self.saving_input.as_str().to_owned();

        // Split into dir part and filename prefix
        let path = std::path::Path::new(&input);
        let (dir, prefix) = if input.ends_with('/') {
            (path, "")
        } else {
            let dir = path.parent().unwrap_or(std::path::Path::new("."));
            let prefix = path
                .file_name()
                .map(|f| f.to_str().unwrap_or(""))
                .unwrap_or("");
            (dir, prefix)
        };

        let dir_str = if dir == std::path::Path::new("") {
            std::path::Path::new(".")
        } else {
            dir
        };
        let expanded_dir = expand_tilde(dir_str.to_str().unwrap_or("."));

        // If prefix changed, rebuild candidate list
        let full_prefix = input.trim_end_matches(prefix).to_string();
        if self.autocomplete_prefix != full_prefix || self.autocomplete_candidates.is_empty() {
            self.autocomplete_prefix = full_prefix.clone();
            self.autocomplete_idx = 0;

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
            self.autocomplete_candidates = candidates;
        }

        if self.autocomplete_candidates.is_empty() {
            return;
        }

        // Find longest common prefix on first tab; cycle on subsequent tabs
        let common = longest_common_prefix(&self.autocomplete_candidates);
        let current_suffix = self
            .saving_input
            .as_str()
            .strip_prefix(&self.autocomplete_prefix)
            .unwrap_or("");

        if common.len() > current_suffix.len() {
            // Complete to common prefix
            let new_value = format!("{}{}", self.autocomplete_prefix, common);
            self.saving_input = crate::ui::text_input::TextInput::with_value(new_value);
        } else {
            // Cycle to next candidate
            self.autocomplete_idx =
                (self.autocomplete_idx + 1) % self.autocomplete_candidates.len();
            let completion = &self.autocomplete_candidates[self.autocomplete_idx];
            let new_value = format!("{}{}", self.autocomplete_prefix, completion);
            self.saving_input = crate::ui::text_input::TextInput::with_value(new_value);
        }
    }

    fn join_path_autocomplete(&mut self) {
        let input = self.join_path_input.as_str().to_owned();
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
        if self.autocomplete_prefix != full_prefix || self.autocomplete_candidates.is_empty() {
            self.autocomplete_prefix = full_prefix.clone();
            self.autocomplete_idx = 0;
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
            self.autocomplete_candidates = candidates;
        }
        if self.autocomplete_candidates.is_empty() {
            return;
        }
        let common = longest_common_prefix(&self.autocomplete_candidates);
        let current_suffix = self
            .join_path_input
            .as_str()
            .strip_prefix(&self.autocomplete_prefix)
            .unwrap_or("");
        if common.len() > current_suffix.len() {
            let new_value = format!("{}{}", self.autocomplete_prefix, common);
            self.join_path_input = crate::ui::text_input::TextInput::with_value(new_value);
        } else {
            self.autocomplete_idx =
                (self.autocomplete_idx + 1) % self.autocomplete_candidates.len();
            let completion = &self.autocomplete_candidates[self.autocomplete_idx];
            let new_value = format!("{}{}", self.autocomplete_prefix, completion);
            self.join_path_input = crate::ui::text_input::TextInput::with_value(new_value);
        }
    }
}

// Needed for quick_aggregate string formatting
use std::fmt::Write;

/// Expand a leading `~` to the user's home directory.
fn expand_tilde(input: &str) -> std::path::PathBuf {
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

/// Build an ASCII block-character histogram bar for a frequency table cell.
/// Uses Unicode block elements (▏▎▍▌▋▊▉█) for sub-character precision.
pub(crate) fn build_bar(count: usize, max_count: usize, bar_width: usize) -> String {
    if max_count == 0 {
        return String::new();
    }
    const BLOCKS: [char; 9] = [' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉', '█'];
    let ratio = count as f64 / max_count as f64;
    let total_eighths = (ratio * bar_width as f64 * 8.0).round() as usize;
    let full_blocks = total_eighths / 8;
    let remainder = total_eighths % 8;

    let mut bar = String::with_capacity(bar_width + 1);
    for _ in 0..full_blocks {
        bar.push('▉'); // Using 7/8 block instead of full block creates a 1/8 gap to prevent visual merging
    }
    if full_blocks < bar_width && remainder > 0 {
        bar.push(BLOCKS[remainder]);
    }
    bar
}
