//! Sheet stack — the navigation model for tuitab.
//!
//! A [`Sheet`] holds one data view: a [`crate::data::dataframe::DataFrame`] plus all UI
//! state that belongs to it (cursor position, active sort, search pattern, undo history,
//! and per-mode input widgets).
//!
//! A [`SheetStack`] owns a stack of sheets.  Opening a derived view (frequency table,
//! pivot table, filtered selection) pushes a new sheet; pressing `Esc`/`q` pops it and
//! restores the previous view.  To keep memory usage bounded, any sheet that is not on
//! top of the stack is transparently serialised to a temporary directory via
//! [`crate::data::swap`] and swapped back in when it becomes active again.

use crate::data::dataframe::DataFrame;
use crate::data::doc::Node;
use crate::data::io::doc_io::DocState;
use crate::data::swap;
use crate::types::SheetType;
use crate::ui::text_input::TextInput;
use ratatui::widgets::{ScrollbarState, TableState};
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;

/// A document search result sheet: the tree it came from, and the node each row points
/// at.  This is deliberately not a [`crate::data::view::View`] — the rows are hits
/// scattered across the document, not a projection of one subtree.
pub struct DocHits {
    pub doc: std::sync::Arc<std::sync::RwLock<crate::data::doc::Doc>>,
    pub paths: Vec<crate::data::doc::NodePath>,
    /// Document revision when the search ran.  Paths are absolute, so any later change
    /// can renumber them — a stale list must refuse rather than open the wrong node.
    pub revision: u64,
}

/// One undo entry: the table, plus the document root when the sheet is doc-backed.
pub struct UndoState {
    pub dataframe: DataFrame,
    pub root: Option<Node>,
}

/// A single data sheet in the stack — owns its DataFrame and all view state.
pub struct Sheet {
    /// Human-readable title shown in the table border
    pub title: String,
    /// The actual data
    pub dataframe: DataFrame,
    /// Document tree behind this sheet, for JSON/JSONL/YAML/TOML sources.  Sheets in a
    /// dive chain share one tree, so an edit made deep down is visible when popping back.
    pub doc: Option<DocState>,
    /// Set on a document-search result sheet; `Enter` uses it to jump to the hit.
    pub doc_hits: Option<DocHits>,
    /// Stack of previous states for Undo.  Doc-backed sheets snapshot the tree alongside
    /// the table — undoing only the table would leave the two disagreeing.
    pub undo_stack: Vec<UndoState>,
    /// Stack of states for Redo (populated by pop_undo, cleared by push_undo)
    pub redo_stack: Vec<UndoState>,
    /// ratatui row selection state
    pub table_state: TableState,
    /// Currently highlighted column
    pub cursor_col: usize,
    /// Vertical scrollbar state
    pub scroll_state: ScrollbarState,
    /// The physical row index of the top-most visible row (for virtualized rendering).
    pub top_row: usize,
    /// The index of the left-most visible column (for horizontal scrolling).
    pub left_col: usize,

    // ── Sort state ────────────────────────────────────────────────────────────
    /// Active sort keys, most significant first: `(column name, descending)`.
    ///
    /// A list rather than a single column because `[`/`]` replace the sort while
    /// `z[`/`z]` extend it — and two single-key sorts are not a compound one
    /// (see [`crate::data::dataframe::DataFrame::sort_by_keys`]).
    ///
    /// **Names, not indices.** Deleting a column, moving one with `z←`, or
    /// pinning one all renumber `columns`, and a stored index would then name a
    /// different column than the one the user sorted — or none at all, which
    /// panicked. A name either still resolves or it does not, and
    /// [`Self::resolved_sort_keys`] drops the ones that do not.
    pub sort_keys: Vec<(String, bool)>,

    // ── Search state (/) ──────────────────────────────────────────────────────
    pub search_input: TextInput,
    pub search_pattern: Option<String>,
    /// The column the running search looks in, held by name.
    ///
    /// An index would go stale the moment a save drops a column to its left, and every
    /// reader of it indexes straight into `columns`.  Same reasoning as `sort_keys`.
    pub search_col_name: Option<String>,

    // ── Select by regex state (|) ─────────────────────────────────────────────
    pub select_regex_input: TextInput,

    /// Path typed for `gp` (go to a node by its document path)
    pub path_input: TextInput,
    /// jq program typed for `gq`
    pub query_input: TextInput,
    /// File name to prefill when saving, for sheets whose title is not a path — a query
    /// result is titled with its program, which is no use as a destination.
    pub save_name_hint: Option<String>,

    // ── Expression state (=) ──────────────────────────────────────────────────
    pub expr_input: TextInput,

    // ── Cell edit state ───────────────────────────────────────────────────────
    pub edit_input: TextInput,
    pub edit_row: usize,
    pub edit_col: usize,

    // ── Z Prefix state ────────────────────────────────────────────────────────
    pub rename_column_input: TextInput,
    pub insert_column_input: TextInput,
    pub col_find_input: TextInput,
    pub col_replace_input: TextInput,
    pub col_split_input: TextInput,
    /// True if this sheet represents a directory listing
    pub is_dir_sheet: bool,
    /// For SQLite browser sheets: path to the .db file so we can open individual tables.
    pub sqlite_db_path: Option<std::path::PathBuf>,
    /// For DuckDB browser sheets: path to the .duckdb/.ddb file.
    pub duckdb_db_path: Option<std::path::PathBuf>,
    /// Full path of the file or directory that was loaded to produce this sheet.
    pub source_path: Option<std::path::PathBuf>,
    /// Per-row absolute paths for synthetic file-list sheets (multi-file CLI arg).
    pub explicit_row_paths: Option<Vec<std::path::PathBuf>>,
    /// SQLite DB path this table was drilled-into from (set on table sheets, not overview).
    pub sqlite_source_path: Option<std::path::PathBuf>,
    /// DuckDB DB path this table was drilled-into from.
    pub duckdb_source_path: Option<std::path::PathBuf>,
    /// Directory path this file was opened from.
    pub dir_source_path: Option<std::path::PathBuf>,
    /// For xlsx overview sheets: path to the xlsx file (mirrors sqlite_db_path).
    pub xlsx_db_path: Option<std::path::PathBuf>,
    /// For xlsx data sheets: path to the xlsx file they came from.
    pub xlsx_source_path: Option<std::path::PathBuf>,
    /// CSV/TSV delimiter byte used when loading this sheet.
    pub source_delimiter: Option<u8>,
    /// The `--type` (or the browser's "open as") this sheet was opened with as a file, so
    /// `R` re-reads it the same way.  A table or sheet opened from a listing has none: it
    /// came from the file's own format.
    pub forced_type: Option<String>,
    /// A view built out of another sheet — a drill-down, a node reached by path — rather
    /// than what its file holds.  `R` refuses it: re-reading `source_path` would put the
    /// whole file under a title that says otherwise.
    pub derived_view: bool,
    /// The table, view or sheet inside its file this sheet was opened as.  Kept by name
    /// rather than read back out of the title: a SQLite or DuckDB name may hold ` :: `.
    pub container: Option<String>,
    /// The database table behind this sheet, when it was drilled into from one and can
    /// still be written back to.  Carries the table name, the declared column types and
    /// the values as loaded; see [`crate::data::io::db_write`].
    ///
    /// Needs no serde: `SheetStack::swap_out` replaces only `dataframe`.
    pub table_source: Option<crate::data::io::db_write::TableSource>,

    // ── Pivot Table ───────────────────────────────────────────────────────────
    pub pivot_input: TextInput,
    pub sheet_type: SheetType,

    // ── Special select (Shift+S → r) ──────────────────────────────────────────
    /// Numeric input field for random row selection
    pub select_count_input: TextInput,
}

impl Sheet {
    /// Create a new Sheet with given title and data.
    pub fn new(title: String, dataframe: DataFrame) -> Self {
        let row_count = dataframe.visible_row_count();
        Self {
            title,
            dataframe,
            doc: None,
            doc_hits: None,
            undo_stack: Vec::new(),
            redo_stack: Vec::new(),
            table_state: TableState::default()
                .with_selected(0)
                .with_selected_column(0),
            cursor_col: 0,
            scroll_state: ScrollbarState::new(row_count.saturating_sub(1)),
            top_row: 0,
            left_col: 0,
            sort_keys: Vec::new(),
            search_input: TextInput::new(),
            search_pattern: None,
            search_col_name: None,
            select_regex_input: TextInput::new(),
            path_input: TextInput::new(),
            query_input: TextInput::new(),
            save_name_hint: None,
            expr_input: TextInput::new(),
            edit_input: TextInput::new(),
            edit_row: 0,
            edit_col: 0,
            rename_column_input: TextInput::new(),
            insert_column_input: TextInput::new(),
            col_find_input: TextInput::new(),
            col_replace_input: TextInput::new(),
            col_split_input: TextInput::new(),
            is_dir_sheet: false,
            sqlite_db_path: None,
            duckdb_db_path: None,
            source_path: None,
            source_delimiter: None,
            forced_type: None,
            derived_view: false,
            container: None,
            explicit_row_paths: None,
            sqlite_source_path: None,
            duckdb_source_path: None,
            dir_source_path: None,
            xlsx_db_path: None,
            xlsx_source_path: None,
            table_source: None,
            pivot_input: TextInput::new(),
            sheet_type: SheetType::Normal,
            select_count_input: TextInput::new(),
        }
    }

    /// Snapshot the current state for undo (max 50). Clears redo stack.
    pub fn push_undo(&mut self) {
        if self.undo_stack.len() >= 50 {
            self.undo_stack.remove(0);
        }
        let snapshot = self.snapshot();
        self.undo_stack.push(snapshot);
        self.redo_stack.clear();
    }

    fn snapshot(&self) -> UndoState {
        UndoState {
            dataframe: self.dataframe.clone(),
            root: self
                .doc
                .as_ref()
                .and_then(|d| d.doc.read().ok().map(|g| g.root.clone())),
        }
    }

    fn restore(&mut self, state: UndoState) {
        // A doc-backed sheet rebuilds its table from the restored tree instead of taking
        // the snapshotted one.  The snapshot was taken before whatever view operation
        // happened since (expand, mode switch, a node edit that changed the row count),
        // so reusing it would leave `col_roles`/`row_paths` describing a different shape
        // than the table on screen — and the next cell edit would write to the wrong
        // node.  Rebuilding makes the two agree by construction, and gives `U` the
        // semantics you want anyway: it undoes the document edit, not the view.
        if let (Some(doc), Some(root)) = (self.doc.as_mut(), state.root) {
            if let Ok(mut guard) = doc.doc.write() {
                guard.root = root;
                guard.bump();
            }
            if let Ok(df) = doc.reproject() {
                self.dataframe = df;
                self.clamp_cursor();
                return;
            }
        }
        self.dataframe = state.dataframe;
        self.clamp_cursor();
    }

    /// Restore previous state from undo stack. Saves current state to redo.
    pub fn pop_undo(&mut self) -> bool {
        if let Some(state) = self.undo_stack.pop() {
            if self.redo_stack.len() >= 50 {
                self.redo_stack.remove(0);
            }
            let current = self.snapshot();
            self.redo_stack.push(current);
            self.restore(state);
            true
        } else {
            false
        }
    }

    /// Restore next state from redo stack. Saves current state back to undo.
    pub fn pop_redo(&mut self) -> bool {
        if let Some(state) = self.redo_stack.pop() {
            if self.undo_stack.len() >= 50 {
                self.undo_stack.remove(0);
            }
            let current = self.snapshot();
            self.undo_stack.push(current);
            self.restore(state);
            true
        } else {
            false
        }
    }

    /// True when the document's cell→node mapping still describes the table on screen.
    ///
    /// The mapping is by index, so anything that adds, removes or reorders columns
    /// behind the projection's back invalidates it.  Edits check this first: writing to
    /// a stale mapping would silently modify the wrong node.
    pub fn doc_mapping_ok(&self) -> bool {
        match self.doc.as_ref() {
            None => true,
            Some(d) => {
                d.col_roles.len() == self.dataframe.columns.len()
                    && d.row_paths.len() == self.dataframe.df.height()
            }
        }
    }

    /// Sort keys as column indices, skipping any whose column is gone.
    ///
    /// Resolved at the moment of use rather than kept in step by every
    /// operation that reorders columns — there are three of those, and each one
    /// forgetting is a bug that only shows up later.
    pub fn resolved_sort_keys(&self) -> Vec<(usize, bool)> {
        self.sort_keys
            .iter()
            .filter_map(|(name, desc)| {
                self.dataframe
                    .column_index(name)
                    .ok()
                    .map(|idx| (idx, *desc))
            })
            .collect()
    }

    /// Carry the database a sheet came from onto one built out of its rows.
    ///
    /// A drill-down clones the frame and narrows `row_order`; the row identities inside
    /// are untouched, so the new sheet *can* be written back — it just has to know which
    /// table it belongs to.  Without this it looks like a sheet with no table behind it,
    /// and saving it offers to create one out of the filtered rows, over the table it
    /// was filtered from.
    pub fn inherit_db_origin(&mut self, parent: &Sheet) {
        self.table_source = parent.table_source.clone();
        self.sqlite_source_path = parent.sqlite_source_path.clone();
        self.duckdb_source_path = parent.duckdb_source_path.clone();
        self.source_path = parent.source_path.clone();
        self.derived_view = true;
    }

    /// Put the rows back in the sheet's sort order after its data was replaced.
    ///
    /// Keeping `sort_keys` across a reload without this would leave the headers claiming
    /// an order the rows are not in.  Keys naming a column that is gone are dropped by
    /// [`Self::resolved_sort_keys`].
    pub fn reapply_sort(&mut self) {
        let keys = self.resolved_sort_keys();
        if !keys.is_empty() {
            let _ = self.dataframe.sort_by_keys(&keys);
        }
    }

    /// Which column the running search looks in, resolved now rather than remembered.
    ///
    /// Falls back to the cursor for a name that no longer resolves — a search still has
    /// to search somewhere, and the cursor is where the user is looking.
    pub fn search_col(&self) -> usize {
        self.search_col_name
            .as_deref()
            .and_then(|n| self.dataframe.columns.iter().position(|c| c.name == n))
            .unwrap_or(self.cursor_col)
    }

    /// Reset the view after the table was rebuilt from scratch (a reprojection).  Sort
    /// and search state refer to columns that may no longer exist, so they go too.
    pub fn reset_view_state(&mut self) {
        self.sort_keys.clear();
        self.search_pattern = None;
        self.search_col_name = None;
        self.top_row = 0;
        self.left_col = 0;
        self.cursor_col = 0;
        self.table_state.select(Some(0));
        self.table_state.select_column(Some(0));
        self.clamp_cursor();
    }

    /// Pull the cursor back inside the table after something shrank it.
    pub fn clamp_cursor(&mut self) {
        let cols = self.dataframe.columns.len();
        let rows = self.dataframe.visible_row_count();
        if self.cursor_col >= cols && cols > 0 {
            self.cursor_col = cols.saturating_sub(1);
        }
        if let Some(s) = self.table_state.selected() {
            if s >= rows && rows > 0 {
                self.table_state.select(Some(rows.saturating_sub(1)));
            }
        }
    }
}

/// The topmost sheet is always the active one.
/// Sheets that are not the top are offloaded to disk to save memory.
pub struct SheetStack {
    /// All sheets. The last element is the active (top) sheet.
    sheets: Vec<Sheet>,
    /// Temporary directory owning all swap files — auto-deleted on drop.
    _swap_dir: TempDir,
    swap_root: PathBuf,
    /// Maps sheet stack index → path of its serialized DataFrame swap file.
    swapped: HashMap<usize, PathBuf>,
}

impl SheetStack {
    /// Create a new stack with a single root sheet.
    pub fn new(root_sheet: Sheet) -> Self {
        let swap_dir = TempDir::new().expect("Failed to create temp dir for sheet swap");
        let swap_root = swap_dir.path().to_path_buf();
        Self {
            sheets: vec![root_sheet],
            _swap_dir: swap_dir,
            swap_root,
            swapped: HashMap::new(),
        }
    }

    /// Reference to the active (topmost) sheet.
    pub fn active(&self) -> &Sheet {
        self.sheets.last().expect("Sheet stack must never be empty")
    }

    /// Mutable reference to the active (topmost) sheet.
    pub fn active_mut(&mut self) -> &mut Sheet {
        self.sheets
            .last_mut()
            .expect("Sheet stack must never be empty")
    }

    /// Depth of the stack (1 = only root sheet).
    pub fn depth(&self) -> usize {
        self.sheets.len()
    }

    /// True if there is more than one sheet and we can pop.
    pub fn can_pop(&self) -> bool {
        self.sheets.len() > 1
    }

    /// Push a new sheet on top.
    /// The previous top sheet's DataFrame is offloaded to disk to free memory.
    pub fn push(&mut self, sheet: Sheet) {
        let prev_idx = self.sheets.len() - 1;
        self.swap_out(prev_idx);
        self.sheets.push(sheet);
    }

    /// Pop and return the top sheet.
    /// The new top sheet's DataFrame is restored from disk if it was swapped.
    /// Panics if only the root sheet remains.
    pub fn pop(&mut self) -> Sheet {
        assert!(self.sheets.len() > 1, "Cannot pop the root sheet");
        let popped = self.sheets.pop().unwrap();
        let new_top = self.sheets.len() - 1;
        self.swap_in(new_top);
        popped
    }

    /// Titles of all sheets except the active (topmost) one.
    pub fn sheet_titles_except_active(&self) -> Vec<String> {
        let len = self.sheets.len();
        if len <= 1 {
            return Vec::new();
        }
        self.sheets[..len - 1]
            .iter()
            .map(|s| s.title.clone())
            .collect()
    }

    /// Clone the DataFrame at the given stack index (briefly swapping it in if needed).
    pub fn clone_sheet_dataframe(&mut self, idx: usize) -> Option<DataFrame> {
        if idx >= self.sheets.len() {
            return None;
        }
        let was_swapped = self.swapped.contains_key(&idx);
        if was_swapped {
            self.swap_in(idx);
        }
        let df = self.sheets[idx].dataframe.clone();
        if was_swapped {
            self.swap_out(idx);
        }
        Some(df)
    }

    /// Read a clone of the DataFrame one level below the active sheet (parent).
    /// Briefly swaps it in if it was on disk.
    /// The sheet one below the active one, which is where a drill-down's rows came from.
    pub fn parent(&self) -> Option<&Sheet> {
        self.sheets.len().checked_sub(2).map(|i| &self.sheets[i])
    }

    pub fn clone_parent_dataframe(&mut self) -> Option<DataFrame> {
        let depth = self.sheets.len();
        if depth < 2 {
            return None;
        }
        let parent_idx = depth - 2;
        let was_swapped = self.swapped.contains_key(&parent_idx);
        if was_swapped {
            self.swap_in(parent_idx);
        }

        let df = self.sheets[parent_idx].dataframe.clone();

        if was_swapped {
            self.swap_out(parent_idx);
        }
        Some(df)
    }

    // ── Disk swap internals ───────────────────────────────────────────────────

    fn swap_out(&mut self, idx: usize) {
        if self.swapped.contains_key(&idx) {
            return; // already on disk
        }
        let path = self.swap_root.join(format!("sheet_{}.bin", idx));
        swap::swap_out(&self.sheets[idx].dataframe, &path)
            .expect("Failed to write sheet data to disk");
        // Replace with an empty placeholder to free heap memory
        self.sheets[idx].dataframe = DataFrame::empty();
        self.swapped.insert(idx, path);
    }

    fn swap_in(&mut self, idx: usize) {
        if let Some(path) = self.swapped.remove(&idx) {
            let df = swap::swap_in(&path).expect("Failed to read sheet data from disk");
            self.sheets[idx].dataframe = df;
            let _ = std::fs::remove_file(&path);
        }
    }
}
