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
use crate::data::swap;
use crate::types::SheetType;
use crate::ui::text_input::TextInput;
use ratatui::widgets::{ScrollbarState, TableState};
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;

/// A single data sheet in the stack — owns its DataFrame and all view state.
pub struct Sheet {
    /// Human-readable title shown in the table border
    pub title: String,
    /// The actual data
    pub dataframe: DataFrame,
    /// Stack of previous DataFrame states for Undo functionality
    pub undo_stack: Vec<DataFrame>,
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
    pub sort_col: Option<usize>,
    pub sort_desc: bool,

    // ── Search state (/) ──────────────────────────────────────────────────────
    pub search_input: TextInput,
    pub search_pattern: Option<String>,
    pub search_col: Option<usize>,

    // ── Select by regex state (|) ─────────────────────────────────────────────
    pub select_regex_input: TextInput,

    // ── Expression state (=) ──────────────────────────────────────────────────
    pub expr_input: TextInput,

    // ── Cell edit state ───────────────────────────────────────────────────────
    pub edit_input: TextInput,
    pub edit_row: usize,
    pub edit_col: usize,

    // ── Z Prefix state ────────────────────────────────────────────────────────
    pub rename_column_input: TextInput,
    pub insert_column_input: TextInput,
    /// True if this sheet represents a directory listing
    pub is_dir_sheet: bool,
    /// For SQLite browser sheets: path to the .db file so we can open individual tables.
    pub sqlite_db_path: Option<std::path::PathBuf>,

    // ── Pivot Table ───────────────────────────────────────────────────────────
    pub pivot_input: TextInput,
    pub sheet_type: SheetType,
}

impl Sheet {
    /// Create a new Sheet with given title and data.
    pub fn new(title: String, dataframe: DataFrame) -> Self {
        let row_count = dataframe.visible_row_count();
        Self {
            title,
            dataframe,
            undo_stack: Vec::new(),
            table_state: TableState::default()
                .with_selected(0)
                .with_selected_column(0),
            cursor_col: 0,
            scroll_state: ScrollbarState::new(row_count.saturating_sub(1)),
            top_row: 0,
            left_col: 0,
            sort_col: None,
            sort_desc: false,
            search_input: TextInput::new(),
            search_pattern: None,
            search_col: None,
            select_regex_input: TextInput::new(),
            expr_input: TextInput::new(),
            edit_input: TextInput::new(),
            edit_row: 0,
            edit_col: 0,
            rename_column_input: TextInput::new(),
            insert_column_input: TextInput::new(),
            is_dir_sheet: false,
            sqlite_db_path: None,
            pivot_input: TextInput::new(),
            sheet_type: SheetType::Normal,
        }
    }

    /// Push current DataFrame state to undo stack (max 50)
    pub fn push_undo(&mut self) {
        if self.undo_stack.len() >= 50 {
            self.undo_stack.remove(0);
        }
        self.undo_stack.push(self.dataframe.clone());
    }

    /// Pop previous DataFrame state from undo stack
    pub fn pop_undo(&mut self) -> bool {
        if let Some(df) = self.undo_stack.pop() {
            self.dataframe = df;
            // Ensure cursor/selection bounds are valid for restored dataframe
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
            true
        } else {
            false
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

    /// Read a clone of the DataFrame one level below the active sheet (parent).
    /// Briefly swaps it in if it was on disk.
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
