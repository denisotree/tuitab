use crate::app::App;
use crate::data::dataframe::DataFrame;
use crate::sheet::Sheet;
use crate::types::{Action, AppMode};
use polars::prelude::*;
use ratatui::widgets::ScrollbarState;
use std::collections::HashSet;

impl App {
    pub(crate) fn handle_selection_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::SelectRow => {
                self.select_row(true);
                None
            }
            Action::UnselectRow => {
                self.select_row(false);
                None
            }
            Action::EnterGPrefix => {
                self.mode = AppMode::GPrefix;
                self.status_message = "g: (g)o top  (s)elect all  (u)nselect all".to_string();
                None
            }
            Action::CancelGPrefix => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::SelectAllRows => {
                let s = self.stack.active_mut();
                for &idx in s.dataframe.row_order.iter() {
                    s.dataframe.selected_rows.insert(idx);
                }
                let count = s.dataframe.selected_rows.len();
                self.mode = AppMode::Normal;
                self.status_message = format!("Selected all {} rows", count);
                None
            }
            Action::UnselectAllRows => {
                self.stack.active_mut().dataframe.selected_rows.clear();
                self.mode = AppMode::Normal;
                self.status_message = "All rows unselected".to_string();
                None
            }
            Action::DeleteSelectedRows => {
                self.delete_selected_rows();
                None
            }
            Action::CreateSheetFromSelection => {
                self.create_sheet_from_selection();
                None
            }
            Action::DeduplicateByPinned => {
                if self.mode == AppMode::Calculating {
                    self.deduplicate_by_pinned();
                } else {
                    self.mode = AppMode::Calculating;
                    self.pending_action = Some(Action::DeduplicateByPinned);
                }
                None
            }
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
                None
            }
            Action::ShowHelp => {
                self.mode = AppMode::Help;
                self.status_message = "Press Esc or ? to close help".to_string();
                None
            }
            Action::CloseHelp => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            other => Some(other),
        }
    }

    pub(super) fn select_row(&mut self, select: bool) {
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
        self.move_cursor_down();
        self.mode = AppMode::Normal;
    }

    pub(super) fn delete_selected_rows(&mut self) {
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

    pub(super) fn create_sheet_from_selection(&mut self) {
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

        let selected_physical: Vec<usize> = if has_selected_rows {
            let sel = &df.selected_rows;
            df.row_order
                .iter()
                .filter(|&&i| sel.contains(&i))
                .copied()
                .collect()
        } else {
            df.row_order.iter().copied().collect()
        };

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
            let series = Series::new(col_meta.name.clone().into(), &col_data);
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
            (false, true) => {
                format!("Created sheet from {} selected columns", col_indices.len())
            }
            (false, false) => unreachable!(),
        };

        let derived = Sheet::new(title, new_df);
        self.stack.push(derived);
        self.status_message = status;
    }

    pub(super) fn deduplicate_by_pinned(&mut self) {
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
}
