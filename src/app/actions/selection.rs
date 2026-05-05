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
                self.status_message =
                    "g: (g)o top  (s)elect all  (u)nselect all  (t)oggle all  (e)dit selected"
                        .to_string();
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
            Action::ToggleAllSelection => {
                let s = self.stack.active_mut();
                let visible: Vec<usize> = s.dataframe.row_order.iter().copied().collect();
                let mut inverted = std::collections::HashSet::new();
                for idx in &visible {
                    if !s.dataframe.selected_rows.contains(idx) {
                        inverted.insert(*idx);
                    }
                }
                s.dataframe.selected_rows = inverted;
                let count = s.dataframe.selected_rows.len();
                self.mode = AppMode::Normal;
                self.status_message = format!("Toggled selection ({} selected)", count);
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

            // ── Special select (Shift+S) ────────────────────────────────────
            Action::EnterSPrefix => {
                self.mode = AppMode::SPrefix;
                self.status_message =
                    "S: (r)andom N rows  (d)uplicates  (D) smart dedup".to_string();
                None
            }
            Action::CancelSPrefix => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::StartSelectRandom => {
                self.stack.active_mut().select_count_input.clear();
                self.mode = AppMode::SelectRandomInput;
                self.status_message =
                    "Random select: enter N rows (Enter to apply, Esc to cancel)".to_string();
                None
            }
            Action::SelectRandomInputChar(c) => {
                self.stack.active_mut().select_count_input.insert_char(c);
                None
            }
            Action::SelectRandomBackspace => {
                self.stack.active_mut().select_count_input.delete_backward();
                None
            }
            Action::SelectRandomForwardDelete => {
                self.stack.active_mut().select_count_input.delete_forward();
                None
            }
            Action::SelectRandomCursorLeft => {
                self.stack
                    .active_mut()
                    .select_count_input
                    .move_cursor_left();
                None
            }
            Action::SelectRandomCursorRight => {
                self.stack
                    .active_mut()
                    .select_count_input
                    .move_cursor_right();
                None
            }
            Action::SelectRandomCursorStart => {
                self.stack
                    .active_mut()
                    .select_count_input
                    .move_cursor_start();
                None
            }
            Action::SelectRandomCursorEnd => {
                self.stack.active_mut().select_count_input.move_cursor_end();
                None
            }
            Action::ApplySelectRandom => {
                self.apply_select_random();
                None
            }
            Action::CancelSelectRandom => {
                self.stack.active_mut().select_count_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::SelectDuplicates => {
                self.select_duplicates();
                None
            }
            Action::StartSmartDedup => {
                self.start_smart_dedup();
                None
            }
            Action::DedupTiebreakerUp => {
                if self.dedup_tiebreaker.select_index > 0 {
                    self.dedup_tiebreaker.select_index -= 1;
                }
                None
            }
            Action::DedupTiebreakerDown => {
                let last = self.dedup_tiebreaker.options.len().saturating_sub(1);
                if self.dedup_tiebreaker.select_index < last {
                    self.dedup_tiebreaker.select_index += 1;
                }
                None
            }
            Action::ApplyDedupTiebreaker => {
                self.apply_dedup_tiebreaker();
                None
            }
            Action::CancelDedupTiebreaker => {
                self.dedup_tiebreaker = crate::app_state::DedupTiebreakerState::default();
                self.mode = AppMode::Normal;
                self.status_message = "Smart dedup cancelled".to_string();
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

    fn apply_select_random(&mut self) {
        use rand::prelude::IndexedRandom;
        let s = self.stack.active_mut();
        let raw = s.select_count_input.as_str().trim().to_string();
        s.select_count_input.clear();
        let n: usize = match raw.parse() {
            Ok(v) if v > 0 => v,
            _ => {
                self.mode = AppMode::Normal;
                self.status_message = format!("Invalid N: '{}'", raw);
                return;
            }
        };
        let visible: Vec<usize> = s.dataframe.row_order.iter().copied().collect();
        let total = visible.len();
        let take = n.min(total);
        let mut rng = rand::rng();
        let chosen: Vec<usize> = visible.choose_multiple(&mut rng, take).copied().collect();
        s.dataframe.selected_rows.clear();
        s.dataframe.selected_rows.extend(chosen);
        self.mode = AppMode::Normal;
        self.status_message = format!("Selected {} random row(s) of {} visible", take, total);
    }

    fn select_duplicates(&mut self) {
        use std::collections::HashMap;
        let s = self.stack.active_mut();
        let n_cols = s.dataframe.col_count();
        let mut groups: HashMap<Vec<String>, Vec<usize>> = HashMap::new();
        for &phys in s.dataframe.row_order.iter() {
            let key: Vec<String> = (0..n_cols)
                .map(|c| s.dataframe.get_physical(phys, c))
                .collect();
            groups.entry(key).or_default().push(phys);
        }
        s.dataframe.selected_rows.clear();
        let mut count = 0usize;
        for rows in groups.into_values() {
            if rows.len() > 1 {
                for r in rows {
                    s.dataframe.selected_rows.insert(r);
                    count += 1;
                }
            }
        }
        self.mode = AppMode::Normal;
        if count == 0 {
            self.status_message = "No duplicate rows found".to_string();
        } else {
            self.status_message = format!("Selected {} duplicate row(s)", count);
        }
    }

    fn start_smart_dedup(&mut self) {
        let s = self.stack.active();
        let pinned: Vec<usize> = s
            .dataframe
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.pinned)
            .map(|(i, _)| i)
            .collect();
        if pinned.is_empty() {
            let n_cols = s.dataframe.col_count();
            let all_cols: Vec<usize> = (0..n_cols).collect();
            self.smart_dedup_with_keys(&all_cols, None);
            return;
        }
        let mut options: Vec<Option<(usize, bool)>> = vec![None];
        for (i, c) in s.dataframe.columns.iter().enumerate() {
            if !c.pinned {
                options.push(Some((i, false))); // ASC
                options.push(Some((i, true))); // DESC
            }
        }
        self.dedup_tiebreaker = crate::app_state::DedupTiebreakerState {
            options,
            select_index: 0,
            key_cols: pinned,
        };
        self.mode = AppMode::DedupTiebreakerSelect;
        self.status_message =
            "Smart dedup: pick tiebreaker (Enter to apply, Esc to cancel)".to_string();
    }

    fn apply_dedup_tiebreaker(&mut self) {
        let st = std::mem::take(&mut self.dedup_tiebreaker);
        if st.options.is_empty() {
            self.mode = AppMode::Normal;
            return;
        }
        let pick = st.options[st.select_index.min(st.options.len() - 1)];
        let key_cols = st.key_cols.clone();
        self.smart_dedup_with_keys(&key_cols, Some(pick));
    }

    /// Deduplicate `row_order` by `key_cols`, keeping one row per group.
    ///
    /// `tiebreaker`:
    ///   - `None` → first-seen row in `row_order` is kept (used when keys cover all columns).
    ///   - `Some(None)` → random row from the group is kept.
    ///   - `Some(Some((col, descending)))` → row with min/max value of `col` is kept.
    ///     Comparison tries f64 parse first, falls back to string compare; this handles
    ///     numeric columns and ISO-formatted dates without per-type dispatch.
    fn smart_dedup_with_keys(
        &mut self,
        key_cols: &[usize],
        tiebreaker: Option<Option<(usize, bool)>>,
    ) {
        use rand::prelude::IndexedRandom;
        use std::collections::{HashMap, HashSet};

        let s = self.stack.active_mut();
        s.push_undo();
        let old_count = s.dataframe.visible_row_count();

        let mut groups: HashMap<Vec<String>, Vec<usize>> = HashMap::new();
        for &phys in s.dataframe.row_order.iter() {
            let key: Vec<String> = key_cols
                .iter()
                .map(|&c| s.dataframe.get_physical(phys, c))
                .collect();
            groups.entry(key).or_default().push(phys);
        }

        let keepers: HashSet<usize> = match tiebreaker {
            None => groups.values().map(|rows| rows[0]).collect(),
            Some(None) => {
                let mut rng = rand::rng();
                groups
                    .values()
                    .map(|rows| *rows.choose(&mut rng).unwrap_or(&rows[0]))
                    .collect()
            }
            Some(Some((col, desc))) => groups
                .values()
                .map(|rows| {
                    let mut best_idx = rows[0];
                    let mut best_val = s.dataframe.get_physical(best_idx, col);
                    for &phys in rows.iter().skip(1) {
                        let val = s.dataframe.get_physical(phys, col);
                        let ord = compare_dedup_values(&val, &best_val);
                        let take = if desc {
                            ord == std::cmp::Ordering::Greater
                        } else {
                            ord == std::cmp::Ordering::Less
                        };
                        if take {
                            best_idx = phys;
                            best_val = val;
                        }
                    }
                    best_idx
                })
                .collect(),
        };

        let new_order: Vec<usize> = s
            .dataframe
            .row_order
            .iter()
            .copied()
            .filter(|i| keepers.contains(i))
            .collect();
        s.dataframe.row_order = new_order.into();
        s.dataframe.original_order = s.dataframe.row_order.clone();
        s.dataframe.selected_rows.clear();
        s.dataframe.modified = true;
        s.dataframe.aggregates_cache = None;
        s.table_state.select(Some(0));

        let new_count = s.dataframe.visible_row_count();
        self.mode = AppMode::Normal;
        self.status_message = format!("Smart dedup: {} → {} rows", old_count, new_count);
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

/// Compare two cell values for the smart-dedup tiebreaker.
///
/// Tries numeric parse first so numeric columns sort by value, not by lexical
/// order. Falls back to string compare, which already gives the right order for
/// ISO-8601 dates/datetimes.
fn compare_dedup_values(a: &str, b: &str) -> std::cmp::Ordering {
    if let (Ok(fa), Ok(fb)) = (a.trim().parse::<f64>(), b.trim().parse::<f64>()) {
        return fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal);
    }
    a.cmp(b)
}
