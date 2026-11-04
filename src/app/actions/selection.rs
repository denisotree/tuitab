use crate::app::App;
use crate::data::dataframe::DataFrame;
use crate::sheet::Sheet;
use crate::types::{Action, AppMode};
use polars::prelude::*;
use ratatui::widgets::ScrollbarState;

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
                self.status_message.clear();
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
            Action::AddRowBelow => {
                self.add_empty_row();
                None
            }
            Action::TogglePinColumn => {
                let s = self.stack.active_mut();
                s.push_undo();
                let col = s.cursor_col;
                // The column does not move, so neither does the cursor — it stays on
                // the same data, which now happens to be drawn in the pinned block.
                if s.dataframe.toggle_pin_column(col).is_ok() {
                    let meta = &s.dataframe.columns[col];
                    self.status_message = if meta.pinned {
                        format!("Pinned column '{}'", meta.name)
                    } else {
                        format!("Unpinned column '{}'", meta.name)
                    };
                }
                None
            }
            Action::ShowHelp => {
                self.mode = AppMode::Help;
                self.help_scroll = 0;
                self.status_message = "Press Esc or ? to close help".to_string();
                None
            }
            Action::HelpScroll(by) => {
                self.help_scroll = (i64::from(self.help_scroll) + i64::from(by))
                    .clamp(0, i64::from(u16::MAX)) as u16;
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
                self.status_message.clear();
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

    /// Put an empty row under the cursor (`o`) and land on it, as vim does.
    ///
    /// This is the other half of building a table from nothing: `zi` gives it columns,
    /// this gives it rows.  The new row carries no database identity, so saving turns it
    /// into an INSERT.  `O` fills a row in first — see [`App::handle_row_form_action`].
    pub(super) fn add_empty_row(&mut self) {
        if self.reject_on_doc_sheet("Adding a row") {
            return;
        }
        if self.stack.active().is_dir_sheet {
            self.status_message = "This is a directory listing".to_string();
            return;
        }

        let s = self.stack.active_mut();
        s.push_undo();
        let at = s.table_state.selected().unwrap_or(0) + 1;
        match s.dataframe.insert_empty_row(at) {
            Ok(()) => {
                let at = at.min(s.dataframe.visible_row_count().saturating_sub(1));
                let vis = s.dataframe.visible_row_count();
                s.scroll_state = ScrollbarState::new(vis.saturating_sub(1));
                s.table_state.select(Some(at));
                self.status_message = "Added a row".to_string();
            }
            Err(_) => {
                s.pop_undo();
                s.redo_stack.pop();
                self.status_message = "Add a column first with 'zi'".to_string();
            }
        }
    }

    pub(super) fn delete_selected_rows(&mut self) {
        let s = self.stack.active_mut();
        let count = s.dataframe.selected_rows.len();
        if count == 0 {
            self.status_message = "No rows selected to delete".to_string();
            return;
        }
        s.push_undo();

        // On a doc-backed sheet a row is an array element or an object key, so the
        // deletion has to reach the tree.  Filtering `row_order` alone would only hide
        // the rows, and the next save would bring them back.
        if s.doc.is_some() {
            let selected = s.dataframe.selected_rows.clone();
            let doc = s.doc.as_mut().expect("checked above");
            match doc.delete_rows(&selected) {
                Ok(df) => {
                    s.dataframe = df;
                    s.dataframe.selected_rows.clear();
                    s.dataframe.modified = true;
                    // The frame is rebuilt in the tree's order, so a sort marker left
                    // over from before would claim an ordering the rows no longer have.
                    s.sort_keys.clear();
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
                Err(e) => {
                    s.pop_undo();
                    s.redo_stack.pop();
                    self.status_message = format!("Delete failed: {}", e);
                }
            }
            return;
        }

        let removed: Vec<usize> = s.dataframe.selected_rows.iter().copied().collect();
        s.dataframe.record_deleted_rows(removed);
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

        let mut new_df = DataFrame::from_parts(pdf, new_columns);
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

        let total = s.dataframe.visible_row_count();
        let chosen =
            crate::data::dedup::sample_rows(&s.dataframe, n, crate::data::dedup::random_seed());
        let take = chosen.len();
        s.dataframe.selected_rows = chosen.into_iter().collect();

        self.mode = AppMode::Normal;
        self.status_message = format!("Selected {} random row(s) of {} visible", take, total);
    }

    fn select_duplicates(&mut self) {
        let s = self.stack.active_mut();
        let duplicates = crate::data::dedup::duplicate_rows(&s.dataframe, &[]);
        s.dataframe.selected_rows = duplicates.iter().copied().collect();
        let count = duplicates.len();

        self.mode = AppMode::Normal;
        self.status_message = if count == 0 {
            "No duplicate rows found".to_string()
        } else {
            format!("Selected {} duplicate row(s)", count)
        };
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

    /// `tiebreaker` mirrors the popup: `None` keeps the first of each group,
    /// `Some(None)` picks at random, `Some(Some((col, desc)))` keeps the row
    /// with the largest or smallest value in that column.
    fn smart_dedup_with_keys(
        &mut self,
        key_cols: &[usize],
        tiebreaker: Option<Option<(usize, bool)>>,
    ) {
        use crate::data::dedup::Keep;

        let keep = match tiebreaker {
            None => Keep::First,
            Some(None) => Keep::Random(crate::data::dedup::random_seed()),
            Some(Some((col, true))) => Keep::Max(col),
            Some(Some((col, false))) => Keep::Min(col),
        };

        let s = self.stack.active_mut();
        let old_count = s.dataframe.visible_row_count();

        match crate::data::dedup::deduplicate(&s.dataframe, key_cols, keep) {
            Ok(survivors) => {
                s.push_undo();
                // Dedup drops rows for good, exactly like `d` does — the sheet is the
                // statement of intent, and the SQL popup is where it gets confirmed.
                let kept: std::collections::HashSet<usize> = survivors.iter().copied().collect();
                let dropped: Vec<usize> = s
                    .dataframe
                    .original_order
                    .iter()
                    .copied()
                    .filter(|i| !kept.contains(i))
                    .collect();
                s.dataframe.record_deleted_rows(dropped);
                s.dataframe.row_order = survivors.into();
                s.dataframe.original_order = s.dataframe.row_order.clone();
                s.dataframe.selected_rows.clear();
                s.dataframe.modified = true;
                s.dataframe.aggregates_cache = None;
                s.table_state.select(Some(0));

                let new_count = s.dataframe.visible_row_count();
                self.status_message = format!("Smart dedup: {} → {} rows", old_count, new_count);
            }
            Err(e) => self.status_message = e,
        }
        self.mode = AppMode::Normal;
    }

    /// Keep one row per distinct combination of the pinned columns.
    ///
    /// Exactly smart dedup with no tiebreaker — it was a second copy of the
    /// same grouping loop until the two met in `data::dedup`.
    pub(super) fn deduplicate_by_pinned(&mut self) {
        let pinned: Vec<usize> = self
            .stack
            .active()
            .dataframe
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.pinned)
            .map(|(i, _)| i)
            .collect();

        if pinned.is_empty() {
            self.mode = AppMode::Normal;
            self.status_message = "No pinned columns to deduplicate by".to_string();
            return;
        }

        self.smart_dedup_with_keys(&pinned, None);
    }
}
