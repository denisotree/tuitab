use crate::app::App;
use crate::types::{Action, AppMode, SheetType};
use crate::ui::text_input::TextInput;

impl App {
    pub(crate) fn handle_pivot_action(&mut self, action: Action) -> Option<Action> {
        match action {
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
                None
            }
            Action::ApplyPivotTable => {
                if self.mode == AppMode::Calculating {
                    self.apply_pivot_table();
                } else {
                    self.mode = AppMode::Calculating;
                    self.pending_action = Some(Action::ApplyPivotTable);
                }
                None
            }
            Action::CancelPivotTable => {
                self.expression.autocomplete_candidates.clear();
                self.pivot.history_idx = None;
                self.mode = AppMode::Normal;
                self.stack.active_mut().pivot_input.clear();
                None
            }
            Action::PivotAutocomplete => {
                self.pivot_autocomplete();
                None
            }
            Action::PivotHistoryPrev => {
                self.pivot_history_prev();
                None
            }
            Action::PivotHistoryNext => {
                self.pivot_history_next();
                None
            }
            Action::PivotInput(c) => {
                self.expression.autocomplete_candidates.clear();
                self.stack.active_mut().pivot_input.insert_char(c);
                None
            }
            Action::PivotBackspace => {
                self.expression.autocomplete_candidates.clear();
                self.stack.active_mut().pivot_input.delete_backward();
                None
            }
            Action::PivotForwardDelete => {
                self.stack.active_mut().pivot_input.delete_forward();
                None
            }
            Action::PivotCursorLeft => {
                self.stack.active_mut().pivot_input.move_cursor_left();
                None
            }
            Action::PivotCursorRight => {
                self.stack.active_mut().pivot_input.move_cursor_right();
                None
            }
            Action::PivotCursorStart => {
                self.stack.active_mut().pivot_input.move_cursor_start();
                None
            }
            Action::PivotCursorEnd => {
                self.stack.active_mut().pivot_input.move_cursor_end();
                None
            }
            other => Some(other),
        }
    }

    pub(super) fn pivot_autocomplete(&mut self) {
        const AGG_FUNCS: &[&str] = &["sum", "count", "mean", "median", "min", "max"];

        let s = self.stack.active_mut();
        if self.expression.autocomplete_candidates.is_empty() {
            let input_str = s.pivot_input.as_str();
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
            for func in AGG_FUNCS {
                let lower = func.to_lowercase();
                if lower.starts_with(&word_lower)
                    && !prefix_matches.iter().any(|m| m.as_str() == *func)
                {
                    prefix_matches.push(func.to_string());
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

        let completion =
            self.expression.autocomplete_candidates[self.expression.autocomplete_idx].clone();
        let new_val = format!("{}{}", self.expression.autocomplete_prefix, completion);
        self.stack.active_mut().pivot_input = TextInput::with_value(new_val);
    }

    pub(super) fn pivot_history_prev(&mut self) {
        if self.pivot.history.is_empty() {
            return;
        }
        let new_idx = match self.pivot.history_idx {
            Some(i) if i > 0 => i - 1,
            Some(i) => i,
            None => self.pivot.history.len() - 1,
        };
        self.pivot.history_idx = Some(new_idx);
        let val = self.pivot.history[new_idx].clone();
        self.stack.active_mut().pivot_input = TextInput::with_value(val);
    }

    pub(super) fn pivot_history_next(&mut self) {
        if let Some(idx) = self.pivot.history_idx {
            if idx + 1 < self.pivot.history.len() {
                let new_idx = idx + 1;
                self.pivot.history_idx = Some(new_idx);
                let val = self.pivot.history[new_idx].clone();
                self.stack.active_mut().pivot_input = TextInput::with_value(val);
            } else {
                self.pivot.history_idx = None;
                self.stack.active_mut().pivot_input = TextInput::new();
            }
        }
    }

    pub(super) fn apply_pivot_table(&mut self) {
        let formula_str = self.stack.active().pivot_input.as_str().to_string();
        if formula_str.is_empty() {
            self.mode = AppMode::Normal;
            return;
        }

        if self.pivot.history.last() != Some(&formula_str) {
            self.pivot.history.push(formula_str.clone());
        }
        self.pivot.history_idx = None;
        self.expression.autocomplete_candidates.clear();

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
}
