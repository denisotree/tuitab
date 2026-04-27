use crate::app::{expand_tilde, load_join_context_item_df, App};
use crate::types::{Action, AppMode};

impl App {
    pub(crate) fn handle_join_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::OpenJoin => {
                self.join.other_df = None;
                self.join.other_title.clear();
                self.join.left_keys.clear();
                self.join.right_keys.clear();
                self.join.left_key_index = 0;
                self.join.right_key_index = 0;
                self.join.path_input.clear();
                self.join.path_error = None;
                self.join.pending_queue.clear();

                let s = self.stack.active();
                let is_overview = s.is_dir_sheet
                    || s.sqlite_db_path.is_some()
                    || s.duckdb_db_path.is_some()
                    || s.xlsx_db_path.is_some();

                if is_overview {
                    self.join.context_items = self.collect_join_context_items();
                    if self.join.context_items.is_empty() {
                        self.status_message = "No items available for JOIN".to_string();
                        return None;
                    }
                    self.join.overview_cursor = 0;
                    self.join.overview_selected.clear();
                    self.mode = AppMode::JoinOverviewSelect;
                    self.status_message =
                        "JOIN: select items (Space=toggle, Enter=confirm, min 2)".to_string();
                } else {
                    self.join.source_index = 0;
                    self.join.context_items = self.collect_join_context_items();
                    self.mode = AppMode::JoinSelectSource;
                    self.status_message =
                        "JOIN: select source (↑↓ navigate, Enter select)".to_string();
                }
                None
            }

            // ── JOIN overview multi-select ─────────────────────────────────────
            Action::JoinOverviewUp => {
                if self.join.overview_cursor > 0 {
                    self.join.overview_cursor -= 1;
                }
                None
            }
            Action::JoinOverviewDown => {
                if self.join.overview_cursor + 1 < self.join.context_items.len() {
                    self.join.overview_cursor += 1;
                }
                None
            }
            Action::JoinOverviewToggle => {
                let idx = self.join.overview_cursor;
                if let Some(pos) = self.join.overview_selected.iter().position(|&i| i == idx) {
                    self.join.overview_selected.remove(pos);
                } else {
                    self.join.overview_selected.push(idx);
                }
                None
            }
            Action::JoinOverviewApply => {
                if self.join.overview_selected.len() < 2 {
                    self.status_message =
                        "SELECT at least 2 items for JOIN (Space to toggle)".to_string();
                    return None;
                }
                let mut sorted_sel = self.join.overview_selected.clone();
                sorted_sel.sort_unstable();
                let items: Vec<crate::types::JoinContextItem> = sorted_sel
                    .iter()
                    .map(|&i| self.join.context_items[i].clone())
                    .collect();

                let left_result = load_join_context_item_df(&items[0]);
                let right_result = load_join_context_item_df(&items[1]);

                match (left_result, right_result) {
                    (Ok((left_df, left_title)), Ok((right_df, right_title))) => {
                        let left_sheet = crate::sheet::Sheet::new(left_title, left_df);
                        self.stack.push(left_sheet);
                        self.join.other_df = Some(right_df);
                        self.join.other_title = right_title;
                        self.join.pending_queue = items[2..].to_vec();
                        self.join.type_index = 0;
                        self.join.left_keys.clear();
                        self.join.right_keys.clear();
                        self.join.left_key_index = 0;
                        self.join.right_key_index = 0;
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
                None
            }
            Action::JoinOverviewCancel => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }

            Action::JoinSourceUp => {
                if self.join.source_index > 0 {
                    self.join.source_index -= 1;
                }
                None
            }
            Action::JoinSourceDown => {
                let max =
                    self.join.context_items.len() + self.stack.sheet_titles_except_active().len();
                if self.join.source_index < max {
                    self.join.source_index += 1;
                }
                None
            }
            Action::JoinSourceApply => {
                let ctx_count = self.join.context_items.len();
                if self.join.source_index == 0 {
                    self.join.path_input.clear();
                    self.join.path_error = None;
                    self.mode = AppMode::JoinInputPath;
                    self.status_message = "Type path to file to join with".to_string();
                } else if self.join.source_index <= ctx_count {
                    let item = self.join.context_items[self.join.source_index - 1].clone();
                    match load_join_context_item_df(&item) {
                        Ok((df, title)) => {
                            self.join.other_df = Some(df);
                            self.join.other_title = title;
                            self.join.type_index = 0;
                            self.mode = AppMode::JoinSelectType;
                            self.status_message = "JOIN: select join type".to_string();
                        }
                        Err(e) => {
                            self.status_message = format!("Failed to load: {}", e);
                        }
                    }
                } else {
                    let stack_idx = self.join.source_index - 1 - ctx_count;
                    if let Some(df) = self.stack.clone_sheet_dataframe(stack_idx) {
                        let title = self
                            .stack
                            .sheet_titles_except_active()
                            .into_iter()
                            .nth(stack_idx)
                            .unwrap_or_default();
                        self.join.other_df = Some(df);
                        self.join.other_title = title;
                        self.join.type_index = 0;
                        self.mode = AppMode::JoinSelectType;
                        self.status_message = "JOIN: select join type".to_string();
                    } else {
                        self.status_message = "Failed to access sheet".to_string();
                    }
                }
                None
            }
            Action::JoinSourceCancel => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }

            // JOIN path input
            Action::JoinPathInput(c) => {
                self.join.path_input.insert_char(c);
                None
            }
            Action::JoinPathBackspace => {
                self.join.path_input.delete_backward();
                None
            }
            Action::JoinPathForwardDelete => {
                self.join.path_input.delete_forward();
                None
            }
            Action::JoinPathCursorLeft => {
                self.join.path_input.move_cursor_left();
                None
            }
            Action::JoinPathCursorRight => {
                self.join.path_input.move_cursor_right();
                None
            }
            Action::JoinPathCursorStart => {
                self.join.path_input.move_cursor_start();
                None
            }
            Action::JoinPathCursorEnd => {
                self.join.path_input.move_cursor_end();
                None
            }
            Action::JoinPathAutocomplete => {
                self.join_path_autocomplete();
                None
            }
            Action::JoinPathApply => {
                let raw = self.join.path_input.as_str().to_string();
                let path = expand_tilde(&raw);
                match crate::data::io::load_file(&path, None) {
                    Ok(df) => {
                        self.join.other_title = path
                            .file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or_else(|| raw.clone());
                        self.join.other_df = Some(df);
                        self.join.type_index = 0;
                        self.mode = AppMode::JoinSelectType;
                        self.status_message = "JOIN: select join type".to_string();
                    }
                    Err(e) => {
                        self.join.path_error = Some(format!("{}", e));
                        self.status_message = format!("Error: {}", e);
                    }
                }
                None
            }
            Action::JoinPathCancel => {
                self.mode = AppMode::JoinSelectSource;
                self.join.path_error = None;
                self.status_message = "JOIN: select source".to_string();
                None
            }

            // JOIN type selection
            Action::JoinTypeUp => {
                if self.join.type_index > 0 {
                    self.join.type_index -= 1;
                }
                None
            }
            Action::JoinTypeDown => {
                if self.join.type_index + 1 < crate::data::join::JoinType::all().len() {
                    self.join.type_index += 1;
                }
                None
            }
            Action::JoinTypeApply => {
                self.join.left_keys.clear();
                self.join.left_key_index = 0;
                self.mode = AppMode::JoinSelectLeftKeys;
                self.status_message =
                    "JOIN: select LEFT key columns (Space=toggle, Enter=next)".to_string();
                None
            }
            Action::JoinTypeCancel => {
                self.mode = AppMode::JoinSelectSource;
                self.status_message = "JOIN: select source".to_string();
                None
            }

            // JOIN left key selection
            Action::JoinLeftKeyUp => {
                if self.join.left_key_index > 0 {
                    self.join.left_key_index -= 1;
                }
                None
            }
            Action::JoinLeftKeyDown => {
                let n = self.stack.active().dataframe.columns.len();
                if self.join.left_key_index + 1 < n {
                    self.join.left_key_index += 1;
                }
                None
            }
            Action::JoinLeftKeyToggle => {
                let col_name = self
                    .stack
                    .active()
                    .dataframe
                    .columns
                    .get(self.join.left_key_index)
                    .map(|c| c.name.clone());
                if let Some(name) = col_name {
                    if let Some(pos) = self.join.left_keys.iter().position(|k| k == &name) {
                        self.join.left_keys.remove(pos);
                    } else {
                        self.join.left_keys.push(name);
                    }
                }
                None
            }
            Action::JoinLeftKeyApply => {
                if self.join.left_keys.is_empty() {
                    self.status_message = "Select at least one key column".to_string();
                } else {
                    let right_cols: Vec<String> = self
                        .join
                        .other_df
                        .as_ref()
                        .map(|df| df.columns.iter().map(|c| c.name.clone()).collect())
                        .unwrap_or_default();
                    self.join.right_keys = self
                        .join
                        .left_keys
                        .iter()
                        .filter(|lk| right_cols.contains(lk))
                        .cloned()
                        .collect();
                    self.join.right_key_index = 0;
                    self.mode = AppMode::JoinSelectRightKeys;
                    self.status_message =
                        "JOIN: select RIGHT key columns (Space=toggle, Enter=execute)".to_string();
                }
                None
            }
            Action::JoinLeftKeyCancel => {
                self.mode = AppMode::JoinSelectType;
                self.status_message = "JOIN: select join type".to_string();
                None
            }

            // JOIN right key selection
            Action::JoinRightKeyUp => {
                if self.join.right_key_index > 0 {
                    self.join.right_key_index -= 1;
                }
                None
            }
            Action::JoinRightKeyDown => {
                let n = self
                    .join
                    .other_df
                    .as_ref()
                    .map(|df| df.columns.len())
                    .unwrap_or(0);
                if self.join.right_key_index + 1 < n {
                    self.join.right_key_index += 1;
                }
                None
            }
            Action::JoinRightKeyToggle => {
                let col_name = self
                    .join
                    .other_df
                    .as_ref()
                    .and_then(|df| df.columns.get(self.join.right_key_index))
                    .map(|c| c.name.clone());
                if let Some(name) = col_name {
                    if let Some(pos) = self.join.right_keys.iter().position(|k| k == &name) {
                        self.join.right_keys.remove(pos);
                    } else {
                        self.join.right_keys.push(name);
                    }
                }
                None
            }
            Action::JoinRightKeyApply => {
                if self.join.right_keys.len() != self.join.left_keys.len() {
                    self.status_message = format!(
                        "Key count mismatch: {} left vs {} right — must match",
                        self.join.left_keys.len(),
                        self.join.right_keys.len()
                    );
                } else if self.join.right_keys.is_empty() {
                    self.status_message = "Select at least one right key column".to_string();
                } else {
                    self.execute_join();
                }
                None
            }
            Action::JoinRightKeyCancel => {
                self.mode = AppMode::JoinSelectLeftKeys;
                self.status_message = "JOIN: select LEFT key columns".to_string();
                None
            }

            other => Some(other),
        }
    }
}
