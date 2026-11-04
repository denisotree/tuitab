use crate::app::App;
use crate::types::{Action, AppMode, CopyPending};
use polars::prelude::*;
use ratatui::widgets::ScrollbarState;

/// What one field of a pasted line means for the column it lands in.
///
/// A field the line does not have — a row shorter than the table — is absent, not the
/// empty string: `""` is not a number and would fail the strict cast for every numeric
/// column it touched. An empty field that is actually there is only NULL for a column
/// that cannot hold text; in a text column the empty string is a value, and turning it
/// into NULL behind the user's back is exactly what tuitab promises not to do.
fn paste_field(field: Option<&str>, text_col: bool) -> Option<String> {
    match field {
        None => None,
        Some(v) if v.is_empty() && !text_col => None,
        Some(v) => Some(v.to_string()),
    }
}

impl App {
    pub(crate) fn handle_clipboard_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::EnterYPrefix => {
                self.mode = AppMode::YPrefix;
                self.status_message.clear();
                None
            }
            Action::CancelYPrefix => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::CopyNodePath => {
                self.copy_node_path();
                None
            }
            Action::CopyCurrentCell => {
                let s = self.stack.active();
                let row = s.table_state.selected().unwrap_or(0);
                let col = s.cursor_col;
                let phys = s.dataframe.row_order.get(row).copied().unwrap_or(0);
                // The value, not the rendering of it: a Float column is displayed to two
                // decimals, and copying that back into a cell would silently round it.
                let val = s.dataframe.get_editable(phys, col);
                match crate::clipboard::copy_text(&val) {
                    Ok(_) => self.status_message = format!("Copied cell value: {}", val),
                    Err(e) => self.status_message = format!("Clipboard error: {}", e),
                }
                self.mode = AppMode::Normal;
                None
            }
            Action::OpenCopyFormat(pending) => {
                if pending == CopyPending::SmartColumn
                    && self.stack.active().dataframe.selected_rows.is_empty()
                {
                    let s = self.stack.active();
                    let row = s.table_state.selected().unwrap_or(0);
                    let phys = s.dataframe.row_order.get(row).copied().unwrap_or(0);
                    let val = s.dataframe.get_editable(phys, s.cursor_col);
                    self.status_message = match crate::clipboard::copy_text(&val) {
                        Ok(_) => format!("Copied cell value: {}", val),
                        Err(e) => format!("Clipboard error: {}", e),
                    };
                    self.mode = AppMode::Normal;
                } else {
                    self.copy.pending = Some(pending);
                    self.copy.format_index = 0;
                    self.mode = AppMode::CopyFormatSelect;
                }
                None
            }
            Action::CopyFormatSelectUp => {
                if self.copy.format_index > 0 {
                    self.copy.format_index -= 1;
                }
                None
            }
            Action::CopyFormatSelectDown => {
                let max = self.copy_format_option_count().saturating_sub(1);
                if self.copy.format_index < max {
                    self.copy.format_index += 1;
                }
                None
            }
            Action::CancelCopyFormat => {
                self.copy.pending = None;
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::ApplyCopyFormat => {
                match self.execute_copy_with_format() {
                    Ok(msg) => self.status_message = msg,
                    Err(e) => self.status_message = format!("Clipboard error: {}", e),
                }
                self.copy.pending = None;
                self.mode = AppMode::Normal;
                None
            }
            Action::PasteRows => {
                self.paste_rows();
                None
            }
            Action::PasteCell => {
                self.paste_cell();
                None
            }
            other => Some(other),
        }
    }

    pub(super) fn copy_format_option_count(&self) -> usize {
        match self.copy.pending {
            Some(CopyPending::SmartRows | CopyPending::WholeTable) => 4,
            Some(CopyPending::SmartColumn | CopyPending::WholeColumn) => 3,
            None => 0,
        }
    }

    fn effective_col_indices(df: &crate::data::dataframe::DataFrame) -> Vec<usize> {
        let selected: Vec<usize> = df
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.selected)
            .map(|(i, _)| i)
            .collect();
        if selected.is_empty() {
            (0..df.col_count()).collect()
        } else {
            selected
        }
    }

    pub(super) fn execute_copy_with_format(&self) -> color_eyre::Result<String> {
        let s = self.stack.active();
        let df = &s.dataframe;
        match self.copy.pending {
            Some(CopyPending::SmartRows) => {
                let col_indices = Self::effective_col_indices(df);
                let headers: Vec<&str> = col_indices
                    .iter()
                    .map(|&i| df.columns[i].name.as_str())
                    .collect();
                if df.selected_rows.is_empty() {
                    let row = s.table_state.selected().unwrap_or(0);
                    let phys = df.row_order.get(row).copied().unwrap_or(0);
                    let row_data: Vec<String> = col_indices
                        .iter()
                        .map(|&c| df.format_display(phys, c))
                        .collect();
                    let rows = vec![row_data];
                    self.copy_rows_with_format(&headers, &rows)
                        .map(|fmt| format!("Copied row ({})", fmt))
                } else {
                    let mut sorted_phys: Vec<usize> = df.selected_rows.iter().copied().collect();
                    sorted_phys.sort_unstable();
                    let rows: Vec<Vec<String>> = sorted_phys
                        .iter()
                        .map(|&phys| {
                            col_indices
                                .iter()
                                .map(|&c| df.format_display(phys, c))
                                .collect()
                        })
                        .collect();
                    let count = rows.len();
                    self.copy_rows_with_format(&headers, &rows)
                        .map(|fmt| format!("Copied {} rows ({})", count, fmt))
                }
            }
            Some(CopyPending::SmartColumn) => {
                let col = s.cursor_col;
                let mut sorted_phys: Vec<usize> = df.selected_rows.iter().copied().collect();
                sorted_phys.sort_unstable();
                let values: Vec<String> = sorted_phys
                    .iter()
                    .map(|&phys| df.format_display(phys, col))
                    .collect();
                let count = values.len();
                self.copy_column_with_format(&values)
                    .map(|fmt| format!("Copied {} values ({})", count, fmt))
            }
            Some(CopyPending::WholeColumn) => {
                let col = s.cursor_col;
                let values: Vec<String> = (0..df.visible_row_count())
                    .map(|r| df.format_display(df.row_order[r], col))
                    .collect();
                let count = values.len();
                self.copy_column_with_format(&values)
                    .map(|fmt| format!("Copied {} values ({})", count, fmt))
            }
            Some(CopyPending::WholeTable) => {
                let col_indices = Self::effective_col_indices(df);
                let headers: Vec<&str> = col_indices
                    .iter()
                    .map(|&i| df.columns[i].name.as_str())
                    .collect();
                let rows: Vec<Vec<String>> = (0..df.visible_row_count())
                    .map(|r| {
                        let phys = df.row_order[r];
                        col_indices
                            .iter()
                            .map(|&c| df.format_display(phys, c))
                            .collect()
                    })
                    .collect();
                let count = rows.len();
                self.copy_rows_with_format(&headers, &rows)
                    .map(|fmt| format!("Copied {} rows ({})", count, fmt))
            }
            None => Ok(String::new()),
        }
    }

    fn copy_rows_with_format(
        &self,
        headers: &[&str],
        rows: &[Vec<String>],
    ) -> color_eyre::Result<&'static str> {
        match self.copy.format_index {
            0 => {
                crate::clipboard::copy_tsv(headers, rows)?;
                Ok("TSV")
            }
            1 => {
                crate::clipboard::copy_csv(headers, rows)?;
                Ok("CSV")
            }
            2 => {
                crate::clipboard::copy_json(headers, rows)?;
                Ok("JSON")
            }
            _ => {
                crate::clipboard::copy_markdown(headers, rows)?;
                Ok("Markdown")
            }
        }
    }

    fn copy_column_with_format(&self, values: &[String]) -> color_eyre::Result<&'static str> {
        match self.copy.format_index {
            0 => {
                crate::clipboard::copy_column_newline(values)?;
                Ok("newline-separated")
            }
            1 => {
                crate::clipboard::copy_column_comma(values)?;
                Ok("comma-separated")
            }
            _ => {
                crate::clipboard::copy_column_comma_quoted(values)?;
                Ok("comma-separated, quoted")
            }
        }
    }

    /// Copy the document path of the cell under the cursor, so a value can be referred
    /// to elsewhere — in a bug report, a script, a `jq` expression.
    pub(super) fn copy_node_path(&mut self) {
        self.mode = crate::types::AppMode::Normal;
        let Some(path) = self.cursor_node_path(true) else {
            self.status_message = "No document path here".to_string();
            return;
        };
        let text = crate::data::doc::path_to_string(&path);
        let text = if text.is_empty() {
            "(root)".to_string()
        } else {
            text
        };
        match crate::clipboard::copy_text(&text) {
            Ok(()) => self.status_message = format!("Copied path: {}", text),
            Err(e) => self.status_message = format!("Copy failed: {}", e),
        }
    }

    /// `p` — paste the clipboard's first line into the cell under the cursor.
    /// Routed through the same write as `e`, so typing, document write-back, undo and
    /// the error message are whatever editing that cell by hand would give.
    pub(super) fn paste_cell(&mut self) {
        let text = match crate::clipboard::paste_from_clipboard() {
            Ok(t) => t,
            Err(e) => {
                self.status_message = format!("Clipboard error: {}", e);
                return;
            }
        };
        let Some(value) = text.lines().next() else {
            self.status_message = "Clipboard is empty".to_string();
            return;
        };
        let value = value.trim_end_matches('\r').to_string();
        let s = self.stack.active_mut();
        let Some(display_row) = s.table_state.selected() else {
            return;
        };
        if display_row >= s.dataframe.visible_row_count() {
            return;
        }
        s.edit_row = s.dataframe.row_order[display_row];
        s.edit_col = s.cursor_col;
        s.edit_input = crate::ui::text_input::TextInput::with_value(value);
        self.apply_edit();
    }

    pub(super) fn paste_rows(&mut self) {
        // A clipboard table has no defined meaning as document rows: the columns are a
        // projection, not the shape of a node.  Pasting into a document is `E`.
        if self.stack.active().doc.is_some() {
            self.status_message =
                "Paste does not apply to a JSON/YAML/TOML view — press E to edit the document"
                    .to_string();
            return;
        }
        match crate::clipboard::paste_from_clipboard() {
            Ok(text) => {
                let s = self.stack.active_mut();
                s.push_undo();
                let df = &mut s.dataframe;
                let col_count = df.col_count();
                if col_count == 0 {
                    s.undo_stack.pop();
                    return;
                }
                let lines: Vec<&str> = text.lines().collect();
                if lines.is_empty() {
                    s.undo_stack.pop();
                    self.status_message = "Clipboard is empty".to_string();
                    return;
                }
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
                    let target = df.df.columns()[col].dtype().clone();
                    let text_col = target == DataType::String;
                    let mut col_data: Vec<Option<String>> = Vec::new();
                    for line in &lines[start..] {
                        let fields: Vec<&str> = line.split('\t').collect();
                        let val = paste_field(fields.get(col).copied(), text_col);
                        col_data.push(val);
                    }
                    let series = Series::new(df.columns[col].name.clone().into(), &col_data);
                    // Clipboard text is text; the column it lands in may not be. Cast
                    // strictly so a value the column cannot hold is named here rather
                    // than becoming a silent NULL or failing the stack below with a
                    // dtype error that says nothing about which column.
                    let target = &target;
                    let series = match series.strict_cast(target) {
                        Ok(cast) => cast,
                        Err(_) => {
                            let msg = format!(
                                "Paste failed: column '{}' holds {}, and the pasted text is not",
                                df.columns[col].name, target
                            );
                            s.undo_stack.pop();
                            self.status_message = msg;
                            return;
                        }
                    };
                    series_vec.push(series.into());
                }
                if let Ok(new_df) = polars::prelude::DataFrame::new_infer_height(series_vec) {
                    let original_height = df.df.height();
                    // The stack has to succeed before anything else is told rows arrived:
                    // `row_order` would otherwise point past the end of the frame, and
                    // `db_rows.ids` — which is indexed by physical row — would be longer
                    // than the frame it indexes.  It fails for real, on a dtype mismatch:
                    // the pasted columns are built as text.
                    if original_height == 0 {
                        df.df = new_df;
                    } else if let Err(e) = df.df.vstack_mut(&new_df) {
                        let msg = format!("Paste failed: {}", e);
                        s.undo_stack.pop();
                        self.status_message = msg;
                        return;
                    }
                    let added = lines.len() - start;
                    for i in 0..added {
                        let new_idx = original_height + i;
                        std::sync::Arc::make_mut(&mut df.row_order).push(new_idx);
                        std::sync::Arc::make_mut(&mut df.original_order).push(new_idx);
                    }
                    df.record_added_rows(added);
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
}

#[cfg(test)]
mod paste_field_tests {
    use super::paste_field;

    #[test]
    fn a_missing_field_is_null_and_an_empty_text_field_is_not() {
        assert_eq!(paste_field(None, false), None);
        assert_eq!(paste_field(None, true), None);
        assert_eq!(paste_field(Some(""), false), None);
        assert_eq!(paste_field(Some(""), true).as_deref(), Some(""));
        assert_eq!(paste_field(Some("7"), false).as_deref(), Some("7"));
    }
}
