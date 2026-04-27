use crate::app::App;
use crate::types::{Action, AppMode, CopyPending};
use polars::prelude::*;
use ratatui::widgets::ScrollbarState;

impl App {
    pub(crate) fn handle_clipboard_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::EnterYPrefix => {
                self.mode = AppMode::YPrefix;
                self.status_message =
                    "y: (c)cell  (r)rows  (z)col.values  (Z)whole col  (R)whole table  Esc=cancel"
                        .to_string();
                None
            }
            Action::CancelYPrefix => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::CopyCurrentCell => {
                let s = self.stack.active();
                let row = s.table_state.selected().unwrap_or(0);
                let col = s.cursor_col;
                let phys = s.dataframe.row_order.get(row).copied().unwrap_or(0);
                let val = s.dataframe.format_display(phys, col);
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
                    let val = s.dataframe.format_display(phys, s.cursor_col);
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

    pub(super) fn paste_rows(&mut self) {
        match crate::clipboard::paste_from_clipboard() {
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
                    let series = Series::new(df.columns[col].name.clone().into(), &col_data);
                    series_vec.push(series.into());
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
}
