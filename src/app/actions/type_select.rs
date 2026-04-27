use crate::app::App;
use crate::types::{Action, AppMode};

impl App {
    pub(crate) fn handle_type_select_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::OpenTypeSelect => {
                self.type_select.index = 0;
                let s = self.stack.active();
                let col = s.cursor_col;
                if col < s.dataframe.columns.len() {
                    let current_type = s.dataframe.columns[col].col_type;
                    if let Some(idx) = crate::types::ColumnType::all()
                        .iter()
                        .position(|t| *t == current_type)
                    {
                        self.type_select.index = idx;
                    }
                }
                self.mode = AppMode::TypeSelect;
                self.status_message =
                    "Select column type (↑↓ navigate, Enter apply, Esc cancel)".to_string();
                None
            }
            Action::TypeSelectUp => {
                let n = crate::types::ColumnType::all().len();
                if self.type_select.index > 0 {
                    self.type_select.index -= 1;
                } else {
                    self.type_select.index = n.saturating_sub(1);
                }
                None
            }
            Action::TypeSelectDown => {
                let n = crate::types::ColumnType::all().len();
                if n > 0 {
                    self.type_select.index = (self.type_select.index + 1) % n;
                }
                None
            }
            Action::ApplyTypeSelect => {
                let col_type = crate::types::ColumnType::all()[self.type_select.index];
                if col_type == crate::types::ColumnType::Currency {
                    self.type_select.currency_index = 0;
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
                None
            }
            Action::CancelTypeSelect => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::CurrencySelectUp => {
                let n = crate::types::CurrencyKind::all().len();
                if self.type_select.currency_index > 0 {
                    self.type_select.currency_index -= 1;
                } else {
                    self.type_select.currency_index = n.saturating_sub(1);
                }
                None
            }
            Action::CurrencySelectDown => {
                let n = crate::types::CurrencyKind::all().len();
                if n > 0 {
                    self.type_select.currency_index =
                        (self.type_select.currency_index + 1) % n;
                }
                None
            }
            Action::ApplyCurrencySelect => {
                let currency =
                    crate::types::CurrencyKind::all()[self.type_select.currency_index];
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
                None
            }
            Action::CancelCurrencySelect => {
                self.mode = AppMode::TypeSelect;
                self.status_message =
                    "Select column type (↑↓ navigate, Enter apply, Esc cancel)".to_string();
                None
            }
            other => Some(other),
        }
    }
}
