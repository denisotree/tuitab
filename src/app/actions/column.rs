use crate::app::App;
use crate::types::{Action, AppMode};
use crate::ui::text_input::TextInput;

impl App {
    pub(crate) fn handle_column_action(&mut self, action: Action) -> Option<Action> {
        match action {
            // ── Z Prefix (Column Operations) ──────────────────────────────────
            Action::EnterZPrefix => {
                self.mode = AppMode::ZPrefix;
                self.status_message =
                    "z: (e)dit name  (d)elete  (i)nsert  (s)elect  (u)nselect  (<-/->) move"
                        .to_string();
                None
            }
            Action::SelectColumn => {
                let s = self.stack.active_mut();
                let col = s.cursor_col;
                s.dataframe.columns[col].selected = true;
                let sel_count = s.dataframe.columns.iter().filter(|c| c.selected).count();
                self.status_message = format!("{} column(s) selected", sel_count);
                self.mode = AppMode::ZPrefix;
                None
            }
            Action::UnselectColumn => {
                let s = self.stack.active_mut();
                let col = s.cursor_col;
                s.dataframe.columns[col].selected = false;
                let sel_count = s.dataframe.columns.iter().filter(|c| c.selected).count();
                self.status_message = if sel_count == 0 {
                    "No columns selected".to_string()
                } else {
                    format!("{} column(s) selected", sel_count)
                };
                self.mode = AppMode::ZPrefix;
                None
            }
            Action::CancelZPrefix => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::StartRenameColumn => {
                let s = self.stack.active_mut();
                s.rename_column_input =
                    TextInput::with_value(s.dataframe.columns[s.cursor_col].name.clone());
                self.mode = AppMode::RenamingColumn;
                self.status_message = "Rename column: ".to_string();
                None
            }
            Action::RenameColumnInput(c) => {
                self.stack.active_mut().rename_column_input.insert_char(c);
                None
            }
            Action::RenameColumnBackspace => {
                self.stack.active_mut().rename_column_input.delete_backward();
                None
            }
            Action::RenameColumnForwardDelete => {
                self.stack
                    .active_mut()
                    .rename_column_input
                    .delete_forward();
                None
            }
            Action::RenameColumnCursorLeft => {
                self.stack
                    .active_mut()
                    .rename_column_input
                    .move_cursor_left();
                None
            }
            Action::RenameColumnCursorRight => {
                self.stack
                    .active_mut()
                    .rename_column_input
                    .move_cursor_right();
                None
            }
            Action::RenameColumnCursorStart => {
                self.stack
                    .active_mut()
                    .rename_column_input
                    .move_cursor_start();
                None
            }
            Action::RenameColumnCursorEnd => {
                self.stack
                    .active_mut()
                    .rename_column_input
                    .move_cursor_end();
                None
            }
            Action::ApplyRenameColumn => {
                self.apply_rename_column();
                None
            }
            Action::CancelRenameColumn => {
                self.stack.active_mut().rename_column_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::DeleteColumn => {
                self.delete_column();
                None
            }
            Action::StartInsertColumn => {
                self.stack.active_mut().insert_column_input.clear();
                self.mode = AppMode::InsertingColumn;
                self.status_message = "Insert column: ".to_string();
                None
            }
            Action::InsertColumnInput(c) => {
                self.stack.active_mut().insert_column_input.insert_char(c);
                None
            }
            Action::InsertColumnBackspace => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .delete_backward();
                None
            }
            Action::InsertColumnForwardDelete => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .delete_forward();
                None
            }
            Action::InsertColumnCursorLeft => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .move_cursor_left();
                None
            }
            Action::InsertColumnCursorRight => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .move_cursor_right();
                None
            }
            Action::InsertColumnCursorStart => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .move_cursor_start();
                None
            }
            Action::InsertColumnCursorEnd => {
                self.stack
                    .active_mut()
                    .insert_column_input
                    .move_cursor_end();
                None
            }
            Action::ApplyInsertColumn => {
                self.apply_insert_column();
                None
            }
            Action::CancelInsertColumn => {
                self.stack.active_mut().insert_column_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::MoveColumnLeft => {
                self.move_col_left();
                None
            }
            Action::MoveColumnRight => {
                self.move_col_right();
                None
            }
            Action::AdjustColumnWidth => {
                self.adjust_column_width();
                None
            }
            Action::AdjustAllColumnWidths => {
                self.adjust_all_column_widths();
                None
            }
            Action::IncreasePrecision => {
                self.adjust_precision(1);
                None
            }
            Action::DecreasePrecision => {
                self.adjust_precision(-1);
                None
            }
            Action::CreatePctColumn => {
                self.create_pct_column();
                None
            }
            Action::OpenPartitionSelect => {
                self.open_partition_select();
                None
            }
            Action::ApplyPartitionedPct => {
                self.apply_partitioned_pct();
                None
            }
            Action::PartitionSelectUp => {
                if self.partition.select_index > 0 {
                    self.partition.select_index -= 1;
                }
                None
            }
            Action::PartitionSelectDown => {
                let ncols = self.stack.active().dataframe.columns.len();
                if self.partition.select_index + 1 < ncols {
                    self.partition.select_index += 1;
                }
                None
            }
            Action::TogglePartitionSelection => {
                let s = self.stack.active();
                let col_name = s.dataframe.columns[self.partition.select_index]
                    .name
                    .clone();
                if self.partition.selected.contains(&col_name) {
                    self.partition.selected.remove(&col_name);
                } else {
                    self.partition.selected.insert(col_name);
                }
                None
            }
            Action::CancelPartitionSelect => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            other => Some(other),
        }
    }
}
