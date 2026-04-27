use crate::app::App;
use crate::types::{Action, AppMode};

impl App {
    pub(crate) fn handle_edit_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::OpenExternalEditor => {
                self.open_in_editor_pending = true;
                None
            }
            Action::StartEdit => {
                self.start_edit();
                None
            }
            Action::EditInput(c) => {
                self.stack.active_mut().edit_input.insert_char(c);
                None
            }
            Action::EditBackspace => {
                self.stack.active_mut().edit_input.delete_backward();
                None
            }
            Action::EditForwardDelete => {
                self.stack.active_mut().edit_input.delete_forward();
                None
            }
            Action::EditCursorLeft => {
                self.stack.active_mut().edit_input.move_cursor_left();
                None
            }
            Action::EditCursorRight => {
                self.stack.active_mut().edit_input.move_cursor_right();
                None
            }
            Action::EditCursorStart => {
                self.stack.active_mut().edit_input.move_cursor_start();
                None
            }
            Action::EditCursorEnd => {
                self.stack.active_mut().edit_input.move_cursor_end();
                None
            }
            Action::ApplyEdit => {
                self.apply_edit();
                None
            }
            Action::CancelEdit => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            other => Some(other),
        }
    }

    pub(super) fn start_edit(&mut self) {
        use crate::ui::text_input::TextInput;
        let s = self.stack.active_mut();
        if let Some(display_row) = s.table_state.selected() {
            if display_row < s.dataframe.visible_row_count() {
                let physical_row = s.dataframe.row_order[display_row];
                let col = s.cursor_col;
                s.edit_input =
                    TextInput::with_value(s.dataframe.get_physical(physical_row, col).to_string());
                s.edit_row = physical_row;
                s.edit_col = col;
                self.mode = AppMode::Editing;
            }
        }
    }

    pub(super) fn apply_edit(&mut self) {
        let s = self.stack.active_mut();
        s.push_undo();
        let new_value = s.edit_input.as_str().to_string();
        let row = s.edit_row;
        let col = s.edit_col;
        match s.dataframe.set_cell(row, col, new_value.clone()) {
            Ok(_) => {
                self.mode = AppMode::Normal;
                self.status_message = format!("Cell updated: '{}'", new_value);
            }
            Err(e) => {
                self.mode = AppMode::Normal;
                self.status_message = format!("Edit error: '{}'", e);
            }
        }
    }
}
