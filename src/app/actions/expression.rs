use crate::app::App;
use crate::types::{Action, AppMode};

impl App {
    pub(crate) fn handle_expression_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::StartExpression => {
                self.stack.active_mut().expr_input.clear();
                self.mode = AppMode::ExpressionInput;
                self.status_message = "Expression: ".to_string();
                self.expression.history_idx = None;
                self.expression.autocomplete_candidates.clear();
                None
            }
            Action::ExpressionInputChar(c) => {
                self.expression.autocomplete_candidates.clear();
                self.stack.active_mut().expr_input.insert_char(c);
                None
            }
            Action::ExpressionBackspace => {
                self.expression.autocomplete_candidates.clear();
                self.stack.active_mut().expr_input.delete_backward();
                None
            }
            Action::ExpressionForwardDelete => {
                self.expression.autocomplete_candidates.clear();
                self.stack.active_mut().expr_input.delete_forward();
                None
            }
            Action::ExpressionCursorLeft => {
                self.stack.active_mut().expr_input.move_cursor_left();
                None
            }
            Action::ExpressionCursorRight => {
                self.stack.active_mut().expr_input.move_cursor_right();
                None
            }
            Action::ExpressionCursorStart => {
                self.stack.active_mut().expr_input.move_cursor_start();
                None
            }
            Action::ExpressionCursorEnd => {
                self.stack.active_mut().expr_input.move_cursor_end();
                None
            }
            Action::ApplyExpression => {
                self.apply_expression();
                None
            }
            Action::CancelExpression => {
                self.stack.active_mut().expr_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::ExpressionAutocomplete => {
                self.expr_autocomplete();
                None
            }
            Action::ExpressionHistoryPrev => {
                self.expr_history_prev();
                None
            }
            Action::ExpressionHistoryNext => {
                self.expr_history_next();
                None
            }
            other => Some(other),
        }
    }
}
