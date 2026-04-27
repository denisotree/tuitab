use crate::app::App;
use crate::types::{Action, AppMode};

impl App {
    pub(crate) fn handle_search_action(&mut self, action: Action) -> Option<Action> {
        match action {
            // ── Search (/) ────────────────────────────────────────────────────
            Action::StartSearch => {
                let s = self.stack.active_mut();
                s.search_col = Some(s.cursor_col);
                s.search_input.clear();
                self.mode = AppMode::Searching;
                self.status_message = "Search (regex): ".to_string();
                None
            }
            Action::SearchInput(c) => {
                self.stack.active_mut().search_input.insert_char(c);
                None
            }
            Action::SearchBackspace => {
                self.stack.active_mut().search_input.delete_backward();
                None
            }
            Action::SearchForwardDelete => {
                self.stack.active_mut().search_input.delete_forward();
                None
            }
            Action::SearchCursorLeft => {
                self.stack.active_mut().search_input.move_cursor_left();
                None
            }
            Action::SearchCursorRight => {
                self.stack.active_mut().search_input.move_cursor_right();
                None
            }
            Action::SearchCursorStart => {
                self.stack.active_mut().search_input.move_cursor_start();
                None
            }
            Action::SearchCursorEnd => {
                self.stack.active_mut().search_input.move_cursor_end();
                None
            }
            Action::ApplySearch => {
                self.apply_search();
                None
            }
            Action::CancelSearch => {
                self.stack.active_mut().search_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::SearchNext => {
                self.search_next();
                None
            }
            Action::SearchPrev => {
                self.search_prev();
                None
            }
            Action::ClearSearch => {
                let s = self.stack.active_mut();
                s.search_pattern = None;
                s.search_col = None;
                self.status_message = "Search cleared".to_string();
                None
            }

            Action::SelectByValue => {
                self.select_by_value();
                None
            }

            // ── Select by regex (|) ───────────────────────────────────────────
            Action::StartSelectByRegex => {
                self.stack.active_mut().select_regex_input.clear();
                self.mode = AppMode::SelectByRegex;
                self.status_message = "Select by regex: ".to_string();
                None
            }
            Action::SelectRegexInput(c) => {
                self.expression.autocomplete_candidates.clear();
                self.stack.active_mut().select_regex_input.insert_char(c);
                None
            }
            Action::SelectRegexBackspace => {
                self.expression.autocomplete_candidates.clear();
                self.stack.active_mut().select_regex_input.delete_backward();
                None
            }
            Action::SelectRegexForwardDelete => {
                self.stack.active_mut().select_regex_input.delete_forward();
                None
            }
            Action::SelectRegexCursorLeft => {
                self.stack.active_mut().select_regex_input.move_cursor_left();
                None
            }
            Action::SelectRegexCursorRight => {
                self.stack
                    .active_mut()
                    .select_regex_input
                    .move_cursor_right();
                None
            }
            Action::SelectRegexCursorStart => {
                self.stack
                    .active_mut()
                    .select_regex_input
                    .move_cursor_start();
                None
            }
            Action::SelectRegexCursorEnd => {
                self.stack.active_mut().select_regex_input.move_cursor_end();
                None
            }
            Action::ApplySelectByRegex => {
                self.apply_select_by_regex();
                None
            }
            Action::CancelSelectByRegex => {
                self.expression.autocomplete_candidates.clear();
                self.stack.active_mut().select_regex_input.clear();
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::SelectRegexAutocomplete => {
                self.select_regex_autocomplete();
                None
            }

            other => Some(other),
        }
    }
}
