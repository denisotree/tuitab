use crate::app::App;
use crate::types::Action;

impl App {
    pub(crate) fn handle_navigation_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::MoveDown => {
                self.move_cursor_down();
                None
            }
            Action::MoveUp => {
                self.move_cursor_up();
                None
            }
            Action::MoveLeft => {
                self.move_cursor_left();
                None
            }
            Action::MoveRight => {
                self.move_cursor_right();
                None
            }
            Action::PageDown => {
                self.page_down();
                None
            }
            Action::PageUp => {
                self.page_up();
                None
            }
            Action::GoTop => {
                let s = self.stack.active_mut();
                s.table_state.select(Some(0));
                s.top_row = 0;
                s.scroll_state = s.scroll_state.position(0);
                None
            }
            Action::GoBottom => {
                let s = self.stack.active_mut();
                let last = s.dataframe.visible_row_count().saturating_sub(1);
                s.table_state.select(Some(last));
                s.top_row = last;
                s.scroll_state = s.scroll_state.position(last);
                None
            }
            other => Some(other),
        }
    }
}
