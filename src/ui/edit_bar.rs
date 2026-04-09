use crate::app::App;
use crate::theme::EverforestTheme as T;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, BorderType, Clear, Paragraph};
use ratatui::Frame;

/// Render the cell editing popup at the bottom of the screen (Phase 8).
pub fn render(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let edit_area = Rect {
        x: 0,
        y: area.height.saturating_sub(3),
        width: area.width,
        height: 3,
    };

    let sheet = app.stack.active();
    let col_name = if sheet.edit_col < sheet.dataframe.col_count() {
        sheet.dataframe.columns[sheet.edit_col].name.as_str()
    } else {
        "?"
    };

    let input = Paragraph::new(sheet.edit_input.as_str())
        .style(T::filter_input_style())
        .block(
            Block::bordered()
                .title(format!(" Edit: {} ", col_name))
                .border_type(BorderType::Rounded)
                .border_style(T::separator_style()),
        );

    frame.render_widget(Clear, edit_area);
    frame.render_widget(input, edit_area);

    let text_len = sheet.edit_input.cursor_pos();
    frame.set_cursor_position((edit_area.x + 1 + text_len, edit_area.y + 1));
}
