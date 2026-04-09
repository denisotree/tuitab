use crate::app::App;
use crate::theme::EverforestTheme as T;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, BorderType, Clear, Paragraph};
use ratatui::Frame;

/// Render the select-by-regex input as a floating popup near the bottom of the screen.
pub fn render(frame: &mut Frame, app: &App) {
    let area = frame.area();

    let popup_area = Rect {
        x: 0,
        y: area.height.saturating_sub(4),
        width: area.width,
        height: 3,
    };

    let sheet = app.stack.active();
    let col_name = sheet
        .dataframe
        .columns
        .get(sheet.cursor_col)
        .map(|m| m.name.as_str())
        .unwrap_or("?");

    let input = Paragraph::new(format!("|{}", sheet.select_regex_input.as_str()))
        .style(T::filter_input_style())
        .block(
            Block::bordered()
                .title(format!(" Select by regex: {} ", col_name))
                .border_type(BorderType::Rounded)
                .border_style(T::separator_style()),
        );

    frame.render_widget(Clear, popup_area);
    frame.render_widget(input, popup_area);

    let prefix_len = 1; // "|"
    let text_len = sheet.select_regex_input.cursor_pos();
    frame.set_cursor_position((popup_area.x + 1 + prefix_len + text_len, popup_area.y + 1));
}
