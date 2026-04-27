use crate::theme::EverforestTheme as T;
use crate::ui::popup::centered_rect;
use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph},
    Frame,
};

pub fn render_input_popup(
    frame: &mut Frame,
    title: &str,
    input: &crate::ui::text_input::TextInput,
    error_msg: Option<&str>,
    area: Rect,
) {
    let popup_area = centered_rect(60, 20, area);
    frame.render_widget(Clear, popup_area);

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(T::PURPLE));

    let mut lines = vec![Line::from(vec![
        Span::styled("> ", Style::default().fg(T::YELLOW)),
        Span::raw(input.as_str()),
    ])];

    if let Some(err) = error_msg {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(err, Style::default().fg(T::RED))));
    }

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, popup_area);

    let prefix_len = 2;
    let text_len = input.cursor_pos();
    frame.set_cursor_position((popup_area.x + 1 + prefix_len + text_len, popup_area.y + 1));
}
