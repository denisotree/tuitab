use crate::theme::EverforestTheme as T;
use crate::ui::popup::centered_rect;
use ratatui::{
    layout::Rect,
    style::Style,
    widgets::{Block, Borders, Clear, List, ListItem},
    Frame,
};

pub fn render_type_select_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    use crate::types::ColumnType;
    let popup_area = centered_rect(40, 50, area);
    frame.render_widget(Clear, popup_area);

    let items: Vec<ListItem> = ColumnType::all()
        .iter()
        .enumerate()
        .map(|(i, ct)| {
            let is_active = i == app.type_select.index;
            let prefix = if is_active { "▶ " } else { "  " };
            let text = format!("{}{}", prefix, ct.display_name());
            let style = if is_active {
                Style::default().fg(T::YELLOW).bg(T::BG2)
            } else {
                Style::default().fg(T::FG)
            };
            ListItem::new(text).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Select Column Type (↑↓ navigate, Enter apply, Esc cancel) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}

pub fn render_currency_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    use crate::types::CurrencyKind;
    let popup_area = centered_rect(40, 40, area);
    frame.render_widget(Clear, popup_area);

    let items: Vec<ListItem> = CurrencyKind::all()
        .iter()
        .enumerate()
        .map(|(i, ck)| {
            let is_active = i == app.type_select.currency_index;
            let prefix = if is_active { "▶ " } else { "  " };
            let text = format!("{}{}", prefix, ck.display_name());
            let style = if is_active {
                Style::default().fg(T::YELLOW).bg(T::BG2)
            } else {
                Style::default().fg(T::FG)
            };
            ListItem::new(text).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Select Currency (↑↓ navigate, Enter apply, Esc back) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}
