use crate::theme::EverforestTheme as T;
use crate::ui::popup::centered_rect;
use ratatui::{
    layout::Rect,
    style::Style,
    widgets::{Block, Borders, Clear, List, ListItem},
    Frame,
};

pub fn render_dedup_tiebreaker_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    let popup_area = centered_rect(50, 60, area);
    frame.render_widget(Clear, popup_area);

    let s = app.stack.active();
    let st = &app.dedup_tiebreaker;

    let items: Vec<ListItem> = st
        .options
        .iter()
        .enumerate()
        .map(|(i, opt)| {
            let is_active = i == st.select_index;
            let prefix = if is_active { "> " } else { "  " };
            let label = match opt {
                None => "[Random]".to_string(),
                Some((col, desc)) => {
                    let name = s
                        .dataframe
                        .columns
                        .get(*col)
                        .map(|c| c.name.as_str())
                        .unwrap_or("?");
                    let dir = if *desc {
                        "DESC (keep max)"
                    } else {
                        "ASC (keep min)"
                    };
                    format!("{}  {}", name, dir)
                }
            };
            let text = format!("{}{}", prefix, label);
            let mut style = Style::default().fg(T::FG);
            if is_active {
                style = style.bg(T::BG2);
            }
            ListItem::new(text).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Smart dedup: pick tiebreaker (Enter to apply, Esc to cancel) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}
