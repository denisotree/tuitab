use crate::theme::EverforestTheme as T;
use crate::ui::popup::centered_rect;
use ratatui::{
    layout::Rect,
    style::Style,
    widgets::{Block, Borders, Clear, List, ListItem},
    Frame,
};

pub fn render_aggregator_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    let popup_area = centered_rect(40, 50, area);
    frame.render_widget(Clear, popup_area);

    let items: Vec<ListItem> = crate::data::aggregator::AggregatorKind::all()
        .iter()
        .enumerate()
        .map(|(i, agg)| {
            let is_selected = app.aggregator.selected.contains(agg);
            let is_active = i == app.aggregator.select_index;
            let checkbox = if is_selected { "[x]" } else { "[ ]" };
            let prefix = if is_active { "> " } else { "  " };
            let text = format!("{}{} {}", prefix, checkbox, agg.name());
            let mut style = Style::default().fg(T::FG);
            if is_selected {
                style = style.fg(T::GREEN);
            }
            if is_active {
                style = style.bg(T::BG2);
            }
            ListItem::new(text).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Select Aggregators (Space to toggle, Enter to apply) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}

pub fn render_partition_select_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    let popup_area = centered_rect(40, 60, area);
    frame.render_widget(Clear, popup_area);

    let s = app.stack.active();
    let items: Vec<ListItem> = s
        .dataframe
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let is_selected = app.partition.selected.contains(&col.name);
            let is_active = i == app.partition.select_index;
            let checkbox = if is_selected { "[x]" } else { "[ ]" };
            let prefix = if is_active { "> " } else { "  " };
            let text = format!("{}{} {}", prefix, checkbox, col.name);
            let mut style = Style::default().fg(T::FG);
            if is_selected {
                style = style.fg(T::GREEN);
            }
            if is_active {
                style = style.bg(T::BG2);
            }
            ListItem::new(text).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Partition By (Space to toggle, Enter to apply) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}
