use crate::theme::EverforestTheme as T;
use crate::ui::popup::centered_rect;
use ratatui::{
    layout::Rect,
    style::Style,
    widgets::{Block, Borders, Clear, List, ListItem},
    Frame,
};

pub fn render_join_overview_select_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    let popup_area = centered_rect(55, 65, area);
    frame.render_widget(Clear, popup_area);

    let items = &app.join.context_items;
    let cursor = app.join.overview_cursor;
    let selected = &app.join.overview_selected;
    let n_selected = selected.len();

    let list_items: Vec<ListItem> = items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let is_cursor = i == cursor;
            let is_sel = selected.contains(&i);
            let check = if is_sel { "[✓]" } else { "[ ]" };
            let arrow = if is_cursor { "▶ " } else { "  " };
            let text = format!("{}{} {}", arrow, check, item.label());
            let style = if is_cursor && is_sel {
                Style::default().fg(T::YELLOW).bg(T::BG2)
            } else if is_cursor {
                Style::default().fg(T::FG).bg(T::BG2)
            } else if is_sel {
                Style::default().fg(T::GREEN)
            } else {
                Style::default().fg(T::FG)
            };
            ListItem::new(text).style(style)
        })
        .collect();

    let title = format!(
        " JOIN: select items — {} selected (Space=toggle, Enter=confirm) ",
        n_selected
    );
    let list = List::new(list_items).block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}

pub fn render_join_source_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    let popup_area = centered_rect(50, 60, area);
    frame.render_widget(Clear, popup_area);

    let ctx_items = &app.join.context_items;
    let ctx_count = ctx_items.len();
    let other_titles = app.stack.sheet_titles_except_active();
    let mut items: Vec<ListItem> = Vec::new();

    let browse_active = app.join.source_index == 0;
    let prefix = if browse_active { "▶ " } else { "  " };
    let style = if browse_active {
        Style::default().fg(T::YELLOW).bg(T::BG2)
    } else {
        Style::default().fg(T::FG)
    };
    items.push(ListItem::new(format!("{}[Browse file...]", prefix)).style(style));

    for (i, ctx) in ctx_items.iter().enumerate() {
        let idx = i + 1;
        let is_active = idx == app.join.source_index;
        let pfx = if is_active { "▶ " } else { "  " };
        let style = if is_active {
            Style::default().fg(T::YELLOW).bg(T::BG2)
        } else {
            Style::default().fg(T::GREEN)
        };
        items.push(ListItem::new(format!("{}↳ {}", pfx, ctx.label())).style(style));
    }

    for (i, title) in other_titles.iter().enumerate() {
        let idx = i + 1 + ctx_count;
        let is_active = idx == app.join.source_index;
        let pfx = if is_active { "▶ " } else { "  " };
        let style = if is_active {
            Style::default().fg(T::YELLOW).bg(T::BG2)
        } else {
            Style::default().fg(T::FG)
        };
        items.push(ListItem::new(format!("{}{}", pfx, title)).style(style));
    }

    let list = List::new(items).block(
        Block::default()
            .title(" JOIN: Select source (↑↓, Enter) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}

pub fn render_join_type_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    use crate::data::join::JoinType;
    let popup_area = centered_rect(40, 35, area);
    frame.render_widget(Clear, popup_area);

    let items: Vec<ListItem> = JoinType::all()
        .iter()
        .enumerate()
        .map(|(i, jt)| {
            let is_active = i == app.join.type_index;
            let prefix = if is_active { "▶ " } else { "  " };
            let style = if is_active {
                Style::default().fg(T::YELLOW).bg(T::BG2)
            } else {
                Style::default().fg(T::FG)
            };
            ListItem::new(format!("{}{}", prefix, jt.label())).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" JOIN: Select join type (↑↓, Enter) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}

pub fn render_join_key_popup(
    frame: &mut Frame,
    title: &str,
    columns: &[String],
    selected_keys: &[String],
    cursor_index: usize,
    area: Rect,
) {
    let popup_area = centered_rect(45, 60, area);
    frame.render_widget(Clear, popup_area);

    let items: Vec<ListItem> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let is_selected = selected_keys.contains(col);
            let is_active = i == cursor_index;
            let order = selected_keys
                .iter()
                .position(|k| k == col)
                .map(|p| (p + 1).to_string())
                .unwrap_or_default();
            let checkbox = if is_selected {
                format!("[{}]", order)
            } else {
                "[ ]".to_string()
            };
            let prefix = if is_active { "> " } else { "  " };
            let text = format!("{}{} {}", prefix, checkbox, col);
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
            .title(format!(" {} (Space toggle, Enter apply) ", title))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}
