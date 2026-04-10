use crate::theme::EverforestTheme as T;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph},
    Frame,
};

/// Render a universal input popup on top of the screen.
pub fn render_input_popup(
    frame: &mut Frame,
    title: &str,
    input: &crate::ui::text_input::TextInput,
    error_msg: Option<&str>,
    area: Rect,
) {
    let popup_area = centered_rect(60, 20, area);

    // Clear the background to avoid text overlapping from the table below
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
        lines.push(Line::from("")); // empty line
        lines.push(Line::from(Span::styled(err, Style::default().fg(T::RED))));
    }

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, popup_area);

    let prefix_len = 2; // "> "
    let text_len = input.cursor_pos();
    frame.set_cursor_position((popup_area.x + 1 + prefix_len + text_len, popup_area.y + 1));
}

/// Helper to center a rect
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

pub fn render_aggregator_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    let popup_area = centered_rect(40, 50, area);
    frame.render_widget(Clear, popup_area);

    let items: Vec<ratatui::widgets::ListItem> = crate::data::aggregator::AggregatorKind::all()
        .iter()
        .enumerate()
        .map(|(i, agg)| {
            let is_selected = app.agg_selected.contains(agg);
            let is_active = i == app.agg_select_index;

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

            ratatui::widgets::ListItem::new(text).style(style)
        })
        .collect();

    let list = ratatui::widgets::List::new(items).block(
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
    let items: Vec<ratatui::widgets::ListItem> = s
        .dataframe
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let is_selected = app.partition_selected.contains(&col.name);
            let is_active = i == app.partition_select_index;

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

            ratatui::widgets::ListItem::new(text).style(style)
        })
        .collect();

    let list = ratatui::widgets::List::new(items).block(
        Block::default()
            .title(" Partition By (Space to toggle, Enter to apply) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );

    frame.render_widget(list, popup_area);
}

pub fn render_confirm_popup(frame: &mut Frame, message: &str, area: Rect) {
    let popup_area = centered_rect(40, 20, area);
    frame.render_widget(Clear, popup_area);

    let block = Block::default()
        .title(" Confirm ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(T::RED));

    // Create styled message with instructions
    let lines = vec![
        Line::from(""), // Vertical padding
        Line::from(vec![Span::styled(message, Style::default().fg(T::YELLOW))]),
    ];

    let paragraph = Paragraph::new(lines)
        .alignment(ratatui::layout::Alignment::Center)
        .block(block);

    frame.render_widget(paragraph, popup_area);
}

/// Render the help overlay with keybinding reference (? key)
pub fn render_help_popup(frame: &mut Frame, area: Rect) {
    let popup_area = centered_rect(70, 85, area);
    frame.render_widget(Clear, popup_area);

    let block = Block::default()
        .title(" Help — press Esc or ? to close ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(T::AQUA));

    let help_text = vec![
        Line::from(Span::styled(
            "── Navigation ─────────────────────────",
            Style::default().fg(T::GREEN),
        )),
        Line::from("  hjkl / arrows     Move cursor"),
        Line::from("  gg / G            Go to top / bottom"),
        Line::from("  Ctrl+F/B          Page down / up"),
        Line::from("  Backspace / q     Pop sheet / quit"),
        Line::from(""),
        Line::from(Span::styled(
            "── Search & Selection ──────────────────",
            Style::default().fg(T::GREEN),
        )),
        Line::from("  /                 Search (regex filter)"),
        Line::from("  |                 Select rows by regex"),
        Line::from("  | !=expr          Select rows by Expression"),
        Line::from("  s / u             Select / unselect row"),
        Line::from("  gs / gu           Select all / unselect all"),
        Line::from(""),
        Line::from(Span::styled(
            "── Clipboard ───────────────────────────",
            Style::default().fg(T::GREEN),
        )),
        Line::from("  y → y             Copy current row (TSV)"),
        Line::from("  y → c             Copy current cell"),
        Line::from("  y → l             Copy current column"),
        Line::from("  y → s             Copy selected rows"),
        Line::from("  p                 Paste rows"),
        Line::from(""),
        Line::from(Span::styled(
            "── Column Operations (z prefix) ────────",
            Style::default().fg(T::GREEN),
        )),
        Line::from("  ze                Rename column"),
        Line::from("  zd                Delete column"),
        Line::from("  zi                Insert empty column"),
        Line::from("  zs / zu           Select / unselect column (mark with *)"),
        Line::from("  zf                Column % of Total"),
        Line::from("  zF                Partitioned Column %"),
        Line::from("  z←/→             Move column left/right"),
        Line::from("  z. / z,          Increase / decrease precision"),
        Line::from("  !                 Pin / unpin column"),
        Line::from("  _                 Fit column width"),
        Line::from(""),
        Line::from(Span::styled(
            "── Type Assignment (t) ─────────────────",
            Style::default().fg(T::GREEN),
        )),
        Line::from("  t                Open column type menu"),
        Line::from("  tc               Currency (popup)"),
        Line::from(""),
        Line::from(Span::styled(
            "── Derived Sheets & Analytics ──────────",
            Style::default().fg(T::GREEN),
        )),
        Line::from("  Shift+F           Frequency table"),
        Line::from("  gF               Multi-col frequency (pinned)"),
        Line::from("  gD               Deduplicate by pinned cols"),
        Line::from("  Enter            Transpose row / drill-down"),
        Line::from("  I                Describe sheet (statistics)"),
        Line::from("  =                Add computed column"),
        Line::from("  v                View chart"),
        Line::from(""),
        Line::from(Span::styled(
            "── File ────────────────────────────────",
            Style::default().fg(T::GREEN),
        )),
        Line::from("  Ctrl+S            Save / export"),
        Line::from("  Shift+U          Undo"),
        Line::from("  ?                This help"),
    ];

    let paragraph = Paragraph::new(help_text)
        .block(block)
        .wrap(ratatui::widgets::Wrap { trim: false });

    frame.render_widget(paragraph, popup_area);
}

/// Render the type selection popup (t key)
pub fn render_type_select_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    use crate::types::ColumnType;
    let popup_area = centered_rect(40, 50, area);
    frame.render_widget(Clear, popup_area);

    let items: Vec<ratatui::widgets::ListItem> = ColumnType::all()
        .iter()
        .enumerate()
        .map(|(i, ct)| {
            let is_active = i == app.type_select_index;
            let prefix = if is_active { "▶ " } else { "  " };
            let text = format!("{}{}", prefix, ct.display_name());
            let style = if is_active {
                Style::default().fg(T::YELLOW).bg(T::BG2)
            } else {
                Style::default().fg(T::FG)
            };
            ratatui::widgets::ListItem::new(text).style(style)
        })
        .collect();

    let list = ratatui::widgets::List::new(items).block(
        Block::default()
            .title(" Select Column Type (↑↓ navigate, Enter apply, Esc cancel) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );

    frame.render_widget(list, popup_area);
}

/// Render the currency selection popup
pub fn render_currency_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    use crate::types::CurrencyKind;
    let popup_area = centered_rect(40, 40, area);
    frame.render_widget(Clear, popup_area);

    let items: Vec<ratatui::widgets::ListItem> = CurrencyKind::all()
        .iter()
        .enumerate()
        .map(|(i, ck)| {
            let is_active = i == app.currency_select_index;
            let prefix = if is_active { "▶ " } else { "  " };
            let text = format!("{}{}", prefix, ck.display_name());
            let style = if is_active {
                Style::default().fg(T::YELLOW).bg(T::BG2)
            } else {
                Style::default().fg(T::FG)
            };
            ratatui::widgets::ListItem::new(text).style(style)
        })
        .collect();

    let list = ratatui::widgets::List::new(items).block(
        Block::default()
            .title(" Select Currency (↑↓ navigate, Enter apply, Esc back) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );

    frame.render_widget(list, popup_area);
}

pub fn render_chart_agg_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    let popup_area = centered_rect(35, 50, area);
    frame.render_widget(ratatui::widgets::Clear, popup_area);

    let items: Vec<ratatui::widgets::ListItem> = crate::types::ChartAgg::all()
        .iter()
        .enumerate()
        .map(|(i, agg)| {
            let is_active = i == app.chart_agg_index;
            let prefix = if is_active { "> " } else { "  " };
            let style = if is_active {
                Style::default().fg(T::YELLOW)
            } else {
                Style::default().fg(T::FG)
            };
            ratatui::widgets::ListItem::new(Line::from(Span::styled(
                format!("{}{}", prefix, agg.label()),
                style,
            )))
        })
        .collect();

    let list = ratatui::widgets::List::new(items).block(
        Block::default()
            .title(" Select aggregation (↑↓, Enter) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );

    frame.render_widget(list, popup_area);
}
