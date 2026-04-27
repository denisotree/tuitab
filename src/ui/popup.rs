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
        Line::from("  y → c             Copy current cell (direct)"),
        Line::from("  y → r             Sel. rows or current row → format popup"),
        Line::from("  y → z             Sel. rows in current col → format popup"),
        Line::from("                    (no selection → copies current cell)"),
        Line::from("  y → Z             Entire current column → format popup"),
        Line::from("  y → R             Entire table → format popup"),
        Line::from("  (yr, yR respect column selection via zs)"),
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
        Line::from("  R                 Reload file from disk"),
        Line::from("  Shift+U          Undo"),
        Line::from("  Ctrl+R            Redo"),
        Line::from("  J                 JOIN with another table"),
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
            let is_active = i == app.type_select.index;
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
            let is_active = i == app.type_select.currency_index;
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

/// JOIN overview multi-select popup — shown when J is pressed on a directory/DB/xlsx overview.
pub fn render_join_overview_select_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    let popup_area = centered_rect(55, 65, area);
    frame.render_widget(Clear, popup_area);

    let items = &app.join.context_items;
    let cursor = app.join.overview_cursor;
    let selected = &app.join.overview_selected;

    let n_selected = selected.len();
    let list_items: Vec<ratatui::widgets::ListItem> = items
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
            ratatui::widgets::ListItem::new(text).style(style)
        })
        .collect();

    let title = format!(
        " JOIN: select items — {} selected (Space=toggle, Enter=confirm) ",
        n_selected
    );
    let list = ratatui::widgets::List::new(list_items).block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}

/// JOIN step 1 — source selection popup
pub fn render_join_source_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    let popup_area = centered_rect(50, 60, area);
    frame.render_widget(Clear, popup_area);

    let ctx_items = &app.join.context_items;
    let ctx_count = ctx_items.len();
    let other_titles = app.stack.sheet_titles_except_active();
    let mut items: Vec<ratatui::widgets::ListItem> = Vec::new();

    // ── [Browse file...] ──
    let browse_active = app.join.source_index == 0;
    let prefix = if browse_active { "▶ " } else { "  " };
    let style = if browse_active {
        Style::default().fg(T::YELLOW).bg(T::BG2)
    } else {
        Style::default().fg(T::FG)
    };
    items.push(ratatui::widgets::ListItem::new(format!("{}[Browse file...]", prefix)).style(style));

    // ── Context items (sibling tables / files / sheets) ──
    for (i, ctx) in ctx_items.iter().enumerate() {
        let idx = i + 1;
        let is_active = idx == app.join.source_index;
        let pfx = if is_active { "▶ " } else { "  " };
        let style = if is_active {
            Style::default().fg(T::YELLOW).bg(T::BG2)
        } else {
            Style::default().fg(T::GREEN)
        };
        items.push(
            ratatui::widgets::ListItem::new(format!("{}↳ {}", pfx, ctx.label())).style(style),
        );
    }

    // ── Stack sheets ──
    for (i, title) in other_titles.iter().enumerate() {
        let idx = i + 1 + ctx_count;
        let is_active = idx == app.join.source_index;
        let pfx = if is_active { "▶ " } else { "  " };
        let style = if is_active {
            Style::default().fg(T::YELLOW).bg(T::BG2)
        } else {
            Style::default().fg(T::FG)
        };
        items.push(ratatui::widgets::ListItem::new(format!("{}{}", pfx, title)).style(style));
    }

    let list = ratatui::widgets::List::new(items).block(
        Block::default()
            .title(" JOIN: Select source (↑↓, Enter) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}

/// JOIN step 2 — join type selection popup
pub fn render_join_type_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    use crate::data::join::JoinType;
    let popup_area = centered_rect(40, 35, area);
    frame.render_widget(Clear, popup_area);

    let items: Vec<ratatui::widgets::ListItem> = JoinType::all()
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
            ratatui::widgets::ListItem::new(format!("{}{}", prefix, jt.label())).style(style)
        })
        .collect();

    let list = ratatui::widgets::List::new(items).block(
        Block::default()
            .title(" JOIN: Select join type (↑↓, Enter) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE)),
    );
    frame.render_widget(list, popup_area);
}

/// JOIN steps 3 & 4 — key column selection (shared, parameterised by title and columns)
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

    let items: Vec<ratatui::widgets::ListItem> = columns
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
            ratatui::widgets::ListItem::new(text).style(style)
        })
        .collect();

    let list = ratatui::widgets::List::new(items).block(
        Block::default()
            .title(format!(" {} (Space toggle, Enter apply) ", title))
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
            let is_active = i == app.chart.agg_index;
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

pub fn render_copy_format_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    use crate::types::CopyPending;
    let popup_area = centered_rect(44, 40, area);
    frame.render_widget(Clear, popup_area);

    let pending = match app.copy.pending {
        Some(p) => p,
        None => return,
    };

    let row_options: &[&str] = &[
        "TSV (with header)",
        "CSV (with header)",
        "JSON (array of objects)",
        "Markdown table",
    ];
    let col_options: &[&str] = &[
        "Newline-separated",
        "Comma-separated",
        "Comma-separated, single-quoted",
    ];

    let (title, options) = match pending {
        CopyPending::SmartRows => {
            let df = &app.stack.active().dataframe;
            let n_sel = df.selected_rows.len();
            let n_col_sel = df.columns.iter().filter(|c| c.selected).count();
            let col_note = if n_col_sel > 0 {
                format!(", {} cols", n_col_sel)
            } else {
                String::new()
            };
            let t = if n_sel > 0 {
                format!(" Copy {} Selected Rows{} ", n_sel, col_note)
            } else {
                format!(" Copy Current Row{} ", col_note)
            };
            (t, row_options)
        }
        CopyPending::SmartColumn => {
            let count = app.stack.active().dataframe.selected_rows.len();
            (
                format!(" Copy Column Values ({} selected rows) ", count),
                col_options,
            )
        }
        CopyPending::WholeColumn => {
            let col = app.stack.active().cursor_col;
            let name = &app.stack.active().dataframe.columns[col].name;
            (format!(" Copy Column \"{}\" ", name), col_options)
        }
        CopyPending::WholeTable => {
            let df = &app.stack.active().dataframe;
            let n_col_sel = df.columns.iter().filter(|c| c.selected).count();
            let col_note = if n_col_sel > 0 {
                format!(" ({} cols selected)", n_col_sel)
            } else {
                String::new()
            };
            (format!(" Copy Table{} ", col_note), row_options)
        }
    };

    let items: Vec<ratatui::widgets::ListItem> = options
        .iter()
        .enumerate()
        .map(|(i, opt)| {
            let is_active = i == app.copy.format_index;
            let prefix = if is_active { "▶ " } else { "  " };
            let style = if is_active {
                Style::default().fg(T::YELLOW).bg(T::BG2)
            } else {
                Style::default().fg(T::FG)
            };
            ratatui::widgets::ListItem::new(Line::from(Span::styled(
                format!("{}{}", prefix, opt),
                style,
            )))
        })
        .collect();

    let list = ratatui::widgets::List::new(items).block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(T::PURPLE))
            .title_bottom(Line::from(Span::styled(
                " ↑↓ navigate · Enter apply · Esc cancel ",
                Style::default().fg(T::GREY1),
            ))),
    );

    frame.render_widget(list, popup_area);
}
