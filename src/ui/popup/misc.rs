use crate::theme::EverforestTheme as T;
use crate::ui::popup::centered_rect;
use ratatui::{
    layout::{Alignment, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph},
    Frame,
};

pub fn render_confirm_popup(frame: &mut Frame, message: &str, area: Rect) {
    let popup_area = centered_rect(40, 20, area);
    frame.render_widget(Clear, popup_area);

    let block = Block::default()
        .title(" Confirm ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(T::RED));

    let lines = vec![
        Line::from(""),
        Line::from(vec![Span::styled(message, Style::default().fg(T::YELLOW))]),
    ];

    let paragraph = Paragraph::new(lines)
        .alignment(Alignment::Center)
        .block(block);
    frame.render_widget(paragraph, popup_area);
}

pub fn render_chart_agg_popup(frame: &mut Frame, app: &crate::app::App, area: Rect) {
    let popup_area = centered_rect(35, 50, area);
    frame.render_widget(Clear, popup_area);

    let items: Vec<ListItem> = crate::types::ChartAgg::all()
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
            ListItem::new(Line::from(Span::styled(
                format!("{}{}", prefix, agg.label()),
                style,
            )))
        })
        .collect();

    let list = List::new(items).block(
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

    let items: Vec<ListItem> = options
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
            ListItem::new(Line::from(Span::styled(
                format!("{}{}", prefix, opt),
                style,
            )))
        })
        .collect();

    let list = List::new(items).block(
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
