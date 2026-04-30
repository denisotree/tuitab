use crate::app::App;
use crate::theme::EverforestTheme as T;
use crate::types::AppMode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::Paragraph;
use ratatui::Frame;

/// Render the bottom status bar with mode indicator, status message, and position info.
pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let sheet = app.stack.active();
    let df = &sheet.dataframe;

    // Selection count badge width (shown when rows are selected)
    let sel_count = df.selected_rows.len();

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(10), // mode indicator
            Constraint::Min(20),    // status message
            Constraint::Length(8),  // selection count (shown when > 0)
            Constraint::Length(26), // row/col position
        ])
        .split(area);

    // ── Mode indicator ─────────────────────────────────────────────────────────
    let mode_text = match app.mode {
        AppMode::Normal => " NORMAL ",
        AppMode::Searching => " SEARCH ",
        AppMode::SelectByRegex => " SELECT ",
        AppMode::ExpressionInput => " EXPR   ",
        AppMode::TypeSelect => " TYPE   ",
        AppMode::Editing => " EDIT   ",
        AppMode::Loading => " LOADING",
        AppMode::Calculating => " CALC   ",
        AppMode::AggregatorSelect => " AGG    ",
        AppMode::GPrefix => " G-MODE ",
        AppMode::Chart => " CHART  ",
        AppMode::Saving => " SAVING ",
        AppMode::ZPrefix => " Z-MODE ",
        AppMode::RenamingColumn => " RENAME ",
        AppMode::InsertingColumn => " INSERT ",
        AppMode::ConfirmQuit => " CONFIRM",
        AppMode::YPrefix => " COPY    ",
        AppMode::CopyFormatSelect => " COPY-FMT",
        AppMode::CurrencySelect => " CURRENCY",
        AppMode::PivotTableInput => " PIVOT   ",
        AppMode::PartitionSelect => " PART.   ",
        AppMode::Help => " HELP   ",
        AppMode::ChartAggSelect => " CHART  ",
        AppMode::JoinSelectSource
        | AppMode::JoinInputPath
        | AppMode::JoinSelectType
        | AppMode::JoinSelectLeftKeys
        | AppMode::JoinSelectRightKeys
        | AppMode::JoinOverviewSelect => " JOIN   ",
        AppMode::ColReplacingFind | AppMode::ColReplacingReplace => " REPLACE",
        AppMode::ColSplitting => " SPLIT  ",
        AppMode::ColumnMove => " COL-MOV",
        AppMode::BulkEditing => " BULK-ED",
        AppMode::SPrefix => " S-MODE ",
        AppMode::SelectRandomInput => " RAND-N ",
        AppMode::DedupTiebreakerSelect => " DEDUP  ",
    };

    frame.render_widget(
        Paragraph::new(mode_text).style(T::mode_indicator_style()),
        chunks[0],
    );

    // ── Status message + modified flag ─────────────────────────────────────────
    let modified = if df.modified { " [*]" } else { "" };
    let stack_depth = app.stack.depth();
    let depth_hint = if stack_depth > 1 {
        format!("[sheets: {}] ", stack_depth)
    } else {
        String::new()
    };
    let base_status = format!(" {}{}{}", depth_hint, app.status_message, modified);
    let final_status = if let Some((ref name, current, total)) = app.background_task {
        let spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        let spin = spinner_chars[(app.spinner_tick as usize) % spinner_chars.len()];
        if total > 0 {
            let pct = (current as f64 / total as f64 * 100.0) as usize;
            format!(
                "{} {} {}% ({}/{}) |{}",
                spin, name, pct, current, total, base_status
            )
        } else {
            format!("{} {}... |{}", spin, name, base_status)
        }
    } else if app.mode == AppMode::Calculating {
        let spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        let spin = spinner_chars[(app.spinner_tick as usize) % spinner_chars.len()];
        format!("{} {} |{}", spin, app.status_message, base_status)
    } else {
        base_status
    };

    frame.render_widget(
        Paragraph::new(final_status).style(T::status_bar_style()),
        chunks[1],
    );

    // ── Selection count badge ──────────────────────────────────────────────────
    if sel_count > 0 {
        let sel_text = format!(" {}● ", sel_count);
        frame.render_widget(
            Paragraph::new(Span::styled(
                sel_text,
                Style::default().fg(T::YELLOW).add_modifier(Modifier::BOLD),
            ))
            .style(T::status_bar_style()),
            chunks[2],
        );
    }

    // ── Cursor position ────────────────────────────────────────────────────────
    let row = sheet.table_state.selected().unwrap_or(0) + 1;
    let total_rows = df.visible_row_count();
    let col = sheet.cursor_col + 1;
    let total_cols = df.col_count();
    let pos_text = format!(" row {}/{} col {}/{} ", row, total_rows, col, total_cols);
    frame.render_widget(
        Paragraph::new(pos_text).style(T::status_bar_style()),
        chunks[3],
    );
}
