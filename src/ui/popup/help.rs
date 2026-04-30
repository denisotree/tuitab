use crate::theme::EverforestTheme as T;
use crate::ui::popup::centered_rect;
use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
    Frame,
};

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
        Line::from("  Shift+S r         Select N random rows"),
        Line::from("  Shift+S d         Select all duplicate rows"),
        Line::from("  Shift+S D         Smart dedup (asks tiebreaker if pinned cols)"),
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
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, popup_area);
}
