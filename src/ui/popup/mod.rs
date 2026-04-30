use ratatui::layout::{Constraint, Direction, Layout, Rect};

mod aggregator;
mod dedup_tiebreaker;
mod help;
mod input;
mod join;
mod misc;
mod type_select;

pub use aggregator::{render_aggregator_popup, render_partition_select_popup};
pub use dedup_tiebreaker::render_dedup_tiebreaker_popup;
pub use help::render_help_popup;
pub use input::render_input_popup;
pub use join::{
    render_join_key_popup, render_join_overview_select_popup, render_join_source_popup,
    render_join_type_popup,
};
pub use misc::{render_chart_agg_popup, render_confirm_popup, render_copy_format_popup};
pub use type_select::{render_currency_popup, render_type_select_popup};

pub(super) fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
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
