//! Rendering layer for tuitab, built on top of [`ratatui`].
//!
//! The top-level entry point is [`render`], which dispatches to sub-views based on
//! the current [`crate::types::AppMode`]:
//!
//! | Sub-module | Rendered when |
//! |---|---|
//! | [`table_view`] | All normal and overlay modes — the main data grid |
//! | [`status_bar`] | Always rendered below the table |
//! | [`charts`] | [`crate::types::AppMode::Chart`] — full-screen chart view |
//! | [`search_bar`] | [`crate::types::AppMode::Searching`] — `/` text search overlay |
//! | [`expr_bar`] | [`crate::types::AppMode::ExpressionInput`] — `=` expression overlay |
//! | [`edit_bar`] | [`crate::types::AppMode::Editing`] — cell edit overlay |
//! | [`select_regex_bar`] | [`crate::types::AppMode::SelectByRegex`] — regex row selection |
//! | [`popup`] | Modal dialogs: aggregator, type, currency, pivot, help, confirm-quit |
//! | [`text_input`] | Reusable cursor-aware text field state (not a widget itself) |

use crate::app::App;
use crate::theme::EverforestTheme as T;
use crate::types::AppMode;
use ratatui::layout::{Alignment, Constraint, Direction, Layout};
use ratatui::style::Style;
use ratatui::widgets::Paragraph;
use ratatui::Frame;

pub mod charts;
pub mod edit_bar;
pub mod expr_bar;
pub mod search_bar;
pub mod select_regex_bar;
pub mod status_bar;
pub mod table_view;
pub mod text_input;

pub mod popup;

/// Top-level render function: dispatches to the appropriate sub-views based on app mode.
/// Since Frequency table is now a regular Sheet on the stack, all modes route to table_view.
pub fn render(frame: &mut Frame, app: &mut App) {
    match app.mode {
        AppMode::Normal
        | AppMode::Searching
        | AppMode::SelectByRegex
        | AppMode::ExpressionInput
        | AppMode::TypeSelect
        | AppMode::Editing
        | AppMode::AggregatorSelect
        | AppMode::Saving
        | AppMode::GPrefix
        | AppMode::ZPrefix
        | AppMode::RenamingColumn
        | AppMode::InsertingColumn
        | AppMode::RowForm
        | AppMode::ColReplacingFind
        | AppMode::ColReplacingReplace
        | AppMode::ColSplitting
        | AppMode::ColumnMove
        | AppMode::BulkEditing
        | AppMode::Calculating
        | AppMode::ConfirmQuit
        | AppMode::YPrefix
        | AppMode::CopyFormatSelect
        | AppMode::CurrencySelect
        | AppMode::PivotTableInput
        | AppMode::PartitionSelect
        | AppMode::WindowFnSelect
        | AppMode::WindowDirSelect
        | AppMode::WindowOrderSelect
        | AppMode::Help
        | AppMode::ChartAggSelect
        | AppMode::JoinSelectSource
        | AppMode::JoinInputPath
        | AppMode::JoinSelectType
        | AppMode::JoinSelectLeftKeys
        | AppMode::JoinSelectRightKeys
        | AppMode::JoinOverviewSelect
        | AppMode::SPrefix
        | AppMode::SelectRandomInput
        | AppMode::DedupTiebreakerSelect
        | AppMode::SaveShapeSelect
        | AppMode::SqlConfirm
        | AppMode::TableNameInput
        | AppMode::OpenAsSelect
        | AppMode::DocSearching
        | AppMode::PathInput
        | AppMode::QueryInput => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(3),    // table
                    Constraint::Length(1), // status bar
                ])
                .split(frame.area());

            table_view::render(frame, app, chunks[0]);
            status_bar::render(frame, app, chunks[1]);

            if app.mode == AppMode::Searching || app.mode == AppMode::DocSearching {
                search_bar::render(frame, app);
            }
            if app.mode == AppMode::SelectByRegex {
                select_regex_bar::render(frame, app);
            }
            if app.mode == AppMode::ExpressionInput {
                expr_bar::render(frame, app);
            }
            if app.mode == AppMode::Editing {
                edit_bar::render(frame, app);
            }
            if app.mode == AppMode::Saving {
                popup::render_input_popup(
                    frame,
                    "Save & Export (Enter to confirm, Esc to cancel)",
                    &app.save.input,
                    app.save.error.as_deref(),
                    frame.area(),
                );
            }
            if app.mode == AppMode::RowForm {
                popup::render_row_form_popup(
                    frame,
                    &app.row_form,
                    &app.stack.active().dataframe.columns,
                    frame.area(),
                );
            }
            if app.mode == AppMode::RenamingColumn {
                popup::render_input_popup(
                    frame,
                    "Rename Column (Enter to confirm, Esc to cancel)",
                    &app.stack.active().rename_column_input,
                    None,
                    frame.area(),
                );
            }
            if app.mode == AppMode::InsertingColumn {
                popup::render_input_popup(
                    frame,
                    "Insert Empty Column (Enter to confirm, Esc to cancel)",
                    &app.stack.active().insert_column_input,
                    None,
                    frame.area(),
                );
            }
            if app.mode == AppMode::BulkEditing {
                let s = app.stack.active();
                let count = s.dataframe.selected_rows.len();
                let col_name = s
                    .dataframe
                    .columns
                    .get(s.edit_col)
                    .map(|c| c.name.as_str())
                    .unwrap_or("?");
                let title = format!(
                    "Bulk edit '{}' for {} rows — Enter to apply, Esc to cancel",
                    col_name, count
                );
                popup::render_input_popup(frame, &title, &s.edit_input, None, frame.area());
            }
            if app.mode == AppMode::ColReplacingFind {
                let title = if app.col_op_literal {
                    "Find (literal) — Enter to continue, Esc to cancel"
                } else {
                    "Find (regexp) — Enter to continue, Esc to cancel"
                };
                popup::render_input_popup(
                    frame,
                    title,
                    &app.stack.active().col_find_input,
                    None,
                    frame.area(),
                );
            }
            if app.mode == AppMode::ColReplacingReplace {
                let title = if app.col_op_literal {
                    "Replace with — Enter to apply, Esc to cancel"
                } else {
                    "Replace with (regexp) — Enter to apply, Esc to cancel"
                };
                popup::render_input_popup(
                    frame,
                    title,
                    &app.stack.active().col_replace_input,
                    None,
                    frame.area(),
                );
            }
            if app.mode == AppMode::ColSplitting {
                popup::render_input_popup(
                    frame,
                    "Split by delimiter — Enter to apply, Esc to cancel",
                    &app.stack.active().col_split_input,
                    None,
                    frame.area(),
                );
            }
            if app.mode == AppMode::AggregatorSelect {
                popup::render_aggregator_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::WindowFnSelect {
                popup::render_window_fn_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::WindowOrderSelect {
                popup::render_window_order_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::WindowDirSelect {
                popup::render_window_dir_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::PartitionSelect {
                popup::render_partition_select_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::ConfirmQuit {
                popup::render_confirm_popup(frame, "Unsaved changes. Quit? (y/n)", frame.area());
            }
            if app.mode == AppMode::Help {
                popup::render_help_popup(frame, frame.area());
            }

            if app.mode == AppMode::TypeSelect {
                popup::render_type_select_popup(frame, app, frame.area());
            }

            if app.mode == AppMode::CurrencySelect {
                popup::render_currency_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::PivotTableInput {
                popup::render_input_popup(
                    frame,
                    "Pivot Table: aggregation formula (Tab=autocomplete, ↑↓=history)",
                    &app.stack.active().pivot_input,
                    None,
                    frame.area(),
                );
            }
            if app.mode == AppMode::ChartAggSelect {
                popup::render_chart_agg_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::JoinOverviewSelect {
                popup::render_join_overview_select_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::JoinSelectSource {
                popup::render_join_source_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::JoinInputPath {
                use crate::ui::search_bar;
                search_bar::render_join_path_bar(frame, app);
            }
            if app.mode == AppMode::JoinSelectType {
                popup::render_join_type_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::JoinSelectLeftKeys {
                popup::render_join_key_popup(
                    frame,
                    "LEFT key columns (Space toggle, a all, Enter next)",
                    &app.stack.active().dataframe.columns,
                    &app.join.left_keys,
                    &[],
                    app.join.left_key_index,
                    frame.area(),
                );
            }
            if app.mode == AppMode::JoinSelectRightKeys {
                if let Some(ref right) = app.join.other_df {
                    let left = &app.stack.active().dataframe;
                    let conflicts: Vec<String> = app
                        .join
                        .left_keys
                        .iter()
                        .zip(&app.join.right_keys)
                        .filter(|(lk, rk)| {
                            crate::data::join::key_type_mismatch(left, lk, right, rk).is_some()
                        })
                        .map(|(_, rk)| rk.clone())
                        .collect();
                    popup::render_join_key_popup(
                        frame,
                        "RIGHT key columns (Space toggle, Enter apply)",
                        &right.columns,
                        &app.join.right_keys,
                        &conflicts,
                        app.join.right_key_index,
                        frame.area(),
                    );
                }
            }
            if app.mode == AppMode::CopyFormatSelect {
                popup::render_copy_format_popup(frame, app, frame.area());
            }
            if app.mode == AppMode::QueryInput {
                popup::render_input_popup(
                    frame,
                    "jq query (e.g. .[] | select(.ok)) — Enter to run, Esc to cancel",
                    &app.stack.active().query_input,
                    None,
                    frame.area(),
                );
            }
            if app.mode == AppMode::PathInput {
                popup::render_input_popup(
                    frame,
                    "Go to path (e.g. servers[1].host) — Enter to jump, Esc to cancel",
                    &app.stack.active().path_input,
                    None,
                    frame.area(),
                );
            }
            if app.mode == AppMode::TableNameInput {
                popup::render_input_popup(
                    frame,
                    "Table name (Enter to confirm, Esc to cancel)",
                    &app.save.table_input,
                    app.save.error.as_deref(),
                    frame.area(),
                );
            }
            if app.mode == AppMode::SqlConfirm {
                popup::render_sql_confirm(frame, app, frame.area());
            }
            if app.mode == AppMode::SaveShapeSelect {
                let items: Vec<(String, String)> = app
                    .save
                    .shapes
                    .iter()
                    .map(|s| (s.label().to_string(), s.hint().to_string()))
                    .collect();
                popup::render_choice_popup(
                    frame,
                    "Save this table as",
                    &items,
                    app.save.shape_index,
                    frame.area(),
                );
            }
            if app.mode == AppMode::OpenAsSelect {
                let items: Vec<(String, String)> = crate::app::OPEN_AS_FORMATS
                    .iter()
                    .map(|f| (f.name().to_string(), String::new()))
                    .collect();
                popup::render_choice_popup(
                    frame,
                    "Open as",
                    &items,
                    app.save.shape_index,
                    frame.area(),
                );
            }
            if app.mode == AppMode::SelectRandomInput {
                popup::render_input_popup(
                    frame,
                    "Random select: enter N rows (Enter to apply, Esc to cancel)",
                    &app.stack.active().select_count_input,
                    None,
                    frame.area(),
                );
            }
            if app.mode == AppMode::DedupTiebreakerSelect {
                popup::render_dedup_tiebreaker_popup(frame, app, frame.area());
            }
        }
        AppMode::Loading => {
            let loading_text = "⏳ Loading file...";
            let spinner = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
            let s_idx = (app.spinner_tick as usize) % spinner.len();
            let loading = Paragraph::new(format!("{} {}", spinner[s_idx], loading_text))
                .alignment(Alignment::Center)
                .style(Style::default().fg(T::YELLOW).bg(T::BG0));
            frame.render_widget(loading, frame.area());
        }
        AppMode::Chart => {
            charts::render(frame, app);
        }
    }
}
