use crate::app::App;
use crate::theme::EverforestTheme as T;
use ratatui::layout::{Constraint, Margin, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Cell, HighlightSpacing, Row, Scrollbar, ScrollbarOrientation, Table,
};
use ratatui::Frame;
use unicode_width::UnicodeWidthStr;

/// Render the active sheet's data table with header, rows, selection marks, and scrollbar.
pub fn render(frame: &mut Frame, app: &mut App, area: Rect) {
    let stack_depth = app.stack.depth();
    let sheet = app.stack.active_mut();
    let df = &mut sheet.dataframe;

    let max_width = area.width.saturating_sub(2); // Border takes 1 cell on each side

    // ── Calculate visible columns (Horizontal Scrolling) ─────────────────────
    let cursor_col = sheet.cursor_col;

    let pinned_cols: Vec<usize> = df
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.pinned)
        .map(|(i, _)| i)
        .collect();
    let unpinned_cols: Vec<usize> = df
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.pinned)
        .map(|(i, _)| i)
        .collect();

    let mut pinned_width: u16 = 0;
    let mut visible_pinned: Vec<usize> = Vec::new();
    for &i in &pinned_cols {
        let w = df.columns[i].width + 1;
        if pinned_width + w > max_width {
            break;
        }
        pinned_width += w;
        visible_pinned.push(i);
    }

    let remaining_width = max_width.saturating_sub(pinned_width);

    let mut left_col = sheet.left_col;
    if !unpinned_cols.contains(&left_col) {
        left_col = unpinned_cols.first().copied().unwrap_or(0);
    }

    if unpinned_cols.contains(&cursor_col) {
        if let Some(pos) = unpinned_cols.iter().position(|&x| x == cursor_col) {
            let left_pos = unpinned_cols
                .iter()
                .position(|&x| x == left_col)
                .unwrap_or(0);

            if pos < left_pos {
                left_col = cursor_col;
            } else {
                loop {
                    let mut w = 0;
                    let current_left_pos = unpinned_cols
                        .iter()
                        .position(|&x| x == left_col)
                        .unwrap_or(0);
                    for &col_idx in unpinned_cols.iter().take(pos + 1).skip(current_left_pos) {
                        w += df.columns[col_idx].width + 1;
                    }
                    if w <= remaining_width || left_col == cursor_col {
                        break;
                    }
                    if let Some(next_pos) = unpinned_cols
                        .iter()
                        .position(|&x| x == left_col)
                        .map(|p| p + 1)
                    {
                        if next_pos < unpinned_cols.len() {
                            left_col = unpinned_cols[next_pos];
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    sheet.left_col = left_col;

    let mut visible_unpinned: Vec<usize> = Vec::new();
    let mut widths_override: Vec<u16> = Vec::new();

    for &i in &visible_pinned {
        widths_override.push(df.columns[i].width + 1);
    }

    let insert_border = !visible_pinned.is_empty() && !unpinned_cols.is_empty();
    let mut border_added = false;
    let mut current_w = 0;

    if insert_border {
        let border_w = 1;
        if remaining_width > border_w {
            current_w += border_w;
            border_added = true;
        }
    }

    if let Some(start_idx) = unpinned_cols.iter().position(|&x| x == left_col) {
        let mut idx = start_idx;
        while idx < unpinned_cols.len() {
            let col_idx = unpinned_cols[idx];
            let col_w = df.columns[col_idx].width + 1;
            if current_w + col_w > remaining_width {
                let diff = remaining_width.saturating_sub(current_w);
                if diff > 0 {
                    if border_added && visible_unpinned.is_empty() {
                        // The very first unpinned column; add border first
                        widths_override.push(1);
                    }
                    widths_override.push(diff);
                    visible_unpinned.push(col_idx);
                } else if idx == start_idx {
                    if border_added && visible_unpinned.is_empty() {
                        widths_override.push(1);
                    }
                    widths_override.push(remaining_width);
                    visible_unpinned.push(col_idx);
                }
                break;
            }
            if border_added && visible_unpinned.is_empty() {
                widths_override.push(1);
            }
            widths_override.push(col_w);
            visible_unpinned.push(col_idx);
            current_w += col_w;
            idx += 1;
        }
    }

    let mut visible_cols = visible_pinned;
    if border_added && !visible_unpinned.is_empty() {
        visible_cols.push(usize::MAX); // Special marker for border
    }
    visible_cols.extend(visible_unpinned);

    // ── Header row ─────────────────────────────────────────────────────────────
    // Each cell: left-aligned name (+ optional sort arrow), right-aligned type icon
    let header_cells: Vec<Cell> = visible_cols
        .iter()
        .enumerate()
        .map(|(i, &actual_col_idx)| {
            if actual_col_idx == usize::MAX {
                return Cell::from(Span::styled("│", T::separator_style()));
            }

            let col = &df.columns[actual_col_idx];
            let icon_ch = col.col_type.icon();
            let icon_str = icon_ch.to_string();

            let sort_mark = if sheet.sort_col == Some(actual_col_idx) {
                if sheet.sort_desc {
                    " ▼"
                } else {
                    " ▲"
                }
            } else {
                ""
            };
            let pin_mark = if col.pinned { "!" } else { "" };
            let sel_mark = if col.selected { "*" } else { "" };
            let name_raw = format!("{}{}{}{}", pin_mark, sel_mark, col.name, sort_mark);
            let name_w = UnicodeWidthStr::width(name_raw.as_str());
            let cell_w = widths_override[i] as usize; // Use overridden bounded width

            let (name_display, padding) = if cell_w < 2 {
                (String::new(), 0usize)
            } else if name_w < cell_w {
                let padding = cell_w - name_w - 1;
                (name_raw, padding)
            } else {
                let max_name = cell_w.saturating_sub(1);
                let truncated: String = name_raw
                    .chars()
                    .scan(0usize, |acc, c: char| {
                        let w = UnicodeWidthStr::width(c.to_string().as_str());
                        if *acc + w <= max_name {
                            *acc += w;
                            Some(c)
                        } else {
                            None
                        }
                    })
                    .collect();
                (truncated, 0)
            };

            let (name_style, icon_style) = if actual_col_idx == cursor_col {
                (
                    T::selected_col_header_style(),
                    Style::default()
                        .fg(Color::Rgb(0x23, 0x2A, 0x2E))
                        .bg(T::AQUA)
                        .add_modifier(Modifier::BOLD | Modifier::ITALIC),
                )
            } else {
                (
                    T::header_style(),
                    Style::default()
                        .fg(T::BG3)
                        .bg(T::GREEN)
                        .add_modifier(Modifier::BOLD | Modifier::ITALIC),
                )
            };

            let spaces = " ".repeat(padding);
            let line = Line::from(vec![
                Span::styled(name_display, name_style),
                Span::styled(spaces, name_style),
                Span::styled(icon_str, icon_style),
            ]);
            Cell::from(line)
        })
        .collect();

    let header = Row::new(header_cells).style(T::header_style()).height(1);

    // ── Aggregates & Dynamic bounds ───────────────────────────────────────────
    let aggregates = df.compute_aggregates();
    let max_aggs = aggregates.iter().map(|a| a.len()).max().unwrap_or(0) as u16;
    let footer_height = if max_aggs > 0 { max_aggs } else { 0 };
    let non_row_height = 3 + footer_height; // Borders(2) + Header(1) + Footer

    // ── Column width constraints ───────────────────────────────────────────────
    let widths: Vec<Constraint> = widths_override
        .iter()
        .map(|&w| Constraint::Length(w))
        .collect();

    // ── Data rows ─────────────────────────────────────────────────────────────
    // All styling is per-cell so we have full control independent of ratatui's
    // row_highlight_style / column_highlight_style (which can't express our matrix).
    //
    // Priority matrix (active = cursor row):
    //  ┌────────────────────┬──────────────────────┬──────────────────────────┐
    //  │                    │  normal column       │  active column           │
    //  ├────────────────────┼──────────────────────┼──────────────────────────┤
    //  │ normal row         │ normal_row_style     │ normal_row_style + bold* │
    //  │ active row         │ active_row_style     │ active_row_col_style     │
    //  │ selected row       │ selected_mark_style  │ selected_mark_style      │
    //  │ selected+active    │ selected_active_row  │ selected_active_col      │
    //  └────────────────────┴──────────────────────┴──────────────────────────┘
    // *bold for active column on normal row handled by column_highlight_style below.
    let cursor_col = sheet.cursor_col;
    let active_display_row = sheet.table_state.selected().unwrap_or(0);

    // Virtualization: we only render enough rows to fill the area height
    let table_height = area.height.saturating_sub(non_row_height) as usize;

    // Auto-adjust top_row if cursor went out of bounds (bounds check logic)
    // We get mutable access inside render for convenience of this adjustment
    let mut top_row = sheet.top_row;
    if active_display_row < top_row {
        top_row = active_display_row;
    } else if active_display_row >= top_row + table_height && table_height > 0 {
        top_row = active_display_row.saturating_sub(table_height) + 1;
    }

    // Ensure top_row is within bounds
    let max_top = df.visible_row_count().saturating_sub(table_height);
    top_row = top_row.min(max_top);

    let end_row = (top_row + table_height).min(df.visible_row_count());

    let rows: Vec<Row> = (top_row..end_row)
        .map(|display_row| {
            let physical = df.row_order[display_row];
            let is_selected = df.selected_rows.contains(&physical);
            let is_active = display_row == active_display_row;

            let cells: Vec<Cell> = visible_cols
                .iter()
                .enumerate()
                .map(|(i, &col)| {
                    if col == usize::MAX {
                        return Cell::from(Span::styled("│", T::separator_style()));
                    }

                    let mut text = crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(
                        &df.get_val(display_row, col),
                    );
                    let col_meta = &df.columns[col];
                    let mut is_negative_currency = false;
                    if !text.is_empty() {
                        let p = col_meta.precision as usize;
                        if col_meta.col_type == crate::types::ColumnType::Percentage {
                            if let Ok(f) = text.parse::<f64>() {
                                text = format!("{:.*}%", p, f * 100.0);
                            }
                        } else if col_meta.col_type == crate::types::ColumnType::Currency {
                            if let Ok(f) = text.parse::<f64>() {
                                let sym = col_meta.currency.map(|k| k.symbol()).unwrap_or("$");
                                let prefix =
                                    col_meta.currency.map(|k| k.is_prefix()).unwrap_or(true);

                                if f < 0.0 {
                                    is_negative_currency = true;
                                    let abs_f = f.abs();
                                    if prefix {
                                        text = format!("({}{:.*})", sym, p, abs_f);
                                    } else {
                                        text = format!("({:.*}{})", p, abs_f, sym);
                                    }
                                } else {
                                    if prefix {
                                        text = format!("{}{:.*}", sym, p, f);
                                    } else {
                                        text = format!("{:.*}{}", p, f, sym);
                                    }
                                }
                            }
                        } else if col_meta.col_type == crate::types::ColumnType::Float {
                            if let Ok(f) = text.parse::<f64>() {
                                text = format!("{:.*}", p, f);
                            }
                        }
                    }
                    let is_active_col = col == cursor_col;
                    let display_chars = widths_override[i] as usize;

                    // Simple truncation for partially visible cells to prevent wrapping or overflowing
                    let truncated_text: String = text
                        .chars()
                        .scan(0usize, |acc, c: char| {
                            let w = UnicodeWidthStr::width(c.to_string().as_str());
                            if *acc + w <= display_chars {
                                *acc += w;
                                Some(c)
                            } else {
                                None
                            }
                        })
                        .collect();

                    let mut style = match (is_active, is_selected, is_active_col) {
                        // Active + selected + active column → orange inversion
                        (true, true, true) => T::selected_active_col_style(),
                        // Active + selected → yellow inversion
                        (true, true, false) => T::selected_active_row_style(),
                        // Active row, active column → teal inversion
                        (true, false, true) => T::active_row_col_style(),
                        // Active row, other column → full inversion
                        (true, false, false) => T::active_row_style(),
                        // Selected, any column → yellow text on normal bg
                        (false, true, _) => T::selected_mark_style(display_row),
                        // Normal row handled by Row-level style below
                        (false, false, _) => T::normal_row_style(display_row),
                    };

                    // Apply red color for negative currency if not highlighted by selection/cursor
                    if is_negative_currency && !is_selected && !is_active {
                        style = style.fg(T::RED);
                    }

                    // Directory coloring (Phase 13 / F1 Feature)
                    // If this is the "Name" column of a directory listing, apply colors
                    if !is_selected
                        && !is_active
                        && df.columns.len() == 5
                        && df.columns[0].name == "Name"
                        && df.columns[1].name == "Is Directory"
                        && df.columns[4].name == "Supported"
                        && col == 0
                    {
                        // Name column
                        let is_dir_str = crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(
                            &df.get_val(display_row, 1),
                        );
                        let is_supported_str =
                            crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(
                                &df.get_val(display_row, 4),
                            );

                        if is_dir_str == "true" {
                            style = style.fg(T::BLUE);
                        } else if is_supported_str == "true" {
                            style = style.fg(T::GREEN);
                        } else {
                            style = style.fg(T::RED);
                        }
                    }

                    Cell::from(truncated_text).style(style)
                })
                .collect();

            // Row-level style: ratatui uses this as a base but per-cell styles
            // take priority, so we just set a sensible fallback.
            let row_style = if is_active && is_selected {
                T::selected_active_row_style()
            } else if is_active {
                T::active_row_style()
            } else if is_selected {
                T::selected_mark_style(display_row)
            } else {
                T::normal_row_style(display_row)
            };
            Row::new(cells).style(row_style)
        })
        .collect();

    // ── Aggregates footer row (Phase 12) ──────────────────────────────────────
    let has_aggregates = footer_height > 0;

    let title = format!(
        " {}{}{} ",
        sheet.title,
        if df.modified { " [*]" } else { "" },
        // Show stack depth if > 1
        if stack_depth > 1 {
            format!(" [{}/{}]", stack_depth, stack_depth)
        } else {
            String::new()
        }
    );

    let make_block = |title: String| {
        Block::bordered()
            .title(title)
            .border_type(BorderType::Rounded)
            .border_style(T::separator_style())
            .style(Style::default().bg(T::BG0))
    };

    let table = if has_aggregates {
        let footer_cells: Vec<Cell> = visible_cols
            .iter()
            .enumerate()
            .map(|(i, &col_idx)| {
                if col_idx == usize::MAX {
                    return Cell::from(Span::styled("│", T::separator_style()));
                }
                let col_aggs = &aggregates[col_idx];
                if col_aggs.is_empty() {
                    Cell::from("")
                } else {
                    let display_chars = widths_override[i] as usize;
                    let text: Vec<String> = col_aggs
                        .iter()
                        .map(|(agg, val)| {
                            let full = format!("{}={}", agg.name(), val);
                            full.chars()
                                .scan(0usize, |acc, c: char| {
                                    let w = UnicodeWidthStr::width(c.to_string().as_str());
                                    if *acc + w <= display_chars {
                                        *acc += w;
                                        Some(c)
                                    } else {
                                        None
                                    }
                                })
                                .collect()
                        })
                        .collect();
                    Cell::from(text.join("\n")).style(T::footer_style())
                }
            })
            .collect();

        let footer = Row::new(footer_cells)
            .style(T::footer_style())
            .height(footer_height);

        Table::new(rows, &widths)
            .header(header)
            .footer(footer)
            // No row_highlight_style / column_highlight_style — all styling is per-cell
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol("▶ ")
            .block(make_block(title))
    } else {
        Table::new(rows, &widths)
            .header(header)
            // No row_highlight_style / column_highlight_style — all styling is per-cell
            .highlight_spacing(HighlightSpacing::Always)
            .highlight_symbol("▶ ")
            .block(make_block(title))
    };

    // Need mutable access to sheet state for rendering
    // s is already mutable reference to the active sheet
    let s = sheet;
    s.top_row = top_row; // Save back the bounds-checked top_row

    // Create a temporary relative table state for rendering the slice
    let relative_col = visible_cols
        .iter()
        .position(|&c| c == cursor_col)
        .unwrap_or(0);
    let mut relative_state = ratatui::widgets::TableState::default()
        .with_selected(Some(active_display_row.saturating_sub(top_row)))
        .with_selected_column(Some(relative_col));

    frame.render_stateful_widget(table, area, &mut relative_state);

    // ── Vertical scrollbar ────────────────────────────────────────────────────
    frame.render_stateful_widget(
        Scrollbar::default()
            .orientation(ScrollbarOrientation::VerticalRight)
            .style(T::scrollbar_style()),
        area.inner(Margin {
            vertical: 1,
            horizontal: 0,
        }),
        &mut s.scroll_state,
    );

    // ── Horizontal scrollbar ──────────────────────────────────────────────────
    let mut horizontal_scroll =
        ratatui::widgets::ScrollbarState::new(s.dataframe.col_count().saturating_sub(1))
            .position(cursor_col);

    frame.render_stateful_widget(
        Scrollbar::default()
            .orientation(ScrollbarOrientation::HorizontalBottom)
            .style(T::scrollbar_style()),
        area.inner(Margin {
            vertical: 0,
            horizontal: 1,
        }),
        &mut horizontal_scroll,
    );
}
