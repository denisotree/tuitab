use crate::app::App;
use crate::app_state::ChartDrillKey;
use crate::theme::EverforestTheme as T;
use crate::types::{ChartAgg, ColumnType};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Bar, BarChart, BarGroup, Block, Borders, Chart, Dataset, GraphType};
use ratatui::Frame;
use std::collections::HashMap;

// Colour palette for multi-colour bar charts
const CHART_COLORS: &[Color] = &[
    T::BLUE,
    T::GREEN,
    T::YELLOW,
    T::ORANGE,
    T::PURPLE,
    T::AQUA,
    T::RED,
];

pub fn render(frame: &mut Frame, app: &mut App) {
    let s = app.stack.active();
    let cur_col = s.cursor_col;

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(1)])
        .split(frame.area());

    // Two-column contextual chart when a reference (pinned) column is set
    if let Some(ref_col) = app.chart.ref_col {
        let ref_type = s.dataframe.columns[ref_col].col_type;
        let cur_type = s.dataframe.columns[cur_col].col_type;
        let is_date = |ct: ColumnType| matches!(ct, ColumnType::Date | ColumnType::Datetime);
        let is_numeric = |ct: ColumnType| {
            matches!(
                ct,
                ColumnType::Integer
                    | ColumnType::Float
                    | ColumnType::Percentage
                    | ColumnType::Currency
            )
        };

        if is_date(ref_type) {
            // Line chart: X = date groups, Y = aggregated numeric/count
            render_line_chart(
                frame,
                app,
                ref_col,
                cur_col,
                is_numeric(cur_type),
                chunks[0],
            );
        } else {
            // Bar chart: X = categories, Y = aggregated numeric
            render_grouped_bar_chart(frame, app, ref_col, cur_col, chunks[0]);
        }
    } else {
        // Single-column chart: histogram or frequency
        render_single_chart(frame, app, cur_col, chunks[0]);
    }

    crate::ui::status_bar::render(frame, app, chunks[1]);
}

// ── Single-column chart (histogram or frequency) ───────────────────────────

fn render_single_chart(frame: &mut Frame, app: &mut App, col: usize, area: ratatui::layout::Rect) {
    let s = app.stack.active();
    let col_meta = &s.dataframe.columns[col];
    let col_name = col_meta.name.clone();
    let col_type = col_meta.col_type;

    let is_numeric = matches!(col_type, ColumnType::Integer | ColumnType::Float);
    let max_bars: usize = ((area.width.saturating_sub(2)) / 4).max(2) as usize;

    let (bars_data, drill_keys): (Vec<(String, f64)>, Vec<ChartDrillKey>) = if is_numeric {
        let h = compute_histogram_bins(s, col, col_type, max_bars);
        let keys = h
            .ranges
            .iter()
            .map(|&(lo, hi)| ChartDrillKey::Range(lo, hi))
            .collect();
        let data = h.labels.into_iter().zip(h.counts).map(|(l, v)| (l, v as f64)).collect();
        (data, keys)
    } else {
        let freq = frequency_bars(s, col, max_bars);
        let keys = freq
            .iter()
            .map(|(label, _)| ChartDrillKey::Exact(label.clone()))
            .collect();
        let data = freq.into_iter().map(|(l, v)| (l, v as f64)).collect();
        (data, keys)
    };

    app.chart.drill_keys = drill_keys;
    let n = bars_data.len();
    if n > 0 && app.chart.cursor_bin >= n {
        app.chart.cursor_bin = n - 1;
    }
    let cursor = app.chart.cursor_bin;

    let chart_title = if is_numeric {
        format!(" Histogram: '{}' ", col_name)
    } else {
        format!(" Frequency: '{}' (top {}) ", col_name, bars_data.len())
    };

    render_f64_bar_chart(frame, bars_data, &chart_title, area, Some(cursor));
}

// ── Smart histogram with Freedman-Diaconis binning ─────────────────────────

struct HistogramBins {
    labels: Vec<String>,
    counts: Vec<u64>,
    ranges: Vec<(f64, f64)>,
}

fn compute_histogram_bins(
    s: &crate::sheet::Sheet,
    col: usize,
    col_type: ColumnType,
    max_bars: usize,
) -> HistogramBins {
    let mut nums: Vec<f64> = Vec::new();
    for i in 0..s.dataframe.visible_row_count() {
        let val_str =
            crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(&s.dataframe.get_val(i, col));
        if let Ok(v) = val_str.parse::<f64>() {
            if v.is_finite() {
                nums.push(v);
            }
        }
    }

    if nums.is_empty() {
        return HistogramBins { labels: vec![], counts: vec![], ranges: vec![] };
    }

    nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = nums.len();

    if nums[0] >= nums[n - 1] {
        let v = nums[0];
        return HistogramBins {
            labels: vec![format_val(v, col_type)],
            counts: vec![n as u64],
            ranges: vec![(v, v + f64::EPSILON)],
        };
    }

    let q1 = nums[(n as f64 * 0.25) as usize];
    let q3 = nums[((n as f64 * 0.75) as usize).min(n - 1)];
    let iqr = q3 - q1;

    let fd_width = if iqr > 1e-10 {
        2.0 * iqr * (n as f64).powf(-1.0 / 3.0)
    } else {
        let k = ((n as f64).log2().ceil() as usize + 1).max(2);
        (nums[n - 1] - nums[0]) / k as f64
    };

    let lo = nums[((n as f64 * 0.02) as usize).min(n - 1)];
    let hi = nums[((n as f64 * 0.98) as usize).min(n - 1)];
    let range = (hi - lo).max(fd_width);

    let num_bins = ((range / fd_width).ceil() as usize).max(2).min(max_bars);
    let step = range / num_bins as f64;

    let mut buckets = vec![0u64; num_bins];
    for &v in &nums {
        let b = if v <= lo {
            0
        } else if v >= hi {
            num_bins - 1
        } else {
            ((v - lo) / step).floor() as usize
        };
        buckets[b.min(num_bins - 1)] += 1;
    }

    let mut labels = Vec::with_capacity(num_bins);
    let mut ranges = Vec::with_capacity(num_bins);
    for i in 0..num_bins {
        let b_start = lo + i as f64 * step;
        let b_end = lo + (i + 1) as f64 * step;
        labels.push(format!(
            "{}-{}",
            format_val(b_start, col_type),
            format_val(b_end, col_type)
        ));
        ranges.push((b_start, b_end));
    }

    HistogramBins { labels, counts: buckets, ranges }
}

fn format_val(v: f64, col_type: ColumnType) -> String {
    if col_type == ColumnType::Integer {
        format!("{}", v as i64)
    } else {
        format!("{:.1}", v)
    }
}

// ── Frequency bar chart (categorical columns) ──────────────────────────────

fn frequency_bars(s: &crate::sheet::Sheet, col: usize, max_bars: usize) -> Vec<(String, u64)> {
    let mut counts: HashMap<String, u64> = HashMap::new();
    for i in 0..s.dataframe.visible_row_count() {
        let val =
            crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(&s.dataframe.get_val(i, col));
        *counts.entry(val).or_insert(0) += 1;
    }
    let mut freq: Vec<(String, u64)> = counts.into_iter().collect();
    freq.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    freq.truncate(max_bars);
    freq
}

// ── Grouped bar chart: categorical × numeric ──────────────────────────────

fn render_grouped_bar_chart(
    frame: &mut Frame,
    app: &mut App,
    ref_col: usize,
    cur_col: usize,
    area: ratatui::layout::Rect,
) {
    let s = app.stack.active();
    let ref_name = s.dataframe.columns[ref_col].name.clone();
    let cur_name = s.dataframe.columns[cur_col].name.clone();
    let agg = app.chart.agg;

    let (counts, vals) = collect_groups(s, ref_col, cur_col);
    let max_bars: usize = ((area.width.saturating_sub(2)) / 4).max(2) as usize;

    let mut groups: Vec<(String, f64)> = counts
        .iter()
        .map(|(k, &cnt)| {
            let v = vals.get(k).map(|vs| vs.as_slice()).unwrap_or(&[]);
            (k.clone(), agg.apply_group(cnt, v))
        })
        .collect();
    groups.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    groups.truncate(max_bars);

    app.chart.drill_keys = groups
        .iter()
        .map(|(label, _)| ChartDrillKey::Exact(label.clone()))
        .collect();
    let n = groups.len();
    if n > 0 && app.chart.cursor_bin >= n {
        app.chart.cursor_bin = n - 1;
    }
    let cursor = app.chart.cursor_bin;

    let title = format!(" {}({}) by '{}' ", agg.label(), cur_name, ref_name);
    render_f64_bar_chart(frame, groups, &title, area, Some(cursor));
}

// ── Line chart: date × numeric (or count) ─────────────────────────────────

fn render_line_chart(
    frame: &mut Frame,
    app: &mut App,
    ref_col: usize,
    cur_col: usize,
    cur_is_numeric: bool,
    area: ratatui::layout::Rect,
) {
    let s = app.stack.active();
    let ref_name = s.dataframe.columns[ref_col].name.clone();
    let cur_name = s.dataframe.columns[cur_col].name.clone();
    let agg = if cur_is_numeric {
        app.chart.agg
    } else {
        ChartAgg::Count
    };

    let (counts, vals) = collect_groups(s, ref_col, cur_col);
    if counts.is_empty() {
        return;
    }

    // Sort groups by key (ISO date strings sort lexicographically)
    let mut sorted_keys: Vec<String> = counts.keys().cloned().collect();
    sorted_keys.sort();

    let data_points: Vec<(f64, f64)> = sorted_keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            let cnt = *counts.get(k).unwrap_or(&0);
            let vs = vals.get(k).map(|v| v.as_slice()).unwrap_or(&[]);
            let y = agg.apply_group(cnt, vs);
            (i as f64, y)
        })
        .collect();

    let x_len = sorted_keys.len();
    let x_max = (x_len.saturating_sub(1)) as f64;
    let y_vals: Vec<f64> = data_points.iter().map(|(_, y)| *y).collect();
    let mut y_min = y_vals.iter().cloned().fold(f64::INFINITY, f64::min);
    let mut y_max = y_vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if !y_min.is_finite() {
        y_min = 0.0;
    }
    if !y_max.is_finite() {
        y_max = 1.0;
    }
    let y_pad = ((y_max - y_min) * 0.05).max(1.0);

    // Sample up to ~6 x-axis labels
    let label_count = 6.min(x_len);
    let x_labels: Vec<Line<'_>> = (0..label_count)
        .map(|i| {
            let idx = if label_count <= 1 {
                0
            } else {
                i * (x_len - 1) / (label_count - 1)
            };
            let label = sorted_keys[idx].chars().take(10).collect::<String>();
            Line::from(Span::raw(label))
        })
        .collect();

    let y_mid = (y_min + y_max) / 2.0;
    let y_labels: Vec<Line<'_>> = vec![
        Line::from(Span::raw(format!("{:.1}", y_min))),
        Line::from(Span::raw(format!("{:.1}", y_mid))),
        Line::from(Span::raw(format!("{:.1}", y_max))),
    ];

    // Populate drill_keys and clamp cursor
    app.chart.drill_keys = sorted_keys
        .iter()
        .map(|k| ChartDrillKey::Exact(k.clone()))
        .collect();
    if x_len > 0 && app.chart.cursor_bin >= x_len {
        app.chart.cursor_bin = x_len - 1;
    }
    let cursor = app.chart.cursor_bin;

    let cursor_point = [data_points[cursor]];
    let cursor_label = sorted_keys[cursor].chars().take(10).collect::<String>();
    let cursor_y = data_points[cursor].1;

    let main_dataset = Dataset::default()
        .name(cur_name.as_str())
        .marker(symbols::Marker::HalfBlock)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(T::GREEN))
        .data(&data_points);

    let cursor_dataset = Dataset::default()
        .name("")
        .marker(symbols::Marker::Block)
        .graph_type(GraphType::Scatter)
        .style(Style::default().fg(T::YELLOW))
        .data(&cursor_point);

    let title = if cur_is_numeric {
        format!(
            " {}({}) over '{}' │ {} = {:.1} ",
            agg.label(),
            cur_name,
            ref_name,
            cursor_label,
            cursor_y
        )
    } else {
        format!(
            " count('{}') over '{}' │ {} = {:.0} ",
            cur_name, ref_name, cursor_label, cursor_y
        )
    };

    let chart = Chart::new(vec![main_dataset, cursor_dataset])
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .style(Style::default().fg(T::FG).bg(T::BG0)),
        )
        .x_axis(
            Axis::default()
                .title(Span::styled(ref_name, Style::default().fg(T::GREY1)))
                .bounds([0.0, x_max])
                .labels(x_labels),
        )
        .y_axis(
            Axis::default()
                .bounds([y_min - y_pad, y_max + y_pad])
                .labels(y_labels),
        );

    frame.render_widget(chart, area);
}

// ── Shared grouping helper ─────────────────────────────────────────────────

fn collect_groups(
    s: &crate::sheet::Sheet,
    ref_col: usize,
    cur_col: usize,
) -> (HashMap<String, usize>, HashMap<String, Vec<f64>>) {
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut vals: HashMap<String, Vec<f64>> = HashMap::new();

    for i in 0..s.dataframe.visible_row_count() {
        let key = crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(
            &s.dataframe.get_val(i, ref_col),
        );
        *counts.entry(key.clone()).or_insert(0) += 1;
        let val_str = crate::data::dataframe::DataFrame::anyvalue_to_string_fmt(
            &s.dataframe.get_val(i, cur_col),
        );
        if let Ok(v) = val_str.parse::<f64>() {
            if v.is_finite() {
                vals.entry(key).or_default().push(v);
            }
        }
    }
    (counts, vals)
}

// ── f64 bar chart: handles scaling, actual text values, colour, layout ─────

fn render_f64_bar_chart(
    frame: &mut Frame,
    bars_data: Vec<(String, f64)>,
    title: &str,
    area: ratatui::layout::Rect,
    cursor: Option<usize>,
) {
    if bars_data.is_empty() {
        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .style(Style::default().fg(T::FG).bg(T::BG0));
        let p = ratatui::widgets::Paragraph::new("No data")
            .block(block)
            .style(Style::default().fg(T::GREY1));
        frame.render_widget(p, area);
        return;
    }

    let n = bars_data.len();
    let available = area.width.saturating_sub(2) as usize;

    let max_val = bars_data
        .iter()
        .map(|(_, v)| *v)
        .fold(f64::NEG_INFINITY, f64::max)
        .max(1.0);
    let text_vals: Vec<String> = bars_data
        .iter()
        .map(|(_, v)| {
            if v.fract().abs() < 0.005 {
                format!("{}", *v as i64)
            } else {
                format!("{:.2}", v)
            }
        })
        .collect();

    let max_label_len = bars_data
        .iter()
        .map(|(l, _)| l.chars().count())
        .max()
        .unwrap_or(1);
    let use_horizontal = n * (max_label_len.max(3) + 1) > available;

    if use_horizontal {
        render_horizontal_bars(frame, bars_data, text_vals, title, area, cursor);
        return;
    }

    let scaled: Vec<u64> = bars_data
        .iter()
        .map(|(_, v)| ((v / max_val) * 10_000.0) as u64)
        .collect();

    let bar_width = (available.saturating_sub(n.saturating_sub(1)))
        .checked_div(n)
        .unwrap_or(3)
        .max(3) as u16;

    let bars: Vec<Bar> = bars_data
        .iter()
        .enumerate()
        .map(|(i, (label, _))| {
            let is_cursor = cursor == Some(i);
            let color = CHART_COLORS[i % CHART_COLORS.len()];
            let text = if is_cursor {
                format!("▶{}", text_vals[i])
            } else {
                text_vals[i].clone()
            };
            let val_style = if is_cursor {
                Style::default().fg(T::BG0).bg(T::YELLOW)
            } else {
                Style::default().fg(T::BG0).bg(color)
            };
            Bar::default()
                .value(scaled[i])
                .label(label.as_str())
                .text_value(text)
                .style(Style::default().fg(color))
                .value_style(val_style)
        })
        .collect();

    let group = BarGroup::default().bars(&bars);
    let barchart = BarChart::default()
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .style(Style::default().fg(T::FG).bg(T::BG0)),
        )
        .bar_width(bar_width)
        .bar_gap(1)
        .group_gap(0)
        .max(10_000)
        .data(group);

    frame.render_widget(barchart, area);
}

// ── Horizontal bar chart rendered as Paragraph ────────────────────────────

fn render_horizontal_bars(
    frame: &mut Frame,
    bars_data: Vec<(String, f64)>,
    text_vals: Vec<String>,
    title: &str,
    area: ratatui::layout::Rect,
    cursor: Option<usize>,
) {
    use ratatui::text::Text;
    use ratatui::widgets::Paragraph;

    if bars_data.is_empty() {
        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .style(Style::default().fg(T::FG).bg(T::BG0));
        frame.render_widget(
            Paragraph::new("No data")
                .block(block)
                .style(Style::default().fg(T::GREY1)),
            area,
        );
        return;
    }

    let max_val = bars_data
        .iter()
        .map(|(_, v)| *v)
        .fold(f64::NEG_INFINITY, f64::max)
        .max(1.0);
    let max_text_len = text_vals.iter().map(|s| s.len()).max().unwrap_or(1);
    let label_width = bars_data
        .iter()
        .map(|(l, _)| l.chars().count())
        .max()
        .unwrap_or(1)
        .min(24);

    // inner width = area.width - 2 borders
    let inner_w = area.width.saturating_sub(2) as usize;
    // bar zone = inner_w - label_width - " │ " (3) - value text (max_text_len + 1 space)
    let bar_zone = inner_w
        .saturating_sub(label_width)
        .saturating_sub(3)
        .saturating_sub(max_text_len + 1)
        .max(1);

    let mut lines: Vec<Line<'static>> = Vec::new();
    for (i, (label, val)) in bars_data.iter().enumerate() {
        let is_cursor = cursor == Some(i);
        let color = CHART_COLORS[i % CHART_COLORS.len()];
        let bar_len = ((val / max_val) * bar_zone as f64).round() as usize;
        let bar_str: String = "█".repeat(bar_len);
        let empty_len = bar_zone.saturating_sub(bar_len);
        let empty_str: String = " ".repeat(empty_len);

        // Truncate/pad label; prepend cursor indicator
        let label_truncated: String = label.chars().take(label_width).collect();
        let label_padded = if is_cursor {
            // prefix "▶" and pad to label_width
            let inner: String = label.chars().take(label_width.saturating_sub(1)).collect();
            format!("{:>width$}", format!("▶{}", inner), width = label_width)
        } else {
            format!("{:>width$}", label_truncated, width = label_width)
        };
        let label_style = if is_cursor {
            Style::default().fg(T::YELLOW)
        } else {
            Style::default().fg(T::GREY1)
        };

        let val_text = format!(" {}", text_vals[i]);
        let val_style = if is_cursor {
            Style::default().fg(T::YELLOW)
        } else {
            Style::default().fg(T::FG)
        };

        lines.push(Line::from(vec![
            Span::styled(label_padded, label_style),
            Span::raw(" │"),
            Span::styled(bar_str, Style::default().fg(color)),
            Span::raw(empty_str),
            Span::styled(val_text, val_style),
        ]));
    }

    let n = lines.len();
    // Visible rows inside the block (minus 2 border lines)
    let visible = area.height.saturating_sub(2) as usize;

    // Scroll so the cursor row is always in view (cursor at bottom of window when scrolled down)
    let scroll: usize = if let Some(c) = cursor {
        if visible == 0 || c < visible {
            0
        } else {
            c + 1 - visible
        }
    } else {
        0
    };

    // Build title with scroll position indicators
    let above = scroll;
    let below = n.saturating_sub(scroll + visible);
    let scroll_hint = match (above > 0, below > 0) {
        (true, true) => format!(" ↑{} ↓{} ", above, below),
        (true, false) => format!(" ↑{} ", above),
        (false, true) => format!(" ↓{} ", below),
        (false, false) => String::new(),
    };
    let full_title = if scroll_hint.is_empty() {
        title.to_string()
    } else {
        format!("{}{}", title.trim_end_matches(' '), scroll_hint)
    };

    let text = Text::from(lines);
    let block = Block::default()
        .title(full_title)
        .borders(Borders::ALL)
        .style(Style::default().fg(T::FG).bg(T::BG0));
    let para = Paragraph::new(text)
        .block(block)
        .scroll((scroll as u16, 0));
    frame.render_widget(para, area);
}
