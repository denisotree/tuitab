//! The key reference: one table that the `?` popup and the prefix hints are both
//! drawn from, so a key added to the key layer cannot reach one and miss the other.
//! `tests/keymap_tests.rs` checks the table against `handle_key_event`.

use crate::theme::EverforestTheme as T;
use crate::ui::popup::centered_rect;
use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph},
    Frame,
};
use unicode_width::UnicodeWidthStr;

/// Keys that do one thing, and what it is. A key is written as it is pressed:
/// `j`, `Ctrl+F`, `Enter`, `←`, a chord as `zJ`. A line break in `what` starts
/// a note under the key.
pub struct Key {
    pub keys: &'static [&'static str],
    pub what: &'static str,
}

pub struct Section {
    pub title: &'static str,
    pub keys: &'static [Key],
}

const fn k(keys: &'static [&'static str], what: &'static str) -> Key {
    Key { keys, what }
}

pub const SECTIONS: &[Section] = &[
    Section {
        title: "Navigation",
        keys: &[
            k(&["h", "j", "k", "l"], "Move cursor"),
            k(&["←", "↓", "↑", "→"], "Move cursor"),
            k(&["gg", "Home"], "Go to top"),
            k(&["G", "End"], "Go to bottom"),
            k(&["Ctrl+F", "PageDown"], "Page down"),
            k(&["Ctrl+B", "PageUp"], "Page up"),
            k(&["q", "Esc"], "Back one sheet / quit"),
        ],
    },
    Section {
        title: "Search & Selection",
        keys: &[
            k(&["/"], "Search (regex)"),
            k(&["n", "N"], "Next / previous match"),
            k(&["c"], "Clear search highlights"),
            k(&[","], "Select rows equal to the current cell"),
            k(&["|"], "Select rows by regex\n!=expr selects by expression"),
            k(&["s", "u"], "Select / unselect row"),
            k(&["gs", "gu"], "Select all / unselect all"),
            k(&["gt"], "Invert the selection"),
            k(&["Sr"], "Select N random rows"),
            k(&["Sd"], "Select all duplicate rows"),
            k(&["SD"], "Smart dedup (asks a tiebreaker if cols are pinned)"),
            k(&["d"], "Delete the selected rows"),
            k(&["\""], "New sheet from the selected rows"),
        ],
    },
    Section {
        title: "Clipboard",
        keys: &[
            k(&["yc"], "Copy current cell"),
            k(&["yp"], "Copy the document path of the cell"),
            k(&["yr"], "Selected rows or current row → format"),
            k(&["yz"], "Column values of selected rows → format\nno selection: copies the current cell"),
            k(&["yZ"], "Entire current column → format"),
            k(&["yR"], "Entire table → format\nyr and yR keep only the columns marked with zs"),
            k(&["p"], "Paste into the cell under the cursor"),
            k(&["P"], "Paste rows"),
        ],
    },
    Section {
        title: "Columns",
        keys: &[
            k(&["ze"], "Rename column"),
            k(&["zd"], "Delete column"),
            k(&["zi"], "Insert empty column"),
            k(&["zs", "zu"], "Mark / unmark column (*)"),
            k(&["zr"], "Find & replace in column"),
            k(&["zg"], "Find & replace with a regex"),
            k(&["zx"], "Split column by a delimiter"),
            k(&["zf"], "Column % of total"),
            k(&["zF"], "Partitioned % of total"),
            k(
                &["zw"],
                "Window column (rank, running total, lag)\na running total asks what orders the rows;\nthe table itself is not re-sorted",
            ),
            k(&["zJ"], "One column per key of the JSON in this column"),
            k(&["z←", "z→", "zh", "zl"], "Move column left / right"),
            k(&["z.", "z,", "z>", "z<"], "More / fewer decimal places"),
            k(&["!"], "Pin / unpin column"),
            k(&["_"], "Cycle column width"),
            k(&["g_"], "Cycle all column widths"),
            k(&["="], "Add computed column"),
            k(&["t"], "Column type menu (Currency asks which)"),
            k(&["+"], "Footer aggregators"),
            k(&["-"], "Clear footer aggregators"),
            k(&["Z"], "Quick aggregate into the status bar"),
            k(&["gW"], "Unpivot: unpinned columns become rows"),
        ],
    },
    Section {
        title: "Sorting",
        keys: &[
            k(&["[", "]"], "Sort ascending / descending"),
            k(&["z[", "z]"], "Add another sort key (asc / desc)"),
            k(&["r"], "Reset to the original row order"),
        ],
    },
    Section {
        title: "Derived Sheets & Analytics",
        keys: &[
            k(&["F"], "Frequency table"),
            k(&["gF"], "Multi-column frequency (pinned)"),
            k(&["gb"], "Group by pinned cols (aggregates from +)"),
            k(&["gD", "gd", "gU"], "Deduplicate by pinned cols"),
            k(&["Enter"], "Transpose row / drill down / dive into node"),
            k(&["T"], "Transpose the table"),
            k(&["I"], "Describe sheet (statistics)"),
            k(&["V"], "Chart the column"),
            k(&["W"], "Pivot table"),
            k(&["J"], "JOIN with another table"),
        ],
    },
    Section {
        title: "JSON / YAML / TOML",
        keys: &[
            k(&["zEnter"], "Dive into the cell's node"),
            k(&["g/"], "Search the whole document → hit list"),
            k(&["gp"], "Go to a node by its path"),
            k(&["gq"], "Run a jq query → new sheet"),
            k(&["m"], "Cycle records / key-value / scalars"),
            k(&["(", ")"], "Expand / fold a nested column"),
            k(&["zo"], "Open a file as a format (directory sheet)"),
        ],
    },
    Section {
        title: "Editing & File",
        keys: &[
            k(
                &["e"],
                "Edit cell (a JSON scalar keeps its type)\n\\N in a DB cell means SQL NULL",
            ),
            k(&["E"], "Edit in $EDITOR (a JSON container as text)"),
            k(&["ge"], "Set one value on every selected row"),
            k(&["o"], "Add an empty row below the cursor"),
            k(&["O"], "New-row form — a field per column, checked by type"),
            k(
                &["Ctrl+S"],
                "Save / export / convert format\non a DB table: write edits and column changes back",
            ),
            k(&["R"], "Reload from disk"),
            k(&["U"], "Undo"),
            k(&["Ctrl+R"], "Redo"),
            k(&["?"], "This help"),
        ],
    },
];

/// Key column width in the `?` popup.
const KEY_W: usize = 18;

/// Word-wrap `text` to `width` columns; a word longer than that gets a line of its own.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let mut lines = vec![String::new()];
    for word in text.split(' ') {
        let line = lines.last_mut().expect("never empty");
        let need = UnicodeWidthStr::width(word) + usize::from(!line.is_empty());
        if !line.is_empty() && UnicodeWidthStr::width(line.as_str()) + need > width {
            lines.push(word.to_string());
        } else {
            if !line.is_empty() {
                line.push(' ');
            }
            line.push_str(word);
        }
    }
    lines
}

/// The `?` popup's lines at `width` columns inside its border.
fn help_lines(width: usize) -> Vec<Line<'static>> {
    let what_w = width.saturating_sub(KEY_W + 2).max(10);
    let mut out = Vec::new();
    for (i, section) in SECTIONS.iter().enumerate() {
        if i > 0 {
            out.push(Line::from(""));
        }
        out.push(Line::from(Span::styled(
            format!("── {} ", section.title),
            Style::default().fg(T::GREEN),
        )));
        for key in section.keys {
            let label = key.keys.join(" ");
            for (j, para) in key.what.split('\n').enumerate() {
                for (n, text) in wrap(para, what_w).into_iter().enumerate() {
                    let head = if j == 0 && n == 0 { label.as_str() } else { "" };
                    let pad = KEY_W.saturating_sub(UnicodeWidthStr::width(head));
                    out.push(Line::from(format!("  {head}{} {text}", " ".repeat(pad))));
                }
            }
        }
    }
    out
}

/// Draw the `?` popup scrolled down `scroll` lines. The scroll is clamped to what
/// the popup can show, and written back so the next key moves from there.
pub fn render_help_popup(frame: &mut Frame, area: Rect, scroll: &mut u16) {
    let popup_area = centered_rect(70, 85, area);
    frame.render_widget(Clear, popup_area);

    let lines = help_lines(popup_area.width.saturating_sub(2) as usize);
    let shown = popup_area.height.saturating_sub(2) as usize;
    let last = lines.len().saturating_sub(shown);
    *scroll = (*scroll as usize).min(last) as u16;

    let title = if last == 0 {
        " Help — Esc or ? closes ".to_string()
    } else {
        format!(
            " Help — j/k, Ctrl+F/B scroll · Esc or ? closes · {}/{} ",
            *scroll as usize + shown.min(lines.len()),
            lines.len()
        )
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(T::AQUA));
    frame.render_widget(
        Paragraph::new(lines).block(block).scroll((*scroll, 0)),
        popup_area,
    );
}

/// The keys of one prefix — `z` gives `J  One column per key …` — as the help lists them.
fn prefix_keys(prefix: char) -> Vec<(String, &'static str)> {
    SECTIONS
        .iter()
        .flat_map(|s| s.keys.iter())
        .filter_map(|key| {
            let rest: Vec<&str> = key
                .keys
                .iter()
                .filter_map(|k| k.strip_prefix(prefix).filter(|r| !r.is_empty()))
                .collect();
            let what = key.what.split('\n').next().unwrap_or_default();
            (!rest.is_empty()).then(|| (rest.join(" "), what))
        })
        .collect()
}

/// The hint shown while a prefix waits for its second key: every key it takes, in
/// a box above the status bar's right end, in as many columns as the height needs.
pub fn render_prefix_popup(frame: &mut Frame, area: Rect, prefix: char) {
    let keys = prefix_keys(prefix);
    if keys.is_empty() || area.height < 3 {
        return;
    }
    let key_w = keys
        .iter()
        .map(|(k, _)| UnicodeWidthStr::width(k.as_str()))
        .max()
        .unwrap_or(1);
    let cell: Vec<String> = keys
        .iter()
        .map(|(k, what)| {
            let pad = key_w - UnicodeWidthStr::width(k.as_str());
            format!("{k}{}  {what}", " ".repeat(pad))
        })
        .collect();
    let col_w = cell
        .iter()
        .map(|c| UnicodeWidthStr::width(c.as_str()))
        .max()
        .unwrap_or(1)
        + 2;
    let rows_fit = (area.height as usize - 2).max(1);
    let cols = cell.len().div_ceil(rows_fit);
    let rows = cell.len().div_ceil(cols);

    let lines: Vec<Line> = (0..rows)
        .map(|r| {
            let text: String = (0..cols)
                .filter_map(|c| cell.get(c * rows + r))
                .map(|s| {
                    let pad = col_w - UnicodeWidthStr::width(s.as_str());
                    format!("{s}{}", " ".repeat(pad))
                })
                .collect();
            Line::from(text)
        })
        .collect();

    let width = ((col_w * cols) as u16 + 2).min(area.width);
    let height = (rows as u16 + 2).min(area.height);
    let popup = Rect {
        x: area.x + area.width - width,
        y: area.y + area.height - height,
        width,
        height,
    };
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .title(format!(" {prefix}… — Esc cancels "))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(T::AQUA));
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}
