use ratatui::style::{Color, Modifier, Style};

/// Everforest Dark Medium color palette and style helpers
pub struct EverforestTheme;

#[allow(dead_code)]
impl EverforestTheme {
    // ── Backgrounds ──────────────────────────────────────────────────────────
    pub const BG_DIM: Color = Color::Rgb(0x23, 0x2A, 0x2E); // #232A2E
    pub const BG0: Color = Color::Rgb(0x2D, 0x35, 0x3B); // #2D353B — main bg
    pub const BG1: Color = Color::Rgb(0x34, 0x3F, 0x44); // #343F44 — cursor line
    pub const BG2: Color = Color::Rgb(0x3D, 0x48, 0x4D); // #3D484D — selection
    pub const BG3: Color = Color::Rgb(0x47, 0x52, 0x58); // #475258 — header/status
    pub const BG4: Color = Color::Rgb(0x4F, 0x58, 0x5E); // #4F585E — separators

    // ── Foregrounds ──────────────────────────────────────────────────────────
    pub const FG: Color = Color::Rgb(0xD3, 0xC6, 0xAA); // #D3C6AA — main text
    pub const RED: Color = Color::Rgb(0xE6, 0x7E, 0x80); // #E67E80
    pub const ORANGE: Color = Color::Rgb(0xE6, 0x98, 0x75); // #E69875
    pub const YELLOW: Color = Color::Rgb(0xDB, 0xBC, 0x7F); // #DBBC7F
    pub const GREEN: Color = Color::Rgb(0xA7, 0xC0, 0x80); // #A7C080
    pub const AQUA: Color = Color::Rgb(0x83, 0xC0, 0x92); // #83C092
    pub const BLUE: Color = Color::Rgb(0x7F, 0xBB, 0xB3); // #7FBBB3
    pub const PURPLE: Color = Color::Rgb(0xD6, 0x99, 0xB6); // #D699B6
    pub const GREY0: Color = Color::Rgb(0x7A, 0x84, 0x78); // #7A8478
    pub const GREY1: Color = Color::Rgb(0x85, 0x92, 0x89); // #859289
    pub const GREY2: Color = Color::Rgb(0x9D, 0xA9, 0xA0); // #9DA9A0

    // ── Style helpers ─────────────────────────────────────────────────────────

    pub fn header_style() -> Style {
        Style::default()
            .fg(Self::GREEN)
            .bg(Self::BG3)
            .add_modifier(Modifier::BOLD)
    }

    pub fn normal_row_style(index: usize) -> Style {
        let bg = if index % 2 == 0 { Self::BG0 } else { Self::BG_DIM };
        Style::default().fg(Self::FG).bg(bg)
    }

    /// Cursor (active) row — full inversion: bright fg becomes bg, dark bg becomes fg.
    /// The active column cell gets an extra BOLD marker; other cells are bold too.
    pub fn active_row_style() -> Style {
        Style::default()
            .fg(Self::BG_DIM)   // dark text on bright
            .bg(Self::FG)       // #D3C6AA — cream/wheat background
            .add_modifier(Modifier::BOLD)
    }

    /// The single cell at the intersection of cursor row AND cursor column.
    /// Slightly deeper colour so the column is still distinguishable.
    pub fn active_row_col_style() -> Style {
        Style::default()
            .fg(Self::BG_DIM)
            .bg(Self::AQUA)     // teal, same as the header indicator
            .add_modifier(Modifier::BOLD)
    }

    /// Header cell of the currently active column.
    pub fn selected_col_header_style() -> Style {
        Style::default()
            .fg(Self::BG_DIM)
            .bg(Self::AQUA)
            .add_modifier(Modifier::BOLD)
    }

    /// Row selected by the user ('s' key) — yellow accent text, normal background.
    /// **No** background fill — the row blends with alternating rows but text pops.
    pub fn selected_mark_style(index: usize) -> Style {
        let bg = if index % 2 == 0 { Self::BG0 } else { Self::BG_DIM };
        Style::default()
            .fg(Self::YELLOW)
            .bg(bg)
            .add_modifier(Modifier::BOLD)
    }

    /// Selected row AND it is the active (cursor) row — invert, but tint with YELLOW.
    pub fn selected_active_row_style() -> Style {
        Style::default()
            .fg(Self::BG_DIM)
            .bg(Self::YELLOW)   // yellow bg, dark text
            .add_modifier(Modifier::BOLD)
    }

    /// Active-column cell on a selected+active row.
    pub fn selected_active_col_style() -> Style {
        Style::default()
            .fg(Self::BG_DIM)
            .bg(Self::ORANGE)
            .add_modifier(Modifier::BOLD)
    }

    /// Kept for aggregators footer compatibility.
    pub fn selected_row_style() -> Style {
        Style::default()
            .fg(Self::AQUA)
            .bg(Self::BG2)
            .add_modifier(Modifier::BOLD)
    }

    pub fn status_bar_style() -> Style {
        Style::default().fg(Self::FG).bg(Self::BG3)
    }

    pub fn mode_indicator_style() -> Style {
        Style::default()
            .fg(Self::BG0)
            .bg(Self::GREEN)
            .add_modifier(Modifier::BOLD)
    }

    pub fn filter_input_style() -> Style {
        Style::default().fg(Self::YELLOW).bg(Self::BG1)
    }

    pub fn error_style() -> Style {
        Style::default().fg(Self::RED)
    }

    pub fn sort_asc_style() -> Style {
        Style::default().fg(Self::BLUE)
    }

    pub fn sort_desc_style() -> Style {
        Style::default().fg(Self::ORANGE)
    }

    pub fn frequency_bar_style() -> Style {
        Style::default().fg(Self::GREEN).bg(Self::BG_DIM)
    }

    pub fn scrollbar_style() -> Style {
        Style::default().fg(Self::GREY1)
    }

    pub fn separator_style() -> Style {
        Style::default().fg(Self::GREY0)
    }

    pub fn footer_style() -> Style {
        Style::default()
            .fg(Self::PURPLE)
            .bg(Self::BG3)
            .add_modifier(Modifier::ITALIC)
    }
}
