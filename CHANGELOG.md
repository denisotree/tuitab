# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.2] - 2026-05-05

### Fixed
- Remove `strip = true` from release profile — `cargo install tuitab` no longer requires the `llvm-tools` rustup component

## [0.4.1] - 2026-05-05

### Added
- Status-bar viewport-clip indicator: `[clip 71/80]` when the cursor column's allocated viewport width is smaller than its stored width, so it's clear when content gets cut off purely because the terminal is too narrow

### Changed
- `_` column-width toggle simplified from three modes (Default → Compact → Expanded → Default) to two (Default ↔ Fit). Fit measures content width across all rows; Default restores the load-time bounded width. Header width remains the floor in both
- Column header now has a 1-char left padding so its name doesn't visually touch the previous column's type icon (paired with `column_spacing(0)` to remove the redundant ratatui gap)

### Fixed
- Multi-line cell content (cells containing `\n`, e.g. "Geo allowed list" with newline-separated countries) no longer makes columns expand to full screen width — `calc_column_width` now uses the longest single line instead of summing all lines via `UnicodeWidthStr::width()` on the whole string. Cell rendering also stops at the first `\n` so only the first line is shown
- Drill-down + `q` panic: `Failed to read sheet data from disk: io error` after multiple drill-downs followed by pop. The previous refactor introduced an asymmetric serde impl for `ColumnWidthMode` (auto-derived `Serialize` wrote a `u32` enum index, custom `Deserialize` tried to read a `String` length), causing bincode to read past the swap file's EOF
- `build_column_plan` over-allocated viewport space by 4 chars (highlight symbol `▶ ` and ratatui's default `column_spacing=1` were not subtracted), so the last visible column was silently clipped by ratatui below the width handed to it. Fixed with `max_width = area.width - 4` and explicit `column_spacing(0)` on the table

## [0.4.0] - 2026-04-30

### Added
- `Shift+S` Special select prefix mode with three subcommands:
  - `Shift+S r` — random selection of N visible rows (N entered in popup)
  - `Shift+S d` — select all rows that have an exact duplicate (full row match)
  - `Shift+S D` — smart deduplication: dedup by all columns when no pinned columns; with pinned columns, opens a tiebreaker popup to pick column + ASC/DESC (or random) for choosing which row to keep
- Bulk edit (`ge`) now pre-fills the input with the value of the active cell, so you can quickly tweak or replace it
- Column string operations under `z` prefix:
  - `zr` — find/replace in a column (literal)
  - `zg` — find/replace in a column (regex)
  - `zx` — split a column by delimiter into N new columns
- `ColumnType::FileSize` — integer bytes rendered as human-readable `1.5 KB` / `2.3 MB` / etc.; directory listings now use it so the Size column is sortable numerically
- Three-state column width cycle (`_`): Default (load-time auto-width) → Compact (header-only) → Expanded (full content). Replaces the old binary expand toggle
- Column move mode (`z←` / `z→`): repeated arrows reorder the column until any other key exits

### Changed
- `gt` (toggle all) now performs true per-row inversion: previously selected rows become unselected and vice versa, instead of the old "all-or-nothing" behaviour
- `cargo audit` ignore for `RUSTSEC-2025-0141` (bincode unmaintained warning) — bincode is still pulled transitively by `polars-utils` and we'll drop it once polars upgrades

### Fixed
- Opening a file from a directory listing (`tuitab ~/Downloads/`) when cwd is not the parent directory: previously failed with "No such file or directory" because the relative path was built from the sheet title; now uses the full `source_path` of the directory sheet, and sub-directories propagate `source_path` correctly
- File size in directory listings now displays in human-readable form (B/KB/MB/GB) instead of raw byte count
- `clear_aggregators` and `apply_aggregators` now push undo, so column aggregator changes are reversible

## [0.3.8] - 2026-04-29

### Added
- Chart cursor navigation: `←`/`→` move a highlight across histogram/frequency bars and line-chart points; Enter drills into matching rows
- Histogram drill-down: Enter on a bar opens a filtered table sheet; `q`/Esc returns to the chart
- Pin/unpin (`!`) now restores the column's original position when unpinning
- `bundled-duckdb` Cargo feature (default-enabled): DuckDB is compiled from source by default; pass `--no-default-features` when a system DuckDB library is available to skip the ~5 min C++ compilation

### Fixed
- Save-dialog Tab-completion no longer bleeds expression-autocomplete state — opening Ctrl+S after typing a formula no longer shows formula candidates in the file-path popup
- Chart cursor (`→`) no longer advances past the last bar
- Histogram over a constant-value column now renders and drills down correctly (bin range was too narrow to match any value)
- Chart aggregation selector navigation now wraps at list boundaries, consistent with other selectors

### Changed
- Internal: `handle_action()` decomposed into 8 focused per-domain modules (`chart`, `aggregator`, `edit`, `type_select`, `clipboard`, `io`, `pivot`, `selection`)
- Internal: `table_view::render()` split into `build_column_plan`, `make_header_row`, `make_data_rows`, `make_footer_row`
- Internal: App state extracted into `JoinState`, `ExpressionState`, `ChartState`, `SaveState`, `CopyState`, etc.
- Internal: `ui/popup.rs` and `data/io.rs` split into format-specific sub-modules
- Internal: date constants, comparison helper, and type-conversion helpers moved to the data layer
- Build: `polars` uses `default-features = false`; `arboard` drops unused `image` feature; release profile uses `lto = "thin"`, `strip = true`; SQLite always bundled

## [0.3.7] - 2026-04-24

### Fixed
- Pivot table (`Shift+W`) no longer fails with "explicit column references are not allowed in the `aggregate_function` of `pivot`" when using compound formulas like `sum(col) / count(col)` — replaced `col("pivot_value").first()` with the correct `element().first()` placeholder
- Autocomplete now consistently prioritises prefix matches over substring matches and sorts each group alphabetically
- Column auto-width (`_`) now always expands on first press instead of randomly collapsing — `width_expanded` was incorrectly initialised to `true`, so the first press collapsed the column to header width
- Column auto-width now measures actual displayed values (respecting float precision, currency symbols, percentage formatting) instead of hardcoded per-type estimates
- Groupby (`Shift+F`) and pivot (`Shift+W`) now preserve the source column's display type (`Currency`, `Percentage`, etc.) on the resulting columns — previously all aggregated value columns were downgraded to `Float`

## [0.3.6] - 2026-04-24

### Fixed
- Excel files with duplicate or empty column headers no longer crash on open — empty headers are renamed to `column_N`, duplicate names get a `_2`, `_3`, … suffix

## [0.3.5] - 2026-04-24

### Fixed
- Removed `rust-toolchain.toml` and nightly toolchain experiments that caused `cargo install` failures and forced nightly Rust on all users; CI and release builds are now fully on stable

## [0.3.4] - 2026-04-23

### Fixed
- docs.rs build: remove `components` from `rust-toolchain.toml` so docs.rs correctly applies the pinned `nightly-2026-03-15` toolchain; CI jobs now install `rustfmt`/`clippy` components explicitly

## [0.3.3] - 2026-04-23

### Fixed
- docs.rs build: pin `nightly-2026-03-15` toolchain to work around `polars-ops 0.53` accessing private nightly Rust Unicode APIs (`core::unicode::{Cased, Case_Ignorable}`) removed in nightly ≥ 2026-03-25

## [0.3.2] - 2026-04-23

### Added
- Copy/yank system: `yr` (rows), `yz` (column values), `yZ` (whole column), `yR` (whole table), `yc` (cell) — each opens a format popup (TSV, CSV, JSON, Markdown)
- Redo: `Ctrl+R` complements existing undo (`U` / `Shift+U`)
- JSON export/copy now preserves column order (previously keys were sorted alphabetically)
- Copy and save operations output display-formatted values — percentage columns export as `"30%"`, not `"0.3"`
- `Pct` column in frequency tables (`Shift+F`, `gF`) is now typed as Percentage and displays as `"42.3%"`
- String columns containing `"30%"` values can be converted to Float, Percentage, or Integer via `t` (percent suffix stripped and scaled appropriately)

### Fixed
- Aggregation footer now shows results for Percentage columns on first use (previously showed empty until type was reassigned via `t`)
- Precision resets to 2 decimal places when switching a column to Float/Percentage/Currency type
- Error message when adding an incompatible aggregator now references `t` instead of removed keybindings

## [0.3.1] - 2026-04-22

### Fixed
- Opening a multi-sheet xlsx file directly (e.g. `tuitab file.xlsx`) now shows the sheet overview instead of opening the first sheet

## [0.3.0] - 2026-04-22

### Added
- JOIN contextual sources: pressing `J` now shows sibling items from the same origin — tables from the same SQLite/DuckDB database, files from the same directory, sheets from the same xlsx file
- Multi-select JOIN from overview sheets (directory listing, SQLite/DuckDB/xlsx table browser): select N items with Space, confirm with Enter to chain-join them sequentially
- Chain JOIN: after joining, pressing `J` again continues chaining additional tables onto the result
- xlsx multi-sheet browser: opening an xlsx file with multiple sheets now shows a sheet overview; pressing Enter drills into the selected sheet
- `Shift+E` — open current cell value in `$EDITOR`/`$VISUAL`/`vi`; saves back to the cell if the text was changed
- Tilde expansion (`~/...`) in JOIN path input and save dialog — both file loading and Tab-autocomplete now correctly expand `~` to the home directory
- Tab-completion for JOIN file path input now works with `~/` prefixes

### Fixed
- DuckDB tables with exotic column types (STRUCT, LIST, TIMESTAMP WITH TIME ZONE, etc.) no longer cause a panic on open — all values are read via `CAST(col AS VARCHAR)`

## [0.2.0] - 2026-04-21

### Added
- Save dialog now remembers the original file path and shows relative path by default (e.g., `db/prices.csv` instead of just `prices.csv`)
- Tab-completion for file paths in save dialog — completes to common prefix or cycles through matching files
- DateTime recovery: when converting a Datetime column to Date, the original time is preserved and can be restored when converting back to Datetime
- `date()` function for computed columns — extracts date from Datetime or parses date from string (e.g., `=date(timestamp_col)`)
- File reload via **Shift+R** — reloads current file from disk while preserving scroll position and selection
- Automatic Date/Datetime parsing from string columns supporting multiple input formats (`%Y-%m-%d`, `%Y-%m-%d %H:%M:%S`, ISO 8601, etc.)

### Changed
- Save dialog behavior improved to work correctly with files in subdirectories
- Source path tracking on sheets enables better save/reload functionality

## [0.1.5] - 2026-04-14

### Changed
- polars dependency updated from 0.46 to 0.53; fixes docs.rs build failure caused by `polars-ops 0.46` accessing private nightly Rust stdlib functions (`core::unicode::Case_Ignorable`, `core::unicode::Cased`)

## [0.1.4] - 2026-04-14

### Changed
- docs.rs badge fixed: `documentation` metadata now points to `https://docs.rs/tuitab`; added `[package.metadata.docs.rs]` with `bundled-sqlite` feature so docs.rs builds successfully without a system `libsqlite3`
- Comprehensive rustdoc documentation added: module-level `//!` overviews for `data`, `ui`, `app`, `sheet`, and `theme`; `///` doc comments on public structs, enums, and methods throughout the codebase
- README relative links replaced with absolute GitHub URLs for correct rendering on docs.rs

## [0.1.3] - 2026-04-10

### Added
- Human-readable file sizes in directory listing (e.g. "1.2 KB", "3.4 MB") instead of raw byte counts
- TXT files are now read as a single-column table — each line becomes one row in a "Line" column
- SQLite files now open a table browser showing all tables with name, row count, column count, and SQL definition; pressing Enter drills into the selected table
- Column selection in z-mode: `zs` marks a column with `*`, `zu` unmarks it; pressing `"` with selected columns creates a new sheet containing only those columns (combines with row selection)

### Fixed
- Save dialog (Ctrl+S) now pre-fills with the current sheet's filename instead of the original CLI argument

## [0.1.2] - 2026-04-09

### Added
- Binary alias `ttb` (short for tuitab)

### Fixed
- Missing file error now prints a clean message instead of a backtrace
- `run()` entry point exposed in library crate for external use

## [0.1.1] - 2026-04-09

- Version bump to 0.1.1.

## [0.1.0] - 2026-04-08

### Added

- Multi-format file support: CSV/TSV (auto-delimiter detection), JSON, Parquet, Excel (xlsx/xls), SQLite
- Keyboard-driven navigation: vim-style `hjkl`, `g`/`G` jump, page up/down, column-width cycling
- Row filtering: text search `/`, select by value `,`, expression filter `|!=expr`
- Sorting: ascending/descending on any column, sort reset `r`
- Computed columns via `=expr` with arithmetic, string ops, and date math
- Pivot tables via `W` with column/aggregation autocomplete and input history
- Column statistics via `I`: type, count, nulls, unique, min, max, mean, median, mode, stdev, quantiles (q5–q95)
- Charts via `V`: histogram (Freedman-Diaconis binning), frequency bar chart, line chart (date × numeric), grouped bar chart (category × numeric). Pin a reference column with `!` for two-column charts. Aggregation popup for numeric charts
- Table transpose via `T` (in-place, no phantom columns)
- Frequency table via `F`
- Row selection, yank/paste, delete
- Sheet-from-selection via `"`
- Clipboard integration
- Column aggregators in footer (sum, count, avg, median, stdev, percentiles)
- Column type assignment via `t`
- Export to CSV/Parquet/Excel via Ctrl+S
- Pipe mode: `cat data.csv | tuitab -t csv`
- Everforest dark colour theme
- Non-English keyboard remapping
- Three binary aliases: `tuitab`, `ttab`, `tt`

[Unreleased]: https://github.com/denisotree/tuitab/compare/v0.4.2...HEAD
[0.4.2]: https://github.com/denisotree/tuitab/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/denisotree/tuitab/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/denisotree/tuitab/compare/v0.3.8...v0.4.0
[0.3.8]: https://github.com/denisotree/tuitab/compare/v0.3.7...v0.3.8
[0.3.7]: https://github.com/denisotree/tuitab/compare/v0.3.6...v0.3.7
[0.3.6]: https://github.com/denisotree/tuitab/compare/v0.3.5...v0.3.6
[0.3.5]: https://github.com/denisotree/tuitab/compare/v0.3.4...v0.3.5
[0.3.4]: https://github.com/denisotree/tuitab/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/denisotree/tuitab/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/denisotree/tuitab/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/denisotree/tuitab/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/denisotree/tuitab/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/denisotree/tuitab/compare/v0.1.5...v0.2.0
[0.1.5]: https://github.com/denisotree/tuitab/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/denisotree/tuitab/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/denisotree/tuitab/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/denisotree/tuitab/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/denisotree/tuitab/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/denisotree/tuitab/releases/tag/v0.1.0
