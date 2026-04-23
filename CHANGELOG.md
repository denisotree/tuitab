# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/denisotree/tuitab/compare/v0.3.3...HEAD
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
