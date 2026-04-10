# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.3] - 2026-04-10

### Added
- Human-readable file sizes in directory listing (e.g. "1.2 KB", "3.4 MB") instead of raw byte counts
- TXT files are now read as a single-column table — each line becomes one row in a "Line" column
- SQLite files now open a table browser showing all tables with name, row count, column count, and SQL definition; pressing Enter drills into the selected table
- Column selection in z-mode: `zs` marks a column with `*`, `zu` unmarks it; pressing `"` with selected columns creates a new sheet containing only those columns (combines with row selection)

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

[Unreleased]: https://github.com/denisotree/tuitab/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/denisotree/tuitab/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/denisotree/tuitab/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/denisotree/tuitab/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/denisotree/tuitab/releases/tag/v0.1.0
