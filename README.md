# tuitab

[![CI](https://github.com/denisotree/tuitab/actions/workflows/ci.yml/badge.svg)](https://github.com/denisotree/tuitab/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/tuitab.svg)](https://crates.io/crates/tuitab)
[![docs.rs](https://img.shields.io/docsrs/tuitab)](https://docs.rs/tuitab)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A fast, keyboard-driven terminal explorer for tabular data.
Open CSV, JSON, Parquet, Excel and SQLite files directly in your terminal —
filter, sort, pivot, compute new columns, and visualise distributions without
leaving the shell.

```text
tuitab data.csv
ttab   data.csv   # short alias
ttb    data.csv   # shortest alias
cat data.csv | tuitab -t csv
```

---

## Features

- **Multi-format input** — CSV/TSV (auto-delimiter), JSON, Parquet, Excel (xlsx/xls), SQLite
- **Keyboard-driven navigation** — vim-style `hjkl`, `g`/`G` jump, column-width resize
- **Filtering** — regex search `/`, row selection by value `,`, expression filter `|!=expr`
- **Sorting** — ascending/descending on any column, multi-key sort
- **Computed columns** — `=expr` syntax with arithmetic, string ops, date math
- **Pivot tables** — `W` opens a pivot formula input with autocomplete
- **Statistics** — `I` shows per-column stats (type, count, nulls, unique, min, max, mean, median, mode, stdev, quantiles)
- **Charts** — `V` renders histogram, frequency bar chart, line chart (date × numeric), and grouped bar chart (category × numeric). Pin a reference column with `!` first for 2-column charts
- **Transpose** — `T` transposes the current table in-place
- **Export** — Ctrl+S saves as CSV/Parquet/Excel
- **Clipboard** — yank rows to clipboard, paste
- **Aggregators** — column footers with sum, count, avg, median, percentiles
- **Theme** — Everforest dark colour palette

---

## Installation

### Cargo (crates.io)

```sh
cargo install tuitab
```

This installs three aliases: `tuitab`, `ttab`, and `tt`.

### Homebrew (macOS / Linux)

```sh
brew tap denisotree/tuitab
brew install tuitab
```

### apt (Debian / Ubuntu)

```sh
# Download the .deb from the latest GitHub release, then:
sudo dpkg -i tuitab_0.1.0_amd64.deb
```

Or add the apt repository (see [releases page](https://github.com/denisotree/tuitab/releases)).

### Arch Linux (AUR)

```sh
# Using yay:
yay -S tuitab

# Or build manually:
git clone https://aur.archlinux.org/tuitab.git
cd tuitab && makepkg -si
```

### Pre-built binaries

Download for your platform from the [Releases page](https://github.com/denisotree/tuitab/releases).

---

## Usage

```text
tuitab [OPTIONS] [FILE]

Arguments:
  [FILE]  Path to a data file, directory, or '-' to read from stdin

Options:
  -d, --delimiter <CHAR>   Column delimiter (auto-detected if omitted)
  -t, --type <FORMAT>      Data format when reading from stdin (csv, json, parquet)
  -h, --help               Print help
  -V, --version            Print version
```

### Pipe mode

```sh
psql -c "SELECT * FROM orders" --csv | tuitab -t csv
sqlite3 app.db ".mode csv" ".headers on" "SELECT * FROM users" | tuitab -t csv
```

---

## Key bindings

### Navigation

| Key | Action |
|-----|--------|
| `h` / `l` | Move column left / right |
| `j` / `k` | Move row down / up |
| `g` / `G` | Jump to first / last row |
| `0` / `$` | Jump to first / last column |
| `PgUp` / `PgDn` | Page up / down |

### Filtering & search

| Key | Action |
|-----|--------|
| `/` | Start text search |
| `n` / `N` | Next / previous match |
| `c` | Clear search highlight |
| `,` | Select rows by cell value |
| `\|` | Filter rows by expression (e.g. `\|!=age > 30`) |

### Columns

| Key | Action |
|-----|--------|
| `=` | Add computed column (expression) |
| `t` | Set column type |
| `!` | Pin / unpin column (used as reference for charts & pivot) |
| `_` | Cycle column width |
| `+` / `-` | Add / clear column aggregator |
| `Z` | Quick aggregate (sum visible selection) |

### Sorting

| Key | Action |
|-----|--------|
| `Enter` | Sort ascending by current column |
| `Shift+Enter` | Sort descending |
| `r` | Reset sort order |

### Views

| Key | Action |
|-----|--------|
| `V` | Chart current column (histogram / frequency). With a pinned column: line chart or bar chart |
| `I` | Column statistics (describe sheet) |
| `T` | Transpose table |
| `F` | Frequency table |
| `W` | Pivot table |
| `?` | Help popup |

### Rows

| Key | Action |
|-----|--------|
| `s` / `u` | Select / unselect row |
| `"` | Create new sheet from selected rows |
| `d` | Delete selected rows |
| `y` prefix | Yank (copy) operations |
| `p` | Paste rows |

### File

| Key | Action |
|-----|--------|
| `Ctrl+S` | Save file |
| `q` | Quit |

---

## Chart types

Press `V` on a column to open a chart. Behaviour depends on whether a reference
column is pinned with `!`:

| Cursor column | Pinned column (`!`) | Chart |
|--------------|---------------------|-------|
| Numeric | — | Histogram (Freedman-Diaconis bins) |
| Categorical | — | Frequency bar chart |
| Numeric | Date / Datetime | Line chart (aggregation popup: sum/count/avg/median/min/max) |
| Categorical | Date / Datetime | Line chart (auto count) |
| Numeric | Categorical | Grouped bar chart (aggregation popup) |

Charts automatically switch between vertical and horizontal layout based on label length.

---

## Expression syntax

Used in `=` (computed columns) and `|!=` (row filter):

```text
age * 2
price / 100
name contains "Smith"
date > 2024-01-01
amount in (100, 200, 300)
category != "N/A"
```

---

## Acknowledgements

tuitab is inspired by [VisiData](https://www.visidata.org) — a brilliant terminal
spreadsheet multitool by [Saul Pwanson](https://github.com/saulpw). If you find
tuitab useful, check out VisiData too.

## Contributing

Bug reports, feature requests, and pull requests are welcome.
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache-2.0 — see [LICENSE](LICENSE).
