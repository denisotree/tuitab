# tuitab

[![CI](https://github.com/denisotree/tuitab/actions/workflows/ci.yml/badge.svg)](https://github.com/denisotree/tuitab/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/tuitab.svg)](https://crates.io/crates/tuitab)
[![docs.rs](https://img.shields.io/docsrs/tuitab)](https://docs.rs/tuitab)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/denisotree/tuitab/blob/master/LICENSE)

A fast, keyboard-driven terminal explorer for tabular data.
Open CSV, JSON, Parquet, Excel and SQLite files directly in your terminal —
filter, sort, pivot, compute new columns, and visualise distributions without
leaving the shell.

```text
tuitab data.csv
tuitab orders.csv customers.csv   # open multiple files as a browseable list
ttab   data.csv                   # short alias
ttb    data.csv                   # shortest alias
cat data.csv | tuitab -t csv
```

---

## Features

- **Multi-format input** — CSV/TSV (auto-delimiter), JSON, Parquet, Excel (xlsx/xls), SQLite, DuckDB
- **Keyboard-driven navigation** — vim-style `hjkl`, `g`/`G` jump, column-width resize
- **Filtering** — regex search `/`, row selection by value `,`, expression filter `|!=expr`
- **Sorting** — ascending/descending on any column, multi-key sort
- **Computed columns** — `=expr` syntax with arithmetic, string ops, date math
- **JOIN** — `J` opens a step-by-step wizard to join the current table with another file or open sheet (INNER / LEFT / RIGHT / FULL OUTER, multi-column keys)
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
tuitab [OPTIONS] [FILES]...

Arguments:
  [FILES]...  One or more files to open, a directory, or '-' for stdin.
              Pass multiple files to browse them as a list.

Options:
  -d, --delimiter <CHAR>   Column delimiter (auto-detected if omitted)
  -t, --type <FORMAT>      Data format when reading from stdin (csv, json, parquet)
  -h, --help               Print help
  -V, --version            Print version
```

### Open multiple files at once

```sh
tuitab orders.csv customers.csv products.csv
```

A directory-style listing opens with all three files as rows. Press `Enter` on
any row to open that file. Press `Esc` or `q` to return to the list.

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
| `J` | JOIN with another table |
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

## JOIN

Press `J` in normal mode to start a step-by-step wizard that joins the current
table with another table.

### Step 1 — pick the right-hand table

A popup lists two options:

- **`[Browse file...]`** — type a file path (Tab for autocomplete). Any format
  tuitab can open is accepted: CSV, Parquet, JSON, Excel, SQLite, DuckDB.
- **Open sheets** — if you already have other sheets open in the stack (e.g.
  you navigated to them earlier), they appear here and can be selected directly.

### Step 2 — choose join type

| Option | SQL equivalent | Rows kept |
|--------|---------------|-----------|
| `INNER` | `INNER JOIN` | Only rows with a match in both tables |
| `LEFT` | `LEFT JOIN` | All rows from the left table; unmatched right cells are null |
| `RIGHT` | `RIGHT JOIN` | All rows from the right table; unmatched left cells are null |
| `OUTER` | `FULL OUTER JOIN` | All rows from both tables |

### Step 3 — select left key columns

A checkbox list of the current table's columns appears. Press `Space` to toggle
columns that will be used as join keys. The order of selection matters: key 1
on the left matches key 1 on the right. Press `Enter` to continue.

### Step 4 — select right key columns

Same checkbox list for the right-hand table. Columns whose names match the
selected left keys are pre-selected. Adjust as needed and press `Enter` to
execute the join.

The key count must match: if you selected two left keys, select exactly two
right keys. An error is shown in the status bar if they don't match.

### Result

A new sheet is pushed onto the stack with the title `left JOIN right`. Press
`Esc` or `q` to return to the original table.

Duplicate column names (non-key columns present in both tables) receive a
`_right` suffix in the result automatically.

### Example

```sh
# orders.csv has columns: order_id, customer_id, amount
# customers.csv has columns: customer_id, name, country

tuitab orders.csv
```

1. Press `J`
2. Select `[Browse file...]`, type `customers.csv`, press `Enter`
3. Select `LEFT`, press `Enter`
4. Toggle `customer_id` on the left, press `Enter`
5. Toggle `customer_id` on the right, press `Enter`

Result: orders enriched with customer `name` and `country`.

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
See [CONTRIBUTING.md](https://github.com/denisotree/tuitab/blob/master/CONTRIBUTING.md) for guidelines.

## License

Apache-2.0 — see [LICENSE](https://github.com/denisotree/tuitab/blob/master/LICENSE).
