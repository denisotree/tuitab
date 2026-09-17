# Recipes

> 🇷🇺 [Эта страница на русском](../ru/recipes.md) · [← Documentation index](README.md)

Short, task-oriented how-tos. See [Keybindings](keybindings.md) for the full key
list.

## Summarise a column

- **Statistics** — press `I` for a Describe sheet: count, nulls, unique, min,
  max, mean, median, mode, stdev, and quantiles per column.
- **Frequency** — press `F` for value counts of the cursor column. Pin several
  columns and press `gF` for a multi-column frequency.
- **Footer aggregator** — press `+`, multi-select with `Space` (sum, avg,
  median, count, distinct, percentiles, min, max, …). Clear with `-`.
- **Quick total** — press `Z` to aggregate the visible / selected values into
  the status bar.

## Find & remove duplicates

- **Highlight duplicates** — `Shift+S` then `d` selects every row that has a
  duplicate.
- **Keep one per group** — `Shift+S` then `D` does a smart dedup; if you've
  pinned columns, it asks for a tiebreaker to decide which row to keep.
- **Drop by key** — pin the key columns, then `gD` keeps the first row per group.
- After selecting, press `d` to delete or `"` to split the selection into a new
  sheet.

## Compute a share of total

- **% of total** — on a numeric column, press `zf` to add a "% of total" column.
- **% within groups** — press `zF`, choose the partition columns, and get a
  share-of-partition column.

## Rank, running totals and group aggregates

- **Window column** — press `zw` on a column: `row_number`, `rank`,
  `dense_rank`, `cum_sum`, `lag`, `lead`, a group's `sum`/`avg`/`min`/`max`/
  `count` repeated on its rows, or `pct_of_total`. A rank asks which end comes
  first; a running total asks which column orders the rows — the table itself is
  not re-sorted — and both then ask for the partition columns. The new column
  lands next to the one it describes.
- **Group by** — pin the grouping columns with `!`, mark the aggregates you want
  with `+`, then press `gb`.
- **Sort by several keys** — sort with `[` / `]` as usual, then move to the next
  column and press `z[` / `z]` to add it as a further key. Chaining two plain
  sorts does not do this: the second is free to reorder rows that tie on its own
  key.

## Reshape

- **Transpose a row** — press `Enter` to flip the current row into a
  Column / Value view (good for wide records).
- **Transpose the table** — press `T` to swap rows and columns; press `T` again
  to undo.
- **Pivot** — see [Pivot tables](pivot.md).
- **Compare two tables** — `J`, pick the other table, choose **DIFF**, select the
  key columns. See [Finding what differs](join.md#finding-what-differs-between-two-tables).
- **Random sample** — `Shift+S` then `r`, enter how many rows.

## Edit data

- **One cell** — press `e` (or `E` to open it in `$EDITOR`).
- **Many rows at once** — select rows, then `ge` to set the same value on all of
  them.
- **Rename a column** — `ze`. **Delete** — `zd`. **Insert blank** — `zi`.
- **Find & replace** — `zr` (literal) or `zg` (regex) within the column.
- **Split a column** — `zx`, then enter the delimiter.
- **Decimal places** — `z.` more, `z,` fewer.
- **Add a row** — `o` for a blank one below the cursor, or `O` for a form with
  one field per column. The form is generated from the sheet, labels each field
  with the column's type, and checks the value as you type: one the column cannot
  hold is marked red and `Enter` refuses until it is fixed. A blank field is
  NULL, and the first `Enter` says how many are blank and waits for a second.

![The new-row form](https://raw.githubusercontent.com/denisotree/tuitab/master/.github/assets/rowform.gif)

## Set column types

Press `t` to pick a type: String, Integer, Float, Date, Datetime, Boolean,
Percentage, Currency, or File size. Choosing **Currency** then lets you pick
**USD · EUR · GBP · JPY**, which formats the column with the right symbol.

## Copy to the clipboard

| Keys | Copies |
|------|--------|
| `yc` | The current cell |
| `yr` | Selected rows (or current row) — choose TSV / CSV / JSON / Markdown |
| `yz` | The cursor column for selected rows |
| `yZ` | The whole cursor column |
| `yR` | The whole table — choose TSV / CSV / JSON / Markdown |

Mark columns with `zs` to include only those in `yr` / `yR`. `P` pastes rows from
the clipboard; `p` pastes into the cell under the cursor.

## Export to a file

Press `Ctrl+S` and enter a path. The format follows the extension:

| Extension | Format |
|-----------|--------|
| `.csv` / `.tsv` | CSV / TSV |
| `.parquet` | Parquet |
| `.arrow` / `.feather` / `.ipc` | Arrow / Feather |
| `.json` | JSON |
| `.jsonl` | JSONL / NDJSON |
| `.yaml` / `.yml` | YAML |
| `.toml` | TOML |
| `.xlsx` / `.xls` | Excel |
| `.db` / `.sqlite` / `.sqlite3` | SQLite |
| `.duckdb` / `.ddb` | DuckDB |

Converting between structured formats is just a different extension: open
`config.toml`, save as `config.yaml`. Saving a plain table (CSV, Parquet, a
pivot) to JSON/YAML/TOML asks which shape to produce and remembers the answer for
the rest of the session; a sheet that already carries a document is never asked.
Saving to a database asks for a table name and writes real column types — see
[Databases](database.md).

`Tab` autocompletes the path. Undo any edit with `U`, redo with `Ctrl+R`, or
reload from disk with `R`.

## Filter to what matters

- **Search** — `/` highlights cells matching a regex; `n` / `N` to step through.
- **Select by value** — `,` selects rows equal to the current cell.
- **Select by expression** — `|` then `!=expr`, e.g. `|!=amount > 1000`
  (see [Expressions](expressions.md)).
- Then `"` to make a sheet from the selection, or `d` to delete it.

## See also

- [Databases](database.md) — editing a table and writing it back.
- [Expressions](expressions.md) — the language behind `=`, `|` and the pivot formula.
