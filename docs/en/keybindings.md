# Keybindings

> 🇷🇺 [Эта страница на русском](../ru/keybindings.md) · [← Documentation index](README.md)

The complete command reference. Press `?` in tuitab for a built-in summary.
All keys below are for **Normal mode** unless noted. Many commands act on the
**cursor column** or the **selected rows**.

## Navigation

| Key | Action |
|-----|--------|
| `h` `j` `k` `l` / arrows | Move cursor left / down / up / right |
| `gg` | Jump to the first row |
| `G` / `End` | Jump to the last row |
| `Home` | Jump to the first row |
| `Ctrl+F` / `PageDown` | Page down |
| `Ctrl+B` / `PageUp` | Page up |
| `q` / `Esc` | Quit — or pop back one sheet if you've drilled in |

## Sorting

| Key | Action |
|-----|--------|
| `[` | Sort ascending by the cursor column |
| `]` | Sort descending by the cursor column |
| `gb` | Group by the pinned columns, computing the aggregates marked with `+` |
| `zw` | Add a window column: rank, running total, lag/lead, group share. A running total asks which column orders the rows — the table itself is not re-sorted — then a direction, then the partitions |
| `z[` | Add the cursor column as a further sort key, ascending |
| `z]` | Add the cursor column as a further sort key, descending |
| `r` | Reset to the original row order |

## Search & row selection

| Key | Action |
|-----|--------|
| `/` | Search — highlights cells matching a regex |
| `n` / `N` | Jump to next / previous match |
| `c` | Clear search highlights |
| `,` | Select rows whose cell equals the current value |
| `\|` | Select rows by regex, or by expression with the `!=` prefix (e.g. `\|!=age > 30`) |
| `s` / `u` | Select / unselect the current row |
| `gs` / `gu` | Select all / unselect all rows |
| `gt` | Invert the current selection |
| `Shift+S` `r` | Select **N** random rows |
| `Shift+S` `d` | Select all rows that have duplicates |
| `Shift+S` `D` | Smart dedup — keeps one row per group (asks for a tiebreaker if columns are pinned) |
| `d` | Delete the selected rows |
| `"` | Create a new sheet from the selected rows |

See [Expressions](expressions.md) for the `|!=` filter language.

## Columns

| Key | Action |
|-----|--------|
| `!` (`Shift+1`) | Pin / unpin the column (stays visible when scrolling; used by charts & pivot) |
| `_` | Cycle the column width |
| `g_` | Cycle widths for all columns |
| `=` | Add a computed column from an [expression](expressions.md) |
| `t` | Open the column **type** menu (String, Integer, Float, Date, Datetime, Boolean, Percentage, Currency, File size) — choosing **Currency** lets you pick USD / EUR / GBP / JPY |
| `+` | Add a column footer aggregator (multi-select with `Space`) |
| `-` | Clear the column's aggregators |
| `Z` | Quick-aggregate the visible / selected values into the status bar |

### Column operations (`z` prefix)

| Key | Action |
|-----|--------|
| `ze` | Rename the column |
| `zd` | Delete the column |
| `zi` | Insert an empty column |
| `zs` / `zu` | Mark / unmark the column (marked columns are shown with `*` and respected by copy) |
| `z←` / `zh` | Move the column left |
| `z→` / `zl` | Move the column right |
| `z.` / `z>` | Increase decimal precision |
| `z,` / `z<` | Decrease decimal precision |
| `zf` | Add a "% of total" column |
| `zF` | Add partitioned "% of total" columns (pick the partition columns) |
| `zr` | Find & replace text in the column |
| `zg` | Find & replace with a regex |
| `zx` | Split the column by a delimiter |
| `zo` | On a directory listing: reopen the selected file as a chosen format |
| `zEnter` | Dive into the node in the current cell (JSON/YAML/TOML) |

## Rows, sheets & analytics

| Key | Action |
|-----|--------|
| `Enter` | Transpose the current row into a Column/Value view (drill-down); on a JSON/YAML/TOML sheet, dive into the row's node |
| `T` | Transpose the whole table (press again to undo) |
| `I` | Describe sheet — per-column statistics |
| `F` | Frequency table for the cursor column |
| `gF` | Multi-column frequency table (groups by the pinned columns) |
| `gD` / `gd` / `gU` | Deduplicate by the pinned columns (keeps the first row per group) |
| `V` | [Chart](charts.md) the cursor column |
| `W` | [Pivot table](pivot.md) |
| `J` | [JOIN](join.md) with another table |
| `e` | Edit the current cell |
| `E` | Edit the current cell in `$EDITOR`; on a JSON/YAML/TOML sheet a container cell opens as its real subtree, so keys can be added, removed and reordered |
| `m` | Cycle how the current node is projected: records / key-value / scalars |
| `(` / `)` | Expand a nested column into one column per key, or fold it back |
| `ge` | Bulk-edit — set the same value on every selected row |
| `o` | Add an empty row below the cursor |
| `O` | New-row form — one field per column, each checked against the column's type |

The form (`O`) generates itself from the sheet's columns: `↑` `↓` or `Tab` /
`Shift+Tab` walk the fields, `←` `→` move within one, `Enter` inserts the row at the
end of the table, `Esc` cancels. A value the column cannot hold is marked as you type
and `Enter` refuses until it is fixed — so an integer column never quietly turns into
text. A field left empty is NULL, not an empty string; if any field is still blank the
first `Enter` says how many and waits, and a second `Enter` inserts. On a database
sheet `\N` also means NULL, the same as when editing a cell. Not available on a
JSON/YAML/TOML sheet.

## Clipboard (`y` prefix)

| Key | Action |
|-----|--------|
| `yc` | Copy the current cell |
| `yr` | Copy selected rows (or the current row) — pick a format |
| `yz` | Copy the cursor column for the selected rows — pick a format |
| `yZ` | Copy the entire cursor column — pick a format |
| `yR` | Copy the entire table — pick a format |
| `p` | Paste the clipboard into the cell under the cursor |
| `P` | Paste rows from the clipboard |

Row/table copies offer **TSV · CSV · JSON · Markdown**; column copies offer
newline-, comma-, or quoted-comma-separated. `yr` and `yR` respect columns
marked with `zs`.

## File

| Key | Action |
|-----|--------|
| `Ctrl+S` | Save / export / convert (CSV, TSV, Parquet, Arrow/Feather, JSON, JSONL, YAML, TOML, Excel, SQLite, or DuckDB) |
| `R` | Reload the file from disk |
| `U` / `Shift+U` | Undo (up to 50 steps) |
| `Ctrl+R` | Redo |
| `?` | Toggle the help overlay |

Saving a plain table (CSV, Parquet, SQL, a pivot) to JSON/YAML/TOML asks which shape to
produce — `[{col: val}, …]`, `{col: [val, …]}`, or `{a: b}` for a two-column table — and
remembers the answer for the rest of the session. A sheet that already carries a
document is never asked: its tree is re-serialised as it is. Saving onto a database
shows every statement first — see [Databases](database.md).

## Charts view

| Key | Action |
|-----|--------|
| `h` `k` / `←` `↑` | Previous bar / series |
| `l` `j` / `→` `↓` | Next bar / series |
| `Enter` | Drill down into the selected bar |
| `V` / `q` / `Esc` | Close the chart |

## Input modes

When you're typing into a prompt (search, expression, pivot formula, rename,
save path, …):

| Key | Action |
|-----|--------|
| `←` `→` `Home` `End` | Move the text cursor |
| `Backspace` / `Delete` | Delete left / right |
| `Tab` | Autocomplete (column names / file paths, where available) |
| `↑` / `↓` | Previous / next entry in history (expression & pivot inputs) |
| `Enter` | Apply |
| `Esc` | Cancel |

## JSON / YAML / TOML sheets

These formats open as a table over the real document, not as a flattened copy. The
format comes from the extension, from `--type`, or — for an unrecognised extension —
from the contents: `deploy.conf` holding TOML opens as TOML. A file with no extension at
all still defaults to CSV, and is only re-read as JSON when it starts with `[` or `{`.

| Key | Action |
|-----|--------|
| `Enter` | Dive into the node of the current row |
| `zEnter` | Dive into the node of the current cell |
| `yp` | Copy the document path of the current cell (`servers[1].host`) |
| `q` / `Esc` | Go back up one level |
| `g/` | Search the whole document — see below |
| `gp` | Go to a node by its document path |
| `gq` | Run a jq query and open its result as a sheet |
| `m` | Cycle the layout: records / key-value / scalars |
| `(` | Expand the cursor column of containers into `parent.key` / `parent[0]` columns |
| `)` | Fold the innermost expansion back into one column |
| `e` | Edit a scalar — the value keeps its type, and a string stays a string |
| `E` | Edit a container as text in `$EDITOR` (add, remove or reorder keys) |
| `d` | Delete the selected rows — removes them from the document |
| `Ctrl+S` | Save; a different extension converts the format |

`gq` runs a jq program over the document — `.[] | select(.ok)`,
`.users | to_entries | map({name: .key, age: .value.age})` — and opens the result as an
ordinary sheet: diving, editing and saving all work on it, because the result is just
another document. It keeps the source's format, so saving writes the kind of file you
were already looking at, but it does not inherit the source path: `Ctrl+S` on a query
result will not offer to overwrite what the query ran against.

`/` searches what is on screen, which on a document sheet is one subtree rendered into
cells — it cannot see a key three levels down. `g/` searches the tree itself and pushes a
sheet of hits: one row per match, with its path, the value there, and whether the pattern
matched the key or the value. The paths are absolute from the document root, so `g/`
means the same thing whether you run it at the top or three levels in. `Enter` on a hit
opens the node — a container as itself, a
scalar's parent so the value is seen in context — with the cursor on the match. Nothing
is opened automatically. The pattern is a case-insensitive regex, and the list stops at
2000 hits, which the status line says. A hit list is a snapshot: if the document changes
after the search, opening a hit is refused rather than risking a path that now points
somewhere else — run `g/` again.

When there is nothing more urgent to report, the status line shows the document path of
the cell under the cursor — `servers[1].host` — which is what you need to refer to a
value anywhere else. `yp` copies it and `gp` jumps to one, prefilled with where you are;
the three use the same syntax, so a copied path can be pasted straight back. A key that
would be ambiguous in dotted form is written `["awkward.key"]`. A path that does not
resolve says how far it got — `servers[1] exists` — instead of only that it failed.

A conversion that cannot carry everything says so in the status line as it saves —
multi-document YAML written as JSON, or a commented TOML written as anything else.

In a records view a column is a key, so `zd`, `ze`, `zi` and `z←`/`z→` change that key
in every record that has it: delete it, rename it in place, add it as null before the
cursor column, or swap it with its neighbour. A rename that clashes with an existing
key in any record changes nothing. Key/value and list views have fixed columns.

Operations that would reshape the table without a matching change in the document —
paste, computed columns, find & replace and split — are refused with a note pointing
at `E`.

Every sheet in a dive chain shares one document, so an edit made three levels down is
there when you come back up, and `U` undoes it at any level.

Expanding is a view operation: `(` never changes the document, so saving still writes
the real nesting rather than flattened `parent.key` names. Expanded cells stay editable
and write to the node they actually address.

Saving re-serialises the document, so nesting, key order and TOML datetimes survive.

A TOML file saved back as TOML keeps its comments and layout: it is written through its
own source rather than rebuilt, so editing one value in a config leaves the rest of the
file byte-for-byte alone. Two things still go: the comment on a key you *rename* (a
rename is a removal plus an insertion as far as the file is concerned), and comments
inside an array or table whose length changed. Converting to another format drops
comments entirely — YAML and JSON have nowhere to put them.

## Keyboard layouts

Non-QWERTY layouts — **ЙЦУКЕН** (Russian), **QWERTZ** (German), and **AZERTY**
(French) — are transparently remapped to their QWERTY positions, so the hotkeys
work without switching layout.
