# Getting started

> 🇷🇺 [Эта страница на русском](../ru/getting-started.md) · [← Documentation index](README.md)

## Install

The quickest route is Cargo:

```sh
cargo install tuitab
```

This installs three identical commands — `tuitab` and the shorter aliases `ttab`
and `ttb`. `cargo binstall tuitab` skips the build and fetches the pre-built
`tuitab` from the release, without the aliases. Other options (Homebrew, AUR,
Debian, pre-built binaries) are in the
[main README](https://github.com/denisotree/tuitab/blob/master/README.md#installation).

## Open a file

Pass any supported file as the first argument:

```sh
tuitab data.csv
tuitab report.parquet
tuitab books.xlsx
tuitab app.db
```

| Format | Extensions | Notes |
|--------|-----------|-------|
| CSV / TSV | `.csv`, `.tsv` | Delimiter auto-detected; override with `-d` |
| JSON | `.json` | Opens as a tree over the real document |
| JSONL / NDJSON | `.jsonl`, `.ndjson` | One record per line |
| YAML | `.yaml`, `.yml` | Tree, like JSON |
| TOML | `.toml` | Tree, like JSON |
| Parquet | `.parquet` | Columnar, fast |
| Arrow / Feather | `.arrow`, `.feather`, `.ipc` | Read and written |
| Excel | `.xlsx`, `.xls` | Multi-sheet workbooks open as a sheet list |
| Markdown | `.md`, `.markdown` | A page is one row: frontmatter fields are columns, the text is `body`, the path is `file` |
| SQLite | `.sqlite`, `.sqlite3`, `.db`, or any name | Tables open as a table list; recognised by the file header, so `.aup3`, `.gnucash` or no extension at all work too |
| DuckDB | `.duckdb`, `.ddb`, `.db`, or any name | Which engine a file is comes from its own header, not its name |

For workbooks and databases, tuitab first shows an overview (sheets or tables);
press `Enter` on a row to open it, `Esc` / `q` to go back. Tables can be edited
and saved back — see [Databases](database.md).

JSON, YAML and TOML are not flattened into a copy: the sheet is a view over the
document itself, `Enter` dives into a nested object or list, and saving
re-serialises the tree. Converting between them is just a different extension on
`Ctrl+S`.

![A TOML file edited in place and saved as YAML](https://raw.githubusercontent.com/denisotree/tuitab/master/.github/assets/tree.gif)

### Force a format

`-t` is required for stdin, which reads csv, tsv, txt, json, jsonl, yaml and toml.
For a file it overrides both the extension and the contents:

```sh
tuitab -t yaml deploy.conf     # a YAML file that is not called .yaml
tuitab -t csv export.dat       # a CSV file under a name nothing recognises
```

An unknown extension is decided by the contents, so `deploy.conf` usually opens
correctly with no flag at all, and a SQLite or DuckDB file opens as a database
whatever it is called — `-t sqlite` is not needed.

### Open a database that does not exist yet

```sh
tuitab inventory.sqlite        # nothing there — a blank sheet opens
```

Add columns, give them types, add rows, `Ctrl+S`, and tuitab writes a real typed
table. See [Databases](database.md).

### Override the delimiter

When auto-detection guesses wrong (or you have a `;`-separated file):

```sh
tuitab data.csv -d ';'
```

## Browse several files

Pass more than one path and tuitab opens a **file list** instead of a single
table:

```sh
tuitab orders.csv customers.csv products.parquet
```

Each file becomes a row showing its name, size, and modified time. `Enter` opens
the highlighted file; `Esc` / `q` returns to the list.

## Read many files as one table

A quoted glob pattern reaches tuitab instead of the shell, and every file it
matches is read as one table:

```sh
tuitab 'data/*.csv'
tuitab 'content/**/index.md'
```

The quotes are the whole difference. Unquoted, the shell expands the pattern into
several arguments and you get the file list above — useful for picking one file,
not for reading them together.

Tabular files have to agree on their columns; the one that does not is named in
the error rather than folded in, because a table stacked out of frames that
disagree is a mess with a row count. Markdown pages are records, so a pattern over
them unions instead: a field one page lacks arrives NULL, which is what turns a
static site into a table you can group and count.

A pattern is not a file. The sheet has no path to reload from, and `Ctrl+S` asks
where to write rather than offering a name with a `*` in it.

## Browse a directory

Point tuitab at a folder (or run it with no arguments to use the current one):

```sh
tuitab ./reports/
tuitab               # current directory
```

You get a navigable file browser — open files with `Enter`, step back out with
`Esc` / `q`.

## Read from a pipe

tuitab reads from stdin when data is piped in. Tell it the format with `-t`:

```sh
cat data.csv | tuitab -t csv
psql -c "SELECT * FROM orders" --csv | tuitab -t csv
echo '[{"id":1,"name":"Alice"}]'   | tuitab -t json
```

Stdin understands `csv`, `tsv`, `txt`, `json`, `jsonl`/`ndjson`, `yaml`/`yml`
and `toml`. (`txt` is treated as CSV.) To read Parquet, Arrow, Excel, Markdown,
SQLite, or DuckDB, open the file by path instead of piping it. `-` works as an
explicit stdin path: `cat data.csv | tuitab -t csv -`.

## The screen

```text
┌ sample.csv ───────────────────────────────────────┐
│  id  s│ name        s│ age #│ salary  ~│ department s│   ← header row (name + type icon)
│▌ 1    │ Alice Johnson│ 30   │ 75000.00 │ Engineering │   ← cursor row (highlighted)
│  2    │ Bob Smith    │ 45   │ 92000.50 │ Management  │
│  …                                                  │
├─────────────────────────────────────────────────────┤
│ NORMAL   Loaded 20 rows                  row 3/20 col 3/5 │  ← status bar
└─────────────────────────────────────────────────────┘
```

- The **header** shows each column name plus a small **type icon**
  (`s` string, `#` integer, `~` float, `d` date, `t` datetime, `%` percentage,
  `$` currency, `B` file size, `?` boolean).
- The **status bar** shows the current mode, a message, and your position.
- Press `?` at any time for the in-app help overlay.

## Next steps

- Learn the [keybindings](keybindings.md).
- Try [charts](charts.md), [pivot tables](pivot.md), and [JOINs](join.md).
- Edit a table and write it back with [Databases](database.md).
- Let an assistant compute over your files with the [MCP server](mcp.md)
  (`tuitab --mcp`).
- Browse task-oriented [recipes](recipes.md).
