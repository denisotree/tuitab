# Databases

> 🇷🇺 [Эта страница на русском](../ru/database.md) · [← Documentation index](README.md)

SQLite and DuckDB are the two formats tuitab does not merely read. A table
opened from one can be edited and written back into the same file — and tuitab
shows every statement it is about to run before a single one of them does.

![Editing a table and saving it back](https://raw.githubusercontent.com/denisotree/tuitab/master/.github/assets/database.gif)

## Open a table

```sh
tuitab shop.sqlite
tuitab warehouse.duckdb
tuitab app.db           # which engine it is comes from the file's own header
```

The first screen is the database's own contents — every table and view, with its
row and column counts. `Enter` opens the highlighted one; `Esc` / `q` goes back.
The title bar then reads `shop.sqlite :: orders`, and `[*]` appears next to it as
soon as something is unsaved.

A view opens read-only. So does the overview itself: saving either onto the
database it came from is refused, with `This table or view is read-only; save to
a different file instead.`

### DuckDB 1.x and 2.x files

DuckDB 2.0 changed its file format, and DuckDB 1.x cannot open a file in the new
one. tuitab reads and edits both, and a file stays in the format it was made
with — editing a 1.x file never turns it into one 1.x can no longer open. A new
`.duckdb` that tuitab creates is in the 1.x format, so any DuckDB opens it.

## Edit

Everything that works on an ordinary sheet works here: `e` edits a cell, `ge`
sets one value on every selected row, `d` deletes selected rows, `P` pastes rows,
`o` adds a blank one and `O` opens a [form](recipes.md#edit-data) checked against
the column types. Columns take `zi` (insert), `zd` (delete), `ze` (rename), `t`
(change type), and `z` + `←` / `→` to reorder.

**NULL is visible and typable.** A NULL cell shows as `NULL` in italics rather
than as a blank, and typing `\N` into a cell on a database sheet writes a real
NULL. An empty string stays an empty string.

## Save it back

`Ctrl+S` prefills the path the table came from. Confirm it and nothing is written
yet — a popup opens with the exact SQL:

```text
┌ shop.sqlite → orders · 1 UPDATE ─────────────────────────┐
│ UPDATE "orders" SET "qty" = 12 WHERE "rowid" = 1         │
│                                                          │
│ ↑↓ · PgUp/PgDn · g/G · Enter run · Esc cancel            │
└──────────────────────────────────────────────────────────┘
```

Scroll it with `↑` `↓`, `PgUp` / `PgDn` and `g` / `G`. `Enter` runs the whole
list in one transaction; `Esc` cancels and nothing happens. The rest of the
database — other tables, indexes, views, triggers — is untouched either way.

Structural changes appear as `ALTER TABLE` statements in the same list:
`ALTER TABLE "orders" DROP COLUMN "note"`, grouped under a `SCHEMA` heading.

### When the table has to be rebuilt

Neither engine can reorder columns, and SQLite has no `ALTER COLUMN`, so
reordering columns — or changing a column's type on SQLite — means the table is
dropped and created again with its rows copied across. The popup says so before
you confirm:

```text
-- this table will be rebuilt: dropped and created again, with its rows copied
```

A rebuild reproduces the column names, types, `NOT NULL`, `DEFAULT` and a
single-column primary key. Anything it could not carry across intact — a `CHECK`,
a `UNIQUE`, a foreign key, a collation — is a refusal rather than a silent loss:
`'orders' has to be rebuilt to do this, and tuitab will not rebuild it: …`. Save
to a different file instead.

### Saving somewhere else

Point `Ctrl+S` at a different `.db` and tuitab copies the **whole database**
first — every table, index, view and trigger — then applies your changes to the
copy. The original is left exactly as it was. The destination has to be a file
that does not exist yet; an existing one is refused with `… already exists.
Saving a database elsewhere writes a fresh copy of the whole file — remove it
first or choose another name.`

### Derived sheets add a table, they do not edit one

A pivot, a JOIN result, a frequency table or a Describe sheet has no table behind
it. Saving one onto a database asks for a table name and **creates** that table;
the tables already in the file are not touched. This is also how you park a
result next to the data it came from.

## Build a database from nothing

Point tuitab at a database file that does not exist and it opens a blank sheet —
the title reads `inventory.sqlite [new]`:

```sh
tuitab inventory.sqlite
```

1. `zi` inserts a column and asks for its name; repeat for the rest.
2. `t` gives each one a type — Integer, Float, Date, Boolean, Currency and so on.
3. `o` adds a blank row, or `O` opens the form, which labels every field with its
   column's type and refuses a value the column cannot hold.
4. `Ctrl+S`, then a name for the table.

What lands on disk is a real typed table: an Integer column is declared `INTEGER`
and stores integers, a NULL stays NULL. SQLite and DuckDB both.

## See also

- [Getting started](getting-started.md) — opening files and formats.
- [Recipes](recipes.md) — editing, clipboard, export.
- [MCP server](mcp.md) — letting an assistant change a table, behind a flag and a
  two-step handshake.
