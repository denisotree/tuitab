# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.6] - 2026-09-16

### Added

- **Columns of a JSON/YAML/TOML records view can be deleted, renamed, inserted
  and moved.** A column there is a key, so `zd`, `ze`, `zi` and `z←`/`z→` now
  change that key in every record that holds it instead of being refused with a
  pointer at `E`. Records are sparse: a record without the key is skipped. A
  rename that clashes with an existing key in any one record changes nothing; a
  new key is null and sits before the cursor column (TOML writes it once it is
  filled); a move that the first-seen key order would not show is rolled back
  and says so. The table is rebuilt from the document after each, so cell edits
  keep reaching the right node, and `U` undoes them in the document. Key/value
  and list views keep their fixed columns.

### Fixed

- **`ze` on a document sheet renamed only the table header.** The key in the
  document stayed as it was, so the new name vanished on the next reprojection
  and never reached the saved file.

### Security

- rustls bumped to 0.23.45 for RUSTSEC-2026-0285.

## [0.9.5] - 2026-08-19

### Added

- **A glob pattern now works on the command line, not only through MCP.**
  `tuitab 'data/*.csv'` reads every file the pattern matches as one table, and
  `tuitab 'content/**/index.md'` turns a directory of pages into a table of
  their frontmatter. The reading is the one the MCP server already gave it,
  through the same code: tabular files have to agree on their columns and the
  one that does not is named rather than folded in, while markdown pages are
  records and are unioned, a field a page lacks arriving NULL.

  The quotes are the whole difference. Unquoted, the shell expands the pattern
  first and you get the file listing, which is for picking one file rather than
  reading them together.

### Fixed

- **A quoted pattern used to open a blank sheet instead of the files.**
  `Path::exists` is false for `data/*.csv` however many files it matches, so
  the terminal took it for a name, found nothing there, and offered to create
  it — a writable one-column sheet titled `*.csv`, `Ctrl+S` away from a file
  with a star in its name. No error at any point, which is what made it worth
  fixing rather than documenting. A pattern that matches nothing now says so,
  and a pattern-backed sheet has no path to write back to, so `Ctrl+S` asks
  where to put it.

  A pattern passed alongside other arguments used to answer "no such file",
  which was the same lie in a different place; it now explains that a pattern
  goes on its own.

## [0.9.4] - 2026-08-18

### Fixed

- **A database over 10 MB opened as a listing you could not open anything from.**
  Past that size the file is read on a background thread, and that loader
  rebuilt the table but dropped everything else it knew — including the fact
  that it had opened a database. `Enter` reads exactly that to tell "this row
  names a table" from "this row is data", so with it gone the drill-in fell
  through to transposing the row, and transposing the transpose went on
  forever. Small databases were unaffected, which is why this survived four
  releases ([#43](https://github.com/denisotree/tuitab/issues/43)).

  The loader dropped two more things with it. A workbook of several sheets over
  10 MB opened its first sheet instead of the sheet list, leaving the others
  unreachable; and `--type` was ignored above the threshold, so
  `--type jsonl big.txt` read plain text. All three came from the same place:
  three separate answers to "what is this file", of which the background one was
  the poorest. There is now a single `open_target` every open goes through — the
  command line, the background loader, and opening a file from a directory
  listing — so size decides only where the work happens, never what the file is
  taken to be.

### Security

- **Bump `h2` 0.4.13 → 0.4.16** for RUSTSEC-2026-0258, unbounded empty DATA
  frames. It arrives through polars' `object_store` → `reqwest` → `hyper`, the
  S3 and Azure HTTP stack that tuitab never enters — it opens local files — so
  the code is as unreachable as the two quick-xml advisories the audit already
  ignores. Unlike those, this one has a fixed version to move to, so it is
  moved to rather than ignored.

### Documentation

- **The guides caught up with the last four releases.** `docs/en` and `docs/ru`
  were written once, at 0.5.0, and only `keybindings`, `expressions` and `mcp`
  were touched since — so the recipes still said `p` pastes rows (0.8.1 swapped
  it with `P`), getting started still said a `.db` is probed as SQLite and then
  DuckDB (0.8.0 made the file's own header decide), and the stdin format list
  named four of the seven it accepts. The format tables gained JSONL, YAML,
  TOML, Arrow and Markdown-with-frontmatter, the export table gained everything
  writable, and the recipes gained `zw`, `gb` and `z[`/`z]` from 0.7.0 and the
  `O` form from 0.9.3. The MCP page documents the schema fields it never
  mentioned — `format`, `delimiter`, `sample_rows`, `output.limit`, `columns` —
  and says that a database is refused without a `container` rather than
  gently preferring one.
- **A page about databases**, which the documentation had never had while the
  README carried two paragraphs on it: opening a table, editing it, what the
  confirmation popup shows, when a table has to be rebuilt and when that is
  refused, what saving elsewhere copies, and building a database from nothing.
  Everything on it was checked against the running binary rather than against
  this changelog — which is how one claim died: a pivot or JOIN saved onto a
  database does not refuse, it adds a table.
- **The keybinding pages' File table rendered as literal pipes.** A paragraph
  sat between the first row and the rest, which ends a markdown table, so `R`,
  `U`, `Ctrl+R` and `?` were text. Its `Ctrl+S` list was also missing Arrow and
  DuckDB, both writable.
- **`--type` in the README's usage block** still described itself as a stdin-only
  flag, nine lines below an example using it to override a file's extension.

### Fixed

- **`pivot.gif` had been demonstrating the wrong column since 0.8.0.** Pinning
  stopped reordering the frame that release — the pinned column is drawn in the
  frozen block without being moved — and the tape still stepped two columns
  right after pinning, landing on `units` instead of `category`. Nothing refuses
  that, so the recorded pivot spread across twenty-eight unit counts. Three
  new recordings alongside it: the `O` row form, editing a database table with
  the SQL shown first, and JSON/YAML/TOML as a document.

## [0.9.3] - 2026-08-18

### Added

- **`O` opens a form for the new row.** One field per column, generated from the
  sheet itself and labelled with the column's type. `↑` `↓` or `Tab` /
  `Shift+Tab` walk the fields, `←` `→` move inside one, `Enter` inserts the row
  at the end of the table, `Esc` cancels. Each value is checked against its
  column as it is typed: a value the column cannot hold is marked red with the
  reason under the fields, and `Enter` refuses until it is fixed. This is the
  first insert path that asks the question at all — editing a cell with `e`
  answers a value the column cannot hold by turning the whole column into text
  and saying "Cell updated", so an Int64 column loaded from a Parquet file could
  become strings without a word, and only a database save would ever complain.
  A field left blank is NULL rather than an empty string, and because a blank
  field is more often a row someone is still filling in than a deliberate NULL,
  the first `Enter` says how many are blank and waits for a second one. On a
  database sheet `\N` means NULL here too, the same as when editing a cell.

### Changed

- **`O` no longer inserts a blank row above the cursor** — that was the same
  thing `o` does, in the other direction, and the form is what the key is for
  now. `o` is unchanged: a blank row below the cursor.

## [0.9.2] - 2026-08-13

### Added

- **A source may be a glob pattern.** `data/*.csv`, `content/**/index.md`: every
  file it matches is read as one table, in sorted order, whatever the format.
  Until now any pattern answered "No such file" — `Path::exists` is false for a
  pattern however many files it would match — while the error for a directory of
  mixed extensions advised using one, an instruction nothing could follow. A
  pattern matching nothing now says exactly that (`glob matched no files: …`),
  which is a different problem from a path that is not there; files that do not
  agree on their columns are refused with the one that broke the agreement named.
- **Markdown with frontmatter is a table.** A page is one row: its frontmatter
  fields are columns (YAML between `---`, TOML between `+++`), the page text is
  `body`, and `file` is where it came from. Pages need not carry the same fields —
  one a page lacks arrives as NULL. With a pattern that makes a static site
  checkable against a database in a single call: `content/**/index.md` grouped,
  joined and counted, where before it needed an export script.

### Fixed

- **A directory in the MCP server lists its files, as the instructions always
  said.** It was handed instead to the CSV reader — the default for a path with no
  extension — which quietly concatenated a directory of like files and refused one
  holding a `cover.jpg` beside an `index.md`, in Polars' words and with Polars'
  advice. The terminal has always listed; now both do.

## [0.9.1] - 2026-08-12

### Fixed

- **A date in a spreadsheet came back as its serial number.** Excel stores a date
  as a count of days with a format hung on it, and tuitab read the count: the
  29th of January 2026 arrived as `46051`. It is now written the way every other
  reader here writes a date — `2026-01-29`, or `2026-01-29 14:30:00` when the
  cell carries a time — which is what the CSV loader leaves in a date column too.
  The bug is as old as the xlsx reader; it only became visible in 0.9.0, when
  those columns stopped being text and the number was left standing on its own.

## [0.9.0] - 2026-08-12

### Documentation

- The MCP guide (`docs/en/mcp.md`, `docs/ru/mcp.md`) had said nothing about
  writing since the tools arrived in 0.8.0. It now covers `output.table`,
  `pipelines`, backticked column names, and the whole two-step path for changing
  what already exists. The keybinding pages had `p` as "paste rows" from before
  0.8.1 swapped it with `P`.

### Changed

- **Overwriting a file the user already has needs `--mcp-write`, and is planned.**
  A report is no less theirs than a table, and `output.overwrite` on a `.csv` or
  an `.xlsx` used to replace one on a boolean alone — no flag, no plan, the old
  file gone before anybody could be told what it was. It now takes the same three
  things as replacing a table: the flag, `output.overwrite`, and a plan applied by
  name. The plan says what is about to go — the path, its size, when it was last
  written — against what would replace it. Writing a path that does not exist is
  still one call and needs no flag: there is nothing there to lose. `--mcp-write`
  accordingly means "may change what already exists", not "may change rows", and
  its help text says so.
- **Replacing a table through `output.overwrite` is now planned, not done.** It
  was the one destructive act in the whole MCP surface that happened inside a
  single call: `tuitab_write` shows the SQL and waits for `tuitab_write_apply`,
  while a query with `overwrite` dropped a table and wrote over it in one go —
  and it destroys more than any `set` can, the previous table entire along with
  the indexes and triggers hanging off it. It now answers with a plan id, the
  `DROP`/`CREATE`/`INSERT` statements, how many rows the old table holds against
  how many the new one will, and warnings for every index, trigger and view that
  goes with it. Nothing is written until that plan is applied. The refusal that
  precedes it says what is at stake — `'inventory' already exists and holds 2
  rows. Replacing it destroys them` — rather than naming the flag that would do
  it, which read as an instruction to re-send the call with the flag set.

  This matters where there is no person watching: an agent that answers a
  refusal by escalating cannot be talked out of it by a better error message, and
  a second deliberate call is the only gate that does not depend on the model
  behaving.

### Fixed

- **Every number in a spreadsheet was text.** Each cell reached tuitab through
  its string rendering, so a column of money came out `string` and `sum` over it
  answered "the column is string, and sum needs a numeric one" — an .xlsx source
  was unusable for the arithmetic the tool exists to do. A column is now offered
  to Int64 and then Float64, and keeps its text only when some cell is not a
  number; an empty cell is missing rather than the empty string, which is what
  gives the cast anything to bite on. Whole numbers stay whole: an id does not
  come back as `1.00`.
- **A NULL saved to .xlsx was written as the word "null"**, which read back as a
  perfectly good label and turned the whole column into text. It is a blank cell.
- **A column whose name has a space in it could not be referred to at all.**
  Backticks now quote a column name — `` `К выплате` * 1 `` — and every other
  form was worse than useless: bare broke on the space, `[...]` and backticks
  were rejected outright, and double quotes *parsed*, silently producing a
  column of nulls because a quoted string is a text literal. That last one is
  now refused in words that name backticks, unless the text is a number, which
  keeps `"3" * 1` — the documented way to coerce a string column — working.
- **A leading minus is an expression.** `-quantity` and `if(x, a, -b)` no longer
  answer "Unexpected token: Minus".
- **`tuitab_inspect` gives every spreadsheet sheet's size.** It answered
  `rows: null, columns: 0` while the instructions promised counts, so learning
  what a sheet held cost a call per sheet.
- **One failing pipeline no longer discards the others.** A call with several
  `pipelines` returns each one's result or its error, so a typo in the fourth
  question stops costing the three answers beside it. A single `ops` call still
  fails as a whole, and a call whose every pipeline failed is still an error.
- **`ops` is optional, as the schema always said.** With none, the result is the
  table itself — copying one into another file needed a made-up `limit` big
  enough not to cut anything, which silently truncates when the guess is wrong.
- **A field the operation does not have is named, with a suggestion.** Writing
  `aggregate` where `group_by` wants `agg` was ignored, and the failure arrived a
  step later as "grouping needs at least one aggregate" — pointing away from the
  typo and at the one thing that had been supplied. A list where an object
  belongs now says so too, instead of reporting the field it could not find
  inside it.
- **`insert` no longer walks past the schema's DEFAULTs.** Every column was
  named in the statement and the missing ones were given NULL, so
  `DEFAULT 'direct'` and `DEFAULT (datetime('now'))` never ran and a CHECK
  constraint waved the result through — NULL satisfies CHECK in SQLite. A column
  with a DEFAULT and no value is now left out of the statement, which is the only
  way a DEFAULT ever applies; a column without one is NULL as before. The trade
  is named rather than hidden: an explicit `null` on insert is a value the row
  does not have, so a DEFAULT beats it, and forcing NULL into a defaulted column
  means inserting the row and then `set`ting that column — a real NULL on `set`
  is still a real NULL.
- **Writing to a database no longer claims the values were formatted for
  reading.** That note belongs to .xlsx and .csv; on a `.sqlite` it made a
  correctly typed table look unfit to query.
- **A refused plan says which kind of refusal it is.** Applying a plan that a
  later plan or a completed write had retired answered "No plan is waiting. Call
  tuitab_write to make one", as though it had never existed.
- **`Some("x")` no longer appears in a message for a person.** The drift error
  quotes the values instead: `the database has 'CHANGED', expected 'PLAN_B'`.

## [0.8.2] - 2026-08-12

### Changed

- Bump dependencies: `clap` 4.6.5 → 4.6.6, `jaq-json` 2.0.1 → 2.0.2,
  `rust_xlsxwriter` 0.96.0 → 0.97.1. No user-facing behaviour change.
- **The MCP write gate is on the table, not the file.** `output.path` pointing at
  a `.sqlite`/`.duckdb` file that already existed was refused outright without
  `--mcp-write`, which made a database of several tables impossible to build: the
  first write created the file and the second hit "already exists" — including on
  the file the server had just made itself. Adding a table leaves every other
  table in the file alone, so it now needs nothing extra. Replacing one destroys
  rows somebody has, and that still needs `--mcp-write`, on top of the
  `output.overwrite` the caller has to ask for.

## [0.8.1] - 2026-08-12

### Changed

- **`p` and `P` have swapped.** `p` pastes into the cell under the cursor and
  `P` pastes rows — the way round that matches how often each is wanted. In
  0.8.0 the cell paste arrived on `P`, which put the rarer operation on the
  easier key.

## [0.8.0] - 2026-08-12

### Added

- **The MCP server can now change a database table**, behind a flag and a
  handshake. Start it with `--mcp-write` and two tools appear: `tuitab_write`
  works out what a change would do and answers with the exact SQL, the rows it
  would touch as they stand, and a plan id — writing nothing; `tuitab_write_apply`
  runs precisely that plan, in one transaction, refusing if the table changed in
  between. That pair is what replaces the terminal's confirmation popup, which a
  model has no way to answer. Without the flag neither tool exists, and calling
  one by name says which flag turns it on.

  A change is one of `set`, `delete`, `insert` or `alter`, with `where` taking
  the same predicates as `tuitab_query`'s filter. A JSON `null` writes a real
  NULL. `delete` always needs a `where` — emptying a table is not something a
  missing argument can ask for. Underneath it is the terminal's own machinery, so
  a change tuitab would refuse in the TUI is refused here for the same reason and
  with the same sentence.
- **`tuitab_inspect` now says what a database actually declares**: each column's
  SQL type, NOT NULL, PRIMARY KEY, DEFAULT and whether it is generated, alongside
  the type tuitab inferred. The container listing gained per-table row and column
  counts and the `CREATE` statement, so finding the right table no longer costs a
  call per table.
- **Views are listed and readable** in both engines, and marked as not writable.
  DuckDB could not read one at all before.
- **`output.table`** names the table a query writes into a `.sqlite`/`.duckdb`
  file, so a model can build a database of several tables. Adding a table beside
  existing ones needs nothing extra; replacing one still needs `output.overwrite`.

- **A database can now be built from nothing.** `tuitab inventory.sqlite` on a
  file that does not exist opens a blank sheet — one empty column, titled
  `[new]` — instead of exiting with an error. Add columns with `zi`, give them
  types with `t`, add rows with `o` (below the cursor) and `O` (above), type the
  values, and `Ctrl+S` creates the file. It asks what to call the table,
  defaulting to the filename, then writes a real typed table: an Integer column
  is declared `INTEGER` and stores integers, a NULL stays NULL, and the rows go
  in the order the sheet shows them. Works for `.duckdb` too, which had no
  writer at all before.

  Creating into a database that already exists adds a table to it rather than
  refusing — the table is named explicitly, so it is a deliberate act. Replacing
  a table that is already there shows the `DROP TABLE` in the confirmation popup
  first. Once written, the sheet adopts the table it just made, so the next
  `Ctrl+S` is an ordinary writeback with its usual confirmation.

  A path whose directory does not exist, or whose extension tuitab cannot write
  (including no extension at all), is still an error — those are what a typo
  looks like.
- **`o` and `O` add an empty row** below and above the cursor, vim-style. Until
  now the only way to add a row was pasting tab-separated text from the
  clipboard. The cells start as NULL, so an untouched one reaches a database as
  NULL rather than as an empty string.

- **Schema changes reach the database too.** Adding a column (`zi`, `=`, `zx`),
  dropping one (`zd`) and renaming one (`ze`) used to block the save outright;
  they now become `ALTER TABLE` statements in the same confirmation popup as the
  row changes, run in the same transaction, ordered so they cannot collide with
  each other — drops, then renames, then adds, then the rows. A new column is
  created with the type the sheet is showing and filled with its current values,
  so a computed column arrives as ordinary data. On DuckDB, changing a column's
  type with `t` becomes `ALTER COLUMN … TYPE`; SQLite has no such statement and
  says so rather than pretending.

  What a column *is* is tracked, not what happened to it: each column remembers
  the name it had in the table, so renaming twice is one `RENAME COLUMN` and not
  two, renaming a column you added in this session is just an `ADD COLUMN`, and a
  `zr` find-and-replace — which sets the column to text as a side effect — never
  turns into a type change.

  Before anything runs, a drop that the engine would reject is refused with a
  sentence instead: the primary key, a column an index is built on, a column a
  view or trigger names. Retyping to a percentage, currency or file size is
  refused outright — those are ways of *displaying* a number (a percentage is
  stored divided by 100), and writing one back would silently rescale the
  column.
- **Reordering columns, and changing a type on SQLite, rebuild the table.**
  Neither engine can move a column, and SQLite has no `ALTER COLUMN` at all, so
  both mean creating the table again with the shape the sheet has and copying the
  rows across. The popup says so before the user confirms and shows every
  statement including the `DROP TABLE`; indexes and triggers are recreated from
  their own definitions, `rowid`s are copied explicitly so nothing loses its
  identity, and foreign keys are checked before the transaction commits.

  The synthesized `CREATE TABLE` carries names, types, NOT NULL, DEFAULT and a
  single-column primary key — and nothing else, so anything else is a refusal
  rather than a guess: a CHECK, a UNIQUE, a multi-column key, a collation,
  `AUTOINCREMENT`, `WITHOUT ROWID`, `STRICT`, a generated column, a foreign key
  in either direction, or a view built on the table. Adding a column is not a
  reason to rebuild: `ADD COLUMN` appends, as everywhere else, and the sheet
  reloads to show where it landed.

- **`show_statements`** on `tuitab_write`: the plan still returns the first
  twenty statements by default, but a model asked to show the user everything can
  now request up to two hundred.

- **`P` pastes the clipboard into the cell under the cursor.** `y c` copies a
  cell and `p` only ever appended rows, so the obvious round trip — copy a value,
  put it somewhere else — had no key and failed with a type error instead. `P`
  takes the clipboard's first line and writes it exactly as typing it with `e`
  would: the same type check, the same document write-back, the same undo entry.

### Changed

- **Database columns arrive with the types their table declares.** Every value
  comes back from both engines as text, so a column was a string column and
  `score > 100` was a comparison polars refused outright — numeric filtering over
  a database did not work at all. Columns declared as integers and reals are now
  cast to them, per column and best-effort, since SQLite lets an `INTEGER` column
  hold text and such a column is still worth reading. `BOOLEAN` deliberately
  stays text: SQLite stores it as the integers 1 and 0, and a boolean column
  would render `true` where the re-read database says `1`.

  The drift check that guards a write now compares the *number* rather than its
  spelling for such columns — DuckDB renders a `DOUBLE` `1.0` as `"1.0"` where
  Rust gives `"1"` — which also removes a fragility that predated this: a change
  of float rendering between engine versions would have reported drift on rows
  nobody touched.
- **Typing a value a column cannot hold no longer discards it.** The cast back
  into the column was non-strict, so `abc` in an integer column became a silent
  NULL; it now stays visible as text and the save refuses it by name.
- **A database source needs a `container`.** Reading one without a table named
  used to hand back a listing dressed up as data, including raw `CREATE TABLE`
  text, while the server's own instructions said there was no SQL anywhere.
  `tuitab_inspect` gives that listing properly.
- **Exporting to SQLite writes a real table, not a dump.** It used to create one
  called `data` with every column declared `TEXT` and every value formatted the
  way the screen shows it — a currency column arrived as `'$1 234,50'` — writing
  row by row with no transaction. It now asks for the table name, declares the
  types the sheet shows, writes raw values in one transaction, and keeps NULL
  apart from an empty string. Through the MCP server the table is named
  `result`; it used to be `data`.
- **Pinning a column (`!`) no longer moves it.** It used to walk the column to
  the front of the frame one swap at a time, which the renderer never needed —
  it builds the pinned block from the flag. Two consequences: unpinning now
  restores the original order (with two columns pinned at once it did not), and
  keeping a column in sight while scrolling is no longer indistinguishable from
  asking for the table's columns to be reordered. Pinned columns are no longer
  written first when exporting to CSV, xlsx, parquet or Arrow.

- **A SQLite or DuckDB table can now be edited and saved back.** Editing a
  database table used to be a dead end: the sheet took the edits and there was
  nowhere to put them. `Ctrl+S` onto the file the table came from now derives
  the change set — `UPDATE` for edited cells, `DELETE` for removed rows,
  `INSERT` for pasted ones — shows every statement it is about to run, and
  executes them in one transaction only after confirmation. Rows sharing an edit
  collapse into a single `UPDATE … WHERE rowid IN (…)`, so a bulk edit reads as
  one statement rather than five thousand. Nothing else in the file is touched:
  other tables, indexes, views and triggers are exactly as they were, and
  unchanged rows are never rewritten.

  Rows are addressed by `rowid`, captured at load and kept out of the table.
  Before anything is written, the affected rows are re-read inside the same
  transaction and compared against the values as loaded; if the table changed
  underneath — another tool, another tuitab sheet — the save is refused with
  what differs and nothing is written. Values are parsed into the column's
  declared type first, so `'abc'` in an `INTEGER` column stops the save before
  any SQL exists, naming the column and the row on screen.

  Saving to a *different* `.db` copies the whole database first (`VACUUM INTO`
  for SQLite, `CHECKPOINT` plus a file copy for DuckDB) and applies the same
  statements to the copy, leaving the original alone. No confirmation there —
  there is nothing of the user's to lose, which is also why the destination has
  to be a file that does not exist yet rather than one that is about to stop
  existing.

  Operations that renumber rows or change the column set — a window column,
  transpose, pivot, join, group-by, dropping or renaming a column, retyping one
  — make an in-place write impossible, and saying so is better than guessing:
  the save is refused with the reason and a pointer to "save to a different
  file".
- **NULL is now visible and typable.** SQLite and DuckDB loaders used to turn
  NULL into an empty string, which made the two indistinguishable on screen and
  silently converted one into the other on the way back. A NULL cell now renders
  as a dim italic `NULL`, opening it for editing shows `\N`, and typing `\N`
  means SQL NULL. An emptied cell stays an empty string in a text column and
  becomes NULL in a numeric one, which is the only reading `''` has there. The
  sentinel is read only on sheets that came from a database: a CSV has no null
  to mean, and a JSON sheet edits its document tree, where those two characters
  are just two characters.
- **Arrow / Feather files** open and save (`.arrow`, `.feather`, `.ipc`).
- **A save into a database keeps the sheet as the user arranged it.** Pins,
  widths, aggregators, precision, currency and column selection now survive the
  reload that follows a write; so do the sort and the running search, which are
  remembered by column name and therefore unbothered by a save that dropped a
  column. A type assigned by hand with `t` comes back too, along with the tag
  that tells the next save the column is a reading of the data rather than the
  data. The filter is still cleared — the rows may not be the same rows — and the
  row selection is restored by row identity rather than by position, so a save
  that deleted rows above it does not move it.
- **The SQL confirmation stops keeping readable text past 2000 statements** and
  says how many it is not showing. Those statements still run; what changes is
  that a whole-table edit no longer holds a second copy of the data purely so a
  popup can display it.

### Fixed

- **Copying left the clipboard empty on Linux.** Every copy opened its own
  clipboard connection and closed it on the way out — but on X11 and Wayland the
  text is not stored anywhere, it is served by the process that set it over that
  very connection. So the status line said the value was copied and there was
  nothing to paste, in tuitab or anywhere else. tuitab now holds one clipboard
  for the life of the process. (X11 being X11, the content still goes when tuitab
  exits unless a clipboard manager keeps it. macOS was never affected.)
- **`y c` copied the cell as it is drawn, not as it is.** A float column is shown
  to two decimals, so copying `1234.5678` yielded `1234.57` — invisible until the
  value was pasted back into a table. The single-cell copy now takes the raw
  value, the same one `e` puts in the edit line; a NULL in a database sheet
  copies as `\N` and pastes back as a real NULL. Yanking rows or a whole column
  through the format popup is a display-form export and is unchanged.
- **A pasted row shorter than the table no longer fails the whole paste.** A
  field the pasted line does not have was read as the empty string, which is not
  a number, so one missing trailing column refused every row with
  `column 'amount' holds f64`. A missing field is now NULL, which is what it is.
  A paste that does fail no longer eats an undo entry either.
- **A negative currency could not be typed or pasted back.** It is displayed in
  brackets — `($5.00)` — and read back through a filter that keeps only digits,
  a dot and a minus, so the brackets went and the value came back positive. The
  sign is now taken from the brackets before they are stripped.
- **`Ctrl+S` on a table opened from a database always failed.** The save dialog
  fell back to the sheet title and offered `/path/db.sqlite :: users` as the
  filename, whose extension parses as `.sqlite :: users` — so every save ended
  in `Unsupported save format`. Such a sheet now knows the file it came from.
  Sheets opened from an `.xlsx` had the same defect and the same fix.
- **Exporting onto an existing database would have dropped a table into it.**
  `save_sqlite` runs `DROP TABLE IF EXISTS data` before writing, which was
  harmless while database files were never the default save target and is not
  now. It refuses a file that already holds any other table.
- **A save of nothing but new rows ran without a single check.** The column-shape
  check was conditional on the plan changing the schema, and the drift check only
  ever looks at rows that already exist, so an INSERT-only plan went to the
  database blind. The shape check now runs on every write.
- **A BLOB column was editable, and editing one stored six letters.** The sheet
  shows a marker where the bytes are; writing that back would have replaced an
  image with a description of it. Editing such a column is refused by name, and
  the rest of the row still saves. The marker now carries the size
  (`[BLOB 5 bytes]`), so the drift check notices a blob swapped for one of a
  different length.
- **Reordering the columns of a DuckDB table with a generated column** rebuilt it
  as an ordinary one and copied the computed values in. DuckDB's catalogue has no
  flag for it, but its stored `CREATE TABLE` does, so the rebuild is refused the
  same way SQLite's already was.
- **Saving a view — or a `WITHOUT ROWID` table — back into its own file** fell
  through to the *create a table* branch and offered `DROP TABLE` on the thing it
  had just been read from. On a `WITHOUT ROWID` table that would have succeeded,
  replacing it with a plain one. It is refused with a sentence; saving elsewhere
  still works.
- **A find-and-replace no longer leaves a type change behind.** `t` → Integer
  then `zr` showed a String column while planning `ALTER COLUMN … TYPE INTEGER`,
  which on SQLite means rebuilding the whole table for nothing.
- **Two writers no longer collide instantly.** SQLite connections take the write
  lock up front and wait a few seconds for another writer, instead of returning
  `database is locked` the moment two saves overlap; DuckDB, which allows one
  process at a time and cannot wait, now says so in a sentence.
- **The `.db` extension names no engine, and is no longer guessed at.** Every
  reader and writer now asks the file's own header, so a DuckDB database called
  `data.db` opens in the terminal and over MCP alike, and a table written into an
  existing file speaks that file's dialect.
- **Swapping two column names now takes one save.** It used to be refused with a
  suggestion to do it in two; the plan parks one column under a scratch name and
  unwinds the cycle in three statements.
- **`R` on a sheet whose file has gone** cleared the undo history before finding
  out the reload would fail. It now clears nothing until there are rows to put in
  place of the old ones.
- **A row added with `o` no longer jumps to the bottom** when the sort is reset —
  it goes back to where it was inserted.
- **A write through MCP's `output.path` invalidates the server's cache**, so
  inspecting the file it just wrote no longer answers from a frame taken before
  the write.
- **`Ctrl+S` on the overview of a database wrote the database's own table listing
  back into it as a table** — and, when the name it suggested happened to be
  free, without showing a single statement first. The sheet then became that
  table, and it was the root sheet, so there was nowhere to go back to. Saving
  the overview into its own file is refused; exporting it elsewhere still works.
- **A drilled-into sheet offered to create a table out of the rows it was
  filtered to**, over the table they came from. Such a sheet now carries the
  table it came from, so saving it is an ordinary writeback of the rows it shows.
  The same applies to a drill-down out of a chart.
- **Replacing a table said nothing about what the `DROP TABLE` took with it.**
  The popup now lists the indexes and triggers that will be lost and the views
  that will break, and replacing a table another table has a foreign key into is
  refused outright — that one leaves the database itself inconsistent.
- **`t` → Date on a text column of timestamps wrote the truncated dates back.**
  The sheet parses `2024-01-01 09:30:00` down to the day and turns anything it
  cannot read into NULL; saving then wrote that reading into the table. Such a
  column is no longer written at all, and the sheet says so instead of doing it
  quietly. `t` → Boolean on a `BOOLEAN` column, which used to plan an UPDATE of
  every row to write `true` where the table already said `1`, now plans nothing —
  while a real edit in either column still reaches the database.
- **MCP's `output.path` could change a database without `--mcp-write`**, with no
  plan to read and no drift check, and with `output.overwrite` it could drop a
  table. Writing into a database file that already exists now needs the flag; a
  file that does not exist yet is still created without it. It also cannot write
  into the file the query read, and replacing a name that belongs to a view is
  refused in words rather than by the engine mid-transaction.
- **A failed `tuitab_write` left the previous plan applicable.** Calling it now
  retires the pending plan whatever happens next, and the answer names the plan
  it replaced, so `tuitab_write_apply` cannot run something the conversation has
  moved on from.
- **`alter` ignored anything it did not recognise** and answered "no change" — a
  typo, or asking for a retype or a reorder, looked like a table that was already
  in the requested shape. Unknown keys are refused by name.
- **The MCP cache did not notice a commit made through a write-ahead log.** Both
  engines commit into a file beside the database, so a change by another process
  left the main file's timestamp alone and a query could keep answering with
  pre-commit rows. The cache now stamps the companions and the size too, and an
  unreadable file counts as a miss rather than a match.
- **A declared `format` of `sqlite`, `parquet` or `xlsx` was accepted and then
  ignored** — the override only ever covered the document formats, so
  `{"path": "a.csv", "format": "parquet"}` quietly read the file as CSV.
- **Any failure to read a table's rowid made it look like a view.** A locked file
  or an unreadable page came back as a read-only sheet, and the model was told
  the table was a view. Only "there is no rowid here" falls back now. Likewise, a
  catalogue query that fails no longer invents an unconstrained text column for
  every column in the table, which had been silently switching off the NOT NULL,
  DEFAULT and generated-column checks built on it.
- **DuckDB's rebuild preflight was missing three checks SQLite's had** — a
  foreign key pointing into the table, a view built on it, and a leftover scratch
  table from an interrupted run.
- **MCP's `output.table` was validated trimmed and used untrimmed**, so `"  x  "`
  passed the check and created a table with the spaces in its name, and
  `output.path` did not expand `~`, creating a directory called `~` instead.
- **A generated column in DuckDB was treated as ordinary data.** No catalogue
  says which columns are computed — `duckdb_columns()` has no such field and
  `information_schema` leaves both `is_generated` and the expression NULL — but
  the stored `CREATE TABLE` does, and it is read now. Editing one is refused by
  name and a new row no longer lists it, the way both have always worked on
  SQLite.
- **`output.path` is checked before the query runs.** An extension nothing can
  write, or a directory that is not there, used to surface only after the joins
  and group-bys had been computed.
- **Errors from the engines reach the model with no idea what failed.** A polars
  complaint about shapes, an io error from a write, a jq parse failure — all
  arrived bare. Each now says what was being attempted, while tuitab's own
  refusals still arrive verbatim: those are written to be read.
- **The MCP frame cache held one file**, so comparing two of them re-read both on
  every call. It holds four now, newest first.

## [0.7.0] - 2026-08-04

### Fixed

- **Repeating `z` + arrow moved the column every other time, and moved the
  cursor in between.** The first chord leaves the app in a column-move mode
  where bare arrows keep reordering — but the chord people repeat is the whole
  `z` + arrow, and that `z` fell through to the mode's catch-all and left it, so
  the arrow behind it moved the cursor. The mode has behaved this way since
  before 0.6.0; what made it visible is the compound sort added here, because
  the drifted cursor now had `z[` to land on and the sort went to a column the
  user never pointed at. `z` re-opens the prefix from column-move mode, on
  Cyrillic layouts too — `я`, and `р`/`д` for left and right.
- **The sort arrow never fitted in the header.** Column widths are measured from
  the name alone, so `!`, `*` and `▲` had no room reserved and the whole string
  was truncated from the right — cutting off the arrow, which is last. A
  compound sort made it worse by needing a third column for the rank digit. The
  name now gives way instead, since it is the part with information to spare.
- **A running total could only be ordered by re-sorting the table.** `cum_sum`,
  `lag`, `lead` and `row_number` read the frame as it stood, so totalling by
  date meant sorting by date first — a change to the table nobody asked for, and
  one the documentation used to recommend. `zw` now asks which column orders the
  rows, and `window` takes `order_by`: the rows are put in that order to compute
  the column and the answer comes back where they already are. Ties keep their
  relative order and empty values sort last. The eight functions that read no
  order refuse an `order_by` rather than dropping it, since
  `RANK() OVER (ORDER BY x)` is a reasonable thing to ask for and a bad thing to
  answer differently.
- **A rank in the TUI was always ascending.** The server's `window` operation
  accepts `desc`; `zw` had no way to say it, so one of the two surfaces could
  not ask for a top-N at all. Picking `rank` or
  `dense_rank` now asks which end comes first, before the partition picker.
  Ascending stays the default on both surfaces, and a descending rank is named
  `<col>_rank_desc` so it can sit beside the ascending one.
- **`is_empty` matched nothing, on any file.** It compared the column against a
  null literal, and in three-valued logic `x == null` is null rather than true.
  Since polars reads a blank CSV field as null, a blank cell matched neither
  `is_empty` nor `not_empty` — it fell out of both halves of a partition that is
  supposed to cover every row.
- **Filtering a date column was impossible.** Polars refuses to compare a
  temporal column with a string and nothing cast either side. Dates now compare
  as ISO text, which orders identically to chronology.
- **A quoted number no longer fails against a numeric column.** `"30"` where
  `30` was meant is a slip worth absorbing; `"a lot"` is still a type error.
- **Integer ids above 2^53 stopped being distinguishable.** JSON has one number
  type, so an id arrived as `f64` and promoted the column to float; two
  neighbouring ids then matched the same query. Integral literals stay integral.
- **A whole-frame condition reported "1 row matched".** `mean(age) > 30` lowers
  to a single verdict about the table, which applies to all its rows or none —
  enumerating it answered about row zero and called that the answer.
- **A refused filter said only that it had been refused.** Five different causes
  shared one message; the real reason now reaches the caller.
- **`z[` left the z-prefix open**, so the next key was read as a z-command:
  pressing `d` to delete selected rows deleted a column instead, and a compound
  sort could not be typed at all.
- **Deleting or moving a sorted column corrupted the sort.** Keys were stored as
  column positions, which every column operation renumbers — deleting one left a
  key pointing past the end and the next `z[` panicked. Keys are stored by name
  and resolved when used.
- **`zw` on a JSON/YAML/TOML sheet broke it permanently.** Adding a column
  desynchronised the table from its document, after which editing refused for
  the rest of the session. It now declines, as `zf` already did.
- **A new column taking an existing name desynchronised the table.** Polars
  replaces the old column while the metadata was appended regardless, so a
  reader received one more header than there were values in each row. Both
  `window` and `compute` now refuse the collision.
- **Transposing a table that merely had a pinned column called `column`**
  inverted it instead, dropping a column. Recognising its own output is now a
  flag nothing but the transpose sets, rather than a shape ordinary data can
  have — `build_multi_frequency_table` pins its group columns, so a frequency
  table followed by a transpose was enough to trigger it.
- **A sort polars refused returned quietly**, leaving the rows as they were with
  nothing to distinguish that from an already-sorted table.
- **A projection changed a column's reported type.** `select` built its frame by
  hand and skipped the reconciliation every other derived table gets.
- **Adding a window column threw away the sheet's state.** `r` no longer
  restored the file's own row order, and a selection disappeared under the user,
  because the added column was built as a fresh table rather than as an
  addition.
- **`zw` refused eight of its twelve functions on a text column**, with a
  message about percent columns nobody had asked for — it was being judged by a
  gate written for `zF`. `row_number` reads no column at all. A function that
  does need numbers now names itself when it declines.
- **A window column is inserted beside the column it describes** rather than at
  the far right, where a wide table put it off-screen.
- **A frequency table ordered its groups differently between runs.** Groups came
  back in hash order and ties in count fell where they may. Order is now first
  appearance in the data, which is stable and means something.
- **A group of blank cells was counted as zero rows** and given a zero share,
  because the count skipped missing values while claiming to count rows.
- **A frequency table silently dropped an aggregate over one of its grouping
  columns**, having promised to refuse aggregates it cannot compute.
- **`zF` and `T` corrupted a JSON/YAML/TOML sheet**, exactly as `zw` did: the
  table stopped matching its document, editing refused for the rest of the
  session, and the result vanished at the next reprojection. `zf` had always
  declined; these two now do too.
- **Sorting and every chord were unreachable on a non-Latin layout.** The layout
  translation ran in a fallback at the bottom of Normal mode — a second copy of
  the key bindings that was already missing the brackets, and that never saw the
  second key of a chord at all, so `zw`, `gb`, `z[` and `z]` could not be typed.
  Translation now happens once, before anything reads the key, and the duplicate
  table is gone.
- **Random sampling was quadratic in the size of the table.** Restoring the
  table's order used a linear scan per comparison, so a thousand rows out of a
  million was on the order of 10^10 operations. Positions are drawn and sorted
  instead.
- **An aggregate in an MCP `compute` ignored the preceding `filter`.** A pipeline
  that narrowed the rows and then asked for `amount / sum(amount)` divided by the
  total of every row in the file, dropped ones included — a share-of-total that
  looked entirely reasonable and was not. Every pipeline operation now hands the
  next one a materialised frame, so nothing downstream can reach past the view to
  the data underneath.
- **A column whose name looks like a regular expression addressed the wrong
  columns — or none.** Polars reads a name that starts with `^` and ends with `$`
  as a pattern selecting every column it matches, and tuitab passed header names
  straight through. A file with a column literally called `^total$` turned a
  frequency table into "unable to find column Count" and a pivot into "not
  found", in the TUI as much as over MCP. Column references are now built as
  names rather than parsed as patterns.
- **A row filter that legitimately matched nothing was silently recomputed by a
  different evaluator.** The fallback to per-row evaluation triggered on an empty
  result rather than on the vectorised path failing, so "no rows matched" — an
  answer — was treated as a failure and answered again with different semantics.
- **Retyping a column left aggregators behind that its new type cannot carry.**
  Assigning `sum` to a numeric column and then switching it to String with `t`
  kept the aggregator marked on the column while the footer and every frequency
  table silently skipped it. Incompatible aggregators are now dropped when the
  type changes.

### Added

- **`and`, `or` and `not` in expressions**, so `department == "HR" or department
  == "Marketing"` works wherever an expression does — including the `|` row
  filter, which needed no interface change to gain it. `or` binds loosest, then
  `and`, then `not`. Also `contains(col, regex)`.
- **The MCP server reports any random seed it drew for you**, so an unseeded
  sample or dedup can be repeated by passing the seed back.
- **Window functions.** `zw` adds a column computed from the rows around each
  row — `row_number`, `rank`, `dense_rank`, `cum_sum`, `lag`, `lead`, a group's
  `sum`/`avg`/`min`/`max`/`count` repeated on its rows, and `pct_of_total`. It
  reuses the partition picker `zF` already had, so a window can be scoped to a
  group. In MCP: `{"window": {"fn": "rank", "col": "salary", "over":
  ["department"], "desc": true}}`.

  `cum_sum`, `lag`, `lead` and `row_number` read the rows in their current
  order, so sort first. `zf` and `zF` now call the same function rather than
  building their own window expression.
- **Transposing, deduplicating and random sampling reached MCP**, having lived
  only in the terminal: `{"transpose": {}}`, `{"dedup": {...}}`,
  `{"duplicates": {...}}`, `{"sample": {"n": 100, "seed": 42}}`.
- **`gb` groups by the pinned columns**, computing the aggregates marked with
  `+` on the others. Unlike `F`, which ranks groups by how many rows fall in
  each, this returns exactly the aggregates asked for, in the order asked for.
- **A grand total.** `{"aggregate": [...]}` over MCP answers "what is the total
  revenue" without inventing a grouping column to hang it on.
- **`any_of` gives the MCP filter an OR**, and a predicate may now compare two
  columns: `{"col": "revenue", "op": "gt", "value": {"col": "cost"}}`.
- **Sorting by several keys at once.** `z[` and `z]` add the cursor column as a
  further, less significant key to the sort already running, and the header shows
  each key's rank. In MCP: `{"sort": {"by": [{"col": "region"}, {"col": "amount",
  "desc": true}]}}`.

  Two sorts in sequence are not an equivalent: a single-key sort does not promise
  to preserve the order of rows that tie on its key, so the first ordering may or
  may not survive the second. It happens to survive on small frames, which is
  worse than failing outright.
- The in-app help (`?`) documents sorting, which it never has.

### Changed

- **The two surfaces are now held together by the compiler.** A test classifies
  every one of the 317 terminal actions as sharing a function with a named
  server operation, expressible with what the server has, belonging to the
  interface alone, or a declared gap — and the match is exhaustive, so adding an
  action breaks the build until someone answers "and how does a model do this?".
  A mirror match does the same for every server operation. Two gaps are declared
  today: find & replace in a column, and splitting a column by a delimiter.
- **Random selection takes a seed.** Sampling and the random dedup keeper drew
  from the system source, so the answer could not be repeated — poor form for a
  tool whose numbers get quoted. Both now accept a seed, and MCP reports the one
  it used.
- **Both surfaces now run the same filter.** The terminal's typed expressions and
  the server's structured predicates compile into one expression tree and one
  evaluator, where before they shared nothing — which is how the server ended up
  with a predicate language the terminal could not express, and the terminal kept
  a fallback the server never inherited. Roughly 170 lines of duplicate logic
  went with it. Grouping moved the same way, so `gb` and `group_by` cannot drift.
- Derived tables — frequency, pivot, transpose, group-by, describe — are built
  through one constructor rather than eleven copies of the same struct literal.
  The type correction that one copy had, where an average over an integer column
  is reported as a float rather than inheriting the integer label, now applies to
  all of them.

## [0.6.0] - 2026-08-03

### Added

- **`tuitab --mcp` runs tuitab as an MCP server**, so an AI assistant can compute over a
  data file with the same engine the TUI uses instead of doing the arithmetic itself.
  Register it with a client — for example `claude mcp add tuitab -- tuitab --mcp` — and
  the assistant gets four tools:
  - `tuitab_inspect` — sheets or tables, column names with inferred types, row count,
    sample rows. The first call in any session, so column names are read rather than
    guessed.
  - `tuitab_query` — a pipeline of `filter`, `select`, `sort`, `compute`, `group_by`,
    `frequency`, `pivot`, `join` and `limit`, applied in order. Several pipelines in one
    call share a single load of the file. Results come back as JSON, or go to a file via
    `output.path` in any supported format.
  - `tuitab_describe` — the per-column profile behind the `I` key.
  - `tuitab_jq` — jq programs over nested JSON, JSONL, YAML and TOML.
- There is no SQL and no arbitrary code in the tool surface: the model sends structured
  operations, each one mapping onto a function tuitab already had, and gets back numbers
  Polars computed. The server ships its own documentation — the tool list and usage notes
  reach the model on connect.
- Numbers are returned raw rather than formatted: percentages arrive as fractions, and
  currency as plain numbers. Files written through `output.path` keep the display
  formatting, because those are for a person to read.
- No new dependencies. MCP over stdio is newline-delimited JSON-RPC, which the existing
  `serde_json` already covers, so the TUI build is unchanged in size and in what it pulls
  in.

### Changed

- The per-column profile moved out of the TUI into `data::describe`, shared by the `I`
  key and `tuitab_describe`. Behaviour is unchanged, and the extraction was verified
  against the previous implementation cell by cell.
- **`I` now reports a stable `mode`.** Ties were broken by hash iteration order, so a
  column of unique values gave a different answer on different runs — Rust seeds its
  hasher randomly per process. The most frequent value now wins, and equal counts break
  by first appearance.
- The MCP profile labels the standard deviation `stdev_pop`, because the one `describe`
  computes is the population figure while the footer aggregator of the same name is the
  sample one. The TUI label is unchanged.

## [0.5.0] - 2026-07-28

### Added

- **JSON, JSONL/NDJSON, YAML and TOML open as a table over the real document.**
  The table is a projection of the parsed tree, not a flattened copy: editing a cell
  writes into the document, and saving re-serialises it, so nesting, key order and
  TOML datetimes survive.
- Converting between the structured formats is a different extension on save —
  `config.toml` saved as `config.yaml`. A conversion that cannot carry everything says
  so in the status line.
- **A TOML file saved back as TOML keeps its comments and layout**, because it is
  written through its own source rather than rebuilt.
- `Enter` / `zEnter` dive into the node of the current row or cell; `q`/`Esc` comes
  back. Sheets in a dive chain share one document, so an edit three levels down is
  there when you return, and `U` undoes it at any level.
- `m` cycles how a node is projected: records, key/value, scalars. An object or a TOML
  document opens as key/value rows rather than one very wide row.
- `(` / `)` expand a nested column into `parent.key` / `parent[0]` columns and fold it
  back. Expanding is a view operation — the saved file keeps its real nesting, and
  expanded cells stay editable.
- `E` on a container cell opens its real subtree in `$EDITOR`, which is how keys are
  added, removed and reordered. If the result does not parse, nothing is written and
  the typed text is kept.
- `g/` searches the whole document rather than the rows on screen and lists the hits
  with their paths; `Enter` opens the node with the cursor on the match.
- `gp` jumps to a node by its path, `yp` copies the path of the current cell, and the
  status line shows it when there is nothing more urgent to report.
- `gq` runs a jq query and opens the result as a sheet of its own.
- `zo` on a directory listing reopens a file as an explicitly chosen format, and a file
  with an unhelpful extension is identified by its contents.
- Saving a plain table to a structured format asks which shape to produce — records,
  columns, or key/value — and remembers the answer.
- `--type` now also forces a format for a file, not just for stdin.

### Changed

- JSON no longer goes through the Polars reader, which flattened nesting on save.
- Release binaries are stripped, taking them from 121 MB to 97 MB.
- Operations that would reshape the table without a matching change in the document —
  paste, computed columns, the `z` column operations — are refused on a document sheet
  and point at `E`. Deleting rows, which is a change to the data, removes the array
  element or the key.

### Fixed

- Upgraded calamine, which moves the `.xlsx` parser to a quick-xml without the
  known denial-of-service advisories.

## [0.4.3] - 2026-06-14

### Changed
- Bump dependencies: `rusqlite` 0.31 → 0.40, `calamine` 0.22 → 0.35, `rust_xlsxwriter` 0.80 → 0.95, `rand` 0.9 → 0.10, `serde_json` → 1.0.150. Excel reading migrated to calamine's `Data` cell type and its `Result`-returning `worksheet_range`; random-row selection migrated to rand's renamed `sample` API. No user-facing behaviour change.

### Documentation
- Overhauled the README with animated demos and added a bilingual (English / Русский) documentation set under `docs/`.

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

[Unreleased]: https://github.com/denisotree/tuitab/compare/v0.9.6...HEAD
[0.9.6]: https://github.com/denisotree/tuitab/compare/v0.9.5...v0.9.6
[0.9.5]: https://github.com/denisotree/tuitab/compare/v0.9.4...v0.9.5
[0.9.4]: https://github.com/denisotree/tuitab/compare/v0.9.3...v0.9.4
[0.9.3]: https://github.com/denisotree/tuitab/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/denisotree/tuitab/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/denisotree/tuitab/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/denisotree/tuitab/compare/v0.8.2...v0.9.0
[0.8.2]: https://github.com/denisotree/tuitab/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/denisotree/tuitab/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/denisotree/tuitab/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/denisotree/tuitab/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/denisotree/tuitab/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/denisotree/tuitab/compare/v0.4.3...v0.5.0
[0.4.3]: https://github.com/denisotree/tuitab/compare/v0.4.2...v0.4.3
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
