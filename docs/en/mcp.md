# MCP server

> 🇷🇺 [Эта страница на русском](../ru/mcp.md) · [← Documentation index](README.md)

`tuitab --mcp` turns the binary into a [Model Context Protocol](https://modelcontextprotocol.io)
server. An assistant handed a data file can then compute over it with tuitab's
engine instead of doing the arithmetic in its head.

The point is narrow and worth stating plainly: **a language model asked to sum a
column will produce a plausible number.** Sending the question to a real engine
and reading back the answer is the difference between a report you can act on
and one you have to check.

## Connecting

**Claude Code** — one command:

```sh
claude mcp add tuitab -- tuitab --mcp
```

Add `-s user` to make it available in every project rather than the current one.

**Claude Desktop** — `~/Library/Application Support/Claude/claude_desktop_config.json`
on macOS, `%APPDATA%\Claude\claude_desktop_config.json` on Windows:

```json
{
  "mcpServers": {
    "tuitab": {
      "command": "/opt/homebrew/bin/tuitab",
      "args": ["--mcp"]
    }
  }
}
```

The full path matters more than it looks: a desktop app does not start from your
shell and may not see your `PATH`. `which tuitab` will tell you yours.

**Cursor, Windsurf, Zed** — the same block in their `mcp.json`.

**Anything else** — it is an ordinary stdio server. Launch it as a subprocess,
write newline-delimited JSON-RPC to its stdin, read replies from stdout.
Protocol versions `2025-06-18`, `2025-03-26` and `2024-11-05` are all accepted.

## Checking it works

Without any client at all:

```sh
printf '%s\n' \
 '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}' \
 '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
 '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' | tuitab --mcp
```

The second reply lists the tools — four, or six with `--mcp-write`. In Claude
Code, `/mcp` shows the same thing.

## The tools

| Tool | What it answers |
|------|-----------------|
| `tuitab_inspect` | What is in this file — sheets, tables and views, column names, inferred types, row count, a few sample rows. For a database also each column's declared SQL type, NOT NULL, PRIMARY KEY and DEFAULT, the `CREATE` statement, and every table's row and column counts |
| `tuitab_query` | Everything computational, as a pipeline of operations |
| `tuitab_describe` | A statistical profile of every column |
| `tuitab_jq` | A jq program over nested JSON, JSONL, YAML or TOML |
| `tuitab_calc` | Any calculation, with or without a file — see [Calculations](#calculations) |

`tuitab_inspect` comes first in any session. Column names guessed from a file
name are wrong often enough to cost a round trip. It takes `sample_rows` (default
5) if the default is too few or too many.

Every tool that reads a file takes the same `source`: `path`, plus `container` for a sheet or
table, `delimiter` to override CSV auto-detection, and `format` to override the
extension (`{"path": "deploy.conf", "format": "yaml"}`), and `detect_epoch: false` to keep
integer epoch columns as integers (see [Dates & time](expressions.md#dates--time)), and
`null_values` — a list such as `["undefined", "-"]` — for text that means "no value"
in every column of a CSV. A misspelt field is refused with the one it was close to,
rather than ignored. A bare path string works too. `tuitab_describe` takes `columns` to profile only some of them;
`tuitab_query` takes `output.limit` (default 100 rows).

Started with `--mcp-write`, the server has two more — `tuitab_write` and
`tuitab_write_apply` — and everything under [Changing what already
exists](#changing-what-already-exists) applies. Without the flag they do not
exist at all, and asking for one by name says which flag turns it on.

## Operations

`tuitab_query` takes a `source` and a list of `ops` applied in order, each to the
result of the last.

```json
{
  "source": {"path": "/data/sales.xlsx", "container": "Q3"},
  "ops": [
    {"filter": [{"col": "status", "op": "eq", "value": "closed"}]},
    {"compute": {"name": "margin", "expr": "revenue - cost"}},
    {"group_by": {"by": ["region"], "agg": [{"col": "margin", "fn": "sum"}]}},
    {"sort": {"by": [{"col": "margin:sum", "desc": true}]}}
  ]
}
```

| Operation | Notes |
|-----------|-------|
| `filter` | Entries are combined with AND. Operators: `eq`, `ne`, `gt`, `ge`, `lt`, `le`, `in`, `not_in`, `contains` (regex), `between`, `is_empty`, `not_empty` |
| `select` | Keep these columns, in this order |
| `sort` | `{"col": …, "desc": …}` for one key, `{"by": [{…}, {…}]}` for several |
| `compute` | A derived column from an expression |
| `group_by` | Exactly the aggregates asked for, in the order asked for |
| `aggregate` | A grand total — one row, no grouping |
| `frequency` | Distribution ranked by count, with `Count` and `Pct` |
| `pivot` | Rows by `index`, columns by `on`, cells by `formula` |
| `unpivot` | The reverse: `{"index": ["Time"], "columns": "*", "name": "country", "value": "rps", "drop_nulls": true}` turns each listed column (`"*"` — every one outside `index`) into rows of (`name`, `value`), `index` repeated on each. `name`/`value` default to `variable`/`value`; text beside numbers is refused |
| `join` | Against a second file, `how`: inner / left / right / outer / anti / semi / diff. anti and semi keep this table's columns and treat NULL keys as equal. diff compares the tables like `git diff`: a leading `_diff` column holds `=` `~` `-` `+`, and `<col>_right` carries the other table's values on `~` rows. Keys must be unique on both sides |
| `dedup` | One row per key. `keep`: first, last, min, max, random |
| `duplicates` | Only the rows whose key repeats |
| `sample` | `n` rows at random |
| `window` | A column computed from the rows around each row. `order_by` sets the window's own order without touching the table's |
| `transpose` | The table on end, or one row of it |
| `limit` | The first `n` rows |
| `expand_json` | One column per top-level key of the JSON objects in a text column: `{"col": "client", "prefix": "client_"}` (`prefix` defaults to `<col>_`). Every cell is parsed once, so several keys cost one pass; types follow the JSON, a nested object or array is its JSON text |

For OR, wrap alternatives in `any_of`:

```json
{"filter": [
  {"any_of": [{"col": "region", "op": "eq", "value": "North"},
              {"col": "region", "op": "eq", "value": "South"}]},
  {"col": "amount", "op": "gt", "value": 1000}
]}
```

which reads as `(North OR South) AND amount > 1000`. One level of nesting.

To negate, wrap a predicate — or an `any_of` — in `not`:

```json
{"filter": [{"not": {"col": "message", "op": "contains", "value": "https?://"}}]}
```

It keeps every row where the predicate is not true. A row with nothing in the
column is kept, so `not` and the filter it negates add up to every row. One
level: `not` inside `not` is refused. The same works in `tuitab_write`'s `where`.

To compare two columns, give a column where a constant would go:

```json
{"col": "revenue", "op": "gt", "value": {"col": "cost"}}
```

SQL's `HAVING` is a `filter` placed after `group_by`.

An aggregate can read only some of its group's rows: give it a `where` — the
same predicates `filter` takes, `any_of` and `not` included — and an `as` to
name its column. Several such counters fit in one pass:

```json
{"group_by": {"by": ["day"], "agg": [
  {"col": "*", "fn": "count"},
  {"col": "*", "fn": "count", "as": "n503",
   "where": [{"col": "response_code", "op": "contains", "value": "^503"}]},
  {"col": "ms", "fn": "p95", "as": "slow_p95",
   "where": [{"col": "ms", "op": "gt", "value": 1000}]}
]}}
```

The same works in `aggregate`; `frequency` refuses it.

`ops` may be left out entirely, and then the result is the table as it stands —
which is what copying one file into another asks for.

A column whose name has a space in it is written in backticks inside an
expression: `` {"compute": {"name": "pay", "expr": "`amount due` * 1.2"}} ``.
Double quotes are a *text literal*, so `"amount due" * 1.2` is arithmetic on the
words rather than the column; it is refused rather than answered with a column of
nulls. `"3" * 1` still works — that is the way to read a number out of a text
column.

### Several questions in one call

`pipelines` runs a list of them against one load of the file:

```json
{"source": {"path": "/data/sales.csv"},
 "pipelines": [
   {"name": "by_region", "ops": [{"frequency": {"by": ["region"]}}]},
   {"name": "totals",    "ops": [{"aggregate": [{"col": "amount", "fn": "sum"}]}]}
 ]}
```

The answer is `{"results": [...]}`, each entry carrying its `name`. One pipeline
failing does not take the others with it — that entry holds an `error` and the
rest hold their rows. A call whose every pipeline failed is an error, and so is a
single `ops` call that fails.

### Window functions

```json
{"window": {"fn": "rank", "col": "salary", "over": ["department"],
            "as": "rank_in_dept", "desc": true}}
```

`row_number`, `rank`, `dense_rank`, `cum_sum`, `lag`, `lead`, the partition's
`sum` / `avg` / `min` / `max` / `count` / `median` / `mad` repeated on its rows, and
`pct_of_total`. `mad` is the median absolute deviation from the median, unscaled:
with `median` it gives a robust z-score per group, `(x - median) / (1.4826 * mad)`,
which a single outlier cannot drag. `mad` is an aggregate for `group_by` and
`aggregate` too.

`over` restarts the window per group; without it the window is the whole table.

`order_by` settles what "before" means for `cum_sum`, `lag`, `lead` and
`row_number` — the four that read the rows in order:

```json
{"window": {"fn": "cum_sum", "col": "amount", "order_by": ["date"],
            "over": ["region"], "as": "running_total"}}
```

It orders the rows **for the window only**: the answer comes back against the
rows where they already are, so the table itself is untouched. Ties keep their
relative order and empty values sort last, so the same question gives the same
answer twice. `desc` runs the order the other way.

Without `order_by` those four read the frame as it stands, which is whatever
order the file happened to be written in. The other ten refuse an `order_by`
rather than ignoring it — a sum is a sum whatever order it is added in, and a
rank orders by the value it reads.

Use `window` with `over` for a share within a group, and `compute` with
`amount / sum(amount)` for a share of the whole table.

## Calculations

Not every question is about a table. A loan payment, a percentage change, the mean
of numbers the user pasted — a model does that arithmetic in its head just as
unreliably as it sums a column. `tuitab_calc` takes expressions as text and computes
them:

```json
{"steps": [
  {"name": "rate",    "expr": "0.12 / 12"},
  {"name": "payment", "expr": "pmt(rate, 360, 250000)"},
  {"name": "total",   "expr": "round(payment * 360, 2)"}
]}
```

Each step may use the names of the steps before it, and every step's value comes
back — the model quotes intermediate results instead of copying them between calls.
A single `{"expr": "…"}` works too.

**Numbers are decimals**, 28 significant digits, not floats: `0.1 + 0.2` is `0.3`,
and a sum of money does not drift by a fraction of a cent. Values come back as
strings, so no digit is lost in JSON, each with a `precision`:

| `precision` | Meaning | Examples |
|-------------|---------|----------|
| `exact` | No digit was lost | `0.1 + 0.2`, `1.07^10`, `round(x, 2)`, `stdev_p(…)` of whole squares |
| `rounded` | A decimal rounded to 28 significant digits | `1/3`, `sqrt(2)`, `pi`, anything computed from a rounded value |
| `approximate` | Floating point, about 15 digits | `ln`, `exp`, `norm_cdf`, trigonometry, fractional powers, `irr`, results beyond ±7.9·10²⁸ |

| Kind | Available |
|------|-----------|
| Operators | `+ - * / %`, `^` (or `**`; right-associative, `-2^2` is `-4`), `== != < > <= >=`, `and or not`, parentheses, lists `[1, 2, 3]` |
| Constants | `pi`, `e` |
| Math | `abs`, `round(x[, places])` (half away from zero; negative places round to tens, hundreds…), `floor`, `ceil`, `trunc`, `sqrt`, `exp`, `ln`, `log10`, `log(x[, base])`, `sin cos tan asin acos atan` (radians), `radians`, `degrees`, `min`, `max`, `factorial`, `gcd`, `lcm`, `if(cond, a, b)` |
| Statistics | `sum`, `product`, `count`, `mean`/`avg`, `median`, `variance` and `stdev` (sample), `var_p` and `stdev_p` (population), `percentile(list, p)` with `p` from 0 to 1 (as `PERCENTILE.INC`) — each takes numbers, lists, or both. `norm_cdf(z)`, `norm_inv(p)`, `erf`, `erfc` for the standard normal distribution: p-values and critical values. `holm(p-values)` and `bh(p-values)` return the list adjusted for multiple comparisons (Holm–Bonferroni and Benjamini–Hochberg), in the order given |
| Distributions | `t_cdf(x, df)`, `t_inv(p, df)` (Student's t), `chi2_cdf(x, df)`, `chi2_inv(p, df)`, `poisson_cdf(k, mean)`, `binom_cdf(k, n, p)`. A CDF is P(X ≤ x); an upper-tail p-value is `1 - …_cdf(…)`, which cannot go below about 10⁻¹⁵ |
| Finance | `pmt(rate, nper, pv[, fv[, type]])`, `fv(rate, nper, pmt[, pv[, type]])`, `pv(rate, nper, pmt[, fv[, type]])`, `npv(rate, flows…)`, `irr(flows[, guess])` — spreadsheet signatures and signs: money paid out is negative |
| Rank tests | `mannwhitney(a, b, alternative[, method])` (Mann–Whitney U) and `ks2(a, b, alternative[, method])` (two-sample Kolmogorov–Smirnov) return the p-value; `mannwhitney_u(a, b)` and `ks2_d(a, b, alternative)` the statistic. `alternative` is `two_sided`, `less` or `greater`; `method` is `auto`, `exact` or `asymptotic` (`asymp` for `ks2`). They follow `scipy.stats` 1.18 — the same statistic, the same choice of method, the same p-values; an asymptotic two-sided `ks2` p-value below about 10⁻¹⁵ reads 0 |

Samples rarely fit in an expression. `lists` binds a name to a numeric column of a
query's result — the same `source` and `ops` as `tuitab_query` — and every step can
use it as a list:

```json
{"lists": {
   "incident": {"source": "cpu.csv", "ops": [{"filter": {"col": "Time", "op": "between", "value": ["2026-09-02 16:40", "2026-09-02 19:00"]}}], "col": "value"},
   "usual":    {"source": "cpu.csv", "ops": [{"filter": {"col": "hour", "op": "in", "value": [16, 17, 18]}}], "col": "value"}
 },
 "expr": "mannwhitney(incident, usual, greater)"}
```

A mistake names the step and says what is wrong: `b (a + c): unknown name 'c' —
earlier steps: a`, `division by zero`, `'=' is not an operator here: compare with
'=='`. A step name cannot shadow a function or a constant.

## Files larger than memory

A CSV, TSV, Parquet, Arrow or JSONL file — or a pattern of them — is never loaded
whole. It is scanned, the pipeline's leading steps become one plan that reads only
the columns and rows they need, and the plan runs on Polars' streaming engine:

- `filter`, `select`, `compute` (any expression except `json_get` and a date
  difference), `sort`, `limit` and `unpivot` join the plan;
- `group_by`, `aggregate` and `frequency` run the same grouping over the stream and
  collect only the groups;
- `tuitab_inspect` counts the rows as they stream past and collects the sample;
  `tuitab_describe` collects only the `columns` it is asked to profile.

So `frequency` over a 17 GB JSONL file holds the counts, not the file. A step that
needs the whole table — `dedup`, `duplicates`, `sample`, `window`, `pivot`, `join`,
`transpose`, `expand_json`, writing to `output.path` — runs in memory over what the
plan has produced by then. Before it collects, the server counts the rows and
estimates their size; more than half the machine's memory is refused with the
numbers and what streams instead, rather than left to run out of memory. Narrow the
rows or columns first, or put a `group_by` before it.

The answers are the same as for a small file: a streamed pipeline and a loaded one
are held to the same output by the test suite.

## Many files at once

A path may be a pattern, and every file it matches is read as one table, in sorted
order:

```json
{"source": {"path": "/data/monthly/*.csv"}}
{"source": {"path": "/site/content/**/index.md"}}
```

The files have to hold the same columns; the one that does not is named rather than
stacked into a table with holes in it. A pattern that matches nothing answers `glob
matched no files: …` — the pattern is fine, there is nothing there, and that is a
different problem from a path that does not exist.

`?` and `[abc]` work too. A relative pattern is resolved from wherever the server was
started, same as a plain path.

### A markdown page as a row

A page is a record: the frontmatter fields are its columns, `body` is the page text,
and `file` is the path it came from. YAML frontmatter between `---` fences and TOML
between `+++` are both read; a `---` inside the body is a rule, not a fence.

```json
{"source": {"path": "/site/content/en/teas/*/index.md"},
 "ops": [{"filter": [{"col": "draft", "op": "ne", "value": true}]},
         {"aggregate": [{"col": "*", "fn": "count"}]}]}
```

Pages need not carry the same fields — a field a page lacks arrives as NULL, the way a
list of JSON objects already behaves. That is what makes a static site checkable
against a database in one call instead of through an export script.

## Reading the answers

Rows come back as arrays matching the `columns` list:

```json
{
  "columns": [{"name": "region", "type": "string"},
              {"name": "amount:sum", "type": "float"}],
  "rows": [["North", 563001.5], ["South", 480000.5]],
  "row_count": 2, "returned": 2, "truncated": false
}
```

- **Percentages are fractions.** `0.42` means 42%.
- **Currency and floats are raw numbers**, not formatted strings — the
  formatting belongs to a person reading a file, not to a model computing.
- **`truncated` matters.** When it is true you are looking at part of the answer.

## Getting a file out

`output.path` writes the result instead of returning rows:

```json
{"output": {"path": "/data/report.xlsx"}}
```

The extension picks the format — `.xlsx`, `.csv`, `.tsv`, `.parquet`, `.arrow`,
`.json`, `.jsonl`, `.yaml`, `.toml`, `.sqlite`, `.duckdb`. Unlike the JSON above,
a file meant for reading *is* formatted for a person: currency symbols, fixed
decimals. A database is not — it is written for the next query, with its columns
typed and NULL left as NULL.

Use it for anything the user wants to keep, and for results too large to return.

For `.sqlite` and `.duckdb`, `output.table` names the table (default `result`):

```json
{"output": {"path": "/data/shop.db", "table": "products"}}
```

Adding a table leaves every other table in the file alone, so a database can be
built up over several calls with nothing extra required.

## Changing what already exists

Creating is one call. **Changing something the user already has — a table, or a
file — takes `--mcp-write`, and happens in two steps.** The first says exactly
what would happen and writes nothing; the second performs precisely that.

The reason is the shape of the client: a model cannot be shown the terminal's
confirmation popup, and where it runs unattended there is nobody to read the
answer either. A second, deliberate call is the only gate that does not depend on
the model behaving.

**Rows in a table** — `tuitab_write` plans, `tuitab_write_apply` performs:

```json
{"source": {"path": "/data/shop.db", "container": "orders"},
 "set": {"status": "archived"},
 "where": [{"col": "id", "op": "in", "value": [3, 7, 11]}]}
```

comes back with the statements, `rows_matched`, those rows as they stand now, and
a `plan_id`. Read them to the user, then:

```json
{"plan_id": "write-1"}
```

One change per call: `set`, `delete` (which always needs a `where`), `insert`, or
`alter` for adding, dropping and renaming columns. The filter resolves to real
rowids rather than being pasted into SQL, and `apply` refuses if the table has
changed since the plan was made — nothing is written and you start over. Any new
plan discards the previous one.

On `insert`, a column with no value — left out, or given `null` — is left out of
the statement, so the schema's `DEFAULT` runs; a column with no `DEFAULT` is
NULL. A `DEFAULT` therefore beats an explicit `null`; to force NULL into a
defaulted column, insert the row and then `set` it.

**A whole table, or a file** — the same handshake, entered through `output`:

```json
{"output": {"path": "/data/shop.db", "table": "products", "overwrite": true}}
```

Replacing a table answers with the `DROP`, `CREATE` and `INSERT` statements, how
many rows the old table holds against how many the new one will, and warnings for
every index, trigger and view that goes with it. Overwriting a plain file answers
with its size and when it was last written. Neither writes anything until
`tuitab_write_apply` runs the plan.

Without `output.overwrite` the refusal says what is at stake — `'inventory'
already exists and holds 47 rows` — rather than naming the flag that would do it.

A query cannot write into the file it read.

## Things that catch people out

**Paths are resolved by the server, not by the model.** A relative path is
relative to wherever the client launched the process, which is rarely where the
data is. Give absolute paths.

**A database always needs a `container`.** `tuitab_inspect` without one lists the
tables and views; every other call is refused — `'shop.db' holds 4 tables and
views; pass 'container' to pick one` — because a database with no table named is
a listing, not data. An `.xlsx` is gentler: without a `container` it lists its
sheets.

**A directory lists its files.** It does not read them — that is what a pattern is
for. The listing is how you find your way around; `dir/*.csv` is how you compute
over what is in there.

**Nested data has a limit.** `tuitab_query` flattens JSON, YAML and TOML into a
table. When the structure is deeper than that survives, `tuitab_jq` takes a jq
program instead.

**Random results report their seed.** An unseeded `sample` or `dedup` returns the
seed it drew, so the same answer can be had again by passing it back.

## What it will not do

There is no SQL and no arbitrary code — `tuitab_calc` reads a closed arithmetic
language, not a programming one. The model sends structured operations,
each one mapping onto a function tuitab already had, and gets back numbers Polars
computed. That is a deliberate limit, not an unfinished one: an operation
validated against a schema fails loudly when it is wrong, where a mistyped SQL
string quietly returns the wrong number.

Not supported: subqueries, self-joins, and SQL of any kind. The model never
writes SQL even when changing a table — tuitab composes the statements and shows
them, and the model's part is to relay them and apply the plan by name.

## Where the documentation lives

The server ships its own. The tool list and usage notes reach the model on
connect, so it does not need this page — this one is for you.
