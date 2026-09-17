# JOIN

> 🇷🇺 [Эта страница на русском](../ru/join.md) · [← Documentation index](README.md)

Press `J` to combine the current table with another one through a step-by-step
wizard. The other table can be **any file tuitab opens** (CSV, TSV, JSON, JSONL,
YAML, TOML, Parquet, Arrow, Excel, Markdown, SQLite, DuckDB) or another sheet you
already have open.

![JOIN wizard](https://raw.githubusercontent.com/denisotree/tuitab/master/.github/assets/join.gif)

## The four steps

### 1. Pick the right-hand table

A popup lists:

- **`[Browse file…]`** — type a path (`Tab` autocompletes) to any supported file.
- **Open sheets** — any other sheets already on your stack.

### 2. Choose the join type

| Option | SQL equivalent | Rows kept |
|--------|----------------|-----------|
| `INNER` | `INNER JOIN` | Only rows with a match in both tables |
| `LEFT` | `LEFT JOIN` | All left rows; unmatched right cells are null |
| `RIGHT` | `RIGHT JOIN` | All right rows; unmatched left cells are null |
| `OUTER` | `FULL OUTER JOIN` | All rows from both tables |
| `ANTI` | `WHERE NOT EXISTS (…)` | Left rows with no match in the right table |
| `SEMI` | `WHERE EXISTS (…)` | Left rows with a match in the right table |
| `DIFF` | — | Both tables compared row by row, like `git diff` — see below |

ANTI and SEMI keep only the left table's columns, and a NULL key matches a NULL
key (in DIFF too) — two empty cells are the same value. The other four follow SQL, where
NULL matches nothing.

### 3. Select the left key columns

A checkbox list of the current table's columns. Toggle with `Space`, or press `a` to
take every column (again to clear). The **order** you pick them in matters —
left key 1 matches right key 1, and so on. Each column shows its type. Press
`Enter` to continue.

### 4. Select the right key columns

The same list for the other table. Columns whose names match your left keys are
pre-selected. Adjust and press `Enter` to run the join.

> The key counts must match: two left keys ⇒ exactly two right keys. A mismatch
> shows an error in the status bar.

> Paired keys must have the same type. A right key whose type cannot meet its
> left partner is shown in red, and `Enter` names both columns and their types
> instead of joining. Change one of them with `t` and run JOIN again; to retype a
> file's column, open it as a sheet first. Integers of different widths, floats
> of different widths and datetimes of different precision are matched as they
> are.

## Result

A new sheet is pushed onto the stack titled `left JOIN right` (`left ANTI JOIN
right`, `left SEMI JOIN right`, `left DIFF right`). Press `Esc` / `q`
to pop back to the original table. Non-key columns that exist in both tables get
a `_right` suffix so nothing is overwritten.

## Worked example

```sh
# orders.csv:    order_id, customer_id, amount
# customers.csv: customer_id, name, country
tuitab orders.csv
```

1. Press `J`.
2. Choose **`[Browse file…]`**, type `customers.csv`, press `Enter`.
3. Choose **LEFT**, press `Enter`.
4. Toggle `customer_id` on the left, press `Enter`.
5. Toggle `customer_id` on the right, press `Enter`.

The result is every order enriched with its customer's `name` and `country`.

## Finding what differs between two tables

DIFF compares two tables the way `git diff` compares two files. The keys say
which rows are the same row; every other column both tables have is compared.

```sh
tuitab march.csv
```

1. Press `J`, choose **`[Browse file…]`**, type `april.csv`, press `Enter`.
2. Choose **DIFF**, press `Enter`.
3. Toggle the column that identifies a row (say `id`), press `Enter`.
4. The right key is already matched by name — press `Enter`.

```
 _diff │ id name   city   amount  name_right city_right amount_right
 =     │ 1  Anna   Lisbon 100
 =     │ 2  Boris         200
 ~     │ 3  Clara  Porto  300                           350
 -     │ 4  Dmitry Faro   400
 +     │ 5  Elena  Braga  500
```

- `_diff` is pinned first: `=` same, `~` changed (yellow, the differing cells
  highlighted), `-` only in the left table (red), `+` only in the right table
  (green).
- `<column>_right` shows the right table's value only where it differs from the
  left one.
- Left rows keep their order; added rows follow. The status bar counts each kind.
- NULL equals NULL, in keys and in cells; a value that became NULL is a change.
- `_diff` is an ordinary column: filter on it to see only the changes, or press
  `F` on it for the counts.

Press `a` at step 3 to key on every column: then a changed row shows as a `-` row
and a `+` row, exactly as in git, and nothing is `~`.

A key has to be unique on both sides — with two rows per key there is no telling
which to compare with which, so DIFF asks for more key columns instead. Columns
only one table has are carried along but not compared.

## See also

- [Pivot tables](pivot.md) to summarise the joined result.
- [Keybindings](keybindings.md) for sheet navigation (`Esc` / `q` to pop back).
