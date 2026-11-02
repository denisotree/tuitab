# Pivot tables

> 🇷🇺 [Эта страница на русском](../ru/pivot.md) · [← Documentation index](README.md)

A pivot table cross-tabulates your data: one set of columns becomes the **rows**,
another column's values become the **columns**, and an aggregation fills the
cells.

![Pivot table](https://raw.githubusercontent.com/denisotree/tuitab/master/.github/assets/pivot.gif)

## How it works

Unlike a spreadsheet wizard, tuitab reads the pivot layout from the table state:

1. **Rows (index)** — the columns you've **pinned** with `!`.
2. **Columns (spread)** — the column under the **cursor**.
3. **Cells** — the **aggregation formula** you type after pressing `W`.

So the `W` prompt only asks for the formula, e.g. `sum(revenue)`.

## Aggregation formula

Use these aggregation functions over a column:

| Function | Result |
|----------|--------|
| `sum(col)` | Total |
| `count(col)` | Number of values |
| `mean(col)` | Average |
| `median(col)` | Median |
| `min(col)` / `max(col)` | Smallest / largest |

Formulas can combine aggregations with arithmetic:

```text
sum(revenue)
count(order_id)
sum(revenue) / sum(units)      # average price per unit
```

The input remembers history (`↑` / `↓`) and autocompletes column names (`Tab`).

## Step by step

```sh
tuitab sales.csv     # columns: date, region, category, units, revenue
```

1. Move to `region` and pin it with `!` — this becomes the row index.
2. Move the cursor to `category` — its values become the columns.
3. Press `W`, type `sum(revenue)`, press `Enter`.

The result is a grid of **region × category** with summed revenue in each cell,
opened as a new sheet. Press `Esc` / `q` to pop back.

> Pin **more than one** column to nest the row index (e.g. pin `region` *and*
> `category`, then pivot another column).

## The other way: unpivot

A wide table — a column per country, per endpoint — goes long with `gW`: pin the
columns that identify a row (`Time`), press `gW`, and every other column becomes
rows of (`variable`, `value`) on a new sheet, the pinned columns repeated on each.
Over MCP it is the `unpivot` operation.

## See also

- [Charts](charts.md) — the grouped bar chart is the visual cousin of a pivot.
- [Recipes](recipes.md) — frequency tables (`F`) for quick single-column counts.
