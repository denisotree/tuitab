//! Wide to long: the columns named by `columns` become rows of (`name`, `value`),
//! with `index` repeated on each.

use crate::data::dataframe::DataFrame;
use polars::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub struct Spec {
    /// The columns that identify a row, kept on every row the unpivot makes.
    pub index: Vec<String>,
    /// The columns to turn into rows; `None` is every column outside `index`.
    pub columns: Option<Vec<String>>,
    /// The new column holding which column a value came from.
    pub name: String,
    /// The new column holding the value.
    pub value: String,
    pub drop_nulls: bool,
}

/// The plan, on a lazy frame of the given schema — the eager path below and the
/// streaming one both build it here.
pub fn unpivot_lazy(lf: LazyFrame, schema: &Schema, spec: &Spec) -> Result<LazyFrame, String> {
    if spec.index.is_empty() {
        return Err(
            "'unpivot' needs at least one 'index' column — the ones that say which row \
                    a value belongs to"
                .to_string(),
        );
    }
    let has = |c: &str| schema.get(c).is_some();
    for c in &spec.index {
        if !has(c) {
            return Err(format!("unpivot: no column '{}'", c));
        }
    }
    let columns: Vec<String> = match &spec.columns {
        Some(list) => {
            for c in list {
                if !has(c) {
                    return Err(format!("unpivot: no column '{}'", c));
                }
            }
            list.clone()
        }
        None => schema
            .iter_names()
            .map(|n| n.to_string())
            .filter(|n| !spec.index.contains(n))
            .collect(),
    };
    if columns.is_empty() {
        return Err("unpivot: no columns left to turn into rows".to_string());
    }
    for new in [&spec.name, &spec.value] {
        if spec.index.contains(new) {
            return Err(format!(
                "unpivot: '{}' is an index column; choose another name for the new one",
                new
            ));
        }
    }
    if spec.name == spec.value {
        return Err("unpivot: 'name' and 'value' must differ".to_string());
    }

    let value_type = common_type(schema, &columns)?;
    let frames: Vec<LazyFrame> = columns
        .iter()
        .map(|c| {
            let mut exprs: Vec<Expr> = spec.index.iter().map(|i| col(i.as_str())).collect();
            exprs.push(lit(c.as_str()).alias(spec.name.as_str()));
            exprs.push(
                col(c.as_str())
                    .cast(value_type.clone())
                    .alias(spec.value.as_str()),
            );
            lf.clone().select(exprs)
        })
        .collect();
    let mut long = concat(frames, UnionArgs::default()).map_err(|e| e.to_string())?;
    if spec.drop_nulls {
        long = long.filter(col(spec.value.as_str()).is_not_null());
    }
    Ok(long)
}

/// The one type every unpivoted value fits: a number when they are all numbers, text
/// when all text, the shared type when they agree.  Text beside numbers is refused —
/// casting the numbers to text would hand back a value column no aggregate touches.
fn common_type(schema: &Schema, columns: &[String]) -> Result<DataType, String> {
    let types: Vec<(&String, &DataType)> = columns
        .iter()
        .map(|c| (c, schema.get(c.as_str()).unwrap()))
        .collect();
    let first = types[0].1;
    if types.iter().all(|(_, t)| *t == first) {
        return Ok(first.clone());
    }
    if types.iter().all(|(_, t)| t.is_primitive_numeric()) {
        return Ok(if types.iter().all(|(_, t)| t.is_integer()) {
            DataType::Int64
        } else {
            DataType::Float64
        });
    }
    let text: Vec<&str> = types
        .iter()
        .filter(|(_, t)| !t.is_primitive_numeric())
        .map(|(c, _)| c.as_str())
        .collect();
    let numbers: Vec<&str> = types
        .iter()
        .filter(|(_, t)| t.is_primitive_numeric())
        .map(|(c, _)| c.as_str())
        .collect();
    Err(format!(
        "unpivot: {} hold text and {} hold numbers; one value column cannot be both. \
         Unpivot them separately, or name only one kind in 'columns'",
        text.join(", "),
        numbers.join(", ")
    ))
}

/// Unpivot the visible rows of a sheet.
pub fn unpivot(df: &DataFrame, spec: &Spec) -> Result<DataFrame, String> {
    let visible = df.get_visible_df()?;
    let schema = visible.schema().clone();
    let long = unpivot_lazy(visible.lazy(), &schema, spec)?
        .collect()
        .map_err(|e| e.to_string())?;
    let mut out = crate::data::io::wrap_polars_df(long).map_err(|e| e.to_string())?;
    // The index columns keep what the sheet had set on them.
    for (i, name) in spec.index.iter().enumerate() {
        if let Ok(src) = df.column_index(name) {
            let mut meta = df.columns[src].clone();
            meta.pinned = false;
            out.columns[i] = meta;
        }
    }
    out.calc_widths(40, 1000);
    Ok(out)
}
