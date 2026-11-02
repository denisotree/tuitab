//! Grouping and totalling, shared by the TUI and the MCP server.
//!
//! Distinct from [`crate::data::dataframe::DataFrame::build_frequency_table`],
//! which answers a different question. A frequency table ranks values by how
//! often they occur: it always carries a `Count`, always sorts by it
//! descending, and appends a share column. That is what you want from "which
//! products sell most". A group-by is the other thing — exactly the aggregates
//! asked for, in the order asked for, with no ordering imposed.
//!
//! Both live here rather than in either surface, so `gg` in the terminal and
//! `group_by` over MCP cannot drift apart. The arithmetic itself is still
//! tuitab's: [`AggregatorKind::to_expr`] is what defines its median, its p95
//! and its sample standard deviation.

use crate::data::aggregator::AggregatorKind;
use crate::data::column::ColumnMeta;
use crate::data::dataframe::DataFrame;
use crate::types::ColumnType;
use polars::prelude::*;

/// One aggregate to compute: a column and the function to apply to it.
#[derive(Clone, Debug)]
pub struct AggSpec {
    /// A column name, or `*` for a row count.
    pub col: String,
    pub kind: AggregatorKind,
    /// Aggregate only the rows these predicates pick — a conditional aggregate.
    pub where_: Vec<crate::data::filter::Clause>,
    /// The result column's name; required with `where_`, whose answers would
    /// otherwise share one name.
    pub as_name: Option<String>,
}

impl AggSpec {
    pub fn new(col: impl Into<String>, kind: AggregatorKind) -> Self {
        Self {
            col: col.into(),
            kind,
            where_: Vec::new(),
            as_name: None,
        }
    }

    /// The name the result column will carry: `as` when given, else `salary:sum`,
    /// or plain `count` for a row count.
    pub fn output_name(&self) -> String {
        if let Some(name) = &self.as_name {
            return name.clone();
        }
        if self.col == "*" {
            self.kind.name().to_string()
        } else {
            format!("{}:{}", self.col, self.kind.name())
        }
    }
}

/// Build the Polars expressions and the column metadata for a set of
/// aggregates, refusing any the source column cannot carry.
///
/// The refusal matters: `build_frequency_table` skips an incompatible
/// aggregator in silence, which hands the caller a short answer with nothing to
/// notice. Asking for the sum of a column of names is a mistake worth hearing
/// about.
fn build_aggregates(
    df: &DataFrame,
    agg: &[AggSpec],
) -> Result<(Vec<Expr>, Vec<ColumnMeta>), String> {
    let mut exprs = Vec::with_capacity(agg.len());
    let mut metas = Vec::with_capacity(agg.len());

    for spec in agg {
        // The rows a conditional aggregate reads: the predicates as one mask, where a
        // NULL verdict picks nothing.
        let mask = if spec.where_.is_empty() {
            None
        } else {
            let expr = crate::data::filter::clauses_to_expr(df, &spec.where_)?
                .ok_or("an empty 'where' picks every row — leave it out")?;
            for p in spec.where_.iter().flat_map(|c| c.predicates()) {
                df.column_index(&p.col)?;
            }
            Some(
                expr.to_polars_expr_with(Some(df.df.schema().as_ref()))?
                    .fill_null(lit(false)),
            )
        };

        // A row count needs no source column.
        if spec.col == "*" {
            if spec.kind != AggregatorKind::Count {
                return Err(format!(
                    "'{}' needs a column; only 'count' works with '*'",
                    spec.kind.name()
                ));
            }
            let alias = spec.output_name();
            let counted = match &mask {
                Some(m) => m.clone().cast(DataType::UInt32).sum(),
                None => len(),
            };
            exprs.push(counted.alias(alias.as_str()));
            let mut meta = ColumnMeta::new(alias);
            meta.col_type = ColumnType::Integer;
            metas.push(meta);
            continue;
        }

        let source = &df.columns[df.column_index(&spec.col)?];

        if !spec.kind.is_compatible(source.col_type) {
            return Err(format!(
                "Cannot compute {} over '{}': the column is {}, and {} needs a numeric one",
                spec.kind.name(),
                spec.col,
                source.col_type.name(),
                spec.kind.name()
            ));
        }

        let target = crate::data::column_expr(&spec.col);
        let target = match &mask {
            Some(m) => target.filter(m.clone()),
            None => target,
        };
        let expr = spec.kind.to_expr_of(target).ok_or_else(|| {
            format!(
                "'{}' is not available as a group aggregate",
                spec.kind.name()
            )
        })?;

        let alias = spec.output_name();
        exprs.push(expr.alias(alias.as_str()));

        let mut meta = ColumnMeta::new(alias);
        if spec.kind.preserves_col_type() {
            meta.col_type = source.col_type;
            meta.currency = source.currency;
            meta.precision = source.precision;
        } else {
            meta.col_type = ColumnType::Integer;
        }
        metas.push(meta);
    }

    Ok((exprs, metas))
}

/// Group the visible rows by `by`, computing exactly `agg`.
///
/// Groups come back in order of first appearance. That is `group_by_stable`
/// rather than `group_by`: the unstable variant returns them in whatever order
/// the hash table produced, which would make the same question give differently
/// ordered answers on different runs.
pub fn group_by(df: &DataFrame, by: &[String], agg: &[AggSpec]) -> Result<DataFrame, String> {
    group_by_from(df, df.get_visible_df()?.lazy(), by, agg)
}

/// [`group_by`] over any lazy source of `df`'s columns — its own rows, or a file
/// scanned and streamed; `df` supplies the column types and names.
pub fn group_by_from(
    df: &DataFrame,
    source: LazyFrame,
    by: &[String],
    agg: &[AggSpec],
) -> Result<DataFrame, String> {
    if by.is_empty() {
        return Err(
            "grouping needs at least one column to group by — for a grand total, total() it"
                .to_string(),
        );
    }
    if agg.is_empty() {
        return Err("grouping needs at least one aggregate".to_string());
    }

    let mut metas: Vec<ColumnMeta> = Vec::with_capacity(by.len() + agg.len());
    for name in by {
        metas.push(df.columns[df.column_index(name)?].clone());
    }

    let (exprs, agg_metas) = build_aggregates(df, agg)?;
    metas.extend(agg_metas);

    let grouped = source
        .group_by_stable(
            by.iter()
                .map(|s| crate::data::column_expr(s.as_str()))
                .collect::<Vec<_>>(),
        )
        .agg(exprs);
    let grouped = crate::data::collect(grouped).map_err(|e| format!("group_by failed: {}", e))?;

    Ok(DataFrame::from_parts(grouped, metas))
}

/// Aggregate the visible rows as one group — a grand total.
///
/// Its own function rather than `group_by` with an empty `by`: an empty list is
/// easy to pass by accident, and "what is the total revenue" deserves to be
/// asked directly rather than as a degenerate case of something else.
pub fn total(df: &DataFrame, agg: &[AggSpec]) -> Result<DataFrame, String> {
    total_from(df, df.get_visible_df()?.lazy(), agg)
}

/// [`total`] over any lazy source of `df`'s columns.
pub fn total_from(df: &DataFrame, source: LazyFrame, agg: &[AggSpec]) -> Result<DataFrame, String> {
    if agg.is_empty() {
        return Err("a total needs at least one aggregate".to_string());
    }

    let (exprs, metas) = build_aggregates(df, agg)?;

    let totalled = crate::data::collect(source.select(exprs))
        .map_err(|e| format!("aggregate failed: {}", e))?;

    Ok(DataFrame::from_parts(totalled, metas))
}

/// Distribution of `by`, ranked by how often each combination occurs.
///
/// The counterpart to [`group_by`]: this always carries a `Count`, sorts by it
/// descending and adds a `Pct` share. Use it for "how many of each"; use
/// `group_by` when the aggregates and the ordering are yours to choose.
///
/// The ASCII `Bar` column that [`DataFrame::build_frequency_table`] appends is
/// dropped — it is a sparkline for a terminal, and meaningless in JSON or in a
/// saved file. Callers that want it should use the builder directly.
pub fn frequency(df: &DataFrame, by: &[String], agg: &[AggSpec]) -> Result<DataFrame, String> {
    frequency_from(df, df.get_visible_df()?.lazy(), by, agg)
}

/// [`frequency`] over any lazy source of `df`'s columns.
pub fn frequency_from(
    df: &DataFrame,
    source: LazyFrame,
    by: &[String],
    agg: &[AggSpec],
) -> Result<DataFrame, String> {
    if by.is_empty() {
        return Err("a frequency table needs at least one column to count by".to_string());
    }

    let group_indices: Vec<usize> = by
        .iter()
        .map(|name| df.column_index(name))
        .collect::<Result<_, _>>()?;

    // Same refusal as `group_by`: an aggregate the column cannot carry is an
    // error, not something to drop quietly on the way through.
    let mut aggregated: Vec<(usize, Vec<AggregatorKind>)> = Vec::new();
    for spec in agg {
        if !spec.where_.is_empty() || spec.as_name.is_some() {
            return Err(
                "frequency takes plain aggregates; for one over some rows use group_by \
                 with 'where' and 'as'"
                    .to_string(),
            );
        }
        let idx = df.column_index(&spec.col)?;
        let source = &df.columns[idx];
        if !spec.kind.is_compatible(source.col_type) {
            return Err(format!(
                "Cannot compute {} over '{}': the column is {}, and {} needs a numeric one",
                spec.kind.name(),
                spec.col,
                source.col_type.name(),
                spec.kind.name()
            ));
        }
        match aggregated.iter_mut().find(|(i, _)| *i == idx) {
            Some((_, kinds)) => kinds.push(spec.kind),
            None => aggregated.push((idx, vec![spec.kind])),
        }
    }

    let (pdf, metas) = if group_indices.len() == 1 {
        df.build_frequency_table_from(source, group_indices[0], &aggregated)?
    } else {
        df.build_multi_frequency_table_from(source, &group_indices, &aggregated)?
    };

    let mut result = DataFrame::from_parts(pdf, metas);
    if let Some(bar) = result.columns.iter().position(|c| c.name == "Bar") {
        result.drop_column(bar)?;
    }
    Ok(result)
}
