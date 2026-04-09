use crate::data::dataframe::DataFrame;
use polars::prelude::*;

impl DataFrame {
    /// Sort visible rows by column `col_idx` using Polars native `arg_sort`.
    ///
    /// Builds a sub-DataFrame of visible rows via `get_visible_df()`, runs
    /// `Series::arg_sort()` (SIMD / radix-sort on native types — no String
    /// conversion), then maps the resulting indices back to physical row
    /// positions and updates `row_order`.
    pub fn sort_by(&mut self, col_idx: usize, descending: bool) {
        if col_idx >= self.df.width() || col_idx >= self.columns.len() {
            return;
        }

        let col_name = self.columns[col_idx].name.clone();

        // Build a sub-DataFrame containing only the visible rows
        let visible = match self.get_visible_df() {
            Ok(df) => df,
            Err(_) => return,
        };

        // Retrieve column and run native arg_sort
        let series = match visible.column(&col_name) {
            Ok(c) => c.as_materialized_series().clone(),
            Err(_) => return,
        };

        let sort_options = SortOptions {
            descending,
            nulls_last: true,
            ..Default::default()
        };

        let sorted_idx = series.arg_sort(sort_options);

        // Map arg_sort indices (positions inside `visible`) back to physical
        // row indices (positions inside the original `self.df`).
        let new_order: Vec<usize> = sorted_idx
            .into_no_null_iter()
            .map(|i| self.row_order[i as usize])
            .collect();

        self.row_order = std::sync::Arc::new(new_order);
        self.aggregates_cache = None;
    }

    /// Reset row_order to the original load order.
    pub fn reset_sort(&mut self) {
        self.row_order = self.original_order.clone();
        self.aggregates_cache = None;
    }
}
