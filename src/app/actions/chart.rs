use crate::app::App;
use crate::app_state::ChartDrillKey;
use crate::types::{Action, AppMode, ChartAgg, ColumnType};

impl App {
    pub(crate) fn handle_chart_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::OpenChart => {
                if self.mode == AppMode::Chart || self.mode == AppMode::ChartAggSelect {
                    self.mode = AppMode::Normal;
                    self.chart.ref_col = None;
                    self.status_message.clear();
                } else {
                    self.open_chart();
                }
                None
            }
            Action::ChartAggSelectUp => {
                if self.chart.agg_index > 0 {
                    self.chart.agg_index -= 1;
                }
                None
            }
            Action::ChartAggSelectDown => {
                let max = ChartAgg::all().len() - 1;
                if self.chart.agg_index < max {
                    self.chart.agg_index += 1;
                }
                None
            }
            Action::ApplyChartAgg => {
                self.chart.agg = ChartAgg::all()[self.chart.agg_index];
                self.chart.cursor_bin = 0;
                self.chart.drill_keys.clear();
                self.mode = AppMode::Chart;
                let s = self.stack.active();
                let col_name = s.dataframe.columns[s.cursor_col].name.clone();
                self.status_message = format!(
                    "Chart: {} — ← → navigate | Enter: drill down | v/q/Esc: exit",
                    col_name
                );
                None
            }
            Action::CancelChartAgg => {
                self.mode = AppMode::Normal;
                self.chart.ref_col = None;
                self.status_message.clear();
                None
            }
            Action::ChartCursorPrev => {
                if self.chart.cursor_bin > 0 {
                    self.chart.cursor_bin -= 1;
                }
                None
            }
            Action::ChartCursorNext => {
                self.chart.cursor_bin += 1;
                None
            }
            Action::ChartDrillDown => {
                self.chart_drill_down();
                None
            }
            other => Some(other),
        }
    }

    pub(super) fn open_chart(&mut self) {
        let s = self.stack.active();
        let cur_col = s.cursor_col;
        let cur_type = s.dataframe.columns[cur_col].col_type;

        let ref_col = s
            .dataframe
            .columns
            .iter()
            .enumerate()
            .find(|(i, c)| c.pinned && *i != cur_col)
            .map(|(i, _)| i);

        let ref_type = ref_col.map(|i| s.dataframe.columns[i].col_type);

        let is_date = |ct: ColumnType| matches!(ct, ColumnType::Date | ColumnType::Datetime);
        let is_numeric = |ct: ColumnType| {
            matches!(
                ct,
                ColumnType::Integer
                    | ColumnType::Float
                    | ColumnType::Percentage
                    | ColumnType::Currency
            )
        };
        let is_categorical = |ct: ColumnType| !is_date(ct) && !is_numeric(ct);

        let col_name = s.dataframe.columns[cur_col].name.clone();

        if let (Some(ref_idx), Some(rtype)) = (ref_col, ref_type) {
            if is_date(rtype) && is_numeric(cur_type) {
                self.chart.ref_col = Some(ref_idx);
                self.chart.agg_index = 0;
                self.mode = AppMode::ChartAggSelect;
                self.status_message =
                    "Select aggregation for line chart (↑↓ navigate, Enter confirm)".to_string();
                return;
            }
            if is_date(rtype) && is_categorical(cur_type) {
                self.chart.ref_col = Some(ref_idx);
                self.chart.agg = ChartAgg::Count;
                self.chart.cursor_bin = 0;
                self.chart.drill_keys.clear();
                self.mode = AppMode::Chart;
                self.status_message = format!(
                    "Line chart: count('{}') by date — ← → navigate | Enter: drill | Esc: exit",
                    col_name
                );
                return;
            }
            if is_categorical(rtype) && is_numeric(cur_type) {
                self.chart.ref_col = Some(ref_idx);
                self.chart.agg_index = 0;
                self.mode = AppMode::ChartAggSelect;
                self.status_message =
                    "Select aggregation for bar chart (↑↓ navigate, Enter confirm)".to_string();
                return;
            }
        }

        self.chart.ref_col = None;
        self.chart.cursor_bin = 0;
        self.chart.drill_keys.clear();
        self.mode = AppMode::Chart;
        self.status_message = format!(
            "Chart: {} — ← → navigate | Enter: drill down | v/q/Esc: exit",
            col_name
        );
    }

    pub(super) fn chart_drill_down(&mut self) {
        let cursor = self.chart.cursor_bin;
        let key = match self.chart.drill_keys.get(cursor) {
            Some(k) => k,
            None => return,
        };

        let s = self.stack.active();
        let filter_col = self.chart.ref_col.unwrap_or(s.cursor_col);
        let df = s.dataframe.clone();

        let display_indices: Vec<usize> = match key {
            ChartDrillKey::Exact(ref target) => df.find_rows_by_value(filter_col, target),
            ChartDrillKey::Range(lo, hi) => df.find_rows_in_range(filter_col, *lo, *hi),
        };

        if display_indices.is_empty() {
            self.status_message = "No matching rows found".to_string();
            return;
        }

        let matching: Vec<usize> = display_indices
            .iter()
            .map(|&di| df.row_order[di])
            .collect();

        let label = match self.chart.drill_keys.get(cursor) {
            Some(ChartDrillKey::Exact(s)) => s.clone(),
            Some(ChartDrillKey::Range(lo, hi)) => format!("{:.1}-{:.1}", lo, hi),
            None => return,
        };

        let mut new_df = df.clone();
        new_df.row_order = matching.clone().into();
        new_df.original_order = matching.into();
        new_df.aggregates_cache = None;

        let col_name = new_df.columns[filter_col].name.clone();
        let sheet = crate::sheet::Sheet::new(
            format!("Filter: {} = {}", col_name, label),
            new_df,
        );
        self.chart.drill_return = true;
        self.stack.push(sheet);
        self.mode = AppMode::Normal;
        self.status_message = format!(
            "Drilled into {} = {} — q/Esc: back to chart",
            col_name, label
        );
    }
}
