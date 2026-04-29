use crate::app::App;
use crate::data::aggregator::AggregatorKind;
use crate::data::dataframe::DataFrame;
use crate::types::{Action, AppMode, ColumnType};
use std::fmt::Write as _;

impl App {
    pub(crate) fn handle_aggregator_action(&mut self, action: Action) -> Option<Action> {
        match action {
            Action::OpenAggregatorSelect => {
                let s = self.stack.active();
                let col = s.cursor_col;
                let col_meta = &s.dataframe.columns[col];

                self.aggregator.select_index = 0;
                self.aggregator.selected = col_meta.aggregators.iter().cloned().collect();

                self.mode = AppMode::AggregatorSelect;
                self.status_message = "Space to toggle, Enter to apply, Esc to cancel".to_string();
                None
            }
            Action::ApplyAggregators => {
                if self.mode == AppMode::Calculating {
                    self.apply_aggregators();
                } else {
                    self.mode = AppMode::Calculating;
                    self.pending_action = Some(Action::ApplyAggregators);
                }
                None
            }
            Action::AggregatorSelectUp => {
                if self.aggregator.select_index > 0 {
                    self.aggregator.select_index -= 1;
                } else {
                    let max = AggregatorKind::all().len();
                    if max > 0 {
                        self.aggregator.select_index = max - 1;
                    }
                }
                None
            }
            Action::AggregatorSelectDown => {
                self.aggregator.select_index += 1;
                if self.aggregator.select_index >= AggregatorKind::all().len() {
                    self.aggregator.select_index = 0;
                }
                None
            }
            Action::ToggleAggregatorSelection => {
                let all_aggs = AggregatorKind::all();
                if self.aggregator.select_index < all_aggs.len() {
                    let agg = all_aggs[self.aggregator.select_index];
                    if self.aggregator.selected.contains(&agg) {
                        self.aggregator.selected.remove(&agg);
                    } else {
                        self.aggregator.selected.insert(agg);
                    }
                }
                None
            }
            Action::ClearAggregators => {
                let s = self.stack.active_mut();
                let col = s.cursor_col;
                s.dataframe.clear_aggregators(col);
                self.mode = AppMode::Normal;
                self.status_message = "Aggregators cleared".to_string();
                None
            }
            Action::CancelAggregatorSelect => {
                self.mode = AppMode::Normal;
                self.status_message.clear();
                None
            }
            Action::QuickAggregate => {
                self.quick_aggregate();
                None
            }
            other => Some(other),
        }
    }

    pub(super) fn apply_aggregators(&mut self) {
        let s = self.stack.active_mut();
        let col = s.cursor_col;
        s.dataframe.clear_aggregators(col);

        let mut errs = Vec::new();
        for agg in AggregatorKind::all() {
            if self.aggregator.selected.contains(agg) {
                if let Err(e) = s.dataframe.add_aggregator(col, *agg) {
                    errs.push(e.to_string());
                }
            }
        }

        s.dataframe.aggregates_cache = None;
        let _ = s.dataframe.compute_aggregates();

        self.mode = AppMode::Normal;
        if errs.is_empty() {
            self.status_message = "Aggregators applied successfully".to_string();
        } else {
            self.status_message = format!("Errors: {}", errs.join(", "));
        }
    }

    pub(super) fn quick_aggregate(&mut self) {
        let s = self.stack.active();
        let col = s.cursor_col;
        let col_meta = &s.dataframe.columns[col];
        let col_type = col_meta.col_type;
        let values: Vec<String> = (0..s.dataframe.visible_row_count())
            .map(|i| DataFrame::anyvalue_to_string_fmt(&s.dataframe.get_val(i, col)))
            .collect();

        let total = values.len();
        let distinct: std::collections::HashSet<String> = values.iter().cloned().collect();
        let distinct_count = distinct.len();
        let min_val = values.iter().min().unwrap_or(&String::new()).clone();
        let max_val = values.iter().max().unwrap_or(&String::new()).clone();

        let mut msg = format!(
            "{}: count={}  distinct={}  min={}  max={}",
            col_meta.name, total, distinct_count, min_val, max_val
        );

        if matches!(col_type, ColumnType::Integer | ColumnType::Float) {
            let nums: Vec<f64> = values
                .iter()
                .filter_map(|s| s.parse::<f64>().ok())
                .collect();
            if !nums.is_empty() {
                let sum: f64 = nums.iter().sum();
                let avg = sum / nums.len() as f64;
                let _ = write!(msg, "  sum={}  avg={:.2}", sum as i64, avg);
            }
        }

        self.status_message = msg;
    }
}
