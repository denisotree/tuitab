use crate::data::aggregator::AggregatorKind;
use crate::data::dataframe::DataFrame;
use crate::types::{ChartAgg, CopyPending, JoinContextItem};
use crate::ui::text_input::TextInput;
use std::collections::HashSet;

#[derive(Default)]
pub struct SaveState {
    pub input: TextInput,
    pub error: Option<String>,
}

#[derive(Default)]
pub struct AggregatorState {
    pub select_index: usize,
    pub selected: HashSet<AggregatorKind>,
}

#[derive(Default)]
pub struct TypeSelectState {
    pub index: usize,
    pub currency_index: usize,
}

#[derive(Default)]
pub struct PartitionState {
    pub select_index: usize,
    pub selected: HashSet<String>,
}

#[derive(Default)]
pub struct ExpressionState {
    pub history: Vec<String>,
    pub history_idx: Option<usize>,
    pub autocomplete_candidates: Vec<String>,
    pub autocomplete_idx: usize,
    pub autocomplete_prefix: String,
}

#[derive(Default)]
pub struct PivotState {
    pub history: Vec<String>,
    pub history_idx: Option<usize>,
}

pub struct ChartState {
    pub ref_col: Option<usize>,
    pub agg: ChartAgg,
    pub agg_index: usize,
}

impl Default for ChartState {
    fn default() -> Self {
        Self {
            ref_col: None,
            agg: ChartAgg::Count,
            agg_index: 0,
        }
    }
}

#[derive(Default)]
pub struct JoinState {
    pub source_index: usize,
    pub other_df: Option<DataFrame>,
    pub other_title: String,
    pub type_index: usize,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    pub left_key_index: usize,
    pub right_key_index: usize,
    pub path_input: TextInput,
    pub path_error: Option<String>,
    pub context_items: Vec<JoinContextItem>,
    pub overview_cursor: usize,
    pub overview_selected: Vec<usize>,
    pub pending_queue: Vec<JoinContextItem>,
}

#[derive(Default)]
pub struct CopyState {
    pub pending: Option<CopyPending>,
    pub format_index: usize,
}
