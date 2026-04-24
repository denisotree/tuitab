use crate::data::aggregator::AggregatorKind;
use crate::data::expression::Expr;
use crate::types::{ColumnType, CurrencyKind};
use serde::{Deserialize, Serialize};

/// Metadata about a single data column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    /// Column name (from CSV header, or auto-generated)
    pub name: String,
    /// Inferred (or user-assigned) type of the column data
    pub col_type: ColumnType,
    /// Display width in characters (auto-calculated)
    pub width: u16,
    /// Minimum width (length of column name + 2 for type icon)
    pub min_width: u16,
    /// Active aggregators assigned by the user (Phase 12)
    pub aggregators: Vec<AggregatorKind>,
    /// Expression for computed columns (None for regular data columns)
    pub expression: Option<Expr>,
    /// Number of decimal places to display for numeric types
    pub precision: u8,
    /// Whether this column is pinned to the left
    pub pinned: bool,
    /// Currency kind, used when col_type == Currency
    pub currency: Option<CurrencyKind>,
    /// Whether the column width is currently expanded to content width (toggle state for _ / g_)
    pub width_expanded: bool,
    /// Whether this column is selected (zs/zu in z-prefix mode)
    pub selected: bool,
    /// Backup of original Datetime values before converting to Date
    /// Stores formatted datetime strings for recovery
    pub backup_datetime_str: Option<Vec<Option<String>>>,
}

impl ColumnMeta {
    pub fn new(name: String) -> Self {
        let name_w = unicode_width::UnicodeWidthStr::width(name.as_str()) as u16;
        // +2: 1 separator space + 1 char for the type icon so the icon never covers the name
        let min_width = name_w + 2;
        Self {
            name,
            col_type: ColumnType::String,
            min_width,
            width: min_width.max(8), // default minimum 8 chars
            aggregators: Vec::new(),
            expression: None,
            precision: 2,
            pinned: false,
            currency: None,
            width_expanded: false,
            selected: false,
            backup_datetime_str: None,
        }
    }
}
