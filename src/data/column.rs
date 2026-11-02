use crate::data::aggregator::AggregatorKind;
use crate::data::expression::Expr;
use crate::types::{ColumnType, CurrencyKind};
use serde::{Deserialize, Serialize};

/// Two-state toggle for column display width (`_` key).
/// Header width (name + 2 chars padding) is the floor in both modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ColumnWidthMode {
    /// Width auto-calculated at load time (bounded, ~40 chars max, samples first 1000 rows).
    #[default]
    Default,
    /// Fitted to full content width across all rows. Never less than the header width.
    Fit,
}

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
    /// Current width display mode (Default / Fit).
    /// Old sessions that lack this field get Default.
    #[serde(default)]
    pub width_mode: ColumnWidthMode,
    /// Width saved the first time calc_column_width runs (= load-time width).
    /// Used by the Default mode to restore the original auto-calculated width.
    #[serde(default)]
    pub default_width: u16,
    /// Whether this column is selected (zs/zu in z-prefix mode)
    pub selected: bool,
    /// Backup of original Datetime values before converting to Date
    /// Stores formatted datetime strings for recovery
    pub backup_datetime_str: Option<Vec<Option<String>>>,

    /// The name this column had in the database table the sheet was loaded from.
    ///
    /// `None` means it was not there — created by `zi`, `=` or `zx` — or that the sheet
    /// did not come from a database at all.  Renaming changes `name` and leaves this
    /// alone, which is what makes A→B→C a single `RENAME COLUMN` rather than two, and
    /// what tells an added column apart from a renamed one after the fact.
    #[serde(default)]
    pub db_origin: Option<String>,

    /// The type the user deliberately assigned with `t`.
    ///
    /// Separate from [`Self::col_type`] because `col_replace` (`zr`/`zg`) force-sets
    /// `String` as a side effect of a find-and-replace, and a find-and-replace must
    /// never turn into an `ALTER COLUMN … TYPE`.
    #[serde(default)]
    pub db_retype: Option<ColumnType>,

    /// Set when this column was read as integers holding an epoch and shown as a
    /// datetime: the unit those integers were in.  Saving writes the integers back, and
    /// retyping to Integer restores them, so reading a file never changes what it holds.
    #[serde(default)]
    pub epoch_unit: Option<EpochUnit>,
    /// A datetime read with a time zone: held, shown and compared as UTC without one
    /// (this build has no zone database), and written back with it by the formats
    /// that store one.
    #[serde(default)]
    pub time_zone: Option<String>,
}

/// The unit of an integer epoch column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EpochUnit {
    Seconds,
    Millis,
    Micros,
    Nanos,
}

impl EpochUnit {
    pub fn label(self) -> &'static str {
        match self {
            EpochUnit::Seconds => "s",
            EpochUnit::Millis => "ms",
            EpochUnit::Micros => "us",
            EpochUnit::Nanos => "ns",
        }
    }

    pub fn parse(label: &str) -> Option<Self> {
        Some(match label {
            "s" => EpochUnit::Seconds,
            "ms" => EpochUnit::Millis,
            "us" => EpochUnit::Micros,
            "ns" => EpochUnit::Nanos,
            _ => return None,
        })
    }

    /// How many of this unit make a nanosecond's worth of scale: s→1e9, ms→1e6, …
    pub fn nanos(self) -> i64 {
        match self {
            EpochUnit::Seconds => 1_000_000_000,
            EpochUnit::Millis => 1_000_000,
            EpochUnit::Micros => 1_000,
            EpochUnit::Nanos => 1,
        }
    }

    /// The unit whose values span 2001–2286: the range a real timestamp lies in, and
    /// the ranges of the four units do not overlap.
    pub fn of_range(min: i64, max: i64) -> Option<Self> {
        let unit = |v: i64| -> Option<Self> {
            match v {
                1_000_000_000..=9_999_999_999 => Some(EpochUnit::Seconds),
                1_000_000_000_000..=9_999_999_999_999 => Some(EpochUnit::Millis),
                1_000_000_000_000_000..=9_999_999_999_999_999 => Some(EpochUnit::Micros),
                1_000_000_000_000_000_000..=i64::MAX => Some(EpochUnit::Nanos),
                _ => None,
            }
        };
        match (unit(min), unit(max)) {
            (Some(a), Some(b)) if a == b => Some(a),
            _ => None,
        }
    }

    /// Integers in this unit as a datetime column (UTC, no zone), and back.
    pub fn to_datetime(
        self,
        s: &polars::prelude::Series,
    ) -> polars::prelude::PolarsResult<polars::prelude::Series> {
        use polars::prelude::{DataType, TimeUnit};
        let ints = s.cast(&DataType::Int64)?;
        match self {
            EpochUnit::Seconds => {
                (&ints * 1000i64).cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
            }
            EpochUnit::Millis => ints.cast(&DataType::Datetime(TimeUnit::Milliseconds, None)),
            EpochUnit::Micros => ints.cast(&DataType::Datetime(TimeUnit::Microseconds, None)),
            EpochUnit::Nanos => ints.cast(&DataType::Datetime(TimeUnit::Nanoseconds, None)),
        }
    }

    pub fn to_integers(
        self,
        s: &polars::prelude::Series,
    ) -> polars::prelude::PolarsResult<polars::prelude::Series> {
        use polars::prelude::{DataType, TimeUnit};
        let ns = s
            .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))?
            .cast(&DataType::Int64)?;
        Ok(&ns / self.nanos())
    }
}

/// Whether a column's name says it holds a time: `time`, `ts`, `timestamp`,
/// `datetime`, `date`, `epoch`, or a name ending `_time`, `_ts`, `_timestamp`, `_at`.
pub fn names_a_time(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    matches!(
        n.as_str(),
        "time" | "ts" | "timestamp" | "datetime" | "date" | "epoch"
    ) || ["_time", "_ts", "_timestamp", "_at"]
        .iter()
        .any(|end| n.ends_with(end))
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
            width_mode: ColumnWidthMode::Default,
            default_width: 0,
            selected: false,
            backup_datetime_str: None,
            db_origin: None,
            db_retype: None,
            epoch_unit: None,
            time_zone: None,
        }
    }
}
