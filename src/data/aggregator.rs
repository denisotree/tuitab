use crate::types::ColumnType;
use serde::{Deserialize, Serialize};

/// Available aggregation functions for a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregatorKind {
    Count,
    Distinct,
    Sum,
    Avg,
    Min,
    Max,
    Median,
    Stdev,
    P5,
    P25,
    P50,
    P75,
    P95,
    Random,
    List,
    Set,
}

impl AggregatorKind {
    /// Short name displayed in the footer cell.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Distinct => "distinct",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Median => "median",
            Self::Stdev => "stdev",
            Self::P5 => "p5",
            Self::P25 => "p25",
            Self::P50 => "p50",
            Self::P75 => "p75",
            Self::P95 => "p95",
            Self::Random => "random",
            Self::List => "list",
            Self::Set => "set",
        }
    }

    /// All aggregator variants in display order.
    #[allow(dead_code)]
    pub fn all() -> &'static [AggregatorKind] {
        &[
            Self::Count,
            Self::Distinct,
            Self::Sum,
            Self::Avg,
            Self::Min,
            Self::Max,
            Self::Median,
            Self::Stdev,
            Self::P5,
            Self::P25,
            Self::P50,
            Self::P75,
            Self::P95,
            Self::Random,
            Self::List,
            Self::Set,
        ]
    }

    /// Whether this aggregator is compatible with the given column type.
    /// Count, Distinct, Min, Max work with all types.
    /// Numeric aggregators (Sum, Avg, Median, Stdev, percentiles) require Integer or Float.
    pub fn is_compatible(&self, col_type: ColumnType) -> bool {
        match self {
            Self::Count | Self::Distinct | Self::Min | Self::Max | Self::Random | Self::List | Self::Set => true,
            Self::Sum | Self::Avg | Self::Median | Self::Stdev => matches!(col_type, ColumnType::Integer | ColumnType::Float | ColumnType::Boolean | ColumnType::Percentage | ColumnType::Currency),
            _ => matches!(col_type, ColumnType::Integer | ColumnType::Float | ColumnType::Percentage | ColumnType::Currency),
        }
    }

    /// Map to Polars expression
    pub fn to_expr(&self, col_name: &str) -> Option<polars::lazy::dsl::Expr> {
        let c = polars::lazy::dsl::col(col_name);
        match self {
            Self::Count => Some(c.count()),
            Self::Distinct => Some(c.n_unique()),
            Self::Sum => Some(c.sum()),
            Self::Avg => Some(c.mean()),
            Self::Min => Some(c.min()),
            Self::Max => Some(c.max()),
            Self::Median => Some(c.median()),
            Self::Stdev => Some(c.std(1)),
            Self::P5 => Some(c.quantile(polars::lazy::dsl::lit(0.05), polars::prelude::QuantileMethod::Linear)),
            Self::P25 => Some(c.quantile(polars::lazy::dsl::lit(0.25), polars::prelude::QuantileMethod::Linear)),
            Self::P50 => Some(c.quantile(polars::lazy::dsl::lit(0.50), polars::prelude::QuantileMethod::Linear)),
            Self::P75 => Some(c.quantile(polars::lazy::dsl::lit(0.75), polars::prelude::QuantileMethod::Linear)),
            Self::P95 => Some(c.quantile(polars::lazy::dsl::lit(0.95), polars::prelude::QuantileMethod::Linear)),
            _ => None,
        }
    }

    /// Compute the aggregation on the provided list of string values.
    /// (We pass `ColumnType` and `precision` to know how to parse and format).
    pub fn compute(
        &self,
        values: &[String],
        col_type: ColumnType,
        precision: u8,
        currency: Option<crate::types::CurrencyKind>,
    ) -> String {
        let n = values.len() as f64;
        if n == 0.0 {
            return String::new();
        }
        match self {
            Self::Count => values.len().to_string(),

            Self::Distinct => {
                let unique: std::collections::HashSet<&String> = values.iter().collect();
                unique.len().to_string()
            }

            Self::Sum => {
                let sum: f64 = values.iter().filter_map(|s| {
                    if let Ok(n) = s.parse::<f64>() {
                        Some(n)
                    } else if s.to_lowercase() == "true" {
                        Some(1.0)
                    } else if s.to_lowercase() == "false" {
                        Some(0.0)
                    } else {
                        None
                    }
                }).sum();
                format_numeric(sum, col_type, precision, currency)
            }

            Self::Avg => {
                let count = values.iter().filter(|s| !s.is_empty()).count() as f64;
                if count == 0.0 {
                    return String::new();
                }
                let sum: f64 = values.iter().filter_map(|s| s.parse::<f64>().ok()).sum();
                format_numeric(sum / count, col_type, precision, currency)
            }

            Self::Min => {
                if matches!(col_type, ColumnType::Integer | ColumnType::Float | ColumnType::Currency) {
                    values
                        .iter()
                        .filter_map(|s| s.parse::<f64>().ok())
                        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .map(|v| format_numeric(v, col_type, precision, currency))
                        .unwrap_or_else(String::new)
                } else {
                    values.iter().min().map(|s| s.to_string()).unwrap_or_else(String::new)
                }
            }

            Self::Max => {
                if matches!(col_type, ColumnType::Integer | ColumnType::Float | ColumnType::Currency) {
                    values
                        .iter()
                        .filter_map(|s| s.parse::<f64>().ok())
                        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .map(|v| format_numeric(v, col_type, precision, currency))
                        .unwrap_or_else(String::new)
                } else {
                    values.iter().max().map(|s| s.to_string()).unwrap_or_else(String::new)
                }
            }

            Self::Median => {
                let mut nums: Vec<f64> =
                    values.iter().filter_map(|s| s.parse::<f64>().ok()).collect();
                if nums.is_empty() {
                    return String::new();
                }
                nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let mid = nums.len() / 2;
                let median = if nums.len() % 2 == 0 {
                    (nums[mid - 1] + nums[mid]) / 2.0
                } else {
                    nums[mid]
                };
                format_numeric(median, col_type, precision, currency)
            }

            Self::Stdev => {
                let nums: Vec<f64> = values.iter().filter_map(|s| s.parse::<f64>().ok()).collect();
                if nums.len() < 2 {
                    return String::new();
                }
                let count = nums.len() as f64;
                let mean = nums.iter().sum::<f64>() / count;
                let variance = nums.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (count - 1.0);
                format_numeric(variance.sqrt(), col_type, precision, currency)
            }

            Self::P5 => percentile(values, 5.0, col_type, precision, currency),
            Self::P25 => percentile(values, 25.0, col_type, precision, currency),
            Self::P50 => percentile(values, 50.0, col_type, precision, currency),
            Self::P75 => percentile(values, 75.0, col_type, precision, currency),
            Self::P95 => percentile(values, 95.0, col_type, precision, currency),
            
            Self::Random => {
                use std::time::{SystemTime, UNIX_EPOCH};
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos() as usize;
                values[nanos % values.len()].clone()
            }
            Self::List => {
                let max_chars = 50;
                let mut result = String::new();
                for (i, val) in values.iter().enumerate() {
                    if i > 0 { result.push_str(", "); }
                    if result.len() + val.len() > max_chars {
                        result.push_str("...");
                        break;
                    }
                    result.push_str(val);
                }
                result
            }
            Self::Set => {
                let mut unique: Vec<String> = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for val in values {
                    if seen.insert(val) {
                        unique.push(val.clone());
                    }
                }
                let max_chars = 50;
                let mut result = String::new();
                for (i, val) in unique.iter().enumerate() {
                    if i > 0 { result.push_str(", "); }
                    if result.len() + val.len() > max_chars {
                        result.push_str("...");
                        break;
                    }
                    result.push_str(val);
                }
                result
            }
        }
    }
}

/// Compute a percentile using linear interpolation.
fn percentile(
    values: &[String],
    pct: f64,
    col_type: ColumnType,
    precision: u8,
    currency: Option<crate::types::CurrencyKind>,
) -> String {
    let mut nums: Vec<f64> = values.iter().filter_map(|s| s.parse::<f64>().ok()).collect();
    if nums.is_empty() {
        return String::new();
    }
    nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let k = (nums.len() as f64 - 1.0) * pct / 100.0;
    let f = k.floor() as usize;
    let c = k.ceil() as usize;
    let val = if f == c {
        nums[f]
    } else {
        let d0 = nums[f] * (c as f64 - k);
        let d1 = nums[c] * (k - f as f64);
        d0 + d1
    };
    format_numeric(val, col_type, precision, currency)
}

/// Format a numeric value, omitting decimal places for integers.
pub fn format_numeric(
    v: f64,
    col_type: ColumnType,
    precision: u8,
    currency: Option<crate::types::CurrencyKind>,
) -> String {
    let p = precision as usize;
    if matches!(col_type, ColumnType::Integer | ColumnType::Boolean) && v.fract() == 0.0 {
        format!("{}", v as i64)
    } else if col_type == ColumnType::Percentage {
        format!("{:.*}%", p, v * 100.0)
    } else if col_type == ColumnType::Currency {
        let sym = currency.map(|k| k.symbol()).unwrap_or("$");
        let prefix = currency.map(|k| k.is_prefix()).unwrap_or(true);
        if v < 0.0 {
            let abs_v = v.abs();
            if prefix {
                format!("({}{:.*})", sym, p, abs_v)
            } else {
                format!("({:.*}{})", p, abs_v, sym)
            }
        } else {
            if prefix {
                format!("{}{:.*}", sym, p, v)
            } else {
                format!("{:.*}{}", p, v, sym)
            }
        }
    } else {
        format!("{:.*}", p, v)
    }
}

