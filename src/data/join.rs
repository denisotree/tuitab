use crate::data::dataframe::DataFrame;
use crate::data::io::wrap_polars_df;
use color_eyre::Result;
use polars::prelude::{DataFrameJoinOps, JoinArgs, JoinType as PolarsJoinType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
}

impl JoinType {
    pub fn all() -> &'static [JoinType] {
        &[Self::Inner, Self::Left, Self::Right, Self::Outer]
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Inner => "INNER  (only matching rows)",
            Self::Left => "LEFT   (all left rows)",
            Self::Right => "RIGHT  (all right rows)",
            Self::Outer => "OUTER  (all rows from both)",
        }
    }
}

pub fn join_dataframes(
    left: &DataFrame,
    right: &DataFrame,
    left_keys: &[String],
    right_keys: &[String],
    join_type: JoinType,
) -> Result<DataFrame> {
    let left_key_strs: Vec<&str> = left_keys.iter().map(|s| s.as_str()).collect();
    let right_key_strs: Vec<&str> = right_keys.iter().map(|s| s.as_str()).collect();

    let args = JoinArgs::new(match join_type {
        JoinType::Inner => PolarsJoinType::Inner,
        JoinType::Left => PolarsJoinType::Left,
        JoinType::Right => PolarsJoinType::Right,
        JoinType::Outer => PolarsJoinType::Full,
    });

    let result = left
        .df
        .join(&right.df, &left_key_strs, &right_key_strs, args, None)?;

    wrap_polars_df(result)
}
