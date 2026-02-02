//! Join operations for DataFrame.

use super::DataFrame;
use polars::prelude::JoinType as PlJoinType;
use polars::prelude::PolarsError;

/// Join type for DataFrame joins (PySpark-compatible)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
}

/// Join with another DataFrame on the given columns. Preserves case_sensitive on result.
pub fn join(
    left: &DataFrame,
    right: &DataFrame,
    on: Vec<&str>,
    how: JoinType,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::{col, JoinBuilder, JoinCoalesce, IntoLazy};
    let left_lf = left.df.as_ref().clone().lazy();
    let right_lf = right.df.as_ref().clone().lazy();
    let on_exprs: Vec<polars::prelude::Expr> = on.iter().map(|name| col(*name)).collect();
    let polars_how: PlJoinType = match how {
        JoinType::Inner => PlJoinType::Inner,
        JoinType::Left => PlJoinType::Left,
        JoinType::Right => PlJoinType::Right,
        JoinType::Outer => PlJoinType::Full, // PySpark Outer = Polars Full
    };
    let joined = JoinBuilder::new(left_lf)
        .with(right_lf)
        .how(polars_how)
        .on(&on_exprs)
        .coalesce(JoinCoalesce::KeepColumns)
        .finish();
    let pl_df = joined.collect()?;
    Ok(super::DataFrame::from_polars_with_options(pl_df, case_sensitive))
}
