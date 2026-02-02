//! DataFrame transformation operations: filter, select, with_column, order_by.

use super::DataFrame;
use polars::prelude::{col, Expr, IntoLazy, PolarsError, SortMultipleOptions};

/// Select columns (returns a new DataFrame). Preserves case_sensitive on result.
pub fn select(
    df: &DataFrame,
    cols: Vec<&str>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let selected = df.df.select(cols)?;
    Ok(super::DataFrame::from_polars_with_options(selected, case_sensitive))
}

/// Filter rows using a Polars expression. Preserves case_sensitive on result.
pub fn filter(
    df: &DataFrame,
    condition: Expr,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let lf = df.df.as_ref().clone().lazy().filter(condition);
    let out_df = lf.collect()?;
    Ok(super::DataFrame::from_polars_with_options(out_df, case_sensitive))
}

/// Add or replace a column using an expression. Preserves case_sensitive on result.
pub fn with_column(
    df: &DataFrame,
    column_name: &str,
    expr: Expr,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let lf = df.df.as_ref().clone().lazy();
    let lf_with_col = lf.with_column(expr.alias(column_name));
    let pl_df = lf_with_col.collect()?;
    Ok(super::DataFrame::from_polars_with_options(pl_df, case_sensitive))
}

/// Order by columns (sort). Preserves case_sensitive on result.
pub fn order_by(
    df: &DataFrame,
    column_names: Vec<&str>,
    ascending: Vec<bool>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let mut asc = ascending;
    while asc.len() < column_names.len() {
        asc.push(true);
    }
    asc.truncate(column_names.len());
    let lf = df.df.as_ref().clone().lazy();
    let exprs: Vec<Expr> = column_names.iter().map(|name| col(*name)).collect();
    let descending: Vec<bool> = asc.iter().map(|&a| !a).collect();
    let sorted = lf.sort_by_exprs(
        exprs,
        SortMultipleOptions::new().with_order_descending_multi(descending),
    );
    let pl_df = sorted.collect()?;
    Ok(super::DataFrame::from_polars_with_options(pl_df, case_sensitive))
}
