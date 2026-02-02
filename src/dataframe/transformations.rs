//! DataFrame transformation operations: filter, select, with_column, order_by,
//! union, distinct, drop, dropna, fillna, limit, with_column_renamed.

use super::DataFrame;
use polars::prelude::{col, Expr, IntoLazy, PolarsError, UnionArgs, UniqueKeepStrategy};

/// Select columns (returns a new DataFrame). Preserves case_sensitive on result.
pub fn select(
    df: &DataFrame,
    cols: Vec<&str>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let selected = df.df.select(cols)?;
    Ok(super::DataFrame::from_polars_with_options(
        selected,
        case_sensitive,
    ))
}

/// Filter rows using a Polars expression. Preserves case_sensitive on result.
pub fn filter(
    df: &DataFrame,
    condition: Expr,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let lf = df.df.as_ref().clone().lazy().filter(condition);
    let out_df = lf.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        out_df,
        case_sensitive,
    ))
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
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
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
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Union (unionAll): stack another DataFrame vertically. Schemas must match (same columns, same order).
pub fn union(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let lf1 = left.df.as_ref().clone().lazy();
    let lf2 = right.df.as_ref().clone().lazy();
    let out = polars::prelude::concat([lf1, lf2], UnionArgs::default())?.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        out,
        case_sensitive,
    ))
}

/// Union by name: stack vertically, aligning columns by name. Right columns are reordered to match left; missing columns in right become nulls.
pub fn union_by_name(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let left_names = left.df.get_column_names();
    let right_df = right.df.as_ref();
    let right_names = right_df.get_column_names();
    let resolve_right = |name: &str| -> Option<String> {
        if case_sensitive {
            right_names
                .iter()
                .find(|n| n.as_str() == name)
                .map(|s| s.as_str().to_string())
        } else {
            let name_lower = name.to_lowercase();
            right_names
                .iter()
                .find(|n| n.as_str().to_lowercase() == name_lower)
                .map(|s| s.as_str().to_string())
        }
    };
    let mut exprs: Vec<Expr> = Vec::with_capacity(left_names.len());
    for left_col in left_names.iter() {
        let left_str = left_col.as_str();
        if let Some(r) = resolve_right(left_str) {
            exprs.push(col(r.as_str()));
        } else {
            exprs.push(Expr::Literal(polars::prelude::LiteralValue::Null).alias(left_str));
        }
    }
    let right_aligned = right_df.clone().lazy().select(exprs).collect()?;
    let lf1 = left.df.as_ref().clone().lazy();
    let lf2 = right_aligned.lazy();
    let out = polars::prelude::concat([lf1, lf2], UnionArgs::default())?.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        out,
        case_sensitive,
    ))
}

/// Distinct: drop duplicate rows (all columns or subset).
pub fn distinct(
    df: &DataFrame,
    subset: Option<Vec<&str>>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let lf = df.df.as_ref().clone().lazy();
    let subset_names: Option<Vec<String>> =
        subset.map(|cols| cols.iter().map(|s| (*s).to_string()).collect());
    let lf = lf.unique(subset_names, UniqueKeepStrategy::First);
    let pl_df = lf.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Drop one or more columns.
pub fn drop(
    df: &DataFrame,
    columns: Vec<&str>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let resolved: Vec<String> = columns
        .iter()
        .map(|c| df.resolve_column_name(c))
        .collect::<Result<Vec<_>, _>>()?;
    let all_names = df.df.get_column_names();
    let to_keep: Vec<&str> = all_names
        .iter()
        .filter(|n| !resolved.iter().any(|r| r == n.as_str()))
        .map(|n| n.as_str())
        .collect();
    let pl_df = df.df.select(to_keep)?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Drop rows with nulls (all columns or subset).
pub fn dropna(
    df: &DataFrame,
    subset: Option<Vec<&str>>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let lf = df.df.as_ref().clone().lazy();
    let subset_exprs: Option<Vec<Expr>> = match subset {
        Some(cols) => Some(cols.iter().map(|c| col(*c)).collect()),
        None => Some(
            df.df
                .get_column_names()
                .iter()
                .map(|n| col(n.as_str()))
                .collect(),
        ),
    };
    let lf = lf.drop_nulls(subset_exprs);
    let pl_df = lf.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Fill nulls with a literal expression (applied to all columns). For column-specific fill, use a map in a future extension.
pub fn fillna(
    df: &DataFrame,
    value_expr: Expr,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let exprs: Vec<Expr> = df
        .df
        .get_column_names()
        .iter()
        .map(|n| col(n.as_str()).fill_null(value_expr.clone()))
        .collect();
    let pl_df = df
        .df
        .as_ref()
        .clone()
        .lazy()
        .with_columns(exprs)
        .collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Limit: return first n rows.
pub fn limit(df: &DataFrame, n: usize, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    let pl_df = df.df.as_ref().clone().head(Some(n));
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Rename a column (old_name -> new_name).
pub fn with_column_renamed(
    df: &DataFrame,
    old_name: &str,
    new_name: &str,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let resolved = df.resolve_column_name(old_name)?;
    let mut pl_df = df.df.as_ref().clone();
    pl_df.rename(resolved.as_str(), new_name.into())?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}
