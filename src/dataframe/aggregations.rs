//! GroupBy and aggregation operations.

use super::DataFrame;
use crate::column::Column;
use polars::prelude::{
    col, len, lit, when, DataFrame as PlDataFrame, DataType, Expr, LazyFrame, LazyGroupBy,
    NamedFrom, PolarsError, SchemaNamesAndDtypes, Series,
};
use std::collections::HashMap;

/// Disambiguate duplicate output names in aggregation expressions (PySpark parity: issue #368).
/// When multiple aggs produce the same name (e.g. sum("value"), avg("value") both "value"),
/// suffix with _1, _2, ... so Polars does not error.
pub(crate) fn disambiguate_agg_output_names(aggregations: Vec<Expr>) -> Vec<Expr> {
    let mut name_count: HashMap<String, u32> = HashMap::new();
    aggregations
        .into_iter()
        .map(|e| {
            let base_name = polars_plan::utils::expr_output_name(&e)
                .map(|s| s.to_string())
                .unwrap_or_else(|_| "_".to_string());
            let count = name_count.entry(base_name.clone()).or_insert(0);
            *count += 1;
            let final_name = if *count == 1 {
                base_name
            } else {
                format!("{}_{}", base_name, *count - 1)
            };
            if *count == 1 {
                e
            } else {
                e.alias(final_name.as_str())
            }
        })
        .collect()
}

/// GroupedData - represents a DataFrame grouped by certain columns.
/// Similar to PySpark's GroupedData. Holds LazyGroupBy for lazy agg.
pub struct GroupedData {
    pub(crate) lf: LazyFrame,
    pub(crate) lazy_grouped: LazyGroupBy,
    pub(crate) grouping_cols: Vec<String>,
    pub(crate) case_sensitive: bool,
}

impl GroupedData {
    /// Resolve aggregation column name against LazyFrame schema (case-sensitive or -insensitive).
    fn resolve_column(&self, name: &str) -> Result<String, PolarsError> {
        let schema = self.lf.clone().collect_schema()?;
        let names: Vec<String> = schema
            .iter_names_and_dtypes()
            .map(|(n, _)| n.to_string())
            .collect();
        if self.case_sensitive {
            if names.iter().any(|n| n == name) {
                return Ok(name.to_string());
            }
        } else {
            let name_lower = name.to_lowercase();
            for n in &names {
                if n.to_lowercase() == name_lower {
                    return Ok(n.clone());
                }
            }
        }
        let available = names.join(", ");
        Err(PolarsError::ColumnNotFound(
            format!(
                "Column '{}' not found in grouped DataFrame. Available: [{}].",
                name, available
            )
            .into(),
        ))
    }

    /// Count rows in each group
    pub fn count(&self) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let agg_expr = vec![len().alias("count")];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Sum a column in each group
    pub fn sum(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str()).sum().alias(format!("sum({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        let all_cols: Vec<String> = pl_df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let grouping_cols: Vec<&str> = self.grouping_cols.iter().map(|s| s.as_str()).collect();
        let mut reordered_cols: Vec<&str> = Vec::new();
        for gc in &grouping_cols {
            if all_cols.iter().any(|c| c == gc) {
                reordered_cols.push(gc);
            }
        }
        for col_name in &all_cols {
            if !grouping_cols.iter().any(|gc| *gc == col_name) {
                reordered_cols.push(col_name);
            }
        }
        if !reordered_cols.is_empty() {
            pl_df = pl_df.select(reordered_cols)?;
        }
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Average (mean) of one or more columns in each group (PySpark: df.groupBy("x").avg("a", "b")).
    pub fn avg(&self, columns: &[&str]) -> Result<DataFrame, PolarsError> {
        if columns.is_empty() {
            return Err(PolarsError::ComputeError(
                "avg requires at least one column".into(),
            ));
        }
        use polars::prelude::*;
        let agg_expr: Vec<Expr> = columns
            .iter()
            .map(|c| {
                let resolved = self.resolve_column(c)?;
                Ok(col(resolved.as_str()).mean().alias(format!("avg({c})")))
            })
            .collect::<Result<Vec<_>, PolarsError>>()?;
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Minimum value of a column in each group
    pub fn min(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str()).min().alias(format!("min({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Maximum value of a column in each group
    pub fn max(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str()).max().alias(format!("max({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// First value of a column in each group (order not guaranteed unless explicitly sorted).
    pub fn first(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str()).first().alias(format!("first({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Last value of a column in each group (order not guaranteed unless explicitly sorted).
    pub fn last(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str()).last().alias(format!("last({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Approximate count of distinct values in each group (uses n_unique; same as count_distinct for exact).
    pub fn approx_count_distinct(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::{col, DataType};
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .n_unique()
            .cast(DataType::Int64)
            .alias(format!("approx_count_distinct({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Any value from the group (PySpark any_value). Uses first value.
    pub fn any_value(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .first()
            .alias(format!("any_value({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Boolean AND across group (PySpark bool_and / every).
    pub fn bool_and(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .all(true)
            .alias(format!("bool_and({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Boolean OR across group (PySpark bool_or / some).
    pub fn bool_or(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .any(true)
            .alias(format!("bool_or({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Product of column values in each group (PySpark product).
    pub fn product(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .product()
            .alias(format!("product({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Collect column values into list per group (PySpark collect_list).
    pub fn collect_list(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .implode()
            .alias(format!("collect_list({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Collect distinct column values into list per group (PySpark collect_set).
    pub fn collect_set(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .unique()
            .implode()
            .alias(format!("collect_set({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Count rows where condition column is true (PySpark count_if).
    pub fn count_if(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .cast(DataType::Int64)
            .sum()
            .alias(format!("count_if({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Percentile of column (PySpark percentile). p in 0.0..=1.0.
    pub fn percentile(&self, column: &str, p: f64) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .quantile(lit(p), QuantileMethod::Linear)
            .alias(format!("percentile({column}, {p})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Value of value_col where ord_col is maximum (PySpark max_by).
    pub fn max_by(&self, value_col: &str, ord_col: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let vc = self.resolve_column(value_col)?;
        let oc = self.resolve_column(ord_col)?;
        let st = as_struct(vec![
            col(oc.as_str()).alias("_ord"),
            col(vc.as_str()).alias("_val"),
        ]);
        let agg_expr = vec![st
            .sort(SortOptions::default().with_order_descending(true))
            .first()
            .struct_()
            .field_by_name("_val")
            .alias(format!("max_by({value_col}, {ord_col})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Value of value_col where ord_col is minimum (PySpark min_by).
    pub fn min_by(&self, value_col: &str, ord_col: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let vc = self.resolve_column(value_col)?;
        let oc = self.resolve_column(ord_col)?;
        let st = as_struct(vec![
            col(oc.as_str()).alias("_ord"),
            col(vc.as_str()).alias("_val"),
        ]);
        let agg_expr = vec![st
            .sort(SortOptions::default())
            .first()
            .struct_()
            .field_by_name("_val")
            .alias(format!("min_by({value_col}, {ord_col})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Population covariance between two columns in each group (PySpark covar_pop).
    pub fn covar_pop(&self, col1: &str, col2: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::DataType;
        let c1_res = self.resolve_column(col1)?;
        let c2_res = self.resolve_column(col2)?;
        let c1 = col(c1_res.as_str()).cast(DataType::Float64);
        let c2 = col(c2_res.as_str()).cast(DataType::Float64);
        let n = len().cast(DataType::Float64);
        let sum_ab = (c1.clone() * c2.clone()).sum();
        let sum_a = col(c1_res.as_str()).sum().cast(DataType::Float64);
        let sum_b = col(c2_res.as_str()).sum().cast(DataType::Float64);
        let cov = (sum_ab - sum_a * sum_b / n.clone()) / n;
        let agg_expr = vec![cov.alias(format!("covar_pop({col1}, {col2})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Sample covariance between two columns in each group (PySpark covar_samp). ddof=1.
    pub fn covar_samp(&self, col1: &str, col2: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::DataType;
        let c1_res = self.resolve_column(col1)?;
        let c2_res = self.resolve_column(col2)?;
        let c1 = col(c1_res.as_str()).cast(DataType::Float64);
        let c2 = col(c2_res.as_str()).cast(DataType::Float64);
        let n = len().cast(DataType::Float64);
        let sum_ab = (c1.clone() * c2.clone()).sum();
        let sum_a = col(c1_res.as_str()).sum().cast(DataType::Float64);
        let sum_b = col(c2_res.as_str()).sum().cast(DataType::Float64);
        let cov = when(len().gt(lit(1)))
            .then((sum_ab - sum_a * sum_b / n.clone()) / (len() - lit(1)).cast(DataType::Float64))
            .otherwise(lit(f64::NAN));
        let agg_expr = vec![cov.alias(format!("covar_samp({col1}, {col2})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Pearson correlation between two columns in each group (PySpark corr).
    pub fn corr(&self, col1: &str, col2: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::DataType;
        let c1_res = self.resolve_column(col1)?;
        let c2_res = self.resolve_column(col2)?;
        let c1 = col(c1_res.as_str()).cast(DataType::Float64);
        let c2 = col(c2_res.as_str()).cast(DataType::Float64);
        let n = len().cast(DataType::Float64);
        let n1 = (len() - lit(1)).cast(DataType::Float64);
        let sum_ab = (c1.clone() * c2.clone()).sum();
        let sum_a = col(c1_res.as_str()).sum().cast(DataType::Float64);
        let sum_b = col(c2_res.as_str()).sum().cast(DataType::Float64);
        let sum_a2 = (c1.clone() * c1).sum();
        let sum_b2 = (c2.clone() * c2).sum();
        let cov_samp = (sum_ab - sum_a.clone() * sum_b.clone() / n.clone()) / n1.clone();
        let var_a = (sum_a2 - sum_a.clone() * sum_a / n.clone()) / n1.clone();
        let var_b = (sum_b2 - sum_b.clone() * sum_b / n.clone()) / n1.clone();
        let std_a = var_a.sqrt();
        let std_b = var_b.sqrt();
        let corr_expr = when(len().gt(lit(1)))
            .then(cov_samp / (std_a * std_b))
            .otherwise(lit(f64::NAN));
        let agg_expr = vec![corr_expr.alias(format!("corr({col1}, {col2})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Regression count of (y, x) pairs where both non-null (PySpark regr_count).
    pub fn regr_count(&self, y_col: &str, x_col: &str) -> Result<DataFrame, PolarsError> {
        let yc = self.resolve_column(y_col)?;
        let xc = self.resolve_column(x_col)?;
        let agg_expr = vec![crate::functions::regr_count_expr(yc.as_str(), xc.as_str())
            .alias(format!("regr_count({y_col}, {x_col})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Regression average of x (PySpark regr_avgx).
    pub fn regr_avgx(&self, y_col: &str, x_col: &str) -> Result<DataFrame, PolarsError> {
        let yc = self.resolve_column(y_col)?;
        let xc = self.resolve_column(x_col)?;
        let agg_expr = vec![crate::functions::regr_avgx_expr(yc.as_str(), xc.as_str())
            .alias(format!("regr_avgx({y_col}, {x_col})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Regression average of y (PySpark regr_avgy).
    pub fn regr_avgy(&self, y_col: &str, x_col: &str) -> Result<DataFrame, PolarsError> {
        let yc = self.resolve_column(y_col)?;
        let xc = self.resolve_column(x_col)?;
        let agg_expr = vec![crate::functions::regr_avgy_expr(yc.as_str(), xc.as_str())
            .alias(format!("regr_avgy({y_col}, {x_col})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Regression slope (PySpark regr_slope).
    pub fn regr_slope(&self, y_col: &str, x_col: &str) -> Result<DataFrame, PolarsError> {
        let yc = self.resolve_column(y_col)?;
        let xc = self.resolve_column(x_col)?;
        let agg_expr = vec![crate::functions::regr_slope_expr(yc.as_str(), xc.as_str())
            .alias(format!("regr_slope({y_col}, {x_col})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Regression intercept (PySpark regr_intercept).
    pub fn regr_intercept(&self, y_col: &str, x_col: &str) -> Result<DataFrame, PolarsError> {
        let yc = self.resolve_column(y_col)?;
        let xc = self.resolve_column(x_col)?;
        let agg_expr = vec![
            crate::functions::regr_intercept_expr(yc.as_str(), xc.as_str())
                .alias(format!("regr_intercept({y_col}, {x_col})")),
        ];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Regression R-squared (PySpark regr_r2).
    pub fn regr_r2(&self, y_col: &str, x_col: &str) -> Result<DataFrame, PolarsError> {
        let yc = self.resolve_column(y_col)?;
        let xc = self.resolve_column(x_col)?;
        let agg_expr = vec![crate::functions::regr_r2_expr(yc.as_str(), xc.as_str())
            .alias(format!("regr_r2({y_col}, {x_col})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Regression sum (x - avg_x)^2 (PySpark regr_sxx).
    pub fn regr_sxx(&self, y_col: &str, x_col: &str) -> Result<DataFrame, PolarsError> {
        let yc = self.resolve_column(y_col)?;
        let xc = self.resolve_column(x_col)?;
        let agg_expr = vec![crate::functions::regr_sxx_expr(yc.as_str(), xc.as_str())
            .alias(format!("regr_sxx({y_col}, {x_col})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Regression sum (y - avg_y)^2 (PySpark regr_syy).
    pub fn regr_syy(&self, y_col: &str, x_col: &str) -> Result<DataFrame, PolarsError> {
        let yc = self.resolve_column(y_col)?;
        let xc = self.resolve_column(x_col)?;
        let agg_expr = vec![crate::functions::regr_syy_expr(yc.as_str(), xc.as_str())
            .alias(format!("regr_syy({y_col}, {x_col})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Regression sum (x - avg_x)(y - avg_y) (PySpark regr_sxy).
    pub fn regr_sxy(&self, y_col: &str, x_col: &str) -> Result<DataFrame, PolarsError> {
        let yc = self.resolve_column(y_col)?;
        let xc = self.resolve_column(x_col)?;
        let agg_expr = vec![crate::functions::regr_sxy_expr(yc.as_str(), xc.as_str())
            .alias(format!("regr_sxy({y_col}, {x_col})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Kurtosis of a column in each group (PySpark kurtosis). Fisher definition, bias=true.
    pub fn kurtosis(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .cast(DataType::Float64)
            .kurtosis(true, true)
            .alias(format!("kurtosis({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Skewness of a column in each group (PySpark skewness). bias=true.
    pub fn skewness(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let c = self.resolve_column(column)?;
        let agg_expr = vec![col(c.as_str())
            .cast(DataType::Float64)
            .skew(true)
            .alias(format!("skewness({column})"))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Apply multiple aggregations at once (generic agg method).
    /// Duplicate output names are disambiguated with _1, _2, ... (PySpark parity, issue #368).
    pub fn agg(&self, aggregations: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        let disambiguated = disambiguate_agg_output_names(aggregations);
        let lf = self.lazy_grouped.clone().agg(disambiguated);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Apply multiple aggregations expressed as robin-sparkless Columns.
    /// This is a convenience for downstream bindings that work purely with
    /// `Column` instead of `polars::Expr`, and wraps the generic `agg` API.
    pub fn agg_columns(&self, aggregations: Vec<Column>) -> Result<DataFrame, PolarsError> {
        let exprs: Vec<Expr> = aggregations.into_iter().map(|c| c.into_expr()).collect();
        self.agg(exprs)
    }

    /// Get grouping columns
    pub fn grouping_columns(&self) -> &[String] {
        &self.grouping_cols
    }

    /// Pivot a column for pivot-table aggregation (PySpark: groupBy(...).pivot(pivot_col).sum(value_col)).
    /// Returns PivotedGroupedData; call .sum(column), .avg(column), etc. to run the aggregation.
    pub fn pivot(&self, pivot_col: &str, values: Option<Vec<String>>) -> PivotedGroupedData {
        PivotedGroupedData {
            lf: self.lf.clone(),
            grouping_cols: self.grouping_cols.clone(),
            pivot_col: pivot_col.to_string(),
            values,
            case_sensitive: self.case_sensitive,
        }
    }
}

/// Result of GroupedData.pivot(pivot_col); has .sum(), .avg(), etc. (PySpark pivot table).
pub struct PivotedGroupedData {
    pub(crate) lf: LazyFrame,
    pub(crate) grouping_cols: Vec<String>,
    pub(crate) pivot_col: String,
    pub(crate) values: Option<Vec<String>>,
    pub(crate) case_sensitive: bool,
}

/// PySpark: pivot column names use string representation; null → "null".
fn pivot_value_to_column_name(av: polars::prelude::AnyValue<'_>) -> String {
    use polars::prelude::AnyValue;
    match av {
        AnyValue::Null => "null".to_string(),
        AnyValue::String(s) => s.to_string(),
        _ => av.to_string(),
    }
}

fn pivot_values_from_lf(lf: &LazyFrame, pivot_col: &str) -> Result<Vec<String>, PolarsError> {
    use polars::prelude::*;
    let pl_df = lf
        .clone()
        .select([col(pivot_col)])
        .unique(None, Default::default())
        .collect()?;
    let s = pl_df.column(pivot_col)?;
    let mut out = Vec::with_capacity(s.len());
    for i in 0..s.len() {
        let av = s.get(i)?;
        out.push(pivot_value_to_column_name(av));
    }
    // PySpark parity: deterministic column order when values not provided (lexicographic)
    out.sort();
    Ok(out)
}

impl PivotedGroupedData {
    fn resolve_column(&self, name: &str) -> Result<String, PolarsError> {
        let schema = self.lf.clone().collect_schema()?;
        let names: Vec<String> = schema
            .iter_names_and_dtypes()
            .map(|(n, _)| n.to_string())
            .collect();
        if self.case_sensitive {
            if names.iter().any(|n| n == name) {
                return Ok(name.to_string());
            }
        } else {
            let name_lower = name.to_lowercase();
            for n in &names {
                if n.to_lowercase() == name_lower {
                    return Ok(n.clone());
                }
            }
        }
        let available = names.join(", ");
        Err(PolarsError::ColumnNotFound(
            format!(
                "Column '{}' not found in pivot DataFrame. Available: [{}].",
                name, available
            )
            .into(),
        ))
    }

    fn pivot_values(&self) -> Result<Vec<String>, PolarsError> {
        if let Some(ref v) = self.values {
            return Ok(v.clone());
        }
        let resolved = self.resolve_column(&self.pivot_col)?;
        pivot_values_from_lf(&self.lf, &resolved)
    }

    fn pivot_agg(
        &self,
        value_col: &str,
        agg_fn: fn(Expr) -> Expr,
    ) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let pivot_resolved = self.resolve_column(&self.pivot_col)?;
        let value_resolved = self.resolve_column(value_col)?;
        let pivot_vals = self.pivot_values()?;
        if pivot_vals.is_empty() {
            let by: Vec<&str> = self.grouping_cols.iter().map(|s| s.as_str()).collect();
            let lf = self.lf.clone().group_by(by).agg(vec![]);
            let pl_df = lf.collect()?;
            return Ok(super::DataFrame::from_polars_with_options(
                pl_df,
                self.case_sensitive,
            ));
        }
        let mut agg_exprs: Vec<Expr> = Vec::with_capacity(pivot_vals.len());
        use polars::prelude::{DataType, LiteralValue};
        for v in &pivot_vals {
            // PySpark: pivot_col can be any type; compare as string for column names. Null → is_null().
            let pred = if v == "null" {
                col(pivot_resolved.as_str()).is_null()
            } else {
                col(pivot_resolved.as_str())
                    .cast(DataType::String)
                    .eq(lit(v.as_str()))
            };
            let then_expr = col(value_resolved.as_str());
            let expr = when(pred)
                .then(then_expr)
                .otherwise(Expr::Literal(LiteralValue::Null));
            // PySpark parity: pivot value with no matching rows → null (not 0)
            let has_any = expr
                .clone()
                .is_not_null()
                .cast(DataType::UInt32)
                .sum()
                .gt(lit(0));
            let agg_expr = when(has_any)
                .then(agg_fn(expr))
                .otherwise(Expr::Literal(LiteralValue::Null))
                .alias(v.as_str());
            agg_exprs.push(agg_expr);
        }
        let by: Vec<&str> = self.grouping_cols.iter().map(|s| s.as_str()).collect();
        let lf = self.lf.clone().group_by(by).agg(agg_exprs);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Pivot then sum (PySpark: groupBy(...).pivot(...).sum(column)).
    pub fn sum(&self, value_col: &str) -> Result<DataFrame, PolarsError> {
        self.pivot_agg(value_col, polars::prelude::Expr::sum)
    }

    /// Pivot then mean (PySpark: groupBy(...).pivot(...).avg(column)).
    pub fn avg(&self, value_col: &str) -> Result<DataFrame, PolarsError> {
        self.pivot_agg(value_col, polars::prelude::Expr::mean)
    }

    /// Pivot then min (PySpark: groupBy(...).pivot(...).min(column)).
    pub fn min(&self, value_col: &str) -> Result<DataFrame, PolarsError> {
        self.pivot_agg(value_col, polars::prelude::Expr::min)
    }

    /// Pivot then max (PySpark: groupBy(...).pivot(...).max(column)).
    pub fn max(&self, value_col: &str) -> Result<DataFrame, PolarsError> {
        self.pivot_agg(value_col, polars::prelude::Expr::max)
    }

    /// Pivot then count (PySpark: groupBy(...).pivot(...).count()). Counts rows per group per pivot value.
    pub fn count(&self) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let pivot_vals = self.pivot_values()?;
        if pivot_vals.is_empty() {
            let by: Vec<&str> = self.grouping_cols.iter().map(|s| s.as_str()).collect();
            let lf = self.lf.clone().group_by(by).agg(vec![]);
            let pl_df = lf.collect()?;
            return Ok(super::DataFrame::from_polars_with_options(
                pl_df,
                self.case_sensitive,
            ));
        }
        let mut agg_exprs: Vec<Expr> = Vec::with_capacity(pivot_vals.len());
        use polars::prelude::{DataType, LiteralValue};
        let pivot_resolved = self.resolve_column(&self.pivot_col)?;
        for v in &pivot_vals {
            let pred = if v == "null" {
                col(pivot_resolved.as_str()).is_null()
            } else {
                col(pivot_resolved.as_str())
                    .cast(DataType::String)
                    .eq(lit(v.as_str()))
            };
            let expr = when(pred)
                .then(lit(1))
                .otherwise(Expr::Literal(LiteralValue::Null));
            let has_any = expr
                .clone()
                .is_not_null()
                .cast(DataType::UInt32)
                .sum()
                .gt(lit(0));
            let agg_expr = when(has_any)
                .then(expr.sum())
                .otherwise(Expr::Literal(LiteralValue::Null))
                .alias(v.as_str());
            agg_exprs.push(agg_expr);
        }
        let by: Vec<&str> = self.grouping_cols.iter().map(|s| s.as_str()).collect();
        let lf = self.lf.clone().group_by(by).agg(agg_exprs);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }
}

/// Cube/rollup: multiple grouping sets then union (PySpark cube / rollup).
pub struct CubeRollupData {
    pub(super) lf: LazyFrame,
    pub(super) grouping_cols: Vec<String>,
    pub(super) case_sensitive: bool,
    pub(super) is_cube: bool,
}

impl CubeRollupData {
    /// Count rows per grouping set (PySpark cube/rollup .count()).
    pub fn count(&self) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        self.agg(vec![len().alias("count")])
    }

    /// Run aggregation on each grouping set and union results. Missing keys become null.
    /// Duplicate agg output names are disambiguated (issue #368).
    pub fn agg(&self, aggregations: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let aggregations = disambiguate_agg_output_names(aggregations);
        let subsets: Vec<Vec<String>> = if self.is_cube {
            // All subsets of grouping_cols (2^n)
            let n = self.grouping_cols.len();
            (0..1 << n)
                .map(|mask| {
                    self.grouping_cols
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| (mask & (1 << i)) != 0)
                        .map(|(_, c)| c.clone())
                        .collect()
                })
                .collect()
        } else {
            // Prefixes: [all], [all-1], ..., []
            (0..=self.grouping_cols.len())
                .map(|len| self.grouping_cols[..len].to_vec())
                .collect()
        };

        let schema = self.lf.clone().collect_schema()?;
        let mut parts: Vec<PlDataFrame> = Vec::with_capacity(subsets.len());
        for subset in subsets {
            if subset.is_empty() {
                // Single row: no grouping keys, one row of aggregates over full table
                let lf = self.lf.clone().select(&aggregations);
                let mut part = lf.collect()?;
                let n = part.height();
                for gc in &self.grouping_cols {
                    let dtype = schema.get(gc).cloned().unwrap_or(DataType::Null);
                    let null_series = null_series_for_dtype(gc.as_str(), n, &dtype)?;
                    part.with_column(null_series)?;
                }
                // Reorder to [grouping_cols..., agg_cols]
                let mut order: Vec<&str> = self.grouping_cols.iter().map(|s| s.as_str()).collect();
                for name in part.get_column_names() {
                    if !self.grouping_cols.iter().any(|g| g == name) {
                        order.push(name);
                    }
                }
                part = part.select(order)?;
                parts.push(part);
            } else {
                let grouped = self
                    .lf
                    .clone()
                    .group_by(subset.iter().map(|s| col(s.as_str())).collect::<Vec<_>>());
                let mut part = grouped.agg(aggregations.clone()).collect()?;
                part = reorder_groupby_columns(&mut part, &subset)?;
                let n = part.height();
                for gc in &self.grouping_cols {
                    if subset.iter().any(|s| s == gc) {
                        continue;
                    }
                    let dtype = schema.get(gc).cloned().unwrap_or(DataType::Null);
                    let null_series = null_series_for_dtype(gc.as_str(), n, &dtype)?;
                    part.with_column(null_series)?;
                }
                let mut order: Vec<&str> = self.grouping_cols.iter().map(|s| s.as_str()).collect();
                for name in part.get_column_names() {
                    if !self.grouping_cols.iter().any(|g| g == name) {
                        order.push(name);
                    }
                }
                part = part.select(order)?;
                parts.push(part);
            }
        }

        if parts.is_empty() {
            return Ok(super::DataFrame::from_polars_with_options(
                PlDataFrame::empty(),
                self.case_sensitive,
            ));
        }
        let first_schema = parts[0].schema();
        let order: Vec<&str> = first_schema.iter_names().map(|s| s.as_str()).collect();
        for p in parts.iter_mut().skip(1) {
            *p = p.select(order.clone())?;
        }
        let lazy_frames: Vec<_> = parts.into_iter().map(|p| p.lazy()).collect();
        let out = polars::prelude::concat(lazy_frames, UnionArgs::default())?.collect()?;
        Ok(super::DataFrame::from_polars_with_options(
            out,
            self.case_sensitive,
        ))
    }
}

fn null_series_for_dtype(name: &str, n: usize, dtype: &DataType) -> Result<Series, PolarsError> {
    let name = name.into();
    let s = match dtype {
        DataType::Int32 => Series::new(name, vec![None::<i32>; n]),
        DataType::Int64 => Series::new(name, vec![None::<i64>; n]),
        DataType::Float32 => Series::new(name, vec![None::<f32>; n]),
        DataType::Float64 => Series::new(name, vec![None::<f64>; n]),
        DataType::String => {
            let v: Vec<Option<String>> = (0..n).map(|_| None).collect();
            Series::new(name, v)
        }
        DataType::Boolean => Series::new(name, vec![None::<bool>; n]),
        DataType::Date => Series::new(name, vec![None::<i32>; n]).cast(dtype)?,
        DataType::Datetime(_, _) => Series::new(name, vec![None::<i64>; n]).cast(dtype)?,
        _ => Series::new(name, vec![None::<i64>; n]).cast(dtype)?,
    };
    Ok(s)
}

/// Reorder columns after groupBy to match PySpark order: grouping columns first, then aggregations
pub(super) fn reorder_groupby_columns(
    pl_df: &mut PlDataFrame,
    grouping_cols: &[String],
) -> Result<PlDataFrame, PolarsError> {
    let all_cols: Vec<String> = pl_df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    let mut reordered_cols: Vec<&str> = Vec::new();
    for gc in grouping_cols {
        if all_cols.iter().any(|c| c == gc) {
            reordered_cols.push(gc);
        }
    }
    for col_name in &all_cols {
        if !grouping_cols.iter().any(|gc| gc == col_name) {
            reordered_cols.push(col_name);
        }
    }
    if !reordered_cols.is_empty() && reordered_cols.len() == all_cols.len() {
        pl_df.select(reordered_cols)
    } else {
        Ok(pl_df.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::{functions, DataFrame, SparkSession};

    fn test_df() -> DataFrame {
        let spark = SparkSession::builder()
            .app_name("agg_tests")
            .get_or_create();
        let tuples = vec![
            (1i64, 10i64, "a".to_string()),
            (1i64, 20i64, "a".to_string()),
            (2i64, 30i64, "b".to_string()),
        ];
        spark
            .create_dataframe(tuples, vec!["k", "v", "label"])
            .unwrap()
    }

    #[test]
    fn group_by_count_single_group() {
        let df = test_df();
        let grouped = df.group_by(vec!["k"]).unwrap();
        let out = grouped.count().unwrap();
        assert_eq!(out.count().unwrap(), 2);
        let cols = out.columns().unwrap();
        assert!(cols.contains(&"k".to_string()));
        assert!(cols.contains(&"count".to_string()));
    }

    #[test]
    fn group_by_sum() {
        let df = test_df();
        let grouped = df.group_by(vec!["k"]).unwrap();
        let out = grouped.sum("v").unwrap();
        assert_eq!(out.count().unwrap(), 2);
        let cols = out.columns().unwrap();
        assert!(cols.iter().any(|c| c.starts_with("sum(")));
    }

    #[test]
    fn group_by_empty_groups() {
        let spark = SparkSession::builder()
            .app_name("agg_tests")
            .get_or_create();
        let tuples: Vec<(i64, i64, String)> = vec![];
        let df = spark.create_dataframe(tuples, vec!["a", "b", "c"]).unwrap();
        let grouped = df.group_by(vec!["a"]).unwrap();
        let out = grouped.count().unwrap();
        assert_eq!(out.count().unwrap(), 0);
    }

    #[test]
    fn group_by_agg_multi() {
        let df = test_df();
        let grouped = df.group_by(vec!["k"]).unwrap();
        let out = grouped
            .agg(vec![
                polars::prelude::len().alias("cnt"),
                polars::prelude::col("v").sum().alias("total"),
            ])
            .unwrap();
        assert_eq!(out.count().unwrap(), 2);
        let cols = out.columns().unwrap();
        assert!(cols.contains(&"k".to_string()));
        assert!(cols.contains(&"cnt".to_string()));
        assert!(cols.contains(&"total".to_string()));
    }

    #[test]
    fn group_by_agg_columns_multi() {
        let df = test_df();
        let grouped = df.group_by(vec!["k"]).unwrap();
        let v_col = functions::col("v");
        let aggs = vec![functions::count(&v_col), functions::sum(&v_col)];
        let out = grouped.agg_columns(aggs).unwrap();
        assert_eq!(out.count().unwrap(), 2);
        let cols = out.columns().unwrap();
        assert!(cols.contains(&"k".to_string()));
        assert_eq!(cols.len(), 3);
    }
}
