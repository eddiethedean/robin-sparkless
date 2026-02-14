//! GroupBy and aggregation operations.

use super::DataFrame;
use polars::prelude::{
    col, len, lit, when, DataFrame as PlDataFrame, DataType, Expr, LazyGroupBy, NamedFrom,
    PolarsError, Series,
};

/// GroupedData - represents a DataFrame grouped by certain columns.
/// Similar to PySpark's GroupedData
pub struct GroupedData {
    // Underlying Polars DataFrame (before grouping). Used by some Python-only paths
    // (e.g. grouped vectorized UDF execution). When the `pyo3` feature is not
    // enabled this field is effectively unused, so we allow dead_code there.
    #[cfg_attr(not(feature = "pyo3"), allow(dead_code))]
    pub(crate) df: PlDataFrame,
    pub(crate) lazy_grouped: LazyGroupBy,
    pub(crate) grouping_cols: Vec<String>,
    pub(crate) case_sensitive: bool,
}

impl GroupedData {
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
        let agg_expr = vec![col(column).sum().alias(format!("sum({column})"))];
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
            .map(|c| col(*c).mean().alias(format!("avg({c})")))
            .collect();
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
        let agg_expr = vec![col(column).min().alias(format!("min({column})"))];
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
        let agg_expr = vec![col(column).max().alias(format!("max({column})"))];
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
        let agg_expr = vec![col(column).first().alias(format!("first({column})"))];
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
        let agg_expr = vec![col(column).last().alias(format!("last({column})"))];
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
        let agg_expr = vec![col(column)
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
        let agg_expr = vec![col(column).first().alias(format!("any_value({column})"))];
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
        let agg_expr = vec![col(column).all(true).alias(format!("bool_and({column})"))];
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
        let agg_expr = vec![col(column).any(true).alias(format!("bool_or({column})"))];
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
        let agg_expr = vec![col(column).product().alias(format!("product({column})"))];
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
        let agg_expr = vec![col(column)
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
        let agg_expr = vec![col(column)
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
        let agg_expr = vec![col(column)
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
        let agg_expr = vec![col(column)
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
        let st = as_struct(vec![
            col(ord_col).alias("_ord"),
            col(value_col).alias("_val"),
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
        let st = as_struct(vec![
            col(ord_col).alias("_ord"),
            col(value_col).alias("_val"),
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
        let c1 = col(col1).cast(DataType::Float64);
        let c2 = col(col2).cast(DataType::Float64);
        let n = len().cast(DataType::Float64);
        let sum_ab = (c1.clone() * c2.clone()).sum();
        let sum_a = col(col1).sum().cast(DataType::Float64);
        let sum_b = col(col2).sum().cast(DataType::Float64);
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
        let c1 = col(col1).cast(DataType::Float64);
        let c2 = col(col2).cast(DataType::Float64);
        let n = len().cast(DataType::Float64);
        let sum_ab = (c1.clone() * c2.clone()).sum();
        let sum_a = col(col1).sum().cast(DataType::Float64);
        let sum_b = col(col2).sum().cast(DataType::Float64);
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
        let c1 = col(col1).cast(DataType::Float64);
        let c2 = col(col2).cast(DataType::Float64);
        let n = len().cast(DataType::Float64);
        let n1 = (len() - lit(1)).cast(DataType::Float64);
        let sum_ab = (c1.clone() * c2.clone()).sum();
        let sum_a = col(col1).sum().cast(DataType::Float64);
        let sum_b = col(col2).sum().cast(DataType::Float64);
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
        let agg_expr = vec![crate::functions::regr_count_expr(y_col, x_col)
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
        let agg_expr = vec![crate::functions::regr_avgx_expr(y_col, x_col)
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
        let agg_expr = vec![crate::functions::regr_avgy_expr(y_col, x_col)
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
        let agg_expr = vec![crate::functions::regr_slope_expr(y_col, x_col)
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
        let agg_expr = vec![crate::functions::regr_intercept_expr(y_col, x_col)
            .alias(format!("regr_intercept({y_col}, {x_col})"))];
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
        let agg_expr = vec![crate::functions::regr_r2_expr(y_col, x_col)
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
        let agg_expr = vec![crate::functions::regr_sxx_expr(y_col, x_col)
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
        let agg_expr = vec![crate::functions::regr_syy_expr(y_col, x_col)
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
        let agg_expr = vec![crate::functions::regr_sxy_expr(y_col, x_col)
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
        let agg_expr = vec![col(column)
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
        let agg_expr = vec![col(column)
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

    /// Apply multiple aggregations at once (generic agg method)
    pub fn agg(&self, aggregations: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        let lf = self.lazy_grouped.clone().agg(aggregations);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(super::DataFrame::from_polars_with_options(
            pl_df,
            self.case_sensitive,
        ))
    }

    /// Get grouping columns
    pub fn grouping_columns(&self) -> &[String] {
        &self.grouping_cols
    }
}

/// Cube/rollup: multiple grouping sets then union (PySpark cube / rollup).
pub struct CubeRollupData {
    pub(super) df: PlDataFrame,
    pub(super) grouping_cols: Vec<String>,
    pub(super) case_sensitive: bool,
    pub(super) is_cube: bool,
}

impl CubeRollupData {
    /// Run aggregation on each grouping set and union results. Missing keys become null.
    pub fn agg(&self, aggregations: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
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

        let schema = self.df.schema();
        let mut parts: Vec<PlDataFrame> = Vec::with_capacity(subsets.len());
        for subset in subsets {
            if subset.is_empty() {
                // Single row: no grouping keys, one row of aggregates over full table
                let lf = self.df.clone().lazy().select(aggregations.clone());
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
                    .df
                    .clone()
                    .lazy()
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
    use crate::{DataFrame, SparkSession};

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
        use polars::prelude::*;
        let df = test_df();
        let grouped = df.group_by(vec!["k"]).unwrap();
        let out = grouped
            .agg(vec![len().alias("cnt"), col("v").sum().alias("total")])
            .unwrap();
        assert_eq!(out.count().unwrap(), 2);
        let cols = out.columns().unwrap();
        assert!(cols.contains(&"k".to_string()));
        assert!(cols.contains(&"cnt".to_string()));
        assert!(cols.contains(&"total".to_string()));
    }
}
