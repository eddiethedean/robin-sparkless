//! GroupBy and aggregation operations.

use super::DataFrame;
use polars::prelude::{DataFrame as PlDataFrame, Expr, LazyGroupBy, PolarsError};

/// GroupedData - represents a DataFrame grouped by certain columns.
/// Similar to PySpark's GroupedData
pub struct GroupedData {
    pub(super) lazy_grouped: LazyGroupBy,
    pub(super) grouping_cols: Vec<String>,
    pub(super) case_sensitive: bool,
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
        let agg_expr = vec![col(column).sum().alias(format!("sum({})", column))];
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

    /// Average (mean) of a column in each group
    pub fn avg(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let agg_expr = vec![col(column).mean().alias(format!("avg({})", column))];
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
        let agg_expr = vec![col(column).min().alias(format!("min({})", column))];
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
        let agg_expr = vec![col(column).max().alias(format!("max({})", column))];
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
        let agg_expr = vec![col(column).first().alias(format!("first({})", column))];
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
        let agg_expr = vec![col(column).last().alias(format!("last({})", column))];
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
            .alias(format!("approx_count_distinct({})", column))];
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
        let agg_expr = vec![col(column).first().alias(format!("any_value({})", column))];
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
        let agg_expr = vec![col(column).all(true).alias(format!("bool_and({})", column))];
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
        let agg_expr = vec![col(column).any(true).alias(format!("bool_or({})", column))];
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
        let agg_expr = vec![col(column).product().alias(format!("product({})", column))];
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
            .alias(format!("collect_list({})", column))];
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
            .alias(format!("collect_set({})", column))];
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
            .alias(format!("count_if({})", column))];
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
            .alias(format!("percentile({}, {})", column, p))];
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
            .alias(format!("max_by({}, {})", value_col, ord_col))];
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
            .alias(format!("min_by({}, {})", value_col, ord_col))];
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
