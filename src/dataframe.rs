use crate::column::Column;
use crate::schema::StructType;
use polars::prelude::{DataFrame as PlDataFrame, Expr, PolarsError};
use std::sync::Arc;

/// Join type for DataFrame joins (PySpark-compatible)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
}

/// DataFrame - main tabular data structure.
/// Thin wrapper around an eager Polars `DataFrame`.
pub struct DataFrame {
    df: Arc<PlDataFrame>,
}

impl DataFrame {
    /// Create a new DataFrame from a Polars DataFrame
    pub fn from_polars(df: PlDataFrame) -> Self {
        DataFrame { df: Arc::new(df) }
    }

    /// Create an empty DataFrame
    pub fn empty() -> Self {
        DataFrame {
            df: Arc::new(PlDataFrame::empty()),
        }
    }

    /// Get the schema of the DataFrame
    pub fn schema(&self) -> Result<StructType, PolarsError> {
        Ok(StructType::from_polars_schema(&self.df.schema()))
    }

    /// Get column names
    pub fn columns(&self) -> Result<Vec<String>, PolarsError> {
        Ok(self
            .df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect())
    }

    /// Count the number of rows (action - triggers execution)
    pub fn count(&self) -> Result<usize, PolarsError> {
        Ok(self.df.height())
    }

    /// Show the first n rows
    pub fn show(&self, n: Option<usize>) -> Result<(), PolarsError> {
        let n = n.unwrap_or(20);
        // Use Polars' built-in display
        println!("{}", self.df.head(Some(n)));
        Ok(())
    }

    /// Collect the DataFrame (action - triggers execution)
    /// Returns the materialized Polars DataFrame
    pub fn collect(&self) -> Result<Arc<PlDataFrame>, PolarsError> {
        Ok(self.df.clone())
    }

    /// Select columns (returns a new DataFrame)
    pub fn select(&self, cols: Vec<&str>) -> Result<DataFrame, PolarsError> {
        let selected = self.df.select(cols)?;
        Ok(DataFrame::from_polars(selected))
    }

    /// Filter rows using a Polars expression.
    /// Internally converts to a lazy frame and materializes again.
    pub fn filter(&self, condition: Expr) -> Result<DataFrame, PolarsError> {
        use polars::prelude::IntoLazy;
        let lf = self.df.as_ref().clone().lazy().filter(condition);
        let df = lf.collect()?;
        Ok(DataFrame::from_polars(df))
    }

    /// Get a column reference by name (for building expressions)
    pub fn column(&self, name: &str) -> Result<Column, PolarsError> {
        let col_names = self.df.get_column_names();
        if !col_names.iter().any(|n| *n == name) {
            return Err(PolarsError::ColumnNotFound(name.to_string().into()));
        }
        Ok(Column::new(name.to_string()))
    }

    /// Add or replace a column using an expression
    pub fn with_column(&self, column_name: &str, expr: Expr) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let lf = self.df.as_ref().clone().lazy();
        let lf_with_col = lf.with_column(expr.alias(column_name));
        let pl_df = lf_with_col.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
    }

    /// Group by columns (returns GroupedData for aggregation)
    pub fn group_by(&self, column_names: Vec<&str>) -> Result<GroupedData, PolarsError> {
        use polars::prelude::*;
        let exprs: Vec<Expr> = column_names.iter().map(|name| col(*name)).collect();
        let lazy_grouped = self.df.as_ref().clone().lazy().group_by(exprs);
        Ok(GroupedData {
            lazy_grouped,
            grouping_cols: column_names.iter().map(|s| s.to_string()).collect(),
        })
    }

    /// Join with another DataFrame on the given columns
    ///
    /// # Example
    /// ```
    /// use robin_sparkless::{DataFrame, JoinType};
    /// # fn example(left: DataFrame, right: DataFrame) -> Result<DataFrame, polars::prelude::PolarsError> {
    /// let joined = left.join(&right, vec!["dept_id"], JoinType::Inner)?;
    /// # Ok(joined)
    /// # }
    /// ```
    pub fn join(
        &self,
        other: &DataFrame,
        on: Vec<&str>,
        how: JoinType,
    ) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let left_lf = self.df.as_ref().clone().lazy();
        let right_lf = other.df.as_ref().clone().lazy();
        let on_exprs: Vec<Expr> = on.iter().map(|name| col(*name)).collect();
        use polars::prelude::JoinType as PlJoinType;
        let polars_how = match how {
            crate::JoinType::Inner => PlJoinType::Inner,
            crate::JoinType::Left => PlJoinType::Left,
            crate::JoinType::Right => PlJoinType::Right,
            crate::JoinType::Outer => PlJoinType::Full,
        };
        use polars::prelude::JoinCoalesce;
        let joined = JoinBuilder::new(left_lf)
            .with(right_lf)
            .how(polars_how)
            .on(&on_exprs)
            .coalesce(JoinCoalesce::KeepColumns)
            .finish();
        let pl_df = joined.collect()?;
        Ok(Self::from_polars(pl_df))
    }

    /// Order by columns (sort)
    pub fn order_by(
        &self,
        column_names: Vec<&str>,
        ascending: Vec<bool>,
    ) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        // Ensure ascending vec matches column_names length, defaulting to true
        let mut asc = ascending;
        while asc.len() < column_names.len() {
            asc.push(true);
        }
        asc.truncate(column_names.len());

        // Use lazy sort for consistency with other operations
        let lf = self.df.as_ref().clone().lazy();
        let exprs: Vec<Expr> = column_names.iter().map(|name| col(*name)).collect();
        // with_order_descending_multi expects descending flags (true = descending),
        // but we have ascending flags (true = ascending), so invert them
        let descending: Vec<bool> = asc.iter().map(|&a| !a).collect();
        let sorted = lf.sort_by_exprs(
            exprs,
            SortMultipleOptions::new().with_order_descending_multi(descending),
        );
        let pl_df = sorted.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
    }
}

/// GroupedData - represents a DataFrame grouped by certain columns
/// Similar to PySpark's GroupedData
pub struct GroupedData {
    lazy_grouped: polars::prelude::LazyGroupBy,
    grouping_cols: Vec<String>,
}

impl GroupedData {
    /// Count rows in each group
    pub fn count(&self) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let agg_expr = vec![len().alias("count")];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
    }

    /// Sum a column in each group
    pub fn sum(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        // Use the column name with sum() to match PySpark's sum(column) behavior
        // PySpark returns "sum(column)" as the alias
        let agg_expr = vec![col(column).sum().alias(format!("sum({})", column))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;

        // Polars returns columns in a different order than PySpark
        // PySpark: [grouping_cols..., agg_cols...]
        // Polars: might be [agg_cols..., grouping_cols...] or different
        // Reorder to match PySpark: grouping columns first, then aggregations
        let all_cols: Vec<String> = pl_df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let grouping_cols: Vec<&str> = self.grouping_cols.iter().map(|s| s.as_str()).collect();
        let mut reordered_cols: Vec<&str> = Vec::new();

        // Add grouping columns first
        for gc in &grouping_cols {
            if all_cols.iter().any(|c| c == gc) {
                reordered_cols.push(gc);
            }
        }

        // Then add aggregation columns
        for col_name in &all_cols {
            if !grouping_cols.iter().any(|gc| *gc == col_name) {
                reordered_cols.push(col_name);
            }
        }

        if !reordered_cols.is_empty() {
            pl_df = pl_df.select(reordered_cols)?;
        }

        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
    }

    /// Average (mean) of a column in each group
    pub fn avg(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let agg_expr = vec![col(column).mean().alias(format!("avg({})", column))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
    }

    /// Minimum value of a column in each group
    pub fn min(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let agg_expr = vec![col(column).min().alias(format!("min({})", column))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
    }

    /// Maximum value of a column in each group
    pub fn max(&self, column: &str) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let agg_expr = vec![col(column).max().alias(format!("max({})", column))];
        let lf = self.lazy_grouped.clone().agg(agg_expr);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
    }

    /// Apply multiple aggregations at once (generic agg method)
    ///
    /// # Example
    /// ```
    /// use polars::prelude::*;
    /// use robin_sparkless::{DataFrame, GroupedData};
    ///
    /// # fn example(grouped_data: &GroupedData) -> Result<DataFrame, polars::prelude::PolarsError> {
    /// // Apply multiple aggregations
    /// let df = grouped_data.agg(vec![
    ///     col("salary").sum().alias("total_salary"),
    ///     col("salary").mean().alias("avg_salary"),
    /// ])?;
    /// #     Ok(df)
    /// # }
    /// ```
    pub fn agg(&self, aggregations: Vec<Expr>) -> Result<DataFrame, PolarsError> {
        let lf = self.lazy_grouped.clone().agg(aggregations);
        let mut pl_df = lf.collect()?;
        pl_df = reorder_groupby_columns(&mut pl_df, &self.grouping_cols)?;
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
    }

    /// Get grouping columns
    pub fn grouping_columns(&self) -> &[String] {
        &self.grouping_cols
    }
}

/// Reorder columns after groupBy to match PySpark order: grouping columns first, then aggregations
fn reorder_groupby_columns(
    pl_df: &mut PlDataFrame,
    grouping_cols: &[String],
) -> Result<PlDataFrame, PolarsError> {
    let all_cols: Vec<String> = pl_df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    let mut reordered_cols: Vec<&str> = Vec::new();

    // Add grouping columns first (in their original order)
    for gc in grouping_cols {
        if all_cols.iter().any(|c| c == gc) {
            reordered_cols.push(gc);
        }
    }

    // Then add aggregation columns (in their original order)
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

impl Clone for DataFrame {
    fn clone(&self) -> Self {
        DataFrame {
            df: self.df.clone(),
        }
    }
}
