use polars::prelude::{col, DataFrame as PlDataFrame, Expr, IntoLazy, PolarsError};
use crate::column::Column;
use crate::schema::StructType;
use std::sync::Arc;

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
        DataFrame { df: Arc::new(PlDataFrame::empty()) }
    }

    /// Get the schema of the DataFrame
    pub fn schema(&self) -> Result<StructType, PolarsError> {
        Ok(StructType::from_polars_schema(&self.df.schema()))
    }

    /// Get column names
    pub fn columns(&self) -> Result<Vec<String>, PolarsError> {
        Ok(self.df.get_column_names().iter().map(|s| s.to_string()).collect())
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
        let sorted = lf.sort_by_exprs(exprs, SortMultipleOptions::new().with_order_descending_multi(asc));
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
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars(pl_df))
    }

    /// Get grouping columns
    pub fn grouping_columns(&self) -> &[String] {
        &self.grouping_cols
    }
}

impl Clone for DataFrame {
    fn clone(&self) -> Self {
        DataFrame { df: self.df.clone() }
    }
}
