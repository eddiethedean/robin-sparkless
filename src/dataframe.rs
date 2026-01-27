use polars::prelude::{col, DataFrame as PlDataFrame, Expr, PolarsError};
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
}

impl Clone for DataFrame {
    fn clone(&self) -> Self {
        DataFrame { df: self.df.clone() }
    }
}
