//! DataFrame module: main tabular type and submodules for transformations, aggregations, joins.

mod aggregations;
mod joins;
mod transformations;

pub use aggregations::GroupedData;
pub use joins::{join, JoinType};
pub use transformations::{filter, order_by, select, with_column};

use crate::column::Column;
use crate::schema::StructType;
use polars::prelude::{DataFrame as PlDataFrame, Expr, PolarsError};
use std::sync::Arc;

/// Default for `spark.sql.caseSensitive` (PySpark default is false = case-insensitive).
const DEFAULT_CASE_SENSITIVE: bool = false;

/// DataFrame - main tabular data structure.
/// Thin wrapper around an eager Polars `DataFrame`.
pub struct DataFrame {
    pub(crate) df: Arc<PlDataFrame>,
    /// When false (default), column names are matched case-insensitively (PySpark behavior).
    pub(crate) case_sensitive: bool,
}

impl DataFrame {
    /// Create a new DataFrame from a Polars DataFrame (case-insensitive column matching by default).
    pub fn from_polars(df: PlDataFrame) -> Self {
        DataFrame {
            df: Arc::new(df),
            case_sensitive: DEFAULT_CASE_SENSITIVE,
        }
    }

    /// Create a new DataFrame from a Polars DataFrame with explicit case sensitivity.
    /// When `case_sensitive` is false, column resolution is case-insensitive (PySpark default).
    pub fn from_polars_with_options(df: PlDataFrame, case_sensitive: bool) -> Self {
        DataFrame {
            df: Arc::new(df),
            case_sensitive,
        }
    }

    /// Create an empty DataFrame
    pub fn empty() -> Self {
        DataFrame {
            df: Arc::new(PlDataFrame::empty()),
            case_sensitive: DEFAULT_CASE_SENSITIVE,
        }
    }

    /// Resolve a logical column name to the actual column name in the schema.
    /// When case_sensitive is false, matches case-insensitively.
    pub fn resolve_column_name(&self, name: &str) -> Result<String, PolarsError> {
        let names = self.df.get_column_names();
        if self.case_sensitive {
            if names.iter().any(|n| *n == name) {
                return Ok(name.to_string());
            }
        } else {
            let name_lower = name.to_lowercase();
            for n in names {
                if n.to_lowercase() == name_lower {
                    return Ok(n.to_string());
                }
            }
        }
        let available: Vec<String> = self
            .df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        Err(PolarsError::ColumnNotFound(
            format!(
                "Column '{}' not found. Available columns: [{}]. Check spelling and case sensitivity (spark.sql.caseSensitive).",
                name,
                available.join(", ")
            )
            .into(),
        ))
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
        println!("{}", self.df.head(Some(n)));
        Ok(())
    }

    /// Collect the DataFrame (action - triggers execution)
    pub fn collect(&self) -> Result<Arc<PlDataFrame>, PolarsError> {
        Ok(self.df.clone())
    }

    /// Select columns (returns a new DataFrame).
    /// Column names are resolved according to case sensitivity.
    pub fn select(&self, cols: Vec<&str>) -> Result<DataFrame, PolarsError> {
        let resolved: Vec<String> = cols
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
        transformations::select(self, refs, self.case_sensitive)
    }

    /// Filter rows using a Polars expression.
    pub fn filter(&self, condition: Expr) -> Result<DataFrame, PolarsError> {
        transformations::filter(self, condition, self.case_sensitive)
    }

    /// Get a column reference by name (for building expressions).
    /// Respects case sensitivity: when false, "Age" resolves to column "age" if present.
    pub fn column(&self, name: &str) -> Result<Column, PolarsError> {
        let resolved = self.resolve_column_name(name)?;
        Ok(Column::new(resolved))
    }

    /// Add or replace a column using an expression
    pub fn with_column(&self, column_name: &str, expr: Expr) -> Result<DataFrame, PolarsError> {
        transformations::with_column(self, column_name, expr, self.case_sensitive)
    }

    /// Group by columns (returns GroupedData for aggregation).
    /// Column names are resolved according to case sensitivity.
    pub fn group_by(&self, column_names: Vec<&str>) -> Result<GroupedData, PolarsError> {
        use polars::prelude::*;
        let resolved: Vec<String> = column_names
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let exprs: Vec<Expr> = resolved.iter().map(|name| col(name.as_str())).collect();
        let lazy_grouped = self.df.as_ref().clone().lazy().group_by(exprs);
        Ok(GroupedData {
            lazy_grouped,
            grouping_cols: resolved,
            case_sensitive: self.case_sensitive,
        })
    }

    /// Join with another DataFrame on the given columns.
    /// Join column names are resolved on the left (and right must have matching names).
    pub fn join(
        &self,
        other: &DataFrame,
        on: Vec<&str>,
        how: JoinType,
    ) -> Result<DataFrame, PolarsError> {
        let resolved: Vec<String> = on
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let on_refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
        join(self, other, on_refs, how, self.case_sensitive)
    }

    /// Order by columns (sort).
    /// Column names are resolved according to case sensitivity.
    pub fn order_by(
        &self,
        column_names: Vec<&str>,
        ascending: Vec<bool>,
    ) -> Result<DataFrame, PolarsError> {
        let resolved: Vec<String> = column_names
            .iter()
            .map(|c| self.resolve_column_name(c))
            .collect::<Result<Vec<_>, _>>()?;
        let refs: Vec<&str> = resolved.iter().map(|s| s.as_str()).collect();
        transformations::order_by(self, refs, ascending, self.case_sensitive)
    }

    /// Union (unionAll): stack another DataFrame vertically. Schemas must match (same columns, same order).
    pub fn union(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::union(self, other, self.case_sensitive)
    }

    /// Union by name: stack vertically, aligning columns by name.
    pub fn union_by_name(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::union_by_name(self, other, self.case_sensitive)
    }

    /// Distinct: drop duplicate rows (all columns or optional subset).
    pub fn distinct(&self, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        transformations::distinct(self, subset, self.case_sensitive)
    }

    /// Drop one or more columns.
    pub fn drop(&self, columns: Vec<&str>) -> Result<DataFrame, PolarsError> {
        transformations::drop(self, columns, self.case_sensitive)
    }

    /// Drop rows with nulls (all columns or optional subset).
    pub fn dropna(&self, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        transformations::dropna(self, subset, self.case_sensitive)
    }

    /// Fill nulls with a literal expression (applied to all columns).
    pub fn fillna(&self, value: Expr) -> Result<DataFrame, PolarsError> {
        transformations::fillna(self, value, self.case_sensitive)
    }

    /// Limit: return first n rows.
    pub fn limit(&self, n: usize) -> Result<DataFrame, PolarsError> {
        transformations::limit(self, n, self.case_sensitive)
    }

    /// Rename a column (old_name -> new_name).
    pub fn with_column_renamed(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<DataFrame, PolarsError> {
        transformations::with_column_renamed(self, old_name, new_name, self.case_sensitive)
    }

    /// Replace values in a column (old_value -> new_value). PySpark replace.
    pub fn replace(
        &self,
        column_name: &str,
        old_value: Expr,
        new_value: Expr,
    ) -> Result<DataFrame, PolarsError> {
        transformations::replace(self, column_name, old_value, new_value, self.case_sensitive)
    }

    /// Cross join with another DataFrame (cartesian product). PySpark crossJoin.
    pub fn cross_join(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::cross_join(self, other, self.case_sensitive)
    }

    /// Summary statistics. PySpark describe.
    pub fn describe(&self) -> Result<DataFrame, PolarsError> {
        transformations::describe(self, self.case_sensitive)
    }

    /// No-op: execution is eager by default. PySpark cache.
    pub fn cache(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op: execution is eager by default. PySpark persist.
    pub fn persist(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// No-op. PySpark unpersist.
    pub fn unpersist(&self) -> Result<DataFrame, PolarsError> {
        Ok(self.clone())
    }

    /// Set difference: rows in self not in other. PySpark subtract / except.
    pub fn subtract(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::subtract(self, other, self.case_sensitive)
    }

    /// Set intersection: rows in both self and other. PySpark intersect.
    pub fn intersect(&self, other: &DataFrame) -> Result<DataFrame, PolarsError> {
        transformations::intersect(self, other, self.case_sensitive)
    }

    /// Write this DataFrame to a Delta table at the given path.
    /// Requires the `delta` feature. If `overwrite` is true, replaces the table; otherwise appends.
    #[cfg(feature = "delta")]
    pub fn write_delta(
        &self,
        path: impl AsRef<std::path::Path>,
        overwrite: bool,
    ) -> Result<(), PolarsError> {
        crate::delta::write_delta(self.df.as_ref(), path, overwrite)
    }

    /// Stub when `delta` feature is disabled.
    #[cfg(not(feature = "delta"))]
    pub fn write_delta(
        &self,
        _path: impl AsRef<std::path::Path>,
        _overwrite: bool,
    ) -> Result<(), PolarsError> {
        Err(PolarsError::InvalidOperation(
            "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
        ))
    }
}

impl Clone for DataFrame {
    fn clone(&self) -> Self {
        DataFrame {
            df: self.df.clone(),
            case_sensitive: self.case_sensitive,
        }
    }
}
