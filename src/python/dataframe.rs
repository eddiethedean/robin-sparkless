//! Python DataFrame, GroupedData, DataFrameStat, DataFrameNa, DataFrameWriter (PySpark sql).

use crate::dataframe::JoinType;
use crate::dataframe::{CubeRollupData, WriteFormat, WriteMode};
use crate::functions::SortOrder;
use crate::{DataFrame, GroupedData};
use polars::prelude::{col, lit, Expr, NULL};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::path::Path;
use std::sync::RwLock;

use super::column::PyColumn;
use super::order::PySortOrder;
use super::session::get_default_session;

fn py_any_to_expr(value: &pyo3::Bound<'_, pyo3::types::PyAny>) -> PyResult<Expr> {
    let expr = if value.is_none() {
        lit(NULL)
    } else if let Ok(x) = value.extract::<i64>() {
        lit(x)
    } else if let Ok(x) = value.extract::<f64>() {
        lit(x)
    } else if let Ok(x) = value.extract::<bool>() {
        lit(x)
    } else if let Ok(x) = value.extract::<String>() {
        lit(x.as_str())
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "replace: to_replace and value must be None, int, float, bool, or str",
        ));
    };
    Ok(expr)
}

/// Python wrapper for DataFrame.
#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    pub inner: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    /// Return the number of rows in the DataFrame.
    ///
    /// Triggers evaluation of the lazy plan. For a lazy pipeline, prefer ``limit(0).count()``
    /// only when you need the count; otherwise continue with lazy operations.
    ///
    /// Returns:
    ///     int: Row count.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn count(&self) -> PyResult<usize> {
        self.inner
            .count()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Print the first n rows to stdout in a tabular format.
    ///
    /// Args:
    ///     n: Number of rows to show. Default 20.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    #[pyo3(signature = (n=None))]
    fn show(&self, n: Option<usize>) -> PyResult<()> {
        self.inner
            .show(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Write the DataFrame as a Delta Lake table at the given path.
    ///
    /// Args:
    ///     path: Local path or URI for the Delta table. Directory will be created if needed.
    ///     overwrite: If True, replace existing table; if False, append (or error if exists).
    ///
    /// Raises:
    ///     RuntimeError: If write fails. Requires the ``delta`` feature.
    #[cfg(feature = "delta")]
    fn write_delta(&self, path: &str, overwrite: bool) -> PyResult<()> {
        self.inner
            .write_delta(Path::new(path), overwrite)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Materialize the DataFrame and return rows as a list of dicts.
    ///
    /// Each element is a dict mapping column name to value (Python types: int, float, bool,
    /// str, None). Triggers full evaluation of the lazy plan.
    ///
    /// Returns:
    ///     list[dict]: One dict per row. Column names are keys.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails or a value type cannot be converted to Python.
    fn collect(&self, py: Python<'_>) -> PyResult<PyObject> {
        let pl_df = self
            .inner
            .collect()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let df = pl_df.as_ref();
        let names = df.get_column_names();
        let nrows = df.height();
        let rows = pyo3::types::PyList::empty(py);
        for i in 0..nrows {
            let row_dict = PyDict::new(py);
            for (col_idx, name) in names.iter().enumerate() {
                let s = df.get_columns().get(col_idx).ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("column index out of range")
                })?;
                let av = s
                    .get(i)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                let py_val = any_value_to_py(py, av)?;
                row_dict.set_item(name.as_str(), py_val)?;
            }
            rows.append(&*row_dict)?;
        }
        Ok(rows.into())
    }

    /// Return a DataFrame with only rows where the condition is true.
    ///
    /// Args:
    ///     condition: Boolean Column expression (e.g. ``col("age") > 18``), or literal
    ///         ``True`` (no filter) / ``False`` (filter to zero rows) for PySpark parity.
    ///
    /// Returns:
    ///     DataFrame (lazy) with filtered rows.
    ///
    /// Raises:
    ///     TypeError: If condition is not a Column or literal bool (True/False).
    ///     RuntimeError: If the expression cannot be applied.
    fn filter(&self, condition: &pyo3::Bound<'_, pyo3::types::PyAny>) -> PyResult<PyDataFrame> {
        let expr: Expr = if let Ok(py_col) = condition.downcast::<PyColumn>() {
            py_col.borrow().inner.expr().clone()
        } else if let Ok(b) = condition.extract::<bool>() {
            if b {
                lit(true)
            } else {
                lit(false)
            }
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "condition must be a Column or literal bool (True/False)",
            ));
        };
        let df = self
            .inner
            .filter(expr)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with the specified columns or expressions.
    ///
    /// Args:
    ///     *cols: Column names (str) and/or Column expressions (e.g. ``regexp_extract_all(col("s"), r"\\d+", 0).alias("m")``).
    ///         Supports both ``select("a", "b")`` and ``select([col("a"), col("b")])`` (PySpark-style).
    ///         Order is preserved. Column names are resolved according to schema.
    ///
    /// Returns:
    ///     DataFrame (lazy). Missing columns raise RuntimeError at execution.
    ///
    /// Raises:
    ///     TypeError: If an item is not str or Column.
    ///     RuntimeError: If a column name is not in the schema or expression evaluation fails.
    #[pyo3(signature = (*cols))]
    fn select(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let items: Vec<Bound<'_, pyo3::types::PyAny>> = if cols.len() == 1 {
            let first = cols.get_item(0)?;
            if let Ok(lst) = first.downcast::<PyList>() {
                lst.iter().collect()
            } else {
                vec![first]
            }
        } else {
            cols.iter().map(|o| o.clone()).collect()
        };
        let mut exprs: Vec<Expr> = Vec::with_capacity(items.len());
        for item in items.iter() {
            // Try Column before str: PySpark Column objects are often convertible to str (e.g. "(2 + x)"),
            // so we must treat as expression first to support select(col("a") * 2, lit(3) + col("x")).
            if let Ok(py_col) = item.extract::<PyRef<PyColumn>>() {
                exprs.push(py_col.inner.expr().clone());
            } else if let Ok(name) = item.extract::<std::string::String>() {
                let resolved = self
                    .inner
                    .resolve_column_name(&name)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                exprs.push(col(resolved.as_str()));
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "select() items must be str (column name) or Column (expression)",
                ));
            }
        }
        let df = self
            .inner
            .select_exprs(exprs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Add a new column or replace an existing one with the result of an expression.
    ///
    /// Args:
    ///     column_name: Name of the column to add or replace.
    ///     expr: Column expression (e.g. ``col("a") + col("b")``).
    ///
    /// Returns:
    ///     DataFrame (lazy) with the new/updated column.
    ///
    /// Raises:
    ///     RuntimeError: If the expression cannot be evaluated.
    fn with_column(&self, column_name: &str, expr: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .with_column(column_name, &expr.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Sort by one or more columns by name.
    ///
    /// Args:
    ///     cols: Column names to sort by, in order of precedence.
    ///     ascending: Optional list of booleans, one per column. True = ascending, False = descending.
    ///         If omitted, all columns are sorted ascending.
    ///
    /// Returns:
    ///     DataFrame (lazy) with rows sorted.
    ///
    /// Raises:
    ///     RuntimeError: If a column is not in the schema.
    #[pyo3(signature = (cols, ascending=None))]
    fn order_by(&self, cols: Vec<String>, ascending: Option<Vec<bool>>) -> PyResult<PyDataFrame> {
        let asc = ascending.unwrap_or_else(|| vec![true; cols.len()]);
        let df = self
            .inner
            .order_by(cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(), asc)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Sort by column expressions with explicit nulls-first/last (e.g. asc(col("a")), desc(col("b"))).
    ///
    /// Args:
    ///     sort_orders: List of SortOrder from ``asc()``, ``desc()``, ``asc_nulls_first()``, etc.
    ///
    /// Returns:
    ///     DataFrame (lazy) with rows sorted.
    ///
    /// Raises:
    ///     RuntimeError: If an expression cannot be evaluated.
    fn order_by_exprs(&self, sort_orders: Vec<PySortOrder>) -> PyResult<PyDataFrame> {
        let orders: Vec<SortOrder> = sort_orders.into_iter().map(|po| po.inner).collect();
        let df = self
            .inner
            .order_by_exprs(orders)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Group rows by the given columns for aggregation via ``GroupedData.agg()``, ``sum()``, etc.
    ///
    /// Args:
    ///     cols: Column names to group by.
    ///
    /// Returns:
    ///     GroupedData: Use ``.agg()``, ``.count()``, ``.sum(column)``, etc.
    ///
    /// Raises:
    ///     RuntimeError: If a column name is not in the schema.
    fn group_by(&self, cols: Vec<String>) -> PyResult<PyGroupedData> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let gd = self
            .inner
            .group_by(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyGroupedData { inner: gd })
    }

    /// Create a CUBE grouping: aggregates over all combinations of the given columns (including null/global).
    ///
    /// Args:
    ///     cols: Column names for the CUBE dimensions.
    ///
    /// Returns:
    ///     CubeRollupData: Call ``.agg(...)`` to compute aggregates.
    ///
    /// Raises:
    ///     RuntimeError: If a column is not in the schema.
    fn cube(&self, cols: Vec<String>) -> PyResult<PyCubeRollupData> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let cr = self
            .inner
            .cube(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyCubeRollupData { inner: cr })
    }

    /// Create a ROLLUP grouping: aggregates at each level of the hierarchy (e.g. (a,b), (a), ()).
    ///
    /// Args:
    ///     cols: Column names for the ROLLUP hierarchy, in order.
    ///
    /// Returns:
    ///     CubeRollupData: Call ``.agg(...)`` to compute aggregates.
    ///
    /// Raises:
    ///     RuntimeError: If a column is not in the schema.
    fn rollup(&self, cols: Vec<String>) -> PyResult<PyCubeRollupData> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let cr = self
            .inner
            .rollup(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyCubeRollupData { inner: cr })
    }

    /// Return a DataFrameWriter to save the DataFrame (chain ``.mode()``, ``.format()``, then ``.save(path)``).
    ///
    /// Stub: writeTo (DataFrameWriterV2 / catalog tables) not supported.
    /// Use df.write().parquet(path) or df.write().csv(path) instead.
    #[pyo3(name = "writeTo")]
    fn write_to(&self) -> PyResult<PyObject> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "writeTo (catalog tables) is not supported; use df.write().parquet(path) or df.write().csv(path)",
        ))
    }

    /// Returns:
    ///     DataFrameWriter: Default mode "overwrite", format "parquet".
    fn write(&self) -> PyDataFrameWriter {
        PyDataFrameWriter {
            df: self.inner.clone(),
            mode: RwLock::new(WriteMode::Overwrite),
            format: RwLock::new(WriteFormat::Parquet),
            options: RwLock::new(std::collections::HashMap::new()),
            partition_by: RwLock::new(Vec::new()),
        }
    }

    /// Register this DataFrame as a temp view (PySpark: df.createOrReplaceTempView(name)).
    /// Uses the default session from SparkSession.builder().get_or_create().
    #[cfg(feature = "sql")]
    #[pyo3(name = "createOrReplaceTempView")]
    fn create_or_replace_temp_view(&self, name: &str, _py: Python<'_>) -> PyResult<()> {
        let session = get_default_session().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "create_or_replace_temp_view: no default session. Call SparkSession.builder().get_or_create() first.",
            )
        })?;
        session.create_or_replace_temp_view(name, self.inner.clone());
        Ok(())
    }

    /// Register this DataFrame as a temp view (alias for create_or_replace_temp_view).
    #[cfg(feature = "sql")]
    #[pyo3(name = "createTempView")]
    fn create_temp_view(&self, name: &str, py: Python<'_>) -> PyResult<()> {
        self.create_or_replace_temp_view(name, py)
    }

    /// Register this DataFrame as a global temp view (stub: uses same catalog as temp view).
    #[cfg(feature = "sql")]
    #[pyo3(name = "createGlobalTempView")]
    fn create_global_temp_view(&self, name: &str, py: Python<'_>) -> PyResult<()> {
        self.create_or_replace_temp_view(name, py)
    }

    /// Register this DataFrame as a global temp view (stub: uses same catalog as temp view).
    #[cfg(feature = "sql")]
    #[pyo3(name = "createOrReplaceGlobalTempView")]
    fn create_or_replace_global_temp_view(&self, name: &str, py: Python<'_>) -> PyResult<()> {
        self.create_or_replace_temp_view(name, py)
    }

    /// Join with another DataFrame on one or more column names.
    ///
    /// Args:
    ///     other: Right DataFrame.
    ///     on: Column names to join on. Must exist in both DataFrames with compatible types.
    ///     how: Join type: "inner", "left", "right", or "outer". Default "inner".
    ///
    /// Returns:
    ///     DataFrame (lazy) with joined rows. Join columns appear once.
    ///
    /// Raises:
    ///     ValueError: If ``how`` is not one of the allowed values.
    ///     RuntimeError: If join execution fails.
    #[pyo3(signature = (other, on, how="inner"))]
    fn join(&self, other: &PyDataFrame, on: Vec<String>, how: &str) -> PyResult<PyDataFrame> {
        // PySpark: join(other) with no on = cross join
        if on.is_empty() {
            let df = self
                .inner
                .cross_join(&other.inner)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            return Ok(PyDataFrame { inner: df });
        }
        let join_type = match how.to_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "outer" => JoinType::Outer,
            "left_semi" | "semi" => JoinType::LeftSemi,
            "left_anti" | "anti" => JoinType::LeftAnti,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "how must be 'inner', 'left', 'right', 'outer', 'left_semi', or 'left_anti'",
                ));
            }
        };
        let on_refs: Vec<&str> = on.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .join(&other.inner, on_refs, join_type)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Concatenate rows of another DataFrame. Schemas must match in order and type.
    ///
    /// Args:
    ///     other: DataFrame to stack below this one. Same number and order of columns required.
    ///
    /// Returns:
    ///     DataFrame (lazy). Duplicate rows are retained; use ``distinct()`` to deduplicate.
    ///
    /// Raises:
    ///     RuntimeError: If schemas are incompatible.
    fn union(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .union(&other.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Concatenate rows of another DataFrame by matching column names (column order may differ).
    ///
    /// Args:
    ///     other: DataFrame to stack. Columns with the same name are aligned; missing columns become null.
    ///
    /// Returns:
    ///     DataFrame (lazy). Column order follows this DataFrame.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn union_by_name(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .union_by_name(&other.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return rows with duplicates removed. Optionally consider only a subset of columns for uniqueness.
    ///
    /// Args:
    ///     subset: If provided, only these columns determine uniqueness; otherwise all columns.
    ///
    /// Returns:
    ///     DataFrame (lazy) with one row per distinct key.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    #[pyo3(signature = (subset=None))]
    fn distinct(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .inner
            .distinct(sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with the specified columns removed.
    ///
    /// Args:
    ///     cols: Column names to drop. Ignored if a name is not in the schema.
    ///
    /// Returns:
    ///     DataFrame (lazy) without those columns.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn drop(&self, cols: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .drop(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Pivot (wide format). PySpark pivot. Stub: not yet implemented.
    #[pyo3(signature = (pivot_col, values=None))]
    fn pivot(&self, pivot_col: &str, values: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let _ = (pivot_col, values);
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "pivot is not yet implemented; use crosstab(col1, col2) for two-column cross-tabulation.",
        ))
    }

    /// Drop rows containing null values. Optionally only in specified columns.
    ///
    /// Args:
    ///     subset: If provided, drop a row only when any of these columns is null; otherwise any null drops the row.
    ///
    /// Returns:
    ///     DataFrame (lazy) with null rows removed.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    #[pyo3(signature = (subset=None))]
    fn dropna(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .inner
            .dropna(sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Replace null values with the result of the given expression (e.g. ``lit(0)``).
    ///
    /// Args:
    ///     value: Column expression; can be a literal or another column. Applied per row.
    ///
    /// Returns:
    ///     DataFrame (lazy) with nulls filled.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn fillna(&self, value: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .fillna(value.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Replace values in columns (PySpark replace). Where column equals to_replace, use value.
    ///
    /// Args:
    ///     to_replace: Value to replace (None, int, float, bool, or str).
    ///     value: Replacement value. Same types supported.
    ///     subset: Optional list of column names. If None, applies to all columns.
    ///
    /// Returns:
    ///     DataFrame with replacements applied.
    #[pyo3(signature = (to_replace, value, subset=None))]
    fn replace(
        &self,
        to_replace: &pyo3::Bound<'_, pyo3::types::PyAny>,
        value: &pyo3::Bound<'_, pyo3::types::PyAny>,
        subset: Option<Vec<String>>,
    ) -> PyResult<PyDataFrame> {
        let old_expr = py_any_to_expr(to_replace)?;
        let new_expr = py_any_to_expr(value)?;
        let cols: Vec<String> = match &subset {
            Some(s) => s.clone(),
            None => self
                .inner
                .columns()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?,
        };
        let mut df = self.inner.clone();
        for col_name in cols {
            df = df
                .replace(col_name.as_str(), old_expr.clone(), new_expr.clone())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        }
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with at most the first n rows.
    ///
    /// Args:
    ///     n: Maximum number of rows (non-negative integer).
    ///
    /// Returns:
    ///     DataFrame (lazy). Fewer rows if the dataset has fewer than n rows.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn limit(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .limit(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with one column renamed.
    ///
    /// Args:
    ///     old: Current column name.
    ///     new: New column name. Must not conflict with existing names if different from old.
    ///
    /// Returns:
    ///     DataFrame (lazy) with the renamed column.
    ///
    /// Raises:
    ///     RuntimeError: If ``old`` is not in the schema or execution fails.
    fn with_column_renamed(&self, old: &str, new: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .with_column_renamed(old, new)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with a random sample of rows.
    ///
    /// Args:
    ///     with_replacement: If True, rows may be repeated; if False, each row at most once.
    ///     fraction: Fraction of rows to sample in [0, 1]. 1.0 means all rows.
    ///     seed: Optional random seed for reproducibility.
    ///
    /// Returns:
    ///     DataFrame (lazy) with approximately fraction * total rows.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    #[pyo3(signature = (with_replacement=false, fraction=1.0, seed=None))]
    fn sample(
        &self,
        with_replacement: bool,
        fraction: f64,
        seed: Option<u64>,
    ) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sample(with_replacement, fraction, seed)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return the first row as a single-row DataFrame. Equivalent to ``head(1)``.
    ///
    /// Returns:
    ///     DataFrame (lazy) with at most one row. Empty DataFrame if source is empty.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn first(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .first()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return the first n rows.
    ///
    /// Args:
    ///     n: Number of rows (non-negative integer).
    ///
    /// Returns:
    ///     DataFrame (lazy) with at most n rows.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn head(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .head(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return the last n rows. Requires evaluating the full dataset to determine order.
    ///
    /// Args:
    ///     n: Number of rows (non-negative integer).
    ///
    /// Returns:
    ///     DataFrame (lazy) with at most n rows from the end.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn tail(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .tail(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return the first n rows. Alias for ``head(n)``.
    fn take(&self, n: usize) -> PyResult<PyDataFrame> {
        self.head(n)
    }

    /// Return True if the DataFrame has zero rows, False otherwise. Triggers evaluation.
    ///
    /// Returns:
    ///     bool: True if empty.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn is_empty(&self) -> PyResult<bool> {
        Ok(self.inner.is_empty())
    }

    /// Serialize each row to a JSON object string. Returns one string per row (newline-delimited style).
    ///
    /// Returns:
    ///     list[str]: One JSON object string per row.
    ///
    /// Raises:
    ///     RuntimeError: If execution or serialization fails.
    #[pyo3(name = "toJSON")]
    fn to_json(&self) -> PyResult<Vec<String>> {
        self.inner
            .to_json()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Alias for toJSON (snake_case).
    #[pyo3(name = "to_json")]
    fn to_json_snake(&self) -> PyResult<Vec<String>> {
        self.to_json()
    }

    /// Return a string representation of the logical plan (for debugging).
    ///
    /// Returns:
    ///     str: Plan description. Does not trigger full execution.
    fn explain(&self) -> PyResult<String> {
        Ok(self.inner.explain())
    }

    /// Return the schema as a human-readable string (column names and types).
    ///
    /// Returns:
    ///     str: Schema string. Does not trigger full execution.
    ///
    /// Raises:
    ///     RuntimeError: If schema cannot be computed.
    fn print_schema(&self) -> PyResult<String> {
        self.inner
            .print_schema()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Checkpoint the plan (break lineage for optimization).
    fn checkpoint(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .checkpoint()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Local checkpoint (same process).
    fn local_checkpoint(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .local_checkpoint()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Repartition into num_partitions (logical; may not change physical layout in Sparkless).
    fn repartition(&self, num_partitions: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .repartition(num_partitions)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Coalesce to fewer partitions.
    fn coalesce(&self, num_partitions: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .coalesce(num_partitions)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Skip the first n rows.
    fn offset(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .offset(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Best-effort local collection: returns list of rows (same as collect()). PySpark .data.
    fn data(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.collect(py)
    }

    /// Persist (no-op in Sparkless; returns self for API compatibility).
    fn persist(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .persist()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Unpersist (no-op in Sparkless; returns self).
    fn unpersist(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .unpersist()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// No-op: query planner hint. Returns self for chaining.
    fn hint(&self, _name: &str, _params: Vec<i32>) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .hint(_name, &_params)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// No-op: Polars has no range partitioning. Returns self.
    #[pyo3(name = "repartitionByRange")]
    #[pyo3(signature = (num_partitions, *cols))]
    fn repartition_by_range(
        &self,
        num_partitions: usize,
        cols: Vec<String>,
    ) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .repartition_by_range(num_partitions, refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// No-op: same as orderBy for compatibility. Returns self.
    #[pyo3(name = "sortWithinPartitions")]
    #[pyo3(signature = (*cols))]
    fn sort_within_partitions(&self, cols: Vec<String>) -> PyResult<PyDataFrame> {
        use crate::functions::{asc, col};
        let sorts: Vec<SortOrder> = cols.iter().map(|c| asc(&col(c.as_str()))).collect();
        let df = self
            .inner
            .sort_within_partitions(&sorts)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// No-op: returns False (semantic comparison not implemented).
    #[pyo3(name = "sameSemantics")]
    fn same_semantics(&self, other: &PyDataFrame) -> bool {
        self.inner.same_semantics(&other.inner)
    }

    /// No-op: returns 0 (semantic hash not implemented).
    #[pyo3(name = "semanticHash")]
    fn semantic_hash(&self) -> u64 {
        self.inner.semantic_hash()
    }

    /// Return list of column names.
    fn columns(&self) -> PyResult<Vec<String>> {
        self.inner
            .columns()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// No-op: eager execution. Returns self.
    fn cache(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .cache()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Always True (eager single-node execution).
    #[pyo3(name = "isLocal")]
    fn is_local(&self) -> bool {
        self.inner.is_local()
    }

    /// Return empty list (no file sources in eager execution).
    #[pyo3(name = "inputFiles")]
    fn input_files(&self) -> Vec<String> {
        self.inner.input_files()
    }

    /// Stub: RDD API is not supported. Raises NotImplementedError.
    fn rdd(&self) -> PyResult<PyObject> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "RDD is not supported in Sparkless; use collect() or toLocalIterator() for local data",
        ))
    }

    /// Stub: foreach is not supported (no distributed execution). Raises NotImplementedError.
    fn foreach(&self, _f: &Bound<'_, pyo3::types::PyAny>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "foreach is not supported in Sparkless",
        ))
    }

    /// Stub: foreachPartition is not supported. Raises NotImplementedError.
    #[pyo3(name = "foreachPartition")]
    fn foreach_partition(&self, _f: &Bound<'_, pyo3::types::PyAny>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "foreachPartition is not supported in Sparkless",
        ))
    }

    /// Stub: mapInPandas is not supported. Raises NotImplementedError.
    #[pyo3(name = "mapInPandas")]
    fn map_in_pandas(
        &self,
        _func: &Bound<'_, pyo3::types::PyAny>,
        _schema: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyDataFrame> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "mapInPandas is not supported in Sparkless",
        ))
    }

    /// Stub: mapPartitions is not supported. Raises NotImplementedError.
    #[pyo3(name = "mapPartitions")]
    fn map_partitions(
        &self,
        _f: &Bound<'_, pyo3::types::PyAny>,
        _schema: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyDataFrame> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "mapPartitions is not supported in Sparkless",
        ))
    }

    /// Returns an iterable of rows (same as collect()). Best-effort local iterator.
    #[pyo3(name = "toLocalIterator")]
    fn to_local_iterator(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.collect(py)
    }

    /// Stub: storage level not applicable (eager execution). Returns None.
    #[pyo3(name = "storageLevel")]
    fn storage_level(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(py.None())
    }

    /// Always False; streaming is not supported.
    #[pyo3(name = "isStreaming")]
    fn is_streaming(&self) -> PyResult<bool> {
        Ok(false)
    }

    /// No-op; streaming/watermark not supported. Returns self for chaining.
    #[pyo3(name = "withWatermark")]
    fn with_watermark(&self, _event_time: &str, _delay_threshold: &str) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame {
            inner: self.inner.clone(),
        })
    }

    /// Split into multiple DataFrames by weight fractions; optional seed for reproducibility.
    fn random_split(&self, weights: Vec<f64>, seed: Option<u64>) -> PyResult<Vec<PyDataFrame>> {
        let dfs = self
            .inner
            .random_split(&weights, seed)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(dfs
            .into_iter()
            .map(|df| PyDataFrame { inner: df })
            .collect())
    }

    /// Summary statistics (count, mean, stddev, min, max) for numeric columns.
    fn summary(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .summary()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Rename all columns to the given names (e.g. after toDF in Scala).
    #[pyo3(name = "toDF")]
    fn to_df(&self, names: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .to_df(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Alias for toDF (snake_case). PySpark df.to_df(*cols).
    #[pyo3(name = "to_df")]
    fn to_df_snake(&self, names: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .to_df(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Select using SQL-like expression strings.
    fn select_expr(&self, exprs: Vec<String>) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .select_expr(&exprs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Select columns whose names match the regex pattern.
    fn col_regex(&self, pattern: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .col_regex(pattern)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Add or replace multiple columns (dict[name, Column] or list of (name, Column)).
    fn with_columns(&self, mapping: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyDataFrame> {
        let mut exprs: Vec<(String, crate::column::Column)> = Vec::new();
        if let Ok(dict) = mapping.downcast::<PyDict>() {
            for (k, v) in dict.iter() {
                let name: String = k.extract()?;
                let col: PyRef<PyColumn> = v.extract()?;
                exprs.push((name, col.inner.clone()));
            }
        } else if let Ok(list) = mapping.downcast::<pyo3::types::PyList>() {
            for item in list.iter() {
                let tuple: (String, PyRef<PyColumn>) = item.extract()?;
                exprs.push((tuple.0, tuple.1.inner.clone()));
            }
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "with_columns expects dict[str, Column] or list[tuple[str, Column]]",
            ));
        }
        let df = self
            .inner
            .with_columns(&exprs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Rename columns (dict[old, new] or list of (old, new)).
    fn with_columns_renamed(
        &self,
        mapping: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyDataFrame> {
        let mut renames: Vec<(String, String)> = Vec::new();
        if let Ok(dict) = mapping.downcast::<PyDict>() {
            for (k, v) in dict.iter() {
                let old_name: String = k.extract()?;
                let new_name: String = v.extract()?;
                renames.push((old_name, new_name));
            }
        } else if let Ok(list) = mapping.downcast::<pyo3::types::PyList>() {
            for item in list.iter() {
                let tuple: (String, String) = item.extract()?;
                renames.push(tuple);
            }
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "with_columns_renamed expects dict[str, str] or list[tuple[str, str]]",
            ));
        }
        let df = self
            .inner
            .with_columns_renamed(&renames)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrameStat for covariance, correlation, and correlation matrix.
    ///
    /// Returns:
    ///     DataFrameStat: Use ``.cov(col1, col2)``, ``.corr(col1, col2)``, ``.corr_matrix()``.
    fn stat(&self) -> PyDataFrameStat {
        PyDataFrameStat {
            df: self.inner.clone(),
        }
    }

    /// Correlation matrix or scalar. PySpark: corr() -> matrix, corr(col1, col2) -> float.
    #[pyo3(signature = (col1=None, col2=None))]
    fn corr(&self, col1: Option<&str>, col2: Option<&str>, py: Python<'_>) -> PyResult<PyObject> {
        match (col1, col2) {
            (Some(c1), Some(c2)) => {
                let r = self
                    .inner
                    .corr_cols(c1, c2)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                Ok(r.into_py(py))
            }
            _ => {
                let df = self
                    .inner
                    .corr()
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                Ok(PyDataFrame { inner: df }.into_py(py))
            }
        }
    }

    /// Sample covariance of two columns (scalar). PySpark df.cov(col1, col2).
    fn cov(&self, col1: &str, col2: &str) -> PyResult<f64> {
        self.inner
            .cov_cols(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Return a DataFrameNa for null handling: fill nulls or drop rows with nulls.
    ///
    /// Returns:
    ///     DataFrameNa: Use ``.fill(value)`` or ``.drop(subset=None)``.
    fn na(&self) -> PyDataFrameNa {
        PyDataFrameNa {
            df: self.inner.clone(),
        }
    }

    /// Materialize rows as a list of dicts. Same as ``collect()``; name for API compatibility (no pandas dependency).
    ///
    /// Returns:
    ///     list[dict]: One dict per row, column names as keys.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    #[pyo3(name = "toPandas")]
    fn to_pandas(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.collect(py)
    }

    /// Alias for toPandas (snake_case).
    #[pyo3(name = "to_pandas")]
    fn to_pandas_snake(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.to_pandas(py)
    }
}

/// Python wrapper for DataFrame.stat() (cov, corr).
#[pyclass(name = "DataFrameStat")]
pub struct PyDataFrameStat {
    df: DataFrame,
}

#[pymethods]
impl PyDataFrameStat {
    /// Return the sample covariance of two numeric columns.
    ///
    /// Args:
    ///     col1: First column name.
    ///     col2: Second column name.
    ///
    /// Returns:
    ///     float: Covariance. Nulls are excluded.
    ///
    /// Raises:
    ///     RuntimeError: If columns are missing or not numeric.
    fn cov(&self, col1: &str, col2: &str) -> PyResult<f64> {
        self.df
            .stat()
            .cov(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Return the Pearson correlation of two numeric columns.
    ///
    /// Args:
    ///     col1: First column name.
    ///     col2: Second column name.
    ///
    /// Returns:
    ///     float: Correlation in [-1, 1]. Nulls are excluded.
    ///
    /// Raises:
    ///     RuntimeError: If columns are missing or not numeric.
    fn corr(&self, col1: &str, col2: &str) -> PyResult<f64> {
        self.df
            .stat()
            .corr(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Return a DataFrame with the correlation matrix of all numeric columns.
    ///
    /// Returns:
    ///     DataFrame: Rows and columns are numeric column names; values are Pearson correlations.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn corr_matrix(&self) -> PyResult<PyDataFrame> {
        let df = self
            .df
            .stat()
            .corr_matrix()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for DataFrame.na() (fill, drop).
#[pyclass(name = "DataFrameNa")]
pub struct PyDataFrameNa {
    df: DataFrame,
}

#[pymethods]
impl PyDataFrameNa {
    /// Replace null values with the result of the given expression.
    ///
    /// Args:
    ///     value: Column expression (e.g. ``lit(0)`` or ``col("other")``). Evaluated per row.
    ///
    /// Returns:
    ///     DataFrame (lazy) with nulls filled.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn fill(&self, value: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .df
            .na()
            .fill(value.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Drop rows that contain null in any column or, if subset is given, in any of those columns.
    ///
    /// Args:
    ///     subset: Optional list of column names. If provided, drop only when one of these is null.
    ///
    /// Returns:
    ///     DataFrame (lazy) with null rows removed.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    #[pyo3(signature = (subset=None))]
    fn drop(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .df
            .na()
            .drop(sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for GroupedData.
#[pyclass(name = "GroupedData")]
pub struct PyGroupedData {
    inner: GroupedData,
}

#[pymethods]
impl PyGroupedData {
    /// Return a DataFrame with one row per group and a count column (number of rows in each group).
    ///
    /// Returns:
    ///     DataFrame (lazy) with grouping columns and count.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn count(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .count()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with one row per group and the sum of the given column.
    ///
    /// Args:
    ///     column: Numeric column name to sum.
    ///
    /// Returns:
    ///     DataFrame (lazy). Raises RuntimeError if column is missing or not numeric.
    fn sum(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sum(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with one row per group and the mean of the given column.
    ///
    /// Args:
    ///     column: Numeric column name. Nulls excluded from mean.
    ///
    /// Returns:
    ///     DataFrame (lazy). Raises RuntimeError if column is missing or not numeric.
    fn avg(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .avg(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with one row per group and the minimum of the given column.
    ///
    /// Args:
    ///     column: Column name (numeric or comparable).
    ///
    /// Returns:
    ///     DataFrame (lazy). Raises RuntimeError if column is missing.
    #[pyo3(name = "min")]
    fn min_(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .min(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with one row per group and the maximum of the given column.
    ///
    /// Args:
    ///     column: Column name (numeric or comparable).
    ///
    /// Returns:
    ///     DataFrame (lazy). Raises RuntimeError if column is missing.
    fn max(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .max(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Aggregate groups with one or more expressions (e.g. sum(col("x")), avg(col("y"))).
    ///
    /// Args:
    ///     exprs: List of aggregation Column expressions (from ``sum()``, ``avg()``, ``count()``, ``min()``, ``max()``, etc.).
    ///
    /// Returns:
    ///     DataFrame (lazy) with one row per group and the aggregated columns.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn agg(&self, exprs: Vec<PyRef<PyColumn>>) -> PyResult<PyDataFrame> {
        let aggregations: Vec<Expr> = exprs.iter().map(|c| c.inner.expr().clone()).collect();
        let df = self
            .inner
            .agg(aggregations)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn any_value(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .any_value(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn bool_and(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .bool_and(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn bool_or(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .bool_or(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn product(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .product(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn collect_list(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .collect_list(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn collect_set(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .collect_set(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn count_if(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .count_if(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn percentile(&self, column: &str, p: f64) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .percentile(column, p)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn max_by(&self, value_column: &str, ord_column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .max_by(value_column, ord_column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn min_by(&self, value_column: &str, ord_column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .min_by(value_column, ord_column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn covar_pop(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .covar_pop(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn covar_samp(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .covar_samp(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn corr(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .corr(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_count(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_count(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_avgx(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_avgx(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_avgy(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_avgy(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_slope(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_slope(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_intercept(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_intercept(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_r2(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_r2(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_sxx(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_sxx(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_syy(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_syy(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_sxy(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_sxy(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn kurtosis(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .kurtosis(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn skewness(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .skewness(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for CubeRollupData (cube/rollup then agg).
#[pyclass(name = "CubeRollupData")]
pub struct PyCubeRollupData {
    inner: CubeRollupData,
}

#[pymethods]
impl PyCubeRollupData {
    /// Aggregate the CUBE/ROLLUP result with one or more expressions.
    ///
    /// Args:
    ///     exprs: List of aggregation Column expressions (e.g. sum(col("x"))).
    ///
    /// Returns:
    ///     DataFrame (lazy) with grouping dimensions and aggregated columns.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn agg(&self, exprs: Vec<PyRef<PyColumn>>) -> PyResult<PyDataFrame> {
        let aggregations: Vec<Expr> = exprs.iter().map(|c| c.inner.expr().clone()).collect();
        let df = self
            .inner
            .agg(aggregations)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for DataFrameWriter (write().mode().format().save()).
#[pyclass(name = "DataFrameWriter")]
pub struct PyDataFrameWriter {
    df: crate::DataFrame,
    mode: RwLock<WriteMode>,
    format: RwLock<WriteFormat>,
    options: RwLock<std::collections::HashMap<String, String>>,
    partition_by: RwLock<Vec<String>>,
}

impl PyDataFrameWriter {
    fn build_writer(&self) -> crate::dataframe::DataFrameWriter<'_> {
        let mode = self
            .mode
            .read()
            .ok()
            .map(|g| *g)
            .unwrap_or(WriteMode::Overwrite);
        let format = self
            .format
            .read()
            .ok()
            .map(|g| *g)
            .unwrap_or(WriteFormat::Parquet);
        let mut w = self.df.write().mode(mode).format(format);
        if let Ok(opts) = self.options.read() {
            for (k, v) in opts.iter() {
                w = w.option(k.clone(), v.clone());
            }
        }
        if let Ok(cols) = self.partition_by.read() {
            if !cols.is_empty() {
                w = w.partition_by(cols.clone());
            }
        }
        w
    }
}

#[pymethods]
impl PyDataFrameWriter {
    /// Set the write mode for ``save()``.
    ///
    /// Args:
    ///     mode: "append" to add to existing data, or "overwrite" (default) to replace.
    ///
    /// Returns:
    ///     Self for chaining.
    fn mode<'py>(slf: PyRef<'py, Self>, mode: &str) -> PyResult<PyRef<'py, Self>> {
        {
            let mut guard = slf
                .mode
                .try_write()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            *guard = match mode.to_lowercase().as_str() {
                "append" => WriteMode::Append,
                _ => WriteMode::Overwrite,
            };
        }
        Ok(slf)
    }

    /// Set the output file format for ``save()``.
    ///
    /// Args:
    ///     format: "parquet" (default), "csv", or "json".
    ///
    /// Returns:
    ///     Self for chaining.
    fn format<'py>(slf: PyRef<'py, Self>, format: &str) -> PyResult<PyRef<'py, Self>> {
        {
            let mut guard = slf
                .format
                .try_write()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            *guard = match format.to_lowercase().as_str() {
                "csv" => WriteFormat::Csv,
                "json" => WriteFormat::Json,
                _ => WriteFormat::Parquet,
            };
        }
        Ok(slf)
    }

    /// Add an option (PySpark: option(key, value)). Returns self for chaining.
    fn option<'py>(slf: PyRef<'py, Self>, key: &str, value: &str) -> PyResult<PyRef<'py, Self>> {
        if let Ok(mut opts) = slf.options.try_write() {
            opts.insert(key.to_string(), value.to_string());
        }
        Ok(slf)
    }

    /// Add options from a dict (PySpark: options(**kwargs)). Returns self for chaining.
    fn options<'py>(
        slf: PyRef<'py, Self>,
        _py: Python<'_>,
        opts: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyRef<'py, Self>> {
        let dict = opts.downcast::<pyo3::types::PyDict>()?;
        if let Ok(mut guard) = slf.options.try_write() {
            for (k, v) in dict.iter() {
                let k_str: String = k.extract()?;
                let v_str: String = v.extract()?;
                guard.insert(k_str, v_str);
            }
        }
        Ok(slf)
    }

    /// Partition output by the given columns (PySpark: partitionBy(*cols)). Returns self for chaining.
    #[pyo3(signature = (*cols))]
    fn partition_by<'py>(slf: PyRef<'py, Self>, cols: Vec<String>) -> PyResult<PyRef<'py, Self>> {
        if let Ok(mut guard) = slf.partition_by.try_write() {
            *guard = cols;
        }
        Ok(slf)
    }

    /// Write as Parquet (PySpark: parquet(path)).
    fn parquet(&self, path: &str) -> PyResult<()> {
        self.build_writer()
            .parquet(Path::new(path))
            .map_err(|e: polars::prelude::PolarsError| {
                pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
            })
    }

    /// Write as CSV (PySpark: csv(path)).
    fn csv(&self, path: &str) -> PyResult<()> {
        self.build_writer()
            .csv(Path::new(path))
            .map_err(|e: polars::prelude::PolarsError| {
                pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
            })
    }

    /// Write as JSON lines (PySpark: json(path)).
    fn json(&self, path: &str) -> PyResult<()> {
        self.build_writer()
            .json(Path::new(path))
            .map_err(|e: polars::prelude::PolarsError| {
                pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
            })
    }

    /// Write the DataFrame to the given path using the current mode and format.
    ///
    /// Args:
    ///     path: Local file or directory path. For Parquet/Delta, typically a directory.
    ///
    /// Raises:
    ///     RuntimeError: If write fails (e.g. permission, disk, or format error).
    fn save(&self, path: &str) -> PyResult<()> {
        self.build_writer()
            .save(Path::new(path))
            .map_err(|e: polars::prelude::PolarsError| {
                pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
            })
    }
}

/// Convert Polars AnyValue to a Python object.
pub(crate) fn any_value_to_py(
    py: Python<'_>,
    av: polars::prelude::AnyValue<'_>,
) -> PyResult<PyObject> {
    use polars::prelude::{AnyValue, TimeUnit};
    use pyo3::conversion::IntoPyObjectExt;
    match av {
        AnyValue::Null => py.None().into_bound_py_any(py).map(Into::into),
        AnyValue::Boolean(b) => b.into_bound_py_any(py).map(Into::into),
        AnyValue::Int8(i) => (i as i64).into_bound_py_any(py).map(Into::into),
        AnyValue::Int32(i) => i.into_bound_py_any(py).map(Into::into),
        AnyValue::Int64(i) => i.into_bound_py_any(py).map(Into::into),
        AnyValue::UInt32(u) => u.into_bound_py_any(py).map(Into::into),
        AnyValue::UInt64(u) => u.into_bound_py_any(py).map(Into::into),
        AnyValue::Float32(f) => f.into_bound_py_any(py).map(Into::into),
        AnyValue::Float64(f) => f.into_bound_py_any(py).map(Into::into),
        AnyValue::String(s) => s.to_string().into_bound_py_any(py).map(Into::into),
        AnyValue::StringOwned(s) => s.to_string().into_bound_py_any(py).map(Into::into),
        AnyValue::Date(days) => {
            let epoch = crate::date_utils::epoch_naive_date();
            let d = epoch + chrono::TimeDelta::days(days as i64);
            d.format("%Y-%m-%d")
                .to_string()
                .into_bound_py_any(py)
                .map(Into::into)
        }
        AnyValue::Datetime(us, tu, _) | AnyValue::DatetimeOwned(us, tu, _) => {
            let micros = match tu {
                TimeUnit::Microseconds => us,
                TimeUnit::Milliseconds => us.saturating_mul(1000),
                TimeUnit::Nanoseconds => us.saturating_div(1000),
            };
            let dt = chrono::DateTime::from_timestamp_micros(micros).unwrap_or_default();
            let s = dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
            s.into_bound_py_any(py).map(Into::into)
        }
        AnyValue::List(s) => {
            let py_list = pyo3::types::PyList::empty(py);
            for i in 0..s.len() {
                let av = s
                    .get(i)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                let py_val = any_value_to_py(py, av)?;
                py_list.append(py_val)?;
            }
            Ok(py_list.into())
        }
        other => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "unsupported type for collect: {:?}",
            other
        ))),
    }
}
