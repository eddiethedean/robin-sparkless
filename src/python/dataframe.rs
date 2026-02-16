//! Python DataFrame, GroupedData, DataFrameStat, DataFrameNa, DataFrameWriter (PySpark sql).
#![allow(clippy::needless_return)] // many branches return Ok(...); clippy prefers trailing expr

use crate::dataframe::JoinType;
use crate::dataframe::{CubeRollupData, SaveMode, WriteFormat, WriteMode};
use crate::functions::{asc_nulls_last, desc_nulls_last, SortOrder};
#[cfg(feature = "pyo3")]
use crate::python::udf::{execute_grouped_vectorized_aggs, GroupedAggSpec};
use crate::{DataFrame, GroupedData, PivotedGroupedData};
use polars::prelude::{col, lit, Expr, NULL};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyTuple};
use std::path::Path;
use std::sync::RwLock;

use crate::python::py_to_json_value;
use serde_json::Value as JsonValue;

use super::column::PyColumn;
use super::order::PySortOrder;
use super::session::get_default_session;

/// Normalize cube/rollup *cols: one list -> use it; else each arg as column name.
fn normalize_cube_rollup_cols(cols: &Bound<'_, PyTuple>) -> PyResult<Vec<String>> {
    if cols.len() == 1 {
        let first = cols.get_item(0)?;
        if let Ok(list) = first.downcast::<PyList>() {
            return list
                .iter()
                .map(|ob| ob.extract::<String>())
                .collect::<PyResult<Vec<_>>>();
        }
    }
    (0..cols.len())
        .map(|i| cols.get_item(i).and_then(|ob| ob.extract::<String>()))
        .collect::<PyResult<Vec<_>>>()
}

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

/// Value for na.fill: Column expression or scalar (int, float, bool, str). PySpark parity.
fn py_fill_value_to_expr(value: &pyo3::Bound<'_, pyo3::types::PyAny>) -> PyResult<Expr> {
    if let Ok(pc) = value.downcast::<PyColumn>() {
        return Ok(pc.borrow().inner.expr().clone());
    }
    py_any_to_expr(value)
}

/// Extract inner DataFrame from a PyDataFrame or DataFrame-like (e.g. has .inner or ._df). For union/unionByName.
fn py_any_to_dataframe(other: &Bound<'_, PyAny>) -> PyResult<DataFrame> {
    if let Ok(py_df) = other.downcast::<PyDataFrame>() {
        return Ok(py_df.borrow().inner.clone());
    }
    if let Ok(inner) = other.getattr("inner") {
        if let Ok(py_df) = inner.downcast::<PyDataFrame>() {
            return Ok(py_df.borrow().inner.clone());
        }
    }
    if let Ok(inner) = other.getattr("_df") {
        if let Ok(py_df) = inner.downcast::<PyDataFrame>() {
            return Ok(py_df.borrow().inner.clone());
        }
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "union/unionAll/unionByName: other must be a DataFrame or object with .inner or ._df (DataFrame-like)",
    ))
}

/// Normalize subset argument: None, list of str, or single str -> Option<Vec<String>> (fixes #406).
fn py_subset_to_option_vec(
    subset: Option<&Bound<'_, pyo3::types::PyAny>>,
) -> PyResult<Option<Vec<String>>> {
    let Some(obj) = subset else {
        return Ok(None);
    };
    if obj.is_none() {
        return Ok(None);
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(item.extract::<String>()?);
        }
        return Ok(Some(out));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Some(vec![s]));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "subset must be None, a list of column names (str), or a single column name (str)",
    ))
}

/// Extract one or more Column expressions from a single Column, list, or tuple (for agg(*exprs)).
fn python_exprs_to_columns(exprs: &Bound<'_, PyAny>, _py: Python<'_>) -> PyResult<Vec<Expr>> {
    if let Ok(py_col) = exprs.downcast::<PyColumn>() {
        return Ok(vec![py_col.borrow().inner.expr().clone()]);
    }
    if let Ok(list) = exprs.downcast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            let py_col = item.downcast::<PyColumn>()?;
            out.push(py_col.borrow().inner.expr().clone());
        }
        return Ok(out);
    }
    if let Ok(tup) = exprs.downcast::<PyTuple>() {
        let mut out = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            let py_col = item.downcast::<PyColumn>()?;
            out.push(py_col.borrow().inner.expr().clone());
        }
        return Ok(out);
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "agg() requires a Column or a list/tuple of Columns",
    ))
}

/// Convert group_by cols argument (str, Column, or list of str/Column) to (exprs, names) for group_by_exprs.
fn py_group_by_cols_to_exprs_and_names(
    cols: &Bound<'_, PyAny>,
) -> PyResult<(Vec<Expr>, Vec<String>)> {
    // Single str
    if let Ok(s) = cols.extract::<String>() {
        return Ok((vec![col(s.as_str())], vec![s]));
    }
    // Single Column
    if let Ok(py_col) = cols.downcast::<PyColumn>() {
        let c = py_col.borrow();
        return Ok((
            vec![c.inner.expr().clone()],
            vec![c.inner.name().to_string()],
        ));
    }
    // List or tuple
    if let Ok(list) = cols.downcast::<PyList>() {
        let mut exprs = Vec::with_capacity(list.len());
        let mut names = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(s) = item.extract::<String>() {
                exprs.push(col(s.as_str()));
                names.push(s);
            } else if let Ok(py_col) = item.downcast::<PyColumn>() {
                let c = py_col.borrow();
                exprs.push(c.inner.expr().clone());
                names.push(c.inner.name().to_string());
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "group_by() cols must be str (column name) or Column (e.g. col(\"x\")); each list item must be str or Column",
                ));
            }
        }
        return Ok((exprs, names));
    }
    if let Ok(tup) = cols.downcast::<PyTuple>() {
        let mut exprs = Vec::with_capacity(tup.len());
        let mut names = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            if let Ok(s) = item.extract::<String>() {
                exprs.push(col(s.as_str()));
                names.push(s);
            } else if let Ok(py_col) = item.downcast::<PyColumn>() {
                let c = py_col.borrow();
                exprs.push(c.inner.expr().clone());
                names.push(c.inner.name().to_string());
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "group_by() cols must be str (column name) or Column (e.g. col(\"x\")); each tuple item must be str or Column",
                ));
            }
        }
        return Ok((exprs, names));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "group_by() cols must be a column name (str), a Column (e.g. col(\"x\")), or a list/tuple of str or Column",
    ))
}

/// Convert a single Python arg (str or Column) to an aggregation Expr with alias (for GroupedData.sum/min/max).
fn py_col_or_name_to_agg_expr<F, G>(
    column: &Bound<'_, PyAny>,
    agg_name: &str,
    expr_agg: F,
    name_agg: G,
) -> PyResult<Expr>
where
    F: FnOnce(Expr) -> Expr,
    G: FnOnce(&str) -> Expr,
{
    use polars::prelude::*;
    if let Ok(name) = column.extract::<String>() {
        Ok(name_agg(name.as_str()).alias(format!("{agg_name}({name})")))
    } else if let Ok(pycol) = column.downcast::<PyColumn>() {
        let c = pycol.borrow();
        let name = c.inner.name().to_string();
        Ok(expr_agg(c.inner.expr().clone()).alias(format!("{agg_name}({name})")))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "{}() requires column name (str) or Column expression (e.g. col(\"x\"))",
            agg_name
        )))
    }
}

/// Join `on` parameter: accept str, Column, or list/tuple of str/Column (PySpark compatibility, #175, #353).
struct JoinOn(Vec<String>);

fn py_join_on_to_column_names(ob: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = ob.extract::<String>() {
        return Ok(vec![s]);
    }
    if let Ok(py_col) = ob.downcast::<PyColumn>() {
        return Ok(vec![py_col.borrow().inner.name().to_string()]);
    }
    if let Ok(list) = ob.downcast::<PyList>() {
        let mut v = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(s) = item.extract::<String>() {
                v.push(s);
            } else if let Ok(py_col) = item.downcast::<PyColumn>() {
                v.push(py_col.borrow().inner.name().to_string());
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "join 'on' list items must be str (column name) or Column (e.g. col(\"id\"))",
                ));
            }
        }
        return Ok(v);
    }
    if let Ok(tup) = ob.downcast::<PyTuple>() {
        let mut v = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            if let Ok(s) = item.extract::<String>() {
                v.push(s);
            } else if let Ok(py_col) = item.downcast::<PyColumn>() {
                v.push(py_col.borrow().inner.name().to_string());
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "join 'on' tuple items must be str (column name) or Column (e.g. col(\"id\"))",
                ));
            }
        }
        return Ok(v);
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "join 'on' must be str (column name), Column (e.g. col(\"id\")), or list/tuple of str or Column",
    ))
}

impl FromPyObject<'_> for JoinOn {
    fn extract_bound(ob: &Bound<'_, PyAny>) -> PyResult<Self> {
        py_join_on_to_column_names(ob).map(JoinOn)
    }
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

    /// Register this DataFrame as an in-memory table by name (saved-tables namespace). Readable via ``read_delta(name)`` or ``spark.table(name)``.
    #[cfg(feature = "sql")]
    fn write_delta_table(&self, name: &str, _py: Python<'_>) -> PyResult<()> {
        let session = get_default_session().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "write_delta_table: no default session. Call SparkSession.builder().get_or_create() first.",
            )
        })?;
        session.register_table(name, self.inner.clone());
        Ok(())
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
                    pyo3::exceptions::PyIndexError::new_err("column index out of range")
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
        return Ok(PyDataFrame { inner: df });
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
            #[allow(clippy::map_clone)]
            // Bound<'_, PyAny> from tuple iterator; clone needed for Vec
            cols.iter().map(|o| o.clone()).collect()
        };
        let mut exprs: Vec<Expr> = Vec::with_capacity(items.len());
        for item in items.iter() {
            // Tuple of two Columns: posexplode(...).alias("pos", "val") (fixes #411).
            if let Ok(tup) = item.downcast::<PyTuple>() {
                if tup.len() == 2 {
                    if let (Ok(a), Ok(b)) = (
                        tup.get_item(0)?.extract::<PyRef<PyColumn>>(),
                        tup.get_item(1)?.extract::<PyRef<PyColumn>>(),
                    ) {
                        exprs.push(a.inner.expr().clone());
                        exprs.push(b.inner.expr().clone());
                        continue;
                    }
                }
            }
            // Try Column before str: PySpark Column objects are often convertible to str (e.g. "(2 + x)"),
            // so we must treat as expression first to support select(col("a") * 2, lit(3) + col("x")).
            if let Ok(py_col) = item.extract::<PyRef<PyColumn>>() {
                exprs.push(py_col.inner.expr().clone());
            } else if let Ok(name) = item.extract::<std::string::String>() {
                if name == "*" {
                    // Expand "*" to all columns (fixes #404).
                    let all_names = self
                        .inner
                        .columns()
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    for c in all_names {
                        exprs.push(col(c.as_str()));
                    }
                } else {
                    let resolved = self
                        .inner
                        .resolve_column_name(&name)
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    exprs.push(col(resolved.as_str()));
                }
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "select() items must be str (column name), Column (expression), or (Column, Column) from posexplode(...).alias(...)",
                ));
            }
        }
        let df = self
            .inner
            .select_exprs(exprs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
    }

    /// Sort by column names or by SortOrder expressions (PySpark orderBy parity).
    ///
    /// Args:
    ///     cols: Either a list of column names (str), a single SortOrder (e.g. ``col("x").desc_nulls_last()``),
    ///         or a list of SortOrder. When column names are used, ``ascending`` applies.
    ///     ascending: Optional bool or list of bools. Single bool applies to all columns (PySpark parity).
    ///         True = ascending, False = descending. If omitted, all columns are sorted ascending.
    ///
    /// Returns:
    ///     DataFrame (lazy) with rows sorted.
    ///
    /// Raises:
    ///     RuntimeError: If a column is not in the schema or sort cannot be evaluated.
    #[pyo3(signature = (cols, ascending=None))]
    fn order_by(
        &self,
        cols: &Bound<'_, PyAny>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        /// Resolve ascending: None -> all true; single bool -> replicate to len; list of bool -> use as-is.
        fn resolve_ascending(asc: Option<&Bound<'_, PyAny>>, len: usize) -> PyResult<Vec<bool>> {
            let Some(asc) = asc else {
                return Ok(vec![true; len]);
            };
            if let Ok(b) = asc.extract::<bool>() {
                return Ok(vec![b; len]);
            }
            if let Ok(list) = asc.downcast::<PyList>() {
                let v: Vec<bool> = list.extract().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(
                        "order_by ascending must be a bool or a list of bools",
                    )
                })?;
                if v.len() != len {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "order_by ascending list length ({}) must match number of columns ({})",
                        v.len(),
                        len
                    )));
                }
                return Ok(v);
            }
            Err(pyo3::exceptions::PyTypeError::new_err(
                "order_by ascending must be a bool or a list of bools",
            ))
        }

        // Single Column: ascending bool or default true (PySpark orderBy(col("x"), ascending=False))
        if let Ok(py_col) = cols.downcast::<PyColumn>() {
            let asc = resolve_ascending(ascending, 1)?[0];
            let order = if asc {
                py_col.borrow().inner.asc_nulls_last()
            } else {
                py_col.borrow().inner.desc_nulls_last()
            };
            let df = self
                .inner
                .order_by_exprs(vec![order])
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            return Ok(PyDataFrame { inner: df });
        }
        // Single PySortOrder: ascending is ignored
        if let Ok(sort_order) = cols.downcast::<PySortOrder>() {
            let orders = vec![sort_order.borrow().inner.clone()];
            let df = self
                .inner
                .order_by_exprs(orders)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            return Ok(PyDataFrame { inner: df });
        }
        // List: either [SortOrder, ...] or [str, ...] or [Column, ...]
        if let Ok(py_list) = cols.downcast::<PyList>() {
            if py_list.is_empty() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "order_by requires at least one column or sort order",
                ));
            }
            // Try list of PySortOrder first
            let mut orders = Vec::with_capacity(py_list.len());
            for item in py_list.iter() {
                if let Ok(po) = item.downcast::<PySortOrder>() {
                    orders.push(po.borrow().inner.clone());
                } else {
                    orders.clear();
                    break;
                }
            }
            if !orders.is_empty() {
                let df = self
                    .inner
                    .order_by_exprs(orders)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                return Ok(PyDataFrame { inner: df });
            }
            // List of Column: apply ascending (single bool or list of bools)
            let mut col_orders = Vec::with_capacity(py_list.len());
            for item in py_list.iter() {
                if let Ok(py_col) = item.downcast::<PyColumn>() {
                    col_orders.push(py_col.borrow().inner.clone());
                } else {
                    col_orders.clear();
                    break;
                }
            }
            if !col_orders.is_empty() {
                let asc = resolve_ascending(ascending, col_orders.len())?;
                let orders: Vec<_> = col_orders
                    .into_iter()
                    .zip(asc)
                    .map(|(c, a)| {
                        if a {
                            asc_nulls_last(&c)
                        } else {
                            desc_nulls_last(&c)
                        }
                    })
                    .collect();
                let df = self
                    .inner
                    .order_by_exprs(orders)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                return Ok(PyDataFrame { inner: df });
            }
            // List of column names
            let col_names: Vec<String> = py_list.extract().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err(
                    "order_by cols must be column names (str), Column(s), or SortOrder(s); each list item must be str, Column, or SortOrder",
                )
            })?;
            let asc = resolve_ascending(ascending, col_names.len())?;
            let df = self
                .inner
                .order_by(
                    col_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    asc,
                )
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            return Ok(PyDataFrame { inner: df });
        }
        Err(pyo3::exceptions::PyTypeError::new_err(
            "order_by cols must be a column name (str), Column (e.g. col(\"x\")), SortOrder (e.g. col(\"x\").desc_nulls_last()), or a list of str/Column/SortOrder",
        ))
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
        return Ok(PyDataFrame { inner: df });
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
    ///     TypeError: If an item is not str or Column.
    ///     RuntimeError: If a column name is not in the schema.
    fn group_by(&self, cols: &Bound<'_, PyAny>) -> PyResult<PyGroupedData> {
        let (exprs, names) = py_group_by_cols_to_exprs_and_names(cols)?;
        let gd = self
            .inner
            .group_by_exprs(exprs, names)
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
    #[pyo3(signature = (*cols))]
    fn cube(&self, cols: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<PyCubeRollupData> {
        let names = normalize_cube_rollup_cols(cols)?;
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let cr = self
            .inner
            .cube(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyCubeRollupData { inner: cr })
    }

    /// Create a ROLLUP grouping: aggregates at each level of the hierarchy (e.g. (a,b), (a), ()).
    ///
    /// Args:
    ///     cols: Column names for the ROLLUP hierarchy (variadic or single list), in order.
    ///
    /// Returns:
    ///     CubeRollupData: Call ``.agg(...)`` to compute aggregates.
    ///
    /// Raises:
    ///     RuntimeError: If a column is not in the schema.
    #[pyo3(signature = (*cols))]
    fn rollup(&self, cols: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<PyCubeRollupData> {
        let names = normalize_cube_rollup_cols(cols)?;
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let cr = self
            .inner
            .rollup(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyCubeRollupData { inner: cr })
    }

    /// Global aggregation (no groupBy): apply one or more aggregate expressions over the whole
    /// DataFrame, returning a single-row DataFrame (PySpark: df.agg(F.sum("x"), F.avg("y"))).
    ///
    /// Args:
    ///     *exprs: One or more Column expressions (e.g. df.agg(sum(col("x")), avg(col("y")))),
    ///         or a single list/tuple of Columns (e.g. df.agg([sum(col("x")), avg(col("y"))])).
    ///
    /// Returns:
    ///     DataFrame with one row and one column per expression.
    #[pyo3(signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>, py: Python<'_>) -> PyResult<PyDataFrame> {
        let arg: Bound<'_, PyAny> = if exprs.len() == 1 {
            exprs.get_item(0)?
        } else {
            exprs.clone().into_any()
        };
        let aggregations = python_exprs_to_columns(&arg, py)?;
        let df = self
            .inner
            .agg(aggregations)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    /// Return a DataFrameWriter to save the DataFrame (chain ``.mode()``, ``.format()``, then ``.save(path)``).
    ///
    /// Stub: writeTo (DataFrameWriterV2 / catalog tables) not supported.
    /// Use df.write().parquet(path) or df.write().csv(path) instead.
    #[pyo3(name = "writeTo")]
    fn write_to(&self) -> PyResult<PyObject> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "writeTo (catalog tables) is not supported; use df.write.format('parquet').save(path) or df.write.parquet(path)",
        ))
    }

    /// Returns:
    ///     DataFrameWriter: Default mode "overwrite", format "parquet". Use as property: df.write.format(...).save(...) (fixes #374).
    #[getter]
    fn write(&self) -> PyDataFrameWriter {
        PyDataFrameWriter {
            df: self.inner.clone(),
            mode: RwLock::new(WriteMode::Overwrite),
            format: RwLock::new(WriteFormat::Parquet),
            options: RwLock::new(std::collections::HashMap::new()),
            partition_by: RwLock::new(Vec::new()),
        }
    }

    /// Return a DataFrame with the given alias (PySpark: df.alias("t")).
    /// Used for subquery/join naming.
    fn alias(&self, name: &str) -> PyDataFrame {
        PyDataFrame {
            inner: self.inner.alias(name),
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

    #[cfg(not(feature = "sql"))]
    #[pyo3(name = "createOrReplaceTempView")]
    fn create_or_replace_temp_view(&self, _name: &str, _py: Python<'_>) -> PyResult<()> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "createOrReplaceTempView() requires the 'sql' feature. Build with: maturin develop --features 'pyo3,sql'.",
        ))
    }

    /// Register this DataFrame as a temp view (alias for create_or_replace_temp_view).
    #[cfg(feature = "sql")]
    #[pyo3(name = "createTempView")]
    fn create_temp_view(&self, name: &str, py: Python<'_>) -> PyResult<()> {
        self.create_or_replace_temp_view(name, py)
    }

    #[cfg(not(feature = "sql"))]
    #[pyo3(name = "createTempView")]
    fn create_temp_view(&self, _name: &str, _py: Python<'_>) -> PyResult<()> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "createTempView() requires the 'sql' feature. Build with: maturin develop --features 'pyo3,sql'.",
        ))
    }

    /// Register this DataFrame as a global temp view (PySpark: createGlobalTempView). Persists across sessions.
    #[cfg(feature = "sql")]
    #[pyo3(name = "createGlobalTempView")]
    fn create_global_temp_view(&self, name: &str, _py: Python<'_>) -> PyResult<()> {
        let session = crate::python::session::get_default_session().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "createGlobalTempView: no default session. Call SparkSession.builder().get_or_create() first.",
            )
        })?;
        session.create_global_temp_view(name, self.inner.clone());
        Ok(())
    }

    #[cfg(not(feature = "sql"))]
    #[pyo3(name = "createGlobalTempView")]
    fn create_global_temp_view(&self, _name: &str, _py: Python<'_>) -> PyResult<()> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "createGlobalTempView() requires the 'sql' feature. Build with: maturin develop --features 'pyo3,sql'.",
        ))
    }

    /// Register this DataFrame as a global temp view (PySpark: createOrReplaceGlobalTempView). Persists across sessions.
    #[cfg(feature = "sql")]
    #[pyo3(name = "createOrReplaceGlobalTempView")]
    fn create_or_replace_global_temp_view(&self, name: &str, py: Python<'_>) -> PyResult<()> {
        let _ = py;
        let session = crate::python::session::get_default_session().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "createOrReplaceGlobalTempView: no default session. Call SparkSession.builder().get_or_create() first.",
            )
        })?;
        session.create_or_replace_global_temp_view(name, self.inner.clone());
        Ok(())
    }

    #[cfg(not(feature = "sql"))]
    #[pyo3(name = "createOrReplaceGlobalTempView")]
    fn create_or_replace_global_temp_view(&self, _name: &str, _py: Python<'_>) -> PyResult<()> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "createOrReplaceGlobalTempView() requires the 'sql' feature. Build with: maturin develop --features 'pyo3,sql'.",
        ))
    }

    /// Join with another DataFrame on one or more column names.
    ///
    /// Args:
    ///     other: Right DataFrame.
    ///     on: Column name (str) or list/tuple of column names. Must exist in both DataFrames with compatible types. PySpark compatibility: single column can be passed as ``on="id"``.
    ///     how: Join type: "inner", "left", "right", or "outer". Default "inner".
    ///
    /// Returns:
    ///     DataFrame (lazy) with joined rows. Join columns appear once.
    ///
    /// Raises:
    ///     ValueError: If ``how`` is not one of the allowed values.
    ///     RuntimeError: If join execution fails.
    #[pyo3(signature = (other, on, how="inner"))]
    fn join(&self, other: &PyDataFrame, on: JoinOn, how: &str) -> PyResult<PyDataFrame> {
        let on = on.0;
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
        return Ok(PyDataFrame { inner: df });
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
    ///     TypeError: If other is not a DataFrame or DataFrame-like (has ``.inner`` or ``._df``).
    fn union(&self, other: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let other_df = py_any_to_dataframe(other)?;
        let df = self
            .inner
            .union(&other_df)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Alias for union (PySpark unionAll).
    #[pyo3(name = "unionAll")]
    fn union_all(&self, other: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let other_df = py_any_to_dataframe(other)?;
        let df = self
            .inner
            .union_all(&other_df)
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
    ///     TypeError: If other is not a DataFrame or DataFrame-like (has ``.inner`` or ``._df``).
    #[pyo3(signature = (other, allow_missing_columns=true))]
    fn union_by_name(
        &self,
        other: &Bound<'_, PyAny>,
        allow_missing_columns: bool,
    ) -> PyResult<PyDataFrame> {
        let other_df = py_any_to_dataframe(other)?;
        let df = self
            .inner
            .union_by_name(&other_df, allow_missing_columns)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Set intersection: rows that appear in both this and other (by all columns). PySpark intersect.
    ///
    /// Args:
    ///     other: DataFrame or DataFrame-like (has ``.inner`` or ``._df``).
    ///
    /// Returns:
    ///     DataFrame with distinct rows that appear in both.
    fn intersect(&self, other: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let other_df = py_any_to_dataframe(other)?;
        let df = self
            .inner
            .intersect(&other_df)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Set difference keeping duplicates: rows in this DataFrame minus matching rows in other. PySpark exceptAll.
    ///
    /// Args:
    ///     other: DataFrame or DataFrame-like (has ``.inner`` or ``._df``).
    ///
    /// Returns:
    ///     DataFrame (duplicates preserved per PySpark exceptAll semantics).
    #[pyo3(name = "exceptAll")]
    fn except_all(&self, other: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let other_df = py_any_to_dataframe(other)?;
        let df = self
            .inner
            .except_all(&other_df)
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
        return Ok(PyDataFrame { inner: df });
    }

    /// Drop duplicate rows (PySpark drop_duplicates / dropDuplicates). Same as distinct(subset=...).
    ///
    /// Args:
    ///     subset: If provided, only these columns determine uniqueness; otherwise all columns.
    ///
    /// Returns:
    ///     DataFrame with one row per distinct key (or per distinct subset when subset is given).
    #[pyo3(signature = (subset=None))]
    fn drop_duplicates(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        self.distinct(subset)
    }

    /// PySpark alias for drop_duplicates.
    #[pyo3(name = "dropDuplicates", signature = (subset=None))]
    fn drop_duplicates_camel(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        self.distinct(subset)
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
        return Ok(PyDataFrame { inner: df });
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
    #[pyo3(signature = (subset=None, how="any", thresh=None))]
    fn dropna(
        &self,
        subset: Option<Vec<String>>,
        how: &str,
        thresh: Option<usize>,
    ) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .inner
            .dropna(sub, how, thresh)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    /// Replace null values. Value can be a scalar (int, float, bool, str) or a Column expression.
    ///
    /// Args:
    ///     value: Scalar or Column (e.g. ``lit(0)`` or ``0``). If subset is given, only those columns are filled.
    ///     subset: Optional list of column names. If None, all columns are filled.
    ///
    /// Returns:
    ///     DataFrame (lazy) with nulls filled.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    #[pyo3(signature = (value, subset=None))]
    fn fillna(
        &self,
        value: &Bound<'_, PyAny>,
        subset: Option<Vec<String>>,
    ) -> PyResult<PyDataFrame> {
        let expr = py_fill_value_to_expr(value)?;
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .inner
            .fillna(expr, sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
    }

    /// Return the first n rows (PySpark parity: head() defaults to 1 row).
    ///
    /// Args:
    ///     n: Number of rows (default 1). Non-negative integer.
    ///
    /// Returns:
    ///     DataFrame (lazy) with at most n rows.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    #[pyo3(signature = (n=1))]
    fn head(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .head(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
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
    /// Args:
    ///     mode: Optional; PySpark accepts "simple", "extended", "codegen", "cost", "formatted".
    ///           Ignored for now (eager backend); present for API parity.
    ///
    /// Returns:
    ///     str: Plan description. Does not trigger full execution.
    #[pyo3(signature = (mode=None))]
    fn explain(&self, mode: Option<&str>) -> PyResult<String> {
        let _ = mode;
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
        return Ok(PyDataFrame { inner: df });
    }

    /// Local checkpoint (same process).
    fn local_checkpoint(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .local_checkpoint()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    /// Repartition into num_partitions (logical; may not change physical layout in Sparkless).
    fn repartition(&self, num_partitions: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .repartition(num_partitions)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    /// Coalesce to fewer partitions.
    fn coalesce(&self, num_partitions: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .coalesce(num_partitions)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    /// Skip the first n rows.
    fn offset(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .offset(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
    }

    /// Unpersist (no-op in Sparkless; returns self).
    fn unpersist(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .unpersist()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    /// No-op: query planner hint. Returns self for chaining.
    fn hint(&self, _name: &str, _params: Vec<i32>) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .hint(_name, &_params)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
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

    /// Apply a function to each row and flatten (PySpark flatMap parity). Fixes #375.
    ///
    /// Args:
    ///     func: Callable that takes a row (dict) and returns an iterable of rows (dicts).
    ///
    /// Returns:
    ///     DataFrame with flattened rows. Requires a default SparkSession (builder().get_or_create()).
    fn flat_map(
        &self,
        py: Python<'_>,
        func: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyDataFrame> {
        let rows_py = self.collect(py)?;
        let rows_list = rows_py.downcast_bound::<PyList>(py).map_err(|_| {
            pyo3::exceptions::PyRuntimeError::new_err("flat_map: collect() did not return a list")
        })?;
        let flat_py = PyList::empty(py);
        for i in 0..rows_list.len() {
            let row = rows_list.get_item(i)?;
            let result = func.call1((row,))?;
            for item in result.try_iter()? {
                flat_py.append(item?)?;
            }
        }
        let session = get_default_session().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "flatMap requires a default session; call SparkSession.builder().get_or_create() first",
            )
        })?;
        if flat_py.is_empty() {
            let df = session
                .create_dataframe_from_rows(vec![], vec![])
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            return Ok(PyDataFrame { inner: df });
        }
        let first = flat_py.get_item(0)?;
        let order: Vec<String> = first
            .downcast::<PyDict>()
            .map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err(
                    "flat_map: function must return iterable of dicts (row dicts)",
                )
            })?
            .keys()
            .iter()
            .map(|k| k.extract::<String>())
            .collect::<PyResult<Vec<_>>>()?;
        let schema: Vec<(String, String)> = order
            .iter()
            .map(|n| (n.clone(), "string".to_string()))
            .collect();
        let mut rows_rust: Vec<Vec<JsonValue>> = Vec::with_capacity(flat_py.len());
        for i in 0..flat_py.len() {
            let item = flat_py.get_item(i)?;
            let dict = item.downcast::<PyDict>().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err(
                    "flat_map: function must return iterable of dicts",
                )
            })?;
            let row: Vec<JsonValue> = order
                .iter()
                .map(|name| {
                    let v = dict
                        .get_item(name.as_str())
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| py.None().into_bound(py));
                    py_to_json_value(&v)
                })
                .collect::<PyResult<Vec<_>>>()?;
            rows_rust.push(row);
        }
        let df = session
            .create_dataframe_from_rows(rows_rust, schema)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Alias for flat_map (camelCase). Fixes #375.
    #[pyo3(name = "flatMap")]
    fn flat_map_camel(
        &self,
        py: Python<'_>,
        func: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyDataFrame> {
        self.flat_map(py, func)
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

    /// Summary statistics (count, mean, stddev, min, max) for numeric columns. PySpark describe.
    fn describe(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .describe()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Summary statistics (alias for describe). PySpark summary.
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
        return Ok(PyDataFrame { inner: df });
    }

    /// Alias for toDF (snake_case). PySpark df.to_df(*cols).
    #[pyo3(name = "to_df")]
    fn to_df_snake(&self, names: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .to_df(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    /// Select using SQL-like expression strings.
    fn select_expr(&self, exprs: Vec<String>) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .select_expr(&exprs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    /// Select columns whose names match the regex pattern.
    fn col_regex(&self, pattern: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .col_regex(pattern)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
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
        return Ok(PyDataFrame { inner: df });
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
        use pyo3::conversion::IntoPyObjectExt;
        match (col1, col2) {
            (Some(c1), Some(c2)) => {
                let r = self
                    .inner
                    .corr_cols(c1, c2)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                Ok(r.into_py_any(py)?)
            }
            _ => {
                let df = self
                    .inner
                    .corr()
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                Ok(PyDataFrame { inner: df }.into_py_any(py)?)
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
        return Ok(PyDataFrame { inner: df });
    }
}

/// Python wrapper for DataFrame.na() (fill, drop). PySpark df.na.fill(value, subset=...) / na.drop(subset=..., how=..., thresh=...).
#[pyclass(name = "DataFrameNa")]
pub struct PyDataFrameNa {
    df: DataFrame,
}

#[pymethods]
impl PyDataFrameNa {
    /// Replace null values. Value can be a scalar (int, float, bool, str) or a Column expression.
    ///
    /// Args:
    ///     value: Scalar or Column (e.g. ``0`` or ``lit(0)`` or ``col("other")``).
    ///     subset: Optional list of column names (or single column name). If provided, only those columns are filled.
    ///
    /// Returns:
    ///     DataFrame (lazy) with nulls filled.
    #[pyo3(signature = (value, subset=None))]
    fn fill(
        &self,
        value: &Bound<'_, PyAny>,
        subset: Option<Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let expr = py_fill_value_to_expr(value)?;
        let subset_vec = py_subset_to_option_vec(subset.as_ref())?;
        let sub: Option<Vec<&str>> = subset_vec
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .df
            .na()
            .fill(expr, sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    /// Drop rows with nulls. PySpark na.drop(subset=..., how=..., thresh=...).
    ///
    /// Args:
    ///     subset: Optional list of column names. If None, all columns are considered.
    ///     how: "any" (default) drop if any null in subset; "all" drop only if all null in subset.
    ///     thresh: If set, keep row if it has at least this many non-null values in subset (overrides how).
    ///
    /// Returns:
    ///     DataFrame (lazy) with null rows removed.
    #[pyo3(signature = (subset=None, how="any", thresh=None))]
    fn drop(
        &self,
        subset: Option<Vec<String>>,
        how: &str,
        thresh: Option<usize>,
    ) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .df
            .na()
            .drop(sub, how, thresh)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    /// Replace values in columns. PySpark na.replace(to_replace, value, subset=None).
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
        to_replace: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        subset: Option<Vec<String>>,
    ) -> PyResult<PyDataFrame> {
        let old_expr = py_any_to_expr(to_replace)?;
        let new_expr = py_any_to_expr(value)?;
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .df
            .na()
            .replace(old_expr, new_expr, sub)
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
        return Ok(PyDataFrame { inner: df });
    }

    /// Return a DataFrame with one row per group and the sum of the given column (PySpark: sum("v") or sum(F.col("v"))).
    ///
    /// Args:
    ///     column: Column name (str) or Column expression (e.g. col("v")).
    ///
    /// Returns:
    ///     DataFrame (lazy). Raises RuntimeError if column is missing or not numeric.
    fn sum(&self, column: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyDataFrame> {
        use polars::prelude::*;
        let expr =
            py_col_or_name_to_agg_expr(column, "sum", |e| e.clone().sum(), |n| col(n).sum())?;
        let df = self
            .inner
            .agg(vec![expr])
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with one row per group and the mean of the given column(s) (PySpark: avg("a", "b") or avg(F.col("v"))).
    ///
    /// Args:
    ///     cols: One or more column names (str) or Column expressions (e.g. col("v")). Nulls excluded from mean.
    ///
    /// Returns:
    ///     DataFrame. Raises RuntimeError if a column is missing or not numeric.
    #[pyo3(signature = (*columns))]
    fn avg(&self, columns: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<PyDataFrame> {
        use polars::prelude::*;
        let mut agg_exprs: Vec<Expr> = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            let item = columns.get_item(i)?;
            if let Ok(name) = item.extract::<String>() {
                agg_exprs.push(col(name.as_str()).mean().alias(format!("avg({name})")));
            } else if let Ok(pycol) = item.downcast::<PyColumn>() {
                let c = pycol.borrow();
                let name = c.inner.name().to_string();
                agg_exprs.push(c.inner.expr().clone().mean().alias(format!("avg({name})")));
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "avg() requires column names (str) or Column expressions (e.g. col(\"x\"))",
                ));
            }
        }
        if agg_exprs.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "avg() requires at least one column",
            ));
        }
        let df = self
            .inner
            .agg(agg_exprs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with one row per group and the minimum of the given column (PySpark: min("v") or min(F.col("v"))).
    ///
    /// Args:
    ///     column: Column name (str) or Column expression (e.g. col("v")).
    ///
    /// Returns:
    ///     DataFrame (lazy). Raises RuntimeError if column is missing.
    #[pyo3(name = "min")]
    fn min_(&self, column: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyDataFrame> {
        use polars::prelude::*;
        let expr =
            py_col_or_name_to_agg_expr(column, "min", |e| e.clone().min(), |n| col(n).min())?;
        let df = self
            .inner
            .agg(vec![expr])
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return a DataFrame with one row per group and the maximum of the given column (PySpark: max("v") or max(F.col("v"))).
    ///
    /// Args:
    ///     column: Column name (str) or Column expression (e.g. col("v")).
    ///
    /// Returns:
    ///     DataFrame (lazy). Raises RuntimeError if column is missing.
    fn max(&self, column: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyDataFrame> {
        use polars::prelude::*;
        let expr =
            py_col_or_name_to_agg_expr(column, "max", |e| e.clone().max(), |n| col(n).max())?;
        let df = self
            .inner
            .agg(vec![expr])
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Pivot a column for pivot-table aggregation (PySpark: groupBy(...).pivot(pivot_col).sum(value_col)).
    ///
    /// Args:
    ///     pivot_col: Name of the column whose distinct values become output columns.
    ///     values: Optional list of pivot values; if None, values are taken from the data.
    ///
    /// Returns:
    ///     PivotedGroupedData: Call .sum(column), .avg(column), .min(column), .max(column) to run the aggregation.
    #[pyo3(signature = (pivot_col, values=None))]
    fn pivot(
        &self,
        pivot_col: &str,
        values: Option<Vec<String>>,
    ) -> PyResult<PyPivotedGroupedData> {
        let piv = self.inner.pivot(pivot_col, values);
        Ok(PyPivotedGroupedData { inner: piv })
    }

    /// Aggregate groups with one or more expressions (e.g. sum(col("x")), avg(col("y"))).
    ///
    /// Args:
    ///     exprs: List of aggregation Column expressions (from ``sum()``, ``avg()``, ``count()``,
    ///         ``min()``, ``max()``, etc.), or grouped vectorized UDFs created via
    ///         ``pandas_udf(..., function_type="grouped_agg")``.
    ///
    /// Returns:
    ///     DataFrame (lazy) with one row per group and the aggregated columns.
    ///
    /// Raises:
    ///     RuntimeError: If execution fails.
    fn agg(&self, exprs: Vec<PyRef<PyColumn>>) -> PyResult<PyDataFrame> {
        #[cfg(feature = "pyo3")]
        {
            use crate::column::Column;
            use crate::session::get_thread_udf_session;

            let mut native_exprs: Vec<Expr> = Vec::new();
            let mut grouped_specs: Vec<GroupedAggSpec> = Vec::new();

            // Session is required only for grouped Python UDF aggregations.
            let session_opt = get_thread_udf_session();

            for py_col in &exprs {
                let col: &Column = &py_col.inner;
                if let Some((ref udf_name, ref args)) = col.udf_call {
                    // Interpret as a potential grouped Python UDF aggregation.
                    let session = session_opt.as_ref().ok_or_else(|| {
                        pyo3::exceptions::PyRuntimeError::new_err(
                            "grouped Python UDFs in groupBy().agg require an active SparkSession; call SparkSession.builder().get_or_create() first.",
                        )
                    })?;
                    let entry = session
                        .udf_registry
                        .get_python_udf(udf_name, session.is_case_sensitive())
                        .ok_or_else(|| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "Python UDF '{}' not found",
                                udf_name
                            ))
                        })?;
                    match entry.kind {
                        crate::udf_registry::PythonUdfKind::GroupedVectorizedAgg => {
                            grouped_specs.push(GroupedAggSpec {
                                output_name: col.name().to_string(),
                                udf_name: udf_name.clone(),
                                args: args.clone(),
                            });
                        }
                        _ => {
                            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                                "Python scalar and non-grouped vectorized UDFs are not supported in groupBy().agg; use pandas_udf(..., function_type=\"grouped_agg\") instead.",
                            ));
                        }
                    }
                } else {
                    native_exprs.push(col.expr().clone());
                }
            }

            // If we have both native aggs and grouped UDFs, require separate calls for now.
            if !native_exprs.is_empty() && !grouped_specs.is_empty() {
                return Err(pyo3::exceptions::PyNotImplementedError::new_err(
                    "Mixing grouped Python UDF aggregations with built-in aggregations in a single groupBy().agg call is not yet supported; run them in separate agg() calls.",
                ));
            }

            if grouped_specs.is_empty() {
                // Only native aggregations: use existing path.
                let df = self
                    .inner
                    .agg(native_exprs)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                return Ok(PyDataFrame { inner: df });
            }

            // Only grouped Python UDF aggregations.
            let session = session_opt.ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "grouped Python UDFs in groupBy().agg require an active SparkSession; call SparkSession.builder().get_or_create() first.",
                )
            })?;
            let df = execute_grouped_vectorized_aggs(
                &DataFrame {
                    df: std::sync::Arc::new(self.inner.df.clone()),
                    case_sensitive: self.inner.case_sensitive,
                    alias: None,
                },
                &self.inner.grouping_cols,
                &grouped_specs,
                self.inner.case_sensitive,
                &session,
            )
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(PyDataFrame { inner: df })
        }

        #[cfg(not(feature = "pyo3"))]
        {
            let aggregations: Vec<Expr> = exprs.iter().map(|c| c.inner.expr().clone()).collect();
            let df = self
                .inner
                .agg(aggregations)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            return Ok(PyDataFrame { inner: df });
        }
    }

    fn any_value(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .any_value(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn bool_and(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .bool_and(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn bool_or(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .bool_or(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn product(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .product(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn collect_list(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .collect_list(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn collect_set(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .collect_set(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn count_if(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .count_if(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn percentile(&self, column: &str, p: f64) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .percentile(column, p)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn max_by(&self, value_column: &str, ord_column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .max_by(value_column, ord_column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn min_by(&self, value_column: &str, ord_column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .min_by(value_column, ord_column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn covar_pop(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .covar_pop(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn covar_samp(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .covar_samp(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn corr(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .corr(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn regr_count(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_count(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn regr_avgx(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_avgx(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn regr_avgy(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_avgy(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn regr_slope(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_slope(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn regr_intercept(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_intercept(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn regr_r2(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_r2(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn regr_sxx(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_sxx(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn regr_syy(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_syy(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn regr_sxy(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_sxy(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn kurtosis(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .kurtosis(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }

    fn skewness(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .skewness(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        return Ok(PyDataFrame { inner: df });
    }
}

/// Python wrapper for PivotedGroupedData (groupBy(...).pivot(...).sum(...)).
#[pyclass(name = "PivotedGroupedData")]
pub struct PyPivotedGroupedData {
    inner: PivotedGroupedData,
}

#[pymethods]
impl PyPivotedGroupedData {
    /// Pivot then sum (PySpark: groupBy(...).pivot(...).sum(column)).
    fn sum(&self, value_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sum(value_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Pivot then mean (PySpark: groupBy(...).pivot(...).avg(column)).
    fn avg(&self, value_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .avg(value_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Pivot then min (PySpark: groupBy(...).pivot(...).min(column)).
    #[pyo3(name = "min")]
    fn min_(&self, value_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .min(value_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Pivot then max (PySpark: groupBy(...).pivot(...).max(column)).
    fn max(&self, value_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .max(value_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Pivot then count (PySpark: groupBy(...).pivot(...).count()). Counts rows per group per pivot value.
    fn count(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .count()
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
    /// Count rows per grouping set (PySpark cube/rollup .count()).
    fn count(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .count()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

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
        return Ok(PyDataFrame { inner: df });
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

    /// Save the DataFrame as an in-memory table (PySpark: saveAsTable).
    ///
    /// Registers the DataFrame in the session's saved-tables namespace. Session-scoped;
    /// readable via ``spark.table(name)`` or ``spark.read.table(name)`` (temp view
    /// takes precedence if same name exists). format, partitionBy, and options are
    /// accepted for API compatibility but ignored for in-memory tables.
    ///
    /// Args:
    ///     name: Table name.
    ///     format: Ignored for in-memory (API compatibility).
    ///     mode: "error" (default), "overwrite", "append", or "ignore".
    ///     partitionBy: Ignored for in-memory (API compatibility).
    ///     **options: Ignored for in-memory (API compatibility).
    ///
    /// Raises:
    ///     RuntimeError: If no default session, or mode is "error" and table exists.
    #[cfg(feature = "sql")]
    #[pyo3(name = "saveAsTable")]
    #[pyo3(signature = (name, format=None, mode=None, partition_by=None))]
    fn save_as_table(
        &self,
        _py: Python<'_>,
        name: &str,
        format: Option<&str>,
        mode: Option<&str>,
        partition_by: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) -> PyResult<()> {
        let _ = (format, partition_by); // ignored for in-memory (API compatibility)
        let session = get_default_session().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "saveAsTable: no default session. Call SparkSession.builder().get_or_create() first.",
            )
        })?;
        let save_mode = match mode.unwrap_or("error").to_lowercase().as_str() {
            "error" | "errorifexists" => SaveMode::ErrorIfExists,
            "overwrite" => SaveMode::Overwrite,
            "append" => SaveMode::Append,
            "ignore" => SaveMode::Ignore,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "saveAsTable mode must be one of 'error', 'overwrite', 'append', 'ignore', got '{}'",
                    other
                )));
            }
        };
        self.df
            .write()
            .save_as_table(&session, name, save_mode)
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
    match &av {
        AnyValue::Null => py.None().into_bound_py_any(py).map(Into::into),
        AnyValue::Boolean(b) => (*b).into_bound_py_any(py).map(Into::into),
        AnyValue::Int8(i) => (*i as i64).into_bound_py_any(py).map(Into::into),
        AnyValue::Int16(i) => (*i as i64).into_bound_py_any(py).map(Into::into),
        AnyValue::Int32(i) => (*i).into_bound_py_any(py).map(Into::into),
        AnyValue::Int64(i) => (*i).into_bound_py_any(py).map(Into::into),
        AnyValue::UInt8(u) => (*u as i64).into_bound_py_any(py).map(Into::into),
        AnyValue::UInt16(u) => (*u as i64).into_bound_py_any(py).map(Into::into),
        AnyValue::UInt32(u) => (*u).into_bound_py_any(py).map(Into::into),
        AnyValue::UInt64(u) => (*u).into_bound_py_any(py).map(Into::into),
        AnyValue::Float32(f) => (*f).into_bound_py_any(py).map(Into::into),
        AnyValue::Float64(f) => (*f).into_bound_py_any(py).map(Into::into),
        AnyValue::String(s) => s.to_string().into_bound_py_any(py).map(Into::into),
        AnyValue::StringOwned(s) => s.to_string().into_bound_py_any(py).map(Into::into),
        AnyValue::Binary(b) => pyo3::types::PyBytes::new(py, b)
            .into_bound_py_any(py)
            .map(Into::into),
        AnyValue::BinaryOwned(b) => pyo3::types::PyBytes::new(py, b)
            .into_bound_py_any(py)
            .map(Into::into),
        AnyValue::Date(days) => {
            let epoch = crate::date_utils::epoch_naive_date();
            let d = epoch + chrono::TimeDelta::days(*days as i64);
            d.format("%Y-%m-%d")
                .to_string()
                .into_bound_py_any(py)
                .map(Into::into)
        }
        AnyValue::Datetime(us, tu, _) | AnyValue::DatetimeOwned(us, tu, _) => {
            let micros = match tu {
                TimeUnit::Microseconds => *us,
                TimeUnit::Milliseconds => us.saturating_mul(1000),
                TimeUnit::Nanoseconds => us.saturating_div(1000),
            };
            let dt = chrono::DateTime::from_timestamp_micros(micros).unwrap_or_default();
            let s = dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
            s.into_bound_py_any(py).map(Into::into)
        }
        AnyValue::List(s) => {
            // Map column (List(Struct{key, value})): return Python dict {} not list (fixes #380).
            let dtype = s.dtype();
            let (is_map, fields_ref) = match dtype {
                polars::datatypes::DataType::List(inner) => {
                    if let polars::datatypes::DataType::Struct(fields) = inner.as_ref() {
                        let names: Vec<String> =
                            fields.iter().map(|f| f.name.as_str().to_string()).collect();
                        (names == ["key", "value"], Some(fields))
                    } else {
                        (false, None)
                    }
                }
                polars::datatypes::DataType::Struct(fields) => {
                    let names: Vec<String> =
                        fields.iter().map(|f| f.name.as_str().to_string()).collect();
                    (names == ["key", "value"], Some(fields))
                }
                _ => (false, None),
            };
            if is_map {
                if let Some(fields) = fields_ref {
                    let py_dict = pyo3::types::PyDict::new(py);
                    for i in 0..s.len() {
                        let av = s.get(i).map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
                        })?;
                        if let AnyValue::Struct(_, _, _) = &av {
                            let mut key_py = None;
                            let mut val_py = None;
                            for (fld_av, fld) in av._iter_struct_av().zip(fields.iter()) {
                                let py_val = any_value_to_py(py, fld_av.clone())?;
                                if fld.name.as_str() == "key" {
                                    key_py = Some(py_val);
                                } else if fld.name.as_str() == "value" {
                                    val_py = Some(py_val);
                                }
                            }
                            if let (Some(k), Some(v)) = (key_py, val_py) {
                                let k_str = k.extract::<String>(py).ok();
                                if let Some(kk) = k_str {
                                    py_dict.set_item(kk, v)?;
                                }
                            }
                        }
                    }
                    return Ok(py_dict.into());
                }
            }
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
        AnyValue::Struct(_, _, fields) => {
            let py_dict = pyo3::types::PyDict::new(py);
            for (fld_av, fld) in av._iter_struct_av().zip(fields.iter()) {
                let py_val = any_value_to_py(py, fld_av.clone())?;
                py_dict.set_item(fld.name.as_str(), py_val)?;
            }
            Ok(py_dict.into())
        }
        AnyValue::StructOwned(payload) => {
            let (values, fields) = payload.as_ref();
            let py_dict = pyo3::types::PyDict::new(py);
            for (fld_av, fld) in values.iter().zip(fields.iter()) {
                let py_val = any_value_to_py(py, fld_av.clone())?;
                py_dict.set_item(fld.name.as_str(), py_val)?;
            }
            Ok(py_dict.into())
        }
        // Duration, Time, Categorical, Decimal, etc.: use string representation so
        // collect() never returns None for non-null values (issue #211).
        other => other.to_string().into_bound_py_any(py).map(Into::into),
    }
}
