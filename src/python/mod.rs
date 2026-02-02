//! Python bindings for robin-sparkless (PyO3).
//! Compiled only when the `pyo3` feature is enabled.

use crate::dataframe::JoinType;
use crate::functions::{avg, coalesce, col as rs_col, count, max, min, sum as rs_sum};
use crate::column::Column as RsColumn;
use crate::{DataFrame, GroupedData, SparkSession};
use polars::prelude::Expr;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::Path;

/// Python module entry point.
#[pymodule]
fn robin_sparkless(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySparkSession>()?;
    m.add_class::<PySparkSessionBuilder>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PyWhenBuilder>()?;
    m.add_class::<PyThenBuilder>()?;
    m.add_class::<PyGroupedData>()?;
    m.add("col", wrap_pyfunction!(py_col, m)?)?;
    m.add("lit", wrap_pyfunction!(py_lit, m)?)?;
    m.add("when", wrap_pyfunction!(py_when, m)?)?;
    m.add("coalesce", wrap_pyfunction!(py_coalesce, m)?)?;
    m.add("sum", wrap_pyfunction!(py_sum, m)?)?;
    m.add("avg", wrap_pyfunction!(py_avg, m)?)?;
    m.add("min", wrap_pyfunction!(py_min, m)?)?;
    m.add("max", wrap_pyfunction!(py_max, m)?)?;
    m.add("count", wrap_pyfunction!(py_count, m)?)?;
    Ok(())
}

#[pyfunction]
fn py_col(name: &str) -> PyColumn {
    PyColumn {
        inner: rs_col(name),
    }
}

#[pyfunction]
fn py_lit(value: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyColumn> {
    let inner = if value.is_none() {
        use polars::prelude::*;
        RsColumn::from_expr(lit(NULL), None)
    } else if let Ok(x) = value.extract::<i64>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = value.extract::<f64>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = value.extract::<bool>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = value.extract::<String>() {
        RsColumn::from_expr(polars::prelude::lit(x.as_str()), None)
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "lit() supports only None, int, float, bool, str",
        ));
    };
    Ok(PyColumn { inner })
}

#[pyfunction]
fn py_when(condition: &PyColumn) -> PyWhenBuilder {
    PyWhenBuilder {
        condition: condition.inner.expr().clone(),
    }
}

#[pyfunction]
fn py_coalesce(columns: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    Ok(PyColumn {
        inner: coalesce(&refs),
    })
}

#[pyfunction]
fn py_sum(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rs_sum(&column.inner),
    }
}

#[pyfunction]
fn py_avg(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: avg(&column.inner),
    }
}

#[pyfunction]
fn py_min(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: min(&column.inner),
    }
}

#[pyfunction]
fn py_max(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: max(&column.inner),
    }
}

#[pyfunction]
fn py_count(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: count(&column.inner),
    }
}

/// Python wrapper for Column (expression).
#[pyclass(name = "Column")]
struct PyColumn {
    inner: RsColumn,
}

#[pymethods]
impl PyColumn {
    fn alias(&self, name: &str) -> Self {
        PyColumn {
            inner: self.inner.alias(name),
        }
    }

    fn is_null(&self) -> Self {
        PyColumn {
            inner: self.inner.is_null(),
        }
    }

    fn is_not_null(&self) -> Self {
        PyColumn {
            inner: self.inner.is_not_null(),
        }
    }

    fn gt(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.gt(other.inner.expr().clone()),
        }
    }

    fn ge(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.gt_eq(other.inner.expr().clone()),
        }
    }

    fn lt(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.lt(other.inner.expr().clone()),
        }
    }

    fn le(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.lt_eq(other.inner.expr().clone()),
        }
    }

    fn eq(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.eq(other.inner.expr().clone()),
        }
    }

    fn ne(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.neq(other.inner.expr().clone()),
        }
    }

    fn and_(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: RsColumn::from_expr(
                self.inner.expr().clone().and(other.inner.expr().clone()),
                None,
            ),
        }
    }

    fn or_(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: RsColumn::from_expr(
                self.inner.expr().clone().or(other.inner.expr().clone()),
                None,
            ),
        }
    }

    fn upper(&self) -> Self {
        PyColumn {
            inner: self.inner.upper(),
        }
    }

    fn lower(&self) -> Self {
        PyColumn {
            inner: self.inner.lower(),
        }
    }

    #[pyo3(signature = (start, length=None))]
    fn substr(&self, start: i64, length: Option<i64>) -> Self {
        PyColumn {
            inner: self.inner.substr(start, length),
        }
    }
}

/// Python wrapper for WhenBuilder (when(cond).then(val).otherwise(val)).
#[pyclass(name = "WhenBuilder")]
struct PyWhenBuilder {
    condition: Expr,
}

#[pymethods]
impl PyWhenBuilder {
    fn then(&self, value: &PyColumn) -> PyThenBuilder {
        let when_then = polars::prelude::when(self.condition.clone()).then(value.inner.expr().clone());
        PyThenBuilder { when_then }
    }
}

/// Python wrapper for ThenBuilder (.otherwise(val)).
#[pyclass(name = "ThenBuilder")]
struct PyThenBuilder {
    when_then: polars::prelude::Then,
}

#[pymethods]
impl PyThenBuilder {
    fn otherwise(&self, value: &PyColumn) -> PyColumn {
        let expr = self.when_then.clone().otherwise(value.inner.expr().clone());
        PyColumn {
            inner: RsColumn::from_expr(expr, None),
        }
    }
}

/// Python wrapper for SparkSession.
#[pyclass(name = "SparkSession")]
struct PySparkSession {
    inner: SparkSession,
}

#[pymethods]
impl PySparkSession {
    #[new]
    fn new() -> Self {
        PySparkSession {
            inner: SparkSession::new(None, None, std::collections::HashMap::new()),
        }
    }

    /// Create a SparkSession via builder().app_name(...).get_or_create()
    #[classmethod]
    fn builder(_cls: &Bound<'_, pyo3::types::PyType>) -> PyResult<PySparkSessionBuilder> {
        Ok(PySparkSessionBuilder {
            app_name: None,
            master: None,
            config: std::collections::HashMap::new(),
        })
    }

    fn is_case_sensitive(&self) -> bool {
        self.inner.is_case_sensitive()
    }

    /// Create a DataFrame from a list of 3-tuples (id, age, name) and column names.
    /// data: list of (int, int, str), column_names: list of 3 strings e.g. ["id", "age", "name"].
    fn create_dataframe(
        &self,
        _py: Python<'_>,
        data: &Bound<'_, pyo3::types::PyAny>,
        column_names: Vec<String>,
    ) -> PyResult<PyDataFrame> {
        let data_rust: Vec<(i64, i64, String)> = data
            .extract()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let names_ref: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .create_dataframe(data_rust, names_ref)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn read_csv(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_csv(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn read_parquet(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_parquet(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn read_json(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_json(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for SparkSessionBuilder.
#[pyclass(name = "SparkSessionBuilder")]
struct PySparkSessionBuilder {
    app_name: Option<String>,
    master: Option<String>,
    config: std::collections::HashMap<String, String>,
}

#[pymethods]
impl PySparkSessionBuilder {
    fn app_name<'a>(mut slf: PyRefMut<'a, Self>, name: &str) -> PyRefMut<'a, Self> {
        slf.app_name = Some(name.to_string());
        slf
    }

    fn master<'a>(mut slf: PyRefMut<'a, Self>, master: &str) -> PyRefMut<'a, Self> {
        slf.master = Some(master.to_string());
        slf
    }

    fn config<'a>(mut slf: PyRefMut<'a, Self>, key: &str, value: &str) -> PyRefMut<'a, Self> {
        slf.config.insert(key.to_string(), value.to_string());
        slf
    }

    fn get_or_create(slf: PyRef<'_, Self>) -> PySparkSession {
        let mut config = std::collections::HashMap::new();
        for (k, v) in &slf.config {
            config.insert(k.clone(), v.clone());
        }
        let inner = SparkSession::new(
            slf.app_name.clone(),
            slf.master.clone(),
            config,
        );
        PySparkSession { inner }
    }
}

/// Python wrapper for DataFrame.
#[pyclass(name = "DataFrame")]
struct PyDataFrame {
    inner: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    fn count(&self) -> PyResult<usize> {
        self.inner
            .count()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    #[pyo3(signature = (n=None))]
    fn show(&self, n: Option<usize>) -> PyResult<()> {
        self.inner
            .show(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Collect as list of dicts (column name -> value per row).
    fn collect(&self, py: Python<'_>) -> PyResult<PyObject> {
        let pl_df = self
            .inner
            .collect()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let df = pl_df.as_ref();
        let names = df.get_column_names();
        let nrows = df.height();
        let rows = pyo3::types::PyList::empty_bound(py);
        for i in 0..nrows {
            let row_dict = PyDict::new_bound(py);
            for (col_idx, name) in names.iter().enumerate() {
                let s = df.get_columns().get(col_idx).ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("column index out of range")
                })?;
                let av = s.get(i).map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
                })?;
                let py_val = any_value_to_py(py, av)?;
                row_dict.set_item(name.as_str(), py_val)?;
            }
            rows.append(&*row_dict)?;
        }
        Ok(rows.to_object(py))
    }

    fn filter(&self, condition: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .filter(condition.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn select(&self, cols: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .select(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn with_column(&self, column_name: &str, expr: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .with_column(column_name, expr.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(signature = (cols, ascending=None))]
    fn order_by(&self, cols: Vec<String>, ascending: Option<Vec<bool>>) -> PyResult<PyDataFrame> {
        let asc = ascending.unwrap_or_else(|| vec![true; cols.len()]);
        let df = self
            .inner
            .order_by(
                cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                asc,
            )
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn group_by(&self, cols: Vec<String>) -> PyResult<PyGroupedData> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let gd = self
            .inner
            .group_by(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyGroupedData { inner: gd })
    }

    #[pyo3(signature = (other, on, how="inner"))]
    fn join(&self, other: &PyDataFrame, on: Vec<String>, how: &str) -> PyResult<PyDataFrame> {
        let join_type = match how.to_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "outer" => JoinType::Outer,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "how must be 'inner', 'left', 'right', or 'outer'",
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

    fn union(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .union(&other.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn union_by_name(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .union_by_name(&other.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

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

    fn drop(&self, cols: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .drop(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

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

    fn fillna(&self, value: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .fillna(value.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn limit(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .limit(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn with_column_renamed(&self, old: &str, new: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .with_column_renamed(old, new)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for GroupedData.
#[pyclass(name = "GroupedData")]
struct PyGroupedData {
    inner: GroupedData,
}

#[pymethods]
impl PyGroupedData {
    fn count(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .count()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn sum(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sum(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn avg(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .avg(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(name = "min")]
    fn min_(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .min(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn max(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .max(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn agg(&self, exprs: Vec<PyRef<PyColumn>>) -> PyResult<PyDataFrame> {
        let aggregations: Vec<Expr> = exprs.iter().map(|c| c.inner.expr().clone()).collect();
        let df = self
            .inner
            .agg(aggregations)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Convert Polars AnyValue to a Python object.
fn any_value_to_py(py: Python<'_>, av: polars::prelude::AnyValue<'_>) -> PyResult<PyObject> {
    use polars::prelude::AnyValue;
    let obj: Py<pyo3::types::PyAny> = match av {
        AnyValue::Null => py.None().into_py(py),
        AnyValue::Boolean(b) => b.into_py(py),
        AnyValue::Int32(i) => i.into_py(py),
        AnyValue::Int64(i) => i.into_py(py),
        AnyValue::UInt32(u) => u.into_py(py),
        AnyValue::UInt64(u) => u.into_py(py),
        AnyValue::Float32(f) => f.into_py(py),
        AnyValue::Float64(f) => f.into_py(py),
        AnyValue::String(s) => s.to_string().into_py(py),
        other => {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "unsupported type for collect: {:?}",
                other
            )))
        }
    };
    Ok(obj.to_object(py))
}
