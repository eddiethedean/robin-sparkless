//! Sort order and when/then/otherwise builders (PySpark sql order, when/otherwise).

use crate::column::Column as RsColumn;
use crate::functions::SortOrder;
use polars::prelude::Expr;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::column::PyColumn;

/// Python wrapper for SortOrder (used with order_by_exprs).
#[pyclass]
#[derive(Clone)]
pub struct PySortOrder {
    pub inner: SortOrder,
}

/// Python wrapper for PySpark-style Window specification.
///
/// Minimal parity for ``Window.partitionBy(...).orderBy(...)`` used with
/// ``row_number().over(window)`` in selects/withColumn.
#[derive(Clone)]
#[pyclass(name = "Window")]
pub struct PyWindow {
    pub(crate) partition_by: Vec<String>,
    pub(crate) order_by: Option<RsColumn>,
}

#[pymethods]
impl PyWindow {
    /// Classmethod: Window.partitionBy(*cols) -> Window
    ///
    /// PySpark-style usage:
    ///     win = Window.partitionBy("dept").orderBy(col("salary"))
    #[classmethod]
    #[pyo3(name = "partitionBy")]
    #[pyo3(signature = (*cols))]
    fn partition_by_cls(
        _cls: &Bound<'_, pyo3::types::PyType>,
        cols: &Bound<'_, PyTuple>,
    ) -> PyResult<Self> {
        let mut names = Vec::with_capacity(cols.len());
        for item in cols.iter() {
            names.push(item.extract::<String>()?);
        }
        Ok(Self {
            partition_by: names,
            order_by: None,
        })
    }

    /// Set ordering columns for the window. Currently supports a single Column.
    ///
    /// PySpark-style usage:
    ///     win = Window.partitionBy("dept").orderBy(F.col("salary"))
    #[pyo3(name = "orderBy")]
    #[pyo3(signature = (*cols))]
    fn order_by(&self, cols: Vec<PyRef<PyColumn>>) -> PyResult<Self> {
        if cols.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Window.orderBy requires at least one column",
            ));
        }
        if cols.len() > 1 {
            return Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "Window.orderBy with multiple columns is not yet supported; use a single column",
            ));
        }
        Ok(Self {
            partition_by: self.partition_by.clone(),
            order_by: Some(cols[0].inner.clone()),
        })
    }
}

/// Python wrapper for the two-arg form when(cond, value). Supports .otherwise(default) for PySpark parity.
#[pyclass(name = "WhenThen")]
pub struct PyWhenThen {
    pub(crate) condition: Expr,
    pub(crate) then_value: Expr,
}

#[pymethods]
impl PyWhenThen {
    /// Set the default value when the condition is false. Returns the complete conditional Column.
    ///
    /// PySpark: ``F.when(cond, val).otherwise(default)``.
    ///
    /// Args:
    ///     value: Column expression for the "else" value.
    ///
    /// Returns:
    ///     Column: The full when-then-otherwise expression.
    fn otherwise(&self, value: &PyColumn) -> PyColumn {
        let when_then = polars::prelude::when(self.condition.clone()).then(self.then_value.clone());
        let expr = when_then.otherwise(value.inner.expr().clone());
        PyColumn {
            inner: RsColumn::from_expr(expr, None),
        }
    }
}

/// Python wrapper for WhenBuilder (when(cond).then(val).otherwise(val)).
#[pyclass(name = "WhenBuilder")]
pub struct PyWhenBuilder {
    pub condition: Expr,
}

#[pymethods]
impl PyWhenBuilder {
    /// Set the value to use when the condition is true. Chain ``.otherwise(default)`` to complete the expression.
    ///
    /// Args:
    ///     value: Column expression for the "then" value.
    ///
    /// Returns:
    ///     ThenBuilder: Call ``.otherwise(default)`` to get the final Column.
    fn then(&self, value: &PyColumn) -> PyThenBuilder {
        let when_then =
            polars::prelude::when(self.condition.clone()).then(value.inner.expr().clone());
        PyThenBuilder { when_then }
    }
}

/// Python wrapper for ThenBuilder (.otherwise(val)).
#[pyclass(name = "ThenBuilder")]
pub struct PyThenBuilder {
    when_then: polars::prelude::Then,
}

#[pymethods]
impl PyThenBuilder {
    /// Set the default value when no when-then clause matches. Returns the complete conditional Column.
    ///
    /// Args:
    ///     value: Column expression for the "else" value.
    ///
    /// Returns:
    ///     Column: The full when-then-otherwise expression.
    fn otherwise(&self, value: &PyColumn) -> PyColumn {
        let expr = self.when_then.clone().otherwise(value.inner.expr().clone());
        PyColumn {
            inner: RsColumn::from_expr(expr, None),
        }
    }
}

/// Python wrapper for row_number() in the Python API (PySpark-style).
///
/// Use with a Window:
///     win = Window.partitionBy("dept").orderBy(col("salary"))
///     df.withColumn("rn", row_number().over(win))
#[pyclass(name = "RowNumber")]
pub struct PyRowNumber {
    pub(crate) descending: bool,
}

#[pymethods]
impl PyRowNumber {
    /// Apply this row_number() to a Window and return a Column expression.
    ///
    /// Args:
    ///     window: Window specification built via Window.partitionBy(...).orderBy(...).
    ///
    /// Returns:
    ///     Column: row_number over the given partition/order.
    fn over(&self, window: &PyWindow) -> PyResult<PyColumn> {
        let order_col = window.order_by.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "Window.orderBy(...) must be called before row_number().over(window)",
            )
        })?;
        let refs: Vec<&str> = window.partition_by.iter().map(|s| s.as_str()).collect();
        let col = order_col.row_number(self.descending).over(&refs);
        Ok(PyColumn { inner: col })
    }
}
