//! Sort order and when/then/otherwise builders (PySpark sql order, when/otherwise).

use crate::column::Column as RsColumn;
use crate::functions::SortOrder;
use polars::prelude::Expr;
use pyo3::prelude::*;

use super::column::PyColumn;

/// Python wrapper for SortOrder (used with order_by_exprs).
#[pyclass]
#[derive(Clone)]
pub struct PySortOrder {
    pub inner: SortOrder,
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
