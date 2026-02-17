//! Sort order and when/then/otherwise builders (PySpark sql order, when/otherwise).

use crate::column::Column as RsColumn;
use crate::functions::SortOrder;
use polars::prelude::{col, ChainedThen, ChainedWhen, Expr, Then};
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::column::PyColumn;

enum PyWhenThenState {
    Single(Then),
    Chained(ChainedThen),
}

/// Convert a Python value (str or Column) to a partition column name (for partitionBy).
fn py_to_partition_name(item: &Bound<'_, pyo3::types::PyAny>) -> PyResult<String> {
    if let Ok(s) = item.extract::<String>() {
        return Ok(s);
    }
    if let Ok(py_col) = item.downcast::<PyColumn>() {
        return Ok(py_col.borrow().inner.name().to_string());
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "Window.partitionBy(*) requires column names (str) or Column expressions",
    ))
}

/// Order spec for Window: either a plain column (ascending) or a SortOrder (asc/desc with nulls).
#[derive(Clone)]
pub(crate) enum WindowOrderSpec {
    Column(RsColumn),
    SortOrder(SortOrder),
}

/// Convert a Python value (str, Column, or SortOrder) to WindowOrderSpec (for orderBy).
fn py_to_order_spec(item: &Bound<'_, pyo3::types::PyAny>) -> PyResult<WindowOrderSpec> {
    if let Ok(sort_order) = item.extract::<PyRef<'_, PySortOrder>>() {
        return Ok(WindowOrderSpec::SortOrder(sort_order.inner.clone()));
    }
    if let Ok(py_col) = item.downcast::<PyColumn>() {
        return Ok(WindowOrderSpec::Column(py_col.borrow().inner.clone()));
    }
    if let Ok(name) = item.extract::<String>() {
        return Ok(WindowOrderSpec::Column(RsColumn::from_expr(
            col(name.as_str()),
            None,
        )));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "Window.orderBy(*) requires column names (str), Column expressions, or SortOrder (asc/desc)",
    ))
}

/// Python wrapper for SortOrder (used with order_by_exprs).
#[pyclass]
#[derive(Clone)]
pub struct PySortOrder {
    pub inner: SortOrder,
}

/// Frame bound constants for PySpark parity (Window.unboundedPreceding, etc.).
pub const WINDOW_UNBOUNDED_PRECEDING: i64 = i64::MIN;
pub const WINDOW_CURRENT_ROW: i64 = 0;
pub const WINDOW_UNBOUNDED_FOLLOWING: i64 = i64::MAX;

/// Python wrapper for PySpark-style Window specification.
///
/// Minimal parity for ``Window.partitionBy(...).orderBy(...)`` used with
/// ``row_number().over(window)`` in selects/withColumn.
#[derive(Clone)]
#[pyclass(name = "Window")]
pub struct PyWindow {
    pub(crate) partition_by: Vec<String>,
    pub(crate) order_by: Option<WindowOrderSpec>,
    /// Optional frame: ("rows", start, end) or ("range", start, end). Stored for API parity; Polars backend uses default frame for now.
    pub(crate) frame: Option<(String, i64, i64)>,
}

#[pymethods]
impl PyWindow {
    /// No-arg constructor (PySpark: Window() for unbounded window). Chain .partitionBy(...).orderBy(...).
    ///
    /// Usage:
    ///     w = Window().partitionBy("dept").orderBy("salary")
    ///     w = Window.partitionBy("dept").orderBy("salary")  # classmethod also works
    #[new]
    fn new() -> Self {
        Self {
            partition_by: vec![],
            order_by: None,
            frame: None,
        }
    }

    /// Classmethod: Window.partitionBy(*cols) -> Window
    ///
    /// PySpark-style usage:
    ///     win = Window.partitionBy("dept").orderBy(col("salary"))
    /// Partition by column names or Column expressions (PySpark: partitionBy("col1", "col2") or partitionBy(col("col1"))).
    #[classmethod]
    #[pyo3(name = "partitionBy")]
    #[pyo3(signature = (*cols))]
    fn partition_by_cls(
        _cls: &Bound<'_, pyo3::types::PyType>,
        cols: &Bound<'_, PyTuple>,
    ) -> PyResult<Self> {
        let mut names = Vec::with_capacity(cols.len());
        for item in cols.iter() {
            names.push(py_to_partition_name(&item)?);
        }
        Ok(Self {
            partition_by: names,
            order_by: None,
            frame: None,
        })
    }

    /// Set ordering column(s). Accepts column names (str), Column expressions, or SortOrder (F.asc/F.desc).
    ///
    /// PySpark-style usage:
    ///     win = Window.partitionBy("dept").orderBy("salary")
    ///     win = Window.partitionBy("dept").orderBy(col("salary"))
    ///     win = Window.partitionBy("k").orderBy(desc("v"))  # F.desc("v")
    #[pyo3(name = "orderBy")]
    #[pyo3(signature = (*cols))]
    fn order_by(&self, cols: &Bound<'_, PyTuple>) -> PyResult<Self> {
        if cols.len() == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Window.orderBy requires at least one column",
            ));
        }
        if cols.len() > 1 {
            return Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "Window.orderBy with multiple columns is not yet supported; use a single column",
            ));
        }
        let order_spec = py_to_order_spec(&cols.get_item(0)?)?;
        Ok(Self {
            partition_by: self.partition_by.clone(),
            order_by: Some(order_spec),
            frame: self.frame.clone(),
        })
    }

    /// Set frame to rows between start and end (PySpark rowsBetween). Returns self for chaining.
    /// start/end: use Window.unboundedPreceding, Window.currentRow, Window.unboundedFollowing or int.
    #[pyo3(name = "rowsBetween")]
    #[pyo3(signature = (start, end))]
    fn rows_between(&self, start: i64, end: i64) -> Self {
        Self {
            partition_by: self.partition_by.clone(),
            order_by: self.order_by.clone(),
            frame: Some(("rows".to_string(), start, end)),
        }
    }

    /// Set frame to range between start and end (PySpark rangeBetween). Returns self for chaining.
    #[pyo3(name = "rangeBetween")]
    #[pyo3(signature = (start, end))]
    fn range_between(&self, start: i64, end: i64) -> Self {
        Self {
            partition_by: self.partition_by.clone(),
            order_by: self.order_by.clone(),
            frame: Some(("range".to_string(), start, end)),
        }
    }

    /// PySpark constant: first row of partition (use as start in rowsBetween/rangeBetween).
    #[classattr]
    #[pyo3(name = "unboundedPreceding")]
    fn unbounded_preceding() -> i64 {
        WINDOW_UNBOUNDED_PRECEDING
    }

    /// PySpark constant: current row (use as start/end in rowsBetween/rangeBetween).
    #[classattr]
    #[pyo3(name = "currentRow")]
    fn current_row() -> i64 {
        WINDOW_CURRENT_ROW
    }

    /// PySpark constant: last row of partition (use as end in rowsBetween/rangeBetween).
    #[classattr]
    #[pyo3(name = "unboundedFollowing")]
    fn unbounded_following() -> i64 {
        WINDOW_UNBOUNDED_FOLLOWING
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
    /// Set the value to use when the condition is true. Chain ``.otherwise(default)`` or ``.when(cond).then(val)`` to complete.
    ///
    /// Args:
    ///     value: Column expression for the "then" value.
    ///
    /// Returns:
    ///     ThenBuilder: Call ``.otherwise(default)`` or ``.when(cond).then(val)`` for chained when-then.
    fn then(&self, value: &PyColumn) -> PyThenBuilder {
        let when_then =
            polars::prelude::when(self.condition.clone()).then(value.inner.expr().clone());
        PyThenBuilder {
            state: PyWhenThenState::Single(when_then),
        }
    }
}

/// Python wrapper for ThenBuilder (.when(cond).then(val) and .otherwise(val)).
#[pyclass(name = "ThenBuilder")]
pub struct PyThenBuilder {
    state: PyWhenThenState,
}

#[pymethods]
impl PyThenBuilder {
    /// Chain another when-then clause (PySpark: when(a).then(x).when(b).then(y).otherwise(z)).
    ///
    /// Args:
    ///     condition: Boolean Column for the next branch.
    ///
    /// Returns:
    ///     ChainedWhenBuilder: Call ``.then(value)`` to add the branch, then ``.when()`` or ``.otherwise()`` on the returned ThenBuilder.
    fn when(&self, condition: &PyColumn) -> PyChainedWhenBuilder {
        let chained_when = match &self.state {
            PyWhenThenState::Single(t) => t.clone().when(condition.inner.expr().clone()),
            PyWhenThenState::Chained(ct) => ct.clone().when(condition.inner.expr().clone()),
        };
        PyChainedWhenBuilder {
            inner: chained_when,
        }
    }

    /// Set the default value when no when-then clause matches. Returns the complete conditional Column.
    ///
    /// Args:
    ///     value: Column expression for the "else" value.
    ///
    /// Returns:
    ///     Column: The full when-then-otherwise expression.
    fn otherwise(&self, value: &PyColumn) -> PyColumn {
        let expr = match &self.state {
            PyWhenThenState::Single(t) => t.clone().otherwise(value.inner.expr().clone()),
            PyWhenThenState::Chained(ct) => ct.clone().otherwise(value.inner.expr().clone()),
        };
        PyColumn {
            inner: RsColumn::from_expr(expr, None),
        }
    }
}

/// Python wrapper for chained when (when(a).then(x).when(b); call .then(y) to get back ThenBuilder).
#[pyclass(name = "ChainedWhenBuilder")]
pub struct PyChainedWhenBuilder {
    inner: ChainedWhen,
}

#[pymethods]
impl PyChainedWhenBuilder {
    /// Set the value for this when clause. Returns ThenBuilder so you can call .when() again or .otherwise().
    fn then(&self, value: &PyColumn) -> PyThenBuilder {
        let chained_then = self.inner.clone().then(value.inner.expr().clone());
        PyThenBuilder {
            state: PyWhenThenState::Chained(chained_then),
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
        let order_spec = window.order_by.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "Window.orderBy(...) must be called before row_number().over(window)",
            )
        })?;
        let (order_col, descending) = match order_spec {
            WindowOrderSpec::Column(c) => (c.clone(), self.descending),
            WindowOrderSpec::SortOrder(s) => {
                (RsColumn::from_expr(s.expr().clone(), None), s.descending)
            }
        };
        let refs: Vec<&str> = window.partition_by.iter().map(|s| s.as_str()).collect();
        let col = order_col.row_number(descending).over(&refs);
        Ok(PyColumn { inner: col })
    }
}

/// Python wrapper for dense_rank() in the Python API (PySpark-style). Use with Window: dense_rank().over(win).
#[pyclass(name = "DenseRank")]
pub struct PyDenseRank {
    pub(crate) descending: bool,
}

#[pymethods]
impl PyDenseRank {
    /// Apply this dense_rank() to a Window and return a Column expression.
    fn over(&self, window: &PyWindow) -> PyResult<PyColumn> {
        let order_spec = window.order_by.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "Window.orderBy(...) must be called before dense_rank().over(window)",
            )
        })?;
        let (order_col, descending) = match order_spec {
            WindowOrderSpec::Column(c) => (c.clone(), self.descending),
            WindowOrderSpec::SortOrder(s) => {
                (RsColumn::from_expr(s.expr().clone(), None), s.descending)
            }
        };
        let refs: Vec<&str> = window.partition_by.iter().map(|s| s.as_str()).collect();
        let col = order_col.dense_rank(descending).over(&refs);
        Ok(PyColumn { inner: col })
    }
}
