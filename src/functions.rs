use pyo3::prelude::*;
use datafusion::prelude::*;
use datafusion::logical_expr::Expr;
use crate::column::Column;
use crate::expression::python_to_literal;

pub fn register_functions(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(crate::functions::col, m)?)?;
    m.add_function(wrap_pyfunction!(crate::functions::lit, m)?)?;
    m.add_function(wrap_pyfunction!(crate::functions::count, m)?)?;
    m.add_function(wrap_pyfunction!(crate::functions::sum, m)?)?;
    m.add_function(wrap_pyfunction!(crate::functions::avg, m)?)?;
    m.add_function(wrap_pyfunction!(crate::functions::max, m)?)?;
    m.add_function(wrap_pyfunction!(crate::functions::min, m)?)?;
    m.add_function(wrap_pyfunction!(crate::functions::when, m)?)?;
    m.add_function(wrap_pyfunction!(crate::functions::coalesce, m)?)?;
    Ok(())
}

#[pyfunction]
pub fn col(name: &str) -> Column {
    Column::from_name(name.to_string())
}

#[pyfunction]
pub fn lit(value: &PyAny) -> PyResult<Column> {
    let expr = python_to_literal(value)?;
    Ok(Column::from_expr(expr, None))
}

// Helper to get built-in aggregate function from a default context
// In production, this would get functions from the actual SessionContext
pub(crate) fn get_builtin_aggregate(name: &str) -> std::sync::Arc<datafusion::logical_expr::AggregateUDF> {
    use std::sync::Arc;
    use datafusion::execution::context::SessionContext;
    
    // Get built-in aggregate function from a default context
    // This is a workaround - proper implementation would cache these or get from the actual context
    let ctx = SessionContext::new();
    let state = ctx.state();
    
    // Try to get the function from the registry
    if let Some(udf) = state.aggregate_functions().get(name) {
        return udf.clone();
    }
    
    // Fallback: create a minimal placeholder (will fail at runtime but allows compilation)
    // In production, all built-in functions should be registered
    panic!("Built-in aggregate function '{}' not found in registry. This should not happen.", name);
}

#[pyfunction]
pub fn count(col: &Column) -> Column {
    use datafusion::logical_expr::expr::AggregateFunction;
    use std::sync::Arc;
    
    let udf = get_builtin_aggregate("count");
    let expr = Expr::AggregateFunction(AggregateFunction {
        func: udf,
        distinct: false,
        args: vec![col.expr().clone()],
        filter: None,
        order_by: None,
        null_treatment: None,
    });
    Column::from_expr(expr, Some("count".to_string()))
}

#[pyfunction]
pub fn sum(col: &Column) -> Column {
    use datafusion::logical_expr::expr::AggregateFunction;
    use std::sync::Arc;
    
    let udf = get_builtin_aggregate("sum");
    let expr = Expr::AggregateFunction(AggregateFunction {
        func: udf,
        distinct: false,
        args: vec![col.expr().clone()],
        filter: None,
        order_by: None,
        null_treatment: None,
    });
    Column::from_expr(expr, Some("sum".to_string()))
}

#[pyfunction]
pub fn avg(col: &Column) -> Column {
    use datafusion::logical_expr::expr::AggregateFunction;
    use std::sync::Arc;
    
    let udf = get_builtin_aggregate("avg");
    let expr = Expr::AggregateFunction(AggregateFunction {
        func: udf,
        distinct: false,
        args: vec![col.expr().clone()],
        filter: None,
        order_by: None,
        null_treatment: None,
    });
    Column::from_expr(expr, Some("avg".to_string()))
}

#[pyfunction]
pub fn max(col: &Column) -> Column {
    use datafusion::logical_expr::expr::AggregateFunction;
    use std::sync::Arc;
    
    let udf = get_builtin_aggregate("max");
    let expr = Expr::AggregateFunction(AggregateFunction {
        func: udf,
        distinct: false,
        args: vec![col.expr().clone()],
        filter: None,
        order_by: None,
        null_treatment: None,
    });
    Column::from_expr(expr, Some("max".to_string()))
}

#[pyfunction]
pub fn min(col: &Column) -> Column {
    use datafusion::logical_expr::expr::AggregateFunction;
    use std::sync::Arc;
    
    let udf = get_builtin_aggregate("min");
    let expr = Expr::AggregateFunction(AggregateFunction {
        func: udf,
        distinct: false,
        args: vec![col.expr().clone()],
        filter: None,
        order_by: None,
        null_treatment: None,
    });
    Column::from_expr(expr, Some("min".to_string()))
}

#[pyfunction]
pub fn when(condition: &Column, value: &PyAny) -> PyResult<Column> {
    use datafusion::logical_expr::Expr;
    use crate::expression::python_to_literal;
    
    let value_expr = python_to_literal(value)?;
    // Use DataFusion's when/then/otherwise API
    // For now, create a simple conditional expression
    // Note: This is simplified - full Case API may be more complex
    let expr = datafusion::prelude::when(condition.expr().clone(), value_expr)
        .otherwise(datafusion::prelude::lit(datafusion::common::ScalarValue::Null))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Failed to create when expression: {}", e)
        ))?;
    Ok(Column::from_expr(expr, Some("when".to_string())))
}

#[pyfunction]
pub fn coalesce(cols: Vec<PyRef<Column>>) -> PyResult<Column> {
    use datafusion::logical_expr::Expr;
    
    if cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "coalesce requires at least one column"
        ));
    }
    
    // Use DataFusion's coalesce function if available, otherwise build manually
    let mut expr = cols[0].expr().clone();
    for col_expr in cols.iter().skip(1) {
        // Build coalesce using when/then/otherwise
        expr = datafusion::prelude::when(expr.clone().is_not_null(), expr.clone())
            .otherwise(col_expr.expr().clone())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create coalesce expression: {}", e)
            ))?;
    }
    
    Ok(Column::from_expr(expr, Some("coalesce".to_string())))
}
