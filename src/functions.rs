use pyo3::prelude::*;
use datafusion::prelude::*;
use crate::column::Column;
use crate::expression::python_to_literal;

pub fn register_functions(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(col, m)?)?;
    m.add_function(wrap_pyfunction!(lit, m)?)?;
    m.add_function(wrap_pyfunction!(count, m)?)?;
    m.add_function(wrap_pyfunction!(sum, m)?)?;
    m.add_function(wrap_pyfunction!(avg, m)?)?;
    m.add_function(wrap_pyfunction!(max, m)?)?;
    m.add_function(wrap_pyfunction!(min, m)?)?;
    m.add_function(wrap_pyfunction!(when, m)?)?;
    m.add_function(wrap_pyfunction!(coalesce, m)?)?;
    Ok(())
}

#[pyfunction]
pub fn col(name: &str) -> Column {
    Column::new(name.to_string())
}

#[pyfunction]
pub fn lit(value: &PyAny) -> PyResult<Column> {
    let expr = python_to_literal(value)?;
    Ok(Column::from_expr(expr, None))
}

#[pyfunction]
pub fn count(col: &Column) -> Column {
    let expr = count(col.expr().clone());
    Column::from_expr(expr, Some("count".to_string()))
}

#[pyfunction]
pub fn sum(col: &Column) -> Column {
    let expr = sum(col.expr().clone());
    Column::from_expr(expr, Some("sum".to_string()))
}

#[pyfunction]
pub fn avg(col: &Column) -> Column {
    let expr = avg(col.expr().clone());
    Column::from_expr(expr, Some("avg".to_string()))
}

#[pyfunction]
pub fn max(col: &Column) -> Column {
    let expr = max(col.expr().clone());
    Column::from_expr(expr, Some("max".to_string()))
}

#[pyfunction]
pub fn min(col: &Column) -> Column {
    let expr = min(col.expr().clone());
    Column::from_expr(expr, Some("min".to_string()))
}

#[pyfunction]
pub fn when(condition: &Column, value: &PyAny) -> PyResult<Column> {
    use datafusion::logical_expr::Expr;
    use crate::expression::python_to_literal;
    
    let value_expr = python_to_literal(value)?;
    // Note: DataFusion's when/then/otherwise is more complex
    // For now, create a simple case expression
    let expr = Expr::Case {
        expr: None,
        when_then_expr: vec![(condition.expr().clone(), value_expr)],
        else_expr: None,
    };
    Ok(Column::from_expr(expr, Some("when".to_string())))
}

#[pyfunction]
pub fn coalesce(cols: Vec<&Column>) -> PyResult<Column> {
    use datafusion::logical_expr::Expr;
    
    if cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "coalesce requires at least one column"
        ));
    }
    
    let mut expr = cols[0].expr().clone();
    for col in cols.iter().skip(1) {
        // Build coalesce: COALESCE(expr1, expr2, ...)
        // DataFusion doesn't have a direct coalesce, so we use a case expression
        // For simplicity, we'll use the first non-null
        expr = Expr::Case {
            expr: None,
            when_then_expr: vec![(
                expr.is_not_null(),
                expr.clone()
            )],
            else_expr: Some(Box::new(col.expr().clone())),
        };
    }
    
    Ok(Column::from_expr(expr, Some("coalesce".to_string())))
}
