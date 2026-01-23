use pyo3::prelude::*;
use datafusion::prelude::*;
use datafusion::logical_expr::{Expr, Operator};
use arrow::datatypes::DataType;
use crate::column::Column;

/// Convert a PySpark Column to a DataFusion Expr
pub fn column_to_expr(col: &Column) -> PyResult<Expr> {
    col.to_expr()
}

/// Convert a Python value to a DataFusion literal Expr
pub fn python_to_literal(value: &PyAny) -> PyResult<Expr> {
    if let Ok(val) = value.extract::<i64>() {
        Ok(lit(val))
    } else if let Ok(val) = value.extract::<i32>() {
        Ok(lit(val))
    } else if let Ok(val) = value.extract::<f64>() {
        Ok(lit(val))
    } else if let Ok(val) = value.extract::<f32>() {
        Ok(lit(val))
    } else if let Ok(val) = value.extract::<bool>() {
        Ok(lit(val))
    } else if let Ok(val) = value.extract::<String>() {
        Ok(lit(val))
    } else if value.is_none() {
        // NULL literal
        Ok(Expr::Literal(datafusion::common::ScalarValue::Null))
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            format!("Unsupported literal type: {:?}", value.get_type().name())
        ))
    }
}

/// Parse a string expression to DataFusion Expr
/// This is a simplified parser - in production you'd use DataFusion's SQL parser
pub fn parse_string_expr(s: &str, schema: Option<&arrow::datatypes::Schema>) -> PyResult<Expr> {
    // For now, treat as column reference
    // In production, you'd parse the string properly
    Ok(col(s))
}

/// Convert PyAny to Expr (handles Column, literals, or strings)
pub fn pyany_to_expr(value: &PyAny, schema: Option<&arrow::datatypes::Schema>) -> PyResult<Expr> {
    // Try Column first
    if let Ok(col_ref) = value.extract::<PyRef<Column>>() {
        return col_ref.to_expr();
    }
    
    // Try literal
    if let Ok(expr) = python_to_literal(value) {
        return Ok(expr);
    }
    
    // Try string
    if let Ok(s) = value.extract::<String>() {
        return parse_string_expr(&s, schema);
    }
    
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Cannot convert to expression. Expected Column, literal, or string."
    ))
}

/// Build binary expression from operator and operands
pub fn build_binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

/// Build comparison operators
pub fn build_comparison(left: Expr, op_str: &str, right: Expr) -> PyResult<Expr> {
    let op = match op_str {
        "<" => Operator::Lt,
        "<=" => Operator::LtEq,
        ">" => Operator::Gt,
        ">=" => Operator::GtEq,
        "=" | "==" => Operator::Eq,
        "!=" | "<>" => Operator::NotEq,
        _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Unsupported comparison operator: {}", op_str)
        )),
    };
    Ok(build_binary_expr(left, op, right))
}

/// Build arithmetic operators
pub fn build_arithmetic(left: Expr, op_str: &str, right: Expr) -> PyResult<Expr> {
    let op = match op_str {
        "+" => Operator::Plus,
        "-" => Operator::Minus,
        "*" => Operator::Multiply,
        "/" => Operator::Divide,
        "%" => Operator::Modulo,
        _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Unsupported arithmetic operator: {}", op_str)
        )),
    };
    Ok(build_binary_expr(left, op, right))
}
