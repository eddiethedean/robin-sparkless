//! Column reference and literal builders.

use crate::column::Column;
use polars::prelude::{Expr, lit};

/// Get a column by name
pub fn col(name: &str) -> Column {
    Column::new(name.to_string())
}

/// Grouping set marker (PySpark grouping). Stub: returns 0 (no GROUPING SETS in robin-sparkless).
pub fn grouping(column: &Column) -> Column {
    let _ = column;
    Column::from_expr(lit(0i32), Some("grouping".to_string()))
}

/// Grouping set id (PySpark grouping_id). Stub: returns 0.
pub fn grouping_id(_columns: &[Column]) -> Column {
    Column::from_expr(lit(0i64), Some("grouping_id".to_string()))
}

/// Create a literal column from a value
pub fn lit_i32(value: i32) -> Column {
    let expr: Expr = lit(value);
    Column::from_expr(expr, None)
}

pub fn lit_i64(value: i64) -> Column {
    let expr: Expr = lit(value);
    Column::from_expr(expr, None)
}

pub fn lit_f64(value: f64) -> Column {
    let expr: Expr = lit(value);
    Column::from_expr(expr, None)
}

pub fn lit_bool(value: bool) -> Column {
    let expr: Expr = lit(value);
    Column::from_expr(expr, None)
}

pub fn lit_str(value: &str) -> Column {
    let expr: Expr = lit(value);
    Column::from_expr(expr, None)
}

/// Typed null literal column. Returns `Err` on unknown type name.
/// See [`parse_type_name`](super::types::parse_type_name) for supported type strings (e.g. `"boolean"`, `"string"`, `"bigint"`).
pub fn lit_null(dtype: &str) -> Result<Column, String> {
    Column::lit_null(dtype)
}
