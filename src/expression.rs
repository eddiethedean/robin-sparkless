use polars::prelude::*;
use crate::column::Column;

/// Convert a Column to a Polars Expr
pub fn column_to_expr(col: &Column) -> Expr {
    col.expr().clone()
}

/// Helper functions to create literal expressions
pub fn lit_i32(val: i32) -> Expr {
    lit(val)
}

pub fn lit_i64(val: i64) -> Expr {
    lit(val)
}

pub fn lit_f64(val: f64) -> Expr {
    lit(val)
}

pub fn lit_bool(val: bool) -> Expr {
    lit(val)
}

pub fn lit_str(val: &str) -> Expr {
    lit(val)
}
