//! ANSI SQL semantics helpers (spark.sql.ansi.enabled).

use polars::prelude::*;
use robin_sparkless_core::EngineError;

use crate::udf_context::get_thread_ansi_enabled;

/// Build divide expression: ANSI-on throws on divide-by-zero at evaluation; off returns null (PySpark 3.5).
pub fn div_expr(left: Expr, right: Expr) -> Expr {
    if get_thread_ansi_enabled() {
        let args = [right.clone()];
        left.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_ansi_divide(cols)),
            &args,
            |_schema, fields| Ok(fields[0].clone()),
        )
    } else {
        let zero = right.clone().eq(lit(0i64));
        polars::prelude::when(zero)
            .then(lit(NULL))
            .otherwise(left / right)
    }
}

/// Build add expression respecting ANSI overflow rules.
pub fn add_expr(left: Expr, right: Expr) -> Expr {
    if get_thread_ansi_enabled() {
        let args = [right.clone()];
        left.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_ansi_add(cols)),
            &args,
            |_schema, fields| Ok(fields[0].clone()),
        )
    } else {
        left + right
    }
}

pub fn sub_expr(left: Expr, right: Expr) -> Expr {
    if get_thread_ansi_enabled() {
        let args = [right.clone()];
        left.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_ansi_subtract(cols)),
            &args,
            |_schema, fields| Ok(fields[0].clone()),
        )
    } else {
        left - right
    }
}

pub fn mul_expr(left: Expr, right: Expr) -> Expr {
    if get_thread_ansi_enabled() {
        let args = [right.clone()];
        left.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_ansi_multiply(cols)),
            &args,
            |_schema, fields| Ok(fields[0].clone()),
        )
    } else {
        left * right
    }
}

/// Map EngineError for ANSI violations (for eager paths).
pub fn ansi_err(msg: impl Into<String>) -> EngineError {
    EngineError::User(format!("[ARITHMETIC_OVERFLOW] {}", msg.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn div_expr_builds_without_panic() {
        let _ = div_expr(lit(1i64), lit(2i64));
    }
}
