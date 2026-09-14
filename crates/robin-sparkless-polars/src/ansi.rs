//! ANSI SQL semantics helpers (spark.sql.ansi.enabled).

use polars::prelude::*;
use robin_sparkless_core::EngineError;

use crate::udf_context::get_thread_ansi_enabled;

pub(crate) fn arithmetic_field(fields: &[Field]) -> PolarsResult<Field> {
    let dtype = match (&fields[0].dtype, &fields[1].dtype) {
        (left, DataType::Unknown(_)) => left.clone(),
        (DataType::Unknown(_), right) => right.clone(),
        (DataType::Date, DataType::Int32 | DataType::Int64) => DataType::Date,
        (DataType::Datetime(unit, time_zone), DataType::Duration(_)) => {
            DataType::Datetime(*unit, time_zone.clone())
        }
        (DataType::Int32, DataType::Int32) => DataType::Int32,
        (DataType::Int32 | DataType::Int64, DataType::Int32 | DataType::Int64) => DataType::Int64,
        (DataType::Float32, DataType::Float32) => DataType::Float32,
        _ => DataType::Float64,
    };
    Ok(Field::new(fields[0].name().clone(), dtype))
}

/// Build divide expression: ANSI-on throws on divide-by-zero at evaluation; off returns null (PySpark 3.5).
pub fn div_expr(left: Expr, right: Expr) -> Expr {
    if get_thread_ansi_enabled() {
        let args = [right.clone()];
        left.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_ansi_divide(cols)),
            &args,
            |_schema, fields| arithmetic_field(fields),
        )
    } else {
        let zero_int = right.clone().eq(lit(0i64));
        let zero_float = right.clone().eq(lit(0.0f64));
        let zero = zero_int.or(zero_float);
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
            |_schema, fields| arithmetic_field(fields),
        )
    } else {
        let args = [right.clone()];
        left.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_try_add(cols)),
            &args,
            |_schema, fields| arithmetic_field(fields),
        )
    }
}

pub fn sub_expr(left: Expr, right: Expr) -> Expr {
    if get_thread_ansi_enabled() {
        let args = [right.clone()];
        left.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_ansi_subtract(cols)),
            &args,
            |_schema, fields| arithmetic_field(fields),
        )
    } else {
        let args = [right.clone()];
        left.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_try_subtract(cols)),
            &args,
            |_schema, fields| arithmetic_field(fields),
        )
    }
}

pub fn mul_expr(left: Expr, right: Expr) -> Expr {
    if get_thread_ansi_enabled() {
        let args = [right.clone()];
        left.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_ansi_multiply(cols)),
            &args,
            |_schema, fields| arithmetic_field(fields),
        )
    } else {
        let args = [right.clone()];
        left.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_try_multiply(cols)),
            &args,
            |_schema, fields| arithmetic_field(fields),
        )
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
