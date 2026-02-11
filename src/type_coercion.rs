use polars::prelude::*;

/// Comparison operators of interest for PySpark-style coercion.
///
/// We keep a local alias to avoid leaking polars::prelude in public signatures unnecessarily.
pub type CompareOp = polars::prelude::Operator;

/// Type precedence for ANSI SQL type coercion
/// Higher precedence types can be coerced to, lower precedence types are coerced from
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)] // Decimal reserved for future use
enum TypePrecedence {
    Int = 1,
    Long = 2,
    Decimal = 3,
    Float = 4,
    Double = 5,
    String = 6,
}

/// Convert Polars DataType to TypePrecedence
fn dtype_to_precedence(dtype: &DataType) -> Option<TypePrecedence> {
    match dtype {
        DataType::Int32 => Some(TypePrecedence::Int),
        DataType::Int64 => Some(TypePrecedence::Long),
        DataType::Float32 => Some(TypePrecedence::Float),
        DataType::Float64 => Some(TypePrecedence::Double),
        DataType::String => Some(TypePrecedence::String),
        // TODO: Add Decimal type when available
        _ => None,
    }
}

/// Determine the common type for two columns based on PySpark's type precedence rules
/// Returns the tightest (highest precedence) common type that both can be coerced to
pub fn find_common_type(left: &DataType, right: &DataType) -> Result<DataType, PolarsError> {
    let left_prec = dtype_to_precedence(left);
    let right_prec = dtype_to_precedence(right);

    match (left_prec, right_prec) {
        (Some(l), Some(r)) => {
            // Return the type with higher precedence
            let target_prec = if l > r { l } else { r };
            match target_prec {
                TypePrecedence::Int => Ok(DataType::Int32),
                TypePrecedence::Long => Ok(DataType::Int64),
                TypePrecedence::Float => Ok(DataType::Float32),
                TypePrecedence::Double => Ok(DataType::Float64),
                TypePrecedence::String => Ok(DataType::String),
                _ => Err(PolarsError::ComputeError(
                    format!(
                        "Type coercion: unsupported type precedence {target_prec:?}. Supported: Int32, Int64, Float32, Float64, String."
                    )
                    .into(),
                )),
            }
        }
        _ => {
            // If types don't match known precedence, try to find a common numeric type
            if is_numeric(left) && is_numeric(right) {
                // For numeric types, prefer double for comparisons
                Ok(DataType::Float64)
            } else if left == right {
                // Same type, no coercion needed
                Ok(left.clone())
            } else {
                Err(PolarsError::ComputeError(
                    format!(
                        "Type coercion: cannot find common type for {left:?} and {right:?}. Hint: use cast() to align types, or ensure both are numeric or both are string."
                    )
                    .into(),
                ))
            }
        }
    }
}

/// Check if a DataType is numeric
fn is_numeric(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Coerce a column expression to a target type
pub fn coerce_to_type(expr: Expr, target_type: DataType) -> Expr {
    expr.cast(target_type)
}

/// Coerce two expressions to their common type for comparison
pub fn coerce_for_comparison(
    left: Expr,
    right: Expr,
    left_type: &DataType,
    right_type: &DataType,
) -> Result<(Expr, Expr), PolarsError> {
    if left_type == right_type {
        // Same type, no coercion needed
        return Ok((left, right));
    }

    let common_type = find_common_type(left_type, right_type)?;

    let left_coerced = if left_type == &common_type {
        left
    } else {
        coerce_to_type(left, common_type.clone())
    };

    let right_coerced = if right_type == &common_type {
        right
    } else {
        coerce_to_type(right, common_type)
    };

    Ok((left_coerced, right_coerced))
}

/// Coerce two expressions for PySpark-style comparison semantics.
///
/// This extends [`coerce_for_comparison`] to handle string–numeric combinations by
/// parsing string values to numbers (double) instead of erroring, mirroring PySpark:
///
/// - String values that parse as numbers (e.g. "123", " 45.6 ") are compared numerically.
/// - Non‑numeric strings behave as null under numeric comparison (non-matching in filters).
///
/// For plain numeric–numeric inputs, it delegates to [`coerce_for_comparison`].
pub fn coerce_for_pyspark_comparison(
    left: Expr,
    right: Expr,
    left_type: &DataType,
    right_type: &DataType,
    _op: &CompareOp,
) -> Result<(Expr, Expr), PolarsError> {
    use crate::column::Column;

    // Fast-path: both numeric -> existing numeric coercion.
    if is_numeric(left_type) && is_numeric(right_type) {
        return coerce_for_comparison(left, right, left_type, right_type);
    }

    // Helper to wrap an Expr in try_to_number (double) semantics when it represents a value
    // that should be interpreted as numeric if possible.
    fn wrap_try_to_number(expr: Expr) -> Result<Expr, PolarsError> {
        let col = Column::from_expr(expr, None);
        let coerced = crate::functions::try_to_number(&col, None)
            .map_err(|e| PolarsError::ComputeError(e.into()))?;
        Ok(coerced.into_expr())
    }

    // String–numeric (or numeric–string): route string side through try_to_number and
    // cast numeric side to Float64 so both sides line up.
    let string_numeric = (left_type == &DataType::String && is_numeric(right_type))
        || (right_type == &DataType::String && is_numeric(left_type));

    if string_numeric {
        let left_out = if left_type == &DataType::String {
            wrap_try_to_number(left)?
        } else if is_numeric(left_type) {
            coerce_to_type(left, DataType::Float64)
        } else {
            left
        };

        let right_out = if right_type == &DataType::String {
            wrap_try_to_number(right)?
        } else if is_numeric(right_type) {
            coerce_to_type(right, DataType::Float64)
        } else {
            right
        };

        return Ok((left_out, right_out));
    }

    // Equal non-numeric types: leave as-is for now.
    if left_type == right_type && !is_numeric(left_type) {
        return Ok((left, right));
    }

    // Fallback to generic comparison coercion (may error with a clear message).
    coerce_for_comparison(left, right, left_type, right_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{df, IntoLazy};

    #[test]
    fn numeric_numeric_uses_standard_coercion() {
        let df = df!(
            "a" => &[1i32, 2, 3],
            "b" => &[1i64, 2, 3]
        )
        .unwrap();

        let a = col("a");
        let b = col("b");
        let (ac, bc) = coerce_for_pyspark_comparison(
            a.clone(),
            b.clone(),
            &DataType::Int32,
            &DataType::Int64,
            &CompareOp::Eq,
        )
        .unwrap();

        // After coercion both sides should be comparable without error and all rows match.
        let out = df.lazy().filter(ac.eq(bc)).collect().unwrap();
        assert_eq!(out.height(), 3);
    }

    #[test]
    fn string_numeric_uses_try_to_number() {
        let df = df!(
            "s" => &["123", " 45.5 ", "abc"],
            "n" => &[123i32, 46, 0]
        )
        .unwrap();

        let s_expr = col("s");
        let n_expr = col("n");

        let (s_coerced, n_coerced) = coerce_for_pyspark_comparison(
            s_expr.clone(),
            n_expr.clone(),
            &DataType::String,
            &DataType::Int32,
            &CompareOp::Eq,
        )
        .unwrap();

        let out = df.lazy().filter(s_coerced.eq(n_coerced)).collect().unwrap();

        // Only the first row matches ("123" == 123); " 45.5 " != 46, "abc" -> null (non-match).
        assert_eq!(out.height(), 1);
    }
}
