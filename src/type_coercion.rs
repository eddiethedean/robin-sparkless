use polars::prelude::*;

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
                        "Type coercion: unsupported type precedence {:?}. Supported: Int32, Int64, Float32, Float64, String.",
                        target_prec
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
                        "Type coercion: cannot find common type for {:?} and {:?}. Hint: use cast() to align types, or ensure both are numeric or both are string.",
                        left, right
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
