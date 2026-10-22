use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use polars::prelude::*;

fn decimal_ordering_key_expr(expr: Expr, scale: usize, integral_width: usize) -> Expr {
    expr.map(
        move |column| {
            let series = column.as_materialized_series();
            let values = if let DataType::Decimal(_, source_scale) = series.dtype() {
                let multiplier = BigInt::from(10).pow(scale.saturating_sub(*source_scale) as u32);
                let mut values = Vec::with_capacity(series.len());
                for idx in 0..series.len() {
                    values.push(match series.get(idx)? {
                        AnyValue::Decimal(value, _, _) => Some(BigInt::from(value) * &multiplier),
                        AnyValue::Null => None,
                        _ => {
                            return Err(PolarsError::ComputeError(
                                "expected a decimal value for decimal comparison".into(),
                            ));
                        }
                    });
                }
                values
            } else {
                let mut values = Vec::with_capacity(series.len());
                let integer_factor = BigInt::from(10).pow(scale as u32);
                for idx in 0..series.len() {
                    let value = series.get(idx)?;
                    let value = match value {
                        AnyValue::Null => None,
                        AnyValue::Int8(value) => Some(BigInt::from(value) * &integer_factor),
                        AnyValue::Int16(value) => Some(BigInt::from(value) * &integer_factor),
                        AnyValue::Int32(value) => Some(BigInt::from(value) * &integer_factor),
                        AnyValue::Int64(value) => Some(BigInt::from(value) * &integer_factor),
                        AnyValue::UInt8(value) => Some(BigInt::from(value) * &integer_factor),
                        AnyValue::UInt16(value) => Some(BigInt::from(value) * &integer_factor),
                        AnyValue::UInt32(value) => Some(BigInt::from(value) * &integer_factor),
                        AnyValue::UInt64(value) => Some(BigInt::from(value) * &integer_factor),
                        AnyValue::Float32(value) => {
                            decimal_string_to_scaled(&value.to_string(), scale)?
                        }
                        AnyValue::Float64(value) => {
                            decimal_string_to_scaled(&value.to_string(), scale)?
                        }
                        _ => {
                            return Err(PolarsError::ComputeError(
                                "expected a numeric value for decimal comparison".into(),
                            ));
                        }
                    };
                    values.push(value);
                }
                values
            };

            let factor = BigInt::from(10).pow(scale as u32);
            let mut keys = Vec::with_capacity(values.len());
            for value in values {
                let Some(value) = value else {
                    keys.push(None);
                    continue;
                };
                let absolute = value.abs();
                let integer = (&absolute / &factor).to_string();
                if integer.len() > integral_width {
                    return Err(PolarsError::ComputeError(
                        "decimal comparison value exceeds supported precision".into(),
                    ));
                }
                let fraction = (&absolute % &factor).to_string();
                let digits = format!(
                    "{}{}{}{}",
                    "0".repeat(integral_width - integer.len()),
                    integer,
                    "0".repeat(scale.saturating_sub(fraction.len())),
                    fraction
                );
                if value.is_negative() {
                    let digits: String = digits
                        .bytes()
                        .map(|digit| (b'9' - (digit - b'0')) as char)
                        .collect();
                    keys.push(Some(format!("0{digits}")));
                } else {
                    keys.push(Some(format!("1{digits}")));
                }
            }
            Ok(Series::new(series.name().clone(), keys).into())
        },
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::String)),
    )
}

fn decimal_string_to_scaled(value: &str, scale: usize) -> PolarsResult<Option<BigInt>> {
    let (mantissa, exponent) = match value.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (
            mantissa,
            exponent.parse::<i32>().map_err(|_| {
                PolarsError::ComputeError("invalid exponent in decimal comparison".into())
            })?,
        ),
        None => (value, 0),
    };
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.trim_start_matches(['-', '+']);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if (whole.is_empty() && fraction.is_empty())
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Ok(None);
    }
    let digits = format!("{whole}{fraction}");
    let mut scaled = BigInt::parse_bytes(digits.as_bytes(), 10).ok_or_else(|| {
        PolarsError::ComputeError("invalid numeric literal in decimal comparison".into())
    })?;
    let shift = scale as i64 - fraction.len() as i64 + i64::from(exponent);
    if shift >= 0 {
        if shift > 4096 {
            return Err(PolarsError::ComputeError(
                "decimal comparison exponent exceeds supported precision".into(),
            ));
        }
        scaled *= BigInt::from(10).pow(shift as u32);
    } else {
        let discarded = shift.unsigned_abs();
        if discarded > 4096 {
            scaled = BigInt::zero();
        } else {
            let divisor = BigInt::from(10).pow(discarded as u32);
            let remainder = &scaled % &divisor;
            scaled /= &divisor;
            if remainder.abs() * 2 >= divisor {
                scaled += 1;
            }
        }
    }
    if negative {
        scaled = -scaled;
    }
    Ok(Some(scaled))
}

/// Comparison operators of interest for PySpark-style coercion.
///
/// We keep a local alias to avoid leaking polars::prelude in public signatures unnecessarily.
pub type CompareOp = polars::prelude::Operator;

/// Type precedence for ANSI SQL type coercion
/// Higher precedence types can be coerced to, lower precedence types are coerced from
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)] // Decimal reserved for future use
enum TypePrecedence {
    Short = 1,
    Int = 2,
    Long = 3,
    Decimal = 4,
    Float = 5,
    Double = 6,
    String = 7,
}

/// Convert Polars DataType to TypePrecedence
fn dtype_to_precedence(dtype: &DataType) -> Option<TypePrecedence> {
    match dtype {
        DataType::Int16 => Some(TypePrecedence::Short),
        DataType::Int32 => Some(TypePrecedence::Int),
        DataType::Int64 => Some(TypePrecedence::Long),
        DataType::Float32 => Some(TypePrecedence::Float),
        DataType::Float64 => Some(TypePrecedence::Double),
        DataType::String => Some(TypePrecedence::String),
        // Decimal: add when Polars exposes Decimal in public API / dtype set we use
        _ => None,
    }
}

/// Determine the common type for two columns based on PySpark's type precedence rules
/// Returns the tightest (highest precedence) common type that both can be coerced to.
/// #681: Unknown dtype is treated as String so join/union with Int64 vs Unknown coerce to String (PySpark parity).
pub fn find_common_type(left: &DataType, right: &DataType) -> Result<DataType, PolarsError> {
    // #1590: untyped null adopts the other side's type (unionByName with lit(None) vs array).
    if left == &DataType::Null {
        return Ok(right.clone());
    }
    if right == &DataType::Null {
        return Ok(left.clone());
    }
    if left == right {
        return Ok(left.clone());
    }

    fn decimal_common_type(
        left_precision: usize,
        left_scale: usize,
        right_precision: usize,
        right_scale: usize,
    ) -> DataType {
        let integral_digits = left_precision
            .saturating_sub(left_scale)
            .max(right_precision.saturating_sub(right_scale));
        let scale = left_scale.max(right_scale);
        // Keep the integral capacity when the common type reaches Spark's
        // precision limit; reduce scale only by the amount needed to fit.
        let scale = scale.min(38usize.saturating_sub(integral_digits));
        let precision = integral_digits.saturating_add(scale).clamp(1, 38);
        DataType::Decimal(precision, scale)
    }

    match (left, right) {
        (DataType::List(left_inner), DataType::List(right_inner)) => {
            return Ok(DataType::List(Box::new(find_common_type(
                left_inner,
                right_inner,
            )?)));
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields))
            if left_fields.len() == right_fields.len()
                && left_fields
                    .iter()
                    .zip(right_fields)
                    .all(|(left, right)| left.name() == right.name()) =>
        {
            let fields = left_fields
                .iter()
                .zip(right_fields)
                .map(|(left, right)| {
                    Ok(Field::new(
                        left.name().clone(),
                        find_common_type(left.dtype(), right.dtype())?,
                    ))
                })
                .collect::<Result<Vec<_>, PolarsError>>()?;
            return Ok(DataType::Struct(fields));
        }
        (DataType::Decimal(lp, ls), DataType::Decimal(rp, rs)) => {
            return Ok(decimal_common_type(*lp, *ls, *rp, *rs));
        }
        (DataType::Decimal(p, s), integer) | (integer, DataType::Decimal(p, s))
            if matches!(
                integer,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ) =>
        {
            let integer_precision = match integer {
                DataType::Int8 | DataType::UInt8 => 3,
                DataType::Int16 | DataType::UInt16 => 5,
                DataType::Int32 | DataType::UInt32 => 10,
                DataType::Int64 => 20,
                DataType::UInt64 => 20,
                _ => unreachable!(),
            };
            return Ok(decimal_common_type(*p, *s, integer_precision, 0));
        }
        _ => {}
    }

    // #615 / #1596: Date vs Datetime — cast date to datetime (start-of-day) for join/compare parity.
    let date_vs_datetime = (left == &DataType::Date && matches!(right, DataType::Datetime(_, _)))
        || (matches!(left, DataType::Datetime(_, _)) && right == &DataType::Date);
    if date_vs_datetime {
        return Ok(if matches!(left, DataType::Datetime(_, _)) {
            left.clone()
        } else {
            right.clone()
        });
    }

    let left_norm = if matches!(left, DataType::Unknown(_)) {
        DataType::String
    } else {
        left.clone()
    };
    let right_norm = if matches!(right, DataType::Unknown(_)) {
        DataType::String
    } else {
        right.clone()
    };
    let left = &left_norm;
    let right = &right_norm;
    let left_prec = dtype_to_precedence(left);
    let right_prec = dtype_to_precedence(right);

    match (left_prec, right_prec) {
        (Some(l), Some(r)) => {
            // Return the type with higher precedence
            let target_prec = if l > r { l } else { r };
            match target_prec {
                TypePrecedence::Short => Ok(DataType::Int16),
                TypePrecedence::Int => Ok(DataType::Int32),
                TypePrecedence::Long => Ok(DataType::Int64),
                TypePrecedence::Float => Ok(DataType::Float32),
                TypePrecedence::Double => Ok(DataType::Float64),
                TypePrecedence::String => Ok(DataType::String),
                _ => Err(PolarsError::ComputeError(
                    format!(
                        "Type coercion: unsupported type precedence {target_prec:?}. Supported: Int16, Int32, Int64, Float32, Float64, String."
                    )
                    .into(),
                )),
            }
        }
        _ => {
            // If types don't match known precedence, try to find a common type
            if is_numeric(left) && is_numeric(right) {
                Ok(DataType::Float64)
            } else if left == right {
                Ok(left.clone())
            } else if left == &DataType::String || right == &DataType::String {
                // #613: unionByName string vs numeric -> coerce to String (PySpark parity)
                Ok(DataType::String)
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

/// Common type for join keys (#850). When one side is numeric and the other is String, prefer the
/// numeric type so the result schema has a numeric key column (PySpark parity).
pub fn find_common_type_for_join(
    left: &DataType,
    right: &DataType,
) -> Result<DataType, PolarsError> {
    let left_norm = if matches!(left, DataType::Unknown(_)) {
        DataType::String
    } else {
        left.clone()
    };
    let right_norm = if matches!(right, DataType::Unknown(_)) {
        DataType::String
    } else {
        right.clone()
    };
    let left = &left_norm;
    let right = &right_norm;
    // Prefer numeric when one is numeric and one is string (PySpark result schema has numeric key).
    if is_numeric(left) && right == &DataType::String {
        return Ok(left.clone());
    }
    if left == &DataType::String && is_numeric(right) {
        return Ok(right.clone());
    }
    find_common_type(left, right)
}

/// Build (left_expr, right_expr) that cast two columns to a common type and alias to the same name.
/// Use in join key coercion and union_by_name when both sides have the column but types differ.
pub fn coerce_expr_pair(
    left_name: &str,
    right_name: &str,
    left_dtype: &DataType,
    right_dtype: &DataType,
    alias: &str,
) -> Result<(Expr, Expr), PolarsError> {
    let common = find_common_type(left_dtype, right_dtype)?;
    Ok((
        coerce_expr_to_common_type(col(left_name), left_dtype, &common).alias(alias),
        coerce_expr_to_common_type(col(right_name), right_dtype, &common).alias(alias),
    ))
}

/// Like coerce_expr_pair but uses find_common_type_for_join so join result key is numeric when one side is numeric (#850).
pub fn coerce_expr_pair_for_join(
    left_name: &str,
    right_name: &str,
    left_dtype: &DataType,
    right_dtype: &DataType,
    alias: &str,
) -> Result<(Expr, Expr), PolarsError> {
    let common = find_common_type_for_join(left_dtype, right_dtype)?;
    Ok((
        coerce_expr_to_common_type(col(left_name), left_dtype, &common).alias(alias),
        coerce_expr_to_common_type(col(right_name), right_dtype, &common).alias(alias),
    ))
}

/// Convert a numeric expression to Float64 without double-rounding decimals.
fn coerce_numeric_to_float64(expr: Expr, source_type: &DataType) -> Expr {
    if matches!(source_type, DataType::Decimal(..)) {
        expr.cast(DataType::String).cast(DataType::Float64)
    } else {
        expr.cast(DataType::Float64)
    }
}

/// Coerce an expression to a common type while preserving correctly rounded
/// Decimal-to-Double conversion. Polars converts Decimal coefficients to f64
/// before dividing by the scale factor, which can double-round large values.
pub fn coerce_expr_to_common_type(
    expr: Expr,
    source_type: &DataType,
    common_type: &DataType,
) -> Expr {
    if matches!(source_type, DataType::Decimal(..)) && common_type == &DataType::Float64 {
        coerce_numeric_to_float64(expr, source_type)
    } else {
        expr.cast(common_type.clone())
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
            | DataType::Decimal(_, _)
    )
}

/// Public wrapper for use in DataFrame coercion (#974).
pub fn is_numeric_public(dtype: &DataType) -> bool {
    is_numeric(dtype)
}

/// Check if a DataType is date/datetime (temporal types that we can cast from string via try_cast).
fn is_date_or_datetime(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Date | DataType::Datetime(_, _))
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

    fn decimal_shape_of_literal(expr: &Expr) -> Option<(usize, usize)> {
        fn shape_from_str(value: &str) -> Option<(usize, usize)> {
            let (mantissa, exponent) = match value.split_once(['e', 'E']) {
                Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
                None => (value, 0),
            };
            let unsigned_mantissa = mantissa.trim_start_matches(['-', '+']);
            let (whole, fraction) = unsigned_mantissa
                .split_once('.')
                .unwrap_or((unsigned_mantissa, ""));
            let scale = (fraction.len() as i32 - exponent).max(0) as usize;
            let significant_digits = format!("{whole}{fraction}").trim_start_matches('0').len();
            let precision = (significant_digits as i32 + (exponent - fraction.len() as i32).max(0))
                .max(scale as i32)
                .max(1) as usize;
            Some((precision, scale))
        }

        match expr {
            Expr::Alias(inner, _) => decimal_shape_of_literal(inner),
            Expr::Cast { expr, dtype, .. } => dtype.as_literal().and_then(|dtype| match dtype {
                DataType::Decimal(precision, scale) => Some((*precision, *scale)),
                _ => decimal_shape_of_literal(expr),
            }),
            Expr::Literal(value) => match value.get_datatype() {
                DataType::Decimal(precision, scale) => Some((precision, scale)),
                _ => match value.to_any_value()? {
                    // Spark promotes Decimal + Float/Double comparisons to Double.
                    // Keep value-based decimal shaping for integral literals only.
                    AnyValue::Int8(value) => shape_from_str(&value.to_string()),
                    AnyValue::Int16(value) => shape_from_str(&value.to_string()),
                    AnyValue::Int32(value) => shape_from_str(&value.to_string()),
                    AnyValue::Int64(value) => shape_from_str(&value.to_string()),
                    AnyValue::UInt8(value) => shape_from_str(&value.to_string()),
                    AnyValue::UInt16(value) => shape_from_str(&value.to_string()),
                    AnyValue::UInt32(value) => shape_from_str(&value.to_string()),
                    AnyValue::UInt64(value) => shape_from_str(&value.to_string()),
                    _ => None,
                },
            },
            _ => None,
        }
    }

    fn decimal_shape_of_integral_column(expr: &Expr, dtype: &DataType) -> Option<(usize, usize)> {
        // Preserve the tighter value-based shapes above for literals. For a column,
        // use its declared integral width so comparisons never round through a
        // clamped decimal common type.
        if matches!(expr, Expr::Literal(_)) {
            return None;
        }
        Some(match dtype {
            DataType::Int8 | DataType::UInt8 => (3, 0),
            DataType::Int16 | DataType::UInt16 => (5, 0),
            DataType::Int32 | DataType::UInt32 => (10, 0),
            DataType::Int64 | DataType::UInt64 => (20, 0),
            _ => return None,
        })
    }

    fn decimal_shape_of_numeric(expr: &Expr, dtype: &DataType) -> Option<(usize, usize)> {
        decimal_shape_of_literal(expr).or_else(|| decimal_shape_of_integral_column(expr, dtype))
    }

    // Keep decimal comparisons in decimal arithmetic. Promoting a Decimal column
    // and a numeric literal to Float64 loses integer precision above 2^53.
    if is_numeric(left_type) && is_numeric(right_type) {
        if matches!(
            (left_type, right_type),
            (
                DataType::Decimal(_, _),
                DataType::Float32 | DataType::Float64
            ) | (
                DataType::Float32 | DataType::Float64,
                DataType::Decimal(_, _)
            )
        ) {
            // Spark promotes Decimal/Float comparisons to Double. Go through the
            // exact decimal text so the conversion rounds as BigDecimal.doubleValue
            // does; Polars' direct Decimal-to-Float64 cast can round large values up.
            return Ok((
                coerce_numeric_to_float64(left, left_type),
                coerce_numeric_to_float64(right, right_type),
            ));
        }

        let decimal_shape = match (left_type, right_type) {
            (DataType::Decimal(lp, ls), DataType::Decimal(rp, rs)) => {
                Some(((*lp - *ls).max(*rp - *rs), (*ls).max(*rs)))
            }
            (DataType::Decimal(p, s), _) => {
                decimal_shape_of_numeric(&right, right_type).map(|(literal_p, literal_s)| {
                    ((*p - *s).max(literal_p - literal_s), (*s).max(literal_s))
                })
            }
            (_, DataType::Decimal(p, s)) => {
                decimal_shape_of_numeric(&left, left_type).map(|(literal_p, literal_s)| {
                    ((*p - *s).max(literal_p - literal_s), (*s).max(literal_s))
                })
            }
            _ => None,
        };
        if let Some((integral_digits, scale)) = decimal_shape {
            let precision = integral_digits.saturating_add(scale);
            if precision <= 38 {
                let common_decimal = DataType::Decimal(precision.max(1), scale);
                return Ok((
                    coerce_to_type(left, common_decimal.clone()),
                    coerce_to_type(right, common_decimal),
                ));
            }
            // No fixed-precision decimal can represent both operands at this scale.
            // Encode both as lexicographically sortable decimal strings so large
            // values remain comparable without rounding through Float64.
            return Ok((
                decimal_ordering_key_expr(left, scale, integral_digits),
                decimal_ordering_key_expr(right, scale, integral_digits),
            ));
        }
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

    fn wrap_try_to_decimal(
        expr: Expr,
        precision: usize,
        scale: usize,
    ) -> Result<Expr, PolarsError> {
        let col = Column::from_expr(expr, None);
        let type_name = format!("decimal({precision},{scale})");
        let coerced = crate::functions::try_cast(&col, &type_name)
            .map_err(|e| PolarsError::ComputeError(e.into()))?;
        Ok(coerced.into_expr())
    }

    // Spark keeps scale-zero Decimal/string comparisons in decimal arithmetic.
    // Parsing the string as Double here would make adjacent integers above 2^53
    // compare equal. Decimal values with a positive scale use the Double path below.
    match (left_type, right_type) {
        (DataType::Decimal(precision, 0), DataType::String) => {
            return Ok((left, wrap_try_to_decimal(right, *precision, 0)?));
        }
        (DataType::String, DataType::Decimal(precision, 0)) => {
            return Ok((wrap_try_to_decimal(left, *precision, 0)?, right));
        }
        _ => {}
    }

    // String–numeric (or numeric–string): route string side through try_to_number and
    // cast numeric side to Float64 so both sides line up.
    let string_numeric = (left_type == &DataType::String && is_numeric(right_type))
        || (right_type == &DataType::String && is_numeric(left_type));

    if string_numeric {
        let left_out = if left_type == &DataType::String {
            wrap_try_to_number(left)?
        } else if is_numeric(left_type) {
            coerce_numeric_to_float64(left, left_type)
        } else {
            left
        };

        let right_out = if right_type == &DataType::String {
            wrap_try_to_number(right)?
        } else if is_numeric(right_type) {
            coerce_numeric_to_float64(right, right_type)
        } else {
            right
        };

        return Ok((left_out, right_out));
    }

    // Date/datetime vs string: cast string side to the temporal type (PySpark implicit cast).
    // #1084: For literals use direct cast so filter(col("dt") >= "2024-01-12 00:00:00") works
    // when the plan sends string literals; try_cast uses map() which may not handle literals.
    fn wrap_try_to_temporal(expr: Expr, target: &DataType) -> Result<Expr, PolarsError> {
        if matches!(target, DataType::Date | DataType::Datetime(_, _)) {
            if matches!(&expr, Expr::Literal(_)) {
                return Ok(expr.cast(target.clone()));
            }
        }
        let col = Column::from_expr(expr, None);
        let type_name = match target {
            DataType::Date => "date",
            DataType::Datetime(..) => "timestamp",
            _ => {
                return Err(PolarsError::ComputeError(
                    "date or datetime type required".to_string().into(),
                ));
            }
        };
        let coerced = crate::functions::try_cast(&col, type_name)
            .map_err(|e| PolarsError::ComputeError(e.into()))?;
        Ok(coerced.into_expr())
    }

    let temporal_string = (is_date_or_datetime(left_type) && right_type == &DataType::String)
        || (left_type == &DataType::String && is_date_or_datetime(right_type));

    if temporal_string {
        // Spark parses strings compared with timestamps in the session timezone.
        // Parse the column values with the same literal parser, then compare epoch
        // microseconds so Polars timezone metadata cannot affect the result.
        if matches!(left_type, DataType::Datetime(_, _)) && right_type == &DataType::String {
            let left = datetime_to_micros(left, left_type);
            let right = string_to_session_timestamp_micros(right);
            return Ok((left, right));
        }
        if left_type == &DataType::String && matches!(right_type, DataType::Datetime(_, _)) {
            let left = string_to_session_timestamp_micros(left);
            let right = datetime_to_micros(right, right_type);
            return Ok((left, right));
        }
        let left_out = if left_type == &DataType::String {
            wrap_try_to_temporal(left, right_type)?
        } else {
            left
        };
        let right_out = if right_type == &DataType::String {
            wrap_try_to_temporal(right, left_type)?
        } else {
            right
        };
        return Ok((left_out, right_out));
    }

    // #615: Date vs datetime comparison (e.g. datetime_col < date_col): cast Date to Datetime
    // so both sides are comparable (PySpark treats date as start-of-day timestamp).
    let date_vs_datetime = (left_type == &DataType::Date
        && matches!(right_type, DataType::Datetime(_, _)))
        || (matches!(left_type, DataType::Datetime(_, _)) && right_type == &DataType::Date);
    if date_vs_datetime {
        // A Spark DATE becomes midnight in the session timezone when compared
        // with a timestamp. Compare physical microseconds so timezone metadata
        // cannot make otherwise equal instants incompatible in Polars.
        let left_out = if left_type == &DataType::Date {
            date_to_session_timestamp_micros(left)
        } else {
            datetime_to_micros(left, left_type)
        };
        let right_out = if right_type == &DataType::Date {
            date_to_session_timestamp_micros(right)
        } else {
            datetime_to_micros(right, right_type)
        };
        return Ok((left_out, right_out));
    }

    // Spark timestamps have microsecond precision. Normalize differing Polars
    // units to microseconds so a nanosecond column can still compare with values
    // outside the nanosecond epoch range. When only one operand has timezone
    // metadata, drop that metadata while preserving its epoch value; both sides
    // then compare on the same instant representation.
    if matches!(
        (left_type, right_type),
        (DataType::Datetime(_, _), DataType::Datetime(_, _))
    ) {
        return Ok((
            datetime_to_micros(left, left_type),
            datetime_to_micros(right, right_type),
        ));
    }

    // Equal non-numeric types: leave as-is for now.
    if left_type == right_type && !is_numeric(left_type) {
        return Ok((left, right));
    }

    // Fallback to generic comparison coercion (may error with a clear message).
    coerce_for_comparison(left, right, left_type, right_type)
}

fn string_to_session_timestamp_micros(expr: Expr) -> Expr {
    let timezone = crate::get_thread_session_time_zone();
    expr.map(
        move |column| {
            let mut micros = Vec::with_capacity(column.len());
            for index in 0..column.len() {
                let parsed = match column.get(index)? {
                    AnyValue::String(value) => {
                        crate::sql::translator::parse_sql_timestamp_micros(value, &timezone).ok()
                    }
                    AnyValue::StringOwned(value) => {
                        crate::sql::translator::parse_sql_timestamp_micros(&value, &timezone).ok()
                    }
                    AnyValue::Null => None,
                    other => {
                        return Err(PolarsError::ComputeError(
                            format!("expected STRING timestamp, found {other:?}").into(),
                        ));
                    }
                };
                micros.push(parsed);
            }
            Ok(
                Int64Chunked::from_iter_options(column.name().clone(), micros.iter().copied())
                    .into_series()
                    .into(),
            )
        },
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Int64)),
    )
}

fn datetime_to_micros(expr: Expr, dtype: &DataType) -> Expr {
    let DataType::Datetime(_, timezone) = dtype else {
        return expr;
    };
    // Keep the source zone while changing units; the final integer cast strips
    // zone metadata without changing the represented epoch instant.
    expr.cast(DataType::Datetime(TimeUnit::Microseconds, timezone.clone()))
        .cast(DataType::Int64)
}

fn date_to_session_timestamp_micros(expr: Expr) -> Expr {
    let timezone = crate::get_thread_session_time_zone();
    expr.map(
        move |column| {
            use chrono::TimeZone;

            let tz = timezone.parse::<chrono_tz::Tz>().ok();
            let fixed = parse_comparison_fixed_offset(&timezone);
            let mut micros = Vec::with_capacity(column.len());
            for index in 0..column.len() {
                let days = match column.get(index)? {
                    AnyValue::Date(days) => Some(days),
                    AnyValue::Int32(days) => Some(days),
                    AnyValue::Null => None,
                    other => {
                        return Err(PolarsError::ComputeError(
                            format!("expected DATE value, found {other:?}").into(),
                        ));
                    }
                };
                let Some(days) = days else {
                    micros.push(None);
                    continue;
                };
                let Some(date) = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).and_then(|epoch| {
                    epoch.checked_add_signed(chrono::TimeDelta::days(i64::from(days)))
                }) else {
                    micros.push(None);
                    continue;
                };
                let naive = date.and_hms_opt(0, 0, 0).expect("midnight is valid");
                let value = if let Some(tz) = tz {
                    session_local_datetime_micros(tz, &naive)
                } else if let Some(offset) = fixed {
                    offset
                        .from_local_datetime(&naive)
                        .single()
                        .map(|value| value.timestamp_micros())
                } else {
                    return Err(PolarsError::ComputeError(
                        format!("invalid session timezone {timezone:?}").into(),
                    ));
                };
                micros.push(value);
            }
            Ok(
                Int64Chunked::from_iter_options(column.name().clone(), micros.iter().copied())
                    .into_series()
                    .into(),
            )
        },
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Int64)),
    )
}

fn session_local_datetime_micros(
    timezone: chrono_tz::Tz,
    naive: &chrono::NaiveDateTime,
) -> Option<i64> {
    use chrono::TimeZone;

    let instant = |local: &chrono::NaiveDateTime| match timezone.from_local_datetime(local) {
        chrono::LocalResult::Single(value) => Some(value.timestamp_micros()),
        chrono::LocalResult::Ambiguous(first, second) => {
            Some(first.timestamp_micros().min(second.timestamp_micros()))
        }
        chrono::LocalResult::None => None,
    };
    if let Some(value) = instant(naive) {
        return Some(value);
    }

    // atStartOfDay resolves to the first valid local time on or after midnight.
    // A gap can begin on the previous date and end after midnight, so shifting
    // midnight by the full offset change can land too late in the day.
    let mut low = 0_i64;
    let mut high = chrono::TimeDelta::days(2).num_nanoseconds()?;
    let upper = naive.checked_add_signed(chrono::TimeDelta::nanoseconds(high))?;
    instant(&upper)?;
    while low < high {
        let mid = low + (high - low) / 2;
        let candidate = naive.checked_add_signed(chrono::TimeDelta::nanoseconds(mid))?;
        if instant(&candidate).is_some() {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    instant(&naive.checked_add_signed(chrono::TimeDelta::nanoseconds(low))?)
}

fn parse_comparison_fixed_offset(value: &str) -> Option<chrono::FixedOffset> {
    let value = ["UTC", "GMT", "UT"]
        .iter()
        .find_map(|prefix| {
            value
                .strip_prefix(prefix)
                .filter(|rest| rest.starts_with('+') || rest.starts_with('-'))
        })
        .unwrap_or(value);
    if value == "UTC" || value == "Z" {
        return chrono::FixedOffset::east_opt(0);
    }
    let (sign, value) = match value.as_bytes().first()? {
        b'+' => (1, &value[1..]),
        b'-' => (-1, &value[1..]),
        _ => return None,
    };
    let compact = !value.contains(':') && value.len() > 2;
    let parts = if compact {
        match value.len() {
            4 => vec![&value[..2], &value[2..], "0"],
            6 => vec![&value[..2], &value[2..4], &value[4..]],
            _ => return None,
        }
    } else {
        let parts = value.split(':').collect::<Vec<_>>();
        if parts.len() > 3 {
            return None;
        }
        parts
    };
    let hours = parts.first()?.parse::<i32>().ok()?;
    let minutes = parts
        .get(1)
        .map(|s| s.parse::<i32>())
        .transpose()
        .ok()?
        .unwrap_or(0);
    let seconds = parts
        .get(2)
        .map(|s| s.parse::<i32>())
        .transpose()
        .ok()?
        .unwrap_or(0);
    if parts.len() > 3
        || hours > 18
        || minutes > 59
        || seconds > 59
        || (hours == 18 && (minutes != 0 || seconds != 0))
    {
        return None;
    }
    chrono::FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60 + seconds))
}

/// Infer DataType from an expression when it is a literal (for coercion heuristics).
pub fn infer_type_from_expr(expr: &Expr) -> Option<DataType> {
    match expr {
        Expr::Literal(lv) => {
            let dt = lv.get_datatype();
            Some(if matches!(dt, DataType::Unknown(_)) {
                DataType::Float64
            } else {
                dt
            })
        }
        _ => None,
    }
}

/// True when the expression is a string literal whose text does *not* parse as a number.
fn is_non_numeric_string_literal(expr: &Expr) -> bool {
    // Current Polars `LiteralValue` does not expose underlying string contents in a way
    // that lets us cheaply distinguish "numeric-looking" strings from arbitrary text.
    // For eqNullSafe parity, it is acceptable (and desirable for #1471) to treat all
    // string literals as numeric candidates and let `try_to_number` decide:
    //
    // - Numeric strings like "123" are parsed and compared numerically.
    // - Non-numeric strings like "abc" become null and simply don't match.
    //
    // So we never classify a literal as "non-numeric string" here.
    let _ = expr;
    false
}

/// Coerce left/right for eq_null_safe so string–numeric compares like PySpark (try_to_number on string side).
/// Infers types from literals; assumes String for column (so string–numeric gets coerced).
/// When both sides are column refs (no type info), cast both to string so string vs numeric compares (e.g. "100" and 100).
pub fn coerce_for_pyspark_eq_null_safe(
    left: Expr,
    right: Expr,
) -> Result<(Expr, Expr), PolarsError> {
    // If either side is a null literal, leave expressions as-is.
    // Comparing any dtype to a null literal is well-defined, and attempting string/numeric
    // coercion here can introduce invalid comparisons (e.g. string vs i64) in Polars planning.
    if matches!(infer_type_from_expr(&left), Some(DataType::Null))
        || matches!(infer_type_from_expr(&right), Some(DataType::Null))
    {
        return Ok((left, right));
    }

    let left_inferred = infer_type_from_expr(&left);
    let right_inferred = infer_type_from_expr(&right);
    // Left in place for documentation / future parity tuning.
    // Currently `is_non_numeric_string_literal` always returns false (see comment in helper).
    let _left_non_numeric_str_lit = is_non_numeric_string_literal(&left);
    let _right_non_numeric_str_lit = is_non_numeric_string_literal(&right);

    // Derive comparison dtypes with a bit of asymmetry to mirror PySpark-style coercion:
    // - When only one side is a literal string and the other is a column/expr, treat the
    //   non-literal side as numeric (Float64) so string‑numeric coercion (try_to_number)
    //   kicks in for cases like int_col <=> "123" (issue #1471).
    // - When only one side has a non-string literal type, treat the unknown side as String
    //   so string‑numeric detection in coerce_for_pyspark_comparison still works when the
    //   real column type is discovered later.
    let (left_ty, right_ty) = match (left_inferred.clone(), right_inferred.clone()) {
        // Both sides known: respect their literal dtypes.
        (Some(lt), Some(rt)) => (lt, rt),
        // String literal vs unknown expression:
        // - If the unknown side is a *column* expr, keep both as String so
        //   string-column vs string-literal behaves like string equality with
        //   null-safe semantics (issue #260).
        // - Otherwise (e.g. string literal vs numeric literal), treat the unknown
        //   side as numeric (Float64) so string–numeric coercion kicks in (#1471).
        (Some(DataType::String), None) if matches!(right, Expr::Column(_)) => {
            (DataType::String, DataType::String)
        }
        (None, Some(DataType::String)) if matches!(left, Expr::Column(_)) => {
            (DataType::String, DataType::String)
        }
        (Some(DataType::String), None) => (DataType::String, DataType::Float64),
        (None, Some(DataType::String)) => (DataType::Float64, DataType::String),
        // Non-string literal vs unknown expression: treat unknown as the same type as the literal
        // so numeric literals drive numeric comparisons instead of forcing the unknown side to String.
        (Some(lt), None) => (lt.clone(), lt),
        (None, Some(rt)) => {
            // Special-case: string column vs numeric literal (e.g. col(\"str_col\").eqNullSafe(lit(123))).
            // Treat the column side as String and the literal side as its numeric dtype so
            // string–numeric coercion (try_to_number on the string side) kicks in (#266).
            if is_numeric(&rt) && matches!(left, Expr::Column(_)) {
                (DataType::String, rt)
            } else {
                (rt.clone(), rt)
            }
        }
        (None, None) => (DataType::String, DataType::String),
    };

    // Both column refs without literal dtypes: compare as strings (#260).
    // try_to_number on both sides wrongly treats non-numeric strings as null, so
    // NULL <=> "x" becomes true; numeric/string heterogeneous cases use literal branches above.
    if left_inferred.is_none() && right_inferred.is_none() {
        let left_str = left.cast(DataType::String);
        let right_str = right.cast(DataType::String);
        return Ok((left_str, right_str));
    }

    coerce_for_pyspark_comparison(left, right, &left_ty, &right_ty, &CompareOp::Eq)
}

/// #974: Coerce string–numeric for arithmetic (+, -, *, /, %, **). String side is cast via try_to_number to Float64.
pub fn coerce_for_pyspark_arithmetic(
    left: Expr,
    right: Expr,
    left_type: &DataType,
    right_type: &DataType,
) -> Result<(Expr, Expr), PolarsError> {
    use crate::column::Column;
    fn wrap_try_to_number(expr: Expr) -> Result<Expr, PolarsError> {
        let col = Column::from_expr(expr, None);
        let coerced = crate::functions::try_to_number(&col, None)
            .map_err(|e| PolarsError::ComputeError(e.into()))?;
        Ok(coerced.into_expr())
    }
    let string_numeric = (left_type == &DataType::String && is_numeric(right_type))
        || (right_type == &DataType::String && is_numeric(left_type));
    if !string_numeric {
        return Ok((left, right));
    }
    let left_out = if left_type == &DataType::String {
        wrap_try_to_number(left)?
    } else if is_numeric(left_type) {
        coerce_numeric_to_float64(left, left_type)
    } else {
        left
    };
    let right_out = if right_type == &DataType::String {
        wrap_try_to_number(right)?
    } else if is_numeric(right_type) {
        coerce_numeric_to_float64(right, right_type)
    } else {
        right
    };
    Ok((left_out, right_out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{IntoLazy, df};

    #[test]
    fn find_common_type_null_adopts_other_type() -> Result<(), PolarsError> {
        let list_str = DataType::List(Box::new(DataType::String));
        assert_eq!(find_common_type(&DataType::Null, &list_str)?, list_str);
        assert_eq!(find_common_type(&list_str, &DataType::Null)?, list_str);
        Ok(())
    }

    #[test]
    fn find_common_type_date_datetime_prefers_datetime() -> Result<(), PolarsError> {
        let ts = DataType::Datetime(TimeUnit::Microseconds, None);
        assert_eq!(find_common_type(&DataType::Date, &ts)?, ts);
        assert_eq!(find_common_type(&ts, &DataType::Date)?, ts);
        Ok(())
    }

    #[test]
    fn find_common_type_short_promotes_to_integer() -> Result<(), PolarsError> {
        assert_eq!(
            find_common_type(&DataType::Int16, &DataType::Int32)?,
            DataType::Int32
        );
        assert_eq!(
            find_common_type(&DataType::Int16, &DataType::Int16)?,
            DataType::Int16
        );
        Ok(())
    }

    #[test]
    fn numeric_numeric_uses_standard_coercion() -> Result<(), PolarsError> {
        let df = df!(
            "a" => &[1i32, 2, 3],
            "b" => &[1i64, 2, 3]
        )?;

        let a = col("a");
        let b = col("b");
        let (ac, bc) = coerce_for_pyspark_comparison(
            a.clone(),
            b.clone(),
            &DataType::Int32,
            &DataType::Int64,
            &CompareOp::Eq,
        )?;

        // After coercion both sides should be comparable without error and all rows match.
        let out = df.lazy().filter(ac.eq(bc)).collect()?;
        assert_eq!(out.height(), 3);
        Ok(())
    }

    #[test]
    fn decimal_column_and_float_literal_compare_as_double() -> Result<(), PolarsError> {
        let decimal = Series::new("amount".into(), vec![Some(900_719_925_474_099_300i128)])
            .into_decimal(20, 2)?
            .into_series();
        let df = DataFrame::new(1, vec![decimal.into()])?;
        let (left, right) = coerce_for_pyspark_comparison(
            col("amount"),
            lit(9_007_199_254_740_992.0f64),
            &DataType::Decimal(20, 2),
            &DataType::Float64,
            &CompareOp::Eq,
        )?;

        let out = df
            .lazy()
            .select([left.eq(right).alias("matches")])
            .collect()?;
        assert_eq!(out.column("matches")?.bool()?.get(0), Some(true));
        Ok(())
    }

    #[test]
    fn positive_scale_decimal_and_string_compare_without_double_rounding() -> Result<(), PolarsError>
    {
        let decimal = Series::new("amount".into(), vec![Some(90_071_992_547_409_929i128)])
            .into_decimal(18, 1)?
            .into_series();
        let amount_text = Series::new("amount_text".into(), vec!["9007199254740992.9"]);
        let df = DataFrame::new(1, vec![decimal.into(), amount_text.into()])?;
        let (left, right) = coerce_for_pyspark_comparison(
            col("amount"),
            col("amount_text"),
            &DataType::Decimal(18, 1),
            &DataType::String,
            &CompareOp::Eq,
        )?;

        let out = df
            .lazy()
            .select([left.eq(right).alias("matches")])
            .collect()?;
        assert_eq!(out.column("matches")?.bool()?.get(0), Some(true));
        Ok(())
    }

    #[test]
    fn eqnullsafe_string_column_vs_int_literal_coerces_like_pyspark() -> Result<(), PolarsError> {
        // string column vs int literal: non-numeric -> null on string side -> eqNullSafe = False;
        // numeric string \"123\" -> 123 -> eqNullSafe = True (#266).
        let df = df!(
            "str_col" => &["abc", "123"]
        )?;

        let a = col("str_col");
        let lit_123 = lit(123i64);
        let (ac, bc) = coerce_for_pyspark_eq_null_safe(a.clone(), lit_123)?;

        let lf = df.lazy().select(&[
            ac.clone().alias("left"),
            bc.clone().alias("right"),
            ac.clone().eq(bc.clone()).alias("eq_raw"),
            ac.is_null().alias("left_null"),
            bc.is_null().alias("right_null"),
        ]);
        let out = lf.collect()?;

        // String–numeric coercion uses try_to_number (double) and casts numeric side to Float64.
        let left = out.column("left")?.f64()?;
        let right = out.column("right")?.f64()?;
        let eq_raw = out.column("eq_raw")?.bool()?;
        let left_null = out.column("left_null")?.bool()?;
        let right_null = out.column("right_null")?.bool()?;

        // First row: non-numeric \"abc\" -> left null, right 123.0; eqNullSafe is false.
        // Raw Polars .eq() yields null for (null, 123.0); left_null/right_null still distinguish.
        assert!(left.get(0).is_none());
        assert_eq!(right.get(0), Some(123.0));
        assert!(eq_raw.get(0).is_none() || eq_raw.get(0) == Some(false));
        assert_eq!(left_null.get(0), Some(true));
        assert_eq!(right_null.get(0), Some(false));

        // Second row: \"123\" -> 123.0 on both sides; equality = True, non-null.
        assert_eq!(left.get(1), Some(123.0));
        assert_eq!(right.get(1), Some(123.0));
        assert_eq!(eq_raw.get(1), Some(true));
        assert_eq!(left_null.get(1), Some(false));
        assert_eq!(right_null.get(1), Some(false));

        Ok(())
    }

    #[test]
    fn string_numeric_uses_try_to_number() -> Result<(), PolarsError> {
        let df = df!(
            "s" => &["123", " 45.5 ", "abc"],
            "n" => &[123i32, 46, 0]
        )?;

        let s_expr = col("s");
        let n_expr = col("n");

        let (s_coerced, n_coerced) = coerce_for_pyspark_comparison(
            s_expr.clone(),
            n_expr.clone(),
            &DataType::String,
            &DataType::Int32,
            &CompareOp::Eq,
        )?;

        let out = df.lazy().filter(s_coerced.eq(n_coerced)).collect()?;

        // Only the first row matches ("123" == 123); " 45.5 " != 46, "abc" -> null (non-match).
        assert_eq!(out.height(), 1);
        Ok(())
    }

    /// #615: datetime_col < date_col must return rows (PySpark: date as start-of-day for comparison).
    #[test]
    fn date_datetime_comparison_coerces_date_to_datetime() -> Result<(), PolarsError> {
        use chrono::{NaiveDate, NaiveDateTime};
        use polars::prelude::*;

        let ts = NaiveDateTime::parse_from_str("2024-01-14 23:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let dt = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let df = df!(
            "ts_col" => [ts],
            "date_col" => [dt]
        )?;
        let df = df
            .lazy()
            .with_columns([
                col("ts_col").cast(DataType::Datetime(TimeUnit::Microseconds, None)),
                col("date_col").cast(DataType::Date),
            ])
            .collect()?;
        let lf = df.lazy();

        let ts_expr = col("ts_col");
        let date_expr = col("date_col");
        let (ts_c, date_c) = coerce_for_pyspark_comparison(
            ts_expr,
            date_expr,
            &DataType::Datetime(TimeUnit::Microseconds, None),
            &DataType::Date,
            &CompareOp::Lt,
        )?;

        let out = lf.filter(ts_c.lt(date_c)).collect()?;
        assert_eq!(
            out.height(),
            1,
            "#615: datetime < date should return one row"
        );
        Ok(())
    }

    #[test]
    fn eq_null_safe_int_column_vs_string_literal_coerces() -> Result<(), PolarsError> {
        use crate::column::Column;
        use polars::prelude::df;

        let df = df!(
            "val" => &[Some(123i64), Some(456i64), None],
        )?;
        let lf = df.lazy();

        // Simulate the Python eqNullSafe(\"123\") path: the Python binding parses \"123\"
        // as an integer and calls into the engine with an int literal column.
        let col = Column::from_expr(col("val"), None);
        let lit_col = crate::functions::lit_i64(123);
        let result_col = col.eq_null_safe(&lit_col);

        let out = lf
            .with_column(result_col.into_expr().alias("match"))
            .collect()?;

        let matches = out.column("match")?.bool()?;
        let got: Vec<Option<bool>> = matches.into_iter().collect();
        assert_eq!(got, vec![Some(true), Some(false), Some(false)]);

        Ok(())
    }
}
