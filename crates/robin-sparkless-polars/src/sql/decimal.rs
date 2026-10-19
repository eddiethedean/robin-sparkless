//! Spark precision, scale and rounding for SQL decimal arithmetic.
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use polars::prelude::*;
use spark_sql_parser::ast::BinaryOperator;

fn integral_literal_value(expr: &Expr) -> Option<i128> {
    match expr {
        Expr::Literal(value) => value.to_any_value()?.extract::<i128>(),
        Expr::Alias(inner, _) | Expr::KeepName(inner) => integral_literal_value(inner),
        Expr::Function { input, function }
            if matches!(function, polars::prelude::FunctionExpr::Negate) && input.len() == 1 =>
        {
            integral_literal_value(&input[0])?.checked_neg()
        }
        _ => None,
    }
}

fn decimal_type(expr: &Expr, dtype: &DataType) -> Option<(usize, usize)> {
    // Spark uses the minimum decimal precision for an integral literal. An
    // explicitly cast integer or an integer column keeps its declared width.
    if dtype.is_integer() {
        if let Some(value) = integral_literal_value(expr) {
            return Some((value.unsigned_abs().to_string().len(), 0));
        }
    }
    Some(match dtype {
        DataType::Decimal(precision, scale) => (*precision, *scale),
        DataType::Int8 | DataType::UInt8 => (3, 0),
        DataType::Int16 | DataType::UInt16 => (5, 0),
        DataType::Int32 | DataType::UInt32 => (10, 0),
        DataType::Int64 | DataType::UInt64 => (20, 0),
        DataType::Null => (1, 0),
        _ => return None,
    })
}

/// Spark promotes mixed Decimal/Float arithmetic to Double. Convert the
/// Decimal through its exact text representation before parsing as Float64;
/// Polars' direct Decimal-to-Float64 cast can round large values up.
pub(super) fn coerce_decimal_float_operands(
    left: Expr,
    right: Expr,
    schema: &Schema,
) -> PolarsResult<Option<(Expr, Expr)>> {
    let left_type = left.to_field(schema)?.dtype().clone();
    let right_type = right.to_field(schema)?.dtype().clone();
    let mixed_decimal_float = matches!(
        (&left_type, &right_type),
        (DataType::Decimal(..), DataType::Float32 | DataType::Float64)
            | (DataType::Float32 | DataType::Float64, DataType::Decimal(..))
    );
    if !mixed_decimal_float {
        return Ok(None);
    }
    let to_double = |expr: Expr, dtype: &DataType| {
        if matches!(dtype, DataType::Decimal(..)) {
            expr.cast(DataType::String).cast(DataType::Float64)
        } else {
            expr.cast(DataType::Float64)
        }
    };
    Ok(Some((
        to_double(left, &left_type),
        to_double(right, &right_type),
    )))
}

fn coefficient(value: AnyValue<'_>) -> Option<BigInt> {
    match value {
        AnyValue::Null => None,
        AnyValue::Decimal(value, _, _) | AnyValue::Int128(value) => Some(value.into()),
        value => value.extract::<i128>().map(BigInt::from),
    }
}

fn ten_pow(power: usize) -> BigInt {
    BigInt::from(10).pow(power as u32)
}

fn rounded_divide(numerator: BigInt, denominator: BigInt) -> BigInt {
    let negative = numerator.is_negative() != denominator.is_negative();
    let remainder = &numerator % &denominator;
    let mut result = numerator / &denominator;
    if remainder.abs() * 2 >= denominator.abs() {
        result += if negative { -1 } else { 1 };
    }
    result
}

fn rescale(value: BigInt, from: usize, to: usize) -> BigInt {
    if to >= from {
        value * ten_pow(to - from)
    } else {
        rounded_divide(value, ten_pow(from - to))
    }
}

pub(super) fn binary_expr(
    left: Expr,
    right: Expr,
    op: &BinaryOperator,
    schema: &Schema,
    ansi: bool,
    allow_precision_loss: bool,
) -> PolarsResult<Option<Expr>> {
    let left_type = left.to_field(schema)?.dtype().clone();
    let right_type = right.to_field(schema)?.dtype().clone();
    if !matches!(left_type, DataType::Decimal(..)) && !matches!(right_type, DataType::Decimal(..)) {
        return Ok(None);
    }
    let (Some((p1, s1)), Some((p2, s2))) = (
        decimal_type(&left, &left_type),
        decimal_type(&right, &right_type),
    ) else {
        // Spark promotes a decimal combined with floating input to Double.
        return Ok(None);
    };
    let (precision, scale) = match op {
        BinaryOperator::Plus | BinaryOperator::Minus => {
            let scale = s1.max(s2);
            ((p1 - s1).max(p2 - s2) + scale + 1, scale)
        }
        BinaryOperator::Multiply => (p1 + p2 + 1, s1 + s2),
        BinaryOperator::Modulo => {
            let scale = s1.max(s2);
            ((p1 - s1).min(p2 - s2) + scale, scale)
        }
        BinaryOperator::Divide => {
            let scale = 6.max(s1 + p2 + 1);
            if allow_precision_loss {
                (p1 - s1 + s2 + scale, scale)
            } else {
                // Divide reserves both integral and fractional digits when
                // precision loss is disabled (Spark arithmetic.scala).
                let mut integral = (p1 - s1 + s2).min(38);
                let mut fractional = scale.min(38);
                let excess = (integral + fractional).saturating_sub(38);
                if excess > 0 {
                    fractional -= excess / 2 + 1;
                    integral = 38 - fractional;
                }
                (integral + fractional, fractional)
            }
        }
        _ => return Ok(None),
    };
    let scale = if precision > 38 && allow_precision_loss {
        38usize.saturating_sub(precision - scale).max(scale.min(6))
    } else {
        scale.min(38)
    };
    let precision = precision.min(38);
    let op = op.clone();
    let expr = left.map_many(
        move |columns| {
            let left = &columns[0];
            let right = &columns[1];
            let len = if left.is_empty() || right.is_empty() {
                0
            } else {
                left.len().max(right.len())
            };
            let maximum = ten_pow(precision) - 1;
            let mut output = Vec::with_capacity(len);
            for row in 0..len {
                let left = coefficient(left.get(if left.len() == 1 { 0 } else { row })?);
                let right = coefficient(right.get(if right.len() == 1 { 0 } else { row })?);
                let (Some(left), Some(right)) = (left, right) else {
                    output.push(None);
                    continue;
                };
                let value = match op {
                    BinaryOperator::Plus | BinaryOperator::Minus => {
                        let common_scale = s1.max(s2);
                        let left = rescale(left, s1, common_scale);
                        let right = rescale(right, s2, common_scale);
                        let value = if op == BinaryOperator::Plus {
                            left + right
                        } else {
                            left - right
                        };
                        rescale(value, common_scale, scale)
                    }
                    BinaryOperator::Multiply => rescale(left * right, s1 + s2, scale),
                    BinaryOperator::Divide => {
                        if right.is_zero() {
                            if ansi {
                                polars_bail!(ComputeError: "[DIVIDE_BY_ZERO] division by zero");
                            }
                            output.push(None);
                            continue;
                        }
                        if scale + s2 >= s1 {
                            rounded_divide(left * ten_pow(scale + s2 - s1), right)
                        } else {
                            rounded_divide(left, right * ten_pow(s1 - scale - s2))
                        }
                    }
                    BinaryOperator::Modulo => {
                        let common_scale = s1.max(s2);
                        let left = rescale(left, s1, common_scale);
                        let right = rescale(right, s2, common_scale);
                        if right.is_zero() {
                            if ansi {
                                polars_bail!(ComputeError: "[DIVIDE_BY_ZERO] remainder by zero");
                            }
                            output.push(None);
                            continue;
                        }
                        left % right
                    }
                    _ => unreachable!(),
                };
                if value.abs() > maximum {
                    if ansi {
                        polars_bail!(ComputeError: "[ARITHMETIC_OVERFLOW] decimal arithmetic exceeds precision");
                    }
                    output.push(None);
                } else {
                    output.push(value.to_i128());
                }
            }
            Series::new(columns[0].name().clone(), output)
                .into_decimal(precision, scale)
                .map(|series| series.into_series().into())
        },
        &[right],
        move |_schema, fields| {
            Ok(Field::new(fields[0].name().clone(), DataType::Decimal(precision, scale)))
        },
    );
    Ok(Some(expr))
}
