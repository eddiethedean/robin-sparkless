//! Exact arithmetic helpers for decimal-backed Python Column operations.

use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use polars::prelude::*;

#[derive(Clone, Copy)]
pub(crate) enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
}

impl BinaryOp {
    pub(crate) fn expr_marker(self) -> &'static str {
        match self {
            Self::Add => "__robin_decimal_add",
            Self::Subtract => "__robin_decimal_subtract",
            Self::Multiply => "__robin_decimal_multiply",
            Self::Divide => "__robin_decimal_divide",
        }
    }

    pub(crate) fn from_expr_marker(marker: &str) -> Option<Self> {
        match marker {
            "__robin_decimal_add" => Some(Self::Add),
            "__robin_decimal_subtract" => Some(Self::Subtract),
            "__robin_decimal_multiply" => Some(Self::Multiply),
            "__robin_decimal_divide" => Some(Self::Divide),
            _ => None,
        }
    }

    pub(crate) fn engine_expr_marker(self) -> &'static str {
        match self {
            Self::Add => "__robin_expr_ir_add",
            Self::Subtract => "__robin_expr_ir_subtract",
            Self::Multiply => "__robin_expr_ir_multiply",
            Self::Divide => "__robin_expr_ir_divide",
        }
    }

    pub(crate) fn from_engine_expr_marker(marker: &str) -> Option<Self> {
        match marker {
            "__robin_expr_ir_add" => Some(Self::Add),
            "__robin_expr_ir_subtract" => Some(Self::Subtract),
            "__robin_expr_ir_multiply" => Some(Self::Multiply),
            "__robin_expr_ir_divide" => Some(Self::Divide),
            _ => None,
        }
    }
}

fn decimal_shape(dtype: &DataType) -> Option<(usize, usize)> {
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

/// Return Spark's result dtype when either side is a decimal and both sides can
/// be represented exactly as decimals. Floating point and string arithmetic
/// stays on the regular PySpark coercion path.
pub(crate) fn result_type(
    left: &DataType,
    right: &DataType,
    op: BinaryOp,
    allow_precision_loss: bool,
) -> Option<DataType> {
    if !matches!(left, DataType::Decimal(..)) && !matches!(right, DataType::Decimal(..)) {
        return None;
    }
    let (p1, s1) = decimal_shape(left)?;
    let (p2, s2) = decimal_shape(right)?;
    let (precision, mut scale) = match op {
        BinaryOp::Add | BinaryOp::Subtract => {
            let scale = s1.max(s2);
            ((p1 - s1).max(p2 - s2) + scale + 1, scale)
        }
        BinaryOp::Multiply => (p1 + p2 + 1, s1 + s2),
        BinaryOp::Divide => {
            let scale = 6.max(s1 + p2 + 1);
            if allow_precision_loss {
                (p1 - s1 + s2 + scale, scale)
            } else {
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
    };
    if precision > 38 && allow_precision_loss {
        scale = 38usize.saturating_sub(precision - scale).max(scale.min(6));
    } else {
        scale = scale.min(38);
    }
    Some(DataType::Decimal(
        precision.min(38),
        scale.min(precision.min(38)),
    ))
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

/// Evaluate decimal/integer Column arithmetic without a Float64 conversion.
pub(crate) fn evaluate(
    left: &Series,
    right: &Series,
    op: BinaryOp,
    output_type: &DataType,
    ansi_enabled: bool,
) -> PolarsResult<Series> {
    let (DataType::Decimal(precision, scale), Some((_, s1)), Some((_, s2))) = (
        output_type,
        decimal_shape(left.dtype()),
        decimal_shape(right.dtype()),
    ) else {
        return Err(PolarsError::ComputeError(
            "decimal arithmetic received unsupported input types".into(),
        ));
    };
    let len = if left.is_empty() || right.is_empty() {
        0
    } else {
        left.len().max(right.len())
    };
    let maximum = ten_pow(*precision) - 1;
    let mut output = Vec::with_capacity(len);
    for row in 0..len {
        let left_value = coefficient(left.get(if left.len() == 1 { 0 } else { row })?);
        let right_value = coefficient(right.get(if right.len() == 1 { 0 } else { row })?);
        let (Some(left_value), Some(right_value)) = (left_value, right_value) else {
            output.push(None);
            continue;
        };
        let value = match op {
            BinaryOp::Add | BinaryOp::Subtract => {
                let common_scale = s1.max(s2);
                let left_value = rescale(left_value, s1, common_scale);
                let right_value = rescale(right_value, s2, common_scale);
                let value = if matches!(op, BinaryOp::Add) {
                    left_value + right_value
                } else {
                    left_value - right_value
                };
                rescale(value, common_scale, *scale)
            }
            BinaryOp::Multiply => rescale(left_value * right_value, s1 + s2, *scale),
            BinaryOp::Divide => {
                if right_value.is_zero() {
                    if ansi_enabled {
                        polars_bail!(ComputeError: "[DIVIDE_BY_ZERO] division by zero");
                    }
                    output.push(None);
                    continue;
                }
                if *scale + s2 >= s1 {
                    rounded_divide(left_value * ten_pow(*scale + s2 - s1), right_value)
                } else {
                    rounded_divide(left_value, right_value * ten_pow(s1 - *scale - s2))
                }
            }
        };
        if value.abs() > maximum {
            if ansi_enabled {
                polars_bail!(ComputeError: "[ARITHMETIC_OVERFLOW] decimal arithmetic exceeds precision");
            }
            output.push(None);
        } else {
            output.push(value.to_i128());
        }
    }
    Series::new(left.name().clone(), output)
        .into_decimal(*precision, *scale)
        .map(|series| series.into_series())
}
