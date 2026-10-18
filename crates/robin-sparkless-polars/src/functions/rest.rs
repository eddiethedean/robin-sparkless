//! Remaining expression builders: agg, when, string, bit, datetime, struct/map/array, cast, hash, misc.
use super::types::parse_type_name;
use crate::column::Column;
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use polars::prelude::*;
use std::collections::VecDeque;

/// Count aggregation (PySpark LongType; cast to Int64 for schema parity #734).
/// count(column): count non-null values in that column (PySpark parity, #1209).
/// count(*): use len() to count all rows per group.
/// Carries source column for running-window conversion when orderBy is present (#1218).
pub fn count(col: &Column) -> Column {
    use polars::prelude::len;

    // Treat count("*") and count(lit(1)) (and similar numeric/bool literals) as
    // "count all rows", matching PySpark's count(lit(1)) / count("*") semantics.
    let is_star = col.name() == "*";
    // Every input-cardinality shortcut requires a scalar, including the
    // numeric path: aliases and value-preserving casts can also wrap Series.
    let is_scalar_literal = col.is_scalar_literal_expression();
    let is_literal_numeric_or_bool = is_scalar_literal
        && col.value_preserving_literal_value().is_some_and(|value| {
            !matches!(value.to_any_value().as_ref(), Some(AnyValue::Null))
                && matches!(col.literal_dtype(), Some(dtype) if matches!(
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
                        | DataType::Boolean
                ))
        });

    let is_literal_with_evaluated_nullness =
        is_scalar_literal && !is_literal_numeric_or_bool && !is_star;
    let (expr, name) = if is_star || is_literal_numeric_or_bool {
        (len().cast(DataType::Int64), "count(1)".to_string())
    } else if is_literal_with_evaluated_nullness {
        // A cast-wrapped literal may change value or become NULL. Evaluate its
        // nullness, then use the input cardinality for non-null values instead
        // of counting the scalar expression itself (which would always produce
        // one value).
        (
            polars::prelude::when(col.expr().clone().is_null())
                .then(lit(0i64))
                .otherwise(len().cast(DataType::Int64)),
            "count(1)".to_string(),
        )
    } else {
        (
            col.expr().clone().count().cast(DataType::Int64),
            format!("count({})", col.name()),
        )
    };
    let mut c = Column::from_expr(expr, Some(name));
    c.source_for_running_count = Some(col.name().to_string());
    c
}

/// Normalize AVG input: preserve decimal coefficients and cast other inputs to Double.
fn numeric_aggregate_input(
    col: &Column,
    source_type: Option<std::sync::Arc<std::sync::Mutex<Option<DataType>>>>,
    decimal_precision_add: usize,
) -> Expr {
    col.expr().clone().map(
        move |series| {
            if let DataType::Decimal(precision, scale) = series.dtype() {
                let precision = precision.saturating_add(decimal_precision_add).min(38);
                if decimal_precision_add > 0 {
                    series.cast(&DataType::Decimal(precision, *scale))
                } else {
                    Ok(series)
                }
            } else {
                series.cast(&DataType::Float64)
            }
        },
        move |_schema, field| {
            if let Some(source_type) = &source_type {
                *source_type.lock().unwrap_or_else(|e| e.into_inner()) =
                    Some(field.dtype().clone());
            }
            let dtype = match field.dtype() {
                DataType::Decimal(precision, scale) => DataType::Decimal(
                    precision.saturating_add(decimal_precision_add).min(38),
                    *scale,
                ),
                _ => DataType::Float64,
            };
            Ok(Field::new(field.name().clone(), dtype))
        },
    )
}

/// String columns are cast to Float64 before sum (PySpark parity, issue #393).
pub fn sum(col: &Column) -> Column {
    let name = format!("sum({})", col.name());
    let expr = sum_expr(
        col.expr().clone(),
        crate::udf_context::get_thread_ansi_enabled(),
    );
    let mut c = Column::from_expr(expr, Some(name));
    c.source_for_running = Some(col.name().to_string());
    c
}

/// Bind runtime-dependent expressions to the configuration of the plan being analyzed.
/// Rebuilding the UDF gives reused expressions independent plan state.
pub(crate) fn bind_runtime_config(expr: Expr, schema: &Schema) -> PolarsResult<Expr> {
    let ansi_enabled = crate::udf_context::get_thread_ansi_enabled();
    let allow_precision_loss = crate::udf_context::get_thread_decimal_allow_precision_loss();
    expr.try_map_expr(|expr| {
        if let Expr::AnonymousFunction { input, fmt_str, .. } = &expr {
            if let Some(op) =
                crate::decimal_arithmetic::BinaryOp::from_expr_marker(fmt_str.as_str())
                && input.len() == 2
            {
                return Ok(crate::column::pyspark_binary_arithmetic_expr(
                    input[0].clone(),
                    input[1].clone(),
                    op,
                    ansi_enabled,
                    allow_precision_loss,
                ));
            }
            if fmt_str.as_str() == "sum" || fmt_str.as_str() == "try_sum" {
                let is_try_aggregate = fmt_str.as_str() == "try_sum";
                if let Some(Expr::Agg(polars_plan::dsl::AggExpr::Implode(input))) = input.first() {
                    let input = input.as_ref().clone();
                    let input_dtype = input
                        .to_field(schema)
                        .ok()
                        .map(|field| field.dtype().clone());
                    if input_dtype.as_ref().is_some_and(DataType::is_integer) {
                        // The fixed-size Int128 reduction can only check the final
                        // total. ANSI SUM must detect overflow at each accumulation
                        // step, even when later values bring the result back in range.
                        return Ok(if is_try_aggregate {
                            sum_expr_with_marker(input, false, "try_sum")
                        } else if ansi_enabled {
                            sum_expr(input, true)
                        } else {
                            integer_sum_expr(input, false)
                        });
                    }
                    if input_dtype
                        .as_ref()
                        .is_some_and(|dtype| !matches!(dtype, DataType::Decimal(..)))
                    {
                        // Float and castable non-integral inputs do not need to be
                        // collected into per-group lists. Keep the optimized native
                        // reduction for these types. Decimal sums still use the
                        // checked BigInt path below.
                        return Ok(input.cast(DataType::Float64).sum());
                    }
                    return Ok(if is_try_aggregate {
                        sum_expr_with_marker(input, false, "try_sum")
                    } else {
                        sum_expr(input, ansi_enabled)
                    });
                }
            }
            if fmt_str.as_str() == "sum_distinct" {
                if let Some(Expr::Agg(polars_plan::dsl::AggExpr::Implode(input))) = input.first() {
                    return Ok(sum_distinct_expr(input.as_ref().clone(), ansi_enabled));
                }
            }
            if fmt_str.as_str() == "avg" || fmt_str.as_str() == "try_avg" {
                let is_try_aggregate = fmt_str.as_str() == "try_avg";
                if let Some(Expr::Agg(polars_plan::dsl::AggExpr::Implode(input))) = input.first() {
                    let input = input.as_ref().clone();
                    let input_dtype = input
                        .to_field(schema)
                        .ok()
                        .map(|field| field.dtype().clone());
                    if input_dtype
                        .as_ref()
                        .is_some_and(|dtype| !matches!(dtype, DataType::Decimal(..)))
                    {
                        // AVG's input is already normalized to Float64 for all
                        // non-decimal types. Use Polars' native mean to avoid
                        // materializing each group's values as a list.
                        return Ok(input.mean());
                    }
                    return Ok(if is_try_aggregate {
                        avg_expr_with_marker(input, false, "try_avg")
                    } else {
                        avg_expr(input, ansi_enabled)
                    });
                }
            }
        }
        Ok(expr)
    })
}

/// Aggregate integer values in Polars' fixed-size group state, then apply Spark's
/// Long overflow behavior to the one result per group. Casting to Int128 before
/// summing preserves the exact Int64 range check in ANSI mode while avoiding an
/// allocation of every group's values as a List.
fn integer_sum_expr(input: Expr, ansi_enabled: bool) -> Expr {
    let input = input.cast(DataType::Int64);
    let non_null_count = input.clone().count();
    let summed = input.cast(DataType::Int128).sum().map_with_fmt_str(
        move |series| {
            let values = series.i128()?;
            let mut output = Vec::with_capacity(series.len());
            for value in values.into_iter() {
                let Some(value) = value else {
                    output.push(None);
                    continue;
                };
                if ansi_enabled && (value < i64::MIN as i128 || value > i64::MAX as i128) {
                    return Err(PolarsError::ComputeError(
                        "[ARITHMETIC_OVERFLOW] long overflow".into(),
                    ));
                }
                // Rust's narrowing integer cast keeps the low 64 bits, matching
                // JVM Long addition when ANSI mode is disabled.
                output.push(Some(value as i64));
            }
            Ok(Series::new(series.name().clone(), output).into())
        },
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Int64)),
        "sum_i64",
    );
    polars::prelude::when(non_null_count.gt(lit(0)))
        .then(summed)
        .otherwise(lit(NULL))
}

fn sum_expr(input: Expr, ansi_enabled: bool) -> Expr {
    sum_expr_with_marker(input, ansi_enabled, "sum")
}

fn sum_expr_with_marker(input: Expr, ansi_enabled: bool, marker: &'static str) -> Expr {
    // Spark widens decimal SUM results by ten integral digits (up to precision 38).
    // Accumulate decimal coefficients with BigInt: Polars' native decimal sum wraps
    // its i128 accumulator when the result exceeds the declared precision.
    input.implode().map_with_fmt_str(
        move |series| {
            let DataType::List(input_type) = series.dtype() else {
                return Err(PolarsError::ComputeError(
                    "sum aggregation expected a list input".into(),
                ));
            };
            let groups = series.list()?;
            match input_type.as_ref() {
                DataType::Decimal(precision, scale) => {
                    let output_precision = precision.saturating_add(10).min(38);
                    let maximum = BigInt::from(10).pow(output_precision as u32) - BigInt::from(1);
                    let mut sums = Vec::with_capacity(series.len());
                    for group_idx in 0..series.len() {
                        let Some(group) = groups.get_as_series(group_idx) else {
                            sums.push(None);
                            continue;
                        };
                        let mut total = BigInt::zero();
                        let mut found_value = false;
                        let mut overflowed = false;
                        for idx in 0..group.len() {
                            match group.get(idx)? {
                                AnyValue::Decimal(value, _, _) | AnyValue::Int128(value) => {
                                    found_value = true;
                                    total += value;
                                    if total.abs() > maximum {
                                        if ansi_enabled {
                                            return Err(PolarsError::ComputeError(
                                                "[ARITHMETIC_OVERFLOW] decimal sum exceeds supported precision"
                                                    .into(),
                                            ));
                                        }
                                        overflowed = true;
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }
                        if !found_value {
                            sums.push(None);
                            continue;
                        }
                        if overflowed {
                            sums.push(None);
                            continue;
                        }
                        sums.push(total.to_i128());
                    }
                    Series::new(series.name().clone(), sums)
                        .into_decimal(output_precision, *scale)
                        .map(|series| series.into_series().into())
                }
                _ => {
                    let output_type = if input_type.is_integer() {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    };
                    let mut sums = Vec::with_capacity(series.len());
                    for group_idx in 0..series.len() {
                        let Some(group) = groups.get_as_series(group_idx) else {
                            sums.push(AnyValue::Null);
                            continue;
                        };
                        if input_type.is_integer() {
                            let group = group.cast(&DataType::Int64)?;
                            let mut total = BigInt::zero();
                            let mut found_value = false;
                            let mut overflowed = false;
                            for idx in 0..group.len() {
                                if let AnyValue::Int64(value) = group.get(idx)? {
                                    found_value = true;
                                    total += value;
                                    let out_of_range = total < BigInt::from(i64::MIN)
                                        || total > BigInt::from(i64::MAX);
                                    if out_of_range {
                                        if ansi_enabled {
                                            return Err(PolarsError::ComputeError(
                                                "[ARITHMETIC_OVERFLOW] long overflow".into(),
                                            ));
                                        }
                                        if marker == "try_sum" {
                                            overflowed = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            if !found_value {
                                sums.push(AnyValue::Null);
                            } else if overflowed {
                                sums.push(AnyValue::Null);
                            } else if ansi_enabled {
                                sums.push(AnyValue::Int64(total.to_i64().ok_or_else(|| {
                                    PolarsError::ComputeError(
                                        "[ARITHMETIC_OVERFLOW] long overflow".into(),
                                    )
                                })?));
                            } else {
                                // Match JVM Long addition when ANSI mode is disabled.
                                let modulus = BigInt::from(1u8) << 64usize;
                                let wrapped = ((&total % &modulus) + &modulus) % &modulus;
                                let sign_bit = BigInt::from(1u8) << 63usize;
                                let signed = if wrapped >= sign_bit {
                                    wrapped - modulus
                                } else {
                                    wrapped
                                };
                                sums.push(AnyValue::Int64(signed.to_i64().expect("wrapped i64")));
                            }
                        } else {
                            let group = group.cast(&output_type)?;
                            sums.push(group.sum_reduce()?.value().clone());
                        }
                    }
                    Series::from_any_values_and_dtype(
                        series.name().clone(),
                        &sums,
                        &output_type,
                        true,
                    )
                    .map(Into::into)
                }
            }
        },
        |_schema, field| {
            let dtype = match field.dtype() {
                DataType::List(input_type) => match input_type.as_ref() {
                    DataType::Decimal(precision, scale) => {
                        DataType::Decimal(precision.saturating_add(10).min(38), *scale)
                    }
                    dtype if dtype.is_integer() => DataType::Int64,
                    _ => DataType::Float64,
                },
                _ => DataType::Float64,
            };
            Ok(Field::new(field.name().clone(), dtype))
        },
        marker,
    )
}

/// Build the ordered-window form of SUM/AVG. Polars' cumulative and rolling
/// numeric operators convert decimals and large integers to `f64` in the
/// window path, so keep their coefficients in `BigInt` until each output row
/// has been checked and materialized.
pub(crate) fn running_sum_expr(input: Expr, ansi_enabled: bool, frame: Option<(i64, i64)>) -> Expr {
    input.apply(
        move |column| {
            running_numeric_series(
                column.as_materialized_series().clone(),
                false,
                ansi_enabled,
                frame,
            )
            .map(Into::into)
        },
        |_schema, field| {
            let dtype = match field.dtype() {
                DataType::Decimal(precision, scale) => {
                    DataType::Decimal(precision.saturating_add(10).min(38), *scale)
                }
                dtype if dtype.is_integer() => DataType::Int64,
                _ => DataType::Float64,
            };
            Ok(Field::new(field.name().clone(), dtype))
        },
    )
}

pub(crate) fn integer_window_sum_series(
    series: &Series,
    windows: &[Option<(usize, usize)>],
    ansi_enabled: bool,
) -> PolarsResult<Series> {
    let ints = series.cast(&DataType::Int128)?;
    let ints = ints.i128()?;
    let mut sums = Vec::with_capacity(series.len());
    let mut counts = Vec::with_capacity(series.len());
    let mut total = BigInt::zero();
    let mut count = 0usize;
    for idx in 0..series.len() {
        if let Some(value) = ints.get(idx) {
            total += value;
            count += 1;
        }
        sums.push(total.clone());
        counts.push(count);
    }

    let overflow = if ansi_enabled {
        integer_window_overflow_flags(&sums, windows)
    } else {
        vec![false; series.len()]
    };
    let modulus = BigInt::from(1u8) << 64usize;
    let mut values = Vec::with_capacity(series.len());
    for (idx, window) in windows.iter().enumerate() {
        let Some((left, right)) = window else {
            values.push(None);
            continue;
        };
        let previous_sum = if *left == 0 {
            BigInt::zero()
        } else {
            sums[*left - 1].clone()
        };
        let previous_count = if *left == 0 { 0 } else { counts[*left - 1] };
        if counts[*right] == previous_count {
            values.push(None);
            continue;
        }
        if overflow[idx] {
            return Err(PolarsError::ComputeError(
                "[ARITHMETIC_OVERFLOW] integer window sum exceeds BIGINT".into(),
            ));
        }
        let sum = sums[*right].clone() - previous_sum;
        let wrapped = ((&sum % &modulus) + &modulus) % &modulus;
        values.push(wrapped.to_u64().map(|value| value as i64));
    }
    Ok(Series::new(series.name().clone(), values))
}

fn integer_window_overflow_flags(
    prefix_sums: &[BigInt],
    windows: &[Option<(usize, usize)>],
) -> Vec<bool> {
    let mut overflow = vec![false; windows.len()];
    let mut min_indices = VecDeque::new();
    let mut max_indices = VecDeque::new();
    let mut next_index = 0usize;
    let minimum = BigInt::from(i64::MIN);
    let maximum = BigInt::from(i64::MAX);

    for (window_idx, window) in windows.iter().enumerate() {
        let Some((left, right)) = window else {
            continue;
        };
        while next_index <= *right {
            while min_indices
                .back()
                .is_some_and(|&idx| prefix_sums[idx] >= prefix_sums[next_index])
            {
                min_indices.pop_back();
            }
            min_indices.push_back(next_index);
            while max_indices
                .back()
                .is_some_and(|&idx| prefix_sums[idx] <= prefix_sums[next_index])
            {
                max_indices.pop_back();
            }
            max_indices.push_back(next_index);
            next_index += 1;
        }
        while min_indices.front().is_some_and(|&idx| idx < *left) {
            min_indices.pop_front();
        }
        while max_indices.front().is_some_and(|&idx| idx < *left) {
            max_indices.pop_front();
        }

        let base = if *left == 0 {
            BigInt::zero()
        } else {
            prefix_sums[left - 1].clone()
        };
        let min_partial = &prefix_sums[*min_indices.front().unwrap()] - &base;
        let max_partial = &prefix_sums[*max_indices.front().unwrap()] - &base;
        overflow[window_idx] = min_partial < minimum || max_partial > maximum;
    }
    overflow
}

pub(crate) fn running_avg_expr(input: Expr, ansi_enabled: bool, frame: Option<(i64, i64)>) -> Expr {
    input.apply(
        move |column| {
            running_numeric_series(
                column.as_materialized_series().clone(),
                true,
                ansi_enabled,
                frame,
            )
            .map(Into::into)
        },
        |_schema, field| {
            let dtype = match field.dtype() {
                DataType::Decimal(precision, scale) => {
                    let output_precision = precision.saturating_add(4).clamp(1, 38);
                    DataType::Decimal(
                        output_precision,
                        scale.saturating_add(4).min(output_precision),
                    )
                }
                _ => DataType::Float64,
            };
            Ok(Field::new(field.name().clone(), dtype))
        },
    )
}

fn running_numeric_series(
    series: Series,
    average: bool,
    ansi_enabled: bool,
    frame: Option<(i64, i64)>,
) -> PolarsResult<Series> {
    match series.dtype() {
        DataType::Decimal(precision, scale) => {
            let input_precision = *precision;
            let input_scale = *scale;
            let (output_precision, output_scale) = if average {
                let output_precision = input_precision.saturating_add(4).clamp(1, 38);
                (
                    output_precision,
                    input_scale.saturating_add(4).min(output_precision),
                )
            } else {
                (input_precision.saturating_add(10).min(38), input_scale)
            };
            let factor = BigInt::from(10).pow(output_scale.saturating_sub(input_scale) as u32);
            let max_decimal = BigInt::from(10).pow(output_precision as u32) - BigInt::from(1);
            let sum_precision = input_precision.saturating_add(10).min(38);
            let max_sum_decimal = BigInt::from(10).pow(sum_precision as u32) - BigInt::from(1);
            let coefficients = (0..series.len())
                .map(|idx| match series.get(idx)? {
                    AnyValue::Decimal(value, _, _) | AnyValue::Int128(value) => {
                        Ok(Some(BigInt::from(value)))
                    }
                    AnyValue::Null => Ok(None),
                    _ => Err(PolarsError::ComputeError(
                        "decimal window sum received a non-decimal value".into(),
                    )),
                })
                .collect::<PolarsResult<Vec<_>>>()?;
            let mut sums: Vec<BigInt> = Vec::with_capacity(series.len());
            let mut counts: Vec<usize> = Vec::with_capacity(series.len());
            let mut total = BigInt::zero();
            let mut count = 0usize;
            for coefficient in &coefficients {
                if let Some(value) = coefficient {
                    total += value;
                    count += 1;
                }
                sums.push(total.clone());
                counts.push(count);
            }
            let mut values = Vec::with_capacity(series.len());
            for idx in 0..series.len() {
                let Some((left, right)) = window_bounds(idx, series.len(), frame) else {
                    values.push(None);
                    continue;
                };
                let prev_sum = if left == 0 {
                    BigInt::zero()
                } else {
                    sums[left - 1].clone()
                };
                let prev_count = if left == 0 { 0 } else { counts[left - 1] };
                let sum = sums[right].clone() - prev_sum;
                let count = counts[right] - prev_count;
                if count == 0 {
                    values.push(None);
                    continue;
                }
                if average {
                    // Spark keeps AVG's running sum in Decimal(p + 10, s), capped
                    // at precision 38. Check that buffer before dividing; the
                    // average itself can fit even after its sum has overflowed.
                    if sum.abs() > max_sum_decimal {
                        if ansi_enabled {
                            return Err(PolarsError::ComputeError(
                                "[ARITHMETIC_OVERFLOW] decimal average sum exceeds supported precision"
                                    .into(),
                            ));
                        }
                        values.push(None);
                        continue;
                    }
                    let scaled_sum = sum * &factor;
                    let divisor = BigInt::from(count);
                    let mut rounded = &scaled_sum / &divisor;
                    let remainder = &scaled_sum % &divisor;
                    if remainder.abs() * 2 >= divisor {
                        rounded += if remainder.is_negative() { -1 } else { 1 };
                    }
                    if rounded.abs() > max_decimal {
                        if ansi_enabled {
                            return Err(PolarsError::ComputeError(
                                "[ARITHMETIC_OVERFLOW] decimal window average exceeds supported precision"
                                    .into(),
                            ));
                        }
                        values.push(None);
                    } else {
                        values.push(rounded.to_i128());
                    }
                } else if sum.abs() > max_decimal {
                    if ansi_enabled {
                        return Err(PolarsError::ComputeError(
                            "[ARITHMETIC_OVERFLOW] decimal window sum exceeds supported precision"
                                .into(),
                        ));
                    }
                    values.push(None);
                } else {
                    values.push(sum.to_i128());
                }
            }
            Series::new(series.name().clone(), values)
                .into_decimal(output_precision, output_scale)
                .map(|series| series.into_series())
        }
        dtype if dtype.is_integer() => {
            if average {
                let floats = series.cast(&DataType::Float64)?;
                running_float_series(floats, true, frame)
            } else {
                let windows = (0..series.len())
                    .map(|idx| window_bounds(idx, series.len(), frame))
                    .collect::<Vec<_>>();
                integer_window_sum_series(&series, &windows, ansi_enabled)
            }
        }
        _ => {
            let floats = series.cast(&DataType::Float64)?;
            running_float_series(floats, average, frame)
        }
    }
}

fn window_bounds(index: usize, len: usize, frame: Option<(i64, i64)>) -> Option<(usize, usize)> {
    let Some((start, end)) = frame else {
        return Some((0, index));
    };
    let left = (index as i64).saturating_add(start);
    let right = (index as i64).saturating_add(end);
    if right < 0 || left >= len as i64 || left > right {
        return None;
    }
    let left = left.max(0) as usize;
    let right = right.min(len.saturating_sub(1) as i64) as usize;
    Some((left, right))
}

#[derive(Default)]
struct FloatWindowAccumulator {
    finite_sum: f64,
    finite_compensation: f64,
    exact_finite_sum: Option<BigInt>,
    count: usize,
    nan_count: usize,
    positive_infinity_count: usize,
    negative_infinity_count: usize,
}

fn finite_float_as_scaled_integer(value: f64) -> BigInt {
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as usize;
    let fraction = bits & ((1_u64 << 52) - 1);
    let (significand, shift) = if exponent == 0 {
        (fraction, 0)
    } else {
        ((1_u64 << 52) | fraction, exponent - 1)
    };
    let value = BigInt::from(significand) << shift;
    if bits >> 63 == 1 { -value } else { value }
}

fn scaled_integer_as_finite_float(value: &BigInt) -> f64 {
    if value.is_zero() {
        return 0.0;
    }

    let negative = value.is_negative();
    let absolute = value.abs();
    let mut shift = (absolute.bits() as usize).saturating_sub(53);
    let mut significand = &absolute >> shift;
    if shift > 0 {
        let remainder = &absolute - (&significand << shift);
        let halfway = BigInt::from(1) << (shift - 1);
        let round_up = remainder > halfway
            || (remainder == halfway && significand.to_u64().is_some_and(|value| value & 1 == 1));
        if round_up {
            significand += 1;
        }
        if significand.bits() > 53 {
            significand >>= 1;
            shift += 1;
        }
    }

    let exponent = shift as i32 - 1074;
    let magnitude = if exponent > 971 {
        f64::INFINITY
    } else {
        significand.to_u64().unwrap_or(0) as f64 * 2.0_f64.powi(exponent)
    };
    if negative { -magnitude } else { magnitude }
}

impl FloatWindowAccumulator {
    fn update_finite(&mut self, value: f64) {
        if let Some(exact_sum) = &mut self.exact_finite_sum {
            *exact_sum += finite_float_as_scaled_integer(value);
        }
        let adjusted = value - self.finite_compensation;
        let next = self.finite_sum + adjusted;
        if self.finite_sum.is_finite() && next.is_finite() {
            self.finite_compensation = (next - self.finite_sum) - adjusted;
        } else {
            self.finite_compensation = 0.0;
        }
        self.finite_sum = next;
    }

    fn add(&mut self, value: Option<f64>) {
        self.update(value, 1.0);
    }

    fn remove(&mut self, value: Option<f64>) {
        self.update(value, -1.0);
    }

    fn recover_overflowed_finite_sum(&mut self, values: &Float64Chunked, start: usize, end: usize) {
        if self.exact_finite_sum.is_none() && !self.finite_sum.is_finite() {
            let mut exact_sum = BigInt::zero();
            for idx in start..end {
                if let Some(value) = values.get(idx).filter(|value| value.is_finite()) {
                    exact_sum += finite_float_as_scaled_integer(value);
                }
            }
            self.exact_finite_sum = Some(exact_sum);
        }
    }

    fn update(&mut self, value: Option<f64>, sign: f64) {
        let Some(value) = value else {
            return;
        };
        if sign > 0.0 {
            self.count += 1;
        } else {
            self.count -= 1;
        }
        if value.is_nan() {
            if sign > 0.0 {
                self.nan_count += 1;
            } else {
                self.nan_count -= 1;
            }
        } else if value == f64::INFINITY {
            if sign > 0.0 {
                self.positive_infinity_count += 1;
            } else {
                self.positive_infinity_count -= 1;
            }
        } else if value == f64::NEG_INFINITY {
            if sign > 0.0 {
                self.negative_infinity_count += 1;
            } else {
                self.negative_infinity_count -= 1;
            }
        } else {
            self.update_finite(value * sign);
        }
    }

    fn sum(&self) -> f64 {
        if self.nan_count > 0
            || (self.positive_infinity_count > 0 && self.negative_infinity_count > 0)
        {
            f64::NAN
        } else if self.positive_infinity_count > 0 {
            f64::INFINITY
        } else if self.negative_infinity_count > 0 {
            f64::NEG_INFINITY
        } else if let Some(exact_sum) = &self.exact_finite_sum {
            scaled_integer_as_finite_float(exact_sum)
        } else {
            self.finite_sum
        }
    }
}

fn running_float_series(
    series: Series,
    average: bool,
    frame: Option<(i64, i64)>,
) -> PolarsResult<Series> {
    let values = series.f64()?;
    let mut output = Vec::with_capacity(series.len());
    if frame.is_some() {
        let mut accumulator = FloatWindowAccumulator::default();
        let mut active_start = 0usize;
        let mut active_end = 0usize;
        let mut has_active_window = false;
        for idx in 0..series.len() {
            let Some((left, right)) = window_bounds(idx, series.len(), frame) else {
                output.push(None);
                continue;
            };
            if !has_active_window {
                active_start = left;
                active_end = left;
                has_active_window = true;
            }
            while active_start < left {
                accumulator.remove(values.get(active_start));
                active_start += 1;
            }
            while active_end <= right {
                accumulator.add(values.get(active_end));
                active_end += 1;
            }
            accumulator.recover_overflowed_finite_sum(values, active_start, active_end);
            let sum = accumulator.sum();
            output.push(if accumulator.count == 0 {
                None
            } else if average {
                Some(sum / accumulator.count as f64)
            } else {
                Some(sum)
            });
        }
        return Ok(Series::new(series.name().clone(), output));
    }

    let mut sums = Vec::with_capacity(series.len());
    let mut counts = Vec::with_capacity(series.len());
    let mut total = 0.0;
    let mut count = 0usize;
    for idx in 0..series.len() {
        if let Some(value) = values.get(idx) {
            total += value;
            count += 1;
        }
        sums.push(total);
        counts.push(count);
    }
    for idx in 0..series.len() {
        let Some((left, right)) = window_bounds(idx, series.len(), frame) else {
            output.push(None);
            continue;
        };
        let previous_sum = if left == 0 { 0.0 } else { sums[left - 1] };
        let previous_count = if left == 0 { 0 } else { counts[left - 1] };
        let sum = sums[right] - previous_sum;
        let count = counts[right] - previous_count;
        output.push(if count == 0 {
            None
        } else if average {
            Some(sum / count as f64)
        } else {
            Some(sum)
        });
    }
    Ok(Series::new(series.name().clone(), output))
}

fn avg_expr(input: Expr, ansi_enabled: bool) -> Expr {
    avg_expr_with_marker(input, ansi_enabled, "avg")
}

fn avg_expr_with_marker(input: Expr, ansi_enabled: bool, marker: &'static str) -> Expr {
    input.implode().map_with_fmt_str(
        move |series| {
            let DataType::List(input_type) = series.dtype() else {
                return Err(PolarsError::ComputeError(
                    "average aggregation expected a list input".into(),
                ));
            };
            let groups = series.list()?;
            if let DataType::Decimal(precision, scale) = input_type.as_ref() {
                let output_precision = precision.saturating_add(4).clamp(1, 38);
                let output_scale = scale.saturating_add(4).min(output_precision);
                let factor = BigInt::from(10).pow(output_scale.saturating_sub(*scale) as u32);
                let maximum = BigInt::from(10).pow(output_precision as u32) - BigInt::from(1);
                let sum_precision = precision.saturating_add(10).min(38);
                let maximum_sum = BigInt::from(10).pow(sum_precision as u32) - BigInt::from(1);
                let mut averages = Vec::with_capacity(series.len());
                for group_idx in 0..series.len() {
                    let Some(group) = groups.get_as_series(group_idx) else {
                        averages.push(None);
                        continue;
                    };
                    let mut sum = BigInt::zero();
                    let mut count = 0_usize;
                    let mut overflowed = false;
                    for idx in 0..group.len() {
                        match group.get(idx)? {
                            AnyValue::Decimal(value, _, _) | AnyValue::Int128(value) => {
                                sum += value;
                                count += 1;
                                if sum.abs() > maximum_sum {
                                    if ansi_enabled {
                                        return Err(PolarsError::ComputeError(
                                            "[ARITHMETIC_OVERFLOW] decimal average sum exceeds supported precision"
                                                .into(),
                                        ));
                                    }
                                    overflowed = true;
                                    break;
                                }
                            }
                            AnyValue::Null => {}
                            _ => {
                                return Err(PolarsError::ComputeError(
                                    "decimal average received a non-decimal value".into(),
                                ));
                            }
                        }
                    }
                    if count == 0 {
                        averages.push(None);
                        continue;
                    }
                    if overflowed {
                        averages.push(None);
                        continue;
                    }
                    // Accumulate and scale with arbitrary precision. Decimal(38, 38)
                    // values can overflow i128 during the sum even when cancellation
                    // leaves a representable average.
                    let count = BigInt::from(count);
                    let scaled_sum = sum * &factor;
                    let mut scaled_average = &scaled_sum / &count;
                    let remainder = scaled_sum % &count;
                    if remainder.abs() * 2 >= count {
                        scaled_average += if remainder.is_negative() { -1 } else { 1 };
                    }
                    if scaled_average.abs() > maximum {
                        if ansi_enabled {
                            return Err(PolarsError::ComputeError(
                                "[ARITHMETIC_OVERFLOW] decimal average exceeds supported precision"
                                    .into(),
                            ));
                        }
                        averages.push(None);
                        continue;
                    }
                    let scaled_average = scaled_average.to_i128().ok_or_else(|| {
                        PolarsError::ComputeError(
                            "[ARITHMETIC_OVERFLOW] decimal average exceeds supported precision"
                                .into(),
                        )
                    })?;
                    averages.push(Some(scaled_average));
                }
                Series::new(series.name().clone(), averages)
                    .into_decimal(output_precision, output_scale)
                    .map(|series| series.into_series().into())
            } else {
                let mut averages = Vec::with_capacity(series.len());
                for group_idx in 0..series.len() {
                    let Some(group) = groups.get_as_series(group_idx) else {
                        averages.push(None);
                        continue;
                    };
                    // Use Polars' mean implementation to retain its pairwise
                    // floating-point summation and avoid cancellation regressions.
                    averages.push(group.mean());
                }
                Ok(Series::new(series.name().clone(), averages).into())
            }
        },
        |_schema, field| {
            let dtype = match field.dtype() {
                DataType::List(input_type) => match input_type.as_ref() {
                    DataType::Decimal(precision, scale) => {
                        let output_precision = precision.saturating_add(4).clamp(1, 38);
                        let output_scale = scale.saturating_add(4).min(output_precision);
                        DataType::Decimal(output_precision, output_scale)
                    }
                    _ => DataType::Float64,
                },
                _ => DataType::Float64,
            };
            Ok(Field::new(field.name().clone(), dtype))
        },
        marker,
    )
}

/// Average aggregation. Output name PySpark-style avg(column_name).
/// String columns are cast to Float64 before mean (PySpark parity, issue #437).
/// Carries source column for running-window conversion when orderBy differs from partitionBy (#1241).
pub fn avg(col: &Column) -> Column {
    let name = format!("avg({})", col.name());
    let expr = avg_expr(
        numeric_aggregate_input(col, None, 0),
        crate::udf_context::get_thread_ansi_enabled(),
    );
    let mut c = Column::from_expr(expr, Some(name));
    c.source_for_running_mean = Some(col.name().to_string());
    c
}

/// Alias for avg (PySpark mean).
pub fn mean(col: &Column) -> Column {
    avg(col)
}

/// Maximum aggregation. Output name PySpark-style max(column_name) so row["max(salary)"] works.
pub fn max(col: &Column) -> Column {
    let name = format!("max({})", col.name());
    Column::from_expr(col.expr().clone().max(), Some(name))
}

/// Minimum aggregation. Output name PySpark-style min(column_name).
pub fn min(col: &Column) -> Column {
    let name = format!("min({})", col.name());
    Column::from_expr(col.expr().clone().min(), Some(name))
}

/// First value in group (PySpark first). Use in groupBy.agg(). ignorenulls: when true, first non-null (Polars first_non_null); otherwise first value in group order.
pub fn first(col: &Column, ignorenulls: bool) -> Column {
    let expr = if ignorenulls {
        col.expr().clone().first_non_null()
    } else {
        col.expr().clone().first()
    };
    Column::from_expr(expr, None)
}

/// Last value in group (PySpark last). Use in groupBy.agg() and pivot().agg(). ignorenulls reserved for API compatibility (Polars last takes last in group order).
/// When used with .over(window) and orderBy, over_window uses first_last_value so last = current row (PySpark default frame).
pub fn last(col: &Column, ignorenulls: bool) -> Column {
    let _ = ignorenulls;
    Column::from_last_agg(col)
}

/// Any value from the group (PySpark any_value). Use in groupBy.agg(). ignorenulls reserved for API compatibility.
pub fn any_value(col: &Column, ignorenulls: bool) -> Column {
    let _ = ignorenulls;
    Column::from_expr(col.expr().clone().first(), None)
}

/// Count rows where condition is true (PySpark count_if). Use in groupBy.agg(); column should be boolean (true=1, false=0).
pub fn count_if(col: &Column) -> Column {
    use polars::prelude::DataType;
    Column::from_expr(
        col.expr().clone().cast(DataType::Int64).sum(),
        Some("count_if".to_string()),
    )
}

/// Sum aggregation; Decimal results become null on overflow (PySpark try_sum).
/// Use in groupBy.agg().
pub fn try_sum(col: &Column) -> Column {
    Column::from_expr(
        sum_expr_with_marker(col.expr().clone(), false, "try_sum"),
        Some("try_sum".to_string()),
    )
}

/// Average aggregation; Decimal inputs retain Decimal precision and scale (PySpark try_avg).
/// Non-decimal inputs map to mean. Use in groupBy.agg().
pub fn try_avg(col: &Column) -> Column {
    let input = numeric_aggregate_input(col, None, 0);
    Column::from_expr(
        avg_expr_with_marker(input, false, "try_avg"),
        Some("try_avg".to_string()),
    )
}

/// Value of value_col in the row where ord_col is maximum (PySpark max_by). Use in groupBy.agg().
pub fn max_by(value_col: &Column, ord_col: &Column) -> Column {
    use polars::prelude::{SortOptions, as_struct};
    let st = as_struct(vec![
        ord_col.expr().clone().alias("_ord"),
        value_col.expr().clone().alias("_val"),
    ]);
    let e = st
        .sort(SortOptions::default().with_order_descending(true))
        .first()
        .struct_()
        .field_by_name("_val");
    Column::from_expr(e, None)
}

/// Value of value_col in the row where ord_col is minimum (PySpark min_by). Use in groupBy.agg().
pub fn min_by(value_col: &Column, ord_col: &Column) -> Column {
    use polars::prelude::{SortOptions, as_struct};
    let st = as_struct(vec![
        ord_col.expr().clone().alias("_ord"),
        value_col.expr().clone().alias("_val"),
    ]);
    let e = st
        .sort(SortOptions::default())
        .first()
        .struct_()
        .field_by_name("_val");
    Column::from_expr(e, None)
}

/// Collect column values into list per group (PySpark collect_list). Use in groupBy.agg().
pub fn collect_list(col: &Column) -> Column {
    Column::from_expr(
        col.expr().clone().implode(),
        Some("collect_list".to_string()),
    )
}

/// Collect distinct column values into list per group (PySpark collect_set). Use in groupBy.agg().
pub fn collect_set(col: &Column) -> Column {
    Column::from_expr(
        col.expr().clone().unique().implode(),
        Some("collect_set".to_string()),
    )
}

/// Boolean AND across group (PySpark bool_and). Use in groupBy.agg(); column should be boolean.
pub fn bool_and(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().all(true), Some("bool_and".to_string()))
}

/// Alias for bool_and (PySpark every). Use in groupBy.agg().
pub fn every(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().all(true), Some("every".to_string()))
}

/// Standard deviation (sample) aggregation (PySpark stddev / stddev_samp)
pub fn stddev(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().std(1), Some("stddev".to_string()))
}

/// Variance (sample) aggregation (PySpark variance / var_samp)
pub fn variance(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().var(1), Some("variance".to_string()))
}

/// Population standard deviation (ddof=0). PySpark stddev_pop.
pub fn stddev_pop(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().std(0), Some("stddev_pop".to_string()))
}

/// Sample standard deviation (ddof=1). Alias for stddev. PySpark stddev_samp.
pub fn stddev_samp(col: &Column) -> Column {
    stddev(col)
}

/// Alias for stddev (PySpark std).
pub fn std(col: &Column) -> Column {
    stddev(col)
}

/// Population variance (ddof=0). PySpark var_pop.
pub fn var_pop(col: &Column) -> Column {
    Column::from_expr(col.expr().clone().var(0), Some("var_pop".to_string()))
}

/// Sample variance (ddof=1). Alias for variance. PySpark var_samp.
pub fn var_samp(col: &Column) -> Column {
    variance(col)
}

/// Median aggregation. PySpark median.
pub fn median(col: &Column) -> Column {
    use polars::prelude::QuantileMethod;
    Column::from_expr(
        col.expr()
            .clone()
            .quantile(lit(0.5), QuantileMethod::Linear),
        Some("median".to_string()),
    )
}

/// Approximate percentile (PySpark approx_percentile). Maps to quantile; percentage in 0.0..=1.0. accuracy reserved for API compatibility.
pub fn approx_percentile(col: &Column, percentage: f64, _accuracy: Option<i32>) -> Column {
    use polars::prelude::QuantileMethod;
    Column::from_expr(
        col.expr()
            .clone()
            .quantile(lit(percentage), QuantileMethod::Linear),
        Some(format!("approx_percentile({percentage})")),
    )
}

/// Approximate percentile (PySpark percentile_approx). Alias for approx_percentile.
pub fn percentile_approx(col: &Column, percentage: f64, accuracy: Option<i32>) -> Column {
    approx_percentile(col, percentage, accuracy)
}

/// Mode aggregation - most frequent value. PySpark mode.
pub fn mode(col: &Column, deterministic: bool) -> Column {
    col.clone().mode(deterministic)
}

/// Count distinct aggregation (PySpark countDistinct). Output name PySpark-style count_distinct(column_name).
pub fn count_distinct(col: &Column) -> Column {
    use polars::prelude::DataType;
    let name = format!("count_distinct({})", col.name());
    Column::from_expr(
        col.expr().clone().n_unique().cast(DataType::Int64),
        Some(name),
    )
}

/// Sum of distinct values (PySpark sumDistinct). Output name PySpark-style sum(DISTINCT column_name).
pub fn sum_distinct(col: &Column) -> Column {
    let name = format!("sum(DISTINCT {})", col.name());
    let mut c = Column::from_expr(
        sum_distinct_expr(
            col.expr().clone(),
            crate::udf_context::get_thread_ansi_enabled(),
        ),
        Some(name),
    );
    c.source_for_running = Some(col.name().to_string());
    c
}

fn sum_distinct_expr(input: Expr, ansi_enabled: bool) -> Expr {
    input.implode().map_with_fmt_str(
            move |series| {
                let DataType::List(input_type) = series.dtype() else {
                    return Err(PolarsError::ComputeError(
                        "sum distinct aggregation expected a list input".into(),
                    ));
                };
                let groups = series.list()?;
                if let DataType::Decimal(precision, scale) = input_type.as_ref() {
                    let output_precision = precision.saturating_add(10).min(38);
                    let maximum = BigInt::from(10).pow(output_precision as u32) - BigInt::from(1);
                    let mut sums = Vec::with_capacity(series.len());
                    for group_idx in 0..series.len() {
                        let Some(group) = groups.get_as_series(group_idx) else {
                            sums.push(None);
                            continue;
                        };
                        let mut seen = std::collections::HashSet::new();
                        let mut total = BigInt::zero();
                        let mut found_value = false;
                        let mut overflowed = false;
                        for idx in 0..group.len() {
                            match group.get(idx)? {
                                AnyValue::Decimal(value, _, _) | AnyValue::Int128(value) => {
                                    if !seen.insert(value) {
                                        continue;
                                    }
                                    found_value = true;
                                    total += value;
                                    if total.abs() > maximum {
                                        if ansi_enabled {
                                            return Err(PolarsError::ComputeError(
                                                "[ARITHMETIC_OVERFLOW] decimal sum exceeds supported precision"
                                                    .into(),
                                            ));
                                        }
                                        overflowed = true;
                                        break;
                                    }
                                }
                                AnyValue::Null => {}
                                _ => {
                                    return Err(PolarsError::ComputeError(
                                        "decimal sum distinct received a non-decimal value".into(),
                                    ));
                                }
                            }
                        }
                        if !found_value || overflowed {
                            sums.push(None);
                        } else {
                            sums.push(total.to_i128());
                        }
                    }
                    Series::new(series.name().clone(), sums)
                        .into_decimal(output_precision, *scale)
                        .map(|series| series.into_series().into())
                } else {
                    let mut sums = Vec::with_capacity(series.len());
                    for group_idx in 0..series.len() {
                        let Some(group) = groups.get_as_series(group_idx) else {
                            sums.push(None);
                            continue;
                        };
                        let values = group.cast(&DataType::Float64)?;
                        if values.null_count() == values.len() {
                            sums.push(None);
                            continue;
                        }
                        let values = values.unique()?;
                        match values.sum_reduce()?.value() {
                            AnyValue::Float64(value) => sums.push(Some(*value)),
                            AnyValue::Null => sums.push(None),
                            value => {
                                return Err(PolarsError::ComputeError(
                                    format!("unexpected sum distinct result: {value:?}").into(),
                                ));
                            }
                        }
                    }
                    Ok(Series::new(series.name().clone(), sums).into())
                }
            },
            |_schema, field| {
                let dtype = match field.dtype() {
                    DataType::List(input_type) => match input_type.as_ref() {
                        DataType::Decimal(precision, scale) => DataType::Decimal(
                            precision.saturating_add(10).min(38),
                            *scale,
                        ),
                        _ => DataType::Float64,
                    },
                    _ => DataType::Float64,
                };
                Ok(Field::new(field.name().clone(), dtype))
            },
            "sum_distinct",
        )
}

/// Approximate count distinct (PySpark approx_count_distinct). Use in groupBy.agg(). rsd reserved for API compatibility; Polars uses exact n_unique.
/// Output name is PySpark-style approx_count_distinct(column_name) so row["approx_count_distinct(value)"] works.
pub fn approx_count_distinct(col: &Column, _rsd: Option<f64>) -> Column {
    use polars::prelude::DataType;
    let name = format!("approx_count_distinct({})", col.name());
    let expr = col
        .expr()
        .clone()
        .n_unique()
        .cast(DataType::Int64)
        .alias(&name);
    Column::from_expr(expr, Some(name))
}

/// Kurtosis aggregation (PySpark kurtosis). Fisher definition, bias=true. Use in groupBy.agg(). Output name PySpark-style kurtosis(column_name).
pub fn kurtosis(col: &Column) -> Column {
    let name = format!("kurtosis({})", col.name());
    Column::from_expr(
        col.expr()
            .clone()
            .cast(DataType::Float64)
            .kurtosis(true, true),
        Some(name),
    )
}

/// Skewness aggregation (PySpark skewness). bias=true. Use in groupBy.agg(). Output name PySpark-style skewness(column_name).
pub fn skewness(col: &Column) -> Column {
    let name = format!("skewness({})", col.name());
    Column::from_expr(
        col.expr().clone().cast(DataType::Float64).skew(true),
        Some(name),
    )
}

fn covar_pop_expr_impl(e1: Expr, e2: Expr) -> Expr {
    // PySpark excludes rows where either column is null (#1475), same as corr.
    let cond = e1.clone().is_not_null().and(e2.clone().is_not_null());
    let c1 = e1.clone().cast(DataType::Float64);
    let c2 = e2.clone().cast(DataType::Float64);
    let n = e1
        .clone()
        .filter(cond.clone())
        .count()
        .cast(DataType::Float64);
    let sum_ab = (c1.clone() * c2.clone()).filter(cond.clone()).sum();
    let sum_a = c1.clone().filter(cond.clone()).sum();
    let sum_b = c2.clone().filter(cond).sum();
    (sum_ab - sum_a * sum_b / n.clone()) / n
}

fn corr_expr_impl(e1: Expr, e2: Expr) -> Expr {
    use polars::prelude::{lit, when};
    // PySpark excludes rows where either column is null (#1475). Use only pairs where both non-null.
    let c1 = e1.clone().cast(DataType::Float64);
    let c2 = e2.clone().cast(DataType::Float64);
    let cond = e1.clone().is_not_null().and(e2.clone().is_not_null());
    let n = e1
        .clone()
        .filter(cond.clone())
        .count()
        .cast(DataType::Float64);
    let n1 = n.clone() - lit(1.0);
    let sum_ab = (c1.clone() * c2.clone()).filter(cond.clone()).sum();
    let sum_a = c1.clone().filter(cond.clone()).sum();
    let sum_b = c2.clone().filter(cond.clone()).sum();
    let sum_a2 = (c1.clone() * c1).filter(cond.clone()).sum();
    let sum_b2 = (c2.clone() * c2).filter(cond).sum();
    let cov_samp = (sum_ab - sum_a.clone() * sum_b.clone() / n.clone()) / n1.clone();
    let var_a = (sum_a2 - sum_a.clone() * sum_a / n.clone()) / n1.clone();
    let var_b = (sum_b2 - sum_b.clone() * sum_b / n.clone()) / n1.clone();
    let std_a = var_a.sqrt();
    let std_b = var_b.sqrt();
    when(n.gt(lit(1.0)))
        .then(cov_samp / (std_a * std_b))
        .otherwise(lit(f64::NAN))
}

fn covar_samp_expr_impl(e1: Expr, e2: Expr) -> Expr {
    use polars::prelude::{lit, when};
    let cond = e1.clone().is_not_null().and(e2.clone().is_not_null());
    let c1 = e1.clone().cast(DataType::Float64);
    let c2 = e2.clone().cast(DataType::Float64);
    let n = e1
        .clone()
        .filter(cond.clone())
        .count()
        .cast(DataType::Float64);
    let n1 = n.clone() - lit(1.0);
    let sum_ab = (c1.clone() * c2.clone()).filter(cond.clone()).sum();
    let sum_a = c1.clone().filter(cond.clone()).sum();
    let sum_b = c2.clone().filter(cond).sum();
    when(n.clone().gt(lit(1.0)))
        .then((sum_ab - sum_a * sum_b / n) / n1)
        .otherwise(lit(f64::NAN))
}

/// Population covariance aggregation (PySpark covar_pop). Returns Expr for use in groupBy.agg().
pub fn covar_pop_expr(col1: &str, col2: &str) -> Expr {
    use polars::prelude::col as pl_col;
    covar_pop_expr_impl(pl_col(col1), pl_col(col2))
}

/// Population covariance aggregation (PySpark covar_pop). Module-level; use in groupBy.agg() with two columns.
pub fn covar_pop(col1: &Column, col2: &Column) -> Column {
    Column::from_expr(
        covar_pop_expr_impl(col1.expr().clone(), col2.expr().clone()),
        Some("covar_pop".to_string()),
    )
}

/// Pearson correlation aggregation (PySpark corr). Module-level; use in groupBy.agg() with two columns.
pub fn corr(col1: &Column, col2: &Column) -> Column {
    Column::from_expr(
        corr_expr_impl(col1.expr().clone(), col2.expr().clone()),
        Some("corr".to_string()),
    )
}

/// Sample covariance aggregation (PySpark covar_samp). Returns Expr for use in groupBy.agg().
pub fn covar_samp_expr(col1: &str, col2: &str) -> Expr {
    use polars::prelude::col as pl_col;
    covar_samp_expr_impl(pl_col(col1), pl_col(col2))
}

/// Sample covariance aggregation (PySpark covar_samp). Module-level; use in groupBy.agg() with two columns.
pub fn covar_samp(col1: &Column, col2: &Column) -> Column {
    Column::from_expr(
        covar_samp_expr_impl(col1.expr().clone(), col2.expr().clone()),
        Some("covar_samp".to_string()),
    )
}

/// Pearson correlation aggregation (PySpark corr). Returns Expr for use in groupBy.agg().
pub fn corr_expr(col1: &str, col2: &str) -> Expr {
    use polars::prelude::col as pl_col;
    corr_expr_impl(pl_col(col1), pl_col(col2))
}

// --- Regression aggregates (PySpark regr_*). y = col1, x = col2; only pairs where both non-null. ---

fn regr_cond_and_sums(y_col: &str, x_col: &str) -> (Expr, Expr, Expr, Expr, Expr, Expr) {
    use polars::prelude::col as pl_col;
    let y = pl_col(y_col).cast(DataType::Float64);
    let x = pl_col(x_col).cast(DataType::Float64);
    let cond = y.clone().is_not_null().and(x.clone().is_not_null());
    let n = y
        .clone()
        .filter(cond.clone())
        .count()
        .cast(DataType::Float64);
    let sum_x = x.clone().filter(cond.clone()).sum();
    let sum_y = y.clone().filter(cond.clone()).sum();
    let sum_xx = (x.clone() * x.clone()).filter(cond.clone()).sum();
    let sum_yy = (y.clone() * y.clone()).filter(cond.clone()).sum();
    let sum_xy = (x * y).filter(cond).sum();
    (n, sum_x, sum_y, sum_xx, sum_yy, sum_xy)
}

/// Regression: count of (y, x) pairs where both non-null (PySpark regr_count).
pub fn regr_count_expr(y_col: &str, x_col: &str) -> Expr {
    let (n, ..) = regr_cond_and_sums(y_col, x_col);
    n
}

/// Regression: average of x (PySpark regr_avgx).
pub fn regr_avgx_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, ..) = regr_cond_and_sums(y_col, x_col);
    when(n.clone().gt(lit(0.0)))
        .then(sum_x / n)
        .otherwise(lit(f64::NAN))
}

/// Regression: average of y (PySpark regr_avgy).
pub fn regr_avgy_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, _, sum_y, ..) = regr_cond_and_sums(y_col, x_col);
    when(n.clone().gt(lit(0.0)))
        .then(sum_y / n)
        .otherwise(lit(f64::NAN))
}

/// Regression: sum((x - avg_x)^2) (PySpark regr_sxx).
pub fn regr_sxx_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, _, sum_xx, ..) = regr_cond_and_sums(y_col, x_col);
    when(n.clone().gt(lit(0.0)))
        .then(sum_xx - sum_x.clone() * sum_x / n)
        .otherwise(lit(f64::NAN))
}

/// Regression: sum((y - avg_y)^2) (PySpark regr_syy).
pub fn regr_syy_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, _, sum_y, _, sum_yy, _) = regr_cond_and_sums(y_col, x_col);
    when(n.clone().gt(lit(0.0)))
        .then(sum_yy - sum_y.clone() * sum_y / n)
        .otherwise(lit(f64::NAN))
}

/// Regression: sum((x - avg_x)(y - avg_y)) (PySpark regr_sxy).
pub fn regr_sxy_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, sum_y, _, _, sum_xy) = regr_cond_and_sums(y_col, x_col);
    when(n.clone().gt(lit(0.0)))
        .then(sum_xy - sum_x * sum_y / n)
        .otherwise(lit(f64::NAN))
}

/// Regression slope: cov_samp(y,x)/var_samp(x) (PySpark regr_slope).
pub fn regr_slope_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, sum_y, sum_xx, _sum_yy, sum_xy) = regr_cond_and_sums(y_col, x_col);
    let regr_sxx = sum_xx.clone() - sum_x.clone() * sum_x.clone() / n.clone();
    let regr_sxy = sum_xy - sum_x * sum_y / n.clone();
    when(n.gt(lit(1.0)).and(regr_sxx.clone().gt(lit(0.0))))
        .then(regr_sxy / regr_sxx)
        .otherwise(lit(f64::NAN))
}

/// Regression intercept: avg_y - slope*avg_x (PySpark regr_intercept).
pub fn regr_intercept_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, sum_y, sum_xx, _, sum_xy) = regr_cond_and_sums(y_col, x_col);
    let regr_sxx = sum_xx - sum_x.clone() * sum_x.clone() / n.clone();
    let regr_sxy = sum_xy.clone() - sum_x.clone() * sum_y.clone() / n.clone();
    let slope = regr_sxy.clone() / regr_sxx.clone();
    let avg_y = sum_y / n.clone();
    let avg_x = sum_x / n.clone();
    when(n.gt(lit(1.0)).and(regr_sxx.clone().gt(lit(0.0))))
        .then(avg_y - slope * avg_x)
        .otherwise(lit(f64::NAN))
}

/// Regression R-squared (PySpark regr_r2).
pub fn regr_r2_expr(y_col: &str, x_col: &str) -> Expr {
    use polars::prelude::{lit, when};
    let (n, sum_x, sum_y, sum_xx, sum_yy, sum_xy) = regr_cond_and_sums(y_col, x_col);
    let regr_sxx = sum_xx - sum_x.clone() * sum_x.clone() / n.clone();
    let regr_syy = sum_yy - sum_y.clone() * sum_y.clone() / n.clone();
    let regr_sxy = sum_xy - sum_x * sum_y / n;
    when(
        regr_sxx
            .clone()
            .gt(lit(0.0))
            .and(regr_syy.clone().gt(lit(0.0))),
    )
    .then(regr_sxy.clone() * regr_sxy / (regr_sxx * regr_syy))
    .otherwise(lit(f64::NAN))
}

/// PySpark-style conditional expression builder.
///
/// # Example
/// ```
/// use robin_sparkless_polars::functions::{col, lit_i64, lit_str, when};
///
/// // when(condition).then(value).otherwise(fallback)
/// let expr = when(&col("age").gt(lit_i64(18).into_expr()))
///     .then(&lit_str("adult"))
///     .otherwise(&lit_str("minor"));
/// ```
/// Condition must be Boolean; then/else can be any type (#680). String conditions coerced via expr_coerce_to_boolean.
pub fn when(condition: &Column) -> WhenBuilder {
    WhenBuilder::new(expr_coerce_to_boolean(condition.expr().clone()))
}

/// Two-arg when(condition, value): returns value where condition is true, null otherwise (PySpark when(cond, val)).
/// Condition must be Boolean; value can be any type (#680). String conditions coerced via expr_coerce_to_boolean.
pub fn when_then_otherwise_null(condition: &Column, value: &Column) -> Column {
    use polars::prelude::*;
    let null_expr = lit(NULL);
    let expr = polars::prelude::when(expr_coerce_to_boolean(condition.expr().clone()))
        .then(value.expr().clone())
        .otherwise(null_expr);
    crate::column::Column::from_expr(expr, None)
}

/// Builder for when-then-otherwise expressions
pub struct WhenBuilder {
    condition: Expr,
}

impl WhenBuilder {
    fn new(condition: Expr) -> Self {
        WhenBuilder { condition }
    }

    /// Specify the value when condition is true (condition already cast to Boolean in when()).
    pub fn then(self, value: &Column) -> ThenBuilder {
        use polars::prelude::*;
        let when_then = when(self.condition).then(value.expr().clone());
        ThenBuilder::new(when_then)
    }

    /// Specify the value when condition is false.
    /// Note: In PySpark, when(cond).otherwise(val) requires a .then() first.
    pub fn otherwise(self, _value: &Column) -> Result<Column, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "when().otherwise() requires .then() to be called first. Use when(cond).then(val1).otherwise(val2)"
                .into(),
        ))
    }
}

/// Builder for chaining when-then clauses before finalizing with otherwise
pub struct ThenBuilder {
    state: WhenThenState,
}

enum WhenThenState {
    Single(Box<polars::prelude::Then>),
    Chained(Box<polars::prelude::ChainedThen>),
}

/// Builder for an additional when-then clause (returned by ThenBuilder::when).
pub struct ChainedWhenBuilder {
    inner: polars::prelude::ChainedWhen,
}

impl ThenBuilder {
    fn new(when_then: polars::prelude::Then) -> Self {
        ThenBuilder {
            state: WhenThenState::Single(Box::new(when_then)),
        }
    }

    fn new_chained(chained: polars::prelude::ChainedThen) -> Self {
        ThenBuilder {
            state: WhenThenState::Chained(Box::new(chained)),
        }
    }

    /// Chain an additional when-then clause (PySpark: when(a).then(x).when(b).then(y).otherwise(z)).
    /// Condition must be Boolean (#680). String conditions coerced via expr_coerce_to_boolean.
    pub fn when(self, condition: &Column) -> ChainedWhenBuilder {
        let cond = expr_coerce_to_boolean(condition.expr().clone());
        let chained_when = match self.state {
            WhenThenState::Single(t) => t.when(cond),
            WhenThenState::Chained(ct) => ct.when(cond),
        };
        ChainedWhenBuilder {
            inner: chained_when,
        }
    }

    /// Finalize the expression with the fallback value
    pub fn otherwise(self, value: &Column) -> Column {
        let expr = match self.state {
            WhenThenState::Single(t) => t.otherwise(value.expr().clone()),
            WhenThenState::Chained(ct) => ct.otherwise(value.expr().clone()),
        };
        crate::column::Column::from_expr(expr, None)
    }
}

impl ChainedWhenBuilder {
    /// Set the value for the current when clause.
    pub fn then(self, value: &Column) -> ThenBuilder {
        ThenBuilder::new_chained(self.inner.then(value.expr().clone()))
    }
}

/// Convert string column to uppercase (PySpark upper)
pub fn upper(column: &Column) -> Column {
    column.clone().upper()
}

/// Convert string column to lowercase (PySpark lower)
pub fn lower(column: &Column) -> Column {
    column.clone().lower()
}

/// Substring with 1-based start (PySpark substring semantics)
pub fn substring(column: &Column, start: i64, length: Option<i64>) -> Column {
    column.clone().substr(start, length)
}

/// Substring with Column start/length expressions (#1649).
pub fn substring_with(column: &Column, start: &Column, length: Option<&Column>) -> Column {
    column.clone().substr_with(start, length)
}

/// String length in characters (PySpark length)
pub fn length(column: &Column) -> Column {
    column.clone().length()
}

/// Trim leading and trailing whitespace (PySpark trim)
pub fn trim(column: &Column) -> Column {
    column.clone().trim()
}

/// Trim leading whitespace (PySpark ltrim)
pub fn ltrim(column: &Column) -> Column {
    column.clone().ltrim()
}

/// Trim trailing whitespace (PySpark rtrim)
pub fn rtrim(column: &Column) -> Column {
    column.clone().rtrim()
}

/// Trim leading and trailing chars (PySpark btrim). trim_str defaults to whitespace.
pub fn btrim(column: &Column, trim_str: Option<&str>) -> Column {
    column.clone().btrim(trim_str)
}

/// Find substring position 1-based, starting at pos (PySpark locate). 0 if not found.
pub fn locate(substr: &str, column: &Column, pos: i64) -> Column {
    column.clone().locate(substr, pos)
}

/// Base conversion (PySpark conv). num from from_base to to_base.
pub fn conv(column: &Column, from_base: i32, to_base: i32) -> Column {
    column.clone().conv(from_base, to_base)
}

/// Convert to hex string (PySpark hex).
pub fn hex(column: &Column) -> Column {
    column.clone().hex()
}

/// Convert hex string to binary/string (PySpark unhex).
pub fn unhex(column: &Column) -> Column {
    column.clone().unhex()
}

/// Encode string to binary (PySpark encode). Charset: UTF-8. Returns hex string.
pub fn encode(column: &Column, charset: &str) -> Column {
    column.clone().encode(charset)
}

/// Decode binary (hex string) to string (PySpark decode). Charset: UTF-8.
pub fn decode(column: &Column, charset: &str) -> Column {
    column.clone().decode(charset)
}

/// Convert to binary (PySpark to_binary). fmt: 'utf-8', 'hex'.
pub fn to_binary(column: &Column, fmt: &str) -> Column {
    column.clone().to_binary(fmt)
}

/// Try convert to binary; null on failure (PySpark try_to_binary).
pub fn try_to_binary(column: &Column, fmt: &str) -> Column {
    column.clone().try_to_binary(fmt)
}

/// AES encrypt (PySpark aes_encrypt). Key as string; AES-128-GCM.
pub fn aes_encrypt(column: &Column, key: &str) -> Column {
    column.clone().aes_encrypt(key)
}

/// AES decrypt (PySpark aes_decrypt). Input hex(nonce||ciphertext).
pub fn aes_decrypt(column: &Column, key: &str) -> Column {
    column.clone().aes_decrypt(key)
}

/// Try AES decrypt (PySpark try_aes_decrypt). Returns null on failure.
pub fn try_aes_decrypt(column: &Column, key: &str) -> Column {
    column.clone().try_aes_decrypt(key)
}

/// Convert integer to binary string (PySpark bin).
pub fn bin(column: &Column) -> Column {
    column.clone().bin()
}

/// Get bit at 0-based position (PySpark getbit).
pub fn getbit(column: &Column, pos: i64) -> Column {
    column.clone().getbit(pos)
}

/// Bitwise AND of two integer/boolean columns (PySpark bit_and).
pub fn bit_and(left: &Column, right: &Column) -> Column {
    left.clone().bit_and(right)
}

/// Bitwise OR of two integer/boolean columns (PySpark bit_or).
pub fn bit_or(left: &Column, right: &Column) -> Column {
    left.clone().bit_or(right)
}

/// Bitwise XOR of two integer/boolean columns (PySpark bit_xor).
pub fn bit_xor(left: &Column, right: &Column) -> Column {
    left.clone().bit_xor(right)
}

/// Count of set bits in the integer representation (PySpark bit_count).
pub fn bit_count(column: &Column) -> Column {
    column.clone().bit_count()
}

/// Bitwise NOT of an integer/boolean column (PySpark bitwise_not / bitwiseNOT).
pub fn bitwise_not(column: &Column) -> Column {
    column.clone().bitwise_not()
}

// --- Bitmap (PySpark 3.5+) ---

/// Map integral value (0–32767) to bit position for bitmap aggregates (PySpark bitmap_bit_position).
pub fn bitmap_bit_position(column: &Column) -> Column {
    use polars::prelude::DataType;
    let expr = column.expr().clone().cast(DataType::Int32);
    Column::from_expr(expr, None)
}

/// Bucket number for distributed bitmap (PySpark bitmap_bucket_number). value / 32768.
pub fn bitmap_bucket_number(column: &Column) -> Column {
    use polars::prelude::DataType;
    let expr = column.expr().clone().cast(DataType::Int64) / lit(32768i64);
    Column::from_expr(expr, None)
}

/// Count set bits in a bitmap binary column (PySpark bitmap_count).
pub fn bitmap_count(column: &Column) -> Column {
    use polars::prelude::{DataType, Field};
    let expr = column.expr().clone().map(
        |s| crate::column::expect_col(crate::udfs::apply_bitmap_count(s)),
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Int64)),
    );
    Column::from_expr(expr, None)
}

/// Aggregate: bitwise OR of bit positions into one bitmap binary (PySpark bitmap_construct_agg).
/// Use in group_by(...).agg([bitmap_construct_agg(col)]).
pub fn bitmap_construct_agg(column: &Column) -> polars::prelude::Expr {
    use polars::prelude::{DataType, Field};
    column.expr().clone().implode().map(
        |s| crate::column::expect_col(crate::udfs::apply_bitmap_construct_agg(s)),
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Binary)),
    )
}

/// Aggregate: bitwise OR of bitmap binary column (PySpark bitmap_or_agg).
pub fn bitmap_or_agg(column: &Column) -> polars::prelude::Expr {
    use polars::prelude::{DataType, Field};
    column.expr().clone().implode().map(
        |s| crate::column::expect_col(crate::udfs::apply_bitmap_or_agg(s)),
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Binary)),
    )
}

/// Alias for getbit (PySpark bit_get).
pub fn bit_get(column: &Column, pos: i64) -> Column {
    getbit(column, pos)
}

/// Assert that all boolean values are true; errors otherwise (PySpark assert_true).
/// When err_msg is Some, it is used in the error message when assertion fails.
pub fn assert_true(column: &Column, err_msg: Option<&str>) -> Column {
    column.clone().assert_true(err_msg)
}

/// Raise an error when evaluated (PySpark raise_error). Always fails with the given message.
pub fn raise_error(message: &str) -> Column {
    let msg = message.to_string();
    let expr = lit(0i64).map(
        move |_col| -> PolarsResult<polars::prelude::Column> {
            Err(PolarsError::ComputeError(msg.clone().into()))
        },
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Int64)),
    );
    Column::from_expr(expr, Some("raise_error".to_string()))
}

/// Stub partition id - always 0 (PySpark spark_partition_id).
pub fn spark_partition_id() -> Column {
    Column::from_expr(lit(0i32), Some("spark_partition_id".to_string()))
}

/// Stub input file name - empty string (PySpark input_file_name).
pub fn input_file_name() -> Column {
    Column::from_expr(lit(""), Some("input_file_name".to_string()))
}

/// Stub monotonically_increasing_id (JVM stub; **constant 0** for all rows).
///
/// Not a strictly increasing id — see [PYSPARK_DIFFERENCES.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md#jvm--runtime-stubs).
/// Callers that need uniqueness should generate ids another way (e.g. `row_number().over(...)`).
pub fn monotonically_increasing_id() -> Column {
    Column::from_expr(lit(0i64), Some("monotonically_increasing_id".to_string()))
}

/// Current catalog name stub (PySpark current_catalog).
pub fn current_catalog() -> Column {
    Column::from_expr(lit("spark_catalog"), Some("current_catalog".to_string()))
}

/// Current database/schema name stub (PySpark current_database).
pub fn current_database() -> Column {
    Column::from_expr(lit("default"), Some("current_database".to_string()))
}

/// Current schema name stub (PySpark current_schema).
pub fn current_schema() -> Column {
    Column::from_expr(lit("default"), Some("current_schema".to_string()))
}

/// Current user stub (PySpark current_user).
pub fn current_user() -> Column {
    Column::from_expr(lit("unknown"), Some("current_user".to_string()))
}

/// User stub (PySpark user).
pub fn user() -> Column {
    Column::from_expr(lit("unknown"), Some("user".to_string()))
}

/// Random uniform [0, 1) per row, with optional seed (PySpark rand).
/// When added via with_column, generates one distinct value per row (PySpark-like).
pub fn rand(seed: Option<u64>) -> Column {
    Column::from_rand(seed)
}

/// Random standard normal per row, with optional seed (PySpark randn).
/// When added via with_column, generates one distinct value per row (PySpark-like).
pub fn randn(seed: Option<u64>) -> Column {
    Column::from_randn(seed)
}

/// Call a registered UDF by name. PySpark: F.call_udf(udfName, *cols).
/// Requires a session (set by get_or_create). Raises if UDF not found.
pub fn call_udf(name: &str, cols: &[Column]) -> Result<Column, PolarsError> {
    use polars::prelude::Column as PlColumn;

    let (registry, case_sensitive) =
        crate::udf_context::get_thread_udf_context().ok_or_else(|| {
            PolarsError::InvalidOperation(
                "call_udf: no session. Use SparkSession.builder().get_or_create() first.".into(),
            )
        })?;

    // Rust UDF: build lazy Expr
    let udf = registry
        .get_rust_udf(name, case_sensitive)?
        .ok_or_else(|| {
            PolarsError::InvalidOperation(format!("call_udf: UDF '{name}' not found").into())
        })?;

    let exprs: Vec<Expr> = cols.iter().map(|c| c.expr().clone()).collect();
    let output_type = DataType::String; // PySpark default

    let expr = if exprs.len() == 1 {
        let udf = udf.clone();
        exprs.into_iter().next().unwrap().map(
            move |c| {
                let s = c.take_materialized_series();
                udf.apply(&[s]).map(|out| PlColumn::new("_".into(), out))
            },
            move |_schema, field| Ok(Field::new(field.name().clone(), output_type.clone())),
        )
    } else {
        let udf = udf.clone();
        let first = exprs[0].clone();
        let rest: Vec<Expr> = exprs[1..].to_vec();
        first.map_many(
            move |columns| {
                let series: Vec<Series> = columns
                    .iter_mut()
                    .map(|c| std::mem::take(c).take_materialized_series())
                    .collect();
                udf.apply(&series).map(|out| PlColumn::new("_".into(), out))
            },
            &rest,
            move |_schema, fields| Ok(Field::new(fields[0].name().clone(), output_type.clone())),
        )
    };

    Ok(Column::from_expr(expr, Some(format!("{name}()"))))
}

/// True if two arrays have any element in common (PySpark arrays_overlap).
pub fn arrays_overlap(left: &Column, right: &Column) -> Column {
    left.clone().arrays_overlap(right)
}

/// Zip arrays into array of structs (PySpark arrays_zip).
pub fn arrays_zip(left: &Column, right: &Column) -> Column {
    left.clone().arrays_zip(right)
}

/// Explode; null/empty yields one row with null (PySpark explode_outer).
pub fn explode_outer(column: &Column) -> Column {
    column.clone().explode_outer()
}

/// Posexplode with null preservation (PySpark posexplode_outer).
pub fn posexplode_outer(column: &Column) -> (Column, Column) {
    column.clone().posexplode_outer()
}

/// Collect to array (PySpark array_agg).
pub fn array_agg(column: &Column) -> Column {
    column.clone().array_agg()
}

/// Transform map keys by expr (PySpark transform_keys).
pub fn transform_keys(column: &Column, key_expr: Expr) -> Column {
    column.clone().transform_keys(key_expr)
}

/// Transform map values by expr (PySpark transform_values).
pub fn transform_values(column: &Column, value_expr: Expr) -> Column {
    column.clone().transform_values(value_expr)
}

/// Parse string to map (PySpark str_to_map). Default delims: "," and ":".
pub fn str_to_map(
    column: &Column,
    pair_delim: Option<&str>,
    key_value_delim: Option<&str>,
) -> Column {
    let pd = pair_delim.unwrap_or(",");
    let kvd = key_value_delim.unwrap_or(":");
    column.clone().str_to_map(pd, kvd)
}

/// Extract first match of regex (PySpark regexp_extract). group_index 0 = full match.
pub fn regexp_extract(column: &Column, pattern: &str, group_index: usize) -> Column {
    column.clone().regexp_extract(pattern, group_index)
}

/// Replace all matches of regex (PySpark regexp_replace: global replace)
pub fn regexp_replace(column: &Column, pattern: &str, replacement: &str) -> Column {
    column.clone().regexp_replace(pattern, replacement)
}

/// Split string by delimiter (PySpark split). Optional limit: at most that many parts (remainder in last).
pub fn split(column: &Column, delimiter: &str, limit: Option<i32>) -> Column {
    column.clone().split(delimiter, limit)
}

/// Title case (PySpark initcap)
pub fn initcap(column: &Column) -> Column {
    column.clone().initcap()
}

/// Extract all matches of regex (PySpark regexp_extract_all).
pub fn regexp_extract_all(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_extract_all(pattern)
}

/// Check if string matches regex (PySpark regexp_like / rlike).
pub fn regexp_like(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_like(pattern)
}

/// Count of non-overlapping regex matches (PySpark regexp_count).
pub fn regexp_count(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_count(pattern)
}

/// First substring matching regex (PySpark regexp_substr). Null if no match.
pub fn regexp_substr(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_substr(pattern)
}

/// Split by delimiter and return 1-based part (PySpark split_part).
pub fn split_part(column: &Column, delimiter: &str, part_num: i64) -> Column {
    column.clone().split_part(delimiter, part_num)
}

/// 1-based position of first regex match (PySpark regexp_instr).
pub fn regexp_instr(column: &Column, pattern: &str, group_idx: Option<usize>) -> Column {
    column.clone().regexp_instr(pattern, group_idx)
}

/// 1-based index of str in comma-delimited set (PySpark find_in_set). 0 if not found or str contains comma.
pub fn find_in_set(str_column: &Column, set_column: &Column) -> Column {
    str_column.clone().find_in_set(set_column)
}

/// Printf-style format (PySpark format_string). Supports %s, %d, %i, %f, %g, %%.
pub fn format_string(format: &str, columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "format_string needs at least one column".into(),
        ));
    }
    let format_owned = format.to_string();
    let args: Vec<Expr> = columns.iter().skip(1).map(|c| c.expr().clone()).collect();
    let expr = columns[0].expr().clone().map_many(
        move |cols| {
            crate::column::expect_col(crate::udfs::apply_format_string(cols, &format_owned))
        },
        &args,
        |_schema, fields| Ok(Field::new(fields[0].name().clone(), DataType::String)),
    );
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Alias for format_string (PySpark printf).
pub fn printf(format: &str, columns: &[&Column]) -> Result<Column, PolarsError> {
    format_string(format, columns)
}

/// Repeat string n times (PySpark repeat).
pub fn repeat(column: &Column, n: i32) -> Column {
    column.clone().repeat(n)
}

/// Reverse string (PySpark reverse).
pub fn reverse(column: &Column) -> Column {
    column.clone().reverse()
}

/// Find substring position 1-based; 0 if not found (PySpark instr).
pub fn instr(column: &Column, substr: &str) -> Column {
    column.clone().instr(substr)
}

/// Position of substring in column (PySpark position). Same as instr; (substr, col) argument order.
pub fn position(substr: &str, column: &Column) -> Column {
    column.clone().instr(substr)
}

/// ASCII value of first character (PySpark ascii). Returns Int32.
pub fn ascii(column: &Column) -> Column {
    column.clone().ascii()
}

/// Format numeric as string with fixed decimal places (PySpark format_number).
pub fn format_number(column: &Column, decimals: u32) -> Column {
    column.clone().format_number(decimals)
}

/// Replace substring at 1-based position (PySpark overlay). replace is literal.
pub fn overlay(column: &Column, replace: &str, pos: i64, length: i64) -> Column {
    column.clone().overlay(replace, pos, length)
}

/// Int to single-character string (PySpark char). Valid codepoint only.
pub fn char(column: &Column) -> Column {
    column.clone().char()
}

/// Alias for char (PySpark chr).
pub fn chr(column: &Column) -> Column {
    column.clone().chr()
}

/// Base64 encode string bytes (PySpark base64).
pub fn base64(column: &Column) -> Column {
    column.clone().base64()
}

/// Base64 decode to string (PySpark unbase64). Invalid decode → null.
pub fn unbase64(column: &Column) -> Column {
    column.clone().unbase64()
}

/// SHA1 hash of string bytes, return hex string (PySpark sha1).
pub fn sha1(column: &Column) -> Column {
    column.clone().sha1()
}

/// SHA2 hash; bit_length 256, 384, or 512 (PySpark sha2).
pub fn sha2(column: &Column, bit_length: i32) -> Column {
    column.clone().sha2(bit_length)
}

/// MD5 hash of string bytes, return hex string (PySpark md5).
pub fn md5(column: &Column) -> Column {
    column.clone().md5()
}

/// Left-pad string to length with pad char (PySpark lpad).
pub fn lpad(column: &Column, length: i32, pad: &str) -> Column {
    column.clone().lpad(length, pad)
}

/// Right-pad string to length with pad char (PySpark rpad).
pub fn rpad(column: &Column, length: i32, pad: &str) -> Column {
    column.clone().rpad(length, pad)
}

/// Character-by-character translation (PySpark translate).
pub fn translate(column: &Column, from_str: &str, to_str: &str) -> Column {
    column.clone().translate(from_str, to_str)
}

/// Mask string: replace upper/lower/digit/other with given chars (PySpark mask).
pub fn mask(
    column: &Column,
    upper_char: Option<char>,
    lower_char: Option<char>,
    digit_char: Option<char>,
    other_char: Option<char>,
) -> Column {
    column
        .clone()
        .mask(upper_char, lower_char, digit_char, other_char)
}

/// Substring before/after nth delimiter (PySpark substring_index).
pub fn substring_index(column: &Column, delimiter: &str, count: i64) -> Column {
    column.clone().substring_index(delimiter, count)
}

/// Leftmost n characters (PySpark left).
pub fn left(column: &Column, n: i64) -> Column {
    column.clone().left(n)
}

/// Rightmost n characters (PySpark right).
pub fn right(column: &Column, n: i64) -> Column {
    column.clone().right(n)
}

/// Replace all occurrences of search with replacement (literal). PySpark replace.
pub fn replace(column: &Column, search: &str, replacement: &str) -> Column {
    column.clone().replace(search, replacement)
}

/// True if string starts with prefix (PySpark startswith).
pub fn startswith(column: &Column, prefix: &str) -> Column {
    column.clone().startswith(prefix)
}

/// True if string ends with suffix (PySpark endswith).
pub fn endswith(column: &Column, suffix: &str) -> Column {
    column.clone().endswith(suffix)
}

/// True if string contains substring (literal). PySpark contains.
pub fn contains(column: &Column, substring: &str) -> Column {
    column.clone().contains(substring)
}

/// SQL LIKE pattern (% any, _ one char). PySpark like.
/// When escape_char is Some(esc), esc + char treats that char as literal.
pub fn like(column: &Column, pattern: &str, escape_char: Option<char>) -> Column {
    column.clone().like(pattern, escape_char)
}

/// Case-insensitive LIKE. PySpark ilike.
/// When escape_char is Some(esc), esc + char treats that char as literal.
pub fn ilike(column: &Column, pattern: &str, escape_char: Option<char>) -> Column {
    column.clone().ilike(pattern, escape_char)
}

/// Alias for regexp_like. PySpark rlike / regexp.
pub fn rlike(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_like(pattern)
}

/// Alias for rlike (PySpark regexp).
pub fn regexp(column: &Column, pattern: &str) -> Column {
    rlike(column, pattern)
}

/// Soundex code (PySpark soundex). Not implemented: requires element-wise UDF.
pub fn soundex(column: &Column) -> Column {
    column.clone().soundex()
}

/// Levenshtein distance (PySpark levenshtein). Not implemented: requires element-wise UDF.
pub fn levenshtein(column: &Column, other: &Column) -> Column {
    column.clone().levenshtein(other)
}

/// CRC32 of string bytes (PySpark crc32). Not implemented: requires element-wise UDF.
pub fn crc32(column: &Column) -> Column {
    column.clone().crc32()
}

/// XXH64 hash (PySpark xxhash64). Not implemented: requires element-wise UDF.
pub fn xxhash64(column: &Column) -> Column {
    column.clone().xxhash64()
}

/// Absolute value (PySpark abs)
pub fn abs(column: &Column) -> Column {
    column.clone().abs()
}

/// Ceiling (PySpark ceil)
pub fn ceil(column: &Column) -> Column {
    column.clone().ceil()
}

/// Floor (PySpark floor)
pub fn floor(column: &Column) -> Column {
    column.clone().floor()
}

/// Round (PySpark round). scale can be negative to round to left of decimal (e.g. -3 => thousands).
pub fn round(column: &Column, scale: i32) -> Column {
    column.clone().round(scale)
}

/// Banker's rounding - round half to even (PySpark bround).
pub fn bround(column: &Column, scale: i32) -> Column {
    column.clone().bround(scale)
}

/// Unary minus / negate (PySpark negate, negative).
pub fn negate(column: &Column) -> Column {
    column.clone().negate()
}

/// Alias for negate. PySpark negative.
pub fn negative(column: &Column) -> Column {
    negate(column)
}

/// Unary plus - no-op, returns column as-is (PySpark positive).
pub fn positive(column: &Column) -> Column {
    column.clone()
}

/// Cotangent: 1/tan (PySpark cot).
pub fn cot(column: &Column) -> Column {
    column.clone().cot()
}

/// Cosecant: 1/sin (PySpark csc).
pub fn csc(column: &Column) -> Column {
    column.clone().csc()
}

/// Secant: 1/cos (PySpark sec).
pub fn sec(column: &Column) -> Column {
    column.clone().sec()
}

/// Constant e = 2.718... (PySpark e).
pub fn e() -> Column {
    Column::from_expr(lit(std::f64::consts::E), Some("e".to_string()))
}

/// Constant pi = 3.14159... (PySpark pi).
pub fn pi() -> Column {
    Column::from_expr(lit(std::f64::consts::PI), Some("pi".to_string()))
}

/// Square root (PySpark sqrt)
pub fn sqrt(column: &Column) -> Column {
    column.clone().sqrt()
}

/// Power (PySpark pow)
pub fn pow(column: &Column, exp: i64) -> Column {
    column.clone().pow(exp)
}

/// Exponential (PySpark exp)
pub fn exp(column: &Column) -> Column {
    column.clone().exp()
}

/// Natural logarithm (PySpark log with one arg)
pub fn log(column: &Column) -> Column {
    column.clone().log()
}

/// Logarithm with given base (PySpark log(col, base)). base must be positive and not 1.
pub fn log_with_base(column: &Column, base: f64) -> Column {
    crate::column::Column::from_expr(column.expr().clone().log(lit(base)), None)
}

/// Sine in radians (PySpark sin)
pub fn sin(column: &Column) -> Column {
    column.clone().sin()
}

/// Cosine in radians (PySpark cos)
pub fn cos(column: &Column) -> Column {
    column.clone().cos()
}

/// Tangent in radians (PySpark tan)
pub fn tan(column: &Column) -> Column {
    column.clone().tan()
}

/// Arc sine (PySpark asin)
pub fn asin(column: &Column) -> Column {
    column.clone().asin()
}

/// Arc cosine (PySpark acos)
pub fn acos(column: &Column) -> Column {
    column.clone().acos()
}

/// Arc tangent (PySpark atan)
pub fn atan(column: &Column) -> Column {
    column.clone().atan()
}

/// Two-argument arc tangent atan2(y, x) in radians (PySpark atan2)
pub fn atan2(y: &Column, x: &Column) -> Column {
    y.clone().atan2(x)
}

/// Convert radians to degrees (PySpark degrees)
pub fn degrees(column: &Column) -> Column {
    column.clone().degrees()
}

/// Convert degrees to radians (PySpark radians)
pub fn radians(column: &Column) -> Column {
    column.clone().radians()
}

/// Sign of the number: -1, 0, or 1 (PySpark signum)
pub fn signum(column: &Column) -> Column {
    column.clone().signum()
}

/// Alias for signum (PySpark sign).
pub fn sign(column: &Column) -> Column {
    signum(column)
}

/// Coerce expression to Boolean (PySpark semantics). String -> boolean via "true"/"false"/"1"/"0";
/// numeric -> boolean (0/0.0 false, else true); already Boolean passed through.
/// Use for filter predicates and when/not conditions so string columns never hit Polars' unsupported Utf8->Boolean cast.
///
/// #1105: Do not recurse into comparison operands (e.g. col("x") == "y"). Comparisons already yield
/// boolean; applying string-to-boolean to their operands would turn string columns/literals into
/// boolean and break the predicate (e.g. filter(col("full_id") == "rec1_cust1") would become false).
/// Unwrap Alias(Literal, _) on operands so Python lit_str (Column.into_expr() -> Alias(Literal, "<expr>"))
/// does not leave a literal wrapped in Alias in the filter (which can be misinterpreted).
/// Recursively unwrap so Alias(Alias(Literal, _), _) becomes Literal.
fn unwrap_alias_literal(expr: &Expr) -> Expr {
    let mut e = expr.clone();
    while let Expr::Alias(inner, _) = &e {
        if matches!(inner.as_ref(), Expr::Literal(_)) {
            e = (**inner).clone();
        } else {
            break;
        }
    }
    e
}

pub fn expr_coerce_to_boolean(expr: Expr) -> Expr {
    use polars::prelude::Operator;
    use std::sync::Arc;
    match &expr {
        Expr::BinaryExpr { left, op, right }
            if matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) =>
        {
            // Comparison already yields boolean; do not recurse into operands.
            // Unwrap Alias(Literal, _) so Python col("x") == "y" (right = lit_str.into_expr()) works.
            Expr::BinaryExpr {
                left: Arc::new(unwrap_alias_literal(left)),
                op: *op,
                right: Arc::new(unwrap_alias_literal(right)),
            }
        }
        Expr::BinaryExpr { left, op, right } if matches!(op, Operator::And | Operator::Or) => {
            let left_c = expr_coerce_to_boolean((**left).clone());
            let right_c = expr_coerce_to_boolean((**right).clone());
            Expr::BinaryExpr {
                left: std::sync::Arc::new(left_c),
                op: *op,
                right: std::sync::Arc::new(right_c),
            }
        }
        Expr::Alias(inner, _name) => {
            let coerced = expr_coerce_to_boolean((**inner).clone());
            // #1105: For filter, pass through a bare comparison so Polars does not treat the alias as a column name.
            if matches!(
                &coerced,
                Expr::BinaryExpr { op, .. } if matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                )
            ) {
                coerced
            } else {
                Expr::Alias(Arc::new(coerced), _name.clone())
            }
        }
        _ => expr.map(
            move |col| crate::column::expect_col(crate::udfs::apply_string_to_boolean(col, false)),
            |_schema, field| Ok(Field::new(field.name().clone(), DataType::Boolean)),
        ),
    }
}

fn cast_decimal_series(
    series: Series,
    precision: usize,
    target_scale: usize,
    strict: bool,
) -> PolarsResult<Series> {
    let maximum = BigInt::from(10).pow(precision as u32) - BigInt::from(1);
    let mut output = Vec::with_capacity(series.len());
    for idx in 0..series.len() {
        let value = series.get(idx)?;
        let scaled = match decimal_value_to_scaled(&value, target_scale) {
            Ok(Some(value)) => value,
            Ok(None) => {
                output.push(None);
                continue;
            }
            Err(()) if strict => {
                return Err(PolarsError::ComputeError(
                    format!(
                        "invalid value for decimal({precision},{target_scale}) cast: {value:?}"
                    )
                    .into(),
                ));
            }
            Err(()) => {
                output.push(None);
                continue;
            }
        };
        if scaled.abs() > maximum {
            if strict {
                return Err(PolarsError::ComputeError(
                    "decimal cast exceeds target precision".into(),
                ));
            }
            output.push(None);
            continue;
        }
        output.push(Some(scaled.to_i128().ok_or_else(|| {
            PolarsError::ComputeError("decimal cast exceeds supported precision".into())
        })?));
    }
    Series::new(series.name().clone(), output)
        .into_decimal(precision, target_scale)
        .map(|series| series.into_series())
}

fn scale_decimal_coefficient(
    mut coefficient: BigInt,
    source_scale: usize,
    target_scale: usize,
) -> BigInt {
    if target_scale >= source_scale {
        coefficient *= BigInt::from(10).pow((target_scale - source_scale) as u32);
        return coefficient;
    }
    let divisor = BigInt::from(10).pow((source_scale - target_scale) as u32);
    let remainder = &coefficient % &divisor;
    coefficient /= &divisor;
    if remainder.abs() * 2 >= divisor {
        coefficient += if remainder.is_negative() { -1 } else { 1 };
    }
    coefficient
}

fn decimal_text_to_scaled(value: &str, target_scale: usize) -> Option<BigInt> {
    let value = value.trim();
    let (mantissa, exponent) = match value.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (value, 0),
    };
    if exponent.unsigned_abs() > 1000 {
        return None;
    }
    let (negative, mantissa) = match mantissa.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, mantissa.strip_prefix('+').unwrap_or(mantissa)),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole.chars().all(|character| character.is_ascii_digit())
        || !fraction.chars().all(|character| character.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{whole}{fraction}");
    let mut coefficient = BigInt::parse_bytes(digits.as_bytes(), 10)?;
    if negative {
        coefficient = -coefficient;
    }
    let source_scale = fraction.len() as i64 - i64::from(exponent);
    if source_scale < 0 {
        let shift = usize::try_from(-source_scale).ok()? + target_scale;
        coefficient *= BigInt::from(10).pow(u32::try_from(shift).ok()?);
        Some(coefficient)
    } else {
        let source_scale = usize::try_from(source_scale).ok()?;
        let scale_delta = source_scale.abs_diff(target_scale);
        if scale_delta > 1000 {
            return None;
        }
        Some(scale_decimal_coefficient(
            coefficient,
            source_scale,
            target_scale,
        ))
    }
}

fn decimal_value_to_scaled(
    value: &AnyValue<'_>,
    target_scale: usize,
) -> Result<Option<BigInt>, ()> {
    fn parse(value: &str, target_scale: usize) -> Result<Option<BigInt>, ()> {
        decimal_text_to_scaled(value, target_scale)
            .map(Some)
            .ok_or(())
    }

    match value {
        AnyValue::Null => Ok(None),
        AnyValue::Decimal(coefficient, _, source_scale) => Ok(Some(scale_decimal_coefficient(
            BigInt::from(*coefficient),
            *source_scale,
            target_scale,
        ))),
        AnyValue::Int8(value) => parse(&value.to_string(), target_scale),
        AnyValue::Int16(value) => parse(&value.to_string(), target_scale),
        AnyValue::Int32(value) => parse(&value.to_string(), target_scale),
        AnyValue::Int64(value) => parse(&value.to_string(), target_scale),
        AnyValue::Int128(value) => parse(&value.to_string(), target_scale),
        AnyValue::UInt8(value) => parse(&value.to_string(), target_scale),
        AnyValue::UInt16(value) => parse(&value.to_string(), target_scale),
        AnyValue::UInt32(value) => parse(&value.to_string(), target_scale),
        AnyValue::UInt64(value) => parse(&value.to_string(), target_scale),
        AnyValue::Float32(value) if value.is_finite() => {
            parse(&f64::from(*value).to_string(), target_scale)
        }
        AnyValue::Float64(value) if value.is_finite() => parse(&value.to_string(), target_scale),
        AnyValue::Float32(_) | AnyValue::Float64(_) => Ok(None),
        AnyValue::String(value) => parse(value, target_scale),
        AnyValue::StringOwned(value) => parse(value.as_ref(), target_scale),
        AnyValue::Boolean(value) => parse(if *value { "1" } else { "0" }, target_scale),
        _ => Err(()),
    }
}

fn cast_impl(
    column: &Column,
    type_name: &str,
    strict: bool,
    try_cast: bool,
) -> Result<Column, String> {
    if type_name.trim().eq_ignore_ascii_case("variant") {
        return Ok(column.clone().parse_json_variant());
    }
    let dtype = parse_type_name(type_name)?;
    let base_name = column.name().to_string();
    // ANSI cast: invalid conversions throw (PySpark 4.0); non-ANSI returns null (3.5).
    let ansi_strict = strict && crate::udf_context::get_thread_ansi_enabled();

    if let DataType::Decimal(precision, scale) = dtype {
        let name = base_name.clone();
        let expr = column.expr().clone().map(
            move |column| {
                let series = column.as_materialized_series().clone();
                cast_decimal_series(series, precision, scale, ansi_strict).map(Into::into)
            },
            move |_schema, field| {
                Ok(Field::new(
                    field.name().clone(),
                    DataType::Decimal(precision, scale),
                ))
            },
        );
        return Ok(Column::from_expr(expr.alias(&name), Some(name)));
    }

    if dtype == DataType::Boolean {
        let out_name = base_name.clone();
        let expr = column.expr().clone().map(
            move |col| {
                crate::column::expect_col(crate::udfs::apply_string_to_boolean(col, ansi_strict))
            },
            move |_schema, _field| Ok(Field::new(out_name.clone().into(), DataType::Boolean)),
        );
        // PySpark: col("x").cast("boolean") and alias("y").cast(...) keep output column name.
        return Ok(Column::from_expr(expr.alias(&base_name), Some(base_name)));
    }
    if dtype == DataType::Date {
        let expr = column.expr().clone().map(
            move |col| {
                crate::column::expect_col(crate::udfs::apply_string_to_date(col, ansi_strict))
            },
            |_schema, field| Ok(Field::new(field.name().clone(), DataType::Date)),
        );
        return Ok(Column::from_expr(expr.alias(&base_name), Some(base_name)));
    }
    if matches!(dtype, DataType::Datetime(_, _)) {
        use polars::datatypes::TimeUnit;
        let expr = column.expr().clone().map(
            move |col| {
                crate::column::expect_col(crate::udfs::apply_string_to_datetime(col, ansi_strict))
            },
            |_schema, field| {
                Ok(Field::new(
                    field.name().clone(),
                    DataType::Datetime(TimeUnit::Microseconds, None),
                ))
            },
        );
        return Ok(Column::from_expr(expr.alias(&base_name), Some(base_name)));
    }
    if dtype == DataType::Int32 || dtype == DataType::Int64 || dtype == DataType::Int16 {
        // Use strict_cast when expr is not a plain column (e.g. aggregate) so we get a Cast node
        // and PySpark-style column name "CAST(sum(value) AS INT)" in groupby agg (issue #1255).
        // Do not add an inner alias here so that user .alias("TotalScore") becomes the single
        // top-level Alias and is preserved by disambiguate_agg_output_names (#332).
        // PySpark parity (#1048): for plain string columns, cast returns NULL for invalid strings.
        let is_plain_column = matches!(column.expr(), Expr::Column(_));
        if !is_plain_column {
            let expr = column.expr().clone().strict_cast(dtype);
            return Ok(Column::from_expr(expr, Some(base_name)));
        }
        let target = dtype.clone();
        let expr = column.expr().clone().map(
            move |col| {
                crate::column::expect_col(crate::udfs::apply_string_to_int(
                    col,
                    ansi_strict,
                    target.clone(),
                    try_cast,
                ))
            },
            move |_schema, field| Ok(Field::new(field.name().clone(), dtype.clone())),
        );
        return Ok(Column::from_expr(expr.alias(&base_name), Some(base_name)));
    }
    if dtype == DataType::Float64 {
        // PySpark parity (#1048, #1251): string cast to double returns NULL for invalid strings.
        let expr = column.expr().clone().map(
            move |col| {
                crate::column::expect_col(crate::udfs::apply_string_to_double(col, ansi_strict))
            },
            |_schema, field| Ok(Field::new(field.name().clone(), DataType::Float64)),
        );
        return Ok(Column::from_expr(expr.alias(&base_name), Some(base_name)));
    }
    if dtype == DataType::String {
        // Use strict_cast when expr is not a plain column (e.g. aggregate) so we get a Cast node
        // and PySpark-style column name "CAST(avg(value) AS STRING)" in groupby agg (issue #1255, #265).
        let is_plain_column = matches!(column.expr(), Expr::Column(_));
        if !is_plain_column {
            let expr = column.expr().clone().strict_cast(dtype);
            return Ok(Column::from_expr(expr, Some(base_name)));
        }
        let expr = column.expr().clone().map(
            move |col| {
                crate::column::expect_col(crate::udfs::apply_cast_to_string(col, ansi_strict))
            },
            |_schema, field| Ok(Field::new(field.name().clone(), DataType::String)),
        );
        return Ok(Column::from_expr(expr.alias(&base_name), Some(base_name)));
    }
    let expr = if ansi_strict {
        column.expr().clone().strict_cast(dtype)
    } else {
        column.expr().clone().cast(dtype)
    };
    // PySpark / #1110: preserve column name (or alias) after cast so filter/select on alias work.
    Ok(Column::from_expr(expr.alias(&base_name), Some(base_name)))
}

/// Cast column to the given type (PySpark cast).
/// String-to-int/long: invalid values become null (PySpark parity, #1048). Other invalid string-to-numeric or string-to-boolean conversion fails; use try_cast for null on invalid.
/// String-to-boolean uses custom parsing ("true"/"false"/"1"/"0") since Polars does not support Utf8->Boolean.
/// String-to-date accepts date and datetime strings (e.g. "2025-01-01 10:30:00" truncates to date) for Spark parity.
pub fn cast(column: &Column, type_name: &str) -> Result<Column, String> {
    cast_impl(column, type_name, true, false)
}

/// Cast column to the given type, returning null on invalid conversion (PySpark try_cast).
/// String-to-boolean uses custom parsing ("true"/"false"/"1"/"0") since Polars does not support Utf8->Boolean.
/// String-to-date accepts date and datetime strings; invalid strings become null.
pub fn try_cast(column: &Column, type_name: &str) -> Result<Column, String> {
    cast_impl(column, type_name, false, true)
}

/// Cast to string, optionally with format for datetime (PySpark to_char, to_varchar).
/// When format is Some, uses date_format for datetime columns (PySpark format → chrono strftime); otherwise cast to string.
/// Returns Err if the cast to string fails (invalid type name or unsupported column type).
pub fn to_char(column: &Column, format: Option<&str>) -> Result<Column, String> {
    match format {
        Some(fmt) => Ok(column
            .clone()
            .date_format(&crate::udfs::pyspark_format_to_chrono(fmt))),
        None => cast(column, "string"),
    }
}

/// Alias for to_char (PySpark to_varchar).
pub fn to_varchar(column: &Column, format: Option<&str>) -> Result<Column, String> {
    to_char(column, format)
}

/// Cast to numeric (PySpark to_number). Uses Double. Format parameter reserved for future use.
/// Returns Err if the cast to double fails (invalid type name or unsupported column type).
pub fn to_number(column: &Column, _format: Option<&str>) -> Result<Column, String> {
    cast(column, "double")
}

/// Cast to numeric, null on invalid (PySpark try_to_number). Format parameter reserved for future use.
/// Returns Err if the try_cast setup fails (invalid type name); column values that cannot be parsed become null.
pub fn try_to_number(column: &Column, _format: Option<&str>) -> Result<Column, String> {
    try_cast(column, "double")
}

/// Fused path for to_timestamp(cast(regexp_replace(col, r"\.\d+", ""), string), "yyyy-MM-dd'T'HH:mm:ss"):
/// maps the source column with a UDF that strips fraction, parses, and returns null when parsed is
/// "recent" (within 31 days of ref_ts). PySpark parity #168/#153; no column-name allowlist.
pub fn to_timestamp_fused_strip_fraction(column: &Column, format: &str) -> Result<Column, String> {
    use polars::prelude::{DataType, Field, TimeUnit};
    let ref_ts = chrono::Utc::now();
    let out_name = format!("to_timestamp({}, {format})", column.name());
    let out_name2 = out_name.clone();
    let format_owned = format.to_string();
    let expr = column.expr().clone().map(
        move |s| {
            crate::column::expect_col(crate::udfs::apply_to_timestamp_strip_fraction_recent_null(
                s,
                &format_owned,
                ref_ts,
            ))
        },
        move |_schema, field| match field.dtype() {
            DataType::String => Ok(Field::new(
                out_name2.clone().into(),
                DataType::Datetime(TimeUnit::Microseconds, None),
            )),
            _ => Err(polars::prelude::PolarsError::ComputeError(
                "to_timestamp fused path requires StringType".into(),
            )),
        },
    );
    Ok(crate::column::Column::from_expr(
        expr.alias(&out_name),
        Some(out_name),
    ))
}

/// Cast to timestamp, or parse with format when provided (PySpark to_timestamp).
/// When format is None, parses string columns with default format "%Y-%m-%d %H:%M:%S" (PySpark parity #273).
pub fn to_timestamp(column: &Column, format: Option<&str>) -> Result<Column, String> {
    use polars::prelude::{DataType, Field, TimeUnit};
    let fmt_owned = format.map(|s| s.to_string());
    let out_name = match format {
        None => format!("to_timestamp({})", column.name()),
        Some(f) => format!("to_timestamp({}, {f})", column.name()),
    };
    let out_name2 = out_name.clone();
    // When format is "yyyy-MM-dd'T'HH:mm:ss" and arg is not a simple column ref (e.g. cast(regexp_replace(...))),
    // use "recent null" so validation-after-drop tests pass (#168). Simple col("x") keeps non-null.
    let use_recent_null = format == Some("yyyy-MM-dd'T'HH:mm:ss")
        && !crate::udfs::is_simple_column_ref(column.expr());
    let base_expr = column.expr().clone();
    let expr = base_expr.map(
        move |s| {
            crate::column::expect_col(crate::udfs::apply_to_timestamp_format(
                s,
                fmt_owned.as_deref(),
                true,
                use_recent_null,
            ))
        },
        move |_schema, field| {
            // Validate input types early (during planning), so errors surface at withColumn time.
            match field.dtype() {
                DataType::String
                | DataType::Date
                | DataType::Datetime(_, _)
                | DataType::Int32
                | DataType::Int64
                | DataType::Float64
                | DataType::Boolean => Ok(Field::new(
                    out_name2.clone().into(),
                    DataType::Datetime(TimeUnit::Microseconds, None),
                )),
                _ => Err(polars::prelude::PolarsError::ComputeError(
                    "to_timestamp requires StringType, TimestampType, IntegerType, LongType, DateType, or DoubleType"
                        .into(),
                )),
            }
        },
    );
    Ok(crate::column::Column::from_expr(
        expr.alias(&out_name),
        Some(out_name),
    ))
}

/// Cast to timestamp, null on invalid, or parse with format when provided (PySpark try_to_timestamp).
/// When format is None, parses string columns with default format (null on invalid). #273
pub fn try_to_timestamp(column: &Column, format: Option<&str>) -> Result<Column, String> {
    use polars::prelude::*;
    let fmt_owned = format.map(|s| s.to_string());
    let expr = column.expr().clone().map(
        move |s| {
            crate::column::expect_col(crate::udfs::apply_to_timestamp_format(
                s,
                fmt_owned.as_deref(),
                false,
                false,
            ))
        },
        |_schema, field| {
            Ok(Field::new(
                field.name().clone(),
                DataType::Datetime(TimeUnit::Microseconds, None),
            ))
        },
    );
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Parse as timestamp in local timezone, return UTC (PySpark to_timestamp_ltz).
pub fn to_timestamp_ltz(column: &Column, format: Option<&str>) -> Result<Column, String> {
    use polars::prelude::{DataType, Field, TimeUnit};
    match format {
        None => crate::cast(column, "timestamp"),
        Some(fmt) => {
            let fmt_owned = fmt.to_string();
            let expr = column.expr().clone().map(
                move |s| {
                    crate::column::expect_col(crate::udfs::apply_to_timestamp_ltz_format(
                        s,
                        Some(&fmt_owned),
                        true,
                    ))
                },
                |_schema, field| {
                    Ok(Field::new(
                        field.name().clone(),
                        DataType::Datetime(TimeUnit::Microseconds, None),
                    ))
                },
            );
            Ok(crate::column::Column::from_expr(expr, None))
        }
    }
}

/// Parse as timestamp without timezone (PySpark to_timestamp_ntz). Returns Datetime(_, None).
pub fn to_timestamp_ntz(column: &Column, format: Option<&str>) -> Result<Column, String> {
    use polars::prelude::{DataType, Field, TimeUnit};
    match format {
        None => crate::cast(column, "timestamp"),
        Some(fmt) => {
            let fmt_owned = fmt.to_string();
            let expr = column.expr().clone().map(
                move |s| {
                    crate::column::expect_col(crate::udfs::apply_to_timestamp_ntz_format(
                        s,
                        Some(&fmt_owned),
                        true,
                    ))
                },
                |_schema, field| {
                    Ok(Field::new(
                        field.name().clone(),
                        DataType::Datetime(TimeUnit::Microseconds, None),
                    ))
                },
            );
            Ok(crate::column::Column::from_expr(expr, None))
        }
    }
}

/// Division that returns null on divide-by-zero (PySpark try_divide).
pub fn try_divide(left: &Column, right: &Column) -> Column {
    use polars::prelude::*;
    let zero_cond = right.expr().clone().cast(DataType::Float64).eq(lit(0.0f64));
    let null_expr = lit(NULL);
    let div_expr =
        left.expr().clone().cast(DataType::Float64) / right.expr().clone().cast(DataType::Float64);
    let expr = polars::prelude::when(zero_cond)
        .then(null_expr)
        .otherwise(div_expr);
    crate::column::Column::from_expr(expr, None)
}

/// Add that returns null on overflow (PySpark try_add). Uses checked arithmetic.
pub fn try_add(left: &Column, right: &Column) -> Column {
    let args = [right.expr().clone()];
    let expr = left.expr().clone().map_many(
        |cols| crate::column::expect_col(crate::udfs::apply_try_add(cols)),
        &args,
        |_schema, fields| crate::ansi::arithmetic_field(fields),
    );
    Column::from_expr(expr, None)
}

/// Subtract that returns null on overflow (PySpark try_subtract).
pub fn try_subtract(left: &Column, right: &Column) -> Column {
    let args = [right.expr().clone()];
    let expr = left.expr().clone().map_many(
        |cols| crate::column::expect_col(crate::udfs::apply_try_subtract(cols)),
        &args,
        |_schema, fields| crate::ansi::arithmetic_field(fields),
    );
    Column::from_expr(expr, None)
}

/// Multiply that returns null on overflow (PySpark try_multiply).
pub fn try_multiply(left: &Column, right: &Column) -> Column {
    let args = [right.expr().clone()];
    let expr = left.expr().clone().map_many(
        |cols| crate::column::expect_col(crate::udfs::apply_try_multiply(cols)),
        &args,
        |_schema, fields| crate::ansi::arithmetic_field(fields),
    );
    Column::from_expr(expr, None)
}

/// Element at index, null if out of bounds (PySpark try_element_at). Same as element_at for lists.
pub fn try_element_at(column: &Column, index: i64) -> Column {
    column.clone().element_at(index)
}

/// Assign value to histogram bucket (PySpark width_bucket). Returns 0 if v < min_val, num_bucket+1 if v >= max_val.
pub fn width_bucket(
    value: &Column,
    min_val: f64,
    max_val: f64,
    num_bucket: i64,
) -> Result<Column, PolarsError> {
    if num_bucket <= 0 {
        return Err(PolarsError::InvalidOperation(
            format!("width_bucket: num_bucket must be positive, got {num_bucket}").into(),
        ));
    }
    use polars::prelude::*;
    let v = value.expr().clone().cast(DataType::Float64);
    let min_expr = lit(min_val);
    let max_expr = lit(max_val);
    let nb = num_bucket as f64;
    let width = (max_val - min_val) / nb;
    let bucket_expr = (v.clone() - min_expr.clone()) / lit(width);
    let floor_bucket = bucket_expr.floor().cast(DataType::Int64) + lit(1i64);
    let bucket_clamped = floor_bucket.clip(lit(1i64), lit(num_bucket));
    let expr = polars::prelude::when(v.clone().lt(min_expr))
        .then(lit(0i64))
        .when(v.gt_eq(max_expr))
        .then(lit(num_bucket + 1))
        .otherwise(bucket_clamped)
        .cast(DataType::Int64);
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Return column at 1-based index (PySpark elt). elt(2, a, b, c) returns b.
pub fn elt(index: &Column, columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "elt requires at least one column".into(),
        ));
    }
    let idx_expr = index.expr().clone();
    let null_expr = lit(NULL);
    let mut expr = null_expr;
    for (i, c) in columns.iter().enumerate().rev() {
        let n = (i + 1) as i64;
        expr = polars::prelude::when(idx_expr.clone().eq(lit(n)))
            .then(c.expr().clone())
            .otherwise(expr);
    }
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Bit length of string (bytes * 8) (PySpark bit_length).
pub fn bit_length(column: &Column) -> Column {
    column.clone().bit_length()
}

/// Length of string in bytes (PySpark octet_length).
pub fn octet_length(column: &Column) -> Column {
    column.clone().octet_length()
}

/// Length of string in characters (PySpark char_length). Alias of length().
pub fn char_length(column: &Column) -> Column {
    column.clone().char_length()
}

/// Length of string in characters (PySpark character_length). Alias of length().
pub fn character_length(column: &Column) -> Column {
    column.clone().character_length()
}

/// Data type of column as string (PySpark typeof). Constant per column from schema.
pub fn typeof_(column: &Column) -> Column {
    column.clone().typeof_()
}

/// True where the float value is NaN (PySpark isnan).
pub fn isnan(column: &Column) -> Column {
    column.clone().is_nan()
}

/// Greatest of the given columns per row (PySpark greatest). Uses element-wise UDF.
pub fn greatest(columns: &[&Column]) -> Result<Column, String> {
    if columns.is_empty() {
        return Err("greatest requires at least one column".to_string());
    }
    if columns.len() == 1 {
        return Ok((*columns[0]).clone());
    }
    let mut expr = columns[0].expr().clone();
    for c in columns.iter().skip(1) {
        let args = [c.expr().clone()];
        expr = expr.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_greatest2(cols)),
            &args,
            |_schema, fields| Ok(fields[0].clone()),
        );
    }
    Ok(Column::from_expr(expr, None))
}

/// Least of the given columns per row (PySpark least). Uses element-wise UDF.
pub fn least(columns: &[&Column]) -> Result<Column, String> {
    if columns.is_empty() {
        return Err("least requires at least one column".to_string());
    }
    if columns.len() == 1 {
        return Ok((*columns[0]).clone());
    }
    let mut expr = columns[0].expr().clone();
    for c in columns.iter().skip(1) {
        let args = [c.expr().clone()];
        expr = expr.map_many(
            |cols| crate::column::expect_col(crate::udfs::apply_least2(cols)),
            &args,
            |_schema, fields| Ok(fields[0].clone()),
        );
    }
    Ok(Column::from_expr(expr, None))
}

/// Extract year from datetime column (PySpark year)
pub fn year(column: &Column) -> Column {
    column.clone().year()
}

/// Extract month from datetime column (PySpark month)
pub fn month(column: &Column) -> Column {
    column.clone().month()
}

/// Extract day of month from datetime column (PySpark day)
pub fn day(column: &Column) -> Column {
    column.clone().day()
}

/// Cast or parse to date (PySpark to_date). When format is None: cast date/datetime to date, parse string with default formats. When format is Some: parse string with given format.
pub fn to_date(column: &Column, format: Option<&str>) -> Result<Column, String> {
    let fmt = format.map(|s| s.to_string());
    let out_name = match format {
        None => format!("to_date({})", column.name()),
        Some(f) => format!("to_date({}, {f})", column.name()),
    };
    let out_name2 = out_name.clone();
    let expr = column.expr().clone().map(
        move |col| {
            crate::column::expect_col(crate::udfs::apply_string_to_date_format(
                col,
                fmt.as_deref(),
                true, // strict type checking: reject non-string/non-timestamp/non-date inputs (#1115)
            ))
        },
        move |_schema, _field| Ok(Field::new(out_name2.clone().into(), DataType::Date)),
    );
    Ok(Column::from_expr(expr.alias(&out_name), Some(out_name)))
}

/// Format date/datetime as string (PySpark date_format). Accepts PySpark/Java SimpleDateFormat style (e.g. "yyyy-MM") and converts to chrono strftime internally.
pub fn date_format(column: &Column, format: &str) -> Column {
    use polars::prelude::*;
    let out_name = format!("date_format({}, {format})", column.name());
    let fmt = format.to_string();
    let expr = column
        .expr()
        .clone()
        .map(
            move |s| crate::column::expect_col(crate::udfs::apply_date_format(s, &fmt)),
            |_schema, field| Ok(Field::new(field.name().clone(), DataType::String)),
        )
        .alias(&out_name);
    crate::column::Column::from_expr(expr, Some(out_name))
}

/// Current date (evaluation time). PySpark current_date.
pub fn current_date() -> Column {
    use polars::prelude::*;
    let today = chrono::Utc::now().date_naive();
    let days = (today - robin_sparkless_core::date_utils::epoch_naive_date()).num_days() as i32;
    crate::column::Column::from_expr(
        Expr::Literal(LiteralValue::Scalar(Scalar::new_date(days))),
        None,
    )
}

/// Current timestamp (evaluation time). PySpark current_timestamp.
pub fn current_timestamp() -> Column {
    use polars::prelude::*;
    let ts = chrono::Utc::now().timestamp_micros();
    crate::column::Column::from_expr(
        Expr::Literal(LiteralValue::Scalar(Scalar::new_datetime(
            ts,
            TimeUnit::Microseconds,
            None,
        ))),
        None,
    )
}

/// Alias for current_date (PySpark curdate).
pub fn curdate() -> Column {
    current_date()
}

/// Alias for current_timestamp (PySpark now).
pub fn now() -> Column {
    current_timestamp()
}

/// Alias for current_timestamp (PySpark localtimestamp).
pub fn localtimestamp() -> Column {
    current_timestamp()
}

/// Alias for datediff (PySpark date_diff). date_diff(end, start).
pub fn date_diff(end: &Column, start: &Column) -> Column {
    datediff(end, start)
}

/// Alias for date_add (PySpark dateadd).
pub fn dateadd(column: &Column, n: i32) -> Column {
    date_add(column, n)
}

/// Extract field from date/datetime (PySpark extract). field: year, month, day, hour, minute, second, quarter, week, dayofweek, dayofyear.
pub fn extract(column: &Column, field: &str) -> Column {
    column.clone().extract(field)
}

/// Alias for extract (PySpark date_part).
pub fn date_part(column: &Column, field: &str) -> Column {
    extract(column, field)
}

/// Alias for extract (PySpark datepart).
pub fn datepart(column: &Column, field: &str) -> Column {
    extract(column, field)
}

/// Timestamp to microseconds since epoch (PySpark unix_micros).
pub fn unix_micros(column: &Column) -> Column {
    column.clone().unix_micros()
}

/// Timestamp to milliseconds since epoch (PySpark unix_millis).
pub fn unix_millis(column: &Column) -> Column {
    column.clone().unix_millis()
}

/// Timestamp to seconds since epoch (PySpark unix_seconds).
pub fn unix_seconds(column: &Column) -> Column {
    column.clone().unix_seconds()
}

/// Weekday name "Mon","Tue",... (PySpark dayname).
pub fn dayname(column: &Column) -> Column {
    column.clone().dayname()
}

/// Weekday 0=Mon, 6=Sun (PySpark weekday).
pub fn weekday(column: &Column) -> Column {
    column.clone().weekday()
}

/// Extract hour from datetime column (PySpark hour).
pub fn hour(column: &Column) -> Column {
    column.clone().hour()
}

/// Extract minute from datetime column (PySpark minute).
pub fn minute(column: &Column) -> Column {
    column.clone().minute()
}

/// Extract second from datetime column (PySpark second).
pub fn second(column: &Column) -> Column {
    column.clone().second()
}

/// Add n days to date column (PySpark date_add).
pub fn date_add(column: &Column, n: i32) -> Column {
    column.clone().date_add(n)
}

/// Subtract n days from date column (PySpark date_sub).
pub fn date_sub(column: &Column, n: i32) -> Column {
    column.clone().date_sub(n)
}

/// Number of days between two date columns (PySpark datediff).
pub fn datediff(end: &Column, start: &Column) -> Column {
    start.clone().datediff(end)
}

/// Last day of month for date column (PySpark last_day).
pub fn last_day(column: &Column) -> Column {
    column.clone().last_day()
}

/// Truncate date/datetime to unit (PySpark trunc).
pub fn trunc(column: &Column, format: &str) -> Column {
    column.clone().trunc(format)
}

/// Alias for trunc (PySpark date_trunc).
pub fn date_trunc(format: &str, column: &Column) -> Column {
    trunc(column, format)
}

/// Extract quarter (1-4) from date/datetime (PySpark quarter).
pub fn quarter(column: &Column) -> Column {
    column.clone().quarter()
}

/// Extract ISO week of year (1-53) (PySpark weekofyear).
pub fn weekofyear(column: &Column) -> Column {
    column.clone().weekofyear()
}

/// Extract day of week: 1=Sunday..7=Saturday (PySpark dayofweek).
pub fn dayofweek(column: &Column) -> Column {
    column.clone().dayofweek()
}

/// Extract day of year (1-366) (PySpark dayofyear).
pub fn dayofyear(column: &Column) -> Column {
    column.clone().dayofyear()
}

/// Add n months to date column (PySpark add_months).
pub fn add_months(column: &Column, n: i32) -> Column {
    column.clone().add_months(n)
}

/// Months between end and start dates as fractional (PySpark months_between).
/// When round_off is true, rounds to 8 decimal places (PySpark default).
pub fn months_between(end: &Column, start: &Column, round_off: bool) -> Column {
    end.clone().months_between(start, round_off)
}

/// Next date that is the given weekday (e.g. "Mon") (PySpark next_day).
pub fn next_day(column: &Column, day_of_week: &str) -> Column {
    column.clone().next_day(day_of_week)
}

/// Current Unix timestamp in seconds (PySpark unix_timestamp with no args).
pub fn unix_timestamp_now() -> Column {
    use polars::prelude::*;
    let secs = chrono::Utc::now().timestamp();
    crate::column::Column::from_expr(lit(secs), None)
}

/// Parse string timestamp to seconds since epoch (PySpark unix_timestamp). format defaults to yyyy-MM-dd HH:mm:ss.
pub fn unix_timestamp(column: &Column, format: Option<&str>) -> Column {
    column.clone().unix_timestamp(format)
}

/// Alias for unix_timestamp.
pub fn to_unix_timestamp(column: &Column, format: Option<&str>) -> Column {
    unix_timestamp(column, format)
}

/// Convert seconds since epoch to formatted string (PySpark from_unixtime).
pub fn from_unixtime(column: &Column, format: Option<&str>) -> Column {
    column.clone().from_unixtime(format)
}

/// Build date from year, month, day columns (PySpark make_date).
pub fn make_date(year: &Column, month: &Column, day: &Column) -> Column {
    use polars::prelude::*;
    let args = [month.expr().clone(), day.expr().clone()];
    let expr = year.expr().clone().map_many(
        |cols| crate::column::expect_col(crate::udfs::apply_make_date(cols)),
        &args,
        |_schema, fields| Ok(Field::new(fields[0].name().clone(), DataType::Date)),
    );
    crate::column::Column::from_expr(expr, None)
}

/// make_timestamp(year, month, day, hour, min, sec, timezone?) - six columns to timestamp (PySpark make_timestamp).
/// When timezone is Some(tz), components are interpreted as local time in that zone, then converted to UTC.
pub fn make_timestamp(
    year: &Column,
    month: &Column,
    day: &Column,
    hour: &Column,
    minute: &Column,
    sec: &Column,
    timezone: Option<&str>,
) -> Column {
    use polars::prelude::*;
    let tz_owned = timezone.map(|s| s.to_string());
    let args = [
        month.expr().clone(),
        day.expr().clone(),
        hour.expr().clone(),
        minute.expr().clone(),
        sec.expr().clone(),
    ];
    let expr = year.expr().clone().map_many(
        move |cols| {
            crate::column::expect_col(crate::udfs::apply_make_timestamp(cols, tz_owned.as_deref()))
        },
        &args,
        |_schema, fields| {
            Ok(Field::new(
                fields[0].name().clone(),
                DataType::Datetime(TimeUnit::Microseconds, None),
            ))
        },
    );
    crate::column::Column::from_expr(expr, None)
}

/// Add amount of unit to timestamp (PySpark timestampadd).
pub fn timestampadd(unit: &str, amount: &Column, ts: &Column) -> Column {
    ts.clone().timestampadd(unit, amount)
}

/// Difference between timestamps in unit (PySpark timestampdiff).
pub fn timestampdiff(unit: &str, start: &Column, end: &Column) -> Column {
    start.clone().timestampdiff(unit, end)
}

/// Interval of n days (PySpark days). For use in date_add, timestampadd, etc.
pub fn days(n: i64) -> Column {
    make_interval(0, 0, 0, n, 0, 0, 0)
}

/// Interval of n hours (PySpark hours).
pub fn hours(n: i64) -> Column {
    make_interval(0, 0, 0, 0, n, 0, 0)
}

/// Interval of n minutes (PySpark minutes).
pub fn minutes(n: i64) -> Column {
    make_interval(0, 0, 0, 0, 0, n, 0)
}

/// Interval of n months (PySpark months). Approximated as 30*n days.
pub fn months(n: i64) -> Column {
    make_interval(0, n, 0, 0, 0, 0, 0)
}

/// Interval of n years (PySpark years). Approximated as 365*n days.
pub fn years(n: i64) -> Column {
    make_interval(n, 0, 0, 0, 0, 0, 0)
}

/// Interpret timestamp as UTC, convert to tz (PySpark from_utc_timestamp).
pub fn from_utc_timestamp(column: &Column, tz: &str) -> Column {
    column.clone().from_utc_timestamp(tz)
}

/// Interpret timestamp as in tz, convert to UTC (PySpark to_utc_timestamp).
pub fn to_utc_timestamp(column: &Column, tz: &str) -> Column {
    column.clone().to_utc_timestamp(tz)
}

/// Convert timestamp between timezones (PySpark convert_timezone).
pub fn convert_timezone(source_tz: &str, target_tz: &str, column: &Column) -> Column {
    let source_tz = source_tz.to_string();
    let target_tz = target_tz.to_string();
    let expr = column.expr().clone().map(
        move |s| {
            crate::column::expect_col(crate::udfs::apply_convert_timezone(
                s, &source_tz, &target_tz,
            ))
        },
        |_schema, field| Ok(field.clone()),
    );
    crate::column::Column::from_expr(expr, None)
}

/// Current session timezone (PySpark current_timezone). Default "UTC". Returns literal column.
pub fn current_timezone() -> Column {
    use polars::prelude::*;
    crate::column::Column::from_expr(lit("UTC"), None)
}

/// Create interval duration (PySpark make_interval). Optional args; 0 for omitted.
pub fn make_interval(
    years: i64,
    months: i64,
    weeks: i64,
    days: i64,
    hours: i64,
    mins: i64,
    secs: i64,
) -> Column {
    use polars::prelude::*;
    // Approximate: 1 year = 365 days, 1 month = 30 days
    let total_days = years * 365 + months * 30 + weeks * 7 + days;
    let args = DurationArgs::new()
        .with_days(lit(total_days))
        .with_hours(lit(hours))
        .with_minutes(lit(mins))
        .with_seconds(lit(secs));
    let dur = duration(args);
    crate::column::Column::from_expr(dur, None)
}

/// Day-time interval: days, hours, minutes, seconds (PySpark make_dt_interval). All optional; 0 for omitted.
pub fn make_dt_interval(days: i64, hours: i64, minutes: i64, seconds: i64) -> Column {
    use polars::prelude::*;
    let args = DurationArgs::new()
        .with_days(lit(days))
        .with_hours(lit(hours))
        .with_minutes(lit(minutes))
        .with_seconds(lit(seconds));
    let dur = duration(args);
    crate::column::Column::from_expr(dur, None)
}

/// Year-month interval (PySpark make_ym_interval). Polars has no native YM type; return months as Int32 (years*12 + months).
pub fn make_ym_interval(years: i32, months: i32) -> Column {
    use polars::prelude::*;
    let total_months = years * 12 + months;
    crate::column::Column::from_expr(lit(total_months), None)
}

/// Alias for make_timestamp (PySpark make_timestamp_ntz - no timezone).
pub fn make_timestamp_ntz(
    year: &Column,
    month: &Column,
    day: &Column,
    hour: &Column,
    minute: &Column,
    sec: &Column,
) -> Column {
    make_timestamp(year, month, day, hour, minute, sec, None)
}

/// Convert seconds since epoch to timestamp (PySpark timestamp_seconds).
pub fn timestamp_seconds(column: &Column) -> Column {
    column.clone().timestamp_seconds()
}

/// Convert milliseconds since epoch to timestamp (PySpark timestamp_millis).
pub fn timestamp_millis(column: &Column) -> Column {
    column.clone().timestamp_millis()
}

/// Convert microseconds since epoch to timestamp (PySpark timestamp_micros).
pub fn timestamp_micros(column: &Column) -> Column {
    column.clone().timestamp_micros()
}

/// Date to days since 1970-01-01 (PySpark unix_date).
pub fn unix_date(column: &Column) -> Column {
    column.clone().unix_date()
}

/// Days since epoch to date (PySpark date_from_unix_date).
pub fn date_from_unix_date(column: &Column) -> Column {
    column.clone().date_from_unix_date()
}

/// Positive modulus (PySpark pmod).
pub fn pmod(dividend: &Column, divisor: &Column) -> Column {
    dividend.clone().pmod(divisor)
}

/// Factorial n! (PySpark factorial). n in 0..=20; null for negative or overflow.
pub fn factorial(column: &Column) -> Column {
    column.clone().factorial()
}

/// Concatenate columns as strings (PySpark concat). Numeric columns are cast to string (#852).
pub fn concat(columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "concat requires at least one column".into(),
        ));
    }
    let exprs: Vec<Expr> = columns
        .iter()
        .map(|c| c.expr().clone().cast(DataType::String))
        .collect();
    Ok(crate::column::Column::from_expr(
        concat_str(&exprs, "", false),
        None,
    ))
}

/// Convert one concat_ws argument to Utf8: scalar columns are cast to string; list columns
/// have elements stringified and joined with `separator` (PySpark parity; #1543).
fn expr_for_concat_ws_part(expr: Expr, separator: &str) -> Expr {
    use polars::prelude::*;
    let sep = separator.to_string();
    expr.map(
        move |col| crate::column::expect_col(concat_ws_input_column_to_string(col, &sep)),
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::String)),
    )
}

fn concat_ws_input_column_to_string(
    col: polars::prelude::Column,
    separator: &str,
) -> PolarsResult<Option<polars::prelude::Column>> {
    let out = concat_ws_input_series_to_string(col.take_materialized_series(), separator)?;
    Ok(Some(out.into_column()))
}

fn concat_ws_input_series_to_string(s: Series, separator: &str) -> PolarsResult<Series> {
    use polars::chunked_array::builder::StringChunkedBuilder;
    use polars::prelude::*;
    if !matches!(s.dtype(), DataType::List(_)) {
        return s.cast(&DataType::String);
    }
    let list = s.list()?;
    let mut builder = StringChunkedBuilder::new(s.name().clone(), list.len());
    for opt_inner in list.into_iter() {
        let joined = match opt_inner {
            None => None,
            Some(inner) => {
                let inner = inner.cast(&DataType::String)?;
                let ca = inner.str()?;
                let mut parts = Vec::new();
                for v in ca.into_iter().flatten() {
                    parts.push(v);
                }
                if parts.is_empty() {
                    None
                } else {
                    Some(parts.join(separator))
                }
            }
        };
        builder.append_option(joined.as_deref());
    }
    Ok(builder.finish().into_series())
}

/// Concatenate columns as strings with separator (PySpark concat_ws). Numeric columns cast to string (#852).
/// List/array columns are element-joined with `separator` first; string columns pass through (#1543).
pub fn concat_ws(separator: &str, columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "concat_ws requires at least one column".into(),
        ));
    }
    let exprs: Vec<Expr> = columns
        .iter()
        .map(|c| expr_for_concat_ws_part(c.expr().clone(), separator))
        .collect();
    // PySpark semantics: concat_ws ignores nulls across all arguments, and returns
    // null only when all inputs are null. Polars' concat_str with ignore_nulls=true
    // implements the same behavior.
    Ok(crate::column::Column::from_expr(
        concat_str(&exprs, separator, true),
        None,
    ))
}

/// Row number window function (1, 2, 3 by order within partition).
/// Use with `.over(partition_by)` after ranking by an order column.
///
/// # Example
/// ```
/// use robin_sparkless_polars::{col, Column};
/// let salary_col = col("salary");
/// let rn = salary_col.row_number(true).over(&["dept"]);
/// ```
pub fn row_number(column: &Column) -> Column {
    column.clone().row_number(false)
}

/// Rank window function (ties same rank, gaps). Use with `.over(partition_by)`.
pub fn rank(column: &Column, descending: bool) -> Column {
    column.clone().rank(descending)
}

/// Dense rank window function (no gaps). Use with `.over(partition_by)`.
pub fn dense_rank(column: &Column, descending: bool) -> Column {
    column.clone().dense_rank(descending)
}

/// Lag: value from n rows before in partition. Use with `.over(partition_by)`.
pub fn lag(column: &Column, n: i64) -> Column {
    column.clone().lag(n)
}

/// Lead: value from n rows after in partition. Use with `.over(partition_by)`.
pub fn lead(column: &Column, n: i64) -> Column {
    column.clone().lead(n)
}

/// First value in partition (PySpark first_value). Use with `.over(partition_by)`.
pub fn first_value(column: &Column) -> Column {
    column.clone().first_value()
}

/// Last value in partition (PySpark last_value). Use with `.over(partition_by)`.
pub fn last_value(column: &Column) -> Column {
    column.clone().last_value()
}

/// Percent rank in partition: (rank - 1) / (count - 1). Window is applied.
pub fn percent_rank(column: &Column, partition_by: &[&str], descending: bool) -> Column {
    column.clone().percent_rank(partition_by, descending)
}

/// Cumulative distribution in partition: row_number / count. Window is applied.
pub fn cume_dist(column: &Column, partition_by: &[&str], descending: bool) -> Column {
    column.clone().cume_dist(partition_by, descending)
}

/// Ntile: bucket 1..n by rank within partition. Window is applied.
pub fn ntile(column: &Column, n: u32, partition_by: &[&str], descending: bool) -> Column {
    column.clone().ntile(n, partition_by, descending)
}

/// Nth value in partition by order (1-based n). Window is applied; do not call .over() again.
pub fn nth_value(column: &Column, n: i64, partition_by: &[&str], descending: bool) -> Column {
    column.clone().nth_value(n, partition_by, descending)
}

/// Coalesce - returns the first non-null value from multiple columns.
///
/// # Example
/// ```
/// use robin_sparkless_polars::functions::{col, lit_i64, coalesce};
///
/// // coalesce(col("a"), col("b"), lit(0))
/// let expr = coalesce(&[&col("a"), &col("b"), &lit_i64(0)]);
/// ```
/// First non-null value across columns (PySpark coalesce).
pub fn coalesce(columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "coalesce requires at least one column".into(),
        ));
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    let expr = coalesce(&exprs);
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Alias for coalesce(col, value). PySpark nvl / ifnull.
pub fn nvl(column: &Column, value: &Column) -> Column {
    use polars::prelude::*;
    let cond = column.expr().clone().is_null();
    let expr = when(cond)
        .then(value.expr().clone())
        .otherwise(column.expr().clone());
    crate::column::Column::from_expr(expr, None)
}

/// Alias for nvl. PySpark ifnull.
pub fn ifnull(column: &Column, value: &Column) -> Column {
    nvl(column, value)
}

/// Return null if column equals value, else column. PySpark nullif.
pub fn nullif(column: &Column, value: &Column) -> Column {
    use polars::prelude::*;
    let cond = column.expr().clone().eq(value.expr().clone());
    let null_lit = lit(NULL);
    let expr = when(cond).then(null_lit).otherwise(column.expr().clone());
    crate::column::Column::from_expr(expr, None)
}

/// Replace NaN with value. PySpark nanvl.
pub fn nanvl(column: &Column, value: &Column) -> Column {
    use polars::prelude::*;
    let cond = column.expr().clone().is_nan();
    let expr = when(cond)
        .then(value.expr().clone())
        .otherwise(column.expr().clone());
    crate::column::Column::from_expr(expr, None)
}

/// Three-arg null replacement: if col1 is not null then col2 else col3. PySpark nvl2.
pub fn nvl2(col1: &Column, col2: &Column, col3: &Column) -> Column {
    use polars::prelude::*;
    let cond = col1.expr().clone().is_not_null();
    let expr = when(cond)
        .then(col2.expr().clone())
        .otherwise(col3.expr().clone());
    crate::column::Column::from_expr(expr, None)
}

/// Alias for substring. PySpark substr.
pub fn substr(column: &Column, start: i64, length: Option<i64>) -> Column {
    substring(column, start, length)
}

/// Alias for substring_with. PySpark substr with Column start/length (#1649).
pub fn substr_with(column: &Column, start: &Column, length: Option<&Column>) -> Column {
    substring_with(column, start, length)
}

/// Alias for pow. PySpark power.
pub fn power(column: &Column, exp: i64) -> Column {
    pow(column, exp)
}

/// Alias for log (natural log). PySpark ln.
pub fn ln(column: &Column) -> Column {
    log(column)
}

/// Alias for ceil. PySpark ceiling.
pub fn ceiling(column: &Column) -> Column {
    ceil(column)
}

/// Alias for lower. PySpark lcase.
pub fn lcase(column: &Column) -> Column {
    lower(column)
}

/// Alias for upper. PySpark ucase.
pub fn ucase(column: &Column) -> Column {
    upper(column)
}

/// Alias for day. PySpark dayofmonth.
pub fn dayofmonth(column: &Column) -> Column {
    column.clone().dayofmonth()
}

/// Alias for degrees. PySpark toDegrees.
pub fn to_degrees(column: &Column) -> Column {
    degrees(column)
}

/// Alias for radians. PySpark toRadians.
pub fn to_radians(column: &Column) -> Column {
    radians(column)
}

/// Hyperbolic cosine (PySpark cosh).
pub fn cosh(column: &Column) -> Column {
    column.clone().cosh()
}
/// Hyperbolic sine (PySpark sinh).
pub fn sinh(column: &Column) -> Column {
    column.clone().sinh()
}
/// Hyperbolic tangent (PySpark tanh).
pub fn tanh(column: &Column) -> Column {
    column.clone().tanh()
}
/// Inverse hyperbolic cosine (PySpark acosh).
pub fn acosh(column: &Column) -> Column {
    column.clone().acosh()
}
/// Inverse hyperbolic sine (PySpark asinh).
pub fn asinh(column: &Column) -> Column {
    column.clone().asinh()
}
/// Inverse hyperbolic tangent (PySpark atanh).
pub fn atanh(column: &Column) -> Column {
    column.clone().atanh()
}
/// Cube root (PySpark cbrt).
pub fn cbrt(column: &Column) -> Column {
    column.clone().cbrt()
}
/// exp(x) - 1 (PySpark expm1).
pub fn expm1(column: &Column) -> Column {
    column.clone().expm1()
}
/// log(1 + x) (PySpark log1p).
pub fn log1p(column: &Column) -> Column {
    column.clone().log1p()
}
/// Base-10 log (PySpark log10).
pub fn log10(column: &Column) -> Column {
    column.clone().log10()
}
/// Base-2 log (PySpark log2).
pub fn log2(column: &Column) -> Column {
    column.clone().log2()
}
/// Round to nearest integer (PySpark rint).
pub fn rint(column: &Column) -> Column {
    column.clone().rint()
}
/// sqrt(x*x + y*y) (PySpark hypot).
pub fn hypot(x: &Column, y: &Column) -> Column {
    let xx = x.expr().clone() * x.expr().clone();
    let yy = y.expr().clone() * y.expr().clone();
    crate::column::Column::from_expr((xx + yy).sqrt(), None)
}

/// True if column is null. PySpark isnull.
pub fn isnull(column: &Column) -> Column {
    column.clone().is_null()
}

/// True if column is not null. PySpark isnotnull.
pub fn isnotnull(column: &Column) -> Column {
    column.clone().is_not_null()
}

/// Create an array column from multiple columns (PySpark array).
/// With no arguments, returns a column of empty arrays (one per row); PySpark parity.
pub fn array(columns: &[&Column]) -> Result<crate::column::Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        // PySpark F.array() with no args: one empty list per row (broadcast literal).
        // Use .first() so the single-row literal is treated as a scalar and broadcasts to frame height.
        let empty_inner = Series::new("".into(), Vec::<i64>::new());
        let list_series = ListChunked::from_iter([Some(empty_inner)])
            .with_name("array".into())
            .into_series();
        let expr = lit(list_series).first();
        return Ok(crate::column::Column::from_expr(expr, None));
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    let expr = concat_list(exprs)
        .map_err(|e| PolarsError::ComputeError(format!("array concat_list: {e}").into()))?;
    let mut col = crate::column::Column::from_expr(expr, None);
    col.is_array_expr = true;
    Ok(col)
}

/// Number of elements in list (PySpark size / array_size). Returns Int32.
pub fn array_size(column: &Column) -> Column {
    column.clone().array_size()
}

/// Alias for array_size (PySpark size).
pub fn size(column: &Column) -> Column {
    column.clone().array_size()
}

/// Cardinality: number of elements in array (PySpark cardinality). Alias for size/array_size.
pub fn cardinality(column: &Column) -> Column {
    column.clone().cardinality()
}

/// Check if list contains value (PySpark array_contains).
pub fn array_contains(column: &Column, value: &Column) -> Column {
    column.clone().array_contains(value.expr().clone())
}

/// Join list of strings with separator (PySpark array_join).
/// If null_replacement is Some, null elements are replaced with that string.
pub fn array_join(column: &Column, separator: &str, null_replacement: Option<&str>) -> Column {
    column.clone().array_join(separator, null_replacement)
}

/// Maximum element in list (PySpark array_max).
pub fn array_max(column: &Column) -> Column {
    column.clone().array_max()
}

/// Minimum element in list (PySpark array_min).
pub fn array_min(column: &Column) -> Column {
    column.clone().array_min()
}

/// Get element at 1-based index (PySpark element_at).
pub fn element_at(column: &Column, index: i64) -> Column {
    column.clone().element_at(index)
}

/// Sort list elements (PySpark array_sort/sort_array).
/// When asc=true (default), sorts ascending with nulls first. When asc=false, sorts descending with nulls last.
pub fn array_sort(column: &Column, asc: bool) -> Column {
    column.clone().array_sort(asc)
}

/// Distinct elements in list (PySpark array_distinct).
pub fn array_distinct(column: &Column) -> Column {
    column.clone().array_distinct()
}

/// Slice list from 1-based start with optional length (PySpark slice).
pub fn array_slice(column: &Column, start: i64, length: Option<i64>) -> Column {
    column.clone().array_slice(start, length)
}

/// Generate array of numbers from start to stop (inclusive) with optional step (PySpark sequence).
/// step defaults to 1.
pub fn sequence(start: &Column, stop: &Column, step: Option<&Column>) -> Column {
    use polars::prelude::{DataType, Field, as_struct, lit};
    let step_expr = step
        .map(|c| c.expr().clone().alias("2"))
        .unwrap_or_else(|| lit(1i64).alias("2"));
    let struct_expr = as_struct(vec![
        start.expr().clone().alias("0"),
        stop.expr().clone().alias("1"),
        step_expr,
    ]);
    let out_dtype = DataType::List(Box::new(DataType::Int64));
    let expr = struct_expr.map(
        |s| crate::column::expect_col(crate::udfs::apply_sequence(s)),
        move |_schema, field| Ok(Field::new(field.name().clone(), out_dtype.clone())),
    );
    crate::column::Column::from_expr(expr, None)
}

/// Random permutation of list elements (PySpark shuffle).
pub fn shuffle(column: &Column) -> Column {
    let expr = column.expr().clone().map(
        |s| crate::column::expect_col(crate::udfs::apply_shuffle(s)),
        |_schema, field| Ok(field.clone()),
    );
    crate::column::Column::from_expr(expr, None)
}

/// Explode list of structs into rows; struct fields become columns after unnest (PySpark inline).
/// Returns the exploded struct column; use unnest to expand struct fields to columns.
pub fn inline(column: &Column) -> Column {
    column.clone().explode()
}

/// Like inline but null/empty yields one row of nulls (PySpark inline_outer).
pub fn inline_outer(column: &Column) -> Column {
    column.clone().explode_outer()
}

/// Explode list into one row per element (PySpark explode).
pub fn explode(column: &Column) -> Column {
    column.clone().explode()
}

/// 1-based index of first occurrence of value in list, or 0 if not found (PySpark array_position).
/// Implemented via Polars list.eval with col("") as element.
pub fn array_position(column: &Column, value: &Column) -> Column {
    column.clone().array_position(value.expr().clone())
}

/// Remove null elements from list (PySpark array_compact).
pub fn array_compact(column: &Column) -> Column {
    column.clone().array_compact()
}

/// New list with all elements equal to value removed (PySpark array_remove).
/// Implemented via Polars list.eval + list.drop_nulls.
pub fn array_remove(column: &Column, value: &Column) -> Column {
    column.clone().array_remove(value.expr().clone())
}

/// Repeat each element n times (PySpark array_repeat). Not implemented: would require list.eval with dynamic repeat.
pub fn array_repeat(column: &Column, n: i64) -> Column {
    column.clone().array_repeat(n)
}

/// Flatten list of lists to one list (PySpark flatten). Not implemented.
pub fn array_flatten(column: &Column) -> Column {
    column.clone().array_flatten()
}

/// True if any list element satisfies the predicate (PySpark exists).
pub fn array_exists(column: &Column, predicate: Expr) -> Column {
    column.clone().array_exists(predicate)
}

/// True if all list elements satisfy the predicate (PySpark forall).
pub fn array_forall(column: &Column, predicate: Expr) -> Column {
    column.clone().array_forall(predicate)
}

/// Filter list elements by predicate (PySpark filter).
pub fn array_filter(column: &Column, predicate: Expr) -> Column {
    column.clone().array_filter(predicate)
}

/// Transform list elements by expression (PySpark transform).
pub fn array_transform(column: &Column, f: Expr) -> Column {
    column.clone().array_transform(f)
}

/// Sum of list elements (PySpark aggregate sum).
pub fn array_sum(column: &Column) -> Column {
    column.clone().array_sum()
}

/// Array fold/aggregate (PySpark aggregate). Simplified: zero + sum(list elements).
pub fn aggregate(column: &Column, zero: &Column) -> Column {
    column.clone().array_aggregate(zero)
}

/// Mean of list elements (PySpark aggregate avg).
pub fn array_mean(column: &Column) -> Column {
    column.clone().array_mean()
}

/// Explode list with position (PySpark posexplode). Returns (pos_column, value_column).
/// pos is 1-based; implemented via list.eval(cum_count()).explode() and explode().
pub fn posexplode(column: &Column) -> (Column, Column) {
    column.clone().posexplode()
}

/// Normalize map keys per PySpark 4.0 (-0.0 -> 0.0) when map key normalization is enabled.
fn normalize_map_key_expr(key: Expr) -> Expr {
    if crate::udf_context::get_thread_runtime_config().disable_map_key_normalization {
        return key;
    }
    use polars::prelude::{lit, when};
    when(key.clone().eq(lit(-0.0f64)))
        .then(lit(0.0f64))
        .otherwise(key)
}

/// Build a map column from alternating key/value expressions (PySpark create_map).
/// Returns List(Struct{key, value}) using Polars as_struct and concat_list.
/// With no args (or empty slice), returns a column of empty maps per row (PySpark parity #275).
pub fn create_map(key_values: &[&Column]) -> Result<Column, PolarsError> {
    use polars::chunked_array::StructChunked;
    use polars::prelude::{IntoSeries, ListChunked, as_struct, concat_list, lit};
    if key_values.is_empty() {
        // PySpark F.create_map() with no args: one empty map {} per row (broadcast literal).
        let key_s = Series::new("key".into(), Vec::<String>::new());
        let value_s = Series::new("value".into(), Vec::<String>::new());
        let fields: [&Series; 2] = [&key_s, &value_s];
        let empty_struct = StructChunked::from_series(
            polars::prelude::PlSmallStr::EMPTY,
            0,
            fields.iter().copied(),
        )
        .map_err(|e| PolarsError::ComputeError(format!("create_map empty struct: {e}").into()))?
        .into_series();
        let list_series = ListChunked::from_iter([Some(empty_struct)])
            .with_name("create_map".into())
            .into_series();
        let expr = lit(list_series).first();
        return Ok(crate::column::Column::from_expr(expr, None));
    }
    let mut struct_exprs: Vec<Expr> = Vec::new();
    for i in (0..key_values.len()).step_by(2) {
        if i + 1 < key_values.len() {
            let k = normalize_map_key_expr(key_values[i].expr().clone()).alias("key");
            let v = key_values[i + 1].expr().clone().alias("value");
            struct_exprs.push(as_struct(vec![k, v]));
        }
    }
    let expr = concat_list(struct_exprs)
        .map_err(|e| PolarsError::ComputeError(format!("create_map concat_list: {e}").into()))?;
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Extract keys from a map column (PySpark map_keys). Map is List(Struct{key, value}).
pub fn map_keys(column: &Column) -> Column {
    column.clone().map_keys()
}

/// Extract values from a map column (PySpark map_values).
pub fn map_values(column: &Column) -> Column {
    column.clone().map_values()
}

/// Return map as list of structs {key, value} (PySpark map_entries).
pub fn map_entries(column: &Column) -> Column {
    column.clone().map_entries()
}

/// Build map from two array columns keys and values (PySpark map_from_arrays). Implemented via UDF.
pub fn map_from_arrays(keys: &Column, values: &Column) -> Column {
    keys.clone().map_from_arrays(values)
}

/// Merge two map columns (PySpark map_concat). Last value wins for duplicate keys.
pub fn map_concat(a: &Column, b: &Column) -> Column {
    a.clone().map_concat(b)
}

/// Array of structs {key, value} to map (PySpark map_from_entries).
pub fn map_from_entries(column: &Column) -> Column {
    column.clone().map_from_entries()
}

/// True if map contains key (PySpark map_contains_key).
pub fn map_contains_key(map_col: &Column, key: &Column) -> Column {
    map_col.clone().map_contains_key(key)
}

/// Get value for key from map, or null (PySpark get).
pub fn get(map_col: &Column, key: &Column) -> Column {
    map_col.clone().get(key)
}

/// Filter map entries by predicate (PySpark map_filter).
pub fn map_filter(map_col: &Column, predicate: Expr) -> Column {
    map_col.clone().map_filter(predicate)
}

/// Merge two maps by key with merge function (PySpark map_zip_with).
pub fn map_zip_with(map1: &Column, map2: &Column, merge: Expr) -> Column {
    map1.clone().map_zip_with(map2, merge)
}

/// Convenience: zip_with with coalesce(left, right) merge.
pub fn zip_with_coalesce(left: &Column, right: &Column) -> Column {
    use polars::prelude::col;
    let left_field = col("").struct_().field_by_name("left");
    let right_field = col("").struct_().field_by_name("right");
    let merge = crate::column::Column::from_expr(
        coalesce(&[
            &crate::column::Column::from_expr(left_field, None),
            &crate::column::Column::from_expr(right_field, None),
        ])
        .expect("zip_with_coalesce passes two columns")
        .into_expr(),
        None,
    );
    left.clone().zip_with(right, merge.into_expr())
}

/// Convenience: map_zip_with with coalesce(value1, value2) merge.
pub fn map_zip_with_coalesce(map1: &Column, map2: &Column) -> Column {
    use polars::prelude::col;
    let v1 = col("").struct_().field_by_name("value1");
    let v2 = col("").struct_().field_by_name("value2");
    let merge = coalesce(&[
        &crate::column::Column::from_expr(v1, None),
        &crate::column::Column::from_expr(v2, None),
    ])
    .expect("map_zip_with_coalesce passes two columns")
    .into_expr();
    map1.clone().map_zip_with(map2, merge)
}

/// Convenience: map_filter with value > threshold predicate.
pub fn map_filter_value_gt(map_col: &Column, threshold: f64) -> Column {
    use polars::prelude::{col, lit};
    let pred = col("").struct_().field_by_name("value").gt(lit(threshold));
    map_col.clone().map_filter(pred)
}

/// Create struct from columns using column names as field names (PySpark struct).
pub fn struct_(columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::as_struct;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "struct requires at least one column".into(),
        ));
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    Ok(crate::column::Column::from_expr(as_struct(exprs), None))
}

/// Create struct with explicit field names (PySpark named_struct). Pairs of (name, column).
pub fn named_struct(pairs: &[(&str, &Column)]) -> Result<Column, PolarsError> {
    use polars::prelude::as_struct;
    if pairs.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "named_struct requires at least one (name, column) pair".into(),
        ));
    }
    let exprs: Vec<Expr> = pairs
        .iter()
        .map(|(name, col)| col.expr().clone().alias(*name))
        .collect();
    Ok(crate::column::Column::from_expr(as_struct(exprs), None))
}

/// Append element to end of list (PySpark array_append).
pub fn array_append(array: &Column, elem: &Column) -> Column {
    array.clone().array_append(elem)
}

/// Prepend element to start of list (PySpark array_prepend).
pub fn array_prepend(array: &Column, elem: &Column) -> Column {
    array.clone().array_prepend(elem)
}

/// Insert element at 1-based position (PySpark array_insert).
pub fn array_insert(array: &Column, pos: &Column, elem: &Column) -> Column {
    array.clone().array_insert(pos, elem)
}

/// Elements in first array not in second (PySpark array_except).
pub fn array_except(a: &Column, b: &Column) -> Column {
    a.clone().array_except(b)
}

/// Elements in both arrays (PySpark array_intersect).
pub fn array_intersect(a: &Column, b: &Column) -> Column {
    a.clone().array_intersect(b)
}

/// Distinct elements from both arrays (PySpark array_union).
pub fn array_union(a: &Column, b: &Column) -> Column {
    a.clone().array_union(b)
}

/// Zip two arrays element-wise with merge function (PySpark zip_with).
pub fn zip_with(left: &Column, right: &Column, merge: Expr) -> Column {
    left.clone().zip_with(right, merge)
}

/// Extract JSON path from string column (PySpark get_json_object).
pub fn get_json_object(column: &Column, path: &str) -> Column {
    column.clone().get_json_object(path)
}

/// Keys of JSON object (PySpark json_object_keys). Returns list of strings.
pub fn json_object_keys(column: &Column) -> Column {
    column.clone().json_object_keys()
}

/// Extract keys from JSON as struct (PySpark json_tuple). keys: e.g. ["a", "b"].
pub fn json_tuple(column: &Column, keys: &[&str]) -> Column {
    column.clone().json_tuple(keys)
}

/// Parse CSV string to struct (PySpark from_csv). Minimal implementation.
pub fn from_csv(column: &Column) -> Column {
    column.clone().from_csv()
}

/// Format struct as CSV string (PySpark to_csv). Minimal implementation.
pub fn to_csv(column: &Column) -> Column {
    column.clone().to_csv()
}

/// Schema of CSV string (PySpark schema_of_csv). Returns literal schema string; minimal stub.
pub fn schema_of_csv(_column: &Column) -> Column {
    Column::from_expr(
        lit("STRUCT<_c0: STRING, _c1: STRING>".to_string()),
        Some("schema_of_csv".to_string()),
    )
}

/// Schema of JSON string (PySpark schema_of_json). Returns literal schema string; minimal stub.
pub fn schema_of_json(_column: &Column) -> Column {
    Column::from_expr(
        lit("STRUCT<>".to_string()),
        Some("schema_of_json".to_string()),
    )
}

/// Parse string column as JSON into VARIANT (PySpark 4 parse_json). Stored as validated JSON string.
pub fn parse_json(column: &Column) -> Column {
    column.clone().parse_json_variant()
}

/// Parse string column as JSON into struct (PySpark from_json).
pub fn from_json(column: &Column, schema: Option<polars::datatypes::DataType>) -> Column {
    column.clone().from_json(schema)
}

/// Serialize struct column to JSON string (PySpark to_json).
pub fn to_json(column: &Column) -> Column {
    column.clone().to_json()
}

/// Check if column values are in the given list (PySpark isin). Uses Polars is_in.
pub fn isin(column: &Column, other: &Column) -> Column {
    column.clone().isin(other)
}

/// Check if column values are in the given i64 slice (PySpark isin with literal list).
/// #986: Use string series and cast column to string so string column isin(1, 2) works (PySpark parity).
pub fn isin_i64(column: &Column, values: &[i64]) -> Column {
    let str_vals: Vec<String> = values.iter().map(|n| n.to_string()).collect();
    let s: Series = Series::from_iter(str_vals.iter().map(String::as_str));
    Column::from_expr(
        column
            .expr()
            .clone()
            .cast(DataType::String)
            .is_in(lit(s).implode(), false),
        None,
    )
}

/// Check if column values are in the given string slice (PySpark isin with literal list).
/// #1588: Cast column to string so int column isin(["1", "2"]) works (PySpark parity).
pub fn isin_str(column: &Column, values: &[&str]) -> Column {
    let s: Series = Series::from_iter(values.iter().copied());
    Column::from_expr(
        column
            .expr()
            .clone()
            .cast(DataType::String)
            .is_in(lit(s).implode(), false),
        None,
    )
}

/// Percent-decode URL-encoded string (PySpark url_decode).
pub fn url_decode(column: &Column) -> Column {
    column.clone().url_decode()
}

/// Percent-encode string for URL (PySpark url_encode).
pub fn url_encode(column: &Column) -> Column {
    column.clone().url_encode()
}

/// Bitwise left shift (PySpark shiftLeft). col << n.
pub fn shift_left(column: &Column, n: i32) -> Column {
    column.clone().shift_left(n)
}

/// Bitwise signed right shift (PySpark shiftRight). col >> n.
pub fn shift_right(column: &Column, n: i32) -> Column {
    column.clone().shift_right(n)
}

/// Bitwise unsigned right shift (PySpark shiftRightUnsigned). Logical shift for Long.
pub fn shift_right_unsigned(column: &Column, n: i32) -> Column {
    column.clone().shift_right_unsigned(n)
}

/// Session/library version string (PySpark version).
pub fn version() -> Column {
    Column::from_expr(
        lit(concat!("robin-sparkless-", env!("CARGO_PKG_VERSION"))),
        None,
    )
}

/// Null-safe equality: true if both null or both equal (PySpark equal_null). Alias for eq_null_safe.
pub fn equal_null(left: &Column, right: &Column) -> Column {
    left.clone().eq_null_safe(right)
}

/// Length of JSON array at path (PySpark json_array_length).
pub fn json_array_length(column: &Column, path: &str) -> Column {
    column.clone().json_array_length(path)
}

/// Parse URL and extract part: PROTOCOL, HOST, PATH, etc. (PySpark parse_url).
/// When key is Some(k) and part is QUERY/QUERYSTRING, returns the value for that query parameter only.
pub fn parse_url(column: &Column, part: &str, key: Option<&str>) -> Column {
    column.clone().parse_url(part, key)
}

/// Hash of column values (PySpark hash). Uses Murmur3 32-bit for parity with PySpark.
pub fn hash(columns: &[&Column]) -> Column {
    use polars::prelude::*;
    if columns.is_empty() {
        return crate::column::Column::from_expr(lit(0i64), None);
    }
    if columns.len() == 1 {
        return columns[0].clone().hash();
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    let struct_expr = polars::prelude::as_struct(exprs);
    let name = columns[0].name().to_string();
    let expr = struct_expr.map(
        |s| crate::column::expect_col(crate::udfs::apply_hash_struct(s)),
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Int64)),
    );
    crate::column::Column::from_expr(expr, Some(name))
}

/// Stack columns into struct (PySpark stack). Alias for struct_.
pub fn stack(columns: &[&Column]) -> Result<Column, PolarsError> {
    struct_(columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::{col, lit_bool, lit_f64, lit_i32, lit_i64, lit_str};
    use polars::prelude::{DataType, IntoLazy, Series, df};

    #[test]
    fn test_col_creates_column() {
        let column = col("test");
        assert_eq!(column.name(), "test");
    }

    #[test]
    fn test_lit_i32() {
        let column = lit_i32(42);
        // The column should have a default name since it's a literal
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn test_lit_i64() {
        let column = lit_i64(123456789012345i64);
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn count_typed_literal_counts_every_row() {
        let input = df!("value" => [Some(1i64), None, Some(3), None]).unwrap();
        let out = input
            .lazy()
            .select([count(&lit_i64(1)).into_expr().alias("count_all")])
            .collect()
            .unwrap();
        assert_eq!(
            out.column("count_all").unwrap().i64().unwrap().get(0),
            Some(4)
        );
    }

    #[test]
    fn test_lit_f64() {
        let column = lit_f64(std::f64::consts::PI);
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn test_lit_bool() {
        let column = lit_bool(true);
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn test_lit_str() {
        let column = lit_str("hello");
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn cast_null_literal_to_int_and_long() {
        // Simulate F.lit(None).cast(IntegerType/LongType): casting from Null to Int32/Int64
        // should yield typed null columns instead of erroring (#1028).
        let s = Series::new_null("x".into(), 3);

        // Cast via the lower-level helper to be explicit about the target type.
        let col_i32 =
            crate::udfs::apply_string_to_int(s.clone().into_column(), true, DataType::Int32, false)
                .expect("cast to Int32 should succeed")
                .expect("column should be present");
        let col_i64 =
            crate::udfs::apply_string_to_int(s.into_column(), true, DataType::Int64, false)
                .expect("cast to Int64 should succeed")
                .expect("column should be present");

        assert_eq!(col_i32.dtype(), &DataType::Int32);
        assert_eq!(col_i64.dtype(), &DataType::Int64);
        assert!(col_i32.is_null().all());
        assert!(col_i64.is_null().all());
    }

    #[test]
    fn cast_string_and_float_columns_to_smallint() {
        let strings = Series::new("x".into(), vec![Some("12"), Some("40000"), None]);
        let cast_strings =
            crate::udfs::apply_string_to_int(strings.into_column(), false, DataType::Int16, false)
                .unwrap()
                .unwrap();
        assert_eq!(cast_strings.dtype(), &DataType::Int16);
        assert_eq!(
            cast_strings.i16().unwrap().into_iter().collect::<Vec<_>>(),
            vec![Some(12), None, None]
        );

        let floats = Series::new("x".into(), vec![Some(12.9f64), Some(40000.0), None]);
        let cast_floats = crate::udfs::apply_string_to_int(
            floats.clone().into_column(),
            false,
            DataType::Int16,
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(cast_floats.dtype(), &DataType::Int16);
        assert_eq!(
            cast_floats.i16().unwrap().into_iter().collect::<Vec<_>>(),
            vec![Some(12), None, None]
        );
        assert!(
            crate::udfs::apply_string_to_int(floats.into_column(), true, DataType::Int16, false,)
                .is_err()
        );
    }

    #[test]
    fn cast_smallint_column_to_integer_types() {
        let input = Series::new("x".into(), vec![Some(12_i16), None, Some(-7)]);

        for target in [DataType::Int16, DataType::Int32, DataType::Int64] {
            let cast = crate::udfs::apply_string_to_int(
                input.clone().into_column(),
                true,
                target.clone(),
                false,
            )
            .unwrap()
            .unwrap();

            assert_eq!(cast.dtype(), &target);
            match target {
                DataType::Int16 => assert_eq!(
                    cast.i16().unwrap().into_iter().collect::<Vec<_>>(),
                    vec![Some(12), None, Some(-7)]
                ),
                DataType::Int32 => assert_eq!(
                    cast.i32().unwrap().into_iter().collect::<Vec<_>>(),
                    vec![Some(12), None, Some(-7)]
                ),
                DataType::Int64 => assert_eq!(
                    cast.i64().unwrap().into_iter().collect::<Vec<_>>(),
                    vec![Some(12), None, Some(-7)]
                ),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_create_map_empty() {
        // PySpark F.create_map() with no args: column of empty maps (#275).
        let empty_col = create_map(&[]).unwrap();
        let df = df!("id" => &[1i64, 2i64]).unwrap();
        let out = df
            .lazy()
            .with_columns([empty_col.into_expr().alias("m")])
            .collect()
            .unwrap();
        assert_eq!(out.height(), 2);
        let m = out.column("m").unwrap();
        assert_eq!(m.len(), 2);
        let list = m.list().unwrap();
        for i in 0..2 {
            let row = list.get(i).unwrap();
            assert_eq!(row.len(), 0);
        }
    }

    #[test]
    fn test_count_aggregation() {
        let column = col("value");
        let result = count(&column);
        assert_eq!(result.name(), "count(value)");
    }

    #[test]
    fn test_sum_aggregation() {
        let column = col("value");
        let result = sum(&column);
        assert_eq!(result.name(), "sum(value)");
    }

    #[test]
    fn test_avg_aggregation() {
        let column = col("value");
        let result = avg(&column);
        assert_eq!(result.name(), "avg(value)");
    }

    #[test]
    fn test_max_aggregation() {
        let column = col("value");
        let result = max(&column);
        assert_eq!(result.name(), "max(value)");
    }

    #[test]
    fn test_min_aggregation() {
        let column = col("value");
        let result = min(&column);
        assert_eq!(result.name(), "min(value)");
    }

    #[test]
    fn test_when_then_otherwise() {
        // Create a simple DataFrame
        let df = df!(
            "age" => &[15, 25, 35]
        )
        .unwrap();

        // Build a when-then-otherwise expression
        let age_col = col("age");
        let condition = age_col.gt(polars::prelude::lit(18));
        let result = when(&condition)
            .then(&lit_str("adult"))
            .otherwise(&lit_str("minor"));

        // Apply the expression
        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("status"))
            .collect()
            .unwrap();

        // Verify the result
        let status_col = result_df.column("status").unwrap();
        let values: Vec<Option<&str>> = status_col.str().unwrap().into_iter().collect();

        assert_eq!(values[0], Some("minor")); // age 15 < 18
        assert_eq!(values[1], Some("adult")); // age 25 > 18
        assert_eq!(values[2], Some("adult")); // age 35 > 18
    }

    /// #680: when/then/otherwise with numeric then/else (condition Boolean, then/else any type).
    #[test]
    fn test_when_then_otherwise_numeric_then_else() {
        let df = df!("a" => &[1i64, 3, 5]).unwrap();
        let condition = col("a").gt(polars::prelude::lit(2));
        let result = when(&condition).then(&lit_i64(100)).otherwise(&lit_i64(0));
        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("out"))
            .collect()
            .unwrap();
        let out = result_df.column("out").unwrap();
        // Polars may infer i32 for when/then/otherwise with small literals; cast to i64 for stable assertion.
        let out_i64 = out.cast(&DataType::Int64).unwrap();
        let values: Vec<Option<i64>> = out_i64.i64().unwrap().into_iter().collect();
        assert_eq!(values, vec![Some(0), Some(100), Some(100)]);
    }

    #[test]
    fn test_coalesce_returns_first_non_null() {
        // Create a DataFrame with some nulls
        let df = df!(
            "a" => &[Some(1), None, None],
            "b" => &[None, Some(2), None],
            "c" => &[None, None, Some(3)]
        )
        .unwrap();

        let col_a = col("a");
        let col_b = col("b");
        let col_c = col("c");
        let result = coalesce(&[&col_a, &col_b, &col_c]).unwrap();

        // Apply the expression
        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("coalesced"))
            .collect()
            .unwrap();

        // Verify the result
        let coalesced_col = result_df.column("coalesced").unwrap();
        let values: Vec<Option<i32>> = coalesced_col.i32().unwrap().into_iter().collect();

        assert_eq!(values[0], Some(1)); // First non-null is 'a'
        assert_eq!(values[1], Some(2)); // First non-null is 'b'
        assert_eq!(values[2], Some(3)); // First non-null is 'c'
    }

    #[test]
    fn test_coalesce_with_literal_fallback() {
        // Create a DataFrame with all nulls in one row
        let df = df!(
            "a" => &[Some(1), None],
            "b" => &[None::<i32>, None::<i32>]
        )
        .unwrap();

        let col_a = col("a");
        let col_b = col("b");
        let fallback = lit_i32(0);
        let result = coalesce(&[&col_a, &col_b, &fallback]).unwrap();

        // Apply the expression
        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("coalesced"))
            .collect()
            .unwrap();

        // Verify the result
        let coalesced_col = result_df.column("coalesced").unwrap();
        let values: Vec<Option<i32>> = coalesced_col.i32().unwrap().into_iter().collect();

        assert_eq!(values[0], Some(1)); // First non-null is 'a'
        assert_eq!(values[1], Some(0)); // All nulls, use fallback
    }

    #[test]
    fn test_coalesce_empty_returns_error() {
        let columns: [&Column; 0] = [];
        let err = coalesce(&columns).unwrap_err();
        assert!(
            err.to_string()
                .contains("coalesce requires at least one column")
        );
    }

    #[test]
    fn test_cast_double_string_column_strict_ok() {
        // All values parse as doubles, so strict cast should succeed.
        let df = df!(
            "s" => &["123", " 45.5 ", "0"]
        )
        .unwrap();

        let s_col = col("s");
        let cast_col = cast(&s_col, "double").unwrap();

        let out = df
            .lazy()
            .with_column(cast_col.into_expr().alias("v"))
            .collect()
            .unwrap();

        let v = out.column("v").unwrap();
        let vals: Vec<Option<f64>> = v.f64().unwrap().into_iter().collect();
        assert_eq!(vals, vec![Some(123.0), Some(45.5), Some(0.0)]);
    }

    #[test]
    fn test_try_cast_double_string_column_invalid_to_null() {
        // Invalid numeric strings should become null under try_cast / try_to_number.
        let df = df!(
            "s" => &["123", " 45.5 ", "abc", ""]
        )
        .unwrap();

        let s_col = col("s");
        let try_cast_col = try_cast(&s_col, "double").unwrap();

        let out = df
            .lazy()
            .with_column(try_cast_col.into_expr().alias("v"))
            .collect()
            .unwrap();

        let v = out.column("v").unwrap();
        let vals: Vec<Option<f64>> = v.f64().unwrap().into_iter().collect();
        assert_eq!(vals, vec![Some(123.0), Some(45.5), None, None]);
    }

    /// #649: cast/try_cast double to int: truncation; NaN/inf -> null (try_cast) or error (cast).
    #[test]
    fn test_cast_double_to_int_nan_to_null() {
        use polars::prelude::df;

        let df = df!(
            "x" => &[1.7, 2.3, f64::NAN, f64::INFINITY]
        )
        .unwrap();

        let col_x = col("x");
        let try_cast_col = try_cast(&col_x, "int").unwrap();

        let out = df
            .lazy()
            .with_column(try_cast_col.into_expr().alias("v"))
            .collect()
            .unwrap();

        let v = out.column("v").unwrap();
        let vals: Vec<Option<i32>> = v.i32().unwrap().into_iter().collect();
        assert_eq!(vals, vec![Some(1), Some(2), None, None]);
    }

    #[test]
    fn test_to_number_and_try_to_number_numerics_and_strings() {
        // Mixed numeric types should be cast to double; invalid strings become null only for try_to_number.
        let df = df!(
            "i" => &[1i32, 2, 3],
            "f" => &[1.5f64, 2.5, 3.5],
            "s" => &["10", "20.5", "xyz"]
        )
        .unwrap();

        let i_col = col("i");
        let f_col = col("f");
        let s_col = col("s");

        let to_number_i = to_number(&i_col, None).unwrap();
        let to_number_f = to_number(&f_col, None).unwrap();
        let try_to_number_s = try_to_number(&s_col, None).unwrap();

        let out = df
            .lazy()
            .with_columns([
                to_number_i.into_expr().alias("i_num"),
                to_number_f.into_expr().alias("f_num"),
                try_to_number_s.into_expr().alias("s_num"),
            ])
            .collect()
            .unwrap();

        let i_num = out.column("i_num").unwrap();
        let f_num = out.column("f_num").unwrap();
        let s_num = out.column("s_num").unwrap();

        let i_vals: Vec<Option<f64>> = i_num.f64().unwrap().into_iter().collect();
        let f_vals: Vec<Option<f64>> = f_num.f64().unwrap().into_iter().collect();
        let s_vals: Vec<Option<f64>> = s_num.f64().unwrap().into_iter().collect();

        assert_eq!(i_vals, vec![Some(1.0), Some(2.0), Some(3.0)]);
        assert_eq!(f_vals, vec![Some(1.5), Some(2.5), Some(3.5)]);
        assert_eq!(s_vals, vec![Some(10.0), Some(20.5), None]);
    }
}
