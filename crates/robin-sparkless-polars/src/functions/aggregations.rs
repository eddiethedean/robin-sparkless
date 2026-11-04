//! Aggregations expression builders.

use super::imports::*;
use super::numeric::timestamp_cast_expr;

pub(crate) struct DecimalPrefixExtrema {
    len: usize,
    leaf_count: usize,
    minima: Vec<BigInt>,
    maxima: Vec<BigInt>,
}

impl DecimalPrefixExtrema {
    pub(crate) fn new(prefix_sums: &[BigInt]) -> Self {
        let len = prefix_sums.len();
        let leaf_count = len.next_power_of_two().max(1);
        let mut minima = vec![BigInt::zero(); leaf_count * 2];
        let mut maxima = vec![BigInt::zero(); leaf_count * 2];
        for (idx, prefix_sum) in prefix_sums.iter().enumerate() {
            minima[leaf_count + idx] = prefix_sum.clone();
            maxima[leaf_count + idx] = prefix_sum.clone();
        }
        for idx in (1..leaf_count).rev() {
            let left = idx * 2;
            let right = left + 1;
            minima[idx] = if minima[left].cmp(&minima[right]).is_le() {
                minima[left].clone()
            } else {
                minima[right].clone()
            };
            maxima[idx] = if maxima[left].cmp(&maxima[right]).is_ge() {
                maxima[left].clone()
            } else {
                maxima[right].clone()
            };
        }
        Self {
            len,
            leaf_count,
            minima,
            maxima,
        }
    }

    /// Check whether a running sum starting at zero exceeds `maximum_delta`
    /// anywhere in the prefix range `[query_start, query_end)`.
    pub(crate) fn exceeds_delta_from(
        &self,
        baseline: &BigInt,
        query_start: usize,
        query_end: usize,
        maximum_delta: &BigInt,
    ) -> bool {
        debug_assert!(query_start <= query_end);
        debug_assert!(query_end <= self.len);
        if query_start == query_end {
            return false;
        }

        let lower = baseline - maximum_delta;
        let upper = baseline + maximum_delta;
        let mut left = query_start + self.leaf_count;
        let mut right = query_end + self.leaf_count;
        while left < right {
            if left % 2 == 1 {
                if self.minima[left].cmp(&lower).is_lt() || self.maxima[left].cmp(&upper).is_gt() {
                    return true;
                }
                left += 1;
            }
            if right % 2 == 1 {
                right -= 1;
                if self.minima[right].cmp(&lower).is_lt() || self.maxima[right].cmp(&upper).is_gt()
                {
                    return true;
                }
            }
            left /= 2;
            right /= 2;
        }
        false
    }
}

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

fn integral_literal_decimal_type(expression: &Expr) -> Option<DataType> {
    let column = Column::from_expr(expression.clone(), None);
    let value = match column.value_preserving_literal_value()? {
        LiteralValue::Dyn(polars_plan::prelude::DynLiteralValue::Int(value)) => *value,
        LiteralValue::Scalar(value) if value.dtype().is_integer() => {
            value.as_any_value().extract::<i128>()?
        }
        _ => return None,
    };
    let precision = value.unsigned_abs().to_string().len();
    (precision <= 38).then_some(DataType::Decimal(precision, 0))
}

/// Bind runtime-dependent expressions to the configuration of the plan being analyzed.
/// Rebuilding the UDF gives reused expressions independent plan state.
pub(crate) fn bind_runtime_config(expr: Expr, schema: &Schema) -> PolarsResult<Expr> {
    let ansi_enabled = crate::udf_context::get_thread_ansi_enabled();
    let allow_precision_loss = crate::udf_context::get_thread_decimal_allow_precision_loss();
    expr.try_map_expr(|expr| {
        if let Expr::AnonymousFunction { input, fmt_str, .. } = &expr {
            if matches!(
                fmt_str.as_str(),
                "cast_timestamp"
                    | "try_cast_timestamp"
                    | "cast_timestamp_ntz"
                    | "try_cast_timestamp_ntz"
            ) && input.len() == 1
            {
                return Ok(timestamp_cast_expr(
                    input[0].clone(),
                    fmt_str.as_str().starts_with("cast_"),
                    fmt_str.as_str().ends_with("_ntz"),
                ));
            }
            if let Some(op) =
                crate::decimal_arithmetic::BinaryOp::from_engine_expr_marker(fmt_str.as_str())
                && input.len() == 2
            {
                let mut left = input[0].clone();
                let mut right = input[1].clone();
                let mut original_left_type = left.to_field(schema)?.dtype().clone();
                let mut original_right_type = right.to_field(schema)?.dtype().clone();
                // Spark uses the literal's decimal digits, rather than the full
                // integral type width, when pairing an integer literal with a decimal.
                if matches!(original_right_type, DataType::Decimal(..))
                    && let Some(dtype) = integral_literal_decimal_type(&left)
                {
                    left = left.cast(dtype.clone());
                    original_left_type = dtype;
                }
                if matches!(original_left_type, DataType::Decimal(..))
                    && let Some(dtype) = integral_literal_decimal_type(&right)
                {
                    right = right.cast(dtype.clone());
                    original_right_type = dtype;
                }
                let (left_type, right_type) = crate::column::expr_ir_arithmetic_types(
                    &original_left_type,
                    &original_right_type,
                )
                .unwrap_or((original_left_type, original_right_type));
                if left.to_field(schema)?.dtype() != &left_type {
                    left = left.cast(left_type.clone());
                }
                if right.to_field(schema)?.dtype() != &right_type {
                    right = right.cast(right_type.clone());
                }

                if crate::decimal_arithmetic::result_type(
                    &left_type,
                    &right_type,
                    op,
                    allow_precision_loss,
                )
                .is_some()
                {
                    return Ok(crate::column::pyspark_binary_arithmetic_expr(
                        left,
                        right,
                        op,
                        ansi_enabled,
                        allow_precision_loss,
                    ));
                }

                if (matches!(&left_type, DataType::Decimal(..)) && right_type.is_float())
                    || (matches!(&right_type, DataType::Decimal(..)) && left_type.is_float())
                {
                    let common_type = DataType::Float64;
                    let left = crate::type_coercion::coerce_expr_to_common_type(
                        left,
                        &left_type,
                        &common_type,
                    );
                    let right = crate::type_coercion::coerce_expr_to_common_type(
                        right,
                        &right_type,
                        &common_type,
                    );
                    return Ok(match op {
                        crate::decimal_arithmetic::BinaryOp::Add => left + right,
                        crate::decimal_arithmetic::BinaryOp::Subtract => left - right,
                        crate::decimal_arithmetic::BinaryOp::Multiply => left * right,
                        crate::decimal_arithmetic::BinaryOp::Divide => {
                            crate::ansi::div_expr(left, right)
                        }
                    });
                }

                let (left, right) = if crate::type_coercion::is_numeric_public(&left_type)
                    && crate::type_coercion::is_numeric_public(&right_type)
                {
                    let common_type =
                        crate::type_coercion::find_common_type(&left_type, &right_type)?;
                    (
                        crate::type_coercion::coerce_expr_to_common_type(
                            left,
                            &left_type,
                            &common_type,
                        ),
                        crate::type_coercion::coerce_expr_to_common_type(
                            right,
                            &right_type,
                            &common_type,
                        ),
                    )
                } else {
                    (left, right)
                };

                return Ok(match op {
                    crate::decimal_arithmetic::BinaryOp::Add => crate::ansi::add_expr(left, right),
                    crate::decimal_arithmetic::BinaryOp::Subtract => {
                        crate::ansi::sub_expr(left, right)
                    }
                    crate::decimal_arithmetic::BinaryOp::Multiply => {
                        crate::ansi::mul_expr(left, right)
                    }
                    crate::decimal_arithmetic::BinaryOp::Divide => {
                        crate::ansi::div_expr(left, right)
                    }
                });
            }
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
                            if !found_value || overflowed {
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
            let mut sums: Vec<BigInt> = Vec::with_capacity(series.len() + 1);
            let mut counts: Vec<usize> = Vec::with_capacity(series.len() + 1);
            sums.push(BigInt::zero());
            counts.push(0);
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
            let prefix_extrema = DecimalPrefixExtrema::new(&sums);
            let mut values = Vec::with_capacity(series.len());
            for idx in 0..series.len() {
                let Some((left, right)) = window_bounds(idx, series.len(), frame) else {
                    values.push(None);
                    continue;
                };
                let sum = &sums[right + 1] - &sums[left];
                let count = counts[right + 1] - counts[left];
                if count == 0 {
                    values.push(None);
                    continue;
                }
                if average {
                    // Spark keeps AVG's running sum in Decimal(p + 10, s), capped
                    // at precision 38. Check every intermediate frame sum before
                    // dividing; later values can cancel an earlier overflow.
                    if prefix_extrema.exceeds_delta_from(
                        &sums[left],
                        left + 1,
                        right + 2,
                        &max_sum_decimal,
                    ) {
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
                } else if prefix_extrema.exceeds_delta_from(
                    &sums[left],
                    left + 1,
                    right + 2,
                    &max_sum_decimal,
                ) || sum.abs() > max_decimal
                {
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
    add_compensation: f64,
    remove_compensation: f64,
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
    fn update_finite(&mut self, value: f64, adding: bool) {
        if let Some(exact_sum) = &mut self.exact_finite_sum {
            let scaled = finite_float_as_scaled_integer(value);
            if adding {
                *exact_sum += scaled;
            } else {
                *exact_sum -= scaled;
            }
        }
        // Keep the rounding errors from additions and removals separate. Reusing
        // one Kahan compensation can erase small values when a large value leaves
        // a sliding frame and the next value is added.
        let compensation = if adding {
            &mut self.add_compensation
        } else {
            &mut self.remove_compensation
        };
        let signed_value = if adding { value } else { -value };
        let adjusted = signed_value - *compensation;
        let next = self.finite_sum + adjusted;
        if self.finite_sum.is_finite() && next.is_finite() {
            *compensation = (next - self.finite_sum) - adjusted;
        } else {
            *compensation = 0.0;
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
            self.update_finite(value, sign > 0.0);
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
            // Kahan compensation normally gets folded into the next update. Apply
            // any remainder at read time too, since a frame can end after a null
            // or non-finite value without another finite update to consume it.
            self.finite_sum - self.add_compensation - self.remove_compensation
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
