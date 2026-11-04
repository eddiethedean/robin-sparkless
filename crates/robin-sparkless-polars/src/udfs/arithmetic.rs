//! Arithmetic UDF implementations.

use super::*;

/// PySpark-style addition for Python Column + operator.
/// For string inputs, PySpark treats + as numeric addition: non-numeric strings become null.
/// Decimal and integral inputs take the exact decimal path; other values use Float64 parsing.
pub fn apply_pyspark_add(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    apply_pyspark_add_with_precision_loss(
        columns,
        crate::udf_context::get_thread_ansi_enabled(),
        crate::udf_context::get_thread_decimal_allow_precision_loss(),
    )
}

pub(crate) fn apply_pyspark_add_with_precision_loss(
    columns: &mut [Column],
    ansi_enabled: bool,
    allow_precision_loss: bool,
) -> PolarsResult<Option<Column>> {
    if let Some(output) = pyspark_decimal_binary_arith(
        columns,
        "pyspark_add",
        crate::decimal_arithmetic::BinaryOp::Add,
        ansi_enabled,
        allow_precision_loss,
    )? {
        return Ok(Some(output));
    }
    pyspark_binary_arith(columns, "pyspark_add", |a, b| a + b)
}

/// PySpark-style subtraction with string/number coercion for Python Column operators.
pub fn apply_pyspark_subtract(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    apply_pyspark_subtract_with_precision_loss(
        columns,
        crate::udf_context::get_thread_ansi_enabled(),
        crate::udf_context::get_thread_decimal_allow_precision_loss(),
    )
}

pub(crate) fn apply_pyspark_subtract_with_precision_loss(
    columns: &mut [Column],
    ansi_enabled: bool,
    allow_precision_loss: bool,
) -> PolarsResult<Option<Column>> {
    if let Some(output) = pyspark_decimal_binary_arith(
        columns,
        "pyspark_subtract",
        crate::decimal_arithmetic::BinaryOp::Subtract,
        ansi_enabled,
        allow_precision_loss,
    )? {
        return Ok(Some(output));
    }
    pyspark_binary_arith(columns, "pyspark_subtract", |a, b| a - b)
}

/// PySpark-style multiplication with string/number coercion for Python Column operators.
pub fn apply_pyspark_multiply(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    apply_pyspark_multiply_with_precision_loss(
        columns,
        crate::udf_context::get_thread_ansi_enabled(),
        crate::udf_context::get_thread_decimal_allow_precision_loss(),
    )
}

pub(crate) fn apply_pyspark_multiply_with_precision_loss(
    columns: &mut [Column],
    ansi_enabled: bool,
    allow_precision_loss: bool,
) -> PolarsResult<Option<Column>> {
    if let Some(output) = pyspark_decimal_binary_arith(
        columns,
        "pyspark_multiply",
        crate::decimal_arithmetic::BinaryOp::Multiply,
        ansi_enabled,
        allow_precision_loss,
    )? {
        return Ok(Some(output));
    }
    pyspark_binary_arith(columns, "pyspark_multiply", |a, b| a * b)
}

/// PySpark-style true division with string/number coercion for Python Column operators.
/// Division by zero yields null (Spark/PySpark parity; issue #218).
pub fn apply_pyspark_divide(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    apply_pyspark_divide_with_precision_loss(
        columns,
        crate::udf_context::get_thread_ansi_enabled(),
        crate::udf_context::get_thread_decimal_allow_precision_loss(),
    )
}

#[allow(clippy::useless_conversion)] // ChunkedArray needs .into_iter() for owned iteration
pub(crate) fn apply_pyspark_divide_with_precision_loss(
    columns: &mut [Column],
    ansi_enabled: bool,
    allow_precision_loss: bool,
) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "pyspark_divide needs two columns".into(),
        ));
    }

    if let Some(output) = pyspark_decimal_binary_arith(
        columns,
        "pyspark_divide",
        crate::decimal_arithmetic::BinaryOp::Divide,
        ansi_enabled,
        allow_precision_loss,
    )? {
        return Ok(Some(output));
    }

    let name = columns[0].field().into_owned().name;
    let a_s = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_s = std::mem::take(&mut columns[1]).take_materialized_series();

    let mut ca_a = series_to_f64_pyspark(&a_s, "pyspark_divide")?;
    let mut ca_b = series_to_f64_pyspark(&b_s, "pyspark_divide")?;

    let len_a = ca_a.len();
    let len_b = ca_b.len();
    if len_a == 1 && len_b > 1 {
        let val = ca_a.get(0);
        ca_a = Float64Chunked::from_iter_options(
            name.as_str().into(),
            std::iter::repeat_n(val, len_b),
        );
    } else if len_b == 1 && len_a > 1 {
        let val = ca_b.get(0);
        ca_b = Float64Chunked::from_iter_options(
            name.as_str().into(),
            std::iter::repeat_n(val, len_a),
        );
    }

    let out = Float64Chunked::from_iter_options(
        name.as_str().into(),
        ca_a.into_iter()
            .zip(ca_b.into_iter())
            .map(|(oa, ob)| match (oa, ob) {
                (Some(a), Some(b)) => {
                    if b == 0.0 {
                        None // division by zero -> null (Spark parity, #218)
                    } else {
                        Some(a / b)
                    }
                }
                _ => None,
            }),
    )
    .into_series();

    Ok(Some(Column::new(name, out)))
}

/// PySpark-style modulo with string/number coercion for Python Column operators.
pub fn apply_pyspark_mod(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    pyspark_binary_arith(columns, "pyspark_mod", |a, b| a % b)
}

/// try_add: returns null on overflow.
pub fn apply_try_add(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "try_add needs two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_s = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_s = std::mem::take(&mut columns[1]).take_materialized_series();
    let (a_s, b_s) = broadcast_binary_inputs(a_s, b_s, "try_add")?;
    if let Some(out) = try_narrow_integral_op(&a_s, &b_s, name.as_str(), "try_add", |a, b| {
        a.checked_add(b)
    })? {
        return Ok(Some(Column::new(name, out)));
    }
    if matches!(
        (a_s.dtype(), b_s.dtype()),
        (DataType::Datetime(_, _), DataType::Duration(_))
    ) {
        return Ok(Some(Column::new(name, (&a_s + &b_s)?)));
    }
    if matches!(a_s.dtype(), DataType::Date)
        && matches!(b_s.dtype(), DataType::Int32 | DataType::Int64)
    {
        let dates = a_s.cast(&DataType::Int32)?;
        let days = b_s.cast(&DataType::Int32)?;
        let (dates, days) = binary_series_i32(&dates, &days, "try_add")?;
        let out = Int32Chunked::from_iter_options(
            name.as_str().into(),
            dates.into_iter().zip(&days).map(|(date, days)| {
                date.and_then(|date| days.and_then(|days| date.checked_add(days)))
            }),
        )
        .into_series()
        .cast(&DataType::Date)?;
        return Ok(Some(Column::new(name, out)));
    }
    if matches!(
        (a_s.dtype(), b_s.dtype()),
        (DataType::Float32, DataType::Float32)
    ) {
        let a = a_s.f32().map_err(|e| compute_err("try_add", e))?;
        let b = b_s.f32().map_err(|e| compute_err("try_add", e))?;
        let out = Float32Chunked::from_iter_options(
            name.as_str().into(),
            a.into_iter()
                .zip(b)
                .map(|(a, b)| a.zip(b).map(|(a, b)| a + b)),
        )
        .into_series();
        return Ok(Some(Column::new(name, out)));
    }
    let out = match (a_s.dtype(), b_s.dtype()) {
        (DataType::Int64, DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::Int64, DataType::Int32) => {
            let a64 = a_s.cast(&DataType::Int64)?;
            let b64 = b_s.cast(&DataType::Int64)?;
            let (ca_a, ca_b) = binary_series_i64(&a64, &b64, "try_add")?;
            Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(&ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_add(b)))),
            )
            .into_series()
        }
        (DataType::Int32, DataType::Int32) => {
            let (ca_a, ca_b) = binary_series_i32(&a_s, &b_s, "try_add")?;
            Int32Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(&ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_add(b)))),
            )
            .into_series()
        }
        _ => {
            let (ca_a, ca_b) = binary_series_f64(&a_s, &b_s, "try_add")?;
            Float64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(&ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.map(|b| a + b))),
            )
            .into_series()
        }
    };
    Ok(Some(Column::new(name, out)))
}

/// try_subtract: returns null on overflow.
pub fn apply_try_subtract(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "try_subtract needs two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_s = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_s = std::mem::take(&mut columns[1]).take_materialized_series();
    let (a_s, b_s) = broadcast_binary_inputs(a_s, b_s, "try_subtract")?;
    if let Some(out) = try_narrow_integral_op(&a_s, &b_s, name.as_str(), "try_subtract", |a, b| {
        a.checked_sub(b)
    })? {
        return Ok(Some(Column::new(name, out)));
    }
    if matches!(
        (a_s.dtype(), b_s.dtype()),
        (DataType::Datetime(_, _), DataType::Duration(_))
    ) {
        return Ok(Some(Column::new(name, (&a_s - &b_s)?)));
    }
    if matches!(a_s.dtype(), DataType::Date)
        && matches!(b_s.dtype(), DataType::Int32 | DataType::Int64)
    {
        let dates = a_s.cast(&DataType::Int32)?;
        let days = b_s.cast(&DataType::Int32)?;
        let (dates, days) = binary_series_i32(&dates, &days, "try_subtract")?;
        let out = Int32Chunked::from_iter_options(
            name.as_str().into(),
            dates.into_iter().zip(&days).map(|(date, days)| {
                date.and_then(|date| days.and_then(|days| date.checked_sub(days)))
            }),
        )
        .into_series()
        .cast(&DataType::Date)?;
        return Ok(Some(Column::new(name, out)));
    }
    if matches!(
        (a_s.dtype(), b_s.dtype()),
        (DataType::Float32, DataType::Float32)
    ) {
        let a = a_s.f32().map_err(|e| compute_err("try_subtract", e))?;
        let b = b_s.f32().map_err(|e| compute_err("try_subtract", e))?;
        let out = Float32Chunked::from_iter_options(
            name.as_str().into(),
            a.into_iter()
                .zip(b)
                .map(|(a, b)| a.zip(b).map(|(a, b)| a - b)),
        )
        .into_series();
        return Ok(Some(Column::new(name, out)));
    }
    let out = match (a_s.dtype(), b_s.dtype()) {
        (DataType::Int64, DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::Int64, DataType::Int32) => {
            let a64 = a_s.cast(&DataType::Int64)?;
            let b64 = b_s.cast(&DataType::Int64)?;
            let (ca_a, ca_b) = binary_series_i64(&a64, &b64, "try_subtract")?;
            Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(&ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_sub(b)))),
            )
            .into_series()
        }
        (DataType::Int32, DataType::Int32) => {
            let (ca_a, ca_b) = binary_series_i32(&a_s, &b_s, "try_subtract")?;
            Int32Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(&ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_sub(b)))),
            )
            .into_series()
        }
        _ => {
            let (ca_a, ca_b) = binary_series_f64(&a_s, &b_s, "try_subtract")?;
            Float64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(&ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.map(|b| a - b))),
            )
            .into_series()
        }
    };
    Ok(Some(Column::new(name, out)))
}

/// try_multiply: returns null on overflow.
pub fn apply_try_multiply(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "try_multiply needs two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_s = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_s = std::mem::take(&mut columns[1]).take_materialized_series();
    let (a_s, b_s) = broadcast_binary_inputs(a_s, b_s, "try_multiply")?;
    if let Some(out) = try_narrow_integral_op(&a_s, &b_s, name.as_str(), "try_multiply", |a, b| {
        a.checked_mul(b)
    })? {
        return Ok(Some(Column::new(name, out)));
    }
    if matches!(
        (a_s.dtype(), b_s.dtype()),
        (DataType::Float32, DataType::Float32)
    ) {
        let a = a_s.f32().map_err(|e| compute_err("try_multiply", e))?;
        let b = b_s.f32().map_err(|e| compute_err("try_multiply", e))?;
        let out = Float32Chunked::from_iter_options(
            name.as_str().into(),
            a.into_iter()
                .zip(b)
                .map(|(a, b)| a.zip(b).map(|(a, b)| a * b)),
        )
        .into_series();
        return Ok(Some(Column::new(name, out)));
    }
    let out = match (a_s.dtype(), b_s.dtype()) {
        (DataType::Int64, DataType::Int64)
        | (DataType::Int32, DataType::Int64)
        | (DataType::Int64, DataType::Int32) => {
            let a64 = a_s.cast(&DataType::Int64)?;
            let b64 = b_s.cast(&DataType::Int64)?;
            let (ca_a, ca_b) = binary_series_i64(&a64, &b64, "try_multiply")?;
            Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(&ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_mul(b)))),
            )
            .into_series()
        }
        (DataType::Int32, DataType::Int32) => {
            let (ca_a, ca_b) = binary_series_i32(&a_s, &b_s, "try_multiply")?;
            Int32Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(&ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_mul(b)))),
            )
            .into_series()
        }
        _ => {
            let (ca_a, ca_b) = binary_series_f64(&a_s, &b_s, "try_multiply")?;
            Float64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(&ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.map(|b| a * b))),
            )
            .into_series()
        }
    };
    Ok(Some(Column::new(name, out)))
}

/// ANSI divide: error if any divisor is zero (spark.sql.ansi.enabled=true).
pub fn apply_ansi_divide(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    apply_ansi_divide_with_precision_loss(
        columns,
        crate::udf_context::get_thread_decimal_allow_precision_loss(),
    )
}

pub(crate) fn apply_ansi_divide_with_precision_loss(
    columns: &mut [Column],
    allow_precision_loss: bool,
) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "ansi_divide needs two columns".into(),
        ));
    }
    if let Some(output) = pyspark_decimal_binary_arith(
        columns,
        "ansi_divide",
        crate::decimal_arithmetic::BinaryOp::Divide,
        true,
        allow_precision_loss,
    )? {
        return Ok(Some(output));
    }
    let name = columns[0].field().into_owned().name;
    let a_s = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_s = std::mem::take(&mut columns[1]).take_materialized_series();
    let (a_s, b_s) = broadcast_binary_inputs(a_s, b_s, "ansi_divide")?;
    let (ca_a, ca_b) = binary_series_f64(&a_s, &b_s, "ansi_divide")?;
    for ob in ca_b.iter().flatten() {
        if ob == 0.0 {
            return Err(PolarsError::ComputeError(
                "[DIVIDE_BY_ZERO] division by zero. Use 'try_divide' to tolerate divide-by-zero and return NULL instead. If necessary set spark.sql.ansi.enabled to \"false\" to bypass this error.".into(),
            ));
        }
    }
    let out = Float64Chunked::from_iter_options(
        name.as_str().into(),
        ca_a.into_iter()
            .zip(&ca_b)
            .map(|(oa, ob)| oa.zip(ob).map(|(a, b)| a / b)),
    )
    .into_series();
    Ok(Some(Column::new(name, out)))
}

fn ansi_checked_i64_op(
    columns: &mut [Column],
    op_name: &str,
    err_msg: &str,
    f: impl Fn(i64, i64) -> Option<i64>,
) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            format!("{op_name} needs two columns").into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_s = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_s = std::mem::take(&mut columns[1]).take_materialized_series();
    let (a_s, b_s) = broadcast_binary_inputs(a_s, b_s, op_name)?;
    let out = if matches!(
        (a_s.dtype(), b_s.dtype()),
        (DataType::Int8, DataType::Int8) | (DataType::Int16, DataType::Int16)
    ) {
        let result_type = a_s.dtype().clone();
        let (a_s, b_s) = (a_s.cast(&DataType::Int32)?, b_s.cast(&DataType::Int32)?);
        let (ca_a, ca_b) = binary_series_i32(&a_s, &b_s, op_name)?;
        let mut values = Vec::with_capacity(ca_a.len());
        for (oa, ob) in ca_a.into_iter().zip(&ca_b) {
            match (oa, ob) {
                (Some(a), Some(b)) => {
                    let value = f(a as i64, b as i64).and_then(|value| match &result_type {
                        DataType::Int8 => i8::try_from(value).ok().map(i32::from),
                        DataType::Int16 => i16::try_from(value).ok().map(i32::from),
                        _ => unreachable!("matched a narrow integral result type"),
                    });
                    match value {
                        Some(value) => values.push(Some(value)),
                        None => return Err(PolarsError::ComputeError(err_msg.to_string().into())),
                    }
                }
                _ => values.push(None),
            }
        }
        Int32Chunked::from_iter_options(name.as_str().into(), values.into_iter())
            .into_series()
            .cast(&result_type)?
    } else if matches!(
        (a_s.dtype(), b_s.dtype()),
        (DataType::Int32, DataType::Int32)
    ) {
        let (ca_a, ca_b) = binary_series_i32(&a_s, &b_s, op_name)?;
        let mut values = Vec::with_capacity(ca_a.len());
        for (oa, ob) in ca_a.into_iter().zip(&ca_b) {
            match (oa, ob) {
                (Some(a), Some(b)) => {
                    match f(a as i64, b as i64).and_then(|v| i32::try_from(v).ok()) {
                        Some(v) => values.push(Some(v)),
                        None => return Err(PolarsError::ComputeError(err_msg.to_string().into())),
                    }
                }
                _ => values.push(None),
            }
        }
        Int32Chunked::from_iter_options(name.as_str().into(), values.into_iter()).into_series()
    } else {
        let (a_s, b_s) = match (a_s.dtype(), b_s.dtype()) {
            (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => {
                (a_s.cast(&DataType::Int64)?, b_s.cast(&DataType::Int64)?)
            }
            _ => (a_s, b_s),
        };
        let (ca_a, ca_b) = binary_series_i64(&a_s, &b_s, op_name)?;
        let mut values = Vec::with_capacity(ca_a.len());
        for (oa, ob) in ca_a.into_iter().zip(&ca_b) {
            match (oa, ob) {
                (Some(a), Some(b)) => match f(a, b) {
                    Some(v) => values.push(Some(v)),
                    None => return Err(PolarsError::ComputeError(err_msg.to_string().into())),
                },
                _ => values.push(None),
            }
        }
        Int64Chunked::from_iter_options(name.as_str().into(), values.into_iter()).into_series()
    };
    Ok(Some(Column::new(name, out)))
}

/// ANSI add: error on integer overflow.
pub fn apply_ansi_add(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.iter().any(|column| column.dtype().is_float()) {
        return apply_try_add(columns);
    }
    ansi_checked_i64_op(
        columns,
        "ansi_add",
        "[ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and return NULL instead. If necessary set spark.sql.ansi.enabled to \"false\" to bypass this error.",
        |a, b| a.checked_add(b),
    )
}

/// ANSI subtract: error on integer overflow.
pub fn apply_ansi_subtract(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.iter().any(|column| column.dtype().is_float()) {
        return apply_try_subtract(columns);
    }
    ansi_checked_i64_op(
        columns,
        "ansi_subtract",
        "[ARITHMETIC_OVERFLOW] integer overflow. Use 'try_subtract' to tolerate overflow and return NULL instead. If necessary set spark.sql.ansi.enabled to \"false\" to bypass this error.",
        |a, b| a.checked_sub(b),
    )
}

/// ANSI multiply: error on integer overflow.
pub fn apply_ansi_multiply(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.iter().any(|column| column.dtype().is_float()) {
        return apply_try_multiply(columns);
    }
    ansi_checked_i64_op(
        columns,
        "ansi_multiply",
        "[ARITHMETIC_OVERFLOW] integer overflow. Use 'try_multiply' to tolerate overflow and return NULL instead. If necessary set spark.sql.ansi.enabled to \"false\" to bypass this error.",
        |a, b| a.checked_mul(b),
    )
}

/// Strip PySpark/Java SimpleDateFormat quoted literals: 'T' -> T, '' -> '. So chrono gets unquoted pattern.
pub(super) fn unquote_simple_date_format(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                out.push('\'');
            } else {
                for q in chars.by_ref() {
                    if q == '\'' {
                        break;
                    }
                    out.push(q);
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests_narrow_integer_arithmetic {
    use super::*;

    fn narrow_columns(left: i16, right: i16) -> Vec<Column> {
        vec![
            Series::new("left".into(), [left]).into_column(),
            Series::new("right".into(), [right]).into_column(),
        ]
    }

    #[test]
    fn short_addition_preserves_the_result_schema_and_values() {
        let (left, right) =
            crate::column::expr_ir_arithmetic_types(&DataType::Int16, &DataType::Int16).unwrap();
        let fields = [
            Field::new("left".into(), left),
            Field::new("right".into(), right),
        ];
        assert_eq!(
            crate::ansi::arithmetic_field(&fields).unwrap().dtype(),
            &DataType::Int16
        );

        let mut columns = narrow_columns(12, 30);
        let result = apply_ansi_add(&mut columns).unwrap().unwrap();
        assert_eq!(result.dtype(), &DataType::Int16);
        assert_eq!(result.i16().unwrap().get(0), Some(42));
    }

    #[test]
    fn short_addition_checks_ansi_overflow_and_nulls_try_overflow() {
        let mut ansi_columns = narrow_columns(i16::MAX, 1);
        let error = apply_ansi_add(&mut ansi_columns).unwrap_err().to_string();
        assert!(error.contains("ARITHMETIC_OVERFLOW"), "{error}");

        let mut try_columns = narrow_columns(i16::MAX, 1);
        let result = apply_try_add(&mut try_columns).unwrap().unwrap();
        assert_eq!(result.dtype(), &DataType::Int16);
        assert_eq!(result.i16().unwrap().get(0), None);
    }
}
