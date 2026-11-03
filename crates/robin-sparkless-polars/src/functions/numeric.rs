//! Numeric expression builders.

use super::imports::*;
use super::types::parse_type_name;

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

pub(super) fn timestamp_cast_expr(input: Expr, strict: bool, timestamp_ntz: bool) -> Expr {
    // Bind the session configuration while analyzing the plan. The closure may
    // run on a Polars worker without the originating thread's UDF context.
    let timezone = crate::get_thread_session_time_zone();
    let ansi_strict = strict && crate::udf_context::get_thread_ansi_enabled();
    let source_timestamp_ntz = is_timestamp_ntz_expression(&input);
    let output_timezone =
        (!timestamp_ntz && timezone.parse::<chrono_tz::Tz>().is_ok()).then(|| timezone.clone());
    input.map_with_fmt_str(
        move |column| {
            crate::column::expect_col(crate::udfs::apply_cast_to_timestamp(
                column,
                ansi_strict,
                &timezone,
                timestamp_ntz,
                source_timestamp_ntz,
            ))
        },
        move |_schema, field| {
            let timezone = output_timezone
                .as_deref()
                .map(|timezone| TimeZone::opt_try_new(Some(timezone.to_owned())))
                .transpose()?
                .flatten();
            Ok(Field::new(
                field.name().clone(),
                DataType::Datetime(TimeUnit::Microseconds, timezone),
            ))
        },
        if timestamp_ntz && strict {
            "cast_timestamp_ntz"
        } else if timestamp_ntz {
            "try_cast_timestamp_ntz"
        } else if strict {
            "cast_timestamp"
        } else {
            "try_cast_timestamp"
        },
    )
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
        let timestamp_ntz = type_name.trim().eq_ignore_ascii_case("timestamp_ntz");
        let expr = timestamp_cast_expr(column.expr().clone(), strict, timestamp_ntz);
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
        let is_timestamp_cast = is_timestamp_cast_expression(column.expr());
        let is_plain_column = matches!(column.expr(), Expr::Column(_));
        if !is_timestamp_cast && !is_plain_column {
            let expr = column.expr().clone().strict_cast(dtype);
            return Ok(Column::from_expr(expr, Some(base_name)));
        }
        let timezone = is_timestamp_cast.then(crate::get_thread_session_time_zone);
        let timestamp_ntz = is_timestamp_cast && is_timestamp_ntz_expression(column.expr());
        let expr = column.expr().clone().map(
            move |col| {
                crate::column::expect_col(crate::udfs::apply_cast_to_string_with_timezone(
                    col,
                    ansi_strict,
                    timezone.as_deref(),
                    timestamp_ntz,
                ))
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

fn is_timestamp_cast_expression(expr: &Expr) -> bool {
    match expr {
        Expr::AnonymousFunction { fmt_str, .. } => matches!(
            fmt_str.as_str(),
            "cast_timestamp"
                | "try_cast_timestamp"
                | "cast_timestamp_ntz"
                | "try_cast_timestamp_ntz"
        ),
        Expr::Alias(inner, _) | Expr::Cast { expr: inner, .. } => {
            is_timestamp_cast_expression(inner)
        }
        _ => false,
    }
}

fn is_timestamp_ntz_expression(expr: &Expr) -> bool {
    match expr {
        Expr::AnonymousFunction { fmt_str, .. } => matches!(
            fmt_str.as_str(),
            "cast_timestamp_ntz" | "try_cast_timestamp_ntz"
        ),
        Expr::Alias(inner, _) | Expr::Cast { expr: inner, .. } => {
            is_timestamp_ntz_expression(inner)
        }
        _ => false,
    }
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
    use polars::prelude::{DataType, Field, TimeUnit, TimeZone};
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
                DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
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
    use polars::prelude::{DataType, Field, TimeUnit, TimeZone};
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
                    DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
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
                DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
            ))
        },
    );
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Parse as timestamp in local timezone, return UTC (PySpark to_timestamp_ltz).
pub fn to_timestamp_ltz(column: &Column, format: Option<&str>) -> Result<Column, String> {
    use polars::prelude::{DataType, Field, TimeUnit, TimeZone};
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
                        DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
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
        None => crate::cast(column, "timestamp_ntz"),
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
