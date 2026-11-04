//! Casts UDF implementations.

use super::*;

/// Apply string-to-int cast. Handles string columns: empty/invalid -> null (Spark parity); passes through int columns; others error (strict) or null.
pub fn apply_string_to_int(
    column: Column,
    strict: bool,
    target: DataType,
    try_cast: bool,
) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out: Series = match series.dtype() {
        DataType::Null => {
            // PySpark parity (#1028): lit(None).cast(\"int\"/\"long\") yields a typed null column.
            Series::new_null(name.clone(), series.len()).cast(&target)?
        }
        DataType::String => {
            let ca = series.str().map_err(|e| compute_err("string to int", e))?;
            let mut results: Vec<Option<i64>> = Vec::with_capacity(ca.len());
            for opt_s in ca.into_iter() {
                let v = opt_s.and_then(parse_str_to_int);
                if strict {
                    if let Some(s) = opt_s {
                        if v.is_none_or(|n| target == DataType::Int16 && i16::try_from(n).is_err())
                        {
                            return Err(PolarsError::ComputeError(
                                format!(
                                    "conversion from `str` to `{}` failed in column '{}' for value \"{s}\"",
                                    target,
                                    name.as_str()
                                )
                                .into(),
                            ));
                        }
                    }
                }
                results.push(v);
            }
            match &target {
                DataType::Int16 => {
                    let vals: Vec<Option<i16>> = results
                        .into_iter()
                        .map(|o| o.and_then(|n| i16::try_from(n).ok()))
                        .collect();
                    Int16Chunked::from_iter_options(name.as_str().into(), vals.into_iter())
                        .into_series()
                }
                DataType::Int32 => {
                    let vals: Vec<Option<i32>> = results
                        .into_iter()
                        .map(|o| {
                            o.and_then(|n| {
                                (n >= i64::from(i32::MIN) && n <= i64::from(i32::MAX))
                                    .then_some(n as i32)
                            })
                        })
                        .collect();
                    let chunked =
                        Int32Chunked::from_iter_options(name.as_str().into(), vals.into_iter());
                    chunked.into_series()
                }
                DataType::Int64 => {
                    let chunked =
                        Int64Chunked::from_iter_options(name.as_str().into(), results.into_iter());
                    chunked.into_series()
                }
                _ => unreachable!("target is Int16, Int32, or Int64"),
            }
        }
        DataType::Int16 | DataType::Int32 | DataType::Int64 => series.cast(&target)?,
        DataType::Decimal(_, _) => cast_decimal_to_integer(&series, &target, strict, try_cast)?,
        DataType::Float32 | DataType::Float64 => {
            // #649: f64/f32 -> i32/i64 with truncation; NaN/inf -> null (try_cast) or error (cast).
            let casted = series.cast(&DataType::Float64)?;
            let ca = casted.f64()?;
            let vals_f64: Vec<Option<f64>> = ca.into_iter().collect();
            let mut results: Vec<Option<i64>> = Vec::with_capacity(vals_f64.len());
            for opt_v in vals_f64 {
                let v: Option<i64> = match opt_v {
                    None => None,
                    Some(f) => {
                        if f.is_nan() || f.is_infinite() {
                            if strict {
                                return Err(PolarsError::ComputeError(
                                    format!(
                                        "casting from {} to {} failed for value {} (NaN/inf)",
                                        series.dtype(),
                                        target,
                                        f
                                    )
                                    .into(),
                                ));
                            }
                            None
                        } else {
                            let truncated = f.trunc();
                            if target == DataType::Int16
                                && (truncated < f64::from(i16::MIN)
                                    || truncated > f64::from(i16::MAX))
                            {
                                if strict {
                                    return Err(PolarsError::ComputeError(
                                        format!(
                                            "casting from {} to {} overflow for value {}",
                                            series.dtype(),
                                            target,
                                            f
                                        )
                                        .into(),
                                    ));
                                }
                                results.push(None);
                                continue;
                            }
                            let n = truncated as i64;
                            if target == DataType::Int32
                                && (n < i64::from(i32::MIN) || n > i64::from(i32::MAX))
                            {
                                if strict {
                                    return Err(PolarsError::ComputeError(
                                        format!(
                                            "casting from {} to {} overflow for value {}",
                                            series.dtype(),
                                            target,
                                            f
                                        )
                                        .into(),
                                    ));
                                }
                                None
                            } else {
                                Some(n)
                            }
                        }
                    }
                };
                results.push(v);
            }
            let out_series = match &target {
                DataType::Int16 => {
                    let vals: Vec<Option<i16>> = results
                        .into_iter()
                        .map(|o| o.and_then(|n| i16::try_from(n).ok()))
                        .collect();
                    Int16Chunked::from_iter_options(name.as_str().into(), vals.into_iter())
                        .into_series()
                }
                DataType::Int32 => {
                    let vals: Vec<Option<i32>> = results
                        .into_iter()
                        .map(|o| {
                            o.and_then(|n| {
                                (n >= i64::from(i32::MIN) && n <= i64::from(i32::MAX))
                                    .then_some(n as i32)
                            })
                        })
                        .collect();
                    Int32Chunked::from_iter_options(name.as_str().into(), vals.into_iter())
                        .into_series()
                }
                DataType::Int64 => {
                    Int64Chunked::from_iter_options(name.as_str().into(), results.into_iter())
                        .into_series()
                }
                _ => unreachable!("target is Int16, Int32, or Int64"),
            };
            out_series
        }
        _ => {
            // For other dtypes, when `strict` is true, keep existing error behavior.
            // When `strict` is false (used by the int/long cast path), fall back to a
            // regular cast so engine-generated numeric expressions (e.g. row_number(),
            // rank(), dense_rank()) can be cast to int/long instead of producing all-null.
            if strict {
                return Err(PolarsError::ComputeError(
                    format!(
                        "casting from {} to {} not supported",
                        series.dtype(),
                        target
                    )
                    .into(),
                ));
            }
            series.cast(&target)?
        }
    };
    Ok(Some(Column::new(name, out)))
}

/// Parse string to boolean (PySpark cast string->boolean). "true"/"false"/"1"/"0" case-insensitive.
/// Empty or whitespace-only string -> false (Python bool("") parity; #989).
/// For try_cast: invalid strings -> null. For cast: invalid strings -> error.
fn parse_str_to_bool(s: &str, strict: bool) -> Option<bool> {
    let lower = s.trim().to_lowercase();
    if lower.is_empty() {
        return Some(false);
    }
    match lower.as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ if strict => None, // will error
        _ => None,
    }
}

/// Logical NOT for boolean columns only (PySpark: ~ on Column is boolean NOT).
/// Fails with a clear error for numeric/integer columns; use F.expr("~x") for bitwise NOT (#405, #1236).
pub fn apply_logical_not_boolean_only(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    if series.dtype() != &DataType::Boolean {
        return Err(PolarsError::ComputeError(
            "PySpark: '~' on Column is boolean NOT; cannot apply to numeric/integer columns. Use F.expr(\"~x\") for bitwise NOT.".into(),
        ));
    }
    let ca = series.bool().map_err(|e| compute_err("logical_not", e))?;
    let not_ca = ca.not();
    Ok(Some(Column::new(name, not_ca.into_series())))
}

/// Apply string-to-boolean cast. Handles string columns; passes through boolean; numeric types
/// (0/0.0 -> false, non-zero -> true for PySpark parity #399); null for others (try_cast) or error (cast).
pub fn apply_string_to_boolean(column: Column, strict: bool) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out: BooleanChunked = match series.dtype() {
        DataType::String => {
            let ca = series
                .str()
                .map_err(|e| compute_err("string to boolean", e))?;
            let mut results = Vec::with_capacity(ca.len());
            for opt_s in ca.into_iter() {
                let v = match opt_s {
                    None => None,
                    Some(s) => {
                        let parsed = parse_str_to_bool(s, strict);
                        if strict && parsed.is_none() {
                            return Err(PolarsError::ComputeError(
                                format!("casting from string to boolean failed for value '{s}'")
                                    .into(),
                            ));
                        }
                        parsed
                    }
                };
                results.push(v);
            }
            BooleanChunked::from_iter_options(name.as_str().into(), results.into_iter())
        }
        DataType::Boolean => {
            let ca = series.bool().map_err(|e| compute_err("boolean", e))?;
            BooleanChunked::from_iter_options(name.as_str().into(), ca.into_iter())
        }
        DataType::Int8 => {
            let ca = series.i8().map_err(|e| compute_err("i8", e))?;
            BooleanChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|o| o.map(|v| v != 0)),
            )
        }
        DataType::Int16 => {
            let ca = series.i16().map_err(|e| compute_err("i16", e))?;
            BooleanChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|o| o.map(|v| v != 0)),
            )
        }
        DataType::Int32 => {
            let ca = series.i32().map_err(|e| compute_err("i32", e))?;
            BooleanChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|o| o.map(|v| v != 0)),
            )
        }
        DataType::Int64 => {
            let ca = series.i64().map_err(|e| compute_err("i64", e))?;
            BooleanChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|o| o.map(|v| v != 0)),
            )
        }
        DataType::UInt8 => {
            let ca = series.u8().map_err(|e| compute_err("u8", e))?;
            BooleanChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|o| o.map(|v| v != 0)),
            )
        }
        DataType::UInt16 => {
            let ca = series.u16().map_err(|e| compute_err("u16", e))?;
            BooleanChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|o| o.map(|v| v != 0)),
            )
        }
        DataType::UInt32 => {
            let ca = series.u32().map_err(|e| compute_err("u32", e))?;
            BooleanChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|o| o.map(|v| v != 0)),
            )
        }
        DataType::UInt64 => {
            let ca = series.u64().map_err(|e| compute_err("u64", e))?;
            BooleanChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|o| o.map(|v| v != 0)),
            )
        }
        DataType::Float32 => {
            let ca = series.f32().map_err(|e| compute_err("f32", e))?;
            BooleanChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|o| o.map(|v| v != 0.0)),
            )
        }
        DataType::Float64 => {
            let ca = series.f64().map_err(|e| compute_err("f64", e))?;
            BooleanChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|o| o.map(|v| v != 0.0)),
            )
        }
        DataType::Null => {
            // PySpark parity (#1109): lit(None).cast(BooleanType()) yields typed null column.
            BooleanChunked::from_iter_options(name.as_str().into(), (0..series.len()).map(|_| None))
        }
        _ => {
            if strict {
                return Err(PolarsError::ComputeError(
                    format!("casting from {} to boolean not supported", series.dtype()).into(),
                ));
            }
            BooleanChunked::from_iter_options(name.as_str().into(), (0..series.len()).map(|_| None))
        }
    };
    Ok(Some(Column::new(name, out.into_series())))
}

/// Parse string to f64 (Spark-style to_number/try_to_number for doubles).
pub(super) fn parse_str_to_double(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok()
}

/// Apply string-to-double cast. Handles string columns: empty/invalid -> null (Spark parity);
/// passes through numeric columns; others error (strict) or null.
pub fn apply_string_to_double(column: Column, strict: bool) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out: Series = match series.dtype() {
        DataType::String => {
            let ca = series
                .str()
                .map_err(|e| compute_err("string to double", e))?;
            let mut results: Vec<Option<f64>> = Vec::with_capacity(ca.len());
            for opt_s in ca.into_iter() {
                let v = opt_s.and_then(parse_str_to_double);
                if strict {
                    if let Some(s) = opt_s {
                        if v.is_none() {
                            return Err(PolarsError::ComputeError(
                                format!(
                                    "conversion from `str` to `double` failed in column '{}' for value \"{s}\"",
                                    name.as_str()
                                )
                                .into(),
                            ));
                        }
                    }
                }
                results.push(v);
            }
            Float64Chunked::from_iter_options(name.as_str().into(), results.into_iter())
                .into_series()
        }
        DataType::Decimal(_, _) => {
            series_to_f64_pyspark(&series, "string to double")?.into_series()
        }
        DataType::Float32 | DataType::Float64 => series.cast(&DataType::Float64)?,
        DataType::Int32 | DataType::Int64 | DataType::UInt32 | DataType::UInt64 => {
            series.cast(&DataType::Float64)?
        }
        DataType::Null => {
            // PySpark parity (#260): lit(None).cast("double") yields typed null column.
            Float64Chunked::from_iter_options(
                name.as_str().into(),
                (0..series.len()).map(|_| None::<f64>),
            )
            .into_series()
        }
        _ => {
            if strict {
                return Err(PolarsError::ComputeError(
                    format!("casting from {} to double not supported", series.dtype()).into(),
                ));
            }
            Float64Chunked::from_iter_options(
                name.as_str().into(),
                (0..series.len()).map(|_| None::<f64>),
            )
            .into_series()
        }
    };
    Ok(Some(Column::new(name, out)))
}

/// Format one list row as PySpark's array-to-string cast (`[x, y]`).
fn format_list_row_as_pyspark_string(inner: Series) -> PolarsResult<String> {
    let inner = inner.cast(&DataType::String)?;
    let ca = inner
        .str()
        .map_err(|e| compute_err("cast list element to string", e))?;
    let parts: Vec<&str> = ca.into_iter().flatten().collect();
    Ok(format!("[{}]", parts.join(", ")))
}

/// Cast column to string (PySpark parity). List columns stringify as `[elem1, elem2]` (#1594).
pub fn apply_cast_to_string(column: Column, strict: bool) -> PolarsResult<Option<Column>> {
    apply_cast_to_string_with_timezone(column, strict, None, false)
}

pub(crate) fn apply_cast_to_string_with_timezone(
    column: Column,
    strict: bool,
    session_timezone: Option<&str>,
    timestamp_ntz: bool,
) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::builder::StringChunkedBuilder;

    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out: Series = match series.dtype() {
        DataType::List(_) => {
            let list = series
                .list()
                .map_err(|e| compute_err("cast to string", e))?;
            let mut builder = StringChunkedBuilder::new(name.as_str().into(), list.len());
            for opt_inner in list.into_iter() {
                match opt_inner {
                    None => builder.append_null(),
                    Some(inner) => builder.append_value(format_list_row_as_pyspark_string(inner)?),
                }
            }
            builder.finish().into_series()
        }
        DataType::Datetime(unit, timezone) => {
            let values = series.cast(&DataType::Int64)?;
            let values = values.i64()?;
            let timezone = if let Some(timezone) = timezone.as_ref() {
                Some(parse_cast_timezone(&timezone.to_string()).ok_or_else(|| {
                    PolarsError::ComputeError(
                        format!("invalid timestamp timezone: {timezone}").into(),
                    )
                })?)
            } else if !timestamp_ntz {
                session_timezone.and_then(parse_cast_timezone)
            } else {
                None
            };
            let mut builder = StringChunkedBuilder::new(name.as_str().into(), values.len());
            for value in values.into_iter() {
                let micros = value.and_then(|value| match unit {
                    TimeUnit::Nanoseconds => value.checked_div(1_000),
                    TimeUnit::Microseconds => Some(value),
                    TimeUnit::Milliseconds => value.checked_mul(1_000),
                });
                let formatted = micros.and_then(|micros| {
                    let local_micros = timezone
                        .map(|timezone| utc_micros_to_local_naive_micros(micros, timezone))
                        .unwrap_or(Some(micros))?;
                    chrono::DateTime::from_timestamp_micros(local_micros).map(|local| {
                        local
                            .naive_utc()
                            .format("%Y-%m-%d %H:%M:%S%.6f")
                            .to_string()
                    })
                });
                if let Some(formatted) = formatted {
                    builder.append_value(formatted);
                } else {
                    builder.append_null();
                }
            }
            builder.finish().into_series()
        }
        _ => {
            if strict {
                series.strict_cast(&DataType::String)?
            } else {
                series.cast(&DataType::String)?
            }
        }
    };
    Ok(Some(Column::new(name, out)))
}

/// Apply string-to-date cast. Handles string columns (accepts date and datetime strings, Spark parity); passes through date; casts datetime to date; others error (cast) or null (try_cast).
pub fn apply_string_to_date(column: Column, strict: bool) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let epoch = robin_sparkless_core::date_utils::epoch_naive_date();
    let out: Series = match series.dtype() {
        DataType::String => {
            let ca = series.str().map_err(|e| compute_err("string to date", e))?;
            let mut results = Vec::with_capacity(ca.len());
            for opt_s in ca.into_iter() {
                let v =
                    opt_s.and_then(|s| parse_str_to_date(s).map(|d| (d - epoch).num_days() as i32));
                if strict {
                    if let Some(s) = opt_s {
                        if v.is_none() {
                            return Err(PolarsError::ComputeError(
                                format!(
                                    "conversion from `str` to `date` failed in column '{}' for value \"{s}\"",
                                    name.as_str()
                                )
                                .into(),
                            ));
                        }
                    }
                }
                results.push(v);
            }
            let chunked =
                Int32Chunked::from_iter_options(name.as_str().into(), results.into_iter());
            chunked.into_series().cast(&DataType::Date)?
        }
        DataType::Date => series,
        DataType::Datetime(_, _) => series.cast(&DataType::Date)?,
        DataType::Null => {
            // PySpark parity (#260): lit(None).cast("date") yields typed null column.
            let days = Int32Chunked::from_iter_options(
                name.as_str().into(),
                (0..series.len()).map(|_| None::<i32>),
            );
            days.into_series().cast(&DataType::Date)?
        }
        _ => {
            if strict {
                return Err(PolarsError::ComputeError(
                    format!("casting from {} to date not supported", series.dtype()).into(),
                ));
            }
            let days = Int32Chunked::from_iter_options(
                name.as_str().into(),
                (0..series.len()).map(|_| None::<i32>),
            );
            days.into_series().cast(&DataType::Date)?
        }
    };
    Ok(Some(Column::new(name, out)))
}

/// Apply string-to-datetime cast (#982). Handles string (parse common formats); passes through Datetime; Date->Datetime at midnight; others error (cast) or null (try_cast).
pub fn apply_string_to_datetime(column: Column, strict: bool) -> PolarsResult<Option<Column>> {
    apply_string_to_datetime_with_timezone(column, strict, None)
}

pub(crate) fn apply_string_to_datetime_with_timezone(
    column: Column,
    strict: bool,
    session_timezone: Option<&str>,
) -> PolarsResult<Option<Column>> {
    if let Some(timezone) = session_timezone {
        return apply_cast_to_timestamp(column, strict, timezone, false, false);
    }
    apply_cast_to_timestamp_legacy(column, strict)
}

pub(crate) fn apply_cast_to_timestamp(
    column: Column,
    strict: bool,
    session_timezone: &str,
    timestamp_ntz: bool,
    source_timestamp_ntz: bool,
) -> PolarsResult<Option<Column>> {
    use polars::datatypes::TimeUnit;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let output_timezone = if timestamp_ntz || session_timezone.parse::<Tz>().is_err() {
        None
    } else {
        polars::prelude::TimeZone::opt_try_new(Some(session_timezone.to_owned()))?
    };
    let dtype_out = DataType::Datetime(TimeUnit::Microseconds, output_timezone);
    let out: Series = match series.dtype() {
        DataType::String => {
            let ca = series
                .str()
                .map_err(|e| compute_err("string to datetime", e))?;
            let mut results = Vec::with_capacity(ca.len());
            for opt_s in ca.into_iter() {
                let v = opt_s.and_then(|value| {
                    if timestamp_ntz {
                        parse_str_to_datetime_micros(value)
                    } else {
                        crate::temporal::parse_sql_timestamp_micros(value, session_timezone).ok()
                    }
                });
                if strict {
                    if let Some(s) = opt_s {
                        if v.is_none() {
                            return Err(PolarsError::ComputeError(
                                format!(
                                    "conversion from `str` to `datetime` failed in column '{}' for value \"{s}\"",
                                    name.as_str()
                                )
                                .into(),
                            ));
                        }
                    }
                }
                results.push(v);
            }
            let chunked =
                Int64Chunked::from_iter_options(name.as_str().into(), results.into_iter());
            chunked.into_series().cast(&dtype_out)?
        }
        DataType::Datetime(source_unit, source_timezone) => {
            let physical = series.cast(&DataType::Int64)?;
            let values = physical.i64()?;
            let needs_timezone_conversion = if timestamp_ntz {
                !source_timestamp_ntz || source_timezone.is_some()
            } else {
                source_timestamp_ntz && source_timezone.is_none()
            };
            let timezone = if needs_timezone_conversion {
                Some(parse_cast_timezone(session_timezone).ok_or_else(|| {
                    PolarsError::ComputeError(
                        format!("invalid session timezone: {session_timezone}").into(),
                    )
                })?)
            } else {
                None
            };
            let converted = Int64Chunked::from_iter_options(
                name.as_str().into(),
                values.into_iter().map(|value| {
                    value.and_then(|value| {
                        let micros = match source_unit {
                            TimeUnit::Nanoseconds => value / 1_000,
                            TimeUnit::Microseconds => value,
                            TimeUnit::Milliseconds => value.checked_mul(1_000)?,
                        };
                        match (timestamp_ntz, source_timestamp_ntz, timezone) {
                            (true, false, Some(timezone)) => {
                                utc_micros_to_local_naive_micros(micros, timezone)
                            }
                            (false, true, Some(timezone)) => {
                                local_naive_micros_to_utc(micros, timezone)
                            }
                            _ => Some(micros),
                        }
                    })
                }),
            );
            converted.into_series().cast(&dtype_out)?
        }
        DataType::Date if timestamp_ntz => series.cast(&dtype_out)?,
        DataType::Date => {
            let dates = series.cast(&DataType::String)?;
            let values = dates.str()?;
            let micros = Int64Chunked::from_iter_options(
                name.as_str().into(),
                values.into_iter().map(|value| {
                    value.and_then(|value| {
                        crate::temporal::parse_sql_timestamp_micros(value, session_timezone).ok()
                    })
                }),
            );
            micros.into_series().cast(&dtype_out)?
        }
        DataType::Null => {
            let micros = Int64Chunked::from_iter_options(
                name.as_str().into(),
                (0..series.len()).map(|_| None::<i64>),
            );
            micros.into_series().cast(&dtype_out)?
        }
        _ => {
            if strict {
                return Err(PolarsError::ComputeError(
                    format!("casting from {} to datetime not supported", series.dtype()).into(),
                ));
            }
            let micros = Int64Chunked::from_iter_options(
                name.as_str().into(),
                (0..series.len()).map(|_| None::<i64>),
            );
            micros.into_series().cast(&dtype_out)?
        }
    };
    Ok(Some(Column::new(name, out)))
}

#[derive(Clone, Copy)]
enum CastTimezone {
    Region(Tz),
    Fixed(chrono::FixedOffset),
}

fn parse_cast_timezone(value: &str) -> Option<CastTimezone> {
    value
        .parse::<Tz>()
        .map(CastTimezone::Region)
        .ok()
        .or_else(|| crate::temporal::parse_sql_fixed_offset(value).map(CastTimezone::Fixed))
}

fn local_naive_micros_to_utc(micros: i64, timezone: CastTimezone) -> Option<i64> {
    let naive = chrono::DateTime::from_timestamp_micros(micros)?.naive_utc();
    match timezone {
        CastTimezone::Region(timezone) => {
            crate::temporal::resolve_local_timestamp(timezone, &naive)
        }
        CastTimezone::Fixed(timezone) => Some(
            timezone
                .from_local_datetime(&naive)
                .single()?
                .timestamp_micros(),
        ),
    }
}

fn utc_micros_to_local_naive_micros(micros: i64, timezone: CastTimezone) -> Option<i64> {
    let utc = chrono::DateTime::from_timestamp_micros(micros)?;
    let local = match timezone {
        CastTimezone::Region(timezone) => utc.with_timezone(&timezone).naive_local(),
        CastTimezone::Fixed(timezone) => utc.with_timezone(&timezone).naive_local(),
    };
    Some(local.and_utc().timestamp_micros())
}

fn apply_cast_to_timestamp_legacy(column: Column, strict: bool) -> PolarsResult<Option<Column>> {
    use polars::datatypes::TimeUnit;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let dtype_out = DataType::Datetime(TimeUnit::Microseconds, None);
    let out = match series.dtype() {
        DataType::String => {
            let ca = series
                .str()
                .map_err(|e| compute_err("string to datetime", e))?;
            let mut results = Vec::with_capacity(ca.len());
            for opt_s in ca.into_iter() {
                let value = opt_s.and_then(parse_str_to_datetime_micros);
                if strict && opt_s.is_some() && value.is_none() {
                    return Err(PolarsError::ComputeError(
                        format!(
                            "conversion from `str` to `datetime` failed in column '{}'",
                            name.as_str()
                        )
                        .into(),
                    ));
                }
                results.push(value);
            }
            Int64Chunked::from_iter_options(name.as_str().into(), results.into_iter())
                .into_series()
                .cast(&dtype_out)?
        }
        DataType::Datetime(_, _) | DataType::Date => series.cast(&dtype_out)?,
        DataType::Null => Int64Chunked::full_null(name.as_str().into(), series.len())
            .into_series()
            .cast(&dtype_out)?,
        _ if !strict => Int64Chunked::full_null(name.as_str().into(), series.len())
            .into_series()
            .cast(&dtype_out)?,
        _ => {
            return Err(PolarsError::ComputeError(
                format!("casting from {} to datetime not supported", series.dtype()).into(),
            ));
        }
    };
    Ok(Some(Column::new(name, out)))
}

/// Apply string-to-date with optional format (PySpark to_date(col, format)). When format is None uses default parsing; when Some parses with given format.
pub fn apply_string_to_date_format(
    column: Column,
    format: Option<&str>,
    strict: bool,
) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let epoch = robin_sparkless_core::date_utils::epoch_naive_date();
    let out: Series = match series.dtype() {
        DataType::String => {
            let ca = series.str().map_err(|e| compute_err("string to date", e))?;
            let fmt = format.map(pyspark_format_to_chrono);
            let mut results = Vec::with_capacity(ca.len());
            for opt_s in ca.into_iter() {
                let v = opt_s.and_then(|s| {
                    let parsed = if let Some(ref chrono_fmt) = fmt {
                        chrono::NaiveDate::parse_from_str(s.trim(), chrono_fmt).ok()
                    } else {
                        parse_str_to_date(s)
                    };
                    parsed.map(|d| (d.signed_duration_since(epoch).num_days()) as i32)
                });
                results.push(v);
            }
            let chunked =
                Int32Chunked::from_iter_options(name.as_str().into(), results.into_iter());
            chunked.into_series().cast(&DataType::Date)?
        }
        DataType::Date => series,
        DataType::Datetime(_, _) => series.cast(&DataType::Date)?,
        DataType::Null => {
            let days = Int32Chunked::from_iter_options(
                name.as_str().into(),
                (0..series.len()).map(|_| None::<i32>),
            );
            days.into_series().cast(&DataType::Date)?
        }
        _ => {
            if strict {
                return Err(PolarsError::ComputeError(
                    "to_date requires StringType, TimestampType, or DateType input".into(),
                ));
            }
            let days = Int32Chunked::from_iter_options(
                name.as_str().into(),
                (0..series.len()).map(|_| None::<i32>),
            );
            days.into_series().cast(&DataType::Date)?
        }
    };
    Ok(Some(Column::new(name, out)))
}

// --- to_timestamp_ltz / to_timestamp_ntz ---

/// Parse string as timestamp in local timezone, return UTC micros (PySpark to_timestamp_ltz).
pub fn apply_to_timestamp_ltz_format(
    column: Column,
    format: Option<&str>,
    strict: bool,
) -> PolarsResult<Option<Column>> {
    use chrono::offset::TimeZone;
    use chrono::{Local, NaiveDateTime, Utc};
    use polars::datatypes::TimeUnit;
    let chrono_fmt = format
        .map(pyspark_format_to_chrono)
        .unwrap_or_else(|| "%Y-%m-%d %H:%M:%S".to_string());
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| compute_err("to_timestamp_ltz", e))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                NaiveDateTime::parse_from_str(s, &chrono_fmt)
                    .ok()
                    .and_then(|ndt| {
                        Local
                            .from_local_datetime(&ndt)
                            .single()
                            .map(|dt| dt.with_timezone(&Utc).timestamp_micros())
                    })
            })
        }),
    );
    let out_series = out.into_series().cast(&DataType::Datetime(
        TimeUnit::Microseconds,
        Some(polars::prelude::TimeZone::UTC),
    ))?;
    let _ = strict;
    Ok(Some(Column::new(name, out_series)))
}

/// Parse string as timestamp without timezone (PySpark to_timestamp_ntz). Returns Datetime(_, None).
pub fn apply_to_timestamp_ntz_format(
    column: Column,
    format: Option<&str>,
    strict: bool,
) -> PolarsResult<Option<Column>> {
    use chrono::NaiveDateTime;
    use polars::datatypes::TimeUnit;
    let chrono_fmt = format
        .map(pyspark_format_to_chrono)
        .unwrap_or_else(|| "%Y-%m-%d %H:%M:%S".to_string());
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| compute_err("to_timestamp_ntz", e))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                NaiveDateTime::parse_from_str(s, &chrono_fmt)
                    .ok()
                    .map(|ndt| ndt.and_utc().timestamp_micros())
            })
        }),
    );
    let out_series = out
        .into_series()
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
    let _ = strict;
    Ok(Some(Column::new(name, out_series)))
}
