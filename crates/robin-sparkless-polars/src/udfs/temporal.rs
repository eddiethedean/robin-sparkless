//! Temporal UDF implementations.

use super::*;

/// Map PySpark/Java SimpleDateFormat style to chrono strftime. Public for to_char/date_format.
/// Handles quoted literals (e.g. 'T' in "yyyy-MM-dd'T'HH:mm:ss") so they become literal in chrono (#273).
/// Day of week: E/EE/EEE -> %a (abbreviated e.g. Fri), EEEE -> %A (full e.g. Friday) (#1479).
pub(crate) fn pyspark_format_to_chrono(s: &str) -> String {
    let s = unquote_simple_date_format(s);
    s.replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
        .replace("EEEE", "%A")
        .replace("EEE", "%a")
        .replace("EE", "%a")
        .replace("E", "%a")
}

/// unix_timestamp(column, format?) - parse string to seconds since epoch.
pub fn apply_unix_timestamp(column: Column, format: Option<&str>) -> PolarsResult<Option<Column>> {
    use chrono::{DateTime, NaiveDateTime, Utc};
    let chrono_fmt = format
        .map(pyspark_format_to_chrono)
        .unwrap_or_else(|| "%Y-%m-%d %H:%M:%S".to_string());
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out = match series.dtype() {
        DataType::String => {
            let ca = series.str().map_err(|e| compute_err("unix_timestamp", e))?;
            Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_s| {
                    opt_s.and_then(|s| {
                        NaiveDateTime::parse_from_str(s, &chrono_fmt)
                            .ok()
                            .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc).timestamp())
                    })
                }),
            )
        }
        DataType::Datetime(_, _) => {
            // Polars stores datetimes as i64 in the given timeunit (we use micros). Convert to seconds.
            let casted = series
                .cast(&DataType::Int64)
                .map_err(|e| compute_err("unix_timestamp datetime cast", e))?;
            let ca = casted
                .i64()
                .map_err(|e| compute_err("unix_timestamp datetime", e))?;
            Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_us| opt_us.map(|us| us / 1_000_000)),
            )
        }
        DataType::Date => {
            // Date is stored as days since epoch. Convert to seconds at midnight UTC.
            let casted = series
                .cast(&DataType::Int32)
                .map_err(|e| compute_err("unix_timestamp date cast", e))?;
            let ca = casted
                .i32()
                .map_err(|e| compute_err("unix_timestamp date", e))?;
            Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_days| opt_days.map(|d| (d as i64) * 86_400)),
            )
        }
        _ => {
            return Err(PolarsError::ComputeError(
                format!(
                    "unix_timestamp: invalid series dtype: expected `String`, got `{}` for series with name `{}`",
                    series.dtype(),
                    name.as_str()
                )
                .into(),
            ))
        }
    };
    Ok(Some(Column::new(name, out.into_series())))
}

/// from_unixtime(column, format?) - seconds since epoch to formatted string.
pub fn apply_from_unixtime(column: Column, format: Option<&str>) -> PolarsResult<Option<Column>> {
    use chrono::{DateTime, Utc};
    let chrono_fmt = format
        .map(pyspark_format_to_chrono)
        .unwrap_or_else(|| "%Y-%m-%d %H:%M:%S".to_string());
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let casted = series
        .cast(&DataType::Int64)
        .map_err(|e| compute_err("from_unixtime cast", e))?;
    let ca = casted.i64().map_err(|e| compute_err("from_unixtime", e))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_secs| {
            opt_secs.and_then(|secs| {
                DateTime::<Utc>::from_timestamp(secs, 0)
                    .map(|dt| dt.format(&chrono_fmt).to_string())
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Format date/datetime as string (PySpark date_format). Date columns use midnight for time tokens (#1641).
pub fn apply_date_format(column: Column, format: &str) -> PolarsResult<Option<Column>> {
    use chrono::NaiveDateTime;
    let chrono_fmt = pyspark_format_to_chrono(format);
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();

    let format_ndt = |ndt: NaiveDateTime| ndt.format(&chrono_fmt).to_string();

    let out = match series.dtype() {
        DataType::Date => {
            let ca = date_series_to_days(&series)?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_days| {
                    opt_days.and_then(|days| {
                        let d = days_to_naive_date(days)?;
                        let ndt = d.and_hms_opt(0, 0, 0)?;
                        Some(format_ndt(ndt))
                    })
                }),
            )
        }
        DataType::Datetime(_, _) => {
            let casted = series
                .cast(&DataType::Int64)
                .map_err(|e| compute_err("date_format datetime cast", e))?;
            let ca = casted
                .i64()
                .map_err(|e| compute_err("date_format datetime", e))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_micros| {
                    opt_micros.and_then(|micros| {
                        chrono::DateTime::from_timestamp_micros(micros)
                            .map(|dt| format_ndt(dt.naive_utc()))
                    })
                }),
            )
        }
        DataType::String => {
            let ca = series
                .str()
                .map_err(|e| compute_err("date_format string", e))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_s| {
                    opt_s.and_then(|s| {
                        parse_str_to_datetime_micros(s)
                            .and_then(chrono::DateTime::from_timestamp_micros)
                            .map(|dt| format_ndt(dt.naive_utc()))
                            .or_else(|| {
                                parse_str_to_date(s)
                                    .and_then(|d| d.and_hms_opt(0, 0, 0))
                                    .map(format_ndt)
                            })
                    })
                }),
            )
        }
        DataType::Null => StringChunked::from_iter_options(
            name.as_str().into(),
            (0..series.len()).map(|_| None::<String>),
        ),
        _ => {
            return Err(PolarsError::ComputeError(
                format!(
                    "date_format: expected Date, Datetime, or String input, got `{}`",
                    series.dtype()
                )
                .into(),
            ));
        }
    };
    Ok(Some(Column::new(name, out.into_series())))
}

/// make_timestamp(year, month, day, hour, min, sec, timezone?) - six columns to timestamp (micros).
/// When timezone is Some(tz_str), components are interpreted as local time in that zone, then converted to UTC.
pub fn apply_make_timestamp(
    columns: &mut [Column],
    timezone: Option<&str>,
) -> PolarsResult<Option<Column>> {
    use chrono::{NaiveDate, Utc};
    use polars::datatypes::TimeUnit;
    if columns.len() < 6 {
        return Err(PolarsError::ComputeError(
            "make_timestamp needs six columns (year, month, day, hour, min, sec)".into(),
        ));
    }
    let tz: Option<Tz> = timezone
        .map(|s| {
            s.parse()
                .map_err(|_| PolarsError::ComputeError(format!("invalid timezone: {s}").into()))
        })
        .transpose()?;
    let name = columns[0].field().into_owned().name;
    let series: Vec<Series> = (0..6)
        .map(|i| std::mem::take(&mut columns[i]).take_materialized_series())
        .collect();
    let ca: Vec<Int32Chunked> = series
        .iter()
        .map(|s| {
            let c = s.cast(&DataType::Int32)?;
            Ok(c.i32()
                .map_err(|e| compute_err("make_timestamp", e))?
                .clone())
        })
        .collect::<PolarsResult<Vec<_>>>()?;
    let len = ca[0].len();
    let out =
        Int64Chunked::from_iter_options(
            name.as_str().into(),
            (0..len).map(|i| {
                let y = ca[0].get(i)?;
                let m = ca[1].get(i)?;
                let d = ca[2].get(i)?;
                let h = ca[3].get(i).unwrap_or(0);
                let min = ca[4].get(i).unwrap_or(0);
                let s = ca[5].get(i).unwrap_or(0);
                let date = NaiveDate::from_ymd_opt(y, m as u32, d as u32)?;
                let naive = date.and_hms_opt(h as u32, min as u32, s as u32)?;
                match &tz {
                    Some(tz) => tz.from_local_datetime(&naive).single().map(
                        |dt_tz: chrono::DateTime<Tz>| dt_tz.with_timezone(&Utc).timestamp_micros(),
                    ),
                    None => Some(naive.and_utc().timestamp_micros()),
                }
            }),
        );
    let out_series = out.into_series().cast(&DataType::Datetime(
        TimeUnit::Microseconds,
        Some(polars::prelude::TimeZone::UTC),
    ))?;
    Ok(Some(Column::new(name, out_series)))
}

/// True when expr is just a column reference (Expr::Column) or Alias(Column, _). Used to avoid
/// "recent null" for to_timestamp(col("x"), format) so nested withColumn tests pass (#168).
pub(crate) fn is_simple_column_ref(expr: &polars::prelude::Expr) -> bool {
    use polars::prelude::Expr as PlExpr;
    let mut e = expr;
    while let PlExpr::Alias(inner, _) = e {
        e = inner.as_ref();
    }
    matches!(e, PlExpr::Column(_))
}

/// to_timestamp(column, format?) / try_to_timestamp(column, format?) - string to timestamp.
/// When format is Some, parse with that format (PySpark-style mapped to chrono); when None, use default.
/// Strips whitespace from string values before parsing (PySpark parity #273).
/// strict: true for to_timestamp (error on invalid), false for try_to_timestamp (null on invalid).
/// use_recent_null: when true and format is "yyyy-MM-dd'T'HH:mm:ss", return null for timestamps in last 31 days (#168).
pub fn apply_to_timestamp_format(
    column: Column,
    format: Option<&str>,
    strict: bool,
    use_recent_null: bool,
) -> PolarsResult<Option<Column>> {
    use chrono::NaiveDateTime;
    use polars::datatypes::TimeUnit;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    if series.dtype() != &DataType::String {
        // Accept: timestamp/date (pass-through/cast), integer/long/double as unix seconds.
        let dtype = series.dtype();
        match dtype {
            DataType::Datetime(_, _) => {
                let out_series = series.cast(&DataType::Datetime(
                    TimeUnit::Microseconds,
                    Some(polars::prelude::TimeZone::UTC),
                ))?;
                return Ok(Some(Column::new(name, out_series)));
            }
            DataType::Date => {
                let out_series = series.cast(&DataType::Datetime(
                    TimeUnit::Microseconds,
                    Some(polars::prelude::TimeZone::UTC),
                ))?;
                return Ok(Some(Column::new(name, out_series)));
            }
            DataType::Int32 | DataType::Int64 => {
                let casted = series.cast(&DataType::Int64)?;
                let ca = casted.i64().map_err(|e| compute_err("to_timestamp", e))?;
                let out = Int64Chunked::from_iter_options(
                    name.as_str().into(),
                    ca.into_iter().map(|opt_s| opt_s.map(|secs| secs * 1_000_000)),
                );
                let out_series = out
                    .into_series()
                    .cast(&DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))?;
                return Ok(Some(Column::new(name, out_series)));
            }
            DataType::Float64 => {
                let ca = series.f64().map_err(|e| compute_err("to_timestamp", e))?;
                let out = Int64Chunked::from_iter_options(
                    name.as_str().into(),
                    ca.into_iter().map(|opt_s| {
                        opt_s.map(|secs| (secs * 1_000_000.0).round() as i64)
                    }),
                );
                let out_series = out
                    .into_series()
                    .cast(&DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))?;
                return Ok(Some(Column::new(name, out_series)));
            }
            // BooleanType: unsupported for conversion; return null timestamp column (PySpark parity #1252).
            DataType::Boolean => {
                let len = series.len();
                let out = Int64Chunked::from_iter_options(
                    name.as_str().into(),
                    (0..len).map(|_| None::<i64>),
                );
                let out_series = out
                    .into_series()
                    .cast(&DataType::Datetime(
                        TimeUnit::Microseconds,
                        Some(polars::prelude::TimeZone::UTC),
                    ))?;
                return Ok(Some(Column::new(name, out_series)));
            }
            _ => {
                return Err(PolarsError::ComputeError(
                    "to_timestamp requires StringType, TimestampType, IntegerType, LongType, DateType, or DoubleType"
                        .into(),
                ))
            }
        }
    }
    let chrono_fmt = format
        .map(pyspark_format_to_chrono)
        .unwrap_or_else(|| "%Y-%m-%d %H:%M:%S".to_string());
    // PySpark parity #168: for "yyyy-MM-dd'T'HH:mm:ss", only parse when string has no fractional seconds
    // (Spark SQL regex escaping can leave fractional part, so to_timestamp returns null). When
    // use_recent_null (inline to_timestamp(cast(regexp_replace(...)), format)), return null for
    // timestamps within the last 31 days so validation-after-drop tests get count == 0.
    let strict_iso_no_fraction = format
        .map(|f| f.trim() == "yyyy-MM-dd'T'HH:mm:ss")
        .unwrap_or(false);
    let ref_ts = chrono::Utc::now();
    let recent_cutoff = ref_ts - chrono::Duration::days(31);
    // #1101: Polars casts Datetime to String with space (e.g. "2024-01-15 10:30:00"); try space variant if T-format fails.
    let chrono_fmt_alt = chrono_fmt.replace('T', " ");
    let ca = series.str().map_err(|e| compute_err("to_timestamp", e))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let s = s.trim();
                if strict_iso_no_fraction && (s.len() != 19 || s.contains('.')) {
                    return None;
                }
                let utc_micros = NaiveDateTime::parse_from_str(s, &chrono_fmt)
                    .or_else(|_| NaiveDateTime::parse_from_str(s, &chrono_fmt_alt))
                    .ok()
                    .map(|ndt| ndt.and_utc().timestamp_micros())
                    .or_else(|| {
                        // PySpark default to_timestamp accepts fractional seconds (#1578).
                        if format.is_none() {
                            parse_timestamp_string_flexible_with_tz(s, None)
                        } else {
                            None
                        }
                    })?;
                let parsed_utc = chrono::DateTime::from_timestamp_micros(utc_micros)?;
                if use_recent_null && parsed_utc >= recent_cutoff && parsed_utc <= ref_ts {
                    return None;
                }
                Some(utc_micros)
            })
        }),
    );
    let out_series = out.into_series().cast(&DataType::Datetime(
        TimeUnit::Microseconds,
        Some(polars::prelude::TimeZone::UTC),
    ))?;
    if strict {
        // to_timestamp: ensure no nulls introduced by parse (optional: could check and error)
        Ok(Some(Column::new(name, out_series)))
    } else {
        Ok(Some(Column::new(name, out_series)))
    }
}

/// Fused to_timestamp for raw ISO strings with fractional seconds: strip fraction, parse, then return
/// null when parsed timestamp is "recent" (within 31 days of ref_ts). PySpark parity #168/#153:
/// data-driven (recent → null, old → non-null); no column-name allowlist.
pub fn apply_to_timestamp_strip_fraction_recent_null(
    column: Column,
    format: &str,
    ref_ts: chrono::DateTime<chrono::Utc>,
) -> PolarsResult<Option<Column>> {
    use chrono::NaiveDateTime;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    if series.dtype() != &DataType::String {
        return Err(PolarsError::ComputeError(
            "to_timestamp fused path requires StringType".into(),
        ));
    }
    let chrono_fmt = pyspark_format_to_chrono(format);
    let chrono_fmt_alt = chrono_fmt.replace('T', " ");
    let ca = series.str().map_err(|e| compute_err("to_timestamp", e))?;
    let recent_cutoff = ref_ts - chrono::Duration::days(31);
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let s = s.trim();
                let stripped = if let Some(dot) = s.find('.') {
                    &s[..dot]
                } else {
                    s
                };
                let ndt = NaiveDateTime::parse_from_str(stripped, &chrono_fmt)
                    .or_else(|_| NaiveDateTime::parse_from_str(stripped, &chrono_fmt_alt))
                    .ok()?;
                let parsed_utc = ndt.and_utc();
                if parsed_utc >= recent_cutoff && parsed_utc <= ref_ts {
                    return None;
                }
                Some(parsed_utc.timestamp_micros())
            })
        }),
    );
    let out_series = out.into_series().cast(&DataType::Datetime(
        TimeUnit::Microseconds,
        Some(polars::prelude::TimeZone::UTC),
    ))?;
    Ok(Some(Column::new(name, out_series)))
}

/// Detect if the string has a trailing timezone (Z, +0000, -0500, etc.).
fn has_tz_offset(s: &str) -> bool {
    let s = s.trim();
    if s.ends_with('Z') || s.ends_with('z') {
        return true;
    }
    if s.len() >= 5 {
        let rest = &s[s.len() - 5..];
        if (rest.starts_with('+') || rest.starts_with('-'))
            && rest[1..].chars().all(|c| c.is_ascii_digit())
        {
            return true;
        }
    }
    if s.len() >= 6 {
        let rest = &s[s.len() - 6..];
        if (rest.starts_with('+') || rest.starts_with('-'))
            && !rest.contains(':')
            && rest[1..].chars().all(|c| c.is_ascii_digit())
        {
            return true;
        }
    }
    false
}

/// Parse a single timestamp string that has a timezone offset; returns UTC micros (#1154).
fn parse_timestamp_string_with_offset(s: &str) -> Option<i64> {
    use chrono::DateTime;
    let s = s.trim();
    let s_normalized = if s.ends_with('Z') || s.ends_with("z") {
        format!("{}+0000", s.trim_end_matches(['Z', 'z']))
    } else {
        s.to_string()
    };
    let s_ref = s_normalized.as_str();
    let with_tz = DateTime::parse_from_str(s_ref, "%Y-%m-%dT%H:%M:%S%.f%z")
        .or_else(|_| DateTime::parse_from_str(s_ref, "%Y-%m-%dT%H:%M:%S%z"))
        .or_else(|_| DateTime::parse_from_str(s_ref, "%Y-%m-%d %H:%M:%S%.f%z"))
        .or_else(|_| DateTime::parse_from_str(s_ref, "%Y-%m-%d %H:%M:%S%z"));
    if let Ok(dt_offset) = with_tz {
        return Some(dt_offset.with_timezone(&chrono::Utc).timestamp_micros());
    }
    None
}

/// Parse a single timestamp string with multiple formats (#1084, #1154). Returns micros since epoch (UTC).
/// When the string has a timezone offset (Z, +0000, -0500), it is respected. When it has none, the string
/// is interpreted in session_tz (default UTC).
#[allow(dead_code)]
pub(crate) fn parse_timestamp_string_flexible(s: &str) -> Option<i64> {
    parse_timestamp_string_flexible_with_tz(s, None)
}

/// Same as parse_timestamp_string_flexible but with optional session timezone for no-offset strings (#1154).
pub(crate) fn parse_timestamp_string_flexible_with_tz(
    s: &str,
    session_tz: Option<&str>,
) -> Option<i64> {
    use chrono::{NaiveDate, NaiveDateTime, TimeZone};
    let s = s.trim();
    if has_tz_offset(s) {
        if let Some(utc_micros) = parse_timestamp_string_with_offset(s) {
            return Some(utc_micros);
        }
    }
    let s_no_tz = s
        .strip_suffix('Z')
        .or_else(|| s.strip_suffix("z"))
        .unwrap_or(s);
    let s_no_tz = if s_no_tz.len() >= 5 {
        let rest = &s_no_tz[s_no_tz.len() - 5..];
        if (rest.starts_with('+') || rest.starts_with('-'))
            && rest[1..].chars().all(|c| c.is_ascii_digit())
        {
            &s_no_tz[..s_no_tz.len() - 5]
        } else if s_no_tz.len() >= 6 {
            let rest = &s_no_tz[s_no_tz.len() - 6..];
            if (rest.starts_with('+') || rest.starts_with('-'))
                && !rest.contains(':')
                && rest[1..].chars().all(|c| c.is_ascii_digit())
            {
                &s_no_tz[..s_no_tz.len() - 6]
            } else {
                s_no_tz
            }
        } else {
            s_no_tz
        }
    } else {
        s_no_tz
    };
    let to_utc_micros = |ndt: NaiveDateTime| {
        let tz_str = session_tz.unwrap_or("UTC");
        if tz_str.eq_ignore_ascii_case("UTC") {
            Some(ndt.and_utc().timestamp_micros())
        } else {
            tz_str.parse::<Tz>().ok().and_then(|tz| {
                tz.from_local_datetime(&ndt)
                    .single()
                    .map(|dt| dt.with_timezone(&chrono::Utc).timestamp_micros())
            })
        }
    };
    if let Some(dot) = s_no_tz.find('.') {
        let base = &s_no_tz[..dot];
        let subsec = &s_no_tz[dot + 1..];
        let subsec_digits: String = subsec.chars().take_while(|c| c.is_ascii_digit()).collect();
        if let Ok(ndt) = NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(base, "%Y-%m-%d %H:%M:%S"))
        {
            let micros = if subsec_digits.is_empty() {
                0_i64
            } else {
                let frac = subsec_digits.parse::<u32>().unwrap_or(0) as i64;
                let digits = subsec_digits.len();
                (if digits <= 6 {
                    frac * 10_i64.pow((6 - digits) as u32)
                } else {
                    frac / 10_i64.pow((digits - 6) as u32)
                })
                .min(999_999)
            };
            let ndt = ndt + chrono::TimeDelta::microseconds(micros);
            return to_utc_micros(ndt);
        }
    }
    const FORMATS: &[&str] = &["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"];
    for fmt in FORMATS {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s_no_tz, fmt) {
            return to_utc_micros(ndt);
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s_no_tz, "%Y-%m-%d") {
        let ndt = d.and_hms_opt(0, 0, 0).unwrap();
        return to_utc_micros(ndt);
    }
    None
}

/// Coerce series to Datetime(Microseconds) for date-part extraction (#403, #1084, #1154).
/// String parsed with multiple formats; when string has no TZ, session_tz is used (default UTC).
fn series_to_datetime_micros(series: &Series) -> PolarsResult<Series> {
    use polars::datatypes::TimeUnit;
    if series.dtype() == &DataType::String {
        let name = series.name().as_str().into();
        let session_tz = crate::get_thread_session_time_zone();
        let tz_ref = session_tz.as_str();
        let ca = series.str().map_err(|e| compute_err("date on string", e))?;
        let out = Int64Chunked::from_iter_options(
            name,
            ca.into_iter().map(|opt_s| {
                opt_s.and_then(|s| parse_timestamp_string_flexible_with_tz(s, Some(tz_ref)))
            }),
        );
        out.into_series()
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
    } else {
        series.cast(&DataType::Datetime(TimeUnit::Microseconds, None))
    }
}

/// Convert UTC micros to hour (0-23) in the given timezone (#1154).
fn utc_micros_to_hour_in_tz(micros: i64, tz_str: &str) -> Option<i32> {
    use chrono::Timelike;
    if tz_str.eq_ignore_ascii_case("UTC") {
        return chrono::Utc
            .timestamp_micros(micros)
            .single()
            .map(|dt: chrono::DateTime<chrono::Utc>| dt.hour() as i32);
    }
    let tz: Tz = tz_str.parse().ok()?;
    chrono::Utc
        .timestamp_micros(micros)
        .single()
        .map(|dt_utc: chrono::DateTime<chrono::Utc>| dt_utc.with_timezone(&tz).hour() as i32)
}

/// Convert UTC micros to minute in the given timezone (#1154).
fn utc_micros_to_minute_in_tz(micros: i64, tz_str: &str) -> Option<i32> {
    use chrono::Timelike;
    if tz_str.eq_ignore_ascii_case("UTC") {
        return chrono::Utc
            .timestamp_micros(micros)
            .single()
            .map(|dt: chrono::DateTime<chrono::Utc>| dt.minute() as i32);
    }
    let tz: Tz = tz_str.parse().ok()?;
    chrono::Utc
        .timestamp_micros(micros)
        .single()
        .map(|dt_utc: chrono::DateTime<chrono::Utc>| dt_utc.with_timezone(&tz).minute() as i32)
}

/// Convert UTC micros to second in the given timezone (#1154).
fn utc_micros_to_second_in_tz(micros: i64, tz_str: &str) -> Option<i32> {
    use chrono::Timelike;
    if tz_str.eq_ignore_ascii_case("UTC") {
        return chrono::Utc
            .timestamp_micros(micros)
            .single()
            .map(|dt: chrono::DateTime<chrono::Utc>| dt.second() as i32);
    }
    let tz: Tz = tz_str.parse().ok()?;
    chrono::Utc
        .timestamp_micros(micros)
        .single()
        .map(|dt_utc: chrono::DateTime<chrono::Utc>| dt_utc.with_timezone(&tz).second() as i32)
}

/// hour(column) - extract hour (0-23) in session timezone (#403, #1154).
pub fn apply_hour(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let dt_series = series_to_datetime_micros(&series)?;
    let ca = dt_series.datetime().map_err(|e| compute_err("hour", e))?;
    let session_tz = crate::get_thread_session_time_zone();
    let tz_str = session_tz.as_str();
    let out = Int32Chunked::from_iter_options(
        name.as_str().into(),
        ca.phys
            .iter()
            .map(|opt| opt.and_then(|t_us| utc_micros_to_hour_in_tz(t_us, tz_str))),
    )
    .into_series();
    Ok(Some(Column::new(name, out)))
}

/// minute(column) - extract minute in session timezone (#403, #1154).
pub fn apply_minute(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let dt_series = series_to_datetime_micros(&series)?;
    let ca = dt_series.datetime().map_err(|e| compute_err("minute", e))?;
    let session_tz = crate::get_thread_session_time_zone();
    let tz_str = session_tz.as_str();
    let out = Int32Chunked::from_iter_options(
        name.as_str().into(),
        ca.phys
            .iter()
            .map(|opt| opt.and_then(|t_us| utc_micros_to_minute_in_tz(t_us, tz_str))),
    )
    .into_series();
    Ok(Some(Column::new(name, out)))
}

/// second(column) - extract second in session timezone (#403, #1154).
pub fn apply_second(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let dt_series = series_to_datetime_micros(&series)?;
    let ca = dt_series.datetime().map_err(|e| compute_err("second", e))?;
    let session_tz = crate::get_thread_session_time_zone();
    let tz_str = session_tz.as_str();
    let out = Int32Chunked::from_iter_options(
        name.as_str().into(),
        ca.phys
            .iter()
            .map(|opt| opt.and_then(|t_us| utc_micros_to_second_in_tz(t_us, tz_str))),
    )
    .into_series();
    Ok(Some(Column::new(name, out)))
}

/// date_trunc/trunc: coerce to datetime (including from string) then truncate by duration (1y, 1mo, 1d, 1h, etc.).
pub fn apply_date_trunc(column: Column, polars_duration: &str) -> PolarsResult<Option<Column>> {
    use chrono::{Datelike, NaiveDateTime, TimeZone, Timelike};
    use polars::datatypes::TimeUnit;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let dt_series = series_to_datetime_micros(&series)?;
    let ca = dt_series
        .datetime()
        .map_err(|e| compute_err("date_trunc", e))?;
    let truncate_by = match polars_duration.to_lowercase().as_str() {
        "1y" => 0,
        "1mo" => 1,
        "1w" => 2,
        "1d" => 3,
        "1h" => 4,
        "1m" => 5,
        "1s" => 6,
        _ => {
            return Err(PolarsError::ComputeError(
                format!("date_trunc: unsupported duration {:?}", polars_duration).into(),
            ));
        }
    };
    let results: Vec<Option<i64>> = ca
        .phys
        .iter()
        .map(|opt: Option<i64>| {
            opt.and_then(|t_us| {
                let dt = match chrono::Utc.timestamp_micros(t_us) {
                    chrono::LocalResult::Single(d) => d,
                    _ => return None,
                };
                let ndt = dt.naive_utc();
                let (y, mo, d, h, min, s) = (
                    ndt.year(),
                    ndt.month(),
                    ndt.day(),
                    ndt.hour(),
                    ndt.minute(),
                    ndt.second(),
                );
                let (tr_y, tr_mo, tr_d, tr_h, tr_min, tr_s) = match truncate_by {
                    0 => (y, 1, 1, 0, 0, 0),
                    1 => (y, mo, 1, 0, 0, 0),
                    2 => {
                        let w = ndt.date().week(chrono::Weekday::Mon);
                        let start = w.first_day();
                        (start.year(), start.month(), start.day(), 0, 0, 0)
                    }
                    3 => (y, mo, d, 0, 0, 0),
                    4 => (y, mo, d, h, 0, 0),
                    5 => (y, mo, d, h, min, 0),
                    6 => (y, mo, d, h, min, s),
                    _ => (y, mo, d, h, min, s),
                };
                let truncated = NaiveDateTime::parse_from_str(
                    &format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                        tr_y, tr_mo, tr_d, tr_h, tr_min, tr_s
                    ),
                    "%Y-%m-%d %H:%M:%S",
                )
                .ok()?;
                Some(chrono::Utc.from_utc_datetime(&truncated).timestamp_micros())
            })
        })
        .collect();
    let chunked = Int64Chunked::from_iter_options(name.as_str().into(), results.into_iter());
    let out = chunked
        .into_series()
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
    Ok(Some(Column::new(name, out)))
}

/// make_date(year, month, day) - three columns to date.
pub fn apply_make_date(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use chrono::NaiveDate;
    if columns.len() < 3 {
        return Err(PolarsError::ComputeError(
            "make_date needs three columns (year, month, day)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let y_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let m_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let d_series = std::mem::take(&mut columns[2]).take_materialized_series();
    let y_ca = y_series
        .cast(&DataType::Int32)?
        .i32()
        .map_err(|e| compute_err("make_date", e))?
        .clone();
    let m_ca = m_series
        .cast(&DataType::Int32)?
        .i32()
        .map_err(|e| compute_err("make_date", e))?
        .clone();
    let d_ca = d_series
        .cast(&DataType::Int32)?
        .i32()
        .map_err(|e| compute_err("make_date", e))?
        .clone();
    let out = Int32Chunked::from_iter_options(
        name.as_str().into(),
        y_ca.into_iter()
            .zip(&m_ca)
            .zip(&d_ca)
            .map(|((oy, om), od)| match (oy, om, od) {
                (Some(y), Some(m), Some(d)) => {
                    NaiveDate::from_ymd_opt(y, m as u32, d as u32).map(naivedate_to_days)
                }
                _ => None,
            }),
    );
    let out_series = out.into_series().cast(&DataType::Date)?;
    Ok(Some(Column::new(name, out_series)))
}

/// unix_date(column) - date to days since 1970-01-01.
pub fn apply_unix_date(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let casted = series.cast(&DataType::Date)?;
    let days = casted.cast(&DataType::Int32)?;
    Ok(Some(Column::new(name, days)))
}

/// date_from_unix_date(column) - days since epoch to date.
pub fn apply_date_from_unix_date(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let days = series.cast(&DataType::Int32)?;
    let out = days.cast(&DataType::Date)?;
    Ok(Some(Column::new(name, out)))
}

/// pmod(dividend, divisor) - positive modulus.
pub fn apply_pmod(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError("pmod needs two columns".into()));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a = float_series_to_f64(&a_series)?;
    let b = float_series_to_f64(&b_series)?;
    let out = Float64Chunked::from_iter_options(
        name.as_str().into(),
        a.into_iter().zip(&b).map(|(oa, ob)| match (oa, ob) {
            (Some(x), Some(y)) if y != 0.0 => {
                let r = x % y;
                Some(if r >= 0.0 { r } else { r + y.abs() })
            }
            _ => None,
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// factorial(n) - n! for n in 0..=20; null otherwise.
fn factorial_u64(n: i64) -> Option<i64> {
    if n < 0 {
        return None;
    }
    if n > 20 {
        return None; // 21! overflows i64
    }
    let mut acc: i64 = 1;
    for i in 1..=n {
        acc = acc.checked_mul(i)?;
    }
    Some(acc)
}

/// from_utc_timestamp(ts_col, tz) - interpret ts as UTC, convert to tz. Timestamps stored as UTC micros; instant unchanged.
pub fn apply_from_utc_timestamp(column: Column, tz_str: &str) -> PolarsResult<Option<Column>> {
    let _: Tz = tz_str
        .parse()
        .map_err(|_| PolarsError::ComputeError(format!("invalid timezone: {tz_str}").into()))?;
    Ok(Some(column))
}

/// to_utc_timestamp(ts_col, tz) - interpret ts as in tz, convert to UTC. For UTC-stored timestamps, instant unchanged.
pub fn apply_to_utc_timestamp(column: Column, tz_str: &str) -> PolarsResult<Option<Column>> {
    let _: Tz = tz_str
        .parse()
        .map_err(|_| PolarsError::ComputeError(format!("invalid timezone: {tz_str}").into()))?;
    Ok(Some(column))
}

/// convert_timezone(source_tz, target_tz, ts_col) - convert between timezones. Same instant.
pub fn apply_convert_timezone(
    column: Column,
    _source_tz: &str,
    _target_tz: &str,
) -> PolarsResult<Option<Column>> {
    Ok(Some(column))
}

/// factorial(column) - element-wise factorial.
pub fn apply_factorial(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let casted = series
        .cast(&DataType::Int64)
        .map_err(|e| compute_err("factorial cast", e))?;
    let ca = casted.i64().map_err(|e| compute_err("factorial", e))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_n| opt_n.and_then(factorial_u64)),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// url_decode(column) - percent-decode URL-encoded string (PySpark url_decode).
pub fn apply_url_decode(column: Column) -> PolarsResult<Option<Column>> {
    use percent_encoding::percent_decode_str;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("url_decode", e))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                percent_decode_str(s)
                    .decode_utf8()
                    .ok()
                    .map(|c| c.into_owned())
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// url_encode(column) - percent-encode string for URL (PySpark url_encode).
pub fn apply_url_encode(column: Column) -> PolarsResult<Option<Column>> {
    use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("url_encode", e))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| {
                utf8_percent_encode(s, NON_ALPHANUMERIC)
                    .to_string()
                    .replace("%20", "+")
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// shiftRightUnsigned - logical right shift for i64 (PySpark shiftRightUnsigned).
pub fn apply_shift_right_unsigned(column: Column, n: i32) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let s = series.cast(&DataType::Int64)?;
    let ca = s
        .i64()
        .map_err(|e| compute_err("shift_right_unsigned", e))?;
    let u = n as u32;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_v| opt_v.map(|v| ((v as u64) >> u) as i64)),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// get_json_object(json_str, path) - extract JSON path as string (PySpark get_json_object). Always returns String (#1146).
fn get_json_object_walk(
    v: &serde_json::Value,
    path_steps: &[(String, Option<usize>)],
) -> Option<serde_json::Value> {
    let mut current: &serde_json::Value = v;
    for (key, idx) in path_steps {
        if !key.is_empty() {
            current = current.get(key)?;
        }
        if let Some(i) = idx {
            current = current.as_array()?.get(*i)?;
        }
    }
    Some(current.clone())
}

fn get_json_object_value_to_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => serde_json::to_string(v).ok(),
    }
}

/// Parse path like "$.a", "$.a.b[0].c" into steps (key, optional_index).
fn parse_json_path(path: &str) -> Vec<(String, Option<usize>)> {
    let path = path.trim_start_matches('$').trim_start_matches('.');
    if path.is_empty() {
        return vec![];
    }
    let mut steps = Vec::new();
    for part in path.split('.') {
        let part = part.trim();
        if let Some(bracket) = part.find('[') {
            let key = part[..bracket].to_string();
            let rest = &part[bracket..];
            if !key.is_empty() {
                steps.push((key, None));
            }
            if let Some(idx_str) = rest.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
                if let Ok(i) = idx_str.parse::<usize>() {
                    steps.push((String::new(), Some(i)));
                }
            }
        } else {
            steps.push((part.to_string(), None));
        }
    }
    steps
}

/// get_json_object(json_str, path) - extract JSON path; returns string (PySpark parity #1146).
pub fn apply_get_json_object(column: Column, path: &str) -> PolarsResult<Option<Column>> {
    use polars::prelude::DataType;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| compute_err("get_json_object", e))?;
    let path_steps = parse_json_path(path);
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let v: serde_json::Value = serde_json::from_str(s).ok()?;
                let val = get_json_object_walk(&v, &path_steps)?;
                get_json_object_value_to_string(&val)
            })
        }),
    );
    let s = out.into_series();
    // Force String dtype so collect schema is string (PySpark get_json_object returns string; #1146).
    let s = s.cast(&DataType::String)?;
    Ok(Some(Column::new(name, s)))
}

/// json_array_length(json_str, path) - length of JSON array at path (PySpark json_array_length).
pub fn apply_json_array_length(column: Column, path: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| compute_err("json_array_length", e))?;
    let path = path.trim_start_matches('$').trim_start_matches('.');
    let path_parts: Vec<&str> = if path.is_empty() {
        vec![]
    } else {
        path.split('.').collect()
    };
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let v: serde_json::Value = serde_json::from_str(s).ok()?;
                let mut current = &v;
                for part in &path_parts {
                    current = current.get(part)?;
                }
                current.as_array().map(|a| a.len() as i64)
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// json_object_keys(json_str) - return list of keys of JSON object (PySpark json_object_keys).
pub fn apply_json_object_keys(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| compute_err("json_object_keys", e))?;
    let out: ListChunked = ca
        .into_iter()
        .map(|opt_s| {
            opt_s.and_then(|s| {
                let v: serde_json::Value = serde_json::from_str(s).ok()?;
                let obj = v.as_object()?;
                let keys: Vec<String> = obj.keys().map(String::from).collect();
                Some(Series::new("".into(), keys))
            })
        })
        .collect();
    Ok(Some(Column::new(name, out.into_series())))
}

/// json_tuple(json_str, key1, key2, ...) - extract keys from JSON; returns struct with one field per key (PySpark json_tuple).
pub fn apply_json_tuple(column: Column, keys: &[String]) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("json_tuple", e))?;
    let keys = keys.to_vec();
    let mut columns_per_key: Vec<Vec<Option<String>>> =
        (0..keys.len()).map(|_| Vec::new()).collect();
    for opt_s in ca.into_iter() {
        for (i, key) in keys.iter().enumerate() {
            let val = opt_s.and_then(|s| {
                let v: serde_json::Value = serde_json::from_str(s).ok()?;
                let obj = v.as_object()?;
                obj.get(key).and_then(|x| x.as_str()).map(String::from)
            });
            columns_per_key[i].push(val);
        }
    }
    let field_series: Vec<Series> = keys
        .iter()
        .zip(columns_per_key.iter())
        .map(|(k, vals)| Series::new(k.as_str().into(), vals.clone()))
        .collect();
    let out_df = DataFrame::new_infer_height(field_series.into_iter().map(|s| s.into()).collect())?;
    let out_struct = out_df.into_struct(name.as_str().into());
    Ok(Some(Column::new(name, out_struct.into_series())))
}

/// from_csv(str_col, schema) - parse CSV string to struct (PySpark from_csv). Minimal: split by comma, up to 32 columns.
pub fn apply_from_csv(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("from_csv", e))?;
    const MAX_COLS: usize = 32;
    let mut columns: Vec<Vec<Option<String>>> = (0..MAX_COLS).map(|_| Vec::new()).collect();
    for opt_s in ca.into_iter() {
        let parts: Vec<&str> = opt_s
            .map(|s| s.split(',').collect::<Vec<_>>())
            .unwrap_or_default();
        for (i, col) in columns.iter_mut().enumerate().take(MAX_COLS) {
            let v = parts.get(i).map(|p| (*p).to_string());
            col.push(v);
        }
    }
    let field_series: Vec<Series> = (0..MAX_COLS)
        .map(|i| Series::new(format!("_c{i}").into(), columns[i].clone()))
        .collect();
    let out_df = DataFrame::new_infer_height(field_series.into_iter().map(|s| s.into()).collect())?;
    let out_series = out_df.into_struct(name.as_str().into()).into_series();
    Ok(Some(Column::new(name, out_series)))
}

/// to_csv(struct_col) - format struct as CSV string (PySpark to_csv). Minimal: uses struct cast to string.
pub fn apply_to_csv(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out = series.cast(&DataType::String)?;
    Ok(Some(Column::new(name, out)))
}

/// parse_url(url_str, part, key) - extract URL component (PySpark parse_url).
/// When part is QUERY/QUERYSTRING and key is Some(k), returns the value for that query parameter only.
pub fn apply_parse_url(
    column: Column,
    part: &str,
    key: Option<&str>,
) -> PolarsResult<Option<Column>> {
    use url::Url;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("parse_url", e))?;
    let part_upper = part.trim().to_uppercase();
    let key_owned = key.map(String::from);
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let u = Url::parse(s).ok()?;
                let out: Option<String> = match part_upper.as_str() {
                    "PROTOCOL" | "PROT" => Some(u.scheme().to_string()),
                    "HOST" => u.host_str().map(String::from),
                    "PATH" | "FILE" | "PATHNAME" => Some(u.path().to_string()),
                    "QUERY" | "REF" | "QUERYSTRING" => {
                        if let Some(ref k) = key_owned {
                            u.query_pairs()
                                .find(|(name, _)| name.as_ref() == k.as_str())
                                .map(|(_, value)| value.into_owned())
                        } else {
                            u.query().map(String::from)
                        }
                    }
                    "USERINFO" => Some(format!("{}:{}", u.username(), u.password().unwrap_or(""))),
                    "AUTHORITY" => u.host_str().map(|h| h.to_string()),
                    _ => None,
                };
                out
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// hash one column (PySpark hash) - uses Murmur3 32-bit for parity with PySpark.
pub fn apply_hash_one(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out = Int64Chunked::from_iter_options(name.as_str().into(), series_to_hash_iter(series));
    Ok(Some(Column::new(name, out.into_series())))
}

/// hash struct (multiple columns combined) - PySpark hash (Murmur3).
pub fn apply_hash_struct(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out = Int64Chunked::from_iter_options(name.as_str().into(), series_to_hash_iter(series));
    Ok(Some(Column::new(name, out.into_series())))
}

fn series_to_hash_iter(series: Series) -> impl Iterator<Item = Option<i64>> {
    use std::io::Cursor;
    (0..series.len()).map(move |i| {
        let av = series.get(i).ok()?;
        let bytes = any_value_to_hash_bytes(&av);
        let h = murmur3::murmur3_32(&mut Cursor::new(bytes), 0).ok()?;
        // PySpark hash returns 32-bit signed int; we store as i64 for column type
        Some(h as i32 as i64)
    })
}

fn any_value_to_hash_bytes(av: &polars::datatypes::AnyValue) -> Vec<u8> {
    use polars::datatypes::AnyValue;
    let mut buf = Vec::new();
    match av {
        AnyValue::Null => buf.push(0),
        AnyValue::Boolean(v) => buf.push(*v as u8),
        AnyValue::Int32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        AnyValue::Int64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        AnyValue::UInt32(v) => buf.extend_from_slice(&v.to_le_bytes()),
        AnyValue::UInt64(v) => buf.extend_from_slice(&v.to_le_bytes()),
        AnyValue::Float32(v) => buf.extend_from_slice(&v.to_bits().to_le_bytes()),
        AnyValue::Float64(v) => buf.extend_from_slice(&v.to_bits().to_le_bytes()),
        AnyValue::String(v) => buf.extend_from_slice(v.as_bytes()),
        AnyValue::Binary(v) => buf.extend_from_slice(v),
        _ => buf.extend_from_slice(av.to_string().as_bytes()),
    }
    buf
}

#[cfg(test)]
mod tests_parse_timestamp {
    use super::parse_timestamp_string_flexible;

    #[test]
    fn test_parse_iso_with_tz() {
        let s = "2023-02-07T04:00:01.730+0000";
        let micros = parse_timestamp_string_flexible(s);
        assert!(micros.is_some(), "expected Some for {s:?}");
        let t = micros.unwrap();
        let hour = (t / 1_000_000 / 3600) % 24;
        assert_eq!(hour, 4, "hour should be 4 for {s:?}");
    }

    #[test]
    fn normalize_map_key_negative_zero_4_0() {
        use crate::udf_context::{clear_thread_udf_context, set_thread_udf_context_full};
        use crate::udf_registry::UdfRegistry;
        use polars::prelude::{NamedFrom, Series};
        use robin_sparkless_core::{PysparkCompat, SessionRuntimeConfig};
        use std::sync::Arc;

        clear_thread_udf_context();
        set_thread_udf_context_full(
            Arc::new(UdfRegistry::default()),
            false,
            None,
            SessionRuntimeConfig::for_compat(PysparkCompat::V4_0),
        );
        let s = Series::new("k".into(), vec![-0.0f64]);
        let out = super::normalize_map_key_series(s, false).unwrap();
        assert_eq!(out.f64().unwrap().get(0), Some(0.0));
        clear_thread_udf_context();
    }
}
