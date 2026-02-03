//! Helpers for element-wise UDFs used by map() expressions (soundex, levenshtein, crc32, xxhash64, array_flatten, array_repeat).
//! These run at plan execution time when Polars invokes the closure.

use chrono::Datelike;
use polars::prelude::*;
use std::borrow::Cow;

/// American Soundex code (4 chars). Matches PySpark soundex semantics.
fn soundex_one(s: &str) -> Cow<'_, str> {
    use soundex::american_soundex;
    let code = american_soundex(s);
    Cow::Owned(code.chars().take(4).collect::<String>())
}

/// Apply soundex to a string column; returns a new Column (Series).
pub fn apply_soundex(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("soundex: {}", e).into()))?;
    let out: StringChunked = ca.apply_values(soundex_one);
    Ok(Some(Column::new(name, out.into_series())))
}

/// Apply CRC32 to string bytes (PySpark crc32).
pub fn apply_crc32(column: Column) -> PolarsResult<Option<Column>> {
    use crc32fast::Hasher;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("crc32: {}", e).into()))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| {
                let mut hasher = Hasher::new();
                hasher.update(s.as_bytes());
                hasher.finalize() as i64
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Apply XXH64 hash (PySpark xxhash64).
pub fn apply_xxhash64(column: Column) -> PolarsResult<Option<Column>> {
    use std::hash::Hasher;
    use twox_hash::XxHash64;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("xxhash64: {}", e).into()))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| {
                let mut hasher = XxHash64::default();
                hasher.write(s.as_bytes());
                hasher.finish() as i64
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Levenshtein distance between two string columns (element-wise).
pub fn apply_levenshtein(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use strsim::levenshtein;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "levenshtein needs two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("levenshtein: {}", e).into()))?;
    let b_ca = b_series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("levenshtein: {}", e).into()))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        a_ca.into_iter().zip(b_ca).map(|(a, b)| match (a, b) {
            (Some(a), Some(b)) => Some(levenshtein(a, b) as i64),
            _ => None,
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Flatten list-of-lists to a single list per row (PySpark flatten).
pub fn apply_array_flatten(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_flatten: {}", e).into()))?;
    let inner_dtype = match list_ca.inner_dtype() {
        DataType::List(inner) => *inner.clone(),
        other => other.clone(),
    };
    let out = list_ca.try_apply_amortized(|amort_s| {
        let s = amort_s.as_ref();
        let list_s = s.as_list();
        if list_s.is_empty() {
            return Ok(Series::new_empty(PlSmallStr::EMPTY, &inner_dtype));
        }
        let mut acc: Vec<Series> = Vec::new();
        for elem in list_s.amortized_iter().flatten() {
            acc.push(elem.deep_clone());
        }
        if acc.is_empty() {
            Ok(Series::new_empty(PlSmallStr::EMPTY, &inner_dtype))
        } else {
            let mut result = acc.remove(0);
            for s in acc {
                result.extend(&s)?;
            }
            Ok(result)
        }
    })?;
    Ok(Some(Column::new(name, out.into_series())))
}

/// Repeat each list element n times (PySpark array_repeat).
pub fn apply_array_repeat(column: Column, n: i64) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_repeat: {}", e).into()))?;
    let inner_dtype = list_ca.inner_dtype().clone();
    let n = n.max(0) as usize;
    let out = list_ca.try_apply_amortized(move |amort_s| {
        let list_s = amort_s.as_ref().as_list();
        let mut repeated: Vec<Series> = Vec::new();
        for elem in list_s.amortized_iter().flatten() {
            let taken = elem.deep_clone();
            for _ in 0..n {
                repeated.push(taken.clone());
            }
        }
        if repeated.is_empty() {
            Ok(Series::new_empty(PlSmallStr::EMPTY, &inner_dtype))
        } else {
            let mut result = repeated.remove(0);
            for s in repeated {
                result.extend(&s)?;
            }
            Ok(result)
        }
    })?;
    Ok(Some(Column::new(name, out.into_series())))
}

/// ASCII value of first character (PySpark ascii). Returns Int32.
pub fn apply_ascii(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("ascii: {}", e).into()))?;
    let out = Int32Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_s| opt_s.and_then(|s| s.chars().next().map(|c| c as i32))),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Format numeric column as string with fixed decimal places (PySpark format_number).
pub fn apply_format_number(column: Column, decimals: u32) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let prec = decimals as usize;
    let out: StringChunked = match series.dtype() {
        DataType::Float64 => {
            let ca = series
                .f64()
                .map_err(|e| PolarsError::ComputeError(format!("format_number: {}", e).into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter()
                    .map(|opt_v| opt_v.map(|v| format!("{:.prec$}", v, prec = prec))),
            )
        }
        DataType::Float32 => {
            let ca = series
                .f32()
                .map_err(|e| PolarsError::ComputeError(format!("format_number: {}", e).into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter()
                    .map(|opt_v| opt_v.map(|v| format!("{:.prec$}", v, prec = prec))),
            )
        }
        _ => {
            let f64_series = series.cast(&DataType::Float64).map_err(|e| {
                PolarsError::ComputeError(format!("format_number cast: {}", e).into())
            })?;
            let ca = f64_series
                .f64()
                .map_err(|e| PolarsError::ComputeError(format!("format_number: {}", e).into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter()
                    .map(|opt_v| opt_v.map(|v| format!("{:.prec$}", v, prec = prec))),
            )
        }
    };
    Ok(Some(Column::new(name, out.into_series())))
}

/// Base64 encode string bytes (PySpark base64). Input string UTF-8, output base64 string.
pub fn apply_base64(column: Column) -> PolarsResult<Option<Column>> {
    use base64::Engine;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("base64: {}", e).into()))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| base64::engine::general_purpose::STANDARD.encode(s.as_bytes()))
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Base64 decode to string (PySpark unbase64). Output UTF-8 string; invalid decode → null.
pub fn apply_unbase64(column: Column) -> PolarsResult<Option<Column>> {
    use base64::Engine;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("unbase64: {}", e).into()))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(s.as_bytes())
                    .ok()?;
                String::from_utf8(decoded).ok()
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// SHA1 hash of string bytes, return hex string (PySpark sha1).
pub fn apply_sha1(column: Column) -> PolarsResult<Option<Column>> {
    use sha1::Digest;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("sha1: {}", e).into()))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| {
                let mut hasher = sha1::Sha1::new();
                hasher.update(s.as_bytes());
                format!("{:x}", hasher.finalize())
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// SHA2 hash of string bytes, return hex string (PySpark sha2). bit_length 256 or 384 or 512.
pub fn apply_sha2(column: Column, bit_length: i32) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("sha2: {}", e).into()))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| {
                let bytes = s.as_bytes();
                use sha2::Digest;
                match bit_length {
                    256 => format!("{:x}", sha2::Sha256::digest(bytes)),
                    384 => format!("{:x}", sha2::Sha384::digest(bytes)),
                    512 => format!("{:x}", sha2::Sha512::digest(bytes)),
                    _ => format!("{:x}", sha2::Sha256::digest(bytes)),
                }
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// MD5 hash of string bytes, return hex string (PySpark md5).
pub fn apply_md5(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("md5: {}", e).into()))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_s| opt_s.map(|s| format!("{:x}", md5::compute(s.as_bytes())))),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Int column to single-character string (PySpark char / chr). Valid codepoint only.
pub fn apply_char(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let to_char = |v: i64| -> String {
        let u = v as u32;
        if u <= 0x10FFFF {
            char::from_u32(u).map(|c| c.to_string()).unwrap_or_default()
        } else {
            String::new()
        }
    };
    let out: StringChunked = match series.dtype() {
        DataType::Int32 => {
            let ca = series
                .i32()
                .map_err(|e| PolarsError::ComputeError(format!("char: {}", e).into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_v| opt_v.map(|v| to_char(v as i64))),
            )
        }
        DataType::Int64 => {
            let ca = series
                .i64()
                .map_err(|e| PolarsError::ComputeError(format!("char: {}", e).into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_v| opt_v.map(to_char)),
            )
        }
        _ => {
            let i64_series = series
                .cast(&DataType::Int64)
                .map_err(|e| PolarsError::ComputeError(format!("char cast: {}", e).into()))?;
            let ca = i64_series
                .i64()
                .map_err(|e| PolarsError::ComputeError(format!("char: {}", e).into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_v| opt_v.map(to_char)),
            )
        }
    };
    Ok(Some(Column::new(name, out.into_series())))
}

// --- Datetime: add_months, months_between, next_day ---

fn date_series_to_days(series: &Series) -> PolarsResult<Int32Chunked> {
    let casted = series.cast(&DataType::Date)?;
    let days_series = casted.cast(&DataType::Int32)?;
    Ok(days_series.i32().unwrap().clone())
}

fn days_to_naive_date(days: i32) -> Option<chrono::NaiveDate> {
    let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    base.checked_add_signed(chrono::TimeDelta::days(days as i64))
}

fn naivedate_to_days(d: chrono::NaiveDate) -> i32 {
    let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (d.signed_duration_since(base).num_days()) as i32
}

/// add_months(date_column, n) - add n months to each date.
pub fn apply_add_months(column: Column, n: i32) -> PolarsResult<Option<Column>> {
    use chrono::Months;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = date_series_to_days(&series)?;
    let u = n.unsigned_abs();
    let out = ca.into_iter().map(|opt_d| {
        opt_d.and_then(|days| {
            let d = days_to_naive_date(days)?;
            let next = if n >= 0 {
                d.checked_add_months(Months::new(u))?
            } else {
                d.checked_sub_months(Months::new(u))?
            };
            Some(naivedate_to_days(next))
        })
    });
    let out = Int32Chunked::from_iter_options(name.as_str().into(), out);
    let out_series = out.into_series().cast(&DataType::Date)?;
    Ok(Some(Column::new(name, out_series)))
}

/// next_day(date_column, "Mon") - next occurrence of weekday (Sun=1..Sat=7; "Mon","Tue",...).
fn parse_day_of_week(s: &str) -> Option<u32> {
    let s = s.trim().to_lowercase();
    match s.as_str() {
        "sun" | "sunday" => Some(1),
        "mon" | "monday" => Some(2),
        "tue" | "tuesday" => Some(3),
        "wed" | "wednesday" => Some(4),
        "thu" | "thursday" => Some(5),
        "fri" | "friday" => Some(6),
        "sat" | "saturday" => Some(7),
        _ => None,
    }
}

/// PySpark next_day: 1=Sunday, 2=Monday, ... 7=Saturday. chrono Weekday: Mon=0, ..., Sun=6.
fn chrono_weekday_to_dayofweek(w: chrono::Weekday) -> u32 {
    match w {
        chrono::Weekday::Mon => 2,
        chrono::Weekday::Tue => 3,
        chrono::Weekday::Wed => 4,
        chrono::Weekday::Thu => 5,
        chrono::Weekday::Fri => 6,
        chrono::Weekday::Sat => 7,
        chrono::Weekday::Sun => 1,
    }
}

pub fn apply_next_day(column: Column, day_of_week: &str) -> PolarsResult<Option<Column>> {
    let target = parse_day_of_week(day_of_week)
        .ok_or_else(|| PolarsError::ComputeError(format!("next_day: invalid day '{}'", day_of_week).into()))?;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = date_series_to_days(&series)?;
    let out = ca.into_iter().map(|opt_d| {
        opt_d.and_then(|days| {
            let d = days_to_naive_date(days)?;
            let current = chrono_weekday_to_dayofweek(d.weekday());
            let diff = if target >= current { (target - current) as i64 } else { (7 - (current - target)) as i64 };
            let days_to_add = if diff == 0 { 7 } else { diff }; // same day -> next week
            let next = d.checked_add_signed(chrono::TimeDelta::days(days_to_add))?;
            Some(naivedate_to_days(next))
        })
    });
    let out = Int32Chunked::from_iter_options(name.as_str().into(), out);
    let out_series = out.into_series().cast(&DataType::Date)?;
    Ok(Some(Column::new(name, out_series)))
}

/// months_between(end, start) - returns fractional number of months.
pub fn apply_months_between(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "months_between needs two columns (end, start)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let end_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let start_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let end_ca = date_series_to_days(&end_series)?;
    let start_ca = date_series_to_days(&start_series)?;
    let out = end_ca
        .into_iter()
        .zip(start_ca.into_iter())
        .map(|(oe, os)| {
            match (oe, os) {
                (Some(e), Some(s)) => {
                    let days = (e - s) as f64;
                    Some(days / 30.44) // approximate months
                }
                _ => None,
            }
        });
    let out = Float64Chunked::from_iter_options(name.as_str().into(), out);
    Ok(Some(Column::new(name, out.into_series())))
}

// --- Math (trig, degrees, radians, signum) ---

fn float_series_to_f64(series: &Series) -> PolarsResult<Float64Chunked> {
    let casted = series.cast(&DataType::Float64)?;
    Ok(casted.f64().unwrap().clone())
}

/// Apply sin (radians) to a float column.
pub fn apply_sin(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::sin).into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply cos (radians) to a float column.
pub fn apply_cos(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::cos).into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply tan (radians) to a float column.
pub fn apply_tan(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::tan).into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply asin to a float column.
pub fn apply_asin(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::asin).into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply acos to a float column.
pub fn apply_acos(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::acos).into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply atan to a float column.
pub fn apply_atan(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::atan).into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply atan2(y, x) to two float columns.
pub fn apply_atan2(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "atan2 needs two columns (y, x)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let y_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let x_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let y_ca = float_series_to_f64(&y_series)?;
    let x_ca = float_series_to_f64(&x_series)?;
    let out = y_ca
        .into_iter()
        .zip(x_ca.into_iter())
        .map(|(oy, ox)| match (oy, ox) {
            (Some(y), Some(x)) => Some(f64::atan2(y, x)),
            _ => None,
        });
    let out = Float64Chunked::from_iter_options(name.as_str().into(), out);
    Ok(Some(Column::new(name, out.into_series())))
}

/// Apply degrees (radians -> degrees) to a float column.
pub fn apply_degrees(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca
        .apply_values(|r| r.to_degrees())
        .into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply radians (degrees -> radians) to a float column.
pub fn apply_radians(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca
        .apply_values(|d| d.to_radians())
        .into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply signum (-1, 0, or 1) to a numeric column.
pub fn apply_signum(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca
        .apply_values(|v| {
            if v > 0.0 {
                1.0
            } else if v < 0.0 {
                -1.0
            } else {
                0.0
            }
        })
        .into_series();
    Ok(Some(Column::new(name, out)))
}

/// Element-wise max of two columns (for greatest). Supports Float64, Int64, String.
pub fn apply_greatest2(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "greatest2 needs two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let out = match (a_series.dtype(), b_series.dtype()) {
        (DataType::Float64, _) | (_, DataType::Float64) => {
            let a = float_series_to_f64(&a_series)?;
            let b = float_series_to_f64(&b_series)?;
            let out = Float64Chunked::from_iter_options(
                name.as_str().into(),
                a.into_iter().zip(b.into_iter()).map(|(oa, ob)| match (oa, ob) {
                    (Some(x), Some(y)) => Some(x.max(y)),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }),
            );
            out.into_series()
        }
        (DataType::Int64, _) | (DataType::Int32, _) | (_, DataType::Int64) | (_, DataType::Int32) => {
            let a = a_series.cast(&DataType::Int64)?;
            let b = b_series.cast(&DataType::Int64)?;
            let ca_a = a.i64().unwrap();
            let ca_b = b.i64().unwrap();
            let out = Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter().zip(ca_b.into_iter()).map(|(oa, ob)| match (oa, ob) {
                    (Some(x), Some(y)) => Some(x.max(y)),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }),
            );
            out.into_series()
        }
        (DataType::String, _) | (_, DataType::String) => {
            let a = a_series.cast(&DataType::String)?;
            let b = b_series.cast(&DataType::String)?;
            let ca_a = a.str().unwrap();
            let ca_b = b.str().unwrap();
            let out = StringChunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter().zip(ca_b.into_iter()).map(|(oa, ob)| match (oa, ob) {
                    (Some(x), Some(y)) => Some(if x >= y { x } else { y }),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }),
            );
            out.into_series()
        }
        _ => {
            let a = float_series_to_f64(&a_series)?;
            let b = float_series_to_f64(&b_series)?;
            let out = Float64Chunked::from_iter_options(
                name.as_str().into(),
                a.into_iter().zip(b.into_iter()).map(|(oa, ob)| match (oa, ob) {
                    (Some(x), Some(y)) => Some(x.max(y)),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }),
            );
            out.into_series()
        }
    };
    Ok(Some(Column::new(name, out)))
}

/// Element-wise min of two columns (for least).
pub fn apply_least2(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError("least2 needs two columns".into()));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let out = match (a_series.dtype(), b_series.dtype()) {
        (DataType::Float64, _) | (_, DataType::Float64) => {
            let a = float_series_to_f64(&a_series)?;
            let b = float_series_to_f64(&b_series)?;
            let out = Float64Chunked::from_iter_options(
                name.as_str().into(),
                a.into_iter().zip(b.into_iter()).map(|(oa, ob)| match (oa, ob) {
                    (Some(x), Some(y)) => Some(x.min(y)),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }),
            );
            out.into_series()
        }
        (DataType::Int64, _) | (DataType::Int32, _) | (_, DataType::Int64) | (_, DataType::Int32) => {
            let a = a_series.cast(&DataType::Int64)?;
            let b = b_series.cast(&DataType::Int64)?;
            let ca_a = a.i64().unwrap();
            let ca_b = b.i64().unwrap();
            let out = Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter().zip(ca_b.into_iter()).map(|(oa, ob)| match (oa, ob) {
                    (Some(x), Some(y)) => Some(x.min(y)),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }),
            );
            out.into_series()
        }
        (DataType::String, _) | (_, DataType::String) => {
            let a = a_series.cast(&DataType::String)?;
            let b = b_series.cast(&DataType::String)?;
            let ca_a = a.str().unwrap();
            let ca_b = b.str().unwrap();
            let out = StringChunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter().zip(ca_b.into_iter()).map(|(oa, ob)| match (oa, ob) {
                    (Some(x), Some(y)) => Some(if x <= y { x } else { y }),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }),
            );
            out.into_series()
        }
        _ => {
            let a = float_series_to_f64(&a_series)?;
            let b = float_series_to_f64(&b_series)?;
            let out = Float64Chunked::from_iter_options(
                name.as_str().into(),
                a.into_iter().zip(b.into_iter()).map(|(oa, ob)| match (oa, ob) {
                    (Some(x), Some(y)) => Some(x.min(y)),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }),
            );
            out.into_series()
        }
    };
    Ok(Some(Column::new(name, out)))
}

/// Build map (list of structs {key, value}) from two list columns. PySpark map_from_arrays.
pub fn apply_map_from_arrays(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::builder::get_list_builder;
    use polars::chunked_array::StructChunked;
    use polars::datatypes::Field;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "map_from_arrays needs two columns (keys, values)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let keys_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let values_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let keys_ca = keys_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("map_from_arrays keys: {}", e).into()))?;
    let values_ca = values_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("map_from_arrays values: {}", e).into()))?;
    let key_dtype = keys_ca.inner_dtype().clone();
    let value_dtype = values_ca.inner_dtype().clone();
    let struct_dtype = DataType::Struct(vec![
        Field::new("key".into(), key_dtype),
        Field::new("value".into(), value_dtype),
    ]);
    let mut builder = get_list_builder(&struct_dtype, 64, keys_ca.len(), name.as_str().into());
    for (opt_k, opt_v) in keys_ca.amortized_iter().zip(values_ca.amortized_iter()) {
        match (opt_k, opt_v) {
            (Some(k_amort), Some(v_amort)) => {
                let k_list = k_amort.as_ref().as_list();
                let v_list = v_amort.as_ref().as_list();
                let mut row_structs: Vec<Series> = Vec::new();
                for (opt_ke, opt_ve) in k_list.amortized_iter().zip(v_list.amortized_iter()) {
                    if let (Some(ke), Some(ve)) = (opt_ke, opt_ve) {
                        let ke_s = ke.deep_clone();
                        let ve_s = ve.deep_clone();
                        let len = ke_s.len();
                        let fields: [&Series; 2] = [&ke_s, &ve_s];
                        let st = StructChunked::from_series(
                            PlSmallStr::EMPTY,
                            len,
                            fields.iter().copied(),
                        )
                        .map_err(|e| PolarsError::ComputeError(format!("struct: {}", e).into()))?
                        .into_series();
                        row_structs.push(st);
                    }
                }
                if row_structs.is_empty() {
                    builder
                        .append_series(&Series::new_empty(PlSmallStr::EMPTY, &struct_dtype))
                        .map_err(|e| PolarsError::ComputeError(format!("builder: {}", e).into()))?;
                } else {
                    let mut combined = row_structs.remove(0);
                    for s in row_structs {
                        combined.extend(&s)?;
                    }
                    builder
                        .append_series(&combined)
                        .map_err(|e| PolarsError::ComputeError(format!("builder: {}", e).into()))?;
                }
            }
            _ => {
                builder.append_null();
            }
        }
    }
    let out = builder.finish().into_series();
    Ok(Some(Column::new(name, out)))
}
