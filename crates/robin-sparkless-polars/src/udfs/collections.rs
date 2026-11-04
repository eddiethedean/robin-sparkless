//! Collections UDF implementations.

use super::*;

/// Build array [start, start+step, ...] up to but not past stop (PySpark sequence).
/// Input column is a struct with fields "0"=start, "1"=stop, "2"=step (step optional, default 1).
pub fn apply_sequence(column: Column) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::builder::get_list_builder;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let st = series
        .struct_()
        .map_err(|e| compute_err("sequence: expected struct column", e))?;
    let start_s = st
        .field_by_name("0")
        .map_err(|e| compute_err("sequence field 0", e))?;
    let stop_s = st
        .field_by_name("1")
        .map_err(|e| compute_err("sequence field 1", e))?;
    let step_s = st.field_by_name("2").ok(); // optional
    let start_series = start_s
        .cast(&DataType::Int64)
        .map_err(|e| compute_err("sequence start", e))?;
    let stop_series = stop_s
        .cast(&DataType::Int64)
        .map_err(|e| compute_err("sequence stop", e))?;
    let step_series_opt: Option<Series> = step_s
        .as_ref()
        .map(|s| s.cast(&DataType::Int64))
        .transpose()
        .map_err(|e| compute_err("sequence step", e))?;
    let start_ca = start_series.i64().map_err(|e| compute_err("sequence", e))?;
    let stop_ca = stop_series.i64().map_err(|e| compute_err("sequence", e))?;
    let step_ca = step_series_opt.as_ref().and_then(|s| s.i64().ok());
    let n = start_ca.len();
    let mut builder = get_list_builder(&DataType::Int64, 64, n, name.as_str().into());
    for i in 0..n {
        let start_v = start_ca.get(i);
        let stop_v = stop_ca.get(i);
        let step_v: Option<i64> = step_ca.as_ref().and_then(|ca| ca.get(i)).or(Some(1));
        match (start_v, stop_v, step_v) {
            (Some(s), Some(st), Some(step)) if step != 0 => {
                let mut vals: Vec<i64> = Vec::new();
                if step > 0 {
                    let mut v = s;
                    while v <= st {
                        vals.push(v);
                        v += step;
                    }
                } else {
                    let mut v = s;
                    while v >= st {
                        vals.push(v);
                        v += step;
                    }
                }
                let series = Series::new("".into(), vals);
                builder.append_series(&series)?;
            }
            _ => builder.append_null(),
        }
    }
    Ok(Some(Column::new(name, builder.finish().into_series())))
}

/// Mask a series to null where `mask` is true. Used so withField leaves null when input struct is null (PySpark #1066).
fn series_set_nulls_where(series: &Series, mask: &BooleanChunked) -> PolarsResult<Series> {
    use polars::chunked_array::ops::ChunkSet;
    let name = series.name().to_string();
    let dtype = series.dtype().clone();
    let len = series.len();
    let mask_len = mask.len();
    // If lengths differ, use a truncated or extended mask to match series (e.g. literal broadcast).
    let mask_to_use: BooleanChunked = if mask_len == len {
        mask.clone()
    } else if mask_len > len && len == 1 {
        // Literal: series has 1 row, mask has N; treat as "all masked" for the single row then broadcast is caller's job.
        mask.clone()
    } else {
        mask.clone()
    };
    let masked = match dtype {
        DataType::Int64 => series
            .i64()
            .map_err(|e| compute_err("with_field mask i64", e))?
            .set(&mask_to_use, None)
            .map_err(|e| compute_err("with_field set i64", e))?
            .into_series(),
        DataType::Int32 => series
            .i32()
            .map_err(|e| compute_err("with_field mask i32", e))?
            .set(&mask_to_use, None)
            .map_err(|e| compute_err("with_field set i32", e))?
            .into_series(),
        DataType::String => {
            let ca = series
                .str()
                .map_err(|e| compute_err("with_field mask str", e))?;
            // Manual mask: output length = mask length; where mask is true use None (PySpark #1066).
            let n = mask_to_use.len();
            let mut opts: Vec<Option<String>> = Vec::with_capacity(n);
            for i in 0..n {
                let set_null = mask_to_use.get(i).unwrap_or(false);
                let val = ca
                    .get(if i < ca.len() { i } else { 0 })
                    .map(|s| s.to_string());
                opts.push(if set_null { None } else { val });
            }
            StringChunked::from_iter_options(name.as_str().into(), opts.into_iter()).into_series()
        }
        DataType::Float64 => series
            .f64()
            .map_err(|e| compute_err("with_field mask f64", e))?
            .set(&mask_to_use, None)
            .map_err(|e| compute_err("with_field set f64", e))?
            .into_series(),
        DataType::Boolean => series
            .bool()
            .map_err(|e| compute_err("with_field mask bool", e))?
            .set(&mask_to_use, None)
            .map_err(|e| compute_err("with_field set bool", e))?
            .into_series(),
        DataType::Unknown(uk) => {
            if let Some(dt) = uk.materialize() {
                let casted = series
                    .cast(&dt)
                    .map_err(|e| compute_err("with_field cast unknown", e))?;
                return series_set_nulls_where(&casted, mask);
            }
            series.clone()
        }
        DataType::Struct(_) => {
            // Preserve nested structs: mask each field recursively and rebuild (Issue #1263).
            use polars::chunked_array::StructChunked;
            let st = series
                .struct_()
                .map_err(|e| compute_err("with_field mask struct", e))?;
            let len = st.len();
            let fields_series = st.fields_as_series();
            let masked_fields: Vec<Series> = fields_series
                .iter()
                .map(|s| series_set_nulls_where(s, mask).unwrap_or_else(|_| s.clone()))
                .collect();
            let parent_null = st.is_null() | mask_to_use.clone();
            let outer_validity: polars_arrow::bitmap::Bitmap = (0..len)
                .map(|i| !parent_null.get(i).unwrap_or(false))
                .collect();
            let mut out =
                StructChunked::from_series(name.as_str().into(), len, masked_fields.iter())
                    .map_err(|e| compute_err("with_field rebuild struct", e))?;
            out.rechunk_mut();
            let out = out.with_outer_validity(Some(outer_validity));
            out.into_series()
        }
        DataType::List(_) => {
            // Preserve list type: do not cast to String; clone so nulls are not masked per-element.
            series.clone()
        }
        _ => {
            // Try casting to String so literal/other string-like types get masked (PySpark #1066).
            if let Ok(casted) = series.cast(&DataType::String) {
                return series_set_nulls_where(&casted, mask);
            }
            series.clone()
        }
    };
    Ok(masked.with_name(PlSmallStr::from(name.as_str())))
}

/// Replace or add a struct field (PySpark withField). Used when Polars 0.53+ no longer accepts "*" in with_fields.
/// When the whole struct is null, leaves result null (PySpark behavior; issue #1066).
pub fn apply_struct_with_field(
    struct_col: Column,
    value_col: Column,
    field_name: &str,
) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::StructChunked;
    let name = struct_col.field().into_owned().name;
    let struct_series = struct_col.take_materialized_series();
    let st = struct_series
        .struct_()
        .map_err(|e| compute_err("with_field: expected struct column", e))?;
    let len = st.len();
    let struct_null = struct_series.is_null();
    let fields_series = st.fields_as_series();
    let mut value_series = value_col.take_materialized_series();
    // Materialize Unknown (literal) so broadcast and mask work (PySpark #1066).
    if let DataType::Unknown(uk) = value_series.dtype() {
        if let Some(dt) = uk.materialize() {
            value_series = value_series
                .cast(&dt)
                .map_err(|e| compute_err("with_field materialize literal", e))?;
        }
    }
    // Broadcast literal (length 1) to match struct length so mask can be applied.
    if value_series.len() != len {
        let indices: Vec<u32> = (0..len).map(|_| 0u32).collect();
        let idx_ca = UInt32Chunked::from_vec("".into(), indices);
        value_series = value_series
            .take(&idx_ca)
            .map_err(|e| compute_err("with_field broadcast literal", e))?;
    }
    // Mask value by struct null: where struct is null, value must be null (PySpark #1066).
    // For string/unknown (literal) use a manual loop; other types use series_set_nulls_where.
    let value_masked: Series = match value_series.dtype() {
        DataType::String | DataType::Unknown(_) => {
            let value_str = if value_series.dtype() == &DataType::String {
                value_series.clone()
            } else {
                value_series
                    .cast(&DataType::String)
                    .unwrap_or_else(|_| value_series.clone())
            };
            let name_val = value_series.name().to_string();
            let mut opts: Vec<Option<String>> = Vec::with_capacity(len);
            if let Ok(ca) = value_str.str() {
                for i in 0..len {
                    let set_null = struct_null.get(i).unwrap_or(false);
                    let val = ca
                        .get(if i < ca.len() { i } else { 0 })
                        .map(|s| s.to_string());
                    opts.push(if set_null { None } else { val });
                }
                StringChunked::from_iter_options(name_val.as_str().into(), opts.into_iter())
                    .into_series()
            } else {
                value_series.clone()
            }
        }
        _ => series_set_nulls_where(&value_series, &struct_null)
            .unwrap_or_else(|_| value_series.clone()),
    };
    let mut new_fields: Vec<Series> = Vec::with_capacity(fields_series.len() + 1);
    let mut replaced = false;
    for s in &fields_series {
        let fname = s.name().as_str();
        let new_s = if fname == field_name {
            replaced = true;
            let mut v = value_masked.clone();
            v.rename(PlSmallStr::from(field_name));
            v
        } else {
            s.clone()
        };
        new_fields.push(new_s);
    }
    if !replaced {
        let mut v = value_masked.clone();
        v.rename(PlSmallStr::from(field_name));
        new_fields.push(v);
    }
    // Mask every field by struct_null so when input struct is null we get all-null struct (serializes as null).
    let new_fields: Vec<Series> = new_fields
        .into_iter()
        .map(|s| series_set_nulls_where(&s, &struct_null).unwrap_or(s))
        .collect();
    let outer_validity: polars_arrow::bitmap::Bitmap = (0..len)
        .map(|i| !struct_null.get(i).unwrap_or(false))
        .collect();
    let mut out = StructChunked::from_series(name.as_str().into(), len, new_fields.iter())
        .map_err(|e| compute_err("with_field: build struct", e))?;
    out.rechunk_mut();
    let out = out.with_outer_validity(Some(outer_validity));
    Ok(Some(Column::new(name, out.into_series())))
}

/// Random permutation of list elements (PySpark shuffle). Uses rand::seq::SliceRandom.
pub fn apply_shuffle(column: Column) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::builder::get_list_builder;
    use rand::seq::SliceRandom;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series.list().map_err(|e| compute_err("shuffle", e))?;
    let inner_dtype = list_ca.inner_dtype().clone();
    let mut builder = get_list_builder(&inner_dtype, 64, list_ca.len(), name.as_str().into());
    for opt_list in list_ca.amortized_iter() {
        match opt_list {
            None => builder.append_null(),
            Some(amort) => {
                let list_s = amort.as_ref();
                let n = list_s.len();
                let mut indices: Vec<u32> = (0..n as u32).collect();
                indices.shuffle(&mut rand::thread_rng());
                let idx_ca = UInt32Chunked::from_vec("".into(), indices);
                let taken = list_s
                    .take(&idx_ca)
                    .map_err(|e| compute_err("shuffle take", e))?;
                builder.append_series(&taken)?;
            }
        }
    }
    Ok(Some(Column::new(name, builder.finish().into_series())))
}

// --- Bitmap (PySpark 3.5+) ---

/// Size of bitmap in bytes (32768 bits).
pub(super) const BITMAP_BYTES: usize = 4096;

/// Parse string to datetime (micros since epoch). Accepts ISO and "YYYY-MM-DD HH:MM:SS", "YYYY-MM-DD" (midnight). #982.
pub(super) fn parse_str_to_datetime_micros(s: &str) -> Option<i64> {
    use chrono::{NaiveDate, NaiveDateTime};
    let s = s.trim();
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .or_else(|| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok())
        .or_else(|| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok())
        .or_else(|| {
            if s.len() >= 10 {
                NaiveDate::parse_from_str(&s[0..10], "%Y-%m-%d")
                    .ok()
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
            } else {
                None
            }
        })
        .map(|dt| dt.and_utc().timestamp_micros())
}

/// Parse string to date (PySpark cast string->date). Accepts "YYYY-MM-DD" and "YYYY-MM-DD HH:MM:SS" (truncates to date).
pub(super) fn parse_str_to_date(s: &str) -> Option<chrono::NaiveDate> {
    use chrono::{NaiveDate, NaiveDateTime};
    let s = s.trim();
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .or_else(|| {
            if s.len() >= 10 {
                NaiveDate::parse_from_str(&s[0..10], "%Y-%m-%d").ok()
            } else {
                None
            }
        })
        .or_else(|| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|dt| dt.date())
        })
}

/// Parse string to integer (Spark parity). Empty/whitespace/invalid -> None. Otherwise parse as i64.
/// #983: Also accept float-like strings (e.g. "3.14", "3.0") and truncate to int (PySpark cast str->int).
pub(super) fn parse_str_to_int(s: &str) -> Option<i64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    s.parse::<i64>()
        .ok()
        .or_else(|| s.parse::<f64>().ok().and_then(f64_to_int_trunc))
}

/// Truncate f64 to i64 for cast; None if out of range or NaN/Inf.
fn f64_to_int_trunc(f: f64) -> Option<i64> {
    if f.is_nan() || f.is_infinite() {
        return None;
    }
    let truncated = f.trunc();
    if truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
        Some(truncated as i64)
    } else {
        None
    }
}

pub(super) fn cast_decimal_to_integer(
    series: &Series,
    target: &DataType,
    strict: bool,
    try_cast: bool,
) -> PolarsResult<Series> {
    let DataType::Decimal(_, scale) = series.dtype() else {
        unreachable!("decimal cast helper requires a decimal input")
    };
    let (bits, target_name) = match target {
        DataType::Int16 => (16usize, "SMALLINT"),
        DataType::Int32 => (32usize, "INT"),
        DataType::Int64 => (64usize, "BIGINT"),
        _ => unreachable!("target is Int16, Int32, or Int64"),
    };
    let divisor = BigInt::from(10).pow(*scale as u32);
    let modulus = BigInt::from(1) << bits;
    let sign_bit = BigInt::from(1) << (bits - 1);
    let minimum = -&sign_bit;
    let maximum = &sign_bit - BigInt::from(1);
    let mut cast_values = Vec::with_capacity(series.len());

    for idx in 0..series.len() {
        let value = match series.get(idx)? {
            AnyValue::Null => None,
            AnyValue::Decimal(coefficient, _, _) | AnyValue::Int128(coefficient) => {
                Some(BigInt::from(coefficient) / &divisor)
            }
            other => {
                return Err(PolarsError::ComputeError(
                    format!("decimal to integer received unexpected value {other:?}").into(),
                ));
            }
        };
        let Some(value) = value else {
            cast_values.push(None);
            continue;
        };
        if strict && (value < minimum || value > maximum) {
            return Err(PolarsError::ComputeError(
                format!("[CAST_OVERFLOW] decimal value {value} overflows {target_name}").into(),
            ));
        }
        if try_cast && (value < minimum || value > maximum) {
            cast_values.push(None);
            continue;
        }
        let wrapped = ((&value % &modulus) + &modulus) % &modulus;
        let signed = if wrapped >= sign_bit {
            wrapped - &modulus
        } else {
            wrapped
        };
        cast_values.push(Some(signed));
    }

    match target {
        DataType::Int16 => {
            let values = cast_values
                .into_iter()
                .map(|value| value.map(|value| value.to_i16().expect("wrapped value fits i16")));
            Ok(Int16Chunked::from_iter_options(series.name().clone(), values).into_series())
        }
        DataType::Int32 => {
            let values = cast_values
                .into_iter()
                .map(|value| value.map(|value| value.to_i32().expect("wrapped value fits i32")));
            Ok(Int32Chunked::from_iter_options(series.name().clone(), values).into_series())
        }
        DataType::Int64 => {
            let values = cast_values
                .into_iter()
                .map(|value| value.map(|value| value.to_i64().expect("wrapped value fits i64")));
            Ok(Int64Chunked::from_iter_options(series.name().clone(), values).into_series())
        }
        _ => unreachable!("target is Int16, Int32, or Int64"),
    }
}
