//! Math UDF implementations.

use super::*;

// --- Math (trig, degrees, radians, signum, pow) ---

/// PySpark-style power: base^exp with float exponent and 0^positive=0 (#817 fractional exponent, #863 zero base).
/// Broadcasts scalar (length-1) base or exponent to match the other column's length.
pub fn apply_pow_pyspark(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "pow_pyspark needs base and exponent columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let base_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let exp_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let base_ca = float_series_to_f64(&base_series)?;
    let exp_ca = float_series_to_f64(&exp_series)?;
    let scalar_exp = (exp_ca.len() == 1).then(|| exp_ca.get(0));
    let scalar_base = (base_ca.len() == 1).then(|| base_ca.get(0));
    let out = if let Some(Some(e)) = scalar_exp {
        // Broadcast exponent to match base length
        Float64Chunked::from_iter_options(
            name.as_str().into(),
            base_ca.into_iter().map(|b| {
                let b = b?;
                let result = if b == 0.0 {
                    if e > 0.0 {
                        0.0
                    } else if e == 0.0 {
                        1.0
                    } else {
                        f64::INFINITY
                    }
                } else {
                    b.powf(e)
                };
                Some(result)
            }),
        )
    } else if let Some(Some(b)) = scalar_base {
        // Broadcast base to match exponent length
        Float64Chunked::from_iter_options(
            name.as_str().into(),
            exp_ca.into_iter().map(|e| {
                let e = e?;
                let result = if b == 0.0 {
                    if e > 0.0 {
                        0.0
                    } else if e == 0.0 {
                        1.0
                    } else {
                        f64::INFINITY
                    }
                } else {
                    b.powf(e)
                };
                Some(result)
            }),
        )
    } else {
        Float64Chunked::from_iter_options(
            name.as_str().into(),
            base_ca.into_iter().zip(&exp_ca).map(|(b, e)| {
                let (b, e): (f64, f64) = match (b, e) {
                    (Some(bb), Some(ee)) => (bb, ee),
                    _ => return None,
                };
                let result = if b == 0.0 {
                    if e > 0.0 {
                        0.0
                    } else if e == 0.0 {
                        1.0
                    } else {
                        f64::INFINITY
                    }
                } else {
                    b.powf(e)
                };
                Some(result)
            }),
        )
    };
    Ok(Some(Column::new(name, out.into_series())))
}

pub(super) fn float_series_to_f64(series: &Series) -> PolarsResult<Float64Chunked> {
    if series.dtype() == &DataType::String {
        // PySpark parity (#272): strip whitespace then parse (cast does not strip).
        let ca = series
            .str()
            .map_err(|e| compute_err("float_series_to_f64", e))?;
        let name = series.name().as_str().into();
        let results: Vec<Option<f64>> = ca
            .into_iter()
            .map(|opt_s| opt_s.and_then(parse_str_to_double))
            .collect();
        return Ok(Float64Chunked::from_iter_options(name, results.into_iter()));
    }
    let casted = series.cast(&DataType::Float64)?;
    casted
        .f64()
        .map_err(|e| compute_err("float_series_to_f64", e))
        .cloned()
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

/// Banker's rounding: round half to even (PySpark bround).
fn bround_one(x: f64, scale: i32) -> f64 {
    if x.is_nan() || x.is_infinite() {
        return x;
    }
    let factor = 10_f64.powi(scale);
    let scaled = x * factor;
    let rounded =
        if scaled.fract().abs() > 0.5_f64 - 1e-10 && scaled.fract().abs() < 0.5_f64 + 1e-10 {
            // Half case: round to nearest even
            let floor_val = scaled.trunc();
            if floor_val as i64 % 2 == 0 {
                floor_val
            } else {
                floor_val + scaled.signum()
            }
        } else {
            scaled.round()
        };
    rounded / factor
}

/// Convert number string from one base to another (PySpark conv).
fn conv_one(s: &str, from_base: i32, to_base: i32) -> Option<String> {
    if !(2..=36).contains(&from_base) || !(2..=36).contains(&to_base) {
        return None;
    }
    let s_trim = s.trim();
    if s_trim.is_empty() {
        return None;
    }
    let n = i64::from_str_radix(s_trim, from_base as u32).ok()?;
    let to_b = to_base as u32;
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return Some("0".to_string());
    }
    let mut result = String::new();
    let mut val = if n < 0 {
        result.push('-');
        n.unsigned_abs()
    } else {
        n as u64
    };
    let mut buf = String::new();
    while val > 0 {
        buf.push(DIGITS[(val % to_b as u64) as usize] as char);
        val /= to_b as u64;
    }
    result.push_str(&buf.chars().rev().collect::<String>());
    Some(result)
}

/// Apply conv (base conversion). String: parse from from_base, format in to_base. Int: format value in to_base.
pub fn apply_conv(column: Column, from_base: i32, to_base: i32) -> PolarsResult<Option<Column>> {
    use std::borrow::Cow;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out = if series.dtype() == &DataType::String {
        let ca = series.str().map_err(|e| compute_err("conv", e))?;
        ca.apply(|opt_s| opt_s.and_then(|s| conv_one(s, from_base, to_base).map(Cow::Owned)))
            .into_series()
    } else if series.dtype() == &DataType::Int64 {
        let ca = series.i64().map_err(|e| compute_err("conv", e))?;
        let to_b = to_base as u32;
        const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
        let format_int = |n: i64| -> Option<String> {
            if !(2..=36).contains(&to_b) {
                return None;
            }
            if n == 0 {
                return Some("0".to_string());
            }
            let mut result = String::new();
            let mut val = if n < 0 {
                result.push('-');
                n.unsigned_abs()
            } else {
                n as u64
            };
            let mut buf = String::new();
            while val > 0 {
                buf.push(DIGITS[(val % to_b as u64) as usize] as char);
                val /= to_b as u64;
            }
            result.push_str(&buf.chars().rev().collect::<String>());
            Some(result)
        };
        let out_ca = StringChunked::from_iter_options(
            name.as_str().into(),
            ca.into_iter().map(|opt| opt.and_then(format_int)),
        );
        out_ca.into_series()
    } else {
        let s_str = series.cast(&DataType::String)?;
        let ca = s_str
            .str()
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        ca.apply(|opt_s| opt_s.and_then(|s| conv_one(s, from_base, to_base).map(Cow::Owned)))
            .into_series()
    };
    Ok(Some(Column::new(name, out)))
}

/// Apply hex: integer or string to hex string (PySpark hex).
pub fn apply_hex(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out = if series.dtype() == &DataType::Int64 || series.dtype() == &DataType::Int32 {
        let s = series.cast(&DataType::Int64)?;
        let ca = s
            .i64()
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let out_ca = StringChunked::from_iter_options(
            name.as_str().into(),
            ca.into_iter().map(|opt| opt.map(|n| format!("{n:X}"))),
        );
        out_ca.into_series()
    } else if series.dtype() == &DataType::String {
        let ca = series
            .str()
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let out_ca = StringChunked::from_iter_options(
            name.as_str().into(),
            ca.into_iter().map(|opt| {
                opt.map(|s| {
                    s.as_bytes()
                        .iter()
                        .map(|b| format!("{b:02X}"))
                        .collect::<String>()
                })
            }),
        );
        out_ca.into_series()
    } else {
        let s = series.cast(&DataType::Int64)?;
        let ca = s
            .i64()
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let out_ca = StringChunked::from_iter_options(
            name.as_str().into(),
            ca.into_iter().map(|opt| opt.map(|n| format!("{n:X}"))),
        );
        out_ca.into_series()
    };
    Ok(Some(Column::new(name, out)))
}

/// Apply unhex: hex string to binary/string (PySpark unhex).
pub fn apply_unhex(column: Column) -> PolarsResult<Option<Column>> {
    use std::borrow::Cow;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("unhex", e))?;
    let unhex_one = |s: &str| -> Option<Vec<u8>> {
        let s = s.trim();
        let chars: Vec<char> = if s.len() % 2 == 1 {
            s.chars().skip(1).collect()
        } else {
            s.chars().collect()
        };
        let mut bytes = Vec::with_capacity(chars.len() / 2);
        for chunk in chars.chunks(2) {
            let hex_pair: String = chunk.iter().collect();
            let byte = u8::from_str_radix(&hex_pair, 16).ok()?;
            bytes.push(byte);
        }
        Some(bytes)
    };
    let out = ca
        .apply(|opt_s| {
            opt_s.and_then(|s| {
                unhex_one(s)
                    .and_then(|b| String::from_utf8(b).ok())
                    .map(Cow::Owned)
            })
        })
        .into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply bin: integer to binary string (PySpark bin).
pub fn apply_bin(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let s = series.cast(&DataType::Int64)?;
    let ca = s
        .i64()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let out_ca = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt| opt.map(|n| format!("{n:b}"))),
    );
    Ok(Some(Column::new(name, out_ca.into_series())))
}

/// Apply getbit: get bit at 0-based position (PySpark getbit).
pub fn apply_getbit(column: Column, pos: i64) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let s = series.cast(&DataType::Int64)?;
    let ca = s
        .i64()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let pos = pos.max(0);
    let out_ca = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt| opt.map(|n| (n >> pos) & 1)),
    );
    Ok(Some(Column::new(name, out_ca.into_series())))
}

/// Apply bit_count: count set bits in integer (PySpark bit_count).
pub fn apply_bit_count(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let s = series.cast(&DataType::Int64)?;
    let ca = s
        .i64()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let out_ca = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt| opt.map(|n| i64::from(n.count_ones()))),
    );
    Ok(Some(Column::new(name, out_ca.into_series())))
}

/// Assert that all boolean values are true (PySpark assert_true).
/// PySpark: returns null when input is true; throws when input is false or null.
/// When err_msg is Some, it is used in the error message when assertion fails.
pub fn apply_assert_true(column: Column, err_msg: Option<&str>) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .bool()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let len = ca.len();
    // PySpark: fail on false or null
    let failed = ca.into_iter().any(|opt| match opt {
        Some(true) => false,
        Some(false) | None => true,
    });
    if failed {
        let msg = err_msg
            .map(String::from)
            .unwrap_or_else(|| format!("assert_true failed on column '{name}'"));
        return Err(PolarsError::ComputeError(msg.into()));
    }
    // PySpark: return null on success
    let null_col = BooleanChunked::from_iter_options(name.as_str().into(), (0..len).map(|_| None));
    Ok(Some(Column::new(name, null_col.into_series())))
}

/// Apply rand: uniform [0, 1) per row, with optional seed (PySpark rand).
pub fn apply_rand_with_seed(column: Column, seed: Option<u64>) -> PolarsResult<Option<Column>> {
    use rand::Rng;
    use rand::SeedableRng;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let n = series.len();
    let values: Vec<f64> = if let Some(s) = seed {
        let mut rng = rand::rngs::StdRng::seed_from_u64(s);
        (0..n).map(|_| rng.r#gen::<f64>()).collect()
    } else {
        let mut rng = rand::thread_rng();
        (0..n).map(|_| rng.r#gen::<f64>()).collect()
    };
    let out = Float64Chunked::from_vec(name.as_str().into(), values);
    Ok(Some(Column::new(name, out.into_series())))
}

/// Apply randn: standard normal per row, with optional seed (PySpark randn).
pub fn apply_randn_with_seed(column: Column, seed: Option<u64>) -> PolarsResult<Option<Column>> {
    use rand::SeedableRng;
    use rand_distr::Distribution;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let n = series.len();
    let dist = rand_distr::StandardNormal;
    let values: Vec<f64> = if let Some(s) = seed {
        let mut rng = rand::rngs::StdRng::seed_from_u64(s);
        (0..n).map(|_| dist.sample(&mut rng)).collect()
    } else {
        let mut rng = rand::thread_rng();
        (0..n).map(|_| dist.sample(&mut rng)).collect()
    };
    let out = Float64Chunked::from_vec(name.as_str().into(), values);
    Ok(Some(Column::new(name, out.into_series())))
}

/// Build a Series of `n` uniform [0, 1) values with optional seed (for with_column PySpark-like rand).
pub fn series_rand_n(name: &str, n: usize, seed: Option<u64>) -> Series {
    use rand::Rng;
    use rand::SeedableRng;
    let values: Vec<f64> = if let Some(s) = seed {
        let mut rng = rand::rngs::StdRng::seed_from_u64(s);
        (0..n).map(|_| rng.r#gen::<f64>()).collect()
    } else {
        let mut rng = rand::thread_rng();
        (0..n).map(|_| rng.r#gen::<f64>()).collect()
    };
    Float64Chunked::from_vec(name.into(), values).into_series()
}

/// Build a Series of `n` standard normal values with optional seed (for with_column PySpark-like randn).
pub fn series_randn_n(name: &str, n: usize, seed: Option<u64>) -> Series {
    use rand::SeedableRng;
    use rand_distr::Distribution;
    let dist = rand_distr::StandardNormal;
    let values: Vec<f64> = if let Some(s) = seed {
        let mut rng = rand::rngs::StdRng::seed_from_u64(s);
        (0..n).map(|_| dist.sample(&mut rng)).collect()
    } else {
        let mut rng = rand::thread_rng();
        (0..n).map(|_| dist.sample(&mut rng)).collect()
    };
    Float64Chunked::from_vec(name.into(), values).into_series()
}

/// Apply bitwise AND for two integer columns (PySpark bit_and).
pub fn apply_bit_and(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "bit_and needs two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_cast = a_series.cast(&DataType::Int64)?;
    let b_cast = b_series.cast(&DataType::Int64)?;
    let a = a_cast
        .i64()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let b = b_cast
        .i64()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        a.into_iter().zip(b).map(|(av, bv)| match (av, bv) {
            (Some(x), Some(y)) => Some(x & y),
            _ => None,
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Apply bitwise OR for two integer columns (PySpark bit_or).
pub fn apply_bit_or(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError("bit_or needs two columns".into()));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_cast = a_series.cast(&DataType::Int64)?;
    let b_cast = b_series.cast(&DataType::Int64)?;
    let a = a_cast
        .i64()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let b = b_cast
        .i64()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        a.into_iter().zip(b).map(|(av, bv)| match (av, bv) {
            (Some(x), Some(y)) => Some(x | y),
            _ => None,
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Coerce column to Int64 for bitwise_not. Handles Unknown(Any) from when/otherwise (#859).
pub fn apply_coerce_to_int64_for_bitwise(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out = series.cast(&DataType::Int64).or_else(|_| {
        Series::new_null(PlSmallStr::from(name.as_str()), series.len()).cast(&DataType::Int64)
    })?;
    Ok(Some(Column::new(name, out)))
}

/// Apply bitwise XOR for two integer columns (PySpark bit_xor).
pub fn apply_bit_xor(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "bit_xor needs two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_cast = a_series.cast(&DataType::Int64)?;
    let b_cast = b_series.cast(&DataType::Int64)?;
    let a = a_cast
        .i64()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let b = b_cast
        .i64()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        a.into_iter().zip(b).map(|(av, bv)| match (av, bv) {
            (Some(x), Some(y)) => Some(x ^ y),
            _ => None,
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Apply round to given scale (decimal places). scale can be negative (e.g. -3 => round to thousands).
/// Supports numeric and string columns (PySpark parity). Fixes #869: round(col, -3) no longer triggers integral conversion error.
pub fn apply_round(column: Column, scale: i32) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let factor = 10_f64.powi(scale);
    let out = ca
        .apply_values(|x| (x * factor).round() / factor)
        .into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply bround (banker's rounding) to a float column.
pub fn apply_bround(column: Column, scale: i32) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(|x| bround_one(x, scale)).into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply cot (1/tan) to a float column.
pub fn apply_cot(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(|x| 1.0 / x.tan()).into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply csc (1/sin) to a float column.
pub fn apply_csc(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(|x| 1.0 / x.sin()).into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply sec (1/cos) to a float column.
pub fn apply_sec(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(|x| 1.0 / x.cos()).into_series();
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
    let out = y_ca.into_iter().zip(&x_ca).map(|(oy, ox)| match (oy, ox) {
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
    let out = ca.apply_values(|r| r.to_degrees()).into_series();
    Ok(Some(Column::new(name, out)))
}

/// Apply radians (degrees -> radians) to a float column.
pub fn apply_radians(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(|d| d.to_radians()).into_series();
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

/// Hyperbolic and inverse hyperbolic / extra math.
pub fn apply_cosh(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::cosh).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_sinh(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::sinh).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_tanh(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::tanh).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_acosh(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::acosh).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_asinh(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::asinh).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_atanh(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::atanh).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_cbrt(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::cbrt).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_expm1(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::exp_m1).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_log1p(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::ln_1p).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_log10(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::log10).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_log2(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::log2).into_series();
    Ok(Some(Column::new(name, out)))
}
pub fn apply_rint(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = float_series_to_f64(&series)?;
    let out = ca.apply_values(f64::round).into_series();
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
                a.into_iter().zip(&b).map(|(oa, ob)| match (oa, ob) {
                    (Some(x), Some(y)) => Some(x.max(y)),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }),
            );
            out.into_series()
        }
        (DataType::Int64, _)
        | (DataType::Int32, _)
        | (_, DataType::Int64)
        | (_, DataType::Int32) => {
            let a = a_series.cast(&DataType::Int64)?;
            let b = b_series.cast(&DataType::Int64)?;
            let ca_a = a.i64().map_err(|e| compute_err("greatest", e))?;
            let ca_b = b.i64().map_err(|e| compute_err("greatest", e))?;
            let out = Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter().zip(ca_b).map(|(oa, ob)| match (oa, ob) {
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
            let ca_a = a.str().map_err(|e| compute_err("greatest", e))?;
            let ca_b = b.str().map_err(|e| compute_err("greatest", e))?;
            let out = StringChunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter().zip(ca_b).map(|(oa, ob)| match (oa, ob) {
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
                a.into_iter().zip(&b).map(|(oa, ob)| match (oa, ob) {
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
                a.into_iter().zip(&b).map(|(oa, ob)| match (oa, ob) {
                    (Some(x), Some(y)) => Some(x.min(y)),
                    (Some(x), None) => Some(x),
                    (None, Some(y)) => Some(y),
                    (None, None) => None,
                }),
            );
            out.into_series()
        }
        (DataType::Int64, _)
        | (DataType::Int32, _)
        | (_, DataType::Int64)
        | (_, DataType::Int32) => {
            let a = a_series.cast(&DataType::Int64)?;
            let b = b_series.cast(&DataType::Int64)?;
            let ca_a = a.i64().map_err(|e| compute_err("least", e))?;
            let ca_b = b.i64().map_err(|e| compute_err("least", e))?;
            let out = Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter().zip(ca_b).map(|(oa, ob)| match (oa, ob) {
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
            let ca_a = a.str().map_err(|e| compute_err("least", e))?;
            let ca_b = b.str().map_err(|e| compute_err("least", e))?;
            let out = StringChunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter().zip(ca_b).map(|(oa, ob)| match (oa, ob) {
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
                a.into_iter().zip(&b).map(|(oa, ob)| match (oa, ob) {
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
