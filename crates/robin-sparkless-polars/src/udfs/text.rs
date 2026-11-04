//! Text UDF implementations.

use super::*;

pub fn normalize_map_key_series(
    s: Series,
    disable_map_key_normalization: bool,
) -> PolarsResult<Series> {
    if disable_map_key_normalization {
        return Ok(s);
    }
    match s.dtype() {
        DataType::Float64 => {
            let ca = s.f64().map_err(|e| compute_err("normalize_map_key", e))?;
            let out: Float64Chunked = ca
                .into_iter()
                .map(|v| v.map(|x| if x == -0.0 { 0.0 } else { x }))
                .collect();
            Ok(out.into_series())
        }
        DataType::Float32 => {
            let ca = s.f32().map_err(|e| compute_err("normalize_map_key", e))?;
            let out: Float32Chunked = ca
                .into_iter()
                .map(|v| v.map(|x| if x == -0.0f32 { 0.0f32 } else { x }))
                .collect();
            Ok(out.into_series())
        }
        _ => Ok(s),
    }
}

/// Split string by regex pattern with at most `limit` parts (PySpark split uses regex).
/// When limit <= 0, splits without limit. Empty delimiter: literal split (existing behavior).
fn split_str_by_regex_limit(s: &str, re: &Regex, limit: usize) -> Vec<String> {
    if limit == 1 {
        return vec![s.to_string()];
    }
    if limit == 0 || limit == usize::MAX {
        return re.split(s).map(|p| p.to_string()).collect();
    }
    let mut result = Vec::with_capacity(limit);
    let mut last_end = 0;
    for (count, mat) in re.find_iter(s).enumerate() {
        if count >= limit - 1 {
            break;
        }
        result.push(s[last_end..mat.start()].to_string());
        last_end = mat.end();
    }
    result.push(s[last_end..].to_string());
    result
}

/// Split string by delimiter with at most `limit` parts; remainder in last part (PySpark split with limit).
/// Returns List(String). When limit <= 0, splits without limit.
/// Uses regex for non-empty pattern so "\\.\" and "\\|" work as in PySpark (#1215).
pub fn apply_split_with_limit(
    column: Column,
    delimiter: &str,
    limit: i32,
) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| compute_err("split_with_limit", e))?;
    let n = if limit <= 0 {
        usize::MAX
    } else {
        limit as usize
    };
    let re = if delimiter.is_empty() {
        None
    } else {
        Some(Regex::new(delimiter).map_err(|e| compute_err("split_with_limit pattern", e))?)
    };
    let values_capacity = ca.len().saturating_mul(64);
    let mut builder =
        ListStringChunkedBuilder::new(name.as_str().into(), ca.len(), values_capacity);
    for opt_s in ca.into_iter() {
        match opt_s {
            Some(s) => {
                if delimiter.is_empty() {
                    builder.append_values_iter(s.split(delimiter));
                } else {
                    let parts = split_str_by_regex_limit(s, re.as_ref().unwrap(), n);
                    builder.append_values_iter(parts.iter().map(String::as_str));
                }
            }
            None => builder.append_null(),
        }
    }
    let out = builder.finish();
    Ok(Some(Column::new(name, out.into_series())))
}

/// Split string by regex and return 1-based part (for split_part with regex delimiter).
pub fn apply_split_part_regex(
    column: Column,
    pattern: &str,
    part_num: i64,
) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| compute_err("split_part_regex", e))?;
    let re = Regex::new(pattern).map_err(|e| compute_err("split_part_regex pattern", e))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| {
                let parts: Vec<&str> = re.split(s).collect();
                let idx = if part_num > 0 {
                    (part_num - 1) as usize
                } else {
                    let n = parts.len() as i64;
                    (n + part_num) as usize
                };
                parts.get(idx).map(|&p| p.to_string()).unwrap_or_default()
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Repeat a string literal to match the length of a column (for input_file_name, etc.).
/// Polars lit() in select-only or empty-df contexts produces wrong row count; this fixes it.
pub fn apply_literal_string_repeat(column: Column, value: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let len = series.len();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        (0..len).map(|_| Some(value.to_string())),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Repeat each string by a per-row integer count (PySpark repeat).
pub fn apply_repeat_dynamic(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "repeat needs a string column and count column".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let value = std::mem::take(&mut columns[0]).take_materialized_series();
    let counts = std::mem::take(&mut columns[1])
        .take_materialized_series()
        .cast(&DataType::Int64)?;
    let values = value.str().map_err(|e| compute_err("repeat", e))?;
    let counts = counts.i64().map_err(|e| compute_err("repeat", e))?;
    let out_len =
        if (values.is_empty() && counts.len() <= 1) || (counts.is_empty() && values.len() <= 1) {
            0
        } else {
            values.len().max(counts.len())
        };
    if (values.len() != 1 && values.len() != out_len)
        || (counts.len() != 1 && counts.len() != out_len)
    {
        return Err(PolarsError::ShapeMismatch(
            "repeat: inputs must have equal lengths or be scalars".into(),
        ));
    }
    let value_at = |idx: usize| values.get(if values.len() == 1 { 0 } else { idx });
    let count_at = |idx: usize| counts.get(if counts.len() == 1 { 0 } else { idx });
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        (0..out_len).map(|idx| match (value_at(idx), count_at(idx)) {
            (Some(s), Some(n)) if n > 0 => usize::try_from(n).ok().and_then(|count| {
                // `String::repeat` rejects lengths above `isize::MAX`, which can be
                // smaller than `usize::MAX` on 64-bit targets. Check that limit here
                // so an untrusted count produces Spark's NULL rather than panicking.
                if !s.is_empty() && count > isize::MAX as usize / s.len() {
                    None
                } else {
                    Some(s.repeat(count))
                }
            }),
            (Some(_), Some(_)) => Some(String::new()),
            _ => None,
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// American Soundex code (4 chars). Matches PySpark soundex semantics.
/// Null/empty: PySpark returns null for null, '' for empty string (Phase 7 / test_soundex_null_and_empty).
fn soundex_one(s: &str) -> Cow<'_, str> {
    if s.is_empty() {
        return Cow::Borrowed("");
    }
    use soundex::american_soundex;
    let code = american_soundex(s);
    Cow::Owned(code.chars().take(4).collect::<String>())
}

/// PySpark parity: isnan on string (and other non-float) columns returns all False (no error).
pub fn apply_isnan_pyspark_parity(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let out = match series.dtype() {
        DataType::Float32 | DataType::Float64 => {
            // For numeric columns, PySpark treats only real NaN values as True; None/null is False.
            let bool_ca = series.is_nan().map_err(|e| compute_err("isnan", e))?;
            let out = BooleanChunked::from_iter_options(
                name.as_str().into(),
                bool_ca
                    .into_iter()
                    .map(|opt_b| Some(opt_b.unwrap_or(false))),
            );
            out.into_series()
        }
        // PySpark: isnan on string columns is always false (including the literal "NaN").
        DataType::String => BooleanChunked::from_iter_options(
            name.as_str().into(),
            (0..series.len()).map(|_| Some(false)),
        )
        .into_series(),
        _ => BooleanChunked::from_iter_options(
            name.as_str().into(),
            (0..series.len()).map(|_| Some(false)),
        )
        .into_series(),
    };
    Ok(Some(Column::new(name, out)))
}

/// Apply soundex to a string column; returns a new Column (Series).
pub fn apply_soundex(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("soundex", e))?;
    let out: StringChunked = ca.apply_values(soundex_one);
    Ok(Some(Column::new(name, out.into_series())))
}

/// Apply CRC32 to string bytes (PySpark crc32).
pub fn apply_crc32(column: Column) -> PolarsResult<Option<Column>> {
    use crc32fast::Hasher;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("crc32", e))?;
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

/// Spark XXH64 seed (PySpark xxhash64 uses 42).
const XXH64_SEED: u64 = 42;

/// Apply XXH64 hash (PySpark xxhash64). Uses seed 42 to match Spark.
pub fn apply_xxhash64(column: Column) -> PolarsResult<Option<Column>> {
    use std::hash::Hasher;
    use twox_hash::XxHash64;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("xxhash64", e))?;
    // PySpark xxhash64 returns seed (42) for NULL input (#1146).
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            Some(
                opt_s
                    .map(|s| {
                        let mut hasher = XxHash64::with_seed(XXH64_SEED);
                        hasher.write(s.as_bytes());
                        hasher.finish() as i64
                    })
                    .unwrap_or(42),
            )
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Title case: first letter of each word uppercase, rest lowercase (PySpark initcap).
fn initcap_str(s: &str) -> Cow<'_, str> {
    let mut out = String::with_capacity(s.len());
    let mut word_start = true;
    for c in s.chars() {
        if c.is_alphabetic() || c.is_numeric() {
            if word_start {
                out.extend(c.to_uppercase());
                word_start = false;
            } else {
                out.extend(c.to_lowercase());
            }
        } else {
            out.push(c);
            word_start = true;
        }
    }
    Cow::Owned(out)
}

/// Apply initcap (PySpark initcap).
pub fn apply_initcap(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("initcap", e))?;
    let out = ca.apply_values(initcap_str);
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
    let a_ca = a_series.str().map_err(|e| compute_err("levenshtein", e))?;
    let b_ca = b_series.str().map_err(|e| compute_err("levenshtein", e))?;
    let len_a = a_ca.len();
    let len_b = b_ca.len();
    let len = len_a.max(len_b);
    // Broadcast constant side (e.g. levenshtein(col, lit("Alice"))) so output row count matches the longer column.
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        (0..len).map(|i| {
            let ia = if len_a == 1 { 0 } else { i };
            let ib = if len_b == 1 { 0 } else { i };
            let a = a_ca.get(ia);
            let b = b_ca.get(ib);
            match (a, b) {
                (Some(a), Some(b)) => Some(levenshtein(a, b) as i64),
                _ => None,
            }
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}
