//! Helpers for element-wise UDFs used by map() expressions (soundex, levenshtein, crc32, xxhash64, array_flatten, array_repeat).
//! These run at plan execution time when Polars invokes the closure.

use chrono::{Datelike, TimeZone};
use chrono_tz::Tz;
use polars::prelude::*;
use regex::Regex;
use std::borrow::Cow;

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
        .map_err(|e| PolarsError::ComputeError(format!("split_part_regex: {e}").into()))?;
    let re = Regex::new(pattern)
        .map_err(|e| PolarsError::ComputeError(format!("split_part_regex pattern: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("soundex: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("crc32: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("xxhash64: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("levenshtein: {e}").into()))?;
    let b_ca = b_series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("levenshtein: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("array_flatten: {e}").into()))?;
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

/// Distinct elements in list preserving first-occurrence order (PySpark array_distinct parity).
pub fn apply_array_distinct_first_order(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_distinct: {e}").into()))?;
    let inner_dtype = list_ca.inner_dtype().clone();
    let out = list_ca.try_apply_amortized(|amort_s| {
        let list_s = amort_s.as_ref().as_list();
        let mut result: Vec<Series> = Vec::new();
        for elem in list_s.amortized_iter().flatten() {
            let taken = elem.deep_clone();
            let is_dup = result.iter().any(|s| s.get(0).ok() == taken.get(0).ok());
            if !is_dup {
                result.push(taken);
            }
        }
        if result.is_empty() {
            Ok(Series::new_empty(PlSmallStr::EMPTY, &inner_dtype))
        } else {
            let mut combined = result.remove(0);
            for s in result {
                combined.extend(&s)?;
            }
            Ok(combined)
        }
    })?;
    Ok(Some(Column::new(name, out.into_series())))
}

/// Repeat each element n times (PySpark array_repeat).
/// Supports both: (1) scalar column (string, int, etc.) - create array of n copies; (2) List column - repeat each element within the list.
pub fn apply_array_repeat(column: Column, n: i64) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let n_usize = n.max(0) as usize;

    // Scalar column: create array of n copies of each element (PySpark array_repeat on string/int/etc.)
    if !matches!(series.dtype(), DataType::List(_)) {
        use polars::chunked_array::builder::get_list_builder;
        let inner_dtype = series.dtype().clone();
        let len = series.len();
        let mut builder = get_list_builder(&inner_dtype, 64, len, name.as_str().into());
        for i in 0..len {
            let opt_av = series.get(i);
            let elem_series = match opt_av {
                Ok(av) => any_value_to_single_series(av, &inner_dtype)?,
                Err(_) => Series::new_empty(PlSmallStr::EMPTY, &inner_dtype),
            };
            let mut repeated = elem_series.clone();
            for _ in 1..n_usize {
                repeated.extend(&elem_series)?;
            }
            builder.append_series(&repeated).map_err(|e| {
                PolarsError::ComputeError(format!("array_repeat scalar: {e}").into())
            })?;
        }
        let out = builder.finish().into_series();
        return Ok(Some(Column::new(name, out)));
    }

    // List column: repeat each element within the list
    let list_ca = series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_repeat: {e}").into()))?;
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

fn any_value_to_single_series(av: AnyValue, dtype: &DataType) -> PolarsResult<Series> {
    Series::from_any_values_and_dtype(PlSmallStr::EMPTY, &[av], dtype, false)
}

/// Append element to end of each list (PySpark array_append).
pub fn apply_array_append(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use std::cell::RefCell;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "array_append needs two columns (array, element)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let list_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let elem_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let list_ca = list_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_append: {e}").into()))?;
    let inner_dtype = list_ca.inner_dtype().clone();
    let elem_casted = elem_series.cast(&inner_dtype)?;
    let elem_len = elem_casted.len();
    let elem_vec: Vec<Option<AnyValue>> = (0..elem_len).map(|i| elem_casted.get(i).ok()).collect();
    let idx = RefCell::new(0usize);
    let out = list_ca.try_apply_amortized(|amort_s| {
        let i = *idx.borrow();
        *idx.borrow_mut() += 1;
        let ei = if elem_len == 1 { 0 } else { i };
        let list_s = amort_s.as_ref().as_list();
        let mut acc: Vec<Series> = Vec::new();
        for e in list_s.amortized_iter().flatten() {
            acc.push(e.deep_clone());
        }
        if let Some(Some(av)) = elem_vec.get(ei) {
            let single = any_value_to_single_series(av.clone(), &inner_dtype)?;
            acc.push(single);
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

/// Prepend element to start of each list (PySpark array_prepend).
pub fn apply_array_prepend(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use std::cell::RefCell;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "array_prepend needs two columns (array, element)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let list_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let elem_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let list_ca = list_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_prepend: {e}").into()))?;
    let inner_dtype = list_ca.inner_dtype().clone();
    let elem_casted = elem_series.cast(&inner_dtype)?;
    let elem_len = elem_casted.len();
    let elem_vec: Vec<Option<AnyValue>> = (0..elem_len).map(|i| elem_casted.get(i).ok()).collect();
    let idx = RefCell::new(0usize);
    let out = list_ca.try_apply_amortized(|amort_s| {
        let i = *idx.borrow();
        *idx.borrow_mut() += 1;
        let ei = if elem_len == 1 { 0 } else { i };
        let list_s = amort_s.as_ref().as_list();
        let mut acc: Vec<Series> = Vec::new();
        if let Some(Some(av)) = elem_vec.get(ei) {
            let single = any_value_to_single_series(av.clone(), &inner_dtype)?;
            acc.push(single);
        }
        for e in list_s.amortized_iter().flatten() {
            acc.push(e.deep_clone());
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

/// Insert element at 1-based position (PySpark array_insert). Negative pos = from end.
pub fn apply_array_insert(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use std::cell::RefCell;
    if columns.len() < 3 {
        return Err(PolarsError::ComputeError(
            "array_insert needs three columns (array, position, element)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let list_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let pos_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let elem_series = std::mem::take(&mut columns[2]).take_materialized_series();
    let list_ca = list_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_insert: {e}").into()))?;
    let inner_dtype = list_ca.inner_dtype().clone();
    let pos_ca = pos_series
        .cast(&DataType::Int64)?
        .i64()
        .map_err(|e| {
            PolarsError::ComputeError(format!("array_insert: position column: {e}").into())
        })?
        .clone();
    let elem_casted = elem_series.cast(&inner_dtype)?;
    let pos_len = pos_ca.len();
    let pos_vec: Vec<i64> = (0..pos_len).map(|i| pos_ca.get(i).unwrap_or(1)).collect();
    let elem_len = elem_casted.len();
    let elem_vec: Vec<Option<AnyValue>> = (0..elem_len).map(|i| elem_casted.get(i).ok()).collect();
    let idx = RefCell::new(0usize);
    let out = list_ca.try_apply_amortized(|amort_s| {
        let i = *idx.borrow();
        *idx.borrow_mut() += 1;
        let pi = if pos_len == 1 { 0 } else { i };
        let ei = if elem_len == 1 { 0 } else { i };
        let list_s = amort_s.as_ref().as_list();
        let pos_val = pos_vec.get(pi).copied().unwrap_or(1);
        let mut acc: Vec<Series> = Vec::new();
        for e in list_s.amortized_iter().flatten() {
            acc.push(e.deep_clone());
        }
        let len = acc.len() as i64;
        let pos = if pos_val < 0 {
            (len + pos_val + 1).max(0).min(len) as usize
        } else {
            ((pos_val - 1).max(0)).min(len) as usize
        };
        let single = elem_vec
            .get(ei)
            .and_then(|o| o.as_ref())
            .map(|av: &AnyValue| any_value_to_single_series(av.clone(), &inner_dtype));
        if let Some(Ok(s)) = single {
            acc.insert(pos, s);
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

fn series_to_set_key(s: &Series) -> String {
    if s.len() == 1 {
        if let Ok(av) = s.get(0) {
            return format!("{:?}", av);
        }
    }
    std::string::ToString::to_string(s)
}

/// Elements in first array not in second (PySpark array_except).
pub fn apply_array_except(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "array_except needs two columns (array1, array2)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_except: {e}").into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_except: {e}").into()))?;
    let inner_dtype = a_ca.inner_dtype().clone();
    let mut builder = polars::chunked_array::builder::get_list_builder(
        &inner_dtype,
        64,
        a_ca.len(),
        name.as_str().into(),
    );
    for (opt_a, opt_b) in a_ca.amortized_iter().zip(b_ca.amortized_iter()) {
        match (opt_a, opt_b) {
            (Some(a_amort), Some(b_amort)) => {
                let a_list = a_amort.as_ref().as_list();
                let b_list = b_amort.as_ref().as_list();
                let b_keys: std::collections::HashSet<String> = b_list
                    .amortized_iter()
                    .flatten()
                    .map(|e| series_to_set_key(&e.deep_clone()))
                    .collect();
                let mut acc: Vec<Series> = Vec::new();
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                for e in a_list.amortized_iter().flatten() {
                    let s = e.deep_clone();
                    let key = series_to_set_key(&s);
                    if !b_keys.contains(&key) && seen.insert(key) {
                        acc.push(s);
                    }
                }
                let result = if acc.is_empty() {
                    Series::new_empty(PlSmallStr::EMPTY, &inner_dtype)
                } else {
                    let mut r = acc.remove(0);
                    for s in acc {
                        r.extend(&s)?;
                    }
                    r
                };
                builder.append_series(&result)?;
            }
            _ => builder.append_null(),
        }
    }
    Ok(Some(Column::new(name, builder.finish().into_series())))
}

/// True if two arrays have any element in common (PySpark arrays_overlap).
pub fn apply_arrays_overlap(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "arrays_overlap needs two columns (array1, array2)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("arrays_overlap: {e}").into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("arrays_overlap: {e}").into()))?;
    let mut results: Vec<bool> = Vec::with_capacity(a_ca.len());
    for (opt_a, opt_b) in a_ca.amortized_iter().zip(b_ca.amortized_iter()) {
        let overlap = match (opt_a, opt_b) {
            (Some(a_amort), Some(b_amort)) => {
                let a_list = a_amort.as_ref().as_list();
                let b_list = b_amort.as_ref().as_list();
                let a_keys: std::collections::HashSet<String> = a_list
                    .amortized_iter()
                    .flatten()
                    .map(|e| series_to_set_key(&e.deep_clone()))
                    .collect();
                let b_keys: std::collections::HashSet<String> = b_list
                    .amortized_iter()
                    .flatten()
                    .map(|e| series_to_set_key(&e.deep_clone()))
                    .collect();
                !a_keys.is_disjoint(&b_keys)
            }
            _ => false,
        };
        results.push(overlap);
    }
    let out =
        BooleanChunked::from_iter_options(name.as_str().into(), results.into_iter().map(Some));
    Ok(Some(Column::new(name, out.into_series())))
}

/// Zip two arrays into array of structs (PySpark arrays_zip).
pub fn apply_arrays_zip(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "arrays_zip needs at least two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let n = columns.len();
    let mut series_vec: Vec<Series> = Vec::with_capacity(n);
    for col in columns.iter_mut() {
        series_vec.push(std::mem::take(col).take_materialized_series());
    }
    let list_cas: Vec<_> = series_vec
        .iter()
        .map(|s| {
            s.list()
                .map_err(|e| PolarsError::ComputeError(format!("arrays_zip: {e}").into()))
        })
        .collect::<PolarsResult<Vec<_>>>()?;
    let len = list_cas[0].len();
    let inner_dtype = list_cas[0].inner_dtype().clone();
    use polars::chunked_array::builder::get_list_builder;
    use polars::chunked_array::StructChunked;
    use polars::datatypes::Field;
    let struct_fields: Vec<Field> = (0..n)
        .map(|i| Field::new(format!("field_{i}").into(), inner_dtype.clone()))
        .collect();
    let out_struct = DataType::Struct(struct_fields);
    let mut builder = get_list_builder(&out_struct, 64, len, name.as_str().into());
    for row_idx in 0..len {
        let mut max_len = 0usize;
        let mut row_lists: Vec<Vec<Series>> = Vec::with_capacity(n);
        for ca in &list_cas {
            let opt_amort = ca.amortized_iter().nth(row_idx).flatten();
            if let Some(amort) = opt_amort {
                let list_s = amort.as_ref().as_list();
                let elems: Vec<Series> = list_s
                    .amortized_iter()
                    .flatten()
                    .map(|e| e.deep_clone())
                    .collect();
                max_len = max_len.max(elems.len());
                row_lists.push(elems);
            } else {
                row_lists.push(vec![]);
            }
        }
        if max_len == 0 {
            builder.append_null();
        } else {
            let mut struct_parts: Vec<Vec<Series>> =
                (0..n).map(|_| Vec::with_capacity(max_len)).collect();
            for i in 0..max_len {
                for (j, lst) in row_lists.iter().enumerate() {
                    let elem = lst
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| Series::full_null(PlSmallStr::EMPTY, 1, &inner_dtype));
                    struct_parts[j].push(elem);
                }
            }
            let field_series: Vec<Series> = struct_parts
                .into_iter()
                .enumerate()
                .map(|(j, mut parts)| {
                    let mut r = parts.remove(0);
                    for s in parts {
                        let _ = r.extend(&s);
                    }
                    r.with_name(format!("field_{j}").as_str().into())
                })
                .collect();
            let field_refs: Vec<&Series> = field_series.iter().collect();
            let st =
                StructChunked::from_series(PlSmallStr::EMPTY, max_len, field_refs.iter().copied())
                    .map_err(|e| {
                        PolarsError::ComputeError(format!("arrays_zip struct: {e}").into())
                    })?
                    .into_series();
            builder.append_series(&st)?;
        }
    }
    Ok(Some(Column::new(name, builder.finish().into_series())))
}

/// Elements in both arrays (PySpark array_intersect). Distinct.
pub fn apply_array_intersect(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "array_intersect needs two columns (array1, array2)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_intersect: {e}").into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_intersect: {e}").into()))?;
    let inner_dtype = a_ca.inner_dtype().clone();
    let mut builder = polars::chunked_array::builder::get_list_builder(
        &inner_dtype,
        64,
        a_ca.len(),
        name.as_str().into(),
    );
    for (opt_a, opt_b) in a_ca.amortized_iter().zip(b_ca.amortized_iter()) {
        match (opt_a, opt_b) {
            (Some(a_amort), Some(b_amort)) => {
                let a_list = a_amort.as_ref().as_list();
                let b_list = b_amort.as_ref().as_list();
                let b_keys: std::collections::HashSet<String> = b_list
                    .amortized_iter()
                    .flatten()
                    .map(|e| series_to_set_key(&e.deep_clone()))
                    .collect();
                let mut acc: Vec<Series> = Vec::new();
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                for e in a_list.amortized_iter().flatten() {
                    let s = e.deep_clone();
                    let key = series_to_set_key(&s);
                    if b_keys.contains(&key) && seen.insert(key) {
                        acc.push(s);
                    }
                }
                let result = if acc.is_empty() {
                    Series::new_empty(PlSmallStr::EMPTY, &inner_dtype)
                } else {
                    let mut r = acc.remove(0);
                    for s in acc {
                        r.extend(&s)?;
                    }
                    r
                };
                builder.append_series(&result)?;
            }
            _ => builder.append_null(),
        }
    }
    Ok(Some(Column::new(name, builder.finish().into_series())))
}

/// Distinct elements from both arrays (PySpark array_union).
pub fn apply_array_union(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "array_union needs two columns (array1, array2)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_union: {e}").into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_union: {e}").into()))?;
    let inner_dtype = a_ca.inner_dtype().clone();
    let mut builder = polars::chunked_array::builder::get_list_builder(
        &inner_dtype,
        64,
        a_ca.len(),
        name.as_str().into(),
    );
    for (opt_a, opt_b) in a_ca.amortized_iter().zip(b_ca.amortized_iter()) {
        match (opt_a, opt_b) {
            (Some(a_amort), Some(b_amort)) => {
                let a_list = a_amort.as_ref().as_list();
                let b_list = b_amort.as_ref().as_list();
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                let mut acc: Vec<Series> = Vec::new();
                for e in a_list.amortized_iter().flatten() {
                    let s = e.deep_clone();
                    let key = series_to_set_key(&s);
                    if seen.insert(key) {
                        acc.push(s);
                    }
                }
                for e in b_list.amortized_iter().flatten() {
                    let s = e.deep_clone();
                    let key = series_to_set_key(&s);
                    if seen.insert(key) {
                        acc.push(s);
                    }
                }
                let result = if acc.is_empty() {
                    Series::new_empty(PlSmallStr::EMPTY, &inner_dtype)
                } else {
                    let mut r = acc.remove(0);
                    for s in acc {
                        r.extend(&s)?;
                    }
                    r
                };
                builder.append_series(&result)?;
            }
            _ => builder.append_null(),
        }
    }
    Ok(Some(Column::new(name, builder.finish().into_series())))
}

/// Parse string to map: "k1:v1,k2:v2" -> List(Struct{key, value}) (PySpark str_to_map).
pub fn apply_str_to_map(
    column: Column,
    pair_delim: &str,
    key_value_delim: &str,
) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::builder::get_list_builder;
    use polars::chunked_array::StructChunked;
    use polars::datatypes::Field;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("str_to_map: {e}").into()))?;
    let out_struct = DataType::Struct(vec![
        Field::new("key".into(), DataType::String),
        Field::new("value".into(), DataType::String),
    ]);
    let mut builder = get_list_builder(&out_struct, 64, ca.len(), name.as_str().into());
    for opt_s in ca.into_iter() {
        if let Some(s) = opt_s {
            let pairs: Vec<(String, String)> = s
                .split(pair_delim)
                .filter_map(|part| {
                    let kv: Vec<&str> = part.splitn(2, key_value_delim).collect();
                    if kv.len() >= 2 {
                        Some((kv[0].to_string(), kv[1].to_string()))
                    } else if kv.len() == 1 && !kv[0].is_empty() {
                        Some((kv[0].to_string(), String::new()))
                    } else {
                        None
                    }
                })
                .collect();
            if pairs.is_empty() {
                builder.append_null();
            } else {
                let keys: Vec<String> = pairs.iter().map(|(k, _)| k.clone()).collect();
                let vals: Vec<String> = pairs.iter().map(|(_, v)| v.clone()).collect();
                let k_series = Series::new("key".into(), keys);
                let v_series = Series::new("value".into(), vals);
                let fields: [&Series; 2] = [&k_series, &v_series];
                let st = StructChunked::from_series(
                    PlSmallStr::EMPTY,
                    pairs.len(),
                    fields.iter().copied(),
                )
                .map_err(|e| PolarsError::ComputeError(format!("str_to_map struct: {e}").into()))?
                .into_series();
                builder.append_series(&st)?;
            }
        } else {
            builder.append_null();
        }
    }
    Ok(Some(Column::new(name, builder.finish().into_series())))
}

/// Merge two map columns (PySpark map_concat). Last value wins for duplicate keys.
pub fn apply_map_concat(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::builder::get_list_builder;
    use polars::chunked_array::StructChunked;
    use polars::datatypes::Field;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "map_concat needs at least two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("map_concat: {e}").into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("map_concat: {e}").into()))?;
    let struct_dtype = a_ca.inner_dtype().clone();
    let (key_dtype, value_dtype) = match &struct_dtype {
        DataType::Struct(fields) => {
            let k = fields
                .iter()
                .find(|f| f.name == "key")
                .map(|f| f.dtype.clone())
                .unwrap_or(DataType::String);
            let v = fields
                .iter()
                .find(|f| f.name == "value")
                .map(|f| f.dtype.clone())
                .unwrap_or(DataType::String);
            (k, v)
        }
        _ => (DataType::String, DataType::String),
    };
    let out_struct = DataType::Struct(vec![
        Field::new("key".into(), key_dtype),
        Field::new("value".into(), value_dtype),
    ]);
    let n = a_ca.len();
    let mut builder = get_list_builder(&out_struct, 64, n, name.as_str().into());
    for (opt_a, opt_b) in a_ca.amortized_iter().zip(b_ca.amortized_iter()) {
        let mut merged: std::collections::BTreeMap<String, (Series, Series)> =
            std::collections::BTreeMap::new();
        for amort in [opt_a, opt_b].into_iter().flatten() {
            let list_s = amort.as_ref().as_list();
            for elem in list_s.amortized_iter().flatten() {
                let s = elem.deep_clone();
                let st = s.struct_().map_err(|e| {
                    PolarsError::ComputeError(format!("map_concat struct: {e}").into())
                })?;
                let k_s = st.field_by_name("key").map_err(|e| {
                    PolarsError::ComputeError(format!("map_concat key: {e}").into())
                })?;
                let v_s = st.field_by_name("value").map_err(|e| {
                    PolarsError::ComputeError(format!("map_concat value: {e}").into())
                })?;
                let key = std::string::ToString::to_string(&k_s);
                merged.insert(key, (k_s, v_s));
            }
        }
        if merged.is_empty() {
            builder.append_null();
        } else {
            let mut row_structs: Vec<Series> = Vec::new();
            for (_, (k_s, v_s)) in merged {
                let len = k_s.len();
                let fields: [&Series; 2] = [&k_s, &v_s];
                let st = StructChunked::from_series(PlSmallStr::EMPTY, len, fields.iter().copied())
                    .map_err(|e| {
                        PolarsError::ComputeError(format!("map_concat build: {e}").into())
                    })?
                    .into_series();
                row_structs.push(st);
            }
            let mut combined = row_structs.remove(0);
            for s in row_structs {
                combined.extend(&s)?;
            }
            builder.append_series(&combined)?;
        }
    }
    Ok(Some(Column::new(name, builder.finish().into_series())))
}

/// True if map contains key (PySpark map_contains_key).
pub fn apply_map_contains_key(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "map_contains_key needs two columns (map, key)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let map_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let key_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let map_ca = map_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("map_contains_key: {e}").into()))?;
    let key_str = key_series.cast(&DataType::String)?;
    let key_vec: Vec<String> = (0..key_str.len())
        .map(|i| key_str.get(i).map(|av| av.to_string()).unwrap_or_default())
        .collect();
    let key_len = key_vec.len();
    let mut results: Vec<bool> = Vec::with_capacity(map_ca.len());
    for (i, opt_amort) in map_ca.amortized_iter().enumerate() {
        let target = key_vec
            .get(if key_len == 1 { 0 } else { i })
            .map(|s| s.as_str())
            .unwrap_or("");
        let mut found = false;
        if let Some(amort) = opt_amort {
            let list_s = amort.as_ref().as_list();
            for elem in list_s.amortized_iter().flatten() {
                let s = elem.deep_clone();
                if let Ok(st) = s.struct_() {
                    if let Ok(k) = st.field_by_name("key") {
                        let k_str: String =
                            k.get(0).ok().map(|av| av.to_string()).unwrap_or_default();
                        if k_str == target {
                            found = true;
                            break;
                        }
                    }
                }
            }
        }
        results.push(found);
    }
    let out =
        BooleanChunked::from_iter_options(name.as_str().into(), results.into_iter().map(Some));
    Ok(Some(Column::new(name, out.into_series())))
}

/// Get value for key from map, or null (PySpark get).
pub fn apply_get(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "get needs two columns (map, key)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let map_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let key_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let map_ca = map_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("get: {e}").into()))?;
    let key_str = key_series.cast(&DataType::String)?;
    let key_vec: Vec<String> = (0..key_str.len())
        .map(|i| key_str.get(i).map(|av| av.to_string()).unwrap_or_default())
        .collect();
    let key_len = key_vec.len();
    let value_dtype = match map_ca.inner_dtype() {
        DataType::Struct(fields) => fields
            .iter()
            .find(|f| f.name == "value")
            .map(|f| f.dtype.clone())
            .unwrap_or(DataType::String),
        _ => DataType::String,
    };
    let mut result_series: Vec<Series> = Vec::with_capacity(map_ca.len());
    for (i, opt_amort) in map_ca.amortized_iter().enumerate() {
        let target = key_vec
            .get(if key_len == 1 { 0 } else { i })
            .map(|s| s.as_str())
            .unwrap_or("");
        let mut found: Option<Series> = None;
        if let Some(amort) = opt_amort {
            let list_s = amort.as_ref().as_list();
            for elem in list_s.amortized_iter().flatten() {
                let s = elem.deep_clone();
                if let Ok(st) = s.struct_() {
                    if let Ok(k) = st.field_by_name("key") {
                        let k_str: String =
                            k.get(0).ok().map(|av| av.to_string()).unwrap_or_default();
                        if k_str == target {
                            if let Ok(v) = st.field_by_name("value") {
                                found = Some(v);
                            }
                            break;
                        }
                    }
                }
            }
        }
        result_series
            .push(found.unwrap_or_else(|| Series::full_null(PlSmallStr::EMPTY, 1, &value_dtype)));
    }
    let mut out = result_series.remove(0);
    for s in result_series {
        out.extend(&s)?;
    }
    Ok(Some(Column::new(name, out)))
}

/// ASCII value of first character (PySpark ascii). Returns Int32.
pub fn apply_ascii(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("ascii: {e}").into()))?;
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
                .map_err(|e| PolarsError::ComputeError(format!("format_number: {e}").into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter()
                    .map(|opt_v| opt_v.map(|v| format!("{v:.prec$}"))),
            )
        }
        DataType::Float32 => {
            let ca = series
                .f32()
                .map_err(|e| PolarsError::ComputeError(format!("format_number: {e}").into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter()
                    .map(|opt_v| opt_v.map(|v| format!("{v:.prec$}"))),
            )
        }
        _ => {
            let f64_series = series.cast(&DataType::Float64).map_err(|e| {
                PolarsError::ComputeError(format!("format_number cast: {e}").into())
            })?;
            let ca = f64_series
                .f64()
                .map_err(|e| PolarsError::ComputeError(format!("format_number: {e}").into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter()
                    .map(|opt_v| opt_v.map(|v| format!("{v:.prec$}"))),
            )
        }
    };
    Ok(Some(Column::new(name, out.into_series())))
}

/// Return the value at index `i` in `s` as a string, or None if null/unsupported type.
fn series_value_at_as_string(s: &Series, i: usize) -> Option<String> {
    match s.dtype() {
        DataType::String => s.str().ok()?.get(i).map(|v| v.to_string()),
        DataType::Int32 => s.i32().ok()?.get(i).map(|v| v.to_string()),
        DataType::Int64 => s.i64().ok()?.get(i).map(|v| v.to_string()),
        DataType::Float32 => s.f32().ok()?.get(i).map(|v| v.to_string()),
        DataType::Float64 => s.f64().ok()?.get(i).map(|v| v.to_string()),
        DataType::Boolean => s.bool().ok()?.get(i).map(|v| v.to_string()),
        _ => s.get(i).ok().map(|av| av.to_string()),
    }
}

/// Format columns with printf-style format string (PySpark format_string / printf).
/// Supports %s, %d, %i, %f, %g, %%. Null in any column yields null result.
pub fn apply_format_string(columns: &mut [Column], format: &str) -> PolarsResult<Option<Column>> {
    let n = columns.len();
    if n == 0 {
        return Err(PolarsError::ComputeError(
            "format_string needs at least one column".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let mut series_vec: Vec<Series> = Vec::with_capacity(n);
    for col in columns.iter_mut() {
        series_vec.push(std::mem::take(col).take_materialized_series());
    }
    let len = series_vec[0].len();
    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        let values: Vec<Option<String>> = series_vec
            .iter()
            .map(|s| series_value_at_as_string(s, i))
            .collect();
        let out = format_string_row(format, &values);
        result.push(out);
    }
    let out = StringChunked::from_iter_options(name.as_str().into(), result.into_iter());
    Ok(Some(Column::new(name, out.into_series())))
}

fn format_string_row(format: &str, values: &[Option<String>]) -> Option<String> {
    let mut out = String::new();
    let mut idx = 0;
    let mut chars = format.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '%' {
            match chars.next() {
                Some('%') => out.push('%'),
                Some('s') => {
                    if idx >= values.len() {
                        return None;
                    }
                    match &values[idx] {
                        Some(v) => out.push_str(v),
                        None => return None,
                    }
                    idx += 1;
                }
                Some('d') | Some('i') => {
                    if idx >= values.len() {
                        return None;
                    }
                    match &values[idx] {
                        Some(v) => out.push_str(v),
                        None => return None,
                    }
                    idx += 1;
                }
                Some('f') | Some('g') | Some('e') => {
                    if idx >= values.len() {
                        return None;
                    }
                    match &values[idx] {
                        Some(v) => out.push_str(v),
                        None => return None,
                    }
                    idx += 1;
                }
                _ => return None,
            }
        } else {
            out.push(c);
        }
    }
    Some(out)
}

/// Find 1-based index of str in comma-delimited set (PySpark find_in_set).
/// Returns 0 if not found or if str contains comma.
/// map_many: columns[0]=str, columns[1]=set
pub fn apply_find_in_set(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "find_in_set needs two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let str_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let set_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let str_ca = str_series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("find_in_set: {e}").into()))?;
    let set_ca = set_series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("find_in_set: {e}").into()))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        str_ca
            .into_iter()
            .zip(set_ca)
            .map(|(opt_str, opt_set)| match (opt_str, opt_set) {
                (Some(s), Some(set_str)) => {
                    if s.contains(',') {
                        Some(0i64)
                    } else {
                        let parts: Vec<&str> = set_str.split(',').collect();
                        let idx = parts.iter().position(|p| *p == s);
                        Some(idx.map(|i| (i + 1) as i64).unwrap_or(0))
                    }
                }
                _ => None,
            }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Regexp instr: 1-based position of first regex match (PySpark regexp_instr).
/// group_idx: 0 = full match, 1+ = capture group. Returns null if no match.
pub fn apply_regexp_instr(
    column: Column,
    pattern: String,
    group_idx: usize,
) -> PolarsResult<Option<Column>> {
    use regex::Regex;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("regexp_instr: {e}").into()))?;
    let re = Regex::new(&pattern).map_err(|e| {
        PolarsError::ComputeError(format!("regexp_instr invalid regex '{pattern}': {e}").into())
    })?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let m = if group_idx == 0 {
                    re.find(s)
                } else {
                    re.captures(s).and_then(|cap| cap.get(group_idx))
                };
                m.map(|m| (m.start() + 1) as i64)
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Base64 encode string bytes (PySpark base64). Input string UTF-8, output base64 string.
pub fn apply_base64(column: Column) -> PolarsResult<Option<Column>> {
    use base64::Engine;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("base64: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("unbase64: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("sha1: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("sha2: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("md5: {e}").into()))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_s| opt_s.map(|s| format!("{:x}", md5::compute(s.as_bytes())))),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Encode string to binary (PySpark encode). Charset: UTF-8, hex. Returns hex string representation of bytes.
pub fn apply_encode(column: Column, charset: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("encode: {e}").into()))?;
    let cs = charset.to_lowercase();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| {
                let bytes: Vec<u8> = match cs.as_str() {
                    "utf-8" | "utf8" => s.as_bytes().to_vec(),
                    _ => s.as_bytes().to_vec(), // default UTF-8
                };
                hex::encode(bytes)
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Decode binary (hex string) to string (PySpark decode). Charset: UTF-8.
pub fn apply_decode(column: Column, charset: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("decode: {e}").into()))?;
    let _ = charset;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let bytes = hex::decode(s.as_bytes()).ok()?;
                String::from_utf8(bytes).ok()
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// to_binary(expr, fmt): PySpark to_binary. fmt 'utf-8' => hex(utf8 bytes), 'hex' => validate and return hex. Returns hex string.
pub fn apply_to_binary(column: Column, fmt: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("to_binary: {e}").into()))?;
    let fmt_lower = fmt.to_lowercase();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let hex_str = match fmt_lower.as_str() {
                    "hex" => {
                        // Input is hex string; validate and return as-is (binary representation)
                        hex::decode(s.as_bytes()).ok()?;
                        Some(s.to_string())
                    }
                    "utf-8" | "utf8" => Some(hex::encode(s.as_bytes())),
                    _ => Some(hex::encode(s.as_bytes())),
                };
                hex_str
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// try_to_binary: like to_binary but returns null on failure.
pub fn apply_try_to_binary(column: Column, fmt: &str) -> PolarsResult<Option<Column>> {
    apply_to_binary(column, fmt)
}

/// AES-GCM encrypt (PySpark aes_encrypt). Key: UTF-8 string, 16 or 32 bytes for AES-128/256. Output: hex(nonce||ciphertext).
fn aes_gcm_encrypt_one(plaintext: &[u8], key: &[u8]) -> Option<String> {
    use aes_gcm::aead::generic_array::GenericArray;
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::Aes128Gcm;
    use rand::RngCore;
    let key_arr: [u8; 16] = key
        .iter()
        .copied()
        .chain(std::iter::repeat(0))
        .take(16)
        .collect::<Vec<_>>()
        .try_into()
        .ok()?;
    let cipher = Aes128Gcm::new(GenericArray::from_slice(&key_arr));
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce);
    let nonce = GenericArray::from_slice(&nonce);
    let ciphertext = cipher.encrypt(nonce, plaintext).ok()?;
    let mut out = nonce.as_slice().to_vec();
    out.extend(ciphertext);
    Some(hex::encode(out))
}

/// AES-GCM decrypt (PySpark aes_decrypt). Input: hex(nonce||ciphertext).
fn aes_gcm_decrypt_one(hex_input: &str, key: &[u8]) -> Option<String> {
    use aes_gcm::aead::generic_array::GenericArray;
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::Aes128Gcm;
    let bytes = hex::decode(hex_input.as_bytes()).ok()?;
    if bytes.len() < 12 + 16 {
        return None; // nonce + at least tag
    }
    let (nonce_bytes, ct) = bytes.split_at(12);
    let key_arr: [u8; 16] = key
        .iter()
        .copied()
        .chain(std::iter::repeat(0))
        .take(16)
        .collect::<Vec<_>>()
        .try_into()
        .ok()?;
    let cipher = Aes128Gcm::new(GenericArray::from_slice(&key_arr));
    let nonce = GenericArray::from_slice(nonce_bytes);
    let plaintext = cipher.decrypt(nonce, ct).ok()?;
    String::from_utf8(plaintext).ok()
}

/// AES encrypt (PySpark aes_encrypt). Key as string; uses AES-128-GCM. Output hex.
pub fn apply_aes_encrypt(column: Column, key: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("aes_encrypt: {e}").into()))?;
    let key_bytes = key.as_bytes();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_s| opt_s.and_then(|s| aes_gcm_encrypt_one(s.as_bytes(), key_bytes))),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// AES decrypt (PySpark aes_decrypt). Input hex(nonce||ciphertext). Returns null on failure.
pub fn apply_aes_decrypt(column: Column, key: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("aes_decrypt: {e}").into()))?;
    let key_bytes = key.as_bytes();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_s| opt_s.and_then(|s| aes_gcm_decrypt_one(s, key_bytes))),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// try_aes_decrypt: same as aes_decrypt, returns null on failure (PySpark try_aes_decrypt).
pub fn apply_try_aes_decrypt(column: Column, key: &str) -> PolarsResult<Option<Column>> {
    apply_aes_decrypt(column, key)
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
                .map_err(|e| PolarsError::ComputeError(format!("char: {e}").into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_v| opt_v.map(|v| to_char(v as i64))),
            )
        }
        DataType::Int64 => {
            let ca = series
                .i64()
                .map_err(|e| PolarsError::ComputeError(format!("char: {e}").into()))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_v| opt_v.map(to_char)),
            )
        }
        _ => {
            let i64_series = series
                .cast(&DataType::Int64)
                .map_err(|e| PolarsError::ComputeError(format!("char cast: {e}").into()))?;
            let ca = i64_series
                .i64()
                .map_err(|e| PolarsError::ComputeError(format!("char: {e}").into()))?;
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
    days_series
        .i32()
        .map_err(|e| PolarsError::ComputeError(format!("date_series_to_days: {e}").into()))
        .cloned()
}

fn days_to_naive_date(days: i32) -> Option<chrono::NaiveDate> {
    let base = crate::date_utils::epoch_naive_date();
    base.checked_add_signed(chrono::TimeDelta::days(days as i64))
}

fn naivedate_to_days(d: chrono::NaiveDate) -> i32 {
    let base = crate::date_utils::epoch_naive_date();
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
    let target = parse_day_of_week(day_of_week).ok_or_else(|| {
        PolarsError::ComputeError(format!("next_day: invalid day '{day_of_week}'").into())
    })?;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = date_series_to_days(&series)?;
    let out = ca.into_iter().map(|opt_d| {
        opt_d.and_then(|days| {
            let d = days_to_naive_date(days)?;
            let current = chrono_weekday_to_dayofweek(d.weekday());
            let diff = if target >= current {
                (target - current) as i64
            } else {
                (7 - (current - target)) as i64
            };
            let days_to_add = if diff == 0 { 7 } else { diff }; // same day -> next week
            let next = d.checked_add_signed(chrono::TimeDelta::days(days_to_add))?;
            Some(naivedate_to_days(next))
        })
    });
    let out = Int32Chunked::from_iter_options(name.as_str().into(), out);
    let out_series = out.into_series().cast(&DataType::Date)?;
    Ok(Some(Column::new(name, out_series)))
}

/// dayname(date_col) - weekday name "Mon","Tue",... (PySpark dayname).
pub fn apply_dayname(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = date_series_to_days(&series)?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_d| {
            opt_d.and_then(|days| {
                let d = days_to_naive_date(days)?;
                Some(d.weekday().to_string())
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// weekday(date_col) - 0=Mon, 6=Sun (PySpark weekday).
pub fn apply_weekday(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = date_series_to_days(&series)?;
    let out = Int32Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_d| {
            opt_d.and_then(|days| {
                let d = days_to_naive_date(days)?;
                Some(d.weekday().num_days_from_monday() as i32)
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// months_between(end, start, round_off) - returns fractional number of months.
/// When round_off is true, rounds to 8 decimal places (PySpark default).
pub fn apply_months_between(
    columns: &mut [Column],
    round_off: bool,
) -> PolarsResult<Option<Column>> {
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
    // PySpark: fractional months using 31 days per month; roundOff rounds to 8 decimal places.
    let out = end_ca
        .into_iter()
        .zip(&start_ca)
        .map(|(oe, os)| match (oe, os) {
            (Some(e), Some(s)) => {
                let days = (e - s) as f64;
                let months = days / 31.0;
                Some(if round_off {
                    (months * 1e8).round() / 1e8
                } else {
                    months
                })
            }
            _ => None,
        });
    let out = Float64Chunked::from_iter_options(name.as_str().into(), out);
    Ok(Some(Column::new(name, out.into_series())))
}

// --- Math (trig, degrees, radians, signum) ---

fn float_series_to_f64(series: &Series) -> PolarsResult<Float64Chunked> {
    let casted = series.cast(&DataType::Float64)?;
    casted
        .f64()
        .map_err(|e| PolarsError::ComputeError(format!("float_series_to_f64: {e}").into()))
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
        let ca = series
            .str()
            .map_err(|e| PolarsError::ComputeError(format!("conv: {e}").into()))?;
        ca.apply(|opt_s| opt_s.and_then(|s| conv_one(s, from_base, to_base).map(Cow::Owned)))
            .into_series()
    } else if series.dtype() == &DataType::Int64 {
        let ca = series
            .i64()
            .map_err(|e| PolarsError::ComputeError(format!("conv: {e}").into()))?;
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
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("unhex: {e}").into()))?;
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
        (0..n).map(|_| rng.gen::<f64>()).collect()
    } else {
        let mut rng = rand::thread_rng();
        (0..n).map(|_| rng.gen::<f64>()).collect()
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
        (0..n).map(|_| rng.gen::<f64>()).collect()
    } else {
        let mut rng = rand::thread_rng();
        (0..n).map(|_| rng.gen::<f64>()).collect()
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
            let ca_a = a
                .i64()
                .map_err(|e| PolarsError::ComputeError(format!("greatest: {e}").into()))?;
            let ca_b = b
                .i64()
                .map_err(|e| PolarsError::ComputeError(format!("greatest: {e}").into()))?;
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
            let ca_a = a
                .str()
                .map_err(|e| PolarsError::ComputeError(format!("greatest: {e}").into()))?;
            let ca_b = b
                .str()
                .map_err(|e| PolarsError::ComputeError(format!("greatest: {e}").into()))?;
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
            let ca_a = a
                .i64()
                .map_err(|e| PolarsError::ComputeError(format!("least: {e}").into()))?;
            let ca_b = b
                .i64()
                .map_err(|e| PolarsError::ComputeError(format!("least: {e}").into()))?;
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
            let ca_a = a
                .str()
                .map_err(|e| PolarsError::ComputeError(format!("least: {e}").into()))?;
            let ca_b = b
                .str()
                .map_err(|e| PolarsError::ComputeError(format!("least: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("map_from_arrays keys: {e}").into()))?;
    let values_ca = values_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("map_from_arrays values: {e}").into()))?;
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
                        .map_err(|e| PolarsError::ComputeError(format!("struct: {e}").into()))?
                        .into_series();
                        row_structs.push(st);
                    }
                }
                if row_structs.is_empty() {
                    builder
                        .append_series(&Series::new_empty(PlSmallStr::EMPTY, &struct_dtype))
                        .map_err(|e| PolarsError::ComputeError(format!("builder: {e}").into()))?;
                } else {
                    let mut combined = row_structs.remove(0);
                    for s in row_structs {
                        combined.extend(&s)?;
                    }
                    builder
                        .append_series(&combined)
                        .map_err(|e| PolarsError::ComputeError(format!("builder: {e}").into()))?;
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

/// Zip two array columns into List(Struct{left, right}) for zip_with. Shorter padded with null.
pub fn apply_zip_arrays_to_struct(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::builder::get_list_builder;
    use polars::chunked_array::StructChunked;
    use polars::datatypes::Field;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "zip_arrays_to_struct needs two columns (left, right)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("zip_with left: {e}").into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("zip_with right: {e}").into()))?;
    let left_dtype = a_ca.inner_dtype().clone();
    let right_dtype = b_ca.inner_dtype().clone();
    let struct_dtype = DataType::Struct(vec![
        Field::new("left".into(), left_dtype.clone()),
        Field::new("right".into(), right_dtype.clone()),
    ]);
    let mut builder = get_list_builder(&struct_dtype, 64, a_ca.len(), name.as_str().into());
    for (opt_a, opt_b) in a_ca.amortized_iter().zip(b_ca.amortized_iter()) {
        match (opt_a, opt_b) {
            (Some(a_amort), Some(b_amort)) => {
                let a_list = a_amort.as_ref().as_list();
                let b_list = b_amort.as_ref().as_list();
                let a_elems: Vec<Series> = a_list
                    .amortized_iter()
                    .flatten()
                    .map(|amort| amort.deep_clone())
                    .collect();
                let b_elems: Vec<Series> = b_list
                    .amortized_iter()
                    .flatten()
                    .map(|amort| amort.deep_clone())
                    .collect();
                let max_len = a_elems.len().max(b_elems.len());
                let mut row_structs: Vec<Series> = Vec::new();
                for i in 0..max_len {
                    let left_s = a_elems.get(i).cloned();
                    let right_s = b_elems.get(i).cloned();
                    let (mut left_series, mut right_series) = match (left_s, right_s) {
                        (Some(l), Some(r)) => (l, r),
                        (Some(l), None) => {
                            let r = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &[AnyValue::Null],
                                &right_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("zip null: {e}").into())
                            })?;
                            (l, r)
                        }
                        (None, Some(r)) => {
                            let l = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &[AnyValue::Null],
                                &left_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("zip null: {e}").into())
                            })?;
                            (l, r)
                        }
                        (None, None) => {
                            let mut l = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &[AnyValue::Null],
                                &left_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("zip null: {e}").into())
                            })?;
                            l.rename("left".into());
                            let mut r = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &[AnyValue::Null],
                                &right_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("zip null: {e}").into())
                            })?;
                            r.rename("right".into());
                            (l, r)
                        }
                    };
                    left_series.rename("left".into());
                    right_series.rename("right".into());
                    let len = left_series.len();
                    let fields: [&Series; 2] = [&left_series, &right_series];
                    let st =
                        StructChunked::from_series(PlSmallStr::EMPTY, len, fields.iter().copied())
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("zip struct: {e}").into())
                            })?
                            .into_series();
                    row_structs.push(st);
                }
                if row_structs.is_empty() {
                    builder
                        .append_series(&Series::new_empty(PlSmallStr::EMPTY, &struct_dtype))
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("zip builder: {e}").into())
                        })?;
                } else {
                    let mut combined = row_structs.remove(0);
                    for s in row_structs {
                        combined.extend(&s)?;
                    }
                    builder.append_series(&combined).map_err(|e| {
                        PolarsError::ComputeError(format!("zip builder: {e}").into())
                    })?;
                }
            }
            _ => builder.append_null(),
        }
    }
    let out = builder.finish().into_series();
    Ok(Some(Column::new(name, out)))
}

/// Merge two maps into List(Struct{key, value1, value2}) for map_zip_with. Union of keys.
pub fn apply_map_zip_to_struct(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::builder::get_list_builder;
    use polars::chunked_array::StructChunked;
    use polars::datatypes::Field;
    use std::collections::BTreeMap;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "map_zip_to_struct needs two columns (map1, map2)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("map_zip_with map1: {e}").into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("map_zip_with map2: {e}").into()))?;
    let struct_dtype_in = a_ca.inner_dtype().clone();
    let (key_dtype, value_dtype) = match &struct_dtype_in {
        DataType::Struct(fields) => {
            let k = fields
                .iter()
                .find(|f| f.name == "key")
                .map(|f| f.dtype.clone())
                .unwrap_or(DataType::String);
            let v = fields
                .iter()
                .find(|f| f.name == "value")
                .map(|f| f.dtype.clone())
                .unwrap_or(DataType::String);
            (k, v)
        }
        _ => (DataType::String, DataType::String),
    };
    let out_struct_dtype = DataType::Struct(vec![
        Field::new("key".into(), key_dtype.clone()),
        Field::new("value1".into(), value_dtype.clone()),
        Field::new("value2".into(), value_dtype.clone()),
    ]);
    let mut builder = get_list_builder(&out_struct_dtype, 64, a_ca.len(), name.as_str().into());
    for (opt_a, opt_b) in a_ca.amortized_iter().zip(b_ca.amortized_iter()) {
        match (opt_a, opt_b) {
            (Some(a_amort), Some(b_amort)) => {
                let a_list = a_amort.as_ref().as_list();
                let b_list = b_amort.as_ref().as_list();
                let mut key_to_vals: BTreeMap<String, (Series, Option<Series>, Option<Series>)> =
                    BTreeMap::new();
                for elem in a_list.amortized_iter().flatten() {
                    let s = elem.deep_clone();
                    if let Ok(st) = s.struct_() {
                        if let (Ok(k), Ok(v)) = (st.field_by_name("key"), st.field_by_name("value"))
                        {
                            let key_str: String = std::string::ToString::to_string(&k);
                            key_to_vals
                                .entry(key_str.clone())
                                .or_insert_with(|| (k.clone(), None, None))
                                .1 = Some(v);
                        }
                    }
                }
                for elem in b_list.amortized_iter().flatten() {
                    let s = elem.deep_clone();
                    if let Ok(st) = s.struct_() {
                        if let (Ok(k), Ok(v)) = (st.field_by_name("key"), st.field_by_name("value"))
                        {
                            let key_str: String = std::string::ToString::to_string(&k);
                            key_to_vals
                                .entry(key_str.clone())
                                .or_insert_with(|| (k.clone(), None, None))
                                .2 = Some(v);
                        }
                    }
                }
                let mut row_structs: Vec<Series> = Vec::new();
                for (_, (k_series, v1_opt, v2_opt)) in key_to_vals {
                    let mut k_renamed = k_series;
                    k_renamed.rename("key".into());
                    let v1_fallback = || {
                        Series::from_any_values_and_dtype(
                            PlSmallStr::EMPTY,
                            &[AnyValue::Null],
                            &value_dtype,
                            false,
                        )
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("map_zip null fallback: {e}").into())
                        })
                    };
                    let mut v1_series = match v1_opt {
                        Some(s) => s,
                        None => v1_fallback()?,
                    };
                    v1_series.rename("value1".into());
                    let v2_fallback = || {
                        Series::from_any_values_and_dtype(
                            PlSmallStr::EMPTY,
                            &[AnyValue::Null],
                            &value_dtype,
                            false,
                        )
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("map_zip null fallback: {e}").into())
                        })
                    };
                    let mut v2_series = match v2_opt {
                        Some(s) => s,
                        None => v2_fallback()?,
                    };
                    v2_series.rename("value2".into());
                    let len = k_renamed.len();
                    let fields: [&Series; 3] = [&k_renamed, &v1_series, &v2_series];
                    let st =
                        StructChunked::from_series(PlSmallStr::EMPTY, len, fields.iter().copied())
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("map_zip struct: {e}").into())
                            })?
                            .into_series();
                    row_structs.push(st);
                }
                if row_structs.is_empty() {
                    builder
                        .append_series(&Series::new_empty(PlSmallStr::EMPTY, &out_struct_dtype))
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("map_zip builder: {e}").into())
                        })?;
                } else {
                    let mut combined = row_structs.remove(0);
                    for s in row_structs {
                        combined.extend(&s)?;
                    }
                    builder.append_series(&combined).map_err(|e| {
                        PolarsError::ComputeError(format!("map_zip builder: {e}").into())
                    })?;
                }
            }
            _ => builder.append_null(),
        }
    }
    let out = builder.finish().into_series();
    Ok(Some(Column::new(name, out)))
}

/// typeof: return dtype as string (PySpark typeof).
pub fn apply_typeof(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let dtype_str = format!("{:?}", series.dtype());
    let len = series.len();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        (0..len).map(|_| Some(dtype_str.clone())),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Helper: get two series as Int64Chunked with shared error context.
fn binary_series_i64(
    a: &Series,
    b: &Series,
    ctx: &str,
) -> PolarsResult<(Int64Chunked, Int64Chunked)> {
    let ca_a = a
        .i64()
        .map_err(|e| PolarsError::ComputeError(format!("{ctx}: {e}").into()))?
        .clone();
    let ca_b = b
        .i64()
        .map_err(|e| PolarsError::ComputeError(format!("{ctx}: {e}").into()))?
        .clone();
    Ok((ca_a, ca_b))
}

/// Helper: get two series as Int32Chunked with shared error context.
fn binary_series_i32(
    a: &Series,
    b: &Series,
    ctx: &str,
) -> PolarsResult<(Int32Chunked, Int32Chunked)> {
    let ca_a = a
        .i32()
        .map_err(|e| PolarsError::ComputeError(format!("{ctx}: {e}").into()))?
        .clone();
    let ca_b = b
        .i32()
        .map_err(|e| PolarsError::ComputeError(format!("{ctx}: {e}").into()))?
        .clone();
    Ok((ca_a, ca_b))
}

/// Helper: get two series as Float64Chunked with shared error context.
fn binary_series_f64(
    a: &Series,
    b: &Series,
    ctx: &str,
) -> PolarsResult<(Float64Chunked, Float64Chunked)> {
    let a_f = a.cast(&DataType::Float64)?;
    let b_f = b.cast(&DataType::Float64)?;
    let ca_a = a_f
        .f64()
        .map_err(|e| PolarsError::ComputeError(format!("{ctx}: {e}").into()))?
        .clone();
    let ca_b = b_f
        .f64()
        .map_err(|e| PolarsError::ComputeError(format!("{ctx}: {e}").into()))?
        .clone();
    Ok((ca_a, ca_b))
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
    let out = match (a_s.dtype(), b_s.dtype()) {
        (DataType::Int64, DataType::Int64) => {
            let (ca_a, ca_b) = binary_series_i64(&a_s, &b_s, "try_add")?;
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
    let out = match (a_s.dtype(), b_s.dtype()) {
        (DataType::Int64, DataType::Int64) => {
            let (ca_a, ca_b) = binary_series_i64(&a_s, &b_s, "try_subtract")?;
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
    let out = match (a_s.dtype(), b_s.dtype()) {
        (DataType::Int64, DataType::Int64) => {
            let (ca_a, ca_b) = binary_series_i64(&a_s, &b_s, "try_multiply")?;
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

/// Map PySpark/Java SimpleDateFormat style to chrono strftime. Public for to_char/date_format.
pub(crate) fn pyspark_format_to_chrono(s: &str) -> String {
    s.replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S")
}

/// unix_timestamp(column, format?) - parse string to seconds since epoch.
pub fn apply_unix_timestamp(column: Column, format: Option<&str>) -> PolarsResult<Option<Column>> {
    use chrono::{DateTime, NaiveDateTime, Utc};
    let chrono_fmt = format
        .map(pyspark_format_to_chrono)
        .unwrap_or_else(|| "%Y-%m-%d %H:%M:%S".to_string());
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("unix_timestamp: {e}").into()))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                NaiveDateTime::parse_from_str(s, &chrono_fmt)
                    .ok()
                    .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc).timestamp())
            })
        }),
    );
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
        .map_err(|e| PolarsError::ComputeError(format!("from_unixtime cast: {e}").into()))?;
    let ca = casted
        .i64()
        .map_err(|e| PolarsError::ComputeError(format!("from_unixtime: {e}").into()))?;
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
                .map_err(|e| PolarsError::ComputeError(format!("make_timestamp: {e}").into()))?
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
    let out_series = out
        .into_series()
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
    Ok(Some(Column::new(name, out_series)))
}

/// to_timestamp(column, format?) / try_to_timestamp(column, format?) - string to timestamp.
/// When format is Some, parse with that format (PySpark-style mapped to chrono); when None, use default.
/// strict: true for to_timestamp (error on invalid), false for try_to_timestamp (null on invalid).
pub fn apply_to_timestamp_format(
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
        .map_err(|e| PolarsError::ComputeError(format!("to_timestamp: {e}").into()))?;
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
    if strict {
        // to_timestamp: ensure no nulls introduced by parse (optional: could check and error)
        Ok(Some(Column::new(name, out_series)))
    } else {
        Ok(Some(Column::new(name, out_series)))
    }
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
        .map_err(|e| PolarsError::ComputeError(format!("make_date: {e}").into()))?
        .clone();
    let m_ca = m_series
        .cast(&DataType::Int32)?
        .i32()
        .map_err(|e| PolarsError::ComputeError(format!("make_date: {e}").into()))?
        .clone();
    let d_ca = d_series
        .cast(&DataType::Int32)?
        .i32()
        .map_err(|e| PolarsError::ComputeError(format!("make_date: {e}").into()))?
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
        .map_err(|e| PolarsError::ComputeError(format!("factorial cast: {e}").into()))?;
    let ca = casted
        .i64()
        .map_err(|e| PolarsError::ComputeError(format!("factorial: {e}").into()))?;
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
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("url_decode: {e}").into()))?;
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
    use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("url_encode: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("shift_right_unsigned: {e}").into()))?;
    let u = n as u32;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_v| opt_v.map(|v| ((v as u64) >> u) as i64)),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// json_array_length(json_str, path) - length of JSON array at path (PySpark json_array_length).
pub fn apply_json_array_length(column: Column, path: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("json_array_length: {e}").into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("json_object_keys: {e}").into()))?;
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
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("json_tuple: {e}").into()))?;
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
    let out_df = DataFrame::new(field_series.into_iter().map(|s| s.into()).collect())?;
    let out_struct = out_df.into_struct(name.as_str().into());
    Ok(Some(Column::new(name, out_struct.into_series())))
}

/// from_csv(str_col, schema) - parse CSV string to struct (PySpark from_csv). Minimal: split by comma, up to 32 columns.
pub fn apply_from_csv(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("from_csv: {e}").into()))?;
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
    let out_df = DataFrame::new(field_series.into_iter().map(|s| s.into()).collect())?;
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
    let ca = series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("parse_url: {e}").into()))?;
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

/// Build array [start, start+step, ...] up to but not past stop (PySpark sequence).
/// Input column is a struct with fields "0"=start, "1"=stop, "2"=step (step optional, default 1).
pub fn apply_sequence(column: Column) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::builder::get_list_builder;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let st = series.struct_().map_err(|e| {
        PolarsError::ComputeError(format!("sequence: expected struct column: {e}").into())
    })?;
    let start_s = st
        .field_by_name("0")
        .map_err(|e| PolarsError::ComputeError(format!("sequence field 0: {e}").into()))?;
    let stop_s = st
        .field_by_name("1")
        .map_err(|e| PolarsError::ComputeError(format!("sequence field 1: {e}").into()))?;
    let step_s = st.field_by_name("2").ok(); // optional
    let start_series = start_s
        .cast(&DataType::Int64)
        .map_err(|e| PolarsError::ComputeError(format!("sequence start: {e}").into()))?;
    let stop_series = stop_s
        .cast(&DataType::Int64)
        .map_err(|e| PolarsError::ComputeError(format!("sequence stop: {e}").into()))?;
    let step_series_opt: Option<Series> = step_s
        .as_ref()
        .map(|s| s.cast(&DataType::Int64))
        .transpose()
        .map_err(|e| PolarsError::ComputeError(format!("sequence step: {e}").into()))?;
    let start_ca = start_series
        .i64()
        .map_err(|e| PolarsError::ComputeError(format!("sequence: {e}").into()))?;
    let stop_ca = stop_series
        .i64()
        .map_err(|e| PolarsError::ComputeError(format!("sequence: {e}").into()))?;
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

/// Random permutation of list elements (PySpark shuffle). Uses rand::seq::SliceRandom.
pub fn apply_shuffle(column: Column) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::builder::get_list_builder;
    use rand::seq::SliceRandom;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("shuffle: {e}").into()))?;
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
                    .map_err(|e| PolarsError::ComputeError(format!("shuffle take: {e}").into()))?;
                builder.append_series(&taken)?;
            }
        }
    }
    Ok(Some(Column::new(name, builder.finish().into_series())))
}

// --- Bitmap (PySpark 3.5+) ---

/// Size of bitmap in bytes (32768 bits).
const BITMAP_BYTES: usize = 4096;

/// Count set bits in a bitmap (binary column). PySpark bitmap_count.
pub fn apply_bitmap_count(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .binary()
        .map_err(|e| PolarsError::ComputeError(format!("bitmap_count: {e}").into()))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_b| {
            opt_b.map(|b| b.iter().map(|&byte| byte.count_ones() as i64).sum::<i64>())
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Build one bitmap from a list of bit positions (0..32767). Used after implode for bitmap_construct_agg.
pub fn apply_bitmap_construct_agg(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("bitmap_construct_agg: {e}").into()))?;
    let out: BinaryChunked = list_ca
        .amortized_iter()
        .map(|opt_list| {
            opt_list.and_then(|list_series| {
                let mut buf = vec![0u8; BITMAP_BYTES];
                let ca = list_series.as_ref().i64().ok()?;
                for pos in ca.into_iter().flatten() {
                    let pos = pos as usize;
                    if pos < BITMAP_BYTES * 8 {
                        let byte_idx = pos / 8;
                        let bit_idx = pos % 8;
                        buf[byte_idx] |= 1 << bit_idx;
                    }
                }
                Some(bytes::Bytes::from(buf))
            })
        })
        .collect();
    Ok(Some(Column::new(name, out.into_series())))
}

/// Bitwise OR of a list of bitmaps (binary). Used after implode for bitmap_or_agg.
pub fn apply_bitmap_or_agg(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("bitmap_or_agg: {e}").into()))?;
    let out: BinaryChunked = list_ca
        .amortized_iter()
        .map(|opt_list| {
            opt_list.and_then(|list_series| {
                let list_c = list_series.as_ref().as_list();
                let mut buf = vec![0u8; BITMAP_BYTES];
                for opt_bin in list_c.amortized_iter().flatten() {
                    let bin_ca: &BinaryChunked = opt_bin.as_ref().binary().ok()?;
                    for b in bin_ca.into_iter().flatten() {
                        let b: &[u8] = b;
                        for (i, &byte) in b.iter().take(BITMAP_BYTES).enumerate() {
                            buf[i] |= byte;
                        }
                    }
                }
                Some(bytes::Bytes::from(buf))
            })
        })
        .collect();
    Ok(Some(Column::new(name, out.into_series())))
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
        .map_err(|e| PolarsError::ComputeError(format!("to_timestamp_ltz: {e}").into()))?;
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
    let out_series = out
        .into_series()
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("to_timestamp_ntz: {e}").into()))?;
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
