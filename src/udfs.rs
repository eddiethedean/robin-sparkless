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

// --- Phase 18: array_append, array_prepend, array_insert ---

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
        .map_err(|e| PolarsError::ComputeError(format!("array_append: {}", e).into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("array_prepend: {}", e).into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("array_insert: {}", e).into()))?;
    let inner_dtype = list_ca.inner_dtype().clone();
    let pos_ca = pos_series.cast(&DataType::Int64)?.i64().unwrap().clone();
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

// --- Phase 18: array_except, array_intersect, array_union (set ops) ---

fn series_to_set_key(s: &Series) -> String {
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
        .map_err(|e| PolarsError::ComputeError(format!("array_except: {}", e).into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_except: {}", e).into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("array_intersect: {}", e).into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_intersect: {}", e).into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("array_union: {}", e).into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("array_union: {}", e).into()))?;
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

// --- Phase 18: map_concat, map_from_entries, map_contains_key, get ---

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
        .map_err(|e| PolarsError::ComputeError(format!("map_concat: {}", e).into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("map_concat: {}", e).into()))?;
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
                    PolarsError::ComputeError(format!("map_concat struct: {}", e).into())
                })?;
                let k_s = st.field_by_name("key").map_err(|e| {
                    PolarsError::ComputeError(format!("map_concat key: {}", e).into())
                })?;
                let v_s = st.field_by_name("value").map_err(|e| {
                    PolarsError::ComputeError(format!("map_concat value: {}", e).into())
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
                        PolarsError::ComputeError(format!("map_concat build: {}", e).into())
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
        .map_err(|e| PolarsError::ComputeError(format!("map_contains_key: {}", e).into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("get: {}", e).into()))?;
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
        let mut values: Vec<Option<String>> = Vec::with_capacity(n);
        for s in &series_vec {
            let v = match s.dtype() {
                DataType::String => s.str().unwrap().get(i).map(|v| v.to_string()),
                DataType::Int32 => s.i32().unwrap().get(i).map(|v| v.to_string()),
                DataType::Int64 => s.i64().unwrap().get(i).map(|v| v.to_string()),
                DataType::Float32 => s.f32().unwrap().get(i).map(|v| v.to_string()),
                DataType::Float64 => s.f64().unwrap().get(i).map(|v| v.to_string()),
                DataType::Boolean => s.bool().unwrap().get(i).map(|v| v.to_string()),
                _ => s.get(i).ok().map(|av| av.to_string()),
            };
            values.push(v);
        }
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
        .map_err(|e| PolarsError::ComputeError(format!("find_in_set: {}", e).into()))?;
    let set_ca = set_series
        .str()
        .map_err(|e| PolarsError::ComputeError(format!("find_in_set: {}", e).into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("regexp_instr: {}", e).into()))?;
    let re = Regex::new(&pattern).map_err(|e| {
        PolarsError::ComputeError(format!("regexp_instr invalid regex '{}': {}", pattern, e).into())
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
    let target = parse_day_of_week(day_of_week).ok_or_else(|| {
        PolarsError::ComputeError(format!("next_day: invalid day '{}'", day_of_week).into())
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
    let out = end_ca.into_iter().zip(&start_ca).map(|(oe, os)| {
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

/// Hyperbolic and inverse hyperbolic / extra math (Phase 15 Batch 3).
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
            let ca_a = a.i64().unwrap();
            let ca_b = b.i64().unwrap();
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
            let ca_a = a.str().unwrap();
            let ca_b = b.str().unwrap();
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
            let ca_a = a.i64().unwrap();
            let ca_b = b.i64().unwrap();
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
            let ca_a = a.str().unwrap();
            let ca_b = b.str().unwrap();
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
        .map_err(|e| PolarsError::ComputeError(format!("zip_with left: {}", e).into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("zip_with right: {}", e).into()))?;
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
                                PolarsError::ComputeError(format!("zip null: {}", e).into())
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
                                PolarsError::ComputeError(format!("zip null: {}", e).into())
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
                                PolarsError::ComputeError(format!("zip null: {}", e).into())
                            })?;
                            l.rename("left".into());
                            let mut r = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &[AnyValue::Null],
                                &right_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("zip null: {}", e).into())
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
                                PolarsError::ComputeError(format!("zip struct: {}", e).into())
                            })?
                            .into_series();
                    row_structs.push(st);
                }
                if row_structs.is_empty() {
                    builder
                        .append_series(&Series::new_empty(PlSmallStr::EMPTY, &struct_dtype))
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("zip builder: {}", e).into())
                        })?;
                } else {
                    let mut combined = row_structs.remove(0);
                    for s in row_structs {
                        combined.extend(&s)?;
                    }
                    builder.append_series(&combined).map_err(|e| {
                        PolarsError::ComputeError(format!("zip builder: {}", e).into())
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
        .map_err(|e| PolarsError::ComputeError(format!("map_zip_with map1: {}", e).into()))?;
    let b_ca = b_series
        .list()
        .map_err(|e| PolarsError::ComputeError(format!("map_zip_with map2: {}", e).into()))?;
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
                    let mut v1_series = v1_opt.unwrap_or_else(|| {
                        Series::from_any_values_and_dtype(
                            PlSmallStr::EMPTY,
                            &[AnyValue::Null],
                            &value_dtype,
                            false,
                        )
                        .unwrap()
                    });
                    v1_series.rename("value1".into());
                    let mut v2_series = v2_opt.unwrap_or_else(|| {
                        Series::from_any_values_and_dtype(
                            PlSmallStr::EMPTY,
                            &[AnyValue::Null],
                            &value_dtype,
                            false,
                        )
                        .unwrap()
                    });
                    v2_series.rename("value2".into());
                    let len = k_renamed.len();
                    let fields: [&Series; 3] = [&k_renamed, &v1_series, &v2_series];
                    let st =
                        StructChunked::from_series(PlSmallStr::EMPTY, len, fields.iter().copied())
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("map_zip struct: {}", e).into())
                            })?
                            .into_series();
                    row_structs.push(st);
                }
                if row_structs.is_empty() {
                    builder
                        .append_series(&Series::new_empty(PlSmallStr::EMPTY, &out_struct_dtype))
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("map_zip builder: {}", e).into())
                        })?;
                } else {
                    let mut combined = row_structs.remove(0);
                    for s in row_structs {
                        combined.extend(&s)?;
                    }
                    builder.append_series(&combined).map_err(|e| {
                        PolarsError::ComputeError(format!("map_zip builder: {}", e).into())
                    })?;
                }
            }
            _ => builder.append_null(),
        }
    }
    let out = builder.finish().into_series();
    Ok(Some(Column::new(name, out)))
}

// --- Phase 19: try_add, try_subtract, try_multiply (checked arithmetic) ---

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
            let ca_a = a_s.i64().unwrap();
            let ca_b = b_s.i64().unwrap();
            Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_add(b)))),
            )
            .into_series()
        }
        (DataType::Int32, DataType::Int32) => {
            let ca_a = a_s.i32().unwrap();
            let ca_b = b_s.i32().unwrap();
            Int32Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_add(b)))),
            )
            .into_series()
        }
        _ => {
            let a_f = a_s.cast(&DataType::Float64)?;
            let b_f = b_s.cast(&DataType::Float64)?;
            let ca_a = a_f.f64().unwrap();
            let ca_b = b_f.f64().unwrap();
            Float64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(ca_b)
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
            let ca_a = a_s.i64().unwrap();
            let ca_b = b_s.i64().unwrap();
            Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_sub(b)))),
            )
            .into_series()
        }
        (DataType::Int32, DataType::Int32) => {
            let ca_a = a_s.i32().unwrap();
            let ca_b = b_s.i32().unwrap();
            Int32Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_sub(b)))),
            )
            .into_series()
        }
        _ => {
            let a_f = a_s.cast(&DataType::Float64)?;
            let b_f = b_s.cast(&DataType::Float64)?;
            let ca_a = a_f.f64().unwrap();
            let ca_b = b_f.f64().unwrap();
            Float64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(ca_b)
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
            let ca_a = a_s.i64().unwrap();
            let ca_b = b_s.i64().unwrap();
            Int64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_mul(b)))),
            )
            .into_series()
        }
        (DataType::Int32, DataType::Int32) => {
            let ca_a = a_s.i32().unwrap();
            let ca_b = b_s.i32().unwrap();
            Int32Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.and_then(|b| a.checked_mul(b)))),
            )
            .into_series()
        }
        _ => {
            let a_f = a_s.cast(&DataType::Float64)?;
            let b_f = b_s.cast(&DataType::Float64)?;
            let ca_a = a_f.f64().unwrap();
            let ca_b = b_f.f64().unwrap();
            Float64Chunked::from_iter_options(
                name.as_str().into(),
                ca_a.into_iter()
                    .zip(ca_b)
                    .map(|(oa, ob)| oa.and_then(|a| ob.map(|b| a * b))),
            )
            .into_series()
        }
    };
    Ok(Some(Column::new(name, out)))
}

// --- Phase 17: unix_timestamp, from_unixtime, make_date, timestamp_*, unix_date, date_from_unix_date, pmod, factorial ---

/// Map PySpark/Java SimpleDateFormat style to chrono strftime.
fn pyspark_format_to_chrono(s: &str) -> String {
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
        .map_err(|e| PolarsError::ComputeError(format!("unix_timestamp: {}", e).into()))?;
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
        .map_err(|e| PolarsError::ComputeError(format!("from_unixtime cast: {}", e).into()))?;
    let ca = casted
        .i64()
        .map_err(|e| PolarsError::ComputeError(format!("from_unixtime: {}", e).into()))?;
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
    let y_ca = y_series.cast(&DataType::Int32)?.i32().unwrap().clone();
    let m_ca = m_series.cast(&DataType::Int32)?.i32().unwrap().clone();
    let d_ca = d_series.cast(&DataType::Int32)?.i32().unwrap().clone();
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

/// factorial(column) - element-wise factorial.
pub fn apply_factorial(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let casted = series
        .cast(&DataType::Int64)
        .map_err(|e| PolarsError::ComputeError(format!("factorial cast: {}", e).into()))?;
    let ca = casted
        .i64()
        .map_err(|e| PolarsError::ComputeError(format!("factorial: {}", e).into()))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_n| opt_n.and_then(factorial_u64)),
    );
    Ok(Some(Column::new(name, out.into_series())))
}
