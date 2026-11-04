//! Arrays UDF implementations.

use super::*;

/// Flatten list-of-lists to a single list per row (PySpark flatten).
pub fn apply_array_flatten(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series.list().map_err(|e| compute_err("array_flatten", e))?;
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

/// Build per-row position lists for posexplode (0-based indices).
/// Input must be a List column; each list [v0, v1, ..., v{n-1}] yields [0, 1, ..., n-1].
/// Used by Column::posexplode / posexplode_outer to avoid list.eval limitations on i64.
pub fn apply_posexplode_positions(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series
        .list()
        .map_err(|e| compute_err("posexplode_positions", e))?;

    // Heuristic capacity: assume small average list length.
    let values_capacity = list_ca.len().saturating_mul(4);
    let mut builder = ListPrimitiveChunkedBuilder::<Int64Type>::new(
        name.as_str().into(),
        list_ca.len(),
        values_capacity,
        DataType::Int64,
    );

    for opt_s in list_ca.into_iter() {
        match opt_s {
            Some(s) => {
                let len = s.len();
                builder.append_iter((0..len).map(|i| Some(i as i64)));
            }
            None => builder.append_null(),
        }
    }

    let out = builder.finish().into_series();
    Ok(Some(Column::new(name, out)))
}

/// Distinct elements in list preserving first-occurrence order (PySpark array_distinct parity).
pub fn apply_array_distinct_first_order(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series
        .list()
        .map_err(|e| compute_err("array_distinct", e))?;
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

/// True if list contains value (PySpark array_contains). Supports column or literal values.
pub fn apply_array_contains(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    use polars::prelude::{AnyValue, BooleanChunked};
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "array_contains needs two columns (array, value)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let list_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let value_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let list_nulls = list_series.is_null();
    let list_ca = list_series
        .list()
        .map_err(|e| compute_err("array_contains", e))?;
    let inner_dtype = list_ca.inner_dtype().clone();
    let value_casted = value_series.cast(&inner_dtype)?;
    let elem_len = value_casted.len();
    let elem_vec: Vec<Option<AnyValue>> = (0..elem_len).map(|i| value_casted.get(i).ok()).collect();
    let mut results: Vec<Option<bool>> = Vec::with_capacity(list_ca.len());
    for (row_idx, opt_list) in list_ca.amortized_iter().enumerate() {
        // PySpark: when the array itself is null, array_contains returns null.
        // Note: some list backends may yield a non-None amortized value even when the row is null,
        // so also consult the null bitmap.
        let row_is_null = list_nulls.get(row_idx).unwrap_or(false);
        if opt_list.is_none() || row_is_null {
            results.push(None);
            continue;
        }
        let ei = if elem_len == 1 { 0 } else { row_idx };
        let contains = match (opt_list, elem_vec.get(ei)) {
            (Some(amort), Some(Some(av))) => {
                let val_series = any_value_to_single_series(av.clone(), &inner_dtype)?;
                let val_key = series_to_set_key(&val_series);
                let list_s = amort.as_ref().as_list();
                let mut found = false;
                for e in list_s.amortized_iter().flatten() {
                    let key = series_to_set_key(&e.deep_clone());
                    if key == val_key {
                        found = true;
                        break;
                    }
                }
                Some(found)
            }
            // Non-null array but null/missing value → PySpark returns False.
            _ => Some(false),
        };
        results.push(contains);
    }
    let out = BooleanChunked::from_iter_options(name.as_str().into(), results.into_iter());
    Ok(Some(Column::new(name, out.into_series())))
}

/// Create a per-row typed NULL list column.
/// Used to expand `cast(NULL as array<...>)` into N rows (rather than a single empty list).
pub fn apply_literal_null_list_repeat(
    column: Column,
    dtype: &DataType,
) -> PolarsResult<Option<Column>> {
    use polars::prelude::Series;
    let name = column.field().into_owned().name;
    let len = column.len();
    let out = Series::full_null(name.clone(), len, dtype);
    Ok(Some(Column::new(name, out)))
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
            builder
                .append_series(&repeated)
                .map_err(|e| compute_err("array_repeat scalar", e))?;
        }
        let out = builder.finish().into_series();
        return Ok(Some(Column::new(name, out)));
    }

    // List column: repeat each element within the list
    let list_ca = series.list().map_err(|e| compute_err("array_repeat", e))?;
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

pub(super) fn any_value_to_single_series(av: AnyValue, dtype: &DataType) -> PolarsResult<Series> {
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
        .map_err(|e| compute_err("array_append", e))?;
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
        .map_err(|e| compute_err("array_prepend", e))?;
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
        .map_err(|e| compute_err("array_insert", e))?;
    let inner_dtype = list_ca.inner_dtype().clone();
    let pos_ca = pos_series
        .cast(&DataType::Int64)?
        .i64()
        .map_err(|e| compute_err("array_insert: position column", e))?
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
        .map_err(|e| compute_err("array_except", e))?;
    let b_ca = b_series
        .list()
        .map_err(|e| compute_err("array_except", e))?;
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
        .map_err(|e| compute_err("arrays_overlap", e))?;
    let b_ca = b_series
        .list()
        .map_err(|e| compute_err("arrays_overlap", e))?;
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
        .map(|s| s.list().map_err(|e| compute_err("arrays_zip", e)))
        .collect::<PolarsResult<Vec<_>>>()?;
    let len = list_cas[0].len();
    let inner_dtype = list_cas[0].inner_dtype().clone();
    use polars::chunked_array::StructChunked;
    use polars::chunked_array::builder::get_list_builder;
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
                    .map_err(|e| compute_err("arrays_zip struct", e))?
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
        .map_err(|e| compute_err("array_intersect", e))?;
    let b_ca = b_series
        .list()
        .map_err(|e| compute_err("array_intersect", e))?;
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
    let a_ca = a_series.list().map_err(|e| compute_err("array_union", e))?;
    let b_ca = b_series.list().map_err(|e| compute_err("array_union", e))?;
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
