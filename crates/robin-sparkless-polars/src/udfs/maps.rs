//! Maps UDF implementations.

use super::*;

/// Parse string to map: "k1:v1,k2:v2" -> List(Struct{key, value}) (PySpark str_to_map).
pub fn apply_str_to_map(
    column: Column,
    pair_delim: &str,
    key_value_delim: &str,
) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::StructChunked;
    use polars::chunked_array::builder::get_list_builder;
    use polars::datatypes::Field;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("str_to_map", e))?;
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
                .map_err(|e| compute_err("str_to_map struct", e))?
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
pub fn apply_map_concat(
    columns: &mut [Column],
    disable_map_key_normalization: bool,
) -> PolarsResult<Option<Column>> {
    use polars::chunked_array::StructChunked;
    use polars::chunked_array::builder::get_list_builder;
    use polars::datatypes::Field;
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "map_concat needs at least two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let a_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let b_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let a_ca = a_series.list().map_err(|e| compute_err("map_concat", e))?;
    let b_ca = b_series.list().map_err(|e| compute_err("map_concat", e))?;
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
                let st = s
                    .struct_()
                    .map_err(|e| compute_err("map_concat struct", e))?;
                let k_s = st
                    .field_by_name("key")
                    .map_err(|e| compute_err("map_concat key", e))?;
                let k_s = normalize_map_key_series(k_s, disable_map_key_normalization)?;
                let v_s = st
                    .field_by_name("value")
                    .map_err(|e| compute_err("map_concat value", e))?;
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
                    .map_err(|e| compute_err("map_concat build", e))?
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
        .map_err(|e| compute_err("map_contains_key", e))?;
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

/// Parse string as JSON object; supports one level of string encoding (e.g. "\"{\"k\":1}\"").
fn parse_json_object_str(s: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    let s = s.trim();
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    if let Some(obj) = v.as_object() {
        return Some(obj.clone());
    }
    if let Some(inner) = v.as_str() {
        return parse_json_object_str(inner);
    }
    None
}

/// Validate and normalize JSON for VARIANT / parse_json (PySpark 4). Invalid JSON -> null.
pub fn apply_parse_json_variant(column: Column) -> PolarsResult<Option<Column>> {
    use polars::prelude::*;
    let name = column.field().into_owned().name;
    let ca = column.str()?;
    let results: Vec<Option<String>> = ca
        .into_iter()
        .map(|opt| {
            opt.and_then(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    return None;
                }
                let v: serde_json::Value = serde_json::from_str(trimmed).ok()?;
                serde_json::to_string(&v).ok()
            })
        })
        .collect();
    let out = StringChunked::from_iter_options(name.as_str().into(), results.into_iter());
    Ok(Some(Column::new(name, out.into_series())))
}

/// Convert JSON value to Option<String> for get() result (null -> None, string -> Some, else to_string).
fn json_value_to_option_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// Infer Polars dtype from first non-null JSON value in a slice (for get() on string map column).
fn infer_dtype_from_json_values(values: &[Option<serde_json::Value>]) -> DataType {
    for j in values.iter().flatten() {
        return match j {
            serde_json::Value::Null => continue,
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    DataType::Int64
                } else {
                    DataType::Float64
                }
            }
            serde_json::Value::Bool(_) => DataType::Boolean,
            serde_json::Value::String(_) => DataType::String,
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => DataType::String,
        };
    }
    DataType::String
}

/// Get value for key from map, or null (PySpark get).
/// Handles both List(Struct{key,value}) (create_map) and Struct (createDataFrame dict column).
/// Also handles String column (JSON object per row) when createDataFrame infers dict as string (#1245).
pub fn apply_get(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "get needs two columns (map, key)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let mut map_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let key_series = std::mem::take(&mut columns[1]).take_materialized_series();
    // Work with keys as Utf8 so we can compare string values without extra quotes.
    let key_str_series = key_series
        .cast(&DataType::String)
        .map_err(|e| compute_err("get cast key to string", e))?;
    let key_utf8 = key_str_series
        .str()
        .map_err(|e| compute_err("get key utf8", e))?;
    let key_len = key_utf8.len();
    let map_len = map_series.len();
    if let DataType::Struct(field_metas) = map_series.dtype() {
        // Global map semantics for struct-typed maps: treat each struct field as a map key
        // and use the first non-null value per field as the canonical value for that key.
        let st = map_series
            .struct_()
            .map_err(|e| compute_err("get struct", e))?;
        let fields_series = st.fields_as_series();
        use std::collections::HashMap;
        let mut key_to_field_and_row: HashMap<String, (usize, usize)> = HashMap::new();
        for (field_idx, meta) in field_metas.iter().enumerate() {
            let series = &fields_series[field_idx];
            for row_idx in 0..series.len() {
                if let Ok(av) = series.get(row_idx) {
                    if !matches!(av, polars::prelude::AnyValue::Null) {
                        key_to_field_and_row
                            .entry(meta.name.to_string())
                            .or_insert((field_idx, row_idx));
                        break;
                    }
                }
            }
        }
        let value_dtype = field_metas
            .first()
            .map(|f| f.dtype.clone())
            .unwrap_or(DataType::String);
        let out_len = map_len.max(key_len);
        let mut result_series: Vec<Series> = Vec::with_capacity(out_len);
        for i in 0..out_len {
            let key_idx = if key_len == 1 { 0 } else { i };
            let key_name = key_utf8.get(key_idx).unwrap_or("");
            let one_series =
                if let Some((field_idx, rep_row)) = key_to_field_and_row.get(key_name).copied() {
                    let series = &fields_series[field_idx];
                    let av = series
                        .get(rep_row)
                        .unwrap_or(polars::prelude::AnyValue::Null);
                    any_value_to_single_series(av, &value_dtype)?
                } else {
                    Series::full_null(PlSmallStr::EMPTY, 1, &value_dtype)
                };
            result_series.push(one_series);
        }
        let mut out = result_series.remove(0);
        for s in result_series {
            out.extend(&s)?;
        }
        return Ok(Some(Column::new(name, out)));
    }
    let map_len_initial = map_len;
    if map_len_initial == 1 && key_len > 1 {
        let indices: Vec<u32> = (0..key_len).map(|_| 0u32).collect();
        let idx_ca = UInt32Chunked::from_vec("".into(), indices);
        map_series = map_series
            .take(&idx_ca)
            .map_err(|e| compute_err("get broadcast map", e))?;
    }
    // createDataFrame with dict column infers type "string" and stores JSON object string per row (#1245).
    if map_series.dtype() == &DataType::String {
        let map_ca = map_series
            .str()
            .map_err(|e| compute_err("get map str", e))?;
        let out_len = map_ca.len();
        let values: Vec<Option<serde_json::Value>> = (0..out_len)
            .map(|i| {
                let key_idx = if key_len == 1 { 0 } else { i };
                let key = key_utf8.get(key_idx).unwrap_or("");
                let map_s = map_ca.get(i).unwrap_or("");
                let obj = parse_json_object_str(map_s);
                obj.and_then(|m| m.get(key).or(m.get(key.trim())).cloned())
            })
            .collect();
        let value_dtype = infer_dtype_from_json_values(&values);
        let out_series: Series = match &value_dtype {
            DataType::Int64 => Int64Chunked::from_iter_options(
                name.as_str().into(),
                values.iter().map(|v| v.as_ref().and_then(|j| j.as_i64())),
            )
            .into_series(),
            DataType::Float64 => Float64Chunked::from_iter_options(
                name.as_str().into(),
                values.iter().map(|v| v.as_ref().and_then(|j| j.as_f64())),
            )
            .into_series(),
            DataType::Boolean => BooleanChunked::from_iter_options(
                name.as_str().into(),
                values.iter().map(|v| v.as_ref().and_then(|j| j.as_bool())),
            )
            .into_series(),
            _ => StringChunked::from_iter_options(
                name.as_str().into(),
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(json_value_to_option_string)),
            )
            .into_series(),
        };
        return Ok(Some(Column::new(name, out_series)));
    }
    let map_ca = map_series.list().map_err(|e| compute_err("get", e))?;
    let map_len = map_ca.len();
    let value_dtype = match map_ca.inner_dtype() {
        DataType::Struct(fields) => fields
            .iter()
            .find(|f| f.name == "value")
            .map(|f| f.dtype.clone())
            .unwrap_or(DataType::String),
        _ => DataType::String,
    };
    let mut result_series: Vec<Series> = Vec::with_capacity(map_len);
    for (i, opt_amort) in map_ca.amortized_iter().enumerate() {
        let key_idx = if key_len == 1 { 0 } else { i };
        let target = key_utf8.get(key_idx).unwrap_or("");
        let target_trim = target.trim();
        let mut found: Option<Series> = None;
        if let Some(amort) = opt_amort {
            let list_s = amort.as_ref().as_list();
            for elem in list_s.amortized_iter().flatten() {
                let s = elem.deep_clone();
                if let Ok(st) = s.struct_() {
                    if let Ok(k) = st.field_by_name("key") {
                        let k_str: String = k
                            .get(0)
                            .ok()
                            .map(|av| match av {
                                polars::prelude::AnyValue::String(s) => s.to_string(),
                                _ => av.to_string(),
                            })
                            .unwrap_or_default();
                        if k_str.trim() == target_trim {
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
