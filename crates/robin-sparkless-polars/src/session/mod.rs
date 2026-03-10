//! SparkSession and catalog (temp views, tables, databases).
//!
//! Catalog and table operations use mutexes. If a lock is poisoned (e.g. a thread panicked while
//! holding it), mutations may no-op and lookups may return empty/None; a message is emitted to
//! stderr in that case. These methods are best-effort when the process is in a bad state.

use crate::dataframe::DataFrame;
use crate::error::{EngineError, polars_to_core_error};
use crate::udf_registry::UdfRegistry;
use base64::Engine;
use polars::chunked_array::StructChunked;
use polars::chunked_array::builder::get_list_builder;
use polars::prelude::{
    DataFrame as PlDataFrame, DataType, Field, IntoSeries, NamedFrom, PlSmallStr, PolarsError,
    Series, TimeUnit,
};
use robin_sparkless_core::{SparklessConfig, date_utils};
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::sync::Arc;

mod builder;
pub use builder::SparkSessionBuilder;
mod reader;
pub use reader::DataFrameReader;

/// Parse "array<element_type>" to get inner type string. Returns None if not array<>.
fn parse_array_element_type(type_str: &str) -> Option<String> {
    let s = type_str.trim();
    if !s.to_lowercase().starts_with("array<") || !s.ends_with('>') {
        return None;
    }
    Some(s[6..s.len() - 1].trim().to_string())
}

/// Split by commas only at top level (not inside < >). Used for struct<...> and map<...>.
fn split_at_top_level_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                out.push(s[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    if start <= s.len() {
        out.push(s[start..].trim().to_string());
    }
    out
}

/// Parse "struct<field:type,...>" to get field (name, type) pairs. Supports nested struct/map/array.
fn parse_struct_fields(type_str: &str) -> Option<Vec<(String, String)>> {
    let s = type_str.trim();
    if !s.to_lowercase().starts_with("struct<") || !s.ends_with('>') {
        return None;
    }
    let inner = s[7..s.len() - 1].trim();
    if inner.is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::new();
    for part in split_at_top_level_commas(inner) {
        let part = part.trim();
        if let Some(idx) = part.find(':') {
            let name = part[..idx].trim().to_string();
            let typ = part[idx + 1..].trim().to_string();
            out.push((name, typ));
        }
    }
    Some(out)
}

/// Parse "map<key_type,value_type>" to get (key_type, value_type). Returns None if not map<>.
/// PySpark: MapType(StringType(), StringType()) -> "map<string,string>".
fn parse_map_key_value_types(type_str: &str) -> Option<(String, String)> {
    let s = type_str.trim().to_lowercase();
    if !s.starts_with("map<") || !s.ends_with('>') {
        return None;
    }
    let inner = s[4..s.len() - 1].trim();
    let comma = inner.find(',')?;
    let key_type = inner[..comma].trim().to_string();
    let value_type = inner[comma + 1..].trim().to_string();
    Some((key_type, value_type))
}

/// True if type string is Decimal(precision, scale), e.g. "decimal(10,2)".
fn is_decimal_type_str(type_str: &str) -> bool {
    let s = type_str.trim().to_lowercase();
    s.starts_with("decimal(") && s.contains(')')
}

/// Try to parse a string as Python dict repr (e.g. `"{'E1': 1, 'E2': 'A'}"`) into JSON Value.
/// PR-F / #691: Sparkless can send struct/map values as single-quoted Python repr.
fn python_dict_repr_to_json(s: &str) -> Option<JsonValue> {
    let s = s.trim();
    if !s.starts_with('{') || !s.ends_with('}') {
        return None;
    }
    // Protect escaped single quotes, then convert single quotes to double, then restore.
    let step1 = s.replace("\\'", "\u{0001}");
    let step2 = step1.replace('\'', "\"");
    let step3 = step2.replace('\u{0001}', "\\\"");
    serde_json::from_str(&step3).ok()
}

/// #971: Parse a string that may be a JSON object, a JSON-encoded string (one extra quote layer),
/// or a Python dict repr, into an optional JSON object for struct/map columns.
fn string_to_json_object(s: &str) -> Option<serde_json::Map<String, JsonValue>> {
    let s = s.trim();
    // Direct JSON object
    if let Ok(v) = serde_json::from_str::<JsonValue>(s) {
        if let Some(obj) = v.as_object() {
            return Some(obj.clone());
        }
        // One level of JSON string encoding (e.g. "\"{'E1': 1, 'E2': 'A'}\"")
        if let Some(inner) = v.as_str() {
            return string_to_json_object(inner);
        }
    }
    python_dict_repr_to_json(s).and_then(|v| v.as_object().cloned())
}

/// Map schema type string to Polars DataType (primitives and list for nested use).
/// Decimal(p,s) is mapped to Float64 (Polars dtype-decimal feature not enabled).
/// PR-G/#710: array<element_type> and nested array<array<...>> supported.
/// #976: Nested array (e.g. array<array<long>>) recurses; elem_type "array<long>" maps to List(Int64).
/// Array element types timestamp, date, struct<...>, map<...> supported for create_dataframe_from_rows parity.
fn json_type_str_to_polars(type_str: &str) -> Option<DataType> {
    let trimmed = type_str.trim();
    let s = trimmed.to_lowercase();
    if is_decimal_type_str(&s) {
        return Some(DataType::Float64);
    }
    if let Some(elem_type) = parse_array_element_type(&s) {
        // Special case: array<map<key_type,value_type>> – represented as
        // List(List(Struct{key, value})) so each array element is its own map
        // (list-of-{key,value} pairs) and collect() yields a list of dicts.
        if let Some((key_type, value_type)) = parse_map_key_value_types(elem_type.trim()) {
            let key_dtype = json_type_str_to_polars(key_type.trim())?;
            let value_dtype = json_type_str_to_polars(value_type.trim())?;
            let map_struct = DataType::Struct(vec![
                Field::new("key".into(), key_dtype),
                Field::new("value".into(), value_dtype),
            ]);
            return Some(DataType::List(Box::new(DataType::List(Box::new(
                map_struct,
            )))));
        }
        let inner = json_type_str_to_polars(elem_type.trim())?;
        return Some(DataType::List(Box::new(inner)));
    }
    // Preserve struct field name casing (e.g. "Inner", "E1") for subscript resolution (#339, #1066).
    if let Some(fields) = parse_struct_fields(trimmed) {
        let polars_fields: Vec<Field> = fields
            .into_iter()
            .map(|(name, typ)| {
                let inner = json_type_str_to_polars(typ.trim())?;
                Some(Field::new(name.into(), inner))
            })
            .collect::<Option<Vec<_>>>()?;
        return Some(DataType::Struct(polars_fields));
    }
    if let Some((key_type, value_type)) = parse_map_key_value_types(&s) {
        let key_dtype = json_type_str_to_polars(key_type.trim())?;
        let value_dtype = json_type_str_to_polars(value_type.trim())?;
        return Some(DataType::Struct(vec![
            Field::new("key".into(), key_dtype),
            Field::new("value".into(), value_dtype),
        ]));
    }
    match s.as_str() {
        // Preserve distinction between IntegerType (32-bit) and LongType (64-bit) for explicit schemas.
        "int" | "integer" => Some(DataType::Int32),
        "bigint" | "long" => Some(DataType::Int64),
        "double" | "float" | "double_precision" => Some(DataType::Float64),
        "string" | "str" | "varchar" => Some(DataType::String),
        "boolean" | "bool" => Some(DataType::Boolean),
        "date" => Some(DataType::Date),
        "timestamp" | "datetime" | "timestamp_ntz" => {
            Some(DataType::Datetime(TimeUnit::Microseconds, None))
        }
        _ => None,
    }
}

/// #420: Check that a cell value is compatible with schema type (for verify_schema).
/// Returns Err with a short message when the value is not valid for the given type.
fn verify_json_value_for_type(v: &JsonValue, type_str: &str) -> Result<(), String> {
    let t = type_str.trim().to_lowercase();
    if v.is_null() {
        return Ok(());
    }
    match t.as_str() {
        "int" | "integer" | "bigint" | "long" | "smallint" | "tinyint" => match v {
            JsonValue::Number(_) => Ok(()), // #1265: accept any number (e.g. 10^18 from UDF); build uses i64 or f64
            JsonValue::String(s) if s.parse::<i64>().is_ok() => Ok(()),
            _ => Err(format!(
                "expected bigint/number, got {}",
                value_type_name(v)
            )),
        },
        "double" | "float" | "double_precision" => match v {
            JsonValue::Number(_) => Ok(()),
            JsonValue::String(s) if s.parse::<f64>().is_ok() => Ok(()),
            _ => Err(format!(
                "expected double/number, got {}",
                value_type_name(v)
            )),
        },
        "string" | "str" | "varchar" => Ok(()), // any value can be string
        "boolean" | "bool" => match v {
            JsonValue::Bool(_) => Ok(()),
            _ => Err(format!("expected boolean, got {}", value_type_name(v))),
        },
        _ => Ok(()), // date, timestamp, array, struct, etc.: allow and let build fail if needed
    }
}

fn value_type_name(v: &JsonValue) -> &'static str {
    match v {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

/// Convert Python repr to JSON-like string for parsing (#1016: "[True, False, True]" -> "[true, false, true]").
fn python_repr_to_json_like(s: &str) -> String {
    let mut out = s.to_string();
    // Word-boundary safe: replace Python literals that appear as list elements (comma or bracket before/after).
    out = out
        .replace("True", "true")
        .replace("False", "false")
        .replace("None", "null");
    out
}

/// Normalize a JSON value to an array for array columns (PySpark parity #625).
/// Accepts: Array, Object with "0","1",... keys (Python list serialization), String that parses as JSON array.
/// #1016: String may be Python repr (e.g. "[True, False, True]"); convert to JSON-like and parse.
/// Returns None for null or when value should be treated as single-element list (#611).
fn json_value_to_array(v: &JsonValue) -> Option<Vec<JsonValue>> {
    match v {
        JsonValue::Null => None,
        JsonValue::Array(arr) => Some(arr.clone()),
        JsonValue::Object(obj) => {
            // Python/serialization sometimes sends list as {"0": x, "1": y}. Build sorted by index.
            let mut indices: Vec<usize> =
                obj.keys().filter_map(|k| k.parse::<usize>().ok()).collect();
            indices.sort_unstable();
            if indices.is_empty() {
                return None;
            }
            let arr: Vec<JsonValue> = indices
                .iter()
                .filter_map(|i| obj.get(&i.to_string()).cloned())
                .collect();
            Some(arr)
        }
        JsonValue::String(s) => {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                parsed.as_array().cloned()
            } else {
                // #1016: Python repr e.g. "[True, False, True]" — not valid JSON; normalize and retry.
                let json_like = python_repr_to_json_like(s);
                serde_json::from_str::<JsonValue>(&json_like)
                    .ok()
                    .and_then(|p| p.as_array().cloned())
            }
        }
        _ => None,
    }
}

/// Infer list element type from first non-null array in the column (for schema "list" / "array").
/// #976: When first element is itself an array (nested array), infer inner type and return ("array<inner>", List(inner_dtype)).
fn infer_list_element_type(rows: &[Vec<JsonValue>], col_idx: usize) -> Option<(String, DataType)> {
    for row in rows {
        let v = row.get(col_idx)?;
        let arr = json_value_to_array(v)?;
        let first = arr.first()?;
        return Some(match first {
            JsonValue::String(_) => ("string".to_string(), DataType::String),
            JsonValue::Number(n) => {
                if n.as_i64().is_some() {
                    ("bigint".to_string(), DataType::Int64)
                } else {
                    ("double".to_string(), DataType::Float64)
                }
            }
            JsonValue::Bool(_) => ("boolean".to_string(), DataType::Boolean),
            JsonValue::Null => continue,
            _ if json_value_to_array(first).is_some() => {
                // Nested array: infer inner element type from first inner element.
                let inner_arr = json_value_to_array(first).unwrap();
                let inner_first = match inner_arr.first() {
                    Some(f) => f,
                    None => continue,
                };
                let (inner_type, inner_dtype) = match inner_first {
                    JsonValue::Number(n) => {
                        if n.as_i64().is_some() {
                            ("bigint".to_string(), DataType::Int64)
                        } else {
                            ("double".to_string(), DataType::Float64)
                        }
                    }
                    JsonValue::String(_) => ("string".to_string(), DataType::String),
                    JsonValue::Bool(_) => ("boolean".to_string(), DataType::Boolean),
                    _ => ("string".to_string(), DataType::String),
                };
                (
                    format!("array<{inner_type}>"),
                    DataType::List(Box::new(inner_dtype)),
                )
            }
            _ => ("string".to_string(), DataType::String),
        });
    }
    None
}

/// Build a length-N Series from `Vec<Option<JsonValue>>` for a given type (recursive for struct/array).
fn json_values_to_series(
    values: &[Option<JsonValue>],
    type_str: &str,
    name: &str,
) -> Result<Series, PolarsError> {
    use chrono::{NaiveDate, NaiveDateTime};
    let epoch = date_utils::epoch_naive_date();
    let type_lower = type_str.trim().to_lowercase();

    if let Some(elem_type) = parse_array_element_type(&type_lower) {
        let inner_dtype = json_type_str_to_polars(&elem_type).ok_or_else(|| {
            PolarsError::ComputeError(
                format!("array element type '{elem_type}' not supported").into(),
            )
        })?;
        let mut builder = get_list_builder(&inner_dtype, 64, values.len(), name.into());
        for v in values.iter() {
            if v.as_ref().is_none_or(|x| matches!(x, JsonValue::Null)) {
                builder.append_null();
            } else if let Some(arr) = v.as_ref().and_then(json_value_to_array) {
                // #625: Array, Object with "0","1",..., or string that parses as JSON array (PySpark list parity).
                // #710/PR-G: Nested array (elem_type is array<...>): one list series per element, then vals + from_any_values.
                let elem_series: Vec<Series> = if parse_array_element_type(&elem_type).is_some() {
                    arr.iter()
                        .map(|e| json_values_to_series(&[Some(e.clone())], &elem_type, "elem"))
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    arr.iter()
                        .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                        .collect::<Result<Vec<_>, _>>()?
                };
                let vals: Vec<_> = elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                let s = Series::from_any_values_and_dtype(
                    PlSmallStr::EMPTY,
                    &vals,
                    &inner_dtype,
                    false,
                )
                .map_err(|e| PolarsError::ComputeError(format!("array elem: {e}").into()))?;
                builder.append_series(&s)?;
            } else {
                // #611: PySpark accepts single value as one-element list for array columns.
                let single_arr = [v.clone().unwrap_or(JsonValue::Null)];
                let elem_series: Vec<Series> = if parse_array_element_type(&elem_type).is_some() {
                    single_arr
                        .iter()
                        .map(|e| json_values_to_series(&[Some(e.clone())], &elem_type, "elem"))
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    single_arr
                        .iter()
                        .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                        .collect::<Result<Vec<_>, _>>()?
                };
                let vals: Vec<_> = elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                let arr_series = Series::from_any_values_and_dtype(
                    PlSmallStr::EMPTY,
                    &vals,
                    &inner_dtype,
                    false,
                )
                .map_err(|e| PolarsError::ComputeError(format!("array elem: {e}").into()))?;
                builder.append_series(&arr_series)?;
            }
        }
        return Ok(builder.finish().into_series());
    }

    // Use type_str (not type_lower) so struct field names preserve case (e.g. E1, E2); col("StructValue.E1") then resolves when case_sensitive.
    if let Some(fields) = parse_struct_fields(type_str.trim()) {
        let mut field_series_vec: Vec<Vec<Option<JsonValue>>> = (0..fields.len())
            .map(|_| Vec::with_capacity(values.len()))
            .collect();
        for v in values.iter() {
            // #610: Accept string that parses as JSON object or array (e.g. Python tuple serialized as "[1, \"y\"]").
            // #691/PR-F: Accept Python dict repr string (e.g. "{'a': 1, 'b': 'x'}").
            // #610/#691/#971: Accept string that parses as JSON object/array or Python dict repr; allow one level of JSON encoding.
            let effective: Option<JsonValue> = match v.as_ref() {
                Some(JsonValue::String(s)) => {
                    if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                        if parsed.is_object() || parsed.is_array() {
                            Some(parsed)
                        } else if let Some(inner) = parsed.as_str() {
                            string_to_json_object(inner)
                                .map(JsonValue::Object)
                                .or_else(|| v.clone())
                        } else {
                            v.clone()
                        }
                    } else if let Some(obj) = string_to_json_object(s) {
                        Some(JsonValue::Object(obj))
                    } else {
                        v.clone()
                    }
                }
                _ => v.clone(),
            };
            if effective
                .as_ref()
                .is_none_or(|x| matches!(x, JsonValue::Null))
            {
                for fc in &mut field_series_vec {
                    fc.push(None);
                }
            } else if let Some(arr) = effective
                .as_ref()
                .and_then(|x| x.as_array().cloned())
                .or_else(|| effective.as_ref().and_then(json_value_to_array))
            {
                // #634: Array or object with "0","1",... keys (Python tuple serialization) — positional.
                for (fi, _) in fields.iter().enumerate() {
                    field_series_vec[fi].push(arr.get(fi).cloned());
                }
            } else if let Some(obj) = effective.as_ref().and_then(|x| x.as_object()) {
                // #971: case-insensitive field lookup (Python dict may have "E1"/"E2", schema "e1"/"e2").
                // #1015: Sparkless may send struct as {"E1": v1, "E2": v2}; fallback by position so we build with schema names (a, b) and collect() yields row["nested"]["a"].
                for (fi, (fname, _)) in fields.iter().enumerate() {
                    let val = obj
                        .get(fname)
                        .or_else(|| {
                            obj.keys()
                                .find(|k| k.eq_ignore_ascii_case(fname))
                                .and_then(|k| obj.get(k))
                        })
                        .or_else(|| obj.get(&format!("E{}", fi + 1)));
                    field_series_vec[fi].push(val.cloned());
                }
            } else if let Some(JsonValue::String(s)) = v.as_ref() {
                // #973: final attempt — parse string as object or array (tuple/list serialization).
                let trimmed = s.trim();
                let parsed_obj = string_to_json_object(trimmed);
                let parsed_arr: Option<Vec<JsonValue>> = serde_json::from_str(trimmed)
                    .ok()
                    .and_then(|j: JsonValue| j.as_array().cloned());
                if let Some(obj) = parsed_obj {
                    for (fi, (fname, _)) in fields.iter().enumerate() {
                        let val = obj
                            .get(fname)
                            .or_else(|| {
                                obj.keys()
                                    .find(|k| k.eq_ignore_ascii_case(fname))
                                    .and_then(|k| obj.get(k))
                            })
                            .or_else(|| obj.get(&format!("E{}", fi + 1)));
                        field_series_vec[fi].push(val.cloned());
                    }
                } else if let Some(arr) = parsed_arr {
                    for (fi, _) in fields.iter().enumerate() {
                        field_series_vec[fi].push(arr.get(fi).cloned());
                    }
                } else {
                    return Err(PolarsError::ComputeError(
                        "struct value must be object (by field name) or array (by position). \
                         PySpark accepts dict or tuple/list for struct columns."
                            .into(),
                    ));
                }
            } else {
                return Err(PolarsError::ComputeError(
                    "struct value must be object (by field name) or array (by position). \
                     PySpark accepts dict or tuple/list for struct columns."
                        .into(),
                ));
            }
        }
        let series_per_field: Vec<Series> = fields
            .iter()
            .enumerate()
            .map(|(fi, (fname, ftype))| json_values_to_series(&field_series_vec[fi], ftype, fname))
            .collect::<Result<Vec<_>, _>>()?;
        let field_refs: Vec<&Series> = series_per_field.iter().collect();
        let st = StructChunked::from_series(name.into(), values.len(), field_refs.iter().copied())
            .map_err(|e| PolarsError::ComputeError(format!("struct column: {e}").into()))?
            .into_series();
        return Ok(st);
    }

    // #978: Support integer/smallint/tinyint, double_precision, and decimal(...,...) so json_value_to_series doesn't hit "unsupported type".
    if is_decimal_type_str(&type_lower) {
        let vals: Vec<Option<f64>> = values
            .iter()
            .map(|ov| {
                ov.as_ref().and_then(|v| match v {
                    JsonValue::Number(n) => n.as_f64(),
                    JsonValue::Null => None,
                    _ => None,
                })
            })
            .collect();
        return Ok(Series::new(name.into(), vals));
    }

    match type_lower.as_str() {
        "int" | "integer" | "bigint" | "long" | "smallint" | "tinyint" => {
            let vals: Vec<Option<i64>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::Number(n) => n.as_i64(),
                        JsonValue::String(s) => s.parse::<i64>().ok(),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        "double" | "float" | "double_precision" => {
            let vals: Vec<Option<f64>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::Number(n) => n.as_f64(),
                        JsonValue::String(s) => s.parse::<f64>().ok(),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        "string" | "str" | "varchar" => {
            let vals: Vec<Option<&str>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::String(s) => Some(s.as_str()),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            let owned: Vec<Option<String>> =
                vals.into_iter().map(|o| o.map(|s| s.to_string())).collect();
            Ok(Series::new(name.into(), owned))
        }
        "boolean" | "bool" => {
            // #978: Accept Bool, Number(0/1), and String("true"/"false"/"1"/"0") for boolean column.
            let vals: Vec<Option<bool>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::Bool(b) => Some(*b),
                        JsonValue::Number(n) => n
                            .as_i64()
                            .map(|i| i != 0)
                            .or_else(|| n.as_f64().map(|f| f != 0.0)),
                        JsonValue::String(s) => {
                            let s = s.trim().to_lowercase();
                            match s.as_str() {
                                "true" | "1" => Some(true),
                                "false" | "0" => Some(false),
                                _ => None,
                            }
                        }
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        "date" => {
            let vals: Vec<Option<i32>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::String(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .ok()
                            .map(|d| (d - epoch).num_days() as i32),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            let s = Series::new(name.into(), vals);
            s.cast(&DataType::Date)
                .map_err(|e| PolarsError::ComputeError(format!("date cast: {e}").into()))
        }
        "timestamp" | "datetime" | "timestamp_ntz" => {
            let vals: Vec<Option<i64>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::String(s) => {
                            let parsed = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
                                .or_else(|_| {
                                    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").map_err(
                                        |e| PolarsError::ComputeError(e.to_string().into()),
                                    )
                                })
                                .or_else(|_| {
                                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map_err(
                                        |e| PolarsError::ComputeError(e.to_string().into()),
                                    )
                                })
                                .or_else(|_| {
                                    NaiveDate::parse_from_str(s, "%Y-%m-%d")
                                        .map_err(|e| {
                                            PolarsError::ComputeError(e.to_string().into())
                                        })
                                        .and_then(|d| {
                                            d.and_hms_opt(0, 0, 0).ok_or_else(|| {
                                                PolarsError::ComputeError(
                                                    "date to datetime (0:0:0)".into(),
                                                )
                                            })
                                        })
                                });
                            parsed.ok().map(|dt| dt.and_utc().timestamp_micros())
                        }
                        JsonValue::Object(obj) => parse_datetime_from_json_object(obj),
                        JsonValue::Number(n) => n.as_i64(),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            let s = Series::new(name.into(), vals);
            s.cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                .map_err(|e| PolarsError::ComputeError(format!("datetime cast: {e}").into()))
        }
        "binary" => {
            let vals: Vec<Option<Vec<u8>>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::String(s) => base64::engine::general_purpose::STANDARD
                            .decode(s.as_bytes())
                            .ok(),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        _ => Err(PolarsError::ComputeError(
            format!("json_values_to_series: unsupported type '{type_str}'").into(),
        )),
    }
}

/// Build a single Series from a JsonValue for use as list element or struct field.
fn json_value_to_series_single(
    value: &JsonValue,
    type_str: &str,
    name: &str,
) -> Result<Series, PolarsError> {
    use chrono::NaiveDate;
    use polars::datatypes::ListChunked;
    use polars::prelude::{DataType, Field};
    let epoch = date_utils::epoch_naive_date();
    let type_trimmed = type_str.trim();
    let type_lower = type_trimmed.to_lowercase();
    // #1066: Nested array (e.g. createDataFrame with {"matrix": [[1,2,3],[4,5,6]]}) when type is array<...>.
    if let (JsonValue::Array(arr), Some(elem_type)) = (value, parse_array_element_type(&type_lower))
    {
        let inner_dtype = json_type_str_to_polars(&elem_type).ok_or_else(|| {
            PolarsError::ComputeError(
                format!("array element type '{elem_type}' not supported").into(),
            )
        })?;
        let elem_series: Vec<Series> = if parse_array_element_type(&elem_type).is_some() {
            arr.iter()
                .map(|e| json_values_to_series(&[Some(e.clone())], &elem_type, "elem"))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            arr.iter()
                .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                .collect::<Result<Vec<_>, _>>()?
        };
        let vals: Vec<_> = elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
        let s = Series::from_any_values_and_dtype(PlSmallStr::EMPTY, &vals, &inner_dtype, false)
            .map_err(|e| PolarsError::ComputeError(format!("array elem: {e}").into()))?;
        let mut builder = get_list_builder(&inner_dtype, 64, 1, name.into());
        builder.append_series(&s)?;
        return Ok(builder.finish().into_series());
    }
    // Struct element type (e.g. array<struct<name:string,age:int>>)
    if let Some(fields) = parse_struct_fields(type_trimmed) {
        if let Some(st) = json_object_or_array_to_struct_series(value, &fields, name)? {
            return Ok(st);
        }
        // Null value: build a single null struct of the appropriate dtype.
        if let Some(dtype) = json_type_str_to_polars(type_trimmed) {
            let s = Series::new_null(name.into(), 1).cast(&dtype)?;
            return Ok(s);
        }
    }
    // Map element type (e.g. array<map<string,int>> where elem_type is "map<string,int>")
    if let Some((key_type, value_type)) = parse_map_key_value_types(&type_lower) {
        let key_dtype = json_type_str_to_polars(key_type.trim()).ok_or_else(|| {
            PolarsError::ComputeError(
                format!("array element key type '{key_type}' not supported").into(),
            )
        })?;
        let value_dtype = json_type_str_to_polars(value_type.trim()).ok_or_else(|| {
            PolarsError::ComputeError(
                format!("array element value type '{value_type}' not supported").into(),
            )
        })?;
        // Null map: represent as a null list-of-struct (one element, null).
        if matches!(value, JsonValue::Null) {
            let struct_dtype = DataType::Struct(vec![
                Field::new("key".into(), key_dtype.clone()),
                Field::new("value".into(), value_dtype.clone()),
            ]);
            let lc = ListChunked::full_null_with_dtype(name.into(), 1, &struct_dtype);
            return Ok(lc.into_series());
        }
        // Object map or string that parses to JSON/Python dict repr.
        let obj_opt = match value {
            JsonValue::Object(obj) => Some(obj.clone()),
            JsonValue::String(s) => string_to_json_object(s),
            _ => None,
        };
        if let Some(obj) = obj_opt {
            let st = json_object_to_map_struct_series(
                &obj,
                &key_type,
                &value_type,
                &key_dtype,
                &value_dtype,
                name,
            )?;
            let struct_dtype = st.dtype().clone();
            let mut builder = get_list_builder(&struct_dtype, st.len(), 1, name.into());
            builder.append_series(&st)?;
            return Ok(builder.finish().into_series());
        }
        return Err(PolarsError::ComputeError(
            format!("json_value_to_series: unsupported {type_str} for {value:?}").into(),
        ));
    }
    match (value, type_lower.as_str()) {
        (JsonValue::Null, _) => Ok(Series::new_null(name.into(), 1)),
        (JsonValue::Number(n), "int" | "integer" | "bigint" | "long" | "smallint" | "tinyint") => {
            Ok(Series::new(name.into(), vec![n.as_i64()]))
        }
        (JsonValue::String(s), "int" | "integer" | "bigint" | "long" | "smallint" | "tinyint") => {
            let i = s
                .trim()
                .parse::<i64>()
                .map_err(|e| PolarsError::ComputeError(format!("int parse: {e}").into()))?;
            Ok(Series::new(name.into(), vec![Some(i)]))
        }
        (JsonValue::Number(n), "double" | "float" | "double_precision") => {
            Ok(Series::new(name.into(), vec![n.as_f64()]))
        }
        (JsonValue::String(s), "double" | "float" | "double_precision") => {
            let f = s
                .trim()
                .parse::<f64>()
                .map_err(|e| PolarsError::ComputeError(format!("float parse: {e}").into()))?;
            Ok(Series::new(name.into(), vec![Some(f)]))
        }
        (JsonValue::Number(n), t) if is_decimal_type_str(t) => {
            Ok(Series::new(name.into(), vec![n.as_f64()]))
        }
        (JsonValue::String(s), "string" | "str" | "varchar") => {
            Ok(Series::new(name.into(), vec![s.as_str()]))
        }
        (JsonValue::Number(n), "string" | "str" | "varchar") => {
            // #971: map<string,*> values may be numbers when Python sends dict with mixed types; coerce to string.
            let s = n
                .as_f64()
                .map(|f| f.to_string())
                .or_else(|| n.as_i64().map(|i| i.to_string()))
                .unwrap_or_else(|| "null".to_string());
            Ok(Series::new(name.into(), vec![s.as_str()]))
        }
        (JsonValue::Bool(b), "boolean" | "bool") => Ok(Series::new(name.into(), vec![*b])),
        (JsonValue::Number(n), "boolean" | "bool") => {
            let b = n
                .as_i64()
                .map(|i| i != 0)
                .or_else(|| n.as_f64().map(|f| f != 0.0))
                .unwrap_or(false);
            Ok(Series::new(name.into(), vec![b]))
        }
        (JsonValue::String(s), "boolean" | "bool") => {
            let b = match s.trim().to_lowercase().as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => {
                    return Err(PolarsError::ComputeError(
                        format!("boolean parse: expected true/false/1/0, got {:?}", s).into(),
                    ));
                }
            };
            Ok(Series::new(name.into(), vec![b]))
        }
        (JsonValue::String(s), "date") => {
            let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| PolarsError::ComputeError(format!("date parse: {e}").into()))?;
            let days = (d - epoch).num_days() as i32;
            let s = Series::new(name.into(), vec![days]).cast(&DataType::Date)?;
            Ok(s)
        }
        (JsonValue::Object(obj), "date") => {
            let days = parse_date_from_json_object(obj).ok_or_else(|| {
                PolarsError::ComputeError("date object: missing year/month/day".into())
            })?;
            let s = Series::new(name.into(), vec![days]).cast(&DataType::Date)?;
            Ok(s)
        }
        (JsonValue::String(s), "timestamp" | "datetime" | "timestamp_ntz") => {
            let micros = parse_timestamp_str_to_micros(s)?;
            let s = Series::new(name.into(), vec![Some(micros)])
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
            Ok(s)
        }
        (JsonValue::Number(n), "timestamp" | "datetime" | "timestamp_ntz") => {
            let micros = n.as_i64();
            let s = Series::new(name.into(), vec![micros])
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
            Ok(s)
        }
        (JsonValue::Object(obj), "timestamp" | "datetime" | "timestamp_ntz") => {
            let micros = parse_datetime_from_json_object(obj).ok_or_else(|| {
                PolarsError::ComputeError("datetime object: missing year/month/day".into())
            })?;
            let s = Series::new(name.into(), vec![Some(micros)])
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
            Ok(s)
        }
        _ => Err(PolarsError::ComputeError(
            format!("json_value_to_series: unsupported {type_str} for {value:?}").into(),
        )),
    }
}

/// Parse date from JSON object with year, month, day (e.g. Python datetime.date serialization). Returns days since epoch.
fn parse_date_from_json_object(obj: &serde_json::Map<String, JsonValue>) -> Option<i32> {
    let year = obj.get("year").and_then(|v| v.as_i64())? as i32;
    let month = obj.get("month").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
    let day = obj.get("day").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
    let epoch = date_utils::epoch_naive_date();
    chrono::NaiveDate::from_ymd_opt(year, month, day).map(|d| (d - epoch).num_days() as i32)
}

/// Parse datetime from JSON object (year, month, day, hour?, minute?, second?). Returns microseconds since epoch.
fn parse_datetime_from_json_object(obj: &serde_json::Map<String, JsonValue>) -> Option<i64> {
    let year = obj.get("year").and_then(|v| v.as_i64())? as i32;
    let month = obj.get("month").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
    let day = obj.get("day").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
    let hour = obj.get("hour").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let min = obj.get("minute").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let sec = obj.get("second").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    let d = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
    let dt = d.and_hms_opt(hour, min, sec)?;
    Some(dt.and_utc().timestamp_micros())
}

/// Parse a single timestamp string to microseconds since epoch (for json_value_to_series_single).
fn parse_timestamp_str_to_micros(s: &str) -> Result<i64, PolarsError> {
    use chrono::{NaiveDate, NaiveDateTime};
    let s = s.trim().trim_end_matches('Z');
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        })
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
                .and_then(|d| {
                    d.and_hms_opt(0, 0, 0)
                        .ok_or_else(|| PolarsError::ComputeError("date to datetime (0:0:0)".into()))
                })
        })
        .map(|dt| dt.and_utc().timestamp_micros())
}

/// Build a struct Series from JsonValue::Object or JsonValue::Array (field-order) or Null.
#[allow(dead_code)]
fn json_object_or_array_to_struct_series(
    value: &JsonValue,
    fields: &[(String, String)],
    _name: &str,
) -> Result<Option<Series>, PolarsError> {
    use polars::prelude::StructChunked;
    if matches!(value, JsonValue::Null) {
        return Ok(None);
    }
    // #610: Accept string that parses as JSON object or array. #691/PR-F: Python dict repr.
    let effective = match value {
        JsonValue::String(s) => {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                if parsed.is_object() || parsed.is_array() {
                    parsed
                } else {
                    value.clone()
                }
            } else if let Some(parsed) = python_dict_repr_to_json(s) {
                parsed
            } else {
                value.clone()
            }
        }
        _ => value.clone(),
    };
    let mut field_series: Vec<Series> = Vec::with_capacity(fields.len());
    // #634: Positional struct from array or from object with "0","1",... keys (Python tuple).
    let pos_arr_owned: Option<Vec<JsonValue>> = effective
        .as_array()
        .cloned()
        .or_else(|| json_value_to_array(&effective));
    let pos_arr = pos_arr_owned.as_deref();
    for (idx, (fname, ftype)) in fields.iter().enumerate() {
        let fval = if let Some(arr) = pos_arr {
            arr.get(idx).unwrap_or(&JsonValue::Null)
        } else if let Some(obj) = effective.as_object() {
            // #1015: object may have E1,E2 keys (Sparkless); use schema name then positional so struct has names (a, b).
            obj.get(fname)
                .or_else(|| obj.get(&format!("E{}", idx + 1)))
                .unwrap_or(&JsonValue::Null)
        } else {
            return Err(PolarsError::ComputeError(
                "struct value must be object (by field name) or array (by position). \
                 PySpark accepts dict or tuple/list for struct columns."
                    .into(),
            ));
        };
        let s = json_value_to_series_single(fval, ftype, fname)?;
        field_series.push(s);
    }
    let field_refs: Vec<&Series> = field_series.iter().collect();
    let st = StructChunked::from_series(PlSmallStr::EMPTY, 1, field_refs.iter().copied())
        .map_err(|e| PolarsError::ComputeError(format!("struct from value: {e}").into()))?
        .into_series();
    Ok(Some(st))
}

/// Build a single row's map column value as List(Struct{key, value}) element from a JSON object.
/// PySpark parity #627: create_dataframe_from_rows accepts dict for map columns.
fn json_object_to_map_struct_series(
    obj: &serde_json::Map<String, JsonValue>,
    key_type: &str,
    value_type: &str,
    key_dtype: &DataType,
    value_dtype: &DataType,
    _name: &str,
) -> Result<Series, PolarsError> {
    if obj.is_empty() {
        let key_series = Series::new("key".into(), Vec::<String>::new());
        let value_series = Series::new_empty(PlSmallStr::EMPTY, value_dtype);
        let st = StructChunked::from_series(
            PlSmallStr::EMPTY,
            0,
            [&key_series, &value_series].iter().copied(),
        )
        .map_err(|e| PolarsError::ComputeError(format!("map struct empty: {e}").into()))?
        .into_series();
        return Ok(st);
    }
    let keys: Vec<String> = obj.keys().cloned().collect();
    let mut value_series = None::<Series>;
    for v in obj.values() {
        let s = json_value_to_series_single(v, value_type, "value")?;
        value_series = Some(match value_series.take() {
            None => s,
            Some(mut acc) => {
                acc.extend(&s).map_err(|e| {
                    PolarsError::ComputeError(format!("map value extend: {e}").into())
                })?;
                acc
            }
        });
    }
    let value_series =
        value_series.unwrap_or_else(|| Series::new_empty(PlSmallStr::EMPTY, value_dtype));
    let key_series = Series::new("key".into(), keys.clone());
    let key_series = if key_type.trim().to_lowercase().as_str() == "string"
        || key_type.trim().to_lowercase().as_str() == "str"
        || key_type.trim().to_lowercase().as_str() == "varchar"
    {
        key_series
    } else {
        key_series
            .cast(key_dtype)
            .map_err(|e| PolarsError::ComputeError(format!("map key cast: {e}").into()))?
    };
    let st = StructChunked::from_series(
        PlSmallStr::EMPTY,
        key_series.len(),
        [&key_series, &value_series].iter().copied(),
    )
    .map_err(|e| PolarsError::ComputeError(format!("map struct: {e}").into()))?
    .into_series();
    Ok(st)
}

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::thread_local;

thread_local! {
    /// Thread-local SparkSession for UDF resolution in call_udf. Set by get_or_create.
    static THREAD_UDF_SESSION: RefCell<Option<SparkSession>> = const { RefCell::new(None) };
}

/// Set the thread-local session for UDF resolution (call_udf). Used by get_or_create.
pub(crate) fn set_thread_udf_session(session: SparkSession) {
    let session_tz = session.config.get("spark.sql.session.timeZone").cloned();
    crate::set_thread_udf_context_with_tz(
        Arc::new(session.udf_registry.clone()),
        session.is_case_sensitive(),
        session_tz,
    );
    THREAD_UDF_SESSION.with(|cell| *cell.borrow_mut() = Some(session));
}

/// Get the thread-local session for UDF resolution. (call_udf uses expr's thread context; this is kept for compatibility.)
#[allow(dead_code)]
pub(crate) fn get_thread_udf_session() -> Option<SparkSession> {
    THREAD_UDF_SESSION.with(|cell| cell.borrow().clone())
}

/// Clear the thread-local session used for UDF resolution.
pub(crate) fn clear_thread_udf_session() {
    THREAD_UDF_SESSION.with(|cell| *cell.borrow_mut() = None);
}

/// Catalog of global temporary views (process-scoped). Persists across sessions within the same process.
/// PySpark: createOrReplaceGlobalTempView / spark.table("global_temp.name").
static GLOBAL_TEMP_CATALOG: OnceLock<Arc<Mutex<HashMap<String, DataFrame>>>> = OnceLock::new();

fn global_temp_catalog() -> Arc<Mutex<HashMap<String, DataFrame>>> {
    GLOBAL_TEMP_CATALOG
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

/// Catalog of temporary view names to DataFrames (session-scoped). Uses Arc<Mutex<>> for Send+Sync (Python bindings).
pub type TempViewCatalog = Arc<Mutex<HashMap<String, DataFrame>>>;

/// Catalog of saved table names to DataFrames (session-scoped). Used by saveAsTable.
pub type TableCatalog = Arc<Mutex<HashMap<String, DataFrame>>>;

/// Names of databases/schemas created via CREATE DATABASE / CREATE SCHEMA (session-scoped). Persisted when SQL DDL runs.
pub type DatabaseCatalog = Arc<Mutex<HashSet<String>>>;

/// Main entry point for creating DataFrames and executing queries
/// Similar to PySpark's SparkSession but using Polars as the backend
#[derive(Clone)]
pub struct SparkSession {
    app_name: Option<String>,
    master: Option<String>,
    config: HashMap<String, String>,
    /// Temporary views: name -> DataFrame. Session-scoped; cleared when session is dropped.
    pub(crate) catalog: TempViewCatalog,
    /// Saved tables (saveAsTable): name -> DataFrame. Session-scoped; separate namespace from temp views.
    pub(crate) tables: TableCatalog,
    /// Databases/schemas created via CREATE DATABASE / CREATE SCHEMA. Session-scoped; used by listDatabases/databaseExists.
    pub(crate) databases: DatabaseCatalog,
    /// UDF registry: Rust UDFs. Session-scoped.
    pub(crate) udf_registry: UdfRegistry,
    /// Current database/schema name for this session (PySpark: catalog.currentDatabase()).
    pub(crate) current_database: Arc<Mutex<String>>,
    /// Best-effort cache flags (PySpark: catalog.cacheTable / isCached). No execution impact today.
    pub(crate) cached_tables: Arc<Mutex<HashSet<String>>>,
}

impl SparkSession {
    pub fn new(
        app_name: Option<String>,
        master: Option<String>,
        config: HashMap<String, String>,
    ) -> Self {
        SparkSession {
            app_name,
            master,
            config,
            catalog: Arc::new(Mutex::new(HashMap::new())),
            tables: Arc::new(Mutex::new(HashMap::new())),
            databases: Arc::new(Mutex::new(HashSet::new())),
            udf_registry: UdfRegistry::new(),
            current_database: Arc::new(Mutex::new("default".to_string())),
            cached_tables: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Return the application name for this session (if set).
    pub fn app_name(&self) -> Option<String> {
        self.app_name.clone()
    }

    /// Create a new logical session with isolated catalogs/state (PySpark: SparkSession.newSession()).
    /// Global temp views remain process-scoped; everything else is session-scoped.
    pub fn new_session(&self) -> Self {
        let current_db = self.current_database();
        SparkSession {
            app_name: self.app_name.clone(),
            master: self.master.clone(),
            config: self.config.clone(),
            catalog: Arc::new(Mutex::new(HashMap::new())),
            tables: Arc::new(Mutex::new(HashMap::new())),
            databases: Arc::new(Mutex::new(HashSet::new())),
            udf_registry: UdfRegistry::new(),
            current_database: Arc::new(Mutex::new(current_db)),
            cached_tables: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Return the current database for this session. Defaults to "default".
    pub fn current_database(&self) -> String {
        match self.current_database.lock() {
            Ok(guard) => guard.clone(),
            Err(_) => "default".to_string(),
        }
    }

    /// Set the current database for this session. Errors if the name does not exist.
    pub fn set_current_database(&self, name: &str) -> Result<(), EngineError> {
        if name.is_empty() {
            return Err(EngineError::User(
                "database name cannot be empty".to_string(),
            ));
        }
        if !self.database_exists(name) {
            return Err(EngineError::NotFound(format!(
                "Database '{name}' does not exist"
            )));
        }
        match self.current_database.lock() {
            Ok(mut guard) => {
                *guard = name.to_string();
                Ok(())
            }
            Err(_) => Ok(()),
        }
    }

    pub fn cache_table(&self, name: &str) {
        if name.is_empty() {
            return;
        }
        if let Ok(mut guard) = self.cached_tables.lock() {
            guard.insert(name.to_string());
        }
    }

    pub fn uncache_table(&self, name: &str) {
        if name.is_empty() {
            return;
        }
        if let Ok(mut guard) = self.cached_tables.lock() {
            guard.remove(name);
        }
    }

    pub fn is_cached(&self, name: &str) -> bool {
        if name.is_empty() {
            return false;
        }
        self.cached_tables
            .lock()
            .map(|g| g.contains(name))
            .unwrap_or(false)
    }

    /// Register a DataFrame as a temporary view (PySpark: createOrReplaceTempView).
    /// The view is session-scoped and is dropped when the session is dropped.
    /// If the catalog lock is poisoned (e.g. a thread panicked while holding it), the operation is best-effort and a message is emitted to stderr.
    pub fn create_or_replace_temp_view(&self, name: &str, df: DataFrame) {
        match self.catalog.lock() {
            Ok(mut m) => {
                m.insert(name.to_string(), df);
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: catalog lock poisoned, create_or_replace_temp_view may have failed"
                );
            }
        }
    }

    /// Global temp view (PySpark: createGlobalTempView). Persists across sessions within the same process.
    /// If the global catalog lock is poisoned, the operation is best-effort and a message is emitted to stderr.
    pub fn create_global_temp_view(&self, name: &str, df: DataFrame) {
        match global_temp_catalog().lock() {
            Ok(mut m) => {
                m.insert(name.to_string(), df);
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: global temp catalog lock poisoned, create_global_temp_view may have failed"
                );
            }
        }
    }

    /// Global temp view (PySpark: createOrReplaceGlobalTempView). Persists across sessions within the same process.
    /// If the global catalog lock is poisoned, the operation is best-effort and a message is emitted to stderr.
    pub fn create_or_replace_global_temp_view(&self, name: &str, df: DataFrame) {
        match global_temp_catalog().lock() {
            Ok(mut m) => {
                m.insert(name.to_string(), df);
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: global temp catalog lock poisoned, create_or_replace_global_temp_view may have failed"
                );
            }
        }
    }

    /// Drop a temporary view by name (PySpark: catalog.dropTempView).
    /// No error if the view does not exist.
    /// If the catalog lock is poisoned, the operation is best-effort and a message is emitted to stderr.
    pub fn drop_temp_view(&self, name: &str) {
        match self.catalog.lock() {
            Ok(mut m) => {
                m.remove(name);
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: catalog lock poisoned, drop_temp_view may have failed"
                );
            }
        }
    }

    /// Drop a global temporary view (PySpark: catalog.dropGlobalTempView). Removes from process-wide catalog.
    /// If the global catalog lock is poisoned, returns false and a message is emitted to stderr.
    pub fn drop_global_temp_view(&self, name: &str) -> bool {
        match global_temp_catalog().lock() {
            Ok(mut m) => m.remove(name).is_some(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: global temp catalog lock poisoned, drop_global_temp_view may have failed"
                );
                false
            }
        }
    }

    /// Register a DataFrame as a saved table (PySpark: saveAsTable). Inserts into the tables catalog only.
    /// If the tables lock is poisoned, the operation is best-effort and a message is emitted to stderr.
    pub fn register_table(&self, name: &str, df: DataFrame) {
        match self.tables.lock() {
            Ok(mut m) => {
                m.insert(name.to_string(), df);
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, register_table may have failed"
                );
            }
        }
    }

    /// Register a database/schema name (from CREATE DATABASE / CREATE SCHEMA). Persisted in session for listDatabases/databaseExists.
    /// If the databases lock is poisoned, the operation is best-effort and a message is emitted to stderr.
    pub fn register_database(&self, name: &str) {
        match self.databases.lock() {
            Ok(mut s) => {
                s.insert(name.to_string());
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: databases lock poisoned, register_database may have failed"
                );
            }
        }
    }

    /// List database names: built-in "default", "global_temp", plus any created via CREATE DATABASE / CREATE SCHEMA.
    /// If the databases lock is poisoned, returns only the built-in names and a message is emitted to stderr.
    pub fn list_database_names(&self) -> Vec<String> {
        let mut names: Vec<String> = vec!["default".to_string(), "global_temp".to_string()];
        match self.databases.lock() {
            Ok(guard) => {
                let mut created: Vec<String> = guard.iter().cloned().collect();
                created.sort();
                names.extend(created);
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: databases lock poisoned, list_database_names may be incomplete"
                );
            }
        }
        names
    }

    /// True if the database name exists (default, global_temp, or created via CREATE DATABASE / CREATE SCHEMA).
    /// If the databases lock is poisoned, returns false for custom names and a message is emitted to stderr.
    pub fn database_exists(&self, name: &str) -> bool {
        if name.eq_ignore_ascii_case("default") || name.eq_ignore_ascii_case("global_temp") {
            return true;
        }
        match self.databases.lock() {
            Ok(s) => s.contains(name),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: databases lock poisoned, database_exists may be wrong"
                );
                false
            }
        }
    }

    /// Get a saved table by name (tables map only). Returns None if not in saved tables (temp views not checked).
    /// If the tables lock is poisoned, returns None and a message is emitted to stderr.
    pub fn get_saved_table(&self, name: &str) -> Option<DataFrame> {
        match self.tables.lock() {
            Ok(m) => m.get(name).cloned(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, get_saved_table may have failed"
                );
                None
            }
        }
    }

    /// Return a DataFrame of table names (and optional database) in this session. PySpark: catalog.listTables().
    /// Columns: "name" (string). Schema-qualified names like "my_schema.my_table" are listed as-is.
    /// If the tables lock is poisoned, returns an empty DataFrame and a message is emitted to stderr.
    pub fn list_tables(&self) -> Result<DataFrame, PolarsError> {
        let names = self.list_table_names();
        let rows: Vec<Vec<JsonValue>> = names
            .into_iter()
            .map(|n| vec![JsonValue::String(n)])
            .collect();
        let schema = vec![("name".to_string(), "string".to_string())];
        self.create_dataframe_from_rows(rows, schema, false, false)
    }

    /// Return a DataFrame of database names. PySpark: catalog.listDatabases().
    /// Column: "name" (string). Includes "default", "global_temp", and any created via CREATE DATABASE / CREATE SCHEMA.
    /// PR-C/#799,#798,#797,#796: Tests that assert 'test_schema' or 'test_db' in list expect this DataFrame.
    pub fn list_databases(&self) -> Result<DataFrame, PolarsError> {
        let names = self.list_database_names();
        let rows: Vec<Vec<JsonValue>> = names
            .into_iter()
            .map(|n| vec![JsonValue::String(n)])
            .collect();
        let schema = vec![("name".to_string(), "string".to_string())];
        self.create_dataframe_from_rows(rows, schema, false, false)
    }

    /// True if the name exists in the saved-tables map (not temp views).
    /// If the tables lock is poisoned, returns false and a message is emitted to stderr.
    pub fn saved_table_exists(&self, name: &str) -> bool {
        match self.tables.lock() {
            Ok(m) => m.contains_key(name),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, saved_table_exists may be wrong"
                );
                false
            }
        }
    }

    /// Check if a table or temp view exists (PySpark: catalog.tableExists). True if name is in temp views, saved tables, global temp, or warehouse.
    /// If a catalog lock is poisoned, returns false for that source and a message is emitted to stderr.
    pub fn table_exists(&self, name: &str) -> bool {
        // global_temp.xyz
        if let Some((_db, tbl)) = Self::parse_global_temp_name(name) {
            return match global_temp_catalog().lock() {
                Ok(m) => m.contains_key(tbl),
                Err(_) => {
                    eprintln!(
                        "robin-sparkless-polars: global temp catalog lock poisoned, table_exists may be wrong"
                    );
                    false
                }
            };
        }
        if match self.catalog.lock() {
            Ok(m) => m.contains_key(name),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: catalog lock poisoned, table_exists may be wrong"
                );
                false
            }
        } {
            return true;
        }
        if match self.tables.lock() {
            Ok(m) => m.contains_key(name),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, table_exists may be wrong"
                );
                false
            }
        } {
            return true;
        }
        // #984: schema-qualified name — try unqualified table name
        if name.contains('.') {
            let unqualified = name.rsplit_once('.').map(|(_, tbl)| tbl).unwrap_or(name);
            if match self.catalog.lock() {
                Ok(m) => m.contains_key(unqualified),
                Err(_) => false,
            } || match self.tables.lock() {
                Ok(m) => m.contains_key(unqualified),
                Err(_) => false,
            } {
                return true;
            }
        }
        // Warehouse fallback
        if let Some(warehouse) = self.warehouse_dir() {
            let path = Path::new(warehouse).join(name);
            if path.is_dir() {
                return true;
            }
            if name.contains('.') {
                let path_qualified =
                    Path::new(warehouse).join(name.replace('.', std::path::MAIN_SEPARATOR_STR));
                if path_qualified.is_dir() {
                    return true;
                }
            }
        }
        false
    }

    /// Return global temp view names (process-scoped). PySpark: catalog.listTables(dbName="global_temp").
    /// If the global catalog lock is poisoned, returns empty and a message is emitted to stderr.
    pub fn list_global_temp_view_names(&self) -> Vec<String> {
        match global_temp_catalog().lock() {
            Ok(m) => m.keys().cloned().collect(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: global temp catalog lock poisoned, list_global_temp_view_names may be incomplete"
                );
                Vec::new()
            }
        }
    }

    /// Return temporary view names in this session.
    /// If the catalog lock is poisoned, returns empty and a message is emitted to stderr.
    pub fn list_temp_view_names(&self) -> Vec<String> {
        match self.catalog.lock() {
            Ok(m) => m.keys().cloned().collect(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: catalog lock poisoned, list_temp_view_names may be incomplete"
                );
                Vec::new()
            }
        }
    }

    /// Return saved table names in this session (saveAsTable / write_delta_table).
    /// If the tables lock is poisoned, returns empty and a message is emitted to stderr.
    pub fn list_table_names(&self) -> Vec<String> {
        match self.tables.lock() {
            Ok(m) => m.keys().cloned().collect(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, list_table_names may be incomplete"
                );
                Vec::new()
            }
        }
    }

    /// Drop a saved table by name (removes from tables catalog only). No-op if not present.
    /// If the tables lock is poisoned, returns false and a message is emitted to stderr.
    pub fn drop_table(&self, name: &str) -> bool {
        match self.tables.lock() {
            Ok(mut m) => m.remove(name).is_some(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, drop_table may have failed"
                );
                false
            }
        }
    }

    /// Drop a database/schema by name (from DROP SCHEMA / DROP DATABASE). Removes from registered databases only.
    /// Does not drop "default" or "global_temp". No-op if not present (or if_exists). Returns true if removed.
    /// If the databases lock is poisoned, returns false and a message is emitted to stderr.
    pub fn drop_database(&self, name: &str) -> bool {
        if name.eq_ignore_ascii_case("default") || name.eq_ignore_ascii_case("global_temp") {
            return false;
        }
        // Best-effort: if dropping current db, reset to default.
        if self.current_database().eq(name) {
            let _ = self.set_current_database("default");
        }
        match self.databases.lock() {
            Ok(mut s) => s.remove(name),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: databases lock poisoned, drop_database may have failed"
                );
                false
            }
        }
    }

    /// Parse "global_temp.xyz" into ("global_temp", "xyz"). Returns None for plain names.
    fn parse_global_temp_name(name: &str) -> Option<(&str, &str)> {
        if let Some(dot) = name.find('.') {
            let (db, tbl) = name.split_at(dot);
            if db.eq_ignore_ascii_case("global_temp") {
                return Some((db, tbl.strip_prefix('.').unwrap_or(tbl)));
            }
        }
        None
    }

    /// Return spark.sql.warehouse.dir from config if set. Enables disk-backed saveAsTable.
    pub fn warehouse_dir(&self) -> Option<&str> {
        self.config
            .get("spark.sql.warehouse.dir")
            .map(|s| s.as_str())
            .filter(|s| !s.is_empty())
    }

    /// Resolve a table name to a Delta table path (warehouse/name if it contains _delta_log).
    /// Returns None if the table is not a warehouse-backed Delta table (DESCRIBE DETAIL support).
    #[cfg(feature = "delta")]
    pub fn resolve_delta_table_path(&self, table_name: &str) -> Option<std::path::PathBuf> {
        let warehouse = self.warehouse_dir()?;
        let path = Path::new(warehouse).join(table_name);
        if path.is_dir() && path.join("_delta_log").is_dir() {
            Some(path)
        } else {
            None
        }
    }

    /// Look up a table or temp view by name (PySpark: table(name)).
    /// Resolution order: (1) global_temp.xyz from global catalog, (2) temp view, (3) saved table, (4) warehouse.
    pub fn table(&self, name: &str) -> Result<DataFrame, PolarsError> {
        // global_temp.xyz -> global catalog only
        if let Some((_db, tbl)) = Self::parse_global_temp_name(name) {
            if let Some(df) = global_temp_catalog()
                .lock()
                .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
                .get(tbl)
                .cloned()
            {
                return Ok(df);
            }
            return Err(PolarsError::InvalidOperation(
                format!(
                    "Global temp view '{tbl}' not found. Register it with createOrReplaceGlobalTempView."
                )
                .into(),
            ));
        }
        // Session: temp view, saved table
        if let Some(df) = self
            .catalog
            .lock()
            .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
            .get(name)
            .cloned()
        {
            return Ok(df);
        }
        if let Some(df) = self
            .tables
            .lock()
            .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
            .get(name)
            .cloned()
        {
            return Ok(df);
        }
        // #984: schema-qualified name (e.g. test_schema.test_table): try unqualified table name so table("test_schema.test_table") finds a table registered as "test_table".
        if name.contains('.') {
            let unqualified = name.rsplit_once('.').map(|(_, tbl)| tbl).unwrap_or(name);
            if let Some(df) = self
                .catalog
                .lock()
                .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
                .get(unqualified)
                .cloned()
            {
                return Ok(df);
            }
            if let Some(df) = self
                .tables
                .lock()
                .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
                .get(unqualified)
                .cloned()
            {
                return Ok(df);
            }
        }
        // Warehouse fallback (disk-backed saveAsTable). #760: schema-qualified name try both
        // "schema.table" as single dir and "schema/table" (schema as subdir).
        if let Some(warehouse) = self.warehouse_dir() {
            let wh = Path::new(warehouse);
            let dir = wh.join(name);
            if dir.is_dir() {
                let data_file = dir.join("data.parquet");
                let read_path = if data_file.is_file() { data_file } else { dir };
                return self.read_parquet(&read_path);
            }
            if name.contains('.') {
                let dir_qualified = wh.join(name.replace('.', std::path::MAIN_SEPARATOR_STR));
                if dir_qualified.is_dir() {
                    let data_file = dir_qualified.join("data.parquet");
                    let read_path = if data_file.is_file() {
                        data_file
                    } else {
                        dir_qualified
                    };
                    return self.read_parquet(&read_path);
                }
            }
        }
        Err(PolarsError::InvalidOperation(
            format!(
                "Table or view '{name}' not found. Register it with create_or_replace_temp_view or saveAsTable. \
                (Schema-qualified names like 'schema.table' are supported; use the same name when saving and when calling table().)"
            )
            .into(),
        ))
    }

    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder::new()
    }

    /// Create a session from a [`SparklessConfig`](SparklessConfig).
    /// Equivalent to `SparkSession::builder().with_config(config).get_or_create()`.
    pub fn from_config(config: &SparklessConfig) -> SparkSession {
        Self::builder().with_config(config).get_or_create()
    }

    /// Return a reference to the session config (for catalog/conf compatibility).
    pub fn get_config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Set a config key at runtime (PySpark: spark.conf.set(key, value)).
    /// When key is spark.sql.session.timeZone, also updates the thread UDF context so hour/minute/second use it (#1154).
    pub fn set_config(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();
        if key == "spark.sql.session.timeZone" {
            crate::update_thread_session_time_zone(Some(value.clone()));
        }
        self.config.insert(key, value);
    }

    /// Whether column names are case-sensitive (PySpark: spark.sql.caseSensitive).
    /// Default is false (case-insensitive matching).
    pub fn is_case_sensitive(&self) -> bool {
        self.config
            .get("spark.sql.caseSensitive")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Register a Rust UDF. Session-scoped. Use with call_udf. PySpark: spark.udf.register (Python) or equivalent.
    pub fn register_udf<F>(&self, name: &str, f: F) -> Result<(), PolarsError>
    where
        F: Fn(&[Series]) -> Result<Series, PolarsError> + Send + Sync + 'static,
    {
        self.udf_registry.register_rust_udf(name, f)
    }

    /// Create a DataFrame from a vector of tuples (i64, i64, String)
    ///
    /// # Example
    /// ```
    /// use robin_sparkless_polars::SparkSession;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df = spark.create_dataframe(
    ///     vec![
    ///         (1, 25, "Alice".to_string()),
    ///         (2, 30, "Bob".to_string()),
    ///     ],
    ///     vec!["id", "age", "name"],
    /// )?;
    /// #     let _ = df;
    /// #     Ok(())
    /// # }
    /// ```
    pub fn create_dataframe(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<DataFrame, PolarsError> {
        if column_names.len() != 3 {
            return Err(PolarsError::ComputeError(
                format!(
                    "create_dataframe: expected 3 column names for (i64, i64, String) tuples, got {}. Hint: provide exactly 3 names, e.g. [\"id\", \"age\", \"name\"].",
                    column_names.len()
                )
                .into(),
            ));
        }

        let mut cols: Vec<Series> = Vec::with_capacity(3);

        // First column: i64
        let col0: Vec<i64> = data.iter().map(|t| t.0).collect();
        cols.push(Series::new(column_names[0].into(), col0));

        // Second column: i64
        let col1: Vec<i64> = data.iter().map(|t| t.1).collect();
        cols.push(Series::new(column_names[1].into(), col1));

        // Third column: String
        let col2: Vec<String> = data.iter().map(|t| t.2.clone()).collect();
        cols.push(Series::new(column_names[2].into(), col2));

        let pl_df = PlDataFrame::new_infer_height(cols.iter().map(|s| s.clone().into()).collect())?;
        Ok(DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Same as [`create_dataframe`](Self::create_dataframe) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn create_dataframe_engine(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<DataFrame, EngineError> {
        self.create_dataframe(data, column_names)
            .map_err(polars_to_core_error)
    }

    /// Create a DataFrame from a Polars DataFrame
    pub fn create_dataframe_from_polars(&self, df: PlDataFrame) -> DataFrame {
        DataFrame::from_polars_with_options(df, self.is_case_sensitive())
    }

    /// Infer dtype string from a single JSON value (for schema inference). Returns None for Null.
    fn infer_dtype_from_json_value(v: &JsonValue) -> Option<String> {
        match v {
            JsonValue::Null => None,
            JsonValue::Bool(_) => Some("boolean".to_string()),
            JsonValue::Number(n) => {
                if n.is_i64() {
                    Some("bigint".to_string())
                } else {
                    Some("double".to_string())
                }
            }
            JsonValue::String(_) => {
                // #1103: Keep string columns as string in createDataFrame so that e.g.
                // filter(date_col > string_col) only casts the string in the predicate and the
                // result row still has the string column as string (PySpark parity). Do not infer
                // date/timestamp from date-like strings; user can pass explicit schema if needed.
                Some("string".to_string())
            }
            JsonValue::Array(_) => Some("array".to_string()),
            JsonValue::Object(_) => None, // struct type is inferred in infer_schema_from_json_rows from object keys
        }
    }

    /// Infer a struct type string from a JSON object (for nested struct inference in createDataFrame).
    fn infer_struct_dtype_from_json_object(obj: &serde_json::Map<String, JsonValue>) -> String {
        let mut keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
        keys.sort();
        keys.iter()
            .map(|k| {
                let field_typ = obj
                    .get(*k)
                    .map(|v| match v {
                        JsonValue::Object(inner_obj) => {
                            format!(
                                "struct<{}>",
                                Self::infer_struct_dtype_from_json_object(inner_obj)
                            )
                        }
                        _ => Self::infer_dtype_from_json_value(v)
                            .unwrap_or_else(|| "string".to_string()),
                    })
                    .unwrap_or_else(|| "string".to_string());
                format!("{}:{}", k, field_typ)
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Infer struct type from a column of objects by using the first non-null value per key across all rows.
    /// Fixes E2 when row0 has E2=null and row1 has E2=4 so E2 is inferred as bigint (#339 null_field).
    fn infer_struct_dtype_from_json_rows(
        rows: &[Vec<JsonValue>],
        col_idx: usize,
    ) -> Option<String> {
        let mut all_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
        for row in rows {
            if let Some(JsonValue::Object(obj)) = row.get(col_idx) {
                for k in obj.keys() {
                    all_keys.insert(k.clone());
                }
            }
        }
        let mut key_to_first_non_null: std::collections::HashMap<String, JsonValue> =
            std::collections::HashMap::new();
        for row in rows {
            let Some(JsonValue::Object(obj)) = row.get(col_idx) else {
                continue; // skip null or non-object rows so struct type is inferred from object rows
            };
            for k in &all_keys {
                if key_to_first_non_null.contains_key(k) {
                    continue;
                }
                if let Some(val) = obj.get(k) {
                    if !matches!(val, JsonValue::Null) {
                        key_to_first_non_null.insert(k.clone(), val.clone());
                    }
                }
            }
        }
        let mut keys: Vec<&String> = all_keys.iter().collect();
        keys.sort();
        fn is_int_like(typ: &str) -> bool {
            let t = typ.trim().to_lowercase();
            matches!(
                t.as_str(),
                "bigint" | "int" | "integer" | "long" | "smallint" | "tinyint"
            )
        }
        // #1150: When struct has any int-like field, only expose int-like and nested-struct so
        // getField("E2") yields null for string fields (PySpark parity, test_issue_330_struct_field_alias).
        fn strict_visible(typ: &str) -> bool {
            let t = typ.trim().to_lowercase();
            is_int_like(&t) || t.starts_with("struct<")
        }
        // #1219: When struct has no int-like fields (e.g. all string), expose string/double/float/bool
        // so col("StructVal")["E1"] works for .cast("int") or upper() on struct field.
        fn extended_visible(typ: &str) -> bool {
            let t = typ.trim().to_lowercase();
            strict_visible(typ)
                || matches!(
                    t.as_str(),
                    "string" | "str" | "varchar" | "double" | "float" | "boolean" | "bool"
                )
        }
        let field_types: Vec<(String, String)> = keys
            .iter()
            .filter_map(|k| {
                let field_typ = key_to_first_non_null.get(*k).map(|val| match val {
                    JsonValue::Object(inner_obj) => {
                        format!(
                            "struct<{}>",
                            Self::infer_struct_dtype_from_json_object(inner_obj)
                        )
                    }
                    _ => Self::infer_dtype_from_json_value(val)
                        .unwrap_or_else(|| "string".to_string()),
                })?;
                Some(((*k).to_string(), field_typ))
            })
            .collect();
        // Use strict visibility when any field is int-like or nested struct (PySpark parity:
        // test_issue_330_struct_field_alias nested case expects getField("E2") None when top-level has Nested struct + E2 string).
        let has_int_like = field_types
            .iter()
            .any(|(_, typ)| is_int_like(typ) || typ.starts_with("struct<"));
        let inner = field_types
            .into_iter()
            .filter_map(|(k, field_typ)| {
                let ok = if has_int_like {
                    strict_visible(&field_typ)
                } else {
                    extended_visible(&field_typ)
                };
                if !ok {
                    return None;
                }
                Some(format!("{}:{}", k.as_str(), field_typ))
            })
            .collect::<Vec<_>>()
            .join(",");
        if inner.is_empty() {
            return None;
        }
        Some(format!("struct<{}>", inner))
    }

    /// Infer schema (name, dtype_str) from JSON rows by scanning values per column.
    /// Used by createDataFrame(data, schema=None) when schema is omitted or only column names given.
    /// When the first non-null value is a JSON object, infers struct (PR13 / struct parity).
    /// #1084: If any value in a column infers as string (e.g. "2024-01-2" with single-digit day),
    /// use string for the column so date-like strings stay string and comparisons remain lexicographic.
    pub fn infer_schema_from_json_rows(
        rows: &[Vec<JsonValue>],
        names: &[String],
    ) -> Vec<(String, String)> {
        if names.is_empty() {
            return Vec::new();
        }
        let mut schema: Vec<(String, String)> = names
            .iter()
            .map(|n| (n.clone(), "string".to_string()))
            .collect();
        for (col_idx, (_, dtype_str)) in schema.iter_mut().enumerate() {
            let mut first_non_string: Option<String> = None;
            let mut has_string = false;
            for row in rows {
                let v = row.get(col_idx).unwrap_or(&JsonValue::Null);
                if let JsonValue::Object(_) = v {
                    if let Some(ty) = Self::infer_struct_dtype_from_json_rows(rows, col_idx) {
                        *dtype_str = ty;
                    }
                    break;
                }
                if let Some(dtype) = Self::infer_dtype_from_json_value(v) {
                    if dtype == "string" {
                        has_string = true;
                        break;
                    }
                    first_non_string.get_or_insert(dtype);
                }
            }
            if has_string {
                *dtype_str = "string".to_string();
            } else if let Some(d) = first_non_string {
                *dtype_str = d;
            }
        }
        schema
    }

    /// Create a DataFrame from rows and a schema (arbitrary column count and types).
    ///
    /// `rows`: each inner vec is one row; length must match schema length. Values are JSON-like (i64, f64, string, bool, null, object, array).
    /// `schema`: list of (column_name, dtype_string), e.g. `[("id", "bigint"), ("name", "string")]`.
    /// `verify_schema`: when true (#420), validate each cell type against schema and return a clear error (e.g. "Row 1: column 'age' expected bigint, got string").
    /// `schema_was_inferred`: when true (Python passed no schema), run Phase 6 validation: all-null column and mixed int/float raise.
    /// Supported dtype strings: bigint, int, long, double, float, string, str, varchar, boolean, bool, date, timestamp, datetime, list, array, array<element_type>, struct<field:type,...>.
    /// When `rows` is empty and `schema` is non-empty, returns an empty DataFrame with that schema (issue #519). Use with `write.format("parquet").saveAsTable(...)` then append; PySpark would fail with "can not infer schema from empty dataset".
    pub fn create_dataframe_from_rows(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
        verify_schema: bool,
        schema_was_inferred: bool,
    ) -> Result<DataFrame, PolarsError> {
        // When schema is explicitly empty and rows are not, fail with LENGTH_SHOULD_BE_THE_SAME (Phase 6 / PySpark parity).
        if schema.is_empty() && !rows.is_empty() {
            let got = rows[0].len();
            return Err(PolarsError::InvalidOperation(
                format!(
                    "create_dataframe_from_rows: LENGTH_SHOULD_BE_THE_SAME. Expected 0 fields, got {} (row index 0).",
                    got
                )
                    .into(),
            ));
        }
        // #624: When schema is only column names (all "string"), infer from data (PySpark parity).
        // #731, #769, #772: createDataFrame(rows, ["name", "age"]) yields int/double/bool columns.
        let schema_inferred_in_rust = !schema.is_empty()
            && !rows.is_empty()
            && schema
                .iter()
                .all(|(_, t)| t.trim().eq_ignore_ascii_case("string"));
        let mut schema = if schema_inferred_in_rust {
            let names: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();
            Self::infer_schema_from_json_rows(&rows, &names)
        } else {
            schema
        };
        // When schema was not all-string (e.g. ID:bigint, StructValue:string), still upgrade any "string"
        // column that has Object in the data to struct (so createDataFrame([{"ID":1,"StructValue":{...}}]) gets struct).
        // #1149: When schema_was_inferred (e.g. names-only from Python), do not upgrade numeric/string
        // so Python's first-col Long and later String stay (PySpark parity). But when Python inferred
        // "string" for a dict column (infer_type_from_py_value has no dict case), upgrade to struct
        // so getField("E1") works (#1216).
        if !rows.is_empty() {
            let names: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();
            let inferred = Self::infer_schema_from_json_rows(&rows, &names);
            for (col_idx, (_, dtype_str)) in schema.iter_mut().enumerate() {
                if dtype_str.trim().eq_ignore_ascii_case("string")
                    && inferred.get(col_idx).map(|(_, t)| t) != Some(&"string".to_string())
                {
                    if let Some((_, inferred_type)) = inferred.get(col_idx) {
                        // Only upgrade when Rust inference says struct/map: avoid overwriting
                        // Python's intentional string for non-dict columns (#1149).
                        let inferred_trimmed = inferred_type.trim();
                        if inferred_trimmed.to_lowercase().starts_with("struct<")
                            || inferred_trimmed.to_lowercase().starts_with("map<")
                        {
                            *dtype_str = inferred_type.clone();
                        }
                    }
                }
            }
        }

        // Phase 6 validation: reject all-null columns and mixed int/float (PySpark parity). Run when schema was inferred (Python or Rust).
        let run_inferred_validation = schema_was_inferred || schema_inferred_in_rust;
        if run_inferred_validation && !rows.is_empty() {
            for (col_idx, (_name, dtype_str)) in schema.iter().enumerate() {
                let mut has_null_only = true;
                let mut has_int = false;
                let mut has_float = false;
                let mut has_non_number = false;
                let mut first_non_number: Option<&JsonValue> = None;
                for row in &rows {
                    let v = row.get(col_idx).unwrap_or(&JsonValue::Null);
                    match v {
                        JsonValue::Null => {}
                        JsonValue::Number(n) => {
                            has_null_only = false;
                            if n.is_i64() {
                                has_int = true;
                            } else {
                                has_float = true;
                            }
                        }
                        JsonValue::String(s) => {
                            has_null_only = false;
                            // Treat textual inf/-inf/nan/NaN/Infinity as numeric (DoubleType)
                            // for validation, so createDataFrame with float("inf") / NaN passes.
                            let lower = s.to_ascii_lowercase();
                            if lower == "inf"
                                || lower == "+inf"
                                || lower == "-inf"
                                || lower == "infinity"
                                || lower == "+infinity"
                                || lower == "-infinity"
                                || lower == "nan"
                            {
                                has_float = true;
                            } else if !has_non_number {
                                has_non_number = true;
                                first_non_number = Some(v);
                            }
                        }
                        _ => {
                            has_null_only = false;
                            if !has_non_number {
                                has_non_number = true;
                                first_non_number = Some(v);
                            }
                        }
                    }
                }
                if has_null_only {
                    return Err(PolarsError::InvalidOperation(
                        "Some of types cannot be determined because the column is all null. Use explicit schema.".into(),
                    ));
                }
                let type_lower = dtype_str.trim().to_lowercase();
                let is_numeric = type_lower.as_str() == "bigint"
                    || type_lower.as_str() == "long"
                    || type_lower.as_str() == "double"
                    || type_lower.as_str() == "float"
                    || type_lower.as_str() == "int"
                    || type_lower.as_str() == "integer";
                if is_numeric && has_int && has_float {
                    return Err(PolarsError::InvalidOperation(
                        "Can not merge type DoubleType and LongType. Use explicit schema or consistent types.".into(),
                    ));
                }
                // PySpark parity: when schema is inferred as numeric but data mixes numeric
                // values with non-numeric (e.g. int and string), raise a TypeError-style
                // message "Can not merge type ..." instead of silently coercing.
                if is_numeric && (has_int || has_float) && has_non_number {
                    let numeric_type =
                        if type_lower.as_str() == "double" || type_lower.as_str() == "float" {
                            "DoubleType"
                        } else {
                            "LongType"
                        };
                    let other_type = first_non_number
                        .map(value_type_name)
                        .unwrap_or("non-numeric");
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "Can not merge type {numeric} and {other}. Use explicit schema or consistent types.",
                            numeric = numeric_type,
                            other = match other_type {
                                "string" => "StringType",
                                "boolean" => "BooleanType",
                                "array" => "ArrayType",
                                "object" => "StructType",
                                "null" => "NullType",
                                other => other,
                            }
                        )
                        .into(),
                    ));
                }
            }
        }

        if schema.is_empty() {
            if rows.is_empty() {
                return Ok(DataFrame::from_eager_with_options(
                    PlDataFrame::new(0, vec![])?,
                    self.is_case_sensitive(),
                ));
            }
            return Err(PolarsError::InvalidOperation(
                "create_dataframe_from_rows: schema must not be empty when rows are not empty"
                    .into(),
            ));
        }
        // #1347: Robin-sparkless rejects duplicate field names in schema; PySpark allows them. See PYSPARK_DIFFERENCES.md.
        let mut seen = std::collections::HashSet::new();
        for (name, _) in &schema {
            if !seen.insert(name.clone()) {
                return Err(PolarsError::InvalidOperation(
                    format!(
                        "create_dataframe_from_rows: duplicate column name '{name}' in schema (robin-sparkless rejects duplicate field names; see PYSPARK_DIFFERENCES.md)"
                    )
                    .into(),
                ));
            }
        }
        // #711: PySpark raises when row length differs from schema (LENGTH_SHOULD_BE_THE_SAME).
        let expected_len = schema.len();
        for (row_idx, row) in rows.iter().enumerate() {
            let got = row.len();
            if got != expected_len {
                return Err(PolarsError::InvalidOperation(
                    format!(
                        "create_dataframe_from_rows: LENGTH_SHOULD_BE_THE_SAME. length should be the same. Expected {} fields, got {} (row index {}).",
                        expected_len, got, row_idx
                    )
                    .into(),
                ));
            }
        }
        // #420: When verify_schema is true, validate each cell type before building.
        // #1149: When schema was inferred (e.g. names-only), skip verification so string-in-numeric
        // columns are coerced to null during build (PySpark parity for createDataFrame(data, [names])).
        if verify_schema && !schema_was_inferred {
            for (row_idx, row) in rows.iter().enumerate() {
                for (col_idx, (name, type_str)) in schema.iter().enumerate() {
                    let v = row.get(col_idx).unwrap_or(&JsonValue::Null);
                    if let Err(msg) = verify_json_value_for_type(v, type_str) {
                        return Err(PolarsError::InvalidOperation(
                            format!(
                                "Row {}: column '{}' expected type {} but {}",
                                row_idx, name, type_str, msg
                            )
                            .into(),
                        ));
                    }
                }
            }
        }
        use chrono::{NaiveDate, NaiveDateTime};

        let mut cols: Vec<Series> = Vec::with_capacity(schema.len());

        for (col_idx, (name, type_str)) in schema.iter().enumerate() {
            let type_lower = type_str.trim().to_lowercase();
            let s = match type_lower.as_str() {
                "int" | "integer" | "bigint" | "long" | "smallint" | "tinyint" => {
                    // #1265: values may exceed i64 (e.g. UDF returns 10^18); use Float64 when any value doesn't fit.
                    let mut needs_f64 = false;
                    let i64_vals: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Number(ref n) => {
                                    if let Some(i) = n.as_i64() {
                                        Some(i)
                                    } else {
                                        needs_f64 = true;
                                        n.as_f64().map(|f| f as i64) // placeholder; we'll use f64 column
                                    }
                                }
                                JsonValue::String(s) => s.parse::<i64>().ok(),
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    if needs_f64 {
                        let f64_vals: Vec<Option<f64>> = rows
                            .iter()
                            .map(|row| {
                                let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                                match v {
                                    JsonValue::Number(n) => {
                                        n.as_i64().map(|i| i as f64).or_else(|| n.as_f64())
                                    }
                                    JsonValue::String(s) => s.parse::<i64>().ok().map(|i| i as f64),
                                    JsonValue::Null => None,
                                    _ => None,
                                }
                            })
                            .collect();
                        Series::new(name.as_str().into(), f64_vals)
                    } else {
                        Series::new(name.as_str().into(), i64_vals)
                    }
                }
                "double" | "float" | "double_precision" => {
                    let vals: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Number(n) => n.as_f64(),
                                JsonValue::String(s) => s.parse::<f64>().ok(),
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                _ if is_decimal_type_str(&type_lower) => {
                    let vals: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Number(n) => n.as_f64(),
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "string" | "str" | "varchar" => {
                    let vals: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::String(s) => Some(s),
                                JsonValue::Null => None,
                                other => Some(other.to_string()),
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "boolean" | "bool" => {
                    let vals: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Bool(b) => Some(b),
                                JsonValue::String(s) => {
                                    if s.eq_ignore_ascii_case("true") {
                                        Some(true)
                                    } else if s.eq_ignore_ascii_case("false") {
                                        Some(false)
                                    } else {
                                        None
                                    }
                                }
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "date" => {
                    let epoch = date_utils::epoch_naive_date();
                    let vals: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::String(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                                    .ok()
                                    .map(|d| (d - epoch).num_days() as i32),
                                JsonValue::Object(obj) => parse_date_from_json_object(&obj),
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    let series = Series::new(name.as_str().into(), vals);
                    series
                        .cast(&DataType::Date)
                        .map_err(|e| PolarsError::ComputeError(format!("date cast: {e}").into()))?
                }
                "timestamp" | "datetime" | "timestamp_ntz" => {
                    let vals: Vec<Option<i64>> =
                        rows.iter()
                            .map(|row| {
                                let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                                match v {
                                    JsonValue::String(s) => {
                                        let parsed = NaiveDateTime::parse_from_str(
                                            &s,
                                            "%Y-%m-%dT%H:%M:%S%.f",
                                        )
                                        .map_err(|e| {
                                            PolarsError::ComputeError(e.to_string().into())
                                        })
                                        .or_else(|_| {
                                            NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S")
                                                .map_err(|e| {
                                                    PolarsError::ComputeError(e.to_string().into())
                                                })
                                        })
                                        .or_else(|_| {
                                            NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                                                .map_err(|e| {
                                                    PolarsError::ComputeError(e.to_string().into())
                                                })
                                        })
                                        .or_else(|_| {
                                            NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                                                .map_err(|e| {
                                                    PolarsError::ComputeError(e.to_string().into())
                                                })
                                                .and_then(|d| {
                                                    d.and_hms_opt(0, 0, 0).ok_or_else(|| {
                                                        PolarsError::ComputeError(
                                                            "date to datetime (0:0:0)".into(),
                                                        )
                                                    })
                                                })
                                        });
                                        parsed.ok().map(|dt| dt.and_utc().timestamp_micros())
                                    }
                                    JsonValue::Object(obj) => parse_datetime_from_json_object(&obj),
                                    JsonValue::Number(n) => n.as_i64(),
                                    JsonValue::Null => None,
                                    _ => None,
                                }
                            })
                            .collect();
                    let series = Series::new(name.as_str().into(), vals);
                    series
                        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                        .map_err(|e| {
                            PolarsError::ComputeError(format!("datetime cast: {e}").into())
                        })?
                }
                "list" | "array" => {
                    // PySpark parity: ("col", "list") or ("col", "array"); infer element type from first non-null array.
                    let (elem_type, inner_dtype) = infer_list_element_type(&rows, col_idx)
                        .unwrap_or(("bigint".to_string(), DataType::Int64));
                    let n = rows.len();
                    let mut builder = get_list_builder(&inner_dtype, 64, n, name.as_str().into());
                    for row in rows.iter() {
                        let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                        if let JsonValue::Null = &v {
                            builder.append_null();
                        } else if let Some(arr) = json_value_to_array(&v) {
                            // #625: Array, Object with "0","1",..., or string that parses as JSON array (PySpark list parity).
                            let elem_series: Vec<Series> = arr
                                .iter()
                                .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                                .collect::<Result<Vec<_>, _>>()?;
                            let vals: Vec<_> =
                                elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                            let s = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &vals,
                                &inner_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("array elem: {e}").into())
                            })?;
                            builder.append_series(&s)?;
                        } else {
                            // #611: PySpark accepts single value as one-element list.
                            let single_arr = [v];
                            let elem_series: Vec<Series> = single_arr
                                .iter()
                                .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                                .collect::<Result<Vec<_>, _>>()?;
                            let vals: Vec<_> =
                                elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                            let s = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &vals,
                                &inner_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("array elem: {e}").into())
                            })?;
                            builder.append_series(&s)?;
                        }
                    }
                    builder.finish().into_series()
                }
                _ if parse_array_element_type(&type_lower).is_some() => {
                    let elem_type = parse_array_element_type(&type_lower).unwrap_or_else(|| {
                        unreachable!("guard above ensures parse_array_element_type returned Some")
                    });
                    // Special case: array<map<k,v>> – inner dtype is List(Struct{key,value})
                    // so each array element is its own map (list-of-{key,value} pairs).
                    let inner_dtype = if let Some((key_type, value_type)) =
                        parse_map_key_value_types(&elem_type)
                    {
                        let key_dtype = json_type_str_to_polars(key_type.trim()).ok_or_else(|| {
                            PolarsError::ComputeError(
                                format!(
                                    "create_dataframe_from_rows: array map key type '{key_type}' not supported"
                                )
                                .into(),
                            )
                        })?;
                        let value_dtype =
                            json_type_str_to_polars(value_type.trim()).ok_or_else(|| {
                                PolarsError::ComputeError(
                                    format!(
                                        "create_dataframe_from_rows: array map value type '{value_type}' not supported"
                                    )
                                    .into(),
                                )
                            })?;
                        DataType::List(Box::new(DataType::Struct(vec![
                            Field::new("key".into(), key_dtype),
                            Field::new("value".into(), value_dtype),
                        ])))
                    } else {
                        json_type_str_to_polars(&elem_type).ok_or_else(|| {
                            PolarsError::ComputeError(
                                format!(
                                    "create_dataframe_from_rows: array element type '{elem_type}' not supported"
                                )
                                .into(),
                            )
                        })?
                    };
                    let n = rows.len();
                    let mut builder = get_list_builder(&inner_dtype, 64, n, name.as_str().into());
                    for row in rows.iter() {
                        let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                        if let JsonValue::Null = &v {
                            builder.append_null();
                        } else if let Some(arr) = json_value_to_array(&v) {
                            // #625: Array, Object with "0","1",..., or string that parses as JSON array (PySpark list parity).
                            // #710/PR-G: Nested array: one list series per element, then vals + from_any_values.
                            let elem_series: Vec<Series> =
                                if parse_array_element_type(&elem_type).is_some() {
                                    arr.iter()
                                        .map(|e| {
                                            json_values_to_series(
                                                &[Some(e.clone())],
                                                &elem_type,
                                                "elem",
                                            )
                                        })
                                        .collect::<Result<Vec<_>, _>>()?
                                } else {
                                    arr.iter()
                                        .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                                        .collect::<Result<Vec<_>, _>>()?
                                };
                            let vals: Vec<_> =
                                elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                            let s = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &vals,
                                &inner_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("array elem: {e}").into())
                            })?;
                            builder.append_series(&s)?;
                        } else {
                            // #611: PySpark accepts single value as one-element list.
                            let single_arr = [v];
                            let elem_series: Vec<Series> =
                                if parse_array_element_type(&elem_type).is_some() {
                                    single_arr
                                        .iter()
                                        .map(|e| {
                                            json_values_to_series(
                                                &[Some(e.clone())],
                                                &elem_type,
                                                "elem",
                                            )
                                        })
                                        .collect::<Result<Vec<_>, _>>()?
                                } else {
                                    single_arr
                                        .iter()
                                        .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                                        .collect::<Result<Vec<_>, _>>()?
                                };
                            let vals: Vec<_> =
                                elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                            let s = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &vals,
                                &inner_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("array elem: {e}").into())
                            })?;
                            builder.append_series(&s)?;
                        }
                    }
                    builder.finish().into_series()
                }
                _ if parse_map_key_value_types(&type_lower).is_some() => {
                    let (key_type, value_type) = parse_map_key_value_types(&type_lower)
                        .unwrap_or_else(|| unreachable!("guard ensures Some"));
                    let key_dtype = json_type_str_to_polars(&key_type).ok_or_else(|| {
                        PolarsError::ComputeError(
                            format!(
                                "create_dataframe_from_rows: map key type '{key_type}' not supported"
                            )
                            .into(),
                        )
                    })?;
                    let value_dtype = json_type_str_to_polars(&value_type).ok_or_else(|| {
                        PolarsError::ComputeError(
                            format!(
                                "create_dataframe_from_rows: map value type '{value_type}' not supported"
                            )
                            .into(),
                        )
                    })?;
                    let struct_dtype = DataType::Struct(vec![
                        Field::new("key".into(), key_dtype.clone()),
                        Field::new("value".into(), value_dtype.clone()),
                    ]);
                    let n = rows.len();
                    let mut builder = get_list_builder(&struct_dtype, 64, n, name.as_str().into());
                    for row in rows.iter() {
                        let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                        if matches!(v, JsonValue::Null) {
                            builder.append_null();
                        } else if let Some(obj) = v.as_object() {
                            let st = json_object_to_map_struct_series(
                                obj,
                                &key_type,
                                &value_type,
                                &key_dtype,
                                &value_dtype,
                                name,
                            )?;
                            builder.append_series(&st)?;
                        } else if let JsonValue::String(s) = &v {
                            // #691/PR-F: Map value as string (e.g. Python dict repr "{'k': 'v'}"). #971: also double-encoded or Python repr.
                            let parsed = string_to_json_object(s);
                            if let Some(parsed) = parsed {
                                let st = json_object_to_map_struct_series(
                                    &parsed,
                                    &key_type,
                                    &value_type,
                                    &key_dtype,
                                    &value_dtype,
                                    name,
                                )?;
                                builder.append_series(&st)?;
                            } else {
                                return Err(PolarsError::ComputeError(
                                    format!(
                                        "create_dataframe_from_rows: map column '{name}' expects JSON object (dict), got {:?}",
                                        v
                                    )
                                    .into(),
                                ));
                            }
                        } else {
                            return Err(PolarsError::ComputeError(
                                format!(
                                    "create_dataframe_from_rows: map column '{name}' expects JSON object (dict), got {:?}",
                                    v
                                )
                                .into(),
                            ));
                        }
                    }
                    builder.finish().into_series()
                }
                _ if parse_struct_fields(&type_lower).is_some() => {
                    let values: Vec<Option<JsonValue>> =
                        rows.iter().map(|row| row.get(col_idx).cloned()).collect();
                    json_values_to_series(&values, type_str, name)?
                }
                _ if type_lower == "binary" => {
                    let values: Vec<Option<JsonValue>> =
                        rows.iter().map(|row| row.get(col_idx).cloned()).collect();
                    json_values_to_series(&values, &type_lower, name)?
                }
                _ => {
                    return Err(PolarsError::ComputeError(
                        format!(
                            "create_dataframe_from_rows: unsupported type '{type_str}' for column '{name}'"
                        )
                        .into(),
                    ));
                }
            };
            cols.push(s);
        }

        let pl_df = PlDataFrame::new_infer_height(cols.iter().map(|s| s.clone().into()).collect())?;
        // Keep eager so schema (including struct types) is available for dotted select (#1076).
        Ok(DataFrame::from_eager_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// #419: Create a DataFrame with a single column named "value" from a list of scalar values (PySpark createDataFrame([1,2,3], "bigint")).
    /// Each element of `values` becomes one row. Use from Python/bindings when schema is a single type string.
    pub fn create_dataframe_from_single_column(
        &self,
        values: Vec<JsonValue>,
        type_str: &str,
    ) -> Result<DataFrame, PolarsError> {
        let schema = vec![("value".to_string(), type_str.to_string())];
        let rows: Vec<Vec<JsonValue>> = values.into_iter().map(|v| vec![v]).collect();
        self.create_dataframe_from_rows(rows, schema, false, false)
    }

    /// Same as [`create_dataframe_from_rows`](Self::create_dataframe_from_rows) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn create_dataframe_from_rows_engine(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
        verify_schema: bool,
        schema_was_inferred: bool,
    ) -> Result<DataFrame, EngineError> {
        self.create_dataframe_from_rows(rows, schema, verify_schema, schema_was_inferred)
            .map_err(polars_to_core_error)
    }

    /// Create a DataFrame with a single column `id` (bigint) containing values from start to end (exclusive) with step.
    /// PySpark: spark.range(end) or spark.range(start, end, step).
    ///
    /// - `range(end)` → 0 to end-1, step 1
    /// - `range(start, end)` → start to end-1, step 1
    /// - `range(start, end, step)` → start, start+step, ... up to but not including end
    pub fn range(&self, start: i64, end: i64, step: i64) -> Result<DataFrame, PolarsError> {
        if step == 0 {
            return Err(PolarsError::InvalidOperation(
                "range: step must not be 0".into(),
            ));
        }
        let mut vals: Vec<i64> = Vec::new();
        let mut v = start;
        if step > 0 {
            while v < end {
                vals.push(v);
                v = v.saturating_add(step);
            }
        } else {
            while v > end {
                vals.push(v);
                v = v.saturating_add(step);
            }
        }
        let col = Series::new("id".into(), vals);
        let pl_df = PlDataFrame::new_infer_height(vec![col.into()])?;
        Ok(DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Read a CSV file.
    ///
    /// Uses Polars' CSV reader with default options:
    /// - Header row is inferred (default: true)
    /// - Schema is inferred from first 100 rows
    ///
    /// # Example
    /// ```
    /// use robin_sparkless_polars::SparkSession;
    ///
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df_result = spark.read_csv("data.csv");
    /// // Handle the Result as appropriate in your application
    /// ```
    pub fn read_csv(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        if !path.exists() {
            return Err(PolarsError::ComputeError(
                format!("read_csv: file not found: {}", path.display()).into(),
            ));
        }
        let path_display = path.display();
        // Use LazyCsvReader - call finish() to get LazyFrame, then collect
        let pl_path = PlRefPath::try_from_path(path).map_err(|e| {
            PolarsError::ComputeError(format!("read_csv({path_display}): path: {e}").into())
        })?;
        let lf = LazyCsvReader::new(pl_path)
            .with_has_header(true)
            .with_infer_schema_length(Some(100))
            .finish()
            .map_err(|e| {
                PolarsError::ComputeError(
                    format!(
                        "read_csv({path_display}): {e} Hint: check that the file exists and is valid CSV."
                    )
                    .into(),
                )
            })?;
        Ok(crate::dataframe::DataFrame::from_lazy_with_options(
            lf,
            self.is_case_sensitive(),
        ))
    }

    /// Same as [`read_csv`](Self::read_csv) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn read_csv_engine(&self, path: impl AsRef<Path>) -> Result<DataFrame, EngineError> {
        self.read_csv(path).map_err(polars_to_core_error)
    }

    /// Read a Parquet file.
    ///
    /// Uses Polars' Parquet reader. Parquet files have embedded schema, so
    /// schema inference is automatic.
    ///
    /// # Example
    /// ```
    /// use robin_sparkless_polars::SparkSession;
    ///
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df_result = spark.read_parquet("data.parquet");
    /// // Handle the Result as appropriate in your application
    /// ```
    pub fn read_parquet(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        if !path.exists() {
            return Err(PolarsError::ComputeError(
                format!("read_parquet: file not found: {}", path.display()).into(),
            ));
        }
        // Use LazyFrame::scan_parquet
        let pl_path = PlRefPath::try_from_path(path)
            .map_err(|e| PolarsError::ComputeError(format!("read_parquet: path: {e}").into()))?;
        let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?;
        Ok(crate::dataframe::DataFrame::from_lazy_with_options(
            lf,
            self.is_case_sensitive(),
        ))
    }

    /// Same as [`read_parquet`](Self::read_parquet) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn read_parquet_engine(&self, path: impl AsRef<Path>) -> Result<DataFrame, EngineError> {
        self.read_parquet(path).map_err(polars_to_core_error)
    }

    /// Read a JSON file (JSONL format - one JSON object per line).
    ///
    /// Uses Polars' JSONL reader with default options:
    /// - Schema is inferred from first 100 rows
    ///
    /// # Example
    /// ```
    /// use robin_sparkless_polars::SparkSession;
    ///
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df_result = spark.read_json("data.json");
    /// // Handle the Result as appropriate in your application
    /// ```
    pub fn read_json(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        use std::num::NonZeroUsize;
        let path = path.as_ref();
        if !path.exists() {
            return Err(PolarsError::ComputeError(
                format!("read_json: file not found: {}", path.display()).into(),
            ));
        }
        // Use LazyJsonLineReader - call finish() to get LazyFrame, then collect
        let pl_path = PlRefPath::try_from_path(path)
            .map_err(|e| PolarsError::ComputeError(format!("read_json: path: {e}").into()))?;
        let lf = LazyJsonLineReader::new(pl_path)
            .with_infer_schema_length(NonZeroUsize::new(100))
            .finish()?;
        Ok(crate::dataframe::DataFrame::from_lazy_with_options(
            lf,
            self.is_case_sensitive(),
        ))
    }

    /// Same as [`read_json`](Self::read_json) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn read_json_engine(&self, path: impl AsRef<Path>) -> Result<DataFrame, EngineError> {
        self.read_json(path).map_err(polars_to_core_error)
    }

    /// Execute a SQL query (SELECT only). Tables must be registered with `create_or_replace_temp_view`.
    /// Requires the `sql` feature. Supports: SELECT (columns or *), FROM (single table or JOIN),
    /// WHERE (basic predicates), GROUP BY + aggregates, ORDER BY, LIMIT.
    #[cfg(feature = "sql")]
    pub fn sql(&self, query: &str) -> Result<DataFrame, PolarsError> {
        crate::sql::execute_sql(self, query)
    }

    /// Execute a SQL query (stub when `sql` feature is disabled).
    #[cfg(not(feature = "sql"))]
    pub fn sql(&self, _query: &str) -> Result<DataFrame, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "SQL queries require the 'sql' feature. Build with --features sql.".into(),
        ))
    }

    /// Same as [`table`](Self::table) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn table_engine(&self, name: &str) -> Result<DataFrame, EngineError> {
        self.table(name).map_err(polars_to_core_error)
    }

    /// Returns true if the string looks like a filesystem path (has separators or path exists).
    fn looks_like_path(s: &str) -> bool {
        s.contains('/') || s.contains('\\') || Path::new(s).exists()
    }

    /// Read a Delta table from path (latest version). Internal; use read_delta(name_or_path: &str) for dispatch.
    #[cfg(feature = "delta")]
    pub fn read_delta_path(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        crate::delta::read_delta(path, self.is_case_sensitive())
    }

    /// Read Delta table at path, optional version. Internal; use read_delta_str for dispatch.
    #[cfg(feature = "delta")]
    pub fn read_delta_path_with_version(
        &self,
        path: impl AsRef<Path>,
        version: Option<i64>,
    ) -> Result<DataFrame, PolarsError> {
        crate::delta::read_delta_with_version(path, version, self.is_case_sensitive())
    }

    /// Read a Delta table or in-memory table by name/path. If name_or_path looks like a path, reads from Delta on disk; else resolves as table name (temp view then saved table).
    #[cfg(feature = "delta")]
    pub fn read_delta(&self, name_or_path: &str) -> Result<DataFrame, PolarsError> {
        if Self::looks_like_path(name_or_path) {
            self.read_delta_path(Path::new(name_or_path))
        } else {
            self.table(name_or_path)
        }
    }

    #[cfg(feature = "delta")]
    pub fn read_delta_with_version(
        &self,
        name_or_path: &str,
        version: Option<i64>,
    ) -> Result<DataFrame, PolarsError> {
        if Self::looks_like_path(name_or_path) {
            self.read_delta_path_with_version(Path::new(name_or_path), version)
        } else {
            // In-memory tables have no version; ignore version and return table
            self.table(name_or_path)
        }
    }

    /// Stub when `delta` feature is disabled. Still supports reading by table name.
    #[cfg(not(feature = "delta"))]
    pub fn read_delta(&self, name_or_path: &str) -> Result<DataFrame, PolarsError> {
        if Self::looks_like_path(name_or_path) {
            Err(PolarsError::InvalidOperation(
                "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
            ))
        } else {
            self.table(name_or_path)
        }
    }

    #[cfg(not(feature = "delta"))]
    pub fn read_delta_with_version(
        &self,
        name_or_path: &str,
        version: Option<i64>,
    ) -> Result<DataFrame, PolarsError> {
        let _ = version;
        self.read_delta(name_or_path)
    }

    /// Path-only read_delta (for DataFrameReader.load/format delta). Requires delta feature.
    #[cfg(feature = "delta")]
    pub fn read_delta_from_path(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.read_delta_path(path)
    }

    #[cfg(not(feature = "delta"))]
    pub fn read_delta_from_path(&self, _path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
        ))
    }

    /// Stop the session (cleanup resources)
    /// Best-effort cleanup. If any catalog lock is poisoned, a message is emitted to stderr and cleanup may be partial.
    pub fn stop(&self) {
        match self.catalog.lock() {
            Ok(mut m) => m.clear(),
            Err(_) => eprintln!(
                "robin-sparkless-polars: catalog lock poisoned, stop() cleanup may be partial"
            ),
        }
        match self.tables.lock() {
            Ok(mut m) => m.clear(),
            Err(_) => eprintln!(
                "robin-sparkless-polars: tables lock poisoned, stop() cleanup may be partial"
            ),
        }
        match self.databases.lock() {
            Ok(mut s) => s.clear(),
            Err(_) => eprintln!(
                "robin-sparkless-polars: databases lock poisoned, stop() cleanup may be partial"
            ),
        }
        let _ = self.udf_registry.clear();
        clear_thread_udf_session();
    }
}

impl SparkSession {
    /// Get a DataFrameReader for reading files
    pub fn read(&self) -> DataFrameReader {
        DataFrameReader::new(SparkSession {
            app_name: self.app_name.clone(),
            master: self.master.clone(),
            config: self.config.clone(),
            catalog: self.catalog.clone(),
            tables: self.tables.clone(),
            databases: self.databases.clone(),
            udf_registry: self.udf_registry.clone(),
            current_database: self.current_database.clone(),
            cached_tables: self.cached_tables.clone(),
        })
    }
}

impl Default for SparkSession {
    fn default() -> Self {
        Self::builder().get_or_create()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_spark_session_builder_basic() {
        let spark = SparkSession::builder().app_name("test_app").get_or_create();

        assert_eq!(spark.app_name, Some("test_app".to_string()));
    }

    #[test]
    fn test_spark_session_builder_with_master() {
        let spark = SparkSession::builder()
            .app_name("test_app")
            .master("local[*]")
            .get_or_create();

        assert_eq!(spark.app_name, Some("test_app".to_string()));
        assert_eq!(spark.master, Some("local[*]".to_string()));
    }

    #[test]
    fn test_spark_session_builder_with_config() {
        let spark = SparkSession::builder()
            .app_name("test_app")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "2g")
            .get_or_create();

        assert_eq!(
            spark.config.get("spark.executor.memory"),
            Some(&"4g".to_string())
        );
        assert_eq!(
            spark.config.get("spark.driver.memory"),
            Some(&"2g".to_string())
        );
    }

    /// Batch 8 / #789: spark.conf().get("spark.app.name") returns the app name set in builder.
    #[test]
    fn test_spark_session_conf_returns_app_name() {
        let spark = SparkSession::builder()
            .app_name("test_fixture")
            .get_or_create();
        let conf = spark.get_config();
        assert_eq!(
            conf.get("spark.app.name"),
            Some(&"test_fixture".to_string()),
            "conf should expose spark.app.name for PySpark parity"
        );
    }

    #[test]
    fn test_spark_session_default() {
        let spark = SparkSession::default();
        assert!(spark.app_name.is_none());
        assert!(spark.master.is_none());
        assert!(spark.config.is_empty());
    }

    #[test]
    fn test_create_dataframe_success() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let data = vec![
            (1i64, 25i64, "Alice".to_string()),
            (2i64, 30i64, "Bob".to_string()),
        ];

        let result = spark.create_dataframe(data, vec!["id", "age", "name"]);

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 2);

        let columns = df.columns().unwrap();
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"age".to_string()));
        assert!(columns.contains(&"name".to_string()));
    }

    #[test]
    fn test_create_dataframe_wrong_column_count() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let data = vec![(1i64, 25i64, "Alice".to_string())];

        // Too few columns
        let result = spark.create_dataframe(data.clone(), vec!["id", "age"]);
        assert!(result.is_err());

        // Too many columns
        let result = spark.create_dataframe(data, vec!["id", "age", "name", "extra"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_dataframe_from_rows_empty_schema_with_rows_returns_error() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![vec![]];
        let schema: Vec<(String, String)> = vec![];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        match &result {
            Err(e) => assert!(
                e.to_string().contains("LENGTH_SHOULD_BE_THE_SAME")
                    || e.to_string().contains("Expected 0 fields")
                    || e.to_string().contains("schema must not be empty"),
                "expected error for empty schema with non-empty rows: {}",
                e
            ),
            Ok(_) => panic!("expected error for empty schema with non-empty rows"),
        }
    }

    #[test]
    fn test_create_dataframe_from_rows_empty_data_with_schema() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![];
        let schema = vec![
            ("a".to_string(), "int".to_string()),
            ("b".to_string(), "string".to_string()),
        ];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
        assert_eq!(df.collect_inner().unwrap().get_column_names(), &["a", "b"]);
    }

    /// #419: Single column "value" from scalar values (createDataFrame([1,2,3], "bigint") parity).
    #[test]
    fn test_create_dataframe_from_single_column() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe_from_single_column(vec![json!(1), json!(2), json!(3)], "bigint")
            .unwrap();
        assert_eq!(df.count().unwrap(), 3);
        let rows = df.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("value").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(rows[1].get("value").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(rows[2].get("value").and_then(|v| v.as_i64()), Some(3));
        let df2 = spark
            .create_dataframe_from_single_column(vec![json!("a"), json!("b")], "string")
            .unwrap();
        assert_eq!(df2.count().unwrap(), 2);
        let rows2 = df2.collect_as_json_rows().unwrap();
        assert_eq!(rows2[0].get("value").and_then(|v| v.as_str()), Some("a"));
    }

    /// #420: When verify_schema is true, type mismatch returns clear error with row index and column.
    #[test]
    fn test_create_dataframe_from_rows_verify_schema_raises_on_type_mismatch() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows = vec![
            vec![json!("Alice"), json!(25)],
            vec![json!("Bob"), json!("thirty")], // age as string, schema says bigint
        ];
        let schema = vec![
            ("name".to_string(), "string".to_string()),
            ("age".to_string(), "bigint".to_string()),
        ];
        let result = spark.create_dataframe_from_rows(rows, schema, true, false);
        let err = match &result {
            Ok(_) => panic!("expected error when verify_schema=true and type mismatch"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("Row 1") || err.to_lowercase().contains("row 1"),
            "error should mention row index: {}",
            err
        );
        assert!(
            err.contains("age") || err.to_lowercase().contains("column"),
            "error should mention column: {}",
            err
        );
        assert!(
            err.contains("bigint") || err.to_lowercase().contains("number"),
            "error should mention expected type: {}",
            err
        );
    }

    #[test]
    fn test_create_dataframe_from_rows_empty_schema_empty_data() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![];
        let schema: Vec<(String, String)> = vec![];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
        assert_eq!(df.collect_inner().unwrap().get_column_names().len(), 0);
    }

    /// create_dataframe_from_rows: struct column as JSON object (by field name). PySpark parity #600.
    #[test]
    fn test_create_dataframe_from_rows_struct_as_object() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!({"a": 1, "b": "y"})],
            vec![json!("z"), json!({"a": 2, "b": "w"})],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "nested"]);
    }

    /// create_dataframe_from_rows: struct column as JSON array (by position). PySpark parity #600.
    #[test]
    fn test_create_dataframe_from_rows_struct_as_array() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!([1, "y"])],
            vec![json!("z"), json!([2, "w"])],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "nested"]);
    }

    /// #634: create_dataframe_from_rows accepts struct as object with "0","1",... keys (Python tuple serialization).
    #[test]
    fn test_create_dataframe_from_rows_struct_as_object_numeric_keys() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!({"0": 1, "1": "y"})],
            vec![json!("z"), json!({"0": 2, "1": "w"})],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "nested"]);
    }

    /// #1015: When row data has E1/E2 keys (Sparkless serialization), struct is built with schema names (a, b) so collect() yields row["nested"]["a"].
    #[test]
    fn test_issue_1015_collect_struct_field_names() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![vec![json!("x"), json!({"E1": 1, "E2": "y"})]];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        let rows_out = df.collect_as_json_rows().unwrap();
        assert_eq!(rows_out.len(), 1);
        let nested = rows_out[0]
            .get("nested")
            .and_then(|v| v.as_object())
            .expect("nested");
        assert!(
            nested.contains_key("a"),
            "#1015: collect should use schema field name 'a', not E1"
        );
        assert!(
            nested.contains_key("b"),
            "#1015: collect should use schema field name 'b', not E2"
        );
        assert_eq!(nested.get("a"), Some(&json!(1)));
        assert_eq!(nested.get("b"), Some(&json!("y")));
    }

    /// #1347: create_dataframe_from_rows raises when schema has duplicate column names (stricter than PySpark).
    #[test]
    fn test_create_dataframe_from_rows_duplicate_column_names_raises() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows = vec![vec![json!(1), json!("a"), json!(true)]];
        let schema = vec![
            ("a".to_string(), "bigint".to_string()),
            ("b".to_string(), "string".to_string()),
            ("a".to_string(), "boolean".to_string()),
        ];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        match &result {
            Err(e) => assert!(
                e.to_string().contains("duplicate column name"),
                "expected duplicate column error, got: {}",
                e
            ),
            Ok(_) => panic!("expected error for duplicate column names in schema"),
        }
    }

    /// #711: create_dataframe_from_rows raises when row length differs from schema (PySpark LENGTH_SHOULD_BE_THE_SAME).
    #[test]
    fn test_create_dataframe_from_rows_mismatched_row_length_raises() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("Name".to_string(), "string".to_string()),
            ("Value".to_string(), "integer".to_string()),
        ];
        // Row 0 has 1 element, schema has 2
        let rows_short = vec![vec![json!("Alice")], vec![json!("Bob"), json!(2)]];
        let result = spark.create_dataframe_from_rows(rows_short, schema.clone(), false, false);
        match &result {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.to_lowercase().contains("length"),
                    "expected length error, got: {}",
                    msg
                );
                assert!(
                    msg.contains("1") && msg.contains("2"),
                    "expected 1 and 2 in message, got: {}",
                    msg
                );
            }
            Ok(_) => panic!("expected error for mismatched row length"),
        }
        // Row 1 has 3 elements, schema has 2
        let rows_long = vec![
            vec![json!("Alice"), json!(1)],
            vec![json!("Bob"), json!(2), json!(100)],
        ];
        let result2 = spark.create_dataframe_from_rows(rows_long, schema, false, false);
        match &result2 {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.to_lowercase().contains("length"),
                    "expected length error, got: {}",
                    msg
                );
                assert!(
                    msg.contains("3") && msg.contains("2"),
                    "expected 3 and 2 in message, got: {}",
                    msg
                );
            }
            Ok(_) => panic!("expected error for mismatched row length (too long)"),
        }
    }

    /// #731, #769, #772: create_dataframe_from_rows with schema that is only column names (all "string")
    /// infers types from data; collect returns int/double/bool not strings.
    #[test]
    fn test_create_dataframe_from_rows_all_string_schema_infers_types() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("Alice"), json!(25), json!(50000.5), json!(true)],
            vec![json!("Bob"), json!(30), json!(60000.0), json!(false)],
        ];
        let schema = vec![
            ("name".to_string(), "string".to_string()),
            ("age".to_string(), "string".to_string()),
            ("salary".to_string(), "string".to_string()),
            ("active".to_string(), "string".to_string()),
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let rows_out = df.collect_as_json_rows().unwrap();
        assert_eq!(rows_out.len(), 2);
        // age/salary/active must be number/bool in JSON, not string
        let r0 = &rows_out[0];
        assert_eq!(r0.get("name").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(r0.get("age").and_then(|v| v.as_i64()), Some(25));
        assert_eq!(r0.get("salary").and_then(|v| v.as_f64()), Some(50000.5));
        assert_eq!(r0.get("active").and_then(|v| v.as_bool()), Some(true));
    }

    /// PR13: create_dataframe_from_rows with empty schema + non-empty rows returns error (LENGTH_SHOULD_BE_THE_SAME).
    #[test]
    fn test_create_dataframe_from_rows_infer_struct_from_objects() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("id1"), json!({"a": 1, "b": "x"})],
            vec![json!("id2"), json!({"a": 2, "b": "y"})],
        ];
        let schema = vec![];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        match &result {
            Err(e) => assert!(
                e.to_string().contains("LENGTH_SHOULD_BE_THE_SAME")
                    || e.to_string().contains("Expected 0 fields"),
                "expected LENGTH_SHOULD_BE_THE_SAME: {}",
                e
            ),
            Ok(_) => panic!("expected error for empty schema with non-empty rows"),
        }
    }

    /// #610: create_dataframe_from_rows accepts struct as string that parses to object or array (Sparkless/Python serialization).
    #[test]
    fn test_issue_610_struct_value_as_string_object_or_array() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        // Struct as string that parses to JSON object (e.g. Python dict serialized as string).
        let rows_object: Vec<Vec<JsonValue>> =
            vec![vec![json!("A"), json!(r#"{"a": 1, "b": "x"}"#)]];
        let df1 = spark
            .create_dataframe_from_rows(rows_object, schema.clone(), false, false)
            .unwrap();
        assert_eq!(df1.count().unwrap(), 1);

        // Struct as string that parses to JSON array (e.g. Python tuple (1, "y") serialized as "[1, \"y\"]").
        let rows_array: Vec<Vec<JsonValue>> = vec![vec![json!("B"), json!(r#"[1, "y"]"#)]];
        let df2 = spark
            .create_dataframe_from_rows(rows_array, schema, false, false)
            .unwrap();
        assert_eq!(df2.count().unwrap(), 1);
    }

    /// #691/PR-F: create_dataframe_from_rows accepts struct column as Python dict repr string (e.g. "{'a': 1, 'b': 'x'}").
    #[test]
    fn test_issue_691_struct_value_as_python_dict_repr_string() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![vec![json!("x"), json!("{'a': 1, 'b': 'x'}")]];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 1);
    }

    /// #710/PR-G: create_dataframe_from_rows with nested array type (array<array<bigint>>).
    #[test]
    fn test_issue_710_nested_array_type() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("arr".to_string(), "array<array<bigint>>".to_string()),
        ];
        // One row: id="x", arr=[[1,2],[3,4]]
        let rows: Vec<Vec<JsonValue>> = vec![vec![json!("x"), json!([[1, 2], [3, 4]])]];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 1);
        let collected = df.collect_inner().unwrap();
        let arr_col = collected.column("arr").unwrap();
        assert!(arr_col.dtype().is_list());
        let list = arr_col.list().unwrap();
        assert_eq!(list.len(), 1);
    }

    /// #611: create_dataframe_from_rows accepts single value as one-element array (PySpark parity).
    #[test]
    fn test_issue_611_array_column_single_value_as_one_element() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("arr".to_string(), "array<bigint>".to_string()),
        ];
        // Single number as one-element list (PySpark accepts this).
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!(42)],
            vec![json!("y"), json!([1, 2, 3])],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        let arr_col = collected.column("arr").unwrap();
        let list = arr_col.list().unwrap();
        let row0 = list.get(0).unwrap();
        assert_eq!(
            row0.len(),
            1,
            "#611: single value should become one-element list"
        );
        let row1 = list.get(1).unwrap();
        assert_eq!(row1.len(), 3);
    }

    /// create_dataframe_from_rows: array column with JSON array and null. PySpark parity #601.
    #[test]
    fn test_create_dataframe_from_rows_array_column() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("arr".to_string(), "array<bigint>".to_string()),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!([1, 2, 3])],
            vec![json!("y"), json!([4, 5])],
            vec![json!("z"), json!(null)],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 3);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "arr"]);

        // Issue #601: verify array data round-trips correctly (not just no error).
        let arr_col = collected.column("arr").unwrap();
        let list = arr_col.list().unwrap();
        // Row 0: [1, 2, 3]
        let row0 = list.get(0).unwrap();
        assert_eq!(row0.len(), 3, "row 0 arr should have 3 elements");
        // Row 1: [4, 5]
        let row1 = list.get(1).unwrap();
        assert_eq!(row1.len(), 2);
        // Row 2: null list (representation may be None or empty)
        let row2 = list.get(2);
        assert!(
            row2.is_none() || row2.as_ref().map(|a| a.is_empty()).unwrap_or(false),
            "row 2 arr should be null or empty"
        );
    }

    /// create_dataframe_from_rows: array<timestamp> and array<date> element types (PySpark parity).
    #[test]
    fn test_create_dataframe_from_rows_array_timestamp() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("ts_arr".to_string(), "array<timestamp>".to_string()),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![
                json!("a"),
                json!(["2025-01-15T10:00:00Z", "2025-01-16T12:00:00"]),
            ],
            vec![json!("b"), json!(null)],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .expect("array<timestamp> should be supported");
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "ts_arr"]);
    }

    /// Issue #601: PySpark createDataFrame([(\"x\", [1,2,3]), (\"y\", [4,5])], schema) with ArrayType.
    /// Must not fail with \"array column value must be null or array\" and must produce correct structure.
    #[test]
    fn test_issue_601_array_column_pyspark_parity() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("arr".to_string(), "array<bigint>".to_string()),
        ];
        // Exact PySpark example: rows with string id and list of ints.
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!([1, 2, 3])],
            vec![json!("y"), json!([4, 5])],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .expect("issue #601: create_dataframe_from_rows must accept array column (JSON array)");
        let n = df.count().unwrap();
        assert_eq!(n, 2, "issue #601: expected 2 rows");
        let collected = df.collect_inner().unwrap();
        let arr_col = collected.column("arr").unwrap();
        let list = arr_col.list().unwrap();
        // Verify list lengths match PySpark [1,2,3] and [4,5]
        let row0 = list.get(0).unwrap();
        assert_eq!(
            row0.len(),
            3,
            "issue #601: first row arr must have 3 elements [1,2,3]"
        );
        let row1 = list.get(1).unwrap();
        assert_eq!(
            row1.len(),
            2,
            "issue #601: second row arr must have 2 elements [4,5]"
        );
    }

    /// #1016: create_dataframe_from_rows accepts array<boolean> as Python repr string "[True, False, True]".
    #[test]
    fn test_issue_1016_array_boolean_python_repr() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("flags".to_string(), "array<boolean>".to_string()),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![
                JsonValue::String("a".into()),
                JsonValue::String("[True, False, True]".into()),
            ],
            vec![
                JsonValue::String("b".into()),
                JsonValue::String("[False, True]".into()),
            ],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .expect("#1016: array<boolean> from Python repr string must parse");
        let collected = df.collect_inner().unwrap();
        let flags_col = collected.column("flags").unwrap();
        let list = flags_col.list().unwrap();
        let row0 = list.get(0).unwrap();
        assert_eq!(row0.len(), 3, "#1016: [True, False, True] -> 3 elements");
        let row1 = list.get(1).unwrap();
        assert_eq!(row1.len(), 2, "#1016: [False, True] -> 2 elements");
    }

    /// #624: When schema is empty but rows are not, backend returns LENGTH_SHOULD_BE_THE_SAME (no inference).
    #[test]
    fn test_issue_624_empty_schema_inferred_from_rows() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema: Vec<(String, String)> = vec![];
        let rows: Vec<Vec<JsonValue>> =
            vec![vec![json!("a"), json!(1)], vec![json!("b"), json!(2)]];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        match &result {
            Err(e) => assert!(
                e.to_string().contains("LENGTH_SHOULD_BE_THE_SAME")
                    || e.to_string().contains("Expected 0 fields"),
                "expected LENGTH_SHOULD_BE_THE_SAME: {}",
                e
            ),
            Ok(_) => panic!("expected error for empty schema with non-empty rows"),
        }
    }

    /// #627: create_dataframe_from_rows accepts map column (dict/object). PySpark MapType parity.
    #[test]
    fn test_create_dataframe_from_rows_map_column() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "integer".to_string()),
            ("m".to_string(), "map<string,string>".to_string()),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!(1), json!({"a": "x", "b": "y"})],
            vec![json!(2), json!({"c": "z"})],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "m"]);
        let m_col = collected.column("m").unwrap();
        let list = m_col.list().unwrap();
        let row0 = list.get(0).unwrap();
        assert_eq!(row0.len(), 2, "row 0 map should have 2 entries");
        let row1 = list.get(1).unwrap();
        assert_eq!(row1.len(), 1, "row 1 map should have 1 entry");
    }

    /// #625: create_dataframe_from_rows accepts array column as JSON array or Object (Python list parity).
    #[test]
    fn test_issue_625_array_column_list_or_object() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("arr".to_string(), "array<bigint>".to_string()),
        ];
        // JSON array (Python list) and Object with "0","1","2" keys (some serializations).
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!([1, 2, 3])],
            vec![json!("y"), json!({"0": 4, "1": 5})],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .expect("#625: array column must accept list/array or object representation");
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        let list = collected.column("arr").unwrap().list().unwrap();
        assert_eq!(list.get(0).unwrap().len(), 3);
        assert_eq!(list.get(1).unwrap().len(), 2);
    }

    #[test]
    fn test_create_dataframe_empty() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let data: Vec<(i64, i64, String)> = vec![];

        let result = spark.create_dataframe(data, vec!["id", "age", "name"]);

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
    }

    #[test]
    fn test_create_dataframe_from_polars() {
        use polars::prelude::df;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let polars_df = df!(
            "x" => &[1, 2, 3],
            "y" => &[4, 5, 6]
        )
        .unwrap();

        let df = spark.create_dataframe_from_polars(polars_df);

        assert_eq!(df.count().unwrap(), 3);
        let columns = df.columns().unwrap();
        assert!(columns.contains(&"x".to_string()));
        assert!(columns.contains(&"y".to_string()));
    }

    #[test]
    fn test_read_csv_file_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.read_csv("nonexistent_file.csv");

        assert!(result.is_err());
    }

    #[test]
    fn test_read_parquet_file_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.read_parquet("nonexistent_file.parquet");

        assert!(result.is_err());
    }

    #[test]
    fn test_read_json_file_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.read_json("nonexistent_file.json");

        assert!(result.is_err());
    }

    #[test]
    fn test_rust_udf_dataframe() {
        use crate::functions::{call_udf, col};
        use polars::prelude::DataType;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        spark
            .register_udf("to_str", |cols| cols[0].cast(&DataType::String))
            .unwrap();
        let df = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string()), (2, 30, "Bob".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let col = call_udf("to_str", &[col("id")]).unwrap();
        let df2 = df.with_column("id_str", &col).unwrap();
        let cols = df2.columns().unwrap();
        assert!(cols.contains(&"id_str".to_string()));
        let rows = df2.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("id_str").and_then(|v| v.as_str()), Some("1"));
        assert_eq!(rows[1].get("id_str").and_then(|v| v.as_str()), Some("2"));
    }

    #[test]
    fn test_case_insensitive_filter_select() {
        use crate::expression::lit_i64;
        use crate::functions::col;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 25, "Alice".to_string()),
                    (2, 30, "Bob".to_string()),
                    (3, 35, "Charlie".to_string()),
                ],
                vec!["Id", "Age", "Name"],
            )
            .unwrap();
        // Filter with lowercase column names (PySpark default: case-insensitive)
        let filtered = df
            .filter(col("age").gt(lit_i64(26)).expr().clone())
            .unwrap()
            .select(vec!["name"])
            .unwrap();
        assert_eq!(filtered.count().unwrap(), 2);
        let rows = filtered.collect_as_json_rows().unwrap();
        let names: Vec<&str> = rows
            .iter()
            .map(|r| r.get("name").and_then(|v| v.as_str()).unwrap())
            .collect();
        assert!(names.contains(&"Bob"));
        assert!(names.contains(&"Charlie"));
    }

    #[test]
    fn test_sql_returns_error_without_feature_or_unknown_table() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.sql("SELECT * FROM table");

        assert!(result.is_err());
        match result {
            Err(PolarsError::InvalidOperation(msg)) => {
                let s = msg.to_string();
                // Without sql feature: "SQL queries require the 'sql' feature"
                // With sql feature but no table: "Table or view 'table' not found" or parse error
                assert!(
                    s.contains("SQL") || s.contains("Table") || s.contains("feature"),
                    "unexpected message: {s}"
                );
            }
            _ => panic!("Expected InvalidOperation error"),
        }
    }

    #[test]
    fn test_spark_session_stop() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        // stop() should complete without error
        spark.stop();
    }

    #[test]
    fn test_dataframe_reader_api() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let reader = spark.read();

        // All readers should return errors for non-existent files
        assert!(reader.csv("nonexistent.csv").is_err());
        assert!(reader.parquet("nonexistent.parquet").is_err());
        assert!(reader.json("nonexistent.json").is_err());
    }

    #[test]
    fn test_read_csv_with_valid_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let spark = SparkSession::builder().app_name("test").get_or_create();

        // Create a temporary CSV file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name,age").unwrap();
        writeln!(temp_file, "1,Alice,25").unwrap();
        writeln!(temp_file, "2,Bob,30").unwrap();
        temp_file.flush().unwrap();

        let result = spark.read_csv(temp_file.path());

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 2);

        let columns = df.columns().unwrap();
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"name".to_string()));
        assert!(columns.contains(&"age".to_string()));
    }

    #[test]
    fn test_read_json_with_valid_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let spark = SparkSession::builder().app_name("test").get_or_create();

        // Create a temporary JSONL file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#"{{"id":1,"name":"Alice"}}"#).unwrap();
        writeln!(temp_file, r#"{{"id":2,"name":"Bob"}}"#).unwrap();
        temp_file.flush().unwrap();

        let result = spark.read_json(temp_file.path());

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 2);
    }

    #[test]
    fn test_read_csv_empty_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let spark = SparkSession::builder().app_name("test").get_or_create();

        // Create an empty CSV file (just header)
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name").unwrap();
        temp_file.flush().unwrap();

        let result = spark.read_csv(temp_file.path());

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
    }

    #[test]
    fn test_write_partitioned_parquet() {
        use crate::dataframe::{WriteFormat, WriteMode};
        use std::fs;
        use tempfile::TempDir;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 25, "Alice".to_string()),
                    (2, 30, "Bob".to_string()),
                    (3, 25, "Carol".to_string()),
                ],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("out");
        df.write()
            .mode(WriteMode::Overwrite)
            .format(WriteFormat::Parquet)
            .partition_by(["age"])
            .save(&path)
            .unwrap();
        assert!(path.is_dir());
        let entries: Vec<_> = fs::read_dir(&path).unwrap().collect();
        assert_eq!(
            entries.len(),
            2,
            "expected two partition dirs (age=25, age=30)"
        );
        let names: Vec<String> = entries
            .iter()
            .filter_map(|e| e.as_ref().ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().any(|n| n.starts_with("age=")));
        let df_read = spark.read_parquet(&path).unwrap();
        assert_eq!(df_read.count().unwrap(), 3);
    }

    #[test]
    fn test_save_as_table_error_if_exists() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        // First call succeeds
        df.write()
            .save_as_table(&spark, "t1", SaveMode::ErrorIfExists)
            .unwrap();
        assert!(spark.table("t1").is_ok());
        assert_eq!(spark.table("t1").unwrap().count().unwrap(), 1);
        // Second call with ErrorIfExists fails
        let err = df
            .write()
            .save_as_table(&spark, "t1", SaveMode::ErrorIfExists)
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn test_save_as_table_overwrite() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df1 = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let df2 = spark
            .create_dataframe(
                vec![(2, 30, "Bob".to_string()), (3, 35, "Carol".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        df1.write()
            .save_as_table(&spark, "t_over", SaveMode::ErrorIfExists)
            .unwrap();
        assert_eq!(spark.table("t_over").unwrap().count().unwrap(), 1);
        df2.write()
            .save_as_table(&spark, "t_over", SaveMode::Overwrite)
            .unwrap();
        assert_eq!(spark.table("t_over").unwrap().count().unwrap(), 2);
    }

    #[test]
    fn test_save_as_table_append() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df1 = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let df2 = spark
            .create_dataframe(vec![(2, 30, "Bob".to_string())], vec!["id", "age", "name"])
            .unwrap();
        df1.write()
            .save_as_table(&spark, "t_append", SaveMode::ErrorIfExists)
            .unwrap();
        df2.write()
            .save_as_table(&spark, "t_append", SaveMode::Append)
            .unwrap();
        assert_eq!(spark.table("t_append").unwrap().count().unwrap(), 2);
    }

    /// Empty DataFrame with explicit schema: saveAsTable(Overwrite) then append one row (issue #495).
    #[test]
    fn test_save_as_table_empty_df_then_append() {
        use crate::dataframe::SaveMode;
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let empty_df = spark
            .create_dataframe_from_rows(vec![], schema.clone(), false, false)
            .unwrap();
        assert_eq!(empty_df.count().unwrap(), 0);

        empty_df
            .write()
            .save_as_table(&spark, "t_empty_append", SaveMode::Overwrite)
            .unwrap();
        let r1 = spark.table("t_empty_append").unwrap();
        assert_eq!(r1.count().unwrap(), 0);
        let cols = r1.columns().unwrap();
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"name".to_string()));

        let one_row = spark
            .create_dataframe_from_rows(vec![vec![json!(1), json!("a")]], schema, false, false)
            .unwrap();
        one_row
            .write()
            .save_as_table(&spark, "t_empty_append", SaveMode::Append)
            .unwrap();
        let r2 = spark.table("t_empty_append").unwrap();
        assert_eq!(r2.count().unwrap(), 1);
    }

    /// Empty DataFrame with schema: write.format("parquet").save(path) must not fail (issue #519).
    /// PySpark fails with "can not infer schema from empty dataset"; robin-sparkless uses explicit schema.
    #[test]
    fn test_write_parquet_empty_df_with_schema() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let empty_df = spark
            .create_dataframe_from_rows(vec![], schema, false, false)
            .unwrap();
        assert_eq!(empty_df.count().unwrap(), 0);

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("empty.parquet");
        empty_df
            .write()
            .format(crate::dataframe::WriteFormat::Parquet)
            .mode(crate::dataframe::WriteMode::Overwrite)
            .save(&path)
            .unwrap();
        assert!(path.is_file());

        // Read back and verify schema preserved
        let read_df = spark.read().parquet(path.to_str().unwrap()).unwrap();
        assert_eq!(read_df.count().unwrap(), 0);
        let cols = read_df.columns().unwrap();
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"name".to_string()));
    }

    /// Empty DataFrame with schema + warehouse: saveAsTable(Overwrite) then append (issue #495 disk path).
    #[test]
    fn test_save_as_table_empty_df_warehouse_then_append() {
        use crate::dataframe::SaveMode;
        use serde_json::json;
        use std::sync::atomic::{AtomicU64, Ordering};
        use tempfile::TempDir;

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = TempDir::new().unwrap();
        let warehouse = dir.path().join(format!("wh_{n}"));
        std::fs::create_dir_all(&warehouse).unwrap();
        let spark = SparkSession::builder()
            .app_name("test")
            .config(
                "spark.sql.warehouse.dir",
                warehouse.as_os_str().to_str().unwrap(),
            )
            .get_or_create();

        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let empty_df = spark
            .create_dataframe_from_rows(vec![], schema.clone(), false, false)
            .unwrap();
        empty_df
            .write()
            .save_as_table(&spark, "t_empty_wh", SaveMode::Overwrite)
            .unwrap();
        let r1 = spark.table("t_empty_wh").unwrap();
        assert_eq!(r1.count().unwrap(), 0);

        let one_row = spark
            .create_dataframe_from_rows(vec![vec![json!(1), json!("a")]], schema, false, false)
            .unwrap();
        one_row
            .write()
            .save_as_table(&spark, "t_empty_wh", SaveMode::Append)
            .unwrap();
        let r2 = spark.table("t_empty_wh").unwrap();
        assert_eq!(r2.count().unwrap(), 1);
    }

    #[test]
    fn test_save_as_table_ignore() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df1 = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let df2 = spark
            .create_dataframe(vec![(2, 30, "Bob".to_string())], vec!["id", "age", "name"])
            .unwrap();
        df1.write()
            .save_as_table(&spark, "t_ignore", SaveMode::ErrorIfExists)
            .unwrap();
        df2.write()
            .save_as_table(&spark, "t_ignore", SaveMode::Ignore)
            .unwrap();
        // Still 1 row (ignore did not replace)
        assert_eq!(spark.table("t_ignore").unwrap().count().unwrap(), 1);
    }

    #[test]
    fn test_table_resolution_temp_view_first() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df_saved = spark
            .create_dataframe(
                vec![(1, 25, "Saved".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let df_temp = spark
            .create_dataframe(vec![(2, 30, "Temp".to_string())], vec!["id", "age", "name"])
            .unwrap();
        df_saved
            .write()
            .save_as_table(&spark, "x", SaveMode::ErrorIfExists)
            .unwrap();
        spark.create_or_replace_temp_view("x", df_temp);
        // table("x") must return temp view (PySpark order)
        let t = spark.table("x").unwrap();
        let rows = t.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Temp"));
    }

    /// #629: Exact reproduction – createDataFrame, createOrReplaceTempView, then table() must resolve.
    #[test]
    fn test_issue_629_temp_view_visible_after_create() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("repro").get_or_create();
        let schema = vec![
            ("id".to_string(), "long".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let rows: Vec<Vec<JsonValue>> =
            vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        spark.create_or_replace_temp_view("my_view", df);
        let result = spark
            .table("my_view")
            .unwrap()
            .collect_as_json_rows()
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get("id").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(result[0].get("name").and_then(|v| v.as_str()), Some("a"));
        assert_eq!(result[1].get("id").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(result[1].get("name").and_then(|v| v.as_str()), Some("b"));
    }

    #[test]
    fn test_drop_table() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        df.write()
            .save_as_table(&spark, "t_drop", SaveMode::ErrorIfExists)
            .unwrap();
        assert!(spark.table("t_drop").is_ok());
        assert!(spark.drop_table("t_drop"));
        assert!(spark.table("t_drop").is_err());
        // drop again is no-op, returns false
        assert!(!spark.drop_table("t_drop"));
    }

    /// PR15: list_tables returns DataFrame with "name" column; schema-qualified table names work.
    #[test]
    fn test_list_tables_and_schema_qualified_name() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(vec![(1, 10, "a".to_string())], vec!["id", "x", "name"])
            .unwrap();
        df.write()
            .save_as_table(&spark, "t_plain", SaveMode::ErrorIfExists)
            .unwrap();
        df.write()
            .save_as_table(&spark, "my_schema.my_table", SaveMode::ErrorIfExists)
            .unwrap();

        let list_df = spark.list_tables().unwrap();
        assert_eq!(list_df.count().unwrap(), 2);
        let names: Vec<String> = list_df
            .collect_as_json_rows()
            .unwrap()
            .into_iter()
            .map(|r| {
                r.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        assert!(names.contains(&"t_plain".to_string()));
        assert!(names.contains(&"my_schema.my_table".to_string()));

        assert_eq!(spark.table("t_plain").unwrap().count().unwrap(), 1);
        assert_eq!(
            spark.table("my_schema.my_table").unwrap().count().unwrap(),
            1,
            "schema-qualified table name must resolve"
        );
    }

    /// PR-C: list_databases() returns DataFrame with "name" column; CREATE SCHEMA adds to list.
    /// Only runs when the `sql` feature is enabled (spark.sql() is required).
    #[test]
    #[cfg(feature = "sql")]
    fn test_list_databases_returns_dataframe() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let db_df = spark.list_databases().unwrap();
        let names: Vec<String> = db_df
            .collect_as_json_rows()
            .unwrap()
            .into_iter()
            .map(|r| {
                r.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        assert!(names.contains(&"default".to_string()));
        assert!(names.contains(&"global_temp".to_string()));
        // CREATE SCHEMA adds to list (no IF NOT EXISTS: sqlparser may not support it for CREATE SCHEMA)
        spark
            .sql("CREATE SCHEMA test_schema_for_list_db")
            .expect("CREATE SCHEMA must succeed");
        let db_df2 = spark.list_databases().unwrap();
        let names2: Vec<String> = db_df2
            .collect_as_json_rows()
            .unwrap()
            .into_iter()
            .map(|r| {
                r.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        assert!(
            names2.contains(&"test_schema_for_list_db".to_string()),
            "list_databases() should include schema created by CREATE SCHEMA; got: {:?}",
            names2
        );
    }

    #[test]
    fn test_global_temp_view_persists_across_sessions() {
        // Session 1: create global temp view
        let spark1 = SparkSession::builder().app_name("s1").get_or_create();
        let df1 = spark1
            .create_dataframe(
                vec![(1, 25, "Alice".to_string()), (2, 30, "Bob".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark1.create_or_replace_global_temp_view("people", df1);
        assert_eq!(
            spark1.table("global_temp.people").unwrap().count().unwrap(),
            2
        );

        // Session 2: different session can see global temp view
        let spark2 = SparkSession::builder().app_name("s2").get_or_create();
        let df2 = spark2.table("global_temp.people").unwrap();
        assert_eq!(df2.count().unwrap(), 2);
        let rows = df2.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Alice"));

        // Local temp view in spark2 does not shadow global_temp
        let df_local = spark2
            .create_dataframe(
                vec![(3, 35, "Carol".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark2.create_or_replace_temp_view("people", df_local);
        // table("people") = local temp view (session resolution)
        assert_eq!(spark2.table("people").unwrap().count().unwrap(), 1);
        // table("global_temp.people") = global temp view (unchanged)
        assert_eq!(
            spark2.table("global_temp.people").unwrap().count().unwrap(),
            2
        );

        // Drop global temp view
        assert!(spark2.drop_global_temp_view("people"));
        assert!(spark2.table("global_temp.people").is_err());
    }

    #[test]
    fn test_warehouse_persistence_between_sessions() {
        use crate::dataframe::SaveMode;
        use std::fs;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let warehouse = dir.path().to_str().unwrap();

        // Session 1: save to warehouse
        let spark1 = SparkSession::builder()
            .app_name("w1")
            .config("spark.sql.warehouse.dir", warehouse)
            .get_or_create();
        let df1 = spark1
            .create_dataframe(
                vec![(1, 25, "Alice".to_string()), (2, 30, "Bob".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        df1.write()
            .save_as_table(&spark1, "users", SaveMode::ErrorIfExists)
            .unwrap();
        assert_eq!(spark1.table("users").unwrap().count().unwrap(), 2);

        // Session 2: new session reads from warehouse
        let spark2 = SparkSession::builder()
            .app_name("w2")
            .config("spark.sql.warehouse.dir", warehouse)
            .get_or_create();
        let df2 = spark2.table("users").unwrap();
        assert_eq!(df2.count().unwrap(), 2);
        let rows = df2.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Alice"));

        // Verify parquet was written
        let table_path = dir.path().join("users");
        assert!(table_path.is_dir());
        let entries: Vec<_> = fs::read_dir(&table_path).unwrap().collect();
        assert!(!entries.is_empty());
    }
}
