use crate::dataframe::DataFrame;
use crate::error::EngineError;
use polars::chunked_array::StructChunked;
use polars::chunked_array::builder::get_list_builder;
use polars::prelude::{
    DataFrame as PlDataFrame, DataType, Field, IntoSeries, NamedFrom, PlSmallStr, PolarsError,
    Series, TimeUnit,
};
use robin_sparkless_expr::UdfRegistry;
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::sync::Arc;

/// Parse "array<element_type>" to get inner type string. Returns None if not array<>.
fn parse_array_element_type(type_str: &str) -> Option<String> {
    let s = type_str.trim();
    if !s.to_lowercase().starts_with("array<") || !s.ends_with('>') {
        return None;
    }
    Some(s[6..s.len() - 1].trim().to_string())
}

/// Parse "struct<field:type,...>" to get field (name, type) pairs. Simple parsing, no nested structs.
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
    for part in inner.split(',') {
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

/// Map schema type string to Polars DataType (primitives only for nested use).
/// Decimal(p,s) is mapped to Float64 (Polars dtype-decimal feature not enabled).
fn json_type_str_to_polars(type_str: &str) -> Option<DataType> {
    let s = type_str.trim().to_lowercase();
    if is_decimal_type_str(&s) {
        return Some(DataType::Float64);
    }
    match s.as_str() {
        "int" | "integer" | "bigint" | "long" => Some(DataType::Int64),
        "double" | "float" | "double_precision" => Some(DataType::Float64),
        "string" | "str" | "varchar" => Some(DataType::String),
        "boolean" | "bool" => Some(DataType::Boolean),
        _ => None,
    }
}

/// Normalize a JSON value to an array for array columns (PySpark parity #625).
/// Accepts: Array, Object with "0","1",... keys (Python list serialization), String that parses as JSON array.
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
                None
            }
        }
        _ => None,
    }
}

/// Infer list element type from first non-null array in the column (for schema "list" / "array").
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
    let epoch = crate::date_utils::epoch_naive_date();
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
                let elem_series: Vec<Series> = arr
                    .iter()
                    .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                    .collect::<Result<Vec<_>, _>>()?;
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
                let elem_series: Vec<Series> = single_arr
                    .iter()
                    .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                    .collect::<Result<Vec<_>, _>>()?;
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

    if let Some(fields) = parse_struct_fields(&type_lower) {
        let mut field_series_vec: Vec<Vec<Option<JsonValue>>> = (0..fields.len())
            .map(|_| Vec::with_capacity(values.len()))
            .collect();
        for v in values.iter() {
            // #610: Accept string that parses as JSON object or array (e.g. Python tuple serialized as "[1, \"y\"]").
            let effective: Option<JsonValue> = match v.as_ref() {
                Some(JsonValue::String(s)) => {
                    if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                        if parsed.is_object() || parsed.is_array() {
                            Some(parsed)
                        } else {
                            v.clone()
                        }
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
                .and_then(|x| x.as_array().map(|a| a.clone()))
                .or_else(|| effective.as_ref().and_then(json_value_to_array))
            {
                // #634: Array or object with "0","1",... keys (Python tuple serialization) — positional.
                for (fi, _) in fields.iter().enumerate() {
                    field_series_vec[fi].push(arr.get(fi).cloned());
                }
            } else if let Some(obj) = effective.as_ref().and_then(|x| x.as_object()) {
                for (fi, (fname, _)) in fields.iter().enumerate() {
                    field_series_vec[fi].push(obj.get(fname).cloned());
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

    match type_lower.as_str() {
        "int" | "bigint" | "long" => {
            let vals: Vec<Option<i64>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::Number(n) => n.as_i64(),
                        JsonValue::Null => None,
                        _ => None,
                    })
                })
                .collect();
            Ok(Series::new(name.into(), vals))
        }
        "double" | "float" => {
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
            let vals: Vec<Option<bool>> = values
                .iter()
                .map(|ov| {
                    ov.as_ref().and_then(|v| match v {
                        JsonValue::Bool(b) => Some(*b),
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
    let epoch = crate::date_utils::epoch_naive_date();
    match (value, type_str.trim().to_lowercase().as_str()) {
        (JsonValue::Null, _) => Ok(Series::new_null(name.into(), 1)),
        (JsonValue::Number(n), "int" | "bigint" | "long") => {
            Ok(Series::new(name.into(), vec![n.as_i64()]))
        }
        (JsonValue::Number(n), "double" | "float") => {
            Ok(Series::new(name.into(), vec![n.as_f64()]))
        }
        (JsonValue::Number(n), t) if is_decimal_type_str(t) => {
            Ok(Series::new(name.into(), vec![n.as_f64()]))
        }
        (JsonValue::String(s), "string" | "str" | "varchar") => {
            Ok(Series::new(name.into(), vec![s.as_str()]))
        }
        (JsonValue::Bool(b), "boolean" | "bool") => Ok(Series::new(name.into(), vec![*b])),
        (JsonValue::String(s), "date") => {
            let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| PolarsError::ComputeError(format!("date parse: {e}").into()))?;
            let days = (d - epoch).num_days() as i32;
            let s = Series::new(name.into(), vec![days]).cast(&DataType::Date)?;
            Ok(s)
        }
        _ => Err(PolarsError::ComputeError(
            format!("json_value_to_series: unsupported {type_str} for {value:?}").into(),
        )),
    }
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
    // #610: Accept string that parses as JSON object or array.
    let effective = match value {
        JsonValue::String(s) => {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                if parsed.is_object() || parsed.is_array() {
                    parsed
                } else {
                    value.clone()
                }
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
            obj.get(fname).unwrap_or(&JsonValue::Null)
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
    robin_sparkless_expr::set_thread_udf_context(
        Arc::new(session.udf_registry.clone()),
        session.is_case_sensitive(),
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

/// Builder for creating a SparkSession with configuration options
#[derive(Clone)]
pub struct SparkSessionBuilder {
    app_name: Option<String>,
    master: Option<String>,
    config: HashMap<String, String>,
}

impl Default for SparkSessionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSessionBuilder {
    pub fn new() -> Self {
        SparkSessionBuilder {
            app_name: None,
            master: None,
            config: HashMap::new(),
        }
    }

    pub fn app_name(mut self, name: impl Into<String>) -> Self {
        self.app_name = Some(name.into());
        self
    }

    pub fn master(mut self, master: impl Into<String>) -> Self {
        self.master = Some(master.into());
        self
    }

    pub fn config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }

    pub fn get_or_create(self) -> SparkSession {
        let session = SparkSession::new(self.app_name, self.master, self.config);
        set_thread_udf_session(session.clone());
        session
    }

    /// Apply configuration from a [`SparklessConfig`](crate::config::SparklessConfig).
    /// Merges warehouse dir, case sensitivity, and extra keys into the builder config.
    pub fn with_config(mut self, config: &crate::config::SparklessConfig) -> Self {
        for (k, v) in config.to_session_config() {
            self.config.insert(k, v);
        }
        self
    }
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
        }
    }

    /// Register a DataFrame as a temporary view (PySpark: createOrReplaceTempView).
    /// The view is session-scoped and is dropped when the session is dropped.
    pub fn create_or_replace_temp_view(&self, name: &str, df: DataFrame) {
        let _ = self
            .catalog
            .lock()
            .map(|mut m| m.insert(name.to_string(), df));
    }

    /// Global temp view (PySpark: createGlobalTempView). Persists across sessions within the same process.
    pub fn create_global_temp_view(&self, name: &str, df: DataFrame) {
        let _ = global_temp_catalog()
            .lock()
            .map(|mut m| m.insert(name.to_string(), df));
    }

    /// Global temp view (PySpark: createOrReplaceGlobalTempView). Persists across sessions within the same process.
    pub fn create_or_replace_global_temp_view(&self, name: &str, df: DataFrame) {
        let _ = global_temp_catalog()
            .lock()
            .map(|mut m| m.insert(name.to_string(), df));
    }

    /// Drop a temporary view by name (PySpark: catalog.dropTempView).
    /// No error if the view does not exist.
    pub fn drop_temp_view(&self, name: &str) {
        let _ = self.catalog.lock().map(|mut m| m.remove(name));
    }

    /// Drop a global temporary view (PySpark: catalog.dropGlobalTempView). Removes from process-wide catalog.
    pub fn drop_global_temp_view(&self, name: &str) -> bool {
        global_temp_catalog()
            .lock()
            .map(|mut m| m.remove(name).is_some())
            .unwrap_or(false)
    }

    /// Register a DataFrame as a saved table (PySpark: saveAsTable). Inserts into the tables catalog only.
    pub fn register_table(&self, name: &str, df: DataFrame) {
        let _ = self
            .tables
            .lock()
            .map(|mut m| m.insert(name.to_string(), df));
    }

    /// Register a database/schema name (from CREATE DATABASE / CREATE SCHEMA). Persisted in session for listDatabases/databaseExists.
    pub fn register_database(&self, name: &str) {
        let _ = self.databases.lock().map(|mut s| {
            s.insert(name.to_string());
        });
    }

    /// List database names: built-in "default", "global_temp", plus any created via CREATE DATABASE / CREATE SCHEMA.
    pub fn list_database_names(&self) -> Vec<String> {
        let mut names: Vec<String> = vec!["default".to_string(), "global_temp".to_string()];
        if let Ok(guard) = self.databases.lock() {
            let mut created: Vec<String> = guard.iter().cloned().collect();
            created.sort();
            names.extend(created);
        }
        names
    }

    /// True if the database name exists (default, global_temp, or created via CREATE DATABASE / CREATE SCHEMA).
    pub fn database_exists(&self, name: &str) -> bool {
        if name.eq_ignore_ascii_case("default") || name.eq_ignore_ascii_case("global_temp") {
            return true;
        }
        self.databases
            .lock()
            .map(|s| s.iter().any(|n| n.eq_ignore_ascii_case(name)))
            .unwrap_or(false)
    }

    /// Get a saved table by name (tables map only). Returns None if not in saved tables (temp views not checked).
    pub fn get_saved_table(&self, name: &str) -> Option<DataFrame> {
        self.tables.lock().ok().and_then(|m| m.get(name).cloned())
    }

    /// True if the name exists in the saved-tables map (not temp views).
    pub fn saved_table_exists(&self, name: &str) -> bool {
        self.tables
            .lock()
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
    }

    /// Check if a table or temp view exists (PySpark: catalog.tableExists). True if name is in temp views, saved tables, global temp, or warehouse.
    pub fn table_exists(&self, name: &str) -> bool {
        // global_temp.xyz
        if let Some((_db, tbl)) = Self::parse_global_temp_name(name) {
            return global_temp_catalog()
                .lock()
                .map(|m| m.contains_key(tbl))
                .unwrap_or(false);
        }
        if self
            .catalog
            .lock()
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
        {
            return true;
        }
        if self
            .tables
            .lock()
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
        {
            return true;
        }
        // Warehouse fallback
        if let Some(warehouse) = self.warehouse_dir() {
            let path = Path::new(warehouse).join(name);
            if path.is_dir() {
                return true;
            }
        }
        false
    }

    /// Return global temp view names (process-scoped). PySpark: catalog.listTables(dbName="global_temp").
    pub fn list_global_temp_view_names(&self) -> Vec<String> {
        global_temp_catalog()
            .lock()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Return temporary view names in this session.
    pub fn list_temp_view_names(&self) -> Vec<String> {
        self.catalog
            .lock()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Return saved table names in this session (saveAsTable / write_delta_table).
    pub fn list_table_names(&self) -> Vec<String> {
        self.tables
            .lock()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Drop a saved table by name (removes from tables catalog only). No-op if not present.
    pub fn drop_table(&self, name: &str) -> bool {
        self.tables
            .lock()
            .map(|mut m| m.remove(name).is_some())
            .unwrap_or(false)
    }

    /// Drop a database/schema by name (from DROP SCHEMA / DROP DATABASE). Removes from registered databases only.
    /// Does not drop "default" or "global_temp". No-op if not present (or if_exists). Returns true if removed.
    pub fn drop_database(&self, name: &str) -> bool {
        if name.eq_ignore_ascii_case("default") || name.eq_ignore_ascii_case("global_temp") {
            return false;
        }
        self.databases
            .lock()
            .map(|mut s| s.remove(name))
            .unwrap_or(false)
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
        // Warehouse fallback (disk-backed saveAsTable)
        if let Some(warehouse) = self.warehouse_dir() {
            let dir = Path::new(warehouse).join(name);
            if dir.is_dir() {
                // Read data.parquet (our convention) or the dir (Polars accepts dirs with parquet files)
                let data_file = dir.join("data.parquet");
                let read_path = if data_file.is_file() { data_file } else { dir };
                return self.read_parquet(&read_path);
            }
        }
        Err(PolarsError::InvalidOperation(
            format!(
                "Table or view '{name}' not found. Register it with create_or_replace_temp_view or saveAsTable."
            )
            .into(),
        ))
    }

    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder::new()
    }

    /// Create a session from a [`SparklessConfig`](crate::config::SparklessConfig).
    /// Equivalent to `SparkSession::builder().with_config(config).get_or_create()`.
    pub fn from_config(config: &crate::config::SparklessConfig) -> SparkSession {
        Self::builder().with_config(config).get_or_create()
    }

    /// Return a reference to the session config (for catalog/conf compatibility).
    pub fn get_config(&self) -> &HashMap<String, String> {
        &self.config
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
    /// use robin_sparkless::session::SparkSession;
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
            .map_err(EngineError::from)
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
            JsonValue::String(s) => {
                if chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok() {
                    Some("date".to_string())
                } else if chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
                    || chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").is_ok()
                {
                    Some("timestamp".to_string())
                } else {
                    Some("string".to_string())
                }
            }
            JsonValue::Array(_) => Some("array".to_string()),
            JsonValue::Object(_) => Some("string".to_string()), // struct inference not implemented; treat as string for safety
        }
    }

    /// Infer schema (name, dtype_str) from JSON rows by scanning the first non-null value per column.
    /// Used by createDataFrame(data, schema=None) when schema is omitted or only column names given.
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
            for row in rows {
                let v = row.get(col_idx).unwrap_or(&JsonValue::Null);
                if let Some(dtype) = Self::infer_dtype_from_json_value(v) {
                    *dtype_str = dtype;
                    break;
                }
            }
        }
        schema
    }

    /// Create a DataFrame from rows and a schema (arbitrary column count and types).
    ///
    /// `rows`: each inner vec is one row; length must match schema length. Values are JSON-like (i64, f64, string, bool, null, object, array).
    /// `schema`: list of (column_name, dtype_string), e.g. `[("id", "bigint"), ("name", "string")]`.
    /// Supported dtype strings: bigint, int, long, double, float, string, str, varchar, boolean, bool, date, timestamp, datetime, list, array, array<element_type>, struct<field:type,...>.
    /// When `rows` is empty and `schema` is non-empty, returns an empty DataFrame with that schema (issue #519). Use with `write.format("parquet").saveAsTable(...)` then append; PySpark would fail with "can not infer schema from empty dataset".
    pub fn create_dataframe_from_rows(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
    ) -> Result<DataFrame, PolarsError> {
        // #624: When schema is empty but rows are not, infer schema from rows (PySpark parity).
        let schema = if schema.is_empty() && !rows.is_empty() {
            let ncols = rows[0].len();
            let names: Vec<String> = (0..ncols).map(|i| format!("c{i}")).collect();
            Self::infer_schema_from_json_rows(&rows, &names)
        } else {
            schema
        };

        if schema.is_empty() {
            if rows.is_empty() {
                return Ok(DataFrame::from_polars_with_options(
                    PlDataFrame::new(0, vec![])?,
                    self.is_case_sensitive(),
                ));
            }
            return Err(PolarsError::InvalidOperation(
                "create_dataframe_from_rows: schema must not be empty when rows are not empty"
                    .into(),
            ));
        }
        use chrono::{NaiveDate, NaiveDateTime};

        let mut cols: Vec<Series> = Vec::with_capacity(schema.len());

        for (col_idx, (name, type_str)) in schema.iter().enumerate() {
            let type_lower = type_str.trim().to_lowercase();
            let s = match type_lower.as_str() {
                "int" | "integer" | "bigint" | "long" => {
                    let vals: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Number(n) => n.as_i64(),
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "double" | "float" | "double_precision" => {
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
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "date" => {
                    let epoch = crate::date_utils::epoch_naive_date();
                    let vals: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::String(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                                    .ok()
                                    .map(|d| (d - epoch).num_days() as i32),
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
                    let inner_dtype = json_type_str_to_polars(&elem_type)
                        .ok_or_else(|| {
                            PolarsError::ComputeError(
                                format!(
                                    "create_dataframe_from_rows: array element type '{elem_type}' not supported"
                                )
                                .into(),
                            )
                        })?;
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
        Ok(DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Same as [`create_dataframe_from_rows`](Self::create_dataframe_from_rows) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn create_dataframe_from_rows_engine(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
    ) -> Result<DataFrame, EngineError> {
        self.create_dataframe_from_rows(rows, schema)
            .map_err(EngineError::from)
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
    /// use robin_sparkless::SparkSession;
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
        self.read_csv(path).map_err(EngineError::from)
    }

    /// Read a Parquet file.
    ///
    /// Uses Polars' Parquet reader. Parquet files have embedded schema, so
    /// schema inference is automatic.
    ///
    /// # Example
    /// ```
    /// use robin_sparkless::SparkSession;
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
        self.read_parquet(path).map_err(EngineError::from)
    }

    /// Read a JSON file (JSONL format - one JSON object per line).
    ///
    /// Uses Polars' JSONL reader with default options:
    /// - Schema is inferred from first 100 rows
    ///
    /// # Example
    /// ```
    /// use robin_sparkless::SparkSession;
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
        self.read_json(path).map_err(EngineError::from)
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
        self.table(name).map_err(EngineError::from)
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
    pub fn stop(&self) {
        // Best-effort cleanup. This is primarily for PySpark parity so that `spark.stop()`
        // exists and can be called in teardown.
        let _ = self.catalog.lock().map(|mut m| m.clear());
        let _ = self.tables.lock().map(|mut m| m.clear());
        let _ = self.databases.lock().map(|mut s| s.clear());
        let _ = self.udf_registry.clear();
        clear_thread_udf_session();
    }
}

/// DataFrameReader for reading various file formats
/// Similar to PySpark's DataFrameReader with option/options/format/load/table
pub struct DataFrameReader {
    session: SparkSession,
    options: HashMap<String, String>,
    format: Option<String>,
}

impl DataFrameReader {
    pub fn new(session: SparkSession) -> Self {
        DataFrameReader {
            session,
            options: HashMap::new(),
            format: None,
        }
    }

    /// Add a single option (PySpark: option(key, value)). Returns self for chaining.
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Add multiple options (PySpark: options(**kwargs)). Returns self for chaining.
    pub fn options(mut self, opts: impl IntoIterator<Item = (String, String)>) -> Self {
        for (k, v) in opts {
            self.options.insert(k, v);
        }
        self
    }

    /// Set the format for load() (PySpark: format("parquet") etc).
    pub fn format(mut self, fmt: impl Into<String>) -> Self {
        self.format = Some(fmt.into());
        self
    }

    /// Set the schema (PySpark: schema(schema)). Stub: stores but does not apply yet.
    pub fn schema(self, _schema: impl Into<String>) -> Self {
        self
    }

    /// Load data from path using format (or infer from extension) and options.
    pub fn load(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        let path = path.as_ref();
        let fmt = self.format.clone().or_else(|| {
            path.extension()
                .and_then(|e| e.to_str())
                .map(|s| s.to_lowercase())
        });
        match fmt.as_deref() {
            Some("parquet") => self.parquet(path),
            Some("csv") => self.csv(path),
            Some("json") | Some("jsonl") => self.json(path),
            #[cfg(feature = "delta")]
            Some("delta") => self.session.read_delta_from_path(path),
            _ => Err(PolarsError::ComputeError(
                format!(
                    "load: could not infer format for path '{}'. Use format('parquet'|'csv'|'json') before load.",
                    path.display()
                )
                .into(),
            )),
        }
    }

    /// Return the named table/view (PySpark: table(name)).
    pub fn table(&self, name: &str) -> Result<DataFrame, PolarsError> {
        self.session.table(name)
    }

    fn apply_csv_options(
        &self,
        reader: polars::prelude::LazyCsvReader,
    ) -> polars::prelude::LazyCsvReader {
        use polars::prelude::NullValues;
        let mut r = reader;
        if let Some(v) = self.options.get("header") {
            let has_header = v.eq_ignore_ascii_case("true") || v == "1";
            r = r.with_has_header(has_header);
        }
        if let Some(v) = self.options.get("inferSchema") {
            if v.eq_ignore_ascii_case("true") || v == "1" {
                let n = self
                    .options
                    .get("inferSchemaLength")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(100);
                r = r.with_infer_schema_length(Some(n));
            } else {
                // inferSchema=false: do not infer types (PySpark parity #543)
                r = r.with_infer_schema_length(Some(0));
            }
        } else if let Some(v) = self.options.get("inferSchemaLength") {
            if let Ok(n) = v.parse::<usize>() {
                r = r.with_infer_schema_length(Some(n));
            }
        }
        if let Some(sep) = self.options.get("sep") {
            if let Some(b) = sep.bytes().next() {
                r = r.with_separator(b);
            }
        }
        if let Some(null_val) = self.options.get("nullValue") {
            r = r.with_null_values(Some(NullValues::AllColumnsSingle(null_val.clone().into())));
        }
        r
    }

    fn apply_json_options(
        &self,
        reader: polars::prelude::LazyJsonLineReader,
    ) -> polars::prelude::LazyJsonLineReader {
        use std::num::NonZeroUsize;
        let mut r = reader;
        if let Some(v) = self.options.get("inferSchemaLength") {
            if let Ok(n) = v.parse::<usize>() {
                r = r.with_infer_schema_length(NonZeroUsize::new(n));
            }
        }
        r
    }

    pub fn csv(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        let path_display = path.display();
        let pl_path = PlRefPath::try_from_path(path).map_err(|e| {
            PolarsError::ComputeError(format!("csv({path_display}): path: {e}").into())
        })?;
        let reader = LazyCsvReader::new(pl_path);
        let reader = if self.options.is_empty() {
            reader
                .with_has_header(true)
                .with_infer_schema_length(Some(100))
        } else {
            self.apply_csv_options(
                reader
                    .with_has_header(true)
                    .with_infer_schema_length(Some(100)),
            )
        };
        let lf = reader.finish().map_err(|e| {
            PolarsError::ComputeError(format!("read csv({path_display}): {e}").into())
        })?;
        let pl_df = lf.collect().map_err(|e| {
            PolarsError::ComputeError(
                format!("read csv({path_display}): collect failed: {e}").into(),
            )
        })?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.session.is_case_sensitive(),
        ))
    }

    pub fn parquet(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        let pl_path = PlRefPath::try_from_path(path)
            .map_err(|e| PolarsError::ComputeError(format!("parquet: path: {e}").into()))?;
        let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?;
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.session.is_case_sensitive(),
        ))
    }

    pub fn json(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        use std::num::NonZeroUsize;
        let path = path.as_ref();
        let pl_path = PlRefPath::try_from_path(path)
            .map_err(|e| PolarsError::ComputeError(format!("json: path: {e}").into()))?;
        let reader = LazyJsonLineReader::new(pl_path);
        let reader = if self.options.is_empty() {
            reader.with_infer_schema_length(NonZeroUsize::new(100))
        } else {
            self.apply_json_options(reader.with_infer_schema_length(NonZeroUsize::new(100)))
        };
        let lf = reader.finish()?;
        let pl_df = lf.collect()?;
        Ok(crate::dataframe::DataFrame::from_polars_with_options(
            pl_df,
            self.session.is_case_sensitive(),
        ))
    }

    #[cfg(feature = "delta")]
    pub fn delta(&self, path: impl AsRef<std::path::Path>) -> Result<DataFrame, PolarsError> {
        self.session.read_delta_from_path(path)
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
        let result = spark.create_dataframe_from_rows(rows, schema);
        match &result {
            Err(e) => assert!(e.to_string().contains("schema must not be empty")),
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
        let result = spark.create_dataframe_from_rows(rows, schema);
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
        assert_eq!(df.collect_inner().unwrap().get_column_names(), &["a", "b"]);
    }

    #[test]
    fn test_create_dataframe_from_rows_empty_schema_empty_data() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![];
        let schema: Vec<(String, String)> = vec![];
        let result = spark.create_dataframe_from_rows(rows, schema);
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
        let df = spark.create_dataframe_from_rows(rows, schema).unwrap();
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
        let df = spark.create_dataframe_from_rows(rows, schema).unwrap();
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
        let df = spark.create_dataframe_from_rows(rows, schema).unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "nested"]);
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
            .create_dataframe_from_rows(rows_object, schema.clone())
            .unwrap();
        assert_eq!(df1.count().unwrap(), 1);

        // Struct as string that parses to JSON array (e.g. Python tuple (1, "y") serialized as "[1, \"y\"]").
        let rows_array: Vec<Vec<JsonValue>> = vec![vec![json!("B"), json!(r#"[1, "y"]"#)]];
        let df2 = spark
            .create_dataframe_from_rows(rows_array, schema)
            .unwrap();
        assert_eq!(df2.count().unwrap(), 1);
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
        let df = spark.create_dataframe_from_rows(rows, schema).unwrap();
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
        let df = spark.create_dataframe_from_rows(rows, schema).unwrap();
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
            .create_dataframe_from_rows(rows, schema)
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

    /// #624: When schema is empty but rows are not, infer schema from rows (PySpark parity).
    #[test]
    fn test_issue_624_empty_schema_inferred_from_rows() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema: Vec<(String, String)> = vec![];
        let rows: Vec<Vec<JsonValue>> =
            vec![vec![json!("a"), json!(1)], vec![json!("b"), json!(2)]];
        let df = spark
            .create_dataframe_from_rows(rows, schema)
            .expect("#624: empty schema with non-empty rows should infer schema");
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["c0", "c1"]);
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
        let df = spark.create_dataframe_from_rows(rows, schema).unwrap();
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
            .create_dataframe_from_rows(rows, schema)
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
            .create_dataframe_from_rows(vec![], schema.clone())
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
            .create_dataframe_from_rows(vec![vec![json!(1), json!("a")]], schema)
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
        let empty_df = spark.create_dataframe_from_rows(vec![], schema).unwrap();
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
            .create_dataframe_from_rows(vec![], schema.clone())
            .unwrap();
        empty_df
            .write()
            .save_as_table(&spark, "t_empty_wh", SaveMode::Overwrite)
            .unwrap();
        let r1 = spark.table("t_empty_wh").unwrap();
        assert_eq!(r1.count().unwrap(), 0);

        let one_row = spark
            .create_dataframe_from_rows(vec![vec![json!(1), json!("a")]], schema)
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
        let df = spark.create_dataframe_from_rows(rows, schema).unwrap();
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
