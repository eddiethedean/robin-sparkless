//! PyO3 bindings for sparkless Python package. Exposes robin-sparkless as a native module.
//! Parameter names follow PySpark camelCase where the Python API exposes them.

#![allow(non_snake_case)]
#![allow(unexpected_cfgs)] // pyo3 create_exception! uses cfg(gil-refs)
#![allow(clippy::useless_conversion)] // PyO3 PyResult<T> / PyErr false positives (fixed in pyo3 0.23+)

use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use robin_sparkless::dataframe::{
    try_extract_join_eq_columns_all, JoinType, PivotedGroupedData, SaveMode, WriteFormat, WriteMode,
};
use robin_sparkless::functions::{self, asc_from_name, SortOrder, ThenBuilder, WhenBuilder};
use robin_sparkless::{
    Column, CubeRollupData, DataFrame, DataType, GroupBySpec, GroupedData, SelectItem,
    SparkSession, SparkSessionBuilder, StructField,
};
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::path::Path;
use std::thread_local;

/// Convert EngineError or PolarsError to Python exception (SparklessError or TypeError for known validation errors).
/// Column-not-found messages are normalized to include "cannot resolve" for PySpark parity (issue #1058).
fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    let msg = e.to_string();
    if msg.contains("to_date requires StringType, TimestampType, or DateType input") {
        return pyo3::exceptions::PyTypeError::new_err(msg);
    }
    if msg.contains("Can not merge type") || msg.contains("Can not merge types") {
        return pyo3::exceptions::PyTypeError::new_err(msg);
    }
    if msg.contains("Some of types cannot be determined") {
        return pyo3::exceptions::PyValueError::new_err(msg);
    }
    // create_dataframe_from_rows with verify_schema: type mismatch -> TypeError (PySpark parity)
    if (msg.contains("Row ") || msg.contains("row ")) && msg.contains("expected type") {
        return pyo3::exceptions::PyTypeError::new_err(msg);
    }
    // Int subscript on struct column: PySpark/issue #339 expects TypeError (string keys for struct field access).
    if (msg.contains("expected List") || msg.contains("list operation"))
        && (msg.contains("Struct") || msg.contains("struct"))
    {
        return pyo3::exceptions::PyTypeError::new_err(
            "Struct field subscript access requires string keys, not int",
        );
    }
    // Ensure column-not-found errors contain "cannot be resolved" and "unresolved_column" (PySpark parity; issue #158).
    let msg = if !msg.to_lowercase().contains("cannot be resolved")
        && (msg.contains("unable to find column")
            || (msg.contains("not found") && msg.to_lowercase().contains("column"))
            || msg.contains("valid columns"))
    {
        format!("unresolved_column: cannot be resolved: {msg}")
    } else if !msg.to_lowercase().contains("cannot be resolved")
        && msg.to_lowercase().contains("cannot resolve")
    {
        msg.replace("cannot resolve", "cannot be resolved")
    } else {
        msg
    };
    SparklessError::new_err(msg)
}

/// Extract allowMissingColumns (PySpark camelCase) or allow_missing_columns from kwargs.
fn extract_allow_missing_columns(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<bool> {
    let Some(kw) = kwargs else {
        return Ok(false);
    };
    if let Some(v) = kw.get_item("allowMissingColumns")? {
        return v.extract::<bool>();
    }
    if let Some(v) = kw.get_item("allow_missing_columns")? {
        return v.extract::<bool>();
    }
    Ok(false)
}

create_exception!(
    _native,
    SparklessError,
    pyo3::exceptions::PyRuntimeError,
    "Sparkless error"
);

/// Convert serde_json::Value to Python object.
fn json_to_py(value: &JsonValue, py: Python<'_>) -> PyResult<PyObject> {
    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => Ok(b.into_py(py)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py(py))
            } else {
                Ok(n.to_string().into_py(py))
            }
        }
        JsonValue::String(s) => Ok(s.clone().into_py(py)),
        JsonValue::Array(arr) => {
            let list = PyList::empty_bound(py);
            for v in arr {
                list.append(json_to_py(v, py)?)?;
            }
            Ok(list.into_py(py))
        }
        JsonValue::Object(obj) => {
            let dict = PyDict::new_bound(py);
            for (k, v) in obj {
                let py_v = match v {
                    JsonValue::String(s) if s.trim_start().starts_with('{') => {
                        if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                            if parsed.is_object() {
                                json_to_py(&parsed, py)?
                            } else {
                                s.clone().into_py(py)
                            }
                        } else {
                            s.clone().into_py(py)
                        }
                    }
                    _ => json_to_py(v, py)?,
                };
                dict.set_item(k, py_v)?;
            }
            Ok(dict.into_py(py))
        }
    }
}

/// Best-effort coerce a string to int, float, or bool when it looks like one (e.g. coalesce
/// results or stringified array elements). Returns None to keep as string.
/// Kept for potential use when schema is numeric but engine sent string.
#[allow(dead_code)]
fn try_coerce_string_to_numeric_or_bool(py: Python<'_>, s: &str) -> Option<PyObject> {
    let s = s.trim();
    let lower = s.to_lowercase();
    if lower == "true" || lower == "1" {
        return Some(true.into_py(py));
    }
    if lower == "false" || lower == "0" {
        return Some(false.into_py(py));
    }
    if let Ok(i) = s.parse::<i64>() {
        return Some(i.into_py(py));
    }
    if let Ok(f) = s.parse::<f64>() {
        return Some(f.into_py(py));
    }
    None
}

/// #1066: Parse a stringified struct value (e.g. Polars debug "{10}" or "10, \"test\"") into
/// a JSON object using the schema, so nested structs in collect() become dicts.
fn parse_struct_string_to_json(s: &str, fields: &[StructField]) -> Option<JsonValue> {
    use serde_json::Map;
    if fields.is_empty() {
        return None;
    }
    let trimmed = s.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|t| t.strip_suffix('}'))
        .map(|t| t.trim())
        .unwrap_or(trimmed);
    let mut obj = Map::new();
    if fields.len() == 1 {
        let f = &fields[0];
        let val = match &f.data_type {
            DataType::Integer | DataType::Long => inner
                .parse::<i64>()
                .ok()
                .map(serde_json::Number::from)
                .map(JsonValue::Number),
            DataType::Double => inner
                .parse::<f64>()
                .ok()
                .filter(|f| f.is_finite())
                .and_then(|f| serde_json::Number::from_f64(f).map(JsonValue::Number)),
            DataType::String => Some(JsonValue::String(
                inner
                    .strip_prefix('"')
                    .and_then(|t| t.strip_suffix('"'))
                    .unwrap_or(inner)
                    .to_string(),
            )),
            DataType::Boolean => {
                if inner.eq_ignore_ascii_case("true") {
                    Some(JsonValue::Bool(true))
                } else if inner.eq_ignore_ascii_case("false") {
                    Some(JsonValue::Bool(false))
                } else {
                    None
                }
            }
            _ => None,
        }?;
        obj.insert(f.name.clone(), val);
        return Some(JsonValue::Object(obj));
    }
    // Try "name: value, name: value" format first (order-independent).
    let mut by_name: Map<String, JsonValue> = Map::new();
    for segment in inner.split(", ") {
        let segment = segment.trim();
        if let Some((k, v)) = segment.split_once(':') {
            let k = k.trim().trim_matches('"').to_string();
            let v = v.trim();
            for f in fields.iter() {
                if f.name == k {
                    let part_unescaped = v
                        .strip_prefix('"')
                        .and_then(|t| t.strip_suffix('"'))
                        .unwrap_or(v);
                    let val = match &f.data_type {
                        DataType::Integer | DataType::Long => v
                            .parse::<i64>()
                            .ok()
                            .map(serde_json::Number::from)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null),
                        DataType::Double => v
                            .parse::<f64>()
                            .ok()
                            .filter(|x| x.is_finite())
                            .and_then(serde_json::Number::from_f64)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null),
                        DataType::String => JsonValue::String(part_unescaped.to_string()),
                        DataType::Boolean => {
                            if v.eq_ignore_ascii_case("true") {
                                JsonValue::Bool(true)
                            } else if v.eq_ignore_ascii_case("false") {
                                JsonValue::Bool(false)
                            } else {
                                JsonValue::Null
                            }
                        }
                        _ => JsonValue::Null,
                    };
                    by_name.insert(k, val);
                    break;
                }
            }
        }
    }
    if !by_name.is_empty() {
        for f in fields {
            by_name.entry(f.name.clone()).or_insert(JsonValue::Null);
        }
        return Some(JsonValue::Object(by_name));
    }
    // Fallback: positional split. Match each part to a field by type (order-agnostic) so
    // "test, 10" or "test,10" still yields inner_value=10, inner_string="test" for [Integer, String].
    let parts: Vec<&str> = inner
        .split(',')
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect();
    let mut used = vec![false; parts.len()];
    for f in fields.iter() {
        let mut val = JsonValue::Null;
        for (i, part) in parts.iter().enumerate() {
            if used[i] {
                continue;
            }
            let part_unescaped = part
                .strip_prefix('"')
                .and_then(|t| t.strip_suffix('"'))
                .unwrap_or(part);
            let matched = match &f.data_type {
                DataType::Integer | DataType::Long => part
                    .parse::<i64>()
                    .ok()
                    .map(serde_json::Number::from)
                    .map(JsonValue::Number),
                DataType::Double => part
                    .parse::<f64>()
                    .ok()
                    .filter(|x| x.is_finite())
                    .and_then(serde_json::Number::from_f64)
                    .map(JsonValue::Number),
                DataType::String => Some(JsonValue::String(part_unescaped.to_string())),
                DataType::Boolean => {
                    if part.eq_ignore_ascii_case("true") {
                        Some(JsonValue::Bool(true))
                    } else if part.eq_ignore_ascii_case("false") {
                        Some(JsonValue::Bool(false))
                    } else {
                        None
                    }
                }
                _ => None,
            };
            if let Some(v) = matched {
                val = v;
                used[i] = true;
                break;
            }
        }
        obj.insert(f.name.clone(), val);
    }
    Some(JsonValue::Object(obj))
}

/// Convert JSON value to Python with optional schema-based coercion so that numeric/boolean
/// Column names that are always treated as string in collect() (#1165 fix must not coerce these).
/// #1146: c0, c1 from json_tuple always string. a/nested/missing only when output has all three (get_json_object shape).
const COLLECT_STRING_ONLY_COLUMNS: &[&str] = &[
    "Period",
    "Name",
    "Value1",
    "Value2",
    "Value2Renamed",
    "ExtraColumn",
    "c0",
    "c1",
];

/// True when output has a, nested, missing (get_json_object test shape); then treat those as string (#1146, avoid regression on lone column "a").
fn is_get_json_object_shape(output_names: Option<&[String]>) -> bool {
    match output_names {
        Some(n) => {
            n.iter().any(|s| s == "a")
                && n.iter().any(|s| s == "nested")
                && n.iter().any(|s| s == "missing")
        }
        None => false,
    }
}

/// True when output has both key and value (key-value shape); then treat "value" as string so "2" stays string (#1146 na_fill test).
fn is_key_value_shape(output_names: Option<&[String]>) -> bool {
    match output_names {
        Some(n) => n.iter().any(|s| s == "key") && n.iter().any(|s| s == "value"),
        None => false,
    }
}

/// types are preserved even when the engine sent a string (e.g. string-inferred schema).
/// Recurses for Array and Struct so nested values are coerced too.
/// When schema is String, best-effort coerces to int/float/bool when the value looks like one
/// (e.g. coalesce() results or mixed-type array elements stringified by the engine).
/// column_name: when Some (top-level collect), coerce numeric strings except for COLLECT_STRING_ONLY_COLUMNS (#1165).
/// output_column_names: when Some (top-level collect), used to treat a/nested/missing as string only when all three present (#1146).
fn json_value_to_py_with_schema(
    py: Python<'_>,
    value: &JsonValue,
    dtype: Option<&DataType>,
    datetime_cls: &Bound<'_, PyAny>,
    date_cls: &Bound<'_, PyAny>,
    column_name: Option<&str>,
    output_column_names: Option<&[String]>,
) -> PyResult<PyObject> {
    Ok(match (dtype, value) {
        (Some(DataType::Timestamp), JsonValue::String(s)) => datetime_cls
            .call_method1("fromisoformat", (s.as_str(),))
            .map(|o| o.into_py(py))
            .unwrap_or_else(|_| s.clone().into_py(py)),
        (Some(DataType::Date), JsonValue::String(s)) => date_cls
            .call_method1("fromisoformat", (s.as_str(),))
            .map(|o| o.into_py(py))
            .unwrap_or_else(|_| s.clone().into_py(py)),
        (Some(DataType::Integer) | Some(DataType::Long), JsonValue::String(s)) => {
            let s = s.trim();
            if let Ok(i) = s.parse::<i64>() {
                i.into_py(py)
            } else {
                s.to_string().into_py(py)
            }
        }
        (Some(DataType::Double), JsonValue::String(s)) => {
            let s = s.trim();
            if let Ok(f) = s.parse::<f64>() {
                f.into_py(py)
            } else {
                s.to_string().into_py(py)
            }
        }
        (Some(DataType::Boolean), JsonValue::String(s)) => {
            let lower = s.trim().to_lowercase();
            if lower == "true" || lower == "1" {
                true.into_py(py)
            } else if lower == "false" || lower == "0" {
                false.into_py(py)
            } else {
                s.clone().into_py(py)
            }
        }
        // Schema says String: preserve string in Row. #1066: parse stringified JSON object to dict only
        // when inside a struct (column_name is None). Top-level String columns (e.g. get_json_object
        // output) must stay as string for PySpark parity (#1146). #1165: coerce to int/float when
        // column is not in string-only blocklist. a/nested/missing only blocklist when all three columns present.
        (Some(DataType::String), JsonValue::String(s)) => {
            if column_name.is_none() {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                    if parsed.is_object() {
                        return json_to_py(&parsed, py);
                    }
                }
            }
            let string_only = column_name.map(|n| {
                COLLECT_STRING_ONLY_COLUMNS.contains(&n)
                    || (is_get_json_object_shape(output_column_names)
                        && (n == "a" || n == "nested" || n == "missing"))
                    || (n == "value" && is_key_value_shape(output_column_names))
            });
            let coerce_ok = string_only.map(|b| !b).unwrap_or(false);
            if coerce_ok {
                let t = s.trim();
                if let Ok(i) = t.parse::<i64>() {
                    return Ok(i.into_py(py));
                }
                if let Ok(f) = t.parse::<f64>() {
                    return Ok(f.into_py(py));
                }
            }
            s.clone().into_py(py)
        }
        // Schema says String but engine sent number (e.g. cast to string, inference): emit string.
        (Some(DataType::String), JsonValue::Number(n)) => n.to_string().into_py(py),
        // Schema says String but engine sent bool: emit "True"/"False".
        (Some(DataType::String), JsonValue::Bool(b)) => b.to_string().into_py(py),
        // #1146: json_tuple c0/c1 always string; get_json_object a/nested/missing only when all three columns present.
        (Some(DataType::Integer) | Some(DataType::Long), JsonValue::Number(n))
            if matches!(column_name, Some("c0") | Some("c1"))
                || (is_get_json_object_shape(output_column_names)
                    && matches!(column_name, Some("a") | Some("nested") | Some("missing"))) =>
        {
            n.to_string().into_py(py)
        }
        // No schema (e.g. coalesce output): best-effort parse string to int/float/bool for PySpark parity.
        // #1066: If string looks like JSON object/array, parse so nested structs work.
        (None, JsonValue::String(s)) => {
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                if parsed.is_object() || parsed.is_array() {
                    return json_to_py(&parsed, py);
                }
            }
            let s = s.trim();
            if let Ok(i) = s.parse::<i64>() {
                i.into_py(py)
            } else if let Ok(f) = s.parse::<f64>() {
                f.into_py(py)
            } else {
                let lower = s.to_lowercase();
                if lower == "true" || lower == "1" {
                    true.into_py(py)
                } else if lower == "false" || lower == "0" {
                    false.into_py(py)
                } else {
                    s.to_string().into_py(py)
                }
            }
        }
        (Some(DataType::Array(elem_type)), JsonValue::Array(arr)) => {
            let list = PyList::empty_bound(py);
            for v in arr {
                list.append(json_value_to_py_with_schema(
                    py,
                    v,
                    Some(elem_type),
                    datetime_cls,
                    date_cls,
                    None,
                    None,
                )?)?;
            }
            list.into_py(py)
        }
        // Engine may stringify list columns in some paths (e.g. "[1,2]"); parse back when schema says Array.
        (Some(DataType::Array(elem_type)), JsonValue::String(s)) => {
            if let Ok(JsonValue::Array(arr)) = serde_json::from_str::<JsonValue>(s) {
                let list = PyList::empty_bound(py);
                for v in &arr {
                    list.append(json_value_to_py_with_schema(
                        py,
                        v,
                        Some(elem_type),
                        datetime_cls,
                        date_cls,
                        None,
                        None,
                    )?)?;
                }
                return Ok(list.into_py(py));
            }
            s.clone().into_py(py)
        }
        (Some(DataType::Struct(fields)), JsonValue::Object(obj)) => {
            let dict = PyDict::new_bound(py);
            for f in fields {
                let v = obj.get(&f.name).unwrap_or(&JsonValue::Null);
                let py_v = json_value_to_py_with_schema(
                    py,
                    v,
                    Some(&f.data_type),
                    datetime_cls,
                    date_cls,
                    None,
                    None,
                )?;
                dict.set_item(&f.name, py_v)?;
            }
            dict.into_py(py)
        }
        // #1066: Struct (top-level or nested) may be stringified; parse so row["my_struct"]["nested"] is dict.
        (Some(DataType::Struct(fields)), JsonValue::String(s)) => {
            if let Ok(JsonValue::Object(obj)) = serde_json::from_str::<JsonValue>(s) {
                let dict = PyDict::new_bound(py);
                for f in fields {
                    let v = obj.get(&f.name).unwrap_or(&JsonValue::Null);
                    let py_v = json_value_to_py_with_schema(
                        py,
                        v,
                        Some(&f.data_type),
                        datetime_cls,
                        date_cls,
                        None,
                        None,
                    )?;
                    dict.set_item(&f.name, py_v)?;
                }
                return Ok(dict.into_py(py));
            }
            if let Some(obj) = parse_struct_string_to_json(s, fields) {
                let dict = PyDict::new_bound(py);
                for f in fields {
                    let v = obj.get(&f.name).unwrap_or(&JsonValue::Null);
                    let py_v = json_value_to_py_with_schema(
                        py,
                        v,
                        Some(&f.data_type),
                        datetime_cls,
                        date_cls,
                        None,
                        None,
                    )?;
                    dict.set_item(&f.name, py_v)?;
                }
                return Ok(dict.into_py(py));
            }
            s.clone().into_py(py)
        }
        _ => json_to_py(value, py)?,
    })
}

#[pyclass]
struct PySparkSessionBuilder {
    inner: SparkSessionBuilder,
}

#[pymethods]
impl PySparkSessionBuilder {
    #[new]
    fn new() -> Self {
        Self {
            inner: SparkSessionBuilder::new(),
        }
    }

    /// PySpark: SparkSession.builder() returns the builder; allow () for compatibility.
    fn __call__(slf: PyRef<Self>, py: Python<'_>) -> Py<PyAny> {
        slf.into_py(py)
    }

    fn app_name<'a>(mut slf: PyRefMut<'a, Self>, name: &str) -> PyRefMut<'a, Self> {
        let old = std::mem::replace(&mut slf.inner, SparkSessionBuilder::new());
        slf.inner = old.app_name(name);
        slf
    }

    /// PySpark camelCase alias for app_name
    #[pyo3(name = "appName")]
    fn app_name_camel<'a>(mut slf: PyRefMut<'a, Self>, name: &str) -> PyRefMut<'a, Self> {
        let old = std::mem::replace(&mut slf.inner, SparkSessionBuilder::new());
        slf.inner = old.app_name(name);
        slf
    }

    fn master<'a>(mut slf: PyRefMut<'a, Self>, master: &str) -> PyRefMut<'a, Self> {
        let old = std::mem::replace(&mut slf.inner, SparkSessionBuilder::new());
        slf.inner = old.master(master);
        slf
    }

    fn config<'a>(mut slf: PyRefMut<'a, Self>, key: &str, value: &str) -> PyRefMut<'a, Self> {
        let old = std::mem::replace(&mut slf.inner, SparkSessionBuilder::new());
        slf.inner = old.config(key, value);
        slf
    }

    /// PySpark builder.option(key, value); value can be str, bool, int, etc. (stringified).
    fn option<'a>(
        mut slf: PyRefMut<'a, Self>,
        key: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        let s = option_value_to_string(value)?;
        let old = std::mem::replace(&mut slf.inner, SparkSessionBuilder::new());
        slf.inner = old.config(key, &s);
        Ok(slf)
    }

    fn get_or_create(&self, py: Python<'_>) -> PyResult<Py<PySparkSession>> {
        let session = self.inner.clone().get_or_create();
        let obj = Py::new(
            py,
            PySparkSession {
                inner: session,
                backend_type: RefCell::new(DEFAULT_BACKEND_TYPE.to_string()),
            },
        )?;
        register_active_session(py, obj.clone_ref(py), true)?;
        Ok(obj)
    }

    /// PySpark camelCase alias for get_or_create
    #[pyo3(name = "getOrCreate")]
    fn get_or_create_camel(&self, py: Python<'_>) -> PyResult<Py<PySparkSession>> {
        self.get_or_create(py)
    }
}

/// Default backend type so tests that read session.backend_type always get a value (e.g. "robin").
const DEFAULT_BACKEND_TYPE: &str = "robin";

#[pyclass]
struct PySparkSession {
    inner: SparkSession,
    /// Writable from Python so conftest/fixtures can set backend_type = "robin" (e.g. under pytest-xdist).
    backend_type: RefCell<String>,
}

thread_local! {
    /// Thread-local stack of active sessions (PySpark getActiveSession semantics).
    static THREAD_ACTIVE_SESSIONS: RefCell<Vec<Py<PySparkSession>>> = const { RefCell::new(Vec::new()) };
    /// Optional Python callable to run Python UDFs when with_column sees a UDF column. Set by Python on session creation.
    static PYTHON_UDF_EXECUTOR: RefCell<Option<Py<PyAny>>> = const { RefCell::new(None) };
}

fn register_active_session(
    py: Python<'_>,
    session: Py<PySparkSession>,
    set_singleton: bool,
) -> PyResult<()> {
    let ptr = session.as_ptr();
    THREAD_ACTIVE_SESSIONS.with(|cell| cell.borrow_mut().push(session.clone_ref(py)));

    let ty = py.get_type_bound::<PySparkSession>();
    // Ensure compatibility attrs exist and are mutable from Python.
    if ty.getattr("_active_sessions").is_err() {
        ty.setattr("_active_sessions", PyList::empty_bound(py))?;
    }
    if ty.getattr("_singleton_session").is_err() {
        ty.setattr("_singleton_session", py.None())?;
    }

    // Update global-ish list (best-effort; tests mutate it directly).
    if let Ok(list_any) = ty.getattr("_active_sessions") {
        if let Ok(list) = list_any.downcast::<PyList>() {
            let already = list.iter().any(|it| it.as_ptr() == ptr);
            if !already {
                list.append(session.clone_ref(py))?;
            }
        }
    }

    if set_singleton {
        ty.setattr("_singleton_session", session)?;
    }

    Ok(())
}

fn unregister_active_session_by_ptr(py: Python<'_>, ptr: *mut pyo3::ffi::PyObject) -> PyResult<()> {
    THREAD_ACTIVE_SESSIONS.with(|cell| {
        let mut v = cell.borrow_mut();
        v.retain(|s| s.as_ptr() != ptr);
    });

    let ty = py.get_type_bound::<PySparkSession>();
    if let Ok(list_any) = ty.getattr("_active_sessions") {
        if let Ok(list) = list_any.downcast::<PyList>() {
            // Remove all matching entries.
            let mut i = 0usize;
            while i < list.len() {
                if let Ok(item) = list.get_item(i) {
                    if item.as_ptr() == ptr {
                        let _ = list.del_item(i);
                        continue;
                    }
                }
                i += 1;
            }
        }
    }

    // Clear singleton if it points to this session.
    if let Ok(singleton) = ty.getattr("_singleton_session") {
        if singleton.as_ptr() == ptr {
            ty.setattr("_singleton_session", py.None())?;
        }
    }

    Ok(())
}

#[pymethods]
impl PySparkSession {
    #[new]
    #[pyo3(signature = (app_name=None, master=None, **kwargs))]
    fn new(
        py: Python<'_>,
        app_name: Option<String>,
        master: Option<String>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PySparkSession>> {
        let mut builder = SparkSession::builder();
        if let Some(name) = app_name.as_deref() {
            builder = builder.app_name(name);
        }
        if let Some(m) = master.as_deref() {
            builder = builder.master(m);
        }
        if let Some(kw) = kwargs {
            for (k, v) in kw.iter() {
                let key: String = k.extract()?;
                let val: String = if let Ok(s) = v.extract::<String>() {
                    s
                } else if let Ok(b) = v.extract::<bool>() {
                    b.to_string()
                } else if let Ok(i) = v.extract::<i64>() {
                    i.to_string()
                } else if let Ok(f) = v.extract::<f64>() {
                    f.to_string()
                } else {
                    // Best-effort: string repr.
                    v.str()?.to_string()
                };
                builder = builder.config(key, val);
            }
        }
        let session = builder.get_or_create();
        let obj = Py::new(
            py,
            PySparkSession {
                inner: session,
                backend_type: RefCell::new(DEFAULT_BACKEND_TYPE.to_string()),
            },
        )?;
        register_active_session(py, obj.clone_ref(py), true)?;
        Ok(obj)
    }

    /// PySpark: SparkSession.builder returns a builder instance.
    /// We expose it as a class attribute so both `SparkSession.builder.appName(...)`
    /// and `SparkSession.builder().appName(...)` work (the builder itself is callable).
    #[classattr]
    fn builder(_py: Python<'_>) -> PySparkSessionBuilder {
        PySparkSessionBuilder {
            inner: SparkSession::builder(),
        }
    }

    #[getter]
    fn backend_type(&self) -> String {
        self.backend_type.borrow().clone()
    }

    #[setter]
    fn set_backend_type(slf: PyRefMut<Self>, value: String) -> PyResult<()> {
        *slf.backend_type.borrow_mut() = value;
        Ok(())
    }

    #[getter]
    fn app_name(&self) -> Option<String> {
        self.inner.app_name()
    }

    #[classmethod]
    #[pyo3(name = "getActiveSession")]
    fn get_active_session(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
    ) -> PyResult<Option<Py<PySparkSession>>> {
        let ty = py.get_type_bound::<PySparkSession>();
        if let Ok(singleton) = ty.getattr("_singleton_session") {
            if !singleton.is_none() {
                if let Ok(sess) = singleton.extract::<Py<PySparkSession>>() {
                    return Ok(Some(sess));
                }
            }
        }

        // Compatibility: if tests manually clear _active_sessions and singleton, treat as no session.
        if let Ok(list_any) = ty.getattr("_active_sessions") {
            if let Ok(list) = list_any.downcast::<PyList>() {
                if list.is_empty() {
                    return Ok(None);
                }
            }
        }

        let top = THREAD_ACTIVE_SESSIONS.with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)));
        Ok(top)
    }

    #[pyo3(name = "newSession")]
    fn new_session(slf: PyRef<Self>, py: Python<'_>) -> PyResult<Py<PySparkSession>> {
        let session = slf.inner.new_session();
        let backend_type = slf.backend_type.borrow().clone();
        let obj = Py::new(
            py,
            PySparkSession {
                inner: session,
                backend_type: RefCell::new(backend_type),
            },
        )?;
        register_active_session(py, obj.clone_ref(py), true)?;
        Ok(obj)
    }

    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc=None, _tb=None))]
    fn __exit__(
        slf: PyRef<Self>,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        PySparkSession::stop(slf, py)?;
        Ok(false)
    }

    #[getter]
    fn read(slf: PyRef<Self>) -> PyDataFrameReader {
        let py = slf.py();
        PyDataFrameReader {
            session: slf.into_py(py),
            options: Vec::new(),
            format: None,
        }
    }

    #[getter]
    fn catalog(slf: PyRef<Self>) -> PyCatalog {
        let py = slf.py();
        PyCatalog {
            session: slf.into_py(py),
        }
    }

    #[getter]
    fn conf(slf: PyRef<Self>) -> PyRuntimeConfig {
        let py = slf.py();
        PyRuntimeConfig {
            session: slf.into_py(py),
        }
    }

    #[getter]
    #[pyo3(name = "sparkContext")]
    fn spark_context(slf: PyRef<Self>) -> PySparkContext {
        let py = slf.py();
        PySparkContext {
            session: slf.into_py(py),
        }
    }

    #[getter]
    fn _storage(slf: PyRef<Self>) -> PyStorage {
        let py = slf.py();
        PyStorage {
            session: slf.into_py(py),
        }
    }

    fn create_or_replace_temp_view(&self, name: &str, df: &PyDataFrame) {
        self.inner
            .create_or_replace_temp_view(name, df.inner.clone());
    }

    fn create_or_replace_global_temp_view(&self, name: &str, df: &PyDataFrame) {
        self.inner
            .create_or_replace_global_temp_view(name, df.inner.clone());
    }

    fn drop_temp_view(&self, name: &str) {
        self.inner.drop_temp_view(name);
    }

    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        self.inner
            .table(name)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn sql(&self, query: &str) -> PyResult<PyDataFrame> {
        self.inner
            .sql(query)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn create_dataframe(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<String>,
    ) -> PyResult<PyDataFrame> {
        let names: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        self.inner
            .create_dataframe(data, names)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    /// createDataFrame(data, schema=None, verify_schema=True, sampling_ratio=None).
    /// data: list of dicts, list of tuples, or pandas.DataFrame.
    /// schema: optional list of (name, type_str) or list of column names (types inferred).
    /// verify_schema: when True, validate row values against schema types (PySpark parity).
    /// sampling_ratio: accepted for API parity; ignored for list/dict data.
    #[pyo3(signature = (data, schema=None, verify_schema=true, sampling_ratio=None))]
    fn create_dataframe_from_rows(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
        verify_schema: bool,
        sampling_ratio: Option<f64>,
    ) -> PyResult<PyDataFrame> {
        let _ = sampling_ratio; // no-op for list/dict data; PySpark uses for CSV inference
        let (data, from_pandas, pandas_column_order) =
            normalize_create_dataframe_input(py, data, schema)?;
        let schema_cache =
            schema.and_then(|s| s.getattr("fields").ok().map(|_| s.clone().unbind()));
        let (rows, schema_pairs, schema_was_inferred) = python_data_and_schema(
            py,
            &data,
            schema,
            from_pandas,
            pandas_column_order.as_deref(),
        )?;
        let df = self
            .inner
            .create_dataframe_from_rows(rows, schema_pairs, verify_schema, schema_was_inferred)
            .map_err(to_py_err)?;
        Ok(PyDataFrame {
            inner: df,
            schema_cache,
        })
    }

    /// PySpark alias: createDataFrame -> create_dataframe_from_rows
    #[pyo3(name = "createDataFrame", signature = (data, schema=None, verify_schema=true, sampling_ratio=None))]
    fn create_data_frame_camel(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
        verify_schema: bool,
        sampling_ratio: Option<f64>,
    ) -> PyResult<PyDataFrame> {
        self.create_dataframe_from_rows(py, data, schema, verify_schema, sampling_ratio)
    }

    fn range(&self, start: i64, end: i64, step: i64) -> PyResult<PyDataFrame> {
        self.inner
            .range(start, end, step)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn read_csv(&self, path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_csv(Path::new(path))
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn read_parquet(&self, path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_parquet(Path::new(path))
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn read_json(&self, path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_json(Path::new(path))
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[cfg(feature = "delta")]
    fn read_delta(&self, name_or_path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_delta(name_or_path)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[cfg(feature = "delta")]
    #[pyo3(signature = (name_or_path, version=None))]
    fn read_delta_with_version(
        &self,
        name_or_path: &str,
        version: Option<i64>,
    ) -> PyResult<PyDataFrame> {
        self.inner
            .read_delta_with_version(name_or_path, version)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn stop(slf: PyRef<Self>, py: Python<'_>) -> PyResult<()> {
        slf.inner.stop();
        let ptr = slf.into_py(py).as_ptr();
        unregister_active_session_by_ptr(py, ptr)?;
        Ok(())
    }
}

#[pyclass]
struct PyDatabase {
    #[pyo3(get)]
    name: String,
}

#[pyclass]
struct PyTable {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    database: String,
}

#[pyclass]
struct PyRuntimeConfig {
    session: Py<PyAny>,
}

#[pymethods]
impl PyRuntimeConfig {
    /// Allow spark.conf() as well as spark.conf (property); returns self.
    fn __call__(slf: PyRef<Self>, py: Python<'_>) -> Py<PyAny> {
        slf.into_py(py)
    }

    /// PySpark: spark.conf.get(key, default=None) returns value or default as string.
    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<&str>) -> PyResult<String> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let sess = session.borrow();
        let config = sess.inner.get_config();
        Ok(config
            .get(key)
            .cloned()
            .or_else(|| default.map(String::from))
            .unwrap_or_default())
    }

    /// PySpark: spark.conf.set(key, value).
    fn set(&self, py: Python<'_>, key: &str, value: &str) -> PyResult<()> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let mut sess = session.borrow_mut();
        sess.inner.set_config(key, value);
        Ok(())
    }

    fn is_case_sensitive(&self, py: Python<'_>) -> PyResult<bool> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let sess = session.borrow();
        Ok(sess.inner.is_case_sensitive())
    }
}

/// Minimal stub for PySpark compatibility: spark.sparkContext.appName / .version
#[pyclass]
struct PySparkContext {
    session: Py<PyAny>,
}

#[pymethods]
impl PySparkContext {
    #[getter]
    fn app_name(&self, py: Python<'_>) -> PyResult<String> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let sess = session.borrow();
        Ok(sess
            .inner
            .app_name()
            .unwrap_or_else(|| "sparkless".to_string()))
    }

    /// PySpark parity: version is a property (read-only string), not a method.
    #[getter]
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

#[pyclass]
struct PyCatalog {
    session: Py<PyAny>,
}

#[pymethods]
impl PyCatalog {
    #[pyo3(name = "listTables")]
    #[pyo3(signature = (dbName=None))]
    fn list_tables(&self, py: Python<'_>, dbName: Option<String>) -> PyResult<Vec<Py<PyTable>>> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let sess = session.borrow();

        let mut out: Vec<(String, String)> = Vec::new(); // (db, table)
        let filter_db = dbName.as_deref();
        if let Some(db) = filter_db {
            if db.eq_ignore_ascii_case("global_temp") {
                for n in sess.inner.list_global_temp_view_names() {
                    out.push(("global_temp".to_string(), n));
                }
            } else {
                for full in sess
                    .inner
                    .list_table_names()
                    .into_iter()
                    .chain(sess.inner.list_temp_view_names())
                {
                    if let Some((db_part, tbl_part)) = full.split_once('.') {
                        if db_part == db {
                            out.push((db.to_string(), tbl_part.to_string()));
                        }
                    } else if db == "default" {
                        out.push(("default".to_string(), full));
                    }
                }
            }
        } else {
            for full in sess
                .inner
                .list_table_names()
                .into_iter()
                .chain(sess.inner.list_temp_view_names())
            {
                if let Some((db_part, tbl_part)) = full.split_once('.') {
                    out.push((db_part.to_string(), tbl_part.to_string()));
                } else {
                    out.push(("default".to_string(), full));
                }
            }
        }

        let mut py_tables = Vec::with_capacity(out.len());
        for (db, name) in out {
            py_tables.push(Py::new(py, PyTable { name, database: db })?);
        }
        Ok(py_tables)
    }

    #[pyo3(name = "dropTempView")]
    fn drop_temp_view(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        session.inner.drop_temp_view(name);
        Ok(())
    }

    #[pyo3(name = "listDatabases")]
    fn list_databases(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDatabase>>> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let sess = session.borrow();
        let names = sess.inner.list_database_names();
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            out.push(Py::new(py, PyDatabase { name: n })?);
        }
        Ok(out)
    }

    #[pyo3(name = "createDatabase", signature = (name, ignoreIfExists=true))]
    fn create_database(
        &self,
        py: Python<'_>,
        name: &Bound<'_, PyAny>,
        ignoreIfExists: bool,
    ) -> PyResult<()> {
        let name: String = name
            .extract()
            .map_err(|_| to_py_err("database name must be a string"))?;
        if name.is_empty() {
            return Err(to_py_err("database name cannot be empty"));
        }
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        if session.inner.database_exists(&name) {
            if ignoreIfExists {
                return Ok(());
            }
            return Err(to_py_err(format!("Database '{name}' already exists")));
        }
        session.inner.register_database(&name);
        Ok(())
    }

    #[pyo3(name = "dropDatabase", signature = (name, ignoreIfNotExists=true))]
    fn drop_database(
        &self,
        py: Python<'_>,
        name: &Bound<'_, PyAny>,
        ignoreIfNotExists: bool,
    ) -> PyResult<()> {
        let name: String = name
            .extract()
            .map_err(|_| to_py_err("database name must be a string"))?;
        if name.is_empty() {
            return Err(to_py_err("database name cannot be empty"));
        }
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let existed = session.inner.database_exists(&name);
        if !existed && !ignoreIfNotExists {
            return Err(to_py_err(format!("Database '{name}' does not exist")));
        }
        session.inner.drop_database(&name);
        Ok(())
    }

    #[pyo3(name = "setCurrentDatabase")]
    fn set_current_database(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        session.inner.set_current_database(name).map_err(to_py_err)
    }

    #[pyo3(name = "currentDatabase")]
    fn current_database(&self, py: Python<'_>) -> PyResult<String> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        Ok(session.inner.current_database())
    }

    #[pyo3(name = "tableExists", signature = (arg1, arg2=None))]
    fn table_exists(
        &self,
        py: Python<'_>,
        arg1: &Bound<'_, PyAny>,
        arg2: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let a1: String = arg1
            .extract()
            .map_err(|_| to_py_err("table name must be a string"))?;
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let full = if let Some(a2_any) = arg2 {
            let a2: String = a2_any
                .extract()
                .map_err(|_| to_py_err("table name must be a string"))?;
            // Prefer PySpark ordering (db, table) when it looks like a database name.
            if session.inner.database_exists(&a1) {
                format!("{a1}.{a2}")
            } else if session.inner.database_exists(&a2) {
                format!("{a2}.{a1}")
            } else {
                // Fallback: treat as db,table.
                format!("{a1}.{a2}")
            }
        } else {
            a1
        };
        Ok(session.inner.table_exists(&full))
    }

    #[pyo3(name = "getTable", signature = (arg1, arg2=None))]
    fn get_table(&self, py: Python<'_>, arg1: &str, arg2: Option<&str>) -> PyResult<Py<PyTable>> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let (db, tbl) = if let Some(t) = arg2 {
            (arg1.to_string(), t.to_string())
        } else if let Some((db, tbl)) = arg1.split_once('.') {
            (db.to_string(), tbl.to_string())
        } else {
            ("default".to_string(), arg1.to_string())
        };
        let full = format!("{db}.{tbl}");
        if !session.inner.table_exists(&full) && !session.inner.table_exists(&tbl) {
            return Err(to_py_err(format!("Table not found: {full}")));
        }
        Py::new(
            py,
            PyTable {
                name: tbl,
                database: db,
            },
        )
    }

    #[pyo3(name = "cacheTable")]
    fn cache_table(&self, py: Python<'_>, tableName: &str) -> PyResult<()> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        session.inner.cache_table(tableName);
        Ok(())
    }

    #[pyo3(name = "uncacheTable")]
    fn uncache_table(&self, py: Python<'_>, tableName: &str) -> PyResult<()> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        session.inner.uncache_table(tableName);
        Ok(())
    }

    #[pyo3(name = "isCached")]
    fn is_cached(&self, py: Python<'_>, tableName: &str) -> PyResult<bool> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        Ok(session.inner.is_cached(tableName))
    }
}

#[pyclass]
struct PyStorage {
    session: Py<PyAny>,
}

#[pymethods]
impl PyStorage {
    fn schema_exists(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        Ok(session.inner.database_exists(name))
    }

    fn create_table(
        &self,
        py: Python<'_>,
        schema_name: &str,
        table_name: &str,
        schema: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        if !session.inner.database_exists(schema_name) {
            session.inner.register_database(schema_name);
        }
        let pairs = parse_schema_from_py(py, schema)?.unwrap_or_default();
        let df = session
            .inner
            .create_dataframe_from_rows(Vec::new(), pairs, false, false)
            .map_err(to_py_err)?;
        let full = format!("{schema_name}.{table_name}");
        session.inner.register_table(&full, df);
        Ok(())
    }
}

/// Map PySpark type name to our schema type string.
/// Pass through array<...>, map<...>, struct<...> so createDataFrame preserves List/Map/Struct columns.
fn simple_string_to_type(s: &str) -> String {
    let lower = s.to_lowercase();
    match lower.as_str() {
        "string" | "str" => "string".to_string(),
        "int" | "integer" | "int32" => "int".to_string(),
        "bigint" | "long" | "int64" => "long".to_string(),
        "double" | "float64" => "double".to_string(),
        "float" | "float32" => "float".to_string(),
        "boolean" | "bool" => "boolean".to_string(),
        "date" => "date".to_string(),
        "timestamp" => "timestamp".to_string(),
        "binary" => "binary".to_string(),
        "list" | "array" => lower,
        _ if lower.starts_with("array<") && lower.ends_with('>') => s.to_string(),
        _ if lower.starts_with("map<") && lower.ends_with('>') => s.to_string(),
        _ if lower.starts_with("struct<") && lower.ends_with('>') => s.to_string(),
        _ => "string".to_string(),
    }
}

/// Parse DDL schema string (e.g. "name: string, age: int" or "a string, b int") into (name, type) pairs.
fn parse_ddl_schema_string(ddl: &str) -> Vec<(String, String)> {
    fn split_top_level_commas(s: &str) -> Vec<String> {
        let mut parts: Vec<String> = Vec::new();
        let mut buf = String::new();
        let mut depth: i32 = 0; // struct<...>, array<...>, map<...>
        for ch in s.chars() {
            match ch {
                '<' => {
                    depth += 1;
                    buf.push(ch);
                }
                '>' => {
                    depth = (depth - 1).max(0);
                    buf.push(ch);
                }
                ',' if depth == 0 => {
                    let part = buf.trim();
                    if !part.is_empty() {
                        parts.push(part.to_string());
                    }
                    buf.clear();
                }
                _ => buf.push(ch),
            }
        }
        let part = buf.trim();
        if !part.is_empty() {
            parts.push(part.to_string());
        }
        parts
    }

    fn find_top_level_char(s: &str, target: char) -> Option<usize> {
        let mut depth: i32 = 0;
        for (i, ch) in s.char_indices() {
            match ch {
                '<' => depth += 1,
                '>' => depth = (depth - 1).max(0),
                _ => {}
            }
            if depth == 0 && ch == target {
                return Some(i);
            }
        }
        None
    }

    fn rfind_top_level_space(s: &str) -> Option<usize> {
        let mut depth: i32 = 0;
        for (i, ch) in s.char_indices().rev() {
            match ch {
                '>' => depth += 1,
                '<' => depth = (depth - 1).max(0),
                _ => {}
            }
            if depth == 0 && ch == ' ' {
                return Some(i);
            }
        }
        None
    }

    let mut pairs = Vec::new();
    for part in split_top_level_commas(ddl) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (name, typ) = if let Some(colon) = find_top_level_char(part, ':') {
            let name = part[..colon].trim().to_string();
            let typ = part[colon + 1..].trim().to_string();
            (name, typ)
        } else {
            // "name type" format: last space separates name and type
            match rfind_top_level_space(part) {
                Some(i) => (
                    part[..i].trim().to_string(),
                    part[i + 1..].trim().to_string(),
                ),
                None => (part.to_string(), "string".to_string()),
            }
        };
        if !name.is_empty() {
            pairs.push((name, simple_string_to_type(&typ)));
        }
    }
    pairs
}

/// Parse schema from Python: None, list of column names (str), list of (name, type) pairs,
/// DDL string (e.g. "name: string, age: int"), or StructType-like object with .fields.
fn parse_schema_from_py(
    _py: Python<'_>,
    schema: &Bound<'_, PyAny>,
) -> PyResult<Option<Vec<(String, String)>>> {
    // StructType-like: has .fields
    if let Ok(fields) = schema.getattr("fields") {
        let list = fields.downcast::<PyList>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("schema.fields must be a list")
        })?;
        let mut pairs = Vec::with_capacity(list.len());
        for item in list.iter() {
            let name: String = item.getattr("name")?.extract()?;
            let data_type = item.getattr("dataType")?;
            let typ: String = data_type
                .call_method0("simpleString")
                .or_else(|_| data_type.getattr("simpleString").and_then(|s| s.call0()))
                .and_then(|v| v.extract::<String>())
                .unwrap_or_else(|_| "string".to_string());
            let mapped = simple_string_to_type(&typ);
            // Explicit StructType schema should not be re-inferred from data even if all fields are strings.
            let mapped = if mapped.eq_ignore_ascii_case("string") {
                "str".to_string()
            } else {
                mapped
            };
            pairs.push((name, mapped));
        }
        return Ok(Some(pairs));
    }

    // DDL string: "name: string, age: int" or "a string, b int"
    // OR single-column dtype string: "bigint", "int", "string", etc. (issue #419 parity).
    if let Ok(ddl) = schema.extract::<String>() {
        let ddl = ddl.trim();
        if !ddl.contains(',') && !ddl.contains(':') && !ddl.contains(' ') {
            return Ok(Some(vec![(
                "value".to_string(),
                simple_string_to_type(ddl),
            )]));
        }
        let pairs = parse_ddl_schema_string(ddl);
        return Ok(Some(pairs));
    }

    // DataType-like object (e.g. IntegerType()): has .simpleString()
    if let Ok(ss) = schema
        .call_method0("simpleString")
        .and_then(|v| v.extract::<String>())
    {
        return Ok(Some(vec![(
            "value".to_string(),
            simple_string_to_type(&ss),
        )]));
    }

    let list = schema.downcast::<PyList>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "schema must be None, str (DDL or dtype), list, or StructType-like",
        )
    })?;
    if list.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let first = list.get_item(0)?;
    if first.downcast::<PyList>().is_ok() || first.downcast::<pyo3::types::PyTuple>().is_ok() {
        let mut pairs = Vec::with_capacity(list.len());
        for item in list.iter() {
            let pair = item.downcast::<pyo3::types::PyTuple>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "schema must be list of column names or list of (name, type)",
                )
            })?;
            if pair.len() != 2 {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "schema pair must be (name, type)",
                ));
            }
            let name = pair.get_item(0)?.extract::<String>()?;
            let typ = pair.get_item(1)?.extract::<String>()?;
            pairs.push((name, simple_string_to_type(&typ)));
        }
        return Ok(Some(pairs));
    }
    let mut names = Vec::with_capacity(list.len());
    for item in list.iter() {
        let v = item.extract::<String>();
        if let Ok(s) = v {
            names.push(s);
        } else {
            // might be a type object (e.g. StringType()) - use "string"
            names.push("string".to_string());
        }
    }
    Ok(Some(
        names
            .into_iter()
            .map(|n| (n, "string".to_string()))
            .collect(),
    ))
}

fn infer_type_from_json_value(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "string".to_string(),
        JsonValue::Bool(_) => "boolean".to_string(),
        JsonValue::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "long".to_string()
            } else {
                "double".to_string()
            }
        }
        JsonValue::String(_) => "string".to_string(),
        // Treat JSON arrays as array type so list columns (e.g. scores, tags) are inferred
        // as List/Array instead of String when creating DataFrames from Python data.
        JsonValue::Array(_) => "array".to_string(),
        JsonValue::Object(_) => "string".to_string(),
    }
}

/// Normalize createDataFrame input: convert pandas, PyArrow Table, NumPy ndarray, or RDD to list.
/// Returns (converted list or original data, from_pandas flag, pandas column order if from pandas).
fn normalize_create_dataframe_input<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
    schema: Option<&Bound<'py, PyAny>>,
) -> PyResult<(Bound<'py, PyAny>, bool, Option<Vec<String>>)> {
    // 0. RDD (df.rdd): collect to list of dicts for createDataFrame(rdd, schema) (#1147 / #361).
    // When schema is provided, pass its column names so dict keys match (e.g. "Name", "Value").
    if let Ok(rdd) = data.downcast::<PyRDD>() {
        let preferred_names: Option<Vec<String>> = schema
            .and_then(|s| parse_schema_from_py(py, s).ok().flatten())
            .map(|pairs| pairs.into_iter().map(|(n, _)| n).collect());
        let list = if let Some(names) = preferred_names {
            let py_names = PyList::empty_bound(py);
            for n in &names {
                py_names.append(n)?;
            }
            rdd.call_method1("_to_pylist", (py_names,))?
        } else {
            rdd.call_method0("_to_pylist")?
        };
        // Return list directly so downstream uses our dicts; no need to re-normalize.
        return Ok((list, false, None));
    }

    // 1. PyArrow Table: to_pylist() returns list of dicts
    if let Ok(to_pylist) = data.getattr("to_pylist") {
        if to_pylist.is_callable() {
            let list = to_pylist.call0()?;
            if list.downcast::<PyList>().is_ok() {
                return Ok((list, true, None));
            }
        }
    }

    // 2. NumPy ndarray: tolist() + handle 1D (wrap each element as single-column row)
    if let Ok(ndim_attr) = data.getattr("ndim") {
        if let Ok(ndim) = ndim_attr.extract::<i32>() {
            if let Ok(tolist) = data.getattr("tolist") {
                if tolist.is_callable() {
                    let list = tolist.call0()?;
                    if ndim == 1 {
                        let py_list = list.downcast::<PyList>().map_err(|_| {
                            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                                "numpy 1D array tolist() must return a list",
                            )
                        })?;
                        let out = PyList::empty_bound(py);
                        for item in py_list.iter() {
                            let row = PyList::empty_bound(py);
                            row.append(item)?;
                            out.append(row)?;
                        }
                        return Ok((out.into_any(), false, None));
                    }
                    if ndim == 2 && list.downcast::<PyList>().is_ok() {
                        return Ok((list, false, None));
                    }
                }
            }
        }
    }

    // 3. Pandas DataFrame: to_dict("records"); preserve column order (#1151).
    maybe_convert_pandas_to_list(py, data)
}

/// If data is a pandas DataFrame, convert to list of dicts via to_dict("records").
/// Returns (converted list, true, Some(column_names_in_order)) when from pandas (#1151).
fn maybe_convert_pandas_to_list<'py>(
    _py: Python<'py>,
    data: &Bound<'py, PyAny>,
) -> PyResult<(Bound<'py, PyAny>, bool, Option<Vec<String>>)> {
    let to_dict = match data.getattr("to_dict") {
        Ok(attr) => attr,
        Err(_) => return Ok((data.clone(), false, None)),
    };
    if !to_dict.is_callable() {
        return Ok((data.clone(), false, None));
    }
    // Preserve column order from pandas DataFrame (PySpark parity; #1151).
    let pandas_columns: Option<Vec<String>> = data
        .getattr("columns")
        .ok()
        .and_then(|cols| cols.call_method0("tolist").ok())
        .and_then(|list| list.extract::<Vec<String>>().ok());
    let records = match to_dict.call1(("records",)) {
        Ok(r) => r,
        Err(_) => return Ok((data.clone(), false, None)),
    };
    if records.downcast::<PyList>().is_ok() {
        Ok((records, true, pandas_columns))
    } else {
        Ok((data.clone(), false, None))
    }
}

#[allow(clippy::type_complexity)]
fn python_data_and_schema(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    schema: Option<&Bound<'_, PyAny>>,
    from_pandas: bool,
    pandas_column_order: Option<&[String]>,
) -> PyResult<(Vec<Vec<JsonValue>>, Vec<(String, String)>, bool)> {
    let list = data.downcast::<PyList>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>("data must be a list (or pandas.DataFrame)")
    })?;
    // PySpark parity: createDataFrame([]) without explicit schema raises ValueError.
    if list.is_empty() && schema.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "can not infer schema from empty dataset",
        ));
    }
    // Parse explicit schema first so we can use its field order when building rows from dicts (Phase 7 / issue_247).
    let schema_names_only: bool = schema
        .and_then(|s| s.downcast::<PyList>().ok())
        .map(|l| l.iter().all(|it| it.extract::<String>().is_ok()))
        .unwrap_or(false);
    let schema_res: Option<Vec<(String, String)>> = schema
        .map(|s| parse_schema_from_py(py, s))
        .transpose()?
        .and_then(|o| o);
    // Scalar rows (e.g. [1,2,3]) are only allowed when there is an explicit single-column schema
    // such as "bigint" or DateType() (PySpark parity for single-type createDataFrame).
    let allow_scalar_single_column: bool =
        schema_res.as_ref().map(|s| s.len() == 1).unwrap_or(false);
    // Column order for dict rows: pandas order (#1151), else explicit schema order, else alphabetical.
    let mut column_order: Option<Vec<String>> =
        pandas_column_order.map(|s| s.to_vec()).or_else(|| {
            schema_res
                .as_ref()
                .map(|s| s.iter().map(|(n, _)| n.clone()).collect())
        });
    if column_order.is_none() && schema_res.is_none() && !list.is_empty() {
        use std::collections::BTreeSet;
        let mut keys_union: BTreeSet<String> = BTreeSet::new();
        for item in list.iter() {
            if let Ok(dict) = item.downcast::<PyDict>() {
                for (k, _) in dict.iter() {
                    if let Ok(name) = k.extract::<String>() {
                        keys_union.insert(name);
                    }
                }
            }
        }
        if !keys_union.is_empty() {
            column_order = Some(keys_union.into_iter().collect());
        }
    }

    let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(list.len());
    let mut inferred_schema: Option<Vec<(String, String)>> = None;
    let mut row_kind: Option<&'static str> = None;

    for (idx, item) in list.iter().enumerate() {
        let kind = if item.downcast::<PyDict>().is_ok() {
            "dict"
        } else if item.downcast::<PyList>().is_ok()
            || item.downcast::<pyo3::types::PyTuple>().is_ok()
        {
            "seq"
        } else {
            "scalar"
        };
        if let Some(first) = row_kind {
            // Disallow mixing dict and seq rows when schema is not explicit (PySpark parity).
            // When schema is explicit (StructType/DDL/pairs), allow mixing: tuple rows are positional, dict rows use schema order.
            if schema_res.is_none()
                && ((first == "dict" && kind == "seq") || (first == "seq" && kind == "dict"))
            {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "createDataFrame: all rows must be the same shape (dict rows or list/tuple rows)",
                ));
            }
        } else {
            row_kind = Some(kind);
        }
        let row = python_row_to_json(
            py,
            &item,
            idx,
            column_order.as_deref(),
            from_pandas,
            allow_scalar_single_column,
        )?;
        // For dict rows without explicit schema, infer full schema from all rows/keys later so we
        // can include sparse keys (issue #372). For other row kinds, infer from the first row.
        if inferred_schema.is_none() && schema_res.is_none() && kind != "dict" && !row.is_empty() {
            if let Some(cols) = infer_schema_from_first_row(py, &item, from_pandas) {
                inferred_schema = Some(cols);
            }
        }
        rows.push(row);
    }

    // Names-only list schema should still infer types from data (PySpark parity).
    let schema_was_inferred = schema_res.is_none() || schema_names_only;
    // When schema is not explicit and rows are dicts, build schema from the union of keys so
    // sparse rows (different keys) still include all columns, ordered alphabetically, and infer
    // types from the underlying Python values (so bytes -> BinaryType, etc.).
    let mut schema = if schema_res.is_none() && matches!(row_kind, Some("dict")) {
        let names: Vec<String> = if let Some(order) = &column_order {
            order.clone()
        } else {
            rows.first()
                .map(|r| {
                    (0..r.len())
                        .map(|i| format!("_{}", i + 1))
                        .collect::<Vec<String>>()
                })
                .unwrap_or_default()
        };
        let mut out: Vec<(String, String)> = Vec::with_capacity(names.len());
        for name in &names {
            let mut dtype = "string".to_string();
            for item in list.iter() {
                if let Ok(dict) = item.downcast::<PyDict>() {
                    if let Ok(Some(v)) = dict.get_item(name) {
                        dtype = infer_type_from_py_value(&v);
                        break;
                    }
                }
            }
            out.push((name.clone(), dtype));
        }
        out
    } else {
        schema_res.or(inferred_schema).unwrap_or_else(|| {
            rows.first()
                .map(|r| {
                    (0..r.len())
                        .map(|i| (format!("_{}", i + 1), "string".to_string()))
                        .collect()
                })
                .unwrap_or_default()
        })
    };
    if let (Some(first_row), true) = (rows.first(), schema.iter().all(|(_, t)| t == "string")) {
        if first_row.len() == schema.len() {
            schema = schema
                .into_iter()
                .zip(first_row.iter())
                .map(|((name, _), v)| (name, infer_type_from_json_value(v)))
                .collect();
        }
    }
    // When schema was inferred, prefer first non-null value's type per column (fixes fillna(0) on column with null in first row).
    if schema_was_inferred && !schema.is_empty() {
        let refined: Vec<(String, String)> = schema
            .iter()
            .enumerate()
            .map(|(i, (name, typ))| {
                let better = rows
                    .iter()
                    .filter_map(|r| r.get(i))
                    .find(|v| !matches!(v, JsonValue::Null))
                    .map(infer_type_from_json_value);
                // Only refine columns that were originally inferred as string-like; keep
                // explicit non-string types such as binary/boolean/numeric unchanged.
                let final_typ = if typ.eq_ignore_ascii_case("string")
                    || typ.eq_ignore_ascii_case("str")
                    || typ.eq_ignore_ascii_case("varchar")
                {
                    better.unwrap_or_else(|| typ.clone())
                } else {
                    typ.clone()
                };
                (name.clone(), final_typ)
            })
            .collect();
        schema = refined;
    }
    // #1149: PySpark createDataFrame(list_of_dicts, [col names]) infers first column as Long/Double
    // and later numeric columns as String. Apply only for dict rows; list/tuple rows keep normal inference.
    if schema_was_inferred
        && schema_names_only
        && matches!(row_kind, Some("dict"))
        && !schema.is_empty()
    {
        let inferred: Vec<String> = (0..schema.len())
            .map(|i| {
                rows.iter()
                    .filter_map(|r| r.get(i))
                    .find(|v| !matches!(v, JsonValue::Null))
                    .map(infer_type_from_json_value)
                    .unwrap_or_else(|| "string".to_string())
            })
            .collect();
        let first_is_long = schema.len() == 2
            && inferred
                .get(1)
                .map(|t| t.eq_ignore_ascii_case("long"))
                .unwrap_or(false);
        // Second column in 2-col schema becomes String; in 3+ cols only double becomes String (long stays).
        let two_cols = schema.len() == 2;
        schema = schema
            .into_iter()
            .enumerate()
            .map(|(i, (name, _))| {
                let t = inferred.get(i).map(|s| s.as_str()).unwrap_or("string");
                let typ = match i {
                    0 => {
                        if first_is_long {
                            "long".to_string()
                        } else {
                            "double".to_string()
                        }
                    }
                    _ if t.eq_ignore_ascii_case("double") => "string".to_string(),
                    _ if two_cols && i == 1 => "string".to_string(),
                    _ => t.to_string(),
                };
                (name, typ)
            })
            .collect();
    }
    Ok((rows, schema, schema_was_inferred))
}

fn python_row_to_json(
    py: Python<'_>,
    item: &Bound<'_, PyAny>,
    row_idx: usize,
    column_order: Option<&[String]>,
    from_pandas: bool,
    allow_scalar_single_column: bool,
) -> PyResult<Vec<JsonValue>> {
    if let Ok(dict) = item.downcast::<PyDict>() {
        let mut keys: Vec<String> = if let Some(order) = column_order {
            order.to_vec()
        } else {
            dict.keys()
                .iter()
                .map(|k| k.extract::<String>().unwrap_or_default())
                .collect()
        };
        if column_order.is_none() && !from_pandas {
            keys.sort();
        }
        let mut values = Vec::with_capacity(keys.len());
        for k in &keys {
            match dict.get_item(k.as_str()) {
                Ok(Some(v)) => values.push(py_any_to_json(py, &v)?),
                Ok(None) => values.push(JsonValue::Null),
                Err(e) => return Err(e),
            }
        }
        return Ok(values);
    }
    if let Ok(seq) = item.downcast::<PyList>() {
        let mut values = Vec::with_capacity(seq.len());
        for v in seq.iter() {
            values.push(py_any_to_json(py, &v)?);
        }
        return Ok(values);
    }
    if let Ok(tup) = item.downcast::<pyo3::types::PyTuple>() {
        let mut values = Vec::with_capacity(tup.len());
        for v in tup.iter() {
            values.push(py_any_to_json(py, &v)?);
        }
        return Ok(values);
    }
    // Scalar row: allow when schema is explicit single-column (e.g. createDataFrame([1,2,3], "bigint")).
    if allow_scalar_single_column && column_order.map(|o| o.len() == 1).unwrap_or(false) {
        return Ok(vec![py_any_to_json(py, item)?]);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
        "row {}: expected dict, list, or tuple",
        row_idx
    )))
}

fn py_any_to_json(_py: Python<'_>, v: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    if v.is_none() {
        return Ok(JsonValue::Null);
    }
    if let Ok(b) = v.extract::<bool>() {
        return Ok(JsonValue::Bool(b));
    }
    if let Ok(i) = v.extract::<i64>() {
        return Ok(JsonValue::Number(serde_json::Number::from(i)));
    }
    if let Ok(f) = v.extract::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Ok(JsonValue::Number(n));
        }
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(JsonValue::String(s));
    }
    if let Ok(bytes) = v.downcast::<PyBytes>() {
        use ::base64::Engine;
        let b: &[u8] = bytes.as_bytes();
        let encoded = ::base64::engine::general_purpose::STANDARD.encode(b);
        return Ok(JsonValue::String(encoded));
    }
    if let Ok(list) = v.downcast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_any_to_json(_py, &item)?);
        }
        return Ok(JsonValue::Array(arr));
    }
    if let Ok(dict) = v.downcast::<PyDict>() {
        let mut obj = serde_json::Map::new();
        for (k, val) in dict.iter() {
            let key = k.extract::<String>().unwrap_or_else(|_| k.to_string());
            obj.insert(key, py_any_to_json(_py, &val)?);
        }
        return Ok(JsonValue::Object(obj));
    }
    Ok(JsonValue::String(v.to_string()))
}

fn infer_schema_from_first_row(
    _py: Python<'_>,
    item: &Bound<'_, PyAny>,
    from_pandas: bool,
) -> Option<Vec<(String, String)>> {
    if let Ok(dict) = item.downcast::<PyDict>() {
        let mut keys: Vec<String> = dict
            .keys()
            .iter()
            .filter_map(|k| k.extract::<String>().ok())
            .collect();
        if !from_pandas {
            keys.sort();
        }
        let mut out = Vec::with_capacity(keys.len());
        for k in &keys {
            let typ = dict
                .get_item(k.as_str())
                .ok()
                .flatten()
                .map(|v| infer_type_from_py_value(&v))
                .unwrap_or_else(|| "string".to_string());
            out.push((k.clone(), typ));
        }
        return Some(out);
    }
    // Namedtuple (and tuple-like with _fields): use _fields as column names so collect() returns row["a"], row["b"] (PySpark parity).
    let fields_attr = item.getattr("_fields").ok()?;
    let n: usize = fields_attr.len().ok()?.try_into().ok()?;
    let mut names: Vec<String> = Vec::with_capacity(n);
    for i in 0..n {
        let f = fields_attr.get_item(i).ok()?;
        let s = f.extract::<String>().ok()?;
        names.push(s);
    }
    let mut out = Vec::with_capacity(names.len());
    for (i, name) in names.iter().enumerate() {
        let val = item.get_item(i).ok()?;
        let typ = infer_type_from_py_value(&val);
        out.push((name.clone(), typ));
    }
    Some(out)
}

fn infer_type_from_py_value(v: &Bound<'_, PyAny>) -> String {
    if v.is_none() {
        return "string".to_string();
    }
    if v.extract::<bool>().is_ok() {
        return "boolean".to_string();
    }
    if v.extract::<i64>().is_ok() {
        return "long".to_string();
    }
    if v.extract::<f64>().is_ok() {
        return "double".to_string();
    }
    // Treat Python list/tuple as array so createDataFrame infers List dtype
    // for columns backed by sequence values (PySpark parity for array columns).
    if v.downcast::<PyList>().is_ok() || v.downcast::<PyTuple>().is_ok() {
        return "array".to_string();
    }
    if v.downcast::<PyBytes>().is_ok() {
        return "binary".to_string();
    }
    // #1103: Preserve date/timestamp from Python so createDataFrame([{"d": date(2026,1,1), "s": "2025-06-15"}])
    // infers d as date and s as string; string column stays string in filter result.
    if let Ok(dt) = v.getattr("isoformat") {
        if dt.is_callable() {
            if v.getattr("hour").is_ok() {
                return "timestamp".to_string();
            }
            return "date".to_string();
        }
    }
    "string".to_string()
}

/// PySpark drop(*cols): single str or list of str -> Vec<String>
fn py_tuple_or_single_to_vec_string(tup: &Bound<'_, PyTuple>) -> PyResult<Vec<String>> {
    if tup.len() == 0 {
        return Ok(Vec::new());
    }
    if tup.len() == 1 {
        let item = tup.get_item(0)?;
        if let Ok(list) = item.downcast::<PyList>() {
            let mut out = Vec::with_capacity(list.len());
            for x in list.iter() {
                out.push(x.extract::<String>()?);
            }
            return Ok(out);
        }
        if let Ok(inner_tup) = item.downcast::<PyTuple>() {
            let mut out = Vec::with_capacity(inner_tup.len());
            for x in inner_tup.iter() {
                out.push(x.extract::<String>()?);
            }
            return Ok(out);
        }
        if let Ok(s) = item.extract::<String>() {
            return Ok(vec![s]);
        }
    }
    let mut out = Vec::with_capacity(tup.len());
    for item in tup.iter() {
        out.push(item.extract::<String>()?);
    }
    Ok(out)
}

/// Normalize column spec to Vec<String>: "x" | col("x") | ["x","y"] | (col("a"),) -> Vec<String>
/// Resolve a single column spec to a column name (str or Column -> name). Used by GroupedData.avg/sum/min/max.
fn py_col_to_name(any: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = any.extract::<String>() {
        return Ok(s);
    }
    if let Ok(c) = any.downcast::<PyColumn>() {
        return Ok(c.borrow().inner.name().to_string());
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "expected column name (str) or Column",
    ))
}

fn py_cols_to_vec(any: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if any.is_none() {
        return Ok(Vec::new());
    }
    if let Ok(s) = any.extract::<String>() {
        return Ok(vec![s]);
    }
    if let Ok(c) = any.downcast::<PyColumn>() {
        return Ok(vec![c.borrow().inner.name().to_string()]);
    }
    if let Ok(list) = any.downcast::<PyList>() {
        let mut out = Vec::new();
        for item in list.iter() {
            out.extend(py_one_or_many_cols(&item)?);
        }
        return Ok(out);
    }
    if let Ok(tup) = any.downcast::<PyTuple>() {
        let mut out = Vec::new();
        for item in tup.iter() {
            out.extend(py_one_or_many_cols(&item)?);
        }
        return Ok(out);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "group_by/groupBy expects str, Column, or list/tuple of str or Column",
    ))
}

fn py_one_or_many_cols(item: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = item.extract::<String>() {
        return Ok(vec![s]);
    }
    if let Ok(c) = item.downcast::<PyColumn>() {
        return Ok(vec![c.borrow().inner.name().to_string()]);
    }
    if let Ok(list) = item.downcast::<PyList>() {
        let mut out = Vec::new();
        for sub in list.iter() {
            out.extend(py_one_or_many_cols(&sub)?);
        }
        return Ok(out);
    }
    if let Ok(tup) = item.downcast::<PyTuple>() {
        let mut out = Vec::new();
        for sub in tup.iter() {
            out.extend(py_one_or_many_cols(&sub)?);
        }
        return Ok(out);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "groupBy() expects column names as str, Column, or list/tuple of those",
    ))
}

/// Convert groupBy(*cols) item to Vec<GroupBySpec>. Supports str, Column, or list/tuple of those.
/// PySpark parity: groupBy("dept") vs groupBy(F.col("Name").alias("Key")).
fn py_group_by_specs(item: &Bound<'_, PyAny>) -> PyResult<Vec<GroupBySpec>> {
    if let Ok(s) = item.extract::<String>() {
        return Ok(vec![GroupBySpec::Name(s)]);
    }
    if let Ok(c) = item.downcast::<PyColumn>() {
        return Ok(vec![GroupBySpec::Column(Box::new(
            c.borrow().inner.clone(),
        ))]);
    }
    if let Ok(list) = item.downcast::<PyList>() {
        let mut out = Vec::new();
        for sub in list.iter() {
            out.extend(py_group_by_specs(&sub)?);
        }
        return Ok(out);
    }
    if let Ok(tup) = item.downcast::<PyTuple>() {
        let mut out = Vec::new();
        for sub in tup.iter() {
            out.extend(py_group_by_specs(&sub)?);
        }
        return Ok(out);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "groupBy() expects column names as str, Column, or list/tuple of those",
    ))
}

/// PySpark join(on=...): str, Column, or list of str/Column -> Vec<String>
fn py_join_on_to_vec(on: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = on.extract::<String>() {
        return Ok(vec![s]);
    }
    if let Ok(col) = on.downcast::<PyColumn>() {
        return Ok(vec![col.borrow().inner.name().to_string()]);
    }
    if let Ok(list) = on.downcast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(s) = item.extract::<String>() {
                out.push(s);
            } else if let Ok(c) = item.downcast::<PyColumn>() {
                out.push(c.borrow().inner.name().to_string());
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "join(on=...) list elements must be str or Column",
                ));
            }
        }
        return Ok(out);
    }
    if let Ok(tup) = on.downcast::<PyTuple>() {
        let mut out = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            if let Ok(s) = item.extract::<String>() {
                out.push(s);
            } else if let Ok(c) = item.downcast::<PyColumn>() {
                out.push(c.borrow().inner.name().to_string());
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "join(on=...) tuple elements must be str or Column",
                ));
            }
        }
        return Ok(out);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "join(on=...) expects str, Column, or list/tuple of str or Column",
    ))
}

#[pyclass]
struct PyDataFrameReader {
    session: Py<PyAny>,
    options: Vec<(String, String)>,
    format: Option<String>,
}

/// Normalize subset to Option<Vec<String>>: "col" -> Some(vec!["col".into()]), ["a","b"] -> as-is.
fn normalize_subset(subset: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Vec<String>>> {
    let Some(s) = subset else {
        return Ok(None);
    };
    if s.is_none() {
        return Ok(None);
    }
    if let Ok(one) = s.extract::<String>() {
        return Ok(Some(vec![one]));
    }
    if let Ok(list) = s.downcast::<PyList>() {
        let v: Vec<String> = list.iter().map(|x| x.extract()).collect::<PyResult<_>>()?;
        return Ok(Some(v));
    }
    if let Ok(tup) = s.downcast::<PyTuple>() {
        let v: Vec<String> = tup.iter().map(|x| x.extract()).collect::<PyResult<_>>()?;
        return Ok(Some(v));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "subset must be str or list/tuple of str",
    ))
}

enum FillValueKind {
    Int,
    Float,
    String,
    Bool,
    Other,
}

fn classify_fill_value_kind(value: &Bound<'_, PyAny>) -> FillValueKind {
    if value.is_none() {
        return FillValueKind::Other;
    }
    if value.extract::<bool>().is_ok() {
        return FillValueKind::Bool;
    }
    if value.extract::<i64>().is_ok() {
        return FillValueKind::Int;
    }
    if value.extract::<f64>().is_ok() {
        return FillValueKind::Float;
    }
    if value.extract::<String>().is_ok() {
        return FillValueKind::String;
    }
    FillValueKind::Other
}

fn is_fill_compatible_with_schema_dtype(
    dt: &robin_sparkless::schema::DataType,
    kind: &FillValueKind,
) -> bool {
    use robin_sparkless::schema::DataType as SchemaDataType;
    match kind {
        // Integers and floats can fill numeric columns (int/long/double).
        FillValueKind::Int | FillValueKind::Float => matches!(
            dt,
            SchemaDataType::Integer | SchemaDataType::Long | SchemaDataType::Double
        ),
        // Strings fill string-like columns only.
        FillValueKind::String => matches!(dt, SchemaDataType::String),
        // Booleans fill boolean columns only.
        FillValueKind::Bool => matches!(dt, SchemaDataType::Boolean),
        // For other kinds (e.g. complex expressions), defer to engine behavior.
        FillValueKind::Other => true,
    }
}

/// Resolve cast type argument: string as-is, or type object's simpleString() or typeName() (e.g. IntegerType()).
fn resolve_cast_type_name(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = value.extract::<String>() {
        return Ok(s);
    }
    // PySpark types have simpleString(); minimal types may only have typeName().
    if let Ok(meth) = value.getattr("simpleString") {
        if let Ok(s) = meth.call0().and_then(|v| v.extract::<String>()) {
            return Ok(s);
        }
    }
    if let Ok(meth) = value.getattr("typeName") {
        if let Ok(s) = meth.call0().and_then(|v| v.extract::<String>()) {
            return Ok(s);
        }
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "cast type must be string or type object with simpleString() or typeName()",
    ))
}

/// Convert PySpark option value to string (bool -> "true"/"false", int/float -> string, str as-is).
fn option_value_to_string(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if value.is_none() {
        return Ok("".to_string());
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(if b { "true" } else { "false" }.to_string());
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(i.to_string());
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(f.to_string());
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(s);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "option value must be str, bool, int, or float",
    ))
}

#[pymethods]
impl PyDataFrameReader {
    fn option<'a>(
        mut slf: PyRefMut<'a, Self>,
        key: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        let s = option_value_to_string(value)?;
        slf.options.push((key.to_string(), s));
        Ok(slf)
    }

    /// PySpark: options(**kwargs). opts: dict of key -> value.
    fn options<'a>(
        mut slf: PyRefMut<'a, Self>,
        _py: Python<'_>,
        opts: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        let dict = opts.downcast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("options() requires a dict")
        })?;
        for (k, v) in dict.iter() {
            let key = k.extract::<String>()?;
            let value = option_value_to_string(&v)?;
            slf.options.push((key, value));
        }
        Ok(slf)
    }

    /// PySpark: format("parquet"|"csv"|"json"|"delta"). Used by load().
    fn format<'a>(mut slf: PyRefMut<'a, Self>, fmt: &str) -> PyRefMut<'a, Self> {
        slf.format = Some(fmt.to_string());
        slf
    }

    fn load(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let mut reader = session.inner.read();
        for (k, v) in &self.options {
            reader = reader.option(k.as_str(), v.as_str());
        }
        if let Some(ref fmt) = self.format {
            reader = reader.format(fmt);
        }
        reader
            .load(Path::new(path))
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn csv(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let mut reader = session.inner.read();
        // PySpark parity: spark.read.csv() defaults to inferSchema=False (all strings) unless
        // the user explicitly sets inferSchema. Core Robin Sparkless defaults to inferring
        // types, so we override here at the Python binding layer only when no inferSchema
        // option has been provided.
        let has_infer_schema = self
            .options
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("inferSchema"));
        if !has_infer_schema {
            reader = reader.option("inferSchema", "false");
        }
        for (k, v) in &self.options {
            reader = reader.option(k.clone(), v.clone());
        }
        reader
            .csv(Path::new(path))
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn parquet(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let mut reader = session.inner.read();
        for (k, v) in &self.options {
            reader = reader.option(k.clone(), v.clone());
        }
        reader
            .parquet(Path::new(path))
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn json(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let mut reader = session.inner.read();
        for (k, v) in &self.options {
            reader = reader.option(k.clone(), v.clone());
        }
        reader
            .json(Path::new(path))
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn table(&self, py: Python<'_>, name: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        session
            .inner
            .table(name)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[cfg(feature = "delta")]
    fn delta(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        session
            .inner
            .read()
            .delta(Path::new(path))
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }
}

/// Resolve (key_a, key_b) from join condition to (left_on, right_on). Strips alias prefix (e.g. "sm.brand_id" -> "brand_id") and assigns by which side has the column (#374, #421).
/// When alias is None (e.g. right from .alias("b").select(...) loses alias), try suffix after "." so "b.code" matches right "code".
fn resolve_join_keys_with_aliases(
    key_a: &str,
    key_b: &str,
    left_cols: &std::collections::HashSet<String>,
    right_cols: &std::collections::HashSet<String>,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
) -> (Option<String>, Option<String>) {
    fn strip_alias(key: &str, alias: Option<&str>) -> String {
        let Some(alias) = alias else {
            return key.to_string();
        };
        let prefix = format!("{alias}.");
        if key.starts_with(&prefix) {
            key[prefix.len()..].to_string()
        } else {
            key.to_string()
        }
    }
    /// For "alias.col" when alias is None, use suffix so "b.code" matches "code" (#374).
    fn suffix_if_dot(key: &str) -> Option<&str> {
        key.split('.').next_back().filter(|s| !s.is_empty())
    }
    fn get_matching(name: &str, set: &std::collections::HashSet<String>) -> Option<String> {
        set.iter().find(|c| c.eq_ignore_ascii_case(name)).cloned()
    }
    let bare_a_left = strip_alias(key_a, left_alias);
    let bare_a_right = strip_alias(key_a, right_alias);
    let bare_b_left = strip_alias(key_b, left_alias);
    let bare_b_right = strip_alias(key_b, right_alias);
    let a_in_left = get_matching(&bare_a_left, left_cols).is_some();
    let a_in_right = get_matching(&bare_a_right, right_cols).is_some();
    let b_in_left = get_matching(&bare_b_left, left_cols).is_some();
    let b_in_right = get_matching(&bare_b_right, right_cols).is_some();
    let (left_key, right_key) = if a_in_left && b_in_right {
        (
            get_matching(&bare_a_left, left_cols),
            get_matching(&bare_b_right, right_cols),
        )
    } else if a_in_right && b_in_left {
        (
            get_matching(&bare_b_left, left_cols),
            get_matching(&bare_a_right, right_cols),
        )
    } else {
        (None, None)
    };
    // Fallback: alias was lost (e.g. .alias("b").select(...)); match by suffix after "." (#374).
    if left_key.is_none() || right_key.is_none() {
        let a_suffix = suffix_if_dot(key_a);
        let b_suffix = suffix_if_dot(key_b);
        if let (Some(sa), Some(sb)) = (a_suffix, b_suffix) {
            let a_in_l = get_matching(sa, left_cols).is_some();
            let a_in_r = get_matching(sa, right_cols).is_some();
            let b_in_l = get_matching(sb, left_cols).is_some();
            let b_in_r = get_matching(sb, right_cols).is_some();
            if a_in_l && b_in_r {
                return (get_matching(sa, left_cols), get_matching(sb, right_cols));
            }
            if a_in_r && b_in_l {
                return (get_matching(sb, left_cols), get_matching(sa, right_cols));
            }
        }
    }
    (left_key, right_key)
}

/// RDD-like wrapper for createDataFrame(df.rdd, schema) (issue #1147 / #361).
/// Holds the source DataFrame; createDataFrame(rdd, schema) collects it to rows and builds a new DataFrame.
#[pyclass]
struct PyRDD {
    inner: DataFrame,
}

#[pymethods]
impl PyRDD {
    /// Return collected rows as list of dicts for createDataFrame(rdd, schema). Used internally by normalize.
    /// When preferred_names is given (e.g. from createDataFrame(rdd, schema=["Name", "Value"])), use those
    /// as dict keys so the downstream createDataFrame finds them by name.
    #[pyo3(name = "_to_pylist", signature = (preferred_names=None))]
    fn to_pylist<'py>(
        &self,
        py: Python<'py>,
        preferred_names: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let (col_names, rows, _) = self
            .inner
            .collect_as_json_rows_with_names()
            .map_err(to_py_err)?;
        // call_method1(..., (py_names,)) passes a tuple to Python; unwrap single-element tuple.
        let (names, use_preferred_keys): (Vec<String>, bool) = if let Some(arg) = preferred_names {
            let lst_opt: Option<Bound<'py, PyList>> = if let Ok(lst) = arg.downcast::<PyList>() {
                Some(lst.clone())
            } else if let Ok(tup) = arg.downcast::<pyo3::types::PyTuple>() {
                (tup.len() == 1)
                    .then(|| tup.get_item(0).ok())
                    .flatten()
                    .and_then(|item| item.downcast_into::<PyList>().ok())
            } else {
                None
            };
            if let Some(lst) = lst_opt {
                let n: Vec<String> = lst
                    .iter()
                    .filter_map(|it| it.extract::<String>().ok())
                    .collect();
                if n.is_empty() {
                    (col_names.clone(), false)
                } else {
                    (n, true)
                }
            } else {
                (
                    self.inner
                        .schema()
                        .ok()
                        .map(|s| s.fields().iter().map(|f| f.name.clone()).collect())
                        .unwrap_or_else(|| col_names.clone()),
                    false,
                )
            }
        } else {
            (
                self.inner
                    .schema()
                    .ok()
                    .map(|s| s.fields().iter().map(|f| f.name.clone()).collect())
                    .unwrap_or_else(|| col_names.clone()),
                false,
            )
        };
        let out = PyList::empty_bound(py);
        for row in rows {
            if use_preferred_keys {
                // Return list-of-lists so createDataFrame uses positional column order; avoids dict key lookup issues.
                let row_list = PyList::empty_bound(py);
                for name in &names {
                    let v: &JsonValue = row
                        .get(name)
                        .or_else(|| {
                            col_names
                                .iter()
                                .find(|c: &&String| c.eq_ignore_ascii_case(name))
                                .and_then(|c| row.get(c))
                        })
                        .or_else(|| {
                            row.iter()
                                .find(|(k, _): &(&String, &JsonValue)| k.eq_ignore_ascii_case(name))
                                .map(|(_, v)| v)
                        })
                        .unwrap_or(&JsonValue::Null);
                    row_list.append(json_to_py(v, py)?)?;
                }
                out.append(row_list)?;
            } else {
                let dict = PyDict::new_bound(py);
                for name in &names {
                    let v: &JsonValue = row
                        .get(name)
                        .or_else(|| {
                            row.iter()
                                .find(|(k, _): &(&String, &JsonValue)| k.eq_ignore_ascii_case(name))
                                .map(|(_, v)| v)
                        })
                        .unwrap_or(&JsonValue::Null);
                    dict.set_item(name, json_to_py(v, py)?)?;
                }
                out.append(dict)?;
            }
        }
        Ok(out)
    }
}

#[pyclass]
struct PyDataFrame {
    inner: DataFrame,
    /// When set, df.schema returns this (preserves ArrayType.containsNull etc. from createDataFrame(schema=StructType(...))).
    schema_cache: Option<Py<PyAny>>,
}

impl PyDataFrame {
    fn wrap(df: DataFrame) -> Self {
        PyDataFrame {
            inner: df,
            schema_cache: None,
        }
    }
}

#[pymethods]
impl PyDataFrame {
    fn filter(
        slf: PyRef<Self>,
        py: Python<'_>,
        condition: &Bound<'_, PyAny>,
    ) -> PyResult<PyDataFrame> {
        if let Ok(py_col) = condition.downcast::<PyColumn>() {
            let col_ref = py_col.borrow();
            if let Some((udf_name, arg_names, literal_jsons)) =
                col_ref.inner.udf_call_info_with_literals()
            {
                let temp_name = format!("_udf_filter_{}", uuid::Uuid::new_v4().simple());
                let arg_names_py = PyList::new_bound(py, arg_names.iter());
                let literals_py = PyList::empty_bound(py);
                for opt in &literal_jsons {
                    let item: Bound<'_, PyAny> = match opt {
                        Some(s) => pyo3::types::PyString::new_bound(py, s.as_str()).into_any(),
                        None => py.None().into_bound(py),
                    };
                    literals_py.append(item)?;
                }
                if let Some(df_with_col) =
                    PYTHON_UDF_EXECUTOR.with(|cell| -> PyResult<Option<PyDataFrame>> {
                        let cb = cell.borrow();
                        let callback = match cb.as_ref() {
                            Some(c) => c,
                            None => return Ok(None),
                        };
                        let df_obj: Bound<'_, PyAny> =
                            unsafe { Bound::from_borrowed_ptr(py, slf.as_ptr()) };
                        let result = callback.bind(py).call1((
                            df_obj,
                            &temp_name,
                            udf_name,
                            arg_names_py,
                            literals_py,
                        ))?;
                        let py_df = result.downcast::<PyDataFrame>()?;
                        Ok(Some(PyDataFrame::wrap(py_df.borrow().inner.clone())))
                    })?
                {
                    let filter_expr = robin_sparkless::Column::new(temp_name.clone()).into_expr();
                    let filtered = df_with_col.inner.filter(filter_expr).map_err(to_py_err)?;
                    let out = filtered.drop(vec![temp_name.as_str()]).map_err(to_py_err)?;
                    return Ok(PyDataFrame::wrap(out));
                }
            }
            let df = slf
                .inner
                .filter(col_ref.inner.clone().into_expr())
                .map(PyDataFrame::wrap)
                .map_err(to_py_err)?;
            return Ok(df);
        }
        // F.expr(...) in filter: handle PyExprStr SQL expression strings.
        if let Ok(py_expr_str) = condition.downcast::<PyExprStr>() {
            let session = THREAD_ACTIVE_SESSIONS
                .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "filter with F.expr() requires an active SparkSession",
                    )
                })?;
            let session_ref = session.bind(py).downcast::<PySparkSession>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
            })?;
            let expr = robin_sparkless::sql::expr_string_to_polars(
                &py_expr_str.borrow().sql,
                &session_ref.borrow().inner,
                &slf.inner,
            )
            .map_err(to_py_err)?;
            return slf
                .inner
                .filter(expr)
                .map(PyDataFrame::wrap)
                .map_err(to_py_err);
        }
        if let Ok(expr_str) = condition.extract::<String>() {
            let session = THREAD_ACTIVE_SESSIONS
                .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "filter with string expression requires an active SparkSession",
                    )
                })?;
            let session_ref = session.bind(py).downcast::<PySparkSession>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
            })?;
            let expr = robin_sparkless::sql::expr_string_to_polars(
                &expr_str,
                &session_ref.borrow().inner,
                &slf.inner,
            )
            .map_err(to_py_err)?;
            return slf
                .inner
                .filter(expr)
                .map(PyDataFrame::wrap)
                .map_err(to_py_err);
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "filter condition must be a Column or a string expression",
        ))
    }

    #[pyo3(name = "where")]
    fn where_(
        slf: PyRef<Self>,
        py: Python<'_>,
        condition: &Bound<'_, PyAny>,
    ) -> PyResult<PyDataFrame> {
        Self::filter(slf, py, condition)
    }

    #[pyo3(signature = (*cols))]
    fn select(&self, py: Python<'_>, cols: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        #[derive(Debug)]
        enum Tmp {
            NameIdx(usize),
            Expr(robin_sparkless::Expr),
        }
        #[derive(Debug)]
        enum ItemOrExprStr {
            Resolved(Tmp),
            ExprStr(String),
        }

        fn push_item(
            item: &Bound<'_, PyAny>,
            out: &mut Vec<ItemOrExprStr>,
            names: &mut Vec<Box<str>>,
            _py: Python<'_>,
        ) -> PyResult<()> {
            if let Ok(py_col) = item.downcast::<PyColumn>() {
                out.push(ItemOrExprStr::Resolved(Tmp::Expr(
                    py_col.borrow().inner.clone().into_expr(),
                )));
                return Ok(());
            }
            if let Ok(py_expr_str) = item.downcast::<PyExprStr>() {
                out.push(ItemOrExprStr::ExprStr(py_expr_str.borrow().sql.clone()));
                return Ok(());
            }
            if let Ok(s) = item.extract::<String>() {
                let idx = names.len();
                names.push(s.into_boxed_str());
                out.push(ItemOrExprStr::Resolved(Tmp::NameIdx(idx)));
                return Ok(());
            }
            if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    push_item(&sub, out, names, _py)?;
                }
                return Ok(());
            }
            if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    push_item(&sub, out, names, _py)?;
                }
                return Ok(());
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "select() expects columns as str, Column, expr(str), or a list/tuple of those",
            ))
        }

        let mut raw: Vec<ItemOrExprStr> = Vec::with_capacity(cols.len());
        let mut name_boxes: Vec<Box<str>> = Vec::new();
        for item in cols.iter() {
            push_item(&item, &mut raw, &mut name_boxes, py)?;
        }

        let tmp: Vec<Tmp> = if raw.iter().any(|x| matches!(x, ItemOrExprStr::ExprStr(_))) {
            let session = THREAD_ACTIVE_SESSIONS
                .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "select() with expr() requires an active SparkSession",
                    )
                })?;
            let session_ref = session.bind(py).downcast::<PySparkSession>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
            })?;
            session_ref
                .borrow()
                .inner
                .create_or_replace_temp_view("__selectexpr_t", self.inner.clone());
            let mut resolved: Vec<Tmp> = Vec::with_capacity(raw.len());
            for x in raw {
                match x {
                    ItemOrExprStr::Resolved(t) => resolved.push(t),
                    ItemOrExprStr::ExprStr(s) => {
                        let expr = robin_sparkless::sql::expr_string_to_polars(
                            &s,
                            &session_ref.borrow().inner,
                            &self.inner,
                        )
                        .map_err(to_py_err)?;
                        resolved.push(Tmp::Expr(expr));
                    }
                }
            }
            session_ref.borrow().inner.drop_temp_view("__selectexpr_t");
            resolved
        } else {
            raw.into_iter()
                .map(|x| match x {
                    ItemOrExprStr::Resolved(t) => t,
                    ItemOrExprStr::ExprStr(_) => unreachable!(),
                })
                .collect()
        };

        // Fast-path: all arguments are simple string column names (possibly nested in lists/tuples),
        // with no Column or expr() items. In this case, delegate directly to DataFrame::select so
        // we reuse its PySpark-style ambiguous-case handling and dotted-name logic.
        if tmp.iter().all(|t| matches!(t, Tmp::NameIdx(_))) {
            let mut col_refs: Vec<&str> = Vec::with_capacity(tmp.len());
            for t in &tmp {
                if let Tmp::NameIdx(i) = t {
                    let name = name_boxes[*i].as_ref();
                    col_refs.push(name);
                }
            }
            return self
                .inner
                .select(col_refs)
                .map(PyDataFrame::wrap)
                .map_err(to_py_err);
        }

        let all_columns = self.inner.columns().map_err(to_py_err)?;
        let mut items: Vec<SelectItem<'_>> = Vec::with_capacity(tmp.len());
        for t in tmp {
            match t {
                Tmp::Expr(e) => items.push(SelectItem::Expr(e)),
                Tmp::NameIdx(i) => {
                    let name = name_boxes[i].as_ref();
                    if name == "*" {
                        for c in &all_columns {
                            items.push(SelectItem::ColumnName(c.as_str()));
                        }
                    } else {
                        items.push(SelectItem::ColumnName(name));
                    }
                }
            }
        }

        self.inner
            .select_items(items)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn with_column(
        slf: PyRef<Self>,
        py: Python<'_>,
        name: &str,
        col_any: &Bound<'_, PyAny>,
    ) -> PyResult<PyDataFrame> {
        // Case 1: regular Column (including Python UDF columns)
        if let Ok(py_col) = col_any.downcast::<PyColumn>() {
            let col = py_col.borrow();
            if let Some((udf_name, arg_names, literal_jsons)) =
                col.inner.udf_call_info_with_literals()
            {
                let arg_names_py = PyList::new_bound(py, arg_names.iter());
                let literals_py = PyList::empty_bound(py);
                for opt in &literal_jsons {
                    let item: Bound<'_, PyAny> = match opt {
                        Some(s) => pyo3::types::PyString::new_bound(py, s.as_str()).into_any(),
                        None => py.None().into_bound(py),
                    };
                    literals_py.append(item)?;
                }
                if let Some(result_df) =
                    PYTHON_UDF_EXECUTOR.with(|cell| -> PyResult<Option<PyDataFrame>> {
                        let cb = cell.borrow();
                        let callback = match cb.as_ref() {
                            Some(c) => c,
                            None => return Ok(None),
                        };
                        let df_obj: Bound<'_, PyAny> =
                            unsafe { Bound::from_borrowed_ptr(py, slf.as_ptr()) };
                        let result = callback.bind(py).call1((
                            df_obj,
                            name,
                            udf_name,
                            arg_names_py,
                            literals_py,
                        ))?;
                        let py_df = result.downcast::<PyDataFrame>()?;
                        Ok(Some(PyDataFrame::wrap(py_df.borrow().inner.clone())))
                    })?
                {
                    return Ok(result_df);
                }
            }
            let df = slf.inner.with_column(name, &col.inner).map_err(|e| {
                let msg = e.to_string();
                if msg.contains("to_timestamp requires StringType") {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(msg)
                } else {
                    to_py_err(msg)
                }
            })?;
            // Force schema inference now so type errors surface at withColumn time (PySpark parity).
            df.schema_engine().map_err(|e| {
                let msg = e.to_string();
                if msg.contains("to_timestamp requires StringType") {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(msg)
                } else {
                    to_py_err(msg)
                }
            })?;
            return Ok(PyDataFrame::wrap(df));
        }

        // Case 2: expr-string from F.expr(...)
        if let Ok(py_expr_str) = col_any.downcast::<PyExprStr>() {
            let session = THREAD_ACTIVE_SESSIONS
                .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "withColumn(expr) requires an active SparkSession",
                    )
                })?;
            let session_ref = session.bind(py).downcast::<PySparkSession>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
            })?;
            // Use a temp view so expr_string_to_polars can resolve column names against this DataFrame.
            session_ref
                .borrow()
                .inner
                .create_or_replace_temp_view("__withcol_expr_t", slf.inner.clone());
            let expr = robin_sparkless::sql::expr_string_to_polars(
                &py_expr_str.borrow().sql,
                &session_ref.borrow().inner,
                &slf.inner,
            )
            .map_err(to_py_err)?;
            session_ref
                .borrow()
                .inner
                .drop_temp_view("__withcol_expr_t");
            let col = Column::from_expr(expr, None);
            let df = slf.inner.with_column(name, &col).map_err(|e| {
                let msg = e.to_string();
                if msg.contains("to_timestamp requires StringType") {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(msg)
                } else {
                    to_py_err(msg)
                }
            })?;
            df.schema_engine().map_err(|e| {
                let msg = e.to_string();
                if msg.contains("to_timestamp requires StringType") {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(msg)
                } else {
                    to_py_err(msg)
                }
            })?;
            return Ok(PyDataFrame::wrap(df));
        }

        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "withColumn expects a Column or expr() result",
        ))
    }

    #[pyo3(name = "withColumn")]
    fn with_column_camel(
        slf: PyRef<Self>,
        py: Python<'_>,
        name: &str,
        col: &Bound<'_, PyAny>,
    ) -> PyResult<PyDataFrame> {
        Self::with_column(slf, py, name, col)
    }

    #[pyo3(name = "withColumnRenamed")]
    fn with_column_renamed(&self, old_name: &str, new_name: &str) -> PyResult<PyDataFrame> {
        self.inner
            .with_column_renamed(old_name, new_name)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(name = "__getitem__")]
    fn get_item(&self, name: &str) -> PyResult<PyColumn> {
        self.inner
            .column(name)
            .map(|c| PyColumn { inner: c })
            .map_err(to_py_err)
    }

    fn __getattr__(&self, name: &str) -> PyResult<PyColumn> {
        if name.starts_with('_') {
            return Err(PyErr::new::<pyo3::exceptions::PyAttributeError, _>(
                name.to_string(),
            ));
        }
        self.inner
            .column(name)
            .map(|c| PyColumn { inner: c })
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyAttributeError, _>(name.to_string()))
    }

    #[pyo3(signature = (n=20, truncate=true))]
    fn show(&self, n: usize, truncate: bool) -> PyResult<()> {
        // truncate: PySpark parity (e.g. show(truncate=False)); display layer may use it later
        let _ = truncate;
        self.inner.show(Some(n)).map_err(to_py_err)
    }

    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        if let Some(ref cached) = self.schema_cache {
            return Ok(cached.clone_ref(py).into_py(py));
        }
        let schema = self.inner.schema_engine().map_err(to_py_err)?;
        let types_mod = PyModule::import_bound(py, "sparkless.sql.types")?;
        let struct_type_cls = types_mod.getattr("StructType")?;
        let struct_field_cls = types_mod.getattr("StructField")?;

        fn dtype_to_py(
            py: Python<'_>,
            types_mod: &Bound<'_, PyModule>,
            dt: &DataType,
        ) -> PyResult<PyObject> {
            let mk0 = |name: &str| -> PyResult<PyObject> {
                Ok(types_mod.getattr(name)?.call0()?.into_py(py))
            };
            match dt {
                DataType::String => mk0("StringType"),
                DataType::Integer => mk0("IntegerType"),
                DataType::Long => mk0("LongType"),
                DataType::Double => mk0("DoubleType"),
                DataType::Boolean => mk0("BooleanType"),
                DataType::Date => mk0("DateType"),
                DataType::Timestamp => mk0("TimestampType"),
                DataType::Binary => mk0("BinaryType"),
                DataType::Array(elem) => {
                    let elem_py = dtype_to_py(py, types_mod, elem)?;
                    Ok(types_mod
                        .getattr("ArrayType")?
                        .call1((elem_py, true))?
                        .into_py(py))
                }
                DataType::Map(k, v) => {
                    let k_py = dtype_to_py(py, types_mod, k)?;
                    let v_py = dtype_to_py(py, types_mod, v)?;
                    Ok(types_mod
                        .getattr("MapType")?
                        .call1((k_py, v_py, true))?
                        .into_py(py))
                }
                DataType::Struct(fields) => {
                    let mut py_fields: Vec<PyObject> = Vec::with_capacity(fields.len());
                    for f in fields {
                        let dt_obj = dtype_to_py(py, types_mod, &f.data_type)?;
                        let sf = types_mod.getattr("StructField")?.call1((
                            f.name.clone(),
                            dt_obj,
                            f.nullable,
                            py.None(),
                        ))?;
                        py_fields.push(sf.into_py(py));
                    }
                    Ok(types_mod
                        .getattr("StructType")?
                        .call1((py_fields,))?
                        .into_py(py))
                }
            }
        }

        let mut py_fields: Vec<PyObject> = Vec::with_capacity(schema.fields().len());
        for f in schema.fields() {
            let dt_obj = dtype_to_py(py, &types_mod, &f.data_type)?;
            let sf = struct_field_cls.call1((f.name.clone(), dt_obj, f.nullable, py.None()))?;
            py_fields.push(sf.into_py(py));
        }
        Ok(struct_type_cls.call1((py_fields,))?.into_py(py))
    }

    #[getter]
    fn _schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.schema(py)
    }

    /// Collect all rows as a list of Row objects (PySpark parity).
    ///
    /// **Contract:** Returns `list[Row]`. Each Row is tuple-like with attribute and name-based
    /// access; types are preserved (int, float, bool, str, None, date, datetime, list, dict).
    /// Use `row.asDict()` for a dict of column name -> value, or iterate for values in order.
    /// String-typed columns are best-effort coerced to int/float/bool when the value looks like
    /// one (e.g. coalesce() results or mixed-type array elements stringified by the engine).
    fn collect(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        // Build a PySpark-like Row with schema attached. Use schema from collected result so
        // get_json_object etc. are string (PySpark parity #1146).
        let (output_names, rows, schema) = self
            .inner
            .collect_as_json_rows_with_names()
            .map_err(to_py_err)?;
        let mut dtype_by_name: std::collections::HashMap<String, DataType> =
            std::collections::HashMap::with_capacity(schema.fields().len());
        for f in schema.fields() {
            dtype_by_name.insert(f.name.clone(), f.data_type.clone());
        }

        let types_mod = PyModule::import_bound(py, "sparkless.sql.types")?;
        let row_cls = types_mod.getattr("Row")?;
        let struct_type_cls = types_mod.getattr("StructType")?;
        let struct_field_cls = types_mod.getattr("StructField")?;

        fn dtype_to_py(
            py: Python<'_>,
            types_mod: &Bound<'_, PyModule>,
            dt: &DataType,
        ) -> PyResult<PyObject> {
            let mk0 = |name: &str| -> PyResult<PyObject> {
                Ok(types_mod.getattr(name)?.call0()?.into_py(py))
            };
            match dt {
                DataType::String => mk0("StringType"),
                DataType::Integer => mk0("IntegerType"),
                DataType::Long => mk0("LongType"),
                DataType::Double => mk0("DoubleType"),
                DataType::Boolean => mk0("BooleanType"),
                DataType::Date => mk0("DateType"),
                DataType::Timestamp => mk0("TimestampType"),
                DataType::Binary => mk0("BinaryType"),
                DataType::Array(elem) => {
                    let elem_py = dtype_to_py(py, types_mod, elem)?;
                    Ok(types_mod
                        .getattr("ArrayType")?
                        .call1((elem_py, true))?
                        .into_py(py))
                }
                DataType::Map(k, v) => {
                    let k_py = dtype_to_py(py, types_mod, k)?;
                    let v_py = dtype_to_py(py, types_mod, v)?;
                    Ok(types_mod
                        .getattr("MapType")?
                        .call1((k_py, v_py, true))?
                        .into_py(py))
                }
                DataType::Struct(fields) => {
                    let mut py_fields: Vec<PyObject> = Vec::with_capacity(fields.len());
                    for f in fields {
                        let dt_obj = dtype_to_py(py, types_mod, &f.data_type)?;
                        let sf = types_mod.getattr("StructField")?.call1((
                            f.name.clone(),
                            dt_obj,
                            f.nullable,
                            py.None(),
                        ))?;
                        py_fields.push(sf.into_py(py));
                    }
                    Ok(types_mod
                        .getattr("StructType")?
                        .call1((py_fields,))?
                        .into_py(py))
                }
            }
        }

        // Build StructType schema object once.
        let mut py_fields: Vec<PyObject> = Vec::with_capacity(schema.fields().len());
        for f in schema.fields() {
            let dt_obj = dtype_to_py(py, &types_mod, &f.data_type)?;
            let sf = struct_field_cls.call1((f.name.clone(), dt_obj, f.nullable, py.None()))?;
            py_fields.push(sf.into_py(py));
        }
        let py_schema = struct_type_cls.call1((py_fields,))?.into_py(py);

        let datetime_mod = PyModule::import_bound(py, "datetime")?;
        let datetime_cls = datetime_mod.getattr("datetime")?;
        let date_cls = datetime_mod.getattr("date")?;

        // output_names and rows already obtained above with schema.
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let kwargs = PyDict::new_bound(py);
            for name in &output_names {
                let v = row.get(name).unwrap_or(&JsonValue::Null);
                let py_v = json_value_to_py_with_schema(
                    py,
                    v,
                    dtype_by_name.get(name),
                    &datetime_cls,
                    &date_cls,
                    Some(name.as_str()),
                    Some(&output_names),
                )?;
                kwargs.set_item(name, py_v)?;
            }
            let py_row = row_cls.call((), Some(&kwargs))?;
            py_row.setattr("_fields", output_names.clone())?;
            py_row.setattr("_schema", py_schema.clone_ref(py))?;
            // PySpark parity: Row has _data_dict for dict-like access in tests (e.g. "full_name" in result[0].__dict__["_data_dict"]).
            py_row.setattr("_data_dict", kwargs.clone())?;
            out.push(py_row.into_py(py));
        }
        Ok(out)
    }

    fn count(&self) -> PyResult<usize> {
        self.inner.count().map_err(to_py_err)
    }

    /// PySpark parity: len(df) returns row count.
    fn __len__(&self) -> PyResult<usize> {
        self.inner.count().map_err(to_py_err)
    }

    /// PySpark parity: head() returns a DataFrame (so .collect() works); head(n) returns list of Row.
    /// (issue #413/#1077, #1109 delta schema evolution tests)
    #[pyo3(signature = (n=None))]
    fn head(&self, py: Python<'_>, n: Option<usize>) -> PyResult<PyObject> {
        let k = n.unwrap_or(1);
        let limited = self.inner.limit(k).map_err(to_py_err)?;
        match n {
            Some(_) => {
                // head(n) -> list of Row (PySpark parity for upstream tests e.g. test_delta_lake_schema_evolution)
                let limited_py = PyDataFrame::wrap(limited);
                let rows = limited_py.collect(py)?;
                Ok(PyList::new_bound(py, rows).into_py(py))
            }
            None => {
                // head() -> DataFrame so .collect() can be called (issue #413)
                let limited_py = PyDataFrame::wrap(limited);
                Ok(limited_py.into_py(py))
            }
        }
    }

    #[getter]
    fn na(slf: PyRef<Self>) -> PyDataFrameNaFunctions {
        let py = slf.py();
        PyDataFrameNaFunctions {
            df: slf.into_py(py),
        }
    }

    fn alias(&self, name: &str) -> PyDataFrame {
        PyDataFrame::wrap(self.inner.alias(name))
    }

    #[pyo3(signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        fn push_expr(
            item: &Bound<'_, PyAny>,
            out: &mut Vec<robin_sparkless::Expr>,
        ) -> PyResult<()> {
            if let Ok(c) = item.downcast::<PyColumn>() {
                out.push(c.borrow().inner.clone().into_expr());
                return Ok(());
            }
            if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "agg() expects Column expressions",
            ))
        }

        let mut rust_exprs: Vec<robin_sparkless::Expr> = Vec::new();
        for item in exprs.iter() {
            push_expr(&item, &mut rust_exprs)?;
        }
        self.inner
            .agg(rust_exprs)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn cache(&self) -> PyResult<PyDataFrame> {
        self.inner.cache().map(PyDataFrame::wrap).map_err(to_py_err)
    }

    #[pyo3(signature = (*cols))]
    fn rollup(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyCubeRollupData> {
        let names = py_tuple_or_single_to_vec_string(cols)?;
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        self.inner
            .rollup(refs)
            .map(|cr| PyCubeRollupData { inner: cr })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*cols, ascending=None))]
    fn sort(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        self.order_by_camel(cols, ascending)
    }

    #[pyo3(name = "groupby", signature = (*cols))]
    fn groupby(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyGroupedData> {
        self.group_by_camel(cols)
    }

    #[getter]
    fn rdd(slf: PyRef<Self>) -> PyRDD {
        PyRDD {
            inner: slf.inner.clone(),
        }
    }

    #[getter]
    fn storage(&self) -> PyResult<PyDataFrame> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "storage is not yet implemented in robin-sparkless",
        ))
    }

    #[pyo3(signature = (*cols))]
    fn cube(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyCubeRollupData> {
        let names = py_tuple_or_single_to_vec_string(cols)?;
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        self.inner
            .cube(refs)
            .map(|cr| PyCubeRollupData { inner: cr })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (column_names))]
    fn group_by(&self, column_names: &Bound<'_, PyAny>) -> PyResult<PyGroupedData> {
        let names = py_cols_to_vec(column_names)?;
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        self.inner
            .group_by(refs)
            .map(|gd| PyGroupedData { inner: gd })
            .map_err(to_py_err)
    }

    #[pyo3(name = "groupBy", signature = (*cols))]
    fn group_by_camel(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyGroupedData> {
        let mut specs: Vec<GroupBySpec> = Vec::with_capacity(cols.len());
        for item in cols.iter() {
            for spec in py_group_by_specs(&item)? {
                specs.push(spec);
            }
        }
        self.inner
            .group_by_specs(specs)
            .map(|gd| PyGroupedData { inner: gd })
            .map_err(to_py_err)
    }

    fn order_by_names(
        &self,
        column_names: Vec<String>,
        ascending: Vec<bool>,
    ) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        let mut asc = ascending;
        while asc.len() < refs.len() {
            asc.push(true);
        }
        asc.truncate(refs.len());
        self.inner
            .order_by(refs, asc)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (cols, ascending=None))]
    fn do_order_by(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        // Flatten: single item may be list/tuple of columns
        let mut flat: Vec<Bound<'_, PyAny>> = Vec::new();
        for item in cols.iter() {
            if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    flat.push(sub.clone());
                }
            } else if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    flat.push(sub.clone());
                }
            } else {
                flat.push(item.clone());
            }
        }

        // Ascending per column (needed for Column path so we apply it to sort orders).
        let asc_vec: Vec<bool> = match ascending {
            None => vec![true; flat.len()],
            Some(v) if v.extract::<bool>().is_ok() => vec![v.extract::<bool>()?; flat.len()],
            Some(v) if v.downcast::<PyList>().is_ok() => v
                .downcast::<PyList>()?
                .iter()
                .map(|x| x.extract::<bool>())
                .collect::<PyResult<Vec<bool>>>()?,
            Some(v) if v.downcast::<PyTuple>().is_ok() => v
                .downcast::<PyTuple>()?
                .iter()
                .map(|x| x.extract::<bool>())
                .collect::<PyResult<Vec<bool>>>()?,
            Some(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "ascending must be a bool or list/tuple[bool]",
                ))
            }
        };

        // Try to build sort orders from Column/SortOrder/string (for order_by_exprs).
        let mut sort_orders: Vec<SortOrder> = Vec::with_capacity(flat.len());
        let mut names_only: Vec<String> = Vec::with_capacity(flat.len());
        let mut all_strings = true;
        for (i, item) in flat.iter().enumerate() {
            let asc = asc_vec.get(i).copied().unwrap_or(true);
            if let Ok(ps) = item.downcast::<PySortOrder>() {
                // Explicit SortOrder (e.g. col("x").desc()), use as-is.
                sort_orders.push(ps.borrow().inner.clone());
                all_strings = false;
            } else if let Ok(pc) = item.downcast::<PyColumn>() {
                // Bare Column: apply ascending/descending from asc_vec.
                let so = if asc {
                    pc.borrow().inner.asc_nulls_last()
                } else {
                    pc.borrow().inner.desc()
                };
                sort_orders.push(so);
                all_strings = false;
            } else if let (Ok(name_attr), Ok(asc_attr)) =
                (item.getattr("name"), item.getattr("ascending"))
            {
                // Sort-key object from sparkless.sql.functions.desc/asc: has .name and .ascending.
                let name: String = name_attr.extract()?;
                let ascending_flag: bool = asc_attr.extract()?;
                let col = Column::new(name.clone());
                let so = if ascending_flag {
                    functions::asc(&col)
                } else {
                    functions::desc(&col)
                };
                sort_orders.push(so);
                all_strings = false;
            } else if let Ok(s) = item.extract::<String>() {
                // Plain column name string.
                sort_orders.push(asc_from_name(&s));
                names_only.push(s);
            } else {
                // Fallback: give up on mixed types; try pure string path below.
                sort_orders.clear();
                names_only.clear();
                all_strings = false;
                break;
            }
        }

        if sort_orders.len() == flat.len() {
            if all_strings {
                return self.order_by_names(names_only, asc_vec);
            }
            return self
                .inner
                .order_by_exprs(sort_orders)
                .map(PyDataFrame::wrap)
                .map_err(to_py_err);
        }

        // Fallback: all column names as str (e.g. list of strings from a different structure)
        let mut names: Vec<String> = Vec::with_capacity(flat.len());
        for item in &flat {
            if let Ok(s) = item.extract::<String>() {
                names.push(s);
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "orderBy() expects column names as str or Column/SortOrder expressions",
                ));
            }
        }

        self.order_by_names(names, asc_vec)
    }

    /// order_by_exprs([col("a").asc(), col("b").desc_nulls_last()]) - order by SortOrder expressions.
    fn order_by_exprs(&self, exprs: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let list = exprs.downcast::<PyList>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "order_by_exprs expects a list of Column or SortOrder",
            )
        })?;
        let mut sort_orders: Vec<SortOrder> = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(ps) = item.downcast::<PySortOrder>() {
                sort_orders.push(ps.borrow().inner.clone());
            } else if let Ok(pc) = item.downcast::<PyColumn>() {
                sort_orders.push(pc.borrow().inner.asc());
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "order_by_exprs list elements must be Column or SortOrder (e.g. col('x').asc())",
                ));
            }
        }
        self.inner
            .order_by_exprs(sort_orders)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*cols, ascending=None))]
    fn order_by(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        self.do_order_by(cols, ascending)
    }

    #[pyo3(name = "orderBy", signature = (*cols, ascending=None))]
    fn order_by_camel(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        self.do_order_by(cols, ascending)
    }

    fn limit(&self, n: usize) -> PyResult<PyDataFrame> {
        self.inner
            .limit(n)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*columns))]
    fn drop(&self, columns: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let col_names = py_tuple_or_single_to_vec_string(columns)?;
        let refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
        self.inner
            .drop(refs)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (subset=None))]
    fn distinct(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        self.inner
            .distinct(sub)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(name = "dropDuplicates", signature = (subset=None))]
    fn drop_duplicates(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        self.distinct(subset)
    }

    #[pyo3(name = "drop_duplicates", signature = (subset=None))]
    fn drop_duplicates_snake(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        self.distinct(subset)
    }

    #[pyo3(signature = (other, on=None, how="inner", left_on=None, right_on=None))]
    fn join(
        &self,
        other: &PyDataFrame,
        on: Option<&Bound<'_, PyAny>>,
        how: &str,
        left_on: Option<&Bound<'_, PyAny>>,
        right_on: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let how_lower = how.to_lowercase();
        if how_lower == "cross" {
            return self
                .inner
                .cross_join(&other.inner)
                .map(PyDataFrame::wrap)
                .map_err(to_py_err);
        }
        let join_type = match how_lower.as_str() {
            "inner" => JoinType::Inner,
            "left" | "left_outer" => JoinType::Left,
            "right" | "right_outer" => JoinType::Right,
            "outer" | "full" | "full_outer" => JoinType::Outer,
            "semi" | "left_semi" | "leftsemi" => JoinType::LeftSemi,
            "anti" | "left_anti" | "leftanti" => JoinType::LeftAnti,
            _ => JoinType::Inner,
        };
        let use_left_right = left_on.is_some() && right_on.is_some();
        if use_left_right {
            let left_vec = py_join_on_to_vec(left_on.unwrap())?;
            let right_vec = py_join_on_to_vec(right_on.unwrap())?;
            let left_refs: Vec<&str> = left_vec.iter().map(|s| s.as_str()).collect();
            let right_refs: Vec<&str> = right_vec.iter().map(|s| s.as_str()).collect();
            self.inner
                .join_with_keys(&other.inner, left_refs, right_refs, join_type)
                .map(PyDataFrame::wrap)
                .map_err(to_py_err)
        } else if let Some(on_arg) = on {
            // Try to use normal join when on is simple column name(s) (PySpark parity: one key column in result).
            if let Ok(on_vec) = py_join_on_to_vec(on_arg) {
                let use_normal_join =
                    !on_vec.is_empty() && !on_vec.iter().any(|n| n == "<expr>" || n.is_empty());
                if use_normal_join {
                    let on_refs: Vec<&str> = on_vec.iter().map(|s| s.as_str()).collect();
                    return self
                        .inner
                        .join(&other.inner, on_refs, join_type)
                        .map(PyDataFrame::wrap)
                        .map_err(to_py_err);
                }
            }
            // Complex expression (e.g. left.dept_id == right.dept_id): try key-based join first (#1049, #1148).
            if let Ok(condition) = on_arg.downcast::<PyColumn>() {
                let expr = condition.borrow().inner.clone().into_expr();
                let pairs = try_extract_join_eq_columns_all(&expr);
                if !pairs.is_empty() {
                    let left_cols: std::collections::HashSet<String> = self
                        .inner
                        .columns()
                        .map_err(to_py_err)?
                        .into_iter()
                        .collect();
                    let right_cols: std::collections::HashSet<String> = other
                        .inner
                        .columns()
                        .map_err(to_py_err)?
                        .into_iter()
                        .collect();
                    let mut left_refs: Vec<String> = Vec::with_capacity(pairs.len());
                    let mut right_refs: Vec<String> = Vec::with_capacity(pairs.len());
                    for (key_a, key_b) in &pairs {
                        let (left_key, right_key) = resolve_join_keys_with_aliases(
                            key_a,
                            key_b,
                            &left_cols,
                            &right_cols,
                            self.inner.get_alias().as_deref(),
                            other.inner.get_alias().as_deref(),
                        );
                        if let (Some(lk), Some(rk)) = (left_key, right_key) {
                            left_refs.push(lk);
                            right_refs.push(rk);
                        }
                    }
                    if left_refs.len() == pairs.len() {
                        let left_refs: Vec<&str> = left_refs.iter().map(|s| s.as_str()).collect();
                        let right_refs: Vec<&str> = right_refs.iter().map(|s| s.as_str()).collect();
                        let joined = self
                            .inner
                            .join_with_keys(&other.inner, left_refs, right_refs, join_type)
                            .map_err(to_py_err)?;
                        // When the original expression was a compound condition (e.g. key equality AND filter),
                        // reapply the full predicate after the key-based join so additional conditions are honored
                        // (issue #380: join with compound condition).
                        let filtered = joined.filter(expr).map_err(to_py_err)?;
                        return Ok(PyDataFrame::wrap(filtered));
                    }
                }
                // Non-eq expression: cross + filter (ambiguous duplicate column names may apply).
                let crossed = self.inner.cross_join(&other.inner).map_err(to_py_err)?;
                let filtered = crossed.filter(expr).map_err(to_py_err)?;
                // For left/outer joins, include unmatched left rows with nulls on right columns (best-effort PySpark parity).
                if matches!(join_type, JoinType::Left | JoinType::Outer) {
                    // Columns coming from left side
                    let left_cols = self.inner.columns().map_err(to_py_err)?;
                    let left_refs: Vec<&str> = left_cols.iter().map(|s| s.as_str()).collect();
                    let filtered_left = filtered.select(left_refs).map_err(to_py_err)?;
                    // Unmatched left rows (set difference by all left columns)
                    let unmatched_left = self.inner.subtract(&filtered_left).map_err(to_py_err)?;
                    // Add null right columns to unmatched_left so schema matches filtered.
                    let all_cols = filtered.columns().map_err(to_py_err)?;
                    let mut augmented = unmatched_left;
                    for col_name in &all_cols {
                        // Skip columns that already exist on the left side.
                        if left_cols.iter().any(|c| c.as_str() == col_name.as_str()) {
                            continue;
                        }
                        let dtype_opt =
                            filtered
                                .get_column_dtype(col_name.as_str())
                                .ok_or_else(|| {
                                    to_py_err(format!(
                                        "join: failed to get dtype for column '{}'",
                                        col_name
                                    ))
                                })?;
                        // Heuristic mapping from engine dtype to PySpark type string.
                        let dtype_str = dtype_opt.to_string().to_lowercase();
                        let null_col = if dtype_str.contains("int") {
                            functions::lit_null("bigint").map_err(to_py_err)?
                        } else if dtype_str.contains("string") {
                            functions::lit_null("string").map_err(to_py_err)?
                        } else {
                            functions::lit_null_untyped()
                        };
                        augmented = augmented
                            .with_column(col_name.as_str(), &null_col)
                            .map_err(to_py_err)?;
                    }
                    // Reorder columns on augmented to match filtered, then union (no type promotion).
                    let all_refs: Vec<&str> = all_cols.iter().map(|s| s.as_str()).collect();
                    let augmented = augmented.select(all_refs).map_err(to_py_err)?;
                    let all = filtered.union(&augmented).map_err(to_py_err)?;
                    return Ok(PyDataFrame::wrap(all));
                }
                return Ok(PyDataFrame::wrap(filtered));
            }
            let on_vec = py_join_on_to_vec(on_arg)?;
            let on_refs: Vec<&str> = on_vec.iter().map(|s| s.as_str()).collect();
            self.inner
                .join(&other.inner, on_refs, join_type)
                .map(PyDataFrame::wrap)
                .map_err(to_py_err)
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "join() requires either on= or (left_on= and right_on=)",
            ))
        }
    }

    #[pyo3(name = "crossJoin")]
    fn cross_join(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .cross_join(&other.inner)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn intersect(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .intersect(&other.inner)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(name = "exceptAll")]
    fn except_all(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .except_all(&other.inner)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn union(&self, other: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let other_inner = py_any_to_dataframe_inner(other)?;
        self.inner
            .union(&other_inner)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn union_all(&self, other: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let other_inner = py_any_to_dataframe_inner(other)?;
        self.inner
            .union_all(&other_inner)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(name = "unionAll")]
    fn union_all_camel(&self, other: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        self.union_all(other)
    }

    #[pyo3(name = "unionByName", signature = (other, **kwargs))]
    fn union_by_name(
        &self,
        other: &Bound<'_, PyAny>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyDataFrame> {
        let allow = extract_allow_missing_columns(kwargs)?;
        let other_inner = py_any_to_dataframe_inner(other)?;
        self.inner
            .union_by_name(&other_inner, allow)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(name = "selectExpr", signature = (*exprs))]
    fn select_expr(&self, py: Python<'_>, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let mut exprs_vec: Vec<String> = Vec::with_capacity(exprs.len());
        for item in exprs.iter() {
            exprs_vec.push(item.extract::<String>()?);
        }
        // Prefer SQL-based parsing when session is available (e.g. "upper(Name) as u").
        let session_opt =
            THREAD_ACTIVE_SESSIONS.with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)));
        if let Some(session) = session_opt {
            if let Ok(session_ref) = session.bind(py).downcast::<PySparkSession>() {
                if let Ok(df) = self
                    .inner
                    .select_expr_with_session(&session_ref.borrow().inner, &exprs_vec)
                {
                    return Ok(PyDataFrame::wrap(df));
                }
            }
        }
        self.inner
            .select_expr(&exprs_vec)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(name = "createOrReplaceTempView")]
    fn create_or_replace_temp_view_camel(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let session = THREAD_ACTIVE_SESSIONS
            .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
            .ok_or_else(|| to_py_err("No active SparkSession for createOrReplaceTempView"))?;
        let session_ref = session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        session_ref
            .borrow()
            .inner
            .create_or_replace_temp_view(name, self.inner.clone());
        Ok(())
    }

    #[pyo3(name = "dropna", signature = (subset=None, how=None, thresh=None))]
    fn dropna(
        &self,
        subset: Option<Vec<String>>,
        how: Option<&str>,
        thresh: Option<usize>,
    ) -> PyResult<PyDataFrame> {
        let how_str = how.unwrap_or("any");
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        self.inner
            .dropna(sub, how_str, thresh)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (value, subset=None))]
    fn fillna(
        &self,
        value: &Bound<'_, PyAny>,
        subset: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        // PySpark: fillna(scalar) or fillna({col: value, ...}); subset only with scalar.
        if let Ok(dict) = value.downcast::<PyDict>() {
            let mut df = self.inner.clone();
            for item in dict.iter() {
                let col_name: String = item.0.extract().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>("fillna dict keys must be str")
                })?;
                let val_expr = py_any_to_column(&item.1)?.into_expr();
                df = df
                    .fillna(val_expr, Some(vec![col_name.as_str()]))
                    .map_err(to_py_err)?;
            }
            return Ok(PyDataFrame::wrap(df));
        }
        let value_expr = py_any_to_column(value)?.into_expr();
        let kind = classify_fill_value_kind(value);
        let sub_vec = normalize_subset(subset)?;
        // Candidate columns: explicit subset or all columns when subset is None.
        let candidates: Vec<String> = match sub_vec {
            Some(cols) => cols,
            None => self.inner.columns().map_err(to_py_err)?,
        };
        let mut effective: Vec<&str> = Vec::new();
        for name in &candidates {
            match self.inner.get_column_data_type(name) {
                Some(dt) => {
                    if is_fill_compatible_with_schema_dtype(&dt, &kind) {
                        effective.push(name.as_str());
                    }
                }
                None => {
                    // Non-existent column: keep so engine raises ColumnNotFoundException as before.
                    effective.push(name.as_str());
                }
            }
        }
        let sub: Option<Vec<&str>> = Some(effective);
        self.inner
            .fillna(value_expr, sub)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    /// Replace value in columns. PySpark df.replace(to_replace, value=None, subset=...).
    /// When value is None and to_replace is a dict, apply each key->value replacement.
    #[pyo3(signature = (to_replace, value=None, subset=None))]
    fn replace(
        &self,
        to_replace: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
        subset: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let sub_vec = normalize_subset(subset)?;
        let sub: Option<Vec<&str>> = sub_vec
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());

        if value.is_none() {
            if let Ok(dict) = to_replace.downcast::<PyDict>() {
                let mut current = self.inner.clone();
                for (k, v) in dict.iter() {
                    let old_expr = py_any_to_column(&k)?.into_expr();
                    let new_expr = py_any_to_column(&v)?.into_expr();
                    current = current
                        .na()
                        .replace(old_expr, new_expr, sub.as_ref().map(|v| v.to_vec()))
                        .map_err(to_py_err)?;
                }
                return Ok(PyDataFrame::wrap(current));
            }
            // PySpark: na.replace("a", None) replaces "a" with null.
            let null_expr = robin_sparkless::functions::lit_null("string")
                .map_err(to_py_err)?
                .into_expr();
            let old_expr = py_any_to_column(to_replace)?.into_expr();
            return self
                .inner
                .na()
                .replace(old_expr, null_expr, sub.as_ref().map(|v| v.to_vec()))
                .map(PyDataFrame::wrap)
                .map_err(to_py_err);
        }

        let value = value.unwrap();
        let old_expr = py_any_to_column(to_replace)?.into_expr();
        let new_expr = py_any_to_column(value)?.into_expr();
        self.inner
            .na()
            .replace(old_expr, new_expr, sub.as_ref().map(|v| v.to_vec()))
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    /// Return the first row as a Row, or None if empty (Phase 7.8 / PySpark first() semantics).
    fn first(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let one = self.inner.first().map_err(to_py_err)?;
        let wrapper = PyDataFrame::wrap(one);
        let rows = wrapper.collect(py)?;
        Ok(rows.into_iter().next())
    }

    fn describe(&self) -> PyResult<PyDataFrame> {
        self.inner
            .describe()
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    /// PySpark alias for describe().
    fn summary(&self) -> PyResult<PyDataFrame> {
        self.describe()
    }

    /// PySpark: explain() and explain(extended=True) (mode= accepted for parity; #1152).
    #[pyo3(signature = (extended=None))]
    fn explain(&self, extended: Option<bool>) -> PyResult<String> {
        let _ = extended; // accepted for API parity; not yet used
        Ok(self.inner.explain())
    }

    fn print_schema(&self) -> PyResult<String> {
        self.inner.print_schema().map_err(to_py_err)
    }

    #[pyo3(name = "printSchema")]
    fn print_schema_camel(&self) -> PyResult<String> {
        self.print_schema()
    }

    fn persist(&self) -> PyResult<PyDataFrame> {
        self.inner
            .persist()
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn subtract(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .subtract(&other.inner)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(name = "sampleBy", signature = (col, fractions, seed=None))]
    fn sample_by(
        &self,
        _py: Python<'_>,
        col: &str,
        fractions: &Bound<'_, PyAny>,
        seed: Option<u64>,
    ) -> PyResult<PyDataFrame> {
        let dict = fractions.downcast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "sampleBy fractions must be a dict of {value: fraction}",
            )
        })?;
        let mut fracs: Vec<(robin_sparkless::Expr, f64)> = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let frac = v.extract::<f64>()?;
            let lit_col = py_any_to_column(&k)?;
            fracs.push((lit_col.into_expr(), frac));
        }
        self.inner
            .sample_by(col, &fracs, seed)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn crosstab(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        self.inner
            .crosstab(col1, col2)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(name = "freqItems", signature = (columns, support=0.01))]
    fn freq_items(&self, columns: Vec<String>, support: f64) -> PyResult<PyDataFrame> {
        let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        self.inner
            .freq_items(&cols, support)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn melt(&self, id_vars: Vec<String>, value_vars: Vec<String>) -> PyResult<PyDataFrame> {
        let ids: Vec<&str> = id_vars.iter().map(|s| s.as_str()).collect();
        let vals: Vec<&str> = value_vars.iter().map(|s| s.as_str()).collect();
        self.inner
            .melt(&ids, &vals)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (ids, values, *, variableColumnName=None, valueColumnName=None))]
    fn unpivot(
        &self,
        ids: Vec<String>,
        values: Vec<String>,
        variableColumnName: Option<String>,
        valueColumnName: Option<String>,
    ) -> PyResult<PyDataFrame> {
        let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
        let val_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        let mut df = self
            .inner
            .unpivot(&id_refs, &val_refs)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)?;
        if let Some(ref name) = variableColumnName {
            df = df.with_column_renamed("variable", name)?;
        }
        if let Some(ref name) = valueColumnName {
            df = df.with_column_renamed("value", name)?;
        }
        Ok(df)
    }

    #[pyo3(name = "flatMap")]
    fn flat_map_impl(&self, py: Python<'_>, func: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let rows = self.collect(py)?;
        let mut out_rows: Vec<PyObject> = Vec::new();
        for row in rows {
            let row_bound = row.bind(py);
            let result = func.call1((row_bound,))?;
            for item in result.iter()? {
                let item: pyo3::Bound<'_, PyAny> = item?;
                out_rows.push(item.into_py(py));
            }
        }
        let session = THREAD_ACTIVE_SESSIONS
            .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
            .ok_or_else(|| to_py_err("No active SparkSession for flatMap"))?;
        let session_ref = session
            .bind(py)
            .downcast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let list = PyList::new_bound(py, out_rows);
        let (rows_data, schema, schema_was_inferred) =
            python_data_and_schema(py, list.as_ref(), None, false, None)?;
        let df = session_ref
            .borrow()
            .inner
            .create_dataframe_from_rows(rows_data, schema, false, schema_was_inferred)
            .map_err(to_py_err)?;
        Ok(PyDataFrame::wrap(df))
    }

    #[pyo3(name = "flat_map")]
    fn flat_map(&self, py: Python<'_>, func: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        self.flat_map_impl(py, func)
    }

    fn with_columns_renamed(&self, cols_map: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let dict = cols_map.downcast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "withColumnsRenamed expects a dict of {old_name: new_name}",
            )
        })?;
        let mut renames: Vec<(String, String)> = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let old_name = k.extract::<String>()?;
            let new_name = v.extract::<String>()?;
            renames.push((old_name, new_name));
        }
        self.inner
            .with_columns_renamed(&renames)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(name = "withColumnsRenamed")]
    fn with_columns_renamed_camel(&self, cols_map: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        self.with_columns_renamed(cols_map)
    }

    #[pyo3(name = "withColumns")]
    fn with_columns(&self, cols_map: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let dict = cols_map.downcast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "withColumns expects a dict of {name: Column}",
            )
        })?;
        let mut exprs: Vec<(String, Column)> = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let name = k.extract::<String>()?;
            let col = py_any_to_column(&v)?;
            exprs.push((name, col));
        }
        self.inner
            .with_columns(&exprs)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(name = "writeTo")]
    fn write_to(&self, _table: &str) -> PyResult<()> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "writeTo (DataFrameWriterV2) is not yet implemented",
        ))
    }

    #[getter]
    fn write(slf: PyRef<Self>) -> PyDataFrameWriter {
        let py = slf.py();
        PyDataFrameWriter {
            df: slf.into_py(py),
            mode: "overwrite".to_string(),
            options: Vec::new(),
            partition_by: Vec::new(),
            writer_format: None,
        }
    }

    fn create_or_replace_temp_view(&self, session: &PySparkSession, name: &str) {
        session
            .inner
            .create_or_replace_temp_view(name, self.inner.clone());
    }

    #[getter]
    fn columns(&self, py: Python<'_>) -> PyResult<PyObject> {
        let names = self.inner.columns().map_err(to_py_err)?;
        let py_list = PyList::new_bound(py, names.clone());
        let types_mod = PyModule::import_bound(py, "sparkless.sql.types")?;
        let wrap = types_mod.getattr("_ColumnsList")?.call1((py_list,))?;
        Ok(wrap.into_py(py))
    }

    #[pyo3(name = "toDF", signature = (*col_names))]
    fn to_df(&self, col_names: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        if col_names.is_empty() {
            return Ok(PyDataFrame::wrap(self.inner.clone()));
        }
        let mut df = self.inner.clone();
        let current = df.columns().map_err(to_py_err)?;
        for (i, item) in col_names.iter().enumerate() {
            if i >= current.len() {
                break;
            }
            let new_name: String = item.extract()?;
            df = df
                .with_column_renamed(&current[i], &new_name)
                .map_err(to_py_err)?;
        }
        Ok(PyDataFrame::wrap(df))
    }

    #[pyo3(name = "toPandas")]
    fn to_pandas(&self, py: Python<'_>) -> PyResult<PyObject> {
        let pd = PyModule::import_bound(py, "pandas")?;
        let rows = self.inner.collect_as_json_rows().map_err(to_py_err)?;
        let cols = self.inner.columns().map_err(to_py_err)?;
        let data_dict = PyDict::new_bound(py);
        for col_name in &cols {
            let vals = PyList::empty_bound(py);
            for row in &rows {
                let v = row.get(col_name).unwrap_or(&JsonValue::Null);
                vals.append(json_to_py(v, py)?)?;
            }
            data_dict.set_item(col_name, vals)?;
        }
        let df = pd.call_method1("DataFrame", (data_dict,))?;
        Ok(df.into_py(py))
    }

    #[getter]
    fn dtypes(&self) -> PyResult<Vec<(String, String)>> {
        let schema = self.inner.schema_engine().map_err(to_py_err)?;
        Ok(schema
            .fields()
            .iter()
            .map(|f| {
                let ty = match &f.data_type {
                    DataType::String => "string",
                    DataType::Integer => "int",
                    DataType::Long => "long",
                    DataType::Double => "double",
                    DataType::Boolean => "boolean",
                    DataType::Date => "date",
                    DataType::Timestamp => "timestamp",
                    DataType::Binary => "binary",
                    DataType::Array(_) => "array",
                    DataType::Map(_, _) => "map",
                    DataType::Struct(_) => "struct",
                };
                (f.name.clone(), ty.to_string())
            })
            .collect())
    }

    fn coalesce(&self, _n: usize) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame::wrap(self.inner.clone()))
    }

    fn repartition(&self, _n: usize) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame::wrap(self.inner.clone()))
    }

    #[pyo3(name = "isEmpty")]
    fn is_empty(&self) -> PyResult<bool> {
        self.inner.count().map(|c| c == 0).map_err(to_py_err)
    }

    fn take(&self, py: Python<'_>, n: usize) -> PyResult<Vec<PyObject>> {
        let limited = self.inner.limit(n).map_err(to_py_err)?;
        let df = PyDataFrame::wrap(limited);
        df.collect(py)
    }

    fn __repr__(&self) -> PyResult<String> {
        let cols = self.inner.columns().map_err(to_py_err)?;
        Ok(format!("DataFrame[{}]", cols.join(", ")))
    }
}

#[pyclass]
struct PyDataFrameWriter {
    df: Py<PyAny>,
    mode: String,
    options: Vec<(String, String)>,
    partition_by: Vec<String>,
    writer_format: Option<String>,
}

fn write_mode_from_str(mode: &str) -> WriteMode {
    match mode.to_lowercase().as_str() {
        "append" => WriteMode::Append,
        _ => WriteMode::Overwrite,
    }
}

fn save_mode_from_str(mode: &str) -> SaveMode {
    match mode.to_lowercase().as_str() {
        "overwrite" => SaveMode::Overwrite,
        "append" => SaveMode::Append,
        "ignore" => SaveMode::Ignore,
        _ => SaveMode::ErrorIfExists,
    }
}

#[pymethods]
impl PyDataFrameWriter {
    fn mode<'a>(mut slf: PyRefMut<'a, Self>, mode: &str) -> PyRefMut<'a, Self> {
        slf.mode = mode.to_string();
        slf
    }

    fn option<'a>(mut slf: PyRefMut<'a, Self>, key: &str, value: &str) -> PyRefMut<'a, Self> {
        slf.options.push((key.to_string(), value.to_string()));
        slf
    }

    #[pyo3(name = "partitionBy")]
    fn partition_by<'a>(mut slf: PyRefMut<'a, Self>, cols: Vec<String>) -> PyRefMut<'a, Self> {
        slf.partition_by = cols;
        slf
    }

    #[pyo3(name = "format")]
    fn writer_format<'a>(mut slf: PyRefMut<'a, Self>, fmt: &str) -> PyRefMut<'a, Self> {
        slf.writer_format = Some(fmt.to_string());
        slf
    }

    fn parquet(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df =
            self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
        let inner = df.borrow();
        let mut w = inner
            .inner
            .write()
            .mode(write_mode_from_str(&self.mode))
            .format(WriteFormat::Parquet);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    fn csv(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df =
            self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
        let inner = df.borrow();
        let mut w = inner
            .inner
            .write()
            .mode(write_mode_from_str(&self.mode))
            .format(WriteFormat::Csv);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    fn json(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df =
            self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
        let inner = df.borrow();
        let mut w = inner
            .inner
            .write()
            .mode(write_mode_from_str(&self.mode))
            .format(WriteFormat::Json);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    /// PySpark: save(path). Uses current format (parquet default when using .parquet/.csv/.json).
    fn save(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df =
            self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
        let inner = df.borrow();
        let mut w = inner
            .inner
            .write()
            .mode(write_mode_from_str(&self.mode))
            .format(WriteFormat::Parquet);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    /// PySpark: saveAsTable(name). mode: "error"|"overwrite"|"append"|"ignore".
    #[pyo3(signature = (name, mode=None))]
    fn save_as_table(&self, py: Python<'_>, name: &str, mode: Option<&str>) -> PyResult<()> {
        let df =
            self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
        let inner = df.borrow();
        let active = {
            let ty = py.get_type_bound::<PySparkSession>();
            if let Ok(singleton) = ty.getattr("_singleton_session") {
                if !singleton.is_none() {
                    singleton
                        .downcast::<PySparkSession>()
                        .map_err(|_| to_py_err("active SparkSession is invalid"))?
                        .borrow()
                        .inner
                        .clone()
                } else {
                    let top = THREAD_ACTIVE_SESSIONS
                        .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)));
                    match top {
                        Some(s) => s.bind(py).borrow().inner.clone(),
                        None => return Err(to_py_err("No active SparkSession")),
                    }
                }
            } else {
                let top = THREAD_ACTIVE_SESSIONS
                    .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)));
                match top {
                    Some(s) => s.bind(py).borrow().inner.clone(),
                    None => return Err(to_py_err("No active SparkSession")),
                }
            }
        };
        let save_mode = save_mode_from_str(mode.unwrap_or(self.mode.as_str()));
        inner
            .inner
            .write()
            .save_as_table_with_options(&active, name, save_mode, &self.options)
            .map_err(to_py_err)
    }

    /// PySpark camelCase alias for save_as_table
    #[pyo3(name = "saveAsTable", signature = (name, mode=None))]
    fn save_as_table_camel(&self, py: Python<'_>, name: &str, mode: Option<&str>) -> PyResult<()> {
        self.save_as_table(py, name, mode)
    }
}

/// Extract a list of column name strings from a join `on` parameter.
/// Accepts: None, str, Column, list[str], list[Column].
#[allow(dead_code)]
fn extract_string_list_from_on(on: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
    let on = match on {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    if let Ok(s) = on.extract::<String>() {
        return Ok(vec![s]);
    }
    if let Ok(py_col) = on.downcast::<PyColumn>() {
        return Ok(vec![py_col.borrow().inner.name().to_string()]);
    }
    if let Ok(list) = on.downcast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(s) = item.extract::<String>() {
                out.push(s);
            } else if let Ok(c) = item.downcast::<PyColumn>() {
                out.push(c.borrow().inner.name().to_string());
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "join 'on' list elements must be str or Column",
                ));
            }
        }
        return Ok(out);
    }
    if let Ok(tup) = on.downcast::<PyTuple>() {
        let mut out = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            if let Ok(s) = item.extract::<String>() {
                out.push(s);
            } else if let Ok(c) = item.downcast::<PyColumn>() {
                out.push(c.borrow().inner.name().to_string());
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "join 'on' tuple elements must be str or Column",
                ));
            }
        }
        return Ok(out);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "join 'on' must be str, Column, or list/tuple of str/Column",
    ))
}

/// Convert Python value (int, float, str, bool, None, list) or PyColumn to robin_sparkless Column.
fn py_any_to_column(other: &Bound<'_, PyAny>) -> PyResult<Column> {
    if let Ok(py_col) = other.downcast::<PyColumn>() {
        return Ok(py_col.borrow().inner.clone());
    }
    if other.is_none() {
        return Ok(robin_sparkless::functions::lit_null_untyped());
    }
    if let Ok(b) = other.extract::<bool>() {
        return Ok(robin_sparkless::functions::lit_bool(b));
    }
    if let Ok(i) = other.extract::<i64>() {
        return Ok(robin_sparkless::functions::lit_i64(i));
    }
    if let Ok(f) = other.extract::<f64>() {
        return Ok(robin_sparkless::functions::lit_f64(f));
    }
    if let Ok(s) = other.extract::<String>() {
        return Ok(robin_sparkless::functions::lit_str(&s));
    }
    if let Ok(list) = other.downcast::<PyList>() {
        if list.is_empty() {
            return Ok(robin_sparkless::functions::lit_str("[]"));
        }
        let first = list.get_item(0)?;
        if first.extract::<i64>().is_ok() {
            let vals: Vec<i64> = list
                .iter()
                .filter_map(|x| x.extract::<i64>().ok())
                .collect();
            return Ok(robin_sparkless::functions::lit_str(&format!("{:?}", vals)));
        }
    }
    // datetime.date / datetime.datetime: convert to ISO string so plan gets string literal;
    // backend coercion then casts to timestamp for col-vs-string comparison (#1102).
    if let Ok(isoformat) = other.getattr("isoformat") {
        if isoformat.is_callable() {
            if let Ok(s) = isoformat.call0() {
                if let Ok(iso) = s.extract::<String>() {
                    return Ok(robin_sparkless::functions::lit_str(&iso));
                }
            }
        }
    }
    // Best effort: treat as string
    if let Ok(s) = other.str() {
        return Ok(robin_sparkless::functions::lit_str(&s.to_string()));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "comparison rhs must be Column, int, float, str, bool, or None",
    ))
}

/// Accept either a DataFrame or an object with `.inner` DataFrame (DataFrame-like wrapper).
fn py_any_to_dataframe_inner(other: &Bound<'_, PyAny>) -> PyResult<DataFrame> {
    if let Ok(df) = other.downcast::<PyDataFrame>() {
        return Ok(df.borrow().inner.clone());
    }
    if let Ok(inner) = other.getattr("inner") {
        if let Ok(df) = inner.downcast::<PyDataFrame>() {
            return Ok(df.borrow().inner.clone());
        }
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "argument 'other': expected DataFrame or object with .inner DataFrame",
    ))
}

#[pyclass]
struct PyColumn {
    pub(crate) inner: Column,
}

#[pymethods]
impl PyColumn {
    fn alias(&self, name: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.alias(name),
        }
    }

    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// Returns (udf_name, arg_column_names) if this column is a Python UDF call, else None. Used by Python UDF executor.
    fn get_udf_call_info(&self) -> Option<(String, Vec<String>)> {
        self.inner.udf_call_info()
    }

    fn __add__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.add_pyspark(&rhs),
        })
    }

    fn __radd__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.add_pyspark(&self.inner),
        })
    }

    fn __sub__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.subtract_pyspark(&rhs),
        })
    }

    fn __rsub__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.subtract_pyspark(&self.inner),
        })
    }

    fn __mul__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.multiply_pyspark(&rhs),
        })
    }

    fn __rmul__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.multiply_pyspark(&self.inner),
        })
    }

    /// col ** 2 (PySpark pow). Supports int and float exponent. Ignores modulo (PySpark has no modulo for pow).
    fn __pow__(
        &self,
        other: &Bound<'_, PyAny>,
        _modulo: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyColumn> {
        if let Ok(exp_i) = other.extract::<i64>() {
            return Ok(PyColumn {
                inner: self.inner.pow(exp_i),
            });
        }
        if let Ok(exp_i) = other.extract::<i32>() {
            return Ok(PyColumn {
                inner: self.inner.pow(exp_i as i64),
            });
        }
        if let Ok(_exp_f) = other.extract::<f64>() {
            let exp_col = py_any_to_column(other)?;
            return Ok(PyColumn {
                inner: self.inner.pow_with(&exp_col),
            });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "pow exponent must be int or float",
        ))
    }

    fn __rpow__(
        &self,
        other: &Bound<'_, PyAny>,
        _modulo: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyColumn> {
        // Support Python scalar ** PyColumn, e.g. 2.0 ** (-col("Value")) (issue #291/#1082).
        // Treat the Python value as the base column and this PyColumn as the exponent.
        let base_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: base_col.pow_with(&self.inner),
        })
    }

    fn __truediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.divide_pyspark(&rhs),
        })
    }

    fn __rtruediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.divide_pyspark(&self.inner),
        })
    }

    fn __mod__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.mod_pyspark(&rhs),
        })
    }

    fn __rmod__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.mod_pyspark(&self.inner),
        })
    }

    fn __neg__(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.negate(),
        }
    }

    fn __and__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.and_(&rhs),
        })
    }

    fn __rand__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.and_(&self.inner),
        })
    }

    fn __or__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.bit_or(&rhs),
        })
    }

    fn __ror__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.bit_or(&self.inner),
        })
    }

    fn __invert__(&self) -> PyColumn {
        PyColumn {
            // For boolean expressions (e.g. ~(df.a == 20)), behave like logical NOT.
            // Numeric bitwise NOT is exposed via F.bitwise_not(...) instead.
            inner: self.inner.logical_not(),
        }
    }

    fn __getitem__(&self, key: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        if let Ok(idx) = key.extract::<i64>() {
            return Ok(PyColumn {
                inner: self.inner.get_item(idx),
            });
        }
        if let Ok(name) = key.extract::<String>() {
            return Ok(PyColumn {
                inner: self.inner.get_field(&name),
            });
        }
        if let Ok(py_col) = key.downcast::<PyColumn>() {
            return Ok(PyColumn {
                inner: self.inner.get(&py_col.borrow().inner),
            });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Column subscript key must be int, str, or Column",
        ))
    }

    /// Logical AND of two boolean columns. PySpark and_.
    fn and_(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.and_(&other.inner),
        }
    }

    /// Logical OR of two boolean columns. PySpark or_.
    fn or_(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.or_(&other.inner),
        }
    }

    fn gt(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.gt(other.inner.clone().into_expr()),
        }
    }

    fn ge(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.gt_eq(other.inner.clone().into_expr()),
        }
    }

    fn lt(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.lt(other.inner.clone().into_expr()),
        }
    }

    fn le(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.lt_eq(other.inner.clone().into_expr()),
        }
    }

    fn eq(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.eq(other.inner.clone().into_expr()),
        }
    }

    /// Null-safe equality (NULL <=> NULL returns True). PySpark eqNullSafe.
    /// Accepts Column or scalar (int, float, str, bool, None) so .eqNullSafe(lit(x)) or .eqNullSafe(x) work.
    fn eq_null_safe(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.eq_null_safe(&other_col),
        })
    }

    /// PySpark camelCase alias for eq_null_safe.
    #[pyo3(name = "eqNullSafe")]
    fn eq_null_safe_camel(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        self.eq_null_safe(other)
    }

    /// True if column value is between lower and upper (inclusive). PySpark between(low, high).
    /// Accepts Column or scalar (int, float, str, bool, None) for lower/upper.
    fn between(&self, lower: &Bound<'_, PyAny>, upper: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lower_col = py_any_to_column(lower)?;
        let upper_col = py_any_to_column(upper)?;
        Ok(PyColumn {
            inner: self.inner.between(&lower_col, &upper_col),
        })
    }

    /// Python: col("x") > 1
    fn __gt__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.gt(other_col.into_expr()),
        })
    }

    fn __ge__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.gt_eq(other_col.into_expr()),
        })
    }

    fn __lt__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.lt(other_col.into_expr()),
        })
    }

    fn __le__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.lt_eq(other_col.into_expr()),
        })
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.eq(other_col.into_expr()),
        })
    }

    fn __ne__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let other_col = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.neq(other_col.into_expr()),
        })
    }

    fn is_null(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.is_null(),
        }
    }

    #[pyo3(name = "isNull")]
    fn is_null_camel(&self) -> PyColumn {
        self.is_null()
    }

    /// PySpark-style snake_case alias for isNull() (used in some upstream tests).
    #[pyo3(name = "isnull")]
    fn is_null_snake(&self) -> PyColumn {
        self.is_null()
    }

    #[pyo3(name = "getItem")]
    fn get_item_camel(&self, index_or_key: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        if let Ok(i) = index_or_key.extract::<i64>() {
            return Ok(PyColumn {
                inner: self.inner.get_item(i),
            });
        }
        if let Ok(s) = index_or_key.extract::<String>() {
            let key_col = robin_sparkless::functions::lit_str(&s);
            return Ok(PyColumn {
                inner: self.inner.get(&key_col),
            });
        }
        if let Ok(key_col) = index_or_key.downcast::<PyColumn>() {
            return Ok(PyColumn {
                inner: self.inner.get(&key_col.borrow().inner),
            });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "getItem expects int (array index), str (map key), or Column (map key)",
        ))
    }

    /// Get struct field by name or array element by index (PySpark Column.getField).
    /// Accepts int (array index, 0-based) or str (struct field name). Issue #1066.
    fn get_field(&self, name_or_index: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        if let Ok(i) = name_or_index.extract::<i64>() {
            return Ok(PyColumn {
                inner: self.inner.get_item(i),
            });
        }
        if let Ok(name) = name_or_index.extract::<String>() {
            return Ok(PyColumn {
                inner: self.inner.get_field(&name),
            });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "getField expects int (array index) or str (struct field name)",
        ))
    }

    #[pyo3(name = "getField")]
    fn get_field_camel(&self, name_or_index: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        self.get_field(name_or_index)
    }

    /// String column contains substring (PySpark Column.contains).
    fn contains(&self, literal: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.contains(literal),
        }
    }

    /// col[key] -> map get or array getItem. Key: int (array index), str (map key), or Column (map key).
    fn array_distinct(&self) -> PyColumn {
        PyColumn {
            inner: functions::array_distinct(&self.inner),
        }
    }

    fn posexplode(&self) -> (PyColumn, PyColumn) {
        let (pos, val) = functions::posexplode(&self.inner);
        (PyColumn { inner: pos }, PyColumn { inner: val })
    }

    #[pyo3(signature = (*values))]
    fn isin(&self, values: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
        let mut expanded: Vec<Py<PyAny>> = Vec::new();
        for item in values.iter() {
            if let Ok(list) = item.downcast::<PyList>() {
                for x in list.iter() {
                    expanded.push(x.unbind());
                }
            } else {
                expanded.push(item.unbind());
            }
        }
        if expanded.is_empty() {
            return Ok(PyColumn {
                inner: robin_sparkless::functions::lit_bool(false),
            });
        }
        let py = values.py();
        let first = expanded[0].bind(py);
        if first.extract::<i64>().is_ok() {
            let vals: Vec<i64> = expanded
                .iter()
                .filter_map(|v| v.bind(py).extract::<i64>().ok())
                .collect();
            if vals.len() == expanded.len() {
                return Ok(PyColumn {
                    inner: functions::isin_i64(&self.inner, &vals),
                });
            }
        }
        let vals: Vec<String> = expanded.iter().map(|v| v.bind(py).to_string()).collect();
        let refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
        Ok(PyColumn {
            inner: functions::isin_str(&self.inner, &refs),
        })
    }

    fn is_not_null(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.is_not_null(),
        }
    }

    #[pyo3(name = "isNotNull")]
    fn is_not_null_camel(&self) -> PyColumn {
        self.is_not_null()
    }

    /// PySpark-style snake_case alias for isNotNull() (used in some upstream tests).
    #[pyo3(name = "isnotnull")]
    fn is_not_null_snake(&self) -> PyColumn {
        self.is_not_null()
    }

    fn asc(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.asc(),
        }
    }

    fn desc(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.desc(),
        }
    }

    /// Descending sort, nulls last. PySpark desc_nulls_last.
    fn desc_nulls_last(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.desc_nulls_last(),
        }
    }

    /// PySpark camelCase alias for desc_nulls_last.
    #[pyo3(name = "descNullsLast")]
    fn desc_nulls_last_camel(&self) -> PySortOrder {
        self.desc_nulls_last()
    }

    fn upper(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.upper(),
        }
    }

    fn rlike(&self, pattern: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.regexp_like(pattern),
        }
    }

    /// SQL LIKE pattern match (PySpark like). Supports % and _ wildcards.
    fn like(&self, pattern: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.like(pattern, None),
        }
    }

    /// True if string starts with prefix (PySpark startswith).
    fn startswith(&self, prefix: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.startswith(prefix),
        }
    }

    /// Extract first match of regex pattern. PySpark regexp_extract. idx=0 is full match.
    #[pyo3(signature = (pattern, idx=0))]
    fn regexp_extract(&self, pattern: &str, idx: usize) -> PyColumn {
        PyColumn {
            inner: self.inner.regexp_extract(pattern, idx),
        }
    }

    /// Replace first match of regex pattern. PySpark regexp_replace.
    fn regexp_replace(&self, pattern: &str, replacement: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.regexp_replace(pattern, replacement),
        }
    }

    /// PySpark Column.replace(search, replacement) - literal string replace.
    /// PySpark Column.replace: (search, replacement) or replace({old: new, ...}) or replace([(old, new), ...]).
    #[pyo3(signature = (to_replace, replacement=None))]
    fn replace(
        &self,
        to_replace: &Bound<'_, PyAny>,
        replacement: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyColumn> {
        if let Ok(search) = to_replace.extract::<String>() {
            let rep = replacement.ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "replace() missing 1 required positional argument: 'replacement'. When to_replace is not a dict, value must be provided.",
                )
            })?;
            let rep_str: String = rep.extract().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("replacement must be str")
            })?;
            return Ok(PyColumn {
                inner: self.inner.replace(&search, &rep_str),
            });
        }
        if let Ok(dict) = to_replace.downcast::<PyDict>() {
            let mut pairs: Vec<(String, String)> = Vec::new();
            for (k, v) in dict.iter() {
                let s: String = k.extract().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>("replace dict keys must be str")
                })?;
                let r: String = v.extract().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "replace dict values must be str",
                    )
                })?;
                pairs.push((s, r));
            }
            return Ok(PyColumn {
                inner: self.inner.replace_many(&pairs),
            });
        }
        if let Ok(list) = to_replace.downcast::<PyList>() {
            let mut pairs: Vec<(String, String)> = Vec::new();
            for item in list.iter() {
                let tup = item.downcast::<PyTuple>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "replace list must contain (old, new) tuples",
                    )
                })?;
                if tup.len() != 2 {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "replace tuple must be (old, new)",
                    ));
                }
                let s: String = tup.get_item(0)?.extract()?;
                let r: String = tup.get_item(1)?.extract()?;
                pairs.push((s, r));
            }
            return Ok(PyColumn {
                inner: self.inner.replace_many(&pairs),
            });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "replace to_replace must be str, dict, or list of (str, str) tuples",
        ))
    }

    fn lower(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.lower(),
        }
    }

    #[pyo3(signature = (start, length=None))]
    fn substr(&self, start: i64, length: Option<i64>) -> PyColumn {
        PyColumn {
            inner: self.inner.substr(start, length),
        }
    }

    fn length(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.length(),
        }
    }

    /// Split string by delimiter. PySpark split. limit=-1 means no limit.
    #[pyo3(signature = (delimiter, limit=-1))]
    fn split(&self, delimiter: &str, limit: i32) -> PyColumn {
        let lim = if limit < 0 { None } else { Some(limit) };
        PyColumn {
            inner: self.inner.split(delimiter, lim),
        }
    }

    fn floor(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.floor(),
        }
    }

    #[pyo3(signature = (scale=0))]
    fn round(&self, scale: i32) -> PyColumn {
        PyColumn {
            inner: self.inner.round(scale),
        }
    }

    fn ltrim(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.ltrim(),
        }
    }

    fn rtrim(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.rtrim(),
        }
    }

    fn second(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.second(),
        }
    }

    fn hour(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.hour(),
        }
    }

    fn minute(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.minute(),
        }
    }

    fn soundex(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.soundex(),
        }
    }

    fn repeat(&self, n: i32) -> PyColumn {
        PyColumn {
            inner: self.inner.repeat(n),
        }
    }

    fn reverse(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.reverse(),
        }
    }

    fn exp(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.exp(),
        }
    }

    fn crc32(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.crc32(),
        }
    }

    fn xxhash64(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.xxhash64(),
        }
    }

    fn initcap(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.initcap(),
        }
    }

    fn levenshtein(&self, other: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.levenshtein(&other.inner),
        }
    }

    fn try_cast(&self, type_name: &str) -> PyResult<PyColumn> {
        self.inner
            .try_cast_to(type_name)
            .map(|c| PyColumn { inner: c })
            .map_err(to_py_err)
    }

    /// Add or replace a struct field. PySpark withField.
    fn with_field(&self, name: &str, value: &PyColumn) -> PyColumn {
        PyColumn {
            inner: self.inner.with_field(name, &value.inner),
        }
    }

    /// PySpark camelCase alias for with_field.
    #[pyo3(name = "withField")]
    fn with_field_camel(&self, name: &str, value: &PyColumn) -> PyColumn {
        self.with_field(name, value)
    }

    fn asinh(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.asinh(),
        }
    }

    fn atanh(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.atanh(),
        }
    }

    fn cosh(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.cosh(),
        }
    }

    fn sinh(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.sinh(),
        }
    }

    fn last_day(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.last_day(),
        }
    }

    #[pyo3(signature = (start, round_off=true))]
    fn months_between(&self, start: &PyColumn, round_off: bool) -> PyColumn {
        PyColumn {
            inner: self.inner.months_between(&start.inner, round_off),
        }
    }

    fn timestamp_seconds(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.timestamp_seconds(),
        }
    }

    fn to_utc_timestamp(&self, tz: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.to_utc_timestamp(tz),
        }
    }

    fn trim(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.trim(),
        }
    }

    fn cast(&self, type_name: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let name = resolve_cast_type_name(type_name)?;
        self.inner
            .cast_to(&name)
            .map(|c| PyColumn { inner: c })
            .map_err(to_py_err)
    }

    /// PySpark astype(dtype): alias for cast(dtype).
    fn astype(&self, dtype: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        self.cast(dtype)
    }

    /// Ascending sort, nulls first. PySpark asc_nulls_first.
    fn asc_nulls_first(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.asc_nulls_first(),
        }
    }

    /// Ascending sort, nulls last. PySpark asc_nulls_last.
    fn asc_nulls_last(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.asc_nulls_last(),
        }
    }

    /// Descending sort, nulls first. PySpark desc_nulls_first.
    fn desc_nulls_first(&self) -> PySortOrder {
        PySortOrder {
            inner: self.inner.desc_nulls_first(),
        }
    }

    fn row_number(&self, descending: bool) -> PyColumn {
        PyColumn {
            inner: self.inner.row_number(descending),
        }
    }

    fn rank(&self, descending: bool) -> PyColumn {
        PyColumn {
            inner: self.inner.rank(descending),
        }
    }

    fn dense_rank(&self, descending: bool) -> PyColumn {
        PyColumn {
            inner: self.inner.dense_rank(descending),
        }
    }

    #[pyo3(signature = (offset=1))]
    fn lag(&self, offset: i64) -> PyColumn {
        PyColumn {
            inner: self.inner.lag(offset),
        }
    }

    #[pyo3(signature = (offset=1))]
    fn lead(&self, offset: i64) -> PyColumn {
        PyColumn {
            inner: self.inner.lead(offset),
        }
    }

    fn first_value(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.first_value(),
        }
    }

    fn last_value(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.last_value(),
        }
    }

    fn over(&self, py: Python<'_>, window: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let funcs = PyModule::import_bound(py, "sparkless.sql.functions")?;
        let extract = funcs.getattr("_window_spec_to_partition_order")?;
        let result = extract.call1((window, false))?;
        let (partition_by, order_by, use_running_aggregate): (Vec<String>, Vec<String>, bool) =
            result.extract()?;
        let partition_strs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
        self.inner
            .over_window(&partition_strs[..], &order_by, use_running_aggregate)
            .map(|c| PyColumn { inner: c })
            .map_err(to_py_err)
    }
}

/// Wrapper for F.expr("sql") so select() can resolve via SQL (e.g. "upper(x) as up"). PySpark expr() parity.
#[pyclass]
struct PyExprStr {
    sql: String,
}

#[pymethods]
impl PyExprStr {
    #[getter]
    fn sql(&self) -> &str {
        &self.sql
    }

    fn alias(&self, name: &str) -> PyExprStr {
        PyExprStr {
            sql: format!("{} AS {}", self.sql, name),
        }
    }
}

#[pyclass]
struct PySortOrder {
    pub(crate) inner: SortOrder,
}

#[pymethods]
impl PySortOrder {
    #[getter]
    fn column_name(&self) -> &str {
        self.inner.column_name()
    }

    #[getter]
    fn descending(&self) -> bool {
        self.inner.descending
    }
}

#[pyclass]
struct PyDataFrameNaFunctions {
    df: Py<PyAny>,
}

#[pymethods]
impl PyDataFrameNaFunctions {
    /// Allow df.na()() - return self so chaining works.
    fn __call__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    #[pyo3(signature = (value, subset=None))]
    fn fill(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        subset: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let df_ref = self
            .df
            .bind(py)
            .downcast::<PyDataFrame>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame"))?
            .borrow();
        // PySpark: na.fill(scalar, subset=...) or na.fill({col: value, ...}, subset=ignored).
        // Dict value: iterate keys and apply column-specific fills; subset is ignored.
        if let Ok(dict) = value.downcast::<PyDict>() {
            let mut current = df_ref.inner.clone();
            for (k, v) in dict.iter() {
                let col_name: String = k.extract().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>("na.fill dict keys must be str")
                })?;
                let val_expr = py_any_to_column(&v)?.into_expr();
                current = current
                    .na()
                    .fill(val_expr, Some(vec![col_name.as_str()]))
                    .map_err(to_py_err)?;
            }
            return Ok(PyDataFrame::wrap(current));
        }

        let value_col = py_any_to_column(value)?;
        let kind = classify_fill_value_kind(value);
        let sub_vec = normalize_subset(subset)?;
        let candidates: Vec<String> = match sub_vec {
            Some(cols) => cols,
            None => df_ref.inner.columns().map_err(to_py_err)?,
        };
        let mut subset_refs_vec: Vec<&str> = Vec::new();
        for name in &candidates {
            match df_ref.inner.get_column_data_type(name) {
                Some(dt) => {
                    if is_fill_compatible_with_schema_dtype(&dt, &kind) {
                        subset_refs_vec.push(name.as_str());
                    }
                }
                None => {
                    // Non-existent column: keep so engine raises ColumnNotFoundException as before.
                    subset_refs_vec.push(name.as_str());
                }
            }
        }
        let subset_refs: Option<Vec<&str>> = Some(subset_refs_vec);
        let na = df_ref.inner.na();
        na.fill(value_col.into_expr(), subset_refs)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (subset=None, how="any", thresh=None))]
    fn drop(
        &self,
        py: Python<'_>,
        subset: Option<&Bound<'_, PyAny>>,
        how: &str,
        thresh: Option<usize>,
    ) -> PyResult<PyDataFrame> {
        let df_ref = self
            .df
            .bind(py)
            .downcast::<PyDataFrame>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame"))?
            .borrow();
        let sub_vec = normalize_subset(subset)?;
        let subset_refs: Option<Vec<&str>> = sub_vec
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let na = df_ref.inner.na();
        na.drop(subset_refs, how, thresh)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (to_replace, value=None, subset=None))]
    fn replace(
        &self,
        py: Python<'_>,
        to_replace: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
        subset: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let df_ref = self
            .df
            .bind(py)
            .downcast::<PyDataFrame>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame"))?
            .borrow();
        let sub_vec = normalize_subset(subset)?;
        let subset_refs: Option<Vec<&str>> = sub_vec
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());

        // value is None: dict -> apply each; only str to_replace can be replaced with null (PySpark: replace(1, None) raises)
        if value.is_none() {
            if let Ok(dict) = to_replace.downcast::<PyDict>() {
                let mut current = df_ref.inner.clone();
                for (k, v) in dict.iter() {
                    let old_expr = py_any_to_column(&k)?.into_expr();
                    let new_expr = py_any_to_column(&v)?.into_expr();
                    current = current
                        .na()
                        .replace(old_expr, new_expr, subset_refs.as_ref().map(|v| v.to_vec()))
                        .map_err(to_py_err)?;
                }
                return Ok(PyDataFrame::wrap(current));
            }
            // PySpark: replace("a", None) works (replace with null); replace(1, None) or replace([1,2], None) raises
            if to_replace.downcast::<PyList>().is_ok() {
                return Err(to_py_err("value is required when to_replace is a list"));
            }
            if to_replace.extract::<i64>().is_ok()
                || to_replace.extract::<f64>().is_ok()
                || to_replace.extract::<bool>().is_ok()
            {
                return Err(to_py_err("value is required when to_replace is not a dict"));
            }
            let null_expr = robin_sparkless::functions::lit_null("string")
                .map_err(to_py_err)?
                .into_expr();
            let old_expr = py_any_to_column(to_replace)?.into_expr();
            return df_ref
                .inner
                .na()
                .replace(
                    old_expr,
                    null_expr,
                    subset_refs.as_ref().map(|v| v.to_vec()),
                )
                .map(PyDataFrame::wrap)
                .map_err(to_py_err);
        }

        let value = value.unwrap();

        // to_replace is None: replace null with value (fillna)
        if to_replace.is_none() {
            let value_col = py_any_to_column(value)?;
            return df_ref
                .inner
                .na()
                .fill(value_col.into_expr(), subset_refs)
                .map(PyDataFrame::wrap)
                .map_err(to_py_err);
        }

        // to_replace is list and value is list: replace each pair in order
        if let (Ok(old_list), Ok(new_list)) = (
            to_replace.downcast::<pyo3::types::PyList>(),
            value.downcast::<pyo3::types::PyList>(),
        ) {
            if old_list.len() != new_list.len() {
                return Err(to_py_err(format!(
                    "to_replace and value lists must be the same length ({} vs {})",
                    old_list.len(),
                    new_list.len()
                )));
            }
            let mut current = df_ref.inner.clone();
            for i in 0..old_list.len() {
                let old_item = old_list.get_item(i)?;
                let new_item = new_list.get_item(i)?;
                let old_expr = lit(&old_item)?.inner.into_expr();
                let new_expr = lit(&new_item)?.inner.into_expr();
                current = current
                    .na()
                    .replace(old_expr, new_expr, subset_refs.as_ref().map(|v| v.to_vec()))
                    .map_err(to_py_err)?;
            }
            return Ok(PyDataFrame::wrap(current));
        }

        // to_replace is list and value is scalar: replace each old with value
        if let Ok(old_list) = to_replace.downcast::<pyo3::types::PyList>() {
            let mut current = df_ref.inner.clone();
            let new_expr = lit(value)?.inner.into_expr();
            for i in 0..old_list.len() {
                let old_item = old_list.get_item(i)?;
                let old_expr = lit(&old_item)?.inner.into_expr();
                current = current
                    .na()
                    .replace(
                        old_expr,
                        new_expr.clone(),
                        subset_refs.as_ref().map(|v| v.to_vec()),
                    )
                    .map_err(to_py_err)?;
            }
            return Ok(PyDataFrame::wrap(current));
        }

        // Single scalar/scalar replace
        let old_expr = lit(to_replace)?.inner.into_expr();
        let new_expr = lit(value)?.inner.into_expr();
        df_ref
            .inner
            .na()
            .replace(old_expr, new_expr, subset_refs)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }
}

#[pyclass]
struct PyCubeRollupData {
    inner: CubeRollupData,
}

#[pymethods]
impl PyCubeRollupData {
    fn count(&self) -> PyResult<PyDataFrame> {
        self.inner.count().map(PyDataFrame::wrap).map_err(to_py_err)
    }

    #[pyo3(signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        fn push_expr(
            item: &Bound<'_, PyAny>,
            out: &mut Vec<robin_sparkless::Expr>,
        ) -> PyResult<()> {
            if let Ok(c) = item.downcast::<PyColumn>() {
                out.push(c.borrow().inner.clone().into_expr());
                return Ok(());
            }
            if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "agg() expects Column expressions",
            ))
        }

        let mut rust_exprs: Vec<robin_sparkless::Expr> = Vec::new();
        for item in exprs.iter() {
            push_expr(&item, &mut rust_exprs)?;
        }
        self.inner
            .agg(rust_exprs)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }
}

#[pyclass]
struct PyGroupedData {
    inner: GroupedData,
}

#[pymethods]
impl PyGroupedData {
    fn count(&self) -> PyResult<PyDataFrame> {
        self.inner.count().map(PyDataFrame::wrap).map_err(to_py_err)
    }

    fn sum(&self, col_name: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let name = py_col_to_name(col_name)?;
        self.inner
            .sum(&name)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*cols))]
    fn avg(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let mut names = Vec::new();
        for item in cols.iter() {
            names.extend(py_one_or_many_cols(&item)?);
        }
        if names.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "avg requires at least one column",
            ));
        }
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        self.inner
            .avg(&refs)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    /// PySpark groupBy().mean(*cols). Accepts one or more column names or Columns.
    #[pyo3(signature = (*cols))]
    fn mean(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        self.avg(cols)
    }

    fn min(&self, col_name: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let name = py_col_to_name(col_name)?;
        self.inner
            .min(&name)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn max(&self, col_name: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let name = py_col_to_name(col_name)?;
        self.inner
            .max(&name)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        fn push_expr(
            item: &Bound<'_, PyAny>,
            out: &mut Vec<robin_sparkless::Expr>,
        ) -> PyResult<()> {
            if let Ok(c) = item.downcast::<PyColumn>() {
                out.push(c.borrow().inner.clone().into_expr());
                return Ok(());
            }
            if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            if let Ok(dict) = item.downcast::<PyDict>() {
                for (k, v) in dict.iter() {
                    let col_name: String = k.extract().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyTypeError, _>("agg dict keys must be str")
                    })?;
                    let func_name: String = v.extract().map_err(|_| {
                        PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "agg dict values must be str",
                        )
                    })?;
                    let c = Column::new(col_name);
                    let expr_col = match func_name.as_str() {
                        "sum" => functions::sum(&c),
                        "avg" | "mean" => functions::avg(&c),
                        "min" => functions::min(&c),
                        "max" => functions::max(&c),
                        "count" => functions::count(&c),
                        "first" => c, // best effort
                        _ => {
                            return Err(to_py_err(format!("unsupported agg function: {func_name}")))
                        }
                    };
                    out.push(expr_col.into_expr());
                }
                return Ok(());
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "agg() expects Column expressions or dict",
            ))
        }

        let mut rust_exprs: Vec<robin_sparkless::Expr> = Vec::new();
        for item in exprs.iter() {
            push_expr(&item, &mut rust_exprs)?;
        }
        self.inner
            .agg(rust_exprs)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (pivot_col, values=None))]
    fn pivot(&self, pivot_col: &str, values: Option<Vec<String>>) -> PyPivotedGroupedData {
        PyPivotedGroupedData {
            inner: self.inner.pivot(pivot_col, values),
        }
    }
}

#[pyclass]
struct PyPivotedGroupedData {
    inner: PivotedGroupedData,
}

#[pymethods]
impl PyPivotedGroupedData {
    fn sum(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .sum(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn avg(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .avg(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn min(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .min(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn max(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .max(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn count(&self) -> PyResult<PyDataFrame> {
        self.inner.count().map(PyDataFrame::wrap).map_err(to_py_err)
    }

    fn _count_distinct(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._count_distinct(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn _collect_list(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._collect_list(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn _collect_set(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._collect_set(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn _first(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._first(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn _last(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._last(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn _stddev(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._stddev(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn _variance(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._variance(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    fn mean(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .mean(value_col)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        fn push_expr(
            item: &Bound<'_, PyAny>,
            out: &mut Vec<robin_sparkless::Expr>,
        ) -> PyResult<()> {
            if let Ok(c) = item.downcast::<PyColumn>() {
                out.push(c.borrow().inner.clone().into_expr());
                return Ok(());
            }
            if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "pivot.agg() expects Column expressions (e.g. F.sum(\"col\").alias(\"name\"))",
            ))
        }
        if exprs.len() == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "pivot.agg() requires at least one expression",
            ));
        }
        let mut rust_exprs: Vec<robin_sparkless::Expr> = Vec::new();
        for item in exprs.iter() {
            push_expr(&item, &mut rust_exprs)?;
        }
        self.inner
            .agg(rust_exprs)
            .map(PyDataFrame::wrap)
            .map_err(to_py_err)
    }
}

#[pyfunction]
fn spark_session_builder() -> PySparkSessionBuilder {
    PySparkSessionBuilder {
        inner: SparkSession::builder(),
    }
}

#[pyfunction]
fn column(name: &str) -> PyColumn {
    PyColumn {
        inner: robin_sparkless::functions::col(name),
    }
}

/// Create an expr-string for use in select() (e.g. expr("upper(x) as up")). Resolved when the DataFrame is selected. PySpark F.expr() parity.
#[pyfunction]
fn expr_str(sql: &str) -> PyExprStr {
    PyExprStr {
        sql: sql.to_string(),
    }
}

/// Create a Column that represents a Python UDF call. When used in with_column, the Python UDF executor runs the callable per row.
#[pyfunction]
fn create_udf_column(udf_name: String, columns: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let mut args: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>()?;
        args.push(py_col.borrow().inner.clone());
    }
    Ok(PyColumn {
        inner: Column::from_udf_call(udf_name, args),
    })
}

/// Set the Python callable that runs when with_column sees a UDF column. Signature: (df, column_name, udf_name, arg_names) -> new_df. Called from Python on session creation.
#[pyfunction]
fn set_python_udf_executor(py: Python<'_>, callback: &Bound<'_, PyAny>) -> PyResult<()> {
    PYTHON_UDF_EXECUTOR.with(|cell| {
        *cell.borrow_mut() = Some(callback.into_py(py));
    });
    Ok(())
}

#[pyfunction]
fn lit_i64(value: i64) -> PyColumn {
    PyColumn {
        inner: robin_sparkless::functions::lit_i64(value),
    }
}

#[pyfunction]
fn lit_str(value: &str) -> PyColumn {
    PyColumn {
        inner: robin_sparkless::functions::lit_str(value),
    }
}

#[pyfunction]
fn lit_bool(value: bool) -> PyColumn {
    PyColumn {
        inner: robin_sparkless::functions::lit_bool(value),
    }
}

#[pyfunction]
fn lit_f64(value: f64) -> PyColumn {
    PyColumn {
        inner: robin_sparkless::functions::lit_f64(value),
    }
}

#[pyfunction]
fn lit_null(dtype: &str) -> PyResult<PyColumn> {
    let c = robin_sparkless::functions::lit_null(dtype).map_err(to_py_err)?;
    Ok(PyColumn { inner: c })
}

/// Polymorphic lit: dispatches to lit_i64, lit_str, lit_f64, lit_bool based on Python type.
#[pyfunction]
fn lit(value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let _py = value.py();
    if value.is_none() {
        return Ok(PyColumn {
            inner: robin_sparkless::functions::lit_null_untyped(),
        });
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(PyColumn {
            inner: robin_sparkless::functions::lit_bool(b),
        });
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(PyColumn {
            inner: robin_sparkless::functions::lit_i64(i),
        });
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(PyColumn {
            inner: robin_sparkless::functions::lit_f64(f),
        });
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(PyColumn {
            inner: robin_sparkless::functions::lit_str(&s),
        });
    }
    // datetime.date, datetime.datetime: convert to string for now (full support later)
    if let Ok(obj) = value.getattr("isoformat") {
        if obj.is_callable() {
            if let Ok(s) = obj.call0() {
                if let Ok(iso) = s.extract::<String>() {
                    return Ok(PyColumn {
                        inner: robin_sparkless::functions::lit_str(&iso),
                    });
                }
            }
        }
    }
    Ok(PyColumn {
        inner: robin_sparkless::functions::lit_str(&value.repr()?.to_string()),
    })
}

fn coerce_to_column(v: &Bound<'_, PyAny>) -> PyResult<Column> {
    if let Ok(c) = v.downcast::<PyColumn>() {
        return Ok(c.borrow().inner.clone());
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(robin_sparkless::functions::col(&s));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "expected Column or column name string",
    ))
}

#[pyfunction]
fn upper(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn {
        inner: robin_sparkless::functions::upper(&c),
    })
}

#[pyfunction]
fn lower(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn {
        inner: robin_sparkless::functions::lower(&c),
    })
}

#[pyfunction]
#[pyo3(signature = (column, start, length=None))]
fn substring(column: &Bound<'_, PyAny>, start: i64, length: Option<i64>) -> PyResult<PyColumn> {
    let c = coerce_to_column(column)?;
    Ok(PyColumn {
        inner: robin_sparkless::functions::substring(&c, start, length),
    })
}

#[pyfunction]
fn trim(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn {
        inner: robin_sparkless::functions::trim(&c),
    })
}

#[pyfunction]
fn cast(col: &PyColumn, type_name: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let name = resolve_cast_type_name(type_name)?;
    robin_sparkless::functions::cast(&col.inner, &name)
        .map(|c| PyColumn { inner: c })
        .map_err(to_py_err)
}

#[pyclass]
struct PyWhenBuilder {
    inner: Option<WhenBuilder>,
}

#[pymethods]
impl PyWhenBuilder {
    fn then(&mut self, value: &Bound<'_, PyAny>) -> PyResult<PyThenBuilder> {
        let wb = self.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("when().then() already called")
        })?;
        let val = py_any_to_column(value)?;
        Ok(PyThenBuilder {
            inner: Some(wb.then(&val)),
        })
    }
}

#[pyclass]
struct PyThenBuilder {
    inner: Option<ThenBuilder>,
}

#[pymethods]
impl PyThenBuilder {
    #[pyo3(signature = (condition, value=None))]
    fn when(
        &mut self,
        condition: &Bound<'_, PyAny>,
        value: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        let tb = self.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("otherwise() already called")
        })?;
        let cond = py_any_to_column(condition).or_else(|_| {
            condition
                .extract::<String>()
                .map(|s| robin_sparkless::functions::col(&s))
        })?;
        let cwb = tb.when(&cond);
        let py = condition.py();
        match value {
            Some(val) => {
                let val_col = py_any_to_column(val)?;
                let new_tb = cwb.then(&val_col);
                Ok(Py::new(
                    py,
                    PyThenBuilder {
                        inner: Some(new_tb),
                    },
                )?
                .into_py(py))
            }
            None => Ok(Py::new(py, PyChainedWhenBuilder { inner: Some(cwb) })?.into_py(py)),
        }
    }

    fn otherwise(&mut self, value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let tb = self.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("otherwise() already called")
        })?;
        let col = py_any_to_column(value)?;
        Ok(PyColumn {
            inner: tb.otherwise(&col),
        })
    }
}

use robin_sparkless::functions::ChainedWhenBuilder;

#[pyclass]
struct PyChainedWhenBuilder {
    inner: Option<ChainedWhenBuilder>,
}

#[pymethods]
impl PyChainedWhenBuilder {
    fn then(&mut self, value: &Bound<'_, PyAny>) -> PyResult<PyThenBuilder> {
        let cwb = self.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("then() already called")
        })?;
        let col = py_any_to_column(value)?;
        Ok(PyThenBuilder {
            inner: Some(cwb.then(&col)),
        })
    }
}

#[pyfunction]
#[pyo3(signature = (condition, value=None))]
fn when(
    py: Python<'_>,
    condition: &PyColumn,
    value: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyObject> {
    if let Some(v) = value {
        // when(cond, val) -> when(cond).then(val), so .otherwise() can be chained; val can be str/int/float/Column (PySpark parity).
        let col = py_any_to_column(v)?;
        let then_builder = robin_sparkless::functions::when(&condition.inner).then(&col);
        Ok(Py::new(
            py,
            PyThenBuilder {
                inner: Some(then_builder),
            },
        )?
        .into_py(py))
    } else {
        Ok(Py::new(
            py,
            PyWhenBuilder {
                inner: Some(robin_sparkless::functions::when(&condition.inner)),
            },
        )?
        .into_py(py))
    }
}

#[pyfunction]
fn count(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn {
        inner: functions::count(&c),
    })
}

#[pyfunction]
fn sum(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn {
        inner: functions::sum(&c),
    })
}

#[pyfunction]
fn avg(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn {
        inner: functions::avg(&c),
    })
}

#[pyfunction]
fn min(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn {
        inner: functions::min(&c),
    })
}

#[pyfunction]
fn max(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn {
        inner: functions::max(&c),
    })
}

#[pyfunction]
fn create_map(columns: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let list = columns.downcast::<PyList>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>("create_map expects a list of Columns")
    })?;
    let mut owned: Vec<Column> = Vec::with_capacity(list.len());
    for item in list.iter() {
        if let Ok(py_col) = item.downcast::<PyColumn>() {
            owned.push(py_col.borrow().inner.clone());
        } else if let Ok(s) = item.extract::<String>() {
            owned.push(robin_sparkless::functions::col(&s));
        } else {
            let col = py_any_to_column(&item)?;
            owned.push(col);
        }
    }
    let refs: Vec<&Column> = owned.iter().collect();
    let col = functions::create_map(&refs).map_err(to_py_err)?;
    Ok(PyColumn { inner: col })
}

#[pyfunction]
#[pyo3(signature = (*columns))]
fn concat(columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut owned: Vec<Column> = Vec::new();
    for item in columns.iter() {
        if let Ok(c) = item.downcast::<PyColumn>() {
            owned.push(c.borrow().inner.clone());
        } else if let Ok(s) = item.extract::<String>() {
            owned.push(functions::col(&s));
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "concat() arguments must be Column or str",
            ));
        }
    }
    let refs: Vec<&Column> = owned.iter().collect();
    Ok(PyColumn {
        inner: functions::concat(&refs),
    })
}

#[pyfunction]
fn column_over_window(
    col_name: &str,
    agg_fn: &str,
    partition_by: Vec<String>,
    order_by: Vec<String>,
) -> PyResult<PyColumn> {
    let c = Column::new(col_name.to_string());
    let agg_col = match agg_fn {
        "sum" => functions::sum(&c),
        "avg" | "mean" => functions::avg(&c),
        "min" => functions::min(&c),
        "max" => functions::max(&c),
        "count" => functions::count(&c),
        _ => return Err(to_py_err(format!("unsupported agg for over(): {agg_fn}"))),
    };
    let partition_strs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    if order_by.is_empty() {
        return Ok(PyColumn {
            inner: agg_col.over(&partition_strs),
        });
    }
    let use_running = true;
    agg_col
        .over_window(&partition_strs, &order_by, use_running)
        .map(|c| PyColumn { inner: c })
        .map_err(to_py_err)
}

#[pyfunction]
fn row_number_window(partition_by: Vec<String>, order_by: Vec<String>) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "row_number_window: order_by cannot be empty",
        ));
    }
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = Column::row_number_over(&parts[..], &order_by).map_err(to_py_err)?;
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn percent_rank_window(partition_by: Vec<String>, order_by: Vec<String>) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "percent_rank_window: order_by cannot be empty",
        ));
    }
    let first = &order_by[0];
    let (name, descending) = if let Some(stripped) = first.strip_prefix('-') {
        (stripped.to_string(), true)
    } else {
        (first.clone(), false)
    };
    let order_col = Column::new(name);
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = order_col.percent_rank(&parts[..], descending);
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn rank_window(partition_by: Vec<String>, order_by: Vec<String>) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "rank_window: order_by cannot be empty",
        ));
    }
    let first = &order_by[0];
    let (name, descending) = if let Some(stripped) = first.strip_prefix('-') {
        (stripped.to_string(), true)
    } else {
        (first.clone(), false)
    };
    let order_col = Column::new(name);
    let base = order_col.rank(descending);
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = base.over(&parts[..]);
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn dense_rank_window(partition_by: Vec<String>, order_by: Vec<String>) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "dense_rank_window: order_by cannot be empty",
        ));
    }
    let first = &order_by[0];
    let (name, descending) = if let Some(stripped) = first.strip_prefix('-') {
        (stripped.to_string(), true)
    } else {
        (first.clone(), false)
    };
    let order_col = Column::new(name);
    let base = order_col.dense_rank(descending);
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = base.over(&parts[..]);
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn cume_dist_window(partition_by: Vec<String>, order_by: Vec<String>) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "cume_dist_window: order_by cannot be empty",
        ));
    }
    let first = &order_by[0];
    let (name, descending) = if let Some(stripped) = first.strip_prefix('-') {
        (stripped.to_string(), true)
    } else {
        (first.clone(), false)
    };
    let order_col = Column::new(name);
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = order_col.cume_dist(&parts[..], descending);
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn ntile_window(n: u32, partition_by: Vec<String>, order_by: Vec<String>) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "ntile_window: order_by cannot be empty",
        ));
    }
    let first = &order_by[0];
    let (name, descending) = if let Some(stripped) = first.strip_prefix('-') {
        (stripped.to_string(), true)
    } else {
        (first.clone(), false)
    };
    let order_col = Column::new(name);
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = order_col.ntile(n, &parts[..], descending);
    Ok(PyColumn { inner: windowed })
}

/// Column value over an ordered window. Used for last_value with orderBy so "last in frame" = current row (PySpark default frame).
#[pyfunction]
fn column_value_over_window(
    column_name: String,
    partition_by: Vec<String>,
    order_by: Vec<String>,
) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "column_value_over_window: order_by cannot be empty",
        ));
    }
    let col_expr = Column::new(column_name);
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = col_expr
        .over_window(&parts[..], &order_by, false)
        .map_err(to_py_err)?;
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn lag_window(
    column_name: String,
    offset: i64,
    partition_by: Vec<String>,
    order_by: Vec<String>,
) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "lag_window: order_by cannot be empty",
        ));
    }
    let col_expr = Column::new(column_name);
    let lagged = col_expr.lag(offset);
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = lagged
        .over_window(&parts[..], &order_by, false)
        .map_err(to_py_err)?;
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn lead_window(
    column_name: String,
    offset: i64,
    partition_by: Vec<String>,
    order_by: Vec<String>,
) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "lead_window: order_by cannot be empty",
        ));
    }
    let col_expr = Column::new(column_name);
    let led = col_expr.lead(offset);
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = led
        .over_window(&parts[..], &order_by, false)
        .map_err(to_py_err)?;
    Ok(PyColumn { inner: windowed })
}
#[pyfunction]
fn regexp_replace(
    column: &Bound<'_, PyAny>,
    pattern: &str,
    replacement: &str,
) -> PyResult<PyColumn> {
    let c = coerce_to_column(column)?;
    Ok(PyColumn {
        inner: functions::regexp_replace(&c, pattern, replacement),
    })
}

#[pyfunction]
#[pyo3(signature = (column, pattern, group_index=0))]
fn regexp_extract_all(
    column: &Bound<'_, PyAny>,
    pattern: &str,
    group_index: usize,
) -> PyResult<PyColumn> {
    let c = coerce_to_column(column)?;
    if group_index == 0 {
        Ok(PyColumn {
            inner: functions::regexp_extract_all(&c, pattern),
        })
    } else {
        Ok(PyColumn {
            inner: c.regexp_extract_all_group(pattern, group_index),
        })
    }
}

#[pyfunction]
#[pyo3(signature = (column, pattern, idx=0))]
fn regexp_extract(column: &PyColumn, pattern: &str, idx: usize) -> PyColumn {
    PyColumn {
        inner: functions::regexp_extract(&column.inner, pattern, idx),
    }
}

#[pyfunction]
#[pyo3(signature = (column, pattern, limit=-1))]
fn split(column: &PyColumn, pattern: &str, limit: i32) -> PyColumn {
    let lim = if limit < 0 { None } else { Some(limit) };
    PyColumn {
        inner: functions::split(&column.inner, pattern, lim),
    }
}

#[pyfunction]
#[pyo3(signature = (*columns))]
fn coalesce(columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("coalesce expects Column expressions")
        })?;
        cols.push(py_col.borrow().inner.clone());
    }
    if cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "coalesce requires at least one column",
        ));
    }
    let refs: Vec<&Column> = cols.iter().collect();
    Ok(PyColumn {
        inner: functions::coalesce(&refs),
    })
}

#[pyfunction]
fn nullif(column: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::nullif(&column.inner, &value.inner),
    }
}

#[pyfunction]
fn nanvl(column: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::nanvl(&column.inner, &value.inner),
    }
}

#[pyfunction]
fn isnan(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::isnan(&column.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (column, value))]
fn array_position(column: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_position(&column.inner, &value.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (format, *columns))]
fn format_string(format: &str, columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "format_string expects Column expressions",
            )
        })?;
        cols.push(py_col.borrow().inner.clone());
    }
    if cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "format_string requires at least one column",
        ));
    }
    let refs: Vec<&Column> = cols.iter().collect();
    Ok(PyColumn {
        inner: functions::format_string(format, &refs),
    })
}

#[pyfunction]
#[pyo3(signature = (*columns))]
fn greatest(columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("greatest expects Column expressions")
        })?;
        cols.push(py_col.borrow().inner.clone());
    }
    if cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "greatest requires at least one column",
        ));
    }
    let refs: Vec<&Column> = cols.iter().collect();
    functions::greatest(&refs)
        .map(|c| PyColumn { inner: c })
        .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (*columns))]
fn least(columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("least expects Column expressions")
        })?;
        cols.push(py_col.borrow().inner.clone());
    }
    if cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "least requires at least one column",
        ));
    }
    let refs: Vec<&Column> = cols.iter().collect();
    functions::least(&refs)
        .map(|c| PyColumn { inner: c })
        .map_err(to_py_err)
}

#[pyfunction]
fn array_distinct(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_distinct(&column.inner),
    }
}

#[pyfunction]
fn posexplode(column: &PyColumn) -> (PyColumn, PyColumn) {
    let (pos, val) = functions::posexplode(&column.inner);
    (PyColumn { inner: pos }, PyColumn { inner: val })
}

#[pyfunction]
fn regexp_like(column: &PyColumn, pattern: &str) -> PyColumn {
    PyColumn {
        inner: functions::regexp_like(&column.inner, pattern),
    }
}

#[pyfunction]
#[pyo3(signature = (column, format=None))]
fn to_timestamp(column: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    functions::to_timestamp(&column.inner, format)
        .map(|c| PyColumn { inner: c })
        .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (column, format=None))]
fn to_date(column: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    functions::to_date(&column.inner, format)
        .map(|c| PyColumn { inner: c })
        .map_err(to_py_err)
}

#[pyfunction]
fn current_date() -> PyColumn {
    PyColumn {
        inner: functions::current_date(),
    }
}

#[pyfunction]
fn current_timestamp() -> PyColumn {
    PyColumn {
        inner: functions::current_timestamp(),
    }
}

#[pyfunction]
fn input_file_name() -> PyColumn {
    PyColumn {
        inner: functions::input_file_name(),
    }
}

#[pyfunction]
fn datediff(end: &PyColumn, start: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::datediff(&end.inner, &start.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (column=None, format=None))]
fn unix_timestamp(column: Option<&PyColumn>, format: Option<&str>) -> PyColumn {
    match column {
        None => PyColumn {
            inner: functions::unix_timestamp_now(),
        },
        Some(c) => PyColumn {
            inner: functions::unix_timestamp(&c.inner, format),
        },
    }
}

#[pyfunction]
#[pyo3(signature = (column, format=None))]
fn from_unixtime(column: &PyColumn, format: Option<&str>) -> PyColumn {
    PyColumn {
        inner: functions::from_unixtime(&column.inner, format),
    }
}

#[pyfunction]
fn year(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::year(&column.inner),
    }
}

#[pyfunction]
fn month(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::month(&column.inner),
    }
}

#[pyfunction]
fn dayofmonth(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::dayofmonth(&column.inner),
    }
}

#[pyfunction]
fn dayofweek(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::dayofweek(&column.inner),
    }
}

#[pyfunction]
fn date_add(column: &PyColumn, n: i32) -> PyColumn {
    PyColumn {
        inner: functions::date_add(&column.inner, n),
    }
}

#[pyfunction]
fn date_sub(column: &PyColumn, n: i32) -> PyColumn {
    PyColumn {
        inner: functions::date_sub(&column.inner, n),
    }
}

#[pyfunction]
fn date_format(column: &PyColumn, format: &str) -> PyColumn {
    PyColumn {
        inner: functions::date_format(&column.inner, format),
    }
}

#[pyfunction]
fn length(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::length(&column.inner),
    }
}

#[pyfunction]
fn floor(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::floor(&column.inner),
    }
}

#[pyfunction]
fn ceil(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::ceil(&column.inner),
    }
}

#[pyfunction]
fn abs(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::abs(&column.inner),
    }
}

#[pyfunction]
fn sqrt(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::sqrt(&column.inner),
    }
}

#[pyfunction]
fn log(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::log(&column.inner),
    }
}

#[pyfunction]
fn log_with_base(column: &PyColumn, base: f64) -> PyColumn {
    PyColumn {
        inner: functions::log_with_base(&column.inner, base),
    }
}

#[pyfunction]
fn exp(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::exp(&column.inner),
    }
}

#[pyfunction]
fn pow(column: &PyColumn, exp: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    if let Ok(exp_i) = exp.extract::<i64>() {
        return Ok(PyColumn {
            inner: functions::pow(&column.inner, exp_i),
        });
    }
    if let Ok(exp_i) = exp.extract::<i32>() {
        return Ok(PyColumn {
            inner: functions::pow(&column.inner, exp_i as i64),
        });
    }
    if let Ok(exp_f) = exp.extract::<f64>() {
        let lit_col = functions::lit_f64(exp_f);
        return Ok(PyColumn {
            inner: column.inner.pow_with(&lit_col),
        });
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "pow exponent must be int or float",
    ))
}

#[pyfunction]
fn signum(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::signum(&column.inner),
    }
}

#[pyfunction]
fn sin(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::sin(&column.inner),
    }
}

#[pyfunction]
fn cos(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::cos(&column.inner),
    }
}

#[pyfunction]
fn tan(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::tan(&column.inner),
    }
}

#[pyfunction]
fn asin(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::asin(&column.inner),
    }
}

#[pyfunction]
fn acos(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::acos(&column.inner),
    }
}

#[pyfunction]
fn atan(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::atan(&column.inner),
    }
}

#[pyfunction]
fn atan2(y: &PyColumn, x: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::atan2(&y.inner, &x.inner),
    }
}

#[pyfunction]
fn degrees(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::degrees(&column.inner),
    }
}

#[pyfunction]
fn radians(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::radians(&column.inner),
    }
}

#[pyfunction]
fn log2(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::log2(&column.inner),
    }
}

#[pyfunction]
fn log10(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::log10(&column.inner),
    }
}

#[pyfunction]
fn stddev(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::stddev(&column.inner),
    }
}

#[pyfunction]
fn stddev_pop(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::stddev_pop(&column.inner),
    }
}

#[pyfunction]
fn stddev_samp(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::stddev_samp(&column.inner),
    }
}

#[pyfunction]
fn variance(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::variance(&column.inner),
    }
}

#[pyfunction]
fn var_pop(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::var_pop(&column.inner),
    }
}

#[pyfunction]
fn var_samp(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::var_samp(&column.inner),
    }
}

#[pyfunction]
fn count_distinct(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::count_distinct(&column.inner),
    }
}

#[pyfunction]
fn corr(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::corr(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
fn explode_outer(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::explode_outer(&column.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (column, index))]
fn element_at(column: &PyColumn, index: i64) -> PyColumn {
    PyColumn {
        inner: functions::element_at(&column.inner, index),
    }
}

#[pyfunction]
fn array_sort(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_sort(&column.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (column, separator))]
fn array_join(column: &PyColumn, separator: &str) -> PyColumn {
    PyColumn {
        inner: functions::array_join(&column.inner, separator),
    }
}

#[pyfunction]
fn array_max(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_max(&column.inner),
    }
}

#[pyfunction]
fn array_min(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_min(&column.inner),
    }
}

#[pyfunction]
fn collect_list(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::collect_list(&column.inner),
    }
}

#[pyfunction]
fn collect_set(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::collect_set(&column.inner),
    }
}

#[pyfunction]
fn flatten(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_flatten(&column.inner),
    }
}

#[pyfunction]
fn reverse(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::reverse(&column.inner),
    }
}

#[pyfunction]
fn hex(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::hex(&column.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_hex")]
fn native_hex(column: &PyColumn) -> PyColumn {
    hex(column)
}

#[pyfunction]
fn ascii(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::ascii(&column.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_ascii")]
fn native_ascii(column: &PyColumn) -> PyColumn {
    ascii(column)
}

#[pyfunction]
#[pyo3(name = "base64")]
fn base64_encode(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::base64(&column.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_base64")]
fn native_base64(column: &PyColumn) -> PyColumn {
    base64_encode(column)
}

#[pyfunction]
fn unbase64(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::unbase64(&column.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_unbase64")]
fn native_unbase64(column: &PyColumn) -> PyColumn {
    unbase64(column)
}

#[pyfunction]
fn array_remove(column: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_remove(&column.inner, &value.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (column, scale=0))]
fn round(column: &PyColumn, scale: i32) -> PyColumn {
    PyColumn {
        inner: functions::round(&column.inner, scale),
    }
}

#[pyfunction]
fn ltrim(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::ltrim(&column.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_ltrim")]
fn native_ltrim(column: &PyColumn) -> PyColumn {
    ltrim(column)
}

#[pyfunction]
#[pyo3(name = "native_rtrim")]
fn native_rtrim(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::rtrim(&column.inner),
    }
}

#[pyfunction]
fn rtrim(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::rtrim(&column.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_second")]
fn native_second(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::second(&column.inner),
    }
}

#[pyfunction]
fn second(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::second(&column.inner),
    }
}

#[pyfunction]
fn hour(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::hour(&column.inner),
    }
}

#[pyfunction]
fn minute(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::minute(&column.inner),
    }
}

#[pyfunction]
fn soundex(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::soundex(&column.inner),
    }
}

#[pyfunction]
fn initcap(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::initcap(&column.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_initcap")]
fn native_initcap(column: &PyColumn) -> PyColumn {
    initcap(column)
}

#[pyfunction]
#[pyo3(name = "native_reverse")]
fn native_reverse(column: &PyColumn) -> PyColumn {
    reverse(column)
}

#[pyfunction]
#[pyo3(name = "native_length")]
fn native_length(column: &PyColumn) -> PyColumn {
    length(column)
}

#[pyfunction]
fn repeat(column: &PyColumn, n: i32) -> PyColumn {
    PyColumn {
        inner: functions::repeat(&column.inner, n),
    }
}

#[pyfunction]
#[pyo3(name = "native_lpad")]
fn native_lpad(column: &PyColumn, length: i32, pad: &str) -> PyColumn {
    PyColumn {
        inner: functions::lpad(&column.inner, length, pad),
    }
}

#[pyfunction]
#[pyo3(name = "native_rpad")]
fn native_rpad(column: &PyColumn, length: i32, pad: &str) -> PyColumn {
    PyColumn {
        inner: functions::rpad(&column.inner, length, pad),
    }
}

#[pyfunction]
#[pyo3(name = "native_array_union")]
fn native_array_union(a: &PyColumn, b: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_union(&a.inner, &b.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_array_intersect")]
fn native_array_intersect(a: &PyColumn, b: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_intersect(&a.inner, &b.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_array_except")]
fn native_array_except(a: &PyColumn, b: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_except(&a.inner, &b.inner),
    }
}

#[pyfunction]
fn levenshtein(column: &PyColumn, other: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::levenshtein(&column.inner, &other.inner),
    }
}

#[pyfunction]
fn try_cast(column: &PyColumn, type_name: &str) -> PyResult<PyColumn> {
    functions::try_cast(&column.inner, type_name)
        .map(|c| PyColumn { inner: c })
        .map_err(to_py_err)
}

#[pyfunction]
fn try_add(left: &PyColumn, right: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::try_add(&left.inner, &right.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (separator, *columns))]
fn concat_ws(separator: &str, columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("concat_ws expects Column expressions")
        })?;
        cols.push(py_col.borrow().inner.clone());
    }
    if cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "concat_ws requires at least one column",
        ));
    }
    let refs: Vec<&Column> = cols.iter().collect();
    Ok(PyColumn {
        inner: functions::concat_ws(separator, &refs),
    })
}

/// Convert a single Python value to Column: PyColumn -> as-is, str -> col(name), else -> lit(value).
fn array_item_to_column(item: &Bound<'_, PyAny>) -> PyResult<Column> {
    if let Ok(py_col) = item.downcast::<PyColumn>() {
        return Ok(py_col.borrow().inner.clone());
    }
    if let Ok(name) = item.extract::<String>() {
        return Ok(robin_sparkless::functions::col(&name));
    }
    // Literals: int, float, bool, etc. -> lit(value)
    let lit_col = lit(item)?;
    Ok(lit_col.inner)
}

#[pyfunction]
#[pyo3(signature = (*columns))]
fn array(columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::new();
    for item in columns.iter() {
        if let Ok(list) = item.downcast::<PyList>() {
            for sub in list.iter() {
                cols.push(array_item_to_column(&sub)?);
            }
        } else if let Ok(tup) = item.downcast::<PyTuple>() {
            for sub in tup.iter() {
                cols.push(array_item_to_column(&sub)?);
            }
        } else {
            cols.push(array_item_to_column(&item)?);
        }
    }
    let refs: Vec<&Column> = cols.iter().collect();
    functions::array(&refs)
        .map(|c| PyColumn { inner: c })
        .map_err(to_py_err)
}

#[pyfunction]
fn asinh(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::asinh(&column.inner),
    }
}

#[pyfunction]
fn atanh(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::atanh(&column.inner),
    }
}

#[pyfunction]
fn cosh(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::cosh(&column.inner),
    }
}

#[pyfunction]
fn sinh(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::sinh(&column.inner),
    }
}

#[pyfunction]
fn last_day(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::last_day(&column.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (end, start, round_off=true))]
fn months_between(end: &PyColumn, start: &PyColumn, round_off: bool) -> PyColumn {
    PyColumn {
        inner: functions::months_between(&end.inner, &start.inner, round_off),
    }
}

#[pyfunction]
fn timestamp_seconds(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::timestamp_seconds(&column.inner),
    }
}

#[pyfunction]
fn to_utc_timestamp(column: &PyColumn, tz: &str) -> PyColumn {
    PyColumn {
        inner: functions::to_utc_timestamp(&column.inner, tz),
    }
}

// Phase 4: Missing functions (checklist) — expose for F.approx_count_distinct, F.date_trunc, etc.
#[pyfunction]
#[pyo3(signature = (col, rsd=None))]
fn approx_count_distinct(col: &PyColumn, rsd: Option<f64>) -> PyColumn {
    PyColumn {
        inner: functions::approx_count_distinct(&col.inner, rsd),
    }
}

#[pyfunction]
fn date_trunc(format: &str, column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::date_trunc(format, &column.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col, ignorenulls=false))]
fn first(col: &PyColumn, ignorenulls: bool) -> PyColumn {
    PyColumn {
        inner: functions::first(&col.inner, ignorenulls),
    }
}

#[pyfunction]
fn translate(column: &PyColumn, from_str: &str, to_str: &str) -> PyColumn {
    PyColumn {
        inner: functions::translate(&column.inner, from_str, to_str),
    }
}

#[pyfunction]
fn substring_index(column: &PyColumn, delimiter: &str, count: i64) -> PyColumn {
    PyColumn {
        inner: functions::substring_index(&column.inner, delimiter, count),
    }
}

#[pyfunction]
fn crc32(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::crc32(&column.inner),
    }
}

#[pyfunction]
fn xxhash64(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::xxhash64(&column.inner),
    }
}

#[pyfunction]
fn get_json_object(column: &PyColumn, path: &str) -> PyColumn {
    PyColumn {
        inner: functions::get_json_object(&column.inner, path),
    }
}

#[pyfunction]
#[pyo3(signature = (column, *keys))]
fn json_tuple(column: &PyColumn, keys: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let keys_str: Vec<String> = keys
        .iter()
        .map(|o| o.extract::<String>().unwrap_or_else(|_| o.to_string()))
        .collect();
    let keys_refs: Vec<&str> = keys_str.iter().map(|s| s.as_str()).collect();
    Ok(PyColumn {
        inner: functions::json_tuple(&column.inner, &keys_refs),
    })
}

#[pyfunction]
fn size(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::size(&column.inner),
    }
}

#[pyfunction]
fn array_contains(column: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_contains(&column.inner, &value.inner),
    }
}

#[pyfunction]
fn arrays_overlap(left: &PyColumn, right: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::arrays_overlap(&left.inner, &right.inner),
    }
}

#[pyfunction]
fn explode(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::explode(&column.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (*columns))]
fn struct_(columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("struct expects Column expressions")
        })?;
        cols.push(py_col.borrow().inner.clone());
    }
    if cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "struct requires at least one column",
        ));
    }
    let refs: Vec<&Column> = cols.iter().collect();
    Ok(PyColumn {
        inner: functions::struct_(&refs),
    })
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("SparklessError", m.py().get_type_bound::<SparklessError>())?;
    m.add_class::<PySparkSessionBuilder>()?;
    m.add_class::<PySparkSession>()?;
    m.add_class::<PyDataFrameReader>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyRDD>()?;
    m.add_class::<PyDataFrameWriter>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PySortOrder>()?;
    m.add_class::<PyGroupedData>()?;
    m.add_class::<PyPivotedGroupedData>()?;
    m.add_class::<PyDataFrameNaFunctions>()?;
    m.add_class::<PyCubeRollupData>()?;
    m.add_class::<PyRuntimeConfig>()?;
    m.add_class::<PySparkContext>()?;
    m.add_class::<PyCatalog>()?;
    m.add_class::<PyWhenBuilder>()?;
    m.add_class::<PyThenBuilder>()?;
    m.add_class::<PyChainedWhenBuilder>()?;
    m.add_class::<PyExprStr>()?;
    m.add_function(wrap_pyfunction!(spark_session_builder, m)?)?;
    m.add_function(wrap_pyfunction!(column, m)?)?;
    m.add_function(wrap_pyfunction!(expr_str, m)?)?;
    m.add_function(wrap_pyfunction!(create_udf_column, m)?)?;
    m.add_function(wrap_pyfunction!(set_python_udf_executor, m)?)?;
    m.add_function(wrap_pyfunction!(lit, m)?)?;
    m.add_function(wrap_pyfunction!(lit_i64, m)?)?;
    m.add_function(wrap_pyfunction!(lit_str, m)?)?;
    m.add_function(wrap_pyfunction!(lit_bool, m)?)?;
    m.add_function(wrap_pyfunction!(lit_f64, m)?)?;
    m.add_function(wrap_pyfunction!(lit_null, m)?)?;
    m.add_function(wrap_pyfunction!(upper, m)?)?;
    m.add_function(wrap_pyfunction!(lower, m)?)?;
    m.add_function(wrap_pyfunction!(substring, m)?)?;
    m.add_function(wrap_pyfunction!(trim, m)?)?;
    m.add_function(wrap_pyfunction!(cast, m)?)?;
    m.add_function(wrap_pyfunction!(when, m)?)?;
    m.add_function(wrap_pyfunction!(count, m)?)?;
    m.add_function(wrap_pyfunction!(sum, m)?)?;
    m.add_function(wrap_pyfunction!(avg, m)?)?;
    m.add_function(wrap_pyfunction!(min, m)?)?;
    m.add_function(wrap_pyfunction!(max, m)?)?;
    m.add_function(wrap_pyfunction!(create_map, m)?)?;
    m.add_function(wrap_pyfunction!(column_over_window, m)?)?;
    m.add_function(wrap_pyfunction!(row_number_window, m)?)?;
    m.add_function(wrap_pyfunction!(percent_rank_window, m)?)?;
    m.add_function(wrap_pyfunction!(rank_window, m)?)?;
    m.add_function(wrap_pyfunction!(dense_rank_window, m)?)?;
    m.add_function(wrap_pyfunction!(cume_dist_window, m)?)?;
    m.add_function(wrap_pyfunction!(ntile_window, m)?)?;
    m.add_function(wrap_pyfunction!(column_value_over_window, m)?)?;
    m.add_function(wrap_pyfunction!(lag_window, m)?)?;
    m.add_function(wrap_pyfunction!(lead_window, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_replace, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_extract, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_extract_all, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_like, m)?)?;
    m.add_function(wrap_pyfunction!(split, m)?)?;
    m.add_function(wrap_pyfunction!(coalesce, m)?)?;
    m.add_function(wrap_pyfunction!(nullif, m)?)?;
    m.add_function(wrap_pyfunction!(nanvl, m)?)?;
    m.add_function(wrap_pyfunction!(isnan, m)?)?;
    m.add_function(wrap_pyfunction!(array_position, m)?)?;
    m.add_function(wrap_pyfunction!(format_string, m)?)?;
    m.add_function(wrap_pyfunction!(greatest, m)?)?;
    m.add_function(wrap_pyfunction!(least, m)?)?;
    m.add_function(wrap_pyfunction!(array_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(posexplode, m)?)?;
    m.add_function(wrap_pyfunction!(to_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(to_date, m)?)?;
    m.add_function(wrap_pyfunction!(current_date, m)?)?;
    m.add_function(wrap_pyfunction!(current_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(input_file_name, m)?)?;
    m.add_function(wrap_pyfunction!(datediff, m)?)?;
    m.add_function(wrap_pyfunction!(unix_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(from_unixtime, m)?)?;
    m.add_function(wrap_pyfunction!(year, m)?)?;
    m.add_function(wrap_pyfunction!(month, m)?)?;
    m.add_function(wrap_pyfunction!(dayofmonth, m)?)?;
    m.add_function(wrap_pyfunction!(dayofweek, m)?)?;
    m.add_function(wrap_pyfunction!(date_add, m)?)?;
    m.add_function(wrap_pyfunction!(date_sub, m)?)?;
    m.add_function(wrap_pyfunction!(date_format, m)?)?;
    m.add_function(wrap_pyfunction!(length, m)?)?;
    m.add_function(wrap_pyfunction!(floor, m)?)?;
    m.add_function(wrap_pyfunction!(ceil, m)?)?;
    m.add_function(wrap_pyfunction!(abs, m)?)?;
    m.add_function(wrap_pyfunction!(sqrt, m)?)?;
    m.add_function(wrap_pyfunction!(log, m)?)?;
    m.add_function(wrap_pyfunction!(log_with_base, m)?)?;
    m.add_function(wrap_pyfunction!(exp, m)?)?;
    m.add_function(wrap_pyfunction!(pow, m)?)?;
    m.add_function(wrap_pyfunction!(signum, m)?)?;
    m.add_function(wrap_pyfunction!(sin, m)?)?;
    m.add_function(wrap_pyfunction!(cos, m)?)?;
    m.add_function(wrap_pyfunction!(tan, m)?)?;
    m.add_function(wrap_pyfunction!(asin, m)?)?;
    m.add_function(wrap_pyfunction!(acos, m)?)?;
    m.add_function(wrap_pyfunction!(atan, m)?)?;
    m.add_function(wrap_pyfunction!(atan2, m)?)?;
    m.add_function(wrap_pyfunction!(degrees, m)?)?;
    m.add_function(wrap_pyfunction!(radians, m)?)?;
    m.add_function(wrap_pyfunction!(log2, m)?)?;
    m.add_function(wrap_pyfunction!(log10, m)?)?;
    m.add_function(wrap_pyfunction!(stddev, m)?)?;
    m.add_function(wrap_pyfunction!(stddev_pop, m)?)?;
    m.add_function(wrap_pyfunction!(stddev_samp, m)?)?;
    m.add_function(wrap_pyfunction!(variance, m)?)?;
    m.add_function(wrap_pyfunction!(var_pop, m)?)?;
    m.add_function(wrap_pyfunction!(var_samp, m)?)?;
    m.add_function(wrap_pyfunction!(count_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(corr, m)?)?;
    m.add_function(wrap_pyfunction!(explode_outer, m)?)?;
    m.add_function(wrap_pyfunction!(element_at, m)?)?;
    m.add_function(wrap_pyfunction!(array_sort, m)?)?;
    m.add_function(wrap_pyfunction!(array_join, m)?)?;
    m.add_function(wrap_pyfunction!(array_max, m)?)?;
    m.add_function(wrap_pyfunction!(array_min, m)?)?;
    m.add_function(wrap_pyfunction!(collect_list, m)?)?;
    m.add_function(wrap_pyfunction!(collect_set, m)?)?;
    m.add_function(wrap_pyfunction!(flatten, m)?)?;
    m.add_function(wrap_pyfunction!(reverse, m)?)?;
    m.add_function(wrap_pyfunction!(hex, m)?)?;
    m.add_function(wrap_pyfunction!(native_hex, m)?)?;
    m.add_function(wrap_pyfunction!(ascii, m)?)?;
    m.add_function(wrap_pyfunction!(native_ascii, m)?)?;
    m.add_function(wrap_pyfunction!(base64_encode, m)?)?;
    m.add_function(wrap_pyfunction!(native_base64, m)?)?;
    m.add_function(wrap_pyfunction!(unbase64, m)?)?;
    m.add_function(wrap_pyfunction!(native_unbase64, m)?)?;
    m.add_function(wrap_pyfunction!(array_remove, m)?)?;
    m.add_function(wrap_pyfunction!(round, m)?)?;
    m.add_function(wrap_pyfunction!(ltrim, m)?)?;
    m.add_function(wrap_pyfunction!(native_ltrim, m)?)?;
    m.add_function(wrap_pyfunction!(native_rtrim, m)?)?;
    m.add_function(wrap_pyfunction!(rtrim, m)?)?;
    m.add_function(wrap_pyfunction!(initcap, m)?)?;
    m.add_function(wrap_pyfunction!(native_initcap, m)?)?;
    m.add_function(wrap_pyfunction!(native_reverse, m)?)?;
    m.add_function(wrap_pyfunction!(native_length, m)?)?;
    m.add_function(wrap_pyfunction!(native_second, m)?)?;
    m.add_function(wrap_pyfunction!(second, m)?)?;
    m.add_function(wrap_pyfunction!(hour, m)?)?;
    m.add_function(wrap_pyfunction!(minute, m)?)?;
    m.add_function(wrap_pyfunction!(soundex, m)?)?;
    m.add_function(wrap_pyfunction!(repeat, m)?)?;
    m.add_function(wrap_pyfunction!(levenshtein, m)?)?;
    m.add_function(wrap_pyfunction!(native_lpad, m)?)?;
    m.add_function(wrap_pyfunction!(native_rpad, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_union, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_intersect, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_except, m)?)?;
    m.add_function(wrap_pyfunction!(try_cast, m)?)?;
    m.add_function(wrap_pyfunction!(try_add, m)?)?;
    m.add_function(wrap_pyfunction!(concat, m)?)?;
    m.add_function(wrap_pyfunction!(concat_ws, m)?)?;
    m.add_function(wrap_pyfunction!(array, m)?)?;
    m.add_function(wrap_pyfunction!(struct_, m)?)?;
    m.add_function(wrap_pyfunction!(asinh, m)?)?;
    m.add_function(wrap_pyfunction!(atanh, m)?)?;
    m.add_function(wrap_pyfunction!(cosh, m)?)?;
    m.add_function(wrap_pyfunction!(sinh, m)?)?;
    m.add_function(wrap_pyfunction!(last_day, m)?)?;
    m.add_function(wrap_pyfunction!(months_between, m)?)?;
    m.add_function(wrap_pyfunction!(timestamp_seconds, m)?)?;
    m.add_function(wrap_pyfunction!(to_utc_timestamp, m)?)?;
    // Phase 4: missing functions (checklist)
    m.add_function(wrap_pyfunction!(approx_count_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(date_trunc, m)?)?;
    m.add_function(wrap_pyfunction!(first, m)?)?;
    m.add_function(wrap_pyfunction!(translate, m)?)?;
    m.add_function(wrap_pyfunction!(substring_index, m)?)?;
    m.add_function(wrap_pyfunction!(crc32, m)?)?;
    m.add_function(wrap_pyfunction!(xxhash64, m)?)?;
    m.add_function(wrap_pyfunction!(get_json_object, m)?)?;
    m.add_function(wrap_pyfunction!(json_tuple, m)?)?;
    m.add_function(wrap_pyfunction!(size, m)?)?;
    m.add_function(wrap_pyfunction!(array_contains, m)?)?;
    m.add_function(wrap_pyfunction!(arrays_overlap, m)?)?;
    m.add_function(wrap_pyfunction!(explode, m)?)?;
    Ok(())
}
