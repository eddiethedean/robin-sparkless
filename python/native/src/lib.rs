//! PyO3 bindings for sparkless Python package. Exposes robin-sparkless as a native module.
//! Parameter names follow PySpark camelCase where the Python API exposes them.

#![allow(non_snake_case)]
#![allow(unexpected_cfgs)] // pyo3 create_exception! uses cfg(gil-refs)
#![allow(clippy::useless_conversion)] // PyO3 PyResult<T> / PyErr false positives (fixed in pyo3 0.23+)

mod arrow_conversion;
mod column_api;
mod dataframe_api;
mod dataframe_reader;
mod dataframe_writer;
mod external_catalog;

thread_local! {
    /// Thread-local stack of active sessions (PySpark getActiveSession semantics).
    static THREAD_ACTIVE_SESSIONS: RefCell<Vec<Py<PySparkSession>>> = const { RefCell::new(Vec::new()) };
    /// Optional Python callable to run Python UDFs when with_column sees a UDF column. Set by Python on session creation.
    static PYTHON_UDF_EXECUTOR: RefCell<Option<Py<PyAny>>> = const { RefCell::new(None) };
}

mod registration;
mod row_conversion;
mod session_api;
use arrow_conversion::*;
use dataframe_reader::{
    classify_fill_value_kind, is_fill_compatible_with_schema_dtype, normalize_subset,
    option_value_to_string, resolve_cast_type_name, PyDataFrameReader,
};
use dataframe_writer::PyDataFrameWriter;
use row_conversion::*;
use session_api::*;

use polars::prelude::DataFrame as PlDataFrame;
use polars_plan::dsl::AggExpr;
use pyo3::class::basic::CompareOp;
use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use pyo3::IntoPyObjectExt;

type PyObject = Py<PyAny>;

fn into_py_object<'py, T>(py: Python<'py>, value: T) -> PyResult<PyObject>
where
    T: IntoPyObject<'py>,
{
    value.into_py_any(py)
}
use robin_sparkless::dataframe::{
    expr_contains_only_join_key_equalities, try_extract_join_eq_columns_all, JoinType,
    PivotedGroupedData, SaveMode, WriteFormat, WriteMode,
};
use robin_sparkless::functions::{self, asc_from_name, SortOrder, ThenBuilder, WhenBuilder};
use robin_sparkless::{
    schema_from_json, Column, CubeRollupData, DataFrame, DataType, GroupBySpec, GroupedData,
    LogicalField as StructField, SelectItem, SparkSession, SparkSessionBuilder, StructType,
    StructTypePolarsExt,
};
use robin_sparkless_core::{
    normalize_unresolved_column_message, CancellationToken, DataType as CoreDataType, EngineError,
    ErrorCategory, ErrorRetryability,
};
use serde_json::Value as JsonValue;
use std::any::Any;
use std::cell::RefCell;
use std::env;
use std::path::Path;
use std::thread_local;

// When row_number().over(window) is used with no partition, set this so select() sorts result by window order (#1241).
thread_local! {
    static WINDOW_ORDER_SORT_HINT: RefCell<Option<(Vec<String>, Vec<bool>)>> =
        const { RefCell::new(None) };
}

/// When true, collect() returns raw integers for YearMonthIntervalType columns (PySpark 3.x).
fn ym_interval_legacy_collect() -> bool {
    if env::var("PYSPARK_YM_INTERVAL_LEGACY")
        .ok()
        .is_some_and(|v| v.trim() == "1")
    {
        return true;
    }
    robin_sparkless::get_thread_runtime_config().pyspark_compat
        == robin_sparkless_core::PysparkCompat::V3_5
}

fn ym_interval_column_names_from_schema(
    py: Python<'_>,
    schema_obj: &Bound<'_, PyAny>,
) -> PyResult<std::collections::HashSet<String>> {
    let mut out = std::collections::HashSet::new();
    let types_mod = PyModule::import(py, "sparkless.sql.types")?;
    let ym_type = types_mod.getattr("YearMonthIntervalType")?;
    let fields = schema_obj.getattr("fields")?;
    let fields_list = fields.cast::<PyList>()?;
    for f in fields_list.iter() {
        let dt = f.getattr("dataType")?;
        if dt.is_instance(&ym_type)? {
            let fname: String = f.getattr("name")?.extract()?;
            out.insert(fname);
        }
    }
    Ok(out)
}

fn json_months_value(v: &JsonValue) -> Option<i64> {
    match v {
        JsonValue::Number(n) => n.as_i64(),
        JsonValue::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// When true, collect() returns raw integers for interval columns (PySpark 3.x legacy).
fn interval_legacy_collect() -> bool {
    ym_interval_legacy_collect()
}

fn dt_interval_column_names_from_schema(
    py: Python<'_>,
    schema_obj: &Bound<'_, PyAny>,
) -> PyResult<std::collections::HashSet<String>> {
    let mut out = std::collections::HashSet::new();
    let types_mod = PyModule::import(py, "sparkless.sql.types")?;
    let dt_type = types_mod.getattr("DayTimeIntervalType")?;
    let fields = schema_obj.getattr("fields")?;
    let fields_list = fields.cast::<PyList>()?;
    for f in fields_list.iter() {
        let dt = f.getattr("dataType")?;
        if dt.is_instance(&dt_type)? {
            let fname: String = f.getattr("name")?.extract()?;
            out.insert(fname);
        }
    }
    Ok(out)
}

fn variant_column_names_from_schema(
    py: Python<'_>,
    schema_obj: &Bound<'_, PyAny>,
) -> PyResult<std::collections::HashSet<String>> {
    let mut out = std::collections::HashSet::new();
    let types_mod = PyModule::import(py, "sparkless.sql.types")?;
    let variant_type = types_mod.getattr("VariantType")?;
    let fields = schema_obj.getattr("fields")?;
    let fields_list = fields.cast::<PyList>()?;
    for f in fields_list.iter() {
        let dt = f.getattr("dataType")?;
        if dt.is_instance(&variant_type)? {
            let fname: String = f.getattr("name")?.extract()?;
            out.insert(fname);
        }
    }
    Ok(out)
}

fn json_micros_value(v: &JsonValue) -> Option<i64> {
    json_months_value(v)
}

fn json_variant_to_py(py: Python<'_>, v: &JsonValue) -> PyResult<PyObject> {
    match v {
        JsonValue::Null => Ok(py.None().into_py_any(py)?),
        JsonValue::Bool(b) => Ok(b.into_py_any(py)?),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py_any(py)?)
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_py_any(py)?)
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py_any(py)?)
            } else {
                Ok(py.None().into_py_any(py)?)
            }
        }
        JsonValue::String(s) => Ok(s.clone().into_py_any(py)?),
        JsonValue::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_variant_to_py(py, item)?)?;
            }
            Ok(list.into_py_any(py)?)
        }
        JsonValue::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, val) in obj {
                dict.set_item(k, json_variant_to_py(py, val)?)?;
            }
            Ok(dict.into_py_any(py)?)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_cell_py(
    py: Python<'_>,
    name: &str,
    v: &JsonValue,
    ym_interval_cols: &std::collections::HashSet<String>,
    dt_interval_cols: &std::collections::HashSet<String>,
    variant_cols: &std::collections::HashSet<String>,
    interval_legacy: bool,
    ym_interval_cls: Option<&Bound<'_, PyAny>>,
    timedelta_cls: &Bound<'_, PyAny>,
    dtype_by_name: &std::collections::HashMap<String, DataType>,
    output_names: &[String],
    datetime_cls: &Bound<'_, PyAny>,
    date_cls: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    if !interval_legacy && ym_interval_cols.contains(name) {
        if let Some(ym_cls) = ym_interval_cls {
            return match json_months_value(v) {
                Some(m) => Ok(ym_cls.call1((m,))?.into_py_any(py)?),
                None if matches!(v, JsonValue::Null) => Ok(py.None().into_py_any(py)?),
                _ => json_value_to_py_with_schema(
                    py,
                    v,
                    dtype_by_name.get(name),
                    datetime_cls,
                    date_cls,
                    Some(name),
                    Some(output_names),
                ),
            };
        }
    }
    if interval_legacy && ym_interval_cols.contains(name) {
        return match json_months_value(v) {
            Some(m) => Ok(m.into_py_any(py)?),
            None if matches!(v, JsonValue::Null) => Ok(py.None().into_py_any(py)?),
            _ => json_value_to_py_with_schema(
                py,
                v,
                dtype_by_name.get(name),
                datetime_cls,
                date_cls,
                Some(name),
                Some(output_names),
            ),
        };
    }
    if !interval_legacy && dt_interval_cols.contains(name) {
        return match json_micros_value(v) {
            Some(m) => {
                let kwargs = PyDict::new(py);
                kwargs.set_item("microseconds", m)?;
                Ok(timedelta_cls.call((), Some(&kwargs))?.into_py_any(py)?)
            }
            None if matches!(v, JsonValue::Null) => Ok(py.None().into_py_any(py)?),
            _ => json_value_to_py_with_schema(
                py,
                v,
                dtype_by_name.get(name),
                datetime_cls,
                date_cls,
                Some(name),
                Some(output_names),
            ),
        };
    }
    if interval_legacy && dt_interval_cols.contains(name) {
        return match json_micros_value(v) {
            Some(m) => Ok(m.into_py_any(py)?),
            None if matches!(v, JsonValue::Null) => Ok(py.None().into_py_any(py)?),
            _ => json_value_to_py_with_schema(
                py,
                v,
                dtype_by_name.get(name),
                datetime_cls,
                date_cls,
                Some(name),
                Some(output_names),
            ),
        };
    }
    if variant_cols.contains(name) {
        return match v {
            JsonValue::String(s) => {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                    json_variant_to_py(py, &parsed)
                } else {
                    Ok(s.clone().into_py_any(py)?)
                }
            }
            JsonValue::Null => Ok(py.None().into_py_any(py)?),
            other => json_variant_to_py(py, other),
        };
    }
    json_value_to_py_with_schema(
        py,
        v,
        dtype_by_name.get(name),
        datetime_cls,
        date_cls,
        Some(name),
        Some(output_names),
    )
}

/// Convert EngineError or PolarsError to Python exception (SparklessError or TypeError for known validation errors).
fn to_py_err<E: std::fmt::Display + 'static>(e: E) -> PyErr {
    let error_type = &e as &dyn Any;
    if let Some(error) = error_type.downcast_ref::<EngineError>() {
        return to_py_engine_error(error);
    }
    if let Some(error) = error_type.downcast_ref::<polars::error::PolarsError>() {
        let message = error.to_string();
        let mut structured = robin_sparkless::polars::error::polars_to_core_error(error.clone());
        if let Some(callback_error) = external_catalog::engine_error_from_message(&message) {
            if structured.code() == "WRITE_OUTCOME_UNKNOWN" {
                let mut detail = structured.detail().clone();
                if callback_error.code() != "EXTERNAL_CATALOG_CALLBACK_FAILED" {
                    detail.code = callback_error.code().to_string();
                }
                if let Some(operation) = callback_error.operation() {
                    detail = detail.with_operation(operation);
                }
                for (key, value) in &callback_error.detail().metadata {
                    detail = detail.with_metadata(key.clone(), value.clone());
                }
                structured = EngineError::from_detail(detail);
            } else {
                structured = callback_error;
            }
        }
        return to_py_engine_error(&structured);
    }
    to_py_err_msg(&e.to_string())
}

fn error_category_name(category: ErrorCategory) -> &'static str {
    match category {
        ErrorCategory::InvalidInput => "invalid_input",
        ErrorCategory::Execution => "execution",
        ErrorCategory::Io => "io",
        ErrorCategory::Sql => "sql",
        ErrorCategory::NotFound => "not_found",
        ErrorCategory::Cancelled => "cancelled",
        ErrorCategory::Provider => "provider",
        ErrorCategory::Capability => "capability",
        ErrorCategory::Conflict => "conflict",
        ErrorCategory::Internal => "internal",
        ErrorCategory::Other => "other",
        _ => "other",
    }
}

fn error_retryability_name(retryability: ErrorRetryability) -> &'static str {
    match retryability {
        ErrorRetryability::Unspecified => "unspecified",
        ErrorRetryability::Retryable => "retryable",
        ErrorRetryability::Terminal => "terminal",
        ErrorRetryability::OutcomeUnknown => "outcome_unknown",
        _ => "unspecified",
    }
}

fn to_py_engine_error(error: &EngineError) -> PyErr {
    let py_error = to_py_err_msg(&error.to_string());
    Python::attach(|py| {
        let value = py_error.value(py);
        let detail = error.detail();
        if value.getattr("category").is_err() {
            let _ = value.setattr("category", error_category_name(detail.category));
        }
        let _ = value.setattr("engine_category", error_category_name(detail.category));
        let _ = value.setattr("code", detail.code.as_str());
        let _ = value.setattr("operation", detail.operation.as_deref());
        let _ = value.setattr("retryability", error_retryability_name(detail.retryability));
        let metadata = PyDict::new(py);
        for (key, item) in &detail.metadata {
            let _ = metadata.set_item(key, item);
        }
        let _ = value.setattr("metadata", metadata);
        let _ = value.setattr("cause", detail.cause.as_deref());
        let serialized = serde_json::to_value(detail).unwrap_or(JsonValue::Null);
        if let Ok(serialized) = json_to_py(&serialized, py) {
            let _ = value.setattr("error_detail", serialized);
        }
    });
    py_error
}

fn to_py_err_msg(msg: &str) -> PyErr {
    if let Some(error) =
        Python::attach(|py| external_catalog::callback_failure_from_message(py, msg))
    {
        return error;
    }
    let mut msg = msg.to_string();
    for prefix in [
        "user error: ",
        "internal error: ",
        "io error: ",
        "sql error: ",
        "not found: ",
        "provider error: ",
        "conflict: ",
    ] {
        if let Some(rest) = msg.strip_prefix(prefix) {
            msg = rest.to_string();
            break;
        }
    }
    if let Some(message) = msg.strip_prefix("cancelled: ") {
        return SparklessCancelledError::new_err(message.to_string());
    }
    let msg = normalize_unresolved_column_message(&msg);
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
    // ANSI / PySpark 4: arithmetic overflow and divide-by-zero
    if msg.contains("[ARITHMETIC_OVERFLOW]")
        || msg.contains("[DIVIDE_BY_ZERO]")
        || msg.contains("divide by zero")
        || msg.contains("Division by zero")
    {
        return pyo3::exceptions::PyArithmeticError::new_err(msg);
    }
    if msg.contains("conversion from `str`") && msg.contains("failed in column") {
        return pyo3::exceptions::PyValueError::new_err(msg);
    }
    // Int subscript on struct column: PySpark/issue #339 expects TypeError (string keys for struct field access).
    if (msg.contains("expected List") || msg.contains("list operation"))
        && (msg.contains("Struct") || msg.contains("struct"))
    {
        return pyo3::exceptions::PyTypeError::new_err(
            "Struct field subscript access requires string keys, not int",
        );
    }
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

/// Extract verify_schema or verifySchema from kwargs for createDataFrame (default true).
fn extract_verify_schema(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<bool> {
    let Some(kw) = kwargs else {
        return Ok(true);
    };
    if let Some(v) = kw.get_item("verify_schema")? {
        return v.extract::<bool>();
    }
    if let Some(v) = kw.get_item("verifySchema")? {
        return v.extract::<bool>();
    }
    Ok(true)
}

/// Extract sampling_ratio or samplingRatio from kwargs for createDataFrame (default None).
fn extract_sampling_ratio(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Option<f64>> {
    let Some(kw) = kwargs else {
        return Ok(None);
    };
    for key in ["sampling_ratio", "samplingRatio"] {
        if let Some(v) = kw.get_item(key)? {
            if v.is_none() {
                return Ok(None);
            }
            return Ok(Some(v.extract::<f64>()?));
        }
    }
    Ok(None)
}

create_exception!(
    _native,
    SparklessError,
    pyo3::exceptions::PyRuntimeError,
    "Sparkless error"
);

create_exception!(
    _native,
    SparklessCancelledError,
    SparklessError,
    "Sparkless action cancelled"
);

create_exception!(
    _native,
    SparklessExternalCatalogError,
    SparklessError,
    "External catalog provider callback failed"
);

/// Convert serde_json::Value to Python object.
fn json_to_py(value: &JsonValue, py: Python<'_>) -> PyResult<PyObject> {
    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => Ok(b.into_py_any(py)?),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py_any(py)?)
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py_any(py)?)
            } else {
                Ok(n.to_string().into_py_any(py)?)
            }
        }
        JsonValue::String(s) => Ok(s.clone().into_py_any(py)?),
        JsonValue::Array(arr) => {
            let list = PyList::empty(py);
            for v in arr {
                list.append(json_to_py(v, py)?)?;
            }
            Ok(list.into_py_any(py)?)
        }
        JsonValue::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                let py_v = match v {
                    JsonValue::String(s) if s.trim_start().starts_with('{') => {
                        if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                            if parsed.is_object() {
                                json_to_py(&parsed, py)?
                            } else {
                                s.clone().into_py_any(py)?
                            }
                        } else {
                            s.clone().into_py_any(py)?
                        }
                    }
                    _ => json_to_py(v, py)?,
                };
                dict.set_item(k, py_v)?;
            }
            Ok(dict.into_py_any(py)?)
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
        return into_py_object(py, true).ok();
    }
    if lower == "false" || lower == "0" {
        return into_py_object(py, false).ok();
    }
    if let Ok(i) = s.parse::<i64>() {
        return into_py_object(py, i).ok();
    }
    if let Ok(f) = s.parse::<f64>() {
        return into_py_object(py, f).ok();
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
            DataType::Short | DataType::Integer | DataType::Long => inner
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
                        DataType::Short | DataType::Integer | DataType::Long => v
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
                DataType::Short | DataType::Integer | DataType::Long => part
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

/// types are preserved even when the engine sent a string (e.g. string-inferred schema).
/// Recurses for Array and Struct so nested values are coerced too.
/// When schema is String, value is preserved as string (#1261 PySpark parity).
fn json_value_to_py_with_schema(
    py: Python<'_>,
    value: &JsonValue,
    dtype: Option<&DataType>,
    datetime_cls: &Bound<'_, PyAny>,
    date_cls: &Bound<'_, PyAny>,
    column_name: Option<&str>,
    _output_column_names: Option<&[String]>,
) -> PyResult<PyObject> {
    Ok(match (dtype, value) {
        (Some(DataType::Timestamp), JsonValue::String(s)) => {
            // PySpark exposes TimestampType values as naive Python datetimes even
            // though the engine stores these values as UTC instants. TimestampNTZ
            // values remain timezone-free by construction and use the branch below.
            let value = datetime_cls.call_method1("fromisoformat", (s.as_str(),))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("tzinfo", py.None())?;
            value
                .call_method("replace", (), Some(&kwargs))?
                .into_py_any(py)?
        }
        (Some(DataType::TimestampNtz), JsonValue::String(s)) => datetime_cls
            .call_method1("fromisoformat", (s.as_str(),))
            .and_then(|o| o.into_py_any(py))
            .or_else(|_| into_py_object(py, s.clone()))?,
        (Some(DataType::Date), JsonValue::String(s)) => date_cls
            .call_method1("fromisoformat", (s.as_str(),))
            .and_then(|o| o.into_py_any(py))
            .or_else(|_| into_py_object(py, s.clone()))?,
        (
            Some(DataType::Short) | Some(DataType::Integer) | Some(DataType::Long),
            JsonValue::String(s),
        ) => {
            let s = s.trim();
            if let Ok(i) = s.parse::<i64>() {
                i.into_py_any(py)?
            } else {
                s.to_string().into_py_any(py)?
            }
        }
        (Some(DataType::Float | DataType::Double), JsonValue::String(s)) => {
            let s = s.trim();
            if let Ok(f) = s.parse::<f64>() {
                f.into_py_any(py)?
            } else {
                s.to_string().into_py_any(py)?
            }
        }
        (Some(DataType::Decimal(_, _)), JsonValue::String(s)) => PyModule::import(py, "decimal")?
            .getattr("Decimal")?
            .call1((s.as_str(),))?
            .into_py_any(py)?,
        (Some(DataType::Boolean), JsonValue::String(s)) => {
            let lower = s.trim().to_lowercase();
            if lower == "true" || lower == "1" {
                true.into_py_any(py)?
            } else if lower == "false" || lower == "0" {
                false.into_py_any(py)?
            } else {
                s.clone().into_py_any(py)?
            }
        }
        // Polars represents maps as list<struct<key, value>> internally, but JSON
        // serialization exposes them as objects. Decode both schema forms recursively
        // so decimal (and other typed) map values keep their Python types.
        (Some(DataType::Map(key_type, value_type)), JsonValue::Object(obj)) => {
            let dict = PyDict::new(py);
            for (key, value) in obj {
                let py_key = json_value_to_py_with_schema(
                    py,
                    &JsonValue::String(key.clone()),
                    Some(key_type),
                    datetime_cls,
                    date_cls,
                    None,
                    None,
                )?;
                let py_value = json_value_to_py_with_schema(
                    py,
                    value,
                    Some(value_type),
                    datetime_cls,
                    date_cls,
                    None,
                    None,
                )?;
                dict.set_item(py_key, py_value)?;
            }
            dict.into_py_any(py)?
        }
        (Some(DataType::Array(elem_type)), JsonValue::Object(obj)) if matches!(elem_type.as_ref(), DataType::LogicalStruct(fields) if fields.iter().any(|f| f.name == "key") && fields.iter().any(|f| f.name == "value")) =>
        {
            let DataType::LogicalStruct(fields) = elem_type.as_ref() else {
                unreachable!()
            };
            let key_type = &fields.iter().find(|f| f.name == "key").unwrap().data_type;
            let value_type = &fields.iter().find(|f| f.name == "value").unwrap().data_type;
            let dict = PyDict::new(py);
            for (key, value) in obj {
                let py_key = json_value_to_py_with_schema(
                    py,
                    &JsonValue::String(key.clone()),
                    Some(key_type),
                    datetime_cls,
                    date_cls,
                    None,
                    None,
                )?;
                let py_value = json_value_to_py_with_schema(
                    py,
                    value,
                    Some(value_type),
                    datetime_cls,
                    date_cls,
                    None,
                    None,
                )?;
                dict.set_item(py_key, py_value)?;
            }
            dict.into_py_any(py)?
        }
        // Schema says String: preserve string (PySpark parity). Do not coerce to int/float by column name.
        // Tests that expect numeric from inferred String schema are skipped; see GitHub #1274.
        (Some(DataType::String), JsonValue::String(s)) => {
            if column_name.is_none() {
                if let Ok(parsed) = serde_json::from_str::<JsonValue>(s) {
                    if parsed.is_object() {
                        return json_to_py(&parsed, py);
                    }
                }
                return s.clone().into_py_any(py);
            }
            s.clone().into_py_any(py)?
        }
        // Schema says String but engine sent number (e.g. cast to string, inference): emit string.
        (Some(DataType::String), JsonValue::Number(n)) => n.to_string().into_py_any(py)?,
        // Schema says String but engine sent bool: emit "True"/"False".
        (Some(DataType::String), JsonValue::Bool(b)) => b.to_string().into_py_any(py)?,
        // #1146: json_tuple/get_json_object string coercion applies only when values are strings in JSON, not numeric Int64 columns named c0/c1.
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
                i.into_py_any(py)?
            } else if let Ok(f) = s.parse::<f64>() {
                f.into_py_any(py)?
            } else {
                let lower = s.to_lowercase();
                if lower == "true" || lower == "1" {
                    true.into_py_any(py)?
                } else if lower == "false" || lower == "0" {
                    false.into_py_any(py)?
                } else {
                    s.to_string().into_py_any(py)?
                }
            }
        }
        (Some(DataType::Array(elem_type)), JsonValue::Array(arr)) => {
            let list = PyList::empty(py);
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
            list.into_py_any(py)?
        }
        // Engine may stringify list columns in some paths (e.g. "[1,2]"); parse back when schema says Array.
        (Some(DataType::Array(elem_type)), JsonValue::String(s)) => {
            if let Ok(JsonValue::Array(arr)) = serde_json::from_str::<JsonValue>(s) {
                let list = PyList::empty(py);
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
                return list.into_py_any(py);
            }
            s.clone().into_py_any(py)?
        }
        (Some(DataType::LogicalStruct(fields)), JsonValue::Object(obj)) => {
            let dict = PyDict::new(py);
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
            dict.into_py_any(py)?
        }
        // #1066: Struct (top-level or nested) may be stringified; parse so row["my_struct"]["nested"] is dict.
        (Some(DataType::LogicalStruct(fields)), JsonValue::String(s)) => {
            if let Ok(JsonValue::Object(obj)) = serde_json::from_str::<JsonValue>(s) {
                let dict = PyDict::new(py);
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
                return dict.into_py_any(py);
            }
            if let Some(obj) = parse_struct_string_to_json(s, fields) {
                let dict = PyDict::new(py);
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
                return dict.into_py_any(py);
            }
            s.clone().into_py_any(py)?
        }
        _ => json_to_py(value, py)?,
    })
}

#[derive(Default)]
struct ActionRegistry {
    next_id: std::sync::atomic::AtomicU64,
    active: std::sync::Mutex<std::collections::HashMap<u64, CancellationToken>>,
}

impl ActionRegistry {
    fn register(self: &std::sync::Arc<Self>, token: CancellationToken) -> ActionRegistration {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, token);
        ActionRegistration {
            registry: self.clone(),
            id,
        }
    }

    fn cancel_active(&self) -> usize {
        let tokens = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for token in &tokens {
            token.cancel();
        }
        tokens.len()
    }
}

struct ActionRegistration {
    registry: std::sync::Arc<ActionRegistry>,
    id: u64,
}

impl Drop for ActionRegistration {
    fn drop(&mut self) {
        self.registry
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

struct ActiveAction {
    token: CancellationToken,
    _registrations: Vec<ActionRegistration>,
}

impl ActiveAction {
    fn new(
        local: &std::sync::Arc<ActionRegistry>,
        session: Option<&std::sync::Arc<ActionRegistry>>,
    ) -> Self {
        let token = CancellationToken::new();
        let mut registrations = vec![local.register(token.clone())];
        if let Some(session) = session {
            registrations.push(session.register(token.clone()));
        }
        Self {
            token,
            _registrations: registrations,
        }
    }
}

#[pyclass(module = "sparkless.sql.session")]
struct PySparkSession {
    inner: SparkSession,
    external_catalog_tables: external_catalog::ResolvedTables,
    active_actions: std::sync::Arc<ActionRegistry>,
    /// Writable from Python so conftest/fixtures can set backend_type = "robin" (e.g. under pytest-xdist).
    backend_type: std::sync::Mutex<String>,
    /// Set when `stop()` is called; prevents reusing this wrapper from `getOrCreate`.
    stopped: std::sync::atomic::AtomicBool,
    #[cfg(feature = "delta")]
    delta_map_view_paths:
        std::sync::Mutex<std::collections::HashMap<String, std::collections::HashSet<String>>>,
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
        slf.into_py_any(py).unwrap()
    }

    /// PySpark: spark.conf.get(key, default=None) returns value or default as string.
    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<&str>) -> PyResult<String> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
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
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let mut sess = session.borrow_mut();
        sess.inner.set_config(key, value);
        Ok(())
    }

    fn is_case_sensitive(&self, py: Python<'_>) -> PyResult<bool> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
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
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let sess = session.borrow();
        Ok(sess
            .inner
            .app_name()
            .unwrap_or_else(|| "sparkless".to_string()))
    }

    /// PySpark parity: SparkContext.appName camelCase alias for app_name.
    #[getter]
    #[pyo3(name = "appName")]
    fn app_name_camel(&self, py: Python<'_>) -> PyResult<String> {
        self.app_name(py)
    }

    /// PySpark parity: version is a property (read-only string), not a method.
    #[getter]
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }

    /// PySpark parity: set JVM/log4j log level. No-op in sparkless (no JVM).
    #[pyo3(name = "setLogLevel")]
    fn set_log_level(&self, _log_level: &str) {}
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
            .cast::<PySparkSession>()
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
    fn drop_temp_view(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let dropped = session.inner.drop_temp_view(name);
        #[cfg(feature = "delta")]
        session
            .delta_map_view_paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name);
        Ok(dropped)
    }

    #[pyo3(name = "dropGlobalTempView")]
    fn drop_global_temp_view(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let dropped = session.inner.drop_global_temp_view(name);
        #[cfg(feature = "delta")]
        delta_global_map_view_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&format!("global_temp.{name}"));
        Ok(dropped)
    }

    #[pyo3(name = "dropTable")]
    fn drop_table(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        Ok(session.inner.drop_table(name))
    }

    #[pyo3(name = "listDatabases")]
    fn list_databases(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDatabase>>> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?;
        let sess = session.borrow();
        let names = sess.inner.list_database_names();
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            out.push(Py::new(py, PyDatabase { name: n })?);
        }
        Ok(out)
    }

    #[pyo3(name = "setCurrentDatabase")]
    fn set_current_database(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        session.inner.set_current_database(name).map_err(to_py_err)
    }

    #[pyo3(name = "currentDatabase")]
    fn current_database(&self, py: Python<'_>) -> PyResult<String> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
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
            .cast::<PySparkSession>()
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
        session.inner.try_table_exists(&full).map_err(to_py_err)
    }

    #[pyo3(name = "getTable", signature = (tableName))]
    fn get_table(&self, py: Python<'_>, tableName: &str) -> PyResult<Py<PyTable>> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let (db, tbl) = if let Some((db, tbl)) = tableName.split_once('.') {
            (db.to_string(), tbl.to_string())
        } else {
            ("default".to_string(), tableName.to_string())
        };
        let full = format!("{db}.{tbl}");
        if !session.inner.try_table_exists(&full).map_err(to_py_err)?
            && !session.inner.try_table_exists(&tbl).map_err(to_py_err)?
        {
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
            .cast::<PySparkSession>()
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
            .cast::<PySparkSession>()
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
            .cast::<PySparkSession>()
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
            .cast::<PySparkSession>()
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
            .cast::<PySparkSession>()
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

/// PySpark drop(*cols): str or Column (or list/tuple of those).
fn py_tuple_or_single_to_drop_specs(
    tup: &Bound<'_, PyTuple>,
) -> PyResult<Vec<robin_sparkless::DropColumnSpec>> {
    use robin_sparkless::DropColumnSpec;

    fn push_item(item: &Bound<'_, PyAny>, out: &mut Vec<DropColumnSpec>) -> PyResult<()> {
        if let Ok(py_col) = item.cast::<PyColumn>() {
            out.push(DropColumnSpec {
                name: py_col.borrow().inner.name().to_string(),
                from_column_ref: true,
            });
            return Ok(());
        }
        if let Ok(s) = item.extract::<String>() {
            out.push(DropColumnSpec {
                name: s,
                from_column_ref: false,
            });
            return Ok(());
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "drop() expects column names as str or Column",
        ))
    }

    if tup.len() == 0 {
        return Ok(Vec::new());
    }
    if tup.len() == 1 {
        let item = tup.get_item(0)?;
        if let Ok(list) = item.cast::<PyList>() {
            let mut out = Vec::with_capacity(list.len());
            for x in list.iter() {
                push_item(&x, &mut out)?;
            }
            return Ok(out);
        }
        if let Ok(inner_tup) = item.cast::<PyTuple>() {
            let mut out = Vec::with_capacity(inner_tup.len());
            for x in inner_tup.iter() {
                push_item(&x, &mut out)?;
            }
            return Ok(out);
        }
        let mut out = Vec::new();
        push_item(&item, &mut out)?;
        return Ok(out);
    }
    let mut out = Vec::with_capacity(tup.len());
    for item in tup.iter() {
        push_item(&item, &mut out)?;
    }
    Ok(out)
}

/// PySpark drop(*cols): single str or list of str -> Vec<String>
fn py_tuple_or_single_to_vec_string(tup: &Bound<'_, PyTuple>) -> PyResult<Vec<String>> {
    if tup.len() == 0 {
        return Ok(Vec::new());
    }
    if tup.len() == 1 {
        let item = tup.get_item(0)?;
        if let Ok(list) = item.cast::<PyList>() {
            let mut out = Vec::with_capacity(list.len());
            for x in list.iter() {
                out.push(x.extract::<String>()?);
            }
            return Ok(out);
        }
        if let Ok(inner_tup) = item.cast::<PyTuple>() {
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

/// Parse orderBy(..., ascending=...) into a vec of bools (one per column).
/// PySpark: ascending can be None (all True), a single bool, or a list of bools.
fn parse_order_by_ascending(ascending: Option<&Bound<'_, PyAny>>, n: usize) -> PyResult<Vec<bool>> {
    let mut asc = match ascending {
        None => return Ok(vec![true; n]),
        Some(any) => {
            if let Ok(b) = any.extract::<bool>() {
                vec![b; n]
            } else if let Ok(list) = any.cast::<PyList>() {
                let mut v: Vec<bool> = Vec::with_capacity(list.len());
                for item in list.iter() {
                    v.push(item.extract::<bool>()?);
                }
                while v.len() < n {
                    v.push(true);
                }
                v.truncate(n);
                v
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "orderBy() ascending must be a bool or a list of bools",
                ));
            }
        }
    };
    while asc.len() < n {
        asc.push(true);
    }
    asc.truncate(n);
    Ok(asc)
}

/// Normalize column spec to Vec<String>: "x" | col("x") | ["x","y"] | (col("a"),) -> Vec<String>
/// Resolve a single column spec to a column name (str or Column -> name). Used by GroupedData.avg/sum/min/max.
fn py_col_to_name(any: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = any.extract::<String>() {
        return Ok(s);
    }
    if let Ok(c) = any.cast::<PyColumn>() {
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
    if let Ok(c) = any.cast::<PyColumn>() {
        return Ok(vec![c.borrow().inner.name().to_string()]);
    }
    if let Ok(list) = any.cast::<PyList>() {
        let mut out = Vec::new();
        for item in list.iter() {
            out.extend(py_one_or_many_cols(&item)?);
        }
        return Ok(out);
    }
    if let Ok(tup) = any.cast::<PyTuple>() {
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
    if let Ok(c) = item.cast::<PyColumn>() {
        return Ok(vec![c.borrow().inner.name().to_string()]);
    }
    if let Ok(list) = item.cast::<PyList>() {
        let mut out = Vec::new();
        for sub in list.iter() {
            out.extend(py_one_or_many_cols(&sub)?);
        }
        return Ok(out);
    }
    if let Ok(tup) = item.cast::<PyTuple>() {
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
    if let Ok(c) = item.cast::<PyColumn>() {
        return Ok(vec![GroupBySpec::Column(Box::new(
            c.borrow().inner.clone(),
        ))]);
    }
    if let Ok(list) = item.cast::<PyList>() {
        let mut out = Vec::new();
        for sub in list.iter() {
            out.extend(py_group_by_specs(&sub)?);
        }
        return Ok(out);
    }
    if let Ok(tup) = item.cast::<PyTuple>() {
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
    if let Ok(col) = on.cast::<PyColumn>() {
        return Ok(vec![col.borrow().inner.name().to_string()]);
    }
    if let Ok(list) = on.cast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(s) = item.extract::<String>() {
                out.push(s);
            } else if let Ok(c) = item.cast::<PyColumn>() {
                out.push(c.borrow().inner.name().to_string());
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "join(on=...) list elements must be str or Column",
                ));
            }
        }
        return Ok(out);
    }
    if let Ok(tup) = on.cast::<PyTuple>() {
        let mut out = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            if let Ok(s) = item.extract::<String>() {
                out.push(s);
            } else if let Ok(c) = item.cast::<PyColumn>() {
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

/// PySpark allows `join(other, [cond1, cond2, ...])` where each condition is a boolean Column.
/// Combine them with AND. Returns `Some(expr)` when any element is a complex expression (name
/// `"<expr>"`). Returns `None` for column-name lists (all strings) or simple same-name keys.
fn try_combine_join_condition_list(
    on: &Bound<'_, PyAny>,
) -> PyResult<Option<robin_sparkless::Expr>> {
    use robin_sparkless::Expr as RsExpr;

    let items: Vec<Bound<'_, PyAny>> = if let Ok(list) = on.cast::<PyList>() {
        list.iter().collect()
    } else if let Ok(tup) = on.cast::<PyTuple>() {
        tup.iter().collect()
    } else {
        return Ok(None);
    };

    if items.is_empty() {
        return Ok(None);
    }

    let mut condition_exprs: Vec<RsExpr> = Vec::new();
    let mut saw_string = false;

    for item in items {
        if item.extract::<String>().is_ok() {
            saw_string = true;
            continue;
        }
        if let Ok(c) = item.cast::<PyColumn>() {
            let col = c.borrow();
            if col.inner.name() == "<expr>" {
                condition_exprs.push(col.inner.clone().into_expr());
            }
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "join(on=...) list elements must be str or Column",
            ));
        }
    }

    if saw_string {
        if !condition_exprs.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "join(on=...) cannot mix str column names and Column expressions",
            ));
        }
        return Ok(None);
    }

    if condition_exprs.is_empty() {
        return Ok(None);
    }

    let mut combined = condition_exprs.remove(0);
    for e in condition_exprs {
        combined = combined.and(e);
    }
    Ok(Some(combined))
}

/// Resolves join key pairs, assigning each key to the side that owns it.
/// If projection lost an alias, the qualified suffix is used as a fallback.
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

fn rdd_max_rows() -> i64 {
    std::env::var("SPARKLESS_RDD_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000_000)
}

fn check_rdd_dataframe_size(df: &DataFrame) -> PyResult<()> {
    let count = df.count().map_err(to_py_err)? as i64;
    let max = rdd_max_rows();
    if count > max {
        return Err(SparklessError::new_err(format!(
            "RDD operations are limited to {max} rows (source has {count}); \
             set SPARKLESS_RDD_MAX_ROWS to override"
        )));
    }
    Ok(())
}

/// RDD-like wrapper for createDataFrame(df.rdd, schema) (issue #1147 / #361) and df.rdd.flatMap (issue #408 / #1238).
/// When from df.rdd: holds source_df and inner (DataFrame). When from flatMap/map/filter: holds elements (materialized list).
#[pyclass]
struct PyRDD {
    /// When from df.rdd: the DataFrame as PyDataFrame for collect() to get Row objects.
    source_df: Option<Py<PyDataFrame>>,
    /// When from df.rdd: same DataFrame for _to_pylist (collect_as_json_rows).
    inner: Option<DataFrame>,
    /// When from flatMap/map/filter: materialized list of Python objects.
    elements: Option<Vec<PyObject>>,
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
        let Some(ref inner) = self.inner else {
            // Materialized RDD (from flatMap/map): return list of single-key dicts for createDataFrame(rdd, schema).
            let Some(ref elements) = self.elements else {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "RDD has no source",
                ));
            };
            let (names, _use_preferred_keys): (Vec<String>, bool) = if let Some(arg) =
                preferred_names
            {
                let lst_opt: Option<Bound<'py, PyList>> = if let Ok(lst) = arg.cast::<PyList>() {
                    Some(lst.clone())
                } else if let Ok(tup) = arg.cast::<pyo3::types::PyTuple>() {
                    match tup.len() {
                        1 => tup
                            .get_item(0)
                            .ok()
                            .and_then(|item| item.cast::<PyList>().ok().cloned()),
                        _ => None,
                    }
                } else {
                    None
                };
                if let Some(lst) = lst_opt {
                    let n: Vec<String> = lst
                        .iter()
                        .filter_map(|it| it.extract::<String>().ok())
                        .collect();
                    if n.is_empty() {
                        (vec!["_1".to_string()], false)
                    } else {
                        (n, true)
                    }
                } else {
                    (vec!["_1".to_string()], false)
                }
            } else {
                (vec!["_1".to_string()], false)
            };
            let out = PyList::empty(py);
            let key = names.first().map(|s| s.as_str()).unwrap_or("_1");
            for elem in elements {
                let dict = PyDict::new(py);
                dict.set_item(key, elem.bind(py))?;
                out.append(dict)?;
            }
            return Ok(out);
        };
        let (col_names, rows, _) = inner.collect_as_json_rows_with_names().map_err(to_py_err)?;
        // call_method1(..., (py_names,)) passes a tuple to Python; unwrap single-element tuple.
        let (names, use_preferred_keys): (Vec<String>, bool) = if let Some(arg) = preferred_names {
            let lst_opt: Option<Bound<'py, PyList>> = if let Ok(lst) = arg.cast::<PyList>() {
                Some(lst.clone())
            } else if let Ok(tup) = arg.cast::<pyo3::types::PyTuple>() {
                match tup.len() {
                    1 => tup
                        .get_item(0)
                        .ok()
                        .and_then(|item| item.cast::<PyList>().ok().cloned()),
                    _ => None,
                }
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
                    inner
                        .schema()
                        .ok()
                        .map(|s| s.logical_fields().iter().map(|f| f.name.clone()).collect())
                        .unwrap_or_else(|| col_names.clone()),
                    false,
                )
            }
        } else {
            (
                inner
                    .schema()
                    .ok()
                    .map(|s| s.logical_fields().iter().map(|f| f.name.clone()).collect())
                    .unwrap_or_else(|| col_names.clone()),
                false,
            )
        };
        let out = PyList::empty(py);
        for row in rows {
            if use_preferred_keys {
                // Return list-of-lists so createDataFrame uses positional column order; avoids dict key lookup issues.
                let row_list = PyList::empty(py);
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
                let dict = PyDict::new(py);
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

    /// Collect elements (rows when from df.rdd, or materialized list).
    fn collect_elements(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        if let Some(ref elements) = self.elements {
            return Ok(elements.iter().map(|o| o.clone_ref(py)).collect());
        }
        let Some(ref source_df) = self.source_df else {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "RDD has no source",
            ));
        };
        let df = source_df.bind(py).cast::<PyDataFrame>()?;
        check_rdd_dataframe_size(&df.borrow().inner)?;
        let list_obj = df.call_method0("collect")?;
        let list = list_obj.cast::<PyList>()?;
        let mut vec = Vec::with_capacity(list.len());
        for item in list.iter() {
            vec.push(item.into_py_any(py)?);
        }
        Ok(vec)
    }

    fn collect_elements_limited(&self, py: Python<'_>, limit: usize) -> PyResult<Vec<PyObject>> {
        if let Some(ref elements) = self.elements {
            let n = limit.min(elements.len());
            return Ok(elements.iter().take(n).map(|o| o.clone_ref(py)).collect());
        }
        let Some(ref source_df) = self.source_df else {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "RDD has no source",
            ));
        };
        let df = source_df.bind(py).cast::<PyDataFrame>()?;
        let limited = df.borrow().inner.limit(limit).map_err(to_py_err)?;
        let rows = limited.collect_as_json_rows().map_err(to_py_err)?;
        let mut vec = Vec::with_capacity(rows.len());
        for row in rows {
            let dict = PyDict::new(py);
            for (k, v) in row {
                dict.set_item(k, json_to_py(&v, py)?)?;
            }
            vec.push(dict.into_py_any(py)?);
        }
        Ok(vec)
    }

    #[pyo3(name = "flatMap")]
    fn flat_map(slf: PyRef<Self>, py: Python<'_>, func: &Bound<'_, PyAny>) -> PyResult<PyRDD> {
        let elements = slf.collect_elements(py)?;
        let mut out: Vec<PyObject> = Vec::new();
        for item in elements {
            let item_bound = item.bind(py);
            let result = func.call1((item_bound,))?;
            for x in result.try_iter()? {
                let x: Bound<'_, PyAny> = x?;
                out.push(x.into_py_any(py)?);
            }
        }
        Ok(PyRDD {
            source_df: None,
            inner: None,
            elements: Some(out),
        })
    }

    fn map(slf: PyRef<Self>, py: Python<'_>, func: &Bound<'_, PyAny>) -> PyResult<PyRDD> {
        let elements = slf.collect_elements(py)?;
        let mut out: Vec<PyObject> = Vec::with_capacity(elements.len());
        for item in elements {
            let item_bound = item.bind(py);
            let mapped = func.call1((item_bound,))?;
            out.push(mapped.into_py_any(py)?);
        }
        Ok(PyRDD {
            source_df: None,
            inner: None,
            elements: Some(out),
        })
    }

    fn filter(slf: PyRef<Self>, py: Python<'_>, func: &Bound<'_, PyAny>) -> PyResult<PyRDD> {
        let elements = slf.collect_elements(py)?;
        let mut out: Vec<PyObject> = Vec::new();
        for item in elements {
            let item_bound = item.bind(py);
            let keep = func.call1((item_bound,))?.is_truthy()?;
            if keep {
                out.push(item);
            }
        }
        Ok(PyRDD {
            source_df: None,
            inner: None,
            elements: Some(out),
        })
    }

    fn collect(&self, py: Python<'_>) -> PyResult<PyObject> {
        let elements = self.collect_elements(py)?;
        let list = PyList::new(py, elements.iter().map(|o| o.bind(py)))?;
        list.into_py_any(py)
    }

    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        self.collect_elements(py).map(|e| e.len())
    }

    fn take(&self, py: Python<'_>, n: usize) -> PyResult<PyObject> {
        let elements = self.collect_elements_limited(py, n)?;
        let list = PyList::new(py, elements.iter().map(|o| o.bind(py)))?;
        list.into_py_any(py)
    }

    fn first(&self, py: Python<'_>) -> PyResult<PyObject> {
        let elements = self.collect_elements(py)?;
        let first = elements
            .first()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("RDD is empty"))?;
        Ok(first.clone_ref(py))
    }

    fn reduce(&self, py: Python<'_>, func: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let elements = self.collect_elements(py)?;
        if elements.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Cannot reduce empty RDD",
            ));
        }
        let mut acc = elements[0].clone_ref(py);
        for item in elements.iter().skip(1) {
            acc = func.call1((acc.bind(py), item.bind(py)))?.into_py_any(py)?;
        }
        Ok(acc)
    }

    /// PySpark parity: True when the RDD has no elements.
    #[pyo3(name = "isEmpty")]
    fn is_empty_rdd(&self, py: Python<'_>) -> PyResult<bool> {
        if let Some(ref elements) = self.elements {
            return Ok(elements.is_empty());
        }
        if let Some(ref inner) = self.inner {
            return Ok(inner.is_empty());
        }
        Ok(self.collect_elements(py)?.is_empty())
    }

    /// PySpark parity: return an RDD with duplicate elements removed (first occurrence kept).
    fn distinct(slf: PyRef<Self>, py: Python<'_>) -> PyResult<PyRDD> {
        let elements = slf.collect_elements(py)?;
        let mut out: Vec<PyObject> = Vec::new();
        for item in elements {
            let item_bound = item.bind(py);
            let mut duplicate = false;
            for prev in &out {
                if item_bound.eq(prev.bind(py))? {
                    duplicate = true;
                    break;
                }
            }
            if !duplicate {
                out.push(item);
            }
        }
        Ok(PyRDD {
            source_df: None,
            inner: None,
            elements: Some(out),
        })
    }
}

#[pyclass]
struct PyDataFrame {
    inner: DataFrame,
    active_actions: std::sync::Arc<ActionRegistry>,
    session_actions: Option<std::sync::Arc<ActionRegistry>>,
    /// When set, df.schema returns this (preserves ArrayType.containsNull etc. from createDataFrame(schema=StructType(...))).
    schema_cache: Option<Py<PyAny>>,
    /// When true, count()/collect() should raise an error due to mixed dict/list row shapes in
    /// schema inference (PySpark-style behavior for createDataFrame([{...}, (...)]) with no schema).
    mixed_row_shape_error_on_collect: bool,
    /// Original PyArrow schema: Polars maps and arrays of key/value structs share a physical dtype.
    arrow_schema_cache: Option<Py<PyAny>>,
    /// Source Arrow fields whose values or type changed in a derived DataFrame.
    arrow_schema_invalidated_fields: std::collections::HashSet<String>,
    arrow_array_struct_paths: std::collections::HashSet<String>,
    logical_map_columns: std::collections::HashSet<String>,
    logical_map_paths: std::collections::HashSet<String>,
}

#[pyclass(module = "sparkless.sql.session", unsendable)]
struct PyArrowBatchIterator {
    batches: Option<robin_sparkless::DataFrameBatches>,
    template: Py<PyDataFrame>,
    max_rows: usize,
    max_bytes: usize,
    pending: Option<PlDataFrame>,
    pending_offset: usize,
    deferred_error: Option<PyErr>,
    action: Option<ActiveAction>,
    closed: bool,
}

impl PyArrowBatchIterator {
    fn next_source_batch(
        &mut self,
        py: Python<'_>,
    ) -> Option<Result<PlDataFrame, robin_sparkless_core::EngineError>> {
        let batches = self.batches.take()?;
        let (batches, next) = py.detach(move || {
            let mut batches = batches;
            let next = batches.next();
            (batches, next)
        });
        self.batches = Some(batches);
        next
    }

    fn close_inner(&mut self) {
        if self.closed {
            return;
        }
        if let Some(action) = &self.action {
            action.token.cancel();
        }
        self.batches.take();
        self.pending.take();
        self.pending_offset = 0;
        self.action.take();
        self.closed = true;
    }

    fn next_source_row(
        &mut self,
        py: Python<'_>,
    ) -> Option<Result<PlDataFrame, robin_sparkless_core::EngineError>> {
        if let Some(pending) = self.pending.as_ref() {
            let row = pending.slice(self.pending_offset as i64, 1);
            self.pending_offset += 1;
            if self.pending_offset >= pending.height() {
                self.pending.take();
                self.pending_offset = 0;
            }
            return Some(Ok(row));
        }
        self.next_source_batch(py)
    }

    fn combine_rows(rows: &[PlDataFrame]) -> Result<PlDataFrame, polars::prelude::PolarsError> {
        let mut combined = rows.first().expect("non-empty rows").clone();
        for row in &rows[1..] {
            combined.vstack_mut(row)?;
        }
        Ok(combined)
    }

    fn export_candidate(&self, py: Python<'_>, batch: &PlDataFrame) -> PyResult<(PyObject, usize)> {
        let template = self.template.bind(py).borrow();
        let dataframe = template.wrap_derived(DataFrame::from_polars_with_options(
            batch.clone(),
            template.inner.is_case_sensitive(),
        ));
        drop(template);
        let dataframe = Py::new(py, dataframe)?;
        let table = dataframe.bind(py).call_method0("toArrow")?;
        let batches = table.call_method0("to_batches")?;
        if batches.len()? == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "PyArrow returned no RecordBatch for a non-empty DataFrame batch",
            ));
        }
        let batch = batches.get_item(0)?;
        let nbytes = batch.getattr("nbytes")?.extract::<usize>()?;
        Ok((batch.into_py_any(py)?, nbytes))
    }

    fn next_batch(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        if self.closed {
            return Ok(None);
        }

        if self.pending.is_none() {
            if let Some(error) = self.deferred_error.take() {
                self.close_inner();
                return Err(error);
            }
        }

        let mut rows = Vec::new();
        let mut estimated_bytes = 0usize;
        let mut source_ended = false;
        loop {
            if self.pending.is_none() {
                if let Some(error) = self.deferred_error.take() {
                    if rows.is_empty() {
                        self.close_inner();
                        return Err(error);
                    }
                    self.deferred_error = Some(error);
                    break;
                }
            }
            let token = self
                .action
                .as_ref()
                .expect("open batch iterators have an action")
                .token
                .clone();
            if let Err(error) = token.check_cancelled() {
                self.close_inner();
                return Err(to_py_err(error));
            }

            let row = match self.next_source_row(py) {
                Some(Ok(batch)) if batch.height() > 0 => batch,
                Some(Ok(_)) => continue,
                Some(Err(error)) => {
                    if !rows.is_empty() {
                        self.deferred_error = Some(to_py_err(error));
                        break;
                    }
                    self.close_inner();
                    return Err(to_py_err(error));
                }
                None => {
                    source_ended = true;
                    break;
                }
            };
            if token.is_cancelled() {
                self.close_inner();
                return Err(to_py_err("operation cancelled"));
            }

            let row_bytes = row.estimated_size();
            if !rows.is_empty() && estimated_bytes.saturating_add(row_bytes) > self.max_bytes {
                self.pending = Some(row);
                self.pending_offset = 0;
                break;
            }
            estimated_bytes = estimated_bytes.saturating_add(row_bytes);
            rows.push(row);
            if rows.len() >= self.max_rows || estimated_bytes >= self.max_bytes {
                break;
            }
        }

        if rows.is_empty() {
            if source_ended {
                self.close_inner();
            }
            return Ok(None);
        }

        let combined = match Self::combine_rows(&rows) {
            Ok(combined) => combined,
            Err(error) => {
                self.close_inner();
                return Err(to_py_err(error));
            }
        };
        let (batch, nbytes) = match self.export_candidate(py, &combined) {
            Ok(candidate) => candidate,
            Err(error) => {
                self.close_inner();
                return Err(error);
            }
        };
        if nbytes <= self.max_bytes {
            if source_ended {
                self.close_inner();
            }
            return Ok(Some(batch));
        }

        if rows.len() == 1 {
            self.close_inner();
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "a single Arrow row requires {nbytes} bytes, exceeding max_bytes={}",
                self.max_bytes
            )));
        }
        let mut prefix_rows = rows.len() / 2;
        while prefix_rows > 0 {
            let prefix = combined.slice(0, prefix_rows);
            let (batch, prefix_bytes) = match self.export_candidate(py, &prefix) {
                Ok(candidate) => candidate,
                Err(error) => {
                    self.close_inner();
                    return Err(error);
                }
            };
            if prefix_bytes <= self.max_bytes {
                let mut pending = combined.slice(prefix_rows as i64, rows.len() - prefix_rows);
                if let Some(remaining_pending) = self.pending.take() {
                    let remaining_pending = remaining_pending.slice(
                        self.pending_offset as i64,
                        remaining_pending.height() - self.pending_offset,
                    );
                    if let Err(error) = pending.vstack_mut(&remaining_pending) {
                        self.close_inner();
                        return Err(to_py_err(error));
                    }
                }
                self.pending = Some(pending);
                self.pending_offset = 0;
                return Ok(Some(batch));
            }
            prefix_rows /= 2;
        }
        self.close_inner();
        Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "a single Arrow row exceeds max_bytes={}",
            self.max_bytes
        )))
    }
}

#[pymethods]
impl PyArrowBatchIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        self.next_batch(py)?
            .ok_or_else(|| pyo3::exceptions::PyStopIteration::new_err(()))
    }

    fn close(&mut self) {
        self.close_inner();
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) {
        self.close_inner();
    }
}

impl PyDataFrame {
    fn union_aligns_by_name(
        &self,
        left_names: &[String],
        right_names: &[String],
        by_name: bool,
    ) -> bool {
        if by_name {
            return true;
        }
        if left_names.len() != right_names.len() {
            return false;
        }

        // Match transformations::union: its alignment mode follows the left
        // DataFrame's setting, even when the right DataFrame came from another
        // session with a different case-sensitivity setting.
        let case_sensitive = self.inner.is_case_sensitive();
        let right_names: std::collections::HashSet<String> = if case_sensitive {
            right_names.iter().cloned().collect()
        } else {
            right_names.iter().map(|name| name.to_lowercase()).collect()
        };
        left_names.iter().all(|name| {
            if case_sensitive {
                right_names.contains(name)
            } else {
                right_names.contains(&name.to_lowercase())
            }
        })
    }

    fn resolve_union_column_name(&self, names: &[String], name: &str) -> Option<String> {
        if self.inner.is_case_sensitive() {
            names
                .iter()
                .find(|candidate| candidate.as_str() == name)
                .cloned()
        } else {
            let name_lower = name.to_lowercase();
            names
                .iter()
                .find(|candidate| candidate.to_lowercase() == name_lower)
                .cloned()
        }
    }

    fn column_uses_arrow_array_struct(&self, name: &str) -> bool {
        let source_path = if let Ok(resolved) = self.inner.resolve_column_name(name) {
            arrow_field_path("", &resolved)
        } else {
            let mut parts = name.split('.');
            let root = parts.next().unwrap_or(name);
            let root = self
                .inner
                .resolve_column_name(root)
                .unwrap_or_else(|_| root.to_string());
            parts.fold(arrow_field_path("", &root), |path, field| {
                arrow_field_path(&path, field)
            })
        };
        self.arrow_array_struct_paths.iter().any(|path| {
            path == &source_path
                || path
                    .strip_prefix(source_path.as_str())
                    .is_some_and(|tail| tail.starts_with(['.', '[', '{']))
        })
    }

    fn column_dtype_for_metadata(&self, name: &str) -> Option<robin_sparkless::polars::PlDataType> {
        if let Ok(resolved) = self.inner.resolve_column_name(name) {
            if let Some(dtype) = self.inner.get_column_dtype(&resolved) {
                return Some(dtype);
            }
        }

        let (root, nested_fields) = name.split_once('.')?;
        let root = self.inner.resolve_column_name(root).ok()?;
        let mut dtype = self.inner.get_column_dtype(&root)?;
        for field_name in nested_fields.split('.') {
            let fields = match &dtype {
                robin_sparkless::polars::PlDataType::Struct(fields) => fields,
                _ => return None,
            };
            let field = fields.iter().find(|field| {
                field.name().as_str() == field_name
                    || field.name().as_str().eq_ignore_ascii_case(field_name)
            })?;
            dtype = field.dtype().clone();
        }
        Some(dtype)
    }

    fn logical_map_path_tails(&self, name: &str) -> std::collections::HashSet<String> {
        let resolved = self.inner.resolve_column_name(name).ok();
        let source_path = if let Some(source) = &resolved {
            arrow_field_path("", source)
        } else {
            let mut parts = name.split('.');
            let root = parts.next().unwrap_or(name);
            let root = self
                .inner
                .resolve_column_name(root)
                .unwrap_or_else(|_| root.to_string());
            parts.fold(arrow_field_path("", &root), |path, field| {
                arrow_field_path(&path, field)
            })
        };
        let mut tails = std::collections::HashSet::new();
        for path in &self.logical_map_paths {
            if path == &source_path {
                tails.insert(String::new());
            } else if let Some(tail) = path.strip_prefix(source_path.as_str()) {
                if tail.starts_with(['.', '[', '{']) {
                    tails.insert(tail.to_string());
                }
            }
        }
        if self
            .logical_map_columns
            .contains(resolved.as_deref().unwrap_or(name))
        {
            tails.insert(String::new());
        }
        tails
    }

    fn arrow_array_struct_path_tails(&self, name: &str) -> std::collections::HashSet<String> {
        let resolved = self.inner.resolve_column_name(name).ok();
        let source_path = if let Some(source) = &resolved {
            arrow_field_path("", source)
        } else {
            let mut parts = name.split('.');
            let root = parts.next().unwrap_or(name);
            let root = self
                .inner
                .resolve_column_name(root)
                .unwrap_or_else(|_| root.to_string());
            parts.fold(arrow_field_path("", &root), |path, field| {
                arrow_field_path(&path, field)
            })
        };
        let mut tails = std::collections::HashSet::new();
        for path in &self.arrow_array_struct_paths {
            if path == &source_path {
                tails.insert(String::new());
            } else if let Some(tail) = path.strip_prefix(source_path.as_str()) {
                if tail.starts_with(['.', '[', '{']) {
                    tails.insert(tail.to_string());
                }
            }
        }
        tails
    }

    fn expression_column_references(expr: &robin_sparkless::Expr) -> Vec<String> {
        use robin_sparkless::Expr;

        fn visit(expr: &Expr, references: &mut Vec<String>) {
            match expr {
                Expr::Column(name) => references.push(name.to_string()),
                Expr::Alias(inner, _)
                | Expr::Cast { expr: inner, .. }
                | Expr::Sort { expr: inner, .. }
                | Expr::Explode { input: inner, .. }
                | Expr::KeepName(inner)
                | Expr::RenameAlias { expr: inner, .. } => visit(inner, references),
                Expr::BinaryExpr { left, right, .. } => {
                    visit(left, references);
                    visit(right, references);
                }
                Expr::Ternary {
                    predicate,
                    truthy,
                    falsy,
                } => {
                    visit(predicate, references);
                    visit(truthy, references);
                    visit(falsy, references);
                }
                Expr::Function { input, .. }
                | Expr::AnonymousFunction { input, .. }
                | Expr::Display { inputs: input, .. } => {
                    for input in input {
                        visit(input, references);
                    }
                }
                Expr::Gather { expr, idx, .. } => {
                    visit(expr, references);
                    visit(idx, references);
                }
                Expr::SortBy { expr, by, .. } => {
                    visit(expr, references);
                    for by_expr in by {
                        visit(by_expr, references);
                    }
                }
                Expr::Filter { input, by } => {
                    visit(input, references);
                    visit(by, references);
                }
                Expr::Over {
                    function,
                    partition_by,
                    order_by,
                    ..
                } => {
                    visit(function, references);
                    for expr in partition_by {
                        visit(expr, references);
                    }
                    if let Some((expr, _)) = order_by {
                        visit(expr, references);
                    }
                }
                Expr::Slice {
                    input,
                    offset,
                    length,
                } => {
                    visit(input, references);
                    visit(offset, references);
                    visit(length, references);
                }
                _ => {}
            }
        }

        let mut references = Vec::new();
        visit(expr, &mut references);
        references
    }

    fn expression_preserves_map_value(expr: &robin_sparkless::Expr) -> bool {
        use robin_sparkless::Expr;

        match expr {
            Expr::Column(_) => true,
            Expr::Alias(inner, _)
            | Expr::KeepName(inner)
            | Expr::RenameAlias { expr: inner, .. } => Self::expression_preserves_map_value(inner),
            Expr::Ternary { truthy, falsy, .. } => {
                Self::expression_preserves_map_value(truthy)
                    && Self::expression_preserves_map_value(falsy)
            }
            Expr::Function {
                input,
                function: polars::prelude::FunctionExpr::Coalesce,
            } => !input.is_empty() && input.iter().all(Self::expression_preserves_map_value),
            Expr::Literal(value) => value.is_null(),
            _ => false,
        }
    }

    fn copy_computed_map_metadata(
        &self,
        preserves_map_value: bool,
        references: &[String],
        output: &str,
        target: &mut Self,
    ) {
        if !preserves_map_value {
            return;
        }
        let Some(output_dtype) = target.inner.get_column_dtype(output) else {
            return;
        };
        // Keep only paths marked as maps on every matching branch. Physical
        // list/struct dtypes alone cannot distinguish maps from arrays.
        let mut common_map_paths: Option<std::collections::HashSet<String>> = None;
        for source in references {
            let Some(source_dtype) = self.column_dtype_for_metadata(source) else {
                continue;
            };
            if source_dtype == output_dtype {
                let source_map_paths = self.logical_map_path_tails(source);
                if let Some(common_map_paths) = &mut common_map_paths {
                    common_map_paths.retain(|path| source_map_paths.contains(path));
                } else {
                    common_map_paths = Some(source_map_paths);
                }
            }
        }
        let Some(common_map_paths) = common_map_paths else {
            return;
        };
        let output_path = arrow_field_path("", output);
        for tail in common_map_paths {
            target
                .logical_map_paths
                .insert(format!("{output_path}{tail}"));
            if tail.is_empty() {
                target.logical_map_columns.insert(output.to_string());
            }
        }
    }

    fn expr_uses_arrow_array_struct(&self, expr: &robin_sparkless::Expr) -> bool {
        use robin_sparkless::Expr;

        match expr {
            Expr::Column(name) => self.column_uses_arrow_array_struct(name.as_str()),
            Expr::Alias(inner, _)
            | Expr::Cast { expr: inner, .. }
            | Expr::Sort { expr: inner, .. }
            | Expr::Explode { input: inner, .. }
            | Expr::KeepName(inner)
            | Expr::RenameAlias { expr: inner, .. } => self.expr_uses_arrow_array_struct(inner),
            Expr::BinaryExpr { left, right, .. } => {
                self.expr_uses_arrow_array_struct(left) || self.expr_uses_arrow_array_struct(right)
            }
            Expr::Ternary {
                predicate,
                truthy,
                falsy,
            } => {
                self.expr_uses_arrow_array_struct(predicate)
                    || self.expr_uses_arrow_array_struct(truthy)
                    || self.expr_uses_arrow_array_struct(falsy)
            }
            Expr::Function { input, .. }
            | Expr::AnonymousFunction { input, .. }
            | Expr::Display { inputs: input, .. } => input
                .iter()
                .any(|input| self.expr_uses_arrow_array_struct(input)),
            Expr::Gather { expr, idx, .. } => {
                self.expr_uses_arrow_array_struct(expr) || self.expr_uses_arrow_array_struct(idx)
            }
            Expr::SortBy { expr, by, .. } => {
                self.expr_uses_arrow_array_struct(expr)
                    || by
                        .iter()
                        .any(|by_expr| self.expr_uses_arrow_array_struct(by_expr))
            }
            Expr::Filter { input, by } => {
                self.expr_uses_arrow_array_struct(input) || self.expr_uses_arrow_array_struct(by)
            }
            Expr::Over {
                function,
                partition_by,
                order_by,
                ..
            } => {
                self.expr_uses_arrow_array_struct(function)
                    || partition_by
                        .iter()
                        .any(|expr| self.expr_uses_arrow_array_struct(expr))
                    || order_by
                        .as_ref()
                        .is_some_and(|(expr, _)| self.expr_uses_arrow_array_struct(expr))
            }
            Expr::Slice {
                input,
                offset,
                length,
            } => {
                self.expr_uses_arrow_array_struct(input)
                    || self.expr_uses_arrow_array_struct(offset)
                    || self.expr_uses_arrow_array_struct(length)
            }
            _ => false,
        }
    }

    fn mark_array_struct_path(target: &mut Self, output: &str) {
        fn visit(data_type: &DataType, path: &str, paths: &mut std::collections::HashSet<String>) {
            match data_type {
                DataType::Array(element_type) => {
                    let is_key_value_struct = matches!(
                        element_type.as_ref(),
                        DataType::LogicalStruct(fields)
                            if fields.iter().any(|field| field.name == "key")
                                && fields.iter().any(|field| field.name == "value")
                    );
                    if is_key_value_struct {
                        paths.insert(path.to_string());
                    }
                    visit(element_type, &format!("{path}[]"), paths);
                }
                DataType::LogicalStruct(fields) => {
                    for field in fields {
                        visit(
                            &field.data_type,
                            &arrow_field_path(path, &field.name),
                            paths,
                        );
                    }
                }
                DataType::Map(key_type, value_type) => {
                    visit(key_type, &format!("{path}{{key}}"), paths);
                    visit(value_type, &format!("{path}{{value}}"), paths);
                }
                _ => {}
            }
        }

        let output_type = target.inner.schema_engine().ok().and_then(|schema| {
            schema
                .logical_fields()
                .iter()
                .find(|field| field.name == output)
                .map(|field| field.data_type.clone())
        });
        if let Some(output_type) = output_type {
            visit(
                &output_type,
                &arrow_field_path("", output),
                &mut target.arrow_array_struct_paths,
            );
        }
    }

    fn remove_metadata_root(target: &mut Self, root: &str) {
        target.logical_map_columns.remove(root);
        let root = arrow_field_path("", root);
        target.logical_map_paths.retain(|path| {
            path != &root
                && !path
                    .strip_prefix(root.as_str())
                    .is_some_and(|tail| tail.starts_with(['.', '[', '{']))
        });
        target.arrow_array_struct_paths.retain(|path| {
            path != &root
                && !path
                    .strip_prefix(root.as_str())
                    .is_some_and(|tail| tail.starts_with(['.', '[', '{']))
        });
    }

    fn remove_logical_map_metadata_root(target: &mut Self, root: &str) {
        target.logical_map_columns.remove(root);
        let root = arrow_field_path("", root);
        target.logical_map_paths.retain(|path| {
            path != &root
                && !path
                    .strip_prefix(root.as_str())
                    .is_some_and(|tail| tail.starts_with(['.', '[', '{']))
        });
    }

    fn column_source(expr: &robin_sparkless::Expr) -> Option<String> {
        match expr {
            robin_sparkless::Expr::Alias(inner, _) => Self::column_source(inner),
            robin_sparkless::Expr::Cast { expr, .. } => Self::column_source(expr),
            robin_sparkless::Expr::Column(column) => Some(column.to_string()),
            _ => None,
        }
    }

    fn aggregate_value_source(expr: &robin_sparkless::Expr) -> Option<String> {
        use robin_sparkless::Expr;

        match expr {
            Expr::Alias(inner, _)
            | Expr::KeepName(inner)
            | Expr::RenameAlias { expr: inner, .. } => Self::aggregate_value_source(inner),
            Expr::Cast { expr: inner, .. } => Self::aggregate_value_source(inner),
            Expr::Agg(
                AggExpr::First(input)
                | AggExpr::FirstNonNull(input)
                | AggExpr::Last(input)
                | AggExpr::LastNonNull(input),
            ) => Self::column_source(input),
            Expr::Agg(AggExpr::Min { input, .. } | AggExpr::Max { input, .. }) => {
                Self::column_source(input)
            }
            _ => None,
        }
    }

    fn wrap_value_aggregate_derived(
        &self,
        df: DataFrame,
        source: &str,
        leading_columns: usize,
    ) -> Self {
        let mut wrapped = Self::wrap(df);
        wrapped.session_actions = self.session_actions.clone();
        let Some(source_dtype) = self.column_dtype_for_metadata(source) else {
            return wrapped;
        };
        for output in wrapped
            .inner
            .columns()
            .unwrap_or_default()
            .into_iter()
            .skip(leading_columns)
        {
            if wrapped.inner.get_column_dtype(&output).as_ref() == Some(&source_dtype) {
                self.copy_column_metadata(source, &output, &mut wrapped);
            }
        }
        wrapped
    }

    fn wrap_aggregate_derived(
        &self,
        df: DataFrame,
        expressions: &[robin_sparkless::Expr],
        leading_columns: usize,
    ) -> Self {
        let mut wrapped = Self::wrap(df);
        wrapped.session_actions = self.session_actions.clone();
        let outputs = wrapped
            .inner
            .columns()
            .unwrap_or_default()
            .into_iter()
            .skip(leading_columns)
            .collect::<Vec<_>>();
        let sources = expressions
            .iter()
            .map(Self::aggregate_value_source)
            .collect::<Vec<_>>();

        if outputs.len() == sources.len() {
            for (source, output) in sources.iter().zip(outputs) {
                let Some(source) = source else {
                    continue;
                };
                if self.column_dtype_for_metadata(source) == wrapped.inner.get_column_dtype(&output)
                {
                    self.copy_column_metadata(source, &output, &mut wrapped);
                }
            }
            return wrapped;
        }

        // Pivot aggregations can expand one expression into several output
        // columns. Carry metadata only when every matching source has the same
        // logical map/array interpretation.
        for output in outputs {
            let Some(output_dtype) = wrapped.inner.get_column_dtype(&output) else {
                continue;
            };
            let matching_sources = sources
                .iter()
                .flatten()
                .filter(|source| {
                    self.column_dtype_for_metadata(source).as_ref() == Some(&output_dtype)
                })
                .collect::<Vec<_>>();
            let Some(first_source) = matching_sources.first() else {
                continue;
            };
            let first_array_paths = self.arrow_array_struct_path_tails(first_source);
            let first_map_paths = self.logical_map_path_tails(first_source);
            if first_array_paths.is_empty() && first_map_paths.is_empty() {
                continue;
            }
            let all_match = matching_sources.iter().all(|source| {
                self.arrow_array_struct_path_tails(source) == first_array_paths
                    && self.logical_map_path_tails(source) == first_map_paths
            });
            if all_match {
                self.copy_column_metadata(first_source, &output, &mut wrapped);
            }
        }
        wrapped
    }

    fn wrap(df: DataFrame) -> Self {
        PyDataFrame {
            inner: df,
            active_actions: std::sync::Arc::new(ActionRegistry::default()),
            session_actions: None,
            schema_cache: None,
            mixed_row_shape_error_on_collect: false,
            arrow_schema_cache: None,
            arrow_schema_invalidated_fields: Default::default(),
            arrow_array_struct_paths: Default::default(),
            logical_map_columns: Default::default(),
            logical_map_paths: Default::default(),
        }
    }

    fn wrap_with_session(df: DataFrame, session_actions: std::sync::Arc<ActionRegistry>) -> Self {
        let mut wrapped = Self::wrap(df);
        wrapped.session_actions = Some(session_actions);
        wrapped
    }

    fn begin_action(&self) -> ActiveAction {
        ActiveAction::new(&self.active_actions, self.session_actions.as_ref())
    }

    fn create_arrow_batches(
        &self,
        py: Python<'_>,
        max_rows: usize,
        max_bytes: usize,
    ) -> PyResult<PyObject> {
        self.check_mixed_row_shape()?;
        if max_rows == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "max_rows must be greater than zero",
            ));
        }
        if max_bytes == 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "max_bytes must be greater than zero",
            ));
        }

        // Fetch one source row at a time so max_bytes is applied before the producer
        // can materialize a large row batch in memory.
        let action = self.begin_action();
        let batches = self
            .inner
            .collect_batches_with_cancellation(1, &action.token)
            .map_err(to_py_err)?;
        let template = Py::new(py, self.wrap_derived(self.inner.clone()))?;
        Py::new(
            py,
            PyArrowBatchIterator {
                batches: Some(batches),
                template,
                max_rows,
                max_bytes,
                pending: None,
                pending_offset: 0,
                deferred_error: None,
                action: Some(action),
                closed: false,
            },
        )?
        .into_py_any(py)
    }

    fn wrap_derived(&self, df: DataFrame) -> Self {
        let mut wrapped = Self::wrap(df);
        wrapped.session_actions = self.session_actions.clone();
        if let Ok(columns) = wrapped.inner.columns() {
            let output_names: std::collections::HashSet<&str> =
                columns.iter().map(String::as_str).collect();
            wrapped.arrow_array_struct_paths = self
                .arrow_array_struct_paths
                .iter()
                .filter(|path| {
                    output_names.iter().any(|name| {
                        let root = arrow_field_path("", name);
                        path.as_str() == root.as_str()
                            || path
                                .strip_prefix(root.as_str())
                                .is_some_and(|tail| tail.starts_with(['.', '[', '{']))
                    })
                })
                .cloned()
                .collect();
            wrapped.logical_map_columns = self
                .logical_map_columns
                .iter()
                .filter(|name| output_names.contains(name.as_str()))
                .cloned()
                .collect();
            wrapped.logical_map_paths = self
                .logical_map_paths
                .iter()
                .filter(|path| {
                    output_names.iter().any(|name| {
                        let root = arrow_field_path("", name);
                        path.as_str() == root.as_str()
                            || path
                                .strip_prefix(root.as_str())
                                .is_some_and(|tail| tail.starts_with(['.', '[', '{']))
                    })
                })
                .cloned()
                .collect();
        }
        Python::attach(|py| {
            wrapped.arrow_schema_cache = self
                .arrow_schema_cache
                .as_ref()
                .map(|schema| schema.clone_ref(py));
        });
        let output_columns = wrapped.inner.columns().ok();
        wrapped.arrow_schema_invalidated_fields = self
            .arrow_schema_invalidated_fields
            .iter()
            .filter(|name| {
                output_columns
                    .as_ref()
                    .is_some_and(|columns| columns.iter().any(|column| column == *name))
            })
            .cloned()
            .collect();
        wrapped
    }

    fn copy_column_metadata(&self, source: &str, output: &str, target: &mut Self) {
        self.copy_column_metadata_to_path(
            source,
            &arrow_field_path("", output),
            Some(output),
            target,
        );
    }

    fn copy_column_metadata_to_path(
        &self,
        source: &str,
        output_path: &str,
        output_root: Option<&str>,
        target: &mut Self,
    ) {
        let resolved = self.inner.resolve_column_name(source).ok();
        let source_path = if let Some(source) = &resolved {
            arrow_field_path("", source)
        } else {
            // `selectExpr` preserves qualified identifiers as `alias.field.nested`. The
            // DataFrame schema only contains `field`, so discard this DataFrame's alias
            // before resolving the metadata path.
            let qualified_source = self
                .inner
                .get_alias()
                .and_then(|alias| {
                    source
                        .split_once('.')
                        .filter(|(qualifier, _)| {
                            if self.inner.is_case_sensitive() {
                                *qualifier == alias
                            } else {
                                qualifier.to_lowercase() == alias.to_lowercase()
                            }
                        })
                        .map(|(_, nested_source)| nested_source.to_string())
                })
                .unwrap_or_else(|| source.to_string());
            let mut parts = qualified_source.split('.');
            let root = parts.next().unwrap_or(source);
            let root = self
                .inner
                .resolve_column_name(root)
                .unwrap_or_else(|_| root.to_string());
            parts.fold(arrow_field_path("", &root), |path, field| {
                arrow_field_path(&path, field)
            })
        };
        for path in &self.arrow_array_struct_paths {
            if path == &source_path {
                target
                    .arrow_array_struct_paths
                    .insert(output_path.to_string());
            } else if let Some(tail) = path.strip_prefix(source_path.as_str()) {
                if tail.starts_with(['.', '[', '{']) {
                    target
                        .arrow_array_struct_paths
                        .insert(format!("{output_path}{tail}"));
                }
            }
        }
        for path in &self.logical_map_paths {
            if path == &source_path {
                target.logical_map_paths.insert(output_path.to_string());
            } else if let Some(tail) = path.strip_prefix(source_path.as_str()) {
                if tail.starts_with(['.', '[', '{']) {
                    target
                        .logical_map_paths
                        .insert(format!("{output_path}{tail}"));
                }
            }
        }
        if self
            .logical_map_columns
            .contains(resolved.as_deref().unwrap_or(source))
        {
            target.logical_map_paths.insert(output_path.to_string());
            if let Some(output_root) = output_root {
                target.logical_map_columns.insert(output_root.to_string());
            }
        }
    }

    fn copy_struct_map_metadata(
        &self,
        expr: &robin_sparkless::Expr,
        output: &str,
        target: &mut Self,
    ) {
        use robin_sparkless::Expr;

        fn visit(
            source: &PyDataFrame,
            expr: &Expr,
            output_dtype: &robin_sparkless::polars::PlDataType,
        ) -> Option<std::collections::HashSet<String>> {
            match expr {
                Expr::Alias(inner, _)
                | Expr::Cast { expr: inner, .. }
                | Expr::KeepName(inner)
                | Expr::RenameAlias { expr: inner, .. } => visit(source, inner, output_dtype),
                Expr::Column(name) => {
                    if source.column_dtype_for_metadata(name.as_str()).as_ref()
                        == Some(output_dtype)
                    {
                        Some(source.logical_map_path_tails(name.as_str()))
                    } else {
                        Some(std::collections::HashSet::new())
                    }
                }
                Expr::Literal(value) if value.is_null() => None,
                Expr::Ternary { truthy, falsy, .. } => Some(common_paths([
                    visit(source, truthy, output_dtype),
                    visit(source, falsy, output_dtype),
                ])),
                Expr::Function {
                    input,
                    function: polars::prelude::FunctionExpr::Coalesce,
                } => Some(common_paths(
                    input.iter().map(|input| visit(source, input, output_dtype)),
                )),
                Expr::Function {
                    input,
                    function: polars::prelude::FunctionExpr::AsStruct,
                } => {
                    let robin_sparkless::polars::PlDataType::Struct(fields) = output_dtype else {
                        return Some(std::collections::HashSet::new());
                    };
                    if input.len() != fields.len() {
                        return Some(std::collections::HashSet::new());
                    }
                    let mut paths = std::collections::HashSet::new();
                    for (input, field) in input.iter().zip(fields) {
                        if let Some(field_paths) = visit(source, input, field.dtype()) {
                            let field_segment = format!(".{}", arrow_field_path("", &field.name));
                            paths.extend(
                                field_paths
                                    .into_iter()
                                    .map(|tail| format!("{field_segment}{tail}")),
                            );
                        }
                    }
                    Some(paths)
                }
                _ => Some(std::collections::HashSet::new()),
            }
        }

        fn common_paths(
            branches: impl IntoIterator<Item = Option<std::collections::HashSet<String>>>,
        ) -> std::collections::HashSet<String> {
            let mut common: Option<std::collections::HashSet<String>> = None;
            for branch in branches.into_iter().flatten() {
                if let Some(common) = &mut common {
                    common.retain(|path| branch.contains(path));
                } else {
                    common = Some(branch);
                }
            }
            common.unwrap_or_default()
        }

        let Some(output_dtype) = target.inner.get_column_dtype(output) else {
            return;
        };
        let output_path = arrow_field_path("", output);
        let Some(paths) = visit(self, expr, &output_dtype) else {
            return;
        };
        for tail in paths {
            target
                .logical_map_paths
                .insert(format!("{output_path}{tail}"));
            if tail.is_empty() {
                target.logical_map_columns.insert(output.to_string());
            }
        }
    }

    fn wrap_with_column_derived(
        &self,
        df: DataFrame,
        name: &str,
        expr: &robin_sparkless::Expr,
    ) -> Self {
        let mut wrapped = self.wrap_derived(df);
        Self::remove_metadata_root(&mut wrapped, name);
        wrapped
            .arrow_schema_invalidated_fields
            .insert(name.to_string());
        if let Some(source) = Self::column_source(expr) {
            self.copy_column_metadata(&source, name, &mut wrapped);
        } else {
            if self.expr_uses_arrow_array_struct(expr) {
                Self::mark_array_struct_path(&mut wrapped, name);
            }
            let references = Self::expression_column_references(expr);
            self.copy_computed_map_metadata(
                Self::expression_preserves_map_value(expr),
                &references,
                name,
                &mut wrapped,
            );
            self.copy_struct_map_metadata(expr, name, &mut wrapped);
        }
        wrapped
    }

    fn wrap_with_columns_derived(&self, df: DataFrame, expressions: &[(String, Column)]) -> Self {
        let mut wrapped = self.wrap_derived(df);
        for (name, column) in expressions {
            Self::remove_metadata_root(&mut wrapped, name);
            wrapped
                .arrow_schema_invalidated_fields
                .insert(name.to_string());
            if let Some(source) = Self::column_source(column.expr()) {
                self.copy_column_metadata(&source, name, &mut wrapped);
            } else {
                if self.expr_uses_arrow_array_struct(column.expr()) {
                    Self::mark_array_struct_path(&mut wrapped, name);
                }
                let references = Self::expression_column_references(column.expr());
                self.copy_computed_map_metadata(
                    Self::expression_preserves_map_value(column.expr()),
                    &references,
                    name,
                    &mut wrapped,
                );
                self.copy_struct_map_metadata(column.expr(), name, &mut wrapped);
            }
        }
        wrapped
    }

    fn wrap_renamed_columns_derived(&self, df: DataFrame, renames: &[(String, String)]) -> Self {
        let mut wrapped = self.wrap_derived(df);
        let applied = renames
            .iter()
            .filter_map(|(old, new)| {
                self.inner
                    .resolve_column_name(old)
                    .ok()
                    .map(|old| (old, new))
            })
            .collect::<Vec<_>>();
        for (old, new) in &applied {
            Self::remove_metadata_root(&mut wrapped, old);
            Self::remove_metadata_root(&mut wrapped, new);
        }
        for (old, new) in applied {
            self.copy_column_metadata(&old, new, &mut wrapped);
        }
        wrapped
    }

    fn wrap_union_derived(
        &self,
        other: Option<&PyDataFrame>,
        df: DataFrame,
        by_name: bool,
    ) -> Self {
        let mut wrapped = self.wrap_derived(df);
        let Some(other) = other else {
            return wrapped;
        };
        let left_names = self.inner.columns().unwrap_or_default();
        let right_names = other.inner.columns().unwrap_or_default();
        let align_by_name = self.union_aligns_by_name(&left_names, &right_names, by_name);
        for (index, output) in wrapped
            .inner
            .columns()
            .unwrap_or_default()
            .iter()
            .enumerate()
        {
            let source = if align_by_name {
                Self::resolve_union_column_name(other, &right_names, output)
            } else {
                right_names.get(index).cloned()
            };
            if let Some(source) = source {
                other.copy_column_metadata(&source, output, &mut wrapped);
            }
        }
        wrapped
    }

    fn validate_union_metadata(&self, other: &PyDataFrame, by_name: bool) -> PyResult<()> {
        let left_names = self.inner.columns().unwrap_or_default();
        let right_names = other.inner.columns().unwrap_or_default();
        let align_by_name = self.union_aligns_by_name(&left_names, &right_names, by_name);

        for (index, left_name) in left_names.iter().enumerate() {
            let right_name = if align_by_name {
                Self::resolve_union_column_name(other, &right_names, left_name)
            } else {
                right_names.get(index).cloned()
            };
            let Some(right_name) = right_name else {
                continue;
            };
            if self.inner.get_column_dtype(left_name) != other.inner.get_column_dtype(&right_name) {
                continue;
            }

            let left_maps = self.logical_map_path_tails(left_name);
            let right_maps = other.logical_map_path_tails(&right_name);
            let left_arrays = self.arrow_array_struct_path_tails(left_name);
            let right_arrays = other.arrow_array_struct_path_tails(&right_name);
            let has_conflict = left_maps.iter().any(|path| right_arrays.contains(path))
                || right_maps.iter().any(|path| left_arrays.contains(path));
            if has_conflict {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "union: column '{}' has incompatible logical types (map and array<struct<key, value>>) despite matching physical types",
                    left_name
                )));
            }
        }
        Ok(())
    }

    fn wrap_join_derived(&self, other: &PyDataFrame, df: DataFrame) -> Self {
        let mut wrapped = self.wrap_derived(df);
        let output_names = wrapped
            .inner
            .columns()
            .unwrap_or_default()
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let left_names = self
            .inner
            .columns()
            .unwrap_or_default()
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let right_names = other.inner.columns().unwrap_or_default();
        for source in &right_names {
            let has_metadata = other.arrow_array_struct_paths.iter().any(|path| {
                let root = arrow_field_path("", source);
                path.as_str() == root.as_str()
                    || path
                        .strip_prefix(root.as_str())
                        .is_some_and(|tail| tail.starts_with(['.', '[', '{']))
            }) || other.logical_map_paths.iter().any(|path| {
                let root = arrow_field_path("", source);
                path.as_str() == root.as_str()
                    || path
                        .strip_prefix(root.as_str())
                        .is_some_and(|tail| tail.starts_with(['.', '[', '{']))
            });
            if !has_metadata {
                continue;
            }
            let renamed = format!("{source}_right");
            let output = if left_names.contains(source) && output_names.contains(&renamed) {
                Some(renamed)
            } else if output_names.contains(source) {
                Some(source.clone())
            } else {
                None
            };
            if let Some(output) = output {
                other.copy_column_metadata(source, &output, &mut wrapped);
            }
        }
        for name in &other.logical_map_columns {
            let right_name = format!("{name}_right");
            let output_name = if left_names.contains(name) && output_names.contains(&right_name) {
                Some(right_name)
            } else if output_names.contains(name) {
                Some(name.clone())
            } else {
                None
            };
            if let Some(output_name) = output_name {
                wrapped.logical_map_columns.insert(output_name);
            }
        }
        wrapped
    }

    fn wrap_select_derived(&self, df: DataFrame, items: &[SelectItem<'_>]) -> Self {
        fn source_and_output_names(expr: &robin_sparkless::Expr) -> Option<(String, String)> {
            let mut current = expr;
            let mut output = None;
            loop {
                match current {
                    robin_sparkless::Expr::Alias(inner, name) => {
                        output.get_or_insert_with(|| name.to_string());
                        current = inner.as_ref();
                    }
                    robin_sparkless::Expr::Cast { expr, .. } => current = expr.as_ref(),
                    robin_sparkless::Expr::Column(name) => {
                        return Some((
                            name.to_string(),
                            output.unwrap_or_else(|| name.to_string()),
                        ));
                    }
                    _ => return None,
                }
            }
        }

        let mut wrapped = self.wrap_derived(df);
        let output_names = wrapped.inner.columns().unwrap_or_default();
        for output in &output_names {
            Self::remove_metadata_root(&mut wrapped, output);
        }
        for (item_idx, item) in items.iter().enumerate() {
            let (source, output) = match item {
                SelectItem::ColumnName(source) => (
                    source.to_string(),
                    source.split('.').next_back().unwrap_or(source).to_string(),
                ),
                SelectItem::Expr(expr) => {
                    let Some(names) = source_and_output_names(expr) else {
                        if self.expr_uses_arrow_array_struct(expr) {
                            if let Some(output) = output_names.get(item_idx) {
                                Self::mark_array_struct_path(&mut wrapped, output);
                            }
                        }
                        if let Some(output) = output_names.get(item_idx) {
                            let references = Self::expression_column_references(expr);
                            self.copy_computed_map_metadata(
                                Self::expression_preserves_map_value(expr),
                                &references,
                                output,
                                &mut wrapped,
                            );
                            self.copy_struct_map_metadata(expr, output, &mut wrapped);
                        }
                        continue;
                    };
                    names
                }
            };
            self.copy_column_metadata(&source, &output, &mut wrapped);
        }
        wrapped
    }

    fn wrap_select_expr_derived(&self, df: DataFrame, expressions: &[String]) -> Self {
        let mut wrapped = self.wrap_derived(df);
        for expression in expressions {
            let source_projection = robin_sparkless::sql::select_expr_source(expression)
                .ok()
                .flatten();
            let Ok((alias, references)) = robin_sparkless::sql::select_expr_metadata(expression)
            else {
                continue;
            };
            let Some((source, projected_output)) = source_projection else {
                // The SQL expression translator aliases unaliased projections with the original
                // expression string, so use the same output name for metadata propagation.
                let output = alias.unwrap_or_else(|| expression.to_string());
                Self::remove_metadata_root(&mut wrapped, &output);
                if references
                    .iter()
                    .any(|name| self.column_uses_arrow_array_struct(name))
                {
                    Self::mark_array_struct_path(&mut wrapped, &output);
                }
                let preserves_map_value =
                    robin_sparkless::sql::select_expr_preserves_map_value(expression)
                        .unwrap_or(false);
                self.copy_computed_map_metadata(
                    preserves_map_value,
                    &references,
                    &output,
                    &mut wrapped,
                );
                continue;
            };
            let output = alias.unwrap_or(projected_output);
            Self::remove_metadata_root(&mut wrapped, &output);
            self.copy_column_metadata(&source, &output, &mut wrapped);
            if !robin_sparkless::sql::select_expr_preserves_map_value(expression).unwrap_or(false) {
                Self::remove_logical_map_metadata_root(&mut wrapped, &output);
            }
        }
        wrapped
    }

    fn wrap_renamed_derived(&self, df: DataFrame, old_name: &str, new_name: &str) -> Self {
        self.wrap_renamed_columns_derived(df, &[(old_name.to_string(), new_name.to_string())])
    }

    fn check_mixed_row_shape(&self) -> PyResult<()> {
        if self.mixed_row_shape_error_on_collect {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "createDataFrame: all rows must be the same shape (dict rows or list/tuple rows)",
            ));
        }
        Ok(())
    }
}

/// Extracts column names from a join on parameter, accepting a string, Column, or list.
#[allow(dead_code)]
fn extract_string_list_from_on(on: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
    let on = match on {
        Some(v) => v,
        None => return Ok(Vec::new()),
    };
    if let Ok(s) = on.extract::<String>() {
        return Ok(vec![s]);
    }
    if let Ok(py_col) = on.cast::<PyColumn>() {
        return Ok(vec![py_col.borrow().inner.name().to_string()]);
    }
    if let Ok(list) = on.cast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(s) = item.extract::<String>() {
                out.push(s);
            } else if let Ok(c) = item.cast::<PyColumn>() {
                out.push(c.borrow().inner.name().to_string());
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "join 'on' list elements must be str or Column",
                ));
            }
        }
        return Ok(out);
    }
    if let Ok(tup) = on.cast::<PyTuple>() {
        let mut out = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            if let Ok(s) = item.extract::<String>() {
                out.push(s);
            } else if let Ok(c) = item.cast::<PyColumn>() {
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

/// Convert a JSON string (from PyUdfFilterColumn) to a literal Expr for filter (e.g. col(temp).gt(lit)).
fn json_str_to_lit_expr(s: &str) -> PyResult<robin_sparkless::Expr> {
    use robin_sparkless::functions::{lit_bool, lit_f64, lit_i64, lit_null_untyped, lit_str};
    let v: JsonValue = serde_json::from_str(s).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("filter literal JSON: {e}"))
    })?;
    Ok(match v {
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                lit_i64(i).into_expr()
            } else {
                lit_f64(n.as_f64().unwrap_or(0.0)).into_expr()
            }
        }
        JsonValue::String(st) => lit_str(&st).into_expr(),
        JsonValue::Bool(b) => lit_bool(b).into_expr(),
        JsonValue::Null => lit_null_untyped().into_expr(),
        _ => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "filter literal must be number, string, bool, or null",
            ));
        }
    })
}

/// Build filter expression for UDF filter temp column vs literal using the comparison op.
fn udf_filter_comparison_expr(
    temp_name: String,
    op: &str,
    lit_expr: robin_sparkless::Expr,
) -> PyResult<robin_sparkless::Expr> {
    let col = robin_sparkless::Column::new(temp_name);
    let filter_col = match op {
        "gt" => col.gt(lit_expr),
        "ge" => col.gt_eq(lit_expr),
        "lt" => col.lt(lit_expr),
        "le" => col.lt_eq(lit_expr),
        "eq" => col.eq(lit_expr),
        "ne" => col.neq(lit_expr),
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "unsupported UDF filter op: {other}"
            )));
        }
    };
    Ok(filter_col.into_expr())
}

/// When `inner` is a UDF column compared to a literal, return PyUdfFilterColumn for filter() expansion.
fn py_udf_filter_column_from_comparison(
    py: Python<'_>,
    inner: &Column,
    other: &Bound<'_, PyAny>,
    op: &str,
) -> PyResult<Option<PyObject>> {
    if inner.udf_call_info().is_some() {
        let other_json = py_any_to_json_string(other)?;
        let udf_column = Py::new(
            py,
            PyColumn {
                inner: inner.clone(),
            },
        )?;
        return Ok(Some(
            PyUdfFilterColumn {
                udf_column,
                op: op.to_string(),
                other_json,
            }
            .into_py_any(py)?,
        ));
    }
    Ok(None)
}

/// Serialize a simple Python value to JSON for UDF filter (e.g. my_udf("x","y") > 20).
fn py_any_to_json_string(other: &Bound<'_, PyAny>) -> PyResult<String> {
    if other.is_none() {
        return Ok("null".to_string());
    }
    let json_err = |e: serde_json::Error| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("JSON encode failed: {e}"))
    };
    if let Ok(b) = other.extract::<bool>() {
        return serde_json::to_string(&b).map_err(json_err);
    }
    if let Ok(i) = other.extract::<i64>() {
        return serde_json::to_string(&i).map_err(json_err);
    }
    if let Ok(i) = other.extract::<i32>() {
        return serde_json::to_string(&i).map_err(json_err);
    }
    if let Ok(f) = other.extract::<f64>() {
        if !f.is_finite() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "UDF filter comparison rhs float must be finite",
            ));
        }
        return serde_json::to_string(&f).map_err(json_err);
    }
    if let Ok(s) = other.extract::<String>() {
        return serde_json::to_string(&s).map_err(json_err);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "UDF filter comparison rhs must be int, float, str, bool, or None",
    ))
}

/// Extract PyColumn from a Column or Python UDF wrapper (_sparkless_udf_inner).
fn py_any_to_py_column_ref(col_any: &Bound<'_, PyAny>) -> Option<Py<PyColumn>> {
    if let Ok(py_col) = col_any.cast::<PyColumn>() {
        return Some(py_col.clone().unbind());
    }
    if let Ok(inner) = col_any.getattr("_sparkless_udf_inner") {
        if let Ok(py_col) = inner.cast::<PyColumn>() {
            return Some(py_col.clone().unbind());
        }
    }
    None
}

/// Build PyUdfFilterColumn for (udf_column op literal). Used by Python UDF wrapper comparisons.
#[pyfunction]
#[pyo3(name = "make_udf_filter_column")]
fn make_udf_filter_column(
    py: Python<'_>,
    udf_column: &Bound<'_, PyAny>,
    op: &str,
    other: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let py_col = py_any_to_py_column_ref(udf_column).ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "make_udf_filter_column expects a UDF Column",
        )
    })?;
    let inner = &py_col.bind(py).borrow().inner;
    if inner.udf_call_info().is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "make_udf_filter_column expects a UDF Column",
        ));
    }
    py_udf_filter_column_from_comparison(py, inner, other, op)?.ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "make_udf_filter_column could not build comparison",
        )
    })
}

/// Convert Python value (int, float, str, bool, None, list) or PyColumn to robin_sparkless Column.
fn py_decimal_to_column(value: &Bound<'_, PyAny>) -> PyResult<Option<Column>> {
    let py = value.py();
    let decimal_type = PyModule::import(py, "decimal")?.getattr("Decimal")?;
    if !value.is_instance(&decimal_type)? {
        return Ok(None);
    }

    let tuple = value.call_method0("as_tuple")?;
    let sign: u8 = tuple.get_item(0)?.extract()?;
    let digits: Vec<u8> = tuple.get_item(1)?.extract()?;
    let exponent = tuple.get_item(2)?.extract::<i32>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Decimal literals must be finite Spark decimal values",
        )
    })?;
    let scale = exponent.unsigned_abs() as usize * usize::from(exponent < 0);
    let positive_exponent = exponent.max(0) as usize;
    let precision = digits
        .len()
        .saturating_add(positive_exponent)
        .max(scale)
        .max(1);
    if precision > 38 || scale > 38 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Decimal literal exceeds Spark DecimalType maximum precision 38",
        ));
    }

    let mut coefficient = if digits.is_empty() {
        "0".to_string()
    } else {
        digits
            .iter()
            .map(|digit| char::from(b'0' + *digit))
            .collect()
    };
    coefficient.extend(std::iter::repeat_n('0', positive_exponent));
    let mut coefficient = coefficient.parse::<i128>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Decimal literal exceeds Spark DecimalType maximum precision 38",
        )
    })?;
    if sign != 0 {
        coefficient = -coefficient;
    }

    let expression = polars::prelude::lit(polars::prelude::Scalar::new_decimal(
        coefficient,
        precision,
        scale,
    ));
    Ok(Some(Column::from_expr(expression, None)))
}

fn py_any_to_column(other: &Bound<'_, PyAny>) -> PyResult<Column> {
    if let Some(py_col) = py_any_to_py_column_ref(other) {
        return Ok(py_col.bind(other.py()).borrow().inner.clone());
    }
    if let Some(decimal) = py_decimal_to_column(other)? {
        return Ok(decimal);
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
    if let Ok(list) = other.cast::<PyList>() {
        if list.is_empty() {
            return Ok(robin_sparkless::functions::lit_str("[]"));
        }
        let first = list.get_item(0)?;
        if first.extract::<i64>().is_ok() {
            let vals: Vec<i64> = list
                .iter()
                .map(|x| x.extract::<i64>())
                .collect::<PyResult<_>>()
                .map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                        "list comparison values must all be integers",
                    )
                })?;
            return Ok(robin_sparkless::functions::lit_str(&format!("{:?}", vals)));
        }
        if list.iter().any(|x| x.extract::<i64>().is_ok()) {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "list comparison values must have a consistent type",
            ));
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
    if let Ok(df) = other.cast::<PyDataFrame>() {
        return Ok(df.borrow().inner.clone());
    }
    if let Ok(inner) = other.getattr("inner") {
        if let Ok(df) = inner.cast::<PyDataFrame>() {
            return Ok(df.borrow().inner.clone());
        }
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "argument 'other': expected DataFrame or object with .inner DataFrame",
    ))
}

/// Wrapper used to expand Python UDF comparisons into a temporary column and filter.
#[pyclass]
struct PyUdfFilterColumn {
    udf_column: Py<PyColumn>,
    op: String,
    other_json: String,
}

#[pymethods]
impl PyUdfFilterColumn {
    #[getter]
    fn udf_column(&self, py: Python<'_>) -> Py<PyColumn> {
        self.udf_column.clone_ref(py)
    }
    #[getter]
    fn op(&self) -> &str {
        &self.op
    }
    #[getter]
    fn other_json(&self) -> &str {
        &self.other_json
    }
}

/// Internal marker for `df["*"]`: expands to all columns from that DataFrame in `select()` (PySpark parity #1587).
#[pyclass]
struct PyStarColumns {
    columns: Vec<String>,
}

#[pyclass]
struct PyColumn {
    pub(crate) inner: Column,
}

/// Wrapper for SQL expressions supplied through F.expr.
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
            .cast::<PyDataFrame>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame"))?
            .borrow();
        // PySpark: na.fill(scalar, subset=...) or na.fill({col: value, ...}, subset=ignored).
        // Dict value: iterate keys and apply column-specific fills; subset is ignored.
        if let Ok(dict) = value.cast::<PyDict>() {
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
            return Ok(df_ref.wrap_derived(current));
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
            .map(|df| df_ref.wrap_derived(df))
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
            .cast::<PyDataFrame>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame"))?
            .borrow();
        let sub_vec = normalize_subset(subset)?;
        let subset_refs: Option<Vec<&str>> = sub_vec
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let na = df_ref.inner.na();
        na.drop(subset_refs, how, thresh)
            .map(|df| df_ref.wrap_derived(df))
            .map_err(to_py_err)
    }

    /// Uses *values and **kwargs so we accept value as positional or keyword (Python na passes value=...).
    #[pyo3(signature = (to_replace, *values, subset=None, **kwargs))]
    fn replace(
        &self,
        py: Python<'_>,
        to_replace: &Bound<'_, PyAny>,
        values: &Bound<'_, PyTuple>,
        subset: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyDataFrame> {
        let df_ref = self
            .df
            .bind(py)
            .cast::<PyDataFrame>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame"))?
            .borrow();
        let sub_vec = normalize_subset(subset)?;
        let subset_refs: Option<Vec<&str>> = sub_vec
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());

        let value_opt: Option<Bound<'_, PyAny>> = if values.len() >= 1 {
            Some(values.get_item(0)?)
        } else if let Some(kw) = kwargs {
            kw.get_item("value")?
        } else {
            None
        };
        let value_provided = value_opt.is_some();

        if !value_provided {
            if let Ok(dict) = to_replace.cast::<PyDict>() {
                let mut current = df_ref.inner.clone();
                for (k, v) in dict.iter() {
                    let old_expr = py_any_to_column(&k)?.into_expr();
                    let new_expr = py_any_to_column(&v)?.into_expr();
                    current = current
                        .na()
                        .replace(old_expr, new_expr, subset_refs.as_ref().map(|v| v.to_vec()))
                        .map_err(to_py_err)?;
                }
                return Ok(df_ref.wrap_derived(current));
            }
            return Err(to_py_err("value is required when to_replace is not a dict"));
        }

        let value = value_opt.as_ref().unwrap();

        // PySpark: na.replace does not accept None as to_replace; use fillna for nulls (#1210).
        if to_replace.is_none() {
            return Err(to_py_err(
                "na.replace does not accept None as to_replace; use fillna for nulls",
            ));
        }

        // PySpark: replace(..., None) replaces with null.
        if value.is_none() {
            let null_expr = robin_sparkless::functions::lit_null_untyped().into_expr();
            if let Ok(old_list) = to_replace.cast::<pyo3::types::PyList>() {
                let mut current = df_ref.inner.clone();
                for i in 0..old_list.len() {
                    let old_item = old_list.get_item(i)?;
                    let old_expr = lit(&old_item)?.inner.into_expr();
                    current = current
                        .na()
                        .replace(
                            old_expr,
                            null_expr.clone(),
                            subset_refs.as_ref().map(|v| v.to_vec()),
                        )
                        .map_err(to_py_err)?;
                }
                return Ok(df_ref.wrap_derived(current));
            }
            let old_expr = lit(to_replace)?.inner.into_expr();
            return df_ref
                .inner
                .na()
                .replace(old_expr, null_expr, subset_refs)
                .map(|df| df_ref.wrap_derived(df))
                .map_err(to_py_err);
        }

        // to_replace is list and value is list: replace each pair in order
        if let (Ok(old_list), Ok(new_list)) = (
            to_replace.cast::<pyo3::types::PyList>(),
            value.cast::<pyo3::types::PyList>(),
        ) {
            if old_list.len() != new_list.len() {
                return Err(to_py_err(format!(
                    "to_replace and value lists must be the same length ({} vs {})",
                    old_list.len(),
                    new_list.len()
                )));
            }
            let mut current = df_ref.inner.clone();
            let null_expr = robin_sparkless::functions::lit_null_untyped().into_expr();
            for i in 0..old_list.len() {
                let old_item = old_list.get_item(i)?;
                let new_item = new_list.get_item(i)?;
                let old_expr = lit(&old_item)?.inner.into_expr();
                let new_expr = if new_item.is_none() {
                    null_expr.clone()
                } else {
                    lit(&new_item)?.inner.into_expr()
                };
                current = current
                    .na()
                    .replace(old_expr, new_expr, subset_refs.as_ref().map(|v| v.to_vec()))
                    .map_err(to_py_err)?;
            }
            return Ok(df_ref.wrap_derived(current));
        }

        // to_replace is list and value is scalar: replace each old with value
        if let Ok(old_list) = to_replace.cast::<pyo3::types::PyList>() {
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
            return Ok(df_ref.wrap_derived(current));
        }

        // Single scalar/scalar replace
        let old_expr = lit(to_replace)?.inner.into_expr();
        let new_expr = lit(value)?.inner.into_expr();
        df_ref
            .inner
            .na()
            .replace(old_expr, new_expr, subset_refs)
            .map(|df| df_ref.wrap_derived(df))
            .map_err(to_py_err)
    }
}

#[pyclass]
struct PyCubeRollupData {
    inner: CubeRollupData,
    source: PyDataFrame,
    grouping_column_count: usize,
}

#[pymethods]
impl PyCubeRollupData {
    fn count(&self) -> PyResult<PyDataFrame> {
        self.inner
            .count()
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn sum(&self, col_name: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let name = py_col_to_name(col_name)?;
        self.inner
            .sum(&name)
            .map(|df| self.source.wrap_derived(df))
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
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    /// PySpark cube/rollup().mean(*cols).
    #[pyo3(signature = (*cols))]
    fn mean(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        self.avg(cols)
    }

    fn min(&self, col_name: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let name = py_col_to_name(col_name)?;
        self.inner
            .min(&name)
            .map(|df| {
                self.source
                    .wrap_value_aggregate_derived(df, &name, self.grouping_column_count)
            })
            .map_err(to_py_err)
    }

    fn max(&self, col_name: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let name = py_col_to_name(col_name)?;
        self.inner
            .max(&name)
            .map(|df| {
                self.source
                    .wrap_value_aggregate_derived(df, &name, self.grouping_column_count)
            })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*exprs))]
    fn agg(&self, py: Python<'_>, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let mut needs_expr_ctx = false;
        for item in exprs.iter() {
            if py_agg_item_contains_expr_str(&item)? {
                needs_expr_ctx = true;
                break;
            }
        }

        let mut rust_exprs: Vec<robin_sparkless::Expr> = Vec::new();
        if needs_expr_ctx {
            let session = THREAD_ACTIVE_SESSIONS
                .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "agg() with expr() requires an active SparkSession",
                    )
                })?;
            let session_ref = session.bind(py).cast::<PySparkSession>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
            })?;
            let input_df = self.inner.input_dataframe();
            session_ref
                .borrow()
                .inner
                .create_or_replace_temp_view("__agg_expr_t", input_df.clone());
            let expr_ctx = (session_ref.clone(), input_df);
            for item in exprs.iter() {
                py_agg_push_expr(
                    &item,
                    &mut rust_exprs,
                    false,
                    Some((&expr_ctx.0, &expr_ctx.1)),
                )?;
            }
            let _ = session_ref.borrow().inner.drop_temp_view("__agg_expr_t");
        } else {
            for item in exprs.iter() {
                py_agg_push_expr(&item, &mut rust_exprs, false, None)?;
            }
        }
        self.inner
            .agg(rust_exprs.clone())
            .map(|df| {
                self.source
                    .wrap_aggregate_derived(df, &rust_exprs, self.grouping_column_count)
            })
            .map_err(to_py_err)
    }
}

#[pyclass]
struct PyGroupedData {
    inner: GroupedData,
    source: PyDataFrame,
    grouping_column_count: usize,
}

#[pymethods]
impl PyGroupedData {
    fn count(&self) -> PyResult<PyDataFrame> {
        self.inner
            .count()
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn sum(&self, col_name: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let name = py_col_to_name(col_name)?;
        self.inner
            .sum(&name)
            .map(|df| self.source.wrap_derived(df))
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
            .map(|df| self.source.wrap_derived(df))
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
            .map(|df| {
                self.source
                    .wrap_value_aggregate_derived(df, &name, self.grouping_column_count)
            })
            .map_err(to_py_err)
    }

    fn max(&self, col_name: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        let name = py_col_to_name(col_name)?;
        self.inner
            .max(&name)
            .map(|df| {
                self.source
                    .wrap_value_aggregate_derived(df, &name, self.grouping_column_count)
            })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*exprs))]
    fn agg(&self, py: Python<'_>, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let mut needs_expr_ctx = false;
        for item in exprs.iter() {
            if py_agg_item_contains_expr_str(&item)? {
                needs_expr_ctx = true;
                break;
            }
        }

        let mut rust_exprs: Vec<robin_sparkless::Expr> = Vec::new();
        if needs_expr_ctx {
            let session = THREAD_ACTIVE_SESSIONS
                .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "agg() with expr() requires an active SparkSession",
                    )
                })?;
            let session_ref = session.bind(py).cast::<PySparkSession>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
            })?;
            let input_df = self.inner.input_dataframe();
            session_ref
                .borrow()
                .inner
                .create_or_replace_temp_view("__agg_expr_t", input_df.clone());
            let expr_ctx = (session_ref.clone(), input_df);
            for item in exprs.iter() {
                py_agg_push_expr(
                    &item,
                    &mut rust_exprs,
                    true,
                    Some((&expr_ctx.0, &expr_ctx.1)),
                )?;
            }
            let _ = session_ref.borrow().inner.drop_temp_view("__agg_expr_t");
        } else {
            for item in exprs.iter() {
                py_agg_push_expr(&item, &mut rust_exprs, true, None)?;
            }
        }
        self.inner
            .agg(rust_exprs.clone())
            .map(|df| {
                self.source
                    .wrap_aggregate_derived(df, &rust_exprs, self.grouping_column_count)
            })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (pivot_col, values=None))]
    fn pivot(
        &self,
        pivot_col: &str,
        values: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyPivotedGroupedData> {
        let rust_values = if let Some(seq) = values {
            let mut v = Vec::new();
            for item in seq.try_iter()? {
                let item = item?;
                if item.is_none() {
                    v.push("null".to_string());
                } else {
                    v.push(item.extract::<String>()?);
                }
            }
            Some(v)
        } else {
            None
        };
        Ok(PyPivotedGroupedData {
            inner: self.inner.pivot(pivot_col, rust_values),
            source: self.source.wrap_derived(self.source.inner.clone()),
            grouping_column_count: self.grouping_column_count,
        })
    }
}

#[pyclass]
struct PyPivotedGroupedData {
    inner: PivotedGroupedData,
    source: PyDataFrame,
    grouping_column_count: usize,
}

#[pymethods]
impl PyPivotedGroupedData {
    fn sum(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .sum(value_col)
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn avg(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .avg(value_col)
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn min(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .min(value_col)
            .map(|df| {
                self.source
                    .wrap_value_aggregate_derived(df, value_col, self.grouping_column_count)
            })
            .map_err(to_py_err)
    }

    fn max(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .max(value_col)
            .map(|df| {
                self.source
                    .wrap_value_aggregate_derived(df, value_col, self.grouping_column_count)
            })
            .map_err(to_py_err)
    }

    fn count(&self) -> PyResult<PyDataFrame> {
        self.inner
            .count()
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn _count_distinct(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._count_distinct(value_col)
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn _collect_list(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._collect_list(value_col)
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn _collect_set(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._collect_set(value_col)
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn _first(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._first(value_col)
            .map(|df| {
                self.source
                    .wrap_value_aggregate_derived(df, value_col, self.grouping_column_count)
            })
            .map_err(to_py_err)
    }

    fn _last(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._last(value_col)
            .map(|df| {
                self.source
                    .wrap_value_aggregate_derived(df, value_col, self.grouping_column_count)
            })
            .map_err(to_py_err)
    }

    fn _stddev(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._stddev(value_col)
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn _variance(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            ._variance(value_col)
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    fn mean(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .mean(value_col)
            .map(|df| self.source.wrap_derived(df))
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        fn push_expr(
            item: &Bound<'_, PyAny>,
            out: &mut Vec<robin_sparkless::Expr>,
        ) -> PyResult<()> {
            if let Ok(c) = item.cast::<PyColumn>() {
                out.push(c.borrow().inner.clone().into_expr());
                return Ok(());
            }
            // last_value("col") returns _LastValueExpr (window helper); pivot.agg() supports it via _pivot_last_column. F.last("col") returns Column (issue #1348).
            if let Ok(name) = item.getattr("_pivot_last_column") {
                if let Ok(col_name) = name.extract::<String>() {
                    let col = robin_sparkless::functions::col(&col_name);
                    let last_col = robin_sparkless::functions::last(&col, false);
                    out.push(last_col.into_expr());
                    return Ok(());
                }
            }
            if let Ok(list) = item.cast::<PyList>() {
                for sub in list.iter() {
                    push_expr(&sub, out)?;
                }
                return Ok(());
            }
            if let Ok(tup) = item.cast::<PyTuple>() {
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
            .agg(rust_exprs.clone())
            .map(|df| {
                self.source
                    .wrap_aggregate_derived(df, &rust_exprs, self.grouping_column_count)
            })
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
fn column(py: Python<'_>, name: &str) -> PyResult<PyColumn> {
    // PySpark parity #1250: col() raises AssertionError when no active SparkSession.
    if resolve_active_session(py).is_none() {
        return Err(pyo3::exceptions::PyAssertionError::new_err(
            "col() requires an active SparkSession",
        ));
    }
    Ok(PyColumn {
        inner: robin_sparkless::functions::col(name),
    })
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
        let py_col = item.cast::<PyColumn>()?;
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
        *cell.borrow_mut() = Some(into_py_object(py, callback.clone())?);
        Ok(())
    })
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
    if let Some(decimal) = py_decimal_to_column(value)? {
        return Ok(PyColumn { inner: decimal });
    }
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
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "lit supports scalar values and datetime/date objects; list and map literals are not supported",
    ))
}

fn coerce_to_column(v: &Bound<'_, PyAny>) -> PyResult<Column> {
    if let Ok(c) = v.cast::<PyColumn>() {
        return Ok(c.borrow().inner.clone());
    }
    if let Ok(s) = v.extract::<String>() {
        return Ok(robin_sparkless::functions::col(&s));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "expected Column or column name string",
    ))
}

/// Accept int literal or Column for substr/substring position and length (#1649).
fn py_any_to_i64_or_column(v: &Bound<'_, PyAny>) -> PyResult<Column> {
    if let Ok(c) = v.cast::<PyColumn>() {
        return Ok(c.borrow().inner.clone());
    }
    if let Ok(i) = v.extract::<i64>() {
        return Ok(robin_sparkless::functions::lit_i64(i));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "expected int or Column",
    ))
}

/// Create a PySpark-like UNRESOLVED_COLUMN error for regexp_extract_all when the pattern
/// is passed as a plain string instead of a literal Column (e.g. F.lit(r"\\d+")).
/// This mirrors PySpark's behavior where such patterns are interpreted as unresolved
/// column/function parameter names in select(...) projections (#1501).
fn unresolved_column_error_for_pattern(pattern: &str) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyException, _>(format!(
        "AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `{}` cannot be resolved.",
        pattern
    ))
}

/// Extract pattern string from PyAny: a Column that is a string literal (e.g. F.lit(r"\\d+")). #1264, #1501
/// Passing a raw Python string for the pattern now mirrors PySpark's UNRESOLVED_COLUMN behavior
/// for regexp_extract_all in select(...) by raising an AnalysisException-style error.
fn pattern_str_from_pyany(pattern: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = pattern.extract::<String>() {
        return Err(unresolved_column_error_for_pattern(&s));
    }
    if let Ok(py_col) = pattern.cast::<PyColumn>() {
        let col = py_col.borrow().inner.clone();
        if let Some(json_str) = col.literal_as_json_string() {
            if let Ok(JsonValue::String(s)) = serde_json::from_str::<JsonValue>(&json_str) {
                return Ok(s);
            }
        }
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "regexp_extract_all: pattern must be a string or a literal Column (e.g. F.lit(\"pattern\"))",
        ));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "regexp_extract_all: pattern must be str or Column",
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
#[pyo3(signature = (column, start, length))]
fn substring(
    column: &Bound<'_, PyAny>,
    start: &Bound<'_, PyAny>,
    length: &Bound<'_, PyAny>,
) -> PyResult<PyColumn> {
    let c = coerce_to_column(column)?;
    let start_col = py_any_to_i64_or_column(start)?;
    let length_col = py_any_to_i64_or_column(length)?;
    Ok(PyColumn {
        inner: robin_sparkless::functions::substring_with(&c, &start_col, Some(&length_col)),
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
    /// PySpark: chained when expressions support .alias() before or without .otherwise().
    fn alias(&mut self, name: &str) -> PyResult<PyColumn> {
        let col = py_then_builder_to_column(self)?;
        Ok(PyColumn {
            inner: col.alias(name),
        })
    }

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
                .into_py_any(py)?)
            }
            None => Ok(Py::new(py, PyChainedWhenBuilder { inner: Some(cwb) })?.into_py_any(py)?),
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

/// PySpark allows chained `when().when()` without an explicit `.otherwise()`; unmatched rows are null.
fn py_then_builder_to_column(tb: &mut PyThenBuilder) -> PyResult<Column> {
    let builder = tb.inner.take().ok_or_else(|| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("when expression already finalized")
    })?;
    Ok(builder.otherwise(&robin_sparkless::functions::lit_null_untyped()))
}

/// Resolve a withColumn/select expression: Column, finalized chained-when, or incomplete when builders.
fn py_any_to_dataframe_column(col_any: &Bound<'_, PyAny>) -> PyResult<Option<Column>> {
    if let Some(py_col) = py_any_to_py_column_ref(col_any) {
        return Ok(Some(py_col.bind(col_any.py()).borrow().inner.clone()));
    }
    if let Ok(py_tb) = col_any.cast::<PyThenBuilder>() {
        return Ok(Some(py_then_builder_to_column(&mut py_tb.borrow_mut())?));
    }
    if col_any.cast::<PyChainedWhenBuilder>().is_ok() || col_any.cast::<PyWhenBuilder>().is_ok() {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "incomplete when(): call .then(value) before using this expression",
        ));
    }
    Ok(None)
}

fn py_agg_item_contains_expr_str(item: &Bound<'_, PyAny>) -> PyResult<bool> {
    if item.cast::<PyExprStr>().is_ok() {
        return Ok(true);
    }
    if let Ok(list) = item.cast::<PyList>() {
        for sub in list.iter() {
            if py_agg_item_contains_expr_str(&sub)? {
                return Ok(true);
            }
        }
    }
    if let Ok(tup) = item.cast::<PyTuple>() {
        for sub in tup.iter() {
            if py_agg_item_contains_expr_str(&sub)? {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn py_agg_push_expr(
    item: &Bound<'_, PyAny>,
    out: &mut Vec<robin_sparkless::Expr>,
    allow_dict: bool,
    expr_ctx: Option<(&Bound<'_, PySparkSession>, &robin_sparkless::DataFrame)>,
) -> PyResult<()> {
    if let Ok(c) = item.cast::<PyColumn>() {
        out.push(c.borrow().inner.clone().into_expr());
        return Ok(());
    }
    if let Ok(py_expr_str) = item.cast::<PyExprStr>() {
        let (session_ref, input_df) = expr_ctx.ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "agg() with expr() requires an active SparkSession",
            )
        })?;
        let expr = robin_sparkless::sql::expr_string_to_polars(
            &py_expr_str.borrow().sql,
            &session_ref.borrow().inner,
            input_df,
        )
        .map_err(to_py_err)?;
        out.push(expr);
        return Ok(());
    }
    if let Ok(list) = item.cast::<PyList>() {
        for sub in list.iter() {
            py_agg_push_expr(&sub, out, allow_dict, expr_ctx)?;
        }
        return Ok(());
    }
    if let Ok(tup) = item.cast::<PyTuple>() {
        for sub in tup.iter() {
            py_agg_push_expr(&sub, out, allow_dict, expr_ctx)?;
        }
        return Ok(());
    }
    if allow_dict {
        if let Ok(dict) = item.cast::<PyDict>() {
            for (k, v) in dict.iter() {
                let col_name: String = k.extract().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>("agg dict keys must be str")
                })?;
                let func_name: String = v.extract().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>("agg dict values must be str")
                })?;
                let c = Column::new(col_name);
                let expr_col = match func_name.as_str() {
                    "sum" => functions::sum(&c),
                    "avg" | "mean" => functions::avg(&c),
                    "min" => functions::min(&c),
                    "max" => functions::max(&c),
                    "count" => functions::count(&c),
                    "first" => c, // best effort
                    _ => return Err(to_py_err(format!("unsupported agg function: {func_name}"))),
                };
                out.push(expr_col.into_expr());
            }
            return Ok(());
        }
    }
    let msg = if allow_dict {
        "agg() expects Column expressions or dict"
    } else {
        "agg() expects Column expressions"
    };
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(msg))
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
        .into_py_any(py)?)
    } else {
        Ok(Py::new(
            py,
            PyWhenBuilder {
                inner: Some(robin_sparkless::functions::when(&condition.inner)),
            },
        )?
        .into_py_any(py)?)
    }
}

#[pyfunction]
fn count(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    // Special-case count(lit(1)) / count(lit(constant)) so it behaves like
    // PySpark's count(lit(1)) and counts all rows, not just the literal.
    // When the argument is a literal PyColumn, route it through count("*")
    // semantics (len()) instead of counting non-null values.
    if let Ok(py_col) = col.cast::<PyColumn>() {
        let inner = py_col.borrow().inner.clone();
        if let Some(json_str) = inner.literal_as_json_string() {
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&json_str) {
                if json_val.is_number() || json_val.is_boolean() {
                    let star_col = robin_sparkless::functions::col("*");
                    return Ok(PyColumn {
                        inner: functions::count(&star_col),
                    });
                }
            }
        }
    }

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
    let list = columns.cast::<PyList>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>("create_map expects a list of Columns")
    })?;
    let mut owned: Vec<Column> = Vec::with_capacity(list.len());
    for item in list.iter() {
        if let Ok(py_col) = item.cast::<PyColumn>() {
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
        if let Ok(c) = item.cast::<PyColumn>() {
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
        inner: functions::concat(&refs).map_err(to_py_err)?,
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
        .over_window(&partition_strs, &order_by, use_running, false, None)
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
    if partition_by.is_empty() {
        let mut cols: Vec<String> = Vec::with_capacity(order_by.len());
        let mut desc: Vec<bool> = Vec::with_capacity(order_by.len());
        for s in &order_by {
            let s = s.trim();
            desc.push(s.starts_with('-'));
            cols.push(if s.starts_with('-') {
                s.trim_start_matches('-').trim().to_string()
            } else {
                s.to_string()
            });
        }
        WINDOW_ORDER_SORT_HINT.with(|cell| *cell.borrow_mut() = Some((cols, desc)));
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
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let first = order_by[0].trim();
    let descending = first.starts_with('-');
    let order_col = if first.trim_start_matches('-').trim() == "<expr>" {
        robin_sparkless::functions::lit_i32(1)
    } else {
        Column::new(first.trim_start_matches('-').trim().to_string())
    };
    let windowed = order_col.percent_rank_over(&parts[..], &order_by, descending);
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn rank_window(partition_by: Vec<String>, order_by: Vec<String>) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "rank_window: order_by cannot be empty",
        ));
    }
    let first = order_by[0].trim();
    let descending = first.starts_with('-');
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    // Window.orderBy(F.lit(1)) encodes the literal as a synthetic "<expr>" sort key.
    // PySpark accepts this (order is arbitrary but defined); we must not try to resolve
    // "<expr>" as an input column, which would fail with "not found: <expr>".
    // To mirror PySpark behavior, when the sort key is the synthetic "<expr>" name,
    // fall back to ordering by the first partition column (if any).
    let order_col = if first.trim_start_matches('-').trim() == "<expr>" {
        robin_sparkless::functions::lit_i32(1)
    } else {
        Column::new(first.trim_start_matches('-').trim().to_string())
    };
    let windowed = order_col.rank_over(&parts[..], &order_by, descending);
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn dense_rank_window(partition_by: Vec<String>, order_by: Vec<String>) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "dense_rank_window: order_by cannot be empty",
        ));
    }
    let first = order_by[0].trim();
    let descending = first.starts_with('-');
    let order_col = if first.trim_start_matches('-').trim() == "<expr>" {
        robin_sparkless::functions::lit_i32(1)
    } else {
        Column::new(first.trim_start_matches('-').trim().to_string())
    };
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = order_col.dense_rank_over(&parts[..], &order_by, descending);
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn cume_dist_window(partition_by: Vec<String>, order_by: Vec<String>) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "cume_dist_window: order_by cannot be empty",
        ));
    }
    let first = order_by[0].trim();
    let descending = first.starts_with('-');
    let order_col = if first.trim_start_matches('-').trim() == "<expr>" {
        robin_sparkless::functions::lit_i32(1)
    } else {
        Column::new(first.trim_start_matches('-').trim().to_string())
    };
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = order_col.cume_dist_over(&parts[..], &order_by, descending);
    Ok(PyColumn { inner: windowed })
}

#[pyfunction]
fn ntile_window(n: u32, partition_by: Vec<String>, order_by: Vec<String>) -> PyResult<PyColumn> {
    if order_by.is_empty() {
        return Err(SparklessError::new_err(
            "ntile_window: order_by cannot be empty",
        ));
    }
    let first = order_by[0].trim();
    let descending = first.starts_with('-');
    let order_col = if first.trim_start_matches('-').trim() == "<expr>" {
        robin_sparkless::functions::lit_i32(1)
    } else {
        Column::new(first.trim_start_matches('-').trim().to_string())
    };
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = order_col.ntile_over(n, &parts[..], &order_by, descending);
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
        .over_window(&parts[..], &order_by, false, false, None)
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
        .over_window(&parts[..], &order_by, false, false, None)
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
        .over_window(&parts[..], &order_by, false, false, None)
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
    pattern: &Bound<'_, PyAny>,
    group_index: usize,
) -> PyResult<PyColumn> {
    let c = coerce_to_column(column)?;
    let pattern_str = pattern_str_from_pyany(pattern)?;
    if group_index == 0 {
        Ok(PyColumn {
            inner: functions::regexp_extract_all(&c, &pattern_str),
        })
    } else {
        Ok(PyColumn {
            inner: c.regexp_extract_all_group(&pattern_str, group_index),
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
        let py_col = item.cast::<PyColumn>().map_err(|_| {
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
        inner: functions::coalesce(&refs).map_err(to_py_err)?,
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
        let py_col = item.cast::<PyColumn>().map_err(|_| {
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
        inner: functions::format_string(format, &refs).map_err(to_py_err)?,
    })
}

#[pyfunction]
#[pyo3(signature = (*columns))]
fn greatest(columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.cast::<PyColumn>().map_err(|_| {
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
        let py_col = item.cast::<PyColumn>().map_err(|_| {
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
fn array_compact(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::array_compact(&column.inner),
    }
}

#[pyfunction]
fn posexplode(column: &PyColumn) -> (PyColumn, PyColumn) {
    let (pos, val) = functions::posexplode(&column.inner);
    (PyColumn { inner: pos }, PyColumn { inner: val })
}

#[pyfunction]
fn posexplode_outer(column: &PyColumn) -> (PyColumn, PyColumn) {
    let (pos, val) = functions::posexplode_outer(&column.inner);
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
fn to_timestamp_ltz(column: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    functions::to_timestamp_ltz(&column.inner, format)
        .map(|c| PyColumn { inner: c })
        .map_err(to_py_err)
}

#[pyfunction]
#[pyo3(signature = (column, format=None))]
fn to_timestamp_ntz(column: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    functions::to_timestamp_ntz(&column.inner, format)
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
fn monotonically_increasing_id() -> PyColumn {
    PyColumn {
        inner: functions::monotonically_increasing_id(),
    }
}

#[pyfunction]
fn spark_partition_id() -> PyColumn {
    PyColumn {
        inner: functions::spark_partition_id(),
    }
}

#[pyfunction]
#[pyo3(signature = (col, percentage, accuracy=None))]
fn percentile_approx(col: &PyColumn, percentage: f64, accuracy: Option<i32>) -> PyColumn {
    PyColumn {
        inner: functions::percentile_approx(&col.inner, percentage, accuracy),
    }
}

#[pyfunction]
fn schema_of_json(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::schema_of_json(&col.inner),
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
#[pyo3(name = "native_add_months")]
fn native_add_months(column: &PyColumn, months: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let months = py_any_to_i64_or_column(months)?;
    Ok(PyColumn {
        inner: column.inner.add_months_dynamic(&months),
    })
}

#[pyfunction]
fn make_date(year: &PyColumn, month: &PyColumn, day: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::make_date(&year.inner, &month.inner, &day.inner),
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
    if let Ok(exp_col) = exp.cast::<PyColumn>() {
        return Ok(PyColumn {
            inner: column.inner.pow_with(&exp_col.borrow().inner),
        });
    }
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
        "pow exponent must be int, float, or Column",
    ))
}

#[pyfunction]
fn signum(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::signum(&column.inner),
    }
}

#[pyfunction]
fn pmod(dividend: &PyColumn, divisor: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::pmod(&dividend.inner, &divisor.inner),
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
fn sum_distinct(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn {
        inner: functions::sum_distinct(&c),
    })
}

#[pyfunction]
fn corr(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::corr(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
fn covar_pop(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::covar_pop(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
fn covar_samp(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::covar_samp(&col1.inner, &col2.inner),
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
#[pyo3(signature = (column, asc=true))]
fn array_sort(column: &PyColumn, asc: bool) -> PyColumn {
    PyColumn {
        inner: functions::array_sort(&column.inner, asc),
    }
}

#[pyfunction]
#[pyo3(signature = (column, start, length=None))]
fn array_slice(column: &PyColumn, start: i64, length: Option<i64>) -> PyColumn {
    PyColumn {
        inner: functions::array_slice(&column.inner, start, length),
    }
}

#[pyfunction]
#[pyo3(signature = (column, separator, null_replacement=None))]
fn array_join(column: &PyColumn, separator: &str, null_replacement: Option<&str>) -> PyColumn {
    PyColumn {
        inner: functions::array_join(&column.inner, separator, null_replacement),
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
fn repeat(column: &PyColumn, n: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let n = py_any_to_i64_or_column(n)?;
    Ok(PyColumn {
        inner: column.inner.repeat_dynamic(&n),
    })
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

/// Map simple type string (from DDL) to core DataType for from_json schema.
fn py_type_str_to_core_data_type(s: &str) -> CoreDataType {
    let type_str = simple_string_to_type(s);
    match type_str.as_str() {
        "long" => CoreDataType::Long,
        "int" => CoreDataType::Integer,
        "short" | "smallint" => CoreDataType::Short,
        "double" => CoreDataType::Double,
        "float" => CoreDataType::Float,
        _ if type_str.starts_with("decimal(") => {
            let args = type_str
                .trim_start_matches("decimal(")
                .trim_end_matches(')');
            let (precision, scale) = args.split_once(',').unwrap_or(("10", "0"));
            CoreDataType::Decimal(
                precision.trim().parse().unwrap_or(10),
                scale.trim().parse().unwrap_or(0),
            )
        }
        "boolean" => CoreDataType::Boolean,
        "date" => CoreDataType::Date,
        "timestamp" => CoreDataType::Timestamp,
        "timestamp_ntz" => CoreDataType::TimestampNtz,
        "binary" => CoreDataType::Binary,
        _ => CoreDataType::String,
    }
}

/// Parse DDL schema string into Polars struct dtype for from_json.
fn ddl_schema_str_to_polars_dtype(ddl: &str) -> PyResult<polars::prelude::DataType> {
    use polars::prelude::{DataType as PlDataType, Field};
    let trimmed = ddl.trim();
    if trimmed.is_empty() {
        return Ok(PlDataType::String);
    }
    if trimmed.starts_with('{') {
        let st = schema_from_json(trimmed).map_err(to_py_err)?;
        let schema = st.to_polars_schema();
        let fields: Vec<Field> = schema
            .iter()
            .map(|(name, dtype)| Field::new(name.clone(), dtype.clone()))
            .collect();
        return Ok(PlDataType::Struct(fields));
    }
    let pairs = parse_ddl_schema_string(trimmed);
    if pairs.is_empty() {
        return Ok(PlDataType::String);
    }
    let fields: Vec<StructField> = pairs
        .into_iter()
        .map(|(name, typ)| StructField::new(name, py_type_str_to_core_data_type(&typ), true))
        .collect();
    let st = StructType::from_logical_fields(fields);
    let schema = st.to_polars_schema();
    let pl_fields: Vec<Field> = schema
        .iter()
        .map(|(name, dtype)| Field::new(name.clone(), dtype.clone()))
        .collect();
    Ok(PlDataType::Struct(pl_fields))
}

#[pyfunction]
#[pyo3(signature = (column, replace, pos, length=-1))]
fn overlay(column: &PyColumn, replace: &str, pos: i64, length: i64) -> PyColumn {
    PyColumn {
        inner: functions::overlay(&column.inner, replace, pos, length),
    }
}

#[pyfunction]
fn factorial(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::factorial(&column.inner),
    }
}

#[pyfunction]
fn cbrt(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::cbrt(&column.inner),
    }
}

#[pyfunction]
fn hypot(x: &PyColumn, y: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::hypot(&x.inner, &y.inner),
    }
}

#[pyfunction]
fn map_from_arrays(keys: &PyColumn, values: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::map_from_arrays(&keys.inner, &values.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (column, charset="UTF-8"))]
fn encode(column: &PyColumn, charset: &str) -> PyColumn {
    PyColumn {
        inner: functions::encode(&column.inner, charset),
    }
}

#[pyfunction]
#[pyo3(signature = (column, charset="UTF-8"))]
fn decode(column: &PyColumn, charset: &str) -> PyColumn {
    PyColumn {
        inner: functions::decode(&column.inner, charset),
    }
}

#[pyfunction]
#[pyo3(name = "char")]
fn spark_char(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::char(&column.inner),
    }
}

#[pyfunction]
fn to_json(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::to_json(&column.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (column, schema=None))]
fn from_json(column: &PyColumn, schema: Option<&str>) -> PyResult<PyColumn> {
    let pl_dtype = schema.map(ddl_schema_str_to_polars_dtype).transpose()?;
    Ok(PyColumn {
        inner: functions::from_json(&column.inner, pl_dtype),
    })
}

#[pyfunction]
fn parse_json(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::parse_json(&column.inner),
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
        let py_col = item.cast::<PyColumn>().map_err(|_| {
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
        inner: functions::concat_ws(separator, &refs).map_err(to_py_err)?,
    })
}

/// Convert a single Python value to Column: PyColumn -> as-is, str -> col(name), else -> lit(value).
fn array_item_to_column(item: &Bound<'_, PyAny>) -> PyResult<Column> {
    if let Ok(py_col) = item.cast::<PyColumn>() {
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
        if let Ok(list) = item.cast::<PyList>() {
            for sub in list.iter() {
                cols.push(array_item_to_column(&sub)?);
            }
        } else if let Ok(tup) = item.cast::<PyTuple>() {
            // Issue #1115 / test_array_empty_tuple_raises_like_pyspark:
            // F.array(()) must raise (PySpark rejects bare empty tuple); F.array([]) remains valid.
            if tup.len() == 0 {
                return Err(to_py_err(
                    "array() does not accept an empty tuple; use [] instead",
                ));
            }
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
#[pyo3(signature = (col, deterministic=false))]
fn mode(col: &PyColumn, deterministic: bool) -> PyColumn {
    PyColumn {
        inner: functions::mode(&col.inner, deterministic),
    }
}

#[pyfunction]
fn date_trunc(format: &str, column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::date_trunc(format, &column.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_next_day")]
fn native_next_day(column: &PyColumn, day_of_week: &str) -> PyColumn {
    PyColumn {
        inner: functions::next_day(&column.inner, day_of_week),
    }
}

#[pyfunction]
#[pyo3(name = "native_trunc")]
fn native_trunc(column: &PyColumn, format: &str) -> PyColumn {
    PyColumn {
        inner: functions::trunc(&column.inner, format),
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
#[pyo3(signature = (col, ignorenulls=false))]
fn last(col: &PyColumn, ignorenulls: bool) -> PyColumn {
    PyColumn {
        inner: functions::last(&col.inner, ignorenulls),
    }
}

#[pyfunction]
fn translate(column: &PyColumn, from_str: &str, to_str: &str) -> PyColumn {
    PyColumn {
        inner: functions::translate(&column.inner, from_str, to_str),
    }
}

#[pyfunction]
#[pyo3(name = "native_instr")]
fn native_instr(column: &PyColumn, substr: &str) -> PyColumn {
    PyColumn {
        inner: functions::instr(&column.inner, substr),
    }
}

#[pyfunction]
#[pyo3(name = "native_locate")]
#[pyo3(signature = (substr, column, pos=1))]
fn native_locate(substr: &str, column: &PyColumn, pos: i64) -> PyColumn {
    PyColumn {
        inner: functions::locate(substr, &column.inner, pos),
    }
}

#[pyfunction]
fn substring_index(column: &PyColumn, delimiter: &str, count: i64) -> PyColumn {
    PyColumn {
        inner: functions::substring_index(&column.inner, delimiter, count),
    }
}

#[pyfunction]
#[pyo3(name = "native_crc32")]
fn native_crc32(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::crc32(&column.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_xxhash64")]
fn native_xxhash64(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::xxhash64(&column.inner),
    }
}

#[pyfunction]
#[pyo3(name = "native_hash", signature = (*columns))]
fn native_hash(columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.cast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("hash expects Column expressions")
        })?;
        cols.push(py_col.borrow().inner.clone());
    }
    if cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "hash requires at least one column",
        ));
    }
    let refs: Vec<&Column> = cols.iter().collect();
    Ok(PyColumn {
        inner: functions::hash(&refs),
    })
}

#[pyfunction]
#[pyo3(name = "native_typeof")]
fn native_typeof(column: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::typeof_(&column.inner),
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
        let py_col = item.cast::<PyColumn>().map_err(|_| {
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
        inner: functions::struct_(&refs).map_err(to_py_err)?,
    })
}

#[pyfunction]
fn configure_for_multiprocessing() {
    // SAFETY: set once before Polars/Rayon worker pools start (pytest-xdist parent or early import).
    unsafe {
        std::env::set_var("POLARS_MAX_THREADS", "1");
        std::env::set_var("RAYON_NUM_THREADS", "1");
        std::env::set_var("ROBIN_SPARKLESS_MULTIPROCESSING", "1");
    }
}
