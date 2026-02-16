//! Python bindings for robin-sparkless (PyO3).
//! Compiled only when the `pyo3` feature is enabled.
#![allow(non_snake_case)] // PySpark param names (fromBase, errMsg, etc.) for API parity

use crate::column::Column as RsColumn;
use crate::functions::{
    abs, acos, acosh, add_months, array, array_append, array_compact, array_distinct, array_except,
    array_flatten, array_insert, array_intersect, array_prepend, array_remove, array_union, ascii,
    asin, asinh, atan, atan2, atanh, base64, cast as rs_cast, cbrt, ceiling, char_length,
    character_length, chr, contains, convert_timezone, cos, cosh, curdate, current_date,
    current_timestamp, current_timezone, date_add, date_diff, date_format, date_from_unix_date,
    date_part, date_sub, date_trunc, dateadd, datepart, day, dayname, dayofmonth, dayofweek,
    dayofyear, days, decode, degrees, encode, endswith, expm1, extract, factorial, find_in_set,
    format_number, format_string, from_csv, from_unixtime, from_utc_timestamp, get_json_object,
    greatest as rs_greatest, hour, hours, hypot, ifnull, ilike, initcap, isnan as rs_isnan,
    isnotnull, isnull, json_tuple, last_day, lcase, least as rs_least, left, length, like, ln,
    localtimestamp, log, log10, log1p, log2, log_with_base, ltrim, make_date, make_interval,
    make_timestamp, make_timestamp_ntz, md5, minute, minutes, month, months, months_between,
    next_day, now, nullif, nvl, nvl2, overlay, pmod, power, quarter, radians, regexp_count,
    regexp_extract, regexp_extract_all, regexp_instr, regexp_replace, regexp_substr, repeat,
    replace as rs_replace, reverse, right, rint, rlike, rtrim, schema_of_csv, schema_of_json,
    second, sha1, sha2, signum, sin, sinh, soundex, split, split_part, startswith, substr, tan,
    tanh, timestamp_micros, timestamp_millis, timestamp_seconds, timestampadd, timestampdiff,
    to_csv, to_date, to_degrees, to_radians, to_timestamp, to_unix_timestamp, to_utc_timestamp,
    trim, try_cast as rs_try_cast, ucase, unbase64, unix_date, unix_micros, unix_millis,
    unix_seconds, unix_timestamp, unix_timestamp_now, weekday, weekofyear, year, years,
};
use crate::functions::{
    aggregate, array_agg, array_contains, array_join, array_max, array_min, array_position,
    array_size, array_sort, arrays_overlap, arrays_zip, assert_true as rs_assert_true, bit_and,
    bit_count, bit_length, bit_or, bit_xor, bitwise_not, broadcast as rs_broadcast, cardinality,
    concat as rs_concat, concat_ws as rs_concat_ws, create_map,
    current_catalog as rs_current_catalog, current_database as rs_current_database,
    current_schema as rs_current_schema, current_user as rs_current_user, element_at, equal_null,
    exp, explode, explode_outer, floor, get, hash, inline as rs_inline,
    inline_outer as rs_inline_outer, input_file_name as rs_input_file_name, isin, isin_i64,
    isin_str, json_array_length, map_concat, map_contains_key, map_filter_value_gt,
    map_from_entries, map_zip_with_coalesce,
    monotonically_increasing_id as rs_monotonically_increasing_id, named_struct, parse_url,
    posexplode, rand as rs_rand, randn as rs_randn, round, sequence, shift_left, shift_right,
    shuffle, size, spark_partition_id as rs_spark_partition_id, stddev, str_to_map, struct_,
    to_char, to_number, to_varchar, try_add, try_divide, try_multiply, try_subtract, try_to_number,
    try_to_timestamp, typeof_, url_decode, url_encode, user as rs_user, version, width_bucket,
    xxhash64, zip_with_coalesce,
};
use crate::functions::{
    any_value, approx_count_distinct, approx_percentile, avg, bool_and, coalesce, col as rs_col,
    collect_list, collect_set, corr, count, count_if, covar_pop, every, first, kurtosis, max,
    max_by, min, min_by, skewness, sum as rs_sum, try_avg, try_sum,
};
use crate::functions::{
    asc, asc_nulls_first, asc_nulls_last, bround, cot, csc, desc, desc_nulls_first,
    desc_nulls_last, e, median, mode, negate, pi, positive, sec, stddev_pop, var_pop,
};
use crate::functions::{bin, btrim, conv, getbit, hex, locate, unhex};
use crate::functions::{crc32, levenshtein};
use crate::plan;
use crate::SparkSession;
use pyo3::conversion::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::Value as JsonValue;

mod column;
mod dataframe;
mod order;
mod session;
mod udf;
pub(crate) use column::PyColumn;
pub(crate) use dataframe::{
    PyCubeRollupData, PyDataFrame, PyDataFrameNa, PyDataFrameStat, PyDataFrameWriter,
    PyGroupedData, PyPivotedGroupedData,
};
pub(crate) use order::{
    PyChainedWhenBuilder, PyDenseRank, PyRowNumber, PySortOrder, PyThenBuilder, PyWhenBuilder,
    PyWhenThen, PyWindow,
};
use session::parse_return_type;
pub(crate) use session::{
    PyCatalog, PyDataFrameReader, PyRuntimeConfig, PySparkContext, PySparkSession,
    PySparkSessionBuilder, PyUDFRegistration, PyUserDefinedFunction,
};
pub(crate) use udf::execute_python_udf;

/// Convert a Python scalar to serde_json::Value for plan/row data.
/// Bool must be checked before i64 because in Python bool is a subclass of int (True/False extract as 1/0).
/// Supports dict (struct/map) and list (array) for nested values (#198).
pub(crate) fn py_to_json_value(value: &Bound<'_, pyo3::types::PyAny>) -> PyResult<JsonValue> {
    if value.is_none() {
        return Ok(JsonValue::Null);
    }
    if let Ok(x) = value.extract::<bool>() {
        return Ok(JsonValue::Bool(x));
    }
    if let Ok(x) = value.extract::<i64>() {
        return Ok(JsonValue::Number(serde_json::Number::from(x)));
    }
    if let Ok(x) = value.extract::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(x) {
            return Ok(JsonValue::Number(n));
        }
        return Ok(JsonValue::Null);
    }
    if let Ok(x) = value.extract::<String>() {
        return Ok(JsonValue::String(x));
    }
    if let Ok(dict) = value.downcast::<pyo3::types::PyDict>() {
        let mut obj = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key = k.extract::<String>()?;
            let val = py_to_json_value(&v)?;
            obj.insert(key, val);
        }
        return Ok(JsonValue::Object(obj));
    }
    if let Ok(list) = value.downcast::<pyo3::types::PyList>() {
        let arr: Vec<JsonValue> = list
            .iter()
            .map(|v| py_to_json_value(&v))
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(JsonValue::Array(arr));
    }
    // Python datetime.date / datetime.datetime (#239): serialize to ISO string for Rust date/timestamp columns.
    if let Ok(iso) = py_date_or_datetime_to_iso_string(value) {
        return Ok(JsonValue::String(iso));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "create_dataframe_from_rows / execute_plan: row values must be None, int, float, bool, str, dict (struct/map), list (array), datetime.date, or datetime.datetime",
    ))
}

/// Convert Python datetime.date or datetime.datetime to an ISO string for use as JsonValue.
/// Returns Ok(s) if the value has year/month/day (and optionally time); Err if not.
fn py_date_or_datetime_to_iso_string(value: &Bound<'_, pyo3::types::PyAny>) -> PyResult<String> {
    let year: i32 = value
        .getattr("year")
        .ok()
        .and_then(|a| a.extract().ok())
        .ok_or_else(|| {
            pyo3::exceptions::PyTypeError::new_err("expected date/datetime with year")
        })?;
    let month: u32 = value
        .getattr("month")
        .ok()
        .and_then(|a| a.extract().ok())
        .ok_or_else(|| {
            pyo3::exceptions::PyTypeError::new_err("expected date/datetime with month")
        })?;
    let day: u32 = value
        .getattr("day")
        .ok()
        .and_then(|a| a.extract().ok())
        .ok_or_else(|| pyo3::exceptions::PyTypeError::new_err("expected date/datetime with day"))?;
    let has_time = value.getattr("hour").is_ok()
        && value.getattr("minute").is_ok()
        && value.getattr("second").is_ok();
    if has_time {
        let hour: u32 = value.getattr("hour")?.extract()?;
        let minute: u32 = value.getattr("minute")?.extract()?;
        let second: u32 = value.getattr("second")?.extract()?;
        let micros: u32 = value
            .getattr("microsecond")
            .ok()
            .and_then(|a| a.extract().ok())
            .unwrap_or(0);
        if micros > 0 {
            Ok(format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                year, month, day, hour, minute, second, micros
            ))
        } else {
            Ok(format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                year, month, day, hour, minute, second
            ))
        }
    } else {
        Ok(format!("{:04}-{:02}-{:02}", year, month, day))
    }
}

/// Execute a logical plan over in-memory row data and return a lazy DataFrame.
///
/// Use this for programmatic execution of plans (e.g. from a query planner).
/// Call ``.collect()`` on the result to materialize rows as a list of dicts.
///
/// Args:
///     data: List of rows. Each row is either a dict (keyed by column name) or a list
///         of values in schema order. Must match ``schema``.
///     schema: List of (name, dtype_str) column definitions, e.g. [("id", "bigint"), ("name", "string")].
///     plan_json: JSON string of the logical plan (array of op objects). Invalid JSON raises ValueError.
///
/// Returns:
///     DataFrame (lazy). Call ``.collect()`` to get list of dicts.
///
/// Raises:
///     TypeError: If a row is not a dict or list, or a value type is unsupported.
///     ValueError: If plan_json is invalid JSON.
///     RuntimeError: If plan execution fails.
#[pyfunction]
fn py_execute_plan(
    py: Python<'_>,
    data: &Bound<'_, pyo3::types::PyAny>,
    schema: Vec<(String, String)>,
    plan_json: &str,
) -> PyResult<PyDataFrame> {
    let data_list = data
        .extract::<Vec<Bound<'_, pyo3::types::PyAny>>>()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(data_list.len());
    let names: Vec<&str> = schema.iter().map(|(n, _)| n.as_str()).collect();
    for row_any in &data_list {
        if let Ok(dict) = row_any.downcast::<PyDict>() {
            let row: Vec<JsonValue> = names
                .iter()
                .map(|name| {
                    let v = dict
                        .get_item(*name)
                        .ok()
                        .flatten()
                        .unwrap_or_else(|| py.None().into_bound(py));
                    py_to_json_value(&v)
                })
                .collect::<PyResult<Vec<_>>>()?;
            rows.push(row);
        } else if let Ok(list) = row_any.extract::<Vec<Bound<'_, pyo3::types::PyAny>>>() {
            let row: Vec<JsonValue> = list
                .iter()
                .map(|v| py_to_json_value(v))
                .collect::<PyResult<Vec<_>>>()?;
            rows.push(row);
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "execute_plan: each row must be a dict or a list",
            ));
        }
    }
    let plan_values: Vec<JsonValue> = serde_json::from_str(plan_json)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let spark = SparkSession::builder()
        .app_name("execute_plan")
        .get_or_create();
    let df = plan::execute_plan(&spark, rows, schema, &plan_values)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyDataFrame { inner: df })
}

/// Return the set of plan operation names supported by ``_execute_plan`` (issue #202).
///
/// Sparkless and other backends can use this to decide which operations to send
/// to the Robin backend. Includes ``filter``, ``select``, ``limit``, ``offset``,
/// ``orderBy``, ``withColumn``, ``withColumnRenamed``, ``groupBy``, ``join``,
/// ``union``, ``unionByName``, ``distinct``, ``drop``.
///
/// Returns:
///     tuple[str, ...]: Immutable tuple of supported op names.
#[pyfunction]
fn py_supported_plan_operations(py: Python<'_>) -> PyResult<pyo3::Py<pyo3::types::PyTuple>> {
    const OPS: &[&str] = &[
        "filter",
        "select",
        "limit",
        "offset",
        "orderBy",
        "withColumn",
        "withColumnRenamed",
        "groupBy",
        "join",
        "union",
        "unionByName",
        "distinct",
        "drop",
    ];
    let t = pyo3::types::PyTuple::new(py, OPS.iter().copied())?;
    Ok(t.unbind())
}

/// Robin Sparkless: PySpark-compatible DataFrame API with local execution.
///
/// This module provides a subset of the PySpark API backed by Polars. All execution
/// is local (single process). Use it for testing, prototyping, or running pipelines
/// where distributed execution is not required.
///
/// Quick start::
///
///     from robin_sparkless import SparkSession
///     spark = SparkSession.builder().app_name("my_app").get_or_create()
///     df = spark.read_csv("data.csv")
///     df.filter(col("age") > 18).select("name", "age").show()
///
/// Key entry points: ``SparkSession.builder()``, ``col()``, ``lit()``, ``when()``,
/// ``sum()`` / ``avg()`` / ``count()``, and DataFrame methods ``filter``, ``select``,
/// ``group_by``, ``join``, etc.
///
/// Multiprocessing / pytest-xdist: Polars (and thus robin-sparkless) is not fork-safe.
/// When using pytest-xdist (``pytest -n N``) or multiprocessing with fork, workers may
/// crash ("node down: Not properly terminated"). To reduce this risk, call
/// ``configure_for_multiprocessing()`` as early as possible (e.g. in conftest.py), or
/// set ``ROBIN_SPARKLESS_MULTIPROCESSING=1`` before running. Prefer fewer workers
/// (e.g. ``-n 4``) or serial (``-n 0``) when using the Robin backend. For custom
/// multiprocessing, use ``multiprocessing.get_context("spawn")`` instead of fork.
/// Limit Polars to a single thread for fork-safety when using multiprocessing
/// (e.g. pytest-xdist, multiprocessing.Pool). Must be called before any
/// SparkSession/DataFrame operations. See docs on multiprocessing/fork safety.
fn set_polars_single_thread_if_requested() {
    if std::env::var("ROBIN_SPARKLESS_MULTIPROCESSING").is_ok() {
        std::env::set_var("POLARS_MAX_THREADS", "1");
    }
}

/// Configure robin-sparkless for use from forked worker processes (pytest-xdist,
/// multiprocessing with fork). Limits Polars to a single thread to reduce
/// deadlocks. Call this as early as possible, before any SparkSession or
/// DataFrame operations.
///
/// Example for pytest (conftest.py)::
///
///     import robin_sparkless
///     robin_sparkless.configure_for_multiprocessing()
///
/// Alternatively, set ``ROBIN_SPARKLESS_MULTIPROCESSING=1`` before running.
#[pyfunction]
fn py_configure_for_multiprocessing() {
    std::env::set_var("POLARS_MAX_THREADS", "1");
}

/// Register a grouped vectorized (pandas-style) aggregation UDF and return a callable wrapper.
///
/// This is a minimal pandas_udf-style API focused on GROUPED_AGG semantics. It registers the
/// given Python callable as a grouped, vectorized aggregation UDF on the **active SparkSession**
/// (SparkSession.builder().get_or_create()) and returns a callable object that can be used
/// inside groupBy().agg(...).
///
/// Usage (minimal v1):
///
///     import robin_sparkless as rs
///
///     spark = rs.SparkSession.builder().app_name("grouped").get_or_create()
///
///     def mean_udf(values):
///         return sum(values) / len(values)
///
///     mean_udf = rs.pandas_udf(mean_udf, "double", function_type="grouped_agg")
///
///     df = spark.createDataFrame([(1, 10), (1, 20), (2, 5)], ["k", "v"])
///     result = df.group_by(["k"]).agg([mean_udf(rs.col("v")).alias("mean_v")])
///
/// In v1 only function_type="grouped_agg" is supported. The return_type is required and may be
/// a DDL string ("int", "double", "string", etc.) or a DataType-like object with typeName().
#[pyfunction]
#[pyo3(signature = (f, return_type=None, function_type="grouped_agg"))]
fn py_pandas_udf(
    py: Python<'_>,
    f: Bound<'_, pyo3::types::PyAny>,
    return_type: Option<Bound<'_, pyo3::types::PyAny>>,
    function_type: &str,
) -> PyResult<Py<PyUserDefinedFunction>> {
    if function_type != "grouped_agg" {
        return Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "pandas_udf currently supports only function_type='grouped_agg'",
        ));
    }

    // Resolve return dtype using the same helper as spark.udf().register.
    let dtype = parse_return_type(py, return_type.as_ref())
        .map_err(pyo3::exceptions::PyValueError::new_err)?;

    // Derive UDF name from function __name__ (PySpark parity-ish).
    let name: String = f
        .getattr("__name__")
        .ok()
        .and_then(|n| n.extract().ok())
        .unwrap_or_else(|| "pandas_udf".to_string());

    // Get active/default session (same source as DataFrame.createOrReplaceTempView etc.).
    let session = session::get_default_session().ok_or_else(|| {
        pyo3::exceptions::PyRuntimeError::new_err(
            "pandas_udf: no active SparkSession. Call SparkSession.builder().get_or_create() first.",
        )
    })?;

    // Register as grouped, vectorized aggregation UDF.
    session
        .udf_registry
        .register_grouped_vectorized_python_udf(&name, f.unbind(), dtype)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    // Return a UserDefinedFunction wrapper so usage is similar to spark.udf().register.
    Py::new(py, PyUserDefinedFunction { name, session })
}

#[pymodule]
fn robin_sparkless(m: &Bound<'_, PyModule>) -> PyResult<()> {
    set_polars_single_thread_if_requested();
    m.add_class::<PySparkSession>()?;
    m.add_class::<PySparkSessionBuilder>()?;
    m.add_class::<PySparkContext>()?;
    m.add_class::<PyDataFrameReader>()?;
    m.add_class::<PyCatalog>()?;
    m.add_class::<PyRuntimeConfig>()?;
    m.add_class::<PyUDFRegistration>()?;
    m.add_class::<PyUserDefinedFunction>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyDataFrameStat>()?;
    m.add_class::<PyDataFrameNa>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PyPosexplodeResult>()?;
    m.add_class::<PySortOrder>()?;
    m.add_class::<PyWindow>()?;
    m.add_class::<PyRowNumber>()?;
    m.add_class::<PyDenseRank>()?;
    m.add_class::<PyWhenThen>()?;
    m.add_class::<PyWhenBuilder>()?;
    m.add_class::<PyThenBuilder>()?;
    m.add_class::<PyChainedWhenBuilder>()?;
    m.add_class::<PyGroupedData>()?;
    m.add_class::<PyPivotedGroupedData>()?;
    m.add_class::<PyCubeRollupData>()?;
    m.add_class::<PyDataFrameWriter>()?;
    m.add("col", wrap_pyfunction!(py_col, m)?)?;
    m.add("concat", wrap_pyfunction!(py_concat, m)?)?;
    m.add("concat_ws", wrap_pyfunction!(py_concat_ws, m)?)?;
    m.add("row_number", wrap_pyfunction!(py_row_number, m)?)?;
    m.add("dense_rank", wrap_pyfunction!(py_dense_rank, m)?)?;
    m.add("lag", wrap_pyfunction!(py_lag, m)?)?;
    m.add("lead", wrap_pyfunction!(py_lead, m)?)?;
    m.add("call_udf", wrap_pyfunction!(py_call_udf, m)?)?;
    m.add("lit", wrap_pyfunction!(py_lit, m)?)?;
    m.add("when", wrap_pyfunction!(py_when, m)?)?;
    m.add("coalesce", wrap_pyfunction!(py_coalesce, m)?)?;
    m.add("sum", wrap_pyfunction!(py_sum, m)?)?;
    m.add("avg", wrap_pyfunction!(py_avg, m)?)?;
    m.add("min", wrap_pyfunction!(py_min, m)?)?;
    m.add("min_by", wrap_pyfunction!(py_min_by, m)?)?;
    m.add("max", wrap_pyfunction!(py_max, m)?)?;
    m.add("max_by", wrap_pyfunction!(py_max_by, m)?)?;
    m.add("try_sum", wrap_pyfunction!(py_try_sum, m)?)?;
    m.add("try_avg", wrap_pyfunction!(py_try_avg, m)?)?;
    m.add(
        "approx_count_distinct",
        wrap_pyfunction!(py_approx_count_distinct, m)?,
    )?;
    m.add(
        "approx_percentile",
        wrap_pyfunction!(py_approx_percentile, m)?,
    )?;
    m.add("bool_and", wrap_pyfunction!(py_bool_and, m)?)?;
    m.add("collect_list", wrap_pyfunction!(py_collect_list, m)?)?;
    m.add("collect_set", wrap_pyfunction!(py_collect_set, m)?)?;
    m.add("corr", wrap_pyfunction!(py_corr, m)?)?;
    m.add("count", wrap_pyfunction!(py_count, m)?)?;
    m.add("covar_pop", wrap_pyfunction!(py_covar_pop, m)?)?;
    m.add("every", wrap_pyfunction!(py_every, m)?)?;
    m.add("count_if", wrap_pyfunction!(py_count_if, m)?)?;
    m.add("first", wrap_pyfunction!(py_first, m)?)?;
    m.add("kurtosis", wrap_pyfunction!(py_kurtosis, m)?)?;
    m.add("skewness", wrap_pyfunction!(py_skewness, m)?)?;
    m.add("any_value", wrap_pyfunction!(py_any_value, m)?)?;
    m.add("ascii", wrap_pyfunction!(py_ascii, m)?)?;
    m.add("format_number", wrap_pyfunction!(py_format_number, m)?)?;
    m.add("overlay", wrap_pyfunction!(py_overlay, m)?)?;
    m.add("position", wrap_pyfunction!(py_position, m)?)?;
    m.add("char", wrap_pyfunction!(py_char, m)?)?;
    m.add("chr", wrap_pyfunction!(py_chr, m)?)?;
    m.add("base64", wrap_pyfunction!(py_base64, m)?)?;
    m.add("unbase64", wrap_pyfunction!(py_unbase64, m)?)?;
    m.add("sha1", wrap_pyfunction!(py_sha1, m)?)?;
    m.add("sha2", wrap_pyfunction!(py_sha2, m)?)?;
    m.add("md5", wrap_pyfunction!(py_md5, m)?)?;
    m.add("array_compact", wrap_pyfunction!(py_array_compact, m)?)?;
    m.add("array_distinct", wrap_pyfunction!(py_array_distinct, m)?)?;
    m.add("sin", wrap_pyfunction!(py_sin, m)?)?;
    m.add("cos", wrap_pyfunction!(py_cos, m)?)?;
    m.add("tan", wrap_pyfunction!(py_tan, m)?)?;
    m.add("asin", wrap_pyfunction!(py_asin, m)?)?;
    m.add("acos", wrap_pyfunction!(py_acos, m)?)?;
    m.add("atan", wrap_pyfunction!(py_atan, m)?)?;
    m.add("atan2", wrap_pyfunction!(py_atan2, m)?)?;
    m.add("degrees", wrap_pyfunction!(py_degrees, m)?)?;
    m.add("radians", wrap_pyfunction!(py_radians, m)?)?;
    m.add("signum", wrap_pyfunction!(py_signum, m)?)?;
    m.add("quarter", wrap_pyfunction!(py_quarter, m)?)?;
    m.add("weekofyear", wrap_pyfunction!(py_weekofyear, m)?)?;
    m.add("dayofweek", wrap_pyfunction!(py_dayofweek, m)?)?;
    m.add("dayofyear", wrap_pyfunction!(py_dayofyear, m)?)?;
    m.add("add_months", wrap_pyfunction!(py_add_months, m)?)?;
    m.add("months_between", wrap_pyfunction!(py_months_between, m)?)?;
    m.add("next_day", wrap_pyfunction!(py_next_day, m)?)?;
    m.add("cast", wrap_pyfunction!(py_cast, m)?)?;
    m.add("try_cast", wrap_pyfunction!(py_try_cast, m)?)?;
    m.add("isnan", wrap_pyfunction!(py_isnan, m)?)?;
    m.add("greatest", wrap_pyfunction!(py_greatest, m)?)?;
    m.add("least", wrap_pyfunction!(py_least, m)?)?;
    m.add("nvl", wrap_pyfunction!(py_nvl, m)?)?;
    m.add("ifnull", wrap_pyfunction!(py_ifnull, m)?)?;
    m.add("nullif", wrap_pyfunction!(py_nullif, m)?)?;
    m.add("nvl2", wrap_pyfunction!(py_nvl2, m)?)?;
    m.add("substr", wrap_pyfunction!(py_substr, m)?)?;
    m.add("power", wrap_pyfunction!(py_power, m)?)?;
    m.add("ln", wrap_pyfunction!(py_ln, m)?)?;
    m.add("log", wrap_pyfunction!(py_log, m)?)?;
    m.add("ceiling", wrap_pyfunction!(py_ceiling, m)?)?;
    m.add("ceil", wrap_pyfunction!(py_ceiling, m)?)?;
    m.add("floor", wrap_pyfunction!(py_floor, m)?)?;
    m.add("round", wrap_pyfunction!(py_round, m)?)?;
    m.add("exp", wrap_pyfunction!(py_exp, m)?)?;
    m.add("abs", wrap_pyfunction!(py_abs, m)?)?;
    m.add("date_add", wrap_pyfunction!(py_date_add, m)?)?;
    m.add("date_sub", wrap_pyfunction!(py_date_sub, m)?)?;
    m.add("date_format", wrap_pyfunction!(py_date_format, m)?)?;
    m.add("current_date", wrap_pyfunction!(py_current_date, m)?)?;
    m.add(
        "current_timestamp",
        wrap_pyfunction!(py_current_timestamp, m)?,
    )?;
    m.add("char_length", wrap_pyfunction!(py_char_length, m)?)?;
    m.add(
        "character_length",
        wrap_pyfunction!(py_character_length, m)?,
    )?;
    m.add("date_trunc", wrap_pyfunction!(py_date_trunc, m)?)?;
    m.add("array", wrap_pyfunction!(py_array, m)?)?;
    m.add("array_contains", wrap_pyfunction!(py_array_contains, m)?)?;
    m.add("array_max", wrap_pyfunction!(py_array_max, m)?)?;
    m.add("array_min", wrap_pyfunction!(py_array_min, m)?)?;
    m.add("array_position", wrap_pyfunction!(py_array_position, m)?)?;
    m.add("array_size", wrap_pyfunction!(py_array_size, m)?)?;
    m.add("size", wrap_pyfunction!(py_size, m)?)?;
    m.add("array_join", wrap_pyfunction!(py_array_join, m)?)?;
    m.add("array_sort", wrap_pyfunction!(py_array_sort, m)?)?;
    m.add("cardinality", wrap_pyfunction!(py_cardinality, m)?)?;
    m.add("mean", wrap_pyfunction!(py_avg, m)?)?;
    m.add("std", wrap_pyfunction!(py_stddev, m)?)?;
    m.add("sign", wrap_pyfunction!(py_signum, m)?)?;
    m.add("aggregate", wrap_pyfunction!(py_aggregate, m)?)?;
    m.add("lcase", wrap_pyfunction!(py_lcase, m)?)?;
    m.add("ucase", wrap_pyfunction!(py_ucase, m)?)?;
    m.add("day", wrap_pyfunction!(py_day, m)?)?;
    m.add("dayofmonth", wrap_pyfunction!(py_dayofmonth, m)?)?;
    m.add("year", wrap_pyfunction!(py_year, m)?)?;
    m.add("month", wrap_pyfunction!(py_month, m)?)?;
    m.add("hour", wrap_pyfunction!(py_hour, m)?)?;
    m.add("minute", wrap_pyfunction!(py_minute, m)?)?;
    m.add("second", wrap_pyfunction!(py_second, m)?)?;
    m.add("last_day", wrap_pyfunction!(py_last_day, m)?)?;
    m.add("to_degrees", wrap_pyfunction!(py_to_degrees, m)?)?;
    m.add("to_radians", wrap_pyfunction!(py_to_radians, m)?)?;
    m.add("isnull", wrap_pyfunction!(py_isnull, m)?)?;
    m.add("isnotnull", wrap_pyfunction!(py_isnotnull, m)?)?;
    m.add("left", wrap_pyfunction!(py_left, m)?)?;
    m.add("length", wrap_pyfunction!(py_length, m)?)?;
    m.add("right", wrap_pyfunction!(py_right, m)?)?;
    m.add("trim", wrap_pyfunction!(py_trim, m)?)?;
    m.add("ltrim", wrap_pyfunction!(py_ltrim, m)?)?;
    m.add("rtrim", wrap_pyfunction!(py_rtrim, m)?)?;
    m.add("repeat", wrap_pyfunction!(py_repeat, m)?)?;
    m.add("reverse", wrap_pyfunction!(py_reverse, m)?)?;
    m.add("initcap", wrap_pyfunction!(py_initcap, m)?)?;
    m.add("replace", wrap_pyfunction!(py_replace, m)?)?;
    m.add("startswith", wrap_pyfunction!(py_startswith, m)?)?;
    m.add("endswith", wrap_pyfunction!(py_endswith, m)?)?;
    m.add("contains", wrap_pyfunction!(py_contains, m)?)?;
    m.add("like", wrap_pyfunction!(py_like, m)?)?;
    m.add("ilike", wrap_pyfunction!(py_ilike, m)?)?;
    m.add("rlike", wrap_pyfunction!(py_rlike, m)?)?;
    m.add("regexp_count", wrap_pyfunction!(py_regexp_count, m)?)?;
    m.add("regexp_instr", wrap_pyfunction!(py_regexp_instr, m)?)?;
    m.add("regexp_substr", wrap_pyfunction!(py_regexp_substr, m)?)?;
    m.add("regexp_extract", wrap_pyfunction!(py_regexp_extract, m)?)?;
    m.add("regexp_replace", wrap_pyfunction!(py_regexp_replace, m)?)?;
    m.add(
        "regexp_extract_all",
        wrap_pyfunction!(py_regexp_extract_all, m)?,
    )?;
    m.add("soundex", wrap_pyfunction!(py_soundex, m)?)?;
    m.add("levenshtein", wrap_pyfunction!(py_levenshtein, m)?)?;
    m.add("crc32", wrap_pyfunction!(py_crc32, m)?)?;
    m.add("xxhash64", wrap_pyfunction!(py_xxhash64, m)?)?;
    m.add("split", wrap_pyfunction!(py_split, m)?)?;
    m.add("split_part", wrap_pyfunction!(py_split_part, m)?)?;
    m.add("find_in_set", wrap_pyfunction!(py_find_in_set, m)?)?;
    m.add("get_json_object", wrap_pyfunction!(py_get_json_object, m)?)?;
    m.add("json_tuple", wrap_pyfunction!(py_json_tuple, m)?)?;
    m.add("from_csv", wrap_pyfunction!(py_from_csv, m)?)?;
    m.add("to_csv", wrap_pyfunction!(py_to_csv, m)?)?;
    m.add("schema_of_csv", wrap_pyfunction!(py_schema_of_csv, m)?)?;
    m.add("schema_of_json", wrap_pyfunction!(py_schema_of_json, m)?)?;
    m.add("format_string", wrap_pyfunction!(py_format_string, m)?)?;
    m.add("printf", wrap_pyfunction!(py_printf, m)?)?;
    m.add("cosh", wrap_pyfunction!(py_cosh, m)?)?;
    m.add("sinh", wrap_pyfunction!(py_sinh, m)?)?;
    m.add("tanh", wrap_pyfunction!(py_tanh, m)?)?;
    m.add("acosh", wrap_pyfunction!(py_acosh, m)?)?;
    m.add("asinh", wrap_pyfunction!(py_asinh, m)?)?;
    m.add("atanh", wrap_pyfunction!(py_atanh, m)?)?;
    m.add("cbrt", wrap_pyfunction!(py_cbrt, m)?)?;
    m.add("expm1", wrap_pyfunction!(py_expm1, m)?)?;
    m.add("log1p", wrap_pyfunction!(py_log1p, m)?)?;
    m.add("log10", wrap_pyfunction!(py_log10, m)?)?;
    m.add("log2", wrap_pyfunction!(py_log2, m)?)?;
    m.add("rint", wrap_pyfunction!(py_rint, m)?)?;
    m.add("hypot", wrap_pyfunction!(py_hypot, m)?)?;
    m.add("unix_timestamp", wrap_pyfunction!(py_unix_timestamp, m)?)?;
    m.add(
        "to_unix_timestamp",
        wrap_pyfunction!(py_to_unix_timestamp, m)?,
    )?;
    m.add("from_unixtime", wrap_pyfunction!(py_from_unixtime, m)?)?;
    m.add("make_date", wrap_pyfunction!(py_make_date, m)?)?;
    m.add(
        "timestamp_seconds",
        wrap_pyfunction!(py_timestamp_seconds, m)?,
    )?;
    m.add(
        "timestamp_millis",
        wrap_pyfunction!(py_timestamp_millis, m)?,
    )?;
    m.add(
        "timestamp_micros",
        wrap_pyfunction!(py_timestamp_micros, m)?,
    )?;
    m.add("unix_date", wrap_pyfunction!(py_unix_date, m)?)?;
    m.add(
        "date_from_unix_date",
        wrap_pyfunction!(py_date_from_unix_date, m)?,
    )?;
    m.add("pmod", wrap_pyfunction!(py_pmod, m)?)?;
    m.add("factorial", wrap_pyfunction!(py_factorial, m)?)?;
    m.add("array_append", wrap_pyfunction!(py_array_append, m)?)?;
    m.add("array_prepend", wrap_pyfunction!(py_array_prepend, m)?)?;
    m.add("array_insert", wrap_pyfunction!(py_array_insert, m)?)?;
    m.add("array_except", wrap_pyfunction!(py_array_except, m)?)?;
    m.add("array_intersect", wrap_pyfunction!(py_array_intersect, m)?)?;
    m.add("array_union", wrap_pyfunction!(py_array_union, m)?)?;
    m.add(
        "_zip_with_coalesce",
        wrap_pyfunction!(py_zip_with_coalesce, m)?,
    )?;
    m.add("map_concat", wrap_pyfunction!(py_map_concat, m)?)?;
    m.add(
        "_map_filter_value_gt",
        wrap_pyfunction!(py_map_filter_value_gt, m)?,
    )?;
    m.add(
        "map_from_entries",
        wrap_pyfunction!(py_map_from_entries, m)?,
    )?;
    m.add(
        "map_contains_key",
        wrap_pyfunction!(py_map_contains_key, m)?,
    )?;
    m.add(
        "_map_zip_with_coalesce",
        wrap_pyfunction!(py_map_zip_with_coalesce, m)?,
    )?;
    m.add("create_map", wrap_pyfunction!(py_create_map, m)?)?;
    m.add("array_remove", wrap_pyfunction!(py_array_remove, m)?)?;
    m.add("flatten", wrap_pyfunction!(py_flatten, m)?)?;
    m.add("decode", wrap_pyfunction!(py_decode, m)?)?;
    m.add("element_at", wrap_pyfunction!(py_element_at, m)?)?;
    m.add("encode", wrap_pyfunction!(py_encode, m)?)?;
    m.add("get", wrap_pyfunction!(py_get, m)?)?;
    m.add("try_divide", wrap_pyfunction!(py_try_divide, m)?)?;
    m.add("try_add", wrap_pyfunction!(py_try_add, m)?)?;
    m.add("try_subtract", wrap_pyfunction!(py_try_subtract, m)?)?;
    m.add("try_multiply", wrap_pyfunction!(py_try_multiply, m)?)?;
    m.add("width_bucket", wrap_pyfunction!(py_width_bucket, m)?)?;
    m.add("elt", wrap_pyfunction!(py_elt, m)?)?;
    m.add("bit_length", wrap_pyfunction!(py_bit_length, m)?)?;
    m.add("typeof", wrap_pyfunction!(py_typeof, m)?)?;
    m.add("struct", wrap_pyfunction!(py_struct, m)?)?;
    m.add("named_struct", wrap_pyfunction!(py_named_struct, m)?)?;
    m.add("asc", wrap_pyfunction!(py_asc, m)?)?;
    m.add("asc_nulls_first", wrap_pyfunction!(py_asc_nulls_first, m)?)?;
    m.add("asc_nulls_last", wrap_pyfunction!(py_asc_nulls_last, m)?)?;
    m.add("desc", wrap_pyfunction!(py_desc, m)?)?;
    m.add(
        "desc_nulls_first",
        wrap_pyfunction!(py_desc_nulls_first, m)?,
    )?;
    m.add("desc_nulls_last", wrap_pyfunction!(py_desc_nulls_last, m)?)?;
    m.add("bround", wrap_pyfunction!(py_bround, m)?)?;
    m.add("negate", wrap_pyfunction!(py_negate, m)?)?;
    m.add("negative", wrap_pyfunction!(py_negate, m)?)?;
    m.add("positive", wrap_pyfunction!(py_positive, m)?)?;
    m.add("cot", wrap_pyfunction!(py_cot, m)?)?;
    m.add("csc", wrap_pyfunction!(py_csc, m)?)?;
    m.add("sec", wrap_pyfunction!(py_sec, m)?)?;
    m.add("e", wrap_pyfunction!(py_e, m)?)?;
    m.add("pi", wrap_pyfunction!(py_pi, m)?)?;
    m.add("median", wrap_pyfunction!(py_median, m)?)?;
    m.add("mode", wrap_pyfunction!(py_mode, m)?)?;
    m.add("stddev_pop", wrap_pyfunction!(py_stddev_pop, m)?)?;
    m.add("var_pop", wrap_pyfunction!(py_var_pop, m)?)?;
    m.add("btrim", wrap_pyfunction!(py_btrim, m)?)?;
    m.add("locate", wrap_pyfunction!(py_locate, m)?)?;
    m.add("conv", wrap_pyfunction!(py_conv, m)?)?;
    m.add("hex", wrap_pyfunction!(py_hex, m)?)?;
    m.add("unhex", wrap_pyfunction!(py_unhex, m)?)?;
    m.add("bin", wrap_pyfunction!(py_bin, m)?)?;
    m.add("getbit", wrap_pyfunction!(py_getbit, m)?)?;
    m.add("to_char", wrap_pyfunction!(py_to_char, m)?)?;
    m.add("to_varchar", wrap_pyfunction!(py_to_varchar, m)?)?;
    m.add("to_number", wrap_pyfunction!(py_to_number, m)?)?;
    m.add("try_to_number", wrap_pyfunction!(py_try_to_number, m)?)?;
    m.add(
        "try_to_timestamp",
        wrap_pyfunction!(py_try_to_timestamp, m)?,
    )?;
    m.add("str_to_map", wrap_pyfunction!(py_str_to_map, m)?)?;
    m.add("arrays_overlap", wrap_pyfunction!(py_arrays_overlap, m)?)?;
    m.add("arrays_zip", wrap_pyfunction!(py_arrays_zip, m)?)?;
    m.add("explode", wrap_pyfunction!(py_explode, m)?)?;
    m.add("explode_outer", wrap_pyfunction!(py_explode_outer, m)?)?;
    m.add("posexplode", wrap_pyfunction!(py_posexplode, m)?)?;
    m.add("inline", wrap_pyfunction!(py_inline, m)?)?;
    m.add("inline_outer", wrap_pyfunction!(py_inline_outer, m)?)?;
    m.add("sequence", wrap_pyfunction!(py_sequence, m)?)?;
    m.add("shuffle", wrap_pyfunction!(py_shuffle, m)?)?;
    m.add("array_agg", wrap_pyfunction!(py_array_agg, m)?)?;
    m.add("curdate", wrap_pyfunction!(py_curdate, m)?)?;
    m.add("now", wrap_pyfunction!(py_now, m)?)?;
    m.add("localtimestamp", wrap_pyfunction!(py_localtimestamp, m)?)?;
    m.add("date_diff", wrap_pyfunction!(py_date_diff, m)?)?;
    m.add("dateadd", wrap_pyfunction!(py_dateadd, m)?)?;
    m.add("datepart", wrap_pyfunction!(py_datepart, m)?)?;
    m.add("extract", wrap_pyfunction!(py_extract, m)?)?;
    m.add("date_part", wrap_pyfunction!(py_date_part, m)?)?;
    m.add("unix_micros", wrap_pyfunction!(py_unix_micros, m)?)?;
    m.add("unix_millis", wrap_pyfunction!(py_unix_millis, m)?)?;
    m.add("unix_seconds", wrap_pyfunction!(py_unix_seconds, m)?)?;
    m.add("dayname", wrap_pyfunction!(py_dayname, m)?)?;
    m.add("weekday", wrap_pyfunction!(py_weekday, m)?)?;
    m.add("make_timestamp", wrap_pyfunction!(py_make_timestamp, m)?)?;
    m.add(
        "make_timestamp_ntz",
        wrap_pyfunction!(py_make_timestamp_ntz, m)?,
    )?;
    m.add("make_interval", wrap_pyfunction!(py_make_interval, m)?)?;
    m.add("timestampadd", wrap_pyfunction!(py_timestampadd, m)?)?;
    m.add("timestampdiff", wrap_pyfunction!(py_timestampdiff, m)?)?;
    m.add("days", wrap_pyfunction!(py_days, m)?)?;
    m.add("hours", wrap_pyfunction!(py_hours, m)?)?;
    m.add("minutes", wrap_pyfunction!(py_minutes, m)?)?;
    m.add("months", wrap_pyfunction!(py_months, m)?)?;
    m.add("years", wrap_pyfunction!(py_years, m)?)?;
    m.add(
        "from_utc_timestamp",
        wrap_pyfunction!(py_from_utc_timestamp, m)?,
    )?;
    m.add(
        "to_utc_timestamp",
        wrap_pyfunction!(py_to_utc_timestamp, m)?,
    )?;
    m.add(
        "convert_timezone",
        wrap_pyfunction!(py_convert_timezone, m)?,
    )?;
    m.add(
        "current_timezone",
        wrap_pyfunction!(py_current_timezone, m)?,
    )?;
    m.add("to_timestamp", wrap_pyfunction!(py_to_timestamp, m)?)?;
    m.add("to_date", wrap_pyfunction!(py_to_date, m)?)?;
    m.add("isin", wrap_pyfunction!(py_isin, m)?)?;
    m.add("_isin_i64", wrap_pyfunction!(py_isin_i64, m)?)?;
    m.add("_isin_str", wrap_pyfunction!(py_isin_str, m)?)?;
    m.add("url_decode", wrap_pyfunction!(py_url_decode, m)?)?;
    m.add("url_encode", wrap_pyfunction!(py_url_encode, m)?)?;
    m.add("shift_left", wrap_pyfunction!(py_shift_left, m)?)?;
    m.add("shift_right", wrap_pyfunction!(py_shift_right, m)?)?;
    m.add("shiftRight", wrap_pyfunction!(py_shift_right, m)?)?;
    m.add("shiftLeft", wrap_pyfunction!(py_shift_left, m)?)?;
    m.add("version", wrap_pyfunction!(py_version, m)?)?;
    m.add("equal_null", wrap_pyfunction!(py_equal_null, m)?)?;
    m.add(
        "json_array_length",
        wrap_pyfunction!(py_json_array_length, m)?,
    )?;
    m.add("parse_url", wrap_pyfunction!(py_parse_url, m)?)?;
    m.add("hash", wrap_pyfunction!(py_hash, m)?)?;
    m.add("stack", wrap_pyfunction!(py_stack, m)?)?;
    m.add("bit_and", wrap_pyfunction!(py_bit_and, m)?)?;
    m.add("bit_or", wrap_pyfunction!(py_bit_or, m)?)?;
    m.add("bit_xor", wrap_pyfunction!(py_bit_xor, m)?)?;
    m.add("bit_count", wrap_pyfunction!(py_bit_count, m)?)?;
    m.add("bitwise_not", wrap_pyfunction!(py_bitwise_not, m)?)?;
    m.add("bitwiseNOT", wrap_pyfunction!(py_bitwise_not, m)?)?;
    m.add("bit_get", wrap_pyfunction!(py_getbit, m)?)?;
    m.add("assert_true", wrap_pyfunction!(py_assert_true, m)?)?;
    m.add("raise_error", wrap_pyfunction!(py_raise_error, m)?)?;
    m.add(
        "spark_partition_id",
        wrap_pyfunction!(py_spark_partition_id, m)?,
    )?;
    m.add("input_file_name", wrap_pyfunction!(py_input_file_name, m)?)?;
    m.add(
        "monotonically_increasing_id",
        wrap_pyfunction!(py_monotonically_increasing_id, m)?,
    )?;
    m.add("current_catalog", wrap_pyfunction!(py_current_catalog, m)?)?;
    m.add(
        "current_database",
        wrap_pyfunction!(py_current_database, m)?,
    )?;
    m.add("current_schema", wrap_pyfunction!(py_current_schema, m)?)?;
    m.add("current_user", wrap_pyfunction!(py_current_user, m)?)?;
    m.add("user", wrap_pyfunction!(py_user, m)?)?;
    m.add("rand", wrap_pyfunction!(py_rand, m)?)?;
    m.add("randn", wrap_pyfunction!(py_randn, m)?)?;
    m.add("broadcast", wrap_pyfunction!(py_broadcast, m)?)?;
    m.add("_execute_plan", wrap_pyfunction!(py_execute_plan, m)?)?;
    m.add(
        "supported_plan_operations",
        wrap_pyfunction!(py_supported_plan_operations, m)?,
    )?;
    m.add(
        "_configure_for_multiprocessing",
        wrap_pyfunction!(py_configure_for_multiprocessing, m)?,
    )?;
    m.add("pandas_udf", wrap_pyfunction!(py_pandas_udf, m)?)?;
    Ok(())
}

/// Return a Column expression that references a column by name.
///
/// Use the result in ``DataFrame.filter()``, ``DataFrame.select()``,
/// ``DataFrame.with_column()``, ``order_by_exprs()``, and aggregation expressions.
///
/// Args:
///     col: Column name (string). Must match a column in the DataFrame schema.
///
/// Returns:
///     Column: Expression that can be combined with other columns or literals.
#[pyfunction]
fn py_col(col: &str) -> PyColumn {
    PyColumn { inner: rs_col(col) }
}

/// Call a registered UDF by name. PySpark: F.call_udf(udfName, *cols).
#[pyfunction]
#[pyo3(signature = (name, *cols))]
fn py_call_udf(
    _py: Python<'_>,
    name: &str,
    cols: &Bound<'_, pyo3::types::PyTuple>,
) -> PyResult<PyColumn> {
    if cols.len() < 1 {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "call_udf requires at least name and one column",
        ));
    }
    let rs_cols: Vec<RsColumn> = (0..cols.len())
        .map(|i| {
            let item = cols.get_item(i)?;
            if let Ok(py_col) = item.downcast::<PyColumn>() {
                Ok(py_col.borrow().inner.clone())
            } else if let Ok(s) = item.extract::<String>() {
                Ok(rs_col(&s))
            } else {
                Err(pyo3::exceptions::PyTypeError::new_err(
                    "call_udf: each arg must be Column or str (column name)",
                ))
            }
        })
        .collect::<PyResult<Vec<_>>>()?;
    let col = crate::functions::call_udf(name, &rs_cols)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyColumn { inner: col })
}

/// Create a literal Column from a Python scalar value.
///
/// Use for constant values in expressions (e.g. ``filter(col("x") > lit(10))``).
///
/// Args:
///     col: A single value: ``None``, ``int``, ``float``, ``bool``, ``str``,
///         ``datetime.date``, or ``datetime.datetime`` (PySpark parity, see #186).
///
/// Returns:
///     Column: Literal expression with the given value.
///
/// Raises:
///     TypeError: If the value is not a supported type.
#[pyfunction]
fn py_lit(value: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyColumn> {
    use polars::prelude::*;
    let inner = if value.is_none() {
        RsColumn::from_expr(lit(NULL), None)
    } else if let Ok(x) = value.extract::<i64>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = value.extract::<f64>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = value.extract::<bool>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = value.extract::<String>() {
        RsColumn::from_expr(polars::prelude::lit(x.as_str()), None)
    } else if let Ok(expr) = py_lit_date_or_datetime(value) {
        RsColumn::from_expr(expr, None)
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "lit() supports only None, int, float, bool, str, datetime.date, datetime.datetime",
        ));
    };
    Ok(PyColumn { inner })
}

/// Try to build a Date or DateTime literal from Python datetime.date / datetime.datetime.
/// We detect by attributes: values with year/month/day are supported; if they also have
/// hour/minute/second we treat as datetime, else as date (avoids is_instance across the pyo3 boundary).
fn py_lit_date_or_datetime(
    value: &Bound<'_, pyo3::types::PyAny>,
) -> PyResult<polars::prelude::Expr> {
    use chrono::NaiveDate;
    use polars::prelude::{Expr, LiteralValue, TimeUnit};

    let year: Option<i32> = value.getattr("year").ok().and_then(|a| a.extract().ok());
    let month: Option<u32> = value.getattr("month").ok().and_then(|a| a.extract().ok());
    let day: Option<u32> = value.getattr("day").ok().and_then(|a| a.extract().ok());
    let (year, month, day) =
        match (year, month, day) {
            (Some(y), Some(m), Some(d)) => (y, m, d),
            _ => return Err(pyo3::exceptions::PyTypeError::new_err(
                "lit() supports only None, int, float, bool, str, datetime.date, datetime.datetime",
            )),
        };

    // datetime.datetime has hour/minute/second; datetime.date does not
    let has_time = value.getattr("hour").is_ok()
        && value.getattr("minute").is_ok()
        && value.getattr("second").is_ok();

    if has_time {
        let hour: u32 = value.getattr("hour")?.extract()?;
        let minute: u32 = value.getattr("minute")?.extract()?;
        let second: u32 = value.getattr("second")?.extract()?;
        let micros: u32 = value
            .getattr("microsecond")
            .ok()
            .and_then(|a| a.extract().ok())
            .unwrap_or(0);
        let nd = NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("invalid date"))?;
        let ndt = nd
            .and_hms_micro_opt(hour, minute, second, micros)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("invalid datetime"))?;
        let us = ndt.and_utc().timestamp_micros();
        return Ok(Expr::Literal(LiteralValue::DateTime(
            us,
            TimeUnit::Microseconds,
            None,
        )));
    }

    let nd = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("invalid date"))?;
    let days = (nd - crate::date_utils::epoch_naive_date()).num_days() as i32;
    Ok(Expr::Literal(LiteralValue::Date(days)))
}

/// Conditional expression: when(condition) then value else default.
///
/// Two forms:
/// - ``when(condition).then(value).otherwise(default)`` for multiple when-then clauses, chain
///   additional ``.when(cond2).then(val2)`` before ``.otherwise(default)``.
/// - ``when(condition, value)`` returns a WhenThen; call ``.otherwise(default)`` to get a Column (PySpark parity).
///
/// Args:
///     condition: Boolean Column expression.
///     value: Optional. If given, result is value where condition is true, else null.
///
/// Returns:
///     WhenThen if value is provided (use ``.otherwise(default)`` to get Column); otherwise WhenBuilder to chain .then() and .otherwise().
#[pyfunction]
#[pyo3(signature = (condition, value=None))]
fn py_when(
    condition: &PyColumn,
    value: Option<PyRef<PyColumn>>,
    py: Python<'_>,
) -> PyResult<Py<PyAny>> {
    match value {
        Some(v) => Ok(PyWhenThen {
            condition: condition.inner.expr().clone(),
            then_value: v.inner.expr().clone(),
        }
        .into_py_any(py)?),
        None => Ok(PyWhenBuilder {
            condition: condition.inner.expr().clone(),
        }
        .into_py_any(py)?),
    }
}

/// Return a Column that takes the first non-null value across the given columns, per row.
///
/// Args:
///     cols: One or more Column expressions (variadic, PySpark parity). At least one required.
///
/// Returns:
///     Column: For each row, the first non-null value from cols in order.
#[pyfunction]
#[pyo3(signature = (*cols))]
fn py_coalesce(cols: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<PyColumn> {
    let columns: Vec<PyRef<PyColumn>> = (0..cols.len())
        .map(|i| cols.get_item(i).and_then(|ob| ob.extract()))
        .collect::<PyResult<Vec<_>>>()
        .map_err(|e| {
            pyo3::exceptions::PyTypeError::new_err(format!("coalesce() args must be Column: {e}"))
        })?;
    if columns.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "coalesce() requires at least one column",
        ));
    }
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    Ok(PyColumn {
        inner: coalesce(&refs),
    })
}

/// Concatenate string columns without separator (PySpark concat).
///
/// Args:
///     columns: One or more Column expressions.
///
/// Returns:
///     Column: String concatenation of all inputs.
#[pyfunction]
#[pyo3(signature = (*columns))]
fn py_concat(columns: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    if refs.is_empty() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "concat() requires at least one column",
        ));
    }
    Ok(PyColumn {
        inner: rs_concat(&refs),
    })
}

/// Concatenate string columns with separator (PySpark concat_ws).
///
/// Args:
///     sep: Separator string.
///     columns: One or more Column expressions.
///
/// Returns:
///     Column: String concatenation of all inputs with separator between values.
#[pyfunction]
#[pyo3(signature = (sep, *columns))]
fn py_concat_ws(sep: &str, columns: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    if refs.is_empty() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "concat_ws() requires at least one column",
        ));
    }
    Ok(PyColumn {
        inner: rs_concat_ws(sep, &refs),
    })
}

/// Row number window function for use with ``Window.partitionBy(...).orderBy(...)``.
///
/// Usage (PySpark parity-style):
///
///     from robin_sparkless import Window, row_number, col
///     win = Window.partitionBy("dept").orderBy(col("salary"))
///     df = df.with_column("rn", row_number().over(win))
///
/// For more control over ordering direction, use column methods
/// (e.g. ``col("salary").row_number(descending=True).over(["dept"])``).
#[pyfunction]
#[pyo3(signature = (descending=false))]
fn py_row_number(descending: bool) -> PyRowNumber {
    PyRowNumber { descending }
}

/// Dense rank window function. Use with ``dense_rank().over(win)`` (PySpark parity #320).
#[pyfunction]
#[pyo3(signature = (descending=false))]
fn py_dense_rank(descending: bool) -> PyDenseRank {
    PyDenseRank { descending }
}

/// Lag: value from n rows before in partition. Use with ``lag(col("v"), 1).over(["dept"])`` (PySpark parity #319).
#[pyfunction]
#[pyo3(signature = (column, offset=1))]
fn py_lag(column: &PyColumn, offset: i64) -> PyColumn {
    PyColumn {
        inner: column.inner.lag(offset),
    }
}

/// Lead: value from n rows after in partition. Use with ``lead(col("v"), 1).over(["dept"])`` (PySpark parity #319).
#[pyfunction]
#[pyo3(signature = (column, offset=1))]
fn py_lead(column: &PyColumn, offset: i64) -> PyColumn {
    PyColumn {
        inner: column.inner.lead(offset),
    }
}

/// Sum aggregation over a column.
///
/// Use in ``GroupedData.agg()`` (e.g. ``df.group_by("id").agg(sum(col("amount")))``)
/// or in ``select()`` with no grouping.
///
/// Args:
///     col: Numeric Column to sum.
///
/// Returns:
///     Column: Aggregation expression.
#[pyfunction]
fn py_sum(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rs_sum(&col.inner),
    }
}

/// Average (mean) aggregation over a column.
///
/// Use in ``GroupedData.agg()`` or in ``select()``. Ignores nulls.
///
/// Args:
///     col: Numeric Column to average.
///
/// Returns:
///     Column: Aggregation expression.
#[pyfunction]
fn py_avg(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: avg(&col.inner),
    }
}

/// Minimum aggregation over a column.
///
/// Use in ``GroupedData.agg()`` or ``select()``.
///
/// Args:
///     col: Column to take minimum over (numeric, string, or comparable type).
///
/// Returns:
///     Column: Aggregation expression.
#[pyfunction]
fn py_min(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: min(&col.inner),
    }
}

/// Maximum aggregation over a column.
///
/// Use in ``GroupedData.agg()`` or ``select()``.
///
/// Args:
///     col: Column to take maximum over (numeric, string, or comparable type).
///
/// Returns:
///     Column: Aggregation expression.
#[pyfunction]
fn py_max(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: max(&col.inner),
    }
}

#[pyfunction]
fn py_max_by(value_col: &PyColumn, ord_col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: max_by(&value_col.inner, &ord_col.inner),
    }
}

#[pyfunction]
fn py_min_by(value_col: &PyColumn, ord_col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: min_by(&value_col.inner, &ord_col.inner),
    }
}

#[pyfunction]
fn py_try_sum(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: try_sum(&col.inner),
    }
}

#[pyfunction]
fn py_try_avg(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: try_avg(&col.inner),
    }
}

#[pyfunction]
fn py_kurtosis(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: kurtosis(&col.inner),
    }
}

#[pyfunction]
fn py_skewness(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: skewness(&col.inner),
    }
}

/// Count aggregation: number of non-null values in a column.
///
/// Use in ``GroupedData.agg()`` (e.g. ``count(col("id"))``) or ``select()``.
/// Use ``count(lit(1))`` or similar for row count when all rows matter.
///
/// Args:
///     col: Column to count (nulls excluded).
///
/// Returns:
///     Column: Aggregation expression.
#[pyfunction]
fn py_count(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: count(&col.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col, rsd=None))]
fn py_approx_count_distinct(
    col: &Bound<'_, pyo3::types::PyAny>,
    rsd: Option<f64>,
) -> PyResult<PyColumn> {
    let col_column: RsColumn = if let Ok(pycol) = col.downcast::<PyColumn>() {
        pycol.borrow().inner.clone()
    } else if let Ok(name) = col.extract::<String>() {
        rs_col(&name)
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "approx_count_distinct(col, rsd=None): col must be Column or column name (str)",
        ));
    };
    Ok(PyColumn {
        inner: approx_count_distinct(&col_column, rsd),
    })
}

#[pyfunction]
#[pyo3(signature = (col, percentage, accuracy=None))]
fn py_approx_percentile(col: &PyColumn, percentage: f64, accuracy: Option<i32>) -> PyColumn {
    PyColumn {
        inner: approx_percentile(&col.inner, percentage, accuracy),
    }
}

#[pyfunction]
fn py_bool_and(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: bool_and(&col.inner),
    }
}

#[pyfunction]
fn py_collect_list(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: collect_list(&col.inner),
    }
}

#[pyfunction]
fn py_every(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: every(&col.inner),
    }
}

#[pyfunction]
fn py_collect_set(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: collect_set(&col.inner),
    }
}

#[pyfunction]
fn py_corr(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: corr(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
fn py_count_if(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: count_if(&col.inner),
    }
}

#[pyfunction]
fn py_covar_pop(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: covar_pop(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col, ignorenulls=true))]
fn py_first(col: &PyColumn, ignorenulls: bool) -> PyColumn {
    PyColumn {
        inner: first(&col.inner, ignorenulls),
    }
}

#[pyfunction]
#[pyo3(signature = (col, ignorenulls=true))]
fn py_any_value(col: &PyColumn, ignorenulls: bool) -> PyColumn {
    PyColumn {
        inner: any_value(&col.inner, ignorenulls),
    }
}

/// Ascending sort order for use in ``order_by_exprs()`` (nulls last by default).
///
/// Args:
///     col: Column to sort by.
///
/// Returns:
///     SortOrder to pass to ``DataFrame.order_by_exprs([...])``.
#[pyfunction]
fn py_asc(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: asc(&col.inner),
    }
}

/// Ascending sort order with nulls first.
#[pyfunction]
fn py_asc_nulls_first(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: asc_nulls_first(&col.inner),
    }
}

/// Ascending sort order with nulls last.
#[pyfunction]
fn py_asc_nulls_last(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: asc_nulls_last(&col.inner),
    }
}

/// Descending sort order for use in ``order_by_exprs()`` (nulls first by default).
///
/// Args:
///     col: Column to sort by.
///
/// Returns:
///     SortOrder to pass to ``DataFrame.order_by_exprs([...])``.
#[pyfunction]
fn py_desc(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: desc(&col.inner),
    }
}

/// Descending sort order with nulls first.
#[pyfunction]
fn py_desc_nulls_first(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: desc_nulls_first(&col.inner),
    }
}

/// Descending sort order with nulls last.
#[pyfunction]
fn py_desc_nulls_last(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: desc_nulls_last(&col.inner),
    }
}

/// Banker's rounding: round to ``scale`` decimal places (half rounds to nearest even).
///
/// Args:
///     col: Numeric Column.
///     scale: Number of decimal places (can be negative for rounding to tens, hundreds, etc.).
///
/// Returns:
///     Column: Rounded values.
#[pyfunction]
#[pyo3(signature = (col, scale=0))]
fn py_bround(col: &PyColumn, scale: i32) -> PyColumn {
    PyColumn {
        inner: bround(&col.inner, scale),
    }
}

/// Unary minus: negate the column values.
///
/// Args:
///     col: Numeric Column.
///
/// Returns:
///     Column: Negated values.
#[pyfunction]
fn py_negate(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: negate(&col.inner),
    }
}

/// Unary plus; no-op for API compatibility with PySpark.
///
/// Args:
///     col: Column.
///
/// Returns:
///     Column: Same as input.
#[pyfunction]
fn py_positive(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: positive(&col.inner),
    }
}

#[pyfunction]
fn py_cot(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: cot(&col.inner),
    }
}

#[pyfunction]
fn py_csc(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: csc(&col.inner),
    }
}

#[pyfunction]
fn py_sec(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: sec(&col.inner),
    }
}

#[pyfunction]
fn py_e() -> PyColumn {
    PyColumn { inner: e() }
}

#[pyfunction]
fn py_pi() -> PyColumn {
    PyColumn { inner: pi() }
}

#[pyfunction]
fn py_median(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: median(&col.inner),
    }
}

#[pyfunction]
fn py_mode(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: mode(&col.inner),
    }
}

#[pyfunction]
fn py_stddev(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: stddev(&col.inner),
    }
}

#[pyfunction]
fn py_stddev_pop(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: stddev_pop(&col.inner),
    }
}

#[pyfunction]
fn py_var_pop(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: var_pop(&col.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (str, trim=None))]
fn py_btrim(str: &PyColumn, trim: Option<&str>) -> PyColumn {
    PyColumn {
        inner: btrim(&str.inner, trim),
    }
}

#[pyfunction]
#[pyo3(signature = (substr, str, pos=1))]
fn py_locate(substr: &str, str: &PyColumn, pos: Option<i64>) -> PyColumn {
    PyColumn {
        inner: locate(substr, &str.inner, pos.unwrap_or(1)),
    }
}

#[pyfunction]
#[pyo3(signature = (col, fromBase, toBase))]
fn py_conv(col: &PyColumn, fromBase: i32, toBase: i32) -> PyColumn {
    PyColumn {
        inner: conv(&col.inner, fromBase, toBase),
    }
}

#[pyfunction]
fn py_hex(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: hex(&col.inner),
    }
}

#[pyfunction]
fn py_unhex(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: unhex(&col.inner),
    }
}

#[pyfunction]
fn py_bin(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: bin(&col.inner),
    }
}

#[pyfunction]
fn py_getbit(col: &PyColumn, pos: i64) -> PyColumn {
    PyColumn {
        inner: getbit(&col.inner, pos),
    }
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_to_char(col: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    to_char(&col.inner, format)
        .map(|c| PyColumn { inner: c })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_to_varchar(col: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    to_varchar(&col.inner, format)
        .map(|c| PyColumn { inner: c })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_to_number(col: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    to_number(&col.inner, format)
        .map(|c| PyColumn { inner: c })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_try_to_number(col: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    try_to_number(&col.inner, format)
        .map(|c| PyColumn { inner: c })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_try_to_timestamp(col: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    try_to_timestamp(&col.inner, format)
        .map(|c| PyColumn { inner: c })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
#[pyo3(signature = (text, pair_delim=None, key_value_delim=None))]
fn py_str_to_map(
    text: &PyColumn,
    pair_delim: Option<&str>,
    key_value_delim: Option<&str>,
) -> PyColumn {
    PyColumn {
        inner: str_to_map(&text.inner, pair_delim, key_value_delim),
    }
}

#[pyfunction]
fn py_arrays_overlap(a1: &PyColumn, a2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: arrays_overlap(&a1.inner, &a2.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col1, col2))]
fn py_arrays_zip(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: arrays_zip(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
fn py_explode(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: explode(&col.inner),
    }
}

#[pyfunction]
fn py_explode_outer(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: explode_outer(&col.inner),
    }
}

/// Result of posexplode(col); supports .alias("pos", "val") for select (fixes #411).
#[pyclass(name = "PosexplodeResult")]
pub struct PyPosexplodeResult {
    pos: PyColumn,
    val: PyColumn,
}

#[pymethods]
impl PyPosexplodeResult {
    /// Return (pos_column, val_column) with the given names for use in select().
    fn alias(&self, pos_name: &str, val_name: &str) -> (PyColumn, PyColumn) {
        (
            PyColumn {
                inner: self.pos.inner.alias(pos_name),
            },
            PyColumn {
                inner: self.val.inner.alias(val_name),
            },
        )
    }

    /// Unpack as pos_col, val_col = posexplode(...) (backward compatibility).
    fn __iter__(slf: PyRef<'_, Self>) -> PyResult<PyObject> {
        let py = slf.py();
        let tup = pyo3::types::PyTuple::new(
            py,
            [
                PyColumn {
                    inner: slf.pos.inner.clone(),
                }
                .into_py(py),
                PyColumn {
                    inner: slf.val.inner.clone(),
                }
                .into_py(py),
            ],
        );
        Ok(tup?.call_method0("__iter__")?.into_py(py))
    }
}

#[pyfunction]
fn py_posexplode(col: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyPosexplodeResult> {
    let col_column: RsColumn = if let Ok(pycol) = col.downcast::<PyColumn>() {
        pycol.borrow().inner.clone()
    } else if let Ok(name) = col.extract::<String>() {
        rs_col(&name)
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "posexplode() argument must be Column or column name (str)",
        ));
    };
    let (pos, val) = posexplode(&col_column);
    Ok(PyPosexplodeResult {
        pos: PyColumn { inner: pos },
        val: PyColumn { inner: val },
    })
}

#[pyfunction]
fn py_inline(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rs_inline(&col.inner),
    }
}

#[pyfunction]
fn py_inline_outer(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rs_inline_outer(&col.inner),
    }
}

#[pyfunction]
fn py_sequence(start: &PyColumn, stop: &PyColumn, step: Option<&PyColumn>) -> PyColumn {
    PyColumn {
        inner: sequence(&start.inner, &stop.inner, step.map(|c| &c.inner)),
    }
}

#[pyfunction]
fn py_shuffle(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: shuffle(&col.inner),
    }
}

#[pyfunction]
fn py_array_agg(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_agg(&col.inner),
    }
}

#[pyfunction]
fn py_curdate() -> PyColumn {
    PyColumn { inner: curdate() }
}

#[pyfunction]
fn py_now() -> PyColumn {
    PyColumn { inner: now() }
}

#[pyfunction]
fn py_localtimestamp() -> PyColumn {
    PyColumn {
        inner: localtimestamp(),
    }
}

#[pyfunction]
fn py_date_diff(end: &PyColumn, start: &PyColumn) -> PyColumn {
    PyColumn {
        inner: date_diff(&end.inner, &start.inner),
    }
}

#[pyfunction]
fn py_dateadd(start: &PyColumn, days: i32) -> PyColumn {
    PyColumn {
        inner: dateadd(&start.inner, days),
    }
}

#[pyfunction]
fn py_datepart(field: &str, source: &PyColumn) -> PyColumn {
    PyColumn {
        inner: datepart(&source.inner, field),
    }
}

#[pyfunction]
fn py_extract(field: &str, source: &PyColumn) -> PyColumn {
    PyColumn {
        inner: extract(&source.inner, field),
    }
}

#[pyfunction]
fn py_date_part(field: &str, source: &PyColumn) -> PyColumn {
    PyColumn {
        inner: date_part(&source.inner, field),
    }
}

#[pyfunction]
fn py_unix_micros(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: unix_micros(&col.inner),
    }
}

#[pyfunction]
fn py_unix_millis(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: unix_millis(&col.inner),
    }
}

#[pyfunction]
fn py_unix_seconds(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: unix_seconds(&col.inner),
    }
}

#[pyfunction]
fn py_dayname(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: dayname(&col.inner),
    }
}

#[pyfunction]
fn py_weekday(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: weekday(&col.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (years, months, days, hours, mins, secs, timezone=None))]
fn py_make_timestamp(
    years: &PyColumn,
    months: &PyColumn,
    days: &PyColumn,
    hours: &PyColumn,
    mins: &PyColumn,
    secs: &PyColumn,
    timezone: Option<&str>,
) -> PyColumn {
    PyColumn {
        inner: make_timestamp(
            &years.inner,
            &months.inner,
            &days.inner,
            &hours.inner,
            &mins.inner,
            &secs.inner,
            timezone,
        ),
    }
}

#[pyfunction]
#[pyo3(signature = (years, months, days, hours, mins, secs))]
fn py_make_timestamp_ntz(
    years: &PyColumn,
    months: &PyColumn,
    days: &PyColumn,
    hours: &PyColumn,
    mins: &PyColumn,
    secs: &PyColumn,
) -> PyColumn {
    PyColumn {
        inner: make_timestamp_ntz(
            &years.inner,
            &months.inner,
            &days.inner,
            &hours.inner,
            &mins.inner,
            &secs.inner,
        ),
    }
}

#[pyfunction]
fn py_make_interval(
    years: i64,
    months: i64,
    weeks: i64,
    days: i64,
    hours: i64,
    mins: i64,
    secs: i64,
) -> PyColumn {
    PyColumn {
        inner: make_interval(years, months, weeks, days, hours, mins, secs),
    }
}

#[pyfunction]
fn py_timestampadd(unit: &str, amount: &PyColumn, ts: &PyColumn) -> PyColumn {
    PyColumn {
        inner: timestampadd(unit, &amount.inner, &ts.inner),
    }
}

#[pyfunction]
fn py_timestampdiff(unit: &str, start: &PyColumn, end: &PyColumn) -> PyColumn {
    PyColumn {
        inner: timestampdiff(unit, &start.inner, &end.inner),
    }
}

#[pyfunction]
fn py_days(col: i64) -> PyColumn {
    PyColumn { inner: days(col) }
}

#[pyfunction]
fn py_hours(col: i64) -> PyColumn {
    PyColumn { inner: hours(col) }
}

#[pyfunction]
fn py_minutes(n: i64) -> PyColumn {
    PyColumn { inner: minutes(n) }
}

#[pyfunction]
fn py_months(col: i64) -> PyColumn {
    PyColumn { inner: months(col) }
}

#[pyfunction]
fn py_years(col: i64) -> PyColumn {
    PyColumn { inner: years(col) }
}

#[pyfunction]
fn py_from_utc_timestamp(timestamp: &PyColumn, tz: &str) -> PyColumn {
    PyColumn {
        inner: from_utc_timestamp(&timestamp.inner, tz),
    }
}

#[pyfunction]
fn py_to_utc_timestamp(timestamp: &PyColumn, tz: &str) -> PyColumn {
    PyColumn {
        inner: to_utc_timestamp(&timestamp.inner, tz),
    }
}

#[pyfunction]
#[pyo3(signature = (sourceTz, targetTz, sourceTs))]
fn py_convert_timezone(sourceTz: &str, targetTz: &str, sourceTs: &PyColumn) -> PyColumn {
    PyColumn {
        inner: convert_timezone(sourceTz, targetTz, &sourceTs.inner),
    }
}

#[pyfunction]
fn py_current_timezone() -> PyColumn {
    PyColumn {
        inner: current_timezone(),
    }
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_to_timestamp(col: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    to_timestamp(&col.inner, format)
        .map(|c| PyColumn { inner: c })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_to_date(col: &PyColumn, format: Option<&str>) -> PyResult<PyColumn> {
    to_date(&col.inner, format)
        .map(|c| PyColumn { inner: c })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
fn py_isin(col: &PyColumn, other: &PyColumn) -> PyColumn {
    PyColumn {
        inner: isin(&col.inner, &other.inner),
    }
}

#[pyfunction]
fn py_isin_i64(col: &PyColumn, values: Vec<i64>) -> PyColumn {
    PyColumn {
        inner: isin_i64(&col.inner, &values),
    }
}

#[pyfunction]
fn py_isin_str(col: &PyColumn, values: Vec<String>) -> PyColumn {
    let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    PyColumn {
        inner: isin_str(&col.inner, &refs),
    }
}

#[pyfunction]
fn py_url_decode(str: &PyColumn) -> PyColumn {
    PyColumn {
        inner: url_decode(&str.inner),
    }
}

#[pyfunction]
fn py_url_encode(str: &PyColumn) -> PyColumn {
    PyColumn {
        inner: url_encode(&str.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col, numBits))]
fn py_shift_left(col: &PyColumn, numBits: i32) -> PyColumn {
    PyColumn {
        inner: shift_left(&col.inner, numBits),
    }
}

#[pyfunction]
#[pyo3(signature = (col, numBits))]
fn py_shift_right(col: &PyColumn, numBits: i32) -> PyColumn {
    PyColumn {
        inner: shift_right(&col.inner, numBits),
    }
}

#[pyfunction]
#[pyo3(signature = (col1, col2))]
fn py_bit_and(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: bit_and(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col1, col2))]
fn py_bit_or(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: bit_or(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col1, col2))]
fn py_bit_xor(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: bit_xor(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
fn py_bit_count(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: bit_count(&col.inner),
    }
}

#[pyfunction]
fn py_bitwise_not(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: bitwise_not(&col.inner),
    }
}

#[pyfunction]
fn py_version() -> PyColumn {
    PyColumn { inner: version() }
}

#[pyfunction]
#[pyo3(signature = (col, errMsg=None))]
fn py_assert_true(col: &PyColumn, errMsg: Option<&str>) -> PyColumn {
    PyColumn {
        inner: rs_assert_true(&col.inner, errMsg),
    }
}

#[pyfunction]
#[pyo3(signature = (errMsg))]
fn py_raise_error(errMsg: &str) -> PyColumn {
    PyColumn {
        inner: crate::functions::raise_error(errMsg),
    }
}

#[pyfunction]
fn py_spark_partition_id() -> PyColumn {
    PyColumn {
        inner: rs_spark_partition_id(),
    }
}

#[pyfunction]
fn py_input_file_name() -> PyColumn {
    PyColumn {
        inner: rs_input_file_name(),
    }
}

#[pyfunction]
fn py_monotonically_increasing_id() -> PyColumn {
    PyColumn {
        inner: rs_monotonically_increasing_id(),
    }
}

#[pyfunction]
fn py_current_catalog() -> PyColumn {
    PyColumn {
        inner: rs_current_catalog(),
    }
}

#[pyfunction]
fn py_current_database() -> PyColumn {
    PyColumn {
        inner: rs_current_database(),
    }
}

#[pyfunction]
fn py_current_schema() -> PyColumn {
    PyColumn {
        inner: rs_current_schema(),
    }
}

#[pyfunction]
fn py_current_user() -> PyColumn {
    PyColumn {
        inner: rs_current_user(),
    }
}

#[pyfunction]
fn py_user() -> PyColumn {
    PyColumn { inner: rs_user() }
}

#[pyfunction]
fn py_rand(seed: Option<u64>) -> PyColumn {
    PyColumn {
        inner: rs_rand(seed),
    }
}

#[pyfunction]
fn py_randn(seed: Option<u64>) -> PyColumn {
    PyColumn {
        inner: rs_randn(seed),
    }
}

#[pyfunction]
fn py_broadcast(df: &PyDataFrame) -> PyDataFrame {
    PyDataFrame {
        inner: rs_broadcast(&df.inner),
    }
}

#[pyfunction]
fn py_equal_null(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: equal_null(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col, path=None))]
fn py_json_array_length(col: &PyColumn, path: Option<&str>) -> PyColumn {
    PyColumn {
        inner: json_array_length(&col.inner, path.unwrap_or("")),
    }
}

#[pyfunction]
#[pyo3(signature = (url, partToExtract, key=None))]
fn py_parse_url(url: &PyColumn, partToExtract: &str, key: Option<&str>) -> PyColumn {
    PyColumn {
        inner: parse_url(&url.inner, partToExtract, key),
    }
}

#[pyfunction]
fn py_hash(cols: Vec<PyRef<PyColumn>>) -> PyColumn {
    let rs_refs: Vec<&crate::column::Column> = cols.iter().map(|c| &c.inner).collect();
    PyColumn {
        inner: hash(&rs_refs),
    }
}

#[pyfunction]
fn py_stack(cols: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    if cols.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "stack() requires at least one column",
        ));
    }
    let rs_refs: Vec<&crate::column::Column> = cols.iter().map(|c| &c.inner).collect();
    Ok(PyColumn {
        inner: struct_(&rs_refs),
    })
}

#[pyfunction]
fn py_ascii(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ascii(&col.inner),
    }
}

#[pyfunction]
fn py_soundex(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: soundex(&col.inner),
    }
}

#[pyfunction]
fn py_levenshtein(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: levenshtein(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
fn py_crc32(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: crc32(&col.inner),
    }
}

#[pyfunction]
fn py_xxhash64(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: xxhash64(&col.inner),
    }
}

#[pyfunction]
fn py_format_number(col: &PyColumn, d: u32) -> PyColumn {
    PyColumn {
        inner: format_number(&col.inner, d),
    }
}

#[pyfunction]
#[pyo3(signature = (src, replace, pos, len=-1))]
fn py_overlay(src: &PyColumn, replace: &str, pos: i64, len: i64) -> PyColumn {
    PyColumn {
        inner: overlay(&src.inner, replace, pos, len),
    }
}

#[pyfunction]
#[pyo3(signature = (substr, str, start=None))]
fn py_position(substr: &str, str: &PyColumn, start: Option<i64>) -> PyColumn {
    let pos = start.unwrap_or(1);
    PyColumn {
        inner: locate(substr, &str.inner, pos),
    }
}

#[pyfunction]
fn py_char(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: crate::functions::char(&col.inner),
    }
}

#[pyfunction]
fn py_chr(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: chr(&col.inner),
    }
}

#[pyfunction]
fn py_base64(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: base64(&col.inner),
    }
}

#[pyfunction]
fn py_unbase64(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: unbase64(&col.inner),
    }
}

#[pyfunction]
fn py_sha1(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: sha1(&col.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col, numBits))]
fn py_sha2(col: &PyColumn, numBits: i32) -> PyColumn {
    PyColumn {
        inner: sha2(&col.inner, numBits),
    }
}

#[pyfunction]
fn py_md5(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: md5(&col.inner),
    }
}

#[pyfunction]
fn py_array_compact(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_compact(&col.inner),
    }
}

#[pyfunction]
fn py_array_distinct(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_distinct(&col.inner),
    }
}

#[pyfunction]
fn py_sin(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: sin(&col.inner),
    }
}
#[pyfunction]
fn py_cos(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: cos(&col.inner),
    }
}
#[pyfunction]
fn py_tan(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: tan(&col.inner),
    }
}
#[pyfunction]
fn py_asin(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: asin(&col.inner),
    }
}
#[pyfunction]
fn py_acos(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: acos(&col.inner),
    }
}
#[pyfunction]
fn py_atan(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: atan(&col.inner),
    }
}
#[pyfunction]
fn py_atan2(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: atan2(&col1.inner, &col2.inner),
    }
}
#[pyfunction]
fn py_degrees(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: degrees(&col.inner),
    }
}
#[pyfunction]
fn py_radians(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: radians(&col.inner),
    }
}
#[pyfunction]
fn py_signum(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: signum(&col.inner),
    }
}
#[pyfunction]
fn py_quarter(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: quarter(&col.inner),
    }
}
#[pyfunction]
fn py_weekofyear(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: weekofyear(&col.inner),
    }
}
#[pyfunction]
fn py_dayofweek(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: dayofweek(&col.inner),
    }
}
#[pyfunction]
fn py_dayofyear(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: dayofyear(&col.inner),
    }
}
#[pyfunction]
fn py_add_months(start: &PyColumn, months: i32) -> PyColumn {
    PyColumn {
        inner: add_months(&start.inner, months),
    }
}
#[pyfunction]
#[pyo3(signature = (date1, date2, roundOff=true))]
fn py_months_between(date1: &PyColumn, date2: &PyColumn, roundOff: Option<bool>) -> PyColumn {
    let round_off = roundOff.unwrap_or(true);
    PyColumn {
        inner: months_between(&date1.inner, &date2.inner, round_off),
    }
}
#[pyfunction]
#[pyo3(signature = (date, dayOfWeek))]
fn py_next_day(date: &PyColumn, dayOfWeek: &str) -> PyColumn {
    PyColumn {
        inner: next_day(&date.inner, dayOfWeek),
    }
}
#[pyfunction]
fn py_cast(col: &PyColumn, type_name: &pyo3::Bound<'_, pyo3::types::PyAny>) -> PyResult<PyColumn> {
    let s = column::py_any_to_cast_type_name(type_name)?;
    rs_cast(&col.inner, &s)
        .map(|inner| PyColumn { inner })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}
#[pyfunction]
fn py_try_cast(
    col: &PyColumn,
    type_name: &pyo3::Bound<'_, pyo3::types::PyAny>,
) -> PyResult<PyColumn> {
    let s = column::py_any_to_cast_type_name(type_name)?;
    rs_try_cast(&col.inner, &s)
        .map(|inner| PyColumn { inner })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}
#[pyfunction]
fn py_isnan(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rs_isnan(&col.inner),
    }
}
#[pyfunction]
#[pyo3(signature = (*cols))]
fn py_greatest(cols: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<PyColumn> {
    let columns: Vec<PyRef<PyColumn>> = (0..cols.len())
        .map(|i| cols.get_item(i).and_then(|ob| ob.extract()))
        .collect::<PyResult<Vec<_>>>()
        .map_err(|e| {
            pyo3::exceptions::PyTypeError::new_err(format!("greatest() args must be Column: {e}"))
        })?;
    if columns.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "greatest() requires at least one column",
        ));
    }
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    rs_greatest(&refs)
        .map(|inner| PyColumn { inner })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}
#[pyfunction]
#[pyo3(signature = (*cols))]
fn py_least(cols: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<PyColumn> {
    let columns: Vec<PyRef<PyColumn>> = (0..cols.len())
        .map(|i| cols.get_item(i).and_then(|ob| ob.extract()))
        .collect::<PyResult<Vec<_>>>()
        .map_err(|e| {
            pyo3::exceptions::PyTypeError::new_err(format!("least() args must be Column: {e}"))
        })?;
    if columns.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "least() requires at least one column",
        ));
    }
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    rs_least(&refs)
        .map(|inner| PyColumn { inner })
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

#[pyfunction]
fn py_nvl(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: nvl(&col1.inner, &col2.inner),
    }
}
#[pyfunction]
fn py_ifnull(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ifnull(&col1.inner, &col2.inner),
    }
}
#[pyfunction]
fn py_nvl2(col1: &PyColumn, col2: &PyColumn, col3: &PyColumn) -> PyColumn {
    PyColumn {
        inner: nvl2(&col1.inner, &col2.inner, &col3.inner),
    }
}
#[pyfunction]
fn py_substr(str: &PyColumn, pos: i64, len: Option<i64>) -> PyColumn {
    PyColumn {
        inner: substr(&str.inner, pos, len),
    }
}
#[pyfunction]
fn py_power(col1: &PyColumn, col2: i64) -> PyColumn {
    PyColumn {
        inner: power(&col1.inner, col2),
    }
}
#[pyfunction]
fn py_ln(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ln(&col.inner),
    }
}
#[pyfunction]
#[pyo3(signature = (col, base=None))]
fn py_log(col: &PyColumn, base: Option<f64>) -> PyColumn {
    PyColumn {
        inner: match base {
            None => log(&col.inner),
            Some(b) => log_with_base(&col.inner, b),
        },
    }
}
#[pyfunction]
fn py_ceiling(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ceiling(&col.inner),
    }
}

#[pyfunction]
fn py_floor(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: floor(&col.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col, scale=0))]
fn py_round(col: &PyColumn, scale: u32) -> PyColumn {
    PyColumn {
        inner: round(&col.inner, scale),
    }
}

#[pyfunction]
fn py_exp(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: exp(&col.inner),
    }
}

#[pyfunction]
fn py_abs(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: abs(&col.inner),
    }
}

#[pyfunction]
fn py_date_add(col: &PyColumn, days: i32) -> PyColumn {
    PyColumn {
        inner: date_add(&col.inner, days),
    }
}

#[pyfunction]
fn py_date_sub(col: &PyColumn, days: i32) -> PyColumn {
    PyColumn {
        inner: date_sub(&col.inner, days),
    }
}

#[pyfunction]
fn py_date_format(col: &PyColumn, format: &str) -> PyColumn {
    PyColumn {
        inner: date_format(&col.inner, format),
    }
}

#[pyfunction]
fn py_current_date() -> PyColumn {
    PyColumn {
        inner: current_date(),
    }
}

#[pyfunction]
fn py_current_timestamp() -> PyColumn {
    PyColumn {
        inner: current_timestamp(),
    }
}

#[pyfunction]
fn py_char_length(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: char_length(&col.inner),
    }
}

#[pyfunction]
fn py_character_length(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: character_length(&col.inner),
    }
}

#[pyfunction]
fn py_date_trunc(format: &str, col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: date_trunc(format, &col.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (*cols))]
fn py_array(cols: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    let refs: Vec<&RsColumn> = cols.iter().map(|c| &c.inner).collect();
    array(&refs)
        .map(|inner| PyColumn { inner })
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

#[pyfunction]
fn py_array_contains(col: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_contains(&col.inner, &value.inner),
    }
}

#[pyfunction]
fn py_array_max(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_max(&col.inner),
    }
}

#[pyfunction]
fn py_array_min(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_min(&col.inner),
    }
}

#[pyfunction]
fn py_array_position(col: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_position(&col.inner, &value.inner),
    }
}

#[pyfunction]
fn py_array_size(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_size(&col.inner),
    }
}

#[pyfunction]
fn py_size(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: size(&col.inner),
    }
}

#[pyfunction]
fn py_array_join(col: &PyColumn, delimiter: &str) -> PyColumn {
    PyColumn {
        inner: array_join(&col.inner, delimiter),
    }
}

#[pyfunction]
fn py_array_sort(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_sort(&col.inner),
    }
}

#[pyfunction]
fn py_cardinality(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: cardinality(&col.inner),
    }
}

#[pyfunction]
fn py_aggregate(col: &PyColumn, zero: &PyColumn) -> PyColumn {
    PyColumn {
        inner: aggregate(&col.inner, &zero.inner),
    }
}

#[pyfunction]
fn py_lcase(str: &PyColumn) -> PyColumn {
    PyColumn {
        inner: lcase(&str.inner),
    }
}
#[pyfunction]
fn py_ucase(str: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ucase(&str.inner),
    }
}
#[pyfunction]
fn py_day(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: day(&col.inner),
    }
}
#[pyfunction]
fn py_dayofmonth(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: dayofmonth(&col.inner),
    }
}
#[pyfunction]
fn py_year(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: year(&col.inner),
    }
}
#[pyfunction]
fn py_month(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: month(&col.inner),
    }
}
#[pyfunction]
fn py_hour(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: hour(&col.inner),
    }
}
#[pyfunction]
fn py_minute(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: minute(&col.inner),
    }
}
#[pyfunction]
fn py_second(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: second(&col.inner),
    }
}
#[pyfunction]
fn py_last_day(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: last_day(&col.inner),
    }
}
#[pyfunction]
fn py_nullif(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: nullif(&col1.inner, &col2.inner),
    }
}
#[pyfunction]
fn py_to_degrees(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: to_degrees(&col.inner),
    }
}
#[pyfunction]
fn py_to_radians(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: to_radians(&col.inner),
    }
}
#[pyfunction]
fn py_isnull(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: isnull(&col.inner),
    }
}
#[pyfunction]
fn py_isnotnull(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: isnotnull(&col.inner),
    }
}

#[pyfunction]
fn py_left(str: &PyColumn, len: i64) -> PyColumn {
    PyColumn {
        inner: left(&str.inner, len),
    }
}
#[pyfunction]
fn py_right(str: &PyColumn, len: i64) -> PyColumn {
    PyColumn {
        inner: right(&str.inner, len),
    }
}
#[pyfunction]
fn py_length(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: length(&col.inner),
    }
}
#[pyfunction]
fn py_trim(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: trim(&col.inner),
    }
}
#[pyfunction]
fn py_ltrim(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ltrim(&col.inner),
    }
}
#[pyfunction]
fn py_rtrim(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rtrim(&col.inner),
    }
}
#[pyfunction]
fn py_repeat(col: &PyColumn, n: i32) -> PyColumn {
    PyColumn {
        inner: repeat(&col.inner, n),
    }
}
#[pyfunction]
fn py_reverse(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: reverse(&col.inner),
    }
}
#[pyfunction]
fn py_initcap(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: initcap(&col.inner),
    }
}
#[pyfunction]
fn py_replace(src: &PyColumn, search: &str, replace: &str) -> PyColumn {
    PyColumn {
        inner: rs_replace(&src.inner, search, replace),
    }
}
#[pyfunction]
fn py_startswith(str: &PyColumn, prefix: &str) -> PyColumn {
    PyColumn {
        inner: startswith(&str.inner, prefix),
    }
}
#[pyfunction]
fn py_endswith(str: &PyColumn, suffix: &str) -> PyColumn {
    PyColumn {
        inner: endswith(&str.inner, suffix),
    }
}
#[pyfunction]
fn py_contains(left: &PyColumn, right: &str) -> PyColumn {
    PyColumn {
        inner: contains(&left.inner, right),
    }
}
#[pyfunction]
#[pyo3(signature = (str, pattern, escapeChar=None))]
fn py_like(str: &PyColumn, pattern: &str, escapeChar: Option<&str>) -> PyColumn {
    let esc = escapeChar.and_then(|s| s.chars().next());
    PyColumn {
        inner: like(&str.inner, pattern, esc),
    }
}
#[pyfunction]
#[pyo3(signature = (str, pattern, escapeChar=None))]
fn py_ilike(str: &PyColumn, pattern: &str, escapeChar: Option<&str>) -> PyColumn {
    let esc = escapeChar.and_then(|s| s.chars().next());
    PyColumn {
        inner: ilike(&str.inner, pattern, esc),
    }
}
#[pyfunction]
fn py_rlike(str: &PyColumn, regexp: &str) -> PyColumn {
    PyColumn {
        inner: rlike(&str.inner, regexp),
    }
}

#[pyfunction]
fn py_regexp_count(str: &PyColumn, regexp: &str) -> PyColumn {
    PyColumn {
        inner: regexp_count(&str.inner, regexp),
    }
}

#[pyfunction]
fn py_regexp_instr(str: &PyColumn, regexp: &str, idx: Option<usize>) -> PyColumn {
    PyColumn {
        inner: regexp_instr(&str.inner, regexp, idx),
    }
}

#[pyfunction]
fn py_regexp_substr(str: &PyColumn, regexp: &str) -> PyColumn {
    PyColumn {
        inner: regexp_substr(&str.inner, regexp),
    }
}

#[pyfunction]
#[pyo3(signature = (col, pattern, idx=0))]
fn py_regexp_extract(col: &PyColumn, pattern: &str, idx: usize) -> PyColumn {
    PyColumn {
        inner: regexp_extract(&col.inner, pattern, idx),
    }
}

#[pyfunction]
fn py_regexp_replace(col: &PyColumn, pattern: &str, replacement: &str) -> PyColumn {
    PyColumn {
        inner: regexp_replace(&col.inner, pattern, replacement),
    }
}

/// Extract all non-overlapping matches of regex as an array of strings (PySpark regexp_extract_all).
///
/// For each string in the column, finds all matches of the pattern and returns them as an array.
/// Null in → null out. Empty array if no matches.
///
/// Args:
///     col: String Column to extract from.
///     pattern: Regex pattern (string).
///     idx: Regex group index (0 = full match). Currently only 0 is supported.
///
/// Returns:
///     Column: Array of strings (all matches). PySpark-style signature.
#[pyfunction]
#[pyo3(signature = (str, regexp, idx=0))]
fn py_regexp_extract_all(str: &PyColumn, regexp: &str, idx: i32) -> PyResult<PyColumn> {
    if idx != 0 {
        return Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "regexp_extract_all: group index != 0 is not yet supported; use idx=0 for full match",
        ));
    }
    Ok(PyColumn {
        inner: regexp_extract_all(&str.inner, regexp),
    })
}

#[pyfunction]
#[pyo3(signature = (src, delimiter, limit=None))]
fn py_split(src: &PyColumn, delimiter: &str, limit: Option<i32>) -> PyColumn {
    PyColumn {
        inner: split(&src.inner, delimiter, limit),
    }
}
#[pyfunction]
#[pyo3(signature = (src, delimiter, part_num))]
fn py_split_part(src: &PyColumn, delimiter: &str, part_num: i64) -> PyColumn {
    PyColumn {
        inner: split_part(&src.inner, delimiter, part_num),
    }
}

#[pyfunction]
fn py_find_in_set(str: &PyColumn, str_array: &PyColumn) -> PyColumn {
    PyColumn {
        inner: find_in_set(&str.inner, &str_array.inner),
    }
}

#[pyfunction]
fn py_get_json_object(col: &PyColumn, path: &str) -> PyColumn {
    PyColumn {
        inner: get_json_object(&col.inner, path),
    }
}

#[pyfunction]
#[pyo3(signature = (col, *fields))]
fn py_json_tuple(col: &PyColumn, fields: Vec<String>) -> PyColumn {
    let key_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
    PyColumn {
        inner: json_tuple(&col.inner, &key_refs),
    }
}

#[pyfunction]
fn py_from_csv(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: from_csv(&col.inner),
    }
}

#[pyfunction]
fn py_to_csv(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: to_csv(&col.inner),
    }
}

#[pyfunction]
fn py_schema_of_csv(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: schema_of_csv(&col.inner),
    }
}

#[pyfunction]
fn py_schema_of_json(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: schema_of_json(&col.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (format, *cols))]
fn py_format_string(format: &str, cols: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<PyColumn> {
    let columns: Vec<PyRef<PyColumn>> = (0..cols.len())
        .map(|i| cols.get_item(i)?.extract())
        .collect::<PyResult<Vec<_>>>()?;
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    if refs.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "format_string() requires at least one column",
        ));
    }
    Ok(PyColumn {
        inner: format_string(format, &refs),
    })
}

#[pyfunction]
#[pyo3(signature = (format, *cols))]
fn py_printf(format: &str, cols: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<PyColumn> {
    py_format_string(format, cols)
}

#[pyfunction]
fn py_unix_timestamp(timestamp: Option<PyRef<PyColumn>>, format: Option<&str>) -> PyColumn {
    match &timestamp {
        None => PyColumn {
            inner: unix_timestamp_now(),
        },
        Some(c) => PyColumn {
            inner: unix_timestamp(&c.inner, format),
        },
    }
}

#[pyfunction]
fn py_to_unix_timestamp(timestamp: PyRef<PyColumn>, format: Option<&str>) -> PyColumn {
    PyColumn {
        inner: to_unix_timestamp(&timestamp.inner, format),
    }
}

#[pyfunction]
#[pyo3(signature = (timestamp, format=None))]
fn py_from_unixtime(timestamp: &PyColumn, format: Option<&str>) -> PyColumn {
    PyColumn {
        inner: from_unixtime(&timestamp.inner, format),
    }
}

#[pyfunction]
fn py_make_date(year: &PyColumn, month: &PyColumn, day: &PyColumn) -> PyColumn {
    PyColumn {
        inner: make_date(&year.inner, &month.inner, &day.inner),
    }
}

#[pyfunction]
fn py_timestamp_seconds(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: timestamp_seconds(&col.inner),
    }
}

#[pyfunction]
fn py_timestamp_millis(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: timestamp_millis(&col.inner),
    }
}

#[pyfunction]
fn py_timestamp_micros(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: timestamp_micros(&col.inner),
    }
}

#[pyfunction]
fn py_unix_date(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: unix_date(&col.inner),
    }
}

#[pyfunction]
fn py_date_from_unix_date(days: &PyColumn) -> PyColumn {
    PyColumn {
        inner: date_from_unix_date(&days.inner),
    }
}

#[pyfunction]
fn py_pmod(dividend: &PyColumn, divisor: &PyColumn) -> PyColumn {
    PyColumn {
        inner: pmod(&dividend.inner, &divisor.inner),
    }
}

#[pyfunction]
fn py_factorial(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: factorial(&col.inner),
    }
}

#[pyfunction]
fn py_array_append(col: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_append(&col.inner, &value.inner),
    }
}

#[pyfunction]
fn py_array_prepend(col: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_prepend(&col.inner, &value.inner),
    }
}

#[pyfunction]
fn py_array_insert(arr: &PyColumn, pos: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_insert(&arr.inner, &pos.inner, &value.inner),
    }
}

#[pyfunction]
fn py_array_except(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_except(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
fn py_array_intersect(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_intersect(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
fn py_array_union(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_union(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (col1, col2))]
fn py_map_concat(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: map_concat(&col1.inner, &col2.inner),
    }
}

#[pyfunction]
fn py_map_filter_value_gt(map_col: &PyColumn, threshold: f64) -> PyColumn {
    PyColumn {
        inner: map_filter_value_gt(&map_col.inner, threshold),
    }
}

#[pyfunction]
fn py_zip_with_coalesce(left: &PyColumn, right: &PyColumn) -> PyColumn {
    PyColumn {
        inner: zip_with_coalesce(&left.inner, &right.inner),
    }
}

#[pyfunction]
fn py_map_zip_with_coalesce(map1: &PyColumn, map2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: map_zip_with_coalesce(&map1.inner, &map2.inner),
    }
}

#[pyfunction]
fn py_map_from_entries(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: map_from_entries(&col.inner),
    }
}

#[pyfunction]
fn py_map_contains_key(col: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: map_contains_key(&col.inner, &value.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (*cols))]
fn py_create_map(cols: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<PyColumn> {
    let columns: Vec<PyRef<PyColumn>> = (0..cols.len())
        .map(|i| cols.get_item(i).and_then(|ob| ob.extract()))
        .collect::<PyResult<Vec<_>>>()
        .map_err(|e| {
            pyo3::exceptions::PyTypeError::new_err(format!("create_map args must be Column: {e}"))
        })?;
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    let inner =
        create_map(&refs).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyColumn { inner })
}

#[pyfunction]
fn py_array_remove(col: &PyColumn, value: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_remove(&col.inner, &value.inner),
    }
}

#[pyfunction]
fn py_flatten(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: array_flatten(&col.inner),
    }
}

#[pyfunction]
fn py_decode(col: &PyColumn, charset: &str) -> PyColumn {
    PyColumn {
        inner: decode(&col.inner, charset),
    }
}

#[pyfunction]
fn py_element_at(col: &PyColumn, index: i64) -> PyColumn {
    PyColumn {
        inner: element_at(&col.inner, index),
    }
}

#[pyfunction]
fn py_encode(col: &PyColumn, charset: &str) -> PyColumn {
    PyColumn {
        inner: encode(&col.inner, charset),
    }
}

#[pyfunction]
fn py_get(col: &PyColumn, index: &PyColumn) -> PyColumn {
    PyColumn {
        inner: get(&col.inner, &index.inner),
    }
}

#[pyfunction]
fn py_try_divide(left: &PyColumn, right: &PyColumn) -> PyColumn {
    PyColumn {
        inner: try_divide(&left.inner, &right.inner),
    }
}

#[pyfunction]
fn py_try_add(left: &PyColumn, right: &PyColumn) -> PyColumn {
    PyColumn {
        inner: try_add(&left.inner, &right.inner),
    }
}

#[pyfunction]
fn py_try_subtract(left: &PyColumn, right: &PyColumn) -> PyColumn {
    PyColumn {
        inner: try_subtract(&left.inner, &right.inner),
    }
}

#[pyfunction]
fn py_try_multiply(left: &PyColumn, right: &PyColumn) -> PyColumn {
    PyColumn {
        inner: try_multiply(&left.inner, &right.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (value, min_val, max_val, num_bucket))]
fn py_width_bucket(
    value: &PyColumn,
    min_val: f64,
    max_val: f64,
    num_bucket: i64,
) -> PyResult<PyColumn> {
    if num_bucket <= 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "width_bucket: num_bucket must be positive",
        ));
    }
    Ok(PyColumn {
        inner: width_bucket(&value.inner, min_val, max_val, num_bucket),
    })
}

#[pyfunction]
#[pyo3(signature = (index, cols))]
fn py_elt(index: &PyColumn, cols: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    if cols.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "elt() requires at least one column",
        ));
    }
    let refs: Vec<&RsColumn> = cols.iter().map(|c| &c.inner).collect();
    Ok(PyColumn {
        inner: crate::functions::elt(&index.inner, &refs),
    })
}

#[pyfunction]
fn py_bit_length(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: bit_length(&col.inner),
    }
}

#[pyfunction]
fn py_typeof(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: typeof_(&col.inner),
    }
}

/// Create a struct column from one or more columns (PySpark struct parity).
///
/// Args:
///     *cols: One or more Column expressions (variadic). At least one required.
///
/// Returns:
///     Column: Struct with one field per input column (field names from column names/aliases).
#[pyfunction]
#[pyo3(signature = (*cols))]
fn py_struct(cols: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<PyColumn> {
    let columns: Vec<PyRef<PyColumn>> = (0..cols.len())
        .map(|i| cols.get_item(i).and_then(|ob| ob.extract()))
        .collect::<PyResult<Vec<_>>>()
        .map_err(|e| {
            pyo3::exceptions::PyTypeError::new_err(format!("struct() args must be Column: {e}"))
        })?;
    if columns.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "struct() requires at least one column",
        ));
    }
    let refs: Vec<&RsColumn> = columns.iter().map(|c| &c.inner).collect();
    Ok(PyColumn {
        inner: struct_(&refs),
    })
}

#[pyfunction]
#[pyo3(signature = (names, columns))]
fn py_named_struct(names: Vec<String>, columns: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    if names.len() != columns.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "named_struct: names and columns must have same length",
        ));
    }
    if names.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "named_struct() requires at least one (name, column) pair",
        ));
    }
    let pairs: Vec<(&str, &RsColumn)> = names
        .iter()
        .zip(columns.iter())
        .map(|(n, c)| (n.as_str(), &c.inner))
        .collect();
    Ok(PyColumn {
        inner: named_struct(&pairs),
    })
}

#[pyfunction]
fn py_cosh(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: cosh(&col.inner),
    }
}
#[pyfunction]
fn py_sinh(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: sinh(&col.inner),
    }
}
#[pyfunction]
fn py_tanh(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: tanh(&col.inner),
    }
}
#[pyfunction]
fn py_acosh(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: acosh(&col.inner),
    }
}
#[pyfunction]
fn py_asinh(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: asinh(&col.inner),
    }
}
#[pyfunction]
fn py_atanh(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: atanh(&col.inner),
    }
}
#[pyfunction]
fn py_cbrt(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: cbrt(&col.inner),
    }
}
#[pyfunction]
fn py_expm1(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: expm1(&col.inner),
    }
}
#[pyfunction]
fn py_log1p(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: log1p(&col.inner),
    }
}
#[pyfunction]
fn py_log10(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: log10(&col.inner),
    }
}
#[pyfunction]
fn py_log2(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: log2(&col.inner),
    }
}
#[pyfunction]
fn py_rint(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rint(&col.inner),
    }
}
#[pyfunction]
fn py_hypot(col1: &PyColumn, col2: &PyColumn) -> PyColumn {
    PyColumn {
        inner: hypot(&col1.inner, &col2.inner),
    }
}
