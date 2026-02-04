//! Python bindings for robin-sparkless (PyO3).
//! Compiled only when the `pyo3` feature is enabled.

use crate::column::Column as RsColumn;
use crate::dataframe::JoinType;
use crate::dataframe::{CubeRollupData, WriteFormat, WriteMode};
use crate::functions::SortOrder;
use crate::functions::{
    acos, acosh, add_months, array_append, array_compact, array_distinct, array_except,
    array_insert, array_intersect, array_prepend, array_union, ascii, asin, asinh, atan, atan2,
    atanh, base64, cast as rs_cast, cbrt, ceiling, chr, contains, convert_timezone, cos, cosh,
    curdate, current_timezone, date_diff, date_from_unix_date, date_part, dateadd, datepart, day,
    dayname, dayofmonth, dayofweek, dayofyear, days, degrees, endswith, expm1, extract, factorial,
    find_in_set, format_number, format_string, from_unixtime, from_utc_timestamp,
    greatest as rs_greatest, hours, hypot, ifnull, ilike, isnan as rs_isnan, isnotnull, isnull,
    lcase, least as rs_least, left, like, ln, localtimestamp, log10, log1p, log2, make_date,
    make_interval, make_timestamp, make_timestamp_ntz, md5, minutes, months, months_between,
    next_day, now, nvl, nvl2, overlay, pmod, power, quarter, radians, regexp_count, regexp_instr,
    regexp_substr, replace as rs_replace, right, rint, rlike, sha1, sha2, signum, sin, sinh,
    split_part, startswith, substr, tan, tanh, timestamp_micros, timestamp_millis,
    timestamp_seconds, timestampadd, timestampdiff, to_degrees, to_radians, to_timestamp,
    to_unix_timestamp, to_utc_timestamp, try_cast as rs_try_cast, ucase, unbase64, unix_date,
    unix_micros, unix_millis, unix_seconds, unix_timestamp, unix_timestamp_now, weekday,
    weekofyear, years,
};
use crate::functions::{
    array_agg, arrays_overlap, arrays_zip, assert_true as rs_assert_true, bit_and, bit_count,
    bit_length, bit_or, bit_xor, bitwise_not, broadcast as rs_broadcast, create_map,
    current_catalog as rs_current_catalog, current_database as rs_current_database,
    current_schema as rs_current_schema, current_user as rs_current_user, equal_null,
    explode_outer, get, hash, inline as rs_inline, inline_outer as rs_inline_outer,
    input_file_name as rs_input_file_name, isin, isin_i64, isin_str, json_array_length, map_concat,
    map_contains_key, map_filter_value_gt, map_from_entries, map_zip_with_coalesce,
    monotonically_increasing_id as rs_monotonically_increasing_id, named_struct, parse_url,
    rand as rs_rand, randn as rs_randn, sequence, shift_left, shift_right, shuffle,
    spark_partition_id as rs_spark_partition_id, str_to_map, struct_, to_char, to_number,
    to_varchar, try_add, try_divide, try_multiply, try_subtract, try_to_number, try_to_timestamp,
    typeof_, url_decode, url_encode, user as rs_user, version, width_bucket, zip_with_coalesce,
};
use crate::functions::{
    asc, asc_nulls_first, asc_nulls_last, bround, cot, csc, desc, desc_nulls_first,
    desc_nulls_last, e, median, mode, negate, pi, positive, sec, stddev_pop, var_pop,
};
use crate::functions::{avg, coalesce, col as rs_col, count, max, min, sum as rs_sum};
use crate::functions::{bin, btrim, conv, getbit, hex, locate, unhex, when_then_otherwise_null};
use crate::plan;
use crate::{DataFrame, GroupedData, SparkSession};
use polars::prelude::Expr;
use pyo3::conversion::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::Value as JsonValue;
use std::path::Path;
use std::sync::RwLock;

/// Convert a Python scalar to serde_json::Value for plan/row data.
fn py_to_json_value(value: &Bound<'_, pyo3::types::PyAny>) -> PyResult<JsonValue> {
    if value.is_none() {
        return Ok(JsonValue::Null);
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
    if let Ok(x) = value.extract::<bool>() {
        return Ok(JsonValue::Bool(x));
    }
    if let Ok(x) = value.extract::<String>() {
        return Ok(JsonValue::String(x));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "create_dataframe_from_rows / execute_plan: row values must be None, int, float, bool, or str",
    ))
}

/// Execute a logical plan (Phase 25). Returns a DataFrame; call .collect() to get list of dicts.
/// data: list of dicts or list of lists (rows). schema: list of (name, dtype_str).
/// plan_json: JSON string of the plan, e.g. json.dumps([{"op": "filter", "payload": ...}, ...]).
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

/// Python module entry point.
#[pymodule]
fn robin_sparkless(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySparkSession>()?;
    m.add_class::<PySparkSessionBuilder>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyDataFrameStat>()?;
    m.add_class::<PyDataFrameNa>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PySortOrder>()?;
    m.add_class::<PyWhenBuilder>()?;
    m.add_class::<PyThenBuilder>()?;
    m.add_class::<PyGroupedData>()?;
    m.add_class::<PyCubeRollupData>()?;
    m.add_class::<PyDataFrameWriter>()?;
    m.add("col", wrap_pyfunction!(py_col, m)?)?;
    m.add("lit", wrap_pyfunction!(py_lit, m)?)?;
    m.add("when", wrap_pyfunction!(py_when, m)?)?;
    m.add("coalesce", wrap_pyfunction!(py_coalesce, m)?)?;
    m.add("sum", wrap_pyfunction!(py_sum, m)?)?;
    m.add("avg", wrap_pyfunction!(py_avg, m)?)?;
    m.add("min", wrap_pyfunction!(py_min, m)?)?;
    m.add("max", wrap_pyfunction!(py_max, m)?)?;
    m.add("count", wrap_pyfunction!(py_count, m)?)?;
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
    // Phase 14: math, datetime, type/conditional
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
    // Phase 15 Batch 1: aliases and simple
    m.add("nvl", wrap_pyfunction!(py_nvl, m)?)?;
    m.add("ifnull", wrap_pyfunction!(py_ifnull, m)?)?;
    m.add("nvl2", wrap_pyfunction!(py_nvl2, m)?)?;
    m.add("substr", wrap_pyfunction!(py_substr, m)?)?;
    m.add("power", wrap_pyfunction!(py_power, m)?)?;
    m.add("ln", wrap_pyfunction!(py_ln, m)?)?;
    m.add("ceiling", wrap_pyfunction!(py_ceiling, m)?)?;
    m.add("lcase", wrap_pyfunction!(py_lcase, m)?)?;
    m.add("ucase", wrap_pyfunction!(py_ucase, m)?)?;
    m.add("day", wrap_pyfunction!(py_day, m)?)?;
    m.add("dayofmonth", wrap_pyfunction!(py_dayofmonth, m)?)?;
    m.add("to_degrees", wrap_pyfunction!(py_to_degrees, m)?)?;
    m.add("to_radians", wrap_pyfunction!(py_to_radians, m)?)?;
    m.add("isnull", wrap_pyfunction!(py_isnull, m)?)?;
    m.add("isnotnull", wrap_pyfunction!(py_isnotnull, m)?)?;
    // Phase 15 Batch 2: string (left, right, replace, startswith, endswith, contains, like, ilike, rlike)
    m.add("left", wrap_pyfunction!(py_left, m)?)?;
    m.add("right", wrap_pyfunction!(py_right, m)?)?;
    m.add("replace", wrap_pyfunction!(py_replace, m)?)?;
    m.add("startswith", wrap_pyfunction!(py_startswith, m)?)?;
    m.add("endswith", wrap_pyfunction!(py_endswith, m)?)?;
    m.add("contains", wrap_pyfunction!(py_contains, m)?)?;
    m.add("like", wrap_pyfunction!(py_like, m)?)?;
    m.add("ilike", wrap_pyfunction!(py_ilike, m)?)?;
    m.add("rlike", wrap_pyfunction!(py_rlike, m)?)?;
    // Phase 16: string/regex (regexp_count, regexp_instr, regexp_substr, split_part, find_in_set, format_string, printf)
    m.add("regexp_count", wrap_pyfunction!(py_regexp_count, m)?)?;
    m.add("regexp_instr", wrap_pyfunction!(py_regexp_instr, m)?)?;
    m.add("regexp_substr", wrap_pyfunction!(py_regexp_substr, m)?)?;
    m.add("split_part", wrap_pyfunction!(py_split_part, m)?)?;
    m.add("find_in_set", wrap_pyfunction!(py_find_in_set, m)?)?;
    m.add("format_string", wrap_pyfunction!(py_format_string, m)?)?;
    m.add("printf", wrap_pyfunction!(py_printf, m)?)?;
    // Phase 15 Batch 3: math (cosh, sinh, tanh, acosh, asinh, atanh, cbrt, expm1, log1p, log10, log2, rint, hypot)
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
    // Phase 17: unix_timestamp, from_unixtime, make_date, timestamp_*, unix_date, date_from_unix_date, pmod, factorial
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
    // Phase 18: array (append, prepend, insert, except, intersect, union), map (concat, from_entries, contains_key, get), struct
    m.add("array_append", wrap_pyfunction!(py_array_append, m)?)?;
    m.add("array_prepend", wrap_pyfunction!(py_array_prepend, m)?)?;
    m.add("array_insert", wrap_pyfunction!(py_array_insert, m)?)?;
    m.add("array_except", wrap_pyfunction!(py_array_except, m)?)?;
    m.add("array_intersect", wrap_pyfunction!(py_array_intersect, m)?)?;
    m.add("array_union", wrap_pyfunction!(py_array_union, m)?)?;
    m.add(
        "zip_with_coalesce",
        wrap_pyfunction!(py_zip_with_coalesce, m)?,
    )?;
    m.add("map_concat", wrap_pyfunction!(py_map_concat, m)?)?;
    m.add(
        "map_filter_value_gt",
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
        "map_zip_with_coalesce",
        wrap_pyfunction!(py_map_zip_with_coalesce, m)?,
    )?;
    m.add("create_map", wrap_pyfunction!(py_create_map, m)?)?;
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
    // Phase 20: ordering, aggregates, numeric
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
    // Phase 21: string, binary, type, array, map, struct
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
    m.add("explode_outer", wrap_pyfunction!(py_explode_outer, m)?)?;
    m.add("inline", wrap_pyfunction!(py_inline, m)?)?;
    m.add("inline_outer", wrap_pyfunction!(py_inline_outer, m)?)?;
    m.add("sequence", wrap_pyfunction!(py_sequence, m)?)?;
    m.add("shuffle", wrap_pyfunction!(py_shuffle, m)?)?;
    m.add("array_agg", wrap_pyfunction!(py_array_agg, m)?)?;
    // Phase 22: datetime extensions
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
    // Phase 23: JSON, URL, misc
    m.add("isin", wrap_pyfunction!(py_isin, m)?)?;
    m.add("isin_i64", wrap_pyfunction!(py_isin_i64, m)?)?;
    m.add("isin_str", wrap_pyfunction!(py_isin_str, m)?)?;
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
    // Phase 24: bit, control, JVM stubs, random
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
    m.add("execute_plan", wrap_pyfunction!(py_execute_plan, m)?)?;
    Ok(())
}

#[pyfunction]
fn py_col(col: &str) -> PyColumn {
    PyColumn { inner: rs_col(col) }
}

#[pyfunction]
fn py_lit(col: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyColumn> {
    let inner = if col.is_none() {
        use polars::prelude::*;
        RsColumn::from_expr(lit(NULL), None)
    } else if let Ok(x) = col.extract::<i64>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = col.extract::<f64>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = col.extract::<bool>() {
        RsColumn::from_expr(polars::prelude::lit(x), None)
    } else if let Ok(x) = col.extract::<String>() {
        RsColumn::from_expr(polars::prelude::lit(x.as_str()), None)
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "lit() supports only None, int, float, bool, str",
        ));
    };
    Ok(PyColumn { inner })
}

#[pyfunction]
#[pyo3(signature = (condition, value=None))]
fn py_when(
    condition: &PyColumn,
    value: Option<PyRef<PyColumn>>,
    py: Python<'_>,
) -> PyResult<Py<PyAny>> {
    match value {
        Some(v) => {
            let col = when_then_otherwise_null(&condition.inner, &v.inner);
            Ok(PyColumn { inner: col }.into_py_any(py)?)
        }
        None => Ok(PyWhenBuilder {
            condition: condition.inner.expr().clone(),
        }
        .into_py_any(py)?),
    }
}

#[pyfunction]
fn py_coalesce(cols: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    let refs: Vec<&RsColumn> = cols.iter().map(|c| &c.inner).collect();
    Ok(PyColumn {
        inner: coalesce(&refs),
    })
}

#[pyfunction]
fn py_sum(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rs_sum(&col.inner),
    }
}

#[pyfunction]
fn py_avg(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: avg(&col.inner),
    }
}

#[pyfunction]
fn py_min(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: min(&col.inner),
    }
}

#[pyfunction]
fn py_max(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: max(&col.inner),
    }
}

#[pyfunction]
fn py_count(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: count(&col.inner),
    }
}

#[pyfunction]
fn py_asc(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: asc(&col.inner),
    }
}

#[pyfunction]
fn py_asc_nulls_first(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: asc_nulls_first(&col.inner),
    }
}

#[pyfunction]
fn py_asc_nulls_last(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: asc_nulls_last(&col.inner),
    }
}

#[pyfunction]
fn py_desc(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: desc(&col.inner),
    }
}

#[pyfunction]
fn py_desc_nulls_first(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: desc_nulls_first(&col.inner),
    }
}

#[pyfunction]
fn py_desc_nulls_last(col: &PyColumn) -> PySortOrder {
    PySortOrder {
        inner: desc_nulls_last(&col.inner),
    }
}

#[pyfunction]
fn py_bround(col: &PyColumn, scale: i32) -> PyColumn {
    PyColumn {
        inner: bround(&col.inner, scale),
    }
}

#[pyfunction]
fn py_negate(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: negate(&col.inner),
    }
}

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
fn py_btrim(str: &PyColumn, trim: Option<&str>) -> PyColumn {
    PyColumn {
        inner: btrim(&str.inner, trim),
    }
}

#[pyfunction]
fn py_locate(substr: &str, str: &PyColumn, pos: Option<i64>) -> PyColumn {
    PyColumn {
        inner: locate(substr, &str.inner, pos.unwrap_or(1)),
    }
}

#[pyfunction]
#[pyo3(signature = (col, from_base, to_base))]
fn py_conv(col: &PyColumn, from_base: i32, to_base: i32) -> PyColumn {
    PyColumn {
        inner: conv(&col.inner, from_base, to_base),
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
fn py_to_char(col: &PyColumn, format: Option<&str>) -> PyColumn {
    PyColumn {
        inner: to_char(&col.inner, format),
    }
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_to_varchar(col: &PyColumn, format: Option<&str>) -> PyColumn {
    PyColumn {
        inner: to_varchar(&col.inner, format),
    }
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_to_number(col: &PyColumn, format: Option<&str>) -> PyColumn {
    PyColumn {
        inner: to_number(&col.inner, format),
    }
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_try_to_number(col: &PyColumn, format: Option<&str>) -> PyColumn {
    PyColumn {
        inner: try_to_number(&col.inner, format),
    }
}

#[pyfunction]
#[pyo3(signature = (col, format=None))]
fn py_try_to_timestamp(col: &PyColumn, format: Option<&str>) -> PyColumn {
    PyColumn {
        inner: try_to_timestamp(&col.inner, format),
    }
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
fn py_explode_outer(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: explode_outer(&col.inner),
    }
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

// Phase 22: datetime extensions
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
#[pyo3(signature = (year, month, day, hour, minute, sec, timezone=None))]
fn py_make_timestamp(
    year: &PyColumn,
    month: &PyColumn,
    day: &PyColumn,
    hour: &PyColumn,
    minute: &PyColumn,
    sec: &PyColumn,
    timezone: Option<&str>,
) -> PyColumn {
    PyColumn {
        inner: make_timestamp(
            &year.inner,
            &month.inner,
            &day.inner,
            &hour.inner,
            &minute.inner,
            &sec.inner,
            timezone,
        ),
    }
}

#[pyfunction]
#[pyo3(signature = (year, month, day, hour, minute, sec))]
fn py_make_timestamp_ntz(
    year: &PyColumn,
    month: &PyColumn,
    day: &PyColumn,
    hour: &PyColumn,
    minute: &PyColumn,
    sec: &PyColumn,
) -> PyColumn {
    PyColumn {
        inner: make_timestamp_ntz(
            &year.inner,
            &month.inner,
            &day.inner,
            &hour.inner,
            &minute.inner,
            &sec.inner,
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
#[pyo3(signature = (source_tz, target_tz, source_ts))]
fn py_convert_timezone(source_tz: &str, target_tz: &str, source_ts: &PyColumn) -> PyColumn {
    PyColumn {
        inner: convert_timezone(source_tz, target_tz, &source_ts.inner),
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
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}

// Phase 23: JSON, URL, misc
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
#[pyo3(signature = (col, num_bits))]
fn py_shift_left(col: &PyColumn, num_bits: i32) -> PyColumn {
    PyColumn {
        inner: shift_left(&col.inner, num_bits),
    }
}

#[pyfunction]
#[pyo3(signature = (col, num_bits))]
fn py_shift_right(col: &PyColumn, num_bits: i32) -> PyColumn {
    PyColumn {
        inner: shift_right(&col.inner, num_bits),
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
#[pyo3(signature = (col, err_msg=None))]
fn py_assert_true(col: &PyColumn, err_msg: Option<&str>) -> PyColumn {
    PyColumn {
        inner: rs_assert_true(&col.inner, err_msg),
    }
}

#[pyfunction]
#[pyo3(signature = (err_msg))]
fn py_raise_error(err_msg: &str) -> PyColumn {
    PyColumn {
        inner: crate::functions::raise_error(err_msg),
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
#[pyo3(signature = (url, part, key=None))]
fn py_parse_url(url: &PyColumn, part: &str, key: Option<&str>) -> PyColumn {
    PyColumn {
        inner: parse_url(&url.inner, part, key),
    }
}

#[pyfunction]
fn py_hash(cols: Vec<PyRef<PyColumn>>) -> PyColumn {
    let rs_refs: Vec<&crate::column::Column> = cols.iter().map(|c| &(&*c).inner).collect();
    PyColumn {
        inner: hash(&rs_refs),
    }
}

#[pyfunction]
fn py_stack(cols: Vec<PyRef<PyColumn>>) -> PyColumn {
    let rs_refs: Vec<&crate::column::Column> = cols.iter().map(|c| &(&*c).inner).collect();
    PyColumn {
        inner: struct_(&rs_refs),
    }
}

#[pyfunction]
fn py_ascii(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ascii(&col.inner),
    }
}

#[pyfunction]
fn py_format_number(col: &PyColumn, d: u32) -> PyColumn {
    PyColumn {
        inner: format_number(&col.inner, d),
    }
}

#[pyfunction]
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
#[pyo3(signature = (col, num_bits))]
fn py_sha2(col: &PyColumn, num_bits: i32) -> PyColumn {
    PyColumn {
        inner: sha2(&col.inner, num_bits),
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
#[pyo3(signature = (date1, date2, round_off=true))]
fn py_months_between(date1: &PyColumn, date2: &PyColumn, round_off: Option<bool>) -> PyColumn {
    let round_off = round_off.unwrap_or(true);
    PyColumn {
        inner: months_between(&date1.inner, &date2.inner, round_off),
    }
}
#[pyfunction]
#[pyo3(signature = (date, day_of_week))]
fn py_next_day(date: &PyColumn, day_of_week: &str) -> PyColumn {
    PyColumn {
        inner: next_day(&date.inner, day_of_week),
    }
}
#[pyfunction]
fn py_cast(col: &PyColumn, type_name: &str) -> PyResult<PyColumn> {
    rs_cast(&col.inner, type_name)
        .map(|inner| PyColumn { inner })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}
#[pyfunction]
fn py_try_cast(col: &PyColumn, type_name: &str) -> PyResult<PyColumn> {
    rs_try_cast(&col.inner, type_name)
        .map(|inner| PyColumn { inner })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}
#[pyfunction]
fn py_isnan(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: rs_isnan(&col.inner),
    }
}
#[pyfunction]
fn py_greatest(cols: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    let refs: Vec<&RsColumn> = cols.iter().map(|c| &c.inner).collect();
    rs_greatest(&refs)
        .map(|inner| PyColumn { inner })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
}
#[pyfunction]
fn py_least(cols: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    let refs: Vec<&RsColumn> = cols.iter().map(|c| &c.inner).collect();
    rs_least(&refs)
        .map(|inner| PyColumn { inner })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
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
fn py_ceiling(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: ceiling(&col.inner),
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
#[pyo3(signature = (str, pattern, escape_char=None))]
fn py_like(str: &PyColumn, pattern: &str, escape_char: Option<&str>) -> PyColumn {
    let esc = escape_char.and_then(|s| s.chars().next());
    PyColumn {
        inner: like(&str.inner, pattern, esc),
    }
}
#[pyfunction]
#[pyo3(signature = (str, pattern, escape_char=None))]
fn py_ilike(str: &PyColumn, pattern: &str, escape_char: Option<&str>) -> PyColumn {
    let esc = escape_char.and_then(|s| s.chars().next());
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
fn py_format_string(format: &str, cols: Vec<PyRef<PyColumn>>) -> PyColumn {
    let refs: Vec<&RsColumn> = cols.iter().map(|c| &c.inner).collect();
    PyColumn {
        inner: format_string(format, &refs),
    }
}

#[pyfunction]
fn py_printf(format: &str, cols: Vec<PyRef<PyColumn>>) -> PyColumn {
    py_format_string(format, cols)
}

// Phase 17: datetime/unix and math
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

// Phase 18: array, map, struct
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
fn py_create_map(cols: Vec<PyRef<PyColumn>>) -> PyColumn {
    let refs: Vec<&RsColumn> = cols.iter().map(|c| &c.inner).collect();
    PyColumn {
        inner: create_map(&refs),
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
fn py_width_bucket(value: &PyColumn, min_val: f64, max_val: f64, num_bucket: i64) -> PyColumn {
    PyColumn {
        inner: width_bucket(&value.inner, min_val, max_val, num_bucket),
    }
}

#[pyfunction]
#[pyo3(signature = (index, cols))]
fn py_elt(index: &PyColumn, cols: Vec<PyRef<PyColumn>>) -> PyColumn {
    let refs: Vec<&RsColumn> = cols.iter().map(|c| &c.inner).collect();
    PyColumn {
        inner: crate::functions::elt(&index.inner, &refs),
    }
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

#[pyfunction]
fn py_struct(cols: Vec<PyRef<PyColumn>>) -> PyColumn {
    let refs: Vec<&RsColumn> = cols.iter().map(|c| &c.inner).collect();
    PyColumn {
        inner: struct_(&refs),
    }
}

#[pyfunction]
#[pyo3(signature = (names, columns))]
fn py_named_struct(names: Vec<String>, columns: Vec<PyRef<PyColumn>>) -> PyResult<PyColumn> {
    if names.len() != columns.len() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "named_struct: names and columns must have same length",
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

/// Python wrapper for SortOrder (used with order_by_exprs).
#[pyclass]
#[derive(Clone)]
struct PySortOrder {
    inner: SortOrder,
}

/// Python wrapper for Column (expression).
#[pyclass(name = "Column")]
struct PyColumn {
    inner: RsColumn,
}

#[pymethods]
impl PyColumn {
    fn alias(&self, name: &str) -> Self {
        PyColumn {
            inner: self.inner.alias(name),
        }
    }

    fn is_null(&self) -> Self {
        PyColumn {
            inner: self.inner.is_null(),
        }
    }

    fn is_not_null(&self) -> Self {
        PyColumn {
            inner: self.inner.is_not_null(),
        }
    }

    fn gt(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.gt(other.inner.expr().clone()),
        }
    }

    fn ge(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.gt_eq(other.inner.expr().clone()),
        }
    }

    fn lt(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.lt(other.inner.expr().clone()),
        }
    }

    fn le(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.lt_eq(other.inner.expr().clone()),
        }
    }

    fn eq(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.eq(other.inner.expr().clone()),
        }
    }

    fn ne(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.neq(other.inner.expr().clone()),
        }
    }

    fn and_(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: RsColumn::from_expr(
                self.inner.expr().clone().and(other.inner.expr().clone()),
                None,
            ),
        }
    }

    fn or_(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: RsColumn::from_expr(
                self.inner.expr().clone().or(other.inner.expr().clone()),
                None,
            ),
        }
    }

    fn upper(&self) -> Self {
        PyColumn {
            inner: self.inner.upper(),
        }
    }

    fn lower(&self) -> Self {
        PyColumn {
            inner: self.inner.lower(),
        }
    }

    #[pyo3(signature = (start, length=None))]
    fn substr(&self, start: i64, length: Option<i64>) -> Self {
        PyColumn {
            inner: self.inner.substr(start, length),
        }
    }

    fn ascii_(&self) -> Self {
        PyColumn {
            inner: ascii(&self.inner),
        }
    }

    fn format_number(&self, decimals: u32) -> Self {
        PyColumn {
            inner: format_number(&self.inner, decimals),
        }
    }

    fn overlay(&self, replace: &str, pos: i64, length: i64) -> Self {
        PyColumn {
            inner: overlay(&self.inner, replace, pos, length),
        }
    }

    fn char_(&self) -> Self {
        PyColumn {
            inner: crate::functions::char(&self.inner),
        }
    }

    fn chr_(&self) -> Self {
        PyColumn {
            inner: chr(&self.inner),
        }
    }

    fn base64_(&self) -> Self {
        PyColumn {
            inner: base64(&self.inner),
        }
    }

    fn unbase64_(&self) -> Self {
        PyColumn {
            inner: unbase64(&self.inner),
        }
    }

    fn sha1_(&self) -> Self {
        PyColumn {
            inner: sha1(&self.inner),
        }
    }

    fn sha2_(&self, bit_length: i32) -> Self {
        PyColumn {
            inner: sha2(&self.inner, bit_length),
        }
    }

    fn md5_(&self) -> Self {
        PyColumn {
            inner: md5(&self.inner),
        }
    }

    fn array_compact(&self) -> Self {
        PyColumn {
            inner: array_compact(&self.inner),
        }
    }
    fn array_distinct(&self) -> Self {
        PyColumn {
            inner: array_distinct(&self.inner),
        }
    }

    fn array_append(&self, elem: &PyColumn) -> Self {
        PyColumn {
            inner: array_append(&self.inner, &elem.inner),
        }
    }

    fn array_prepend(&self, elem: &PyColumn) -> Self {
        PyColumn {
            inner: array_prepend(&self.inner, &elem.inner),
        }
    }

    fn array_insert(&self, pos: &PyColumn, elem: &PyColumn) -> Self {
        PyColumn {
            inner: array_insert(&self.inner, &pos.inner, &elem.inner),
        }
    }

    fn array_except(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: array_except(&self.inner, &other.inner),
        }
    }

    fn array_intersect(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: array_intersect(&self.inner, &other.inner),
        }
    }

    fn array_union(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: array_union(&self.inner, &other.inner),
        }
    }

    fn map_concat(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: map_concat(&self.inner, &other.inner),
        }
    }

    fn map_filter_value_gt(&self, threshold: f64) -> Self {
        PyColumn {
            inner: map_filter_value_gt(&self.inner, threshold),
        }
    }

    fn zip_with_coalesce(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: zip_with_coalesce(&self.inner, &other.inner),
        }
    }

    fn map_zip_with_coalesce(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: map_zip_with_coalesce(&self.inner, &other.inner),
        }
    }

    fn map_from_entries(&self) -> Self {
        PyColumn {
            inner: map_from_entries(&self.inner),
        }
    }

    fn map_contains_key(&self, key: &PyColumn) -> Self {
        PyColumn {
            inner: map_contains_key(&self.inner, &key.inner),
        }
    }

    fn get(&self, key: &PyColumn) -> Self {
        PyColumn {
            inner: get(&self.inner, &key.inner),
        }
    }

    fn try_divide(&self, right: &PyColumn) -> Self {
        PyColumn {
            inner: try_divide(&self.inner, &right.inner),
        }
    }

    fn try_add(&self, right: &PyColumn) -> Self {
        PyColumn {
            inner: try_add(&self.inner, &right.inner),
        }
    }

    fn try_subtract(&self, right: &PyColumn) -> Self {
        PyColumn {
            inner: try_subtract(&self.inner, &right.inner),
        }
    }

    fn try_multiply(&self, right: &PyColumn) -> Self {
        PyColumn {
            inner: try_multiply(&self.inner, &right.inner),
        }
    }

    fn bit_length(&self) -> Self {
        PyColumn {
            inner: bit_length(&self.inner),
        }
    }

    fn typeof_(&self) -> Self {
        PyColumn {
            inner: typeof_(&self.inner),
        }
    }

    fn sin(&self) -> Self {
        PyColumn {
            inner: sin(&self.inner),
        }
    }
    fn cos(&self) -> Self {
        PyColumn {
            inner: cos(&self.inner),
        }
    }
    fn tan(&self) -> Self {
        PyColumn {
            inner: tan(&self.inner),
        }
    }
    fn asin_(&self) -> Self {
        PyColumn {
            inner: asin(&self.inner),
        }
    }
    fn acos_(&self) -> Self {
        PyColumn {
            inner: acos(&self.inner),
        }
    }
    fn atan_(&self) -> Self {
        PyColumn {
            inner: atan(&self.inner),
        }
    }
    fn atan2(&self, x: &PyColumn) -> Self {
        PyColumn {
            inner: atan2(&self.inner, &x.inner),
        }
    }
    fn degrees_(&self) -> Self {
        PyColumn {
            inner: degrees(&self.inner),
        }
    }
    fn radians_(&self) -> Self {
        PyColumn {
            inner: radians(&self.inner),
        }
    }
    fn signum(&self) -> Self {
        PyColumn {
            inner: signum(&self.inner),
        }
    }
    fn quarter(&self) -> Self {
        PyColumn {
            inner: quarter(&self.inner),
        }
    }
    fn weekofyear(&self) -> Self {
        PyColumn {
            inner: weekofyear(&self.inner),
        }
    }
    fn dayofweek(&self) -> Self {
        PyColumn {
            inner: dayofweek(&self.inner),
        }
    }
    fn dayofyear(&self) -> Self {
        PyColumn {
            inner: dayofyear(&self.inner),
        }
    }
    fn add_months(&self, n: i32) -> Self {
        PyColumn {
            inner: add_months(&self.inner, n),
        }
    }
    fn months_between(&self, start: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.months_between(&start.inner, true),
        }
    }
    fn next_day(&self, day_of_week: &str) -> Self {
        PyColumn {
            inner: next_day(&self.inner, day_of_week),
        }
    }
    fn cast(&self, type_name: &str) -> PyResult<Self> {
        rs_cast(&self.inner, type_name)
            .map(|inner| PyColumn { inner })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
    }
    fn try_cast(&self, type_name: &str) -> PyResult<Self> {
        rs_try_cast(&self.inner, type_name)
            .map(|inner| PyColumn { inner })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
    }
    fn isnan(&self) -> Self {
        PyColumn {
            inner: rs_isnan(&self.inner),
        }
    }
    fn nvl(&self, value: &PyColumn) -> Self {
        PyColumn {
            inner: nvl(&self.inner, &value.inner),
        }
    }
    fn ifnull(&self, value: &PyColumn) -> Self {
        PyColumn {
            inner: ifnull(&self.inner, &value.inner),
        }
    }
    fn power(&self, exp: i64) -> Self {
        PyColumn {
            inner: power(&self.inner, exp),
        }
    }
    fn ln(&self) -> Self {
        PyColumn {
            inner: ln(&self.inner),
        }
    }
    fn ceiling(&self) -> Self {
        PyColumn {
            inner: ceiling(&self.inner),
        }
    }
    fn lcase(&self) -> Self {
        PyColumn {
            inner: lcase(&self.inner),
        }
    }
    fn ucase(&self) -> Self {
        PyColumn {
            inner: ucase(&self.inner),
        }
    }
    fn day(&self) -> Self {
        PyColumn {
            inner: day(&self.inner),
        }
    }
    fn dayofmonth(&self) -> Self {
        PyColumn {
            inner: dayofmonth(&self.inner),
        }
    }
    fn to_degrees(&self) -> Self {
        PyColumn {
            inner: to_degrees(&self.inner),
        }
    }
    fn to_radians(&self) -> Self {
        PyColumn {
            inner: to_radians(&self.inner),
        }
    }
    fn isnull(&self) -> Self {
        PyColumn {
            inner: isnull(&self.inner),
        }
    }
    fn isnotnull(&self) -> Self {
        PyColumn {
            inner: isnotnull(&self.inner),
        }
    }
    fn left(&self, n: i64) -> Self {
        PyColumn {
            inner: left(&self.inner, n),
        }
    }
    fn right(&self, n: i64) -> Self {
        PyColumn {
            inner: right(&self.inner, n),
        }
    }
    fn replace(&self, search: &str, replacement: &str) -> Self {
        PyColumn {
            inner: rs_replace(&self.inner, search, replacement),
        }
    }
    fn startswith(&self, prefix: &str) -> Self {
        PyColumn {
            inner: startswith(&self.inner, prefix),
        }
    }
    fn endswith(&self, suffix: &str) -> Self {
        PyColumn {
            inner: endswith(&self.inner, suffix),
        }
    }
    fn contains(&self, substring: &str) -> Self {
        PyColumn {
            inner: contains(&self.inner, substring),
        }
    }
    fn like(&self, pattern: &str) -> Self {
        PyColumn {
            inner: like(&self.inner, pattern, None),
        }
    }
    fn ilike(&self, pattern: &str) -> Self {
        PyColumn {
            inner: ilike(&self.inner, pattern, None),
        }
    }
    fn rlike(&self, pattern: &str) -> Self {
        PyColumn {
            inner: rlike(&self.inner, pattern),
        }
    }
    fn cosh(&self) -> Self {
        PyColumn {
            inner: cosh(&self.inner),
        }
    }
    fn sinh(&self) -> Self {
        PyColumn {
            inner: sinh(&self.inner),
        }
    }
    fn tanh(&self) -> Self {
        PyColumn {
            inner: tanh(&self.inner),
        }
    }
    fn acosh(&self) -> Self {
        PyColumn {
            inner: acosh(&self.inner),
        }
    }
    fn asinh(&self) -> Self {
        PyColumn {
            inner: asinh(&self.inner),
        }
    }
    fn atanh_(&self) -> Self {
        PyColumn {
            inner: atanh(&self.inner),
        }
    }
    fn cbrt(&self) -> Self {
        PyColumn {
            inner: cbrt(&self.inner),
        }
    }
    fn expm1(&self) -> Self {
        PyColumn {
            inner: expm1(&self.inner),
        }
    }
    fn log1p(&self) -> Self {
        PyColumn {
            inner: log1p(&self.inner),
        }
    }
    fn log10(&self) -> Self {
        PyColumn {
            inner: log10(&self.inner),
        }
    }
    fn log2(&self) -> Self {
        PyColumn {
            inner: log2(&self.inner),
        }
    }
    fn rint(&self) -> Self {
        PyColumn {
            inner: rint(&self.inner),
        }
    }
    fn hypot(&self, other: &PyColumn) -> Self {
        PyColumn {
            inner: hypot(&self.inner, &other.inner),
        }
    }

    /// Array/list size (PySpark size).
    fn size(&self) -> Self {
        PyColumn {
            inner: self.inner.array_size(),
        }
    }

    /// Element at 1-based index (PySpark element_at).
    fn element_at(&self, index: i64) -> Self {
        PyColumn {
            inner: self.inner.element_at(index),
        }
    }

    /// Explode list into one row per element (PySpark explode).
    fn explode(&self) -> Self {
        PyColumn {
            inner: self.inner.explode(),
        }
    }

    /// 1-based index of first occurrence of value in list, or 0 if not found (PySpark array_position).
    fn array_position(&self, value: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.array_position(value.inner.expr().clone()),
        }
    }

    /// New list with all elements equal to value removed (PySpark array_remove).
    fn array_remove(&self, value: &PyColumn) -> Self {
        PyColumn {
            inner: self.inner.array_remove(value.inner.expr().clone()),
        }
    }

    /// Repeat each element n times (PySpark array_repeat). Not implemented.
    fn array_repeat(&self, n: i64) -> Self {
        PyColumn {
            inner: self.inner.array_repeat(n),
        }
    }

    /// Explode list with position (PySpark posexplode). Returns (pos_column, value_column).
    fn posexplode(&self) -> (Self, Self) {
        let (pos, val) = self.inner.posexplode();
        (PyColumn { inner: pos }, PyColumn { inner: val })
    }

    /// First value in partition (PySpark first_value). Use with .over().
    fn first_value(&self) -> Self {
        PyColumn {
            inner: self.inner.first_value(),
        }
    }

    /// Last value in partition (PySpark last_value). Use with .over().
    fn last_value(&self) -> Self {
        PyColumn {
            inner: self.inner.last_value(),
        }
    }

    /// Percent rank in partition. Window is applied; pass partition_by.
    fn percent_rank(&self, partition_by: Vec<String>, descending: bool) -> Self {
        let refs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
        PyColumn {
            inner: self.inner.percent_rank(&refs, descending),
        }
    }

    /// Cumulative distribution in partition. Window is applied; pass partition_by.
    fn cume_dist(&self, partition_by: Vec<String>, descending: bool) -> Self {
        let refs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
        PyColumn {
            inner: self.inner.cume_dist(&refs, descending),
        }
    }

    /// Ntile: bucket 1..n by rank within partition. Window is applied; pass partition_by.
    fn ntile(&self, n: u32, partition_by: Vec<String>, descending: bool) -> Self {
        let refs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
        PyColumn {
            inner: self.inner.ntile(n, &refs, descending),
        }
    }

    /// Nth value in partition by order (1-based n). Window is applied; pass partition_by, do not call .over() again.
    fn nth_value(&self, n: i64, partition_by: Vec<String>, descending: bool) -> Self {
        let refs: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
        PyColumn {
            inner: self.inner.nth_value(n, &refs, descending),
        }
    }

    /// Check if string matches regex (PySpark regexp_like).
    fn regexp_like(&self, pattern: &str) -> Self {
        PyColumn {
            inner: self.inner.regexp_like(pattern),
        }
    }

    /// Count of non-overlapping regex matches (PySpark regexp_count).
    fn regexp_count(&self, pattern: &str) -> Self {
        PyColumn {
            inner: regexp_count(&self.inner, pattern),
        }
    }

    /// 1-based position of first regex match (PySpark regexp_instr).
    fn regexp_instr(&self, pattern: &str, group_idx: Option<usize>) -> Self {
        PyColumn {
            inner: regexp_instr(&self.inner, pattern, group_idx),
        }
    }

    /// First substring matching regex (PySpark regexp_substr).
    fn regexp_substr(&self, pattern: &str) -> Self {
        PyColumn {
            inner: regexp_substr(&self.inner, pattern),
        }
    }

    /// Split by delimiter and return 1-based part (PySpark split_part).
    fn split_part(&self, delimiter: &str, part_num: i64) -> Self {
        PyColumn {
            inner: split_part(&self.inner, delimiter, part_num),
        }
    }

    /// 1-based index in comma-delimited set (PySpark find_in_set).
    fn find_in_set(&self, set_col: &PyColumn) -> Self {
        PyColumn {
            inner: find_in_set(&self.inner, &set_col.inner),
        }
    }

    // Phase 17: datetime/unix and math
    fn unix_timestamp(&self, format: Option<&str>) -> Self {
        PyColumn {
            inner: unix_timestamp(&self.inner, format),
        }
    }
    fn from_unixtime(&self, format: Option<&str>) -> Self {
        PyColumn {
            inner: from_unixtime(&self.inner, format),
        }
    }
    fn timestamp_seconds(&self) -> Self {
        PyColumn {
            inner: timestamp_seconds(&self.inner),
        }
    }
    fn timestamp_millis(&self) -> Self {
        PyColumn {
            inner: timestamp_millis(&self.inner),
        }
    }
    fn timestamp_micros(&self) -> Self {
        PyColumn {
            inner: timestamp_micros(&self.inner),
        }
    }
    fn unix_date(&self) -> Self {
        PyColumn {
            inner: unix_date(&self.inner),
        }
    }
    fn date_from_unix_date(&self) -> Self {
        PyColumn {
            inner: date_from_unix_date(&self.inner),
        }
    }
    fn pmod(&self, divisor: &PyColumn) -> Self {
        PyColumn {
            inner: pmod(&self.inner, &divisor.inner),
        }
    }
    fn factorial(&self) -> Self {
        PyColumn {
            inner: factorial(&self.inner),
        }
    }
}

/// Python wrapper for WhenBuilder (when(cond).then(val).otherwise(val)).
#[pyclass(name = "WhenBuilder")]
struct PyWhenBuilder {
    condition: Expr,
}

#[pymethods]
impl PyWhenBuilder {
    fn then(&self, value: &PyColumn) -> PyThenBuilder {
        let when_then =
            polars::prelude::when(self.condition.clone()).then(value.inner.expr().clone());
        PyThenBuilder { when_then }
    }
}

/// Python wrapper for ThenBuilder (.otherwise(val)).
#[pyclass(name = "ThenBuilder")]
struct PyThenBuilder {
    when_then: polars::prelude::Then,
}

#[pymethods]
impl PyThenBuilder {
    fn otherwise(&self, value: &PyColumn) -> PyColumn {
        let expr = self.when_then.clone().otherwise(value.inner.expr().clone());
        PyColumn {
            inner: RsColumn::from_expr(expr, None),
        }
    }
}

/// Python wrapper for SparkSession.
#[pyclass(name = "SparkSession")]
struct PySparkSession {
    inner: SparkSession,
}

#[pymethods]
impl PySparkSession {
    #[new]
    fn new() -> Self {
        PySparkSession {
            inner: SparkSession::new(None, None, std::collections::HashMap::new()),
        }
    }

    /// Create a SparkSession via builder().app_name(...).get_or_create()
    #[classmethod]
    fn builder(_cls: &Bound<'_, pyo3::types::PyType>) -> PyResult<PySparkSessionBuilder> {
        Ok(PySparkSessionBuilder {
            app_name: None,
            master: None,
            config: std::collections::HashMap::new(),
        })
    }

    fn is_case_sensitive(&self) -> bool {
        self.inner.is_case_sensitive()
    }

    /// Create a DataFrame from a list of 3-tuples (id, age, name) and column names.
    /// data: list of (int, int, str), column_names: list of 3 strings e.g. ["id", "age", "name"].
    fn create_dataframe(
        &self,
        _py: Python<'_>,
        data: &Bound<'_, pyo3::types::PyAny>,
        column_names: Vec<String>,
    ) -> PyResult<PyDataFrame> {
        let data_rust: Vec<(i64, i64, String)> = data
            .extract()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let names_ref: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .create_dataframe(data_rust, names_ref)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Create a DataFrame from a list of dicts (or list of lists) and a schema.
    /// schema: list of (name, dtype_str) e.g. [("id", "bigint"), ("name", "string")].
    /// data: list of dicts (keys = column names) or list of lists (values in schema order).
    fn create_dataframe_from_rows(
        &self,
        py: Python<'_>,
        data: &Bound<'_, pyo3::types::PyAny>,
        schema: Vec<(String, String)>,
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
                    "create_dataframe_from_rows: each row must be a dict or a list",
                ));
            }
        }
        let df = self
            .inner
            .create_dataframe_from_rows(rows, schema)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn read_csv(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_csv(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn read_parquet(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_parquet(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn read_json(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_json(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(feature = "sql")]
    fn create_or_replace_temp_view(&self, name: &str, df: &PyDataFrame) {
        self.inner
            .create_or_replace_temp_view(name, df.inner.clone());
    }

    #[cfg(feature = "sql")]
    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .table(name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(feature = "sql")]
    fn sql(&self, query: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sql(query)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(feature = "delta")]
    fn read_delta(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_delta(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(feature = "delta")]
    fn read_delta_version(&self, path: &str, version: Option<i64>) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_delta_with_version(Path::new(path), version)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for SparkSessionBuilder.
#[pyclass(name = "SparkSessionBuilder")]
struct PySparkSessionBuilder {
    app_name: Option<String>,
    master: Option<String>,
    config: std::collections::HashMap<String, String>,
}

#[pymethods]
impl PySparkSessionBuilder {
    fn app_name<'a>(mut slf: PyRefMut<'a, Self>, name: &str) -> PyRefMut<'a, Self> {
        slf.app_name = Some(name.to_string());
        slf
    }

    fn master<'a>(mut slf: PyRefMut<'a, Self>, master: &str) -> PyRefMut<'a, Self> {
        slf.master = Some(master.to_string());
        slf
    }

    fn config<'a>(mut slf: PyRefMut<'a, Self>, key: &str, value: &str) -> PyRefMut<'a, Self> {
        slf.config.insert(key.to_string(), value.to_string());
        slf
    }

    fn get_or_create(slf: PyRef<'_, Self>) -> PySparkSession {
        let mut config = std::collections::HashMap::new();
        for (k, v) in &slf.config {
            config.insert(k.clone(), v.clone());
        }
        let inner = SparkSession::new(slf.app_name.clone(), slf.master.clone(), config);
        PySparkSession { inner }
    }
}

/// Python wrapper for DataFrame.
#[pyclass(name = "DataFrame")]
struct PyDataFrame {
    inner: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    fn count(&self) -> PyResult<usize> {
        self.inner
            .count()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    #[pyo3(signature = (n=None))]
    fn show(&self, n: Option<usize>) -> PyResult<()> {
        self.inner
            .show(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    #[cfg(feature = "delta")]
    fn write_delta(&self, path: &str, overwrite: bool) -> PyResult<()> {
        self.inner
            .write_delta(Path::new(path), overwrite)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Collect as list of dicts (column name -> value per row).
    fn collect(&self, py: Python<'_>) -> PyResult<PyObject> {
        let pl_df = self
            .inner
            .collect()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let df = pl_df.as_ref();
        let names = df.get_column_names();
        let nrows = df.height();
        let rows = pyo3::types::PyList::empty(py);
        for i in 0..nrows {
            let row_dict = PyDict::new(py);
            for (col_idx, name) in names.iter().enumerate() {
                let s = df.get_columns().get(col_idx).ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("column index out of range")
                })?;
                let av = s
                    .get(i)
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                let py_val = any_value_to_py(py, av)?;
                row_dict.set_item(name.as_str(), py_val)?;
            }
            rows.append(&*row_dict)?;
        }
        Ok(rows.into())
    }

    fn filter(&self, condition: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .filter(condition.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn select(&self, cols: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .select(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn with_column(&self, column_name: &str, expr: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .with_column(column_name, &expr.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(signature = (cols, ascending=None))]
    fn order_by(&self, cols: Vec<String>, ascending: Option<Vec<bool>>) -> PyResult<PyDataFrame> {
        let asc = ascending.unwrap_or_else(|| vec![true; cols.len()]);
        let df = self
            .inner
            .order_by(cols.iter().map(|s| s.as_str()).collect::<Vec<_>>(), asc)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn order_by_exprs(&self, sort_orders: Vec<PySortOrder>) -> PyResult<PyDataFrame> {
        let orders: Vec<SortOrder> = sort_orders.into_iter().map(|po| po.inner).collect();
        let df = self
            .inner
            .order_by_exprs(orders)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn group_by(&self, cols: Vec<String>) -> PyResult<PyGroupedData> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let gd = self
            .inner
            .group_by(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyGroupedData { inner: gd })
    }

    fn cube(&self, cols: Vec<String>) -> PyResult<PyCubeRollupData> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let cr = self
            .inner
            .cube(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyCubeRollupData { inner: cr })
    }

    fn rollup(&self, cols: Vec<String>) -> PyResult<PyCubeRollupData> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let cr = self
            .inner
            .rollup(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyCubeRollupData { inner: cr })
    }

    fn write(&self) -> PyDataFrameWriter {
        PyDataFrameWriter {
            df: self.inner.clone(),
            mode: RwLock::new(WriteMode::Overwrite),
            format: RwLock::new(WriteFormat::Parquet),
        }
    }

    #[pyo3(signature = (other, on, how="inner"))]
    fn join(&self, other: &PyDataFrame, on: Vec<String>, how: &str) -> PyResult<PyDataFrame> {
        let join_type = match how.to_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "outer" => JoinType::Outer,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "how must be 'inner', 'left', 'right', or 'outer'",
                ));
            }
        };
        let on_refs: Vec<&str> = on.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .join(&other.inner, on_refs, join_type)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn union(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .union(&other.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn union_by_name(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .union_by_name(&other.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(signature = (subset=None))]
    fn distinct(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .inner
            .distinct(sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn drop(&self, cols: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .drop(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(signature = (subset=None))]
    fn dropna(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .inner
            .dropna(sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn fillna(&self, value: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .fillna(value.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn limit(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .limit(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn with_column_renamed(&self, old: &str, new: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .with_column_renamed(old, new)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    // Phase 12: sample, first, head, tail, take, is_empty, to_json, explain, print_schema, checkpoint, repartition, coalesce, offset
    #[pyo3(signature = (with_replacement=false, fraction=1.0, seed=None))]
    fn sample(
        &self,
        with_replacement: bool,
        fraction: f64,
        seed: Option<u64>,
    ) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sample(with_replacement, fraction, seed)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn first(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .first()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn head(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .head(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn tail(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .tail(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn take(&self, n: usize) -> PyResult<PyDataFrame> {
        self.head(n)
    }

    fn is_empty(&self) -> PyResult<bool> {
        Ok(self.inner.is_empty())
    }

    fn to_json(&self) -> PyResult<Vec<String>> {
        self.inner
            .to_json()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    fn explain(&self) -> PyResult<String> {
        Ok(self.inner.explain())
    }

    fn print_schema(&self) -> PyResult<String> {
        self.inner
            .print_schema()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    fn checkpoint(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .checkpoint()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn local_checkpoint(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .local_checkpoint()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn repartition(&self, num_partitions: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .repartition(num_partitions)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn coalesce(&self, num_partitions: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .coalesce(num_partitions)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn offset(&self, n: usize) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .offset(n)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Best-effort local collection: returns list of rows (same as collect()). PySpark .data.
    fn data(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.collect(py)
    }

    fn persist(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .persist()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn unpersist(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .unpersist()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Stub: RDD API is not supported. Raises NotImplementedError.
    fn rdd(&self) -> PyResult<PyObject> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "RDD is not supported in Sparkless; use collect() or toLocalIterator() for local data",
        ))
    }

    /// Stub: foreach is not supported (no distributed execution). Raises NotImplementedError.
    fn foreach(&self, _f: &Bound<'_, pyo3::types::PyAny>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "foreach is not supported in Sparkless",
        ))
    }

    /// Stub: foreachPartition is not supported. Raises NotImplementedError.
    #[pyo3(name = "foreachPartition")]
    fn foreach_partition(&self, _f: &Bound<'_, pyo3::types::PyAny>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "foreachPartition is not supported in Sparkless",
        ))
    }

    /// Stub: mapInPandas is not supported. Raises NotImplementedError.
    #[pyo3(name = "mapInPandas")]
    fn map_in_pandas(
        &self,
        _func: &Bound<'_, pyo3::types::PyAny>,
        _schema: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyDataFrame> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "mapInPandas is not supported in Sparkless",
        ))
    }

    /// Stub: mapPartitions is not supported. Raises NotImplementedError.
    #[pyo3(name = "mapPartitions")]
    fn map_partitions(
        &self,
        _f: &Bound<'_, pyo3::types::PyAny>,
        _schema: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyDataFrame> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "mapPartitions is not supported in Sparkless",
        ))
    }

    /// Returns an iterable of rows (same as collect()). Best-effort local iterator.
    #[pyo3(name = "toLocalIterator")]
    fn to_local_iterator(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.collect(py)
    }

    /// Stub: storage level not applicable (eager execution). Returns None.
    #[pyo3(name = "storageLevel")]
    fn storage_level(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(py.None())
    }

    /// Always False; streaming is not supported.
    #[pyo3(name = "isStreaming")]
    fn is_streaming(&self) -> PyResult<bool> {
        Ok(false)
    }

    /// No-op; streaming/watermark not supported. Returns self for chaining.
    #[pyo3(name = "withWatermark")]
    fn with_watermark(&self, _event_time: &str, _delay_threshold: &str) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame {
            inner: self.inner.clone(),
        })
    }

    fn random_split(&self, weights: Vec<f64>, seed: Option<u64>) -> PyResult<Vec<PyDataFrame>> {
        let dfs = self
            .inner
            .random_split(&weights, seed)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(dfs
            .into_iter()
            .map(|df| PyDataFrame { inner: df })
            .collect())
    }

    fn summary(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .summary()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn to_df(&self, names: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let df = self
            .inner
            .to_df(refs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn select_expr(&self, exprs: Vec<String>) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .select_expr(&exprs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn col_regex(&self, pattern: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .col_regex(pattern)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn with_columns(&self, mapping: &Bound<'_, pyo3::types::PyAny>) -> PyResult<PyDataFrame> {
        let mut exprs: Vec<(String, crate::column::Column)> = Vec::new();
        if let Ok(dict) = mapping.downcast::<PyDict>() {
            for (k, v) in dict.iter() {
                let name: String = k.extract()?;
                let col: PyRef<PyColumn> = v.extract()?;
                exprs.push((name, col.inner.clone()));
            }
        } else if let Ok(list) = mapping.downcast::<pyo3::types::PyList>() {
            for item in list.iter() {
                let tuple: (String, PyRef<PyColumn>) = item.extract()?;
                exprs.push((tuple.0, tuple.1.inner.clone()));
            }
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "with_columns expects dict[str, Column] or list[tuple[str, Column]]",
            ));
        }
        let df = self
            .inner
            .with_columns(&exprs)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn with_columns_renamed(
        &self,
        mapping: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyDataFrame> {
        let mut renames: Vec<(String, String)> = Vec::new();
        if let Ok(dict) = mapping.downcast::<PyDict>() {
            for (k, v) in dict.iter() {
                let old_name: String = k.extract()?;
                let new_name: String = v.extract()?;
                renames.push((old_name, new_name));
            }
        } else if let Ok(list) = mapping.downcast::<pyo3::types::PyList>() {
            for item in list.iter() {
                let tuple: (String, String) = item.extract()?;
                renames.push(tuple);
            }
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "with_columns_renamed expects dict[str, str] or list[tuple[str, str]]",
            ));
        }
        let df = self
            .inner
            .with_columns_renamed(&renames)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn stat(&self) -> PyDataFrameStat {
        PyDataFrameStat {
            df: self.inner.clone(),
        }
    }

    /// Correlation matrix of all numeric columns. Returns a DataFrame of pairwise correlations.
    fn corr(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .corr()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn na(&self) -> PyDataFrameNa {
        PyDataFrameNa {
            df: self.inner.clone(),
        }
    }

    fn to_pandas(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.collect(py)
    }
}

/// Python wrapper for DataFrame.stat() (cov, corr).
#[pyclass(name = "DataFrameStat")]
struct PyDataFrameStat {
    df: DataFrame,
}

#[pymethods]
impl PyDataFrameStat {
    fn cov(&self, col1: &str, col2: &str) -> PyResult<f64> {
        self.df
            .stat()
            .cov(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    fn corr(&self, col1: &str, col2: &str) -> PyResult<f64> {
        self.df
            .stat()
            .corr(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }

    /// Correlation matrix of all numeric columns. Returns a DataFrame.
    fn corr_matrix(&self) -> PyResult<PyDataFrame> {
        let df = self
            .df
            .stat()
            .corr_matrix()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for DataFrame.na() (fill, drop).
#[pyclass(name = "DataFrameNa")]
struct PyDataFrameNa {
    df: DataFrame,
}

#[pymethods]
impl PyDataFrameNa {
    fn fill(&self, value: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self
            .df
            .na()
            .fill(value.inner.expr().clone())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(signature = (subset=None))]
    fn drop(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_str()).collect());
        let df = self
            .df
            .na()
            .drop(sub)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for GroupedData.
#[pyclass(name = "GroupedData")]
struct PyGroupedData {
    inner: GroupedData,
}

#[pymethods]
impl PyGroupedData {
    fn count(&self) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .count()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn sum(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sum(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn avg(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .avg(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(name = "min")]
    fn min_(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .min(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn max(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .max(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn agg(&self, exprs: Vec<PyRef<PyColumn>>) -> PyResult<PyDataFrame> {
        let aggregations: Vec<Expr> = exprs.iter().map(|c| c.inner.expr().clone()).collect();
        let df = self
            .inner
            .agg(aggregations)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn any_value(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .any_value(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn bool_and(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .bool_and(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn bool_or(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .bool_or(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn product(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .product(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn collect_list(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .collect_list(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn collect_set(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .collect_set(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn count_if(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .count_if(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn percentile(&self, column: &str, p: f64) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .percentile(column, p)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn max_by(&self, value_column: &str, ord_column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .max_by(value_column, ord_column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn min_by(&self, value_column: &str, ord_column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .min_by(value_column, ord_column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn covar_pop(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .covar_pop(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn covar_samp(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .covar_samp(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn corr(&self, col1: &str, col2: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .corr(col1, col2)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_count(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_count(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_avgx(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_avgx(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_avgy(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_avgy(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_slope(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_slope(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_intercept(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_intercept(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_r2(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_r2(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_sxx(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_sxx(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_syy(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_syy(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn regr_sxy(&self, y_col: &str, x_col: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .regr_sxy(y_col, x_col)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn kurtosis(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .kurtosis(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    fn skewness(&self, column: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .skewness(column)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for CubeRollupData (cube/rollup then agg).
#[pyclass(name = "CubeRollupData")]
struct PyCubeRollupData {
    inner: CubeRollupData,
}

#[pymethods]
impl PyCubeRollupData {
    fn agg(&self, exprs: Vec<PyRef<PyColumn>>) -> PyResult<PyDataFrame> {
        let aggregations: Vec<Expr> = exprs.iter().map(|c| c.inner.expr().clone()).collect();
        let df = self
            .inner
            .agg(aggregations)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for DataFrameWriter (write().mode().format().save()).
#[pyclass(name = "DataFrameWriter")]
struct PyDataFrameWriter {
    df: crate::DataFrame,
    mode: RwLock<WriteMode>,
    format: RwLock<WriteFormat>,
}

#[pymethods]
impl PyDataFrameWriter {
    fn mode<'py>(slf: PyRef<'py, Self>, mode: &str) -> PyRef<'py, Self> {
        *slf.mode.write().unwrap() = match mode.to_lowercase().as_str() {
            "append" => WriteMode::Append,
            _ => WriteMode::Overwrite,
        };
        slf
    }

    fn format<'py>(slf: PyRef<'py, Self>, format: &str) -> PyRef<'py, Self> {
        *slf.format.write().unwrap() = match format.to_lowercase().as_str() {
            "csv" => WriteFormat::Csv,
            "json" => WriteFormat::Json,
            _ => WriteFormat::Parquet,
        };
        slf
    }

    fn save(&self, path: &str) -> PyResult<()> {
        let mode = *self.mode.read().unwrap();
        let format = *self.format.read().unwrap();
        self.df
            .write()
            .mode(mode)
            .format(format)
            .save(Path::new(path))
            .map_err(|e: polars::prelude::PolarsError| {
                pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
            })
    }
}

/// Convert Polars AnyValue to a Python object.
fn any_value_to_py(py: Python<'_>, av: polars::prelude::AnyValue<'_>) -> PyResult<PyObject> {
    use polars::prelude::AnyValue;
    use pyo3::conversion::IntoPyObjectExt;
    match av {
        AnyValue::Null => py.None().into_bound_py_any(py).map(Into::into),
        AnyValue::Boolean(b) => b.into_bound_py_any(py).map(Into::into),
        AnyValue::Int32(i) => i.into_bound_py_any(py).map(Into::into),
        AnyValue::Int64(i) => i.into_bound_py_any(py).map(Into::into),
        AnyValue::UInt32(u) => u.into_bound_py_any(py).map(Into::into),
        AnyValue::UInt64(u) => u.into_bound_py_any(py).map(Into::into),
        AnyValue::Float32(f) => f.into_bound_py_any(py).map(Into::into),
        AnyValue::Float64(f) => f.into_bound_py_any(py).map(Into::into),
        AnyValue::String(s) => s.to_string().into_bound_py_any(py).map(Into::into),
        AnyValue::StringOwned(s) => s.to_string().into_bound_py_any(py).map(Into::into),
        other => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
            "unsupported type for collect: {:?}",
            other
        ))),
    }
}
