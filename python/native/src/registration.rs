//! PyO3 module registration for the public native API.

use super::*;

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("SparklessError", m.py().get_type::<SparklessError>())?;
    m.add(
        "SparklessCancelledError",
        m.py().get_type::<SparklessCancelledError>(),
    )?;
    let external_catalog_error = m.py().get_type::<SparklessExternalCatalogError>();
    for field in [
        "operation",
        "category",
        "code",
        "identifier",
        "version",
        "expected_version",
        "actual_version",
        "retryability",
        "provider_error_type",
    ] {
        external_catalog_error.setattr(field, m.py().None())?;
    }
    m.add("SparklessExternalCatalogError", external_catalog_error)?;
    m.add_function(wrap_pyfunction!(external_catalog::validate_descriptor, m)?)?;
    m.add_function(wrap_pyfunction!(
        external_catalog::validate_write_result,
        m
    )?)?;
    m.add_class::<PySparkSessionBuilder>()?;
    m.add_class::<PyCancellationHandle>()?;
    m.add_class::<PySparkSession>()?;
    m.add_class::<PyDataFrameReader>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyArrowBatchIterator>()?;
    m.add_class::<external_catalog::PyArrowIpcBatchIterator>()?;
    m.add_class::<PyRDD>()?;
    m.add_class::<PyDataFrameWriter>()?;
    m.add_class::<PyStarColumns>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PyUdfFilterColumn>()?;
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
    m.add_function(wrap_pyfunction!(make_udf_filter_column, m)?)?;
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
    m.add_function(wrap_pyfunction!(array_compact, m)?)?;
    m.add_function(wrap_pyfunction!(posexplode, m)?)?;
    m.add_function(wrap_pyfunction!(posexplode_outer, m)?)?;
    m.add_function(wrap_pyfunction!(to_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(to_timestamp_ltz, m)?)?;
    m.add_function(wrap_pyfunction!(to_timestamp_ntz, m)?)?;
    m.add_function(wrap_pyfunction!(to_date, m)?)?;
    m.add_function(wrap_pyfunction!(current_date, m)?)?;
    m.add_function(wrap_pyfunction!(current_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(monotonically_increasing_id, m)?)?;
    m.add_function(wrap_pyfunction!(spark_partition_id, m)?)?;
    m.add_function(wrap_pyfunction!(percentile_approx, m)?)?;
    m.add_function(wrap_pyfunction!(schema_of_json, m)?)?;
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
    m.add_function(wrap_pyfunction!(native_add_months, m)?)?;
    m.add_function(wrap_pyfunction!(make_date, m)?)?;
    m.add_function(wrap_pyfunction!(abs, m)?)?;
    m.add_function(wrap_pyfunction!(sqrt, m)?)?;
    m.add_function(wrap_pyfunction!(log, m)?)?;
    m.add_function(wrap_pyfunction!(log_with_base, m)?)?;
    m.add_function(wrap_pyfunction!(exp, m)?)?;
    m.add_function(wrap_pyfunction!(pow, m)?)?;
    m.add_function(wrap_pyfunction!(signum, m)?)?;
    m.add_function(wrap_pyfunction!(pmod, m)?)?;
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
    m.add_function(wrap_pyfunction!(sum_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(corr, m)?)?;
    m.add_function(wrap_pyfunction!(covar_pop, m)?)?;
    m.add_function(wrap_pyfunction!(covar_samp, m)?)?;
    m.add_function(wrap_pyfunction!(explode_outer, m)?)?;
    m.add_function(wrap_pyfunction!(element_at, m)?)?;
    m.add_function(wrap_pyfunction!(array_sort, m)?)?;
    m.add_function(wrap_pyfunction!(array_slice, m)?)?;
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
    m.add_function(wrap_pyfunction!(overlay, m)?)?;
    m.add_function(wrap_pyfunction!(factorial, m)?)?;
    m.add_function(wrap_pyfunction!(cbrt, m)?)?;
    m.add_function(wrap_pyfunction!(hypot, m)?)?;
    m.add_function(wrap_pyfunction!(map_from_arrays, m)?)?;
    m.add_function(wrap_pyfunction!(encode, m)?)?;
    m.add_function(wrap_pyfunction!(decode, m)?)?;
    m.add_function(wrap_pyfunction!(spark_char, m)?)?;
    m.add_function(wrap_pyfunction!(to_json, m)?)?;
    m.add_function(wrap_pyfunction!(from_json, m)?)?;
    m.add_function(wrap_pyfunction!(parse_json, m)?)?;
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
    m.add_function(wrap_pyfunction!(mode, m)?)?;
    m.add_function(wrap_pyfunction!(date_trunc, m)?)?;
    m.add_function(wrap_pyfunction!(native_next_day, m)?)?;
    m.add_function(wrap_pyfunction!(native_trunc, m)?)?;
    m.add_function(wrap_pyfunction!(first, m)?)?;
    m.add_function(wrap_pyfunction!(last, m)?)?;
    m.add_function(wrap_pyfunction!(translate, m)?)?;
    m.add_function(wrap_pyfunction!(native_instr, m)?)?;
    m.add_function(wrap_pyfunction!(native_locate, m)?)?;
    m.add_function(wrap_pyfunction!(substring_index, m)?)?;
    m.add_function(wrap_pyfunction!(native_crc32, m)?)?;
    m.add_function(wrap_pyfunction!(native_xxhash64, m)?)?;
    m.add_function(wrap_pyfunction!(native_hash, m)?)?;
    m.add_function(wrap_pyfunction!(native_typeof, m)?)?;
    m.add_function(wrap_pyfunction!(get_json_object, m)?)?;
    m.add_function(wrap_pyfunction!(json_tuple, m)?)?;
    m.add_function(wrap_pyfunction!(size, m)?)?;
    m.add_function(wrap_pyfunction!(array_contains, m)?)?;
    m.add_function(wrap_pyfunction!(arrays_overlap, m)?)?;
    m.add_function(wrap_pyfunction!(explode, m)?)?;
    m.add_function(wrap_pyfunction!(configure_for_multiprocessing, m)?)?;
    // #1267: define Python helper in this module so createDataFrame list-of-dicts can convert in Python.
    let py = m.py();
    let code = "def _cdf_dict_rows_to_lists(data, keys):\n    return [[d.get(k) for k in keys] for d in data]";
    if let Ok(builtins) = py.import("builtins") {
        if let Ok(globals) = m.getattr("__dict__") {
            let _ = builtins
                .getattr("exec")
                .and_then(|e| e.call1((code, &globals)));
        }
    }
    Ok(())
}
