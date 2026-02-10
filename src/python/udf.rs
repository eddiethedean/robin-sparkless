//! Python UDF execution: row-at-a-time call into user Python functions.

use crate::column::Column as RsColumn;
use crate::dataframe::DataFrame;
use crate::session::SparkSession;
use crate::udf_registry::PythonUdfKind;
use polars::prelude::*;
use pyo3::prelude::*;

/// Execute a Python UDF: materialize args, call Python row-by-row, return new DataFrame.
pub(crate) fn execute_python_udf(
    df: &DataFrame,
    column_name: &str,
    udf_name: &str,
    args: &[RsColumn],
    case_sensitive: bool,
    session: &SparkSession,
) -> Result<DataFrame, PolarsError> {
    let entry = session
        .udf_registry
        .get_python_udf(udf_name, session.is_case_sensitive())
        .ok_or_else(|| {
            PolarsError::InvalidOperation(format!("Python UDF '{udf_name}' not found").into())
        })?;
    let kind = entry.kind;
    let return_type = entry.return_type.clone();

    // Evaluate arg expressions in df context
    let lf = df.df.as_ref().clone().lazy();
    let mut arg_exprs = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        arg_exprs.push(arg.expr().clone().alias(format!("_udf_arg_{i}")));
    }
    let lf_with_args = lf.with_columns(arg_exprs);
    let pl_df = lf_with_args.collect()?;

    let n_total = pl_df.height();
    let arg_series: Vec<Series> = (0..args.len())
        .map(|i| {
            let name = format!("_udf_arg_{i}");
            pl_df
                .column(&name)
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
                .as_series()
                .cloned()
                .ok_or_else(|| PolarsError::ComputeError(format!("udf arg {i} not found").into()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let result_series = Python::with_gil(|py| {
        let callable = entry.callable.bind(py);
        match kind {
            PythonUdfKind::Scalar => {
                // Existing row-at-a-time execution.
                let mut results: Vec<Option<PyObject>> = Vec::with_capacity(n_total);

                for row_idx in 0..n_total {
                    // Build Python args for this row (tuple for *args)
                    let mut py_row = Vec::with_capacity(args.len());
                    for series in &arg_series {
                        let av = series.get(row_idx).map_err(|e| {
                            PolarsError::ComputeError(format!("udf get row: {e}").into())
                        })?;
                        py_row.push(any_value_to_py(py, &av)?);
                    }

                    // Call UDF(*args) - pass tuple of row values
                    let args_tuple =
                        pyo3::types::PyTuple::new(py, py_row.iter().map(|o| o.bind(py))).map_err(
                            |e| {
                                PolarsError::ComputeError(
                                    format!("Python UDF '{udf_name}' tuple: {e}").into(),
                                )
                            },
                        )?;
                    let ret = callable.call1(args_tuple).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("Python UDF '{udf_name}' failed at row {row_idx}: {e}").into(),
                        )
                    })?;

                    results.push(if ret.is_none() {
                        None
                    } else {
                        Some(ret.into_any().unbind())
                    });
                }

                py_result_to_series(py, &results, &return_type, column_name)
            }
            PythonUdfKind::Vectorized => {
                // Vectorized execution: call UDF once per batch of column values.
                // Batch size and concurrency are controlled by session config.
                let batch_size = session.python_udf_batch_size;
                let total = n_total;
                let mut all_results: Vec<Option<PyObject>> = Vec::with_capacity(total);

                let mut offset = 0usize;
                while offset < total {
                    let end = std::cmp::min(offset + batch_size, total);
                    let batch_len = end - offset;

                    // Build Python arg lists (one list per input column) for this batch.
                    let mut py_args: Vec<PyObject> = Vec::with_capacity(args.len());
                    for series in &arg_series {
                        let mut values: Vec<PyObject> = Vec::with_capacity(batch_len);
                        for row_idx in offset..end {
                            let av = series.get(row_idx).map_err(|e| {
                                PolarsError::ComputeError(format!("udf get row: {e}").into())
                            })?;
                            values.push(any_value_to_py(py, &av)?);
                        }
                        let py_list =
                            pyo3::types::PyList::new(py, values.iter().map(|o| o.bind(py)))
                                .map_err(|e| {
                                    PolarsError::ComputeError(
                                        format!("Python UDF '{udf_name}' list: {e}").into(),
                                    )
                                })?;
                        py_args.push(py_list.into_py(py));
                    }

                    // Call UDF once with lists of values for this batch.
                    let args_tuple =
                        pyo3::types::PyTuple::new(py, py_args.iter().map(|o| o.bind(py)))
                            .map_err(|e| {
                                PolarsError::ComputeError(
                                    format!("Python UDF '{udf_name}' tuple: {e}").into(),
                                )
                            })?;
                    let ret = callable.call1(args_tuple).map_err(|e| {
                        PolarsError::ComputeError(
                            format!(
                                "Python UDF '{udf_name}' failed (vectorized batch starting at {}): {e}",
                                offset
                            )
                            .into(),
                        )
                    })?;

                    // Interpret return value as a sequence of row results for this batch.
                    let seq = ret.iter().map_err(|e| {
                        PolarsError::ComputeError(
                            format!(
                                "Python UDF '{udf_name}' vectorized result not iterable: {e}"
                            )
                            .into(),
                        )
                    })?;
                    let mut batch_results: Vec<Option<PyObject>> =
                        Vec::with_capacity(batch_len);
                    for (idx, res) in seq.enumerate() {
                        let item = res.map_err(|e| {
                            PolarsError::ComputeError(
                                format!(
                                    "Python UDF '{udf_name}' vectorized result at index {} in batch starting at {}: {e}",
                                    idx, offset
                                )
                                .into(),
                            )
                        })?;
                        if item.is_none() {
                            batch_results.push(None);
                        } else {
                            batch_results.push(Some(item.into_any().unbind()));
                        }
                    }

                    if batch_results.len() != batch_len {
                        return Err(PolarsError::ComputeError(
                            format!(
                                "Python UDF '{udf_name}' vectorized result length {} does not match input rows {} in batch starting at {}",
                                batch_results.len(),
                                batch_len,
                                offset
                            )
                            .into(),
                        ));
                    }

                    all_results.extend(batch_results.into_iter());
                    offset = end;
                }

                if all_results.len() != total {
                    return Err(PolarsError::ComputeError(
                        format!(
                            "Python UDF '{udf_name}' total result length {} does not match input rows {}",
                            all_results.len(),
                            total
                        )
                        .into(),
                    ));
                }

                py_result_to_series(py, &all_results, &return_type, column_name)
            }
            PythonUdfKind::GroupedVectorizedAgg => Err(PolarsError::ComputeError(
                "Grouped vectorized Python UDFs are only supported in groupBy().agg(...). Use pandas_udf(..., function_type='grouped_agg') within group_by().agg()."
                    .into(),
            )),
        }
    })?;

    // Drop temp columns and add result
    let col_names: Vec<&str> = pl_df
        .get_column_names()
        .iter()
        .filter(|n| !n.starts_with("_udf_arg_"))
        .map(|s| s.as_str())
        .collect();
    let mut out_df = pl_df.select(col_names)?;
    out_df.with_column(result_series)?;

    Ok(DataFrame::from_polars_with_options(out_df, case_sensitive))
}

/// Execute one or more grouped, vectorized aggregation Python UDFs over groups defined by
/// `grouping_cols`. Each UDF is called once per group with Python lists of values for each
/// argument column and must return exactly one value per group.
///
/// The result DataFrame has one row per group, containing the grouping key columns from the
/// underlying DataFrame plus one column per aggregation specification.
#[cfg(feature = "pyo3")]
pub(crate) struct GroupedAggSpec {
    pub output_name: String,
    pub udf_name: String,
    pub args: Vec<RsColumn>,
}

#[cfg(feature = "pyo3")]
pub(crate) fn execute_grouped_vectorized_aggs(
    df: &DataFrame,
    grouping_cols: &[String],
    specs: &[GroupedAggSpec],
    case_sensitive: bool,
    session: &SparkSession,
) -> Result<DataFrame, PolarsError> {
    if specs.is_empty() {
        // No UDF aggs: just return distinct groups.
        let lf = df
            .df
            .as_ref()
            .clone()
            .lazy()
            .group_by(
                grouping_cols
                    .iter()
                    .map(|c| col(c.as_str()))
                    .collect::<Vec<_>>(),
            )
            .agg([len().alias("_count_for_groups_only")]);
        let mut pl_df = lf.collect()?;
        // Drop helper count column
        let _ = pl_df.drop_in_place("_count_for_groups_only");
        return Ok(DataFrame::from_polars_with_options(pl_df, case_sensitive));
    }

    // Build lazy group-by with imploded argument columns for all specs/args.
    let mut arg_exprs: Vec<Expr> = Vec::new();
    for (spec_idx, spec) in specs.iter().enumerate() {
        for (arg_idx, arg) in spec.args.iter().enumerate() {
            let alias = format!("_pyg_udf_{}_arg_{}", spec_idx, arg_idx);
            arg_exprs.push(arg.expr().clone().implode().alias(&alias));
        }
    }

    let lf = df
        .df
        .as_ref()
        .clone()
        .lazy()
        .group_by(
            grouping_cols
                .iter()
                .map(|c| col(c.as_str()))
                .collect::<Vec<_>>(),
        )
        .agg(arg_exprs);
    let mut pl_df = lf.collect()?;

    let n_groups = pl_df.height();

    // For each spec, call into Python per group and build a Series of results.
    let mut result_series: Vec<Series> = Vec::with_capacity(specs.len());
    Python::with_gil(|py| -> Result<(), PolarsError> {
        for (spec_idx, spec) in specs.iter().enumerate() {
            // Look up UDF entry and ensure it's grouped-vectorized.
            let entry = session
                .udf_registry
                .get_python_udf(&spec.udf_name, session.is_case_sensitive())
                .ok_or_else(|| {
                    PolarsError::InvalidOperation(
                        format!("Python grouped UDF '{}' not found", spec.udf_name).into(),
                    )
                })?;
            if entry.kind != PythonUdfKind::GroupedVectorizedAgg {
                return Err(PolarsError::InvalidOperation(
                    format!(
                        "Python UDF '{}' is not registered as grouped_agg (use pandas_udf(..., function_type='grouped_agg'))",
                        spec.udf_name
                    )
                    .into(),
                ));
            }

            let callable = entry.callable.bind(py);
            let return_type = entry.return_type.clone();

            // Pre-fetch all list-valued Series for this spec's arguments.
            // Each column is a List Series with one list of values per group.
            let mut list_series_per_arg: Vec<Series> = Vec::with_capacity(spec.args.len());
            for (arg_idx, _arg) in spec.args.iter().enumerate() {
                let alias = format!("_pyg_udf_{}_arg_{}", spec_idx, arg_idx);
                let s = pl_df
                    .column(&alias)
                    .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?
                    .as_series()
                    .cloned()
                    .ok_or_else(|| {
                        PolarsError::ComputeError(
                            format!("grouped UDF arg column '{alias}' not found").into(),
                        )
                    })?;
                list_series_per_arg.push(s);
            }

            let mut per_group_results: Vec<Option<PyObject>> = Vec::with_capacity(n_groups);

            for group_idx in 0..n_groups {
                // Build Python args: one list per argument.
                let mut py_args: Vec<PyObject> = Vec::with_capacity(list_series_per_arg.len());
                for s in &list_series_per_arg {
                    let av = s.get(group_idx).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("grouped UDF list get failed for group {group_idx}: {e}")
                                .into(),
                        )
                    })?;
                    if matches!(av, AnyValue::Null) {
                        // Empty group: pass empty list.
                        let empty = pyo3::types::PyList::empty(py);
                        py_args.push(empty.into_py(py));
                        continue;
                    }
                    let inner = match av {
                        AnyValue::List(ref inner_series) => inner_series.clone(),
                        _ => {
                            return Err(PolarsError::ComputeError(
                                "grouped UDF internal error: expected list AnyValue".into(),
                            ))
                        }
                    };
                    let mut values: Vec<PyObject> = Vec::with_capacity(inner.len());
                    for i in 0..inner.len() {
                        let item_av = inner.get(i).map_err(|e| {
                            PolarsError::ComputeError(
                                format!("grouped UDF inner get failed at index {i}: {e}").into(),
                            )
                        })?;
                        values.push(any_value_to_py(py, &item_av)?);
                    }
                    let py_list = pyo3::types::PyList::new(py, values.iter().map(|o| o.bind(py)))
                        .map_err(|e| {
                        PolarsError::ComputeError(
                            format!(
                                "Python grouped UDF '{}' list construction failed: {e}",
                                spec.udf_name
                            )
                            .into(),
                        )
                    })?;
                    py_args.push(py_list.into_py(py));
                }

                // Call UDF once per group.
                let args_tuple = pyo3::types::PyTuple::new(py, py_args.iter().map(|o| o.bind(py)))
                    .map_err(|e| {
                        PolarsError::ComputeError(
                            format!(
                                "Python grouped UDF '{}' tuple creation failed: {e}",
                                spec.udf_name
                            )
                            .into(),
                        )
                    })?;
                let ret = callable.call1(args_tuple).map_err(|e| {
                    PolarsError::ComputeError(
                        format!(
                            "Python grouped UDF '{}' failed for group {}: {e}",
                            spec.udf_name, group_idx
                        )
                        .into(),
                    )
                })?;

                if ret.is_none() {
                    per_group_results.push(None);
                } else {
                    // For grouped_agg we expect a scalar, not a sequence.
                    per_group_results.push(Some(ret.into_any().unbind()));
                }
            }

            // Convert per-group Python results to Series.
            let series =
                py_result_to_series(py, &per_group_results, &return_type, &spec.output_name)?;
            result_series.push(series);
        }
        Ok(())
    })?;

    // Attach result Series and drop temporary arg list columns.
    for s in result_series {
        pl_df.with_column(s)?;
    }

    // Drop the internal list columns used for arguments.
    for (spec_idx, spec) in specs.iter().enumerate() {
        for (arg_idx, _arg) in spec.args.iter().enumerate() {
            let alias = format!("_pyg_udf_{}_arg_{}", spec_idx, arg_idx);
            let _ = pl_df.drop_in_place(&alias);
        }
    }

    Ok(DataFrame::from_polars_with_options(pl_df, case_sensitive))
}

fn any_value_to_py(
    py: Python<'_>,
    av: &polars::prelude::AnyValue,
) -> Result<PyObject, PolarsError> {
    use polars::prelude::AnyValue;
    let obj = match av {
        AnyValue::Null => py.None().into_py(py),
        AnyValue::Boolean(v) => v.into_py(py),
        AnyValue::Int32(v) => (*v as i64).into_py(py),
        AnyValue::Int64(v) => (*v).into_py(py),
        AnyValue::Float32(v) => (*v as f64).into_py(py),
        AnyValue::Float64(v) => (*v).into_py(py),
        AnyValue::String(v) => v.to_string().into_py(py),
        _ => av.to_string().into_py(py),
    };
    Ok(obj)
}

fn py_result_to_series(
    py: Python<'_>,
    results: &[Option<PyObject>],
    dtype: &DataType,
    name: &str,
) -> Result<Series, PolarsError> {
    let values: Vec<Option<serde_json::Value>> = results
        .iter()
        .map(|opt| {
            opt.as_ref()
                .map(|obj| {
                    let bound = obj.clone().bind(py);
                    if bound.is_none() {
                        return Ok(serde_json::Value::Null);
                    }
                    if let Ok(v) = bound.extract::<bool>() {
                        return Ok(serde_json::Value::Bool(v));
                    }
                    if let Ok(v) = bound.extract::<i64>() {
                        return Ok(serde_json::Value::Number(serde_json::Number::from(v)));
                    }
                    if let Ok(v) = bound.extract::<f64>() {
                        if let Some(n) = serde_json::Number::from_f64(v) {
                            return Ok(serde_json::Value::Number(n));
                        }
                        return Ok(serde_json::Value::Null);
                    }
                    if let Ok(v) = bound.extract::<String>() {
                        return Ok(serde_json::Value::String(v));
                    }
                    Ok(serde_json::Value::String(bound.to_string()))
                })
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e: PyErr| PolarsError::ComputeError(e.to_string().into()))?;

    let series = match dtype {
        DataType::Int32 => {
            let v: Vec<Option<i32>> = values
                .iter()
                .map(|o| match o {
                    Some(serde_json::Value::Number(n)) => n.as_i64().map(|x| x as i32),
                    _ => None,
                })
                .collect();
            Series::new(name.into(), v)
        }
        DataType::Int64 => {
            let v: Vec<Option<i64>> = values
                .iter()
                .map(|o| match o {
                    Some(serde_json::Value::Number(n)) => n.as_i64(),
                    _ => None,
                })
                .collect();
            Series::new(name.into(), v)
        }
        DataType::Float64 => {
            let v: Vec<Option<f64>> = values
                .iter()
                .map(|o| match o {
                    Some(serde_json::Value::Number(n)) => n.as_f64(),
                    _ => None,
                })
                .collect();
            Series::new(name.into(), v)
        }
        DataType::Boolean => {
            let v: Vec<Option<bool>> = values
                .iter()
                .map(|o| match o {
                    Some(serde_json::Value::Bool(b)) => Some(*b),
                    _ => None,
                })
                .collect();
            Series::new(name.into(), v)
        }
        _ => {
            let v: Vec<Option<String>> = values
                .iter()
                .map(|o| o.as_ref().map(|v| v.to_string()))
                .collect();
            Series::new(name.into(), v)
        }
    };
    series
        .cast(dtype)
        .map_err(|e| PolarsError::ComputeError(format!("Python UDF result cast: {e}").into()))
}
