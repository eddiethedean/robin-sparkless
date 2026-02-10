//! Python UDF execution: row-at-a-time call into user Python functions.

use crate::column::Column as RsColumn;
use crate::dataframe::DataFrame;
use crate::session::SparkSession;
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
    let (callable, return_type) = &*entry;

    // Evaluate arg expressions in df context
    let lf = df.df.as_ref().clone().lazy();
    let mut arg_exprs = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        arg_exprs.push(arg.expr().clone().alias(format!("_udf_arg_{i}")));
    }
    let lf_with_args = lf.with_columns(arg_exprs);
    let pl_df = lf_with_args.collect()?;

    let n = pl_df.height();
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
        let callable = callable.bind(py);
        let mut results: Vec<Option<PyObject>> = Vec::with_capacity(n);

        for row_idx in 0..n {
            // Build Python args for this row (tuple for *args)
            let mut py_row = Vec::with_capacity(args.len());
            for series in &arg_series {
                let av = series
                    .get(row_idx)
                    .map_err(|e| PolarsError::ComputeError(format!("udf get row: {e}").into()))?;
                py_row.push(any_value_to_py(py, &av)?);
            }

            // Call UDF(*args) - pass tuple of row values
            let args_tuple = pyo3::types::PyTuple::new(py, py_row.iter().map(|o| o.bind(py)))
                .map_err(|e| {
                    PolarsError::ComputeError(format!("Python UDF '{udf_name}' tuple: {e}").into())
                })?;
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
