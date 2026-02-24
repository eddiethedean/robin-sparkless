//! PyO3 bindings for sparkless Python package. Exposes robin-sparkless as a native module.

use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use robin_sparkless::dataframe::{JoinType, PivotedGroupedData, SaveMode, WriteFormat, WriteMode};
use robin_sparkless::functions::{self, SortOrder, ThenBuilder, WhenBuilder};
use robin_sparkless::{Column, DataFrame, GroupedData, SparkSession, SparkSessionBuilder};
use serde_json::Value as JsonValue;
use std::path::Path;

/// Convert EngineError or PolarsError to Python SparklessError.
fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    SparklessError::new_err(e.to_string())
}

create_exception!(_native, SparklessError, pyo3::exceptions::PyRuntimeError, "Sparkless error");

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
                dict.set_item(k, json_to_py(v, py)?)?;
            }
            Ok(dict.into_py(py))
        }
    }
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

    fn get_or_create(&self) -> PyResult<PySparkSession> {
        Ok(PySparkSession {
            inner: self.inner.clone().get_or_create(),
        })
    }
}

#[pyclass]
struct PySparkSession {
    inner: SparkSession,
}

#[pymethods]
impl PySparkSession {
    #[classattr]
    fn builder(py: Python<'_>) -> PySparkSessionBuilder {
        PySparkSessionBuilder {
            inner: SparkSession::builder(),
        }
    }

    fn read(slf: PyRef<Self>) -> PyDataFrameReader {
        let py = slf.py();
        PyDataFrameReader {
            session: slf.into_py(py),
            options: Vec::new(),
            format: None,
        }
    }

    fn catalog(slf: PyRef<Self>) -> PyCatalog {
        let py = slf.py();
        PyCatalog {
            session: slf.into_py(py),
        }
    }

    fn create_or_replace_temp_view(&self, name: &str, df: &PyDataFrame) {
        self.inner.create_or_replace_temp_view(name, df.inner.clone());
    }

    fn create_or_replace_global_temp_view(&self, name: &str, df: &PyDataFrame) {
        self.inner.create_or_replace_global_temp_view(name, df.inner.clone());
    }

    fn drop_temp_view(&self, name: &str) {
        self.inner.drop_temp_view(name);
    }

    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        self.inner
            .table(name)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn sql(&self, query: &str) -> PyResult<PyDataFrame> {
        self.inner
            .sql(query)
            .map(|df| PyDataFrame { inner: df })
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
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    /// createDataFrame(data, schema=None). data: list of dicts or list of tuples (same length).
    /// schema: optional list of (name, type_str) or list of column names (types inferred).
    #[pyo3(signature = (data, schema=None))]
    fn create_dataframe_from_rows(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let (rows, schema) = python_data_and_schema(py, data, schema)?;
        self.inner
            .create_dataframe_from_rows(rows, schema, false)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    /// PySpark alias: createDataFrame -> create_dataframe_from_rows
    #[pyo3(name = "createDataFrame", signature = (data, schema=None))]
    fn create_data_frame_camel(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        self.create_dataframe_from_rows(py, data, schema)
    }

    fn range(&self, start: i64, end: i64, step: i64) -> PyResult<PyDataFrame> {
        self.inner
            .range(start, end, step)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn read_csv(&self, path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_csv(Path::new(path))
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn read_parquet(&self, path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_parquet(Path::new(path))
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn read_json(&self, path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_json(Path::new(path))
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[cfg(feature = "delta")]
    fn read_delta(&self, name_or_path: &str) -> PyResult<PyDataFrame> {
        self.inner
            .read_delta(name_or_path)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[cfg(feature = "delta")]
    fn read_delta_with_version(&self, name_or_path: &str, version: Option<i64>) -> PyResult<PyDataFrame> {
        self.inner
            .read_delta_with_version(name_or_path, version)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn stop(&self) {
        self.inner.stop();
    }
}

#[pyclass]
struct PyCatalog {
    session: Py<PyAny>,
}

#[pymethods]
impl PyCatalog {
    #[pyo3(name = "listTables")]
    fn list_tables(&self, py: Python<'_>) -> PyResult<PyDataFrame> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        let names = session.inner.list_table_names();
        let rows: Vec<Vec<JsonValue>> = names
            .into_iter()
            .map(|n| vec![JsonValue::String(n)])
            .collect();
        let schema = vec![("name".to_string(), "string".to_string())];
        session
            .inner
            .create_dataframe_from_rows(rows, schema, false)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(name = "dropTempView")]
    fn drop_temp_view(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        session.inner.drop_temp_view(name);
        Ok(())
    }

    #[pyo3(name = "listDatabases")]
    fn list_databases(&self, py: Python<'_>) -> PyResult<PyDataFrame> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        let names = session.inner.list_database_names();
        let rows: Vec<Vec<JsonValue>> = names
            .into_iter()
            .map(|n| vec![JsonValue::String(n)])
            .collect();
        let schema = vec![("name".to_string(), "string".to_string())];
        session
            .inner
            .create_dataframe_from_rows(rows, schema, false)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }
}

/// Parse schema from Python: None, list of column names (str), or list of (name, type) pairs.
fn parse_schema_from_py(py: Python<'_>, schema: &Bound<'_, PyAny>) -> PyResult<Option<Vec<(String, String)>>> {
    let list = match schema.downcast::<PyList>() {
        Ok(l) => l,
        Err(_) => return Ok(None),
    };
    if list.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let first = list.get_item(0).map_err(|e| e)?;
    if first.downcast::<PyList>().is_ok() || first.downcast::<pyo3::types::PyTuple>().is_ok() {
        let mut pairs = Vec::with_capacity(list.len());
        for item in list.iter() {
            let pair = item.downcast::<pyo3::types::PyTuple>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("schema must be list of column names or list of (name, type)")
            })?;
            if pair.len() != 2 {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("schema pair must be (name, type)"));
            }
            let name = pair.get_item(0)?.extract::<String>()?;
            let typ = pair.get_item(1)?.extract::<String>()?;
            pairs.push((name, typ));
        }
        return Ok(Some(pairs));
    }
    let mut names = Vec::with_capacity(list.len());
    for item in list.iter() {
        names.push(item.extract::<String>()?);
    }
    Ok(Some(names.into_iter().map(|n| (n, "string".to_string())).collect()))
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
        JsonValue::Array(_) => "string".to_string(),
        JsonValue::Object(_) => "string".to_string(),
    }
}

fn python_data_and_schema(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    schema: Option<&Bound<'_, PyAny>>,
) -> PyResult<(Vec<Vec<JsonValue>>, Vec<(String, String)>)> {
    let list = data.downcast::<PyList>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>("data must be a list")
    })?;
    let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(list.len());
    let mut inferred_schema: Option<Vec<(String, String)>> = None;

    for (idx, item) in list.iter().enumerate() {
        let row = python_row_to_json(py, &item, idx)?;
        if inferred_schema.is_none() && !row.is_empty() {
            if let Some(cols) = infer_schema_from_first_row(py, &item) {
                inferred_schema = Some(cols);
            }
        }
        rows.push(row);
    }

    let schema_res: Option<Vec<(String, String)>> = schema
        .map(|s| parse_schema_from_py(py, s))
        .transpose()?
        .and_then(|o| o);
    let mut schema = schema_res
        .or(inferred_schema)
        .unwrap_or_else(|| {
            rows.first().map(|r| {
                (0..r.len()).map(|i| (format!("_{}", i), "string".to_string())).collect()
            }).unwrap_or_default()
        });
    if let (Some(first_row), true) = (rows.first(), schema.iter().all(|(_, t)| t == "string")) {
        if first_row.len() == schema.len() {
            schema = schema
                .into_iter()
                .zip(first_row.iter())
                .map(|((name, _), v)| (name, infer_type_from_json_value(v)))
                .collect();
        }
    }
    Ok((rows, schema))
}

fn python_row_to_json(py: Python<'_>, item: &Bound<'_, PyAny>, row_idx: usize) -> PyResult<Vec<JsonValue>> {
    if let Ok(dict) = item.downcast::<PyDict>() {
        let mut keys: Vec<String> = dict.keys().iter().map(|k| k.extract::<String>().unwrap_or_default()).collect();
        keys.sort();
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
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
        "row {}: expected dict, list, or tuple",
        row_idx
    )))
}

fn py_any_to_json(py: Python<'_>, v: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
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
    if let Ok(list) = v.downcast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_any_to_json(py, &item)?);
        }
        return Ok(JsonValue::Array(arr));
    }
    if let Ok(dict) = v.downcast::<PyDict>() {
        let mut obj = serde_json::Map::new();
        for (k, val) in dict.iter() {
            let key = k.extract::<String>().unwrap_or_else(|_| k.to_string());
            obj.insert(key, py_any_to_json(py, &val)?);
        }
        return Ok(JsonValue::Object(obj));
    }
    Ok(JsonValue::String(v.to_string()))
}

fn infer_schema_from_first_row(py: Python<'_>, item: &Bound<'_, PyAny>) -> Option<Vec<(String, String)>> {
    let dict = item.downcast::<PyDict>().ok()?;
    let mut keys: Vec<String> = dict.keys().iter().filter_map(|k| k.extract::<String>().ok()).collect();
    keys.sort();
    Some(keys.into_iter().map(|k| (k.clone(), "string".to_string())).collect())
}

#[pyclass]
struct PyDataFrameReader {
    session: Py<PyAny>,
    options: Vec<(String, String)>,
    format: Option<String>,
}

#[pymethods]
impl PyDataFrameReader {
    fn option<'a>(mut slf: PyRefMut<'a, Self>, key: &str, value: &str) -> PyRefMut<'a, Self> {
        slf.options.push((key.to_string(), value.to_string()));
        slf
    }

    /// PySpark: options(**kwargs). opts: dict of key -> value.
    fn options<'a>(mut slf: PyRefMut<'a, Self>, py: Python<'_>, opts: &Bound<'_, PyAny>) -> PyResult<PyRefMut<'a, Self>> {
        let dict = opts.downcast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("options() requires a dict")
        })?;
        for (k, v) in dict.iter() {
            let key = k.extract::<String>()?;
            let value = v.extract::<String>()?;
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
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        let mut reader = session.inner.read();
        for (k, v) in &self.options {
            reader = reader.option(k.as_str(), v.as_str());
        }
        if let Some(ref fmt) = self.format {
            reader = reader.format(fmt);
        }
        reader
            .load(Path::new(path))
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn csv(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        let mut reader = session.inner.read();
        for (k, v) in &self.options {
            reader = reader.option(k.clone(), v.clone());
        }
        reader
            .csv(Path::new(path))
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn parquet(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        let mut reader = session.inner.read();
        for (k, v) in &self.options {
            reader = reader.option(k.clone(), v.clone());
        }
        reader
            .parquet(Path::new(path))
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn json(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        let mut reader = session.inner.read();
        for (k, v) in &self.options {
            reader = reader.option(k.clone(), v.clone());
        }
        reader
            .json(Path::new(path))
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn table(&self, py: Python<'_>, name: &str) -> PyResult<PyDataFrame> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        session.inner
            .table(name)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[cfg(feature = "delta")]
    fn delta(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        session
            .inner
            .read()
            .delta(Path::new(path))
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }
}

#[pyclass]
struct PyDataFrame {
    inner: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    fn filter(&self, condition: &PyColumn) -> PyResult<PyDataFrame> {
        self.inner
            .filter(condition.inner.clone().into_expr())
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn select(&self, cols: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        self.inner
            .select(refs)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn with_column(&self, name: &str, col: &PyColumn) -> PyResult<PyDataFrame> {
        self.inner
            .with_column(name, &col.inner)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn show(&self, n: Option<usize>) -> PyResult<()> {
        self.inner.show(n).map_err(to_py_err)
    }

    fn collect(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let rows = self.inner.collect_as_json_rows().map_err(to_py_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let dict = PyDict::new_bound(py);
            for (k, v) in row {
                dict.set_item(k, json_to_py(&v, py)?)?;
            }
            out.push(dict.into_py(py));
        }
        Ok(out)
    }

    fn count(&self) -> PyResult<usize> {
        self.inner.count().map_err(to_py_err)
    }

    fn group_by(&self, column_names: Vec<String>) -> PyResult<PyGroupedData> {
        let refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        self.inner
            .group_by(refs)
            .map(|gd| PyGroupedData { inner: gd })
            .map_err(to_py_err)
    }

    fn order_by(&self, column_names: Vec<String>, ascending: Vec<bool>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
        self.inner
            .order_by(refs, ascending)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn limit(&self, n: usize) -> PyResult<PyDataFrame> {
        self.inner
            .limit(n)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn drop(&self, columns: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        self.inner
            .drop(refs)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn distinct(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let sub: Option<Vec<&str>> = subset.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect());
        self.inner
            .distinct(sub)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn join(
        &self,
        other: &PyDataFrame,
        on: Vec<String>,
        how: &str,
    ) -> PyResult<PyDataFrame> {
        let on_refs: Vec<&str> = on.iter().map(|s| s.as_str()).collect();
        let how_lower = how.to_lowercase();
        if how_lower == "cross" {
            return self
                .inner
                .cross_join(&other.inner)
                .map(|df| PyDataFrame { inner: df })
                .map_err(to_py_err);
        }
        let join_type = match how_lower.as_str() {
            "inner" => JoinType::Inner,
            "left" | "left_outer" => JoinType::Left,
            "right" | "right_outer" => JoinType::Right,
            "outer" | "full" | "full_outer" => JoinType::Outer,
            _ => JoinType::Inner,
        };
        self.inner
            .join(&other.inner, on_refs, join_type)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn union(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .union(&other.inner)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn union_all(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .union_all(&other.inner)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn write(slf: PyRef<Self>) -> PyDataFrameWriter {
        let py = slf.py();
        PyDataFrameWriter {
            df: slf.into_py(py),
            mode: "overwrite".to_string(),
            options: Vec::new(),
            partition_by: Vec::new(),
        }
    }

    fn create_or_replace_temp_view(&self, session: &PySparkSession, name: &str) {
        session.inner.create_or_replace_temp_view(name, self.inner.clone());
    }

    fn columns(&self) -> PyResult<Vec<String>> {
        self.inner.columns().map_err(to_py_err)
    }
}

#[pyclass]
struct PyDataFrameWriter {
    df: Py<PyAny>,
    mode: String,
    options: Vec<(String, String)>,
    partition_by: Vec<String>,
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

    fn partition_by<'a>(mut slf: PyRefMut<'a, Self>, cols: Vec<String>) -> PyRefMut<'a, Self> {
        slf.partition_by = cols;
        slf
    }

    fn parquet(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df = self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
        })?;
        let inner = df.borrow();
        let mut w = inner.inner.write().mode(write_mode_from_str(&self.mode)).format(WriteFormat::Parquet);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    fn csv(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df = self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
        })?;
        let inner = df.borrow();
        let mut w = inner.inner.write().mode(write_mode_from_str(&self.mode)).format(WriteFormat::Csv);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    fn json(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df = self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
        })?;
        let inner = df.borrow();
        let mut w = inner.inner.write().mode(write_mode_from_str(&self.mode)).format(WriteFormat::Json);
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
        let df = self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
        })?;
        let inner = df.borrow();
        let mut w = inner.inner.write().mode(write_mode_from_str(&self.mode)).format(WriteFormat::Parquet);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    /// PySpark: saveAsTable(name). mode: "error"|"overwrite"|"append"|"ignore".
    #[pyo3(signature = (session, name, mode=None))]
    fn save_as_table(&self, py: Python<'_>, session: &PySparkSession, name: &str, mode: Option<&str>) -> PyResult<()> {
        let df = self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
        })?;
        let inner = df.borrow();
        let save_mode = save_mode_from_str(mode.unwrap_or("error"));
        inner
            .inner
            .write()
            .save_as_table(&session.inner, name, save_mode)
            .map_err(to_py_err)
    }
}

/// Convert Python value (int, float, str, bool, None) or PyColumn to robin_sparkless Column.
fn py_any_to_column(other: &Bound<'_, PyAny>) -> PyResult<Column> {
    if let Ok(py_col) = other.downcast::<PyColumn>() {
        return Ok(py_col.borrow().inner.clone());
    }
    if other.is_none() {
        return robin_sparkless::functions::lit_null("string").map_err(to_py_err);
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
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "comparison rhs must be Column, int, float, str, bool, or None",
    ))
}

#[pyclass]
struct PyColumn {
    inner: Column,
}

#[pymethods]
impl PyColumn {
    fn alias(&self, name: &str) -> PyColumn {
        PyColumn {
            inner: self.inner.alias(name),
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

    fn is_not_null(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.is_not_null(),
        }
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

    fn upper(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.upper(),
        }
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

    fn trim(&self) -> PyColumn {
        PyColumn {
            inner: self.inner.trim(),
        }
    }

    fn cast(&self, type_name: &str) -> PyResult<PyColumn> {
        self.inner
            .cast_to(type_name)
            .map(|c| PyColumn { inner: c })
            .map_err(to_py_err)
    }
}

#[pyclass]
struct PySortOrder {
    inner: SortOrder,
}

#[pyclass]
struct PyGroupedData {
    inner: GroupedData,
}

#[pymethods]
impl PyGroupedData {
    fn count(&self) -> PyResult<PyDataFrame> {
        self.inner
            .count()
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn sum(&self, col_name: &str) -> PyResult<PyDataFrame> {
        self.inner
            .sum(col_name)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn avg(&self, col_name: &str) -> PyResult<PyDataFrame> {
        self.inner
            .avg(&[col_name])
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn min(&self, col_name: &str) -> PyResult<PyDataFrame> {
        self.inner
            .min(col_name)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn max(&self, col_name: &str) -> PyResult<PyDataFrame> {
        self.inner
            .max(col_name)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn agg(&self, py: Python<'_>, exprs: &Bound<'_, PyList>) -> PyResult<PyDataFrame> {
        let mut rust_exprs = Vec::new();
        for item in exprs.iter() {
            let col_ref = item.downcast::<PyColumn>()?;
            rust_exprs.push(col_ref.borrow().inner.clone().into_expr());
        }
        self.inner
            .agg(rust_exprs)
            .map(|df| PyDataFrame { inner: df })
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
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn avg(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .avg(value_col)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn min(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .min(value_col)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn max(&self, value_col: &str) -> PyResult<PyDataFrame> {
        self.inner
            .max(value_col)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn count(&self) -> PyResult<PyDataFrame> {
        self.inner
            .count()
            .map(|df| PyDataFrame { inner: df })
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

#[pyfunction]
fn upper(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: robin_sparkless::functions::upper(&col.inner),
    }
}

#[pyfunction]
fn lower(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: robin_sparkless::functions::lower(&col.inner),
    }
}

#[pyfunction]
#[pyo3(signature = (column, start, length=None))]
fn substring(column: &PyColumn, start: i64, length: Option<i64>) -> PyColumn {
    PyColumn {
        inner: robin_sparkless::functions::substring(&column.inner, start, length),
    }
}

#[pyfunction]
fn trim(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: robin_sparkless::functions::trim(&col.inner),
    }
}

#[pyfunction]
fn cast(col: &PyColumn, type_name: &str) -> PyResult<PyColumn> {
    robin_sparkless::functions::cast(&col.inner, type_name)
        .map(|c| PyColumn { inner: c })
        .map_err(to_py_err)
}

#[pyclass]
struct PyWhenBuilder {
    inner: Option<WhenBuilder>,
}

#[pymethods]
impl PyWhenBuilder {
    fn then(&mut self, value: &PyColumn) -> PyResult<PyThenBuilder> {
        let wb = self
            .inner
            .take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("when().then() already called"))?;
        Ok(PyThenBuilder {
            inner: Some(wb.then(&value.inner)),
        })
    }
}

#[pyclass]
struct PyThenBuilder {
    inner: Option<ThenBuilder>,
}

#[pymethods]
impl PyThenBuilder {
    fn when(&mut self, condition: &PyColumn) -> PyResult<PyChainedWhenBuilder> {
        let tb = self
            .inner
            .take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("otherwise() already called"))?;
        Ok(PyChainedWhenBuilder {
            inner: Some(tb.when(&condition.inner)),
        })
    }

    fn otherwise(&mut self, value: &PyColumn) -> PyResult<PyColumn> {
        let tb = self
            .inner
            .take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("otherwise() already called"))?;
        Ok(PyColumn {
            inner: tb.otherwise(&value.inner),
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
    fn then(&mut self, value: &PyColumn) -> PyResult<PyThenBuilder> {
        let cwb = self
            .inner
            .take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("then() already called"))?;
        Ok(PyThenBuilder {
            inner: Some(cwb.then(&value.inner)),
        })
    }
}

#[pyfunction]
fn when(condition: &PyColumn) -> PyWhenBuilder {
    PyWhenBuilder {
        inner: Some(robin_sparkless::functions::when(&condition.inner)),
    }
}

#[pyfunction]
fn count(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::count(&col.inner),
    }
}

#[pyfunction]
fn sum(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::sum(&col.inner),
    }
}

#[pyfunction]
fn avg(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::avg(&col.inner),
    }
}

#[pyfunction]
fn min(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::min(&col.inner),
    }
}

#[pyfunction]
fn max(col: &PyColumn) -> PyColumn {
    PyColumn {
        inner: functions::max(&col.inner),
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("SparklessError", m.py().get_type_bound::<SparklessError>())?;
    m.add_class::<PySparkSessionBuilder>()?;
    m.add_class::<PySparkSession>()?;
    m.add_class::<PyDataFrameReader>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PyDataFrameWriter>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PySortOrder>()?;
    m.add_class::<PyGroupedData>()?;
    m.add_class::<PyPivotedGroupedData>()?;
    m.add_class::<PyCatalog>()?;
    m.add_class::<PyWhenBuilder>()?;
    m.add_class::<PyThenBuilder>()?;
    m.add_class::<PyChainedWhenBuilder>()?;
    m.add_function(wrap_pyfunction!(spark_session_builder, m)?)?;
    m.add_function(wrap_pyfunction!(column, m)?)?;
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
    Ok(())
}
