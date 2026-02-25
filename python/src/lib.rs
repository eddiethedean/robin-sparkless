//! PyO3 bindings for sparkless Python package. Exposes robin-sparkless as a native module.

use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use robin_sparkless::dataframe::{JoinType, PivotedGroupedData, SaveMode, WriteFormat, WriteMode};
use robin_sparkless::functions::{self, SortOrder, ThenBuilder, WhenBuilder};
use robin_sparkless::{
    Column, DataFrame, DataType, GroupedData, SelectItem, SparkSession, SparkSessionBuilder,
};
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::path::Path;
use std::thread_local;

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

    fn get_or_create(&self, py: Python<'_>) -> PyResult<Py<PySparkSession>> {
        let session = self.inner.clone().get_or_create();
        let obj = Py::new(py, PySparkSession { inner: session })?;
        register_active_session(py, obj.clone_ref(py), true)?;
        Ok(obj)
    }

    /// PySpark camelCase alias for get_or_create
    #[pyo3(name = "getOrCreate")]
    fn get_or_create_camel(&self, py: Python<'_>) -> PyResult<Py<PySparkSession>> {
        self.get_or_create(py)
    }
}

#[pyclass]
struct PySparkSession {
    inner: SparkSession,
}

thread_local! {
    /// Thread-local stack of active sessions (PySpark getActiveSession semantics).
    static THREAD_ACTIVE_SESSIONS: RefCell<Vec<Py<PySparkSession>>> = const { RefCell::new(Vec::new()) };
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
            let already = list
                .iter()
                .any(|it| it.as_ptr() == ptr);
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
        let obj = Py::new(py, PySparkSession { inner: session })?;
        register_active_session(py, obj.clone_ref(py), true)?;
        Ok(obj)
    }

    #[classattr]
    fn builder(py: Python<'_>) -> PySparkSessionBuilder {
        PySparkSessionBuilder {
            inner: SparkSession::builder(),
        }
    }

    #[getter]
    fn app_name(&self) -> Option<String> {
        self.inner.app_name()
    }

    #[classmethod]
    #[pyo3(name = "getActiveSession")]
    fn get_active_session(_cls: &Bound<'_, pyo3::types::PyType>, py: Python<'_>) -> PyResult<Option<Py<PySparkSession>>> {
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
        let obj = Py::new(py, PySparkSession { inner: session })?;
        register_active_session(py, obj.clone_ref(py), true)?;
        Ok(obj)
    }

    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

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
    fn _storage(slf: PyRef<Self>) -> PyStorage {
        let py = slf.py();
        PyStorage {
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
struct PyCatalog {
    session: Py<PyAny>,
}

#[pymethods]
impl PyCatalog {
    #[pyo3(name = "listTables")]
    #[pyo3(signature = (dbName=None))]
    fn list_tables(&self, py: Python<'_>, dbName: Option<String>) -> PyResult<Vec<Py<PyTable>>> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?;
        let sess = session.borrow();

        let mut out: Vec<(String, String)> = Vec::new(); // (db, table)
        let filter_db = dbName.as_deref();
        if let Some(db) = filter_db {
            if db.eq_ignore_ascii_case("global_temp") {
                for n in sess.inner.list_global_temp_view_names() {
                    out.push(("global_temp".to_string(), n));
                }
            } else {
                for full in sess.inner.list_table_names().into_iter().chain(sess.inner.list_temp_view_names()) {
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
            for full in sess.inner.list_table_names().into_iter().chain(sess.inner.list_temp_view_names()) {
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
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        session.inner.drop_temp_view(name);
        Ok(())
    }

    #[pyo3(name = "listDatabases")]
    fn list_databases(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDatabase>>> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?;
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
        let name: String = name.extract().map_err(|_| to_py_err("database name must be a string"))?;
        if name.is_empty() {
            return Err(to_py_err("database name cannot be empty"));
        }
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
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
        let name: String = name.extract().map_err(|_| to_py_err("database name must be a string"))?;
        if name.is_empty() {
            return Err(to_py_err("database name cannot be empty"));
        }
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        let existed = session.inner.database_exists(&name);
        if !existed && !ignoreIfNotExists {
            return Err(to_py_err(format!("Database '{name}' does not exist")));
        }
        session.inner.drop_database(&name);
        Ok(())
    }

    #[pyo3(name = "setCurrentDatabase")]
    fn set_current_database(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        session.inner.set_current_database(name).map_err(to_py_err)
    }

    #[pyo3(name = "currentDatabase")]
    fn current_database(&self, py: Python<'_>) -> PyResult<String> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        Ok(session.inner.current_database())
    }

    #[pyo3(name = "tableExists", signature = (arg1, arg2=None))]
    fn table_exists(
        &self,
        py: Python<'_>,
        arg1: &Bound<'_, PyAny>,
        arg2: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let a1: String = arg1.extract().map_err(|_| to_py_err("table name must be a string"))?;
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        let full = if let Some(a2_any) = arg2 {
            let a2: String = a2_any.extract().map_err(|_| to_py_err("table name must be a string"))?;
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
    fn get_table(
        &self,
        py: Python<'_>,
        arg1: &str,
        arg2: Option<&str>,
    ) -> PyResult<Py<PyTable>> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
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
        Py::new(py, PyTable { name: tbl, database: db })
    }

    #[pyo3(name = "cacheTable")]
    fn cache_table(&self, py: Python<'_>, tableName: &str) -> PyResult<()> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        session.inner.cache_table(tableName);
        Ok(())
    }

    #[pyo3(name = "uncacheTable")]
    fn uncache_table(&self, py: Python<'_>, tableName: &str) -> PyResult<()> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        session.inner.uncache_table(tableName);
        Ok(())
    }

    #[pyo3(name = "isCached")]
    fn is_cached(&self, py: Python<'_>, tableName: &str) -> PyResult<bool> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
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
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        Ok(session.inner.database_exists(name))
    }

    fn create_table(
        &self,
        py: Python<'_>,
        schema_name: &str,
        table_name: &str,
        schema: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?.borrow();
        if !session.inner.database_exists(schema_name) {
            session.inner.register_database(schema_name);
        }
        let pairs = parse_schema_from_py(py, schema)?.unwrap_or_default();
        let df = session
            .inner
            .create_dataframe_from_rows(Vec::new(), pairs, false)
            .map_err(to_py_err)?;
        let full = format!("{schema_name}.{table_name}");
        session.inner.register_table(&full, df);
        Ok(())
    }
}

/// Map PySpark type name to our schema type string.
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
        _ if lower.contains("struct") => "string".to_string(),
        _ if lower.contains("array") => "string".to_string(),
        _ if lower.contains("map") => "string".to_string(),
        _ => "string".to_string(),
    }
}

/// Parse schema from Python: None, list of column names (str), list of (name, type) pairs,
/// or StructType-like object with .fields (each with .name and .dataType.simpleString()).
fn parse_schema_from_py(py: Python<'_>, schema: &Bound<'_, PyAny>) -> PyResult<Option<Vec<(String, String)>>> {
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
            // The Rust backend uses a heuristic: "all dtype == string" => infer from rows. Avoid triggering it.
            let mapped = if mapped.eq_ignore_ascii_case("string") {
                "str".to_string()
            } else {
                mapped
            };
            pairs.push((name, mapped));
        }
        return Ok(Some(pairs));
    }

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
        JsonValue::Array(_) => "string".to_string(),
        JsonValue::Object(_) => "string".to_string(),
    }
}

fn python_data_and_schema(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    schema: Option<&Bound<'_, PyAny>>,
) -> PyResult<(Vec<Vec<JsonValue>>, Vec<(String, String)>)> {
    // Handle pandas DataFrame
    if let Ok(pd_mod) = PyModule::import_bound(py, "pandas") {
        let pd_df_cls = pd_mod.getattr("DataFrame").ok();
        if let Some(cls) = pd_df_cls {
            if data.is_instance(&cls).unwrap_or(false) {
                let columns: Vec<String> = data.getattr("columns")?.getattr("tolist")?.call0()?.extract()?;
                let records = data.call_method0("to_dict")?;
                let records_dict = records.call1(("records",))?;
                // Actually, to_dict("records") returns list of dicts
                let as_list = data.call_method1("to_dict", ("records",))?;
                let list = as_list.downcast::<PyList>().map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>("pandas to_dict failed")
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
                let final_schema = schema_res
                    .or(inferred_schema)
                    .unwrap_or_else(|| columns.into_iter().map(|c| (c, "string".to_string())).collect());
                return Ok((rows, final_schema));
            }
        }
    }

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
    fn option<'a>(mut slf: PyRefMut<'a, Self>, key: &str, value: &Bound<'_, PyAny>) -> PyResult<PyRefMut<'a, Self>> {
        let v = if let Ok(s) = value.extract::<String>() {
            s
        } else if let Ok(b) = value.extract::<bool>() {
            b.to_string()
        } else if let Ok(i) = value.extract::<i64>() {
            i.to_string()
        } else if let Ok(f) = value.extract::<f64>() {
            f.to_string()
        } else {
            value.str()?.to_string()
        };
        slf.options.push((key.to_string(), v));
        Ok(slf)
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
    fn filter(&self, condition: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        if let Ok(py_col) = condition.downcast::<PyColumn>() {
            return self.inner
                .filter(py_col.borrow().inner.clone().into_expr())
                .map(|df| PyDataFrame { inner: df })
                .map_err(to_py_err);
        }
        if let Ok(s) = condition.extract::<String>() {
            // PySpark allows SQL string conditions like "age > 18".
            // Parse using the session's SQL engine is not available here,
            // so we raise a NotImplementedError for now.
            return Err(to_py_err(format!(
                "SQL string conditions in filter/where are not yet supported: {s}"
            )));
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "filter/where condition must be a Column or SQL string",
        ))
    }

    #[pyo3(name = "where")]
    fn where_(&self, condition: &Bound<'_, PyAny>) -> PyResult<PyDataFrame> {
        self.filter(condition)
    }

    #[pyo3(signature = (*cols))]
    fn select(&self, _py: Python<'_>, cols: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        #[derive(Debug)]
        enum Tmp {
            NameIdx(usize),
            Expr(robin_sparkless::Expr),
        }

        fn push_item(
            item: &Bound<'_, PyAny>,
            out: &mut Vec<Tmp>,
            names: &mut Vec<Box<str>>,
        ) -> PyResult<()> {
            if let Ok(py_col) = item.downcast::<PyColumn>() {
                out.push(Tmp::Expr(py_col.borrow().inner.clone().into_expr()));
                return Ok(());
            }
            if let Ok(s) = item.extract::<String>() {
                let idx = names.len();
                names.push(s.into_boxed_str());
                out.push(Tmp::NameIdx(idx));
                return Ok(());
            }
            if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    push_item(&sub, out, names)?;
                }
                return Ok(());
            }
            if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    push_item(&sub, out, names)?;
                }
                return Ok(());
            }
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "select() expects columns as str, Column, or a list/tuple of those",
            ))
        }

        let mut tmp: Vec<Tmp> = Vec::with_capacity(cols.len());
        let mut name_boxes: Vec<Box<str>> = Vec::new();
        for item in cols.iter() {
            push_item(&item, &mut tmp, &mut name_boxes)?;
        }

        let mut items: Vec<SelectItem<'_>> = Vec::with_capacity(tmp.len());
        for t in tmp {
            match t {
                Tmp::Expr(e) => items.push(SelectItem::Expr(e)),
                Tmp::NameIdx(i) => items.push(SelectItem::ColumnName(&name_boxes[i])),
            }
        }

        self.inner
            .select_items(items)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn with_column(&self, name: &str, col: &PyColumn) -> PyResult<PyDataFrame> {
        let df = self.inner.with_column(name, &col.inner).map_err(|e| {
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
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(name = "withColumn")]
    fn with_column_camel(&self, name: &str, col: &PyColumn) -> PyResult<PyDataFrame> {
        self.with_column(name, col)
    }

    #[pyo3(name = "withColumnRenamed")]
    fn with_column_renamed(&self, old_name: &str, new_name: &str) -> PyResult<PyDataFrame> {
        self.inner
            .with_column_renamed(old_name, new_name)
            .map(|df| PyDataFrame { inner: df })
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

    fn show(&self, n: Option<usize>) -> PyResult<()> {
        self.inner.show(n).map_err(to_py_err)
    }

    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
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
                        let sf = types_mod
                            .getattr("StructField")?
                            .call1((f.name.clone(), dt_obj, f.nullable, py.None()))?;
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

    fn collect(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        // Build a PySpark-like Row with schema attached.
        let schema = self.inner.schema_engine().map_err(to_py_err)?;
        let field_names: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
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
                        let sf = types_mod
                            .getattr("StructField")?
                            .call1((f.name.clone(), dt_obj, f.nullable, py.None()))?;
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

        let rows = self.inner.collect_as_json_rows().map_err(to_py_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let kwargs = PyDict::new_bound(py);
            for name in &field_names {
                let v = row.get(name).unwrap_or(&JsonValue::Null);
                let py_v = match (dtype_by_name.get(name), v) {
                    (Some(DataType::Timestamp), JsonValue::String(s)) => datetime_cls
                        .call_method1("fromisoformat", (s.as_str(),))
                        .map(|o| o.into_py(py))
                        .unwrap_or_else(|_| s.clone().into_py(py)),
                    (Some(DataType::Date), JsonValue::String(s)) => date_cls
                        .call_method1("fromisoformat", (s.as_str(),))
                        .map(|o| o.into_py(py))
                        .unwrap_or_else(|_| s.clone().into_py(py)),
                    _ => json_to_py(v, py)?,
                };
                kwargs.set_item(name, py_v)?;
            }
            let py_row = row_cls.call((), Some(&kwargs))?;
            py_row.setattr("_fields", field_names.clone())?;
            py_row.setattr("_schema", py_schema.clone_ref(py))?;
            out.push(py_row.into_py(py));
        }
        Ok(out)
    }

    fn count(&self) -> PyResult<usize> {
        self.inner.count().map_err(to_py_err)
    }

    #[pyo3(signature = (*cols))]
    fn group_by(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyGroupedData> {
        let mut names: Vec<String> = Vec::new();
        for item in cols.iter() {
            if let Ok(s) = item.extract::<String>() {
                names.push(s);
            } else if let Ok(c) = item.downcast::<PyColumn>() {
                names.push(c.borrow().inner.name().to_string());
            } else if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    if let Ok(s) = sub.extract::<String>() {
                        names.push(s);
                    }
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "group_by() expects column names as str or Column",
                ));
            }
        }
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        self.inner
            .group_by(refs)
            .map(|gd| PyGroupedData { inner: gd })
            .map_err(to_py_err)
    }

    #[pyo3(name = "groupBy", signature = (*cols))]
    fn group_by_camel(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyGroupedData> {
        let mut names: Vec<String> = Vec::with_capacity(cols.len());
        for item in cols.iter() {
            if let Ok(s) = item.extract::<String>() {
                names.push(s);
            } else if let Ok(c) = item.downcast::<PyColumn>() {
                names.push(c.borrow().inner.name().to_string());
            } else if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    if let Ok(s) = sub.extract::<String>() {
                        names.push(s);
                    } else if let Ok(c) = sub.downcast::<PyColumn>() {
                        names.push(c.borrow().inner.name().to_string());
                    }
                }
            } else if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    if let Ok(s) = sub.extract::<String>() {
                        names.push(s);
                    } else if let Ok(c) = sub.downcast::<PyColumn>() {
                        names.push(c.borrow().inner.name().to_string());
                    }
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "groupBy() expects column names as str, Column, or list/tuple of those",
                ));
            }
        }
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        self.inner
            .group_by(refs)
            .map(|gd| PyGroupedData { inner: gd })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*cols))]
    fn groupby(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyGroupedData> {
        self.group_by_camel(cols)
    }

    #[pyo3(signature = (*cols))]
    fn order_by(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let mut names: Vec<String> = Vec::new();
        for item in cols.iter() {
            if let Ok(s) = item.extract::<String>() {
                names.push(s);
            } else if let Ok(c) = item.downcast::<PyColumn>() {
                names.push(c.borrow().inner.name().to_string());
            } else if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    if let Ok(s) = sub.extract::<String>() {
                        names.push(s);
                    }
                }
            }
        }
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let ascending: Vec<bool> = vec![true; refs.len()];
        self.inner
            .order_by(refs, ascending)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(name = "orderBy", signature = (*cols, ascending=None))]
    fn order_by_camel(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let mut names: Vec<String> = Vec::new();
        let mut asc_from_sort: Vec<Option<bool>> = Vec::new();

        fn extract_items(item: &Bound<'_, PyAny>, names: &mut Vec<String>, asc: &mut Vec<Option<bool>>) -> PyResult<()> {
            if let Ok(s) = item.extract::<String>() {
                names.push(s);
                asc.push(None);
            } else if let Ok(c) = item.downcast::<PyColumn>() {
                names.push(c.borrow().inner.name().to_string());
                asc.push(None);
            } else if let Ok(so) = item.downcast::<PySortOrder>() {
                let borrowed = so.borrow();
                names.push(borrowed.col_name.clone());
                asc.push(Some(!borrowed.inner.descending));
                return Ok(());
            } else if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    extract_items(&sub, names, asc)?;
                }
            } else if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    extract_items(&sub, names, asc)?;
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "orderBy() expects column names as str, Column, SortOrder, or list/tuple of those",
                ));
            }
            Ok(())
        }

        for item in cols.iter() {
            extract_items(&item, &mut names, &mut asc_from_sort)?;
        }

        let asc_vec: Vec<bool> = match ascending {
            None => asc_from_sort.iter().map(|a| a.unwrap_or(true)).collect(),
            Some(v) if v.extract::<bool>().is_ok() => vec![v.extract::<bool>()?; names.len()],
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

        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        self.inner
            .order_by(refs, asc_vec)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn limit(&self, n: usize) -> PyResult<PyDataFrame> {
        self.inner
            .limit(n)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*cols))]
    fn drop(&self, cols: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let mut names: Vec<String> = Vec::new();
        for item in cols.iter() {
            if let Ok(s) = item.extract::<String>() {
                names.push(s);
            } else if let Ok(c) = item.downcast::<PyColumn>() {
                names.push(c.borrow().inner.name().to_string());
            } else if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    if let Ok(s) = sub.extract::<String>() {
                        names.push(s);
                    } else if let Ok(c) = sub.downcast::<PyColumn>() {
                        names.push(c.borrow().inner.name().to_string());
                    }
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "drop() expects str or Column arguments",
                ));
            }
        }
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
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

    #[pyo3(name = "dropDuplicates", signature = (subset=None))]
    fn drop_duplicates(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        self.distinct(subset)
    }

    #[pyo3(signature = (other, on=None, how="inner"))]
    fn join(
        &self,
        other: &PyDataFrame,
        on: Option<&Bound<'_, PyAny>>,
        how: &str,
    ) -> PyResult<PyDataFrame> {
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
            "semi" | "left_semi" | "leftsemi" => JoinType::Inner, // best-effort
            "anti" | "left_anti" | "leftanti" => JoinType::Inner, // best-effort
            _ => JoinType::Inner,
        };
        let on_names = extract_string_list_from_on(on)?;
        let on_refs: Vec<&str> = on_names.iter().map(|s| s.as_str()).collect();
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

    #[pyo3(name = "unionAll")]
    fn union_all_camel(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.union_all(other)
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
        session.inner.create_or_replace_temp_view(name, self.inner.clone());
    }

    #[pyo3(name = "createOrReplaceTempView")]
    fn create_or_replace_temp_view_camel(&self, py: Python<'_>, name: &str) -> PyResult<()> {
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
                    let top = THREAD_ACTIVE_SESSIONS.with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)));
                    match top {
                        Some(s) => s.bind(py).borrow().inner.clone(),
                        None => return Err(to_py_err("No active SparkSession")),
                    }
                }
            } else {
                let top = THREAD_ACTIVE_SESSIONS.with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)));
                match top {
                    Some(s) => s.bind(py).borrow().inner.clone(),
                    None => return Err(to_py_err("No active SparkSession")),
                }
            }
        };
        active.create_or_replace_temp_view(name, self.inner.clone());
        Ok(())
    }

    #[getter]
    fn columns(&self) -> PyResult<Vec<String>> {
        self.inner.columns().map_err(to_py_err)
    }

    #[pyo3(signature = (other, allow_missing_columns=false))]
    fn union_by_name(
        &self,
        other: &PyDataFrame,
        allow_missing_columns: bool,
    ) -> PyResult<PyDataFrame> {
        self.inner
            .union_by_name(&other.inner, allow_missing_columns)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(name = "unionByName", signature = (other, allowMissingColumns=false))]
    fn union_by_name_camel(&self, other: &PyDataFrame, allowMissingColumns: bool) -> PyResult<PyDataFrame> {
        self.union_by_name(other, allowMissingColumns)
    }

    #[pyo3(name = "selectExpr", signature = (*exprs))]
    fn select_expr(&self, py: Python<'_>, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let mut sql_strs: Vec<String> = Vec::new();
        for item in exprs.iter() {
            sql_strs.push(item.extract::<String>()?);
        }
        let combined = sql_strs.join(", ");
        let query = format!("SELECT {} FROM __self__", combined);
        Err(to_py_err(format!("selectExpr is not yet fully implemented")))
    }

    #[pyo3(name = "withColumnsRenamed")]
    fn with_columns_renamed(&self, col_map: &Bound<'_, PyDict>) -> PyResult<PyDataFrame> {
        let mut df = self.inner.clone();
        for (k, v) in col_map.iter() {
            let old: String = k.extract()?;
            let new: String = v.extract()?;
            df = df.with_column_renamed(&old, &new).map_err(to_py_err)?;
        }
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(name = "fillna", signature = (value, subset=None))]
    fn fillna(&self, py: Python<'_>, value: &Bound<'_, PyAny>, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let col_names: Vec<String> = match subset {
            Some(s) => s,
            None => self.inner.columns().map_err(to_py_err)?,
        };

        if let Ok(dict) = value.downcast::<PyDict>() {
            let mut df = self.inner.clone();
            for (k, v) in dict.iter() {
                let col_name: String = k.extract()?;
                let fill_col = py_any_to_column(&v)?;
                let cond = Column::new(col_name.clone()).is_null();
                let filled = robin_sparkless::functions::when(&cond)
                    .then(&fill_col)
                    .otherwise(&Column::new(col_name.clone()));
                df = df.with_column(&col_name, &filled).map_err(to_py_err)?;
            }
            return Ok(PyDataFrame { inner: df });
        }

        let fill_val = py_any_to_column(value)?;
        let mut df = self.inner.clone();
        for col_name in &col_names {
            let cond = Column::new(col_name.clone()).is_null();
            let filled = robin_sparkless::functions::when(&cond)
                .then(&fill_val)
                .otherwise(&Column::new(col_name.clone()));
            df = df.with_column(col_name, &filled).map_err(to_py_err)?;
        }
        Ok(PyDataFrame { inner: df })
    }

    #[pyo3(name = "dropna", signature = (how="any", thresh=None, subset=None))]
    fn dropna(
        &self,
        how: &str,
        thresh: Option<usize>,
        subset: Option<Vec<String>>,
    ) -> PyResult<PyDataFrame> {
        Err(to_py_err("dropna is not yet implemented"))
    }

    #[getter]
    fn na(slf: PyRef<Self>, py: Python<'_>) -> PyResult<PyObject> {
        let na_mod = PyModule::import_bound(py, "sparkless.dataframe.na")?;
        let cls = na_mod.getattr("DataFrameNaFunctions")?;
        let obj = cls.call1((slf.into_py(py),))?;
        Ok(obj.into_py(py))
    }

    #[pyo3(name = "agg", signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let names: Vec<String> = Vec::new();
        let gd = self.inner.group_by(Vec::<&str>::new()).map_err(to_py_err)?;
        let mut rust_exprs: Vec<robin_sparkless::Expr> = Vec::new();
        for item in exprs.iter() {
            if let Ok(c) = item.downcast::<PyColumn>() {
                rust_exprs.push(c.borrow().inner.clone().into_expr());
            } else if let Ok(dict) = item.downcast::<PyDict>() {
                for (k, v) in dict.iter() {
                    let col_name: String = k.extract()?;
                    let func_name: String = v.extract()?;
                    let c = Column::new(col_name);
                    let expr_col = match func_name.as_str() {
                        "sum" => functions::sum(&c),
                        "avg" | "mean" => functions::avg(&c),
                        "min" => functions::min(&c),
                        "max" => functions::max(&c),
                        "count" => functions::count(&c),
                        _ => return Err(to_py_err(format!("unsupported agg function: {func_name}"))),
                    };
                    rust_exprs.push(expr_col.into_expr());
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "agg() expects Column expressions or dict",
                ));
            }
        }
        gd.agg(rust_exprs)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(name = "sort", signature = (*cols, ascending=None))]
    fn sort(
        &self,
        cols: &Bound<'_, PyTuple>,
        ascending: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        self.order_by_camel(cols, ascending)
    }

    #[pyo3(name = "toDF", signature = (*col_names))]
    fn to_df(&self, col_names: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        if col_names.is_empty() {
            return Ok(PyDataFrame { inner: self.inner.clone() });
        }
        let mut df = self.inner.clone();
        let current = df.columns().map_err(to_py_err)?;
        for (i, item) in col_names.iter().enumerate() {
            if i >= current.len() {
                break;
            }
            let new_name: String = item.extract()?;
            df = df.with_column_renamed(&current[i], &new_name).map_err(to_py_err)?;
        }
        Ok(PyDataFrame { inner: df })
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

    #[pyo3(name = "printSchema")]
    fn print_schema(&self) -> PyResult<()> {
        let schema = self.inner.schema_engine().map_err(to_py_err)?;
        println!("root");
        for f in schema.fields() {
            println!(" |-- {}: {:?} (nullable = {})", f.name, f.data_type, f.nullable);
        }
        Ok(())
    }

    #[getter]
    fn dtypes(&self) -> PyResult<Vec<(String, String)>> {
        let schema = self.inner.schema_engine().map_err(to_py_err)?;
        Ok(schema.fields().iter().map(|f| (f.name.clone(), format!("{:?}", f.data_type))).collect())
    }

    fn describe(&self) -> PyResult<PyDataFrame> {
        Err(to_py_err("describe is not yet implemented"))
    }

    fn coalesce(&self, n: usize) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame { inner: self.inner.clone() })
    }

    fn repartition(&self, n: usize) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame { inner: self.inner.clone() })
    }

    fn cache(&self) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame { inner: self.inner.clone() })
    }

    fn persist(&self) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame { inner: self.inner.clone() })
    }

    fn unpersist(&self) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame { inner: self.inner.clone() })
    }

    #[pyo3(name = "isEmpty")]
    fn is_empty(&self) -> PyResult<bool> {
        self.inner.count().map(|c| c == 0).map_err(to_py_err)
    }

    fn first(&self, py: Python<'_>) -> PyResult<PyObject> {
        let limited = self.inner.limit(1).map_err(to_py_err)?;
        let df = PyDataFrame { inner: limited };
        let rows = df.collect(py)?;
        if rows.is_empty() {
            Ok(py.None())
        } else {
            Ok(rows.into_iter().next().unwrap())
        }
    }

    fn head(&self, py: Python<'_>, n: Option<usize>) -> PyResult<PyObject> {
        let n = n.unwrap_or(1);
        let limited = self.inner.limit(n).map_err(to_py_err)?;
        let df = PyDataFrame { inner: limited };
        let rows = df.collect(py)?;
        if n == 1 {
            if rows.is_empty() {
                Ok(py.None())
            } else {
                Ok(rows.into_iter().next().unwrap())
            }
        } else {
            Ok(PyList::new_bound(py, rows).into_py(py))
        }
    }

    fn take(&self, py: Python<'_>, n: usize) -> PyResult<Vec<PyObject>> {
        let limited = self.inner.limit(n).map_err(to_py_err)?;
        let df = PyDataFrame { inner: limited };
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
    #[pyo3(signature = (name, mode=None))]
    fn save_as_table(&self, py: Python<'_>, name: &str, mode: Option<&str>) -> PyResult<()> {
        let df = self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
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
                    let top = THREAD_ACTIVE_SESSIONS.with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)));
                    match top {
                        Some(s) => s.bind(py).borrow().inner.clone(),
                        None => return Err(to_py_err("No active SparkSession")),
                    }
                }
            } else {
                let top = THREAD_ACTIVE_SESSIONS.with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)));
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
            .save_as_table(&active, name, save_mode)
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
    if let Ok(list) = other.downcast::<PyList>() {
        if list.is_empty() {
            return Ok(robin_sparkless::functions::lit_str("[]"));
        }
        let first = list.get_item(0)?;
        if first.extract::<i64>().is_ok() {
            let vals: Vec<i64> = list.iter().filter_map(|x| x.extract::<i64>().ok()).collect();
            return Ok(robin_sparkless::functions::lit_str(&format!("{:?}", vals)));
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

    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn __add__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: self.inner.add_pyspark(&rhs) })
    }

    fn __radd__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: lhs.add_pyspark(&self.inner) })
    }

    fn __sub__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: self.inner.subtract_pyspark(&rhs) })
    }

    fn __rsub__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: lhs.subtract_pyspark(&self.inner) })
    }

    fn __mul__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: self.inner.multiply_pyspark(&rhs) })
    }

    fn __rmul__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: lhs.multiply_pyspark(&self.inner) })
    }

    fn __truediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: self.inner.divide_pyspark(&rhs) })
    }

    fn __rtruediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: lhs.divide_pyspark(&self.inner) })
    }

    fn __mod__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: self.inner.mod_pyspark(&rhs) })
    }

    fn __rmod__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: lhs.mod_pyspark(&self.inner) })
    }

    fn __neg__(&self) -> PyColumn {
        PyColumn { inner: self.inner.negate() }
    }

    fn __and__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: self.inner.bit_and(&rhs) })
    }

    fn __rand__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: lhs.bit_and(&self.inner) })
    }

    fn __or__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: self.inner.bit_or(&rhs) })
    }

    fn __ror__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: lhs.bit_or(&self.inner) })
    }

    fn __invert__(&self) -> PyColumn {
        PyColumn { inner: self.inner.bitwise_not() }
    }

    fn __getitem__(&self, key: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        if let Ok(idx) = key.extract::<i64>() {
            return Ok(PyColumn { inner: self.inner.get_item(idx) });
        }
        if let Ok(name) = key.extract::<String>() {
            return Ok(PyColumn { inner: self.inner.get_field(&name) });
        }
        if let Ok(py_col) = key.downcast::<PyColumn>() {
            let name = py_col.borrow().inner.name().to_string();
            return Ok(PyColumn { inner: self.inner.get_field(&name) });
        }
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Column subscript key must be int, str, or Column",
        ))
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
        let name = self.inner.name().to_string();
        PySortOrder { inner: self.inner.asc(), col_name: name }
    }

    fn desc(&self) -> PySortOrder {
        let name = self.inner.name().to_string();
        PySortOrder { inner: self.inner.desc(), col_name: name }
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

    fn cast(&self, py: Python<'_>, type_name: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let name = if let Ok(s) = type_name.extract::<String>() {
            s
        } else if let Ok(ss) = type_name.call_method0("simpleString") {
            ss.extract::<String>()?
        } else if let Ok(s) = type_name.str() {
            s.to_string()
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "cast type must be a string or DataType",
            ));
        };
        self.inner
            .cast_to(&name)
            .map(|c| PyColumn { inner: c })
            .map_err(to_py_err)
    }

    fn astype(&self, py: Python<'_>, type_name: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        self.cast(py, type_name)
    }

    fn between(&self, lower: &Bound<'_, PyAny>, upper: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lo = py_any_to_column(lower)?;
        let hi = py_any_to_column(upper)?;
        let ge = self.inner.gt_eq(lo.into_expr());
        let le = self.inner.lt_eq(hi.into_expr());
        Ok(PyColumn { inner: ge.bit_and(&le) })
    }

    #[pyo3(signature = (*values))]
    fn isin(&self, values: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
        let mut result: Option<Column> = None;
        for item in values.iter() {
            if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    let val = py_any_to_column(&sub)?;
                    let eq = self.inner.eq(val.into_expr());
                    result = Some(match result {
                        Some(prev) => prev.bit_or(&eq),
                        None => eq,
                    });
                }
            } else {
                let val = py_any_to_column(&item)?;
                let eq = self.inner.eq(val.into_expr());
                result = Some(match result {
                    Some(prev) => prev.bit_or(&eq),
                    None => eq,
                });
            }
        }
        Ok(PyColumn {
            inner: result.unwrap_or_else(|| Column::from_bool(false)),
        })
    }

    #[pyo3(name = "eqNullSafe")]
    fn eq_null_safe(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn { inner: self.inner.eq_null_safe(&rhs) })
    }

    #[pyo3(name = "withField")]
    fn with_field(&self, name: &str, col: &PyColumn) -> PyColumn {
        PyColumn { inner: self.inner.with_field(name, &col.inner) }
    }

    #[pyo3(name = "dropFields", signature = (*field_names))]
    fn drop_fields(&self, field_names: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
        let _ = field_names;
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "dropFields is not yet implemented",
        ))
    }

    fn desc_nulls_last(&self) -> PySortOrder {
        let name = self.inner.name().to_string();
        PySortOrder { inner: self.inner.desc_nulls_last(), col_name: name }
    }

    fn desc_nulls_first(&self) -> PySortOrder {
        let name = self.inner.name().to_string();
        PySortOrder { inner: self.inner.desc_nulls_first(), col_name: name }
    }

    fn asc_nulls_first(&self) -> PySortOrder {
        let name = self.inner.name().to_string();
        PySortOrder { inner: self.inner.asc_nulls_first(), col_name: name }
    }

    fn asc_nulls_last(&self) -> PySortOrder {
        let name = self.inner.name().to_string();
        PySortOrder { inner: self.inner.asc_nulls_last(), col_name: name }
    }

    fn startswith(&self, prefix: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let s = if let Ok(py_col) = prefix.downcast::<PyColumn>() {
            return Ok(PyColumn {
                inner: self.inner.startswith(&py_col.borrow().inner.name()),
            });
        } else {
            prefix.extract::<String>()?
        };
        Ok(PyColumn { inner: self.inner.startswith(&s) })
    }

    fn endswith(&self, suffix: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let s = if let Ok(py_col) = suffix.downcast::<PyColumn>() {
            return Ok(PyColumn {
                inner: self.inner.endswith(&py_col.borrow().inner.name()),
            });
        } else {
            suffix.extract::<String>()?
        };
        Ok(PyColumn { inner: self.inner.endswith(&s) })
    }

    fn contains(&self, sub: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let s = if let Ok(py_col) = sub.downcast::<PyColumn>() {
            return Ok(PyColumn {
                inner: self.inner.contains(&py_col.borrow().inner.name()),
            });
        } else {
            sub.extract::<String>()?
        };
        Ok(PyColumn { inner: self.inner.contains(&s) })
    }

    #[pyo3(signature = (pattern, escape=None))]
    fn like(&self, pattern: &str, escape: Option<char>) -> PyColumn {
        PyColumn { inner: self.inner.like(pattern, escape) }
    }

    #[pyo3(name = "isNaN")]
    fn is_nan(&self) -> PyColumn {
        PyColumn { inner: self.inner.is_nan() }
    }

    #[pyo3(name = "isNull")]
    fn is_null_camel(&self) -> PyColumn {
        PyColumn { inner: self.inner.is_null() }
    }

    #[pyo3(name = "isNotNull")]
    fn is_not_null_camel(&self) -> PyColumn {
        PyColumn { inner: self.inner.is_not_null() }
    }

    fn otherwise(&self, value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "otherwise() can only be applied on a Column previously generated by when()",
        ))
    }

    fn over(&self, _window: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Column.over() is not yet fully supported from native side",
        ))
    }

    fn __repr__(&self) -> String {
        format!("Column<'{}'>", self.inner.name())
    }
}

#[pyclass]
struct PySortOrder {
    inner: SortOrder,
    col_name: String,
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

    fn mean(&self, col_name: &str) -> PyResult<PyDataFrame> {
        self.avg(col_name)
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

    #[pyo3(signature = (*exprs))]
    fn agg(&self, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        fn push_expr(item: &Bound<'_, PyAny>, out: &mut Vec<robin_sparkless::Expr>) -> PyResult<()> {
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
    Ok(PyColumn { inner: robin_sparkless::functions::upper(&c) })
}

#[pyfunction]
fn lower(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: robin_sparkless::functions::lower(&c) })
}

#[pyfunction]
#[pyo3(signature = (column, start, length=None))]
fn substring(column: &Bound<'_, PyAny>, start: i64, length: Option<i64>) -> PyResult<PyColumn> {
    let c = coerce_to_column(column)?;
    Ok(PyColumn { inner: robin_sparkless::functions::substring(&c, start, length) })
}

#[pyfunction]
fn trim(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: robin_sparkless::functions::trim(&c) })
}

#[pyfunction]
fn cast(py: Python<'_>, col: &PyColumn, type_name: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let name = if let Ok(s) = type_name.extract::<String>() {
        s
    } else if let Ok(ss) = type_name.call_method0("simpleString") {
        ss.extract::<String>()?
    } else {
        type_name.str()?.to_string()
    };
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
        let wb = self
            .inner
            .take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("when().then() already called"))?;
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
    fn when(&mut self, condition: &Bound<'_, PyAny>, value: Option<&Bound<'_, PyAny>>) -> PyResult<PyObject> {
        let tb = self
            .inner
            .take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("otherwise() already called"))?;
        let cond = py_any_to_column(condition).or_else(|_| {
            condition.extract::<String>().map(|s| robin_sparkless::functions::col(&s))
        })?;
        let cwb = tb.when(&cond);
        let py = condition.py();
        match value {
            Some(val) => {
                let val_col = py_any_to_column(val)?;
                let new_tb = cwb.then(&val_col);
                Ok(Py::new(py, PyThenBuilder { inner: Some(new_tb) })?.into_py(py))
            }
            None => {
                Ok(Py::new(py, PyChainedWhenBuilder { inner: Some(cwb) })?.into_py(py))
            }
        }
    }

    fn otherwise(&mut self, value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let tb = self
            .inner
            .take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("otherwise() already called"))?;
        let val = py_any_to_column(value)?;
        Ok(PyColumn {
            inner: tb.otherwise(&val),
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
        let cwb = self
            .inner
            .take()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("then() already called"))?;
        let val = py_any_to_column(value)?;
        Ok(PyThenBuilder {
            inner: Some(cwb.then(&val)),
        })
    }
}

#[pyfunction]
#[pyo3(signature = (condition, value=None))]
fn when(py: Python<'_>, condition: &Bound<'_, PyAny>, value: Option<&Bound<'_, PyAny>>) -> PyResult<PyObject> {
    let cond_col = if let Ok(py_col) = condition.downcast::<PyColumn>() {
        py_col.borrow().inner.clone()
    } else if let Ok(s) = condition.extract::<String>() {
        robin_sparkless::functions::col(&s)
    } else {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "when() condition must be a Column or str",
        ));
    };

    let wb = robin_sparkless::functions::when(&cond_col);

    match value {
        Some(val) => {
            let val_col = py_any_to_column(val)?;
            let tb = wb.then(&val_col);
            Ok(Py::new(py, PyThenBuilder { inner: Some(tb) })?.into_py(py))
        }
        None => {
            Ok(Py::new(py, PyWhenBuilder { inner: Some(wb) })?.into_py(py))
        }
    }
}

#[pyfunction]
fn count(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::count(&c) })
}

#[pyfunction]
fn sum(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::sum(&c) })
}

#[pyfunction]
fn avg(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::avg(&c) })
}

#[pyfunction]
fn min(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::min(&c) })
}

#[pyfunction]
fn max(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::max(&c) })
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
        return Ok(PyColumn { inner: agg_col.over(&partition_strs) });
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
    let first = &order_by[0];
    let (name, descending) = if let Some(stripped) = first.strip_prefix('-') {
        (stripped.to_string(), true)
    } else {
        (first.clone(), false)
    };
    let order_col = Column::new(name);
    let base = order_col.row_number(descending);
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    let windowed = base.over(&parts[..]);
    Ok(PyColumn { inner: windowed })
}
#[pyfunction]
fn regexp_replace(column: &Bound<'_, PyAny>, pattern: &str, replacement: &str) -> PyResult<PyColumn> {
    let c = coerce_to_column(column)?;
    Ok(PyColumn { inner: functions::regexp_replace(&c, pattern, replacement) })
}

#[pyfunction]
#[pyo3(signature = (column, pattern, group_index=0))]
fn regexp_extract_all(column: &Bound<'_, PyAny>, pattern: &str, group_index: usize) -> PyResult<PyColumn> {
    let c = coerce_to_column(column)?;
    if group_index == 0 {
        Ok(PyColumn { inner: functions::regexp_extract_all(&c, pattern) })
    } else {
        Ok(PyColumn { inner: c.regexp_extract_all_group(pattern, group_index) })
    }
}

#[pyfunction]
fn regexp_like(column: &Bound<'_, PyAny>, pattern: &str) -> PyResult<PyColumn> {
    let c = coerce_to_column(column)?;
    Ok(PyColumn { inner: functions::regexp_like(&c, pattern) })
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

// ---- Additional native function bindings ----

#[pyfunction]
fn native_split(col: &Bound<'_, PyAny>, pattern: &str, limit: Option<i32>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::split(&c, pattern, limit) })
}

#[pyfunction]
fn native_explode(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::explode(&c) })
}

#[pyfunction]
fn native_explode_outer(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::explode_outer(&c) })
}

#[pyfunction]
fn native_posexplode(col: &Bound<'_, PyAny>) -> PyResult<(PyColumn, PyColumn)> {
    let c = coerce_to_column(col)?;
    let (pos, val) = functions::posexplode(&c);
    Ok((PyColumn { inner: pos }, PyColumn { inner: val }))
}

#[pyfunction]
fn native_array_contains(col: &Bound<'_, PyAny>, value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    let v = py_any_to_column(value)?;
    Ok(PyColumn { inner: functions::array_contains(&c, &v) })
}

#[pyfunction]
fn native_array_distinct(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::array_distinct(&c) })
}

#[pyfunction]
fn native_array_sort(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::array_sort(&c) })
}

#[pyfunction]
fn native_array_join(col: &Bound<'_, PyAny>, delimiter: &str) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::array_join(&c, delimiter) })
}

#[pyfunction]
fn native_array_max(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::array_max(&c) })
}

#[pyfunction]
fn native_array_min(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::array_min(&c) })
}

#[pyfunction]
fn native_element_at(col: &Bound<'_, PyAny>, index: i64) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::element_at(&c, index) })
}

#[pyfunction]
fn native_size(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::array_size(&c) })
}

#[pyfunction]
fn native_flatten(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::array_flatten(&c) })
}

#[pyfunction]
fn native_array_union(col1: &Bound<'_, PyAny>, col2: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c1 = coerce_to_column(col1)?;
    let c2 = coerce_to_column(col2)?;
    Ok(PyColumn { inner: functions::array_union(&c1, &c2) })
}

#[pyfunction]
fn native_array_intersect(col1: &Bound<'_, PyAny>, col2: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c1 = coerce_to_column(col1)?;
    let c2 = coerce_to_column(col2)?;
    Ok(PyColumn { inner: functions::array_intersect(&c1, &c2) })
}

#[pyfunction]
fn native_array_except(col1: &Bound<'_, PyAny>, col2: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c1 = coerce_to_column(col1)?;
    let c2 = coerce_to_column(col2)?;
    Ok(PyColumn { inner: functions::array_except(&c1, &c2) })
}

#[pyfunction]
fn native_collect_list(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::collect_list(&c) })
}

#[pyfunction]
fn native_collect_set(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::collect_set(&c) })
}

#[pyfunction]
fn native_first(col: &Bound<'_, PyAny>, ignorenulls: Option<bool>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::first(&c, ignorenulls.unwrap_or(false)) })
}

#[pyfunction]
fn native_count_distinct(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::count_distinct(&c) })
}

#[pyfunction]
fn native_approx_count_distinct(col: &Bound<'_, PyAny>, rsd: Option<f64>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::approx_count_distinct(&c, rsd) })
}

#[pyfunction]
fn native_stddev(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::stddev(&c) })
}

#[pyfunction]
fn native_stddev_pop(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::stddev_pop(&c) })
}

#[pyfunction]
fn native_stddev_samp(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::stddev_samp(&c) })
}

#[pyfunction]
fn native_variance(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::variance(&c) })
}

#[pyfunction]
fn native_var_pop(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::var_pop(&c) })
}

#[pyfunction]
fn native_var_samp(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::var_samp(&c) })
}

#[pyfunction]
fn native_corr(col1: &Bound<'_, PyAny>, col2: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c1 = coerce_to_column(col1)?;
    let c2 = coerce_to_column(col2)?;
    Ok(PyColumn { inner: functions::corr(&c1, &c2) })
}

// Math functions
#[pyfunction]
fn native_abs(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::abs(&c) })
}

#[pyfunction]
fn native_ceil(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::ceil(&c) })
}

#[pyfunction]
fn native_floor(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::floor(&c) })
}

#[pyfunction]
fn native_round(col: &Bound<'_, PyAny>, scale: Option<i32>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::round(&c, scale.unwrap_or(0)) })
}

#[pyfunction]
fn native_sqrt(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::sqrt(&c) })
}

#[pyfunction]
fn native_log(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::log(&c) })
}

#[pyfunction]
fn native_exp(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::exp(&c) })
}

#[pyfunction]
fn native_pow(col: &Bound<'_, PyAny>, exponent: i64) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::pow(&c, exponent) })
}

#[pyfunction]
fn native_signum(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::signum(&c) })
}

#[pyfunction]
fn native_sin(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::sin(&c) })
}

#[pyfunction]
fn native_cos(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::cos(&c) })
}

#[pyfunction]
fn native_tan(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::tan(&c) })
}

#[pyfunction]
fn native_asin(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::asin(&c) })
}

#[pyfunction]
fn native_acos(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::acos(&c) })
}

#[pyfunction]
fn native_atan(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::atan(&c) })
}

#[pyfunction]
fn native_atan2(y: &Bound<'_, PyAny>, x: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let cy = coerce_to_column(y)?;
    let cx = coerce_to_column(x)?;
    Ok(PyColumn { inner: functions::atan2(&cy, &cx) })
}

#[pyfunction]
fn native_degrees(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::degrees(&c) })
}

#[pyfunction]
fn native_radians(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::radians(&c) })
}

#[pyfunction]
fn native_log2(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::log2(&c) })
}

#[pyfunction]
fn native_log10(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::log10(&c) })
}

// String functions
#[pyfunction]
fn native_initcap(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::initcap(&c) })
}

#[pyfunction]
fn native_soundex(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::soundex(&c) })
}

#[pyfunction]
fn native_lpad(col: &Bound<'_, PyAny>, length: i32, pad: &str) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::lpad(&c, length, pad) })
}

#[pyfunction]
fn native_rpad(col: &Bound<'_, PyAny>, length: i32, pad: &str) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::rpad(&c, length, pad) })
}

#[pyfunction]
fn native_ltrim(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::ltrim(&c) })
}

#[pyfunction]
fn native_rtrim(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::rtrim(&c) })
}

#[pyfunction]
fn native_instr(col: &Bound<'_, PyAny>, substr: &str) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::instr(&c, substr) })
}

#[pyfunction]
fn native_locate(substr: &str, col: &Bound<'_, PyAny>, pos: Option<i64>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::locate(substr, &c, pos.unwrap_or(1)) })
}

#[pyfunction]
fn native_repeat(col: &Bound<'_, PyAny>, n: i32) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::repeat(&c, n) })
}

#[pyfunction]
fn native_reverse(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::reverse(&c) })
}

#[pyfunction]
fn native_translate(col: &Bound<'_, PyAny>, matching: &str, replace: &str) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::translate(&c, matching, replace) })
}

#[pyfunction]
fn native_length(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::length(&c) })
}

#[pyfunction]
fn native_format_string(fmt: &str, cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let mut owned: Vec<Column> = Vec::with_capacity(cols.len());
    for item in cols.iter() {
        owned.push(coerce_to_column(&item)?);
    }
    let refs: Vec<&Column> = owned.iter().collect();
    Ok(PyColumn { inner: functions::format_string(fmt, &refs) })
}

#[pyfunction]
fn native_concat(cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    if cols.is_empty() {
        return Err(to_py_err("concat requires at least one column"));
    }
    let mut owned: Vec<Column> = Vec::with_capacity(cols.len());
    for item in cols.iter() {
        owned.push(coerce_to_column(&item)?);
    }
    let refs: Vec<&Column> = owned.iter().collect();
    Ok(PyColumn { inner: functions::concat(&refs) })
}

#[pyfunction]
fn native_regexp_extract(col: &Bound<'_, PyAny>, pattern: &str, idx: usize) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::regexp_extract(&c, pattern, idx) })
}

// Date/time functions
#[pyfunction]
fn native_hour(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::hour(&c) })
}

#[pyfunction]
fn native_minute(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::minute(&c) })
}

#[pyfunction]
fn native_second(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::second(&c) })
}

#[pyfunction]
fn native_quarter(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::quarter(&c) })
}

#[pyfunction]
fn native_dayofyear(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::dayofyear(&c) })
}

#[pyfunction]
fn native_weekofyear(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::weekofyear(&c) })
}

#[pyfunction]
fn native_add_months(col: &Bound<'_, PyAny>, n: i32) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::add_months(&c, n) })
}

#[pyfunction]
fn native_months_between(end: &Bound<'_, PyAny>, start: &Bound<'_, PyAny>, round_off: Option<bool>) -> PyResult<PyColumn> {
    let e = coerce_to_column(end)?;
    let s = coerce_to_column(start)?;
    Ok(PyColumn { inner: functions::months_between(&e, &s, round_off.unwrap_or(true)) })
}

#[pyfunction]
fn native_next_day(col: &Bound<'_, PyAny>, day_of_week: &str) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::next_day(&c, day_of_week) })
}

#[pyfunction]
fn native_last_day(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::last_day(&c) })
}

#[pyfunction]
fn native_trunc(col: &Bound<'_, PyAny>, format: &str) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::trunc(&c, format) })
}

#[pyfunction]
fn native_date_trunc(format: &str, col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::date_trunc(format, &c) })
}

// Map functions
#[pyfunction]
fn native_map_keys(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: c.map_keys() })
}

#[pyfunction]
fn native_map_values(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: c.map_values() })
}

// Hash functions
#[pyfunction]
fn native_md5(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::md5(&c) })
}

#[pyfunction]
fn native_sha1(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::sha1(&c) })
}

#[pyfunction]
fn native_sha2(col: &Bound<'_, PyAny>, bit_length: i32) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::sha2(&c, bit_length) })
}

#[pyfunction]
fn native_crc32(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::crc32(&c) })
}

#[pyfunction]
fn native_xxhash64(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::xxhash64(&c) })
}

#[pyfunction]
fn native_base64(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::base64(&c) })
}

#[pyfunction]
fn native_unbase64(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::unbase64(&c) })
}

#[pyfunction]
fn native_ascii(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::ascii(&c) })
}

#[pyfunction]
fn native_hex(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::hex(&c) })
}

#[pyfunction]
fn native_unhex(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::unhex(&c) })
}

#[pyfunction]
fn native_bin(col: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::bin(&c) })
}

#[pyfunction]
fn native_conv(col: &Bound<'_, PyAny>, from_base: i32, to_base: i32) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::conv(&c, from_base, to_base) })
}

#[pyfunction]
fn native_format_number(col: &Bound<'_, PyAny>, d: u32) -> PyResult<PyColumn> {
    let c = coerce_to_column(col)?;
    Ok(PyColumn { inner: functions::format_number(&c, d) })
}

#[pyfunction]
fn native_greatest(cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let mut owned: Vec<Column> = Vec::with_capacity(cols.len());
    for item in cols.iter() {
        owned.push(coerce_to_column(&item)?);
    }
    let refs: Vec<&Column> = owned.iter().collect();
    functions::greatest(&refs).map(|c| PyColumn { inner: c }).map_err(to_py_err)
}

#[pyfunction]
fn native_least(cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    let mut owned: Vec<Column> = Vec::with_capacity(cols.len());
    for item in cols.iter() {
        owned.push(coerce_to_column(&item)?);
    }
    let refs: Vec<&Column> = owned.iter().collect();
    functions::least(&refs).map(|c| PyColumn { inner: c }).map_err(to_py_err)
}

#[pyfunction]
fn native_coalesce(cols: &Bound<'_, PyList>) -> PyResult<PyColumn> {
    if cols.is_empty() {
        return Err(to_py_err("coalesce requires at least one column"));
    }
    let first = coerce_to_column(&cols.get_item(0)?)?;
    let mut result = first;
    for i in 1..cols.len() {
        let next = coerce_to_column(&cols.get_item(i)?)?;
        let is_null = result.is_null();
        result = robin_sparkless::functions::when(&is_null).then(&next).otherwise(&result);
    }
    Ok(PyColumn { inner: result })
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
    m.add_function(wrap_pyfunction!(create_map, m)?)?;
    m.add_function(wrap_pyfunction!(column_over_window, m)?)?;
    m.add_function(wrap_pyfunction!(row_number_window, m)?)?;
    m.add_function(wrap_pyfunction!(concat, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_replace, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_extract_all, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_like, m)?)?;
    m.add_function(wrap_pyfunction!(to_timestamp, m)?)?;
    m.add_function(wrap_pyfunction!(to_date, m)?)?;
    m.add_function(wrap_pyfunction!(current_date, m)?)?;
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
    // New batch of native functions
    m.add_function(wrap_pyfunction!(native_split, m)?)?;
    m.add_function(wrap_pyfunction!(native_explode, m)?)?;
    m.add_function(wrap_pyfunction!(native_explode_outer, m)?)?;
    m.add_function(wrap_pyfunction!(native_posexplode, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_contains, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_sort, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_join, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_max, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_min, m)?)?;
    m.add_function(wrap_pyfunction!(native_element_at, m)?)?;
    m.add_function(wrap_pyfunction!(native_size, m)?)?;
    m.add_function(wrap_pyfunction!(native_flatten, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_union, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_intersect, m)?)?;
    m.add_function(wrap_pyfunction!(native_array_except, m)?)?;
    m.add_function(wrap_pyfunction!(native_collect_list, m)?)?;
    m.add_function(wrap_pyfunction!(native_collect_set, m)?)?;
    m.add_function(wrap_pyfunction!(native_first, m)?)?;
    m.add_function(wrap_pyfunction!(native_count_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(native_approx_count_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(native_stddev, m)?)?;
    m.add_function(wrap_pyfunction!(native_stddev_pop, m)?)?;
    m.add_function(wrap_pyfunction!(native_stddev_samp, m)?)?;
    m.add_function(wrap_pyfunction!(native_variance, m)?)?;
    m.add_function(wrap_pyfunction!(native_var_pop, m)?)?;
    m.add_function(wrap_pyfunction!(native_var_samp, m)?)?;
    m.add_function(wrap_pyfunction!(native_corr, m)?)?;
    m.add_function(wrap_pyfunction!(native_abs, m)?)?;
    m.add_function(wrap_pyfunction!(native_ceil, m)?)?;
    m.add_function(wrap_pyfunction!(native_floor, m)?)?;
    m.add_function(wrap_pyfunction!(native_round, m)?)?;
    m.add_function(wrap_pyfunction!(native_sqrt, m)?)?;
    m.add_function(wrap_pyfunction!(native_log, m)?)?;
    m.add_function(wrap_pyfunction!(native_exp, m)?)?;
    m.add_function(wrap_pyfunction!(native_pow, m)?)?;
    m.add_function(wrap_pyfunction!(native_signum, m)?)?;
    m.add_function(wrap_pyfunction!(native_sin, m)?)?;
    m.add_function(wrap_pyfunction!(native_cos, m)?)?;
    m.add_function(wrap_pyfunction!(native_tan, m)?)?;
    m.add_function(wrap_pyfunction!(native_asin, m)?)?;
    m.add_function(wrap_pyfunction!(native_acos, m)?)?;
    m.add_function(wrap_pyfunction!(native_atan, m)?)?;
    m.add_function(wrap_pyfunction!(native_atan2, m)?)?;
    m.add_function(wrap_pyfunction!(native_degrees, m)?)?;
    m.add_function(wrap_pyfunction!(native_radians, m)?)?;
    m.add_function(wrap_pyfunction!(native_log2, m)?)?;
    m.add_function(wrap_pyfunction!(native_log10, m)?)?;
    m.add_function(wrap_pyfunction!(native_initcap, m)?)?;
    m.add_function(wrap_pyfunction!(native_soundex, m)?)?;
    m.add_function(wrap_pyfunction!(native_lpad, m)?)?;
    m.add_function(wrap_pyfunction!(native_rpad, m)?)?;
    m.add_function(wrap_pyfunction!(native_ltrim, m)?)?;
    m.add_function(wrap_pyfunction!(native_rtrim, m)?)?;
    m.add_function(wrap_pyfunction!(native_instr, m)?)?;
    m.add_function(wrap_pyfunction!(native_locate, m)?)?;
    m.add_function(wrap_pyfunction!(native_repeat, m)?)?;
    m.add_function(wrap_pyfunction!(native_reverse, m)?)?;
    m.add_function(wrap_pyfunction!(native_translate, m)?)?;
    m.add_function(wrap_pyfunction!(native_length, m)?)?;
    m.add_function(wrap_pyfunction!(native_format_string, m)?)?;
    m.add_function(wrap_pyfunction!(native_concat, m)?)?;
    m.add_function(wrap_pyfunction!(native_regexp_extract, m)?)?;
    m.add_function(wrap_pyfunction!(native_hour, m)?)?;
    m.add_function(wrap_pyfunction!(native_minute, m)?)?;
    m.add_function(wrap_pyfunction!(native_second, m)?)?;
    m.add_function(wrap_pyfunction!(native_quarter, m)?)?;
    m.add_function(wrap_pyfunction!(native_dayofyear, m)?)?;
    m.add_function(wrap_pyfunction!(native_weekofyear, m)?)?;
    m.add_function(wrap_pyfunction!(native_add_months, m)?)?;
    m.add_function(wrap_pyfunction!(native_months_between, m)?)?;
    m.add_function(wrap_pyfunction!(native_next_day, m)?)?;
    m.add_function(wrap_pyfunction!(native_last_day, m)?)?;
    m.add_function(wrap_pyfunction!(native_trunc, m)?)?;
    m.add_function(wrap_pyfunction!(native_date_trunc, m)?)?;
    m.add_function(wrap_pyfunction!(native_map_keys, m)?)?;
    m.add_function(wrap_pyfunction!(native_map_values, m)?)?;
    m.add_function(wrap_pyfunction!(native_md5, m)?)?;
    m.add_function(wrap_pyfunction!(native_sha1, m)?)?;
    m.add_function(wrap_pyfunction!(native_sha2, m)?)?;
    m.add_function(wrap_pyfunction!(native_crc32, m)?)?;
    m.add_function(wrap_pyfunction!(native_xxhash64, m)?)?;
    m.add_function(wrap_pyfunction!(native_base64, m)?)?;
    m.add_function(wrap_pyfunction!(native_unbase64, m)?)?;
    m.add_function(wrap_pyfunction!(native_ascii, m)?)?;
    m.add_function(wrap_pyfunction!(native_hex, m)?)?;
    m.add_function(wrap_pyfunction!(native_unhex, m)?)?;
    m.add_function(wrap_pyfunction!(native_bin, m)?)?;
    m.add_function(wrap_pyfunction!(native_conv, m)?)?;
    m.add_function(wrap_pyfunction!(native_format_number, m)?)?;
    m.add_function(wrap_pyfunction!(native_greatest, m)?)?;
    m.add_function(wrap_pyfunction!(native_least, m)?)?;
    m.add_function(wrap_pyfunction!(native_coalesce, m)?)?;
    Ok(())
}
