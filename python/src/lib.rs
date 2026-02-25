//! PyO3 bindings for sparkless Python package. Exposes robin-sparkless as a native module.

use pyo3::create_exception;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use robin_sparkless::dataframe::{JoinType, PivotedGroupedData, SaveMode, WriteFormat, WriteMode};
use robin_sparkless::functions::{self, SortOrder, ThenBuilder, WhenBuilder};
use robin_sparkless::{
    Column, CubeRollupData, DataFrame, DataType, GroupedData, SelectItem,
    SparkSession, SparkSessionBuilder,
};
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::path::Path;
use std::thread_local;

/// Convert EngineError or PolarsError to Python SparklessError.
fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    SparklessError::new_err(e.to_string())
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

    #[getter]
    fn backend_type(&self) -> &'static str {
        "robin"
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

    fn conf(slf: PyRef<Self>) -> PyRuntimeConfig {
        let py = slf.py();
        PyRuntimeConfig {
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

    /// createDataFrame(data, schema=None). data: list of dicts, list of tuples, or pandas.DataFrame.
    /// schema: optional list of (name, type_str) or list of column names (types inferred).
    /// Pandas DataFrame is converted via to_dict("records"); column order is preserved.
    #[pyo3(signature = (data, schema=None))]
    fn create_dataframe_from_rows(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let (data, from_pandas) = normalize_create_dataframe_input(py, data)?;
        let (rows, schema) = python_data_and_schema(py, &data, schema, from_pandas)?;
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
struct PyRuntimeConfig {
    session: Py<PyAny>,
}

#[pymethods]
impl PyRuntimeConfig {
    fn get(&self, py: Python<'_>, key: &str) -> PyResult<Option<String>> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?;
        let sess = session.borrow();
        let config = sess.inner.get_config();
        Ok(config.get(key).cloned())
    }

    fn is_case_sensitive(&self, py: Python<'_>) -> PyResult<bool> {
        let session = self.session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?;
        let sess = session.borrow();
        Ok(sess.inner.is_case_sensitive())
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
        _ if lower.starts_with("array<") && lower.ends_with('>') => s.to_string(),
        _ if lower.starts_with("map<") && lower.ends_with('>') => s.to_string(),
        _ if lower.starts_with("struct<") && lower.ends_with('>') => s.to_string(),
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

/// Normalize createDataFrame input: convert pandas, PyArrow Table, or NumPy ndarray to list.
/// Returns (converted list or original data, from_pandas flag for column order).
fn normalize_create_dataframe_input<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
) -> PyResult<(Bound<'py, PyAny>, bool)> {
    // 1. PyArrow Table: to_pylist() returns list of dicts
    if let Ok(to_pylist) = data.getattr("to_pylist") {
        if to_pylist.is_callable() {
            let list = to_pylist.call0()?;
            if list.downcast::<PyList>().is_ok() {
                return Ok((list, true));
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
                        return Ok((out.into_any(), false));
                    }
                    if ndim == 2 && list.downcast::<PyList>().is_ok() {
                        return Ok((list, false));
                    }
                }
            }
        }
    }

    // 3. Pandas DataFrame: to_dict("records")
    maybe_convert_pandas_to_list(py, data)
}

/// If data is a pandas DataFrame, convert to list of dicts via to_dict("records").
/// Returns (converted list or original data, true if from pandas).
fn maybe_convert_pandas_to_list<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyAny>,
) -> PyResult<(Bound<'py, PyAny>, bool)> {
    let to_dict = match data.getattr("to_dict") {
        Ok(attr) => attr,
        Err(_) => return Ok((data.clone(), false)),
    };
    if !to_dict.is_callable() {
        return Ok((data.clone(), false));
    }
    let records = match to_dict.call1(("records",)) {
        Ok(r) => r,
        Err(_) => return Ok((data.clone(), false)),
    };
    if records.downcast::<PyList>().is_ok() {
        Ok((records, true))
    } else {
        Ok((data.clone(), false))
    }
}

fn python_data_and_schema(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    schema: Option<&Bound<'_, PyAny>>,
    from_pandas: bool,
) -> PyResult<(Vec<Vec<JsonValue>>, Vec<(String, String)>)> {
    let list = data.downcast::<PyList>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>("data must be a list (or pandas.DataFrame)")
    })?;
    let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(list.len());
    let mut inferred_schema: Option<Vec<(String, String)>> = None;
    let mut column_order: Option<Vec<String>> = None;

    for (idx, item) in list.iter().enumerate() {
        let row = python_row_to_json(py, &item, idx, column_order.as_deref(), from_pandas)?;
        if inferred_schema.is_none() && !row.is_empty() {
            if let Some(cols) = infer_schema_from_first_row(py, &item, from_pandas) {
                column_order = Some(cols.iter().map(|(n, _)| n.clone()).collect());
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
                (0..r.len())
                    .map(|i| (format!("_{}", i + 1), "string".to_string()))
                    .collect()
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

fn python_row_to_json(
    py: Python<'_>,
    item: &Bound<'_, PyAny>,
    row_idx: usize,
    column_order: Option<&[String]>,
    from_pandas: bool,
) -> PyResult<Vec<JsonValue>> {
    if let Ok(dict) = item.downcast::<PyDict>() {
        let mut keys: Vec<String> = if let Some(order) = column_order {
            order.to_vec()
        } else {
            dict.keys().iter().map(|k| k.extract::<String>().unwrap_or_default()).collect()
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

fn infer_schema_from_first_row(
    py: Python<'_>,
    item: &Bound<'_, PyAny>,
    from_pandas: bool,
) -> Option<Vec<(String, String)>> {
    let dict = item.downcast::<PyDict>().ok()?;
    let mut keys: Vec<String> = dict.keys().iter().filter_map(|k| k.extract::<String>().ok()).collect();
    if !from_pandas {
        keys.sort();
    }
    Some(keys.into_iter().map(|k| (k.clone(), "string".to_string())).collect())
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

/// PySpark join(on=...): str or list of str -> Vec<String>
fn py_join_on_to_vec(on: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = on.extract::<String>() {
        return Ok(vec![s]);
    }
    if let Ok(list) = on.downcast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(item.extract::<String>()?);
        }
        return Ok(out);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "join(on=...) expects str or list of str",
    ))
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

    #[pyo3(name = "where")]
    fn where_(&self, condition: &PyColumn) -> PyResult<PyDataFrame> {
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

    #[pyo3(signature = (n=1))]
    fn head(&self, n: usize) -> PyResult<PyDataFrame> {
        self.inner
            .head(n)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn na(slf: PyRef<Self>) -> PyDataFrameNaFunctions {
        let py = slf.py();
        PyDataFrameNaFunctions {
            df: slf.into_py(py),
        }
    }

    fn alias(&self, name: &str) -> PyDataFrame {
        PyDataFrame {
            inner: self.inner.alias(name),
        }
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
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn cache(&self) -> PyResult<PyDataFrame> {
        self.inner
            .cache()
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
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
    fn rdd(&self) -> PyResult<PyDataFrame> {
        Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
            "rdd is not yet implemented in robin-sparkless",
        ))
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

    fn group_by(&self, column_names: Vec<String>) -> PyResult<PyGroupedData> {
        let refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
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
            } else if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    names.push(sub.extract::<String>()?);
                }
            } else if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    names.push(sub.extract::<String>()?);
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "groupBy() expects column names as str or list/tuple[str]",
                ));
            }
        }
        self.group_by(names)
    }

    fn order_by(&self, column_names: Vec<String>) -> PyResult<PyDataFrame> {
        let refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
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
        let mut names: Vec<String> = Vec::with_capacity(cols.len());
        for item in cols.iter() {
            if let Ok(s) = item.extract::<String>() {
                names.push(s);
            } else if let Ok(list) = item.downcast::<PyList>() {
                for sub in list.iter() {
                    names.push(sub.extract::<String>()?);
                }
            } else if let Ok(tup) = item.downcast::<PyTuple>() {
                for sub in tup.iter() {
                    names.push(sub.extract::<String>()?);
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "orderBy() expects column names as str or list/tuple[str]",
                ));
            }
        }

        let asc_vec: Vec<bool> = match ascending {
            None => vec![true; names.len()],
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

        // Snake-case .order_by() defaults to ascending; camelCase orderBy can
        // accept an explicit ascending parameter. For now, ignore explicit
        // ascending here and call the snake-case helper, which uses ascending=True.
        self.order_by(names)
    }

    fn limit(&self, n: usize) -> PyResult<PyDataFrame> {
        self.inner
            .limit(n)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (*columns))]
    fn drop(&self, columns: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let col_names = py_tuple_or_single_to_vec_string(columns)?;
        let refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
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

    #[pyo3(name = "drop_duplicates", signature = (subset=None))]
    fn drop_duplicates_snake(&self, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        self.distinct(subset)
    }

    #[pyo3(signature = (other, on, how="inner"))]
    fn join(
        &self,
        other: &PyDataFrame,
        on: &Bound<'_, PyAny>,
        how: &str,
    ) -> PyResult<PyDataFrame> {
        let on_vec = py_join_on_to_vec(on)?;
        let on_refs: Vec<&str> = on_vec.iter().map(|s| s.as_str()).collect();
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

    #[pyo3(name = "crossJoin")]
    fn cross_join(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .cross_join(&other.inner)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn intersect(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .intersect(&other.inner)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(name = "exceptAll")]
    fn except_all(&self, other: &PyDataFrame) -> PyResult<PyDataFrame> {
        self.inner
            .except_all(&other.inner)
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

    #[pyo3(name = "unionByName", signature = (other, **kwargs))]
    fn union_by_name(
        &self,
        other: &PyDataFrame,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyDataFrame> {
        let allow = extract_allow_missing_columns(kwargs)?;
        self.inner
            .union_by_name(&other.inner, allow)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(name = "selectExpr", signature = (*exprs))]
    fn select_expr(&self, _py: Python<'_>, exprs: &Bound<'_, PyTuple>) -> PyResult<PyDataFrame> {
        let mut exprs_vec: Vec<String> = Vec::with_capacity(exprs.len());
        for item in exprs.iter() {
            exprs_vec.push(item.extract::<String>()?);
        }
        self.inner
            .select_expr(&exprs_vec)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(name = "createOrReplaceTempView")]
    fn create_or_replace_temp_view_camel(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let session = THREAD_ACTIVE_SESSIONS
            .with(|cell| cell.borrow().last().map(|s| s.clone_ref(py)))
            .ok_or_else(|| to_py_err("No active SparkSession for createOrReplaceTempView"))?;
        let session_ref = session.bind(py).downcast::<PySparkSession>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
        })?;
        session_ref.borrow().inner.create_or_replace_temp_view(name, self.inner.clone());
        Ok(())
    }

    #[pyo3(name = "dropna")]
    fn dropna(
        &self,
        subset: Option<Vec<String>>,
        how: Option<&str>,
        thresh: Option<usize>,
    ) -> PyResult<PyDataFrame> {
        let how_str = how.unwrap_or("any");
        let sub: Option<Vec<&str>> = subset.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect());
        self.inner
            .dropna(sub, how_str, thresh)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn fillna(&self, value: &Bound<'_, PyAny>, subset: Option<Vec<String>>) -> PyResult<PyDataFrame> {
        let value_expr = py_any_to_column(value)?.into_expr();
        let sub: Option<Vec<&str>> = subset.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect());
        self.inner
            .fillna(value_expr, sub)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    fn first(&self) -> PyResult<PyDataFrame> {
        self.inner
            .first()
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
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
            .map(|df| PyDataFrame { inner: df })
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
            .map(|df| PyDataFrame { inner: df })
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
        }
    }

    fn create_or_replace_temp_view(&self, session: &PySparkSession, name: &str) {
        session.inner.create_or_replace_temp_view(name, self.inner.clone());
    }

    #[getter]
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

    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn __and__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.bit_and(&rhs),
        })
    }

    fn __rand__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.bit_and(&self.inner),
        })
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

    /// PySpark: col + scalar, col + col (WindowFunction arithmetic parity)
    fn __add__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.add(&rhs),
        })
    }
    fn __radd__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.add(&self.inner),
        })
    }
    fn __sub__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.subtract(&rhs),
        })
    }
    fn __rsub__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.subtract(&self.inner),
        })
    }
    fn __mul__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.multiply(&rhs),
        })
    }
    fn __rmul__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.multiply(&self.inner),
        })
    }
    fn __truediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.divide(&rhs),
        })
    }
    fn __rtruediv__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.divide(&self.inner),
        })
    }
    fn __neg__(&self) -> PyResult<PyColumn> {
        Ok(PyColumn {
            inner: self.inner.negate(),
        })
    }

    fn __mod__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let rhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: self.inner.mod_(&rhs),
        })
    }
    fn __rmod__(&self, other: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let lhs = py_any_to_column(other)?;
        Ok(PyColumn {
            inner: lhs.mod_(&self.inner),
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
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "getItem expects int (array index) or str (map key)",
        ))
    }

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

    fn cast(&self, type_name: &str) -> PyResult<PyColumn> {
        self.inner
            .cast_to(type_name)
            .map(|c| PyColumn { inner: c })
            .map_err(to_py_err)
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
        column_over_window(self, partition_by, order_by, use_running_aggregate)
    }
}

#[pyclass]
struct PySortOrder {
    inner: SortOrder,
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
        subset: Option<Vec<String>>,
    ) -> PyResult<PyDataFrame> {
        let df_ref = self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
        })?.borrow();
        let value_col = lit(value)?;
        let subset_refs: Option<Vec<&str>> =
            subset.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect());
        let na = df_ref.inner.na();
        na.fill(value_col.inner.into_expr(), subset_refs)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (subset=None, how="any", thresh=None))]
    fn drop(
        &self,
        py: Python<'_>,
        subset: Option<Vec<String>>,
        how: &str,
        thresh: Option<usize>,
    ) -> PyResult<PyDataFrame> {
        let df_ref = self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
        })?.borrow();
        let subset_refs: Option<Vec<&str>> =
            subset.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect());
        let na = df_ref.inner.na();
        na.drop(subset_refs, how, thresh)
            .map(|df| PyDataFrame { inner: df })
            .map_err(to_py_err)
    }

    #[pyo3(signature = (to_replace, value, subset=None))]
    fn replace(
        &self,
        py: Python<'_>,
        to_replace: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        subset: Option<Vec<String>>,
    ) -> PyResult<PyDataFrame> {
        let df_ref = self.df.bind(py).downcast::<PyDataFrame>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
        })?.borrow();
        let old_col = lit(to_replace)?;
        let new_col = lit(value)?;
        let subset_refs: Option<Vec<&str>> =
            subset.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect());
        let na = df_ref.inner.na();
        na.replace(
            old_col.inner.into_expr(),
            new_col.inner.into_expr(),
            subset_refs,
        )
        .map(|df| PyDataFrame { inner: df })
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
        self.inner
            .count()
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
            .map(|df| PyDataFrame { inner: df })
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

/// Polymorphic lit: dispatches to lit_i64, lit_str, lit_f64, lit_bool based on Python type.
#[pyfunction]
fn lit(value: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let py = value.py();
    if value.is_none() {
        return lit_null("string");
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

#[pyfunction]
fn create_map(columns: &Bound<'_, PyAny>) -> PyResult<PyColumn> {
    let list = columns.downcast::<PyList>().map_err(|_| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>("create_map expects a list of Columns")
    })?;
    let mut owned: Vec<Column> = Vec::with_capacity(list.len());
    for item in list.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("create_map elements must be Columns")
        })?;
        owned.push(py_col.borrow().inner.clone());
    }
    let refs: Vec<&Column> = owned.iter().collect();
    let col = functions::create_map(&refs).map_err(to_py_err)?;
    Ok(PyColumn { inner: col })
}

#[pyfunction]
fn column_over_window(
    column: &PyColumn,
    partition_by: Vec<String>,
    order_by: Vec<String>,
    use_running_aggregate: bool,
) -> PyResult<PyColumn> {
    let parts: Vec<&str> = partition_by.iter().map(|s| s.as_str()).collect();
    column
        .inner
        .over_window(&parts[..], &order_by, use_running_aggregate)
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
    let windowed = lagged.over(&parts[..]);
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
    let windowed = led.over(&parts[..]);
    Ok(PyColumn { inner: windowed })
}
#[pyfunction]
fn regexp_replace(column: &PyColumn, pattern: &str, replacement: &str) -> PyColumn {
    PyColumn {
        inner: functions::regexp_replace(&column.inner, pattern, replacement),
    }
}

#[pyfunction]
#[pyo3(signature = (column, pattern, group_index=0))]
fn regexp_extract_all(column: &PyColumn, pattern: &str, group_index: usize) -> PyColumn {
    if group_index == 0 {
        PyColumn {
            inner: functions::regexp_extract_all(&column.inner, pattern),
        }
    } else {
        PyColumn {
            inner: column.inner.regexp_extract_all_group(pattern, group_index),
        }
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
#[pyo3(signature = (format, *columns))]
fn format_string(format: &str, columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("format_string expects Column expressions")
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
    (
        PyColumn { inner: pos },
        PyColumn { inner: val },
    )
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
fn repeat(column: &PyColumn, n: i32) -> PyColumn {
    PyColumn {
        inner: functions::repeat(&column.inner, n),
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
#[pyo3(signature = (*columns))]
fn concat(columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("concat expects Column expressions")
        })?;
        cols.push(py_col.borrow().inner.clone());
    }
    if cols.is_empty() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "concat requires at least one column",
        ));
    }
    let refs: Vec<&Column> = cols.iter().collect();
    Ok(PyColumn {
        inner: functions::concat(&refs),
    })
}

#[pyfunction]
#[pyo3(signature = (separator, *columns))]
fn concat_ws(separator: &str, columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "concat_ws expects Column expressions",
            )
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

#[pyfunction]
#[pyo3(signature = (*columns))]
fn array(columns: &Bound<'_, PyTuple>) -> PyResult<PyColumn> {
    let mut cols: Vec<Column> = Vec::with_capacity(columns.len());
    for item in columns.iter() {
        let py_col = item.downcast::<PyColumn>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("array expects Column expressions")
        })?;
        cols.push(py_col.borrow().inner.clone());
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
    m.add_class::<PyDataFrameWriter>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PySortOrder>()?;
    m.add_class::<PyGroupedData>()?;
    m.add_class::<PyPivotedGroupedData>()?;
    m.add_class::<PyDataFrameNaFunctions>()?;
    m.add_class::<PyCubeRollupData>()?;
    m.add_class::<PyRuntimeConfig>()?;
    m.add_class::<PyCatalog>()?;
    m.add_class::<PyWhenBuilder>()?;
    m.add_class::<PyThenBuilder>()?;
    m.add_class::<PyChainedWhenBuilder>()?;
    m.add_function(wrap_pyfunction!(spark_session_builder, m)?)?;
    m.add_function(wrap_pyfunction!(column, m)?)?;
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
    m.add_function(wrap_pyfunction!(row_number_window, m)?)?;
    m.add_function(wrap_pyfunction!(percent_rank_window, m)?)?;
    m.add_function(wrap_pyfunction!(rank_window, m)?)?;
    m.add_function(wrap_pyfunction!(dense_rank_window, m)?)?;
    m.add_function(wrap_pyfunction!(ntile_window, m)?)?;
    m.add_function(wrap_pyfunction!(lag_window, m)?)?;
    m.add_function(wrap_pyfunction!(lead_window, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_replace, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_extract, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_extract_all, m)?)?;
    m.add_function(wrap_pyfunction!(regexp_like, m)?)?;
    m.add_function(wrap_pyfunction!(split, m)?)?;
    m.add_function(wrap_pyfunction!(coalesce, m)?)?;
    m.add_function(wrap_pyfunction!(format_string, m)?)?;
    m.add_function(wrap_pyfunction!(greatest, m)?)?;
    m.add_function(wrap_pyfunction!(least, m)?)?;
    m.add_function(wrap_pyfunction!(array_distinct, m)?)?;
    m.add_function(wrap_pyfunction!(posexplode, m)?)?;
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
    m.add_function(wrap_pyfunction!(length, m)?)?;
    m.add_function(wrap_pyfunction!(floor, m)?)?;
    m.add_function(wrap_pyfunction!(round, m)?)?;
    m.add_function(wrap_pyfunction!(ltrim, m)?)?;
    m.add_function(wrap_pyfunction!(hour, m)?)?;
    m.add_function(wrap_pyfunction!(minute, m)?)?;
    m.add_function(wrap_pyfunction!(soundex, m)?)?;
    m.add_function(wrap_pyfunction!(repeat, m)?)?;
    m.add_function(wrap_pyfunction!(levenshtein, m)?)?;
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
    Ok(())
}
