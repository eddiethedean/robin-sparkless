//! Python SparkSession and SparkSessionBuilder (PySpark sql session).

use crate::{DataFrameReader, SparkSession};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock, RwLock};

use super::dataframe::PyDataFrame;

/// Default SparkSession for df.create_or_replace_temp_view(name) (PySpark parity).
/// Set when SparkSession.builder().get_or_create() is called. Stored as inner SparkSession
/// so we can call create_or_replace_temp_view from Rust without going through PyO3.
static DEFAULT_SESSION: OnceLock<Mutex<Option<SparkSession>>> = OnceLock::new();

fn default_session_cell() -> &'static Mutex<Option<SparkSession>> {
    DEFAULT_SESSION.get_or_init(|| Mutex::new(None))
}

/// Return a clone of the default SparkSession if set. Used by df.create_or_replace_temp_view.
/// The clone shares the catalog (Arc) with the session returned by get_or_create.
pub(crate) fn get_default_session() -> Option<SparkSession> {
    default_session_cell().lock().ok().and_then(|g| g.clone())
}

use super::py_to_json_value;
/// Python wrapper for SparkSession.
#[pyclass(name = "SparkSession")]
pub struct PySparkSession {
    inner: SparkSession,
}

#[pymethods]
impl PySparkSession {
    /// Create a default SparkSession with no app name or master URL.
    ///
    /// Prefer ``SparkSession.builder().app_name("...").get_or_create()`` for clarity.
    #[new]
    fn new() -> Self {
        PySparkSession {
            inner: SparkSession::new(None, None, std::collections::HashMap::new()),
        }
    }

    /// Return a builder to configure and create a SparkSession.
    ///
    /// Chain ``.app_name(name)``, optionally ``.master(url)`` and ``.config(key, value)``,
    /// then call ``.get_or_create()`` to build the session.
    ///
    /// Returns:
    ///     SparkSessionBuilder: Fluent builder.
    #[classmethod]
    fn builder(_cls: &Bound<'_, pyo3::types::PyType>) -> PyResult<PySparkSessionBuilder> {
        Ok(PySparkSessionBuilder {
            app_name: None,
            master: None,
            config: std::collections::HashMap::new(),
        })
    }

    /// Return whether column names are matched case-sensitively (default False).
    fn is_case_sensitive(&self) -> bool {
        self.inner.is_case_sensitive()
    }

    /// Create a DataFrame from a list of 3-tuples (id, age, name) and column names.
    ///
    /// For arbitrary schemas use ``create_dataframe_from_rows(data, schema)`` instead.
    ///
    /// Args:
    ///     data: List of (int, int, str) tuples.
    ///     column_names: List of exactly 3 strings, e.g. ["id", "age", "name"].
    ///
    /// Returns:
    ///     DataFrame (lazy).
    ///
    /// Raises:
    ///     RuntimeError: If creation fails (e.g. wrong column count).
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

    /// PySpark alias for ``create_dataframe()`` (3-column int, int, str only).
    #[pyo3(name = "createDataFrame")]
    fn create_data_frame_pyspark_alias(
        &self,
        _py: Python<'_>,
        data: &Bound<'_, pyo3::types::PyAny>,
        column_names: Vec<String>,
    ) -> PyResult<PyDataFrame> {
        self.create_dataframe(_py, data, column_names)
    }

    /// Create a DataFrame from row data and an explicit schema (internal / PySpark createDataFrame equivalent).
    ///
    /// Args:
    ///     data: List of rows. Each row is a dict (keyed by column name) or a list of
    ///         values in the same order as ``schema``. Supported value types: None, int,
    ///         float, bool, str.
    ///     schema: List of (name, dtype_str), e.g. [("id", "bigint"), ("name", "string")].
    ///
    /// Returns:
    ///     DataFrame (lazy).
    ///
    /// Raises:
    ///     TypeError: If a row is not a dict or list, or a value type is unsupported.
    ///     RuntimeError: If creation fails.
    fn _create_dataframe_from_rows(
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

    /// Return a DataFrameReader for reading files (PySpark: spark.read).
    ///
    /// Chain ``.option(key, value)``, ``.format("parquet"|"csv"|"json")``,
    /// then ``.csv(path)``, ``.parquet(path)``, ``.json(path)``, or ``.load(path)``.
    fn read(slf: PyRef<'_, Self>) -> PyDataFrameReader {
        PyDataFrameReader {
            session: slf.inner.clone(),
            options: RwLock::new(HashMap::new()),
            format: RwLock::new(None),
        }
    }

    /// Read a CSV file as a DataFrame.
    ///
    /// Args:
    ///     path: Local file path to the CSV file.
    ///
    /// Returns:
    ///     DataFrame (lazy). Schema is inferred from the file.
    ///
    /// Raises:
    ///     RuntimeError: If the file cannot be read or parsed.
    fn read_csv(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_csv(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Read a Parquet file or directory as a DataFrame.
    ///
    /// Args:
    ///     path: Local path to a Parquet file or directory of Parquet files.
    ///
    /// Returns:
    ///     DataFrame (lazy). Schema is read from the file(s).
    ///
    /// Raises:
    ///     RuntimeError: If the path cannot be read.
    fn read_parquet(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_parquet(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Read a JSON file (newline-delimited JSON, one object per line) as a DataFrame.
    ///
    /// Args:
    ///     path: Local file path.
    ///
    /// Returns:
    ///     DataFrame (lazy). Schema is inferred from the data.
    ///
    /// Raises:
    ///     RuntimeError: If the file cannot be read or parsed.
    fn read_json(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_json(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Register a DataFrame as a temporary view so it can be queried with ``sql()``.
    ///
    /// Args:
    ///     name: View name to use in SQL (e.g. "my_table").
    ///     df: DataFrame to register. It is captured by reference for lazy evaluation.
    ///
    /// Note:
    ///     Requires the ``sql`` feature. Re-registering the same name replaces the view.
    #[cfg(feature = "sql")]
    fn create_or_replace_temp_view(&self, name: &str, df: &PyDataFrame) {
        self.inner
            .create_or_replace_temp_view(name, df.inner.clone());
    }

    /// Return the DataFrame for a previously registered table/view name.
    ///
    /// Args:
    ///     name: View name from ``create_or_replace_temp_view()``.
    ///
    /// Returns:
    ///     DataFrame (lazy) for that view.
    ///
    /// Raises:
    ///     RuntimeError: If the name is not registered.
    #[cfg(feature = "sql")]
    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .table(name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Execute a SQL SELECT query over registered tables/views.
    ///
    /// Tables must be registered with ``create_or_replace_temp_view()`` first.
    /// Only SELECT is supported; the query is translated to the engine's plan.
    ///
    /// Args:
    ///     query: SQL SELECT string.
    ///
    /// Returns:
    ///     DataFrame (lazy) with the query result.
    ///
    /// Raises:
    ///     RuntimeError: If parsing or execution fails.
    #[cfg(feature = "sql")]
    fn sql(&self, query: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sql(query)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Read a Delta Lake table from the given path (latest version).
    ///
    /// Args:
    ///     path: Local path or URI to the Delta table directory.
    ///
    /// Returns:
    ///     DataFrame (lazy). Schema and data from the Delta log.
    ///
    /// Raises:
    ///     RuntimeError: If the table cannot be read. Requires the ``delta`` feature.
    #[cfg(feature = "delta")]
    fn read_delta(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_delta(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Read a Delta Lake table at a specific version (time travel).
    ///
    /// Args:
    ///     path: Local path or URI to the Delta table directory.
    ///     version: Delta table version to read. If None, reads latest.
    ///
    /// Returns:
    ///     DataFrame (lazy) for that version.
    ///
    /// Raises:
    ///     RuntimeError: If the table or version cannot be read. Requires the ``delta`` feature.
    #[cfg(feature = "delta")]
    fn read_delta_version(&self, path: &str, version: Option<i64>) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_delta_with_version(Path::new(path), version)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return the Catalog for table/view operations (PySpark: spark.catalog).
    fn catalog(&self) -> PyCatalog {
        PyCatalog {
            session: self.inner.clone(),
        }
    }

    /// Return the runtime config (PySpark: spark.conf).
    fn conf(&self) -> PyRuntimeConfig {
        PyRuntimeConfig {
            config: self.inner.get_config().clone(),
        }
    }

    /// Return a new session sharing the same catalog (PySpark: spark.newSession).
    #[pyo3(name = "newSession")]
    fn new_session(&self) -> Self {
        PySparkSession {
            inner: self.inner.clone(),
        }
    }

    /// Stop the session (cleanup). No-op for local execution.
    fn stop(&self) {
        self.inner.stop();
    }

    /// Create a DataFrame with single column 'id' (bigint) from start to end with step.
    /// PySpark: spark.range(end) or spark.range(start, end) or spark.range(start, end, step).
    #[pyo3(signature = (start, end=None, step=1))]
    fn range(&self, start: i64, end: Option<i64>, step: i64) -> PyResult<PyDataFrame> {
        let (s, e, st) = match end {
            None => (0i64, start, 1i64), // range(end) -> start=0, end=start
            Some(e) => (start, e, step),
        };
        let df = self
            .inner
            .range(s, e, st)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Session/library version string.
    fn version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    /// UDF not supported. Raises NotImplementedError.
    fn udf(&self) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "UDF not supported; use built-in functions",
        ))
    }

    /// Returns the active SparkSession for this thread (from get_or_create).
    #[classmethod]
    fn get_active_session(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
    ) -> PyResult<Option<Py<PySparkSession>>> {
        get_default_session()
            .map(|inner| Py::new(py, PySparkSession { inner }))
            .transpose()
    }

    /// Returns the default SparkSession (same as getActiveSession).
    #[classmethod]
    fn get_default_session(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
    ) -> PyResult<Option<Py<PySparkSession>>> {
        Self::get_active_session(_cls, py)
    }
}

/// Python wrapper for DataFrameReader (spark.read).
#[pyclass(name = "DataFrameReader")]
pub struct PyDataFrameReader {
    session: SparkSession,
    options: RwLock<HashMap<String, String>>,
    format: RwLock<Option<String>>,
}

impl PyDataFrameReader {
    fn build_reader(&self) -> DataFrameReader {
        let mut reader = DataFrameReader::new(self.session.clone());
        if let Ok(opts) = self.options.read() {
            for (k, v) in opts.iter() {
                reader = reader.option(k.clone(), v.clone());
            }
        }
        if let Ok(guard) = self.format.read() {
            if let Some(ref fmt) = *guard {
                reader = reader.format(fmt.clone());
            }
        }
        reader
    }
}

#[pymethods]
impl PyDataFrameReader {
    /// Add an option (PySpark: option(key, value)). Returns self for chaining.
    fn option<'py>(slf: PyRef<'py, Self>, key: &str, value: &str) -> PyRef<'py, Self> {
        if let Ok(mut opts) = slf.options.write() {
            opts.insert(key.to_string(), value.to_string());
        }
        slf
    }

    /// Add options from a dict (PySpark: options(**kwargs)). Returns self for chaining.
    fn options<'py>(
        slf: PyRef<'py, Self>,
        _py: Python<'_>,
        opts: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<PyRef<'py, Self>> {
        let dict = opts.downcast::<PyDict>()?;
        if let Ok(mut guard) = slf.options.write() {
            for (k, v) in dict.iter() {
                let k_str: String = k.extract()?;
                let v_str: String = v.extract()?;
                guard.insert(k_str, v_str);
            }
        }
        Ok(slf)
    }

    /// Set format for load() (PySpark: format("parquet") etc). Returns self for chaining.
    fn format<'py>(slf: PyRef<'py, Self>, fmt: &str) -> PyRef<'py, Self> {
        if let Ok(mut guard) = slf.format.write() {
            *guard = Some(fmt.to_string());
        }
        slf
    }

    /// Set schema (stub). Returns self for chaining.
    fn schema<'py>(
        slf: PyRef<'py, Self>,
        _schema: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyRef<'py, Self> {
        slf
    }

    /// Load data from path using format (or infer from extension) and options.
    fn load(&self, path: &str) -> PyResult<PyDataFrame> {
        let reader = self.build_reader();
        let df = reader
            .load(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Return the named table/view (PySpark: table(name)).
    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        let reader = self.build_reader();
        let df = reader
            .table(name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Read CSV file. Applies stored options.
    fn csv(&self, path: &str) -> PyResult<PyDataFrame> {
        let reader = self.build_reader();
        let df = reader
            .csv(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Read Parquet file or directory. Applies stored options.
    fn parquet(&self, path: &str) -> PyResult<PyDataFrame> {
        let reader = self.build_reader();
        let df = reader
            .parquet(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Read JSON file (JSONL). Applies stored options.
    fn json(&self, path: &str) -> PyResult<PyDataFrame> {
        let reader = self.build_reader();
        let df = reader
            .json(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(feature = "delta")]
    /// Read Delta table. Requires delta feature.
    fn delta(&self, path: &str) -> PyResult<PyDataFrame> {
        let reader = self.build_reader();
        let df = reader
            .delta(Path::new(path))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }
}

/// Python wrapper for Catalog (spark.catalog).
#[pyclass(name = "Catalog")]
pub struct PyCatalog {
    session: SparkSession,
}

#[pymethods]
impl PyCatalog {
    #[pyo3(name = "dropTempView")]
    fn drop_temp_view(&self, view_name: &str) {
        self.session.drop_temp_view(view_name);
    }

    #[pyo3(name = "dropGlobalTempView")]
    fn drop_global_temp_view(&self, view_name: &str) {
        self.session.drop_global_temp_view(view_name);
    }

    #[pyo3(name = "listTables")]
    fn list_tables(&self, _db_name: Option<&str>) -> Vec<String> {
        self.session.list_temp_view_names()
    }

    #[pyo3(name = "tableExists")]
    fn table_exists(&self, table_name: &str, _db_name: Option<&str>) -> bool {
        self.session.table_exists(table_name)
    }

    #[pyo3(name = "currentDatabase")]
    fn current_database(&self) -> &'static str {
        "default"
    }

    #[pyo3(name = "currentCatalog")]
    fn current_catalog(&self) -> &'static str {
        "spark_catalog"
    }

    #[pyo3(name = "listDatabases")]
    fn list_databases(&self, _pattern: Option<&str>) -> Vec<&'static str> {
        vec!["default"]
    }

    #[pyo3(name = "listCatalogs")]
    fn list_catalogs(&self, _pattern: Option<&str>) -> Vec<&'static str> {
        vec!["spark_catalog"]
    }

    #[pyo3(name = "cacheTable")]
    fn cache_table(
        &self,
        _table_name: &str,
        _storage_level: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) {
        // No-op: no distributed cache
    }

    #[pyo3(name = "uncacheTable")]
    fn uncache_table(&self, _table_name: &str) {
        // No-op
    }

    #[pyo3(name = "clearCache")]
    fn clear_cache(&self) {
        // No-op
    }

    #[pyo3(name = "refreshTable")]
    fn refresh_table(&self, _table_name: &str) {
        // No-op
    }

    #[pyo3(name = "refreshByPath")]
    fn refresh_by_path(&self, _path: &str) {
        // No-op
    }

    #[pyo3(name = "recoverPartitions")]
    fn recover_partitions(&self, _table_name: &str) {
        // No-op
    }

    #[pyo3(name = "createTable")]
    fn create_table(&self, _table_name: &str) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "createTable not supported; use df.write().parquet(path)",
        ))
    }

    #[pyo3(name = "createExternalTable")]
    fn create_external_table(&self, _table_name: &str) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "createExternalTable not supported; use df.write().parquet(path)",
        ))
    }

    #[pyo3(name = "getDatabase")]
    fn get_database(&self, _db_name: &str) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "getDatabase not supported; no catalog databases",
        ))
    }

    #[pyo3(name = "getFunction")]
    fn get_function(&self, _function_name: &str) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "getFunction not supported",
        ))
    }

    #[pyo3(name = "getTable")]
    fn get_table(&self, _table_name: &str) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "getTable not supported; use spark.table(name) for temp views",
        ))
    }

    #[pyo3(name = "databaseExists")]
    fn database_exists(&self, db_name: &str) -> bool {
        db_name == "default"
    }

    #[pyo3(name = "functionExists")]
    fn function_exists(&self, _function_name: &str, _db_name: Option<&str>) -> bool {
        false
    }

    #[pyo3(name = "setCurrentCatalog")]
    fn set_current_catalog(&self, _catalog_name: &str) {
        // No-op
    }

    #[pyo3(name = "setCurrentDatabase")]
    fn set_current_database(&self, _db_name: &str) {
        // No-op
    }

    #[pyo3(name = "registerFunction")]
    fn register_function(
        &self,
        _name: &str,
        _f: &Bound<'_, pyo3::types::PyAny>,
        _return_type: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "registerFunction not supported; UDFs not supported",
        ))
    }

    #[pyo3(name = "isCached")]
    fn is_cached(&self, _table_name: &str) -> bool {
        false
    }

    #[pyo3(name = "listColumns")]
    fn list_columns(&self, _table_name: &str, _db_name: Option<&str>) -> Vec<String> {
        vec![]
    }

    #[pyo3(name = "listFunctions")]
    fn list_functions(&self, _db_name: Option<&str>, _pattern: Option<&str>) -> Vec<String> {
        vec![]
    }
}

/// Python wrapper for RuntimeConfig (spark.conf).
#[pyclass(name = "RuntimeConfig")]
pub struct PyRuntimeConfig {
    config: HashMap<String, String>,
}

#[pymethods]
impl PyRuntimeConfig {
    fn get(&self, key: &str) -> String {
        self.config.get(key).cloned().unwrap_or_default()
    }

    fn set(&self, _key: &str, _value: &str) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "RuntimeConfig.set not supported; config is read-only",
        ))
    }

    #[pyo3(name = "getAll")]
    fn get_all(&self) -> HashMap<String, String> {
        self.config.clone()
    }

    #[pyo3(name = "isModifiable")]
    fn is_modifiable(&self, _key: &str) -> bool {
        false
    }
}

/// Python wrapper for SparkSessionBuilder.
#[pyclass(name = "SparkSessionBuilder")]
pub struct PySparkSessionBuilder {
    app_name: Option<String>,
    master: Option<String>,
    config: std::collections::HashMap<String, String>,
}

#[pymethods]
impl PySparkSessionBuilder {
    /// Set the application name (e.g. for logging).
    ///
    /// Args:
    ///     name: Application name string.
    ///
    /// Returns:
    ///     Self for chaining.
    fn app_name<'a>(mut slf: PyRefMut<'a, Self>, name: &str) -> PyRefMut<'a, Self> {
        slf.app_name = Some(name.to_string());
        slf
    }

    /// Set the master URL (e.g. "local"). Reserved for API compatibility; execution is always local.
    ///
    /// Args:
    ///     master: Master URL string.
    ///
    /// Returns:
    ///     Self for chaining.
    fn master<'a>(mut slf: PyRefMut<'a, Self>, master: &str) -> PyRefMut<'a, Self> {
        slf.master = Some(master.to_string());
        slf
    }

    /// Set a config key-value pair. Keys and values are stored for compatibility; some may be used by the engine.
    ///
    /// Args:
    ///     key: Config key.
    ///     value: Config value.
    ///
    /// Returns:
    ///     Self for chaining.
    fn config<'a>(mut slf: PyRefMut<'a, Self>, key: &str, value: &str) -> PyRefMut<'a, Self> {
        slf.config.insert(key.to_string(), value.to_string());
        slf
    }

    /// Build and return a SparkSession with the current builder configuration.
    ///
    /// The returned session is stored as the default session for df.create_or_replace_temp_view(name)
    /// (PySpark parity: df.createOrReplaceTempView does not take a session argument).
    ///
    /// Returns:
    ///     SparkSession: New session.
    fn get_or_create(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PySparkSession>> {
        let mut config = std::collections::HashMap::new();
        for (k, v) in &slf.config {
            config.insert(k.clone(), v.clone());
        }
        let inner = SparkSession::new(slf.app_name.clone(), slf.master.clone(), config);
        if let Ok(mut guard) = default_session_cell().lock() {
            *guard = Some(inner.clone());
        }
        let session = PySparkSession { inner };
        let py_session = Py::new(py, session)?;
        Ok(py_session)
    }
}
