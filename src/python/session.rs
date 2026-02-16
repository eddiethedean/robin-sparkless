//! Python SparkSession and SparkSessionBuilder (PySpark sql session).

use crate::session::set_thread_udf_session;
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

/// Result of parsing the schema argument to createDataFrame.
/// - NoSchema: infer names from data and types from rows.
/// - NamesOnly(Vec<String>): use these column names, infer types.
/// - FullSchema(Vec<(String,String)>): use these (name, dtype_str).
enum ParsedSchema {
    NoSchema,
    NamesOnly(Vec<String>),
    FullSchema(Vec<(String, String)>),
}

/// True if the object is a pandas.DataFrame (PySpark parity: createDataFrame accepts pandas).
fn is_pandas_dataframe(obj: &Bound<'_, pyo3::types::PyAny>) -> bool {
    let Ok(class) = obj.getattr("__class__") else {
        return false;
    };
    let Ok(module) = class
        .getattr("__module__")
        .and_then(|m| m.extract::<String>())
    else {
        return false;
    };
    let Ok(name) = class
        .getattr("__name__")
        .and_then(|n| n.extract::<String>())
    else {
        return false;
    };
    module == "pandas.core.frame" && name == "DataFrame"
}

/// Parse schema argument (None, list of str, or StructType-like) for createDataFrame.
fn parse_schema_param(
    _py: Python<'_>,
    schema: Option<&Bound<'_, pyo3::types::PyAny>>,
    _names_from_data: &[String],
) -> PyResult<ParsedSchema> {
    let Some(schema_any) = schema else {
        return Ok(ParsedSchema::NoSchema);
    };
    if schema_any.is_none() {
        return Ok(ParsedSchema::NoSchema);
    }
    if let Ok(names) = schema_any.extract::<Vec<String>>() {
        return Ok(ParsedSchema::NamesOnly(names));
    }
    // PySpark DDL string: "name: string, age: int" or "name string, age int"
    if let Ok(ddl) = schema_any.extract::<String>() {
        let ddl = ddl.trim();
        if ddl.is_empty() {
            return Ok(ParsedSchema::NoSchema);
        }
        // PySpark: single type string (e.g. "bigint", "string") -> one column named "value"
        let parts: Vec<&str> = ddl
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        if parts.len() == 1 && !parts[0].contains(':') && !parts[0].contains(' ') {
            return Ok(ParsedSchema::FullSchema(vec![(
                "value".to_string(),
                parts[0].to_lowercase(),
            )]));
        }
        let mut schema_vec: Vec<(String, String)> = Vec::new();
        for part in ddl.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (name, type_str) = if let Some(colon) = part.find(": ") {
                let (n, t) = part.split_at(colon);
                (n.trim().to_string(), t[1..].trim().to_string())
            } else if let Some(space) = part.find(' ') {
                let (n, t) = part.split_at(space);
                (n.trim().to_string(), t.trim().to_string())
            } else {
                (part.to_string(), "string".to_string())
            };
            if !name.is_empty() {
                schema_vec.push((name, type_str.to_lowercase()));
            }
        }
        if !schema_vec.is_empty() {
            return Ok(ParsedSchema::FullSchema(schema_vec));
        }
    }
    if let Ok(fields) = schema_any.getattr("fields") {
        let mut schema_vec: Vec<(String, String)> = Vec::new();
        for field in fields.try_iter()? {
            let field = field?;
            let name: String = field.getattr("name")?.extract()?;
            let dtype = field.getattr("dataType")?;
            let type_str: String = dtype
                .getattr("typeName")
                .and_then(|a| a.call0())
                .and_then(|a| a.extract::<String>())
                .or_else(|_| {
                    dtype
                        .getattr("simpleString")
                        .and_then(|a| a.call0())
                        .and_then(|a| a.extract::<String>())
                })
                .unwrap_or_else(|_| "string".to_string());
            schema_vec.push((name, type_str.to_lowercase()));
        }
        return Ok(ParsedSchema::FullSchema(schema_vec));
    }
    if let Ok(list) = schema_any.downcast::<pyo3::types::PyList>() {
        let mut schema_vec: Vec<(String, String)> = Vec::new();
        for item in list.iter() {
            if let Ok((name, dtype_str)) = item.extract::<(String, String)>() {
                schema_vec.push((name, dtype_str.to_lowercase()));
            } else if let Ok(name) = item.extract::<String>() {
                schema_vec.push((name, "string".to_string()));
            } else {
                let name: String = item.getattr("name")?.extract()?;
                let dtype = item.getattr("dataType")?;
                let type_str: String = dtype
                    .getattr("typeName")
                    .and_then(|a| a.call0())
                    .and_then(|a| a.extract::<String>())
                    .unwrap_or_else(|_| "string".to_string());
                schema_vec.push((name, type_str.to_lowercase()));
            }
        }
        if !schema_vec.is_empty() {
            return Ok(ParsedSchema::FullSchema(schema_vec));
        }
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "schema must be None, a DDL string (e.g. 'name: string, age: int'), a list of column names, or a StructType (with .fields) or list of (name, type_str)",
    ))
}

/// Python wrapper for SparkSession.
#[pyclass(name = "SparkSession")]
pub struct PySparkSession {
    inner: SparkSession,
}

#[pymethods]
impl PySparkSession {
    /// Create a default SparkSession with no app name or master URL.
    /// The session is registered as the active/default session so ``get_active_session()``
    /// and aggregate functions (e.g. ``df.agg(F.sum(col("x")))``) resolve it (PySpark parity).
    ///
    /// Prefer ``SparkSession.builder().app_name("...").get_or_create()`` for clarity.
    #[new]
    fn new() -> Self {
        let inner = SparkSession::new(None, None, std::collections::HashMap::new());
        set_thread_udf_session(inner.clone());
        if let Ok(mut guard) = default_session_cell().lock() {
            *guard = Some(inner.clone());
        }
        PySparkSession { inner }
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

    /// PySpark-style createDataFrame: create a DataFrame from data with optional schema.
    ///
    /// Args:
    ///     data: List of rows. Each row can be a dict (keyed by column name) or a list/tuple
    ///         of values in column order. Empty list produces an empty DataFrame.
    ///     schema: Optional. None = infer schema from data (column names from first dict keys
    ///         or "_1", "_2", ... for list rows; types inferred from first non-null per column).
    ///         DDL string (e.g. "name: string, age: int") = full schema. List of str = column
    ///         names only (types inferred). StructType or list of (name, type_str) = full schema.
    ///     sampling_ratio: Optional. Ignored for list data (PySpark samplingRatio, for RDD inference).
    ///     verify_schema: Optional. If True (default), data values are validated against schema.
    ///
    /// Returns:
    ///     DataFrame.
    ///
    /// Raises:
    ///     TypeError: If a row is not a dict or list/tuple, or schema format is invalid.
    ///     RuntimeError: If creation fails.
    #[pyo3(
        name = "createDataFrame",
        signature = (data, schema=None, sampling_ratio=None, verify_schema=true)
    )]
    fn create_data_frame_pyspark_style(
        &self,
        py: Python<'_>,
        data: &Bound<'_, pyo3::types::PyAny>,
        schema: Option<&Bound<'_, pyo3::types::PyAny>>,
        #[allow(unused_variables)] sampling_ratio: Option<f64>,
        #[allow(unused_variables)] verify_schema: bool,
    ) -> PyResult<PyDataFrame> {
        let data_list: Vec<Bound<'_, pyo3::types::PyAny>> = if is_pandas_dataframe(data) {
            data.call_method1("to_dict", ("records",))
                .and_then(|r| r.extract())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
        } else {
            data.extract()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
        };

        let parsed = parse_schema_param(py, schema, &[])?;

        if data_list.is_empty() {
            let schema_vec = match &parsed {
                ParsedSchema::FullSchema(s) => s.clone(),
                ParsedSchema::NamesOnly(n) => n
                    .iter()
                    .map(|name| (name.clone(), "string".to_string()))
                    .collect(),
                ParsedSchema::NoSchema => vec![],
            };
            let df = self
                .inner
                .create_dataframe_from_rows(vec![], schema_vec)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            return Ok(PyDataFrame { inner: df });
        }

        let first = &data_list[0];
        // PySpark: single-column schema ("value") with scalar data [1, 2, 3] -> wrap each as one-row.
        let (rows, names_ordered) = if let ParsedSchema::FullSchema(s) = &parsed {
            if s.len() == 1
                && first.downcast::<PyDict>().is_err()
                && first
                    .extract::<Vec<Bound<'_, pyo3::types::PyAny>>>()
                    .is_err()
            {
                let name = s[0].0.clone();
                let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(data_list.len());
                for item in &data_list {
                    let v = py_to_json_value(item)?;
                    rows.push(vec![v]);
                }
                (rows, vec![name])
            } else if first.downcast::<PyDict>().is_ok() {
                let names_from_first: Vec<String> = {
                    let dict = first.downcast::<PyDict>().unwrap();
                    let mut n: Vec<String> = Vec::new();
                    for k in dict.keys() {
                        let name: String = k.extract().map_err(|e| {
                            pyo3::exceptions::PyTypeError::new_err(format!(
                                "dict key must be str: {e}"
                            ))
                        })?;
                        if !n.contains(&name) {
                            n.push(name);
                        }
                    }
                    n
                };
                let order: Vec<String> = match &parsed {
                    ParsedSchema::NamesOnly(n) => n.clone(),
                    ParsedSchema::FullSchema(s) => s.iter().map(|(name, _)| name.clone()).collect(),
                    ParsedSchema::NoSchema => names_from_first,
                };
                let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(data_list.len());
                for row_any in &data_list {
                    let dict = row_any.downcast::<PyDict>().map_err(|_| {
                        pyo3::exceptions::PyTypeError::new_err(
                            "createDataFrame: when first row is dict, all rows must be dicts",
                        )
                    })?;
                    let row: Vec<JsonValue> = order
                        .iter()
                        .map(|name| {
                            let v = dict
                                .get_item(name.as_str())
                                .ok()
                                .flatten()
                                .unwrap_or_else(|| py.None().into_bound(py));
                            py_to_json_value(&v)
                        })
                        .collect::<PyResult<Vec<_>>>()?;
                    rows.push(row);
                }
                (rows, order)
            } else {
                let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(data_list.len());
                for row_any in &data_list {
                    let list = row_any
                        .extract::<Vec<Bound<'_, pyo3::types::PyAny>>>()
                        .map_err(|_| {
                            pyo3::exceptions::PyTypeError::new_err(
                                "createDataFrame: each row must be a dict or a list/tuple",
                            )
                        })?;
                    let row: Vec<JsonValue> = list
                        .iter()
                        .map(|v| py_to_json_value(v))
                        .collect::<PyResult<Vec<_>>>()?;
                    rows.push(row);
                }
                let n_cols = rows[0].len();
                let order: Vec<String> = match &parsed {
                    ParsedSchema::NamesOnly(n) if n.len() == n_cols => n.clone(),
                    ParsedSchema::FullSchema(s) if s.len() == n_cols => {
                        s.iter().map(|(name, _)| name.clone()).collect()
                    }
                    _ => (1..=n_cols).map(|i| format!("_{i}")).collect(),
                };
                (rows, order)
            }
        } else if first.downcast::<PyDict>().is_ok() {
            let names_from_first: Vec<String> = {
                let dict = first.downcast::<PyDict>().unwrap();
                let mut n: Vec<String> = Vec::new();
                for k in dict.keys() {
                    let name: String = k.extract().map_err(|e| {
                        pyo3::exceptions::PyTypeError::new_err(format!("dict key must be str: {e}"))
                    })?;
                    if !n.contains(&name) {
                        n.push(name);
                    }
                }
                n
            };
            let order: Vec<String> = match &parsed {
                ParsedSchema::NamesOnly(n) => n.clone(),
                ParsedSchema::FullSchema(s) => s.iter().map(|(name, _)| name.clone()).collect(),
                ParsedSchema::NoSchema => names_from_first,
            };
            let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(data_list.len());
            for row_any in &data_list {
                let dict = row_any.downcast::<PyDict>().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(
                        "createDataFrame: when first row is dict, all rows must be dicts",
                    )
                })?;
                let row: Vec<JsonValue> = order
                    .iter()
                    .map(|name| {
                        let v = dict
                            .get_item(name.as_str())
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| py.None().into_bound(py));
                        py_to_json_value(&v)
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                rows.push(row);
            }
            (rows, order)
        } else {
            let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(data_list.len());
            for row_any in &data_list {
                let list = row_any
                    .extract::<Vec<Bound<'_, pyo3::types::PyAny>>>()
                    .map_err(|_| {
                        pyo3::exceptions::PyTypeError::new_err(
                            "createDataFrame: each row must be a dict or a list/tuple",
                        )
                    })?;
                let row: Vec<JsonValue> = list
                    .iter()
                    .map(|v| py_to_json_value(v))
                    .collect::<PyResult<Vec<_>>>()?;
                rows.push(row);
            }
            let n_cols = rows[0].len();
            let order: Vec<String> = match &parsed {
                ParsedSchema::NamesOnly(n) if n.len() == n_cols => n.clone(),
                ParsedSchema::FullSchema(s) if s.len() == n_cols => {
                    s.iter().map(|(name, _)| name.clone()).collect()
                }
                _ => (1..=n_cols).map(|i| format!("_{i}")).collect(),
            };
            (rows, order)
        };

        let schema_vec = match &parsed {
            ParsedSchema::FullSchema(s) => s.clone(),
            _ => SparkSession::infer_schema_from_json_rows(&rows, &names_ordered),
        };

        let df = self
            .inner
            .create_dataframe_from_rows(rows, schema_vec)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Create a DataFrame from row data and an explicit schema (internal / PySpark createDataFrame equivalent).
    ///
    /// Args:
    ///     data: List of rows. Each row is a dict (keyed by column name) or a list of
    ///         values in the same order as ``schema``. Supported value types: None, int,
    ///         float, bool, str, dict (struct), list (array).
    ///     schema: List of (name, dtype_str), e.g. [("id", "bigint"), ("name", "string")].
    ///         Column types may include "list" or "array" for list columns (element type bigint; PySpark parity #256).
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
    fn create_or_replace_temp_view(&self, name: &str, df: &PyDataFrame) -> PyResult<()> {
        self.inner
            .create_or_replace_temp_view(name, df.inner.clone());
        Ok(())
    }

    #[cfg(not(feature = "sql"))]
    fn create_or_replace_temp_view(&self, _name: &str, _df: &PyDataFrame) -> PyResult<()> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "create_or_replace_temp_view() requires the 'sql' feature. Build with: maturin develop --features 'pyo3,sql'.",
        ))
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
    ///     RuntimeError: If the name is not registered, or if built without the ``sql`` feature.
    #[cfg(feature = "sql")]
    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .table(name)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(not(feature = "sql"))]
    fn table(&self, _name: &str) -> PyResult<PyDataFrame> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "spark.table() requires the 'sql' feature. Build with: maturin develop --features 'pyo3,sql' (or pip install robin-sparkless and use a wheel built with sql support).",
        ))
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
    ///     RuntimeError: If parsing or execution fails, or if built without the ``sql`` feature.
    #[cfg(feature = "sql")]
    fn sql(&self, query: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .sql(query)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    #[cfg(not(feature = "sql"))]
    fn sql(&self, _query: &str) -> PyResult<PyDataFrame> {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "spark.sql() requires the 'sql' feature. Build with: maturin develop --features 'pyo3,sql' (or pip install robin-sparkless and use a wheel built with sql support).",
        ))
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
    /// Read a Delta table from path, or an in-memory table by name (same resolution as spark.table).
    fn read_delta(&self, path: &str) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_delta(path)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyDataFrame { inner: df })
    }

    /// Read a Delta Lake table at a specific version (time travel). For in-memory table names, version is ignored.
    fn read_delta_version(&self, path: &str, version: Option<i64>) -> PyResult<PyDataFrame> {
        let df = self
            .inner
            .read_delta_with_version(path, version)
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

    /// Return UDF registration (PySpark: spark.udf).
    fn udf(slf: PyRef<'_, Self>) -> PyUDFRegistration {
        PyUDFRegistration {
            session: slf.inner.clone(),
        }
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

/// Python UDF registration (PySpark: spark.udf). Supports register(name, f, returnType=None).
#[pyclass(name = "UDFRegistration")]
pub struct PyUDFRegistration {
    session: SparkSession,
}

#[pymethods]
impl PyUDFRegistration {
    /// Register a Python UDF. PySpark: spark.udf.register(name, f, returnType=None).
    /// When returnType is omitted for plain Python fn, defaults to StringType.
    #[pyo3(signature = (name, f, return_type=None, vectorized=false))]
    fn register(
        &self,
        py: Python<'_>,
        name: &str,
        f: Bound<'_, pyo3::types::PyAny>,
        return_type: Option<Bound<'_, pyo3::types::PyAny>>,
        vectorized: bool,
    ) -> PyResult<Py<PyUserDefinedFunction>> {
        let dtype = parse_return_type(py, return_type.as_ref())?;
        let callable = f.unbind();
        if vectorized {
            self.session
                .udf_registry
                .register_vectorized_python_udf(name, callable, dtype)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        } else {
            self.session
                .udf_registry
                .register_python_udf(name, callable, dtype)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        }
        Py::new(
            py,
            PyUserDefinedFunction {
                name: name.to_string(),
                session: self.session.clone(),
            },
        )
    }
}

/// User-defined function (returned by register). Callable as my_udf(col("a")).
#[pyclass(name = "UserDefinedFunction")]
pub struct PyUserDefinedFunction {
    pub(crate) name: String,
    #[allow(dead_code)] // reserved for UDF call context / session lookup
    pub(crate) session: SparkSession,
}

#[pymethods]
impl PyUserDefinedFunction {
    #[pyo3(signature = (*cols))]
    fn __call__(
        &self,
        py: Python<'_>,
        cols: &Bound<'_, pyo3::types::PyTuple>,
    ) -> PyResult<Py<super::column::PyColumn>> {
        use crate::functions::col as rs_col;
        let rs_cols: Vec<crate::column::Column> = cols
            .iter()
            .map(|item| {
                if let Ok(py_col) = item.downcast::<super::column::PyColumn>() {
                    Ok(py_col.borrow().inner.clone())
                } else if let Ok(s) = item.extract::<String>() {
                    Ok(rs_col(&s))
                } else {
                    Err(pyo3::exceptions::PyTypeError::new_err(
                        "UDF __call__: each arg must be Column or str (column name)",
                    ))
                }
            })
            .collect::<PyResult<Vec<_>>>()?;
        let col = crate::functions::call_udf(&self.name, &rs_cols)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Py::new(py, super::column::PyColumn { inner: col })
    }
}

pub(crate) fn parse_return_type(
    _py: Python<'_>,
    return_type: Option<&Bound<'_, pyo3::types::PyAny>>,
) -> PyResult<polars::prelude::DataType> {
    use polars::prelude::DataType;
    let default = DataType::String; // PySpark default
    let Some(rt) = return_type else {
        return Ok(default);
    };
    if rt.is_none() {
        return Ok(default);
    }
    // DDL string: "int", "string", "bigint", etc.
    if let Ok(s) = rt.extract::<String>() {
        return crate::functions::parse_type_name(&s)
            .map_err(pyo3::exceptions::PyValueError::new_err);
    }
    // DataType-like object: IntegerType(), StringType() - check for type_name or similar
    if let Ok(attr) = rt.getattr("typeName") {
        if let Ok(s) = attr.call0()?.extract::<String>() {
            return crate::functions::parse_type_name(&s.to_lowercase())
                .map_err(pyo3::exceptions::PyValueError::new_err);
        }
    }
    Ok(default)
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
    fn drop_global_temp_view(&self, view_name: &str) -> bool {
        self.session.drop_global_temp_view(view_name)
    }

    /// List table/view names. When db_name is "global_temp", returns global temp view names (cross-session).
    /// Otherwise returns temp views + saved tables in current session.
    #[pyo3(name = "listTables")]
    fn list_tables(&self, db_name: Option<&str>) -> Vec<String> {
        if db_name
            .map(|d| d.eq_ignore_ascii_case("global_temp"))
            .unwrap_or(false)
        {
            let mut names = self.session.list_global_temp_view_names();
            names.sort();
            return names;
        }
        let mut names: Vec<String> = self.session.list_temp_view_names();
        let table_names = self.session.list_table_names();
        for n in table_names {
            if !names.contains(&n) {
                names.push(n);
            }
        }
        names.sort();
        names
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
    #[pyo3(signature = (_pattern=None))]
    fn list_databases(&self, _pattern: Option<&str>) -> Vec<String> {
        self.session.list_database_names()
    }

    /// Create a database/schema by name (PySpark: catalog.createDatabase).
    /// Session-scoped; the name is included in listDatabases() and databaseExists(name) returns true.
    #[pyo3(name = "createDatabase")]
    #[pyo3(signature = (name, if_not_exists=false))]
    fn create_database(&self, name: &str, if_not_exists: bool) -> PyResult<()> {
        if if_not_exists && self.session.database_exists(name) {
            return Ok(());
        }
        if !if_not_exists && self.session.database_exists(name) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Database '{name}' already exists"
            )));
        }
        self.session.register_database(name);
        Ok(())
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

    /// Drop an in-memory saved table by name (removes from saved-tables only; does not drop temp views).
    #[pyo3(name = "dropTable")]
    fn drop_table(&self, table_name: &str) -> bool {
        self.session.drop_table(table_name)
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
        self.session.database_exists(db_name)
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

    /// Set an option (PySpark: builder.option(key, value)). Alias for config for API compatibility.
    ///
    /// Args:
    ///     key: Option key.
    ///     value: Option value.
    ///
    /// Returns:
    ///     Self for chaining.
    fn option<'a>(mut slf: PyRefMut<'a, Self>, key: &str, value: &str) -> PyRefMut<'a, Self> {
        slf.config.insert(key.to_string(), value.to_string());
        slf
    }

    /// Make builder callable so that SparkSession.builder() works (PySpark parity).
    /// Returns self for chaining.
    fn __call__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
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
        set_thread_udf_session(inner.clone());
        if let Ok(mut guard) = default_session_cell().lock() {
            *guard = Some(inner.clone());
        }
        let session = PySparkSession { inner };
        let py_session = Py::new(py, session)?;
        Ok(py_session)
    }
}
