//! Python DataFrameReader and read-option validation.

use super::*;

#[pyclass]
pub(super) struct PyDataFrameReader {
    session: Py<PyAny>,
    options: Vec<(String, String)>,
    format: Option<String>,
}

/// Normalize subset to Option<Vec<String>>: "col" -> Some(vec!["col".into()]), ["a","b"] -> as-is.
pub(super) fn normalize_subset(subset: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Vec<String>>> {
    let Some(s) = subset else {
        return Ok(None);
    };
    if s.is_none() {
        return Ok(None);
    }
    if let Ok(one) = s.extract::<String>() {
        return Ok(Some(vec![one]));
    }
    if let Ok(list) = s.cast::<PyList>() {
        let v: Vec<String> = list.iter().map(|x| x.extract()).collect::<PyResult<_>>()?;
        return Ok(Some(v));
    }
    if let Ok(tup) = s.cast::<PyTuple>() {
        let v: Vec<String> = tup.iter().map(|x| x.extract()).collect::<PyResult<_>>()?;
        return Ok(Some(v));
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "subset must be str or list/tuple of str",
    ))
}

pub(super) enum FillValueKind {
    Int,
    Float,
    String,
    Bool,
    Other,
}

pub(super) fn classify_fill_value_kind(value: &Bound<'_, PyAny>) -> FillValueKind {
    if value.is_none() {
        return FillValueKind::Other;
    }
    if value.extract::<bool>().is_ok() {
        return FillValueKind::Bool;
    }
    if value.extract::<i64>().is_ok() {
        return FillValueKind::Int;
    }
    if value.extract::<f64>().is_ok() {
        return FillValueKind::Float;
    }
    if value.extract::<String>().is_ok() {
        return FillValueKind::String;
    }
    FillValueKind::Other
}

pub(super) fn is_fill_compatible_with_schema_dtype(
    dt: &robin_sparkless::schema::DataType,
    kind: &FillValueKind,
) -> bool {
    use robin_sparkless::schema::DataType as SchemaDataType;
    match kind {
        // Integers and floats can fill numeric columns (int/long/float/double).
        FillValueKind::Int | FillValueKind::Float => matches!(
            dt,
            SchemaDataType::Short
                | SchemaDataType::Integer
                | SchemaDataType::Long
                | SchemaDataType::Float
                | SchemaDataType::Double
                | SchemaDataType::Decimal(_, _)
        ),
        // Strings fill string-like columns only.
        FillValueKind::String => matches!(dt, SchemaDataType::String),
        // Booleans fill boolean columns only.
        FillValueKind::Bool => matches!(dt, SchemaDataType::Boolean),
        // For other kinds (e.g. complex expressions), defer to engine behavior.
        FillValueKind::Other => true,
    }
}

/// Resolve cast type argument: string as-is, or type object's simpleString() or typeName() (e.g. IntegerType()).
pub(super) fn resolve_cast_type_name(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = value.extract::<String>() {
        return Ok(s);
    }
    // PySpark types have simpleString(); minimal types may only have typeName().
    if let Ok(meth) = value.getattr("simpleString") {
        if let Ok(s) = meth.call0().and_then(|v| v.extract::<String>()) {
            return Ok(s);
        }
    }
    if let Ok(meth) = value.getattr("typeName") {
        if let Ok(s) = meth.call0().and_then(|v| v.extract::<String>()) {
            return Ok(s);
        }
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "cast type must be string or type object with simpleString() or typeName()",
    ))
}

/// Convert PySpark option value to string (bool -> "true"/"false", int/float -> string, str as-is).
pub(super) fn option_value_to_string(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if value.is_none() {
        return Ok("".to_string());
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(if b { "true" } else { "false" }.to_string());
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(i.to_string());
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(f.to_string());
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(s);
    }
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "option value must be str, bool, int, or float",
    ))
}

impl PyDataFrameReader {
    pub(super) fn new(session: Py<PyAny>) -> Self {
        Self {
            session,
            options: Vec::new(),
            format: None,
        }
    }
}

#[pymethods]
impl PyDataFrameReader {
    fn option<'a>(
        mut slf: PyRefMut<'a, Self>,
        key: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        let s = option_value_to_string(value)?;
        slf.options.push((key.to_string(), s));
        Ok(slf)
    }

    /// PySpark: options(**kwargs). opts: dict of key -> value.
    fn options<'a>(
        mut slf: PyRefMut<'a, Self>,
        _py: Python<'_>,
        opts: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        let dict = opts.cast::<PyDict>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("options() requires a dict")
        })?;
        for (k, v) in dict.iter() {
            let key = k.extract::<String>()?;
            let value = option_value_to_string(&v)?;
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
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let mut reader = session.inner.read();
        for (k, v) in &self.options {
            reader = reader.option(k.as_str(), v.as_str());
        }
        if let Some(ref fmt) = self.format {
            reader = reader.format(fmt);
        }
        let mut wrapped = reader
            .load(Path::new(path))
            .map(|df| PyDataFrame::wrap_with_session(df, session.active_actions.clone()))
            .map_err(to_py_err)?;
        #[cfg(feature = "delta")]
        if self
            .format
            .as_deref()
            .is_some_and(|format| format.eq_ignore_ascii_case("delta"))
        {
            wrapped.logical_map_paths = robin_sparkless::delta::logical_map_paths(path)
                .map_err(to_py_err)?
                .into_iter()
                .collect();
        }
        Ok(wrapped)
    }

    fn csv(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let mut reader = session.inner.read();
        // PySpark parity: spark.read.csv() defaults to inferSchema=False (all strings) unless
        // the user explicitly sets inferSchema. Core Robin Sparkless defaults to inferring
        // types, so we override here at the Python binding layer only when no inferSchema
        // option has been provided.
        let has_infer_schema = self
            .options
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("inferSchema"));
        if !has_infer_schema {
            reader = reader.option("inferSchema", "false");
        }
        for (k, v) in &self.options {
            reader = reader.option(k.clone(), v.clone());
        }
        reader
            .csv(Path::new(path))
            .map(|df| PyDataFrame::wrap_with_session(df, session.active_actions.clone()))
            .map_err(to_py_err)
    }

    fn parquet(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let mut reader = session.inner.read();
        for (k, v) in &self.options {
            reader = reader.option(k.clone(), v.clone());
        }
        reader
            .parquet(Path::new(path))
            .map(|df| PyDataFrame::wrap_with_session(df, session.active_actions.clone()))
            .map_err(to_py_err)
    }

    fn json(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let mut reader = session.inner.read();
        for (k, v) in &self.options {
            reader = reader.option(k.clone(), v.clone());
        }
        reader
            .json(Path::new(path))
            .map(|df| PyDataFrame::wrap_with_session(df, session.active_actions.clone()))
            .map_err(to_py_err)
    }

    fn table(&self, py: Python<'_>, name: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let mut wrapped = session
            .inner
            .table(name)
            .map(|df| PyDataFrame::wrap_with_session(df, session.active_actions.clone()))
            .map_err(to_py_err)?;
        #[cfg(feature = "delta")]
        {
            wrapped.logical_map_paths =
                delta_map_paths_for_catalog_table(&session, name).map_err(to_py_err)?;
        }
        Ok(wrapped)
    }

    #[cfg(feature = "delta")]
    fn delta(&self, py: Python<'_>, path: &str) -> PyResult<PyDataFrame> {
        let session = self
            .session
            .bind(py)
            .cast::<PySparkSession>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession"))?
            .borrow();
        let mut wrapped = session
            .inner
            .read()
            .delta(Path::new(path))
            .map(|df| PyDataFrame::wrap_with_session(df, session.active_actions.clone()))
            .map_err(to_py_err)?;
        wrapped.logical_map_paths = robin_sparkless::delta::logical_map_paths(path)
            .map_err(to_py_err)?
            .into_iter()
            .collect();
        Ok(wrapped)
    }

    /// jdbc(url, table, properties=None) reads through a compiled native database adapter.
    ///
    /// sessionInitStatement and prepareQuery execute configured SQL when hardening permits it.
    /// queryTimeout has driver-specific semantics; SQLite uses a busy timeout and DB2 ignores it.
    /// Partition options are accepted without partitioned execution. fetchsize is not a verified
    /// fetch/materialization bound. See the documentation's per-driver option matrix.
    #[pyo3(signature = (url, table, properties=None))]
    fn jdbc(
        &self,
        py: Python<'_>,
        url: &str,
        table: &str,
        properties: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyDataFrame> {
        #[cfg(not(any(
            feature = "jdbc",
            feature = "jdbc_mysql",
            feature = "jdbc_mariadb",
            feature = "jdbc_mssql",
            feature = "jdbc_oracle",
            feature = "jdbc_db2",
            feature = "sqlite"
        )))]
        {
            let _ = (py, url, table, properties);
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "JDBC support requires building robin-sparkless with a JDBC feature enabled (e.g. 'jdbc', 'jdbc_mysql', 'sqlite').",
            ))
        }

        #[cfg(any(
            feature = "jdbc",
            feature = "jdbc_mysql",
            feature = "jdbc_mariadb",
            feature = "jdbc_mssql",
            feature = "jdbc_oracle",
            feature = "jdbc_db2",
            feature = "sqlite"
        ))]
        {
            let session = self
                .session
                .bind(py)
                .cast::<PySparkSession>()
                .map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected SparkSession")
                })?
                .borrow();

            // Build properties map from options + explicit properties dict
            let mut props: std::collections::HashMap<String, String> =
                std::collections::HashMap::new();

            // Add options set via .option() / .options()
            for (k, v) in &self.options {
                props.insert(k.clone(), v.clone());
            }

            // Add explicit properties dict (overrides options)
            if let Some(dict) = properties {
                for (k, v) in dict.iter() {
                    let key = k.extract::<String>()?;
                    let value = option_value_to_string(&v)?;
                    props.insert(key, value);
                }
            }

            session
                .inner
                .read()
                .jdbc(url, table, &props)
                .map(|df| PyDataFrame::wrap_with_session(df, session.active_actions.clone()))
                .map_err(to_py_err)
        }
    }
}
