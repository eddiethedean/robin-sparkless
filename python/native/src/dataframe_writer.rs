//! Python DataFrameWriter option and save behavior.

use super::*;

#[pyclass]
pub(super) struct PyDataFrameWriter {
    df: Py<PyAny>,
    active_actions: std::sync::Arc<ActionRegistry>,
    mode: String,
    options: Vec<(String, String)>,
    partition_by: Vec<String>,
    writer_format: Option<String>,
}

impl PyDataFrameWriter {
    pub(super) fn new(df: Py<PyAny>, active_actions: std::sync::Arc<ActionRegistry>) -> Self {
        Self {
            df,
            active_actions,
            mode: "overwrite".to_string(),
            options: Vec::new(),
            partition_by: Vec::new(),
            writer_format: None,
        }
    }

    fn begin_action(&self, session_actions: &std::sync::Arc<ActionRegistry>) -> ActiveAction {
        ActiveAction::new(&self.active_actions, Some(session_actions))
    }
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

fn validate_delta_options(options: &[(String, String)], save_mode: SaveMode) -> PyResult<()> {
    if options
        .iter()
        .any(|(key, _)| key.eq_ignore_ascii_case("replaceWhere"))
    {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Delta option 'replaceWhere' is not supported by this writer; refusing to overwrite the full table",
        ));
    }
    if save_mode == SaveMode::Overwrite
        && options.iter().any(|(key, value)| {
            key.eq_ignore_ascii_case("partitionOverwriteMode")
                && value.eq_ignore_ascii_case("dynamic")
        })
    {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Delta option 'partitionOverwriteMode=dynamic' is not supported by this writer; refusing to overwrite the full table",
        ));
    }
    Ok(())
}

#[pymethods]
impl PyDataFrameWriter {
    fn mode<'a>(mut slf: PyRefMut<'a, Self>, mode: &str) -> PyRefMut<'a, Self> {
        slf.mode = mode.to_string();
        slf
    }

    fn option<'a>(
        mut slf: PyRefMut<'a, Self>,
        key: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        slf.options
            .push((key.to_string(), option_value_to_string(value)?));
        Ok(slf)
    }

    /// Accept mappings or ordered pairs, preserving duplicate keys like the Rust writer.
    #[pyo3(signature = (opts=None, **kwargs))]
    fn options<'a>(
        mut slf: PyRefMut<'a, Self>,
        opts: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        let mut pairs = Vec::new();
        if let Some(opts) = opts {
            if let Ok(mapping) = opts.cast::<PyDict>() {
                for (key, value) in mapping.iter() {
                    pairs.push((key.extract::<String>()?, option_value_to_string(&value)?));
                }
            } else {
                for (key, value) in opts.extract::<Vec<(String, Py<PyAny>)>>()? {
                    pairs.push((key, option_value_to_string(value.bind(slf.py()))?));
                }
            }
        }
        if let Some(kwargs) = kwargs {
            for (key, value) in kwargs.iter() {
                pairs.push((key.extract::<String>()?, option_value_to_string(&value)?));
            }
        }
        slf.options.extend(pairs);
        Ok(slf)
    }

    #[pyo3(name = "partitionBy", signature = (*cols))]
    fn partition_by<'a>(
        mut slf: PyRefMut<'a, Self>,
        cols: &Bound<'_, PyTuple>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        slf.partition_by = py_tuple_or_single_to_vec_string(cols)?;
        Ok(slf)
    }

    #[pyo3(name = "format")]
    fn writer_format<'a>(mut slf: PyRefMut<'a, Self>, fmt: &str) -> PyRefMut<'a, Self> {
        slf.writer_format = Some(fmt.to_string());
        slf
    }

    fn parquet(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df =
            self.df.bind(py).cast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
        let inner = df.borrow();
        let mut w = inner
            .inner
            .write()
            .mode(write_mode_from_str(&self.mode))
            .format(WriteFormat::Parquet);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    fn csv(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df =
            self.df.bind(py).cast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
        let inner = df.borrow();
        let mut w = inner
            .inner
            .write()
            .mode(write_mode_from_str(&self.mode))
            .format(WriteFormat::Csv);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    fn json(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df =
            self.df.bind(py).cast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
        let inner = df.borrow();
        let mut w = inner
            .inner
            .write()
            .mode(write_mode_from_str(&self.mode))
            .format(WriteFormat::Json);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    /// PySpark: save(path). Uses the selected format (Parquet by default).
    fn save(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        let df =
            self.df.bind(py).cast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
        let inner = df.borrow();

        let format = self
            .writer_format
            .as_deref()
            .unwrap_or("parquet")
            .to_ascii_lowercase();

        if format == "delta" {
            let path = robin_sparkless_core::maybe_confine_files_path(Path::new(path))
                .map_err(to_py_err)?;

            let merge_schema = self
                .options
                .iter()
                .rev()
                .find(|(key, _)| key.eq_ignore_ascii_case("mergeSchema"))
                .is_some_and(|(_, value)| value.eq_ignore_ascii_case("true") || value == "1");
            let overwrite_schema = self
                .options
                .iter()
                .rev()
                .find(|(key, _)| key.eq_ignore_ascii_case("overwriteSchema"))
                .is_some_and(|(_, value)| value.eq_ignore_ascii_case("true") || value == "1");
            let save_mode = match self.mode.to_ascii_lowercase().as_str() {
                "append" => SaveMode::Append,
                "overwrite" => SaveMode::Overwrite,
                "ignore" => SaveMode::Ignore,
                "error" | "errorifexists" => SaveMode::ErrorIfExists,
                mode => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "unsupported DataFrameWriter save mode: {mode}"
                    )));
                }
            };
            validate_delta_options(&self.options, save_mode)?;

            let mut logical_map_paths = inner.logical_map_paths.iter().cloned().collect::<Vec<_>>();
            logical_map_paths.sort();

            return match inner
                .inner
                .write_delta_with_save_options_and_logical_map_paths(
                    &path,
                    save_mode,
                    merge_schema,
                    overwrite_schema,
                    &self.partition_by,
                    &logical_map_paths,
                ) {
                Ok(()) => Ok(()),
                Err(error)
                    if save_mode == SaveMode::ErrorIfExists
                        && error
                            .to_string()
                            .contains("Delta table already exists (save mode ErrorIfExists)") =>
                {
                    Err(PyErr::new::<pyo3::exceptions::PyFileExistsError, _>(
                        format!("Delta table already exists at {}", path.display()),
                    ))
                }
                Err(error) => Err(to_py_err(error)),
            };
        }

        let write_format = match format.as_str() {
            "parquet" => WriteFormat::Parquet,
            "csv" => WriteFormat::Csv,
            "json" => WriteFormat::Json,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "unsupported DataFrameWriter format for save(): {format}"
                )));
            }
        };

        let mut w = inner
            .inner
            .write()
            .mode(write_mode_from_str(&self.mode))
            .format(write_format);
        for (k, v) in &self.options {
            w = w.option(k, v);
        }
        if !self.partition_by.is_empty() {
            w = w.partition_by(self.partition_by.iter().map(|s| s.as_str()));
        }
        w.save(Path::new(path)).map_err(to_py_err)
    }

    /// PySpark: jdbc(url, table, properties). Properties is a dict of string -> string.
    #[pyo3(signature = (url, table, properties, mode=None))]
    fn jdbc(
        &self,
        py: Python<'_>,
        url: &str,
        table: &str,
        properties: &Bound<'_, PyDict>,
        mode: Option<&str>,
    ) -> PyResult<()> {
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
            let _ = (py, url, table, properties, mode);
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "JDBC/SQLite support requires building robin-sparkless with a JDBC/SQLite feature enabled (e.g. 'jdbc', 'jdbc_mysql', 'sqlite').",
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
            let df = self.df.bind(py).cast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
            let inner = df.borrow();

            let mut props: Vec<(String, String)> = self.options.clone();
            for (k, v) in properties {
                let key: String = k.extract()?;
                let val: String = v.extract()?;
                props.push((key, val));
            }

            let save_mode = mode
                .map(save_mode_from_str)
                .unwrap_or_else(|| save_mode_from_str(&self.mode));

            inner
                .inner
                .write()
                .jdbc(url, table, &props, save_mode)
                .map_err(to_py_err)
        }
    }

    /// PySpark: saveAsTable(name). mode: "error"|"overwrite"|"append"|"ignore".
    #[pyo3(signature = (name, mode=None))]
    fn save_as_table(&self, py: Python<'_>, name: &str, mode: Option<&str>) -> PyResult<()> {
        let session = require_active_session(py)?;
        let session = session.bind(py).cast::<PySparkSession>()?;
        let session = session.borrow();
        let action = self.begin_action(&session.active_actions);
        let token = action.token.clone();
        let df =
            self.df.bind(py).cast::<PyDataFrame>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("expected DataFrame")
            })?;
        let inner = df.borrow();
        let active = session.inner.clone();
        drop(session);
        let save_mode = save_mode_from_str(mode.unwrap_or(self.mode.as_str()));
        if self
            .writer_format
            .as_deref()
            .is_some_and(|format| format.eq_ignore_ascii_case("delta"))
        {
            validate_delta_options(&self.options, save_mode)?;
        }
        // Thread writer-level options (including format) through to the backend so
        // saveAsTable can make format-aware decisions (e.g. Delta overwrite parity).
        let mut options: Vec<(String, String)> = self.options.clone();
        if let Some(fmt) = &self.writer_format {
            options.push(("sparkless.writer.format".to_string(), fmt.to_string()));
        }
        if !inner.logical_map_paths.is_empty() {
            let mut paths = inner.logical_map_paths.iter().cloned().collect::<Vec<_>>();
            paths.sort();
            options.push((
                "sparkless.delta.logicalMapPaths".to_string(),
                serde_json::to_string(&paths).map_err(to_py_err)?,
            ));
        }
        let dataframe = inner.inner.clone();
        drop(inner);
        let name = name.to_string();
        let partition_by = self.partition_by.clone();
        py.detach(move || {
            let mut writer = dataframe.write();
            if !partition_by.is_empty() {
                writer = writer.partition_by(partition_by);
            }
            writer.save_as_table_with_options_and_cancellation(
                &active, &name, save_mode, &options, &token,
            )
        })
        .map_err(to_py_err)
    }

    /// PySpark camelCase alias for save_as_table
    #[pyo3(name = "saveAsTable", signature = (name, mode=None))]
    fn save_as_table_camel(&self, py: Python<'_>, name: &str, mode: Option<&str>) -> PyResult<()> {
        self.save_as_table(py, name, mode)
    }
}
