//! GIL-safe adapter from Python providers to the engine-neutral catalog contract.

use pyo3::exceptions::{PyAttributeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule};
use pyo3::IntoPyObjectExt;
use robin_sparkless_core::engine::{
    ArrowIpcBatchStream, ExternalCatalogProvider, ExternalTableDescriptor, ExternalTableSaveMode,
    ExternalTableWriteBatchRequest, ExternalTableWriteRequest, ExternalTableWriteResult,
};
use robin_sparkless_core::EngineError;
use std::collections::HashMap;
#[cfg(feature = "delta")]
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

type PyObject = Py<PyAny>;

pub(crate) type ResolvedTables = Arc<Mutex<HashMap<String, ExternalTableDescriptor>>>;

#[cfg(feature = "delta")]
pub(crate) fn logical_map_paths(schema: &robin_sparkless_core::StructType) -> HashSet<String> {
    fn visit(dtype: &robin_sparkless_core::DataType, path: &str, paths: &mut HashSet<String>) {
        use robin_sparkless_core::DataType;
        match dtype {
            DataType::Map(key, value) => {
                paths.insert(path.to_string());
                visit(key, &format!("{path}{{key}}"), paths);
                visit(value, &format!("{path}{{value}}"), paths);
            }
            DataType::Array(element) => visit(element, &format!("{path}[]"), paths),
            DataType::Struct(fields) => {
                for field in fields {
                    visit(
                        &field.data_type,
                        &super::arrow_field_path(path, &field.name),
                        paths,
                    );
                }
            }
            _ => {}
        }
    }
    let mut paths = HashSet::new();
    for field in schema.fields() {
        visit(
            &field.data_type,
            &super::arrow_field_path("", &field.name),
            &mut paths,
        );
    }
    paths
}

pub(crate) struct PythonExternalCatalogProvider {
    // Py owns a strong reference and may cross Rust threads; every access acquires the GIL.
    provider: Py<PyAny>,
    tables: ResolvedTables,
}

#[pyclass(module = "sparkless.external_catalog", unsendable)]
pub(crate) struct PyArrowIpcBatchIterator {
    batches: Option<ArrowIpcBatchStream>,
}

#[pymethods]
impl PyArrowIpcBatchIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        let Some(batches) = self.batches.take() else {
            return Err(pyo3::exceptions::PyStopIteration::new_err(()));
        };
        let (batches, next) = py.detach(move || {
            let mut batches = batches;
            let next = batches.next();
            (batches, next)
        });
        self.batches = Some(batches);
        let Some(next) = next else {
            self.batches.take();
            return Err(pyo3::exceptions::PyStopIteration::new_err(()));
        };
        let stream = match next {
            Ok(stream) => stream,
            Err(error) => {
                self.batches.take();
                return Err(super::to_py_err(error));
            }
        };
        let batch = (|| {
            let pyarrow = PyModule::import(py, "pyarrow")?;
            let reader = pyarrow
                .getattr("ipc")?
                .getattr("open_stream")?
                .call1((PyBytes::new(py, &stream.bytes),))?;
            reader.call_method0("read_next_batch")?.into_py_any(py)
        })();
        if batch.is_err() {
            self.batches.take();
        }
        batch
    }

    fn close(&mut self) {
        self.batches.take();
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) {
        self.batches.take();
    }
}

impl PythonExternalCatalogProvider {
    pub(crate) fn new(
        py: Python<'_>,
        provider: Py<PyAny>,
        tables: ResolvedTables,
    ) -> PyResult<Self> {
        if !provider.bind(py).getattr("resolve_table")?.is_callable() {
            return Err(PyTypeError::new_err(
                "provider.resolve_table must be callable",
            ));
        }
        match provider.bind(py).getattr("write_table") {
            Ok(method) if !method.is_callable() => {
                return Err(PyTypeError::new_err(
                    "provider.write_table must be callable",
                ));
            }
            Err(error) if !error.is_instance_of::<PyAttributeError>(py) => return Err(error),
            _ => {}
        }
        match provider.bind(py).getattr("write_table_batches") {
            Ok(method) if !method.is_callable() => {
                return Err(PyTypeError::new_err(
                    "provider.write_table_batches must be callable",
                ));
            }
            Err(error) if !error.is_instance_of::<PyAttributeError>(py) => return Err(error),
            _ => {}
        }
        Ok(Self { provider, tables })
    }
}

fn descriptor_from_python(value: &Bound<'_, PyAny>) -> PyResult<ExternalTableDescriptor> {
    let module = PyModule::import(value.py(), "sparkless.external_catalog")?;
    let json: String = module
        .getattr("_descriptor_to_json")?
        .call1((value,))?
        .extract()?;
    serde_json::from_str(&json).map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "invalid external table descriptor: {error}"
        ))
    })
}

fn callback_error(operation: &str, error: PyErr) -> EngineError {
    EngineError::User(format!("Python external catalog {operation}: {error}"))
}

fn write_result_from_python(value: &Bound<'_, PyAny>) -> PyResult<ExternalTableWriteResult> {
    let module = PyModule::import(value.py(), "sparkless.external_catalog")?;
    let (descriptor_json, committed_version): (String, i64) = module
        .getattr("_write_result_to_parts")?
        .call1((value,))?
        .extract()?;
    let table = serde_json::from_str(&descriptor_json).map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid write receipt: {error}"))
    })?;
    Ok(ExternalTableWriteResult {
        table,
        committed_version,
    })
}

#[pyfunction(name = "_validate_external_table_descriptor")]
pub(crate) fn validate_descriptor(table: &Bound<'_, PyAny>) -> PyResult<()> {
    descriptor_from_python(table)?
        .validate()
        .map_err(super::to_py_err)
}

#[pyfunction(name = "_validate_external_table_write_result")]
pub(crate) fn validate_write_result(
    result: &Bound<'_, PyAny>,
    previous: &Bound<'_, PyAny>,
) -> PyResult<()> {
    write_result_from_python(result)?
        .validate_against(&descriptor_from_python(previous)?)
        .map_err(super::to_py_err)
}

impl ExternalCatalogProvider for PythonExternalCatalogProvider {
    fn resolve_table(
        &self,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
        Python::attach(|py| -> PyResult<_> {
            let value = self
                .provider
                .bind(py)
                .call_method1("resolve_table", (identifier,))?;
            if value.is_none() {
                self.tables
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(identifier);
                return Ok(None);
            }
            let descriptor = descriptor_from_python(&value)?;
            self.tables
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(identifier.to_string(), descriptor.clone());
            Ok(Some(descriptor))
        })
        .map_err(|error| callback_error("resolve_table", error))
    }

    fn write_table(
        &self,
        request: ExternalTableWriteRequest,
    ) -> Result<ExternalTableWriteResult, EngineError> {
        Python::attach(|py| -> PyResult<_> {
            let method = self
                .provider
                .bind(py)
                .getattr("write_table")
                .map_err(|error| {
                    if error.is_instance_of::<PyAttributeError>(py) {
                        pyo3::exceptions::PyNotImplementedError::new_err(
                            "external catalog provider does not support table writes",
                        )
                    } else {
                        error
                    }
                })?;
            let module = PyModule::import(py, "sparkless.external_catalog")?;
            let descriptor_json = serde_json::to_string(&request.table)
                .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
            let table = module
                .getattr("_descriptor_from_json")?
                .call1((descriptor_json,))?;
            let mode = match request.mode {
                ExternalTableSaveMode::ErrorIfExists => "errorifexists",
                ExternalTableSaveMode::Overwrite => "overwrite",
                ExternalTableSaveMode::Append => "append",
                ExternalTableSaveMode::Ignore => "ignore",
            };
            let kwargs = PyDict::new(py);
            kwargs.set_item("identifier", request.identifier)?;
            kwargs.set_item("table", table)?;
            kwargs.set_item(
                "data",
                module
                    .getattr("ArrowIpcStream")?
                    .call1((PyBytes::new(py, &request.data.bytes),))?,
            )?;
            kwargs.set_item(
                "mode",
                module.getattr("ExternalTableSaveMode")?.call1((mode,))?,
            )?;
            kwargs.set_item("format", request.format)?;
            kwargs.set_item("options", request.options)?;
            kwargs.set_item("partition_by", request.partition_by)?;
            let value = module
                .getattr("ExternalTableWriteRequest")?
                .call((), Some(&kwargs))?;
            let result = method.call1((value,))?;
            write_result_from_python(&result)
        })
        .map_err(|error| callback_error("write_table", error))
    }

    fn supports_batch_writes(&self) -> bool {
        Python::attach(|py| {
            self.provider
                .bind(py)
                .getattr("write_table_batches")
                .is_ok_and(|method| method.is_callable())
        })
    }

    fn write_table_batches(
        &self,
        request: ExternalTableWriteBatchRequest,
    ) -> Result<ExternalTableWriteResult, EngineError> {
        Python::attach(|py| -> PyResult<_> {
            let method = self.provider.bind(py).getattr("write_table_batches")?;
            let module = PyModule::import(py, "sparkless.external_catalog")?;
            let descriptor_json = serde_json::to_string(&request.table)
                .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
            let table = module
                .getattr("_descriptor_from_json")?
                .call1((descriptor_json,))?;
            let mode = match request.mode {
                ExternalTableSaveMode::ErrorIfExists => "errorifexists",
                ExternalTableSaveMode::Overwrite => "overwrite",
                ExternalTableSaveMode::Append => "append",
                ExternalTableSaveMode::Ignore => "ignore",
            };
            let batches = Py::new(
                py,
                PyArrowIpcBatchIterator {
                    batches: Some(request.batches),
                },
            )?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("identifier", request.identifier)?;
            kwargs.set_item("table", table)?;
            kwargs.set_item("data", py.None())?;
            kwargs.set_item("batches", batches.bind(py))?;
            kwargs.set_item(
                "mode",
                module.getattr("ExternalTableSaveMode")?.call1((mode,))?,
            )?;
            kwargs.set_item("format", request.format)?;
            kwargs.set_item("options", request.options)?;
            kwargs.set_item("partition_by", request.partition_by)?;
            let write_request = module
                .getattr("ExternalTableWriteRequest")?
                .call((), Some(&kwargs))?;
            let callback_result = method.call1((write_request,));
            let close_result = batches.bind(py).call_method0("close");
            let value = match (callback_result, close_result) {
                (Ok(value), Ok(_)) => value,
                (Err(error), _) => return Err(error),
                (Ok(_), Err(error)) => return Err(error),
            };
            write_result_from_python(&value)
        })
        .map_err(|error| callback_error("write_table_batches", error))
    }
}
