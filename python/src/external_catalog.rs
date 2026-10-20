//! GIL-safe adapter from Python providers to the engine-neutral catalog contract.

use pyo3::exceptions::{PyAttributeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule};
use robin_sparkless_core::engine::{
    ExternalCatalogProvider, ExternalTableDescriptor, ExternalTableSaveMode,
    ExternalTableWriteRequest, ExternalTableWriteResult,
};
use robin_sparkless_core::EngineError;
use std::collections::HashMap;
#[cfg(feature = "delta")]
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub(crate) type ResolvedSchemas = Arc<Mutex<HashMap<String, robin_sparkless_core::StructType>>>;

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
    schemas: ResolvedSchemas,
}

impl PythonExternalCatalogProvider {
    pub(crate) fn new(
        py: Python<'_>,
        provider: Py<PyAny>,
        schemas: ResolvedSchemas,
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
        Ok(Self { provider, schemas })
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
                self.schemas
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(identifier);
                return Ok(None);
            }
            let descriptor = descriptor_from_python(&value)?;
            self.schemas
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(identifier.to_string(), descriptor.schema.clone());
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
}
