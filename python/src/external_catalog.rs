//! GIL-safe adapter from Python providers to the engine-neutral catalog contract.

use pyo3::exceptions::{PyAttributeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyModule, PyString};
use pyo3::IntoPyObjectExt;
use robin_sparkless_core::engine::{
    ArrowIpcBatchStream, ExternalCatalogProvider, ExternalTableDescriptor, ExternalTableSaveMode,
    ExternalTableWriteBatchRequest, ExternalTableWriteRequest, ExternalTableWriteResult,
};
use robin_sparkless_core::EngineError;
use serde::{Deserialize, Serialize};
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
            DataType::LogicalStruct(fields) => {
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
    for field in schema.logical_fields() {
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

const CALLBACK_ERROR_SUFFIX: &str = "\u{1e}";

fn callback_error_prefix() -> &'static str {
    static PREFIX: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PREFIX.get_or_init(|| {
        format!(
            "\u{1e}sparkless-external-catalog-error:{}:",
            uuid::Uuid::new_v4().simple()
        )
    })
}

/// This wire payload crosses Polars' string-only error boundary. It is intentionally private;
/// Python callers receive the native SparklessExternalCatalogError class instead.
#[derive(Debug, Serialize, Deserialize)]
struct CallbackFailure {
    operation: String,
    category: String,
    code: Option<String>,
    identifier: String,
    version: Option<i64>,
    expected_version: Option<i64>,
    actual_version: Option<i64>,
    retryability: String,
    provider_error_type: String,
}

impl CallbackFailure {
    fn message(&self) -> String {
        let mut message = format!(
            "external catalog provider {} callback failed (category: {})",
            self.operation, self.category
        );
        if let Some(code) = &self.code {
            message.push_str(&format!(", code: {code}"));
        }
        message
    }
}

fn safe_identifier(identifier: &str) -> String {
    let identifier = identifier.split(['?', '#']).next().unwrap_or(identifier);
    if let Some((scheme, rest)) = identifier.split_once("://") {
        let authority = rest.split('/').next().unwrap_or(rest);
        let authority = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        let safe_scheme = !scheme.is_empty()
            && scheme
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '+' | '.' | '-'));
        let safe_authority = !authority.is_empty()
            && authority
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | ':' | '[' | ']'));
        if safe_scheme && safe_authority {
            return format!("{scheme}://{authority}")
                .chars()
                .take(256)
                .collect();
        }
        return "<redacted>".into();
    }
    if identifier.len() <= 256
        && identifier
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '$' | '-'))
    {
        identifier.into()
    } else {
        "<redacted>".into()
    }
}

fn is_safe_token(value: &str) -> bool {
    let mut chars = value.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic())
        && value.len() <= 64
        && chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
}

fn safe_token(value: &Bound<'_, PyAny>) -> Option<String> {
    if !value.is_instance_of::<PyString>() {
        return None;
    }
    let value = value.extract::<String>().ok()?;
    is_safe_token(&value).then_some(value)
}

fn safe_instance_field<'py>(
    instance_fields: Option<&Bound<'py, PyDict>>,
    exception: &Bound<'py, PyAny>,
    key: &str,
) -> Option<Bound<'py, PyAny>> {
    instance_fields
        .and_then(|fields| fields.get_item(key).ok().flatten())
        .or_else(|| exception.getattr(key).ok())
}

fn safe_version_field(
    instance_fields: Option<&Bound<'_, PyDict>>,
    exception: &Bound<'_, PyAny>,
    key: &str,
) -> Option<i64> {
    let value = safe_instance_field(instance_fields, exception, key)?;
    if value.is_instance_of::<PyBool>() {
        return None;
    }
    value.extract::<i64>().ok()
}

fn class_category(name: &str) -> String {
    let name = name
        .strip_suffix("Exception")
        .or_else(|| name.strip_suffix("Error"))
        .unwrap_or(name);
    let mut category = String::new();
    let chars: Vec<char> = name.chars().collect();
    for (index, ch) in chars.iter().copied().enumerate() {
        if ch.is_ascii_uppercase() && index > 0 {
            let previous = chars[index - 1];
            let next_is_lower = chars
                .get(index + 1)
                .is_some_and(|next| next.is_ascii_lowercase());
            if previous.is_ascii_lowercase() || previous.is_ascii_digit() || next_is_lower {
                category.push('_');
            }
        }
        category.push(ch.to_ascii_lowercase());
    }
    if !is_safe_token(&category) {
        "provider_failure".into()
    } else {
        category
    }
}

fn callback_failure(
    py: Python<'_>,
    operation: &str,
    identifier: &str,
    version: Option<i64>,
    error: &PyErr,
) -> CallbackFailure {
    let exception = error.value(py);
    let exception_type = exception.get_type();
    let provider_error_type = exception_type
        .getattr("__name__")
        .and_then(|value| value.extract::<String>())
        .ok()
        .filter(|value| is_safe_token(value))
        .unwrap_or_else(|| "Exception".into());
    let is_builtin = exception_type
        .getattr("__module__")
        .and_then(|value| value.extract::<String>())
        .is_ok_and(|module| module == "builtins");
    let instance_fields = exception
        .getattr("__dict__")
        .ok()
        .and_then(|fields| fields.cast_into::<PyDict>().ok());
    let instance_fields = instance_fields.as_ref();

    let explicit_category = safe_instance_field(instance_fields, exception, "category")
        .as_ref()
        .and_then(safe_token);
    let code = safe_instance_field(instance_fields, exception, "code")
        .as_ref()
        .and_then(safe_token);
    let category = explicit_category.unwrap_or_else(|| {
        if !is_builtin {
            class_category(&provider_error_type)
        } else if provider_error_type == "NotImplementedError" {
            "unsupported_operation".into()
        } else if let Some(code) = &code {
            code.clone()
        } else {
            "provider_failure".into()
        }
    });
    let retryability = safe_instance_field(instance_fields, exception, "retryability")
        .as_ref()
        .and_then(safe_token)
        .filter(|value| matches!(value.as_str(), "retryable" | "terminal" | "uncertain"))
        .or_else(|| {
            safe_instance_field(instance_fields, exception, "retryable")
                .filter(|value| value.is_instance_of::<PyBool>())
                .and_then(|value| value.extract::<bool>().ok())
                .map(|retryable| if retryable { "retryable" } else { "terminal" }.to_string())
        })
        .unwrap_or_else(|| match category.as_str() {
            "stale_snapshot" | "schema_mismatch" | "unsupported_operation" => {
                "terminal".to_string()
            }
            _ if operation == "resolve_table" => "retryable".to_string(),
            _ => "uncertain".to_string(),
        });

    CallbackFailure {
        operation: operation.into(),
        category,
        code,
        identifier: safe_identifier(identifier),
        version,
        expected_version: safe_version_field(instance_fields, exception, "expected_version"),
        actual_version: safe_version_field(instance_fields, exception, "actual_version"),
        retryability,
        provider_error_type,
    }
}

fn callback_error(
    py: Python<'_>,
    operation: &str,
    identifier: &str,
    version: Option<i64>,
    error: PyErr,
) -> EngineError {
    let failure = callback_failure(py, operation, identifier, version, &error);
    let serialized = serde_json::to_string(&failure).unwrap_or_else(|_| "{}".into());
    EngineError::User(format!(
        "{} {}{serialized}{CALLBACK_ERROR_SUFFIX}",
        failure.message(),
        callback_error_prefix()
    ))
}

pub(crate) fn callback_failure_from_message(py: Python<'_>, message: &str) -> Option<PyErr> {
    let prefix = callback_error_prefix();
    let start = message.find(prefix)? + prefix.len();
    let end = message[start..].find(CALLBACK_ERROR_SUFFIX)? + start;
    let failure = serde_json::from_str::<CallbackFailure>(&message[start..end]).ok()?;
    let error = super::SparklessExternalCatalogError::new_err(failure.message());
    let value = error.value(py);
    let _ = value.setattr("operation", failure.operation);
    let _ = value.setattr("category", failure.category);
    let _ = value.setattr("code", failure.code);
    let _ = value.setattr("identifier", failure.identifier);
    let _ = value.setattr("version", failure.version);
    let _ = value.setattr("expected_version", failure.expected_version);
    let _ = value.setattr("actual_version", failure.actual_version);
    let _ = value.setattr("retryability", failure.retryability);
    let _ = value.setattr("provider_error_type", failure.provider_error_type);
    Some(error)
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
        .map_err(|error| {
            Python::attach(|py| callback_error(py, "resolve_table", identifier, None, error))
        })
    }

    fn write_table(
        &self,
        request: ExternalTableWriteRequest,
    ) -> Result<ExternalTableWriteResult, EngineError> {
        let failure_identifier = request.identifier.clone();
        let failure_version = request.table.version;
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
            let payload = module
                .getattr("_normalize_python_provider_timestamp_ipc")?
                .call1((PyBytes::new(py, &request.data.bytes), &table))?;
            let mode = match request.mode {
                ExternalTableSaveMode::ErrorIfExists => "errorifexists",
                ExternalTableSaveMode::Overwrite => "overwrite",
                ExternalTableSaveMode::Append => "append",
                ExternalTableSaveMode::Ignore => "ignore",
            };
            let kwargs = PyDict::new(py);
            kwargs.set_item("identifier", request.identifier)?;
            kwargs.set_item("table", table)?;
            kwargs.set_item("data", module.getattr("ArrowIpcStream")?.call1((payload,))?)?;
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
        .map_err(|error| {
            Python::attach(|py| {
                callback_error(
                    py,
                    "write_table",
                    &failure_identifier,
                    failure_version,
                    error,
                )
            })
        })
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
        let failure_identifier = request.identifier.clone();
        let failure_version = request.table.version;
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
        .map_err(|error| {
            Python::attach(|py| {
                callback_error(
                    py,
                    "write_table_batches",
                    &failure_identifier,
                    failure_version,
                    error,
                )
            })
        })
    }
}
