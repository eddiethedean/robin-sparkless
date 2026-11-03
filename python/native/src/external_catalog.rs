//! GIL-safe adapter from Python providers to the engine-neutral catalog contract.

use pyo3::exceptions::{PyAttributeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyModule, PyString};
use pyo3::IntoPyObjectExt;
use robin_sparkless_core::engine::{
    ArrowIpcBatchStream, CatalogBatchWriter, CatalogOwnership, CatalogResolver, CatalogWriter,
    ExternalCatalog, ExternalTableDescriptor, ExternalTableSaveMode,
    ExternalTableWriteBatchRequest, ExternalTableWriteOutcome, ExternalTableWriteRequest,
    ExternalTableWriteResult, PreparedCatalogWrite,
};
use robin_sparkless_core::{EngineError, ErrorCategory, ErrorDetail, ErrorRetryability};
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
    provider: Arc<Py<PyAny>>,
    tables: ResolvedTables,
    materialized_writer: bool,
    batch_writer: bool,
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
        let materialized_writer = match provider.bind(py).getattr("write_table") {
            Ok(method) if !method.is_callable() => {
                return Err(PyTypeError::new_err(
                    "provider.write_table must be callable",
                ));
            }
            Ok(_) => true,
            Err(error) if !error.is_instance_of::<PyAttributeError>(py) => return Err(error),
            Err(_) => false,
        };
        let batch_writer = match provider.bind(py).getattr("write_table_batches") {
            Ok(method) if !method.is_callable() => {
                return Err(PyTypeError::new_err(
                    "provider.write_table_batches must be callable",
                ));
            }
            Ok(_) => true,
            Err(error) if !error.is_instance_of::<PyAttributeError>(py) => return Err(error),
            Err(_) => false,
        };
        if !provider
            .bind(py)
            .getattr("resolve_ownership")?
            .is_callable()
        {
            return Err(PyTypeError::new_err(
                "provider.resolve_ownership must be callable",
            ));
        }
        Ok(Self {
            provider: Arc::new(provider),
            tables,
            materialized_writer,
            batch_writer,
        })
    }

    pub(crate) fn into_external_catalog(self) -> Arc<ExternalCatalog> {
        let provider = Arc::new(self);
        let resolver: Arc<dyn CatalogResolver> = provider.clone();
        let writer = provider.materialized_writer.then(|| {
            let writer: Arc<dyn CatalogWriter> = provider.clone();
            writer
        });
        let batch_writer = provider.batch_writer.then(|| {
            let writer: Arc<dyn CatalogBatchWriter> = provider.clone();
            writer
        });
        Arc::new(ExternalCatalog::new(resolver, None, writer, batch_writer))
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
        .filter(|value| {
            matches!(
                value.as_str(),
                "retryable" | "terminal" | "uncertain" | "outcome_unknown"
            )
        })
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
    error_from_callback_failure(failure)
}

fn error_from_callback_failure(failure: CallbackFailure) -> EngineError {
    let serialized = serde_json::to_string(&failure).unwrap_or_else(|_| "{}".into());
    let retryability = match failure.retryability.as_str() {
        "retryable" => ErrorRetryability::Retryable,
        "terminal" => ErrorRetryability::Terminal,
        "uncertain" | "outcome_unknown" => ErrorRetryability::OutcomeUnknown,
        _ => ErrorRetryability::Unspecified,
    };
    let message = format!(
        "{} {}{serialized}{CALLBACK_ERROR_SUFFIX}",
        failure.message(),
        callback_error_prefix()
    );
    let mut detail = ErrorDetail::new(
        ErrorCategory::Provider,
        failure
            .code
            .as_deref()
            .unwrap_or("EXTERNAL_CATALOG_CALLBACK_FAILED"),
        message,
    )
    .with_operation(failure.operation.clone())
    .with_retryability(retryability)
    .with_metadata("provider_category", failure.category)
    .with_metadata("provider_retryability", failure.retryability)
    .with_metadata("identifier", failure.identifier)
    .with_metadata("provider_error_type", failure.provider_error_type);
    if let Some(version) = failure.version {
        detail = detail.with_metadata("version", version.to_string());
    }
    if let Some(version) = failure.expected_version {
        detail = detail.with_metadata("expected_version", version.to_string());
    }
    if let Some(version) = failure.actual_version {
        detail = detail.with_metadata("actual_version", version.to_string());
    }
    EngineError::from_detail(detail)
}

pub(crate) fn engine_error_from_message(message: &str) -> Option<EngineError> {
    let prefix = callback_error_prefix();
    let start = message.find(prefix)? + prefix.len();
    let end = message[start..].find(CALLBACK_ERROR_SUFFIX)? + start;
    let failure = serde_json::from_str::<CallbackFailure>(&message[start..end]).ok()?;
    Some(error_from_callback_failure(failure))
}

pub(crate) fn callback_failure_from_message(py: Python<'_>, message: &str) -> Option<PyErr> {
    let _structured_error = engine_error_from_message(message)?;
    let failure = {
        let prefix = callback_error_prefix();
        let start = message.find(prefix)? + prefix.len();
        let end = message[start..].find(CALLBACK_ERROR_SUFFIX)? + start;
        serde_json::from_str::<CallbackFailure>(&message[start..end]).ok()?
    };
    let error = super::SparklessExternalCatalogError::new_err(failure.message());
    let value = error.value(py);
    let _ = value.setattr("operation", failure.operation);
    let _ = value.setattr("category", failure.category);
    let _ = value.setattr("code", failure.code);
    let _ = value.setattr("identifier", failure.identifier);
    let _ = value.setattr("version", failure.version);
    let _ = value.setattr("expected_version", failure.expected_version);
    let _ = value.setattr("actual_version", failure.actual_version);
    let retryability = if failure.retryability == "uncertain" {
        "outcome_unknown"
    } else {
        failure.retryability.as_str()
    };
    let _ = value.setattr("retryability", retryability);
    let _ = value.setattr("provider_error_type", failure.provider_error_type);
    Some(error)
}

fn write_result_from_python(value: &Bound<'_, PyAny>) -> PyResult<ExternalTableWriteResult> {
    let module = PyModule::import(value.py(), "sparkless.external_catalog")?;
    let (descriptor_json, committed_version, provider_commit_id): (
        String,
        Option<i64>,
        Option<String>,
    ) = module
        .getattr("_write_result_to_parts")?
        .call1((value,))?
        .extract()?;
    let table = serde_json::from_str(&descriptor_json).map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid write receipt: {error}"))
    })?;
    Ok(ExternalTableWriteResult {
        table,
        committed_version,
        provider_commit_id,
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

impl CatalogResolver for PythonExternalCatalogProvider {
    fn resolve_ownership(&self, identifier: &str) -> Result<CatalogOwnership, EngineError> {
        Python::attach(|py| -> PyResult<_> {
            let value = self
                .provider
                .bind(py)
                .call_method1("resolve_ownership", (identifier,))?;
            let module = PyModule::import(py, "sparkless.external_catalog")?;
            let encoded: String = module
                .getattr("_ownership_to_json")?
                .call1((value,))?
                .extract()?;
            serde_json::from_str(&encoded).map_err(|error| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid external catalog ownership: {error}"
                ))
            })
        })
        .map_err(|error| {
            Python::attach(|py| callback_error(py, "resolve_ownership", identifier, None, error))
        })
    }

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
}

impl PythonExternalCatalogProvider {
    fn write_table_request(
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
            kwargs.set_item("operation_id", request.operation_id)?;
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

    fn write_table_batches_request(
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
            kwargs.set_item("operation_id", request.operation_id)?;
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

enum PythonWriteRequest {
    Materialized(ExternalTableWriteRequest),
    Batches(ExternalTableWriteBatchRequest),
}

struct PythonPreparedWrite {
    provider: Arc<Py<PyAny>>,
    request: PythonWriteRequest,
}

impl PreparedCatalogWrite for PythonPreparedWrite {
    fn commit(
        self: Box<Self>,
        token: &robin_sparkless_core::CancellationToken,
    ) -> ExternalTableWriteOutcome {
        if let Err(error) = token.check_cancelled() {
            return ExternalTableWriteOutcome::NotCommitted(error);
        }
        let (operation_id, provider_name, result) = match self.request {
            PythonWriteRequest::Materialized(request) => {
                let operation_id = request.operation_id.clone();
                let provider_name = request.table.provider.clone();
                let helper = PythonExternalCatalogProvider {
                    provider: Arc::clone(&self.provider),
                    tables: Arc::new(Mutex::new(HashMap::new())),
                    materialized_writer: true,
                    batch_writer: false,
                };
                let result = helper.write_table_request(request);
                (operation_id, provider_name, result)
            }
            PythonWriteRequest::Batches(request) => {
                let operation_id = request.operation_id.clone();
                let provider_name = request.table.provider.clone();
                let helper = PythonExternalCatalogProvider {
                    provider: Arc::clone(&self.provider),
                    tables: Arc::new(Mutex::new(HashMap::new())),
                    materialized_writer: false,
                    batch_writer: true,
                };
                let result = helper.write_table_batches_request(request);
                (operation_id, provider_name, result)
            }
        };
        match result {
            Ok(receipt) => ExternalTableWriteOutcome::Committed(receipt),
            // The provider callback is the irreversible phase. Its retryability advice does
            // not prove whether an effect committed before the callback raised.
            Err(error) => ExternalTableWriteOutcome::OutcomeUnknown {
                provider: provider_name,
                operation_id,
                message: error.to_string(),
            },
        }
    }
}

impl CatalogWriter for PythonExternalCatalogProvider {
    fn prepare_write(
        &self,
        request: ExternalTableWriteRequest,
    ) -> Result<Box<dyn PreparedCatalogWrite>, EngineError> {
        if !self.materialized_writer {
            return Err(EngineError::capability(
                "MATERIALIZED_WRITE_UNAVAILABLE",
                "Python catalog provider does not implement write_table",
            ));
        }
        Ok(Box::new(PythonPreparedWrite {
            provider: Arc::clone(&self.provider),
            request: PythonWriteRequest::Materialized(request),
        }))
    }
}

impl CatalogBatchWriter for PythonExternalCatalogProvider {
    fn prepare_write_batches(
        &self,
        request: ExternalTableWriteBatchRequest,
    ) -> Result<Box<dyn PreparedCatalogWrite>, EngineError> {
        if !self.batch_writer {
            return Err(EngineError::capability(
                "BATCH_WRITE_UNAVAILABLE",
                "Python catalog provider does not implement write_table_batches",
            ));
        }
        Ok(Box::new(PythonPreparedWrite {
            provider: Arc::clone(&self.provider),
            request: PythonWriteRequest::Batches(request),
        }))
    }
}
