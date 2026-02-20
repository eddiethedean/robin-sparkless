//! Engine error type for embedders.
//!
//! Use [`EngineError`] when you want to map robin-sparkless and Polars errors
//! to a single type (e.g. for FFI or CLI) without depending on Polars error types.

use polars::error::PolarsError;
use std::fmt;

/// Unified error type for robin-sparkless operations.
///
/// Embedders (Python, Node, CLI) can map these variants to native errors
/// without depending on `PolarsError`.
#[derive(Debug)]
pub enum EngineError {
    /// User-facing error (invalid input, unsupported operation).
    User(String),
    /// Internal / compute error.
    Internal(String),
    /// I/O error (file not found, permission, etc.).
    Io(String),
    /// SQL parsing or execution error.
    Sql(String),
    /// Resource not found (column, table, file).
    NotFound(String),
    /// Other / unclassified.
    Other(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::User(s) => write!(f, "user error: {s}"),
            EngineError::Internal(s) => write!(f, "internal error: {s}"),
            EngineError::Io(s) => write!(f, "io error: {s}"),
            EngineError::Sql(s) => write!(f, "sql error: {s}"),
            EngineError::NotFound(s) => write!(f, "not found: {s}"),
            EngineError::Other(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<robin_sparkless_core::EngineError> for EngineError {
    fn from(e: robin_sparkless_core::EngineError) -> Self {
        use robin_sparkless_core::EngineError as Core;
        match e {
            Core::User(s) => EngineError::User(s),
            Core::Internal(s) => EngineError::Internal(s),
            Core::Io(s) => EngineError::Io(s),
            Core::Sql(s) => EngineError::Sql(s),
            Core::NotFound(s) => EngineError::NotFound(s),
            Core::Other(s) => EngineError::Other(s),
        }
    }
}

impl From<PolarsError> for EngineError {
    fn from(e: PolarsError) -> Self {
        let msg = e.to_string();
        match &e {
            PolarsError::ColumnNotFound(_) => EngineError::NotFound(msg),
            PolarsError::InvalidOperation(_) => EngineError::User(msg),
            PolarsError::ComputeError(_) => {
                // Fixes #646: surface clear message when filter predicate is not Boolean.
                let lower = msg.to_lowercase();
                if lower.contains("filter") || lower.contains("predicate") {
                    if lower.contains("boolean")
                        || lower.contains("bool")
                        || lower.contains("string")
                    {
                        return EngineError::User(format!(
                            "filter predicate must be Boolean, got non-Boolean expression: {}",
                            msg
                        ));
                    }
                }
                EngineError::Internal(msg)
            }
            PolarsError::IO { .. } => EngineError::Io(msg),
            _ => EngineError::Other(msg),
        }
    }
}

impl From<serde_json::Error> for EngineError {
    fn from(e: serde_json::Error) -> Self {
        EngineError::Internal(e.to_string())
    }
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        EngineError::Io(e.to_string())
    }
}
