//! Engine error type for embedders (Polars conversion in this crate).

use polars::error::PolarsError;
use std::fmt;

/// Unified error type for robin-sparkless operations.
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

impl From<EngineError> for robin_sparkless_core::EngineError {
    fn from(e: EngineError) -> Self {
        use robin_sparkless_core::EngineError as Core;
        match e {
            EngineError::User(s) => Core::User(s),
            EngineError::Internal(s) => Core::Internal(s),
            EngineError::Io(s) => Core::Io(s),
            EngineError::Sql(s) => Core::Sql(s),
            EngineError::NotFound(s) => Core::NotFound(s),
            EngineError::Other(s) => Core::Other(s),
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

/// Map PolarsError to core EngineError for trait boundaries (core trait methods return core::EngineError).
pub fn polars_to_core_error(e: PolarsError) -> robin_sparkless_core::EngineError {
    use robin_sparkless_core::EngineError as Core;
    let msg = e.to_string();
    match &e {
        PolarsError::ColumnNotFound(_) => Core::NotFound(msg),
        PolarsError::InvalidOperation(_) => Core::User(msg),
        PolarsError::ComputeError(_) => {
            let lower = msg.to_lowercase();
            if lower.contains("filter") || lower.contains("predicate") {
                if lower.contains("boolean")
                    || lower.contains("bool")
                    || lower.contains("string")
                {
                    return Core::User(format!(
                        "filter predicate must be Boolean, got non-Boolean expression: {}",
                        msg
                    ));
                }
            }
            Core::Internal(msg)
        }
        PolarsError::IO { .. } => Core::Io(msg),
        _ => Core::Other(msg),
    }
}
