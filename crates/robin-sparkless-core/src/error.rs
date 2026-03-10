//! Engine error type for embedders.
//!
//! Use [`EngineError`] when you want to map robin-sparkless and Polars errors
//! to a single type (e.g. for FFI or CLI) without depending on Polars error types.
//!
//! Note: `From<PolarsError>` for `EngineError` is implemented in the main robin-sparkless
//! crate, which has a Polars dependency.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_error_display() {
        assert_eq!(
            EngineError::User("bad input".into()).to_string(),
            "user error: bad input"
        );
        assert_eq!(
            EngineError::Internal("panic".into()).to_string(),
            "internal error: panic"
        );
        assert_eq!(
            EngineError::Io("file not found".into()).to_string(),
            "io error: file not found"
        );
        assert_eq!(
            EngineError::Sql("parse error".into()).to_string(),
            "sql error: parse error"
        );
        assert_eq!(
            EngineError::NotFound("column x".into()).to_string(),
            "not found: column x"
        );
        assert_eq!(EngineError::Other("misc".into()).to_string(), "misc");
    }

    #[test]
    fn engine_error_from_io() {
        let e = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err: EngineError = e.into();
        assert!(err.to_string().contains("file missing"));
        assert!(err.to_string().contains("io error"));
    }

    #[test]
    fn engine_error_from_serde_json() {
        let bad = b"\x80";
        let e: Result<(), _> = serde_json::from_slice(bad);
        let err: EngineError = e.unwrap_err().into();
        assert!(err.to_string().contains("internal error"));
    }
}
