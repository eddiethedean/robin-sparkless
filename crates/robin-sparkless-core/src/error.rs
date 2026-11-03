//! Structured engine errors with stable categories and machine-readable codes.

use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCategory {
    InvalidInput,
    Execution,
    Io,
    Sql,
    NotFound,
    Cancelled,
    Provider,
    Capability,
    Conflict,
    Internal,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorRetryability {
    Unspecified,
    Retryable,
    Terminal,
    OutcomeUnknown,
}

/// Machine-readable error payload shared by Rust, Python, HTTP, and Connect boundaries.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ErrorDetail {
    pub category: ErrorCategory,
    pub code: String,
    pub message: String,
    pub operation: Option<String>,
    pub retryability: ErrorRetryability,
    pub metadata: BTreeMap<String, String>,
    pub cause: Option<String>,
}

impl ErrorDetail {
    pub fn new(
        category: ErrorCategory,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            category,
            code: code.into(),
            message: message.into(),
            operation: None,
            retryability: ErrorRetryability::Unspecified,
            metadata: BTreeMap::new(),
            cause: None,
        }
    }

    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.operation = Some(operation.into());
        self
    }

    pub fn with_retryability(mut self, retryability: ErrorRetryability) -> Self {
        self.retryability = retryability;
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_cause(mut self, cause: impl Into<String>) -> Self {
        self.cause = Some(cause.into());
        self
    }
}

/// Unified structured error type. Prefixes preserve familiar display strings while callers
/// should branch on [`ErrorDetail::category`] and `code()` instead of parsing messages.
#[derive(Debug)]
pub struct EngineError {
    detail: Box<ErrorDetail>,
    display_prefix: &'static str,
}

impl EngineError {
    pub fn from_detail(detail: ErrorDetail) -> Self {
        let display_prefix = Self::display_prefix(detail.category);
        Self {
            detail: Box::new(detail),
            display_prefix,
        }
    }

    fn display_prefix(category: ErrorCategory) -> &'static str {
        match category {
            ErrorCategory::InvalidInput | ErrorCategory::Capability => "user error: ",
            ErrorCategory::Execution | ErrorCategory::Internal => "internal error: ",
            ErrorCategory::Io => "io error: ",
            ErrorCategory::Sql => "sql error: ",
            ErrorCategory::NotFound => "not found: ",
            ErrorCategory::Cancelled => "cancelled: ",
            ErrorCategory::Provider => "provider error: ",
            ErrorCategory::Conflict => "conflict: ",
            ErrorCategory::Other => "",
        }
    }

    pub fn detail(&self) -> &ErrorDetail {
        &self.detail
    }

    pub fn category(&self) -> ErrorCategory {
        self.detail.category
    }

    pub fn code(&self) -> &str {
        &self.detail.code
    }

    pub fn operation(&self) -> Option<&str> {
        self.detail.operation.as_deref()
    }

    pub fn retryability(&self) -> ErrorRetryability {
        self.detail.retryability
    }

    pub fn user_message(&self) -> &str {
        &self.detail.message
    }

    pub fn with_detail(mut self, detail: ErrorDetail) -> Self {
        self.display_prefix = Self::display_prefix(detail.category);
        self.detail = Box::new(detail);
        self
    }

    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.detail.operation = Some(operation.into());
        self
    }

    pub fn with_retryability(mut self, retryability: ErrorRetryability) -> Self {
        self.detail.retryability = retryability;
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.detail.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_cause(mut self, cause: impl Into<String>) -> Self {
        self.detail.cause = Some(cause.into());
        self
    }

    pub fn invalid_input(message: String) -> Self {
        Self::from_detail(
            ErrorDetail::new(ErrorCategory::InvalidInput, "INVALID_INPUT", message)
                .with_retryability(ErrorRetryability::Terminal),
        )
    }

    pub fn internal(message: String) -> Self {
        Self::from_detail(
            ErrorDetail::new(ErrorCategory::Internal, "INTERNAL", message)
                .with_retryability(ErrorRetryability::Unspecified),
        )
    }

    pub fn io(message: String) -> Self {
        Self::from_detail(ErrorDetail::new(ErrorCategory::Io, "IO_ERROR", message))
    }

    pub fn sql(message: String) -> Self {
        Self::from_detail(ErrorDetail::new(ErrorCategory::Sql, "SQL_ERROR", message))
    }

    pub fn not_found(message: String) -> Self {
        Self::from_detail(
            ErrorDetail::new(ErrorCategory::NotFound, "NOT_FOUND", message)
                .with_retryability(ErrorRetryability::Terminal),
        )
    }

    pub fn cancelled(message: String) -> Self {
        Self::from_detail(
            ErrorDetail::new(ErrorCategory::Cancelled, "CANCELLED", message)
                .with_retryability(ErrorRetryability::Terminal),
        )
    }

    pub fn other(message: String) -> Self {
        Self::from_detail(ErrorDetail::new(ErrorCategory::Other, "OTHER", message))
    }

    pub fn provider(
        code: impl Into<String>,
        message: impl Into<String>,
        retryability: ErrorRetryability,
    ) -> Self {
        Self::from_detail(
            ErrorDetail::new(ErrorCategory::Provider, code, message)
                .with_retryability(retryability),
        )
    }

    pub fn capability(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::from_detail(
            ErrorDetail::new(ErrorCategory::Capability, code, message)
                .with_retryability(ErrorRetryability::Terminal),
        )
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.display_prefix, self.detail.message)
    }
}

impl std::error::Error for EngineError {}

/// Normalize column-not-found text to PySpark-style "cannot be resolved" wording.
pub fn normalize_unresolved_column_message(msg: &str) -> String {
    let lower = msg.to_lowercase();
    if lower.contains("cannot be resolved") {
        if lower.contains("unresolved_column") {
            return msg.to_string();
        }
        return format!("unresolved_column: {msg}");
    }
    if lower.contains("cannot resolve") {
        let msg = msg.replace("cannot resolve", "cannot be resolved");
        if lower.contains("unresolved_column") {
            return msg;
        }
        return format!("unresolved_column: {msg}");
    }
    if msg.contains("unable to find column")
        || (msg.contains("not found") && lower.contains("column"))
        || msg.contains("valid columns")
    {
        return format!("unresolved_column: cannot be resolved: {msg}");
    }
    msg.to_string()
}

impl From<serde_json::Error> for EngineError {
    fn from(error: serde_json::Error) -> Self {
        Self::internal(error.to_string()).with_cause(error.to_string())
    }
}

impl From<std::io::Error> for EngineError {
    fn from(error: std::io::Error) -> Self {
        Self::io(error.to_string()).with_cause(error.to_string())
    }
}
