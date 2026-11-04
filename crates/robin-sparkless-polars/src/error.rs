//! Engine error type for embedders (Polars conversion in this crate).
//!
//! All public engine traits in `robin-sparkless-core` use [`robin_sparkless_core::EngineError`]
//! instead of Polars errors. This module centralizes conversion from [`PolarsError`] so that
//! higher-level APIs and bindings can depend on engine errors and traits, not on Polars types.

use polars::error::PolarsError;

pub use robin_sparkless_core::EngineError;

/// Map PolarsError to core EngineError for trait boundaries (core trait methods return core::EngineError).
/// Column-not-found messages are normalized to PySpark-style "cannot be resolved" wording.
pub fn polars_to_core_error(e: PolarsError) -> robin_sparkless_core::EngineError {
    use robin_sparkless_core::normalize_unresolved_column_message;
    use robin_sparkless_core::{
        EngineError as Core, ErrorCategory, ErrorDetail, ErrorRetryability,
    };
    let msg = e.to_string();
    match &e {
        PolarsError::ColumnNotFound(_) => {
            let base = if msg.to_lowercase().contains("cannot be resolved")
                || msg.to_lowercase().contains("cannot resolve")
            {
                msg
            } else {
                format!("cannot be resolved: {msg}")
            };
            Core::from_detail(
                ErrorDetail::new(
                    ErrorCategory::NotFound,
                    "COLUMN_NOT_FOUND",
                    normalize_unresolved_column_message(&base),
                )
                .with_retryability(ErrorRetryability::Terminal),
            )
        }
        PolarsError::InvalidOperation(_) if msg.starts_with("CatalogCapacityExceeded:") => {
            Core::from_detail(
                ErrorDetail::new(ErrorCategory::Conflict, "CATALOG_CAPACITY_EXCEEDED", msg)
                    .with_retryability(ErrorRetryability::Terminal),
            )
        }
        PolarsError::InvalidOperation(_) if msg.starts_with("WRITE_OUTCOME_UNKNOWN:") => {
            Core::from_detail(
                ErrorDetail::new(ErrorCategory::Provider, "WRITE_OUTCOME_UNKNOWN", msg)
                    .with_retryability(ErrorRetryability::OutcomeUnknown),
            )
        }
        PolarsError::InvalidOperation(_) if msg.starts_with("EXTERNAL_WRITE_NOT_COMMITTED:") => {
            let retryability = if msg.starts_with("EXTERNAL_WRITE_NOT_COMMITTED: terminal:") {
                ErrorRetryability::Terminal
            } else if msg.starts_with("EXTERNAL_WRITE_NOT_COMMITTED: retryable:") {
                ErrorRetryability::Retryable
            } else {
                ErrorRetryability::Unspecified
            };
            Core::from_detail(
                ErrorDetail::new(ErrorCategory::Provider, "EXTERNAL_WRITE_NOT_COMMITTED", msg)
                    .with_retryability(retryability),
            )
        }
        PolarsError::InvalidOperation(_) if msg.starts_with("READ_CAPABILITY_DENIED:") => {
            Core::capability("READ_CAPABILITY_DENIED", msg)
        }
        PolarsError::InvalidOperation(_) if msg.starts_with("WRITE_CAPABILITY_DENIED:") => {
            Core::capability("WRITE_CAPABILITY_DENIED", msg)
        }
        PolarsError::InvalidOperation(_) => Core::from_detail(
            ErrorDetail::new(ErrorCategory::InvalidInput, "INVALID_OPERATION", msg)
                .with_retryability(ErrorRetryability::Terminal),
        ),
        PolarsError::ComputeError(_) if msg.starts_with("cancelled: ") => Core::from_detail(
            ErrorDetail::new(
                ErrorCategory::Cancelled,
                "CANCELLED",
                msg.trim_start_matches("cancelled: "),
            )
            .with_retryability(ErrorRetryability::Terminal),
        ),
        PolarsError::ComputeError(_) => {
            let lower = msg.to_lowercase();
            if lower.contains("filter") || lower.contains("predicate") {
                if lower.contains("boolean") || lower.contains("bool") || lower.contains("string") {
                    return Core::from_detail(
                        ErrorDetail::new(
                            ErrorCategory::InvalidInput,
                            "NON_BOOLEAN_PREDICATE",
                            format!(
                                "filter predicate must be Boolean, got non-Boolean expression: {}",
                                msg
                            ),
                        )
                        .with_retryability(ErrorRetryability::Terminal),
                    );
                }
            }
            Core::from_detail(
                ErrorDetail::new(ErrorCategory::Execution, "POLARS_EXECUTION_FAILED", msg)
                    .with_retryability(ErrorRetryability::Unspecified),
            )
        }
        PolarsError::IO { .. } => Core::io(msg),
        _ => Core::other(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polars_invalid_operation_maps_to_user() {
        let e = PolarsError::InvalidOperation("invalid".into());
        let core = polars_to_core_error(e);
        assert_eq!(
            core.category(),
            robin_sparkless_core::ErrorCategory::InvalidInput
        );
        assert_eq!(core.user_message(), "invalid");
    }

    #[test]
    fn polars_compute_error_maps_to_internal() {
        let e = PolarsError::ComputeError("compute failed".into());
        let core = polars_to_core_error(e);
        assert_eq!(
            core.category(),
            robin_sparkless_core::ErrorCategory::Execution
        );
        assert_eq!(core.user_message(), "compute failed");
    }

    #[test]
    fn polars_column_not_found_adds_cannot_be_resolved() {
        let e = PolarsError::ColumnNotFound("x".into());
        let core = polars_to_core_error(e);
        assert_eq!(
            core.category(),
            robin_sparkless_core::ErrorCategory::NotFound
        );
        assert!(core.user_message().contains("cannot be resolved"));
    }

    #[test]
    fn polars_column_not_found_preserves_existing_cannot_be_resolved() {
        let e = PolarsError::ColumnNotFound("cannot be resolved: col y".into());
        let core = polars_to_core_error(e);
        match &core {
            _ if core.category() == robin_sparkless_core::ErrorCategory::NotFound => {
                assert!(
                    core.user_message().contains("cannot be resolved"),
                    "expected 'cannot be resolved' in {core:?}"
                );
            }
            _ => panic!("expected NotFound, got {core:?}"),
        }
    }
}
