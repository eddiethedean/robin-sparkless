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
    use robin_sparkless_core::EngineError as Core;
    use robin_sparkless_core::normalize_unresolved_column_message;
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
            Core::NotFound(normalize_unresolved_column_message(&base))
        }
        PolarsError::InvalidOperation(_) => Core::User(msg),
        PolarsError::ComputeError(_) => {
            let lower = msg.to_lowercase();
            if lower.contains("filter") || lower.contains("predicate") {
                if lower.contains("boolean") || lower.contains("bool") || lower.contains("string") {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polars_invalid_operation_maps_to_user() {
        let e = PolarsError::InvalidOperation("invalid".into());
        let core = polars_to_core_error(e);
        assert!(matches!(core, robin_sparkless_core::EngineError::User(s) if s == "invalid"));
    }

    #[test]
    fn polars_compute_error_maps_to_internal() {
        let e = PolarsError::ComputeError("compute failed".into());
        let core = polars_to_core_error(e);
        assert!(matches!(
            core,
            robin_sparkless_core::EngineError::Internal(s) if s == "compute failed"
        ));
    }

    #[test]
    fn polars_column_not_found_adds_cannot_be_resolved() {
        let e = PolarsError::ColumnNotFound("x".into());
        let core = polars_to_core_error(e);
        assert!(matches!(
            core,
            robin_sparkless_core::EngineError::NotFound(s) if s.contains("cannot be resolved")
        ));
    }

    #[test]
    fn polars_column_not_found_preserves_existing_cannot_be_resolved() {
        let e = PolarsError::ColumnNotFound("cannot be resolved: col y".into());
        let core = polars_to_core_error(e);
        match &core {
            robin_sparkless_core::EngineError::NotFound(s) => {
                assert!(
                    s.contains("cannot be resolved"),
                    "expected 'cannot be resolved' in {s:?}"
                );
            }
            _ => panic!("expected NotFound, got {core:?}"),
        }
    }
}
