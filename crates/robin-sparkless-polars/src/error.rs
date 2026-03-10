//! Engine error type for embedders (Polars conversion in this crate).

use polars::error::PolarsError;

pub use robin_sparkless_core::EngineError;

/// Map PolarsError to core EngineError for trait boundaries (core trait methods return core::EngineError).
/// Column-not-found messages are normalized to include "cannot resolve" for PySpark parity (issue #1058).
pub fn polars_to_core_error(e: PolarsError) -> robin_sparkless_core::EngineError {
    use robin_sparkless_core::EngineError as Core;
    let msg = e.to_string();
    match &e {
        PolarsError::ColumnNotFound(_) => {
            let normalized = if msg.to_lowercase().contains("cannot resolve") {
                msg
            } else {
                format!("cannot resolve: {msg}")
            };
            Core::NotFound(normalized)
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
    fn polars_column_not_found_adds_cannot_resolve() {
        let e = PolarsError::ColumnNotFound("x".into());
        let core = polars_to_core_error(e);
        assert!(
            matches!(core, robin_sparkless_core::EngineError::NotFound(s) if s.contains("cannot resolve"))
        );
    }

    #[test]
    fn polars_column_not_found_preserves_existing_cannot_resolve() {
        let e = PolarsError::ColumnNotFound("cannot resolve: col y".into());
        let core = polars_to_core_error(e);
        match &core {
            robin_sparkless_core::EngineError::NotFound(s) => {
                assert!(
                    s.contains("cannot resolve"),
                    "expected 'cannot resolve' in {s:?}"
                );
            }
            _ => panic!("expected NotFound, got {core:?}"),
        }
    }
}
