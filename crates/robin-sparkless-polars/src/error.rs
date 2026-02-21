//! Engine error type for embedders (Polars conversion in this crate).

use polars::error::PolarsError;

pub use robin_sparkless_core::EngineError;

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
