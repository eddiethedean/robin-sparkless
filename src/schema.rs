//! Schema types and Polars conversion. Types live in `robin-sparkless-core`; conversion lives here.

pub use robin_sparkless_core::{DataType, StructField, StructType};

pub use crate::schema_conv::StructTypePolarsExt;

/// Parse a schema from a JSON string (e.g. from a host binding).
/// The JSON must match the serialization of [`StructType`] (e.g. from [`StructType::to_json`]).
pub fn schema_from_json(json: &str) -> Result<StructType, crate::error::EngineError> {
    serde_json::from_str(json).map_err(crate::error::EngineError::from)
}
