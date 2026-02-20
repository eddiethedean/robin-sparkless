//! Schema types (from core) and Polars conversion. Conversion lives in schema_conv.

pub use robin_sparkless_core::{DataType, StructField, StructType};

pub use crate::schema_conv::StructTypePolarsExt;

/// Parse a schema from a JSON string (e.g. from a host binding).
pub fn schema_from_json(json: &str) -> Result<StructType, crate::error::EngineError> {
    serde_json::from_str(json).map_err(Into::into)
}
