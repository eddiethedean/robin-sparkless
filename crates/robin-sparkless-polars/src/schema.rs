//! Schema types (from core) and Polars conversion. Conversion lives in schema_conv.

pub use robin_sparkless_core::{DataType, StructField, StructType};

pub use crate::schema_conv::StructTypePolarsExt;

/// Parse a schema from a JSON string (e.g. from a host binding).
pub fn schema_from_json(json: &str) -> Result<StructType, crate::error::EngineError> {
    serde_json::from_str(json).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_from_json_valid() {
        // StructType serializes as {"fields": [...]}
        let json = r#"{"fields":[{"name":"id","data_type":"Long","nullable":false},{"name":"name","data_type":"String","nullable":true}]}"#;
        let schema = schema_from_json(json).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.fields()[0].name, "id");
        assert!(matches!(
            schema.fields()[0].data_type,
            robin_sparkless_core::DataType::Long
        ));
        assert_eq!(schema.fields()[1].name, "name");
        assert!(matches!(
            schema.fields()[1].data_type,
            robin_sparkless_core::DataType::String
        ));
    }

    #[test]
    fn schema_from_json_empty_fields() {
        let json = r#"{"fields":[]}"#;
        let schema = schema_from_json(json).unwrap();
        assert!(schema.fields().is_empty());
    }

    #[test]
    fn schema_from_json_invalid_fails() {
        assert!(schema_from_json("not json").is_err());
        assert!(schema_from_json("[]").is_err());
    }
}
