//! Schema types (from core) and Polars conversion. Conversion lives in schema_conv.

pub use robin_sparkless_core::{DataType, StructField, StructType};

pub use crate::schema_conv::StructTypePolarsExt;

/// Parse a schema from a JSON string (e.g. from a host binding).
pub fn schema_from_json(json: &str) -> Result<StructType, crate::error::EngineError> {
    const MAX_SCHEMA_BYTES: usize = 1024 * 1024;
    const MAX_SCHEMA_DEPTH: usize = 64;
    const MAX_SCHEMA_OBJECTS: usize = 10_000;
    if json.len() > MAX_SCHEMA_BYTES {
        return Err(crate::error::EngineError::User(
            "schema JSON exceeds the 1 MiB limit".into(),
        ));
    }
    let mut depth = 0usize;
    let mut objects = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for ch in json.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' | '[' => {
                depth += 1;
                if depth > MAX_SCHEMA_DEPTH {
                    return Err(crate::error::EngineError::User(
                        "schema JSON nesting exceeds the maximum depth".into(),
                    ));
                }
                if ch == '{' {
                    objects += 1;
                    if objects > MAX_SCHEMA_OBJECTS {
                        return Err(crate::error::EngineError::User(
                            "schema JSON contains too many objects".into(),
                        ));
                    }
                }
            }
            '}' | ']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
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
