use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Maximum combined JSON encoding size of field metadata in one schema.
pub const MAX_LOGICAL_SCHEMA_METADATA_BYTES: usize = 32 * 1024;
/// Maximum nesting depth allowed in field metadata JSON values.
pub const MAX_LOGICAL_SCHEMA_METADATA_DEPTH: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DataType {
    String,
    Byte,
    Integer,
    Short,
    Long,
    Double,
    Float,
    Decimal(u8, u8),
    Boolean,
    Date,
    Timestamp,
    TimestampNtz,
    Binary,
    Array(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
    ArrayWithNullability {
        element_type: Box<DataType>,
        contains_null: bool,
    },
    MapWithNullability {
        key_type: Box<DataType>,
        value_type: Box<DataType>,
        value_contains_null: bool,
    },
    Struct(Vec<StructField>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    /// JSON-compatible Spark field metadata. Empty metadata is omitted when serialized so old
    /// schema JSON remains byte-compatible.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl StructField {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        StructField {
            name,
            data_type,
            nullable,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_metadata(mut self, metadata: BTreeMap<String, serde_json::Value>) -> Self {
        self.metadata = metadata;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructType {
    fields: Vec<StructField>,
}

impl StructType {
    pub fn new(fields: Vec<StructField>) -> Self {
        StructType { fields }
    }

    pub fn fields(&self) -> &[StructField] {
        &self.fields
    }

    /// Validate JSON metadata against the native-schema limits: at most 32 KiB per schema and
    /// sixteen nested JSON containers. This does not claim that a physical engine conversion
    /// can preserve metadata; checked converters must still reject unsupported mappings.
    pub fn validate_metadata(&self) -> Result<(), String> {
        let mut total_bytes = 0usize;
        for field in &self.fields {
            validate_field_metadata(field, 0, &mut total_bytes)?;
        }
        Ok(())
    }

    /// Serialize the schema to a JSON string (array of field objects with name, data_type, nullable).
    /// Useful for bindings that need to expose schema to the host without Polars types.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Serialize the schema to a pretty-printed JSON string.
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

fn validate_field_metadata(
    field: &StructField,
    depth: usize,
    total_bytes: &mut usize,
) -> Result<(), String> {
    if !field.metadata.is_empty() {
        let encoded = serde_json::to_vec(&field.metadata)
            .map_err(|error| format!("field metadata is not JSON-compatible: {error}"))?;
        *total_bytes = total_bytes.saturating_add(encoded.len());
        if *total_bytes > MAX_LOGICAL_SCHEMA_METADATA_BYTES {
            return Err(format!(
                "logical schema metadata exceeds {MAX_LOGICAL_SCHEMA_METADATA_BYTES} bytes"
            ));
        }
        for value in field.metadata.values() {
            validate_json_depth(value, depth, 0)?;
        }
    }
    validate_data_type_metadata(&field.data_type, depth, total_bytes)
}

fn validate_data_type_metadata(
    data_type: &DataType,
    depth: usize,
    total_bytes: &mut usize,
) -> Result<(), String> {
    match data_type {
        DataType::Array(element)
        | DataType::ArrayWithNullability {
            element_type: element,
            ..
        } => validate_data_type_metadata(element, depth + 1, total_bytes),
        DataType::Map(key, value)
        | DataType::MapWithNullability {
            key_type: key,
            value_type: value,
            ..
        } => {
            validate_data_type_metadata(key, depth + 1, total_bytes)?;
            validate_data_type_metadata(value, depth + 1, total_bytes)
        }
        DataType::Struct(fields) => {
            for field in fields {
                validate_field_metadata(field, depth + 1, total_bytes)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_json_depth(
    value: &serde_json::Value,
    field_depth: usize,
    json_depth: usize,
) -> Result<(), String> {
    let depth = field_depth.saturating_add(json_depth);
    if depth > MAX_LOGICAL_SCHEMA_METADATA_DEPTH {
        return Err(format!(
            "logical schema metadata exceeds nesting depth {MAX_LOGICAL_SCHEMA_METADATA_DEPTH}"
        ));
    }
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                validate_json_depth(value, field_depth, json_depth + 1)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                validate_json_depth(value, field_depth, json_depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_struct_field_new() {
        let field = StructField::new("age".to_string(), DataType::Integer, true);
        assert_eq!(field.name, "age");
        assert!(field.nullable);
        assert!(matches!(field.data_type, DataType::Integer));
    }

    #[test]
    fn test_struct_type_new() {
        let fields = vec![
            StructField::new("id".to_string(), DataType::Long, false),
            StructField::new("name".to_string(), DataType::String, true),
        ];
        let schema = StructType::new(fields);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.fields()[0].name, "id");
        assert_eq!(schema.fields()[1].name, "name");
    }

    #[test]
    fn test_struct_type_to_json() {
        let fields = vec![
            StructField::new("id".to_string(), DataType::Long, false),
            StructField::new("name".to_string(), DataType::String, true),
        ];
        let schema = StructType::new(fields);
        let json = schema.to_json().unwrap();
        assert!(json.contains("\"name\":\"id\""));
        assert!(json.contains("\"name\":\"name\""));
        assert!(json.contains("\"data_type\""));
        assert!(json.contains("\"nullable\""));
        let _parsed: StructType = serde_json::from_str(&json).unwrap();
        let pretty = schema.to_json_pretty().unwrap();
        assert!(pretty.contains('\n'));
    }

    #[test]
    fn legacy_schema_json_defaults_metadata_and_new_logical_flags_roundtrip() {
        let legacy = r#"{"fields":[{"name":"id","data_type":"Long","nullable":false}]}"#;
        let parsed: StructType = serde_json::from_str(legacy).unwrap();
        assert!(parsed.fields()[0].metadata.is_empty());
        assert_eq!(parsed.to_json().unwrap(), legacy);

        let mut metadata = BTreeMap::new();
        metadata.insert("owner".into(), serde_json::json!("analytics"));
        metadata.insert(
            "policy".into(),
            serde_json::json!({"tier": 2, "tags": ["pii"]}),
        );
        let schema = StructType::new(vec![
            StructField::new(
                "items".into(),
                DataType::ArrayWithNullability {
                    element_type: Box::new(DataType::Byte),
                    contains_null: false,
                },
                false,
            )
            .with_metadata(metadata),
            StructField::new(
                "attributes".into(),
                DataType::MapWithNullability {
                    key_type: Box::new(DataType::String),
                    value_type: Box::new(DataType::Float),
                    value_contains_null: true,
                },
                true,
            ),
            StructField::new("event_time".into(), DataType::TimestampNtz, true),
        ]);
        schema.validate_metadata().unwrap();
        let roundtrip: StructType = serde_json::from_str(&schema.to_json().unwrap()).unwrap();
        assert_eq!(roundtrip.fields()[0].metadata["owner"], "analytics");
        assert!(matches!(
            roundtrip.fields()[0].data_type,
            DataType::ArrayWithNullability {
                contains_null: false,
                ..
            }
        ));
        assert!(matches!(
            roundtrip.fields()[1].data_type,
            DataType::MapWithNullability {
                value_contains_null: true,
                ..
            }
        ));
    }

    #[test]
    fn logical_schema_metadata_limits_are_enforced() {
        let nested = serde_json::from_str::<serde_json::Value>(&format!(
            "{}0{}",
            "[".repeat(MAX_LOGICAL_SCHEMA_METADATA_DEPTH + 1),
            "]".repeat(MAX_LOGICAL_SCHEMA_METADATA_DEPTH + 1)
        ))
        .unwrap();
        let mut metadata = BTreeMap::new();
        metadata.insert("deep".into(), nested);
        let schema = StructType::new(vec![
            StructField::new("value".into(), DataType::String, true).with_metadata(metadata),
        ]);
        assert!(schema.validate_metadata().is_err());
    }

    #[test]
    fn test_empty_struct_type() {
        let empty = StructType::new(vec![]);
        assert!(empty.fields().is_empty());
    }
}
