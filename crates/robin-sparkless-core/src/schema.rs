use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Maximum combined JSON encoding size of field metadata in one schema.
pub const MAX_LOGICAL_SCHEMA_METADATA_BYTES: usize = 32 * 1024;
/// Maximum nesting depth allowed in field metadata JSON values.
pub const MAX_LOGICAL_SCHEMA_METADATA_DEPTH: usize = 16;

#[derive(Debug, Clone)]
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
    /// Metadata-bearing nested fields; serializes using the existing Struct wire tag.
    LogicalStruct(Vec<LogicalField>),
}

#[derive(Serialize, Deserialize)]
enum TypeWire {
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
    Struct(Vec<LogicalField>),
}
impl Serialize for DataType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let wire = match self {
            Self::String => TypeWire::String,
            Self::Byte => TypeWire::Byte,
            Self::Integer => TypeWire::Integer,
            Self::Short => TypeWire::Short,
            Self::Long => TypeWire::Long,
            Self::Double => TypeWire::Double,
            Self::Float => TypeWire::Float,
            Self::Boolean => TypeWire::Boolean,
            Self::Date => TypeWire::Date,
            Self::Timestamp => TypeWire::Timestamp,
            Self::TimestampNtz => TypeWire::TimestampNtz,
            Self::Binary => TypeWire::Binary,
            Self::Decimal(p, s) => TypeWire::Decimal(*p, *s),
            Self::Array(v) => TypeWire::Array(v.clone()),
            Self::Map(k, v) => TypeWire::Map(k.clone(), v.clone()),
            Self::ArrayWithNullability {
                element_type,
                contains_null,
            } => TypeWire::ArrayWithNullability {
                element_type: element_type.clone(),
                contains_null: *contains_null,
            },
            Self::MapWithNullability {
                key_type,
                value_type,
                value_contains_null,
            } => TypeWire::MapWithNullability {
                key_type: key_type.clone(),
                value_type: value_type.clone(),
                value_contains_null: *value_contains_null,
            },
            Self::Struct(fields) => {
                TypeWire::Struct(fields.iter().cloned().map(LogicalField::from).collect())
            }
            Self::LogicalStruct(fields) => TypeWire::Struct(fields.clone()),
        };
        wire.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match TypeWire::deserialize(deserializer)? {
            TypeWire::String => Self::String,
            TypeWire::Byte => Self::Byte,
            TypeWire::Integer => Self::Integer,
            TypeWire::Short => Self::Short,
            TypeWire::Long => Self::Long,
            TypeWire::Double => Self::Double,
            TypeWire::Float => Self::Float,
            TypeWire::Boolean => Self::Boolean,
            TypeWire::Date => Self::Date,
            TypeWire::Timestamp => Self::Timestamp,
            TypeWire::TimestampNtz => Self::TimestampNtz,
            TypeWire::Binary => Self::Binary,
            TypeWire::Decimal(p, s) => Self::Decimal(p, s),
            TypeWire::Array(v) => Self::Array(v),
            TypeWire::Map(k, v) => Self::Map(k, v),
            TypeWire::ArrayWithNullability {
                element_type,
                contains_null,
            } => Self::ArrayWithNullability {
                element_type,
                contains_null,
            },
            TypeWire::MapWithNullability {
                key_type,
                value_type,
                value_contains_null,
            } => Self::MapWithNullability {
                key_type,
                value_type,
                value_contains_null,
            },
            TypeWire::Struct(fields) => {
                let has_metadata = fields
                    .iter()
                    .any(|f| !f.metadata.is_empty() || f.data_type.has_field_metadata());
                if has_metadata {
                    Self::LogicalStruct(fields)
                } else {
                    Self::Struct(fields.into_iter().map(LogicalField::into_legacy).collect())
                }
            }
        })
    }
}

/// The original three-field Rust schema descriptor remains source-compatible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl StructField {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }

    /// Opt into the additive descriptor when metadata is required.
    pub fn with_metadata(self, metadata: BTreeMap<String, serde_json::Value>) -> LogicalField {
        LogicalField::from(self).with_metadata(metadata)
    }
}

/// A logical field including Spark metadata. Use StructType::from_logical_fields
/// and StructType::logical_fields to retain this information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl LogicalField {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type: data_type.into_logical(),
            nullable,
            metadata: BTreeMap::new(),
        }
    }

    pub fn with_metadata(mut self, metadata: BTreeMap<String, serde_json::Value>) -> Self {
        self.metadata = metadata;
        self
    }

    fn into_legacy(self) -> StructField {
        StructField::new(self.name, self.data_type.into_legacy(), self.nullable)
    }
}

impl From<StructField> for LogicalField {
    fn from(field: StructField) -> Self {
        Self::new(field.name, field.data_type, field.nullable)
    }
}

impl DataType {
    fn has_field_metadata(&self) -> bool {
        match self {
            Self::Struct(fields) => fields.iter().any(|f| f.data_type.has_field_metadata()),
            Self::LogicalStruct(fields) => fields
                .iter()
                .any(|f| !f.metadata.is_empty() || f.data_type.has_field_metadata()),
            Self::Array(inner)
            | Self::ArrayWithNullability {
                element_type: inner,
                ..
            } => inner.has_field_metadata(),
            Self::Map(k, v)
            | Self::MapWithNullability {
                key_type: k,
                value_type: v,
                ..
            } => k.has_field_metadata() || v.has_field_metadata(),
            _ => false,
        }
    }

    fn into_logical(self) -> Self {
        match self {
            Self::Struct(fields) => {
                Self::LogicalStruct(fields.into_iter().map(LogicalField::from).collect())
            }
            Self::LogicalStruct(fields) => Self::LogicalStruct(
                fields
                    .into_iter()
                    .map(|mut f| {
                        f.data_type = f.data_type.into_logical();
                        f
                    })
                    .collect(),
            ),
            Self::Array(inner) => Self::Array(Box::new(inner.into_logical())),
            Self::Map(k, v) => Self::Map(Box::new(k.into_logical()), Box::new(v.into_logical())),
            Self::ArrayWithNullability {
                element_type,
                contains_null,
            } => Self::ArrayWithNullability {
                element_type: Box::new(element_type.into_logical()),
                contains_null,
            },
            Self::MapWithNullability {
                key_type,
                value_type,
                value_contains_null,
            } => Self::MapWithNullability {
                key_type: Box::new(key_type.into_logical()),
                value_type: Box::new(value_type.into_logical()),
                value_contains_null,
            },
            other => other,
        }
    }

    fn into_legacy(self) -> Self {
        match self {
            Self::LogicalStruct(fields) => {
                Self::Struct(fields.into_iter().map(LogicalField::into_legacy).collect())
            }
            Self::Array(inner) => Self::Array(Box::new(inner.into_legacy())),
            Self::Map(k, v) => Self::Map(Box::new(k.into_legacy()), Box::new(v.into_legacy())),
            Self::ArrayWithNullability {
                element_type,
                contains_null,
            } => Self::ArrayWithNullability {
                element_type: Box::new(element_type.into_legacy()),
                contains_null,
            },
            Self::MapWithNullability {
                key_type,
                value_type,
                value_contains_null,
            } => Self::MapWithNullability {
                key_type: Box::new(key_type.into_legacy()),
                value_type: Box::new(value_type.into_legacy()),
                value_contains_null,
            },
            other => other,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StructType {
    fields: Vec<StructField>,
    logical_fields: Vec<LogicalField>,
}

#[derive(Serialize, Deserialize)]
struct SchemaWire {
    fields: Vec<LogicalField>,
}

impl Serialize for StructType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        SchemaWire {
            fields: self.logical_fields.clone(),
        }
        .serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for StructType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_logical_fields(
            SchemaWire::deserialize(deserializer)?.fields,
        ))
    }
}

impl StructType {
    pub fn new(fields: Vec<StructField>) -> Self {
        Self::from_logical_fields(fields.into_iter().map(LogicalField::from).collect())
    }

    pub fn from_logical_fields(fields: Vec<LogicalField>) -> Self {
        let logical_fields: Vec<_> = fields
            .into_iter()
            .map(|mut f| {
                f.data_type = f.data_type.into_logical();
                f
            })
            .collect();
        let fields = logical_fields
            .iter()
            .cloned()
            .map(LogicalField::into_legacy)
            .collect();
        Self {
            fields,
            logical_fields,
        }
    }

    /// Legacy descriptors, without metadata. The slice type is unchanged from 4.16.
    pub fn fields(&self) -> &[StructField] {
        &self.fields
    }

    pub fn logical_fields(&self) -> &[LogicalField] {
        &self.logical_fields
    }

    pub fn validate_metadata(&self) -> Result<(), String> {
        let mut total_bytes = 0usize;
        for field in &self.logical_fields {
            validate_field_metadata(field, 0, &mut total_bytes)?;
        }
        Ok(())
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

fn validate_field_metadata(
    field: &LogicalField,
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
        DataType::LogicalStruct(fields) => {
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
        assert!(parsed.logical_fields()[0].metadata.is_empty());
        assert_eq!(parsed.to_json().unwrap(), legacy);

        let mut metadata = BTreeMap::new();
        metadata.insert("owner".into(), serde_json::json!("analytics"));
        metadata.insert(
            "policy".into(),
            serde_json::json!({"tier": 2, "tags": ["pii"]}),
        );
        let schema = StructType::from_logical_fields(vec![
            LogicalField::new(
                "items".into(),
                DataType::ArrayWithNullability {
                    element_type: Box::new(DataType::Byte),
                    contains_null: false,
                },
                false,
            )
            .with_metadata(metadata),
            LogicalField::new(
                "attributes".into(),
                DataType::MapWithNullability {
                    key_type: Box::new(DataType::String),
                    value_type: Box::new(DataType::Float),
                    value_contains_null: true,
                },
                true,
            ),
            LogicalField::new(
                "required_attributes".into(),
                DataType::MapWithNullability {
                    key_type: Box::new(DataType::String),
                    value_type: Box::new(DataType::Integer),
                    value_contains_null: false,
                },
                true,
            ),
            LogicalField::new("event_time".into(), DataType::TimestampNtz, true),
        ]);
        schema.validate_metadata().unwrap();
        let roundtrip: StructType = serde_json::from_str(&schema.to_json().unwrap()).unwrap();
        assert_eq!(roundtrip.logical_fields()[0].metadata["owner"], "analytics");
        assert!(matches!(
            roundtrip.logical_fields()[0].data_type,
            DataType::ArrayWithNullability {
                contains_null: false,
                ..
            }
        ));
        assert!(matches!(
            roundtrip.logical_fields()[1].data_type,
            DataType::MapWithNullability {
                value_contains_null: true,
                ..
            }
        ));
        assert!(matches!(
            roundtrip.logical_fields()[2].data_type,
            DataType::MapWithNullability {
                value_contains_null: false,
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
        let schema = StructType::from_logical_fields(vec![
            LogicalField::new("value".into(), DataType::String, true).with_metadata(metadata),
        ]);
        assert!(schema.validate_metadata().is_err());
    }

    #[test]
    fn logical_schema_metadata_byte_limit_counts_nested_fields_exactly() {
        fn metadata_with_payload(bytes: usize) -> BTreeMap<String, serde_json::Value> {
            [(
                "payload".to_string(),
                serde_json::Value::String("x".repeat(bytes)),
            )]
            .into()
        }

        let overhead = serde_json::to_vec(&metadata_with_payload(0)).unwrap().len();
        let payload_budget = MAX_LOGICAL_SCHEMA_METADATA_BYTES - 2 * overhead;
        let parent_payload = payload_budget / 2;
        let child_payload = payload_budget - parent_payload;
        let schema_for_child_size = |child_size| {
            StructType::from_logical_fields(vec![
                LogicalField::new(
                    "outer".into(),
                    DataType::LogicalStruct(vec![
                        LogicalField::new("child".into(), DataType::String, true)
                            .with_metadata(metadata_with_payload(child_size)),
                    ]),
                    true,
                )
                .with_metadata(metadata_with_payload(parent_payload)),
            ])
        };

        let at_limit = schema_for_child_size(child_payload);
        let child_metadata = match &at_limit.logical_fields()[0].data_type {
            DataType::LogicalStruct(fields) => &fields[0].metadata,
            _ => unreachable!("test fixture must contain a nested struct"),
        };
        let total_metadata_bytes = serde_json::to_vec(&at_limit.logical_fields()[0].metadata)
            .unwrap()
            .len()
            + serde_json::to_vec(child_metadata).unwrap().len();
        assert_eq!(total_metadata_bytes, MAX_LOGICAL_SCHEMA_METADATA_BYTES);
        assert!(at_limit.validate_metadata().is_ok());
        assert!(
            schema_for_child_size(child_payload + 1)
                .validate_metadata()
                .is_err()
        );
    }

    #[test]
    fn test_empty_struct_type() {
        let empty = StructType::from_logical_fields(vec![]);
        assert!(empty.logical_fields().is_empty());
    }
}
