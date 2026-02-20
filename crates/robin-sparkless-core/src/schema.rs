use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    String,
    Integer,
    Long,
    Double,
    Boolean,
    Date,
    Timestamp,
    Array(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
    Struct(Vec<StructField>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl StructField {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        StructField {
            name,
            data_type,
            nullable,
        }
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
    fn test_empty_struct_type() {
        let empty = StructType::new(vec![]);
        assert!(empty.fields().is_empty());
    }
}
