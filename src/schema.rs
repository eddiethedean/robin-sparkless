use polars::prelude::{DataType as PlDataType, Schema, TimeUnit};
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

    pub fn from_polars_schema(schema: &Schema) -> Self {
        let fields = schema
            .iter()
            .map(|(name, dtype)| StructField {
                name: name.to_string(),
                data_type: polars_type_to_data_type(dtype),
                nullable: true, // Polars doesn't expose nullability in the same way
            })
            .collect();
        StructType { fields }
    }

    pub fn to_polars_schema(&self) -> Schema {
        use polars::prelude::Field;
        let fields: Vec<Field> = self
            .fields
            .iter()
            .map(|f| {
                Field::new(
                    f.name.as_str().into(),
                    data_type_to_polars_type(&f.data_type),
                )
            })
            .collect();
        Schema::from_iter(fields)
    }

    pub fn fields(&self) -> &[StructField] {
        &self.fields
    }
}

fn polars_type_to_data_type(polars_type: &PlDataType) -> DataType {
    match polars_type {
        PlDataType::String => DataType::String,
        // Spark/Sparkless inferSchema uses 64-bit for integral types; map Int32 as Long.
        PlDataType::Int32 | PlDataType::Int64 => DataType::Long,
        // Map both Float32 and Float64 to Double for schema parity.
        PlDataType::Float32 | PlDataType::Float64 => DataType::Double,
        PlDataType::Boolean => DataType::Boolean,
        PlDataType::Date => DataType::Date,
        PlDataType::Datetime(_, _) => DataType::Timestamp,
        PlDataType::List(inner) => DataType::Array(Box::new(polars_type_to_data_type(inner))),
        _ => DataType::String, // Default fallback
    }
}

fn data_type_to_polars_type(data_type: &DataType) -> PlDataType {
    match data_type {
        DataType::String => PlDataType::String,
        DataType::Integer => PlDataType::Int32,
        DataType::Long => PlDataType::Int64,
        DataType::Double => PlDataType::Float64,
        DataType::Boolean => PlDataType::Boolean,
        DataType::Date => PlDataType::Date,
        DataType::Timestamp => PlDataType::Datetime(TimeUnit::Microseconds, None),
        DataType::Array(inner) => PlDataType::List(Box::new(data_type_to_polars_type(inner))),
        _ => PlDataType::String, // Default fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{Field, Schema};

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
    fn test_struct_type_from_polars_schema() {
        // Create a Polars schema
        let polars_schema = Schema::from_iter(vec![
            Field::new("id".into(), PlDataType::Int64),
            Field::new("name".into(), PlDataType::String),
            Field::new("score".into(), PlDataType::Float64),
            Field::new("active".into(), PlDataType::Boolean),
        ]);

        let struct_type = StructType::from_polars_schema(&polars_schema);

        assert_eq!(struct_type.fields().len(), 4);
        assert_eq!(struct_type.fields()[0].name, "id");
        assert!(matches!(struct_type.fields()[0].data_type, DataType::Long));
        assert_eq!(struct_type.fields()[1].name, "name");
        assert!(matches!(
            struct_type.fields()[1].data_type,
            DataType::String
        ));
        assert_eq!(struct_type.fields()[2].name, "score");
        assert!(matches!(
            struct_type.fields()[2].data_type,
            DataType::Double
        ));
        assert_eq!(struct_type.fields()[3].name, "active");
        assert!(matches!(
            struct_type.fields()[3].data_type,
            DataType::Boolean
        ));
    }

    #[test]
    fn test_struct_type_to_polars_schema() {
        let fields = vec![
            StructField::new("id".to_string(), DataType::Long, false),
            StructField::new("name".to_string(), DataType::String, true),
            StructField::new("score".to_string(), DataType::Double, true),
        ];
        let struct_type = StructType::new(fields);

        let polars_schema = struct_type.to_polars_schema();

        assert_eq!(polars_schema.len(), 3);
        assert_eq!(polars_schema.get("id"), Some(&PlDataType::Int64));
        assert_eq!(polars_schema.get("name"), Some(&PlDataType::String));
        assert_eq!(polars_schema.get("score"), Some(&PlDataType::Float64));
    }

    #[test]
    fn test_roundtrip_schema_conversion() {
        // Create a struct type, convert to Polars, convert back
        let original = StructType::new(vec![
            StructField::new("a".to_string(), DataType::Integer, true),
            StructField::new("b".to_string(), DataType::Long, true),
            StructField::new("c".to_string(), DataType::Double, true),
            StructField::new("d".to_string(), DataType::Boolean, true),
            StructField::new("e".to_string(), DataType::String, true),
        ]);

        let polars_schema = original.to_polars_schema();
        let roundtrip = StructType::from_polars_schema(&polars_schema);

        assert_eq!(roundtrip.fields().len(), original.fields().len());
        for (orig, rt) in original.fields().iter().zip(roundtrip.fields().iter()) {
            assert_eq!(orig.name, rt.name);
        }
    }

    #[test]
    fn test_polars_type_to_data_type_basic() {
        assert!(matches!(
            polars_type_to_data_type(&PlDataType::String),
            DataType::String
        ));
        assert!(matches!(
            polars_type_to_data_type(&PlDataType::Int64),
            DataType::Long
        ));
        assert!(matches!(
            polars_type_to_data_type(&PlDataType::Float64),
            DataType::Double
        ));
        assert!(matches!(
            polars_type_to_data_type(&PlDataType::Boolean),
            DataType::Boolean
        ));
        assert!(matches!(
            polars_type_to_data_type(&PlDataType::Date),
            DataType::Date
        ));
    }

    #[test]
    fn test_polars_type_to_data_type_datetime() {
        let datetime_type = PlDataType::Datetime(TimeUnit::Microseconds, None);
        assert!(matches!(
            polars_type_to_data_type(&datetime_type),
            DataType::Timestamp
        ));
    }

    #[test]
    fn test_polars_type_to_data_type_list() {
        let list_type = PlDataType::List(Box::new(PlDataType::Int64));
        match polars_type_to_data_type(&list_type) {
            DataType::Array(inner) => {
                assert!(matches!(*inner, DataType::Long));
            }
            other => panic!("Expected Array type, got {other:?}"),
        }
    }

    #[test]
    fn test_polars_type_to_data_type_fallback() {
        // Unknown type should fall back to String
        let unknown_type = PlDataType::UInt8;
        assert!(matches!(
            polars_type_to_data_type(&unknown_type),
            DataType::String
        ));
    }

    #[test]
    fn test_data_type_to_polars_type_basic() {
        assert_eq!(
            data_type_to_polars_type(&DataType::String),
            PlDataType::String
        );
        assert_eq!(
            data_type_to_polars_type(&DataType::Integer),
            PlDataType::Int32
        );
        assert_eq!(data_type_to_polars_type(&DataType::Long), PlDataType::Int64);
        assert_eq!(
            data_type_to_polars_type(&DataType::Double),
            PlDataType::Float64
        );
        assert_eq!(
            data_type_to_polars_type(&DataType::Boolean),
            PlDataType::Boolean
        );
        assert_eq!(data_type_to_polars_type(&DataType::Date), PlDataType::Date);
    }

    #[test]
    fn test_data_type_to_polars_type_timestamp() {
        let result = data_type_to_polars_type(&DataType::Timestamp);
        assert!(matches!(
            result,
            PlDataType::Datetime(TimeUnit::Microseconds, None)
        ));
    }

    #[test]
    fn test_data_type_to_polars_type_array() {
        let array_type = DataType::Array(Box::new(DataType::Long));
        let result = data_type_to_polars_type(&array_type);
        match result {
            PlDataType::List(inner) => {
                assert_eq!(*inner, PlDataType::Int64);
            }
            other => panic!("Expected List type, got {other:?}"),
        }
    }

    #[test]
    fn test_data_type_to_polars_type_map_fallback() {
        // Map type falls back to String
        let map_type = DataType::Map(Box::new(DataType::String), Box::new(DataType::Long));
        assert_eq!(data_type_to_polars_type(&map_type), PlDataType::String);
    }

    #[test]
    fn test_data_type_to_polars_type_struct_fallback() {
        // Nested Struct falls back to String
        let struct_type = DataType::Struct(vec![StructField::new(
            "nested".to_string(),
            DataType::Integer,
            true,
        )]);
        assert_eq!(data_type_to_polars_type(&struct_type), PlDataType::String);
    }

    #[test]
    fn test_empty_struct_type() {
        let empty = StructType::new(vec![]);
        assert!(empty.fields().is_empty());

        let polars_schema = empty.to_polars_schema();
        assert!(polars_schema.is_empty());
    }
}
