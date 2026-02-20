//! Polars schema conversion for StructType. Kept in main crate (has Polars dependency).

use polars::prelude::{DataType as PlDataType, Field, Schema, TimeUnit};
use robin_sparkless_core::{DataType, StructField, StructType};

/// Extension trait for Polars schema conversion. Implemented for [`StructType`] from core.
/// Bring this trait into scope to use `StructType::from_polars_schema` and `to_polars_schema`.
pub trait StructTypePolarsExt: Sized {
    fn from_polars_schema(schema: &Schema) -> Self;
    fn to_polars_schema(&self) -> Schema;
}

impl StructTypePolarsExt for StructType {
    fn from_polars_schema(schema: &Schema) -> Self {
        let fields = schema
            .iter()
            .map(|(name, dtype)| StructField {
                name: name.to_string(),
                data_type: polars_type_to_data_type(dtype),
                nullable: true, // Polars doesn't expose nullability in the same way
            })
            .collect();
        StructType::new(fields)
    }

    fn to_polars_schema(&self) -> Schema {
        let fields: Vec<Field> = self
            .fields()
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
}

fn polars_type_to_data_type(polars_type: &PlDataType) -> DataType {
    match polars_type {
        PlDataType::String => DataType::String,
        PlDataType::Int32 | PlDataType::Int64 => DataType::Long,
        PlDataType::Float32 | PlDataType::Float64 => DataType::Double,
        PlDataType::Boolean => DataType::Boolean,
        PlDataType::Date => DataType::Date,
        PlDataType::Datetime(_, _) => DataType::Timestamp,
        PlDataType::List(inner) => DataType::Array(Box::new(polars_type_to_data_type(inner))),
        _ => DataType::String,
    }
}

pub(super) fn data_type_to_polars_type(data_type: &DataType) -> PlDataType {
    match data_type {
        DataType::String => PlDataType::String,
        DataType::Integer => PlDataType::Int32,
        DataType::Long => PlDataType::Int64,
        DataType::Double => PlDataType::Float64,
        DataType::Boolean => PlDataType::Boolean,
        DataType::Date => PlDataType::Date,
        DataType::Timestamp => PlDataType::Datetime(TimeUnit::Microseconds, None),
        DataType::Array(inner) => PlDataType::List(Box::new(data_type_to_polars_type(inner))),
        _ => PlDataType::String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{Field, Schema};

    #[test]
    fn test_struct_type_from_polars_schema() {
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
    }

    #[test]
    fn test_roundtrip_schema_conversion() {
        let original = StructType::new(vec![
            StructField::new("a".to_string(), DataType::Integer, true),
            StructField::new("b".to_string(), DataType::Long, true),
            StructField::new("c".to_string(), DataType::Double, true),
        ]);
        let polars_schema = original.to_polars_schema();
        let roundtrip = StructType::from_polars_schema(&polars_schema);
        assert_eq!(roundtrip.fields().len(), original.fields().len());
    }
}
