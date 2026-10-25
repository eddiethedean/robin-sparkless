//! Polars schema conversion for StructType. Kept in main crate (has Polars dependency).

use polars::prelude::{DataType as PlDataType, Field, PolarsError, Schema, TimeUnit, TimeZone};
use robin_sparkless_core::{DataType, StructField, StructType};

/// Extension trait for Polars schema conversion. Implemented for [`StructType`] from core.
/// Bring this trait into scope to use `StructType::from_polars_schema` and `to_polars_schema`.
pub trait StructTypePolarsExt: Sized {
    fn from_polars_schema(schema: &Schema) -> Self;
    fn to_polars_schema(&self) -> Schema;
    /// Convert only types whose semantics can be represented without lossy fallback.
    fn try_from_polars_schema(schema: &Schema) -> Result<Self, PolarsError>;
    /// Convert only fields whose nullability, metadata, and logical types survive Polars' schema.
    fn try_to_polars_schema(&self) -> Result<Schema, PolarsError>;
}

impl StructTypePolarsExt for StructType {
    fn from_polars_schema(schema: &Schema) -> Self {
        let fields = schema
            .iter()
            .map(|(name, dtype)| StructField {
                name: name.to_string(),
                data_type: polars_type_to_data_type(dtype),
                nullable: true, // Polars doesn't expose nullability in the same way
                metadata: Default::default(),
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

    fn try_from_polars_schema(schema: &Schema) -> Result<Self, PolarsError> {
        let fields = schema
            .iter()
            .map(|(name, dtype)| {
                Ok(StructField::new(
                    name.to_string(),
                    checked_polars_type_to_data_type(dtype, name)?,
                    true,
                ))
            })
            .collect::<Result<Vec<_>, PolarsError>>()?;
        Ok(StructType::new(fields))
    }

    fn try_to_polars_schema(&self) -> Result<Schema, PolarsError> {
        self.validate_metadata()
            .map_err(|error| PolarsError::ComputeError(error.into()))?;
        let fields = self
            .fields()
            .iter()
            .map(|field| {
                if !field.nullable {
                    return Err(schema_conversion_error(
                        &field.name,
                        "Polars cannot preserve non-nullable field declarations",
                    ));
                }
                if !field.metadata.is_empty() {
                    return Err(schema_conversion_error(
                        &field.name,
                        "Polars Field does not preserve logical field metadata",
                    ));
                }
                ensure_polars_nested_schema_is_lossless(&field.data_type, &field.name)?;
                Ok(Field::new(
                    field.name.as_str().into(),
                    checked_data_type_to_polars_type(&field.data_type, &field.name)?,
                ))
            })
            .collect::<Result<Vec<_>, PolarsError>>()?;
        Ok(Schema::from_iter(fields))
    }
}

fn schema_conversion_error(path: &str, message: &str) -> PolarsError {
    PolarsError::InvalidOperation(format!("logical schema field '{path}': {message}").into())
}

fn ensure_polars_nested_schema_is_lossless(
    data_type: &DataType,
    path: &str,
) -> Result<(), PolarsError> {
    match data_type {
        DataType::ArrayWithNullability {
            contains_null: false,
            ..
        } => Err(schema_conversion_error(
            path,
            "Polars List cannot preserve non-null element declarations",
        )),
        DataType::Array(element)
        | DataType::ArrayWithNullability {
            element_type: element,
            contains_null: true,
        } => {
            ensure_polars_nested_schema_is_lossless(element, &format!("{path}[]"))?;
            Ok(())
        }
        DataType::Struct(fields) => {
            for field in fields {
                let nested_path = format!("{path}.{}", field.name);
                if !field.nullable {
                    return Err(schema_conversion_error(
                        &nested_path,
                        "Polars Struct fields cannot preserve nested nullability",
                    ));
                }
                if !field.metadata.is_empty() {
                    return Err(schema_conversion_error(
                        &nested_path,
                        "Polars Struct fields cannot preserve nested metadata",
                    ));
                }
                ensure_polars_nested_schema_is_lossless(&field.data_type, &nested_path)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(crate) fn checked_polars_type_to_data_type(
    dtype: &PlDataType,
    path: &str,
) -> Result<DataType, PolarsError> {
    let unsupported = || {
        schema_conversion_error(
            path,
            &format!("physical type {dtype} has no checked logical mapping"),
        )
    };
    Ok(match dtype {
        PlDataType::String => DataType::String,
        PlDataType::Int8 => DataType::Byte,
        PlDataType::Int16 => DataType::Short,
        PlDataType::Int32 => DataType::Integer,
        PlDataType::Int64 => DataType::Long,
        PlDataType::Float32 => DataType::Float,
        PlDataType::Float64 => DataType::Double,
        PlDataType::Decimal(precision, scale) => DataType::Decimal(
            (*precision)
                .try_into()
                .map_err(|_| schema_conversion_error(path, "decimal precision exceeds 255"))?,
            (*scale)
                .try_into()
                .map_err(|_| schema_conversion_error(path, "decimal scale exceeds 255"))?,
        ),
        PlDataType::Boolean => DataType::Boolean,
        PlDataType::Date => DataType::Date,
        PlDataType::Datetime(TimeUnit::Microseconds, Some(timezone))
            if timezone.as_str() == "UTC" =>
        {
            DataType::Timestamp
        }
        PlDataType::Datetime(TimeUnit::Microseconds, None) => DataType::TimestampNtz,
        PlDataType::Binary => DataType::Binary,
        // Polars' physical schema omits nested field nullability and metadata. Reject it here;
        // callers with a logical schema must carry that descriptor alongside the plan.
        PlDataType::List(_) | PlDataType::Struct(_) => return Err(unsupported()),
        _ => return Err(unsupported()),
    })
}

pub(crate) fn checked_data_type_to_polars_type(
    dtype: &DataType,
    path: &str,
) -> Result<PlDataType, PolarsError> {
    let unsupported = |reason: &str| schema_conversion_error(path, reason);
    Ok(match dtype {
        DataType::String => PlDataType::String,
        DataType::Byte => PlDataType::Int8,
        DataType::Short => PlDataType::Int16,
        DataType::Integer => PlDataType::Int32,
        DataType::Long => PlDataType::Int64,
        DataType::Float => PlDataType::Float32,
        DataType::Double => PlDataType::Float64,
        DataType::Decimal(precision, scale) => {
            PlDataType::Decimal(*precision as usize, *scale as usize)
        }
        DataType::Boolean => PlDataType::Boolean,
        DataType::Date => PlDataType::Date,
        DataType::Timestamp => PlDataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
        DataType::TimestampNtz => PlDataType::Datetime(TimeUnit::Microseconds, None),
        DataType::Binary => PlDataType::Binary,
        DataType::Array(element) => {
            PlDataType::List(Box::new(checked_data_type_to_polars_type(element, path)?))
        }
        DataType::ArrayWithNullability { element_type, .. } => PlDataType::List(Box::new(
            checked_data_type_to_polars_type(element_type, path)?,
        )),
        DataType::Struct(fields) => {
            let nested = fields
                .iter()
                .map(|field| {
                    Ok(Field::new(
                        field.name.as_str().into(),
                        checked_data_type_to_polars_type(&field.data_type, path)?,
                    ))
                })
                .collect::<Result<Vec<_>, PolarsError>>()?;
            PlDataType::Struct(nested)
        }
        DataType::Map(_, _) | DataType::MapWithNullability { .. } => {
            return Err(unsupported("Polars has no native logical map type"));
        }
        _ => return Err(unsupported("logical type has no checked Polars mapping")),
    })
}

fn polars_type_to_data_type(polars_type: &PlDataType) -> DataType {
    match polars_type {
        PlDataType::String => DataType::String,
        PlDataType::Int16 => DataType::Short,
        PlDataType::Int32 => DataType::Integer,
        PlDataType::Int64 => DataType::Long,
        // Polars rank(), row_number(), dense_rank() return UInt32; map to Integer for PySpark parity (int not str).
        PlDataType::UInt32 => DataType::Integer,
        PlDataType::UInt64 => DataType::Long,
        PlDataType::Float32 | PlDataType::Float64 => DataType::Double,
        PlDataType::Decimal(precision, scale) => DataType::Decimal(
            (*precision).min(u8::MAX as usize) as u8,
            (*scale).min(u8::MAX as usize) as u8,
        ),
        PlDataType::Boolean => DataType::Boolean,
        PlDataType::Date => DataType::Date,
        PlDataType::Datetime(_, _) => DataType::Timestamp,
        PlDataType::Binary => DataType::Binary,
        PlDataType::List(inner) => DataType::Array(Box::new(polars_type_to_data_type(inner))),
        PlDataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| {
                    StructField::new(
                        f.name().to_string(),
                        polars_type_to_data_type(f.dtype()),
                        true,
                    )
                })
                .collect(),
        ),
        _ => DataType::String,
    }
}

pub(crate) fn data_type_to_polars_type(data_type: &DataType) -> PlDataType {
    match data_type {
        DataType::String => PlDataType::String,
        DataType::Byte => PlDataType::Int8,
        DataType::Short => PlDataType::Int16,
        DataType::Integer => PlDataType::Int32,
        DataType::Long => PlDataType::Int64,
        DataType::Float => PlDataType::Float32,
        DataType::Double => PlDataType::Float64,
        DataType::Decimal(precision, scale) => {
            PlDataType::Decimal(*precision as usize, *scale as usize)
        }
        DataType::Boolean => PlDataType::Boolean,
        DataType::Date => PlDataType::Date,
        DataType::Timestamp => PlDataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
        DataType::TimestampNtz => PlDataType::Datetime(TimeUnit::Microseconds, None),
        DataType::Binary => PlDataType::Binary,
        DataType::Array(inner)
        | DataType::ArrayWithNullability {
            element_type: inner,
            ..
        } => PlDataType::List(Box::new(data_type_to_polars_type(inner))),
        DataType::Struct(fields) => PlDataType::Struct(
            fields
                .iter()
                .map(|f| {
                    Field::new(
                        f.name.as_str().into(),
                        data_type_to_polars_type(&f.data_type),
                    )
                })
                .collect(),
        ),
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

    /// Window rank/row_number return UInt32 in Polars; we map to Integer for PySpark parity (int in Row, not str).
    #[test]
    fn test_window_uint_maps_to_integer_long() {
        let polars_schema = Schema::from_iter(vec![
            Field::new("rn".into(), PlDataType::UInt32),
            Field::new("rank".into(), PlDataType::UInt64),
        ]);
        let struct_type = StructType::from_polars_schema(&polars_schema);
        assert_eq!(struct_type.fields().len(), 2);
        assert!(matches!(
            struct_type.fields()[0].data_type,
            DataType::Integer
        ));
        assert!(matches!(struct_type.fields()[1].data_type, DataType::Long));
    }

    #[test]
    fn checked_conversion_preserves_native_scalar_distinctions() {
        let schema = Schema::from_iter(vec![
            Field::new("tiny".into(), PlDataType::Int8),
            Field::new("single".into(), PlDataType::Float32),
            Field::new(
                "instant".into(),
                PlDataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC)),
            ),
            Field::new(
                "wall_clock".into(),
                PlDataType::Datetime(TimeUnit::Microseconds, None),
            ),
            Field::new("payload".into(), PlDataType::Binary),
        ]);
        let logical = StructType::try_from_polars_schema(&schema).unwrap();
        assert!(matches!(logical.fields()[0].data_type, DataType::Byte));
        assert!(matches!(logical.fields()[1].data_type, DataType::Float));
        assert!(matches!(logical.fields()[2].data_type, DataType::Timestamp));
        assert!(matches!(
            logical.fields()[3].data_type,
            DataType::TimestampNtz
        ));
        assert!(matches!(logical.fields()[4].data_type, DataType::Binary));

        let physical = logical.try_to_polars_schema().unwrap();
        let logical_again = StructType::try_from_polars_schema(&physical).unwrap();
        assert!(matches!(
            logical_again.fields()[0].data_type,
            DataType::Byte
        ));
        assert!(matches!(
            logical_again.fields()[1].data_type,
            DataType::Float
        ));
        assert!(matches!(
            logical_again.fields()[2].data_type,
            DataType::Timestamp
        ));
        assert!(matches!(
            logical_again.fields()[3].data_type,
            DataType::TimestampNtz
        ));
    }

    #[test]
    fn checked_conversion_rejects_types_and_metadata_that_polars_would_erase() {
        let unsigned = Schema::from_iter(vec![Field::new("u".into(), PlDataType::UInt64)]);
        assert!(StructType::try_from_polars_schema(&unsigned).is_err());

        let with_metadata = StructType::new(vec![
            StructField::new("id".into(), DataType::Long, true)
                .with_metadata([("source".to_string(), serde_json::json!("catalog"))].into()),
        ]);
        assert!(with_metadata.try_to_polars_schema().is_err());

        let non_nullable =
            StructType::new(vec![StructField::new("id".into(), DataType::Long, false)]);
        assert!(non_nullable.try_to_polars_schema().is_err());

        let nested = StructType::new(vec![StructField::new(
            "items".into(),
            DataType::ArrayWithNullability {
                element_type: Box::new(DataType::Integer),
                contains_null: false,
            },
            true,
        )]);
        assert!(nested.try_to_polars_schema().is_err());

        let map = StructType::new(vec![StructField::new(
            "attributes".into(),
            DataType::Map(Box::new(DataType::String), Box::new(DataType::Integer)),
            true,
        )]);
        assert!(map.try_to_polars_schema().is_err());

        let map_with_nullability = StructType::new(vec![StructField::new(
            "required_attributes".into(),
            DataType::MapWithNullability {
                key_type: Box::new(DataType::String),
                value_type: Box::new(DataType::Integer),
                value_contains_null: false,
            },
            true,
        )]);
        assert!(map_with_nullability.try_to_polars_schema().is_err());
    }

    #[test]
    fn checked_struct_mapping_keeps_lossy_nested_declarations_in_the_sidecar() {
        let metadata = [("source".to_string(), serde_json::json!("catalog"))]
            .into_iter()
            .collect();
        let schema = StructType::new(vec![StructField::new(
            "payload".into(),
            DataType::Struct(vec![
                StructField::new("id".into(), DataType::Integer, false).with_metadata(metadata),
                StructField::new("label".into(), DataType::String, true),
            ]),
            true,
        )]);

        assert!(schema.try_to_polars_schema().is_err());
        assert!(matches!(
            checked_data_type_to_polars_type(&schema.fields()[0].data_type, "payload").unwrap(),
            PlDataType::Struct(_)
        ));

        let array_schema = StructType::new(vec![StructField::new(
            "items".into(),
            DataType::Array(Box::new(DataType::Struct(vec![StructField::new(
                "id".into(),
                DataType::Integer,
                false,
            )]))),
            true,
        )]);
        assert!(array_schema.try_to_polars_schema().is_err());
        assert!(matches!(
            checked_data_type_to_polars_type(&array_schema.fields()[0].data_type, "items").unwrap(),
            PlDataType::List(_)
        ));
    }
}
