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
            .map(|f| Field::new(f.name.as_str().into(), data_type_to_polars_type(&f.data_type)))
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
        PlDataType::Int32 => DataType::Integer,
        PlDataType::Int64 => DataType::Long,
        PlDataType::Float64 => DataType::Double,
        PlDataType::Boolean => DataType::Boolean,
        PlDataType::Date => DataType::Date,
        PlDataType::Datetime(_, _) => DataType::Timestamp,
        PlDataType::List(inner) => {
            DataType::Array(Box::new(polars_type_to_data_type(inner)))
        }
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
        DataType::Array(inner) => {
            PlDataType::List(Box::new(data_type_to_polars_type(inner)))
        }
        _ => PlDataType::String, // Default fallback
    }
}
