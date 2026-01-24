use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructField {
    #[pyo3(get, set)]
    name: String,
    data_type: DataType,
    #[pyo3(get, set)]
    nullable: bool,
}

#[pymethods]
impl StructField {
    #[new]
    fn new(name: String, data_type: String, nullable: bool) -> Self {
        let dt = match data_type.as_str() {
            "string" => DataType::String,
            "int" | "integer" => DataType::Integer,
            "long" => DataType::Long,
            "double" => DataType::Double,
            "boolean" => DataType::Boolean,
            "date" => DataType::Date,
            "timestamp" => DataType::Timestamp,
            _ => DataType::String,
        };
        StructField {
            name,
            data_type: dt,
            nullable,
        }
    }
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructType {
    fields: Vec<StructField>,
}

#[pymethods]
impl StructType {
    #[new]
    fn new(fields: Vec<StructField>) -> Self {
        StructType { fields }
    }
    
    fn __len__(&self) -> usize {
        self.fields.len()
    }
    
    fn __getitem__(&self, idx: isize) -> PyResult<StructField> {
        let idx = if idx < 0 {
            (self.fields.len() as isize + idx) as usize
        } else {
            idx as usize
        };
        self.fields.get(idx)
            .cloned()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyIndexError, _>("Index out of range"))
    }
}

impl StructType {
    pub fn from_arrow_schema(schema: &arrow::datatypes::Schema) -> Self {
        let fields = schema
            .fields()
            .iter()
            .map(|f| StructField {
                name: f.name().clone(),
                data_type: arrow_type_to_data_type(f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect();
        StructType { fields }
    }
    
    pub fn to_arrow_schema(&self) -> arrow::datatypes::Schema {
        let fields: Vec<arrow::datatypes::Field> = self
            .fields
            .iter()
            .map(|f| arrow::datatypes::Field::new(
                &f.name,
                data_type_to_arrow_type(&f.data_type),
                f.nullable,
            ))
            .collect();
        arrow::datatypes::Schema::new(fields)
    }
}

fn arrow_type_to_data_type(arrow_type: &arrow::datatypes::DataType) -> DataType {
    match arrow_type {
        arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => DataType::String,
        arrow::datatypes::DataType::Int32 => DataType::Integer,
        arrow::datatypes::DataType::Int64 => DataType::Long,
        arrow::datatypes::DataType::Float64 => DataType::Double,
        arrow::datatypes::DataType::Boolean => DataType::Boolean,
        arrow::datatypes::DataType::Date32 | arrow::datatypes::DataType::Date64 => DataType::Date,
        arrow::datatypes::DataType::Timestamp(_, _) => DataType::Timestamp,
        arrow::datatypes::DataType::List(field) => {
            DataType::Array(Box::new(arrow_type_to_data_type(field.data_type())))
        }
        _ => DataType::String, // Default fallback
    }
}

fn data_type_to_arrow_type(data_type: &DataType) -> arrow::datatypes::DataType {
    match data_type {
        DataType::String => arrow::datatypes::DataType::Utf8,
        DataType::Integer => arrow::datatypes::DataType::Int32,
        DataType::Long => arrow::datatypes::DataType::Int64,
        DataType::Double => arrow::datatypes::DataType::Float64,
        DataType::Boolean => arrow::datatypes::DataType::Boolean,
        DataType::Date => arrow::datatypes::DataType::Date32,
        DataType::Timestamp => arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        DataType::Array(inner) => {
            let item_field = arrow::datatypes::Field::new(
                "item",
                data_type_to_arrow_type(inner),
                true,
            );
            arrow::datatypes::DataType::List(Arc::new(item_field))
        }
        _ => arrow::datatypes::DataType::Utf8, // Default fallback
    }
}
