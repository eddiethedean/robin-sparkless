use pyo3::prelude::*;
use pyo3::types::{PyType, PyDict};
use pyo3::IntoPy;
use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use std::sync::Arc;
use std::collections::HashMap;
use datafusion::execution::context::SessionContext;
use tokio::runtime::Runtime;
use crate::dataframe::DataFrame;
use crate::execution::ExecutionEngine;
use serde_json::Value;

#[pyclass]
pub struct SparkSessionBuilder {
    app_name: Option<String>,
    master: Option<String>,
    config: HashMap<String, String>,
}

#[pymethods]
impl SparkSessionBuilder {
    #[new]
    fn new() -> Self {
        SparkSessionBuilder {
            app_name: None,
            master: None,
            config: HashMap::new(),
        }
    }
    
    fn appName(&mut self, name: &str) -> PyResult<()> {
        self.app_name = Some(name.to_string());
        Ok(())
    }
    
    fn master(&mut self, master: &str) -> PyResult<()> {
        self.master = Some(master.to_string());
        Ok(())
    }
    
    fn config(&mut self, key: &str, value: &str) -> PyResult<()> {
        self.config.insert(key.to_string(), value.to_string());
        Ok(())
    }
    
    fn getOrCreate(&self) -> PyResult<SparkSession> {
        Ok(SparkSession::new(
            self.app_name.clone(),
            self.master.clone(),
            self.config.clone(),
        ))
    }
}

#[pyclass]
pub struct SparkSession {
    app_name: Option<String>,
    master: Option<String>,
    config: HashMap<String, String>,
    execution_engine: Arc<ExecutionEngine>,
}

#[pymethods]
impl SparkSession {
    #[new]
    #[pyo3(signature = (app_name=None, master=None, config=HashMap::new()))]
    fn new(
        app_name: Option<String>,
        master: Option<String>,
        config: HashMap<String, String>,
    ) -> Self {
        SparkSession {
            app_name,
            master,
            config,
            execution_engine: Arc::new(ExecutionEngine::new()),
        }
    }
    
    #[classmethod]
    fn builder(_cls: &PyType) -> PyResult<SparkSessionBuilder> {
        Ok(SparkSessionBuilder::new())
    }
    
    #[classmethod]
    fn getActiveSession(_cls: &PyType) -> PyResult<Option<SparkSession>> {
        // In a real implementation, you'd maintain a global session
        // For now, return None
        Ok(None)
    }
    
    fn createDataFrame(
        &self,
        data: &PyAny,
        schema: Option<&PyAny>,
    ) -> PyResult<DataFrame> {
        Python::with_gil(|py| {
            // Handle schema - can be column names (list of strings) or StructType
            let column_names: Option<Vec<String>> = if let Some(schema_obj) = schema {
                if let Ok(names) = schema_obj.extract::<Vec<String>>() {
                    Some(names)
                } else {
                    None
                }
            } else {
                None
            };
            
            // Try to extract as list of tuples
            if let Ok(list) = data.extract::<Vec<PyObject>>() {
                if let Some(cols) = column_names {
                    self.create_dataframe_from_tuples(list, cols, py)
                } else {
                    self.create_dataframe_from_list(list, schema, py)
                }
            } else if let Ok(dict_list) = data.extract::<Vec<HashMap<String, PyObject>>>() {
                self.create_dataframe_from_dict_list(dict_list, schema, py)
            } else {
                Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Unsupported data type for createDataFrame. Expected list of tuples or list of dicts."
                ))
            }
        })
    }
    
    fn read(&self) -> PyResult<DataFrameReader> {
        Python::with_gil(|py| {
            Ok(DataFrameReader {
                session: Py::new(py, self.clone())?,
            })
        })
    }
    
    fn sql(&self, query: &str) -> PyResult<DataFrame> {
        // Execute SQL query using DataFusion
        // This is simplified - in production, you'd properly execute the query
        Python::with_gil(|py| {
            py.allow_threads(|| {
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to create runtime: {}", e)
                    ))?;
                rt.block_on(async {
                    // Placeholder - would execute actual SQL
                    self.create_empty_dataframe()
                })
            })
        })
    }
    
    fn stop(&self) -> PyResult<()> {
        // Cleanup resources
        Ok(())
    }
}

impl SparkSession {
    fn create_dataframe_from_tuples(
        &self,
        data: Vec<PyObject>,
        column_names: Vec<String>,
        py: Python,
    ) -> PyResult<DataFrame> {
        if data.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Cannot create DataFrame from empty list"
            ));
        }
        
        // Get first tuple to determine types and column count
        let first_row = data[0].as_ref(py);
        let first_tuple: Vec<PyObject> = if let Ok(tuple) = first_row.extract::<PyObject>() {
            let tuple_ref = tuple.as_ref(py);
            if let Ok(py_tuple) = tuple_ref.downcast::<pyo3::types::PyTuple>() {
                py_tuple.iter().map(|item| item.to_object(py)).collect()
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    "Expected list of tuples"
                ));
            }
        } else if let Ok(py_tuple) = first_row.downcast::<pyo3::types::PyTuple>() {
            py_tuple.iter().map(|item| item.to_object(py)).collect()
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "Expected list of tuples"
            ));
        };
        
        if first_tuple.len() != column_names.len() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Number of columns ({}) doesn't match number of column names ({})", 
                    first_tuple.len(), column_names.len())
            ));
        }
        
            // Infer types from first row
            let mut fields: Vec<Field> = Vec::new();
            let mut builders: Vec<ColumnBuilder> = Vec::new();
            
            for (col_idx, col_name) in column_names.iter().enumerate() {
                let first_val = first_tuple[col_idx].as_ref(py);
                let (field, mut builder) = self.infer_type_and_create_builder(col_name, first_val)?;
                fields.push(field.clone());
                
                // Add first value
                self.append_value_to_builder(&mut builder, first_val)?;
                
                // Add remaining values
                for row in data.iter().skip(1) {
                    let row_ref = row.as_ref(py);
                    let tuple: Vec<PyObject> = if let Ok(py_tuple) = row_ref.downcast::<pyo3::types::PyTuple>() {
                        py_tuple.iter().map(|item| item.to_object(py)).collect()
                    } else {
                        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                            "All rows must be tuples"
                        ));
                    };
                    
                    if tuple.len() != column_names.len() {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "All rows must have the same number of elements"
                        ));
                    }
                    
                    self.append_value_to_builder(&mut builder, tuple[col_idx].as_ref(py))?;
                }
                
                builders.push(builder);
            }
            
            let arrays: Vec<Arc<dyn Array>> = builders.into_iter()
                .map(|b| self.finish_builder(b))
                .collect();
        
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create RecordBatch: {}", e)
            ))?;
        
        DataFrame::from_record_batch(batch, schema)
    }
    
    fn create_dataframe_from_list(
        &self,
        data: Vec<PyObject>,
        schema: Option<&PyAny>,
        py: Python,
    ) -> PyResult<DataFrame> {
        if data.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Cannot create DataFrame from empty list"
            ));
        }
        
        // Infer schema from first row if not provided
        let first_row = data[0].as_ref(py);
        let inferred_schema = self.infer_schema_from_row(first_row)?;
        
        // Convert data to Arrow arrays
        let fields = inferred_schema.fields();
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
        
        for field in fields {
            let array = self.create_array_from_column(&data, field, py)?;
            arrays.push(array);
        }
        
        let schema = Arc::new(inferred_schema);
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create RecordBatch: {}", e)
            ))?;
        
        DataFrame::from_record_batch(batch, schema)
    }
    
    fn create_dataframe_from_dict_list(
        &self,
        data: Vec<HashMap<String, PyObject>>,
        schema: Option<&PyAny>,
        py: Python,
    ) -> PyResult<DataFrame> {
        // Similar to list but with named columns
        if data.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Cannot create DataFrame from empty list"
            ));
        }
        
        // Get column names from first dict
        let first_dict = &data[0];
        let column_names: Vec<String> = first_dict.keys().cloned().collect();
        
        // Infer schema from first row
        let mut fields: Vec<Field> = Vec::new();
        let mut builders: Vec<ColumnBuilder> = Vec::new();
        
        for col_name in &column_names {
            let first_val = first_dict.get(col_name).unwrap().as_ref(py);
            let (field, mut builder) = self.infer_type_and_create_builder(col_name, first_val)?;
            fields.push(field.clone());
            
            // Add first value
            self.append_value_to_builder(&mut builder, first_val)?;
            
            // Add remaining values
            for row in data.iter().skip(1) {
                if let Some(val) = row.get(col_name) {
                    self.append_value_to_builder(&mut builder, val.as_ref(py))?;
                } else {
                    match &mut builder {
                        ColumnBuilder::String(b) => b.append_null(),
                        ColumnBuilder::Int32(b) => b.append_null(),
                        ColumnBuilder::Int64(b) => b.append_null(),
                        ColumnBuilder::Float64(b) => b.append_null(),
                        ColumnBuilder::Boolean(b) => b.append_null(),
                    }
                }
            }
            
            builders.push(builder);
        }
        
        let arrays: Vec<Arc<dyn Array>> = builders.into_iter()
            .map(|b| self.finish_builder(b))
            .collect();
        
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create RecordBatch: {}", e)
            ))?;
        
        DataFrame::from_record_batch(batch, schema)
    }
    
    fn infer_schema_from_row(&self, row: &PyAny) -> PyResult<arrow::datatypes::Schema> {
        // Simplified schema inference
        // In production, you'd properly infer types
        let fields = vec![
            Field::new("col1", DataType::Utf8, true),
            Field::new("col2", DataType::Utf8, true),
        ];
        Ok(arrow::datatypes::Schema::new(fields))
    }
    
    fn create_array_from_column(
        &self,
        data: &[PyObject],
        field: &Field,
        py: Python,
    ) -> PyResult<Arc<dyn Array>> {
        // Simplified - create string array
        let mut builder = StringBuilder::new();
        for item in data {
            let val = item.as_ref(py).str()?.to_string();
            builder.append_value(val);
        }
        Ok(Arc::new(builder.finish()))
    }
    
    fn infer_type_and_create_builder(
        &self,
        name: &str,
        sample: &PyAny,
    ) -> PyResult<(Field, ColumnBuilder)> {
        // Infer type from sample value
        if sample.is_instance_of::<pyo3::types::PyLong>() {
            if let Ok(val) = sample.extract::<i64>() {
                if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
                    Ok((
                        Field::new(name, DataType::Int32, true),
                        ColumnBuilder::Int32(Int32Builder::new()),
                    ))
                } else {
                    Ok((
                        Field::new(name, DataType::Int64, true),
                        ColumnBuilder::Int64(Int64Builder::new()),
                    ))
                }
            } else {
                Ok((
                    Field::new(name, DataType::Int64, true),
                    ColumnBuilder::Int64(Int64Builder::new()),
                ))
            }
        } else if sample.is_instance_of::<pyo3::types::PyFloat>() {
            Ok((
                Field::new(name, DataType::Float64, true),
                ColumnBuilder::Float64(Float64Builder::new()),
            ))
        } else if sample.is_instance_of::<pyo3::types::PyBool>() {
            Ok((
                Field::new(name, DataType::Boolean, true),
                ColumnBuilder::Boolean(BooleanBuilder::new()),
            ))
        } else {
            // Default to string
            Ok((
                Field::new(name, DataType::Utf8, true),
                ColumnBuilder::String(StringBuilder::new()),
            ))
        }
    }
    
    fn append_value_to_builder(
        &self,
        builder: &mut ColumnBuilder,
        value: &PyAny,
    ) -> PyResult<()> {
        match builder {
            ColumnBuilder::String(ref mut b) => {
                let val_str = value.str()?.to_string();
                b.append_value(val_str);
            }
            ColumnBuilder::Int32(ref mut b) => {
                if let Ok(val) = value.extract::<i32>() {
                    b.append_value(val);
                } else {
                    b.append_null();
                }
            }
            ColumnBuilder::Int64(ref mut b) => {
                if let Ok(val) = value.extract::<i64>() {
                    b.append_value(val);
                } else {
                    b.append_null();
                }
            }
            ColumnBuilder::Float64(ref mut b) => {
                if let Ok(val) = value.extract::<f64>() {
                    b.append_value(val);
                } else {
                    b.append_null();
                }
            }
            ColumnBuilder::Boolean(ref mut b) => {
                if let Ok(val) = value.extract::<bool>() {
                    b.append_value(val);
                } else {
                    b.append_null();
                }
            }
        }
        Ok(())
    }
    
    fn finish_builder(&self, mut builder: ColumnBuilder) -> Arc<dyn Array> {
        match &mut builder {
            ColumnBuilder::String(b) => Arc::new(b.finish()),
            ColumnBuilder::Int32(b) => Arc::new(b.finish()),
            ColumnBuilder::Int64(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::Boolean(b) => Arc::new(b.finish()),
        }
    }
    
    fn create_empty_dataframe(&self) -> PyResult<DataFrame> {
        // Create empty DataFrame using the internal constructor
        DataFrame::empty()
    }
}

enum ColumnBuilder {
    String(StringBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
}

#[pyclass]
pub struct DataFrameReader {
    session: Py<SparkSession>,
}

#[pymethods]
impl DataFrameReader {
    fn csv(&self, path: &str) -> PyResult<DataFrame> {
        // Read CSV file
        Python::with_gil(|py| {
            let session = self.session.borrow(py);
            // In production, use arrow-csv to read the file
            // For now, return empty DataFrame
            session.create_empty_dataframe()
        })
    }
    
        fn parquet(&self, path: &str) -> PyResult<DataFrame> {
        // Read Parquet file
        Python::with_gil(|py| {
            let session = self.session.borrow(py);
            session.create_empty_dataframe()
        })
    }
    
    fn json(&self, path: &str) -> PyResult<DataFrame> {
        // Read JSON file
        Python::with_gil(|py| {
            let session = self.session.borrow(py);
            session.create_empty_dataframe()
        })
    }
}

impl Clone for SparkSession {
    fn clone(&self) -> Self {
        SparkSession {
            app_name: self.app_name.clone(),
            master: self.master.clone(),
            config: self.config.clone(),
            execution_engine: self.execution_engine.clone(),
        }
    }
}
