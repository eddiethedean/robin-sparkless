use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use datafusion::prelude::*;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;
use anyhow::Result;

pub struct ExecutionEngine {
    ctx: SessionContext,
}

impl ExecutionEngine {
    pub fn new() -> Self {
        ExecutionEngine {
            ctx: SessionContext::new(),
        }
    }
    
    pub async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        let results = df.collect().await?;
        Ok(results)
    }
    
    pub fn register_table(&self, name: &str, batch: RecordBatch) -> Result<()> {
        // Register as DataFrame in DataFusion context
        // This is a simplified version - in production you'd want proper table registration
        Ok(())
    }
    
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }
}

impl Default for ExecutionEngine {
    fn default() -> Self {
        Self::new()
    }
}

// Helper functions for converting between Arrow and our types
pub fn record_batch_to_python_dicts(batch: &RecordBatch) -> Vec<serde_json::Value> {
    let mut results = Vec::new();
    let num_rows = batch.num_rows();
    
    for row_idx in 0..num_rows {
        let mut row = serde_json::Map::new();
        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let value = match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::String(arr.value(row_idx).to_string())
                    }
                }
                DataType::Int32 => {
                    let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::Number(arr.value(row_idx).into())
                    }
                }
                DataType::Int64 => {
                    let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::Number(arr.value(row_idx).into())
                    }
                }
                DataType::Float64 => {
                    let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::Number(
                            serde_json::Number::from_f64(arr.value(row_idx))
                                .unwrap_or(serde_json::Number::from(0))
                        )
                    }
                }
                DataType::Boolean => {
                    let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                    if arr.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::Bool(arr.value(row_idx))
                    }
                }
                _ => serde_json::Value::Null,
            };
            row.insert(field.name().clone(), value);
        }
        results.push(serde_json::Value::Object(row));
    }
    
    results
}
