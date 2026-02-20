//! Engine-agnostic SparkSession backend trait.

use crate::engine::{DataFrameBackend, DataFrameReaderBackend};
use crate::error::EngineError;
use serde_json::Value as JsonValue;

/// Backend for SparkSession: creates readers, tables, and DataFrames from data.
pub trait SparkSessionBackend: Send + Sync {
    fn read(&self) -> Box<dyn DataFrameReaderBackend>;
    fn table(&self, name: &str) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn create_dataframe_from_rows(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn create_dataframe(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn sql(&self, query: &str) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn register_table(&self, name: &str, df: &dyn DataFrameBackend);
    fn is_case_sensitive(&self) -> bool;
    fn get_config(&self) -> &std::collections::HashMap<String, String>;
}

// Session builder is backend-specific (not a trait) so root uses the concrete backend's builder.
