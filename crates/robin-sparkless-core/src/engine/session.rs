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
        verify_schema: bool,
        schema_was_inferred: bool,
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn create_dataframe(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn sql(&self, query: &str) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    /// Register `df` under `name` in the session catalog.
    ///
    /// Returns [`EngineError::User`] if `df` is not from the same backend as `self`.
    fn register_table(&self, name: &str, df: &dyn DataFrameBackend) -> Result<(), EngineError>;
    fn is_case_sensitive(&self) -> bool;
    fn get_config(&self) -> &std::collections::HashMap<String, String>;
}

// Session builder is backend-specific (not a trait) so root uses the concrete backend's builder.
