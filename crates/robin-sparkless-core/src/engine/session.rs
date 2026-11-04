//! Session construction and catalog-registration capabilities.

use crate::engine::{
    CancellationToken, DataFrameReaderBackend, ExecutionContext, ExternalCatalog, FrameActions,
    FrameSchema, FrameTransform,
};
use crate::error::EngineError;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Backend session contract. Frame and reader values remain statically associated with the
/// selected backend; runtime erasure belongs in server execution handles.
pub trait SparkSessionBackend: Send + Sync {
    type Frame: FrameTransform + FrameActions + FrameSchema;
    type Reader: DataFrameReaderBackend<Frame = Self::Frame>;

    fn read(&self) -> Self::Reader;
    fn table(&self, name: &str) -> Result<Self::Frame, EngineError>;
    fn create_dataframe_from_rows(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
        verify_schema: bool,
        schema_was_inferred: bool,
    ) -> Result<Self::Frame, EngineError>;
    fn sql(&self, query: &str) -> Result<Self::Frame, EngineError>;

    fn sql_with_cancellation(
        &self,
        query: &str,
        token: &CancellationToken,
    ) -> Result<Self::Frame, EngineError> {
        token.check_cancelled()?;
        let dataframe = self.sql(query)?;
        token.check_cancelled()?;
        Ok(dataframe)
    }

    fn sql_with_context(
        &self,
        query: &str,
        context: &ExecutionContext,
    ) -> Result<Self::Frame, EngineError> {
        context.check_cancelled()?;
        let dataframe = self.sql_with_cancellation(query, context.cancellation())?;
        context.check_cancelled()?;
        Ok(dataframe)
    }

    fn register_table(&self, name: &str, df: &Self::Frame) -> Result<(), EngineError>;

    fn set_external_catalog(
        &self,
        _catalog: Option<Arc<ExternalCatalog>>,
    ) -> Result<(), EngineError> {
        Err(EngineError::invalid_input(
            "this SparkSession backend does not support external catalogs".into(),
        ))
    }

    fn is_case_sensitive(&self) -> bool;
    fn get_config(&self) -> &HashMap<String, String>;
}
