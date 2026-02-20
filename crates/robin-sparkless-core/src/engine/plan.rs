//! Engine-agnostic plan executor trait.

use crate::engine::DataFrameBackend;
use crate::engine::SparkSessionBackend;
use crate::error::EngineError;
use serde_json::Value as JsonValue;

/// Executes a logical plan (JSON list of ops) and returns a DataFrame.
pub trait PlanExecutor: Send + Sync {
    fn execute_plan(
        session: &dyn SparkSessionBackend,
        data: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
        plan: &[JsonValue],
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
}
