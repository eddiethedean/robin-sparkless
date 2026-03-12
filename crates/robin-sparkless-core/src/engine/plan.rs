//! Engine-agnostic plan executor trait.

use crate::engine::DataFrameBackend;
use crate::engine::SparkSessionBackend;
use crate::error::EngineError;
use serde_json::Value as JsonValue;

/// Executes a logical plan (JSON list of ops) and returns a DataFrame.
///
/// The generic type parameter `S` is a backend-specific [`SparkSessionBackend`] implementation
/// (e.g. the Polars-backed `SparkSession` in `robin-sparkless-polars`). Backends implement this
/// trait to expose plan execution in terms of engine-agnostic traits and [`EngineError`].
pub trait PlanExecutor<S: SparkSessionBackend>: Send + Sync {
    fn execute_plan(
        session: &S,
        data: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
        plan: &[JsonValue],
    ) -> Result<Box<dyn DataFrameBackend>, EngineError>;
}
