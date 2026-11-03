//! Engine-neutral execution boundary shared by server runtime and adapters.

use robin_sparkless_core::engine::{ArrowIpcBatchStream, CancellationToken};
use serde_json::Value;
use std::any::Any;
use std::sync::Arc;

/// Opaque engine-owned session state. The adapter validates the owner before downcasting.
#[derive(Clone)]
pub struct EngineSessionHandle {
    owner: &'static str,
    state: Arc<dyn Any + Send + Sync>,
}

impl EngineSessionHandle {
    pub fn new<T: Any + Send + Sync>(owner: &'static str, state: T) -> Self {
        Self {
            owner,
            state: Arc::new(state),
        }
    }

    pub fn owner(&self) -> &'static str {
        self.owner
    }

    pub fn downcast_ref<T: Any>(&self, expected_owner: &'static str) -> Option<&T> {
        (self.owner == expected_owner)
            .then(|| self.state.downcast_ref::<T>())
            .flatten()
    }
}

/// Request data needed by the supported SQL execution surface.
#[derive(Debug, Clone)]
pub struct SqlExecutionRequest {
    pub sql: String,
    pub action: String,
    pub target: Option<String>,
    pub max_rows: usize,
    pub max_result_bytes: usize,
    pub max_cell_bytes: usize,
    pub cancellation: CancellationToken,
}

/// Stable server-side failure returned by an execution adapter.
#[derive(Debug, Clone)]
pub struct ExecutionError {
    pub code: &'static str,
    pub message: String,
    pub details: Option<Value>,
}

/// A SQL result is transport-neutral structured data. HTTP and Connect own their wire encoding.
#[derive(Debug, Clone)]
pub struct SqlExecutionResult {
    pub payload: Value,
}

/// The only engine-specific call exposed to server request orchestration.
pub trait ExecutionPort: Send + Sync {
    fn execute_sql(
        &self,
        session: &EngineSessionHandle,
        request: SqlExecutionRequest,
    ) -> Result<SqlExecutionResult, ExecutionError>;
}

#[derive(Debug, Clone)]
pub struct ConnectExecutionRequest {
    pub serialized_plan: Vec<u8>,
    pub max_batch_rows: usize,
    pub max_response_bytes: usize,
    pub cancellation: CancellationToken,
}

pub struct ConnectExecutionResult {
    /// Encoded transport schema. Only the Connect transport decodes this value.
    pub schema: Vec<u8>,
    pub batches: ArrowIpcBatchStream,
}

pub trait ConnectExecutionPort: Send + Sync {
    fn execute_connect_plan(
        &self,
        session: &EngineSessionHandle,
        request: ConnectExecutionRequest,
    ) -> Result<ConnectExecutionResult, ExecutionError>;
}
