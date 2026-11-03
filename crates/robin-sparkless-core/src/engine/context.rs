//! Immutable per-operation execution inputs shared by planning and actions.

use crate::engine::{CancellationToken, CatalogReadSnapshot};
use crate::runtime_config::SessionRuntimeConfig;
use std::sync::Arc;
use std::sync::Weak;

/// Session settings that influence expression semantics on the execution thread.
#[derive(Clone, Debug)]
pub struct ExecutionConfigSnapshot {
    runtime: SessionRuntimeConfig,
    case_sensitive: bool,
    session_time_zone: Option<String>,
    decimal_allow_precision_loss: bool,
}

impl ExecutionConfigSnapshot {
    pub fn new(
        runtime: SessionRuntimeConfig,
        case_sensitive: bool,
        session_time_zone: Option<String>,
        decimal_allow_precision_loss: bool,
    ) -> Self {
        Self {
            runtime,
            case_sensitive,
            session_time_zone,
            decimal_allow_precision_loss,
        }
    }

    pub fn runtime(&self) -> &SessionRuntimeConfig {
        &self.runtime
    }

    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    pub fn session_time_zone(&self) -> Option<&str> {
        self.session_time_zone.as_deref()
    }

    pub fn decimal_allow_precision_loss(&self) -> bool {
        self.decimal_allow_precision_loss
    }
}

/// Configuration, cancellation, and source visibility captured for one operation.
#[derive(Clone)]
pub struct ExecutionContext {
    operation_id: Arc<str>,
    configuration: Arc<ExecutionConfigSnapshot>,
    cancellation: CancellationToken,
    source_snapshot: Option<Arc<dyn CatalogReadSnapshot>>,
    owner: Option<Weak<()>>,
}

impl std::fmt::Debug for ExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContext")
            .field("configuration", &self.configuration)
            .field("operation_id", &self.operation_id)
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("has_source_snapshot", &self.source_snapshot.is_some())
            .finish()
    }
}

impl ExecutionContext {
    pub fn new(
        operation_id: impl Into<Arc<str>>,
        configuration: ExecutionConfigSnapshot,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            operation_id: operation_id.into(),
            configuration: Arc::new(configuration),
            cancellation,
            source_snapshot: None,
            owner: None,
        }
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub fn with_source_snapshot(mut self, snapshot: Arc<dyn CatalogReadSnapshot>) -> Self {
        self.source_snapshot = Some(snapshot);
        self
    }

    /// Bind this context to a logical session. Cloned contexts remain valid for clones of that
    /// session, while `newSession` contexts receive a distinct identity.
    pub fn bind_to_session(mut self, identity: &Arc<()>) -> Self {
        self.owner = Some(Arc::downgrade(identity));
        self
    }

    pub fn belongs_to_session(&self, identity: &Arc<()>) -> bool {
        self.owner
            .as_ref()
            .is_some_and(|owner| owner.ptr_eq(&Arc::downgrade(identity)))
    }

    pub fn configuration(&self) -> &ExecutionConfigSnapshot {
        &self.configuration
    }

    pub fn runtime_config(&self) -> &SessionRuntimeConfig {
        self.configuration.runtime()
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn source_snapshot(&self) -> Option<&Arc<dyn CatalogReadSnapshot>> {
        self.source_snapshot.as_ref()
    }

    pub fn check_cancelled(&self) -> Result<(), crate::EngineError> {
        self.cancellation.check_cancelled()
    }
}
