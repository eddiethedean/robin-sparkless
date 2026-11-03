//! Engine- and transport-neutral request runtime.
//!
//! Product composition injects an execution port and resource limits. This crate owns
//! admission accounting and request dispatch without importing an engine or wire protocol.

use robin_sparkless_core::engine::CancellationToken;
use sparkless_server_core::{
    ConnectExecutionPort, ConnectExecutionRequest, ConnectExecutionResult, EngineSessionHandle,
    ExecutionError, ExecutionPort, SqlExecutionRequest, SqlExecutionResult,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub type SessionEntries<T> = HashMap<String, Arc<T>>;
pub type SessionEntriesLockResult<'a, T> =
    std::sync::LockResult<std::sync::MutexGuard<'a, SessionEntries<T>>>;

pub struct ServerRuntime<S = ()> {
    execution: Arc<dyn ExecutionPort>,
    connect_execution: Option<Arc<dyn ConnectExecutionPort>>,
    connections: Arc<Semaphore>,
    admission: Admission,
    operation_records: Mutex<OperationRegistry>,
    sessions: SessionRegistry<S>,
}

impl<S> ServerRuntime<S> {
    pub fn new(
        execution: Arc<dyn ExecutionPort>,
        max_connections: usize,
        max_active_operations: usize,
        max_accepted_operations: usize,
    ) -> Self {
        Self {
            execution,
            connect_execution: None,
            connections: Arc::new(Semaphore::new(max_connections)),
            admission: Admission::new(max_active_operations, max_accepted_operations),
            operation_records: Mutex::new(OperationRegistry::default()),
            sessions: SessionRegistry::new(),
        }
    }

    pub fn admission(&self) -> &Admission {
        &self.admission
    }

    pub fn operation_registry(&self) -> &Mutex<OperationRegistry> {
        &self.operation_records
    }

    pub fn session_registry(&self) -> &SessionRegistry<S> {
        &self.sessions
    }

    pub fn with_connect_execution(mut self, execution: Arc<dyn ConnectExecutionPort>) -> Self {
        self.connect_execution = Some(execution);
        self
    }

    pub fn execute_sql(
        &self,
        session: &EngineSessionHandle,
        request: SqlExecutionRequest,
    ) -> Result<SqlExecutionResult, ExecutionError> {
        self.execution.execute_sql(session, request)
    }

    pub fn try_connection(&self) -> Option<ConnectionPermit> {
        Arc::clone(&self.connections)
            .try_acquire_owned()
            .ok()
            .map(|permit| ConnectionPermit { _permit: permit })
    }

    pub fn execute_connect_plan(
        &self,
        session: &EngineSessionHandle,
        request: ConnectExecutionRequest,
    ) -> Result<ConnectExecutionResult, ExecutionError> {
        self.connect_execution
            .as_ref()
            .ok_or_else(|| ExecutionError {
                code: "ConnectExecutionUnavailable",
                message: "the selected execution adapter does not support Connect plans".into(),
                details: None,
            })?
            .execute_connect_plan(session, request)
    }
}

pub struct Admission {
    pub active: Arc<Semaphore>,
    pub accepted: Arc<AtomicUsize>,
    pub max_accepted: usize,
}

impl Admission {
    pub fn new(max_active: usize, max_accepted: usize) -> Self {
        Self {
            active: Arc::new(Semaphore::new(max_active)),
            accepted: Arc::new(AtomicUsize::new(0)),
            max_accepted,
        }
    }

    pub fn reserve(&self) -> Result<AdmissionTicket, AdmissionError> {
        let mut current = self.accepted.load(Ordering::Acquire);
        loop {
            if current >= self.max_accepted {
                return Err(AdmissionError::CapacityExceeded);
            }
            match self.accepted.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(AdmissionTicket(Arc::clone(&self.accepted))),
                Err(updated) => current = updated,
            }
        }
    }

    pub async fn acquire_active(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> Result<ActivePermit, AdmissionError> {
        tokio::time::timeout(timeout, Arc::clone(&self.active).acquire_owned())
            .await
            .map_err(|_| AdmissionError::QueueTimeout)?
            .map(ActivePermit)
            .map_err(|_| AdmissionError::Closed)
    }

    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::Acquire)
    }
}

pub struct AdmissionTicket(pub Arc<AtomicUsize>);

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub struct ActivePermit(OwnedSemaphorePermit);

impl ActivePermit {
    pub fn forget(self) {
        self.0.forget();
    }
}

pub struct ConnectionPermit {
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    CapacityExceeded,
    QueueTimeout,
    Closed,
}

pub use Admission as AdmissionController;

#[derive(Debug, Clone)]
pub struct OperationRecord {
    pub request_fingerprint: [u8; 32],
    pub semantic_fingerprint: Option<[u8; 32]>,
    pub session: String,
    pub session_generation: u64,
    pub reserved_bytes: usize,
    pub execution_state: &'static str,
    pub effect_outcome: &'static str,
    pub receipt: Option<serde_json::Value>,
    pub error: Option<(String, String)>,
    pub error_detail: Option<serde_json::Value>,
}

#[derive(Default)]
pub struct OperationRegistry {
    pub records: HashMap<String, Arc<Mutex<OperationRecord>>>,
    pub bytes: usize,
}

/// Owns the active session map while leaving engine-specific session values in their adapter.
pub struct SessionRegistry<T> {
    entries: Mutex<SessionEntries<T>>,
    active_sessions: AtomicUsize,
    next_generation: AtomicU64,
}

impl<T> SessionRegistry<T> {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            active_sessions: AtomicUsize::new(0),
            next_generation: AtomicU64::new(1),
        }
    }

    pub fn active_count(&self) -> usize {
        self.active_sessions.load(Ordering::Acquire)
    }

    pub fn session_started(&self) {
        self.active_sessions.fetch_add(1, Ordering::AcqRel);
    }

    pub fn session_stopped(&self) {
        self.active_sessions.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn next_generation(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::Relaxed)
    }

    pub fn lock(&self) -> SessionEntriesLockResult<'_, T> {
        self.entries.lock()
    }
}

impl<T> Default for SessionRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub enum ReserveOperation {
    New(Arc<Mutex<OperationRecord>>),
    Existing(Arc<Mutex<OperationRecord>>),
    Conflict,
    Full,
}

impl OperationRegistry {
    pub fn reserve_new(
        &mut self,
        id: String,
        mut record: OperationRecord,
        estimated_bytes: usize,
        max_records: usize,
        max_bytes: usize,
    ) -> ReserveOperation {
        if self.records.len() >= max_records
            || self.bytes.saturating_add(estimated_bytes) > max_bytes
        {
            return ReserveOperation::Full;
        }
        record.reserved_bytes = estimated_bytes;
        let record = Arc::new(Mutex::new(record));
        self.bytes += estimated_bytes;
        self.records.insert(id, Arc::clone(&record));
        ReserveOperation::New(record)
    }
}

/// Settles an operation if its request future is cancelled before execution ownership transfers.
pub struct PendingOperationGuard(pub Option<Arc<Mutex<OperationRecord>>>);

impl PendingOperationGuard {
    pub fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for PendingOperationGuard {
    fn drop(&mut self) {
        let Some(operation) = self.0.take() else {
            return;
        };
        if let Ok(mut record) = operation.lock()
            && record.execution_state == "running"
            && record.effect_outcome == "pending"
        {
            record.execution_state = "failed";
            record.effect_outcome = "cancelled";
            record.error = Some((
                "Cancelled".to_string(),
                "mutation request was cancelled before execution".into(),
            ));
        }
    }
}

/// Construct a fresh cancellation token at the transport boundary.
pub fn cancellation_token() -> CancellationToken {
    CancellationToken::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation_record() -> OperationRecord {
        OperationRecord {
            request_fingerprint: [7; 32],
            semantic_fingerprint: None,
            session: "session".into(),
            session_generation: 1,
            reserved_bytes: 0,
            execution_state: "running",
            effect_outcome: "pending",
            receipt: None,
            error: None,
            error_detail: None,
        }
    }

    #[test]
    fn accepted_admission_is_released_when_ticket_drops() {
        let admission = Admission::new(1, 1);
        let ticket = admission.reserve().unwrap();
        assert_eq!(admission.accepted(), 1);
        assert!(matches!(
            admission.reserve(),
            Err(AdmissionError::CapacityExceeded)
        ));
        drop(ticket);
        assert_eq!(admission.accepted(), 0);
        assert!(admission.reserve().is_ok());
    }

    #[test]
    fn operation_registry_applies_record_and_byte_limits_atomically() {
        let mut registry = OperationRegistry::default();
        let inserted = registry.reserve_new("op-1".into(), operation_record(), 12, 1, 12);
        assert!(matches!(inserted, ReserveOperation::New(_)));
        assert_eq!(registry.bytes, 12);
        assert!(matches!(
            registry.reserve_new("op-2".into(), operation_record(), 1, 1, 100),
            ReserveOperation::Full
        ));
        assert_eq!(registry.bytes, 12);
        assert_eq!(registry.records.len(), 1);
    }

    #[test]
    fn runtime_session_registry_owns_generation_and_lifetime_accounting() {
        let registry = SessionRegistry::<u8>::new();
        assert_eq!(registry.next_generation(), 1);
        assert_eq!(registry.next_generation(), 2);
        registry.session_started();
        assert_eq!(registry.active_count(), 1);
        registry.session_stopped();
        assert_eq!(registry.active_count(), 0);
    }
}
