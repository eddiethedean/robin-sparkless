//! Server-owned identity and RAII state records.

use super::*;
use robin_sparkless_core::engine::ExternalCatalog;

pub(super) struct ServerInner {
    pub(super) config: ServerConfig,
    pub(super) product: ServerProduct,
    pub(super) instance_id: String,
    pub(super) http_address: Mutex<Option<SocketAddr>>,
    pub(super) root_session: SparkSession,
    pub(super) external_catalog: Option<Arc<ExternalCatalog>>,
    pub(super) sessions: Mutex<HashMap<String, Arc<SessionLifetime>>>,
    #[cfg(feature = "connect")]
    pub(super) connect_tombstones: Mutex<HashMap<String, String>>,
    #[cfg(feature = "connect")]
    pub(super) connect_session_lifecycle: Mutex<()>,
    #[cfg(feature = "connect")]
    pub(super) connect_operations: Mutex<HashMap<String, Arc<connect::ConnectReplayRecord>>>,
    #[cfg(feature = "connect")]
    pub(super) connect_active_operations: tokio::sync::watch::Sender<usize>,
    #[cfg(feature = "connect")]
    pub(super) connect_draining: AtomicBool,
    #[cfg(feature = "connect")]
    pub(super) connect_replay_bytes: Arc<AtomicUsize>,
    #[cfg(feature = "connect")]
    pub(super) connect_history_bytes: Arc<AtomicUsize>,
    #[cfg(feature = "connect")]
    pub(super) connect_stream_senders: Arc<Semaphore>,
    #[cfg(feature = "connect")]
    pub(super) connect_address: Mutex<Option<SocketAddr>>,
    pub(super) active_sessions: AtomicUsize,
    pub(super) next_session_generation: AtomicU64,
    pub(super) operation_records: Mutex<OperationRegistry>,
    pub(super) admission: Admission,
    pub(super) connections: Arc<Semaphore>,
    pub(super) mutation_lane: Arc<AsyncMutex<()>>,
    pub(super) catalog_receipts: Mutex<HashMap<String, CatalogReceipt>>,
    pub(super) catalog_sequence: AtomicU64,
    pub(super) draining: AtomicBool,
    pub(super) ready: AtomicBool,
    #[cfg(test)]
    pub(super) test_mutation_before_outcome_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

pub(super) struct Admission {
    pub(super) active: Arc<Semaphore>,
    pub(super) accepted: Arc<AtomicUsize>,
    pub(super) max_accepted: usize,
}

pub(super) struct AdmissionTicket(pub(super) Arc<AtomicUsize>);

pub(super) struct SessionLifetime {
    pub(super) id: String,
    pub(super) session: SparkSession,
    pub(super) generation: u64,
    #[cfg(feature = "connect")]
    pub(super) server_side_session_id: String,
    #[cfg(feature = "connect")]
    pub(super) connect_execution: Arc<Semaphore>,
    pub(super) serial: Arc<tokio::sync::Mutex<()>>,
    pub(super) active: AtomicUsize,
    pub(super) last_activity: Mutex<Instant>,
    pub(super) closing: AtomicBool,
    pub(super) stopped: AtomicBool,
    pub(super) server: Weak<ServerInner>,
}

/// Settles a reserved operation if its request future is cancelled before handing execution to
/// the independent blocking worker.
pub(super) struct PendingOperationGuard(pub(super) Option<Arc<Mutex<OperationRecord>>>);

impl PendingOperationGuard {
    pub(super) fn disarm(&mut self) {
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
                "mutation request was cancelled before execution".to_string(),
            ));
        }
    }
}

pub(super) struct SessionLease(pub(super) Arc<SessionLifetime>);

#[cfg(feature = "connect")]
pub(super) struct ConnectSessionIdentity {
    pub(super) user_id: String,
    pub(super) client_session_id: String,
    pub(super) observed_server_session_id: Option<String>,
}

#[cfg(feature = "connect")]
impl ConnectSessionIdentity {
    pub(super) fn registry_key(&self) -> String {
        let mut hasher = Sha256::new();
        for part in [
            "connect",
            self.user_id.as_str(),
            self.client_session_id.as_str(),
        ] {
            hasher.update((part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        format!("connect:{}", hex_digest(hasher.finalize().into()))
    }
}

#[cfg(feature = "connect")]
pub(super) struct ConnectOperationLease {
    pub(super) record: Arc<Mutex<OperationRecord>>,
    pub(super) _session: SessionLease,
    pub(super) _admission: AdmissionTicket,
    pub(super) replay: Arc<connect::ConnectReplayRecord>,
    pub(super) _active: ConnectActiveOperation,
}

#[cfg(feature = "connect")]
pub(super) struct ConnectActiveOperation(tokio::sync::watch::Sender<usize>);

#[cfg(feature = "connect")]
impl ConnectActiveOperation {
    pub(super) fn new(active: &tokio::sync::watch::Sender<usize>) -> Self {
        active.send_modify(|count| *count += 1);
        Self(active.clone())
    }
}

#[cfg(feature = "connect")]
impl Drop for ConnectActiveOperation {
    fn drop(&mut self) {
        self.0.send_modify(|count| *count -= 1);
    }
}

#[cfg(feature = "connect")]
#[derive(Debug)]
pub(super) enum ConnectReserveError {
    Request(RequestFailure),
    Duplicate,
    Conflict,
}

#[cfg(feature = "connect")]
impl ConnectOperationLease {
    pub(super) fn deadline_marker(&self) -> impl FnOnce() + Send + 'static {
        let record = Arc::clone(&self.record);
        move || {
            if let Ok(mut record) = record.lock()
                && record.execution_state == "running"
                && record.effect_outcome == "pending"
            {
                record.execution_state = "failed";
                record.effect_outcome = "rejected";
                record.error = Some((
                    "CONNECT_RESPONSE_DEADLINE".to_string(),
                    "Connect execution exceeded its response deadline".to_string(),
                ));
            }
        }
    }

    pub(super) fn complete(&self) {
        if let Ok(mut record) = self.record.lock()
            && record.execution_state == "running"
            && record.effect_outcome == "pending"
        {
            record.execution_state = "completed";
            record.effect_outcome = "committed";
            record.error = None;
        }
    }

    pub(super) fn reject(&self, code: &'static str, message: &str) {
        if let Ok(mut record) = self.record.lock()
            && record.execution_state == "running"
            && record.effect_outcome == "pending"
        {
            record.execution_state = "failed";
            record.effect_outcome = "rejected";
            record.error = Some((code.to_string(), bound_text(message, MAX_ERROR_BYTES)));
        }
    }
}

#[cfg(feature = "connect")]
impl Drop for ConnectOperationLease {
    fn drop(&mut self) {
        self.replay.mark_producer_stopped();
        if let Ok(mut record) = self.record.lock()
            && record.execution_state == "running"
            && record.effect_outcome == "pending"
        {
            record.execution_state = "failed";
            record.effect_outcome = "cancelled";
            record.error = Some((
                "Cancelled".to_string(),
                "Connect plan request was cancelled before planning completed".to_string(),
            ));
            let _ = self.replay.finish(Err(tonic::Status::cancelled(
                "Connect plan request was cancelled before planning completed",
            )));
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct StatementRequest {
    pub(super) session: String,
    pub(super) kind: String,
    pub(super) code: String,
    #[serde(default)]
    pub(super) server_instance_id: Option<String>,
    #[serde(default)]
    pub(super) operation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SessionRequest {
    pub(super) session: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct OperationStatusRequest {
    pub(super) server_instance_id: String,
    pub(super) operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StatementClass {
    ReadOnly,
    Mutation {
        target: Option<String>,
        action: &'static str,
    },
}

#[derive(Debug, Clone)]
pub(super) struct OperationRecord {
    pub(super) request_fingerprint: [u8; 32],
    pub(super) semantic_fingerprint: Option<[u8; 32]>,
    pub(super) session: String,
    pub(super) session_generation: u64,
    pub(super) reserved_bytes: usize,
    pub(super) execution_state: &'static str,
    pub(super) effect_outcome: &'static str,
    pub(super) receipt: Option<JsonValue>,
    pub(super) error: Option<(String, String)>,
    pub(super) error_detail: Option<JsonValue>,
}

#[derive(Default)]
pub(super) struct OperationRegistry {
    pub(super) records: HashMap<String, Arc<Mutex<OperationRecord>>>,
    pub(super) bytes: usize,
}

#[derive(Debug, Clone)]
pub(super) struct CatalogReceipt {
    pub(super) identity: String,
    pub(super) version: u64,
}

pub(super) enum ReserveOperation {
    New(Arc<Mutex<OperationRecord>>),
    Existing(Arc<Mutex<OperationRecord>>),
    Conflict,
    Full,
}

#[derive(Debug)]
pub(super) struct RequestFailure {
    pub(super) status: StatusCode,
    pub(super) name: &'static str,
    pub(super) message: String,
}
