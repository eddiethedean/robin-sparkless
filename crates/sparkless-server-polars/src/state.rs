//! Server-owned identity and RAII state records.

use super::*;
use robin_sparkless_core::engine::ExternalCatalog;
#[cfg(feature = "http")]
pub(super) use sparkless_server_runtime::PendingOperationGuard;
pub(super) use sparkless_server_runtime::{AdmissionTicket, OperationRecord, ReserveOperation};

pub(super) struct ServerInner {
    pub(super) config: ServerConfig,
    pub(super) product: ServerProduct,
    pub(super) instance_id: String,
    #[cfg(feature = "http")]
    pub(super) http_address: Mutex<Option<SocketAddr>>,
    pub(super) root_session: SparkSession,
    pub(super) execution: Arc<sparkless_server_runtime::ServerRuntime<SessionLifetime>>,
    pub(super) external_catalog: Option<Arc<ExternalCatalog>>,
    #[cfg(feature = "connect")]
    pub(super) connect_runtime: sparkless_server_connect::ConnectRuntimeState,
    #[cfg(feature = "http")]
    pub(super) mutation_lane: Arc<AsyncMutex<()>>,
    #[cfg(feature = "http")]
    pub(super) catalog_receipts: Mutex<HashMap<String, CatalogReceipt>>,
    #[cfg(feature = "http")]
    pub(super) catalog_sequence: AtomicU64,
    pub(super) draining: AtomicBool,
    pub(super) ready: AtomicBool,
    #[cfg(all(test, feature = "http"))]
    pub(super) test_mutation_before_outcome_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

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

#[cfg(feature = "http")]
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

#[cfg(feature = "http")]
#[derive(Debug, Deserialize)]
pub(super) struct SessionRequest {
    pub(super) session: String,
}

#[cfg(feature = "http")]
#[derive(Debug, Deserialize)]
pub(super) struct OperationStatusRequest {
    pub(super) server_instance_id: String,
    pub(super) operation_id: String,
}

#[cfg(feature = "http")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StatementClass {
    ReadOnly,
    Mutation {
        target: Option<String>,
        action: &'static str,
    },
}

#[cfg(feature = "http")]
#[derive(Debug, Clone)]
pub(super) struct CatalogReceipt {
    pub(super) identity: String,
    pub(super) version: u64,
}

#[derive(Debug)]
pub(super) struct RequestFailure {
    pub(super) status: StatusCode,
    pub(super) name: &'static str,
    pub(super) message: String,
}
