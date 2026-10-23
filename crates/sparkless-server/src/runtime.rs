use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;

use crate::config::ServerConfig;
use crate::sessions::SessionManager;

pub struct RuntimeState {
    pub config: ServerConfig,
    pub sessions: Arc<SessionManager>,
    pub admission: Arc<Semaphore>,
    pub compute: Arc<Semaphore>,
    pub http_requests: Arc<Semaphore>,
    pub health_requests: Arc<Semaphore>,
    pub retained_results: Arc<Semaphore>,
    pub connections: Arc<Semaphore>,
    pub control_connections: Arc<Semaphore>,
    pub control_connection_probes: Arc<Semaphore>,
    ready: AtomicBool,
    draining: AtomicBool,
}

impl RuntimeState {
    pub fn new(config: ServerConfig) -> Arc<Self> {
        let limits = &config.limits;
        let admission_capacity = limits.max_active_operations + limits.max_pending_operations;
        Arc::new(Self {
            sessions: Arc::new(SessionManager::new(limits)),
            admission: Arc::new(Semaphore::new(admission_capacity)),
            compute: Arc::new(Semaphore::new(limits.max_active_operations)),
            http_requests: Arc::new(Semaphore::new(limits.live_http_requests)),
            health_requests: Arc::new(Semaphore::new(4)),
            retained_results: Arc::new(Semaphore::new(limits.retained_result_envelopes)),
            connections: Arc::new(Semaphore::new(limits.live_http_requests)),
            control_connections: Arc::new(Semaphore::new(4)),
            control_connection_probes: Arc::new(Semaphore::new(limits.live_http_requests)),
            config,
            ready: AtomicBool::new(false),
            draining: AtomicBool::new(false),
        })
    }

    pub fn set_ready(&self, value: bool) {
        self.ready.store(value, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire) && !self.is_draining()
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    pub fn begin_drain(&self) {
        self.ready.store(false, Ordering::Release);
        self.draining.store(true, Ordering::Release);
        self.sessions.close_all();
    }

    pub async fn wait_for_compute_drain(&self, timeout: Duration) -> bool {
        let expected = self.config.limits.max_active_operations;
        tokio::time::timeout(timeout, async {
            loop {
                if self.compute.available_permits() == expected {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or(false)
    }

    pub fn capabilities(&self) -> serde_json::Value {
        let limits = &self.config.limits;
        serde_json::json!({
            "protocol_version": crate::protocol::HTTP_AGENT_VERSION,
            "server_version": env!("CARGO_PKG_VERSION"),
            "sql": {
                "statements": ["SELECT", "UNION ALL"],
                "expressions": ["literals", "derived columns", "CAST", "arithmetic", "comparison", "boolean", "IS NULL", "IS NOT NULL"],
                "types": ["boolean", "short", "integer", "long", "double", "string", "date", "timestamp", "decimal(1..38,0..precision)"],
                "read_only": true,
                "named_tables": false,
                "functions": false
            },
            "transports": {"http_agent": true, "spark_connect": false},
            "cancellation": {"cooperative": true, "native_compute_may_continue_after_deadline": true},
            "limits": {
                "active_operations": limits.max_active_operations,
                "pending_operations": limits.max_pending_operations,
                "pending_per_session": limits.max_pending_per_session,
                "sessions": limits.max_sessions,
                "request_bytes": limits.max_request_bytes,
                "sql_bytes": limits.max_sql_bytes,
                "rows": limits.max_rows,
                "response_bytes": limits.max_response_bytes,
                "single_cell_bytes": limits.max_cell_bytes,
                "retained_result_envelopes": limits.retained_result_envelopes,
                "response_deadline_secs": limits.response_deadline_secs,
                "queue_wait_ms": limits.queue_wait_ms
            }
        })
    }
}
