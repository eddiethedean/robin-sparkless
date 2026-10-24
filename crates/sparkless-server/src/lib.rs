//! Native HTTP statement service for Robin Sparkless.
//!
//! The HTTP adapter owns transport/session/operation lifetimes and calls the Rust engine
//! directly. It does not expose the engine's file readers, UDF registry, Python bindings,
//! or Spark Connect transport.

mod config;

pub use config::ServerConfig;
use config::{MIN_MUTATION_RECORD_BYTES, MUTATION_RECORD_BASE_BYTES};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{CONTENT_ENCODING, CONTENT_TYPE};
use hyper::http::{HeaderValue, Method, Request, Response, StatusCode};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use robin_sparkless::{DataType, EngineError, SharedCatalog, SparkSession, StructField};
use robin_sparkless_core::CancellationToken;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use spark_sql_parser::{SparkStatement, ast};
use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedMutexGuard, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;
use uuid::Uuid;

pub const CONTROL_PROTOCOL_VERSION: u32 = 1;
pub const AGENT_PROTOCOL_VERSION: &str = "1";
pub const MUTATION_PROTOCOL_VERSION: &str = "1";
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_ERROR_BYTES: usize = 1024;
const MAX_CATALOG_DATABASES: usize = 128;
const MAX_CATALOG_TABLES: usize = 1024;
const MAX_SESSION_TEMP_VIEWS: usize = 128;
const MAX_TABLE_ACCOUNTED_BYTES: usize = 64 * 1024 * 1024;
const MAX_CATALOG_ACCOUNTED_BYTES: usize = 256 * 1024 * 1024;

type HttpResponse = Response<Full<Bytes>>;

#[derive(Debug, Clone)]
pub struct ServerError {
    message: String,
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ServerError {}

impl From<std::io::Error> for ServerError {
    fn from(value: std::io::Error) -> Self {
        Self {
            message: value.to_string(),
        }
    }
}

/// An HTTP service instance. Each instance owns its catalog, instance identity, session
/// registry, and process-lifetime mutation records.
#[derive(Clone)]
pub struct Server {
    inner: Arc<ServerInner>,
}

struct ServerInner {
    config: ServerConfig,
    instance_id: String,
    root_session: SparkSession,
    sessions: Mutex<HashMap<String, Arc<SessionLifetime>>>,
    active_sessions: AtomicUsize,
    next_session_generation: AtomicU64,
    operation_records: Mutex<OperationRegistry>,
    admission: Admission,
    connections: Arc<Semaphore>,
    mutation_lane: Arc<Mutex<()>>,
    catalog_receipts: Mutex<HashMap<String, CatalogReceipt>>,
    catalog_sequence: AtomicU64,
    draining: AtomicBool,
    ready: AtomicBool,
}

struct Admission {
    active: Arc<Semaphore>,
    accepted: Arc<AtomicUsize>,
    max_accepted: usize,
}

struct AdmissionTicket(Arc<AtomicUsize>);

struct SessionLifetime {
    session: SparkSession,
    generation: u64,
    serial: Arc<tokio::sync::Mutex<()>>,
    active: AtomicUsize,
    closing: AtomicBool,
    stopped: AtomicBool,
    server: Arc<ServerInner>,
}

struct SessionLease(Arc<SessionLifetime>);

#[derive(Debug, Clone, Deserialize)]
struct StatementRequest {
    session: String,
    kind: String,
    code: String,
    #[serde(default)]
    server_instance_id: Option<String>,
    #[serde(default)]
    operation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SessionRequest {
    session: String,
}

#[derive(Debug, Deserialize)]
struct OperationStatusRequest {
    server_instance_id: String,
    operation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StatementClass {
    ReadOnly,
    Mutation {
        target: Option<String>,
        action: &'static str,
    },
}

#[derive(Debug, Clone)]
struct OperationRecord {
    fingerprint: [u8; 32],
    session: String,
    session_generation: u64,
    execution_state: &'static str,
    effect_outcome: &'static str,
    receipt: Option<JsonValue>,
    error: Option<(String, String)>,
}

#[derive(Default)]
struct OperationRegistry {
    records: HashMap<String, Arc<Mutex<OperationRecord>>>,
    bytes: usize,
}

#[derive(Debug, Clone)]
struct CatalogReceipt {
    identity: String,
    version: u64,
}

enum ReserveOperation {
    New(Arc<Mutex<OperationRecord>>),
    Existing(Arc<Mutex<OperationRecord>>),
    Conflict,
    Full,
}

#[derive(Debug)]
struct RequestFailure {
    status: StatusCode,
    name: &'static str,
    message: String,
}

impl Server {
    pub fn new(config: ServerConfig) -> Result<Self, ServerError> {
        let limits_valid = config.max_request_bytes > 0
            && config.max_sql_bytes > 0
            && config.max_sql_bytes <= config.max_request_bytes
            && config.max_result_rows > 0
            && config.max_result_bytes > 0
            && config.max_cell_bytes > 0
            && config.max_cell_bytes <= config.max_result_bytes
            && config.max_active_operations > 0
            && config.max_sessions > 0
            && config.max_mutation_records > 0
            && config.max_mutation_bytes
                >= config
                    .max_mutation_records
                    .saturating_mul(MIN_MUTATION_RECORD_BYTES)
            && !config.body_read_timeout.is_zero()
            && !config.queue_timeout.is_zero()
            && !config.response_timeout.is_zero();
        if !limits_valid {
            return Err(ServerError {
                message: "server limits are invalid or contradictory".into(),
            });
        }
        let catalog = SharedCatalog::new();
        let root_session = SparkSession::builder()
            .app_name("sparkless-server")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.caseSensitive", "false")
            .config(
                "sparkless.server.max_databases",
                MAX_CATALOG_DATABASES.to_string(),
            )
            .config(
                "sparkless.server.max_tables",
                MAX_CATALOG_TABLES.to_string(),
            )
            .config(
                "sparkless.server.max_temp_views",
                MAX_SESSION_TEMP_VIEWS.to_string(),
            )
            .config(
                "sparkless.server.max_table_bytes",
                MAX_TABLE_ACCOUNTED_BYTES.to_string(),
            )
            .config(
                "sparkless.server.max_catalog_bytes",
                MAX_CATALOG_ACCOUNTED_BYTES.to_string(),
            )
            .shared_catalog(catalog)
            .get_or_create();
        root_session.register_database("default");
        let max_accepted = config
            .max_active_operations
            .saturating_add(config.max_pending_operations);
        let inner = Arc::new(ServerInner {
            operation_records: Mutex::new(OperationRegistry::default()),
            admission: Admission {
                active: Arc::new(Semaphore::new(config.max_active_operations)),
                accepted: Arc::new(AtomicUsize::new(0)),
                max_accepted,
            },
            connections: Arc::new(Semaphore::new(128)),
            config,
            instance_id: Uuid::new_v4().to_string(),
            root_session,
            sessions: Mutex::new(HashMap::new()),
            active_sessions: AtomicUsize::new(0),
            next_session_generation: AtomicU64::new(1),
            mutation_lane: Arc::new(Mutex::new(())),
            catalog_receipts: Mutex::new(HashMap::new()),
            catalog_sequence: AtomicU64::new(1),
            draining: AtomicBool::new(false),
            ready: AtomicBool::new(false),
        });
        Ok(Self { inner })
    }

    pub fn instance_id(&self) -> &str {
        &self.inner.instance_id
    }

    pub fn local_addr_configured(&self) -> SocketAddr {
        self.inner.config.bind
    }

    /// Bind, run a native startup witness, then serve until `shutdown` resolves.
    pub async fn serve_until<F>(self, shutdown: F) -> Result<(), ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.serve_until_with_ready(shutdown, |_| Ok(())).await
    }

    /// Bind, run a native startup witness, then invoke `on_ready` with the actual address before
    /// serving until `shutdown` resolves. Used by the owned-child control protocol so readiness
    /// is tied to this listener rather than an unrelated process on the configured port.
    pub async fn serve_until_with_ready<F, R>(
        self,
        shutdown: F,
        on_ready: R,
    ) -> Result<(), ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
        R: FnOnce(SocketAddr) -> Result<(), ServerError>,
    {
        let listener = TcpListener::bind(self.inner.config.bind).await?;
        let bound = listener.local_addr()?;
        self.startup_check().await?;
        self.inner.ready.store(true, Ordering::Release);
        eprintln!(
            "sparkless-server ready address={bound} instance_id={}",
            self.inner.instance_id
        );
        if let Err(error) = on_ready(bound) {
            self.inner.ready.store(false, Ordering::Release);
            return Err(error);
        }
        self.run_listener(listener, shutdown).await
    }

    /// Control-protocol readiness record for an owned launcher process.
    pub fn readiness_record(&self, bound: SocketAddr) -> JsonValue {
        json!({
            "type": "ready",
            "control_protocol_version": CONTROL_PROTOCOL_VERSION,
            "server_version": env!("CARGO_PKG_VERSION"),
            "agent_protocol_version": AGENT_PROTOCOL_VERSION,
            "server_instance_id": self.inner.instance_id,
            "http_url": format!("http://{bound}"),
            "capabilities": self.capabilities_json()
        })
    }

    pub async fn serve(self) -> Result<(), ServerError> {
        self.serve_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }

    async fn startup_check(&self) -> Result<(), ServerError> {
        let session = self.inner.root_session.new_session();
        let token = CancellationToken::new();
        let result = tokio::task::spawn_blocking(move || {
            let df = session.sql_with_cancellation("SELECT 1 AS ready", &token)?;
            let (names, rows, _) =
                df.collect_as_positional_json_with_cancellation(1, &Default::default(), &token)?;
            if names != ["ready"] || rows != [vec![json!(1)]] {
                return Err(EngineError::Internal(
                    "native startup witness mismatch".into(),
                ));
            }
            Ok::<(), EngineError>(())
        })
        .await
        .map_err(|e| ServerError {
            message: format!("native startup task failed: {e}"),
        })?;
        result.map_err(|e| ServerError {
            message: format!("native startup check failed: {e}"),
        })
    }

    async fn run_listener<F>(&self, listener: TcpListener, shutdown: F) -> Result<(), ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::pin!(shutdown);
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    self.inner.draining.store(true, Ordering::Release);
                    self.inner.ready.store(false, Ordering::Release);
                    break;
                }
                accept = listener.accept() => {
                    let (stream, _) = accept?;
                    let Ok(connection_permit) = Arc::clone(&self.inner.connections).try_acquire_owned() else {
                        drop(stream);
                        continue;
                    };
                    let server = self.clone();
                    connections.spawn(async move {
                        let _connection_permit = connection_permit;
                        let _ = serve_connection(stream, server).await;
                    });
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
        let drain = async { while connections.join_next().await.is_some() {} };
        if timeout(Duration::from_secs(10), drain).await.is_err() {
            connections.abort_all();
        }
        Ok(())
    }
}

impl ServerInner {
    fn session_lease(self: &Arc<Self>, id: &str) -> Result<SessionLease, RequestFailure> {
        validate_identifier("session", id)?;
        let mut sessions = self.sessions.lock().map_err(|_| internal_failure())?;
        if let Some(session) = sessions.get(id) {
            if session.closing.load(Ordering::Acquire) {
                return Err(RequestFailure {
                    status: StatusCode::CONFLICT,
                    name: "SessionClosing",
                    message: "session is closing".into(),
                });
            }
            session.active.fetch_add(1, Ordering::AcqRel);
            return Ok(SessionLease(Arc::clone(session)));
        }
        if self.active_sessions.load(Ordering::Acquire) >= self.config.max_sessions {
            return Err(RequestFailure {
                status: StatusCode::TOO_MANY_REQUESTS,
                name: "SessionCapacityExceeded",
                message: "server session capacity is full".into(),
            });
        }
        let generation = self.next_session_generation.fetch_add(1, Ordering::Relaxed);
        let lifetime = Arc::new(SessionLifetime {
            session: self.root_session.new_session(),
            generation,
            serial: Arc::new(tokio::sync::Mutex::new(())),
            active: AtomicUsize::new(1),
            closing: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            server: Arc::clone(self),
        });
        self.active_sessions.fetch_add(1, Ordering::AcqRel);
        sessions.insert(id.to_string(), Arc::clone(&lifetime));
        Ok(SessionLease(lifetime))
    }

    fn close_session(&self, id: &str) -> Result<(), RequestFailure> {
        validate_identifier("session", id)?;
        let session = self
            .sessions
            .lock()
            .map_err(|_| internal_failure())?
            .remove(id);
        if let Some(session) = session {
            session.closing.store(true, Ordering::Release);
            if session.active.load(Ordering::Acquire) == 0 {
                session.stop_once();
            }
        }
        Ok(())
    }

    fn reserve_admission(&self) -> Result<AdmissionTicket, RequestFailure> {
        let accepted = &self.admission.accepted;
        let mut current = accepted.load(Ordering::Acquire);
        loop {
            if current >= self.admission.max_accepted {
                return Err(RequestFailure {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    name: "OperationCapacityExceeded",
                    message: "active and pending operation capacity is full".into(),
                });
            }
            match accepted.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(AdmissionTicket(Arc::clone(accepted))),
                Err(updated) => current = updated,
            }
        }
    }

    fn reserve_operation(
        &self,
        id: &str,
        fingerprint: [u8; 32],
        session: &str,
        generation: u64,
    ) -> ReserveOperation {
        let Ok(mut registry) = self.operation_records.lock() else {
            return ReserveOperation::Full;
        };
        if let Some(existing) = registry.records.get(id) {
            let Ok(record) = existing.lock() else {
                return ReserveOperation::Conflict;
            };
            let current_generation = self
                .sessions
                .lock()
                .ok()
                .and_then(|sessions| sessions.get(session).map(|entry| entry.generation));
            if record.fingerprint != fingerprint
                || record.session != session
                || current_generation.is_some_and(|value| value != record.session_generation)
            {
                return ReserveOperation::Conflict;
            }
            drop(record);
            return ReserveOperation::Existing(Arc::clone(existing));
        }
        let estimated_record_bytes = MUTATION_RECORD_BASE_BYTES
            .saturating_add(id.len())
            .saturating_add(session.len());
        if registry.records.len() >= self.config.max_mutation_records
            || registry.bytes.saturating_add(estimated_record_bytes)
                > self.config.max_mutation_bytes
        {
            return ReserveOperation::Full;
        }
        let operation = Arc::new(Mutex::new(OperationRecord {
            fingerprint,
            session: session.to_string(),
            session_generation: generation,
            execution_state: "running",
            effect_outcome: "pending",
            receipt: None,
            error: None,
        }));
        registry.bytes += estimated_record_bytes;
        registry
            .records
            .insert(id.to_string(), Arc::clone(&operation));
        ReserveOperation::New(operation)
    }

    fn operation(&self, id: &str) -> Option<Arc<Mutex<OperationRecord>>> {
        self.operation_records.lock().ok()?.records.get(id).cloned()
    }

    fn lookup_operation(
        &self,
        id: &str,
        fingerprint: [u8; 32],
        session: &str,
    ) -> Result<Option<Arc<Mutex<OperationRecord>>>, ()> {
        let registry = self.operation_records.lock().map_err(|_| ())?;
        let Some(existing) = registry.records.get(id) else {
            return Ok(None);
        };
        let record = existing.lock().map_err(|_| ())?;
        let current_generation = self
            .sessions
            .lock()
            .map_err(|_| ())?
            .get(session)
            .map(|entry| entry.generation);
        if record.fingerprint != fingerprint
            || record.session != session
            || current_generation.is_some_and(|value| value != record.session_generation)
        {
            return Err(());
        }
        drop(record);
        Ok(Some(Arc::clone(existing)))
    }

    fn operation_json(&self, id: &str, record: &OperationRecord) -> JsonValue {
        operation_json_from_record(&self.instance_id, id, record)
    }
}

impl SessionLifetime {
    fn stop_once(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            self.session.stop();
            self.server.active_sessions.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        let session = &self.0;
        if session.active.fetch_sub(1, Ordering::AcqRel) == 1
            && session.closing.load(Ordering::Acquire)
        {
            session.stop_once();
        }
    }
}

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn serve_connection(stream: TcpStream, server: Server) -> Result<(), hyper::Error> {
    let service = service_fn(move |request| {
        let server = server.clone();
        async move { Ok::<_, Infallible>(server.handle(request).await) }
    });
    http1::Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .await
}

impl Server {
    async fn handle(&self, request: Request<Incoming>) -> HttpResponse {
        match (request.method(), request.uri().path()) {
            (&Method::GET, "/health") => {
                if self.inner.ready.load(Ordering::Acquire)
                    && !self.inner.draining.load(Ordering::Acquire)
                {
                    json_response(StatusCode::OK, json!({"status":"ready"}))
                } else {
                    json_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        json!({"status":"unavailable"}),
                    )
                }
            }
            (&Method::GET, "/capabilities") => self.capabilities(),
            (&Method::POST, "/statements") => self.handle_statements(request).await,
            (&Method::POST, "/close") => self.handle_close(request).await,
            (&Method::POST, "/operations/status") => self.handle_operation_status(request).await,
            _ => transport_error(StatusCode::NOT_FOUND, "NotFound", "route not found"),
        }
    }

    fn capabilities(&self) -> HttpResponse {
        json_response(StatusCode::OK, self.capabilities_json())
    }

    fn capabilities_json(&self) -> JsonValue {
        json!({
            "agent_protocol_version": AGENT_PROTOCOL_VERSION,
            "mutation_protocol_version": MUTATION_PROTOCOL_VERSION,
            "server_instance_id": self.inner.instance_id,
            "transport": "http",
            "spark_connect_supported": false,
            "sql": {
                "reads": ["SELECT", "COUNT(*)", "managed_tables"],
                "mutations": ["CREATE DATABASE", "CREATE TABLE", "INSERT INTO", "DROP TABLE", "DROP DATABASE", "CREATE TEMP VIEW", "DROP VIEW", "USE"]
            },
            "limits": {
                "request_bytes": self.inner.config.max_request_bytes,
                "sql_bytes": self.inner.config.max_sql_bytes,
                "result_rows": self.inner.config.max_result_rows,
                "result_bytes": self.inner.config.max_result_bytes,
                "cell_bytes": self.inner.config.max_cell_bytes,
                "active_operations": self.inner.config.max_active_operations,
                "pending_operations": self.inner.config.max_pending_operations,
                "sessions": self.inner.config.max_sessions,
                "catalog_databases": MAX_CATALOG_DATABASES,
                "catalog_tables": MAX_CATALOG_TABLES,
                "session_temp_views": MAX_SESSION_TEMP_VIEWS,
                "table_accounted_bytes": MAX_TABLE_ACCOUNTED_BYTES,
                "catalog_accounted_bytes": MAX_CATALOG_ACCOUNTED_BYTES,
                "retained_mutations": self.inner.config.max_mutation_records,
                "retained_mutation_bytes": self.inner.config.max_mutation_bytes,
                "body_read_timeout_ms": self.inner.config.body_read_timeout.as_millis(),
                "queue_timeout_ms": self.inner.config.queue_timeout.as_millis(),
                "response_timeout_ms": self.inner.config.response_timeout.as_millis()
            },
            "cancellation": "cooperative; native work retains admission until it returns",
            "catalog_lifetime": "in-memory for this server instance",
            "catalog_byte_accounting": "Polars estimated_size for committed saved tables, materialized session-local view snapshots, staged candidates, and saved/view versions pinned by active statement snapshots; shared buffers can be counted conservatively more than once, and this is not an RSS limit"
        })
    }

    async fn handle_statements(&self, request: Request<Incoming>) -> HttpResponse {
        if self.inner.draining.load(Ordering::Acquire) {
            return transport_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "ServerDraining",
                "server is draining",
            );
        }
        if let Err(error) = check_json_request_headers(request.headers()) {
            return request_failure_response(error);
        }
        let bytes = match read_bounded_body_with_timeout(
            request.into_body(),
            self.inner.config.max_request_bytes,
            self.inner.config.body_read_timeout,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(error) => return request_failure_response(error),
        };
        let parsed: StatementRequest = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => {
                return transport_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    &format!("invalid JSON request: {error}"),
                );
            }
        };
        if parsed.code.len() > self.inner.config.max_sql_bytes {
            return transport_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "SqlTooLarge",
                "SQL text exceeds the configured limit",
            );
        }
        if parsed.kind != "sql" {
            return agent_error("UnsupportedKind", "only kind=sql is supported", None);
        }
        let class = match validate_statement(&parsed.code) {
            Ok(class) => class,
            Err((name, message)) => return agent_error(name, &message, None),
        };
        let is_mutation = matches!(class, StatementClass::Mutation { .. });
        let operation_id = if is_mutation {
            let Some(instance_id) = parsed.server_instance_id.as_deref() else {
                return agent_error(
                    "OperationIdentityRequired",
                    "mutations require server_instance_id",
                    None,
                );
            };
            let Some(operation_id) = parsed.operation_id.as_deref() else {
                return agent_error(
                    "OperationIdentityRequired",
                    "mutations require operation_id",
                    None,
                );
            };
            if validate_identifier("operation_id", operation_id).is_err() {
                return agent_error(
                    "InvalidOperationIdentity",
                    "operation_id must be 1-128 safe ASCII bytes",
                    None,
                );
            }
            if instance_id != self.inner.instance_id {
                return transport_error(
                    StatusCode::CONFLICT,
                    "ServerInstanceMismatch",
                    "operation belongs to a different server instance",
                );
            }
            Some(operation_id.to_string())
        } else {
            None
        };
        let mutation_fingerprint = operation_id
            .as_deref()
            .map(|_| request_fingerprint(&parsed));
        if let (Some(operation_id), Some(fingerprint)) =
            (operation_id.as_deref(), mutation_fingerprint)
        {
            match self
                .inner
                .lookup_operation(operation_id, fingerprint, &parsed.session)
            {
                Ok(Some(existing)) => {
                    return existing_operation_response(&self.inner, operation_id, &existing);
                }
                Err(()) => {
                    return agent_error(
                        "OperationIdentityConflict",
                        "operation identity is already bound to different request content or a different session generation",
                        None,
                    );
                }
                Ok(None) => {}
            }
        }
        let lease = match self.inner.session_lease(&parsed.session) {
            Ok(lease) => lease,
            Err(error) => return request_failure_response(error),
        };
        let operation = if let Some(operation_id) = operation_id.as_deref() {
            let fingerprint = mutation_fingerprint.unwrap_or_else(|| request_fingerprint(&parsed));
            match self.inner.reserve_operation(
                operation_id,
                fingerprint,
                &parsed.session,
                lease.0.generation,
            ) {
                ReserveOperation::New(record) => Some(record),
                ReserveOperation::Existing(record) => {
                    let response = existing_operation_response(&self.inner, operation_id, &record);
                    return response;
                }
                ReserveOperation::Conflict => {
                    return agent_error(
                        "OperationIdentityConflict",
                        "operation identity is already bound to different request content or a different session generation",
                        None,
                    );
                }
                ReserveOperation::Full => {
                    return transport_error(
                        StatusCode::TOO_MANY_REQUESTS,
                        "MutationRetentionCapacityExceeded",
                        "mutation outcome retention capacity is full",
                    );
                }
            }
        } else {
            None
        };
        let ticket = match self.inner.reserve_admission() {
            Ok(ticket) => ticket,
            Err(error) => {
                if let Some(record) = &operation {
                    set_operation_failure(
                        record,
                        "OperationCapacityExceeded",
                        "operation capacity is full",
                        "failed",
                    );
                }
                return request_failure_response(error);
            }
        };
        let session_guard = match timeout(
            self.inner.config.queue_timeout,
            lease.0.serial.clone().lock_owned(),
        )
        .await
        {
            Ok(guard) => guard,
            Err(_) => {
                drop(ticket);
                if let Some(record) = &operation {
                    set_operation_failure(
                        record,
                        "QueueTimeout",
                        "timed out waiting for session statement slot",
                        "failed",
                    );
                }
                return agent_error(
                    "QueueTimeout",
                    "timed out waiting for session statement slot",
                    operation_id
                        .as_deref()
                        .zip(operation.as_ref())
                        .map(|(id, r)| self.operation_json_by_record(id, r)),
                );
            }
        };
        let permit = match timeout(
            self.inner.config.queue_timeout,
            Arc::clone(&self.inner.admission.active).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            _ => {
                drop(session_guard);
                drop(ticket);
                if let Some(record) = &operation {
                    set_operation_failure(
                        record,
                        "QueueTimeout",
                        "timed out waiting for compute admission",
                        "failed",
                    );
                }
                return agent_error(
                    "QueueTimeout",
                    "timed out waiting for compute admission",
                    operation_id
                        .as_deref()
                        .zip(operation.as_ref())
                        .map(|(id, r)| self.operation_json_by_record(id, r)),
                );
            }
        };
        let token = CancellationToken::new();
        let timeout_duration = self.inner.config.response_timeout;
        let session = lease.0.session.clone();
        let class_for_job = class.clone();
        let code = parsed.code.clone();
        let instance = self.inner.clone();
        let limits = ExecutionLimits {
            max_rows: self.inner.config.max_result_rows,
            max_result_bytes: self.inner.config.max_result_bytes,
            max_cell_bytes: self.inner.config.max_cell_bytes,
        };
        let operation_record_for_job = operation.clone();
        let operation_key_for_job = operation_id.clone();
        let token_for_work = token.clone();
        let work = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _ticket = ticket;
            let _session_guard: OwnedMutexGuard<()> = session_guard;
            let _session_lease = lease;
            let result = if let StatementClass::Mutation { action, target } = &class_for_job {
                let _mutation_guard = instance.mutation_lane.lock().map_err(|_| {
                    execution_failure("InternalError", "catalog mutation lane is poisoned")
                })?;
                execute_statement(
                    &session,
                    &code,
                    action,
                    target.as_deref(),
                    limits,
                    &token_for_work,
                )
                .map(|response| {
                    let receipt =
                        record_catalog_mutation(&instance, target.as_deref(), action, &session);
                    (response, receipt)
                })
            } else {
                execute_statement(&session, &code, "read", None, limits, &token_for_work)
                    .map(|response| (response, None))
            };
            if let Some(record) = &operation_record_for_job {
                match &result {
                    Ok((_, receipt)) => set_operation_committed(record, receipt.clone()),
                    Err(error) => set_operation_failure(
                        record,
                        error.name,
                        &error.message,
                        if error.name == "Cancelled" {
                            "cancelled"
                        } else {
                            "failed"
                        },
                    ),
                }
            }
            result.map(|(mut response, receipt)| {
                if let Some(record) = &operation_record_for_job
                    && let Ok(record) = record.lock()
                {
                    let operation = operation_json_from_record(
                        &instance.instance_id,
                        operation_key_for_job.as_deref().unwrap_or_default(),
                        &record,
                    );
                    if let Some(object) = response.as_object_mut() {
                        object.insert("operation".into(), operation);
                        if let Some(receipt) = receipt {
                            object.insert("receipt".into(), receipt);
                        }
                    }
                }
                response
            })
        });
        match timeout(timeout_duration, work).await {
            Ok(Ok(Ok(response))) => json_response(StatusCode::OK, response),
            Ok(Ok(Err(error))) => agent_error(
                error.name,
                &error.message,
                operation_id
                    .as_deref()
                    .zip(operation.as_ref())
                    .map(|(id, r)| self.operation_json_by_record(id, r)),
            ),
            Ok(Err(error)) => {
                if let Some(record) = &operation {
                    set_operation_failure(
                        record,
                        "InternalError",
                        "native execution task failed",
                        "unknown",
                    );
                }
                agent_error(
                    "InternalError",
                    &format!("native execution task failed: {error}"),
                    operation_id
                        .as_deref()
                        .zip(operation.as_ref())
                        .map(|(id, r)| self.operation_json_by_record(id, r)),
                )
            }
            Err(_) => {
                token.cancel();
                // spawn_blocking cannot be aborted. The operation task retains its permits and
                // session reference and will publish the actual outcome when native work ends.
                let extra = operation_id
                    .as_deref()
                    .zip(operation.as_ref())
                    .map(|(id, record)| {
                        if let Ok(mut record) = record.lock() {
                            record.execution_state = "running";
                            record.effect_outcome = "pending";
                            operation_json_from_record(&self.inner.instance_id, id, &record)
                        } else {
                            JsonValue::Null
                        }
                    });
                agent_error(
                    "DeadlineExceeded",
                    "response deadline elapsed; query operation status to establish the final outcome",
                    extra,
                )
            }
        }
    }

    fn operation_json_by_record(
        &self,
        id: &str,
        operation: &Arc<Mutex<OperationRecord>>,
    ) -> JsonValue {
        operation
            .lock()
            .map(|record| operation_json_from_record(&self.inner.instance_id, id, &record))
            .unwrap_or(JsonValue::Null)
    }

    async fn handle_close(&self, request: Request<Incoming>) -> HttpResponse {
        if let Err(error) = check_json_request_headers(request.headers()) {
            return request_failure_response(error);
        }
        let bytes = match read_bounded_body_with_timeout(
            request.into_body(),
            16 * 1024,
            self.inner.config.body_read_timeout,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(error) => return request_failure_response(error),
        };
        let request: SessionRequest = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => {
                return transport_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    &format!("invalid JSON request: {error}"),
                );
            }
        };
        match self.inner.close_session(&request.session) {
            Ok(()) => json_response(StatusCode::OK, json!({"status":"ok"})),
            Err(error) => request_failure_response(error),
        }
    }

    async fn handle_operation_status(&self, request: Request<Incoming>) -> HttpResponse {
        if let Err(error) = check_json_request_headers(request.headers()) {
            return request_failure_response(error);
        }
        let bytes = match read_bounded_body_with_timeout(
            request.into_body(),
            16 * 1024,
            self.inner.config.body_read_timeout,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(error) => return request_failure_response(error),
        };
        let request: OperationStatusRequest = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => {
                return transport_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    &format!("invalid JSON request: {error}"),
                );
            }
        };
        if request.server_instance_id != self.inner.instance_id {
            return transport_error(
                StatusCode::CONFLICT,
                "ServerInstanceMismatch",
                "operation belongs to a different server instance",
            );
        }
        if validate_identifier("operation_id", &request.operation_id).is_err() {
            return transport_error(
                StatusCode::BAD_REQUEST,
                "InvalidOperationIdentity",
                "operation_id must be 1-128 safe ASCII bytes",
            );
        }
        let Some(operation) = self.inner.operation(&request.operation_id) else {
            return transport_error(
                StatusCode::NOT_FOUND,
                "UnknownOperation",
                "operation identity has not been accepted by this server instance",
            );
        };
        let body = operation
            .lock()
            .map(|record| self.inner.operation_json(&request.operation_id, &record))
            .unwrap_or_else(|_| json!({"error":"InternalError"}));
        json_response(StatusCode::OK, body)
    }
}

#[derive(Debug)]
struct ExecutionFailure {
    name: &'static str,
    message: String,
}

#[derive(Clone, Copy)]
struct ExecutionLimits {
    max_rows: usize,
    max_result_bytes: usize,
    max_cell_bytes: usize,
}

fn validate_statement(sql: &str) -> Result<StatementClass, (&'static str, String)> {
    let parsed = spark_sql_parser::parse_spark_sql(sql)
        .map_err(|error| ("UnsupportedSql", error.to_string()))?;
    let class = match &parsed {
        SparkStatement::Sqlparser(statement) => match statement.as_ref() {
            ast::Statement::Query(query) => {
                validate_query(query)?;
                StatementClass::ReadOnly
            }
            ast::Statement::CreateTable(create) => {
                validate_create_table(create)?;
                StatementClass::Mutation {
                    target: Some(validate_object_name(&create.name)?),
                    action: "create_table",
                }
            }
            ast::Statement::Insert(insert) => {
                validate_insert(insert)?;
                let target = match &insert.table {
                    ast::TableObject::TableName(name) => validate_object_name(name)?,
                    _ => {
                        return Err((
                            "UnsupportedSql",
                            "INSERT table functions are not supported".into(),
                        ));
                    }
                };
                StatementClass::Mutation {
                    target: Some(target),
                    action: "append",
                }
            }
            ast::Statement::CreateSchema {
                schema_name,
                with,
                options,
                default_collate_spec,
                clone,
                ..
            } => {
                if with.is_some()
                    || options.is_some()
                    || default_collate_spec.is_some()
                    || clone.is_some()
                {
                    return Err((
                        "UnsupportedSql",
                        "database options, collation, and cloning are not supported".into(),
                    ));
                }
                let schema_name = match schema_name {
                    ast::SchemaName::Simple(name) => name,
                    _ => {
                        return Err((
                            "UnsupportedSql",
                            "database authorization clauses are not supported".into(),
                        ));
                    }
                };
                StatementClass::Mutation {
                    target: Some(validate_single_object_name(schema_name)?),
                    action: "create_database",
                }
            }
            ast::Statement::CreateDatabase {
                db_name,
                location,
                managed_location,
                or_replace,
                transient,
                clone,
                data_retention_time_in_days,
                max_data_extension_time_in_days,
                external_volume,
                catalog,
                replace_invalid_characters,
                default_ddl_collation,
                storage_serialization_policy,
                comment,
                default_charset,
                default_collation,
                catalog_sync,
                catalog_sync_namespace_mode,
                catalog_sync_namespace_flatten_delimiter,
                with_tags,
                with_contacts,
                ..
            } => {
                if location.is_some()
                    || managed_location.is_some()
                    || *or_replace
                    || *transient
                    || clone.is_some()
                    || data_retention_time_in_days.is_some()
                    || max_data_extension_time_in_days.is_some()
                    || external_volume.is_some()
                    || catalog.is_some()
                    || replace_invalid_characters.is_some()
                    || default_ddl_collation.is_some()
                    || storage_serialization_policy.is_some()
                    || comment.is_some()
                    || default_charset.is_some()
                    || default_collation.is_some()
                    || catalog_sync.is_some()
                    || catalog_sync_namespace_mode.is_some()
                    || catalog_sync_namespace_flatten_delimiter.is_some()
                    || with_tags.is_some()
                    || with_contacts.is_some()
                {
                    return Err((
                        "UnsupportedSql",
                        "database locations, options, comments, and owners are not supported"
                            .into(),
                    ));
                }
                StatementClass::Mutation {
                    target: Some(validate_single_object_name(db_name)?),
                    action: "create_database",
                }
            }
            ast::Statement::CreateView(view) => {
                if !view.temporary
                    || view.materialized
                    || view.or_alter
                    || view.secure
                    || view.if_not_exists
                    || view.to.is_some()
                    || !view.columns.is_empty()
                    || !view.options.to_string().is_empty()
                    || !view.cluster_by.is_empty()
                    || view.comment.is_some()
                    || view.with_no_schema_binding
                    || view.params.is_some()
                {
                    return Err((
                        "UnsupportedSql",
                        "only CREATE TEMP VIEW name AS SELECT is supported".into(),
                    ));
                }
                validate_query(&view.query)?;
                StatementClass::Mutation {
                    target: Some(validate_single_object_name(&view.name)?),
                    action: "create_view",
                }
            }
            ast::Statement::Drop {
                object_type,
                names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
                ..
            } => {
                if names.len() != 1
                    || *cascade
                    || *restrict
                    || *purge
                    || *temporary
                    || table.is_some()
                {
                    return Err((
                        "UnsupportedSql",
                        "DROP accepts one object and no cascade, purge, or temporary modifier"
                            .into(),
                    ));
                }
                let (action, allowed) = match object_type {
                    ast::ObjectType::Table => ("drop_table", true),
                    ast::ObjectType::View => ("drop_view", true),
                    ast::ObjectType::Schema | ast::ObjectType::Database => ("drop_database", true),
                    _ => ("", false),
                };
                if !allowed {
                    return Err((
                        "UnsupportedSql",
                        "only DROP TABLE, VIEW, DATABASE, and SCHEMA are supported".into(),
                    ));
                }
                StatementClass::Mutation {
                    target: Some(validate_object_name(&names[0])?),
                    action,
                }
            }
            ast::Statement::Use(use_statement) => {
                let name = match use_statement {
                    ast::Use::Object(name) | ast::Use::Database(name) | ast::Use::Schema(name) => {
                        name
                    }
                    _ => {
                        return Err(("UnsupportedSql", "USE accepts a database name only".into()));
                    }
                };
                let target = validate_single_object_name(name)?;
                StatementClass::Mutation {
                    target: Some(target),
                    action: "use",
                }
            }
            _ => {
                return Err((
                    "UnsupportedSql",
                    "statement is outside the enabled SQL allowlist".into(),
                ));
            }
        },
        _ => {
            return Err((
                "UnsupportedSql",
                "Spark-specific command is outside the enabled SQL allowlist".into(),
            ));
        }
    };

    let mut expression_count = 0usize;
    let expression_result = match &parsed {
        SparkStatement::Sqlparser(statement) => {
            ast::visit_expressions(statement.as_ref(), |expr| {
                expression_count = expression_count.saturating_add(1);
                if expression_count > 10_000 {
                    return ControlFlow::Break("SQL statement exceeds the expression node limit");
                }
                match expr {
                    ast::Expr::Function(function) => {
                        if !function.to_string().eq_ignore_ascii_case("COUNT(*)") {
                            return ControlFlow::Break(
                                "only COUNT(*) is enabled as a SQL function",
                            );
                        }
                    }
                    ast::Expr::BinaryOp { op, .. } => {
                        let operator = op.to_string().to_ascii_uppercase();
                        if ![
                            "+", "-", "*", "/", "%", "=", "!=", "<>", "<", ">", "<=", ">=", "AND",
                            "OR", "||",
                        ]
                        .contains(&operator.as_str())
                        {
                            return ControlFlow::Break(
                                "SQL expression operator is outside the allowlist",
                            );
                        }
                    }
                    ast::Expr::UnaryOp {
                        op:
                            ast::UnaryOperator::Plus
                            | ast::UnaryOperator::Minus
                            | ast::UnaryOperator::Not,
                        ..
                    } => {}
                    ast::Expr::Cast {
                        kind,
                        data_type,
                        array,
                        format,
                        ..
                    } if *kind == ast::CastKind::Cast
                        && !array
                        && format.is_none()
                        && supported_scalar_sql_type(&data_type.to_string()) => {}
                    ast::Expr::TypedString(value)
                        if supported_scalar_sql_type(&value.data_type.to_string()) => {}
                    ast::Expr::Value(_)
                    | ast::Expr::Identifier(_)
                    | ast::Expr::CompoundIdentifier(_)
                    | ast::Expr::Nested(_)
                    | ast::Expr::IsNull(_)
                    | ast::Expr::IsNotNull(_) => {}
                    _ => return ControlFlow::Break("SQL expression is outside the allowlist"),
                }
                ControlFlow::Continue(())
            })
        }
        _ => ControlFlow::Continue(()),
    };
    if let ControlFlow::Break(message) = expression_result {
        return Err(("UnsupportedSql", message.into()));
    }
    Ok(class)
}

fn validate_query(query: &ast::Query) -> Result<(), (&'static str, String)> {
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err((
            "UnsupportedSql",
            "CTEs, locks, fetch, pipe, settings, and format clauses are not supported".into(),
        ));
    }
    if let Some(limit) = &query.limit_clause {
        let limit_text = limit.to_string();
        let parts: Vec<&str> = limit_text.split_whitespace().collect();
        if parts.len() != 2
            || !parts[0].eq_ignore_ascii_case("LIMIT")
            || !parts[1].bytes().all(|b| b.is_ascii_digit())
        {
            return Err((
                "UnsupportedSql",
                "LIMIT must be a nonnegative integer literal without OFFSET".into(),
            ));
        }
    }
    if let Some(order) = &query.order_by {
        match &order.kind {
            ast::OrderByKind::Expressions(expressions)
                if order.interpolate.is_none()
                    && expressions.iter().all(|item| {
                        item.with_fill.is_none()
                            && matches!(
                                item.expr,
                                ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)
                            )
                    }) => {}
            _ => {
                return Err((
                    "UnsupportedSql",
                    "ORDER BY supports column references only".into(),
                ));
            }
        }
    }
    validate_set_expr(&query.body)
}

fn validate_set_expr(expr: &ast::SetExpr) -> Result<(), (&'static str, String)> {
    match expr {
        ast::SetExpr::Select(select) => {
            if select.distinct.is_some()
                || select.select_modifiers.is_some()
                || select.top.is_some()
                || select.into.is_some()
                || !select.lateral_views.is_empty()
                || select.prewhere.is_some()
                || !select.connect_by.is_empty()
                || !select.cluster_by.is_empty()
                || !select.distribute_by.is_empty()
                || !select.sort_by.is_empty()
                || select.having.is_some()
                || !select.named_window.is_empty()
                || select.qualify.is_some()
                || select.value_table_mode.is_some()
                || !matches!(select.group_by, ast::GroupByExpr::Expressions(ref exprs, ref modifiers) if exprs.is_empty() && modifiers.is_empty())
            {
                return Err((
                    "UnsupportedSql",
                    "SELECT modifiers, grouping, windows, and HAVING are not supported".into(),
                ));
            }
            for relation in &select.from {
                if !relation.joins.is_empty() {
                    return Err((
                        "UnsupportedSql",
                        "joins are not enabled by the HTTP SQL contract".into(),
                    ));
                }
                validate_table_factor(&relation.relation)?;
            }
            for item in &select.projection {
                if !matches!(
                    item,
                    ast::SelectItem::Wildcard(_)
                        | ast::SelectItem::UnnamedExpr(_)
                        | ast::SelectItem::ExprWithAlias { .. }
                ) {
                    return Err((
                        "UnsupportedSql",
                        "qualified wildcards and extended projections are not supported".into(),
                    ));
                }
            }
            Ok(())
        }
        ast::SetExpr::SetOperation {
            op: ast::SetOperator::Union,
            set_quantifier: ast::SetQuantifier::All,
            left,
            right,
        } => {
            validate_set_expr(left)?;
            validate_set_expr(right)
        }
        ast::SetExpr::Query(query) => validate_query(query),
        _ => Err((
            "UnsupportedSql",
            "only SELECT and bounded UNION ALL queries are supported".into(),
        )),
    }
}

fn validate_table_factor(factor: &ast::TableFactor) -> Result<(), (&'static str, String)> {
    match factor {
        ast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => {
            if args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
            {
                return Err((
                    "UnsupportedSql",
                    "table functions, hints, samples, and time travel are not supported".into(),
                ));
            }
            validate_object_name(name)?;
            if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
                return Err((
                    "UnsupportedSql",
                    "table alias column lists are not supported".into(),
                ));
            }
            Ok(())
        }
        ast::TableFactor::Derived {
            lateral,
            subquery,
            sample,
            ..
        } if !lateral && sample.is_none() => validate_query(subquery),
        _ => Err((
            "UnsupportedSql",
            "only named managed tables and derived SELECT relations are supported".into(),
        )),
    }
}

fn validate_create_table(create: &ast::CreateTable) -> Result<(), (&'static str, String)> {
    if create.or_replace
        || create.temporary
        || create.external
        || create.dynamic
        || create.global.is_some()
        || create.transient
        || create.volatile
        || create.iceberg
        || !create.constraints.is_empty()
        || create.query.is_some()
        || create.location.is_some()
        || create.file_format.is_some()
        || create.hive_formats.is_some()
        || !matches!(&create.hive_distribution, ast::HiveDistributionStyle::NONE)
        || !matches!(&create.table_options, ast::CreateTableOptions::None)
        || create.without_rowid
        || create.like.is_some()
        || create.clone.is_some()
        || create.version.is_some()
        || create.comment.is_some()
        || create.on_commit.is_some()
        || create.on_cluster.is_some()
        || create.primary_key.is_some()
        || create.order_by.is_some()
        || create.partition_by.is_some()
        || create.cluster_by.is_some()
        || create.clustered_by.is_some()
        || create.inherits.is_some()
        || create.partition_of.is_some()
        || create.for_values.is_some()
        || create.strict
        || create.copy_grants
        || create.enable_schema_evolution.is_some()
        || create.change_tracking.is_some()
        || create.data_retention_time_in_days.is_some()
        || create.max_data_extension_time_in_days.is_some()
        || create.default_ddl_collation.is_some()
        || create.with_aggregation_policy.is_some()
        || create.with_row_access_policy.is_some()
        || create.with_tags.is_some()
        || create.external_volume.is_some()
        || create.base_location.is_some()
        || create.catalog.is_some()
        || create.catalog_sync.is_some()
        || create.storage_serialization_policy.is_some()
        || create.target_lag.is_some()
        || create.warehouse.is_some()
        || create.refresh_mode.is_some()
        || create.initialize.is_some()
        || create.require_user
    {
        return Err((
            "UnsupportedSql",
            "CREATE TABLE accepts only a managed in-memory scalar schema".into(),
        ));
    }
    if create.columns.is_empty() || create.columns.len() > 256 {
        return Err((
            "UnsupportedSql",
            "CREATE TABLE requires 1-256 columns".into(),
        ));
    }
    let mut names = std::collections::HashSet::new();
    for column in &create.columns {
        let name = validate_simple_identifier(&column.name.value)?;
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(("UnsupportedSql", "table column names must be unique".into()));
        }
        let dtype = column.data_type.to_string().to_ascii_lowercase();
        let supported = [
            "string",
            "varchar",
            "char",
            "int",
            "integer",
            "smallint",
            "bigint",
            "long",
            "double",
            "boolean",
            "date",
            "timestamp",
        ]
        .iter()
        .any(|known| dtype == *known)
            || valid_decimal_type_name(&dtype);
        if !supported || !column.options.is_empty() {
            return Err((
                "UnsupportedSql",
                format!(
                    "column '{}' uses an unsupported type or constraint",
                    column.name.value
                ),
            ));
        }
    }
    validate_object_name(&create.name)?;
    Ok(())
}

fn validate_single_object_name(name: &ast::ObjectName) -> Result<String, (&'static str, String)> {
    let value = validate_object_name(name)?;
    if value.contains('.') {
        return Err((
            "UnsupportedSql",
            "database and temporary view names must be unqualified".into(),
        ));
    }
    Ok(value)
}

fn valid_decimal_type_name(dtype: &str) -> bool {
    let dtype = dtype.to_ascii_lowercase().replace(' ', "");
    let Some(arguments) = dtype
        .strip_prefix("decimal(")
        .and_then(|s| s.strip_suffix(')'))
    else {
        return false;
    };
    let Some((precision, scale)) = arguments.split_once(',') else {
        return false;
    };
    let (Ok(precision), Ok(scale)) = (precision.trim().parse::<u8>(), scale.trim().parse::<u8>())
    else {
        return false;
    };
    (1..=38).contains(&precision) && scale <= precision
}

fn supported_scalar_sql_type(dtype: &str) -> bool {
    let dtype = dtype.trim().to_ascii_lowercase();
    [
        "string",
        "varchar",
        "char",
        "int",
        "integer",
        "smallint",
        "bigint",
        "long",
        "double",
        "boolean",
        "date",
        "timestamp",
    ]
    .contains(&dtype.as_str())
        || valid_decimal_type_name(&dtype)
}

fn validate_insert(insert: &ast::Insert) -> Result<(), (&'static str, String)> {
    if !insert.into
        || insert.overwrite
        || insert.table_alias.is_some()
        || !insert.assignments.is_empty()
        || insert.partitioned.is_some()
        || !insert.after_columns.is_empty()
        || insert.on.is_some()
        || insert.returning.is_some()
        || insert.format_clause.is_some()
        || insert.settings.is_some()
        || insert.ignore
        || insert.replace_into
    {
        return Err((
            "UnsupportedSql",
            "only INSERT INTO managed_table [(columns)] VALUES or SELECT is supported".into(),
        ));
    }
    let Some(source) = insert.source.as_ref() else {
        return Err(("UnsupportedSql", "INSERT requires VALUES or SELECT".into()));
    };
    match source.body.as_ref() {
        ast::SetExpr::Values(values) => {
            if values.rows.len() > 10_000
                || values
                    .rows
                    .iter()
                    .flatten()
                    .any(|expr| !matches!(expr, ast::Expr::Value(_)))
            {
                return Err((
                    "UnsupportedSql",
                    "INSERT VALUES accepts at most 10,000 rows of scalar literals".into(),
                ));
            }
        }
        _ => validate_query(source)?,
    }
    Ok(())
}

fn validate_object_name(name: &ast::ObjectName) -> Result<String, (&'static str, String)> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            (
                "UnsupportedSql",
                "function-generated identifiers are not supported".into(),
            )
        })?;
    if parts.is_empty()
        || parts.len() > 2
        || parts
            .iter()
            .any(|part| validate_simple_identifier(part).is_err())
    {
        return Err((
            "UnsupportedSql",
            "only unqualified and database-qualified identifiers are supported".into(),
        ));
    }
    if parts
        .iter()
        .any(|part| part.eq_ignore_ascii_case("global_temp"))
    {
        return Err((
            "UnsupportedSql",
            "global temporary views are not supported by the server".into(),
        ));
    }
    Ok(parts.join("."))
}

fn validate_simple_identifier(name: &str) -> Result<String, (&'static str, String)> {
    if name.is_empty()
        || name.len() > MAX_IDENTIFIER_BYTES
        || !name.is_ascii()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || name.as_bytes()[0].is_ascii_digit()
    {
        return Err(("UnsupportedSql", "identifiers must be 1-128 ASCII letters, digits, or underscores and cannot start with a digit".into()));
    }
    Ok(name.to_string())
}

fn execute_statement(
    session: &SparkSession,
    sql: &str,
    action: &str,
    target: Option<&str>,
    limits: ExecutionLimits,
    token: &CancellationToken,
) -> Result<JsonValue, ExecutionFailure> {
    let _catalog_read_lease = if action == "read" {
        session
            .begin_catalog_read_snapshot()
            .map_err(|error| execution_failure("CatalogSnapshotError", &error.to_string()))?
    } else {
        None
    };
    if action == "use" {
        let use_db = target
            .ok_or_else(|| execution_failure("QueryError", "USE requires a database name"))?;
        session
            .set_current_database(use_db)
            .map_err(|error| execution_failure("QueryError", &error.to_string()))?;
        return Ok(empty_agent_success());
    }
    if action == "append"
        && target.is_some_and(|name| {
            !name.contains('.')
                && session
                    .list_temp_view_names()
                    .iter()
                    .any(|view| view.eq_ignore_ascii_case(name))
        })
    {
        return Err(execution_failure(
            "UnsupportedSql",
            "INSERT INTO must target a saved table, not a temporary view",
        ));
    }
    if action == "drop_table"
        && target
            .is_some_and(|name| !session.saved_table_exists(name) && session.has_temp_view(name))
    {
        return Err(execution_failure(
            "UnsupportedSql",
            "DROP TABLE must target a saved table; use DROP VIEW for a temporary view",
        ));
    }
    let frame = session
        .sql_with_cancellation(sql, token)
        .map_err(map_engine_failure)?;
    let mut fields = frame
        .schema()
        .map_err(|error| execution_failure("QueryError", &error.to_string()))?
        .fields()
        .to_vec();
    if let Ok(SparkStatement::Sqlparser(statement)) = spark_sql_parser::parse_spark_sql(sql)
        && let ast::Statement::Query(query) = statement.as_ref()
    {
        apply_query_nullability(query, &mut fields);
    }
    let schema = spark_schema(&fields)?;
    let empty_response = json!({
        "status":"ok",
        "data":{"application/json":{"schema":schema.clone(),"data":[]}}
    });
    let empty_response_len = serde_json::to_vec(&empty_response)
        .map_err(|error| execution_failure("ResultEncodingError", &error.to_string()))?
        .len();
    let max_data_bytes = limits
        .max_result_bytes
        .saturating_sub(empty_response_len.saturating_sub(2));
    let (_columns, rows, _native_schema) = frame
        .collect_as_positional_json_with_limits(
            limits.max_rows,
            max_data_bytes,
            limits.max_cell_bytes,
            &Default::default(),
            token,
        )
        .map_err(map_engine_failure)?;
    let response = json!({
        "status":"ok",
        "data":{"application/json":{"schema":schema,"data":rows}}
    });
    let encoded_len = serde_json::to_vec(&response)
        .map_err(|error| execution_failure("ResultEncodingError", &error.to_string()))?
        .len();
    if encoded_len > limits.max_result_bytes {
        return Err(execution_failure(
            "ResultLimitExceeded",
            "serialized result exceeds the configured byte limit",
        ));
    }
    Ok(response)
}

fn apply_query_nullability(query: &ast::Query, fields: &mut [StructField]) {
    if let Some(names) = query_projection_names(query)
        && names.len() == fields.len()
    {
        for (field, name) in fields.iter_mut().zip(names) {
            if let Some(name) = name {
                field.name = name;
            }
        }
    }
    let Some(nullable) = query_projection_nullability(query) else {
        return;
    };
    if nullable.len() == fields.len() {
        for (field, nullable) in fields.iter_mut().zip(nullable) {
            field.nullable = nullable;
        }
    }
}

fn query_projection_names(query: &ast::Query) -> Option<Vec<Option<String>>> {
    set_projection_names(&query.body)
}

fn set_projection_names(expr: &ast::SetExpr) -> Option<Vec<Option<String>>> {
    match expr {
        ast::SetExpr::Select(select) => select_projection_names(select),
        ast::SetExpr::Query(query) => query_projection_names(query),
        ast::SetExpr::SetOperation {
            op: ast::SetOperator::Union,
            set_quantifier: ast::SetQuantifier::All,
            left,
            ..
        } => set_projection_names(left),
        _ => None,
    }
}

fn select_projection_names(select: &ast::Select) -> Option<Vec<Option<String>>> {
    let names = select
        .projection
        .iter()
        .map(|item| match item {
            ast::SelectItem::ExprWithAlias { alias, .. } => Some(Some(alias.value.clone())),
            ast::SelectItem::UnnamedExpr(_) => Some(None),
            ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(_, _) => None,
        })
        .collect::<Option<Vec<_>>>()?;
    Some(names)
}

fn query_projection_nullability(query: &ast::Query) -> Option<Vec<bool>> {
    set_projection_nullability(&query.body)
}

fn set_projection_nullability(expr: &ast::SetExpr) -> Option<Vec<bool>> {
    match expr {
        ast::SetExpr::Select(select) => select_projection_nullability(select),
        ast::SetExpr::Query(query) => query_projection_nullability(query),
        ast::SetExpr::SetOperation {
            op: ast::SetOperator::Union,
            set_quantifier: ast::SetQuantifier::All,
            left,
            right,
        } => {
            let left = set_projection_nullability(left)?;
            let right = set_projection_nullability(right)?;
            (left.len() == right.len()).then(|| {
                left.into_iter()
                    .zip(right)
                    .map(|(left, right)| left || right)
                    .collect()
            })
        }
        _ => None,
    }
}

fn select_projection_nullability(select: &ast::Select) -> Option<Vec<bool>> {
    select
        .projection
        .iter()
        .map(|item| match item {
            ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
                Some(expression_nullable(expr))
            }
            ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(_, _) => None,
        })
        .collect()
}

fn expression_nullable(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Value(value) => matches!(value.value, ast::Value::Null),
        ast::Expr::TypedString { .. } => false,
        ast::Expr::Cast { expr, .. }
        | ast::Expr::Nested(expr)
        | ast::Expr::UnaryOp { expr, .. } => expression_nullable(expr),
        ast::Expr::BinaryOp { left, right, .. } => {
            expression_nullable(left) || expression_nullable(right)
        }
        ast::Expr::IsNull(_) | ast::Expr::IsNotNull(_) => false,
        ast::Expr::Function(function) if function.to_string().eq_ignore_ascii_case("COUNT(*)") => {
            false
        }
        ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_) => true,
        _ => true,
    }
}

fn spark_schema(fields: &[StructField]) -> Result<JsonValue, ExecutionFailure> {
    let mut encoded_fields = Vec::with_capacity(fields.len());
    for field in fields {
        encoded_fields.push(json!({"name":field.name,"type":spark_type(&field.data_type)?,"nullable":field.nullable,"metadata":field.metadata}));
    }
    Ok(json!({"type":"struct","fields":encoded_fields}))
}

fn spark_type(data_type: &DataType) -> Result<JsonValue, ExecutionFailure> {
    match data_type {
        DataType::String => Ok(json!("string")),
        DataType::Integer => Ok(json!("integer")),
        DataType::Short => Ok(json!("short")),
        DataType::Long => Ok(json!("long")),
        DataType::Double => Ok(json!("double")),
        DataType::Decimal(precision, scale) => Ok(json!(format!("decimal({precision},{scale})"))),
        DataType::Boolean => Ok(json!("boolean")),
        DataType::Date => Ok(json!("date")),
        DataType::Timestamp => Ok(json!("timestamp")),
        _ => Err(execution_failure(
            "UnsupportedResultType",
            "nested, binary, and unrepresented logical types are disabled on HTTP",
        )),
    }
}

fn empty_agent_success() -> JsonValue {
    json!({"status":"ok","data":{"application/json":{"schema":{"type":"struct","fields":[]},"data":[]}}})
}

fn map_engine_failure(error: EngineError) -> ExecutionFailure {
    let message = error.to_string();
    let name = match error {
        EngineError::Cancelled(_) => "Cancelled",
        EngineError::NotFound(_) => "QueryError",
        EngineError::User(_)
            if message.contains("row limit")
                || message.contains("configured byte limit")
                || message.contains("configured serialized byte limit") =>
        {
            "ResultLimitExceeded"
        }
        EngineError::User(_) if message.contains("CatalogCapacityExceeded") => {
            "CatalogCapacityExceeded"
        }
        _ => "QueryError",
    };
    execution_failure(name, &message)
}

fn execution_failure(name: &'static str, message: &str) -> ExecutionFailure {
    ExecutionFailure {
        name,
        message: bound_text(message, MAX_ERROR_BYTES),
    }
}

fn record_catalog_mutation(
    server: &ServerInner,
    target: Option<&str>,
    action: &str,
    session: &SparkSession,
) -> Option<JsonValue> {
    let Some(target) = target else {
        return Some(json!({"kind":"session","name":"current_database","version":1}));
    };
    if matches!(
        action,
        "use" | "create_database" | "drop_database" | "create_view" | "drop_view"
    ) {
        let version = server.catalog_sequence.fetch_add(1, Ordering::Relaxed);
        return Some(json!({"kind":action,"name":target,"version":version}));
    }
    let canonical = if target.contains('.') {
        target.to_ascii_lowercase()
    } else {
        format!(
            "{}.{}",
            session.current_database().to_ascii_lowercase(),
            target.to_ascii_lowercase()
        )
    };
    let Ok(mut receipts) = server.catalog_receipts.lock() else {
        return Some(
            json!({"kind":"catalog","name":canonical,"version":null,"identity":"unknown"}),
        );
    };
    let receipt = match action {
        "create_table" => receipts
            .entry(canonical.clone())
            .or_insert_with(|| CatalogReceipt {
                identity: Uuid::new_v4().to_string(),
                version: 1,
            })
            .clone(),
        "append" => {
            let receipt = receipts
                .entry(canonical.clone())
                .or_insert_with(|| CatalogReceipt {
                    identity: Uuid::new_v4().to_string(),
                    version: 0,
                });
            receipt.version = receipt.version.saturating_add(1);
            receipt.clone()
        }
        "drop_table" => {
            let removed = receipts.remove(&canonical);
            return Some(json!({"kind":"table","name":canonical,"dropped":removed.is_some()}));
        }
        _ => CatalogReceipt {
            identity: Uuid::new_v4().to_string(),
            version: 1,
        },
    };
    Some(
        json!({"kind":"table","name":canonical,"identity":receipt.identity,"version":receipt.version}),
    )
}

fn set_operation_committed(operation: &Arc<Mutex<OperationRecord>>, receipt: Option<JsonValue>) {
    if let Ok(mut record) = operation.lock() {
        record.execution_state = "succeeded";
        record.effect_outcome = "committed";
        record.receipt = receipt;
        record.error = None;
    }
}

fn set_operation_failure(
    operation: &Arc<Mutex<OperationRecord>>,
    name: &str,
    message: &str,
    outcome: &'static str,
) {
    if let Ok(mut record) = operation.lock() {
        record.execution_state = if outcome == "pending" {
            "running"
        } else {
            "failed"
        };
        record.effect_outcome = outcome;
        record.error = Some((name.to_string(), bound_text(message, MAX_ERROR_BYTES)));
    }
}

fn operation_json_from_record(instance: &str, id: &str, record: &OperationRecord) -> JsonValue {
    json!({
        "server_instance_id":instance,
        "operation_id":id,
        "execution_state":record.execution_state,
        "effect_outcome":record.effect_outcome,
        "receipt":record.receipt,
        "error":record.error.as_ref().map(|(name,message)| json!({"ename":name,"evalue":message}))
    })
}

fn existing_operation_response(
    server: &ServerInner,
    id: &str,
    operation: &Arc<Mutex<OperationRecord>>,
) -> HttpResponse {
    let Ok(record) = operation.lock() else {
        return transport_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "operation state is unavailable",
        );
    };
    if record.execution_state == "succeeded" {
        let mut response = empty_agent_success();
        response["operation"] = operation_json_from_record(&server.instance_id, id, &record);
        if let Some(receipt) = &record.receipt {
            response["receipt"] = receipt.clone();
        }
        json_response(StatusCode::OK, response)
    } else if record.execution_state == "running" {
        agent_error(
            "OperationInProgress",
            "this operation identity is already executing; query operation status",
            Some(operation_json_from_record(&server.instance_id, id, &record)),
        )
    } else {
        let (name, message) = record
            .error
            .clone()
            .unwrap_or_else(|| ("OperationFailed".into(), "mutation failed".into()));
        agent_error_dynamic(
            &name,
            &message,
            Some(operation_json_from_record(&server.instance_id, id, &record)),
        )
    }
}

fn request_fingerprint(request: &StatementRequest) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in [&request.session, &request.kind, &request.code] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.finalize().into()
}

fn validate_identifier(name: &str, value: &str) -> Result<(), RequestFailure> {
    if value.is_empty()
        || value.len() > MAX_IDENTIFIER_BYTES
        || !value.is_ascii()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        return Err(RequestFailure {
            status: StatusCode::BAD_REQUEST,
            name: "InvalidIdentifier",
            message: format!("{name} must be 1-128 safe ASCII bytes"),
        });
    }
    Ok(())
}

fn check_json_request_headers(
    headers: &hyper::HeaderMap<HeaderValue>,
) -> Result<(), RequestFailure> {
    if let Some(content_type) = headers.get(CONTENT_TYPE) {
        let Ok(value) = content_type.to_str() else {
            return Err(RequestFailure {
                status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
                name: "UnsupportedContentType",
                message: "Content-Type must be application/json".into(),
            });
        };
        if !value
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("application/json")
        {
            return Err(RequestFailure {
                status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
                name: "UnsupportedContentType",
                message: "Content-Type must be application/json".into(),
            });
        }
    }
    if headers
        .get(CONTENT_ENCODING)
        .is_some_and(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
    {
        return Err(RequestFailure {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            name: "UnsupportedContentEncoding",
            message: "compressed request bodies are not supported".into(),
        });
    }
    Ok(())
}

async fn read_bounded_body(mut body: Incoming, limit: usize) -> Result<Vec<u8>, RequestFailure> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| RequestFailure {
            status: StatusCode::BAD_REQUEST,
            name: "InvalidBody",
            message: bound_text(&error.to_string(), MAX_ERROR_BYTES),
        })?;
        if let Ok(data) = frame.into_data() {
            if bytes.len().saturating_add(data.len()) > limit {
                return Err(RequestFailure {
                    status: StatusCode::PAYLOAD_TOO_LARGE,
                    name: "RequestTooLarge",
                    message: "request body exceeds the configured byte limit".into(),
                });
            }
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}

async fn read_bounded_body_with_timeout(
    body: Incoming,
    limit: usize,
    wait: Duration,
) -> Result<Vec<u8>, RequestFailure> {
    match timeout(wait, read_bounded_body(body, limit)).await {
        Ok(result) => result,
        Err(_) => Err(RequestFailure {
            status: StatusCode::REQUEST_TIMEOUT,
            name: "BodyReadTimeout",
            message: "timed out while reading the request body".into(),
        }),
    }
}

fn internal_failure() -> RequestFailure {
    RequestFailure {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        name: "InternalError",
        message: "server state is unavailable".into(),
    }
}

fn request_failure_response(error: RequestFailure) -> HttpResponse {
    transport_error(error.status, error.name, &error.message)
}

fn transport_error(status: StatusCode, name: &'static str, message: &str) -> HttpResponse {
    json_response(
        status,
        json!({"status":"error","ename":name,"evalue":bound_text(message, MAX_ERROR_BYTES)}),
    )
}

fn agent_error(name: &'static str, message: &str, operation: Option<JsonValue>) -> HttpResponse {
    agent_error_dynamic(name, message, operation)
}

fn agent_error_dynamic(name: &str, message: &str, operation: Option<JsonValue>) -> HttpResponse {
    let mut body =
        json!({"status":"error","ename":name,"evalue":bound_text(message, MAX_ERROR_BYTES)});
    if let Some(operation) = operation {
        body["operation"] = operation;
    }
    json_response(StatusCode::OK, body)
}

fn json_response(status: StatusCode, json: JsonValue) -> HttpResponse {
    let body = serde_json::to_vec(&json).unwrap_or_else(|_| b"{\"status\":\"error\",\"ename\":\"InternalError\",\"evalue\":\"response encoding failed\"}".to_vec());
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn bound_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::oneshot;

    #[test]
    fn sql_validation_rejects_unhandled_ddl_and_non_ansi_casts() {
        assert!(validate_statement("CREATE TABLE t (id BIGINT) WITH (location = '/tmp')").is_err());
        assert!(validate_statement("CREATE DATABASE d WITH (location = '/tmp')").is_err());
        assert!(validate_statement("SELECT TRY_CAST('1' AS BIGINT)").is_err());
        assert!(validate_statement("SELECT CAST('1' AS ARRAY<INT>)").is_err());
        assert!(validate_statement("SELECT CAST(1 AS BIGINT)").is_ok());
    }

    #[tokio::test]
    async fn mutation_retention_capacity_rejects_before_catalog_effects() {
        let undersized = ServerConfig {
            max_mutation_records: 1,
            max_mutation_bytes: 256,
            ..ServerConfig::default()
        };
        assert!(Server::new(undersized).is_err());

        let config = ServerConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            max_mutation_records: 1,
            max_mutation_bytes: 4096,
            ..ServerConfig::default()
        };
        let server = Server::new(config).unwrap();
        server.startup_check().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        server.inner.ready.store(true, Ordering::Release);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .run_listener(listener, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
            })
        };

        let instance = server.instance_id().to_string();
        let accepted = statement_request(
            address,
            &instance,
            "writer",
            "retained-create",
            "CREATE DATABASE retained_db",
        )
        .await;
        assert_eq!(accepted["status"], "ok");
        assert_eq!(accepted["operation"]["effect_outcome"], "committed");

        let (status, rejected) = request_json(
            address,
            "POST",
            "/statements",
            &json!({
                "session":"writer",
                "kind":"sql",
                "code":"CREATE DATABASE must_not_exist",
                "server_instance_id":instance,
                "operation_id":"rejected-create"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(rejected["ename"], "MutationRetentionCapacityExceeded");
        assert!(!server.inner.root_session.database_exists("must_not_exist"));

        let (status, retained_status) = request_json(
            address,
            "POST",
            "/operations/status",
            &json!({"server_instance_id":instance,"operation_id":"retained-create"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(retained_status["effect_outcome"], "committed");
        assert!(server.inner.operation("rejected-create").is_none());

        let _ = shutdown_tx.send(());
        server_task.await.unwrap().unwrap();
    }

    #[test]
    fn result_schema_preserves_metadata_and_keeps_native_extensions_disabled() {
        let metadata = [("owner".to_string(), json!("analytics"))]
            .into_iter()
            .collect();
        let field = StructField::new("id".into(), DataType::Long, false).with_metadata(metadata);
        let encoded = spark_schema(&[field]).unwrap();
        assert_eq!(encoded["fields"][0]["metadata"]["owner"], "analytics");

        let byte_field = StructField::new("tiny".into(), DataType::Byte, true);
        assert!(spark_schema(&[byte_field]).is_err());
    }

    #[tokio::test]
    async fn shared_catalog_and_mutation_replay_are_scoped_to_server_instance() {
        let config = ServerConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            ..ServerConfig::default()
        };
        let server = Server::new(config.clone()).unwrap();
        server.startup_check().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        server.inner.ready.store(true, Ordering::Release);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_task = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .run_listener(listener, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
            })
        };

        let base = request_json(address, "GET", "/health", &json!({})).await;
        assert_eq!(base.0, StatusCode::OK);

        let query = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_read","kind":"sql","code":"SELECT 1 AS ready"}),
        )
        .await;
        assert_eq!(query.0, StatusCode::OK);
        assert_eq!(query.1["status"], "ok");
        assert_eq!(query.1["data"]["application/json"]["data"], json!([[1]]));
        assert_eq!(
            query.1["data"]["application/json"]["schema"]["fields"][0]["nullable"],
            false
        );
        let exact_bigint = request_json(
            address,
            "POST",
            "/statements",
            &json!({
                "session":"s_schema",
                "kind":"sql",
                "code":"SELECT CAST(9007199254740993 AS BIGINT) AS n"
            }),
        )
        .await;
        assert_eq!(
            exact_bigint.1["data"]["application/json"]["data"],
            json!([[9007199254740993_i64]])
        );
        let decimal = request_json(
            address,
            "POST",
            "/statements",
            &json!({
                "session":"s_schema",
                "kind":"sql",
                "code":"SELECT CAST('1234567890123456789012345678901234.5678' AS DECIMAL(38,4)) AS d"
            }),
        )
        .await;
        assert_eq!(
            decimal.1["data"]["application/json"]["data"],
            json!([["1234567890123456789012345678901234.5678"]])
        );
        let null_result = request_json(
            address,
            "POST",
            "/statements",
            &json!({
                "session":"s_schema",
                "kind":"sql",
                "code":"SELECT CAST(NULL AS BIGINT) AS n"
            }),
        )
        .await;
        assert_eq!(
            null_result.1["data"]["application/json"]["data"],
            json!([[null]])
        );
        assert_eq!(
            null_result.1["data"]["application/json"]["schema"]["fields"][0]["type"],
            "long"
        );
        assert_eq!(
            null_result.1["data"]["application/json"]["schema"]["fields"][0]["nullable"],
            true
        );
        let typed_empty = request_json(
            address,
            "POST",
            "/statements",
            &json!({
                "session":"s_schema",
                "kind":"sql",
                "code":"SELECT CAST(NULL AS BIGINT) AS n WHERE 1 = 0"
            }),
        )
        .await;
        assert_eq!(typed_empty.1["data"]["application/json"]["data"], json!([]));
        assert_eq!(
            typed_empty.1["data"]["application/json"]["schema"]["fields"][0]["type"],
            "long"
        );
        let duplicate_names = request_json(
            address,
            "POST",
            "/statements",
            &json!({
                "session":"s_schema",
                "kind":"sql",
                "code":"SELECT 1 AS x, 2 AS x"
            }),
        )
        .await;
        assert_eq!(
            duplicate_names.1["data"]["application/json"]["data"],
            json!([[1, 2]])
        );
        assert_eq!(
            duplicate_names.1["data"]["application/json"]["schema"]["fields"][0]["name"],
            "x"
        );
        assert_eq!(
            duplicate_names.1["data"]["application/json"]["schema"]["fields"][1]["name"],
            "x"
        );

        let legacy_mutation = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_write","kind":"sql","code":"CREATE DATABASE phase_db"}),
        )
        .await;
        assert_eq!(legacy_mutation.1["ename"], "OperationIdentityRequired");

        let server_id = server.instance_id().to_string();
        let create_database = statement_request(
            address,
            &server_id,
            "s_write",
            "create-db-1",
            "CREATE DATABASE phase_db",
        )
        .await;
        assert_eq!(create_database["status"], "ok");
        assert_eq!(create_database["operation"]["effect_outcome"], "committed");

        let missing_database_table = statement_request(
            address,
            &server_id,
            "s_write",
            "create-in-missing-database",
            "CREATE TABLE missing_db.orphan (id BIGINT)",
        )
        .await;
        assert_eq!(missing_database_table["ename"], "QueryError");
        assert_eq!(
            missing_database_table["operation"]["effect_outcome"],
            "failed"
        );

        let create_table = statement_request(
            address,
            &server_id,
            "s_write",
            "create-table-1",
            "CREATE TABLE phase_db.items (id BIGINT)",
        )
        .await;
        assert_eq!(create_table["status"], "ok");

        let isolated_server = Server::new(config).unwrap();
        let isolated_session = isolated_server.inner.root_session.new_session();
        assert!(
            isolated_session
                .sql("SELECT id FROM phase_db.items")
                .is_err()
        );

        let create_default_table = statement_request(
            address,
            &server_id,
            "s_default",
            "create-default-table",
            "CREATE TABLE common (id BIGINT)",
        )
        .await;
        assert_eq!(create_default_table["status"], "ok");
        let create_qualified_table = statement_request(
            address,
            &server_id,
            "s_write",
            "create-qualified-table",
            "CREATE TABLE phase_db.common (id BIGINT)",
        )
        .await;
        assert_eq!(create_qualified_table["status"], "ok");
        assert_eq!(
            statement_request(
                address,
                &server_id,
                "s_default",
                "insert-default-common",
                "INSERT INTO common VALUES (1)",
            )
            .await["status"],
            "ok"
        );
        assert_eq!(
            statement_request(
                address,
                &server_id,
                "s_write",
                "insert-qualified-common",
                "INSERT INTO phase_db.common VALUES (2)",
            )
            .await["status"],
            "ok"
        );
        assert_eq!(
            statement_request(
                address,
                &server_id,
                "s_write",
                "use-phase-db",
                "USE phase_db",
            )
            .await["status"],
            "ok"
        );
        let current_database_read = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_write","kind":"sql","code":"SELECT id FROM common"}),
        )
        .await;
        assert_eq!(
            current_database_read.1["data"]["application/json"]["data"],
            json!([[2]])
        );
        let default_database_read = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_default","kind":"sql","code":"SELECT id FROM common"}),
        )
        .await;
        assert_eq!(
            default_database_read.1["data"]["application/json"]["data"],
            json!([[1]])
        );

        let insert = json!({
            "session":"s_write",
            "kind":"sql",
            "code":"INSERT INTO phase_db.items VALUES (1)",
            "server_instance_id":server_id,
            "operation_id":"append-1"
        });
        let inserted = request_json(address, "POST", "/statements", &insert).await;
        assert_eq!(inserted.1["status"], "ok");
        assert_eq!(inserted.1["operation"]["effect_outcome"], "committed");
        let duplicate = request_json(address, "POST", "/statements", &insert).await;
        assert_eq!(duplicate.1["status"], "ok");

        let shared_read = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_other","kind":"sql","code":"SELECT COUNT(*) AS n FROM phase_db.items"}),
        )
        .await;
        assert_eq!(shared_read.1["status"], "ok", "{shared_read:?}");
        assert_eq!(
            shared_read.1["data"]["application/json"]["data"],
            json!([[1]])
        );
        assert_eq!(
            shared_read.1["data"]["application/json"]["schema"]["fields"][0]["nullable"],
            false
        );

        let table_snapshot = statement_request(
            address,
            &server_id,
            "s_write",
            "create-table-snapshot-view",
            "CREATE TEMP VIEW items_snapshot AS SELECT id FROM phase_db.items",
        )
        .await;
        assert_eq!(table_snapshot["status"], "ok");
        assert_eq!(
            statement_request(
                address,
                &server_id,
                "s_write",
                "append-after-view",
                "INSERT INTO phase_db.items VALUES (3)",
            )
            .await["status"],
            "ok"
        );
        let pinned_view_read = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_write","kind":"sql","code":"SELECT id FROM items_snapshot"}),
        )
        .await;
        assert_eq!(
            pinned_view_read.1["data"]["application/json"]["data"],
            json!([[1]])
        );

        let create_view = statement_request(
            address,
            &server_id,
            "s_write",
            "create-temp-view",
            "CREATE TEMP VIEW Local_Items AS SELECT 10 AS id",
        )
        .await;
        assert_eq!(create_view["status"], "ok");
        let insert_view = statement_request(
            address,
            &server_id,
            "s_write",
            "insert-temp-view",
            "INSERT INTO local_items VALUES (11)",
        )
        .await;
        assert_eq!(insert_view["ename"], "UnsupportedSql");
        let duplicate_view = statement_request(
            address,
            &server_id,
            "s_write",
            "duplicate-temp-view",
            "CREATE TEMP VIEW local_items AS SELECT 12 AS id",
        )
        .await;
        assert_eq!(duplicate_view["ename"], "QueryError");
        let unchanged_view = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_write","kind":"sql","code":"SELECT id FROM local_items"}),
        )
        .await;
        assert_eq!(
            unchanged_view.1["data"]["application/json"]["data"],
            json!([[10]])
        );
        let replace_view = statement_request(
            address,
            &server_id,
            "s_write",
            "replace-temp-view",
            "CREATE OR REPLACE TEMP VIEW local_items AS SELECT 12 AS id",
        )
        .await;
        assert_eq!(replace_view["status"], "ok");
        let replaced_view = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_write","kind":"sql","code":"SELECT id FROM local_items"}),
        )
        .await;
        assert_eq!(
            replaced_view.1["data"]["application/json"]["data"],
            json!([[12]])
        );
        let view_in_other_session = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_other","kind":"sql","code":"SELECT id FROM local_items"}),
        )
        .await;
        assert_ne!(view_in_other_session.1["status"], "ok");
        let drop_view_as_table = statement_request(
            address,
            &server_id,
            "s_write",
            "drop-view-as-table",
            "DROP TABLE local_items",
        )
        .await;
        assert_eq!(drop_view_as_table["ename"], "UnsupportedSql");
        let view_after_wrong_drop = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_write","kind":"sql","code":"SELECT id FROM local_items"}),
        )
        .await;
        assert_eq!(
            view_after_wrong_drop.1["data"]["application/json"]["data"],
            json!([[12]])
        );

        let conflict = request_json(
            address,
            "POST",
            "/statements",
            &json!({
                "session":"s_write",
                "kind":"sql",
                "code":"INSERT INTO phase_db.items VALUES (2)",
                "server_instance_id":server_id,
                "operation_id":"append-1"
            }),
        )
        .await;
        assert_eq!(conflict.1["ename"], "OperationIdentityConflict");

        let close = request_json(address, "POST", "/close", &json!({"session":"s_write"})).await;
        assert_eq!(close.1["status"], "ok");
        let status = request_json(
            address,
            "POST",
            "/operations/status",
            &json!({"server_instance_id":server_id,"operation_id":"append-1"}),
        )
        .await;
        assert_eq!(status.1["effect_outcome"], "committed");

        let after_close = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_after_close","kind":"sql","code":"SELECT COUNT(*) AS n FROM phase_db.items"}),
        )
        .await;
        assert_eq!(
            after_close.1["data"]["application/json"]["data"],
            json!([[2]])
        );
        let view_after_close = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_after_close","kind":"sql","code":"SELECT id FROM local_items"}),
        )
        .await;
        assert_ne!(view_after_close.1["status"], "ok");

        let unsupported = request_json(
            address,
            "POST",
            "/statements",
            &json!({"session":"s_other","kind":"sql","code":"SELECT read_csv('/etc/passwd')"}),
        )
        .await;
        assert_eq!(unsupported.1["ename"], "UnsupportedSql");

        let _ = shutdown_tx.send(());
        server_task.await.unwrap().unwrap();
    }

    async fn statement_request(
        address: SocketAddr,
        instance: &str,
        session: &str,
        operation: &str,
        sql: &str,
    ) -> JsonValue {
        request_json(
            address,
            "POST",
            "/statements",
            &json!({
                "session":session,
                "kind":"sql",
                "code":sql,
                "server_instance_id":instance,
                "operation_id":operation
            }),
        )
        .await
        .1
    }

    async fn request_json(
        address: SocketAddr,
        method: &str,
        path: &str,
        body: &JsonValue,
    ) -> (StatusCode, JsonValue) {
        let body = if method == "GET" {
            String::new()
        } else {
            serde_json::to_string(body).unwrap()
        };
        let mut stream = TcpStream::connect(address).await.unwrap();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let split = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
            + 4;
        let headers = std::str::from_utf8(&response[..split]).unwrap();
        let status = headers
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u16>()
            .unwrap();
        let body = serde_json::from_slice(&response[split..]).unwrap();
        (StatusCode::from_u16(status).unwrap(), body)
    }
}
