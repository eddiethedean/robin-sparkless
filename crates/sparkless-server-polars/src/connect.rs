//! Spark Connect 4.0.5 gRPC adapter and bounded protobuf boundary.
//!
//! Product-specific RPC handlers translate the frozen relation subset and serve analysis/config
//! against Polars sessions. The Connect crate owns generated gRPC dispatch and replay transport;
//! this adapter shares the server runtime's session, operation, and admission registries.

use crate::{ConnectReserveError, ConnectSessionIdentity, RequestFailure, Server, connect_proto};
use prost::Message;
use robin_sparkless_core::CancellationToken;
use robin_sparkless_core::engine::ArrowIpcBatchStream;
use sha2::{Digest, Sha256};
#[cfg(test)]
use sparkless_server_connect::{
    CONNECT_DETACHED_RETENTION, GoogleRpcErrorInfo, GoogleRpcStatus,
    MAX_CONNECT_REPLAY_OPERATION_BYTES,
};
use sparkless_server_connect::{
    ConnectApplication, ConnectGrpcService, ConnectResponseStream, MAX_CONNECT_PROTO_BYTES,
    preflight_wire_message,
};
pub use sparkless_server_connect::{
    ConnectOutcome, ConnectReplayRecord, MAX_CONNECT_ARROW_BATCH_ROWS,
    MAX_CONNECT_ARROW_RESPONSE_BYTES, MAX_CONNECT_HISTORY_ENTRIES, MAX_CONNECT_LIVE_OPERATIONS,
    MAX_CONNECT_LOCAL_DECODED_BYTES, MAX_CONNECT_LOCAL_IPC_BYTES, MAX_CONNECT_LOCAL_ROWS,
    MAX_CONNECT_LOCAL_SCALAR_BYTES, MAX_CONNECT_RESULT_BYTES, MAX_CONNECT_RESULT_ROWS,
    MAX_CONNECT_SCHEMA_FIELDS, MAX_CONNECT_STREAM_SENDERS, ReplayPoll,
    status_with_standard_details,
};
use sparkless_server_core::{
    ConnectExecutionPort, ConnectExecutionRequest, ConnectExecutionResult, EngineSessionHandle,
    ExecutionError,
};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

pub(super) struct PolarsConnectExecution;

impl ConnectExecutionPort for PolarsConnectExecution {
    fn execute_connect_plan(
        &self,
        session: &EngineSessionHandle,
        request: ConnectExecutionRequest,
    ) -> Result<ConnectExecutionResult, ExecutionError> {
        let session = session
            .downcast_ref::<robin_sparkless::SparkSession>("polars")
            .ok_or_else(|| ExecutionError {
                code: "MismatchedExecutionSession",
                message: "the session handle is not owned by the Polars execution adapter".into(),
                details: None,
            })?;
        session.sync_thread_context();
        let plan =
            connect_proto::Plan::decode(request.serialized_plan.as_slice()).map_err(|error| {
                ExecutionError {
                    code: "CONNECT_PLAN_DECODE_FAILED",
                    message: error.to_string(),
                    details: None,
                }
            })?;
        let translated = plan::translate_plan_for_execution(session, &plan, &request.cancellation)
            .map_err(|status| ExecutionError {
                code: "CONNECT_PLAN_TRANSLATION_FAILED",
                message: status.message().to_string(),
                details: Some(serde_json::json!({ "connect_status_code": status.code() as i32 })),
            })?;
        let schema =
            plan::schema_to_proto(&translated.schema).map_err(|status| ExecutionError {
                code: "CONNECT_SCHEMA_TRANSLATION_FAILED",
                message: status.message().to_string(),
                details: Some(serde_json::json!({ "connect_status_code": status.code() as i32 })),
            })?;
        let schema_bytes = schema.encode_to_vec();
        let output_schema = translated.schema;
        let deferred_count_input = translated.deferred_count_input;
        let max_batch_bytes = request
            .max_response_bytes
            .checked_sub(schema.encoded_len())
            .and_then(|bytes| bytes.checked_sub(512))
            .filter(|bytes| *bytes > 0)
            .ok_or_else(|| ExecutionError {
                code: "CONNECT_ARROW_SCHEMA_LIMIT",
                message: "Connect output schema leaves no room for a bounded Arrow response".into(),
                details: None,
            })?;
        let batches = if let Some(count_input) = deferred_count_input {
            deferred_count_batches(
                session.clone(),
                count_input,
                output_schema,
                request.cancellation,
                request.max_batch_rows,
                max_batch_bytes,
            )
        } else {
            translated
                .dataframe
                .to_arrow_ipc_batches_with_schema_and_cancellation(
                    request.max_batch_rows,
                    max_batch_bytes,
                    &request.cancellation,
                    &output_schema,
                )
                .map_err(|error| ExecutionError {
                    code: "CONNECT_ARROW_STREAM_FAILED",
                    message: error.to_string(),
                    details: None,
                })?
        };
        Ok(ConnectExecutionResult {
            schema: schema_bytes,
            batches,
        })
    }
}

fn deferred_count_batches(
    session: robin_sparkless::SparkSession,
    count_input: robin_sparkless::DataFrame,
    output_schema: robin_sparkless::StructType,
    cancellation: CancellationToken,
    max_rows: usize,
    max_bytes: usize,
) -> ArrowIpcBatchStream {
    let mut batches = None;
    let mut initialized = false;
    ArrowIpcBatchStream::new(std::iter::from_fn(move || {
        if !initialized {
            initialized = true;
            batches = Some(
                count_input
                    .count_with_cancellation(&cancellation)
                    .and_then(|count| {
                        i64::try_from(count).map_err(|_| {
                            robin_sparkless::EngineError::invalid_input(
                                "Connect count result exceeds the signed 64-bit range".into(),
                            )
                        })
                    })
                    .and_then(|count| {
                        session.create_dataframe_from_i64_column("count(1)", vec![count])
                    })
                    .and_then(|frame| {
                        frame.to_arrow_ipc_batches_with_schema_and_cancellation(
                            max_rows,
                            max_bytes,
                            &cancellation,
                            &output_schema,
                        )
                    }),
            );
        }
        match batches.as_mut()? {
            Ok(stream) => stream.next(),
            Err(_) => batches.take().and_then(Result::err).map(Err),
        }
    }))
}

static CONNECT_PRODUCER_STARTS: AtomicU64 = AtomicU64::new(0);
const CONNECT_GRACEFUL_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_FORCE_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_RESPONSE_STALL_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_FIRST_BATCH_ACK_GRACE: Duration = Duration::from_millis(50);

struct CancelTokenOnDrop(robin_sparkless_core::engine::CancellationToken);

impl Drop for CancelTokenOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Default)]
struct ConnectConnectionRegistry {
    next_id: AtomicU64,
    active: Mutex<HashMap<u64, Arc<std::net::TcpStream>>>,
}

struct DrainableTcpStream {
    stream: TcpStream,
    registry: Arc<ConnectConnectionRegistry>,
    id: u64,
}

impl ConnectConnectionRegistry {
    fn track(self: &Arc<Self>, stream: TcpStream) -> io::Result<DrainableTcpStream> {
        let standard_stream = stream.into_std()?;
        let control = Arc::new(standard_stream.try_clone()?);
        let stream = TcpStream::from_std(standard_stream)?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.active
            .lock()
            .map_err(|_| io::Error::other("Connect connection registry is unavailable"))?
            .insert(id, Arc::clone(&control));
        Ok(DrainableTcpStream {
            stream,
            registry: Arc::clone(self),
            id,
        })
    }

    fn force_close_all(&self) -> usize {
        let Ok(mut active) = self.active.lock() else {
            return 0;
        };
        let count = active.len();
        for stream in active.values() {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
        active.clear();
        count
    }
}

impl Drop for DrainableTcpStream {
    fn drop(&mut self) {
        if let Ok(mut active) = self.registry.active.lock() {
            active.remove(&self.id);
        }
    }
}

impl AsyncRead for DrainableTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for DrainableTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

impl tonic::transport::server::Connected for DrainableTcpStream {
    type ConnectInfo = tonic::transport::server::TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        tonic::transport::server::TcpConnectInfo {
            local_addr: self.stream.local_addr().ok(),
            remote_addr: self.stream.peer_addr().ok(),
        }
    }
}

fn trace_connect_rpc(method: &str) {
    if std::env::var_os("SPARKLESS_CONNECT_TRACE").as_deref() == Some(std::ffi::OsStr::new("1")) {
        eprintln!("connect_trace event=rpc method={method}");
    }
}

fn trace_connect_arrow_response(bytes: usize, rows: i64, start_offset: u64) {
    if std::env::var_os("SPARKLESS_CONNECT_TRACE").as_deref() == Some(std::ffi::OsStr::new("1")) {
        eprintln!(
            "connect_trace event=arrow_response bytes={bytes} rows={rows} start_offset={start_offset}"
        );
    }
}

mod analysis;
mod config;
mod plan;

enum InterruptTarget {
    All,
    Tag(String),
    Operation(String),
}

use connect_proto::spark_connect_service_server::SparkConnectServiceServer;

/// Product application hooks for the shared native Spark Connect transport.
#[derive(Clone)]
pub struct ConnectService {
    server: Server,
}

impl Server {
    fn cancel_connect_work_for_shutdown(
        &self,
    ) -> Result<tokio::sync::watch::Receiver<usize>, crate::ServerError> {
        let _lifecycle =
            self.inner
                .connect_runtime
                .lifecycle_lock()
                .map_err(|_| crate::ServerError {
                    message: "Connect session lifecycle state is unavailable".into(),
                })?;
        self.inner.connect_runtime.set_draining(true);
        let operations =
            self.inner
                .connect_runtime
                .operations()
                .map_err(|_| crate::ServerError {
                    message: "Connect operation state is unavailable".into(),
                })?;
        for operation in operations.values() {
            // A terminal response can precede the blocking producer's actual return.
            operation.cancellation().cancel();
            operation.cancel("Spark Connect server is shutting down");
        }
        Ok(self.inner.connect_runtime.active_operations().subscribe())
    }

    async fn wait_for_connect_work_to_stop(
        mut active: tokio::sync::watch::Receiver<usize>,
    ) -> Result<(), crate::ServerError> {
        tokio::time::timeout(
            CONNECT_GRACEFUL_DRAIN_TIMEOUT + CONNECT_FORCE_CLOSE_TIMEOUT,
            async {
                while *active.borrow() != 0 {
                    active.changed().await.map_err(|_| crate::ServerError {
                        message: "Connect producer completion state is unavailable".into(),
                    })?;
                }
                Ok::<(), crate::ServerError>(())
            },
        )
        .await
        .map_err(|_| crate::ServerError {
            message: "Connect native work did not stop after cancellation".into(),
        })?
    }

    /// Construct the generated Spark Connect service with Sparkless' inbound and outbound
    /// protobuf message caps. Callers choose the listener address and lifecycle independently.
    pub fn connect_service(&self) -> SparkConnectServiceServer<ConnectGrpcService<ConnectService>> {
        SparkConnectServiceServer::new(ConnectGrpcService::new(ConnectService {
            server: self.clone(),
        }))
        .max_decoding_message_size(MAX_CONNECT_PROTO_BYTES)
        .max_encoding_message_size(MAX_CONNECT_PROTO_BYTES)
    }

    /// Serve the Connect adapter on a caller-owned address. This can run beside the HTTP
    /// listener using the same `Server` instance and therefore the same sessions and admission.
    pub async fn serve_connect_until<F>(
        &self,
        bind: SocketAddr,
        shutdown: F,
    ) -> Result<(), crate::ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|error| crate::ServerError {
                message: format!("Spark Connect listener failed to bind {bind}: {error}"),
            })?;
        let bound = listener.local_addr().map_err(crate::ServerError::from)?;
        *self
            .inner
            .connect_runtime
            .address()
            .map_err(|_| crate::ServerError {
                message: "Spark Connect listener address state is unavailable".into(),
            })? = Some(bound);
        let result = self.serve_connect_listener_until(listener, shutdown).await;
        if let Ok(mut address) = self.inner.connect_runtime.address()
            && *address == Some(bound)
        {
            *address = None;
        }
        result
    }

    pub(crate) async fn serve_connect_listener_until<F>(
        &self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), crate::ServerError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        {
            let _lifecycle =
                self.inner
                    .connect_runtime
                    .lifecycle_lock()
                    .map_err(|_| crate::ServerError {
                        message: "Connect session lifecycle state is unavailable".into(),
                    })?;
            self.inner.connect_runtime.set_draining(false);
        }
        let connection_registry = Arc::new(ConnectConnectionRegistry::default());
        let incoming_registry = Arc::clone(&connection_registry);
        let incoming = TcpListenerStream::new(listener)
            .map(move |result| result.and_then(|stream| incoming_registry.track(stream)));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = sparkless_server_connect::serve_with_incoming_shutdown(
            self.connect_service(),
            incoming,
            async move {
                let _ = shutdown_rx.await;
            },
        );
        tokio::pin!(serve);
        tokio::pin!(shutdown);
        let mut reaper = tokio::time::interval(Duration::from_secs(5));
        reaper.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                result = &mut serve => {
                    let active = self.cancel_connect_work_for_shutdown()?;
                    Self::wait_for_connect_work_to_stop(active).await?;
                    return result.map_err(|error| crate::ServerError {
                        message: format!("Spark Connect listener failed: {error}"),
                    });
                }
                _ = &mut shutdown => {
                    let active = self.cancel_connect_work_for_shutdown()?;
                    let _ = shutdown_tx.send(());
                    let drain_listener = async {
                        match tokio::time::timeout(CONNECT_GRACEFUL_DRAIN_TIMEOUT, &mut serve).await {
                            Ok(result) => {
                                return result.map_err(|error| crate::ServerError {
                                    message: format!("Spark Connect listener failed: {error}"),
                                });
                            }
                            Err(_) => {
                                let closed = connection_registry.force_close_all();
                                if closed > 0 {
                                    eprintln!("sparkless-server forced {closed} Spark Connect connection(s) closed after the drain timeout");
                                }
                            }
                        }
                        tokio::time::timeout(CONNECT_FORCE_CLOSE_TIMEOUT, &mut serve)
                            .await
                            .map_err(|_| crate::ServerError {
                                message: "Spark Connect connections did not close after forced shutdown".into(),
                            })?
                            .map_err(|error| crate::ServerError {
                                message: format!("Spark Connect listener failed: {error}"),
                            })
                    };
                    let (listener_result, producer_result) = tokio::join!(
                        drain_listener,
                        Self::wait_for_connect_work_to_stop(active),
                    );
                    listener_result?;
                    producer_result?;
                    return Ok(());
                }
                _ = reaper.tick() => {
                    let _ = self.inner.expire_idle_sessions();
                    self.inner.expire_detached_connect_operations();
                }
            }
        }
    }
}

impl ConnectService {
    async fn acquire_planning_resources(
        &self,
        session: &crate::SessionLease,
        deadline: Instant,
        cancellation: robin_sparkless_core::engine::CancellationToken,
    ) -> Result<
        (
            tokio::sync::OwnedMutexGuard<()>,
            tokio::sync::OwnedSemaphorePermit,
            tokio::sync::OwnedSemaphorePermit,
        ),
        Status,
    > {
        let queue_timeout = self.server.inner.config.queue_timeout;
        let queue_deadline =
            tokio::time::Instant::from_std((Instant::now() + queue_timeout).min(deadline));
        let cancellation_wait = wait_for_cancellation(cancellation.clone());
        tokio::pin!(cancellation_wait);
        let session_guard = tokio::select! {
            _ = &mut cancellation_wait => return Err(Status::cancelled("Connect execution was cancelled while queued")),
            result = tokio::time::timeout_at(queue_deadline, session.0.serial.clone().lock_owned()) => {
                result.map_err(|_| Status::deadline_exceeded("timed out waiting for Connect session planning"))?
            }
        };
        let session_compute_permit = tokio::select! {
            _ = &mut cancellation_wait => return Err(Status::cancelled("Connect execution was cancelled while queued")),
            result = tokio::time::timeout_at(queue_deadline, session.0.connect_execution.clone().acquire_owned()) => {
                result.map_err(|_| Status::deadline_exceeded("timed out waiting for Connect session execution"))?
                    .map_err(|_| Status::unavailable("Connect session execution is closed"))?
            }
        };
        let compute_permit = tokio::select! {
            _ = &mut cancellation_wait => return Err(Status::cancelled("Connect execution was cancelled while queued")),
            result = tokio::time::timeout_at(queue_deadline, self.server.inner.execution.admission().active.clone().acquire_owned()) => {
                result.map_err(|_| Status::deadline_exceeded("timed out waiting for Connect compute admission"))?
                    .map_err(|_| Status::unavailable("Connect compute admission is closed"))?
            }
        };
        Ok((session_guard, compute_permit, session_compute_permit))
    }
}

fn request_reattachable(
    options: &[connect_proto::execute_plan_request::RequestOption],
) -> Result<bool, Status> {
    use connect_proto::execute_plan_request::request_option::RequestOption;
    let mut reattachable = None;
    for option in options {
        match option.request_option.as_ref() {
            Some(RequestOption::ReattachOptions(options)) => {
                if reattachable.is_some() {
                    return Err(Status::invalid_argument(
                        "ExecutePlan may contain only one ReattachOptions value",
                    ));
                }
                reattachable = Some(options.reattachable);
            }
            Some(RequestOption::Extension(_)) => {
                return Err(Status::unimplemented(
                    "ExecutePlan request option extensions are not supported",
                ));
            }
            None => {
                return Err(Status::invalid_argument(
                    "ExecutePlan request option must contain a value",
                ));
            }
        }
    }
    Ok(reattachable.unwrap_or(false))
}

async fn wait_for_cancellation(cancellation: robin_sparkless_core::engine::CancellationToken) {
    while !cancellation.is_cancelled() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn validate_operation_tags(tags: &[String]) -> Result<(), Status> {
    if tags.len() > 128
        || tags.iter().map(String::len).sum::<usize>() > 1024
        || tags
            .iter()
            .any(|tag| tag.is_empty() || tag.contains(',') || tag.len() > 256)
    {
        return Err(Status::invalid_argument(
            "operation tags must be nonempty, at most 256 bytes, and may not contain commas",
        ));
    }
    Ok(())
}

fn connect_execution_failure(message: String) -> Status {
    let lower = message.to_ascii_lowercase();
    if lower.contains("exceed") || lower.contains("limit") || lower.contains("too large") {
        Status::resource_exhausted(message)
    } else {
        Status::internal(format!("CONNECT_EXECUTION_FAILED: {message}"))
    }
}

/// The terminal outcome has its own slot, so a full batch queue cannot discard an error.
#[derive(Clone)]
struct ConnectTerminal {
    sender: Arc<Mutex<Option<tokio::sync::oneshot::Sender<ConnectOutcome>>>>,
    replay: Arc<ConnectReplayRecord>,
}

impl ConnectTerminal {
    fn finish(&self, outcome: ConnectOutcome) -> bool {
        let requested = outcome.clone();
        let outcome = self.replay.finish(outcome);
        let accepted = match (&requested, &outcome) {
            (Ok(()), Ok(())) => true,
            (Err(requested), Err(settled)) => {
                requested.code() == settled.code() && requested.message() == settled.message()
            }
            _ => false,
        };
        self.deliver(outcome) && accepted
    }

    fn finish_deadline(&self, status: Status) -> bool {
        let outcome = self.replay.finish(Err(status));
        let should_cancel = outcome.is_err();
        self.deliver(outcome);
        should_cancel
    }

    fn deliver(&self, outcome: ConnectOutcome) -> bool {
        let Some(sender) = self.sender.lock().ok().and_then(|mut sender| sender.take()) else {
            return false;
        };
        let _ = sender.send(outcome);
        true
    }
}

fn connect_deadline_status() -> Status {
    Status::deadline_exceeded("Connect execution exceeded its response deadline")
}

struct ConnectResponseSender {
    sender: tokio::sync::mpsc::Sender<ConnectQueuedResponse>,
    deadline: Instant,
    queue_timeout: Duration,
    active_compute: Arc<tokio::sync::Semaphore>,
    compute_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    runtime: tokio::runtime::Handle,
    cancellation: robin_sparkless_core::engine::CancellationToken,
    terminal: ConnectTerminal,
    trace_enabled: bool,
    operation_id: String,
    send_blocked_since: Arc<Mutex<Option<Instant>>>,
    planning_wait: Duration,
    planning_time: Duration,
    arrow_next_time: Duration,
    replay_accounting_time: Duration,
    blocked_send_time: Duration,
    client_ack_wait_time: Duration,
    first_batch_grace_time: Duration,
    admission_wait_time: Duration,
    response_count: u64,
    response_rows: u64,
    response_bytes: u64,
}

struct ConnectQueuedResponse {
    response: connect_proto::ExecutePlanResponse,
    reservation_id: u64,
}

impl ConnectResponseSender {
    fn acquire_compute_permit(&mut self) -> Result<(), Status> {
        if self.compute_permit.is_some() {
            return Ok(());
        }
        let started = Instant::now();
        let queue_deadline = (started + self.queue_timeout).min(self.deadline);
        let active_compute = Arc::clone(&self.active_compute);
        let cancellation = self.cancellation.clone();
        let result = self.runtime.block_on(async move {
            let cancellation_wait = wait_for_cancellation(cancellation);
            tokio::pin!(cancellation_wait);
            tokio::select! {
                _ = &mut cancellation_wait => Err(Status::cancelled(
                    "Connect execution was cancelled while waiting for compute admission",
                )),
                result = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(queue_deadline),
                    active_compute.acquire_owned(),
                ) => result
                    .map_err(|_| Status::deadline_exceeded(
                        "timed out waiting for Connect compute admission",
                    ))?
                    .map_err(|_| Status::unavailable("Connect compute admission is closed")),
            }
        });
        self.admission_wait_time += started.elapsed();
        self.compute_permit = Some(result?);
        Ok(())
    }

    fn send(
        &mut self,
        item: Result<connect_proto::ExecutePlanResponse, Status>,
        operation: &crate::ConnectOperationLease,
    ) -> bool {
        let response = match item {
            Ok(response) => response,
            Err(status) => {
                self.terminal.finish(Err(status));
                return false;
            }
        };
        let replay_started = self.trace_enabled.then(Instant::now);
        let completes_operation = matches!(
            response.response_type.as_ref(),
            Some(connect_proto::execute_plan_response::ResponseType::ResultComplete(_))
        );
        let now = Instant::now();
        if now >= self.deadline {
            if self.terminal.finish_deadline(connect_deadline_status()) {
                operation.reject(
                    "CONNECT_RESPONSE_DEADLINE",
                    "Connect execution exceeded its response deadline",
                );
                self.cancellation.cancel();
            }
            return false;
        }
        let sender_bytes = response.encoded_len();
        let response_rows = match response.response_type.as_ref() {
            Some(connect_proto::execute_plan_response::ResponseType::ArrowBatch(batch)) => {
                batch.row_count.try_into().unwrap_or(0)
            }
            _ => 0,
        };
        let response_id = response.response_id.clone();
        let waits_for_client_ack = operation.replay.reattachable
            && matches!(
                response.response_type.as_ref(),
                Some(connect_proto::execute_plan_response::ResponseType::ArrowBatch(_))
            );
        let first_full_batch = waits_for_client_ack
            && response.schema.is_some()
            && response_rows == MAX_CONNECT_ARROW_BATCH_ROWS as u64;
        let reservation_id = match operation.replay.reserve_sender_buffer(sender_bytes) {
            Ok(id) => id,
            Err(status) => {
                operation.reject("CONNECT_REPLAY_CAPACITY", status.message());
                self.terminal.finish(Err(status));
                self.cancellation.cancel();
                return false;
            }
        };
        let appended = if completes_operation {
            operation.replay.append_completion(response.clone())
        } else {
            operation.replay.append(response.clone())
        };
        if let Some(started) = replay_started {
            self.replay_accounting_time += started.elapsed();
        }
        if let Err(status) = appended {
            operation.replay.release_sender_buffer(reservation_id);
            operation.reject("CONNECT_REPLAY_CAPACITY", status.message());
            self.terminal.finish(Err(status));
            self.cancellation.cancel();
            return false;
        }
        let blocked = self.sender.capacity() == 0;
        let send_started = Instant::now();
        if blocked && let Ok(mut blocked_since) = self.send_blocked_since.lock() {
            *blocked_since = Some(send_started);
        }
        let result = self.sender.blocking_send(ConnectQueuedResponse {
            response,
            reservation_id,
        });
        if blocked && let Ok(mut blocked_since) = self.send_blocked_since.lock() {
            *blocked_since = None;
        }
        let blocked_send_time = if blocked {
            send_started.elapsed()
        } else {
            Duration::ZERO
        };
        match result {
            Ok(()) => {
                if completes_operation {
                    operation.complete();
                    self.terminal.finish(Ok(()));
                }
                self.record_sent_response(sender_bytes, response_rows, blocked_send_time);
                if waits_for_client_ack {
                    drop(self.compute_permit.take());
                    let ack_started = Instant::now();
                    let ack_deadline =
                        (ack_started + CONNECT_RESPONSE_STALL_TIMEOUT).min(self.deadline);
                    let acknowledged = operation.replay.wait_for_response_release(
                        &response_id,
                        ack_deadline,
                        &self.cancellation,
                    );
                    self.client_ack_wait_time += ack_started.elapsed();
                    if let Err(status) = acknowledged {
                        if self.cancellation.is_cancelled() {
                            return false;
                        }
                        operation.reject("CONNECT_RESPONSE_ACK_TIMEOUT", status.message());
                        self.terminal.finish(Err(status));
                        self.cancellation.cancel();
                        return false;
                    }
                    if first_full_batch {
                        // Let callers observe the first full batch and interrupt before the
                        // producer fills the public client's response queue.
                        let grace_started = Instant::now();
                        let grace = operation.replay.wait_for_cancellation_grace(
                            CONNECT_FIRST_BATCH_ACK_GRACE,
                            self.deadline,
                            &self.cancellation,
                        );
                        self.first_batch_grace_time += grace_started.elapsed();
                        if let Err(status) = grace {
                            if self.cancellation.is_cancelled() {
                                return false;
                            }
                            self.terminal.finish(Err(status));
                            self.cancellation.cancel();
                            return false;
                        }
                    }
                }
                true
            }
            Err(queued) => {
                operation
                    .replay
                    .release_sender_buffer(queued.0.reservation_id);
                if operation.replay.reattachable && !self.cancellation.is_cancelled() {
                    if completes_operation {
                        operation.complete();
                        self.terminal.finish(Ok(()));
                    }
                    self.record_sent_response(sender_bytes, response_rows, blocked_send_time);
                    true
                } else {
                    self.cancellation.cancel();
                    self.blocked_send_time += blocked_send_time;
                    false
                }
            }
        }
    }

    fn record_sent_response(&mut self, bytes: usize, rows: u64, blocked_send_time: Duration) {
        self.blocked_send_time += blocked_send_time;
        self.response_count = self.response_count.saturating_add(1);
        self.response_bytes = self
            .response_bytes
            .saturating_add(bytes.try_into().unwrap_or(u64::MAX));
        self.response_rows = self.response_rows.saturating_add(rows);
    }
}

impl Drop for ConnectResponseSender {
    fn drop(&mut self) {
        if self.trace_enabled {
            eprintln!(
                "connect_trace event=producer_phases operation_id={} admission_wait_ms={:.3} planning_ms={:.3} arrow_next_ms={:.3} replay_accounting_ms={:.3} blocked_send_ms={:.3} client_ack_wait_ms={:.3} first_batch_grace_ms={:.3} admission_reacquire_ms={:.3} responses={} rows={} response_bytes={}",
                self.operation_id,
                self.planning_wait.as_secs_f64() * 1_000.0,
                self.planning_time.as_secs_f64() * 1_000.0,
                self.arrow_next_time.as_secs_f64() * 1_000.0,
                self.replay_accounting_time.as_secs_f64() * 1_000.0,
                self.blocked_send_time.as_secs_f64() * 1_000.0,
                self.client_ack_wait_time.as_secs_f64() * 1_000.0,
                self.first_batch_grace_time.as_secs_f64() * 1_000.0,
                self.admission_wait_time.as_secs_f64() * 1_000.0,
                self.response_count,
                self.response_rows,
                self.response_bytes,
            );
        }
    }
}

struct ConnectExecutionStream {
    receiver: tokio::sync::mpsc::Receiver<ConnectQueuedResponse>,
    cancellation_waker: tokio::sync::mpsc::UnboundedReceiver<()>,
    outcome: Option<tokio::sync::oneshot::Receiver<ConnectOutcome>>,
    cancellation: robin_sparkless_core::engine::CancellationToken,
    replay: Arc<ConnectReplayRecord>,
    _session: crate::SessionLease,
    _sender_permit: tokio::sync::OwnedSemaphorePermit,
    pending_sender_reservation: u64,
    ended: bool,
}

impl tonic::codegen::tokio_stream::Stream for ConnectExecutionStream {
    type Item = Result<connect_proto::ExecutePlanResponse, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        let this = self.get_mut();
        if this.pending_sender_reservation != 0 {
            this.replay
                .release_sender_buffer(std::mem::take(&mut this.pending_sender_reservation));
        }
        if this.ended {
            return Poll::Ready(None);
        }
        match std::pin::Pin::new(&mut this.cancellation_waker).poll_recv(cx) {
            Poll::Ready(Some(())) => {
                if let Some(status) = this.replay.cancellation_terminal_status() {
                    this.ended = true;
                    this.receiver.close();
                    this.outcome = None;
                    this.replay.clear_execution_waker();
                    while let Ok(queued) = this.receiver.try_recv() {
                        this.replay.release_sender_buffer(queued.reservation_id);
                    }
                    return Poll::Ready(Some(Err(status)));
                }
            }
            Poll::Ready(None) | Poll::Pending => {}
        }
        if let Some(outcome) = this.outcome.as_mut() {
            match std::pin::Pin::new(outcome).poll(cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(Ok(()))) => this.outcome = None,
                Poll::Ready(result) => {
                    this.ended = true;
                    this.receiver.close();
                    this.cancellation.cancel();
                    this.replay.clear_execution_waker();
                    while let Ok(queued) = this.receiver.try_recv() {
                        this.replay.release_sender_buffer(queued.reservation_id);
                    }
                    let status = match result {
                        Ok(Err(status)) => status,
                        _ => Status::internal("Connect producer ended without a terminal outcome"),
                    };
                    return Poll::Ready(Some(Err(status)));
                }
            }
        }
        match this.receiver.poll_recv(cx) {
            Poll::Ready(Some(queued)) => {
                this.pending_sender_reservation = queued.reservation_id;
                Poll::Ready(Some(Ok(queued.response)))
            }
            Poll::Ready(None) if this.outcome.is_some() => Poll::Pending,
            Poll::Ready(None) => {
                this.ended = true;
                this.replay.clear_execution_waker();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ConnectExecutionStream {
    fn drop(&mut self) {
        self.receiver.close();
        self.replay.clear_execution_waker();
        self.replay
            .release_sender_buffer(self.pending_sender_reservation);
        while let Ok(queued) = self.receiver.try_recv() {
            self.replay.release_sender_buffer(queued.reservation_id);
        }
        if self.replay.reattachable {
            self.replay.detach();
        } else {
            self.cancellation.cancel();
        }
    }
}

struct ConnectReplayStream {
    record: Arc<ConnectReplayRecord>,
    receiver: tokio::sync::mpsc::Receiver<()>,
    cursor: u64,
    _session: crate::SessionLease,
    _sender_permit: tokio::sync::OwnedSemaphorePermit,
    pending_sender_reservation: u64,
    ended: bool,
}

impl tonic::codegen::tokio_stream::Stream for ConnectReplayStream {
    type Item = Result<connect_proto::ExecutePlanResponse, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        let this = self.get_mut();
        if this.pending_sender_reservation != 0 {
            this.record
                .release_sender_buffer(std::mem::take(&mut this.pending_sender_reservation));
        }
        if this.ended {
            return Poll::Ready(None);
        }
        loop {
            match this.record.next_response_for_sender(this.cursor) {
                Ok(ReplayPoll::Response(response, reservation_id)) => {
                    this.pending_sender_reservation = reservation_id;
                    this.cursor = this.cursor.saturating_add(1);
                    return Poll::Ready(Some(Ok(*response)));
                }
                Ok(ReplayPoll::Error(status)) => {
                    this.ended = true;
                    return Poll::Ready(Some(Err(status)));
                }
                Ok(ReplayPoll::Complete) => {
                    this.ended = true;
                    return Poll::Ready(None);
                }
                Err(status) => {
                    this.ended = true;
                    return Poll::Ready(Some(Err(status)));
                }
                Ok(ReplayPoll::Pending) => match this.receiver.poll_recv(cx) {
                    Poll::Ready(Some(())) => continue,
                    Poll::Ready(None) | Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

impl Drop for ConnectReplayStream {
    fn drop(&mut self) {
        self.record
            .release_sender_buffer(self.pending_sender_reservation);
        self.record.detach();
    }
}

struct ConnectAdmissionGuard {
    replay: Arc<ConnectReplayRecord>,
    armed: bool,
}

impl ConnectAdmissionGuard {
    fn new(replay: Arc<ConnectReplayRecord>) -> Self {
        Self {
            replay,
            armed: true,
        }
    }

    fn hand_off(&mut self) {
        self.armed = false;
    }
}

impl Drop for ConnectAdmissionGuard {
    fn drop(&mut self) {
        if self.armed {
            self.replay.detach();
        }
    }
}

struct ConnectProducerFinished {
    sender: Option<tokio::sync::oneshot::Sender<()>>,
    replay: Arc<ConnectReplayRecord>,
}

impl Drop for ConnectProducerFinished {
    fn drop(&mut self) {
        self.replay.mark_producer_stopped();
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn start_connect_producer(
    operation: crate::ConnectOperationLease,
    execution: Arc<sparkless_server_runtime::ServerRuntime<crate::SessionLifetime>>,
    session: robin_sparkless::SparkSession,
    plan: connect_proto::Plan,
    planning_resources: (
        tokio::sync::OwnedMutexGuard<()>,
        tokio::sync::OwnedSemaphorePermit,
        tokio::sync::OwnedSemaphorePermit,
    ),
    client_session_id: String,
    server_side_session_id: String,
    operation_id: String,
    reattachable: bool,
    cancellation: robin_sparkless_core::engine::CancellationToken,
    sender: tokio::sync::mpsc::Sender<ConnectQueuedResponse>,
    active_compute: Arc<tokio::sync::Semaphore>,
    queue_timeout: Duration,
    deadline: Instant,
    planning_wait: Duration,
) -> tokio::sync::oneshot::Receiver<ConnectOutcome> {
    let producer_sequence = CONNECT_PRODUCER_STARTS.fetch_add(1, Ordering::Relaxed) + 1;
    if std::env::var_os("SPARKLESS_CONNECT_TRACE").as_deref() == Some(std::ffi::OsStr::new("1")) {
        eprintln!("connect_trace event=producer_started sequence={producer_sequence}");
    }
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
    let replay = Arc::clone(&operation.replay);
    let terminal = ConnectTerminal {
        sender: Arc::new(Mutex::new(Some(outcome_tx))),
        replay: Arc::clone(&replay),
    };
    let send_blocked_since: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let runtime = tokio::runtime::Handle::current();
    let timer_cancellation = cancellation.clone();
    let timer_terminal = terminal.clone();
    let mark_deadline = operation.deadline_marker();
    let mut producer_done = replay.producer_done();
    let stall_replay = Arc::clone(&replay);
    let stall_cancellation = cancellation.clone();
    let stall_operation = Arc::clone(&operation.record);
    let stall_send_blocked_since = Arc::clone(&send_blocked_since);
    let stall_terminal = terminal.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if stall_replay.fail_if_detached_stalled(Instant::now()) {
                        if let Ok(mut record) = stall_operation.lock()
                            && record.execution_state == "running"
                            && record.effect_outcome == "pending"
                        {
                            record.execution_state = "failed";
                            record.effect_outcome = "rejected";
                            record.error = Some((
                                "CONNECT_REPLAY_STALL".to_string(),
                                "Connect detached response stream made no progress for 5 seconds".to_string(),
                            ));
                        }
                        stall_cancellation.cancel();
                        break;
                    }
                    let send_stalled = stall_send_blocked_since
                        .lock()
                        .ok()
                        .and_then(|blocked_since| *blocked_since)
                        .is_some_and(|blocked_since| {
                            blocked_since.elapsed() >= CONNECT_RESPONSE_STALL_TIMEOUT
                        });
                    if send_stalled {
                        let status = Status::deadline_exceeded(
                            "Connect response stream stalled without progress for 5 seconds",
                        );
                        if stall_terminal.finish_deadline(status.clone()) {
                            if let Ok(mut record) = stall_operation.lock()
                                && record.execution_state == "running"
                                && record.effect_outcome == "pending"
                            {
                                record.execution_state = "failed";
                                record.effect_outcome = "rejected";
                                record.error = Some((
                                    "CONNECT_REPLAY_STALL".to_string(),
                                    status.message().to_string(),
                                ));
                            }
                            stall_cancellation.cancel();
                        }
                        break;
                    }
                }
                changed = producer_done.changed() => {
                    if changed.is_err() || *producer_done.borrow() {
                        break;
                    }
                }
            }
        }
    });
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                if timer_terminal.finish_deadline(connect_deadline_status()) {
                    mark_deadline();
                    timer_cancellation.cancel();
                }
            }
            _ = finished_rx => {}
        }
    });
    tokio::task::spawn_blocking(move || {
        let _finished = ConnectProducerFinished {
            sender: Some(finished_tx),
            replay,
        };
        let (session_guard, compute_permit, session_compute_permit) = planning_resources;
        let mut sender = ConnectResponseSender {
            sender,
            deadline,
            queue_timeout,
            active_compute,
            compute_permit: Some(compute_permit),
            runtime,
            cancellation: cancellation.clone(),
            terminal,
            trace_enabled: std::env::var_os("SPARKLESS_CONNECT_TRACE").as_deref()
                == Some(std::ffi::OsStr::new("1")),
            operation_id: operation_id.clone(),
            send_blocked_since,
            planning_wait,
            planning_time: Duration::ZERO,
            arrow_next_time: Duration::ZERO,
            replay_accounting_time: Duration::ZERO,
            blocked_send_time: Duration::ZERO,
            client_ack_wait_time: Duration::ZERO,
            first_batch_grace_time: Duration::ZERO,
            admission_wait_time: Duration::ZERO,
            response_count: 0,
            response_rows: 0,
            response_bytes: 0,
        };
        let session_handle = EngineSessionHandle::new("polars", session);
        let planning_started = sender.trace_enabled.then(Instant::now);
        let translated = sparkless_server_connect::execute_plan(
            &execution,
            &session_handle,
            ConnectExecutionRequest {
                serialized_plan: plan.encode_to_vec(),
                max_batch_rows: MAX_CONNECT_ARROW_BATCH_ROWS,
                max_response_bytes: MAX_CONNECT_ARROW_RESPONSE_BYTES,
                cancellation: cancellation.clone(),
            },
        );
        if let Some(started) = planning_started {
            sender.planning_time += started.elapsed();
        }
        drop(session_guard);
        let _session_compute_permit = session_compute_permit;
        let translated = match translated {
            Ok(translated) => translated,
            Err(error) => {
                operation.reject(error.code, &error.message);
                let status_code = error
                    .details
                    .as_ref()
                    .and_then(|value| value.get("connect_status_code"))
                    .and_then(serde_json::Value::as_i64)
                    .and_then(|value| i32::try_from(value).ok())
                    .map(tonic::Code::from_i32)
                    .unwrap_or(tonic::Code::Internal);
                let status = Status::new(status_code, error.message);
                let _ = sender.send(Err(status), &operation);
                return;
            }
        };
        let schema = match connect_proto::DataType::decode(translated.schema.as_slice()) {
            Ok(schema) => schema,
            Err(error) => {
                let status = Status::internal(format!(
                    "execution adapter returned an invalid schema: {error}"
                ));
                operation.reject("CONNECT_SCHEMA_TRANSLATION_FAILED", status.message());
                let _ = sender.send(Err(status), &operation);
                return;
            }
        };
        let mut batches = translated.batches;
        let mut row_offset = 0u64;
        let mut response_bytes = 0usize;
        let mut first_batch = true;
        loop {
            if let Err(status) = sender.acquire_compute_permit() {
                if !cancellation.is_cancelled() {
                    operation.reject("CONNECT_COMPUTE_ADMISSION_TIMEOUT", status.message());
                }
                let _ = sender.send(Err(status), &operation);
                return;
            }
            if sender.sender.is_closed() && !operation.replay.reattachable {
                cancellation.cancel();
                return;
            }
            let arrow_started = sender.trace_enabled.then(Instant::now);
            let next_batch = batches.next();
            if let Some(started) = arrow_started {
                sender.arrow_next_time += started.elapsed();
            }
            let Some(batch) = next_batch else {
                break;
            };
            let batch = match batch {
                Ok(batch) => batch,
                Err(error) => {
                    let status = connect_execution_failure(error.to_string());
                    operation.reject("CONNECT_ARROW_STREAM_FAILED", status.message());
                    let _ = sender.send(Err(status), &operation);
                    return;
                }
            };
            if batch.row_count > MAX_CONNECT_ARROW_BATCH_ROWS as u64 {
                let status = Status::resource_exhausted(format!(
                    "Connect Arrow batch exceeds the {MAX_CONNECT_ARROW_BATCH_ROWS}-row limit"
                ));
                operation.reject("CONNECT_ARROW_ROW_LIMIT", status.message());
                let _ = sender.send(Err(status), &operation);
                return;
            }
            let Some(next_row_offset) = row_offset.checked_add(batch.row_count) else {
                let status = Status::resource_exhausted("Connect result row count overflow");
                operation.reject("CONNECT_RESULT_ROW_LIMIT", status.message());
                let _ = sender.send(Err(status), &operation);
                return;
            };
            if next_row_offset > MAX_CONNECT_RESULT_ROWS {
                let status = Status::resource_exhausted(format!(
                    "Connect result exceeds {MAX_CONNECT_RESULT_ROWS} rows"
                ));
                operation.reject("CONNECT_RESULT_ROW_LIMIT", status.message());
                let _ = sender.send(Err(status), &operation);
                return;
            }
            let row_count = match i64::try_from(batch.row_count) {
                Ok(row_count) => row_count,
                Err(_) => {
                    let status =
                        Status::resource_exhausted("Connect Arrow row count is out of range");
                    operation.reject("CONNECT_ARROW_ROW_LIMIT", status.message());
                    let _ = sender.send(Err(status), &operation);
                    return;
                }
            };
            let response = connect_proto::ExecutePlanResponse {
                session_id: client_session_id.clone(),
                server_side_session_id: server_side_session_id.clone(),
                operation_id: operation_id.clone(),
                response_id: Uuid::new_v4().to_string(),
                metrics: None,
                observed_metrics: Vec::new(),
                schema: first_batch.then(|| schema.clone()),
                response_type: Some(
                    connect_proto::execute_plan_response::ResponseType::ArrowBatch(
                        connect_proto::execute_plan_response::ArrowBatch {
                            row_count,
                            data: batch.bytes,
                            start_offset: Some(row_offset as i64),
                        },
                    ),
                ),
            };
            let encoded_bytes = response.encoded_len();
            if encoded_bytes > MAX_CONNECT_ARROW_RESPONSE_BYTES {
                let status = Status::resource_exhausted(format!(
                    "Connect Arrow response is {encoded_bytes} bytes, exceeding the {MAX_CONNECT_ARROW_RESPONSE_BYTES}-byte limit"
                ));
                operation.reject("CONNECT_ARROW_BYTE_LIMIT", status.message());
                let _ = sender.send(Err(status), &operation);
                return;
            }
            trace_connect_arrow_response(encoded_bytes, row_count, row_offset);
            let Some(next_response_bytes) = response_bytes.checked_add(encoded_bytes) else {
                let status = Status::resource_exhausted("Connect result byte count overflow");
                operation.reject("CONNECT_RESULT_BYTE_LIMIT", status.message());
                let _ = sender.send(Err(status), &operation);
                return;
            };
            if next_response_bytes > MAX_CONNECT_RESULT_BYTES {
                let status = Status::resource_exhausted(format!(
                    "Connect result exceeds {} serialized bytes",
                    MAX_CONNECT_RESULT_BYTES
                ));
                operation.reject("CONNECT_RESULT_BYTE_LIMIT", status.message());
                let _ = sender.send(Err(status), &operation);
                return;
            }
            response_bytes = next_response_bytes;
            row_offset = next_row_offset;
            first_batch = false;
            if !sender.send(Ok(response), &operation) {
                return;
            }
        }
        if first_batch {
            let status = Status::internal("Connect engine returned no schema-bearing Arrow batch");
            operation.reject("CONNECT_ARROW_STREAM_EMPTY", status.message());
            let _ = sender.send(Err(status), &operation);
            return;
        }
        if reattachable {
            let response = connect_proto::ExecutePlanResponse {
                session_id: client_session_id,
                server_side_session_id,
                operation_id,
                response_id: Uuid::new_v4().to_string(),
                metrics: None,
                observed_metrics: Vec::new(),
                schema: None,
                response_type: Some(
                    connect_proto::execute_plan_response::ResponseType::ResultComplete(
                        connect_proto::execute_plan_response::ResultComplete {},
                    ),
                ),
            };
            if response_bytes.saturating_add(response.encoded_len()) > MAX_CONNECT_RESULT_BYTES {
                let status =
                    Status::resource_exhausted("Connect result exceeds the serialized byte limit");
                operation.reject("CONNECT_RESULT_BYTE_LIMIT", status.message());
                let _ = sender.send(Err(status), &operation);
                return;
            }
            let _ = sender.send(Ok(response), &operation);
        } else if sender.terminal.finish(Ok(())) {
            operation.complete();
        }
    });
    outcome_rx
}

#[tonic::async_trait]
impl ConnectApplication for ConnectService {
    type ExecutePlanStream = ConnectResponseStream;
    type ReattachExecuteStream = ConnectResponseStream;

    async fn execute_plan(
        &self,
        request: Request<connect_proto::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        trace_connect_rpc("ExecutePlan");
        let mut request = request.into_inner();
        validate_request_budget("ExecutePlanRequest", &request)?;
        if request.plan.is_none() {
            return Err(Status::invalid_argument(
                "ExecutePlanRequest.plan is required",
            ));
        }
        let reattachable = request_reattachable(&request.request_options)?;
        validate_operation_tags(&request.tags)?;
        let sender_permit = Arc::clone(self.server.inner.connect_runtime.stream_senders())
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted("Connect live-stream sender capacity is full")
            })?;
        let client_session_id = request.session_id.clone();
        let fingerprint = Sha256::digest(request.encode_to_vec()).into();
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let operation_id = request
            .operation_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let deadline = Instant::now() + self.server.inner.config.response_timeout;
        let operation = match self.server.inner.reserve_connect_operation(
            &identity,
            &operation_id,
            fingerprint,
            reattachable,
            request.tags.clone(),
        ) {
            Ok(operation) => operation,
            Err(ConnectReserveError::Request(error)) => return Err(status_from_failure(error)),
            Err(ConnectReserveError::Duplicate) => {
                return Err(Status::already_exists(
                    "INVALID_HANDLE.OPERATION_ALREADY_EXISTS: operation id is already retained",
                ));
            }
            Err(ConnectReserveError::Conflict) => {
                return Err(Status::invalid_argument(
                    "operation id is already bound to another Connect request or session generation",
                ));
            }
        };
        let mut admission_guard = ConnectAdmissionGuard::new(Arc::clone(&operation.replay));
        let plan = request
            .plan
            .take()
            .expect("plan presence was validated above");
        let operation_replay = Arc::clone(&operation.replay);
        let cancellation = operation_replay.cancellation();
        let stream_session = operation._session.clone();
        let planning_started = Instant::now();
        let planning_resources = match self
            .acquire_planning_resources(&operation._session, deadline, cancellation.clone())
            .await
        {
            Ok(resources) => resources,
            Err(status) => {
                operation.reject("CONNECT_PLAN_QUEUE_TIMEOUT", status.message());
                let _ = operation.replay.finish(Err(status.clone()));
                return Err(status);
            }
        };
        let planning_wait = planning_started.elapsed();
        let session = operation._session.0.session.clone();
        let server_side_session_id = operation._session.0.server_side_session_id.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let outcome = start_connect_producer(
            operation,
            Arc::clone(&self.server.inner.execution),
            session,
            plan,
            planning_resources,
            client_session_id,
            server_side_session_id,
            operation_id,
            reattachable,
            cancellation.clone(),
            sender,
            Arc::clone(&self.server.inner.execution.admission().active),
            self.server.inner.config.queue_timeout,
            deadline,
            planning_wait,
        );
        let stream = ConnectExecutionStream {
            receiver,
            cancellation_waker: operation_replay.attach_execution_waker(),
            outcome: Some(outcome),
            cancellation,
            replay: Arc::clone(&operation_replay),
            _session: stream_session,
            _sender_permit: sender_permit,
            pending_sender_reservation: 0,
            ended: false,
        };
        admission_guard.hand_off();
        Ok(Response::new(Box::pin(stream)))
    }

    async fn analyze_plan(
        &self,
        request: Request<connect_proto::AnalyzePlanRequest>,
    ) -> Result<Response<connect_proto::AnalyzePlanResponse>, Status> {
        trace_connect_rpc("AnalyzePlan");
        let request = request.into_inner();
        validate_request_budget("AnalyzePlanRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let admission = self
            .server
            .inner
            .reserve_admission()
            .map_err(status_from_failure)?;
        let deadline = Instant::now() + self.server.inner.config.response_timeout;
        let session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        let analysis_token = robin_sparkless_core::engine::CancellationToken::new();
        let _cancel_on_drop = CancelTokenOnDrop(analysis_token.clone());
        let planning_resources = self
            .acquire_planning_resources(&session, deadline, analysis_token.clone())
            .await?;
        let session_id = identity.client_session_id;
        let server_side_session_id = session.0.server_side_session_id.clone();
        let worker_token = analysis_token.clone();
        let mut planning = tokio::task::spawn_blocking(move || {
            let _admission = admission;
            let (_session_guard, _compute_permit, _session_compute_permit) = planning_resources;
            session.0.session.sync_thread_context();
            let response = analysis::response(
                &session_id,
                &server_side_session_id,
                &session.0.session,
                request.analyze,
                &worker_token,
            )?;
            Ok::<_, Status>(response)
        });
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), &mut planning).await
        {
            Ok(Ok(Ok(response))) => Ok(Response::new(response)),
            Ok(Ok(Err(status))) => Err(status),
            Ok(Err(error)) => Err(Status::internal(format!(
                "Connect analysis worker failed: {error}"
            ))),
            Err(_) => {
                analysis_token.cancel();
                let _ = planning.await;
                Err(Status::deadline_exceeded(
                    "Connect analysis exceeded its deadline",
                ))
            }
        }
    }

    async fn config(
        &self,
        request: Request<connect_proto::ConfigRequest>,
    ) -> Result<Response<connect_proto::ConfigResponse>, Status> {
        trace_connect_rpc("Config");
        let request = request.into_inner();
        validate_request_budget("ConfigRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        let response = config::response(
            &identity.client_session_id,
            &session.0.server_side_session_id,
            &session.0.session,
            request.operation,
        )?;
        Ok(Response::new(response))
    }

    async fn add_artifacts(
        &self,
        _request: Request<tonic::Streaming<connect_proto::AddArtifactsRequest>>,
    ) -> Result<Response<connect_proto::AddArtifactsResponse>, Status> {
        trace_connect_rpc("AddArtifacts");
        Err(Status::unimplemented(
            "AddArtifacts and cached local relations are outside the 4.18 Connect subset",
        ))
    }

    async fn artifact_status(
        &self,
        _request: Request<connect_proto::ArtifactStatusesRequest>,
    ) -> Result<Response<connect_proto::ArtifactStatusesResponse>, Status> {
        trace_connect_rpc("ArtifactStatus");
        Err(Status::unimplemented(
            "ArtifactStatus and cached local relations are outside the 4.18 Connect subset",
        ))
    }

    async fn interrupt(
        &self,
        request: Request<connect_proto::InterruptRequest>,
    ) -> Result<Response<connect_proto::InterruptResponse>, Status> {
        trace_connect_rpc("Interrupt");
        let request = request.into_inner();
        validate_request_budget("InterruptRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        let interrupt_type =
            connect_proto::interrupt_request::InterruptType::try_from(request.interrupt_type)
                .map_err(|_| Status::invalid_argument("unknown Connect interrupt type"))?;
        let target = match (interrupt_type, request.interrupt) {
            (connect_proto::interrupt_request::InterruptType::All, None) => InterruptTarget::All,
            (
                connect_proto::interrupt_request::InterruptType::Tag,
                Some(connect_proto::interrupt_request::Interrupt::OperationTag(tag)),
            ) if !tag.is_empty() && !tag.contains(',') && tag.len() <= 256 => {
                InterruptTarget::Tag(tag)
            }
            (
                connect_proto::interrupt_request::InterruptType::OperationId,
                Some(connect_proto::interrupt_request::Interrupt::OperationId(operation_id)),
            ) => {
                validate_uuid("operation_id", &operation_id)?;
                InterruptTarget::Operation(operation_id)
            }
            (connect_proto::interrupt_request::InterruptType::All, Some(_)) => {
                return Err(Status::invalid_argument(
                    "session-wide interrupt must not include an operation target",
                ));
            }
            (connect_proto::interrupt_request::InterruptType::Tag, _) => {
                return Err(Status::invalid_argument(
                    "tag interrupt requires a nonempty operation_tag without commas",
                ));
            }
            (connect_proto::interrupt_request::InterruptType::OperationId, _) => {
                return Err(Status::invalid_argument(
                    "operation interrupt requires operation_id",
                ));
            }
            (connect_proto::interrupt_request::InterruptType::Unspecified, _) => {
                return Err(Status::invalid_argument(
                    "Connect interrupt_type must be specified",
                ));
            }
        };
        let operations = self
            .server
            .inner
            .connect_operations_for_session(&identity, session.0.generation);
        let mut interrupted_ids = Vec::new();
        for operation in operations {
            let matches = match &target {
                InterruptTarget::All => true,
                InterruptTarget::Tag(tag) => operation.tags.contains(tag),
                InterruptTarget::Operation(id) => operation.operation_id == *id,
            };
            if matches && operation.cancel("Connect execution was interrupted") {
                interrupted_ids.push(operation.operation_id.clone());
            }
        }
        interrupted_ids.sort();
        Ok(Response::new(connect_proto::InterruptResponse {
            session_id: identity.client_session_id,
            server_side_session_id: session.0.server_side_session_id.clone(),
            interrupted_ids,
        }))
    }

    async fn reattach_execute(
        &self,
        request: Request<connect_proto::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        trace_connect_rpc("ReattachExecute");
        let request = request.into_inner();
        validate_request_budget("ReattachExecuteRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let sender_permit = Arc::clone(self.server.inner.connect_runtime.stream_senders())
            .try_acquire_owned()
            .map_err(|_| {
                Status::resource_exhausted("Connect live-stream sender capacity is full")
            })?;
        let session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        validate_uuid("operation_id", &request.operation_id)?;
        let operation = self
            .server
            .inner
            .connect_operation(&identity, &request.operation_id)
            .ok_or_else(|| {
                status_with_standard_details(Status::not_found(
                    "INVALID_HANDLE.OPERATION_NOT_FOUND: operation is not retained",
                ))
            })?;
        if !operation.matches_session(&identity.registry_key(), session.0.generation) {
            return Err(Status::failed_precondition(
                "INVALID_HANDLE.SESSION_CHANGED: operation belongs to a stale session generation",
            ));
        }
        if let Some(response_id) = request.last_response_id.as_deref() {
            validate_uuid("last_response_id", response_id)?;
        }
        let (cursor, receiver) = operation.attach(request.last_response_id.as_deref())?;
        let stream = ConnectReplayStream {
            record: operation,
            receiver,
            cursor,
            _session: session,
            _sender_permit: sender_permit,
            pending_sender_reservation: 0,
            ended: false,
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn release_execute(
        &self,
        request: Request<connect_proto::ReleaseExecuteRequest>,
    ) -> Result<Response<connect_proto::ReleaseExecuteResponse>, Status> {
        trace_connect_rpc("ReleaseExecute");
        let request = request.into_inner();
        validate_request_budget("ReleaseExecuteRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        validate_uuid("operation_id", &request.operation_id)?;
        let operation = self
            .server
            .inner
            .connect_operation(&identity, &request.operation_id)
            .filter(|operation| {
                operation.matches_session(&identity.registry_key(), session.0.generation)
            });
        let Some(release) = request.release else {
            return Err(Status::invalid_argument(
                "ReleaseExecuteRequest.release must be specified",
            ));
        };
        let release_all = matches!(
            &release,
            connect_proto::release_execute_request::Release::ReleaseAll(_)
        );
        let operation_key = crate::connect_operation_key(&identity, &request.operation_id);
        if let Some(operation) = operation.as_ref() {
            match release {
                connect_proto::release_execute_request::Release::ReleaseAll(_) => {
                    // A client can acknowledge completion even when it did not request
                    // reattachment. ReleaseAll frees both replay and idempotency history;
                    // only prefix releases require a reattachable stream.
                    let mut producer_done = operation.producer_done();
                    operation.release_all();
                    if !*producer_done.borrow() {
                        let timeout = self.server.inner.config.response_timeout;
                        let stopped = tokio::time::timeout(timeout, async {
                            while !*producer_done.borrow() {
                                if producer_done.changed().await.is_err() {
                                    break;
                                }
                            }
                        })
                        .await;
                        if stopped.is_err() {
                            return Err(Status::deadline_exceeded(
                                "timed out waiting for released Connect execution to stop",
                            ));
                        }
                    }
                    if !operation.has_error()
                        && let Ok(mut operations) = self.server.inner.connect_runtime.operations()
                    {
                        operations.remove(&operation.operation_key);
                    }
                }
                connect_proto::release_execute_request::Release::ReleaseUntil(until) => {
                    if !operation.reattachable {
                        return Err(Status::failed_precondition(
                            "INVALID_HANDLE.OPERATION_NOT_REATTACHABLE: operation cannot release response prefixes",
                        ));
                    }
                    validate_uuid("response_id", &until.response_id)?;
                    operation.release_until(&until.response_id);
                }
            }
        }
        if release_all {
            self.server.inner.release_operation_record(
                &operation_key,
                &session.0.id,
                session.0.generation,
            );
        }
        Ok(Response::new(connect_proto::ReleaseExecuteResponse {
            session_id: identity.client_session_id,
            server_side_session_id: session.0.server_side_session_id.clone(),
            operation_id: Some(request.operation_id),
        }))
    }

    async fn release_session(
        &self,
        request: Request<connect_proto::ReleaseSessionRequest>,
    ) -> Result<Response<connect_proto::ReleaseSessionResponse>, Status> {
        trace_connect_rpc("ReleaseSession");
        let request = request.into_inner();
        validate_request_budget("ReleaseSessionRequest", &request)?;
        let identity = session_identity(request.user_context, request.session_id, None)?;
        let server_side_session_id = self
            .server
            .inner
            .release_connect_session(&identity, request.allow_reconnect)
            .map_err(status_from_failure)?;
        Ok(Response::new(connect_proto::ReleaseSessionResponse {
            session_id: identity.client_session_id,
            server_side_session_id,
        }))
    }

    async fn fetch_error_details(
        &self,
        request: Request<connect_proto::FetchErrorDetailsRequest>,
    ) -> Result<Response<connect_proto::FetchErrorDetailsResponse>, Status> {
        trace_connect_rpc("FetchErrorDetails");
        let request = request.into_inner();
        validate_request_budget("FetchErrorDetailsRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        let error = self
            .server
            .inner
            .connect_operations_for_session(&identity, session.0.generation)
            .into_iter()
            .find_map(|operation| operation.error_details(&request.error_id));
        Ok(Response::new(connect_proto::FetchErrorDetailsResponse {
            server_side_session_id: session.0.server_side_session_id.clone(),
            session_id: identity.client_session_id,
            root_error_idx: error.as_ref().map(|_| 0),
            errors: error.into_iter().collect(),
        }))
    }
}

fn session_identity(
    user_context: Option<connect_proto::UserContext>,
    session_id: String,
    observed_server_session_id: Option<String>,
) -> Result<ConnectSessionIdentity, Status> {
    let user_context =
        user_context.ok_or_else(|| Status::invalid_argument("user_context is required"))?;
    validate_user_id(&user_context.user_id)?;
    validate_uuid("session_id", &session_id)?;
    if let Some(observed) = observed_server_session_id.as_deref() {
        validate_uuid("client_observed_server_side_session_id", observed)?;
    }
    Ok(ConnectSessionIdentity {
        user_id: user_context.user_id,
        client_session_id: session_id,
        observed_server_session_id,
    })
}

fn validate_user_id(value: &str) -> Result<(), Status> {
    if value.is_empty() || value.len() > 128 {
        return Err(Status::invalid_argument(
            "user_id must be 1-128 UTF-8 bytes",
        ));
    }
    Ok(())
}

fn validate_uuid(name: &str, value: &str) -> Result<(), Status> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| Status::invalid_argument(format!("{name} must be a UUID string")))
}

fn status_from_failure(error: RequestFailure) -> Status {
    let code = match error.status.as_u16() {
        400 => Code::InvalidArgument,
        404 => Code::NotFound,
        409 => Code::FailedPrecondition,
        410 => Code::FailedPrecondition,
        429 => Code::ResourceExhausted,
        503 => Code::Unavailable,
        _ => Code::Internal,
    };
    Status::new(code, format!("{}: {}", error.name, error.message))
}

fn validate_request_budget<M: Message>(name: &str, request: &M) -> Result<(), Status> {
    if request.encoded_len() > MAX_CONNECT_PROTO_BYTES {
        return Err(Status::resource_exhausted(format!(
            "{name} exceeds the {MAX_CONNECT_PROTO_BYTES}-byte protobuf limit"
        )));
    }
    let bytes = request.encode_to_vec();
    let message_name = format!("spark.connect.{name}");
    preflight_wire_message(&message_name, &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectReserveError, ServerConfig};
    use connect_proto::data_type::{Array, Boolean, Kind, Struct, StructField};
    use std::time::Duration;

    #[test]
    fn descriptor_set_has_the_public_service_and_bounds_nested_requests() {
        let service = sparkless_server_connect::descriptor_pool()
            .unwrap()
            .get_service_by_name("spark.connect.SparkConnectService")
            .expect("public Spark service descriptor");
        let methods = service
            .methods()
            .map(|method| method.name().to_string())
            .collect::<std::collections::HashSet<_>>();
        for required in [
            "ExecutePlan",
            "AnalyzePlan",
            "Config",
            "AddArtifacts",
            "ArtifactStatus",
            "Interrupt",
            "ReattachExecute",
            "ReleaseExecute",
            "ReleaseSession",
            "FetchErrorDetails",
        ] {
            assert!(methods.contains(required), "missing RPC {required}");
        }

        let descriptor = sparkless_server_connect::descriptor_pool()
            .unwrap()
            .get_message_by_name("spark.connect.ExecutePlanRequest")
            .unwrap();
        assert_eq!(descriptor.full_name(), "spark.connect.ExecutePlanRequest");
    }

    #[test]
    fn captured_public_client_request_decodes_with_the_generated_schema() {
        let bytes = include_bytes!(
            "../../../tests/connect/fixtures/pyspark-client-4.0.5/execute-range-filter-count.pb"
        );
        let request = connect_proto::ExecutePlanRequest::decode(bytes.as_slice())
            .expect("pinned public-client fixture decodes");
        validate_request_budget("ExecutePlanRequest", &request)
            .expect("captured request is within the frozen structural budgets");
        assert_eq!(request.session_id, "11111111-1111-4111-8111-111111111111");
        assert!(request.plan.is_some());
        assert_eq!(
            request.operation_id.as_deref(),
            Some("22222222-2222-4222-8222-222222222222")
        );
        let mut request_with_unknown_field = bytes.to_vec();
        request_with_unknown_field.extend_from_slice(&[0xa0, 0x06, 0x01]);
        preflight_wire_message(
            "spark.connect.ExecutePlanRequest",
            &request_with_unknown_field,
        )
        .expect("unknown public protobuf fields remain skippable");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn captured_public_analysis_requests_return_native_schemas_without_execution() {
        let service = ConnectService {
            server: Server::new(ServerConfig::default()).unwrap(),
        };
        let schema_request = connect_proto::AnalyzePlanRequest::decode(
            include_bytes!(
                "../../../tests/connect/fixtures/pyspark-client-4.0.5/analyze-range-schema.pb"
            )
            .as_slice(),
        )
        .unwrap();
        let schema_response = service
            .analyze_plan(Request::new(schema_request))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            schema_response.session_id,
            "11111111-1111-4111-8111-111111111111"
        );
        assert!(Uuid::parse_str(&schema_response.server_side_session_id).is_ok());
        let Some(connect_proto::analyze_plan_response::Result::Schema(schema)) =
            schema_response.result
        else {
            panic!("captured schema request must return a schema result");
        };
        let Some(connect_proto::data_type::Kind::Struct(schema)) =
            schema.schema.and_then(|schema| schema.kind)
        else {
            panic!("range schema must be a StructType");
        };
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].name, "id");
        assert!(!schema.fields[0].nullable);
        assert!(matches!(
            schema.fields[0]
                .data_type
                .as_ref()
                .and_then(|field| field.kind.as_ref()),
            Some(connect_proto::data_type::Kind::Long(_))
        ));

        let ddl_request = connect_proto::AnalyzePlanRequest::decode(
            include_bytes!(
                "../../../tests/connect/fixtures/pyspark-client-4.0.5/analyze-ddl-schema.pb"
            )
            .as_slice(),
        )
        .unwrap();
        let ddl_response = service
            .analyze_plan(Request::new(ddl_request))
            .await
            .unwrap()
            .into_inner();
        let Some(connect_proto::analyze_plan_response::Result::DdlParse(ddl)) = ddl_response.result
        else {
            panic!("captured DDL request must return a DDL parse result");
        };
        let Some(connect_proto::data_type::Kind::Struct(schema)) =
            ddl.parsed.and_then(|schema| schema.kind)
        else {
            panic!("DDL parse result must be a StructType");
        };
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "label"]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_queued_execute_plan_settles_its_operation_record() {
        use std::sync::atomic::Ordering;

        let config = ServerConfig {
            max_active_operations: 1,
            ..ServerConfig::default()
        };
        let server = Server::new(config).unwrap();
        let _compute = server
            .inner
            .execution
            .admission()
            .active
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let service = ConnectService {
            server: server.clone(),
        };
        let plan = connect_proto::Plan {
            op_type: Some(connect_proto::plan::OpType::Root(connect_proto::Relation {
                common: None,
                rel_type: Some(connect_proto::relation::RelType::Range(
                    connect_proto::Range {
                        end: 10,
                        step: 1,
                        ..Default::default()
                    },
                )),
            })),
        };
        let request = connect_proto::ExecutePlanRequest {
            session_id: Uuid::new_v4().to_string(),
            operation_id: Some(Uuid::new_v4().to_string()),
            user_context: Some(connect_proto::UserContext {
                user_id: "cancel-test".into(),
                ..Default::default()
            }),
            plan: Some(plan),
            ..Default::default()
        };
        let handle = tokio::spawn(async move { service.execute_plan(Request::new(request)).await });
        let record = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(record) = server
                    .inner
                    .execution
                    .operation_registry()
                    .lock()
                    .unwrap()
                    .records
                    .values()
                    .next()
                    .cloned()
                {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ExecutePlan should reserve an operation before waiting for compute");

        handle.abort();
        assert!(handle.await.is_err());
        assert_eq!(
            server
                .inner
                .execution
                .admission()
                .accepted
                .load(Ordering::Acquire),
            0
        );
        let record = record.lock().unwrap();
        assert_eq!(record.execution_state, "failed");
        assert_eq!(record.effect_outcome, "cancelled");
    }

    #[tokio::test]
    async fn captured_public_config_requests_use_the_frozen_effective_profile() {
        let service = ConnectService {
            server: Server::new(ServerConfig::default()).unwrap(),
        };
        for fixture in [
            "config-create-dataframe-defaults",
            "config-get",
            "config-get-all",
            "config-get-option",
            "config-get-with-default",
            "config-is-modifiable",
            "config-set",
            "config-unset",
        ] {
            let path = format!(
                "{}/../../tests/connect/fixtures/pyspark-client-4.0.5/{fixture}.pb",
                env!("CARGO_MANIFEST_DIR"),
            );
            let bytes = std::fs::read(path).expect("captured config request fixture");
            let request = connect_proto::ConfigRequest::decode(bytes.as_slice()).unwrap();
            let response = service
                .config(Request::new(request))
                .await
                .unwrap_or_else(|status| panic!("{fixture} should be supported: {status}"))
                .into_inner();
            assert!(Uuid::parse_str(&response.server_side_session_id).is_ok());
            if fixture == "config-create-dataframe-defaults" {
                let values = response
                    .pairs
                    .into_iter()
                    .map(|pair| (pair.key, pair.value.unwrap()))
                    .collect::<std::collections::HashMap<_, _>>();
                assert_eq!(values["spark.sql.timestampType"], "TIMESTAMP_LTZ");
                assert_eq!(values["spark.sql.session.timeZone"], "UTC");
                assert_eq!(
                    values["spark.sql.session.localRelationCacheThreshold"],
                    "67108864"
                );
            }
        }
    }

    fn execute_request(relation: connect_proto::Relation) -> connect_proto::ExecutePlanRequest {
        connect_proto::ExecutePlanRequest {
            session_id: Uuid::new_v4().to_string(),
            operation_id: Some(Uuid::new_v4().to_string()),
            user_context: Some(connect_proto::UserContext {
                user_id: "slice-four-test".into(),
                ..Default::default()
            }),
            plan: Some(connect_proto::Plan {
                op_type: Some(connect_proto::plan::OpType::Root(relation)),
            }),
            request_options: vec![connect_proto::execute_plan_request::RequestOption {
                request_option: Some(
                    connect_proto::execute_plan_request::request_option::RequestOption::ReattachOptions(
                        connect_proto::ReattachOptions { reattachable: true },
                    ),
                ),
            }],
            ..Default::default()
        }
    }

    async fn collect_execute_responses(
        service: &ConnectService,
        request: connect_proto::ExecutePlanRequest,
    ) -> Result<Vec<connect_proto::ExecutePlanResponse>, Status> {
        use tonic::codegen::tokio_stream::StreamExt;

        let reattachable = request_reattachable(&request.request_options)?;
        let mut stream = service
            .execute_plan(Request::new(request.clone()))
            .await?
            .into_inner();
        let mut responses = Vec::new();
        while let Some(response) = stream.next().await {
            let response = response?;
            if reattachable {
                service
                    .release_execute(Request::new(release_request(
                        &request,
                        connect_proto::release_execute_request::Release::ReleaseUntil(
                            connect_proto::release_execute_request::ReleaseUntil {
                                response_id: response.response_id.clone(),
                            },
                        ),
                    )))
                    .await?;
            }
            responses.push(response);
        }
        Ok(responses)
    }

    fn reattach_request(
        execute: &connect_proto::ExecutePlanRequest,
        last_response_id: Option<String>,
    ) -> connect_proto::ReattachExecuteRequest {
        connect_proto::ReattachExecuteRequest {
            session_id: execute.session_id.clone(),
            user_context: execute.user_context.clone(),
            operation_id: execute.operation_id.clone().unwrap(),
            last_response_id,
            ..Default::default()
        }
    }

    fn release_request(
        execute: &connect_proto::ExecutePlanRequest,
        release: connect_proto::release_execute_request::Release,
    ) -> connect_proto::ReleaseExecuteRequest {
        connect_proto::ReleaseExecuteRequest {
            session_id: execute.session_id.clone(),
            user_context: execute.user_context.clone(),
            operation_id: execute.operation_id.clone().unwrap(),
            release: Some(release),
            ..Default::default()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_connect_stream_reattaches_after_released_cursor_without_reexecution() {
        use tonic::codegen::tokio_stream::StreamExt;

        let service = ConnectService {
            server: Server::new(ServerConfig::default()).unwrap(),
        };
        let execute = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::Range(
                connect_proto::Range {
                    end: 20_000,
                    step: 1,
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        let operation_id = execute.operation_id.clone().unwrap();
        let mut original = service
            .execute_plan(Request::new(execute.clone()))
            .await
            .unwrap()
            .into_inner();
        let first = original.next().await.unwrap().unwrap();
        assert!(matches!(
            first.response_type,
            Some(connect_proto::execute_plan_response::ResponseType::ArrowBatch(_))
        ));
        let first_response_id = first.response_id.clone();

        let prefix_release = release_request(
            &execute,
            connect_proto::release_execute_request::Release::ReleaseUntil(
                connect_proto::release_execute_request::ReleaseUntil {
                    response_id: first_response_id.clone(),
                },
            ),
        );
        service
            .release_execute(Request::new(prefix_release))
            .await
            .unwrap();
        drop(original);

        let mut resumed = service
            .reattach_execute(Request::new(reattach_request(
                &execute,
                Some(first_response_id.clone()),
            )))
            .await
            .unwrap()
            .into_inner();
        let mut resumed_responses = Vec::new();
        while let Some(response) = resumed.next().await {
            resumed_responses.push(response.unwrap());
        }
        assert_eq!(resumed_responses.len(), 2);
        let Some(connect_proto::execute_plan_response::ResponseType::ArrowBatch(batch)) =
            resumed_responses[0].response_type.as_ref()
        else {
            panic!("reattachment after the first batch must resume with Arrow data");
        };
        assert_eq!(batch.start_offset, Some(16_384));
        assert_ne!(resumed_responses[0].response_id, first_response_id);
        assert!(matches!(
            resumed_responses[1].response_type,
            Some(connect_proto::execute_plan_response::ResponseType::ResultComplete(_))
        ));
        drop(resumed);

        let duplicate = service
            .execute_plan(Request::new(execute.clone()))
            .await
            .err()
            .expect("reexecuting a retained operation is rejected");
        assert_eq!(duplicate.code(), Code::AlreadyExists);
        assert!(duplicate.message().contains("OPERATION_ALREADY_EXISTS"));

        let released = service
            .release_execute(Request::new(release_request(
                &execute,
                connect_proto::release_execute_request::Release::ReleaseAll(
                    connect_proto::release_execute_request::ReleaseAll {},
                ),
            )))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            released.operation_id.as_deref(),
            Some(operation_id.as_str())
        );
        let missing = service
            .reattach_execute(Request::new(reattach_request(&execute, None)))
            .await
            .err()
            .expect("released operation cannot be reattached");
        assert_eq!(missing.code(), Code::NotFound);
        assert!(missing.message().contains("OPERATION_NOT_FOUND"));
        assert_eq!(
            service
                .server
                .inner
                .connect_runtime
                .replay_bytes()
                .load(Ordering::Acquire),
            0
        );
    }

    #[tokio::test]
    async fn release_all_clears_history_for_nonreattachable_operations() {
        let config = ServerConfig {
            max_mutation_records: 1,
            ..ServerConfig::default()
        };
        let service = ConnectService {
            server: Server::new(config).unwrap(),
        };
        let mut execute = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::Range(
                connect_proto::Range {
                    end: 10,
                    step: 1,
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        execute.request_options =
            vec![connect_proto::execute_plan_request::RequestOption {
            request_option: Some(
                connect_proto::execute_plan_request::request_option::RequestOption::ReattachOptions(
                    connect_proto::ReattachOptions { reattachable: false },
                ),
            ),
        }];

        collect_execute_responses(&service, execute.clone())
            .await
            .expect("non-reattachable query completes");
        {
            let registry = service
                .server
                .inner
                .execution
                .operation_registry()
                .lock()
                .unwrap();
            assert_eq!(registry.records.len(), 1);
            assert!(registry.bytes > 0);
        }
        let identity = session_identity(
            execute.user_context.clone(),
            execute.session_id.clone(),
            execute.client_observed_server_side_session_id.clone(),
        )
        .unwrap();
        let operation_key =
            crate::connect_operation_key(&identity, execute.operation_id.as_deref().unwrap());
        // The periodic replay sweep may discard a completed non-reattachable stream before
        // the client sends ReleaseAll; idempotency history must still be released by key.
        service
            .server
            .inner
            .connect_runtime
            .operations()
            .unwrap()
            .remove(&operation_key);

        let released = service
            .release_execute(Request::new(release_request(
                &execute,
                connect_proto::release_execute_request::Release::ReleaseAll(
                    connect_proto::release_execute_request::ReleaseAll {},
                ),
            )))
            .await
            .expect("ReleaseAll is valid for a completed non-reattachable query");
        assert_eq!(released.into_inner().operation_id, execute.operation_id);
        {
            let registry = service
                .server
                .inner
                .execution
                .operation_registry()
                .lock()
                .unwrap();
            assert!(registry.records.is_empty());
            assert_eq!(registry.bytes, 0);
        }

        execute.operation_id = Some(Uuid::new_v4().to_string());
        collect_execute_responses(&service, execute.clone())
            .await
            .expect("freed operation history admits a subsequent query");
        service
            .release_execute(Request::new(release_request(
                &execute,
                connect_proto::release_execute_request::Release::ReleaseAll(
                    connect_proto::release_execute_request::ReleaseAll {},
                ),
            )))
            .await
            .expect("second query history is also released");
    }

    #[tokio::test]
    async fn execute_errors_support_the_pinned_clients_error_detail_lookup() {
        let service = ConnectService {
            server: Server::new(ServerConfig::default()).unwrap(),
        };
        let execute = execute_request(connect_proto::Relation::default());
        let operation_id = execute.operation_id.clone().unwrap();
        let identity = ConnectSessionIdentity {
            user_id: "slice-four-test".to_string(),
            client_session_id: execute.session_id.clone(),
            observed_server_session_id: None,
        };
        let status = collect_execute_responses(&service, execute.clone())
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument);
        let rpc_status = GoogleRpcStatus::decode(status.details())
            .expect("errors expose standard google.rpc.Status details");
        let info = GoogleRpcErrorInfo::decode(rpc_status.details[0].value.as_slice())
            .expect("status includes standard ErrorInfo");
        let error_id = info.metadata.get("errorId").unwrap().clone();
        assert!(Uuid::parse_str(&error_id).is_ok());

        let details = service
            .fetch_error_details(Request::new(connect_proto::FetchErrorDetailsRequest {
                session_id: identity.client_session_id,
                user_context: Some(connect_proto::UserContext {
                    user_id: identity.user_id,
                    ..Default::default()
                }),
                error_id,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(details.root_error_idx, Some(0));
        assert_eq!(details.errors.len(), 1);
        assert_eq!(details.errors[0].message, status.message());
        assert_eq!(
            details.errors[0].error_type_hierarchy[0],
            "org.apache.spark.SparkException"
        );
        assert_eq!(execute.operation_id.as_deref(), Some(operation_id.as_str()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tag_interrupt_cancels_the_matching_native_producer() {
        use tonic::codegen::tokio_stream::StreamExt;

        let service = ConnectService {
            server: Server::new(ServerConfig::default()).unwrap(),
        };
        let mut execute = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::Range(
                connect_proto::Range {
                    end: 1_000_000,
                    step: 1,
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        execute.tags = vec!["cancel-me".to_string()];
        let operation_id = execute.operation_id.clone().unwrap();
        let mut stream = service
            .execute_plan(Request::new(execute.clone()))
            .await
            .unwrap()
            .into_inner();
        assert!(stream.next().await.expect("initial response").is_ok());
        let interrupted = service
            .interrupt(Request::new(connect_proto::InterruptRequest {
                session_id: execute.session_id.clone(),
                user_context: execute.user_context.clone(),
                interrupt_type: connect_proto::interrupt_request::InterruptType::Tag as i32,
                interrupt: Some(connect_proto::interrupt_request::Interrupt::OperationTag(
                    "cancel-me".to_string(),
                )),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(interrupted.interrupted_ids, vec![operation_id.clone()]);

        let terminal = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("stream wakes when the interrupt is accepted")
            .expect("interrupted operation must end with a visible failure")
            .expect_err("responses queued before cancellation must not be sent afterward");
        assert_eq!(terminal.code(), Code::Cancelled);
        assert!(terminal.message().contains("interrupted"));
    }

    #[test]
    fn session_release_cancels_reserved_connect_operations_before_tombstoning() {
        let server = Server::new(ServerConfig::default()).unwrap();
        let identity = identity("release-user", &Uuid::new_v4().to_string(), None);
        let operation_id = Uuid::new_v4().to_string();
        let operation = server
            .inner
            .reserve_connect_operation(
                &identity,
                &operation_id,
                [9; 32],
                true,
                vec!["session-op".to_string()],
            )
            .unwrap();
        let cancellation = operation.replay.cancellation();
        server
            .inner
            .release_connect_session(&identity, false)
            .unwrap();
        assert!(cancellation.is_cancelled());
        assert!(
            server
                .inner
                .connect_operation(&identity, &operation_id)
                .is_none()
        );
        drop(operation);
        assert_eq!(
            server
                .inner
                .execution
                .admission()
                .accepted
                .load(Ordering::Acquire),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn attached_replay_stream_pins_session_until_transport_detaches() {
        use tonic::codegen::tokio_stream::StreamExt;

        let server = Server::new(ServerConfig {
            session_idle_ttl: Duration::from_millis(10),
            ..Default::default()
        })
        .unwrap();
        let service = ConnectService {
            server: server.clone(),
        };
        let execute = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::Range(
                connect_proto::Range {
                    end: 3,
                    step: 1,
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        let identity = identity("slice-four-test", &execute.session_id, None);
        let mut stream = service
            .execute_plan(Request::new(execute))
            .await
            .unwrap()
            .into_inner();
        while stream.next().await.is_some() {}

        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(server.inner.expire_idle_sessions().unwrap(), 0);
        drop(stream);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(server.inner.expire_idle_sessions().unwrap(), 1);
        assert_eq!(
            server
                .inner
                .connect_session_lease(&identity)
                .err()
                .expect("expired Connect session is tombstoned")
                .name,
            "SessionReleased"
        );
    }

    #[test]
    fn detached_expiry_cancels_the_producer_and_reclaims_replay_storage() {
        let process_bytes = Arc::new(AtomicUsize::new(0));
        let history_bytes = Arc::new(AtomicUsize::new(0));
        let record = Arc::new(
            ConnectReplayRecord::new(
                "connect-operation:expiry-test".to_string(),
                "connect:expiry-session".to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                1,
                true,
                Vec::new(),
                Arc::clone(&process_bytes),
                Arc::clone(&history_bytes),
            )
            .unwrap(),
        );
        record
            .append(connect_proto::ExecutePlanResponse {
                response_id: Uuid::new_v4().to_string(),
                response_type: Some(
                    connect_proto::execute_plan_response::ResponseType::ArrowBatch(
                        connect_proto::execute_plan_response::ArrowBatch {
                            row_count: 1,
                            data: vec![7; 4096],
                            start_offset: Some(0),
                        },
                    ),
                ),
                ..Default::default()
            })
            .unwrap();
        assert!(process_bytes.load(Ordering::Acquire) > 0);
        let cancellation = record.cancellation();
        record.detach();
        let detached_at = record.detached_at().unwrap();
        assert!(!record.expire_if_detached(
            detached_at + CONNECT_DETACHED_RETENTION - Duration::from_millis(1)
        ));
        assert!(record.expire_if_detached(detached_at + CONNECT_DETACHED_RETENTION));
        assert!(cancellation.is_cancelled());
        assert_eq!(process_bytes.load(Ordering::Acquire), 0);
        assert_eq!(
            record.attach(None).unwrap_err().code(),
            Code::NotFound,
            "expired operations fail explicitly instead of replaying a partial prefix"
        );
        drop(record);
        assert_eq!(history_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn replay_capacity_and_prefix_release_account_for_retained_bytes() {
        let process_bytes = Arc::new(AtomicUsize::new(0));
        let history_bytes = Arc::new(AtomicUsize::new(0));
        let record = Arc::new(
            ConnectReplayRecord::new(
                "connect-operation:test".to_string(),
                "connect:test-session".to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                1,
                true,
                Vec::new(),
                Arc::clone(&process_bytes),
                Arc::clone(&history_bytes),
            )
            .unwrap(),
        );
        let response_id = Uuid::new_v4().to_string();
        let response = connect_proto::ExecutePlanResponse {
            response_id: response_id.clone(),
            response_type: Some(
                connect_proto::execute_plan_response::ResponseType::ArrowBatch(
                    connect_proto::execute_plan_response::ArrowBatch {
                        row_count: 0,
                        data: vec![0; 4096],
                        start_offset: Some(0),
                    },
                ),
            ),
            ..Default::default()
        };
        record.append(response).unwrap();
        assert!(process_bytes.load(Ordering::Acquire) > 0);
        assert!(record.release_until(&response_id) > 0);
        assert_eq!(process_bytes.load(Ordering::Acquire), 0);

        let oversized = connect_proto::ExecutePlanResponse {
            response_id: Uuid::new_v4().to_string(),
            response_type: Some(
                connect_proto::execute_plan_response::ResponseType::ArrowBatch(
                    connect_proto::execute_plan_response::ArrowBatch {
                        row_count: 0,
                        data: vec![0; MAX_CONNECT_REPLAY_OPERATION_BYTES],
                        start_offset: Some(0),
                    },
                ),
            ),
            ..Default::default()
        };
        assert_eq!(
            record.append(oversized).unwrap_err().code(),
            Code::ResourceExhausted
        );
        assert_eq!(process_bytes.load(Ordering::Acquire), 0);
        drop(record);
        assert_eq!(history_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn sender_reservations_are_released_by_identity_across_detach_and_reattach() {
        let process_bytes = Arc::new(AtomicUsize::new(0));
        let history_bytes = Arc::new(AtomicUsize::new(0));
        let record = Arc::new(
            ConnectReplayRecord::new(
                "connect-operation:reservation-test".to_string(),
                "connect:reservation-session".to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                1,
                true,
                Vec::new(),
                Arc::clone(&process_bytes),
                Arc::clone(&history_bytes),
            )
            .unwrap(),
        );
        let response = connect_proto::ExecutePlanResponse {
            response_id: Uuid::new_v4().to_string(),
            response_type: Some(
                connect_proto::execute_plan_response::ResponseType::ArrowBatch(
                    connect_proto::execute_plan_response::ArrowBatch {
                        row_count: 0,
                        data: vec![0; 500_000],
                        start_offset: Some(0),
                    },
                ),
            ),
            ..Default::default()
        };
        let bytes = response.encoded_len();
        let old_sender = record.reserve_sender_buffer(bytes).unwrap();
        record.append(response).unwrap();
        record.detach();
        let (_cursor, _wake) = record.attach(None).unwrap();
        let ReplayPoll::Response(_, replay_sender) = record.next_response_for_sender(0).unwrap()
        else {
            panic!("retained response must be replayable");
        };

        record.release_sender_buffer(old_sender);
        assert_eq!(record.reserved_sender_bytes(), bytes);
        record.release_all();
        assert_eq!(record.reserved_sender_bytes(), bytes);
        assert_eq!(process_bytes.load(Ordering::Acquire), bytes);
        record.release_sender_buffer(replay_sender);
        assert_eq!(record.reserved_sender_bytes(), 0);
        drop(record);
        assert_eq!(process_bytes.load(Ordering::Acquire), 0);
        assert_eq!(history_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn retained_history_reserves_enough_capacity_for_replay() {
        let process_bytes = Arc::new(AtomicUsize::new(0));
        let history_bytes = Arc::new(AtomicUsize::new(0));
        let record = Arc::new(
            ConnectReplayRecord::new(
                "connect-operation:headroom-test".to_string(),
                "connect:headroom-session".to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                Uuid::new_v4().to_string(),
                1,
                true,
                Vec::new(),
                Arc::clone(&process_bytes),
                Arc::clone(&history_bytes),
            )
            .unwrap(),
        );
        for index in 0..7 {
            record
                .append(connect_proto::ExecutePlanResponse {
                    response_id: Uuid::new_v4().to_string(),
                    response_type: Some(
                        connect_proto::execute_plan_response::ResponseType::ArrowBatch(
                            connect_proto::execute_plan_response::ArrowBatch {
                                row_count: 0,
                                data: vec![0; 900_000],
                                start_offset: Some(index * 900_000),
                            },
                        ),
                    ),
                    ..Default::default()
                })
                .unwrap();
        }
        let _ = record.finish(Ok(()));
        record.detach();
        let (_cursor, _wake) = record.attach(None).unwrap();
        let ReplayPoll::Response(response, reservation) =
            record.next_response_for_sender(0).unwrap()
        else {
            panic!("completed retained response must be replayable");
        };
        assert!(matches!(
            response.response_type.as_ref(),
            Some(connect_proto::execute_plan_response::ResponseType::ArrowBatch(batch))
                if batch.data.len() == 900_000
        ));
        record.release_sender_buffer(reservation);
        assert!(record.completed_successfully());
        record.release_all();
        drop(record);
        assert_eq!(process_bytes.load(Ordering::Acquire), 0);
        assert_eq!(history_bytes.load(Ordering::Acquire), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_deadline_fails_a_backpressured_stream_and_releases_admission() {
        use tonic::codegen::tokio_stream::StreamExt;

        for reattachable in [false, true] {
            let service = ConnectService {
                server: Server::new(ServerConfig {
                    response_timeout: Duration::from_millis(200),
                    ..Default::default()
                })
                .unwrap(),
            };
            let mut request = execute_request(connect_proto::Relation {
                rel_type: Some(connect_proto::relation::RelType::Range(
                    connect_proto::Range {
                        end: 1_000_000,
                        step: 1,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            });
            if !reattachable {
                request.request_options.clear();
            }
            let mut stream = service
                .execute_plan(Request::new(request))
                .await
                .unwrap()
                .into_inner();
            let first = stream.next().await.unwrap().unwrap();
            assert!(matches!(
                first.response_type,
                Some(connect_proto::execute_plan_response::ResponseType::ArrowBatch(_))
            ));
            // Leave the bounded response queue full until the execution deadline expires.
            tokio::time::sleep(Duration::from_millis(300)).await;
            let error = stream
                .next()
                .await
                .expect("deadline must produce a visible stream failure")
                .unwrap_err();
            assert_eq!(error.code(), Code::DeadlineExceeded);
            assert!(stream.next().await.is_none());
            tokio::time::timeout(Duration::from_secs(2), async {
                while service
                    .server
                    .inner
                    .execution
                    .admission()
                    .active
                    .available_permits()
                    != 2
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("a stalled producer must return its compute permit");
        }
    }

    #[tokio::test]
    async fn execute_admission_wait_is_bounded_by_the_acceptance_deadline() {
        let service = ConnectService {
            server: Server::new(ServerConfig {
                response_timeout: Duration::from_millis(50),
                queue_timeout: Duration::from_secs(5),
                ..Default::default()
            })
            .unwrap(),
        };
        let _busy = service
            .server
            .inner
            .execution
            .admission()
            .active
            .clone()
            .acquire_many_owned(2)
            .await
            .unwrap();
        let request = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::Range(
                connect_proto::Range {
                    end: 1,
                    step: 1,
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            service.execute_plan(Request::new(request)),
        )
        .await
        .expect("queueing must not outlive the shorter response deadline");
        assert_eq!(result.err().unwrap().code(), Code::DeadlineExceeded);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_plan_streams_the_public_global_count_result() {
        let service = ConnectService {
            server: Server::new(ServerConfig::default()).unwrap(),
        };
        let request = connect_proto::ExecutePlanRequest::decode(
            include_bytes!(
                "../../../tests/connect/fixtures/pyspark-client-4.0.5/execute-range-filter-count.pb"
            )
            .as_slice(),
        )
        .unwrap();
        let responses = collect_execute_responses(&service, request)
            .await
            .expect("public global count executes");
        let Some(connect_proto::execute_plan_response::ResponseType::ArrowBatch(batch)) =
            responses[0].response_type.as_ref()
        else {
            panic!("expected a count Arrow batch");
        };
        assert_eq!(batch.row_count, 1);
        let decoder = robin_sparkless::SparkSession::builder().get_or_create();
        let (frame, _) = decoder
            .create_dataframe_from_arrow_ipc(
                Some(&batch.data),
                None,
                MAX_CONNECT_ARROW_RESPONSE_BYTES,
                1,
                MAX_CONNECT_LOCAL_DECODED_BYTES,
                MAX_CONNECT_LOCAL_SCALAR_BYTES,
            )
            .expect("count payload decodes");
        let rows = frame.collect_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values().next().and_then(serde_json::Value::as_i64),
            Some(4)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_plan_streams_bounded_arrow_batches_and_reliable_offsets() {
        let service = ConnectService {
            server: Server::new(ServerConfig::default()).unwrap(),
        };
        let request = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::Range(
                connect_proto::Range {
                    end: 20_000,
                    step: 1,
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        let responses = collect_execute_responses(&service, request.clone())
            .await
            .expect("bounded range executes");
        assert_eq!(responses.len(), 3);
        let batches = responses[..2]
            .iter()
            .map(|response| {
                assert_eq!(response.session_id, request.session_id);
                assert_eq!(response.operation_id, request.operation_id.clone().unwrap());
                assert!(Uuid::parse_str(&response.response_id).is_ok());
                assert!(response.encoded_len() <= MAX_CONNECT_ARROW_RESPONSE_BYTES);
                let Some(connect_proto::execute_plan_response::ResponseType::ArrowBatch(batch)) =
                    response.response_type.as_ref()
                else {
                    panic!("expected an Arrow batch response");
                };
                assert!(batch.data.len() < MAX_CONNECT_ARROW_RESPONSE_BYTES);
                (response, batch)
            })
            .collect::<Vec<_>>();
        assert_eq!(batches[0].1.row_count, MAX_CONNECT_ARROW_BATCH_ROWS as i64);
        assert_eq!(batches[0].1.start_offset, Some(0));
        assert_eq!(
            batches[1].1.row_count,
            20_000 - MAX_CONNECT_ARROW_BATCH_ROWS as i64
        );
        assert_eq!(
            batches[1].1.start_offset,
            Some(MAX_CONNECT_ARROW_BATCH_ROWS as i64)
        );
        assert!(batches[0].0.schema.is_some());
        assert!(batches[1].0.schema.is_none());
        assert!(matches!(
            responses[2].response_type,
            Some(connect_proto::execute_plan_response::ResponseType::ResultComplete(_))
        ));

        let decoder = robin_sparkless::SparkSession::builder().get_or_create();
        let mut decoded = Vec::new();
        for (_, batch) in batches {
            let (frame, _) = decoder
                .create_dataframe_from_arrow_ipc(
                    Some(&batch.data),
                    None,
                    MAX_CONNECT_LOCAL_IPC_BYTES,
                    MAX_CONNECT_ARROW_BATCH_ROWS,
                    MAX_CONNECT_LOCAL_DECODED_BYTES,
                    MAX_CONNECT_LOCAL_SCALAR_BYTES,
                )
                .expect("each response is an independently readable Arrow IPC stream");
            let rows = frame.collect_rows().unwrap();
            assert_eq!(rows.len() as i64, batch.row_count);
            decoded.extend(rows.into_iter().map(|row| {
                row.values()
                    .next()
                    .and_then(serde_json::Value::as_i64)
                    .unwrap()
            }));
        }
        assert_eq!(decoded.len(), 20_000);
        assert_eq!(decoded[0], 0);
        assert_eq!(decoded[19_999], 19_999);
    }

    #[tokio::test]
    async fn execute_plan_streams_typed_empty_results_and_local_schema_metadata() {
        let service = ConnectService {
            server: Server::new(ServerConfig::default()).unwrap(),
        };
        let empty_schema = r#"{"fields":[{"metadata":{"origin":"local"},"name":"dup","nullable":false,"type":"integer"},{"metadata":{},"name":"dup","nullable":true,"type":"string"}],"type":"struct"}"#;
        let request = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::LocalRelation(
                connect_proto::LocalRelation {
                    data: None,
                    schema: Some(empty_schema.into()),
                },
            )),
            ..Default::default()
        });
        let responses = collect_execute_responses(&service, request)
            .await
            .expect("schema-only local relation produces an empty result");
        assert_eq!(responses.len(), 2);
        let Some(connect_proto::execute_plan_response::ResponseType::ArrowBatch(batch)) =
            responses[0].response_type.as_ref()
        else {
            panic!("expected an Arrow batch response");
        };
        assert_eq!(batch.row_count, 0);
        assert_eq!(batch.start_offset, Some(0));
        let Some(connect_proto::data_type::Kind::Struct(schema)) = responses[0]
            .schema
            .as_ref()
            .and_then(|schema| schema.kind.as_ref())
        else {
            panic!("expected a struct result schema");
        };
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].name, "dup");
        assert_eq!(schema.fields[1].name, "dup");
        assert!(!schema.fields[0].nullable);
        assert_eq!(
            schema.fields[0].metadata.as_deref(),
            Some(r#"{"origin":"local"}"#)
        );
        assert!(matches!(
            responses[1].response_type,
            Some(connect_proto::execute_plan_response::ResponseType::ResultComplete(_))
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_plan_applies_local_ddl_casts_and_rejects_bad_ipc() {
        let service = ConnectService {
            server: Server::new(ServerConfig::default()).unwrap(),
        };
        let source = robin_sparkless::SparkSession::builder()
            .get_or_create()
            .range(0, 3, 1)
            .unwrap();
        let mut input_batches = source
            .to_arrow_ipc_batches_with_cancellation(
                8,
                MAX_CONNECT_LOCAL_IPC_BYTES,
                &robin_sparkless_core::engine::CancellationToken::new(),
            )
            .unwrap();
        let input = input_batches.next().unwrap().unwrap().bytes;
        let request = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::LocalRelation(
                connect_proto::LocalRelation {
                    data: Some(input),
                    schema: Some("id INT NOT NULL".into()),
                },
            )),
            ..Default::default()
        });
        let responses = collect_execute_responses(&service, request)
            .await
            .expect("local relation casts by declared schema");
        let Some(connect_proto::data_type::Kind::Struct(result_schema)) = responses[0]
            .schema
            .as_ref()
            .and_then(|schema| schema.kind.as_ref())
        else {
            panic!("expected a struct result schema");
        };
        assert!(matches!(
            result_schema.fields[0]
                .data_type
                .as_ref()
                .and_then(|dtype| dtype.kind.as_ref()),
            Some(connect_proto::data_type::Kind::Integer(_))
        ));
        let Some(connect_proto::execute_plan_response::ResponseType::ArrowBatch(batch)) =
            responses[0].response_type.as_ref()
        else {
            panic!("expected an Arrow batch response");
        };
        let decoder = robin_sparkless::SparkSession::builder().get_or_create();
        let (frame, _) = decoder
            .create_dataframe_from_arrow_ipc(
                Some(&batch.data),
                None,
                MAX_CONNECT_LOCAL_IPC_BYTES,
                MAX_CONNECT_ARROW_BATCH_ROWS,
                MAX_CONNECT_LOCAL_DECODED_BYTES,
                MAX_CONNECT_LOCAL_SCALAR_BYTES,
            )
            .unwrap();
        let rows = frame.collect_rows().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.iter()
                .map(|row| row.values().next().unwrap().as_i64().unwrap())
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );

        let malformed = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::LocalRelation(
                connect_proto::LocalRelation {
                    data: Some(vec![1, 2, 3]),
                    schema: Some("id INT".into()),
                },
            )),
            ..Default::default()
        });
        let error = collect_execute_responses(&service, malformed)
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn execute_plan_rejects_mismatched_nested_and_cached_local_relations() {
        let service = ConnectService {
            server: Server::new(ServerConfig::default()).unwrap(),
        };
        let source = robin_sparkless::SparkSession::builder()
            .get_or_create()
            .range(0, 1, 1)
            .unwrap();
        let mut input_batches = source
            .to_arrow_ipc_batches_with_cancellation(
                1,
                MAX_CONNECT_LOCAL_IPC_BYTES,
                &robin_sparkless_core::engine::CancellationToken::new(),
            )
            .unwrap();
        let input = input_batches.next().unwrap().unwrap().bytes;
        let mismatched = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::LocalRelation(
                connect_proto::LocalRelation {
                    data: Some(input),
                    schema: Some(
                        r#"{"fields":[{"metadata":{},"name":"id","nullable":true,"type":"long"},{"metadata":{},"name":"label","nullable":true,"type":"string"}],"type":"struct"}"#.into(),
                    ),
                },
            )),
            ..Default::default()
        });
        let error = collect_execute_responses(&service, mismatched)
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);

        let nested = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::LocalRelation(
                connect_proto::LocalRelation {
                    data: None,
                    schema: Some(
                        r#"{"fields":[{"metadata":{},"name":"items","nullable":true,"type":{"containsNull":true,"elementType":"integer","type":"array"}}],"type":"struct"}"#.into(),
                    ),
                },
            )),
            ..Default::default()
        });
        let error = collect_execute_responses(&service, nested)
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::Unimplemented);

        let cached = execute_request(connect_proto::Relation {
            rel_type: Some(connect_proto::relation::RelType::CachedLocalRelation(
                connect_proto::CachedLocalRelation::default(),
            )),
            ..Default::default()
        });
        let error = collect_execute_responses(&service, cached)
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::Unimplemented);
    }

    #[test]
    fn recursive_schema_depth_and_schema_field_limits_are_rejected() {
        let deep_type = nested_data_type(sparkless_server_connect::MAX_CONNECT_PROTO_DEPTH / 2 + 1);
        assert_eq!(
            preflight_wire_message("spark.connect.DataType", &deep_type.encode_to_vec())
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );

        let fields = (0..sparkless_server_connect::MAX_CONNECT_SCHEMA_FIELDS + 1)
            .map(|index| StructField {
                name: format!("c{index}"),
                data_type: Some(connect_proto::DataType {
                    kind: Some(Kind::Boolean(Boolean {
                        type_variation_reference: 0,
                    })),
                }),
                nullable: true,
                metadata: None,
            })
            .collect();
        let wide_type = connect_proto::DataType {
            kind: Some(Kind::Struct(Struct {
                fields,
                type_variation_reference: 0,
            })),
        };
        assert_eq!(
            preflight_wire_message("spark.connect.DataType", &wide_type.encode_to_vec())
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
    }

    #[test]
    fn request_byte_and_message_node_limits_are_rejected() {
        let too_large = connect_proto::ExecutePlanRequest {
            client_type: Some("x".repeat(MAX_CONNECT_PROTO_BYTES + 1)),
            ..Default::default()
        };
        assert_eq!(
            validate_request_budget("ExecutePlanRequest", &too_large)
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );

        let many_nodes = connect_proto::ExecutePlanRequest {
            request_options: (0..sparkless_server_connect::MAX_CONNECT_PROTO_NODES)
                .map(|_| connect_proto::execute_plan_request::RequestOption {
                    request_option: None,
                })
                .collect(),
            ..Default::default()
        };
        assert_eq!(
            validate_request_budget("ExecutePlanRequest", &many_nodes)
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        let many_nodes_wire = many_nodes.encode_to_vec();
        assert_eq!(
            preflight_wire_message("spark.connect.ExecutePlanRequest", &many_nodes_wire)
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            preflight_wire_message("spark.connect.ExecutePlanRequest", &[0x0a, 0x05, b'x'])
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn connect_session_identity_isolated_and_release_is_tombstoned() {
        let server = Server::new(ServerConfig::default()).unwrap();
        let session_id = Uuid::new_v4().to_string();
        let first_identity = identity("álîce", &session_id, None);
        let lease = server.inner.connect_session_lease(&first_identity).unwrap();
        let original_server_id = lease.0.server_side_session_id.clone();
        let generation = lease.0.generation;
        drop(lease);

        let same_identity = identity("álîce", &session_id, Some(&original_server_id));
        let next_lease = server.inner.connect_session_lease(&same_identity).unwrap();
        assert_eq!(next_lease.0.generation, generation);
        drop(next_lease);

        let separate_user = identity("bob", &session_id, None);
        let other_lease = server.inner.connect_session_lease(&separate_user).unwrap();
        assert_ne!(other_lease.0.generation, generation);
        drop(other_lease);

        let replacement_identity = identity("charlie", &Uuid::new_v4().to_string(), None);
        let old_lease = server
            .inner
            .connect_session_lease(&replacement_identity)
            .unwrap();
        let old_server_id = old_lease.0.server_side_session_id.clone();
        let old_generation = old_lease.0.generation;
        drop(old_lease);
        server
            .inner
            .close_session(&replacement_identity.registry_key())
            .unwrap();
        let replacement = server
            .inner
            .connect_session_lease(&replacement_identity)
            .unwrap();
        assert_ne!(replacement.0.generation, old_generation);
        assert_ne!(replacement.0.server_side_session_id, old_server_id);
        drop(replacement);
        let stale_identity = identity(
            "charlie",
            &replacement_identity.client_session_id,
            Some(&old_server_id),
        );
        assert_eq!(
            server
                .inner
                .connect_session_lease(&stale_identity)
                .err()
                .unwrap()
                .name,
            "StaleSessionIdentity"
        );

        let released_id = server
            .inner
            .release_connect_session(&first_identity, false)
            .unwrap();
        assert_eq!(released_id, original_server_id);
        assert_eq!(
            server
                .inner
                .connect_session_lease(&first_identity)
                .err()
                .unwrap()
                .name,
            "SessionReleased"
        );
        assert_eq!(
            server
                .inner
                .release_connect_session(&first_identity, false)
                .unwrap(),
            original_server_id
        );
        assert_eq!(
            server
                .inner
                .release_connect_session(&first_identity, true)
                .err()
                .unwrap()
                .name,
            "UnsupportedSessionReconnect"
        );
    }

    #[tokio::test]
    async fn shutdown_cancels_work_and_waits_for_released_operation_leases() {
        let server = Server::new(ServerConfig::default()).unwrap();
        let retained_identity = identity("retained", &Uuid::new_v4().to_string(), None);
        let released_identity = identity("released", &Uuid::new_v4().to_string(), None);
        let retained = server
            .inner
            .reserve_connect_operation(
                &retained_identity,
                &Uuid::new_v4().to_string(),
                [1; 32],
                true,
                Vec::new(),
            )
            .unwrap();
        let released = server
            .inner
            .reserve_connect_operation(
                &released_identity,
                &Uuid::new_v4().to_string(),
                [2; 32],
                true,
                Vec::new(),
            )
            .unwrap();
        server
            .inner
            .release_connect_session(&released_identity, false)
            .unwrap();
        assert_eq!(server.inner.connect_runtime.operations().unwrap().len(), 1);

        let active = server.cancel_connect_work_for_shutdown().unwrap();
        assert!(retained.replay.cancellation().is_cancelled());
        assert!(released.replay.cancellation().is_cancelled());
        assert_eq!(*active.borrow(), 2);
        assert!(matches!(
            server.inner.reserve_connect_operation(
                &retained_identity, &Uuid::new_v4().to_string(), [3; 32], true, Vec::new(),
            ),
            Err(ConnectReserveError::Request(error)) if error.status.as_u16() == 503
        ));
        let mut stopped = tokio::spawn(Server::wait_for_connect_work_to_stop(active));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut stopped)
                .await
                .is_err()
        );
        drop(retained);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut stopped)
                .await
                .is_err()
        );
        drop(released);
        tokio::time::timeout(Duration::from_secs(1), stopped)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn connect_operations_share_admission_and_retain_duplicate_protection() {
        let server = Server::new(ServerConfig::default()).unwrap();
        let identity = identity("user", &Uuid::new_v4().to_string(), None);
        let operation_id = Uuid::new_v4().to_string();
        let first = server
            .inner
            .reserve_connect_operation(&identity, &operation_id, [7; 32], false, Vec::new())
            .unwrap();
        first.reject("UNIMPLEMENTED", "slice boundary");
        assert_eq!(
            server
                .inner
                .execution
                .admission()
                .accepted
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        drop(first);
        assert_eq!(
            server
                .inner
                .execution
                .admission()
                .accepted
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        let tickets = (0..server.inner.execution.admission().max_accepted)
            .map(|_| server.inner.reserve_admission().unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(
            server.inner.reserve_connect_operation(
                &identity,
                &operation_id,
                [7; 32],
                false,
                Vec::new()
            ),
            Err(ConnectReserveError::Duplicate)
        ));
        drop(tickets);
        assert!(matches!(
            server.inner.reserve_connect_operation(
                &identity,
                &operation_id,
                [8; 32],
                false,
                Vec::new()
            ),
            Err(ConnectReserveError::Conflict)
        ));
    }

    #[test]
    #[cfg(feature = "http")]
    fn connect_registry_prefixes_are_reserved_from_http_ids() {
        let identity = identity("user", &Uuid::new_v4().to_string(), None);
        assert!(crate::validate_http_session_identifier(&identity.registry_key()).is_err());
        let operation_key = crate::connect_operation_key(&identity, &Uuid::new_v4().to_string());
        assert!(crate::validate_http_operation_identifier(&operation_key).is_err());
        assert!(crate::validate_http_session_identifier("http:session").is_ok());
        assert!(crate::validate_http_operation_identifier("http:operation").is_ok());
    }

    #[test]
    fn concurrent_session_and_operation_admission_cannot_cross_release() {
        let server = Server::new(ServerConfig::default()).unwrap();
        for _ in 0..500 {
            let session_id = Uuid::new_v4().to_string();
            let original_identity = identity("race-user", &session_id, None);
            let session_key = original_identity.registry_key();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));

            let acquire_server = server.clone();
            let acquire_identity = identity("race-user", &session_id, None);
            let acquire_barrier = std::sync::Arc::clone(&barrier);
            let acquire = std::thread::spawn(move || {
                acquire_barrier.wait();
                acquire_server
                    .inner
                    .connect_session_lease(&acquire_identity)
            });

            let release_server = server.clone();
            let release_identity = identity("race-user", &session_id, None);
            let release_barrier = std::sync::Arc::clone(&barrier);
            let release = std::thread::spawn(move || {
                release_barrier.wait();
                release_server
                    .inner
                    .release_connect_session(&release_identity, false)
            });

            barrier.wait();
            let lease = acquire.join().unwrap();
            release.join().unwrap().unwrap();
            match lease {
                Ok(lease) => assert!(lease.0.closing.load(std::sync::atomic::Ordering::Acquire)),
                Err(error) => assert_eq!(error.name, "SessionReleased"),
            }
            assert!(
                !server
                    .inner
                    .execution
                    .session_registry()
                    .lock()
                    .unwrap()
                    .contains_key(&session_key)
            );

            let operation_identity = identity("race-user", &Uuid::new_v4().to_string(), None);
            let operation_session_key = operation_identity.registry_key();
            let operation_id = Uuid::new_v4().to_string();
            let operation_barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
            let reserve_server = server.clone();
            let reserve_identity =
                identity("race-user", &operation_identity.client_session_id, None);
            let reserve_id = operation_id.clone();
            let reserve_barrier = std::sync::Arc::clone(&operation_barrier);
            let reserve = std::thread::spawn(move || {
                reserve_barrier.wait();
                reserve_server.inner.reserve_connect_operation(
                    &reserve_identity,
                    &reserve_id,
                    [3; 32],
                    false,
                    Vec::new(),
                )
            });
            let release_server = server.clone();
            let release_identity =
                identity("race-user", &operation_identity.client_session_id, None);
            let release_barrier = std::sync::Arc::clone(&operation_barrier);
            let release = std::thread::spawn(move || {
                release_barrier.wait();
                release_server
                    .inner
                    .release_connect_session(&release_identity, false)
            });
            operation_barrier.wait();
            let operation = reserve.join().unwrap();
            release.join().unwrap().unwrap();
            match operation {
                Ok(operation) => {
                    assert!(
                        operation
                            ._session
                            .0
                            .closing
                            .load(std::sync::atomic::Ordering::Acquire)
                    );
                    operation.reject("UNIMPLEMENTED", "test operation");
                }
                Err(ConnectReserveError::Request(error)) => {
                    assert_eq!(error.name, "SessionReleased");
                }
                Err(error) => panic!("unexpected Connect reservation error: {error:?}"),
            }
            assert!(
                !server
                    .inner
                    .execution
                    .session_registry()
                    .lock()
                    .unwrap()
                    .contains_key(&operation_session_key)
            );
        }
    }

    #[tokio::test]
    async fn generated_rpc_uses_invalid_session_and_duplicate_operation_statuses() {
        let server = Server::new(ServerConfig::default()).unwrap();
        let service = ConnectService { server };
        let request = connect_proto::ExecutePlanRequest {
            session_id: Uuid::new_v4().to_string(),
            user_context: Some(connect_proto::UserContext {
                user_id: "fixture-user".into(),
                ..Default::default()
            }),
            operation_id: Some(Uuid::new_v4().to_string()),
            plan: Some(connect_proto::Plan {
                op_type: Some(connect_proto::plan::OpType::Root(connect_proto::Relation {
                    rel_type: Some(connect_proto::relation::RelType::Sql(connect_proto::Sql {
                        query: "SELECT 1".into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };
        let first_response = service
            .execute_plan(Request::new(request.clone()))
            .await
            .expect("ExecutePlan returns a response stream");
        let mut first_stream = first_response.into_inner();
        let first = tonic::codegen::tokio_stream::StreamExt::next(&mut first_stream)
            .await
            .expect("unsupported plan yields a stream error")
            .unwrap_err();
        assert_eq!(first.code(), Code::Unimplemented);
        let duplicate = service
            .execute_plan(Request::new(request.clone()))
            .await
            .err()
            .unwrap();
        assert_eq!(duplicate.code(), Code::AlreadyExists);
        assert!(
            duplicate
                .message()
                .contains("INVALID_HANDLE.OPERATION_ALREADY_EXISTS")
        );

        let mut conflicting = request;
        conflicting.client_type = Some("different request bytes".into());
        let conflict = service
            .execute_plan(Request::new(conflicting))
            .await
            .err()
            .unwrap();
        assert_eq!(conflict.code(), Code::InvalidArgument);

        let malformed = connect_proto::ExecutePlanRequest {
            session_id: "not-a-uuid".into(),
            user_context: Some(connect_proto::UserContext {
                user_id: "fixture-user".into(),
                ..Default::default()
            }),
            plan: Some(connect_proto::Plan::default()),
            ..Default::default()
        };
        let malformed_status = service
            .execute_plan(Request::new(malformed))
            .await
            .err()
            .unwrap();
        assert_eq!(malformed_status.code(), Code::InvalidArgument);

        let unsupported = service
            .artifact_status(Request::new(
                connect_proto::ArtifactStatusesRequest::default(),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(unsupported.code(), Code::Unimplemented);
        assert!(
            unsupported
                .message()
                .contains("outside the 4.18 Connect subset")
        );
    }

    #[tokio::test]
    async fn generated_grpc_listener_enforces_the_wire_message_limit() {
        let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);

        let server = Server::new(ServerConfig::default()).unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .serve_connect_until(address, async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2));
        let channel = endpoint.connect().await.unwrap();
        let mut client = tonic::client::Grpc::new(channel);
        client.ready().await.unwrap();
        let request = connect_proto::ExecutePlanRequest {
            client_type: Some("x".repeat(MAX_CONNECT_PROTO_BYTES + 1)),
            ..Default::default()
        };
        let status = client
            .server_streaming(
                Request::new(request),
                http::uri::PathAndQuery::from_static(
                    "/spark.connect.SparkConnectService/ExecutePlan",
                ),
                sparkless_server_connect::BoundedProstCodec::<
                    connect_proto::ExecutePlanRequest,
                    connect_proto::ExecutePlanResponse,
                >::default(),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(
            status.code(),
            Code::OutOfRange,
            "unexpected gRPC size status: {}",
            status.message()
        );

        let _ = shutdown_tx.send(());
        server_task.await.unwrap().unwrap();
    }

    fn identity(
        user_id: &str,
        client_session_id: &str,
        observed_server_session_id: Option<&str>,
    ) -> ConnectSessionIdentity {
        ConnectSessionIdentity {
            user_id: user_id.into(),
            client_session_id: client_session_id.into(),
            observed_server_session_id: observed_server_session_id.map(str::to_string),
        }
    }

    fn nested_data_type(depth: usize) -> connect_proto::DataType {
        let mut data_type = connect_proto::DataType {
            kind: Some(Kind::Boolean(Boolean {
                type_variation_reference: 0,
            })),
        };
        for _ in 0..depth {
            data_type = connect_proto::DataType {
                kind: Some(Kind::Array(Box::new(Array {
                    element_type: Some(Box::new(data_type)),
                    contains_null: true,
                    type_variation_reference: 0,
                }))),
            };
        }
        data_type
    }
}
