//! Spark Connect 4.0.5 gRPC adapter and bounded protobuf boundary.
//!
//! The bounded public service translates the frozen relation subset, serves schema/config
//! analysis, and streams native Arrow results while sharing the HTTP service's identity and
//! admission registries. Response replay and reconnect remain a later delivery slice.

use crate::{ConnectReserveError, ConnectSessionIdentity, RequestFailure, Server, connect_proto};
use bytes::Buf;
use prost::Message;
use prost_reflect::{DescriptorPool, Kind as ReflectKind, MessageDescriptor};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

mod analysis;
mod config;
mod plan;

pub const MAX_CONNECT_PROTO_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_CONNECT_PROTO_DEPTH: usize = 64;
pub const MAX_CONNECT_PROTO_NODES: usize = 10_000;
pub const MAX_CONNECT_SCHEMA_FIELDS: usize = 256;
pub const MAX_CONNECT_LOCAL_IPC_BYTES: usize = 1024 * 1024;
pub const MAX_CONNECT_LOCAL_DECODED_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CONNECT_LOCAL_ROWS: usize = 10_000;
pub const MAX_CONNECT_LOCAL_SCALAR_BYTES: usize = 512 * 1024;
pub const MAX_CONNECT_ARROW_BATCH_ROWS: usize = 16_384;
pub const MAX_CONNECT_ARROW_RESPONSE_BYTES: usize = 1024 * 1024;
pub const MAX_CONNECT_RESULT_ROWS: u64 = 1_000_000;
pub const MAX_CONNECT_RESULT_BYTES: usize = 64 * 1024 * 1024;

/// Tonic codec whose wire scanner rejects structurally excessive protobufs before Prost
/// allocates their generated message trees.
pub(crate) struct BoundedProstCodec<T, U>(PhantomData<(T, U)>);

#[derive(Clone, Debug)]
pub struct BoundedProstDecoder<T>(PhantomData<T>);

impl<T, U> Default for BoundedProstCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T, U> Codec for BoundedProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = tonic_prost::ProstEncoder<T>;
    type Decoder = BoundedProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        tonic_prost::ProstEncoder::default()
    }

    fn decoder(&mut self) -> Self::Decoder {
        BoundedProstDecoder(PhantomData)
    }
}

impl<T> Decoder for BoundedProstDecoder<T>
where
    T: Message + Default,
{
    type Item = T;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let message_name = std::any::type_name::<T>()
            .rsplit("::")
            .next()
            .ok_or_else(|| Status::internal("could not name Connect protobuf request"))?;
        preflight_wire_message(&format!("spark.connect.{message_name}"), src.chunk())?;
        Message::decode(src)
            .map(Some)
            .map_err(|error: prost::DecodeError| Status::internal(error.to_string()))
    }

    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(8 * 1024, 32 * 1024)
    }
}

use connect_proto::spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer};

/// Shared native Spark Connect adapter. It owns no engine, catalog, session registry, or
/// admission pool; requests are resolved through the supplied server instance.
#[derive(Clone)]
pub struct ConnectService {
    server: Server,
}

impl Server {
    /// Construct the generated Spark Connect service with Sparkless' inbound and outbound
    /// protobuf message caps. Callers choose the listener address and lifecycle independently.
    pub fn connect_service(&self) -> SparkConnectServiceServer<ConnectService> {
        SparkConnectServiceServer::new(ConnectService {
            server: self.clone(),
        })
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
        tonic::transport::Server::builder()
            .add_service(self.connect_service())
            .serve_with_shutdown(bind, shutdown)
            .await
            .map_err(|error| crate::ServerError {
                message: format!("Spark Connect listener failed: {error}"),
            })
    }
}

impl ConnectService {
    async fn acquire_planning_resources(
        &self,
        session: &crate::SessionLease,
        deadline: Instant,
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
        let session_guard =
            tokio::time::timeout_at(queue_deadline, session.0.serial.clone().lock_owned())
                .await
                .map_err(|_| {
                    Status::deadline_exceeded("timed out waiting for Connect session planning")
                })?;
        let session_compute_permit = tokio::time::timeout_at(
            queue_deadline,
            session.0.connect_execution.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("timed out waiting for Connect session execution"))?
        .map_err(|_| Status::unavailable("Connect session execution is closed"))?;
        let compute_permit = tokio::time::timeout_at(
            queue_deadline,
            self.server.inner.admission.active.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("timed out waiting for Connect compute admission"))?
        .map_err(|_| Status::unavailable("Connect compute admission is closed"))?;
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

fn connect_execution_failure(message: String) -> Status {
    let lower = message.to_ascii_lowercase();
    if lower.contains("exceed") || lower.contains("limit") || lower.contains("too large") {
        Status::resource_exhausted(message)
    } else {
        Status::internal(format!("CONNECT_EXECUTION_FAILED: {message}"))
    }
}

type ConnectOutcome = Result<(), Status>;

/// The terminal outcome has its own slot, so a full batch queue cannot discard an error.
#[derive(Clone)]
struct ConnectTerminal(Arc<Mutex<Option<tokio::sync::oneshot::Sender<ConnectOutcome>>>>);

impl ConnectTerminal {
    fn finish(&self, outcome: ConnectOutcome) -> bool {
        let Some(sender) = self.0.lock().ok().and_then(|mut sender| sender.take()) else {
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
    sender: tokio::sync::mpsc::Sender<connect_proto::ExecutePlanResponse>,
    deadline: Instant,
    cancellation: robin_sparkless_core::engine::CancellationToken,
    terminal: ConnectTerminal,
}

impl ConnectResponseSender {
    fn send(
        &self,
        item: Result<connect_proto::ExecutePlanResponse, Status>,
        operation: &crate::ConnectOperationLease,
    ) -> bool {
        let mut response = match item {
            Ok(response) => response,
            Err(status) => {
                self.terminal.finish(Err(status));
                return false;
            }
        };
        loop {
            let now = Instant::now();
            if now >= self.deadline {
                operation.reject(
                    "CONNECT_RESPONSE_DEADLINE",
                    "Connect execution exceeded its response deadline",
                );
                self.terminal.finish(Err(connect_deadline_status()));
                self.cancellation.cancel();
                return false;
            }
            match self.sender.try_send(response) {
                Ok(()) => return true,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    self.cancellation.cancel();
                    return false;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => response = returned,
            }
            std::thread::sleep((self.deadline - now).min(Duration::from_millis(10)));
        }
    }
}

struct ConnectExecutionStream {
    receiver: tokio::sync::mpsc::Receiver<connect_proto::ExecutePlanResponse>,
    outcome: Option<tokio::sync::oneshot::Receiver<ConnectOutcome>>,
    cancellation: robin_sparkless_core::engine::CancellationToken,
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
        if this.ended {
            return Poll::Ready(None);
        }
        if let Some(outcome) = this.outcome.as_mut() {
            match std::pin::Pin::new(outcome).poll(cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(Ok(()))) => this.outcome = None,
                Poll::Ready(result) => {
                    this.ended = true;
                    this.receiver.close();
                    this.cancellation.cancel();
                    let status = match result {
                        Ok(Err(status)) => status,
                        _ => Status::internal("Connect producer ended without a terminal outcome"),
                    };
                    return Poll::Ready(Some(Err(status)));
                }
            }
        }
        match this.receiver.poll_recv(cx) {
            Poll::Ready(Some(response)) => Poll::Ready(Some(Ok(response))),
            Poll::Ready(None) if this.outcome.is_some() => Poll::Pending,
            Poll::Ready(None) => {
                this.ended = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ConnectExecutionStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct ConnectProducerFinished(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for ConnectProducerFinished {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn start_connect_producer(
    operation: crate::ConnectOperationLease,
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
    sender: tokio::sync::mpsc::Sender<connect_proto::ExecutePlanResponse>,
    deadline: Instant,
) -> tokio::sync::oneshot::Receiver<ConnectOutcome> {
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
    let terminal = ConnectTerminal(Arc::new(Mutex::new(Some(outcome_tx))));
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let timer_cancellation = cancellation.clone();
    let timer_terminal = terminal.clone();
    let mark_deadline = operation.deadline_marker();
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                if timer_terminal.finish(Err(connect_deadline_status())) {
                    mark_deadline();
                    timer_cancellation.cancel();
                }
            }
            _ = finished_rx => {}
        }
    });
    tokio::task::spawn_blocking(move || {
        let _finished = ConnectProducerFinished(Some(finished_tx));
        let sender = ConnectResponseSender {
            sender,
            deadline,
            cancellation: cancellation.clone(),
            terminal,
        };
        let (session_guard, compute_permit, session_compute_permit) = planning_resources;
        let translated = plan::translate_plan(&session, &plan).and_then(|translated| {
            let schema = plan::schema_to_proto(&translated.schema)?;
            Ok((translated, schema))
        });
        drop(session_guard);
        let _session_compute_permit = session_compute_permit;
        let (translated, schema) = match translated {
            Ok(translated) => translated,
            Err(status) => {
                operation.reject("CONNECT_PLAN_TRANSLATION_FAILED", status.message());
                let _ = sender.send(Err(status), &operation);
                return;
            }
        };
        let dataframe = if let Some(count_input) = translated.deferred_count_input {
            let count_result = count_input
                .count_with_cancellation(&cancellation)
                .and_then(|count| {
                    i64::try_from(count).map_err(|_| {
                        robin_sparkless::EngineError::User(
                            "Connect count result exceeds the signed 64-bit range".into(),
                        )
                    })
                })
                .and_then(|count| {
                    session.create_dataframe_from_i64_column("count(1)", vec![count])
                });
            match count_result {
                Ok(dataframe) => dataframe,
                Err(error) => {
                    let status = connect_execution_failure(error.to_string());
                    operation.reject("CONNECT_COUNT_FAILED", status.message());
                    let _ = sender.send(Err(status), &operation);
                    return;
                }
            }
        } else {
            translated.dataframe
        };
        let schema_overhead = schema.encoded_len();
        let Some(max_arrow_payload_bytes) = MAX_CONNECT_ARROW_RESPONSE_BYTES
            .checked_sub(schema_overhead)
            .and_then(|bytes| bytes.checked_sub(512))
            .filter(|bytes| *bytes > 0)
        else {
            let status = Status::resource_exhausted(
                "Connect output schema leaves no room for a bounded Arrow response",
            );
            operation.reject("CONNECT_ARROW_SCHEMA_LIMIT", status.message());
            let _ = sender.send(Err(status), &operation);
            return;
        };
        let _compute_permit = compute_permit;
        let mut batches = match dataframe.to_arrow_ipc_batches_with_schema_and_cancellation(
            MAX_CONNECT_ARROW_BATCH_ROWS,
            max_arrow_payload_bytes,
            &cancellation,
            &translated.schema,
        ) {
            Ok(batches) => batches,
            Err(error) => {
                let status = connect_execution_failure(error.to_string());
                operation.reject("CONNECT_ARROW_STREAM_FAILED", status.message());
                let _ = sender.send(Err(status), &operation);
                return;
            }
        };
        let mut row_offset = 0u64;
        let mut response_bytes = 0usize;
        let mut first_batch = true;
        loop {
            if sender.sender.is_closed() {
                cancellation.cancel();
                return;
            }
            let Some(batch) = batches.next() else {
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
            if sender.send(Ok(response), &operation) && sender.terminal.finish(Ok(())) {
                operation.complete();
            }
        } else if sender.terminal.finish(Ok(())) {
            operation.complete();
        }
    });
    outcome_rx
}

type ConnectResponseStream = std::pin::Pin<
    Box<
        dyn tonic::codegen::tokio_stream::Stream<
                Item = Result<connect_proto::ExecutePlanResponse, Status>,
            > + Send
            + 'static,
    >,
>;

#[tonic::async_trait]
impl SparkConnectService for ConnectService {
    type ExecutePlanStream = ConnectResponseStream;
    type ReattachExecuteStream = ConnectResponseStream;

    async fn execute_plan(
        &self,
        request: Request<connect_proto::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let mut request = request.into_inner();
        validate_request_budget("ExecutePlanRequest", &request)?;
        if request.plan.is_none() {
            return Err(Status::invalid_argument(
                "ExecutePlanRequest.plan is required",
            ));
        }
        let reattachable = request_reattachable(&request.request_options)?;
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
        let plan = request
            .plan
            .take()
            .expect("plan presence was validated above");
        let planning_resources = match self
            .acquire_planning_resources(&operation._session, deadline)
            .await
        {
            Ok(resources) => resources,
            Err(status) => {
                operation.reject("CONNECT_PLAN_QUEUE_TIMEOUT", status.message());
                return Err(status);
            }
        };
        let session = operation._session.0.session.clone();
        let server_side_session_id = operation._session.0.server_side_session_id.clone();
        let cancellation = robin_sparkless_core::engine::CancellationToken::new();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let outcome = start_connect_producer(
            operation,
            session,
            plan,
            planning_resources,
            client_session_id,
            server_side_session_id,
            operation_id,
            reattachable,
            cancellation.clone(),
            sender,
            deadline,
        );
        let stream = ConnectExecutionStream {
            receiver,
            outcome: Some(outcome),
            cancellation,
            ended: false,
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn analyze_plan(
        &self,
        request: Request<connect_proto::AnalyzePlanRequest>,
    ) -> Result<Response<connect_proto::AnalyzePlanResponse>, Status> {
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
        let planning_resources = self.acquire_planning_resources(&session, deadline).await?;
        let session_id = identity.client_session_id;
        let server_side_session_id = session.0.server_side_session_id.clone();
        let planning = tokio::task::spawn_blocking(move || {
            let _admission = admission;
            let (_session_guard, _compute_permit, _session_compute_permit) = planning_resources;
            let response = analysis::response(
                &session_id,
                &server_side_session_id,
                &session.0.session,
                request.analyze,
            )?;
            Ok::<_, Status>(response)
        });
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), planning).await {
            Ok(Ok(Ok(response))) => Ok(Response::new(response)),
            Ok(Ok(Err(status))) => Err(status),
            Ok(Err(error)) => Err(Status::internal(format!(
                "Connect analysis worker failed: {error}"
            ))),
            Err(_) => Err(Status::deadline_exceeded(
                "Connect analysis exceeded its deadline",
            )),
        }
    }

    async fn config(
        &self,
        request: Request<connect_proto::ConfigRequest>,
    ) -> Result<Response<connect_proto::ConfigResponse>, Status> {
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
        Err(Status::unimplemented(
            "AddArtifacts and cached local relations are outside the 4.18 Connect subset",
        ))
    }

    async fn artifact_status(
        &self,
        _request: Request<connect_proto::ArtifactStatusesRequest>,
    ) -> Result<Response<connect_proto::ArtifactStatusesResponse>, Status> {
        Err(Status::unimplemented(
            "ArtifactStatus and cached local relations are outside the 4.18 Connect subset",
        ))
    }

    async fn interrupt(
        &self,
        request: Request<connect_proto::InterruptRequest>,
    ) -> Result<Response<connect_proto::InterruptResponse>, Status> {
        let request = request.into_inner();
        validate_request_budget("InterruptRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let _session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        Err(Status::unimplemented(
            "Connect operation interruption is implemented in delivery slice 5",
        ))
    }

    async fn reattach_execute(
        &self,
        request: Request<connect_proto::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        let request = request.into_inner();
        validate_request_budget("ReattachExecuteRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let _session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        validate_uuid("operation_id", &request.operation_id)?;
        Err(Status::unimplemented(
            "Connect response reattachment is implemented in delivery slice 5",
        ))
    }

    async fn release_execute(
        &self,
        request: Request<connect_proto::ReleaseExecuteRequest>,
    ) -> Result<Response<connect_proto::ReleaseExecuteResponse>, Status> {
        let request = request.into_inner();
        validate_request_budget("ReleaseExecuteRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let _session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        validate_uuid("operation_id", &request.operation_id)?;
        Err(Status::unimplemented(
            "Connect operation release is implemented in delivery slice 5",
        ))
    }

    async fn release_session(
        &self,
        request: Request<connect_proto::ReleaseSessionRequest>,
    ) -> Result<Response<connect_proto::ReleaseSessionResponse>, Status> {
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
        let request = request.into_inner();
        validate_request_budget("FetchErrorDetailsRequest", &request)?;
        let identity = session_identity(
            request.user_context,
            request.session_id,
            request.client_observed_server_side_session_id,
        )?;
        let _session = self
            .server
            .inner
            .connect_session_lease(&identity)
            .map_err(status_from_failure)?;
        Err(Status::unimplemented(
            "Connect error-detail retrieval is implemented in delivery slice 5",
        ))
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

fn descriptor_pool() -> Result<&'static DescriptorPool, Status> {
    static POOL: OnceLock<Result<DescriptorPool, String>> = OnceLock::new();
    match POOL.get_or_init(|| {
        DescriptorPool::decode(connect_proto::FILE_DESCRIPTOR_SET)
            .map_err(|error| error.to_string())
    }) {
        Ok(pool) => Ok(pool),
        Err(_) => Err(Status::internal("could not load Spark Connect descriptors")),
    }
}

fn preflight_wire_message(message_name: &str, bytes: &[u8]) -> Result<(), Status> {
    if bytes.len() > MAX_CONNECT_PROTO_BYTES {
        return Err(Status::resource_exhausted(format!(
            "Connect protobuf exceeds the {MAX_CONNECT_PROTO_BYTES}-byte limit"
        )));
    }
    let descriptor = descriptor_pool()?
        .get_message_by_name(message_name)
        .ok_or_else(|| Status::internal("generated Connect request descriptor is missing"))?;
    scan_wire_message(&descriptor, bytes, 1, &mut ProtoLimits::default())
}

fn scan_wire_message(
    descriptor: &MessageDescriptor,
    bytes: &[u8],
    depth: usize,
    limits: &mut ProtoLimits,
) -> Result<(), Status> {
    validate_depth(depth)?;
    limits.nodes = limits.nodes.saturating_add(1);
    if limits.nodes > MAX_CONNECT_PROTO_NODES {
        return Err(Status::resource_exhausted(format!(
            "Connect protobuf exceeds {MAX_CONNECT_PROTO_NODES} message nodes"
        )));
    }
    if descriptor.full_name().ends_with(".StructField") {
        limits.schema_fields = limits.schema_fields.saturating_add(1);
        if limits.schema_fields > MAX_CONNECT_SCHEMA_FIELDS {
            return Err(Status::resource_exhausted(format!(
                "Connect schema exceeds {MAX_CONNECT_SCHEMA_FIELDS} fields"
            )));
        }
    }

    let mut cursor = 0;
    while cursor < bytes.len() {
        let key = read_varint(bytes, &mut cursor)?;
        let field_number = u32::try_from(key >> 3)
            .map_err(|_| Status::invalid_argument("protobuf field number is out of range"))?;
        let wire_type = (key & 7) as u8;
        if field_number == 0 || field_number > 536_870_911 {
            return Err(Status::invalid_argument("protobuf field number is invalid"));
        }
        let nested = descriptor
            .get_field(field_number)
            .and_then(|field| match field.kind() {
                ReflectKind::Message(message) => Some(message),
                _ => None,
            });
        if wire_type == 2 {
            let length = usize::try_from(read_varint(bytes, &mut cursor)?)
                .map_err(|_| Status::invalid_argument("protobuf field length is out of range"))?;
            let end = cursor
                .checked_add(length)
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| {
                    Status::invalid_argument("truncated protobuf length-delimited field")
                })?;
            if let Some(nested) = nested {
                scan_wire_message(&nested, &bytes[cursor..end], depth + 1, limits)?;
            }
            cursor = end;
        } else {
            skip_wire_value(bytes, &mut cursor, wire_type, field_number, depth)?;
        }
    }
    Ok(())
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, Status> {
    let mut value = 0_u64;
    for index in 0..10 {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| Status::invalid_argument("truncated protobuf varint"))?;
        *cursor += 1;
        if index == 9 && byte > 1 {
            return Err(Status::invalid_argument(
                "protobuf varint overflows 64 bits",
            ));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Status::invalid_argument("protobuf varint is too long"))
}

fn skip_wire_value(
    bytes: &[u8],
    cursor: &mut usize,
    wire_type: u8,
    field_number: u32,
    depth: usize,
) -> Result<(), Status> {
    match wire_type {
        0 => {
            read_varint(bytes, cursor)?;
        }
        1 => advance_wire_bytes(bytes, cursor, 8)?,
        2 => {
            let length = usize::try_from(read_varint(bytes, cursor)?)
                .map_err(|_| Status::invalid_argument("protobuf field length is out of range"))?;
            advance_wire_bytes(bytes, cursor, length)?;
        }
        3 => skip_group(bytes, cursor, field_number, depth + 1)?,
        4 => {
            return Err(Status::invalid_argument(
                "unexpected protobuf end-group tag",
            ));
        }
        5 => advance_wire_bytes(bytes, cursor, 4)?,
        _ => return Err(Status::invalid_argument("protobuf wire type is invalid")),
    }
    Ok(())
}

fn skip_group(
    bytes: &[u8],
    cursor: &mut usize,
    group_field_number: u32,
    depth: usize,
) -> Result<(), Status> {
    validate_depth(depth)?;
    loop {
        let key = read_varint(bytes, cursor)?;
        let field_number = u32::try_from(key >> 3)
            .map_err(|_| Status::invalid_argument("protobuf field number is out of range"))?;
        let wire_type = (key & 7) as u8;
        if field_number == 0 || field_number > 536_870_911 {
            return Err(Status::invalid_argument("protobuf field number is invalid"));
        }
        if wire_type == 4 {
            if field_number != group_field_number {
                return Err(Status::invalid_argument(
                    "protobuf end-group field does not match",
                ));
            }
            return Ok(());
        }
        skip_wire_value(bytes, cursor, wire_type, field_number, depth)?;
    }
}

fn advance_wire_bytes(bytes: &[u8], cursor: &mut usize, length: usize) -> Result<(), Status> {
    *cursor = cursor
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| Status::invalid_argument("truncated protobuf field"))?;
    Ok(())
}

#[derive(Default)]
struct ProtoLimits {
    nodes: usize,
    schema_fields: usize,
}

fn validate_depth(depth: usize) -> Result<(), Status> {
    if depth > MAX_CONNECT_PROTO_DEPTH {
        Err(Status::resource_exhausted(format!(
            "Connect protobuf nesting exceeds {MAX_CONNECT_PROTO_DEPTH}"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectReserveError, ServerConfig};
    use connect_proto::data_type::{Array, Boolean, Kind, Struct, StructField};
    use std::time::Duration;

    #[test]
    fn descriptor_set_has_the_public_service_and_bounds_nested_requests() {
        let service = descriptor_pool()
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

        let descriptor = descriptor_pool()
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
            .admission
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
                    .operation_records
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
        assert_eq!(server.inner.admission.accepted.load(Ordering::Acquire), 0);
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

        let mut stream = service
            .execute_plan(Request::new(request))
            .await?
            .into_inner();
        let mut responses = Vec::new();
        while let Some(response) = stream.next().await {
            responses.push(response?);
        }
        Ok(responses)
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
                while service.server.inner.admission.active.available_permits() != 2 {
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
            .admission
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
        let deep_type = nested_data_type(MAX_CONNECT_PROTO_DEPTH / 2 + 1);
        assert_eq!(
            preflight_wire_message("spark.connect.DataType", &deep_type.encode_to_vec())
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );

        let fields = (0..MAX_CONNECT_SCHEMA_FIELDS + 1)
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
            request_options: (0..MAX_CONNECT_PROTO_NODES)
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
    async fn connect_operations_share_admission_and_retain_duplicate_protection() {
        let server = Server::new(ServerConfig::default()).unwrap();
        let identity = identity("user", &Uuid::new_v4().to_string(), None);
        let operation_id = Uuid::new_v4().to_string();
        let first = server
            .inner
            .reserve_connect_operation(&identity, &operation_id, [7; 32])
            .unwrap();
        first.reject("UNIMPLEMENTED", "slice boundary");
        assert_eq!(
            server
                .inner
                .admission
                .accepted
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        drop(first);
        assert_eq!(
            server
                .inner
                .admission
                .accepted
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );
        let tickets = (0..server.inner.admission.max_accepted)
            .map(|_| server.inner.reserve_admission().unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(
            server
                .inner
                .reserve_connect_operation(&identity, &operation_id, [7; 32]),
            Err(ConnectReserveError::Duplicate)
        ));
        drop(tickets);
        assert!(matches!(
            server
                .inner
                .reserve_connect_operation(&identity, &operation_id, [8; 32]),
            Err(ConnectReserveError::Conflict)
        ));
    }

    #[test]
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
                    .sessions
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
                    .sessions
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
                hyper::http::uri::PathAndQuery::from_static(
                    "/spark.connect.SparkConnectService/ExecutePlan",
                ),
                tonic_prost::ProstCodec::<
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
