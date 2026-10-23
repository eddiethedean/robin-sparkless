use bytes::Bytes;
use http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE};
use http::{Method, Request, Response, StatusCode, header};
use http_body::Body;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use robin_sparkless::CancellationToken;
use serde_json::Value;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinSet;
use tokio::time::{Instant, Sleep};
use uuid::Uuid;

use crate::config::ServerConfig;
use crate::protocol::{
    CONTROL_PROTOCOL_VERSION, CloseRequest, ControlRecord, HTTP_AGENT_VERSION,
    MAX_CONTROL_FRAME_BYTES, StatementRequest, error_envelope, success_envelope, transport_error,
};
use crate::runtime::RuntimeState;
use crate::sessions::{OperationRegistration, SessionError};
use crate::sql::{self, SqlError};

const MAX_CONNECTION_AGE_SECS: u64 = 60;
const RESPONSE_CHUNK_BYTES: usize = 64 * 1024;
const CONTROL_REQUEST_HEADERS_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_HTTP_REQUEST_HEADERS_BYTES: usize = 32 * 1024;

pub async fn serve(config: ServerConfig) -> Result<(), String> {
    config.validate().map_err(|error| error.to_string())?;
    let address = std::net::SocketAddr::new(config.bind, config.port);
    let listener = TcpListener::bind(address)
        .await
        .map_err(|error| format!("cannot bind HTTP listener at {address}: {error}"))?;
    let bound_address = listener
        .local_addr()
        .map_err(|error| format!("cannot read bound listener address: {error}"))?;
    let state = RuntimeState::new(config.clone());

    let startup_state = Arc::clone(&state);
    tokio::time::timeout(
        Duration::from_secs(config.limits.startup_timeout_secs),
        tokio::task::spawn_blocking(move || startup_state.sessions.verify_engine()),
    )
    .await
    .map_err(|_| "native startup witness exceeded the startup timeout".to_owned())?
    .map_err(|error| format!("native startup witness task failed: {error}"))??;
    state.set_ready(true);

    let launch_id = config
        .launch_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let capabilities = ["http_agent_v1", "read_only_sql", "typed_scalar_results"];
    let http_url = format!("http://{bound_address}");
    if config.control {
        let record = ControlRecord {
            kind: "ready",
            protocol_version: CONTROL_PROTOCOL_VERSION,
            agent_version: HTTP_AGENT_VERSION,
            binary_version: env!("CARGO_PKG_VERSION"),
            launch_id: &launch_id,
            http_url: &http_url,
            capabilities: &capabilities,
        };
        write_control_record(&record)?;
    } else {
        eprintln!("sparkless-server listening at {http_url}");
    }

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    if config.control {
        let control_tx = shutdown_tx.clone();
        let expected_launch_id = launch_id.clone();
        std::thread::Builder::new()
            .name("sparkless-control-input".into())
            .spawn(move || {
                if let Err(error) = read_control_input_blocking(expected_launch_id, control_tx) {
                    eprintln!("control channel closed: {error}");
                }
            })
            .map_err(|error| format!("cannot start control input reader: {error}"))?;
    }
    let reaper_state = Arc::clone(&state);
    let reaper = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            reaper_state.sessions.reap_idle();
        }
    });

    let mut connections = JoinSet::new();
    let shutdown_signal = shutdown_signal()?;
    tokio::pin!(shutdown_signal);
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((mut stream, _peer)) => {
                        let _ = stream.set_nodelay(true);
                        if let Ok(connection_permit) =
                            Arc::clone(&state.connections).try_acquire_owned()
                        {
                            let connection_state = Arc::clone(&state);
                            connections.spawn(async move {
                                let _connection_permit = connection_permit;
                                serve_connection(stream, connection_state).await;
                            });
                        } else if let Ok(probe_permit) =
                            Arc::clone(&state.control_connection_probes).try_acquire_owned()
                        {
                            let connection_state = Arc::clone(&state);
                            connections.spawn(async move {
                                let _probe_permit = probe_permit;
                                let Ok(Ok(Some(request_headers))) = tokio::time::timeout(
                                    CONTROL_REQUEST_HEADERS_TIMEOUT,
                                    read_http_request_headers(&mut stream),
                                )
                                .await
                                else {
                                    return;
                                };
                                if !is_control_request(&request_headers) {
                                    return;
                                }
                                let Ok(control_permit) = Arc::clone(
                                    &connection_state.control_connections,
                                )
                                .try_acquire_owned()
                                else {
                                    return;
                                };
                                drop(_probe_permit);
                                let _control_permit = control_permit;
                                serve_connection(
                                    PrefixedIo::new(stream, request_headers),
                                    connection_state,
                                )
                                .await;
                            });
                        } else {
                            drop(stream);
                        }
                    }
                    Err(error) => {
                        eprintln!("HTTP accept failed: {error}");
                    }
                }
            }
            _ = &mut shutdown_signal => break,
            Some(_) = connections.join_next(), if connections.len() > 32 => {}
        }
    }

    state.begin_drain();
    reaper.abort();
    let connections_drained = tokio::time::timeout(
        Duration::from_secs(config.limits.drain_timeout_secs),
        async { while connections.join_next().await.is_some() {} },
    )
    .await
    .is_ok();
    if !connections_drained {
        connections.abort_all();
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            while connections.join_next().await.is_some() {}
        })
        .await;
    }
    let native_drained = state
        .wait_for_compute_drain(Duration::from_secs(config.limits.drain_timeout_secs))
        .await;
    if !native_drained {
        let forced_drained = state
            .wait_for_compute_drain(Duration::from_secs(config.limits.kill_timeout_secs))
            .await;
        if !forced_drained {
            eprintln!(
                "shutdown deadline expired while native work remained active; terminating owned server process"
            );
            std::process::exit(1);
        }
    }
    if config.control {
        write_control_record(&serde_json::json!({
            "type": "stopped",
            "protocol_version": CONTROL_PROTOCOL_VERSION,
            "launch_id": launch_id
        }))?;
    }
    Ok(())
}

fn shutdown_signal() -> Result<Pin<Box<dyn Future<Output = ()> + Send>>, String> {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|error| format!("cannot register SIGTERM handler: {error}"))?;
        Ok(Box::pin(async move {
            tokio::pin!(ctrl_c);
            tokio::select! {
                _ = &mut ctrl_c => {},
                _ = terminate.recv() => {},
            }
        }))
    }
    #[cfg(not(unix))]
    {
        Ok(Box::pin(async move {
            let _ = ctrl_c.await;
        }))
    }
}

async fn serve_connection<I>(stream: I, state: Arc<RuntimeState>)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |request| {
        let state = Arc::clone(&state);
        async move { Ok::<_, Infallible>(handle_request(request, state).await) }
    });
    let mut builder = http1::Builder::new();
    builder.keep_alive(false);
    let connection = builder.serve_connection(TokioIo::new(stream), service);
    let _ = tokio::time::timeout(Duration::from_secs(MAX_CONNECTION_AGE_SECS), connection).await;
}

async fn read_http_request_headers(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut request_headers = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    loop {
        if stream.read(&mut byte).await? == 0 {
            return Ok(None);
        }
        request_headers.push(byte[0]);
        if request_headers.ends_with(b"\r\n\r\n") {
            return Ok(Some(request_headers));
        }
        if request_headers.len() >= MAX_HTTP_REQUEST_HEADERS_BYTES {
            return Ok(None);
        }
    }
}

fn is_control_request(request_headers: &[u8]) -> bool {
    let Some(request_line) = request_headers.split(|byte| *byte == b'\n').next() else {
        return false;
    };
    let Ok(request_line) = std::str::from_utf8(request_line) else {
        return false;
    };
    let mut parts = request_line.split_ascii_whitespace();
    let (Some(method), Some(target), Some(version)) = (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    if parts.next().is_some() || !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return false;
    }
    let path = target.split_once('?').map_or(target, |(path, _)| path);
    matches!(
        (method, path),
        ("GET", "/live" | "/health" | "/capabilities") | ("POST", "/close")
    )
}

struct PrefixedIo {
    stream: TcpStream,
    prefix: Vec<u8>,
    offset: usize,
}

impl PrefixedIo {
    fn new(stream: TcpStream, prefix: Vec<u8>) -> Self {
        Self {
            stream,
            prefix,
            offset: 0,
        }
    }
}

impl AsyncRead for PrefixedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if this.offset < this.prefix.len() {
            let count = buffer
                .remaining()
                .min(this.prefix.len().saturating_sub(this.offset));
            buffer.put_slice(&this.prefix[this.offset..this.offset + count]);
            this.offset += count;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.stream).poll_read(cx, buffer)
    }
}

impl AsyncWrite for PrefixedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.as_mut().get_mut().stream).poll_write(cx, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.as_mut().get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.as_mut().get_mut().stream).poll_shutdown(cx)
    }
}

async fn handle_request(
    request: Request<Incoming>,
    state: Arc<RuntimeState>,
) -> Response<ServerBody> {
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/live") => control_route(&state, || {
            (StatusCode::OK, serde_json::json!({"status": "alive"}))
        }),
        (&Method::GET, "/health") => health_route(&state),
        (&Method::GET, "/capabilities") => control_route(&state, || {
            if state.is_draining() {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    transport_error("ServiceUnavailable", "server is draining"),
                )
            } else {
                (StatusCode::OK, state.capabilities())
            }
        }),
        (&Method::POST, "/statements") => statement_route(request, state).await,
        (&Method::POST, "/close") => close_route(request, state).await,
        _ => transport_response(
            StatusCode::NOT_FOUND,
            "NotFound",
            "route not found",
            None,
            Duration::from_secs(5),
        ),
    }
}

fn control_route(
    state: &RuntimeState,
    create: impl FnOnce() -> (StatusCode, Value),
) -> Response<ServerBody> {
    let Ok(permit) = Arc::clone(&state.health_requests).try_acquire_owned() else {
        return transport_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "ControlCapacityExceeded",
            "health and control capacity is full",
            None,
            Duration::from_secs(state.config.limits.result_send_timeout_secs),
        );
    };
    let (status, value) = create();
    json_response(
        status,
        value,
        Some(permit),
        Duration::from_secs(state.config.limits.result_send_timeout_secs),
    )
}

fn health_route(state: &RuntimeState) -> Response<ServerBody> {
    control_route(state, || {
        if state.is_ready() {
            (StatusCode::OK, serde_json::json!({"status": "ready"}))
        } else if state.is_draining() {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({"status": "draining"}),
            )
        } else {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({"status": "starting"}),
            )
        }
    })
}

async fn statement_route(
    request: Request<Incoming>,
    state: Arc<RuntimeState>,
) -> Response<ServerBody> {
    let accepted_at = Instant::now();
    if state.is_draining() {
        return transport_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "server is draining",
            None,
            Duration::from_secs(state.config.limits.result_send_timeout_secs),
        );
    }
    let Ok(http_permit) = Arc::clone(&state.http_requests).try_acquire_owned() else {
        return overloaded(&state, "live statement request capacity is full");
    };
    let http_lease = Arc::new(http_permit);
    let body = match read_json_body(request, &state.config).await {
        Ok(body) => body,
        Err(error) => {
            return error.response_with_request_lease(&state, Some(Arc::clone(&http_lease)));
        }
    };
    let request: StatementRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return transport_response_with_request_lease(
                StatusCode::BAD_REQUEST,
                "InvalidJson",
                &error.to_string(),
                Arc::clone(&http_lease),
                send_timeout(&state),
            );
        }
    };
    if let Err(error) = validate_session_id(&request.session) {
        return transport_response_with_request_lease(
            StatusCode::BAD_REQUEST,
            "InvalidSession",
            &error,
            Arc::clone(&http_lease),
            send_timeout(&state),
        );
    }
    if request.code.len() > state.config.limits.max_sql_bytes {
        return transport_response_with_request_lease(
            StatusCode::PAYLOAD_TOO_LARGE,
            "SqlTooLarge",
            "SQL text exceeds the configured byte limit",
            Arc::clone(&http_lease),
            send_timeout(&state),
        );
    }
    if !request.kind.eq_ignore_ascii_case("sql") {
        return agent_error_with_request_lease(
            &state,
            "UnsupportedOperation",
            "only kind=sql is supported",
            Arc::clone(&http_lease),
        );
    }
    let Ok(admission_permit) = Arc::clone(&state.admission).try_acquire_owned() else {
        return transport_response_with_request_lease(
            StatusCode::TOO_MANY_REQUESTS,
            "AdmissionRejected",
            "operation admission queue is full",
            Arc::clone(&http_lease),
            send_timeout(&state),
        );
    };
    let session = match state.sessions.get_or_create(&request.session) {
        Ok(session) => session,
        Err(SessionError::Capacity) => {
            return transport_response_with_request_lease(
                StatusCode::TOO_MANY_REQUESTS,
                "SessionCapacityExceeded",
                "session registry is full",
                Arc::clone(&http_lease),
                send_timeout(&state),
            );
        }
        Err(SessionError::Closing) => {
            return agent_error_with_request_lease(
                &state,
                "SessionClosing",
                "session is closing",
                Arc::clone(&http_lease),
            );
        }
        Err(SessionError::Poisoned) => {
            return transport_response_with_request_lease(
                StatusCode::SERVICE_UNAVAILABLE,
                "SessionUnavailable",
                "session state is unavailable",
                Arc::clone(&http_lease),
                send_timeout(&state),
            );
        }
    };
    let Ok(session_permit) = Arc::clone(&session.request_slots).try_acquire_owned() else {
        return transport_response_with_request_lease(
            StatusCode::TOO_MANY_REQUESTS,
            "AdmissionRejected",
            "session operation queue is full",
            Arc::clone(&http_lease),
            send_timeout(&state),
        );
    };
    if session.is_closing() {
        return agent_error_with_request_lease(
            &state,
            "SessionClosing",
            "session is closing",
            Arc::clone(&http_lease),
        );
    }
    let token = CancellationToken::new();
    let _cancel_on_drop = CancellationOnDrop::new(token.clone());
    let operation_id = Uuid::new_v4().to_string();
    let operation_registration = match session.register_operation(operation_id, token.clone()) {
        Ok(registration) => registration,
        Err(SessionError::Closing) => {
            return agent_error_with_request_lease(
                &state,
                "SessionClosing",
                "session is closing",
                Arc::clone(&http_lease),
            );
        }
        Err(SessionError::Capacity) => {
            return transport_response_with_request_lease(
                StatusCode::TOO_MANY_REQUESTS,
                "AdmissionRejected",
                "session operation queue is full",
                Arc::clone(&http_lease),
                send_timeout(&state),
            );
        }
        Err(SessionError::Poisoned) => {
            return transport_response_with_request_lease(
                StatusCode::SERVICE_UNAVAILABLE,
                "SessionUnavailable",
                "session state is unavailable",
                Arc::clone(&http_lease),
                send_timeout(&state),
            );
        }
    };

    let response_deadline =
        accepted_at + Duration::from_secs(state.config.limits.response_deadline_secs);
    let queue_deadline = accepted_at + Duration::from_millis(state.config.limits.queue_wait_ms);
    let wait_deadline = response_deadline.min(queue_deadline);
    let serial = Arc::clone(&session.serial);
    let serial_guard = match wait_for_queue(
        async move { Ok::<_, ()>(serial.lock_owned().await) },
        token.clone(),
        wait_deadline,
    )
    .await
    {
        Ok(guard) => guard,
        Err(QueueWaitFailure::Cancelled) => {
            return agent_error_with_request_lease(
                &state,
                "SessionClosing",
                "session closed before the operation started",
                Arc::clone(&http_lease),
            );
        }
        Err(QueueWaitFailure::TimedOut) => {
            if Instant::now() >= response_deadline {
                return agent_error_with_request_lease(
                    &state,
                    "DeadlineExceeded",
                    "response deadline expired while waiting for the session",
                    Arc::clone(&http_lease),
                );
            }
            return transport_response_with_request_lease(
                StatusCode::TOO_MANY_REQUESTS,
                "AdmissionRejected",
                "queue wait exceeded the configured limit",
                Arc::clone(&http_lease),
                send_timeout(&state),
            );
        }
    };
    let retained_results = Arc::clone(&state.retained_results);
    let result_permit = match wait_for_queue(
        async move { retained_results.acquire_owned().await },
        token.clone(),
        wait_deadline,
    )
    .await
    {
        Ok(permit) => permit,
        Err(QueueWaitFailure::Cancelled) => {
            return agent_error_with_request_lease(
                &state,
                "SessionClosing",
                "session closed before the operation started",
                Arc::clone(&http_lease),
            );
        }
        Err(QueueWaitFailure::TimedOut) => {
            if Instant::now() >= response_deadline {
                return agent_error_with_request_lease(
                    &state,
                    "DeadlineExceeded",
                    "response deadline expired while waiting for retained result capacity",
                    Arc::clone(&http_lease),
                );
            }
            return transport_response_with_request_lease(
                StatusCode::TOO_MANY_REQUESTS,
                "AdmissionRejected",
                "retained result capacity wait exceeded the configured queue limit",
                Arc::clone(&http_lease),
                send_timeout(&state),
            );
        }
    };
    let compute = Arc::clone(&state.compute);
    let compute_permit = match wait_for_queue(
        async move { compute.acquire_owned().await },
        token.clone(),
        wait_deadline,
    )
    .await
    {
        Ok(permit) => permit,
        Err(QueueWaitFailure::Cancelled) => {
            return agent_error_with_request_lease(
                &state,
                "SessionClosing",
                "session closed before the operation started",
                Arc::clone(&http_lease),
            );
        }
        Err(QueueWaitFailure::TimedOut) => {
            if Instant::now() >= response_deadline {
                return agent_error_with_request_lease(
                    &state,
                    "DeadlineExceeded",
                    "response deadline expired while waiting for native capacity",
                    Arc::clone(&http_lease),
                );
            }
            return transport_response_with_request_lease(
                StatusCode::TOO_MANY_REQUESTS,
                "AdmissionRejected",
                "queue wait exceeded the configured limit",
                Arc::clone(&http_lease),
                send_timeout(&state),
            );
        }
    };

    let worker_state = Arc::clone(&state);
    let worker_session = Arc::clone(&session);
    let worker_http_lease = Arc::clone(&http_lease);
    let worker_token = token.clone();
    let worker_sql = request.code;
    let worker_handle = tokio::task::spawn_blocking(move || {
        let _http_request_lease = worker_http_lease;
        let _admission_permit = admission_permit;
        let _session_permit = session_permit;
        let _compute_permit = compute_permit;
        let _serial_guard = serial_guard;
        let _operation_registration: OperationRegistration = operation_registration;
        let result_reservation = result_permit;
        let query = sql::validate_query(&worker_sql, &worker_state.config.limits)?;
        let query_result = sql::execute_query(
            &worker_session.engine,
            query,
            &worker_token,
            &worker_state.config.limits,
        )?;
        let envelope = success_envelope(query_result.schema, query_result.rows);
        let encoded = serde_json::to_vec(&envelope).map_err(|error| SqlError {
            name: "ResultEncodingError",
            message: error.to_string(),
        })?;
        if encoded.len() > worker_state.config.limits.max_response_bytes {
            return Err(SqlError {
                name: "ResultLimitExceeded",
                message: "complete encoded response exceeds the configured byte limit".into(),
            });
        }
        Ok((encoded, result_reservation))
    });
    match tokio::time::timeout_at(response_deadline, worker_handle).await {
        Err(_) => {
            token.cancel();
            agent_error_with_request_lease(
                &state,
                "DeadlineExceeded",
                "response deadline expired; native cancellation is cooperative",
                Arc::clone(&http_lease),
            )
        }
        Ok(Err(error)) => agent_error_with_request_lease(
            &state,
            "QueryExecutionError",
            &format!("native worker failed: {error}"),
            Arc::clone(&http_lease),
        ),
        Ok(Ok(Err(error))) => agent_error_with_request_lease(
            &state,
            error.name,
            &error.message,
            Arc::clone(&http_lease),
        ),
        Ok(Ok(Ok((encoded, result_reservation)))) => bytes_response_with_request_lease(
            StatusCode::OK,
            Bytes::from(encoded),
            Some(result_reservation),
            Arc::clone(&http_lease),
            send_timeout(&state),
        ),
    }
}

async fn close_route(request: Request<Incoming>, state: Arc<RuntimeState>) -> Response<ServerBody> {
    if state.is_draining() {
        return transport_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "server is draining",
            None,
            send_timeout(&state),
        );
    }
    let Ok(control_permit) = Arc::clone(&state.health_requests).try_acquire_owned() else {
        return overloaded(&state, "control capacity is full");
    };
    let body = match read_json_body(request, &state.config).await {
        Ok(body) => body,
        Err(error) => return error.response_with_control_permit(&state, control_permit),
    };
    let request: CloseRequest = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return transport_response(
                StatusCode::BAD_REQUEST,
                "InvalidJson",
                &error.to_string(),
                Some(control_permit),
                send_timeout(&state),
            );
        }
    };
    if let Err(error) = validate_session_id(&request.session) {
        return transport_response(
            StatusCode::BAD_REQUEST,
            "InvalidSession",
            &error,
            Some(control_permit),
            send_timeout(&state),
        );
    }
    match state.sessions.close(&request.session) {
        Ok(()) => json_response(
            StatusCode::OK,
            serde_json::json!({"status": "ok"}),
            Some(control_permit),
            send_timeout(&state),
        ),
        Err(_) => transport_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "SessionUnavailable",
            "session state is unavailable",
            Some(control_permit),
            send_timeout(&state),
        ),
    }
}

async fn wait_for_queue<T, E>(
    acquisition: impl Future<Output = Result<T, E>>,
    token: CancellationToken,
    deadline: Instant,
) -> Result<T, QueueWaitFailure> {
    tokio::pin!(acquisition);
    loop {
        if token.is_cancelled() {
            return Err(QueueWaitFailure::Cancelled);
        }
        tokio::select! {
            acquired = tokio::time::timeout_at(deadline, &mut acquisition) => return match acquired {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(_)) | Err(_) => Err(QueueWaitFailure::TimedOut),
            },
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
}

enum QueueWaitFailure {
    Cancelled,
    TimedOut,
}

struct CancellationOnDrop {
    token: CancellationToken,
}

impl CancellationOnDrop {
    fn new(token: CancellationToken) -> Self {
        Self { token }
    }
}

impl Drop for CancellationOnDrop {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

enum BodyError {
    BadRequest(&'static str, String),
    UnsupportedMedia(&'static str, String),
    TooLarge,
    Timeout,
}

impl BodyError {
    fn response_with_request_lease(
        self,
        state: &RuntimeState,
        request_lease: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Response<ServerBody> {
        self.response_with_permits(state, request_lease, None)
    }

    fn response_with_control_permit(
        self,
        state: &RuntimeState,
        control_permit: OwnedSemaphorePermit,
    ) -> Response<ServerBody> {
        self.response_with_permits(state, None, Some(control_permit))
    }

    fn response_with_permits(
        self,
        state: &RuntimeState,
        request_lease: Option<Arc<OwnedSemaphorePermit>>,
        control_permit: Option<OwnedSemaphorePermit>,
    ) -> Response<ServerBody> {
        let (status, name, message) = match self {
            Self::BadRequest(name, message) => (StatusCode::BAD_REQUEST, name, message),
            Self::UnsupportedMedia(name, message) => {
                (StatusCode::UNSUPPORTED_MEDIA_TYPE, name, message)
            }
            Self::TooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "RequestTooLarge",
                "request body exceeds the configured byte limit".into(),
            ),
            Self::Timeout => (
                StatusCode::REQUEST_TIMEOUT,
                "BodyReadTimeout",
                "request body was not received before the read deadline".into(),
            ),
        };
        match request_lease {
            Some(lease) => transport_response_with_request_lease(
                status,
                name,
                &message,
                lease,
                send_timeout(state),
            ),
            None => transport_response(status, name, &message, control_permit, send_timeout(state)),
        }
    }
}

async fn read_json_body(
    request: Request<Incoming>,
    config: &ServerConfig,
) -> Result<Bytes, BodyError> {
    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|header| header.to_str().ok())
        .unwrap_or_default();
    if !content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("application/json")
    {
        return Err(BodyError::UnsupportedMedia(
            "UnsupportedContentType",
            "Content-Type must be application/json".into(),
        ));
    }
    if request
        .headers()
        .get(CONTENT_ENCODING)
        .and_then(|header| header.to_str().ok())
        .is_some_and(|encoding| !encoding.eq_ignore_ascii_case("identity"))
    {
        return Err(BodyError::UnsupportedMedia(
            "UnsupportedContentEncoding",
            "compressed request bodies are not supported".into(),
        ));
    }
    if request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|header| header.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > config.limits.max_request_bytes)
    {
        return Err(BodyError::TooLarge);
    }
    let body_deadline = Instant::now() + Duration::from_secs(config.limits.body_read_timeout_secs);
    let mut body = request.into_body();
    let mut bytes = Vec::new();
    loop {
        let frame = match tokio::time::timeout_at(body_deadline, body.frame()).await {
            Err(_) => return Err(BodyError::Timeout),
            Ok(None) => break,
            Ok(Some(Err(error))) => {
                return Err(BodyError::BadRequest("InvalidBody", error.to_string()));
            }
            Ok(Some(Ok(frame))) => frame,
        };
        if let Ok(data) = frame.into_data() {
            if bytes.len().saturating_add(data.len()) > config.limits.max_request_bytes {
                return Err(BodyError::TooLarge);
            }
            bytes.extend_from_slice(&data);
        }
    }
    Ok(Bytes::from(bytes))
}

fn validate_session_id(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
        return Err("session must contain 1 to 256 non-control characters".into());
    }
    Ok(())
}

fn overloaded(state: &RuntimeState, message: &str) -> Response<ServerBody> {
    transport_response(
        StatusCode::TOO_MANY_REQUESTS,
        "AdmissionRejected",
        message,
        None,
        send_timeout(state),
    )
}

fn agent_error_with_request_lease(
    state: &RuntimeState,
    name: &str,
    message: &str,
    request_lease: Arc<OwnedSemaphorePermit>,
) -> Response<ServerBody> {
    json_response_with_request_lease(
        StatusCode::OK,
        error_envelope(name, message),
        request_lease,
        send_timeout(state),
    )
}

fn transport_response(
    status: StatusCode,
    name: &str,
    message: &str,
    permit: Option<OwnedSemaphorePermit>,
    timeout: Duration,
) -> Response<ServerBody> {
    json_response(status, transport_error(name, message), permit, timeout)
}

fn transport_response_with_request_lease(
    status: StatusCode,
    name: &str,
    message: &str,
    request_lease: Arc<OwnedSemaphorePermit>,
    timeout: Duration,
) -> Response<ServerBody> {
    json_response_with_request_lease(
        status,
        transport_error(name, message),
        request_lease,
        timeout,
    )
}

fn json_response(
    status: StatusCode,
    value: Value,
    permit: Option<OwnedSemaphorePermit>,
    timeout: Duration,
) -> Response<ServerBody> {
    let bytes = serde_json::to_vec(&value).unwrap_or_else(|_| {
        br#"{"error":"EncodingError","message":"response encoding failed"}"#.to_vec()
    });
    bytes_response(status, Bytes::from(bytes), permit, timeout)
}

fn json_response_with_request_lease(
    status: StatusCode,
    value: Value,
    request_lease: Arc<OwnedSemaphorePermit>,
    timeout: Duration,
) -> Response<ServerBody> {
    let bytes = serde_json::to_vec(&value).unwrap_or_else(|_| {
        br#"{"error":"EncodingError","message":"response encoding failed"}"#.to_vec()
    });
    bytes_response_with_request_lease(status, Bytes::from(bytes), None, request_lease, timeout)
}

fn bytes_response(
    status: StatusCode,
    bytes: Bytes,
    permit: Option<OwnedSemaphorePermit>,
    timeout: Duration,
) -> Response<ServerBody> {
    let mut response = Response::new(ServerBody::new(bytes, permit, timeout));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        header::HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

fn bytes_response_with_request_lease(
    status: StatusCode,
    bytes: Bytes,
    permit: Option<OwnedSemaphorePermit>,
    request_lease: Arc<OwnedSemaphorePermit>,
    timeout: Duration,
) -> Response<ServerBody> {
    let body = ServerBody::new(bytes, permit, timeout).with_request_lease(request_lease);
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        header::HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

fn send_timeout(state: &RuntimeState) -> Duration {
    Duration::from_secs(state.config.limits.result_send_timeout_secs)
}

struct ServerBody {
    bytes: Bytes,
    offset: usize,
    permit: Option<OwnedSemaphorePermit>,
    request_lease: Option<Arc<OwnedSemaphorePermit>>,
    timeout: Duration,
    timer: Pin<Box<Sleep>>,
    done: bool,
}

impl ServerBody {
    fn new(bytes: Bytes, permit: Option<OwnedSemaphorePermit>, timeout: Duration) -> Self {
        Self {
            bytes,
            offset: 0,
            permit,
            request_lease: None,
            timeout,
            timer: Box::pin(tokio::time::sleep(timeout)),
            done: false,
        }
    }

    fn with_request_lease(mut self, request_lease: Arc<OwnedSemaphorePermit>) -> Self {
        self.request_lease = Some(request_lease);
        self
    }
}

impl Body for ServerBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.done {
            return Poll::Ready(None);
        }
        if self.timer.as_mut().poll(context).is_ready() {
            self.done = true;
            self.permit.take();
            self.request_lease.take();
            return Poll::Ready(Some(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "response send wait exceeded the configured deadline",
            ))));
        }
        if self.offset >= self.bytes.len() {
            self.done = true;
            self.permit.take();
            self.request_lease.take();
            return Poll::Ready(None);
        }
        let end = self
            .offset
            .saturating_add(RESPONSE_CHUNK_BYTES)
            .min(self.bytes.len());
        let frame = http_body::Frame::data(self.bytes.slice(self.offset..end));
        self.offset = end;
        let timeout = self.timeout;
        self.timer.as_mut().reset(Instant::now() + timeout);
        Poll::Ready(Some(Ok(frame)))
    }

    fn is_end_stream(&self) -> bool {
        self.done || self.offset >= self.bytes.len()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let remaining = self.bytes.len().saturating_sub(self.offset) as u64;
        http_body::SizeHint::with_exact(remaining)
    }
}

pub async fn read_control_input(
    expected_launch_id: String,
    shutdown: tokio::sync::watch::Sender<bool>,
) -> Result<(), String> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut frame = Vec::with_capacity(256);
    loop {
        let available = reader.fill_buf().await.map_err(|error| error.to_string())?;
        if available.is_empty() {
            let _ = shutdown.send(true);
            return Ok(());
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.map_or(available.len(), |index| index + 1);
        if frame.len().saturating_add(count) > MAX_CONTROL_FRAME_BYTES {
            return Err("launcher control record exceeds 16 KiB".into());
        }
        frame.extend_from_slice(&available[..count]);
        reader.consume(count);
        if newline.is_some() {
            break;
        }
    }
    let record: Value = serde_json::from_slice(&frame).map_err(|error| error.to_string())?;
    if record.get("type").and_then(Value::as_str) != Some("shutdown")
        || record.get("launch_id").and_then(Value::as_str) != Some(expected_launch_id.as_str())
    {
        return Err("invalid or mismatched launcher control record".into());
    }
    let _ = shutdown.send(true);
    Ok(())
}

fn read_control_input_blocking(
    expected_launch_id: String,
    shutdown: tokio::sync::watch::Sender<bool>,
) -> Result<(), String> {
    use std::io::{BufRead, Read};
    let stdin = std::io::stdin();
    let mut reader = stdin.lock().take((MAX_CONTROL_FRAME_BYTES + 1) as u64);
    let mut frame = Vec::with_capacity(256);
    let bytes_read = reader
        .read_until(b'\n', &mut frame)
        .map_err(|error| error.to_string())?;
    if bytes_read == 0 {
        let _ = shutdown.send(true);
        return Ok(());
    }
    if frame.len() > MAX_CONTROL_FRAME_BYTES {
        return Err("launcher control record exceeds 16 KiB".into());
    }
    let record: Value = serde_json::from_slice(&frame).map_err(|error| error.to_string())?;
    if record.get("type").and_then(Value::as_str) != Some("shutdown")
        || record.get("launch_id").and_then(Value::as_str) != Some(expected_launch_id.as_str())
    {
        return Err("invalid or mismatched launcher control record".into());
    }
    let _ = shutdown.send(true);
    Ok(())
}

fn write_control_record(record: &impl serde::Serialize) -> Result<(), String> {
    use std::io::Write;
    let encoded = serde_json::to_vec(record).map_err(|error| error.to_string())?;
    if encoded.len() > MAX_CONTROL_FRAME_BYTES {
        return Err("server control record exceeds 16 KiB".into());
    }
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    stdout
        .write_all(&encoded)
        .map_err(|error| error.to_string())?;
    stdout.write_all(b"\n").map_err(|error| error.to_string())?;
    stdout.flush().map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Limits;
    use http_body_util::BodyExt;
    use tokio::sync::oneshot;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn health_stays_ready_while_timed_out_native_work_retains_its_permit() {
        let state = RuntimeState::new(ServerConfig {
            limits: Limits {
                max_active_operations: 1,
                ..Limits::default()
            },
            ..ServerConfig::default()
        });
        state.set_ready(true);

        let permit = Arc::clone(&state.compute)
            .acquire_owned()
            .await
            .expect("initial compute permit");
        let token = CancellationToken::new();
        let worker_token = token.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel::<()>();
        let mut worker = tokio::task::spawn_blocking(move || {
            let _compute_permit = permit;
            let _worker_token = worker_token;
            let _ = started_tx.send(());
            let _ = finish_rx.recv();
        });
        started_rx.await.expect("blocking worker started");

        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut worker)
                .await
                .is_err(),
            "the controlled native producer should outlive the response deadline"
        );
        token.cancel();
        assert_eq!(state.compute.available_permits(), 0);
        assert!(Arc::clone(&state.compute).try_acquire_owned().is_err());

        let response = health_route(&state);
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("health response body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("health JSON");
        assert_eq!(value["status"], "ready");
        assert_eq!(state.compute.available_permits(), 0);

        drop(finish_tx);
        worker.await.expect("native producer exits");
        assert!(state.wait_for_compute_drain(Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn operation_queue_wait_obeys_deadline_while_capacity_is_exhausted() {
        let state = RuntimeState::new(ServerConfig {
            limits: Limits {
                max_active_operations: 1,
                ..Limits::default()
            },
            ..ServerConfig::default()
        });
        let _compute_permit = Arc::clone(&state.compute)
            .acquire_owned()
            .await
            .expect("initial compute permit");
        let compute = Arc::clone(&state.compute);
        let result = wait_for_queue(
            async move { compute.acquire_owned().await },
            CancellationToken::new(),
            Instant::now() + Duration::from_millis(10),
        )
        .await;
        assert!(matches!(result, Err(QueueWaitFailure::TimedOut)));
    }
}
