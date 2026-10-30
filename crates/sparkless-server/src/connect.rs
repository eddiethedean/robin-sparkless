//! Spark Connect 4.0.5 gRPC adapter and bounded protobuf boundary.
//!
//! The bounded public service translates the frozen relation subset, serves schema/config
//! analysis, streams native Arrow results, and retains bounded response history for reconnect
//! while sharing the HTTP service's identity and admission registries.

use crate::{ConnectReserveError, ConnectSessionIdentity, RequestFailure, Server, connect_proto};
use bytes::Buf;
use prost::Message;
use prost_reflect::{DescriptorPool, Kind as ReflectKind, MessageDescriptor};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream};
use tonic::codec::{BufferSettings, Codec, DecodeBuf, Decoder};
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

static CONNECT_PRODUCER_STARTS: AtomicU64 = AtomicU64::new(0);
const CONNECT_GRACEFUL_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_FORCE_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

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
pub const MAX_CONNECT_LIVE_OPERATIONS: usize = 128;
pub const MAX_CONNECT_STREAM_SENDERS: usize = 32;
pub const MAX_CONNECT_HISTORY_ENTRIES: usize = 1024;
pub const MAX_CONNECT_HISTORY_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_CONNECT_REPLAY_OPERATION_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CONNECT_REPLAY_PROCESS_BYTES: usize = 32 * 1024 * 1024;
pub const CONNECT_DETACHED_RETENTION: Duration = Duration::from_secs(60);

struct StoredConnectResponse {
    sequence: u64,
    response: connect_proto::ExecutePlanResponse,
    encoded_bytes: usize,
}

struct StoredConnectError {
    id: String,
    class: String,
    message: String,
    created_at: Instant,
}

struct ConnectReplayState {
    responses: VecDeque<StoredConnectResponse>,
    response_ids: HashMap<String, u64>,
    released_response_ids: HashMap<String, u64>,
    released_response_order: VecDeque<String>,
    next_sequence: u64,
    replay_bytes: usize,
    sender_bytes: usize,
    replay_headroom_bytes: usize,
    sender_reservations: HashMap<u64, (usize, bool)>,
    next_sender_reservation: u64,
    history_bytes: usize,
    terminal: Option<Result<(), Status>>,
    attached: bool,
    released: bool,
    expired: bool,
    detached_at: Option<Instant>,
    sender: Option<tokio::sync::mpsc::Sender<()>>,
    cancellation: robin_sparkless_core::engine::CancellationToken,
    producer_stopped: bool,
    producer_done: tokio::sync::watch::Sender<bool>,
    error: Option<StoredConnectError>,
}

/// Bounded in-memory response history for one Connect operation. The producer owns this state
/// after admission; transports only attach cursors and acknowledge response prefixes.
pub(crate) struct ConnectReplayRecord {
    pub(crate) operation_key: String,
    pub(crate) session_key: String,
    pub(crate) session_generation: u64,
    pub(crate) reattachable: bool,
    pub(crate) tags: HashSet<String>,
    pub(crate) operation_id: String,
    state: Mutex<ConnectReplayState>,
    process_bytes: Arc<AtomicUsize>,
    process_history_bytes: Arc<AtomicUsize>,
}

impl ConnectReplayRecord {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        operation_key: String,
        session_key: String,
        client_session_id: String,
        server_side_session_id: String,
        operation_id: String,
        session_generation: u64,
        reattachable: bool,
        tags: Vec<String>,
        process_bytes: Arc<AtomicUsize>,
        process_history_bytes: Arc<AtomicUsize>,
    ) -> Result<Self, ()> {
        let (producer_done, _) = tokio::sync::watch::channel(false);
        let tag_bytes = tags.iter().map(String::len).sum::<usize>();
        let base_history_bytes = operation_key
            .len()
            .saturating_add(session_key.len())
            .saturating_add(client_session_id.len())
            .saturating_add(server_side_session_id.len())
            .saturating_add(operation_id.len())
            .saturating_add(tag_bytes)
            .saturating_add(256);
        if !reserve_history_bytes(&process_history_bytes, base_history_bytes) {
            return Err(());
        }
        Ok(Self {
            operation_key,
            session_key,
            session_generation,
            reattachable,
            tags: tags.into_iter().collect(),
            operation_id,
            state: Mutex::new(ConnectReplayState {
                responses: VecDeque::new(),
                response_ids: HashMap::new(),
                released_response_ids: HashMap::new(),
                released_response_order: VecDeque::new(),
                next_sequence: 0,
                replay_bytes: 0,
                sender_bytes: 0,
                replay_headroom_bytes: 0,
                sender_reservations: HashMap::new(),
                next_sender_reservation: 1,
                history_bytes: base_history_bytes,
                terminal: None,
                attached: true,
                released: false,
                expired: false,
                detached_at: None,
                sender: None,
                cancellation: robin_sparkless_core::engine::CancellationToken::new(),
                producer_stopped: false,
                producer_done,
                error: None,
            }),
            process_bytes,
            process_history_bytes,
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, ConnectReplayState>, Status> {
        self.state
            .lock()
            .map_err(|_| Status::internal("Connect replay state is unavailable"))
    }

    pub(crate) fn is_producer_active(&self) -> bool {
        self.state
            .lock()
            .map(|state| !state.producer_stopped)
            .unwrap_or(false)
    }

    pub(crate) fn retain_in_registry(&self) -> bool {
        self.state
            .lock()
            .map(|state| {
                state
                    .error
                    .as_ref()
                    .is_some_and(|error| error.created_at.elapsed() < CONNECT_DETACHED_RETENTION)
                    || (!state.released
                        && !state.expired
                        && (self.reattachable || !state.producer_stopped))
            })
            .unwrap_or(false)
    }

    pub(crate) fn cancellation(&self) -> robin_sparkless_core::engine::CancellationToken {
        self.state
            .lock()
            .map(|state| state.cancellation.clone())
            .unwrap_or_default()
    }

    pub(crate) fn matches_session(&self, session_key: &str, generation: u64) -> bool {
        self.session_key == session_key && self.session_generation == generation
    }

    fn notify(state: &ConnectReplayState) {
        if let Some(sender) = state.sender.as_ref() {
            let _ = sender.try_send(());
        }
    }

    fn reserve_process_bytes(&self, bytes: usize) -> bool {
        let mut current = self.process_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > MAX_CONNECT_REPLAY_PROCESS_BYTES {
                return false;
            }
            match self.process_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn release_process_bytes(&self, bytes: usize) {
        if bytes != 0 {
            self.process_bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
    }

    fn reserve_history_bytes(&self, bytes: usize) -> bool {
        reserve_history_bytes(&self.process_history_bytes, bytes)
    }

    fn release_history_bytes(&self, bytes: usize) {
        if bytes != 0 {
            self.process_history_bytes
                .fetch_sub(bytes, Ordering::AcqRel);
        }
    }

    fn store_error(&self, state: &mut ConnectReplayState, status: Status) -> Status {
        const ERROR_DETAIL_BASE_BYTES: usize = 96;
        let message = status.message().chars().take(1024).collect::<String>();
        let error_bytes = ERROR_DETAIL_BASE_BYTES.saturating_add(message.len());
        if !self.reserve_history_bytes(error_bytes) {
            return Status::new(status.code(), message);
        }
        state.history_bytes += error_bytes;
        let error_id = Uuid::new_v4().to_string();
        state.error = Some(StoredConnectError {
            id: error_id.clone(),
            class: "SPARKLESS_CONNECT_ERROR".to_string(),
            message: message.clone(),
            created_at: Instant::now(),
        });
        enrich_connect_status(Status::new(status.code(), message), &error_id)
    }

    fn append(&self, response: connect_proto::ExecutePlanResponse) -> Result<(), Status> {
        self.append_inner(response, false)
    }

    fn append_completion(
        &self,
        response: connect_proto::ExecutePlanResponse,
    ) -> Result<(), Status> {
        self.append_inner(response, true)
    }

    fn append_inner(
        &self,
        response: connect_proto::ExecutePlanResponse,
        completes_operation: bool,
    ) -> Result<(), Status> {
        if !self.reattachable {
            return Ok(());
        }
        let encoded_bytes = response.encoded_len();
        let mut state = self.lock()?;
        if state.released || state.expired {
            return Err(Status::cancelled("Connect operation was released"));
        }
        if let Some(Err(status)) = state.terminal.as_ref() {
            return Err(status.clone());
        }
        if completes_operation && state.terminal.is_some() {
            return Err(Status::failed_precondition(
                "Connect operation already has a terminal outcome",
            ));
        }
        let next_replay_bytes = state.replay_bytes.saturating_add(encoded_bytes);
        let largest_response = state
            .responses
            .iter()
            .map(|response| response.encoded_bytes)
            .max()
            .unwrap_or(0)
            .max(encoded_bytes);
        let next_headroom = largest_response;
        let additional_headroom = next_headroom.saturating_sub(state.replay_headroom_bytes);
        if next_replay_bytes
            .saturating_add(state.sender_bytes)
            .saturating_add(next_headroom)
            > MAX_CONNECT_REPLAY_OPERATION_BYTES
        {
            return Err(Status::resource_exhausted(format!(
                "Connect replay and sender buffers exceed {MAX_CONNECT_REPLAY_OPERATION_BYTES} bytes for one operation"
            )));
        }
        if !self.reserve_process_bytes(encoded_bytes.saturating_add(additional_headroom)) {
            return Err(Status::resource_exhausted(format!(
                "Connect replay exceeds the {MAX_CONNECT_REPLAY_PROCESS_BYTES}-byte process budget"
            )));
        }
        const RESPONSE_INDEX_BYTES: usize = 64;
        if !self.reserve_history_bytes(RESPONSE_INDEX_BYTES) {
            self.release_process_bytes(encoded_bytes.saturating_add(additional_headroom));
            return Err(Status::resource_exhausted(format!(
                "Connect response history exceeds the {MAX_CONNECT_HISTORY_BYTES}-byte metadata budget"
            )));
        }
        let sequence = state.next_sequence;
        state.next_sequence = state.next_sequence.checked_add(1).ok_or_else(|| {
            self.release_process_bytes(encoded_bytes.saturating_add(additional_headroom));
            self.release_history_bytes(RESPONSE_INDEX_BYTES);
            Status::resource_exhausted("Connect response sequence overflow")
        })?;
        state.replay_bytes = next_replay_bytes;
        state.replay_headroom_bytes = next_headroom;
        state.history_bytes += RESPONSE_INDEX_BYTES;
        state
            .response_ids
            .insert(response.response_id.clone(), sequence);
        state.responses.push_back(StoredConnectResponse {
            sequence,
            response,
            encoded_bytes,
        });
        if completes_operation {
            state.terminal = Some(Ok(()));
        }
        Self::notify(&state);
        Ok(())
    }

    fn reserve_sender_buffer(&self, bytes: usize) -> Result<u64, Status> {
        if !self.reattachable {
            return Ok(0);
        }
        let mut state = self.lock()?;
        if state
            .replay_bytes
            .saturating_add(state.sender_bytes)
            .saturating_add(state.replay_headroom_bytes)
            .saturating_add(bytes)
            > MAX_CONNECT_REPLAY_OPERATION_BYTES
        {
            return Err(Status::resource_exhausted(format!(
                "Connect replay and sender buffers exceed {MAX_CONNECT_REPLAY_OPERATION_BYTES} bytes for one operation"
            )));
        }
        let id = state.next_sender_reservation;
        let next_id = state
            .next_sender_reservation
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("Connect sender reservation id overflow"))?;
        if !self.reserve_process_bytes(bytes) {
            return Err(Status::resource_exhausted(format!(
                "Connect replay and sender buffers exceed the {MAX_CONNECT_REPLAY_PROCESS_BYTES}-byte process budget"
            )));
        }
        state.next_sender_reservation = next_id;
        state.sender_bytes = state.sender_bytes.saturating_add(bytes);
        state.sender_reservations.insert(id, (bytes, false));
        Ok(id)
    }

    fn release_sender_buffer(&self, reservation_id: u64) {
        if !self.reattachable {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            let Some((released, from_headroom)) = state.sender_reservations.remove(&reservation_id)
            else {
                return;
            };
            state.sender_bytes -= released;
            if from_headroom && !state.released && !state.expired {
                let required_headroom = state
                    .responses
                    .iter()
                    .map(|response| response.encoded_bytes)
                    .max()
                    .unwrap_or(0);
                let restored =
                    released.min(required_headroom.saturating_sub(state.replay_headroom_bytes));
                state.replay_headroom_bytes += restored;
                self.release_process_bytes(released.saturating_sub(restored));
            } else {
                self.release_process_bytes(released);
            }
        }
    }

    pub(crate) fn finish(&self, outcome: ConnectOutcome) -> ConnectOutcome {
        let Ok(mut state) = self.state.lock() else {
            return outcome;
        };
        if let Some(terminal) = state.terminal.clone() {
            return terminal;
        }
        let outcome = match outcome {
            Ok(()) => Ok(()),
            Err(status) => Err(self.store_error(&mut state, status)),
        };
        state.terminal = Some(outcome.clone());
        Self::notify(&state);
        outcome
    }

    pub(crate) fn mark_producer_stopped(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.producer_stopped = true;
            state.producer_done.send_replace(true);
            Self::notify(&state);
        }
    }

    pub(crate) fn producer_done(&self) -> tokio::sync::watch::Receiver<bool> {
        self.state
            .lock()
            .map(|state| state.producer_done.subscribe())
            .unwrap_or_else(|_| tokio::sync::watch::channel(true).1)
    }

    pub(crate) fn detach(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.attached = false;
            state.sender = None;
            if self.reattachable && !state.released && !state.expired {
                state.detached_at = Some(Instant::now());
            } else if !self.reattachable {
                state.cancellation.cancel();
            }
        }
    }

    fn attach(
        self: &Arc<Self>,
        last_response_id: Option<&str>,
    ) -> Result<(u64, tokio::sync::mpsc::Receiver<()>), Status> {
        if !self.reattachable {
            return Err(Status::failed_precondition(
                "INVALID_HANDLE.OPERATION_NOT_REATTACHABLE: operation is not reattachable",
            ));
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let mut state = self.lock()?;
        if state.released || state.expired {
            return Err(Status::not_found(
                "INVALID_HANDLE.OPERATION_NOT_FOUND: retained operation is unavailable",
            ));
        }
        if state.attached {
            return Err(Status::already_exists(
                "Connect operation already has an attached response stream",
            ));
        }
        let cursor = if let Some(response_id) = last_response_id {
            if let Some(sequence) = state
                .response_ids
                .get(response_id)
                .or_else(|| state.released_response_ids.get(response_id))
            {
                sequence.saturating_add(1)
            } else {
                return Err(Status::not_found(
                    "INVALID_HANDLE.RESPONSE_NOT_FOUND: reattach cursor is not retained",
                ));
            }
        } else {
            0
        };
        let first_retained = state
            .responses
            .front()
            .map(|response| response.sequence)
            .unwrap_or(state.next_sequence);
        if cursor < first_retained {
            return Err(Status::out_of_range(
                "INVALID_HANDLE.RESPONSE_NOT_FOUND: requested response prefix has been released",
            ));
        }
        state.attached = true;
        state.detached_at = None;
        state.sender = Some(sender.clone());
        if cursor < state.next_sequence || state.terminal.is_some() {
            let _ = sender.try_send(());
        }
        Ok((cursor, receiver))
    }

    fn next_response_for_sender(&self, cursor: u64) -> Result<ReplayPoll, Status> {
        self.next_response_inner(cursor, true)
    }

    fn next_response_inner(&self, cursor: u64, reserve_sender: bool) -> Result<ReplayPoll, Status> {
        let mut state = self.lock()?;
        if state.released {
            return match state.terminal.clone() {
                Some(Err(status)) => Ok(ReplayPoll::Error(status)),
                Some(Ok(())) => Ok(ReplayPoll::Complete),
                None => Ok(ReplayPoll::Error(Status::cancelled(
                    "Connect operation was released",
                ))),
            };
        }
        let first_retained = state
            .responses
            .front()
            .map(|response| response.sequence)
            .unwrap_or(state.next_sequence);
        if cursor < first_retained {
            return Err(Status::out_of_range(
                "INVALID_HANDLE.RESPONSE_NOT_FOUND: requested response prefix has been released",
            ));
        }
        if let Some(index) = state
            .responses
            .iter()
            .position(|response| response.sequence == cursor)
        {
            if reserve_sender {
                let bytes = state.responses[index].encoded_bytes;
                if bytes > state.replay_headroom_bytes {
                    return Err(Status::resource_exhausted(
                        "Connect replay headroom is unavailable",
                    ));
                }
                if state
                    .replay_bytes
                    .saturating_add(state.sender_bytes)
                    .saturating_add(state.replay_headroom_bytes)
                    > MAX_CONNECT_REPLAY_OPERATION_BYTES
                {
                    return Err(Status::resource_exhausted(format!(
                        "Connect replay and sender buffers exceed {MAX_CONNECT_REPLAY_OPERATION_BYTES} bytes for one operation"
                    )));
                }
                let id = state.next_sender_reservation;
                state.next_sender_reservation = state
                    .next_sender_reservation
                    .checked_add(1)
                    .ok_or_else(|| {
                        Status::resource_exhausted("Connect sender reservation id overflow")
                    })?;
                state.replay_headroom_bytes -= bytes;
                state.sender_bytes = state.sender_bytes.saturating_add(bytes);
                state.sender_reservations.insert(id, (bytes, true));
                return Ok(ReplayPoll::Response(
                    Box::new(state.responses[index].response.clone()),
                    id,
                ));
            }
            return Ok(ReplayPoll::Response(
                Box::new(state.responses[index].response.clone()),
                0,
            ));
        }
        let outcome = state.terminal.clone();
        if let Some(outcome) = outcome
            && cursor >= state.next_sequence
        {
            return match outcome {
                Ok(()) => Ok(ReplayPoll::Complete),
                Err(status) => Ok(ReplayPoll::Error(status)),
            };
        }
        Ok(ReplayPoll::Pending)
    }

    fn release_until(&self, response_id: &str) -> usize {
        let Ok(mut state) = self.state.lock() else {
            return 0;
        };
        let Some(sequence) = state.response_ids.get(response_id).copied() else {
            return 0;
        };
        let mut released_bytes = 0usize;
        while state
            .responses
            .front()
            .is_some_and(|response| response.sequence <= sequence)
        {
            let Some(response) = state.responses.pop_front() else {
                break;
            };
            released_bytes = released_bytes.saturating_add(response.encoded_bytes);
            state.response_ids.remove(&response.response.response_id);
            state
                .released_response_ids
                .insert(response.response.response_id.clone(), response.sequence);
            state
                .released_response_order
                .push_back(response.response.response_id);
        }
        let mut evicted_indexes = 0usize;
        while state.released_response_order.len() > MAX_CONNECT_HISTORY_ENTRIES {
            if let Some(oldest) = state.released_response_order.pop_front() {
                state.released_response_ids.remove(&oldest);
                evicted_indexes += 1;
            }
        }
        let evicted_metadata = evicted_indexes.saturating_mul(64);
        state.history_bytes = state.history_bytes.saturating_sub(evicted_metadata);
        self.release_history_bytes(evicted_metadata);
        state.replay_bytes = state.replay_bytes.saturating_sub(released_bytes);
        self.release_process_bytes(released_bytes);
        let required_headroom = state
            .responses
            .iter()
            .map(|response| response.encoded_bytes)
            .max()
            .unwrap_or(0);
        if required_headroom < state.replay_headroom_bytes {
            let released_headroom = state.replay_headroom_bytes - required_headroom;
            state.replay_headroom_bytes = required_headroom;
            self.release_process_bytes(released_headroom);
        }
        Self::notify(&state);
        released_bytes
    }

    pub(crate) fn release_all(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.released {
            return;
        }
        state.released = true;
        state.attached = false;
        state.detached_at = None;
        let released_bytes = state.replay_bytes;
        state.replay_bytes = 0;
        let released_headroom = state.replay_headroom_bytes;
        state.replay_headroom_bytes = 0;
        let released_indexes = state.response_ids.len() + state.released_response_ids.len();
        let released_metadata = released_indexes.saturating_mul(64);
        state.history_bytes = state.history_bytes.saturating_sub(released_metadata);
        state.responses.clear();
        state.response_ids.clear();
        state.released_response_ids.clear();
        state.released_response_order.clear();
        if !state.producer_stopped {
            state.cancellation.cancel();
        }
        if state.terminal.is_none() {
            let status = Status::cancelled("Connect operation was released");
            state.terminal = Some(Err(self.store_error(&mut state, status)));
        }
        self.release_process_bytes(released_bytes.saturating_add(released_headroom));
        self.release_history_bytes(released_metadata);
        Self::notify(&state);
        state.sender = None;
    }

    pub(crate) fn cancel(&self, message: &str) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.producer_stopped || state.terminal.is_some() {
            return false;
        }
        state.cancellation.cancel();
        let status = Status::cancelled(message.to_string());
        state.terminal = Some(Err(self.store_error(&mut state, status)));
        Self::notify(&state);
        true
    }

    pub(crate) fn expire_if_detached(&self, now: Instant) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.released
            || state.expired
            || state.attached
            || state
                .detached_at
                .is_none_or(|detached| now.duration_since(detached) < CONNECT_DETACHED_RETENTION)
        {
            return false;
        }
        state.expired = true;
        state.detached_at = None;
        state.cancellation.cancel();
        if state.terminal.is_none() {
            let status = Status::deadline_exceeded("Connect detached execution expired");
            state.terminal = Some(Err(self.store_error(&mut state, status)));
        }
        let released_bytes = state.replay_bytes;
        state.replay_bytes = 0;
        let released_headroom = state.replay_headroom_bytes;
        state.replay_headroom_bytes = 0;
        let released_indexes = state.response_ids.len() + state.released_response_ids.len();
        let released_metadata = released_indexes.saturating_mul(64);
        state.history_bytes = state.history_bytes.saturating_sub(released_metadata);
        state.responses.clear();
        state.response_ids.clear();
        state.released_response_ids.clear();
        state.released_response_order.clear();
        Self::notify(&state);
        self.release_process_bytes(released_bytes.saturating_add(released_headroom));
        self.release_history_bytes(released_metadata);
        true
    }

    fn fail_if_detached_stalled(&self, now: Instant) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if !self.reattachable
            || state.producer_stopped
            || state.terminal.is_some()
            || state.attached
            || state
                .detached_at
                .is_none_or(|detached| now.duration_since(detached) < Duration::from_secs(5))
        {
            return false;
        }
        state.cancellation.cancel();
        let status = Status::deadline_exceeded(
            "Connect detached response stream made no progress for 5 seconds",
        );
        state.terminal = Some(Err(self.store_error(&mut state, status)));
        Self::notify(&state);
        true
    }

    pub(crate) fn error_details(
        &self,
        error_id: &str,
    ) -> Option<connect_proto::fetch_error_details_response::Error> {
        let state = self.state.lock().ok()?;
        let error = state.error.as_ref().filter(|error| error.id == error_id)?;
        Some(connect_proto::fetch_error_details_response::Error {
            error_type_hierarchy: vec!["org.apache.spark.SparkException".to_string()],
            message: error.message.clone(),
            stack_trace: Vec::new(),
            cause_idx: None,
            spark_throwable: Some(
                connect_proto::fetch_error_details_response::SparkThrowable {
                    error_class: Some(error.class.clone()),
                    message_parameters: HashMap::new(),
                    query_contexts: Vec::new(),
                    sql_state: None,
                },
            ),
        })
    }

    pub(crate) fn session_identity(&self) -> (&str, u64) {
        (&self.session_key, self.session_generation)
    }

    pub(crate) fn has_error(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.error.is_some())
            .unwrap_or(false)
    }
}

impl Drop for ConnectReplayRecord {
    fn drop(&mut self) {
        if let Ok(state) = self.state.get_mut() {
            let retained_process_bytes = state
                .replay_bytes
                .saturating_add(state.sender_bytes)
                .saturating_add(state.replay_headroom_bytes);
            if retained_process_bytes != 0 {
                self.process_bytes
                    .fetch_sub(retained_process_bytes, Ordering::AcqRel);
            }
            if state.history_bytes != 0 {
                self.process_history_bytes
                    .fetch_sub(state.history_bytes, Ordering::AcqRel);
            }
        }
    }
}

enum ReplayPoll {
    Response(Box<connect_proto::ExecutePlanResponse>, u64),
    Error(Status),
    Complete,
    Pending,
}

enum InterruptTarget {
    All,
    Tag(String),
    Operation(String),
}

fn reserve_history_bytes(counter: &AtomicUsize, bytes: usize) -> bool {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let Some(next) = current.checked_add(bytes) else {
            return false;
        };
        if next > MAX_CONNECT_HISTORY_BYTES {
            return false;
        }
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

#[derive(prost::Message)]
struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

#[derive(prost::Message)]
struct GoogleRpcErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
    #[prost(map = "string, string", tag = "3")]
    metadata: HashMap<String, String>,
}

fn enrich_connect_status(status: Status, error_id: &str) -> Status {
    let mut metadata = HashMap::new();
    metadata.insert(
        "errorClass".to_string(),
        "SPARKLESS_CONNECT_ERROR".to_string(),
    );
    metadata.insert("errorId".to_string(), error_id.to_string());
    metadata.insert(
        "classes".to_string(),
        "[\"org.apache.spark.SparkException\"]".to_string(),
    );
    let details = GoogleRpcStatus {
        code: status.code() as i32,
        message: status.message().to_string(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.ErrorInfo".to_string(),
            value: GoogleRpcErrorInfo {
                reason: "SPARKLESS_CONNECT_ERROR".to_string(),
                domain: "sparkless".to_string(),
                metadata,
            }
            .encode_to_vec(),
        }],
    };
    Status::with_details(
        status.code(),
        status.message().to_string(),
        bytes::Bytes::from(details.encode_to_vec()),
    )
}

fn status_with_standard_details(status: Status) -> Status {
    let details = GoogleRpcStatus {
        code: status.code() as i32,
        message: status.message().to_string(),
        details: Vec::new(),
    };
    Status::with_details(
        status.code(),
        status.message().to_string(),
        bytes::Bytes::from(details.encode_to_vec()),
    )
}

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
    fn cancel_connect_work_for_shutdown(
        &self,
    ) -> Result<tokio::sync::watch::Receiver<usize>, crate::ServerError> {
        let _lifecycle =
            self.inner
                .connect_session_lifecycle
                .lock()
                .map_err(|_| crate::ServerError {
                    message: "Connect session lifecycle state is unavailable".into(),
                })?;
        self.inner.connect_draining.store(true, Ordering::Release);
        let operations = self
            .inner
            .connect_operations
            .lock()
            .map_err(|_| crate::ServerError {
                message: "Connect operation state is unavailable".into(),
            })?;
        for operation in operations.values() {
            // A terminal response can precede the blocking producer's actual return.
            operation.cancellation().cancel();
            operation.cancel("Spark Connect server is shutting down");
        }
        Ok(self.inner.connect_active_operations.subscribe())
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
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|error| crate::ServerError {
                message: format!("Spark Connect listener failed to bind {bind}: {error}"),
            })?;
        let bound = listener.local_addr().map_err(crate::ServerError::from)?;
        *self
            .inner
            .connect_address
            .lock()
            .map_err(|_| crate::ServerError {
                message: "Spark Connect listener address state is unavailable".into(),
            })? = Some(bound);
        let result = self.serve_connect_listener_until(listener, shutdown).await;
        if let Ok(mut address) = self.inner.connect_address.lock()
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
                    .connect_session_lifecycle
                    .lock()
                    .map_err(|_| crate::ServerError {
                        message: "Connect session lifecycle state is unavailable".into(),
                    })?;
            self.inner.connect_draining.store(false, Ordering::Release);
        }
        let connection_registry = Arc::new(ConnectConnectionRegistry::default());
        let incoming_registry = Arc::clone(&connection_registry);
        let incoming = TcpListenerStream::new(listener)
            .map(move |result| result.and_then(|stream| incoming_registry.track(stream)));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let serve = tonic::transport::Server::builder()
            .add_service(self.connect_service())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            });
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
            result = tokio::time::timeout_at(queue_deadline, self.server.inner.admission.active.clone().acquire_owned()) => {
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

type ConnectOutcome = Result<(), Status>;

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
    cancellation: robin_sparkless_core::engine::CancellationToken,
    terminal: ConnectTerminal,
}

struct ConnectQueuedResponse {
    response: connect_proto::ExecutePlanResponse,
    reservation_id: u64,
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
        if let Err(status) = appended {
            operation.replay.release_sender_buffer(reservation_id);
            operation.reject("CONNECT_REPLAY_CAPACITY", status.message());
            self.terminal.finish(Err(status));
            self.cancellation.cancel();
            return false;
        }
        if completes_operation {
            operation.complete();
            self.terminal.finish(Ok(()));
        }
        let send_started = now;
        loop {
            let now = Instant::now();
            if now >= self.deadline {
                operation.replay.release_sender_buffer(reservation_id);
                if self.terminal.finish_deadline(connect_deadline_status()) {
                    operation.reject(
                        "CONNECT_RESPONSE_DEADLINE",
                        "Connect execution exceeded its response deadline",
                    );
                    self.cancellation.cancel();
                }
                return false;
            }
            if now.duration_since(send_started) >= Duration::from_secs(5) {
                operation.replay.release_sender_buffer(reservation_id);
                let status = Status::deadline_exceeded(
                    "Connect response stream stalled without progress for 5 seconds",
                );
                if self.terminal.finish_deadline(status.clone()) {
                    operation.reject("CONNECT_REPLAY_STALL", status.message());
                    self.cancellation.cancel();
                }
                return false;
            }
            match self.sender.try_send(ConnectQueuedResponse {
                response,
                reservation_id,
            }) {
                Ok(()) => return true,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(queued)) => {
                    operation
                        .replay
                        .release_sender_buffer(queued.reservation_id);
                    if operation.replay.reattachable {
                        return true;
                    }
                    self.cancellation.cancel();
                    return false;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => {
                    response = returned.response;
                }
            }
            std::thread::sleep((self.deadline - now).min(Duration::from_millis(10)));
        }
    }
}

struct ConnectExecutionStream {
    receiver: tokio::sync::mpsc::Receiver<ConnectQueuedResponse>,
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
            Poll::Ready(Some(queued)) => {
                this.pending_sender_reservation = queued.reservation_id;
                Poll::Ready(Some(Ok(queued.response)))
            }
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
        self.receiver.close();
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
    deadline: Instant,
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
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let timer_cancellation = cancellation.clone();
    let timer_terminal = terminal.clone();
    let mark_deadline = operation.deadline_marker();
    let mut producer_done = replay.producer_done();
    let stall_replay = Arc::clone(&replay);
    let stall_cancellation = cancellation.clone();
    let stall_operation = Arc::clone(&operation.record);
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
        let sender = ConnectResponseSender {
            sender,
            deadline,
            cancellation: cancellation.clone(),
            terminal,
        };
        let (session_guard, compute_permit, session_compute_permit) = planning_resources;
        session.sync_thread_context();
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
            if sender.sender.is_closed() && !operation.replay.reattachable {
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
            if sender.send(Ok(response), &operation) {
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
        let sender_permit = Arc::clone(&self.server.inner.connect_stream_senders)
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
        let session = operation._session.0.session.clone();
        let server_side_session_id = operation._session.0.server_side_session_id.clone();
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
        let planning_resources = self
            .acquire_planning_resources(
                &session,
                deadline,
                robin_sparkless_core::engine::CancellationToken::new(),
            )
            .await?;
        let session_id = identity.client_session_id;
        let server_side_session_id = session.0.server_side_session_id.clone();
        let planning = tokio::task::spawn_blocking(move || {
            let _admission = admission;
            let (_session_guard, _compute_permit, _session_compute_permit) = planning_resources;
            session.0.session.sync_thread_context();
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
        let sender_permit = Arc::clone(&self.server.inner.connect_stream_senders)
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
        if let Some(operation) = operation.as_ref() {
            if !operation.reattachable {
                return Err(Status::failed_precondition(
                    "INVALID_HANDLE.OPERATION_NOT_REATTACHABLE: operation cannot be released through ReleaseExecute",
                ));
            }
            match release {
                connect_proto::release_execute_request::Release::ReleaseAll(_) => {
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
                        && let Ok(mut operations) = self.server.inner.connect_operations.lock()
                    {
                        operations.remove(&operation.operation_key);
                    }
                }
                connect_proto::release_execute_request::Release::ReleaseUntil(until) => {
                    validate_uuid("response_id", &until.response_id)?;
                    operation.release_until(&until.response_id);
                }
            }
        }
        Ok(Response::new(connect_proto::ReleaseExecuteResponse {
            session_id: identity.client_session_id,
            server_side_session_id: session.0.server_side_session_id.clone(),
            operation_id: operation.map(|operation| operation.operation_id.clone()),
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
                .connect_replay_bytes
                .load(Ordering::Acquire),
            0
        );
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

        let terminal = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match stream.next().await {
                    Some(Ok(_)) => continue,
                    Some(Err(status)) => break status,
                    None => panic!("interrupted operation must end with a visible failure"),
                }
            }
        })
        .await
        .expect("native producer observes cancellation");
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
        assert_eq!(server.inner.admission.accepted.load(Ordering::Acquire), 0);
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
        let detached_at = record.lock().unwrap().detached_at.unwrap();
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
        assert_eq!(record.lock().unwrap().sender_bytes, bytes);
        record.release_all();
        assert_eq!(record.lock().unwrap().sender_bytes, bytes);
        assert_eq!(process_bytes.load(Ordering::Acquire), bytes);
        record.release_sender_buffer(replay_sender);
        assert_eq!(record.lock().unwrap().sender_bytes, 0);
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
        assert!(matches!(record.lock().unwrap().terminal, Some(Ok(()))));
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
        assert_eq!(server.inner.connect_operations.lock().unwrap().len(), 1);

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
