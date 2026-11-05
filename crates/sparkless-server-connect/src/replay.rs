//! Bounded Spark Connect replay lifecycle and retained-response accounting.

use super::connect_proto;
use prost::Message;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tonic::Status;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectReplayError {
    HistoryCapacityExceeded,
}

impl std::fmt::Display for ConnectReplayError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HistoryCapacityExceeded => {
                formatter.write_str("Connect operation history capacity was exceeded")
            }
        }
    }
}

impl std::error::Error for ConnectReplayError {}

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
pub type ConnectOutcome = Result<(), Status>;

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
    execution_waker: Option<tokio::sync::mpsc::UnboundedSender<()>>,
    cancellation: robin_sparkless_core::engine::CancellationToken,
    producer_stopped: bool,
    producer_done: tokio::sync::watch::Sender<bool>,
    error: Option<StoredConnectError>,
}

/// Bounded in-memory response history for one Connect operation. The producer owns this state
/// after admission; transports only attach cursors and acknowledge response prefixes.
pub struct ConnectReplayRecord {
    pub operation_key: String,
    pub session_key: String,
    pub session_generation: u64,
    pub reattachable: bool,
    pub tags: HashSet<String>,
    pub operation_id: String,
    state: Mutex<ConnectReplayState>,
    response_released: Condvar,
    process_bytes: Arc<AtomicUsize>,
    process_history_bytes: Arc<AtomicUsize>,
}

impl ConnectReplayRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
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
    ) -> Result<Self, ConnectReplayError> {
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
            return Err(ConnectReplayError::HistoryCapacityExceeded);
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
                execution_waker: None,
                cancellation: robin_sparkless_core::engine::CancellationToken::new(),
                producer_stopped: false,
                producer_done,
                error: None,
            }),
            response_released: Condvar::new(),
            process_bytes,
            process_history_bytes,
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, ConnectReplayState>, Status> {
        self.state
            .lock()
            .map_err(|_| Status::internal("Connect replay state is unavailable"))
    }

    pub fn is_producer_active(&self) -> bool {
        self.state
            .lock()
            .map(|state| !state.producer_stopped)
            .unwrap_or(false)
    }

    pub fn retain_in_registry(&self) -> bool {
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

    pub fn cancellation(&self) -> robin_sparkless_core::engine::CancellationToken {
        self.state
            .lock()
            .map(|state| state.cancellation.clone())
            .unwrap_or_default()
    }

    pub fn attach_execution_waker(&self) -> tokio::sync::mpsc::UnboundedReceiver<()> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        if let Ok(mut state) = self.state.lock() {
            state.execution_waker = Some(sender.clone());
            if state.cancellation.is_cancelled() {
                let _ = sender.send(());
            }
        }
        receiver
    }

    pub fn clear_execution_waker(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.execution_waker = None;
        }
    }

    pub fn cancellation_terminal_status(&self) -> Option<Status> {
        let state = self.state.lock().ok()?;
        if !state.cancellation.is_cancelled() {
            return None;
        }
        state.terminal.as_ref()?.as_ref().err().cloned()
    }

    pub fn matches_session(&self, session_key: &str, generation: u64) -> bool {
        self.session_key == session_key && self.session_generation == generation
    }

    fn notify(state: &ConnectReplayState) {
        if let Some(sender) = state.sender.as_ref() {
            let _ = sender.try_send(());
        }
    }

    fn notify_execution(state: &ConnectReplayState) {
        if let Some(sender) = state.execution_waker.as_ref() {
            let _ = sender.send(());
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

    pub fn append(&self, response: connect_proto::ExecutePlanResponse) -> Result<(), Status> {
        self.append_inner(response, false)
    }

    pub fn append_completion(
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

    pub fn reserve_sender_buffer(&self, bytes: usize) -> Result<u64, Status> {
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

    pub fn release_sender_buffer(&self, reservation_id: u64) {
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

    pub fn finish(&self, outcome: ConnectOutcome) -> ConnectOutcome {
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
        self.response_released.notify_all();
        outcome
    }

    pub fn mark_producer_stopped(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.producer_stopped = true;
            state.producer_done.send_replace(true);
            Self::notify(&state);
        }
    }

    pub fn producer_done(&self) -> tokio::sync::watch::Receiver<bool> {
        self.state
            .lock()
            .map(|state| state.producer_done.subscribe())
            .unwrap_or_else(|_| tokio::sync::watch::channel(true).1)
    }

    pub fn detach(&self) {
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

    pub fn attach(
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

    pub fn next_response_for_sender(&self, cursor: u64) -> Result<ReplayPoll, Status> {
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

    pub fn release_until(&self, response_id: &str) -> usize {
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
        self.response_released.notify_all();
        released_bytes
    }

    pub fn wait_for_response_release(
        &self,
        response_id: &str,
        deadline: Instant,
        cancellation: &robin_sparkless_core::engine::CancellationToken,
    ) -> Result<(), Status> {
        let mut state = self.lock()?;
        loop {
            if let Some(Err(status)) = state.terminal.as_ref() {
                return Err(status.clone());
            }
            if cancellation.is_cancelled() {
                return Err(Status::cancelled("Connect execution was cancelled"));
            }
            if !state.response_ids.contains_key(response_id) {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(Status::deadline_exceeded(
                    "Connect response stream stalled waiting for client acknowledgment",
                ));
            }
            let wait = (deadline - now).min(Duration::from_millis(100));
            let (next_state, _) = self
                .response_released
                .wait_timeout(state, wait)
                .map_err(|_| Status::internal("Connect replay state is unavailable"))?;
            state = next_state;
        }
    }

    pub fn wait_for_cancellation_grace(
        &self,
        duration: Duration,
        deadline: Instant,
        cancellation: &robin_sparkless_core::engine::CancellationToken,
    ) -> Result<(), Status> {
        let grace_deadline = (Instant::now() + duration).min(deadline);
        let mut state = self.lock()?;
        loop {
            if let Some(Err(status)) = state.terminal.as_ref() {
                return Err(status.clone());
            }
            if cancellation.is_cancelled() {
                return Err(Status::cancelled("Connect execution was cancelled"));
            }
            let now = Instant::now();
            if now >= grace_deadline {
                return Ok(());
            }
            let (next_state, _) = self
                .response_released
                .wait_timeout(state, grace_deadline - now)
                .map_err(|_| Status::internal("Connect replay state is unavailable"))?;
            state = next_state;
        }
    }

    pub fn release_all(&self) {
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
            Self::notify_execution(&state);
        }
        if state.terminal.is_none() {
            let status = Status::cancelled("Connect operation was released");
            state.terminal = Some(Err(self.store_error(&mut state, status)));
        }
        self.release_process_bytes(released_bytes.saturating_add(released_headroom));
        self.release_history_bytes(released_metadata);
        Self::notify(&state);
        self.response_released.notify_all();
        state.sender = None;
    }

    pub fn cancel(&self, message: &str) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.producer_stopped || state.terminal.is_some() {
            return false;
        }
        state.cancellation.cancel();
        let status = Status::cancelled(message.to_string());
        state.terminal = Some(Err(self.store_error(&mut state, status)));
        Self::notify_execution(&state);
        Self::notify(&state);
        self.response_released.notify_all();
        true
    }

    pub fn expire_if_detached(&self, now: Instant) -> bool {
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
        Self::notify_execution(&state);
        Self::notify(&state);
        self.response_released.notify_all();
        self.release_process_bytes(released_bytes.saturating_add(released_headroom));
        self.release_history_bytes(released_metadata);
        true
    }

    pub fn fail_if_detached_stalled(&self, now: Instant) -> bool {
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
        Self::notify_execution(&state);
        Self::notify(&state);
        self.response_released.notify_all();
        true
    }

    pub fn error_details(
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

    pub fn session_identity(&self) -> (&str, u64) {
        (&self.session_key, self.session_generation)
    }

    pub fn detached_at(&self) -> Option<Instant> {
        self.state.lock().ok()?.detached_at
    }

    pub fn reserved_sender_bytes(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.sender_bytes)
            .unwrap_or_default()
    }

    pub fn completed_successfully(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| matches!(state.terminal, Some(Ok(()))))
    }

    pub fn has_error(&self) -> bool {
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

pub enum ReplayPoll {
    Response(Box<connect_proto::ExecutePlanResponse>, u64),
    Error(Status),
    Complete,
    Pending,
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
pub struct GoogleRpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(message, repeated, tag = "3")]
    pub details: Vec<prost_types::Any>,
}

#[derive(prost::Message)]
pub struct GoogleRpcErrorInfo {
    #[prost(string, tag = "1")]
    pub reason: String,
    #[prost(string, tag = "2")]
    pub domain: String,
    #[prost(map = "string, string", tag = "3")]
    pub metadata: HashMap<String, String>,
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

pub fn status_with_standard_details(status: Status) -> Status {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    fn replay_record() -> (Arc<ConnectReplayRecord>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let process_bytes = Arc::new(AtomicUsize::new(0));
        let history_bytes = Arc::new(AtomicUsize::new(0));
        let record = Arc::new(
            ConnectReplayRecord::new(
                "connect-operation:test".into(),
                "connect:test-session".into(),
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
        (record, process_bytes, history_bytes)
    }

    fn arrow_response(bytes: usize) -> connect_proto::ExecutePlanResponse {
        connect_proto::ExecutePlanResponse {
            response_id: Uuid::new_v4().to_string(),
            response_type: Some(
                connect_proto::execute_plan_response::ResponseType::ArrowBatch(
                    connect_proto::execute_plan_response::ArrowBatch {
                        row_count: 1,
                        data: vec![0; bytes].into(),
                        start_offset: Some(0),
                    },
                ),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn generated_descriptor_set_contains_the_public_service() {
        let service = super::super::descriptor_pool()
            .unwrap()
            .get_service_by_name("spark.connect.SparkConnectService")
            .expect("Spark Connect service descriptor is built into this crate");
        assert!(
            service
                .methods()
                .any(|method| method.name() == "ExecutePlan")
        );
    }

    #[test]
    fn replay_expiry_cancels_producer_and_releases_retained_bytes() {
        let (record, process_bytes, history_bytes) = replay_record();
        record.append(arrow_response(4096)).unwrap();
        let cancellation = record.cancellation();
        record.detach();
        let detached_at = record.detached_at().unwrap();
        assert!(record.expire_if_detached(detached_at + CONNECT_DETACHED_RETENTION));
        assert!(cancellation.is_cancelled());
        assert_eq!(process_bytes.load(Ordering::Acquire), 0);
        assert_eq!(record.attach(None).unwrap_err().code(), Code::NotFound);
        drop(record);
        assert_eq!(history_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn replay_prefix_release_reclaims_response_payload_bytes() {
        let (record, process_bytes, history_bytes) = replay_record();
        let response = arrow_response(4096);
        let response_id = response.response_id.clone();
        record.append(response).unwrap();
        assert!(process_bytes.load(Ordering::Acquire) > 0);
        assert!(record.release_until(&response_id) > 0);
        assert_eq!(process_bytes.load(Ordering::Acquire), 0);
        drop(record);
        assert_eq!(history_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn sender_reservations_are_released_by_identity() {
        let (record, process_bytes, history_bytes) = replay_record();
        let response = arrow_response(16_384);
        let encoded_bytes = response.encoded_len();
        let old_sender = record.reserve_sender_buffer(encoded_bytes).unwrap();
        record.append(response).unwrap();
        record.detach();
        let (_cursor, _wake) = record.attach(None).unwrap();
        let ReplayPoll::Response(_, replay_sender) = record.next_response_for_sender(0).unwrap()
        else {
            panic!("retained response must be replayable");
        };
        record.release_sender_buffer(old_sender);
        assert_eq!(record.reserved_sender_bytes(), encoded_bytes);
        record.release_all();
        assert_eq!(record.reserved_sender_bytes(), encoded_bytes);
        record.release_sender_buffer(replay_sender);
        assert_eq!(record.reserved_sender_bytes(), 0);
        drop(record);
        assert_eq!(process_bytes.load(Ordering::Acquire), 0);
        assert_eq!(history_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn oversized_replay_response_fails_without_leaking_capacity() {
        let (record, process_bytes, history_bytes) = replay_record();
        let status = record
            .append(arrow_response(MAX_CONNECT_REPLAY_OPERATION_BYTES))
            .unwrap_err();
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(process_bytes.load(Ordering::Acquire), 0);
        drop(record);
        assert_eq!(history_bytes.load(Ordering::Acquire), 0);
    }
}
