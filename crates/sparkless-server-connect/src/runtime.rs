//! Process-owned Spark Connect transport lifecycle state.

use crate::ConnectReplayRecord;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{Semaphore, watch};

pub const MAX_CONNECT_SESSION_TOMBSTONES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectTombstoneError {
    CapacityExceeded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectRuntimeError {
    Unavailable,
}

impl fmt::Display for ConnectRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("Connect runtime state is unavailable"),
        }
    }
}

impl std::error::Error for ConnectRuntimeError {}

/// State shared by the Connect listener and product session adapter.
///
/// This owns protocol identity tombstones, replay records, transport admission counters, and the
/// bound listener address. Concrete session values and their engine lifetimes remain in the
/// product adapter.
pub struct ConnectRuntimeState {
    tombstones: Mutex<HashMap<String, String>>,
    lifecycle: Mutex<()>,
    operations: Mutex<HashMap<String, Arc<ConnectReplayRecord>>>,
    active_operations: watch::Sender<usize>,
    draining: AtomicBool,
    replay_bytes: Arc<AtomicUsize>,
    history_bytes: Arc<AtomicUsize>,
    stream_senders: Arc<Semaphore>,
    address: Mutex<Option<SocketAddr>>,
}

impl ConnectRuntimeState {
    pub fn new(max_stream_senders: usize) -> Self {
        Self {
            tombstones: Mutex::new(HashMap::new()),
            lifecycle: Mutex::new(()),
            operations: Mutex::new(HashMap::new()),
            active_operations: watch::channel(0).0,
            draining: AtomicBool::new(false),
            replay_bytes: Arc::new(AtomicUsize::new(0)),
            history_bytes: Arc::new(AtomicUsize::new(0)),
            stream_senders: Arc::new(Semaphore::new(max_stream_senders)),
            address: Mutex::new(None),
        }
    }

    pub fn tombstone(&self, key: &str) -> Result<Option<String>, ConnectRuntimeError> {
        self.tombstones
            .lock()
            .map(|tombstones| tombstones.get(key).cloned())
            .map_err(|_| ConnectRuntimeError::Unavailable)
    }

    pub fn record_tombstone(
        &self,
        key: String,
        server_session_id: String,
    ) -> Result<(), ConnectTombstoneError> {
        let mut tombstones = self
            .tombstones
            .lock()
            .map_err(|_| ConnectTombstoneError::Unavailable)?;
        if !tombstones.contains_key(&key) && tombstones.len() >= MAX_CONNECT_SESSION_TOMBSTONES {
            return Err(ConnectTombstoneError::CapacityExceeded);
        }
        tombstones.insert(key, server_session_id);
        Ok(())
    }

    pub fn lifecycle_lock(&self) -> Result<MutexGuard<'_, ()>, ConnectRuntimeError> {
        self.lifecycle
            .lock()
            .map_err(|_| ConnectRuntimeError::Unavailable)
    }

    pub fn operations(
        &self,
    ) -> Result<MutexGuard<'_, HashMap<String, Arc<ConnectReplayRecord>>>, ConnectRuntimeError>
    {
        self.operations
            .lock()
            .map_err(|_| ConnectRuntimeError::Unavailable)
    }

    pub fn active_operations(&self) -> &watch::Sender<usize> {
        &self.active_operations
    }

    pub fn set_draining(&self, draining: bool) {
        self.draining
            .store(draining, std::sync::atomic::Ordering::Release);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn replay_bytes(&self) -> &Arc<AtomicUsize> {
        &self.replay_bytes
    }

    pub fn history_bytes(&self) -> &Arc<AtomicUsize> {
        &self.history_bytes
    }

    pub fn stream_senders(&self) -> &Arc<Semaphore> {
        &self.stream_senders
    }

    pub fn address(&self) -> Result<MutexGuard<'_, Option<SocketAddr>>, ConnectRuntimeError> {
        self.address
            .lock()
            .map_err(|_| ConnectRuntimeError::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn tombstone_history_enforces_its_cap_and_allows_idempotent_updates() {
        let state = ConnectRuntimeState::new(1);
        for index in 0..MAX_CONNECT_SESSION_TOMBSTONES {
            state
                .record_tombstone(format!("session-{index}"), format!("server-{index}"))
                .unwrap();
        }
        assert_eq!(
            state.record_tombstone("one-more".into(), "server-more".into()),
            Err(ConnectTombstoneError::CapacityExceeded)
        );

        state
            .record_tombstone("session-0".into(), "replacement".into())
            .unwrap();
        assert_eq!(
            state.tombstone("session-0").unwrap().as_deref(),
            Some("replacement")
        );
        assert_eq!(state.operations().unwrap().len(), 0);
    }

    #[test]
    fn transport_state_tracks_listener_drain_and_sender_capacity() {
        let state = ConnectRuntimeState::new(1);
        state.set_draining(true);
        assert!(state.is_draining());
        *state.address().unwrap() = Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 15002));
        assert_eq!(state.address().unwrap().unwrap().port(), 15002);

        let permit = Arc::clone(state.stream_senders())
            .try_acquire_owned()
            .expect("first stream sender is admitted");
        assert!(
            Arc::clone(state.stream_senders())
                .try_acquire_owned()
                .is_err()
        );
        drop(permit);
        assert!(
            Arc::clone(state.stream_senders())
                .try_acquire_owned()
                .is_ok()
        );
    }
}
