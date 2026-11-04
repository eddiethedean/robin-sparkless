use crate::{DUCKDB_SOURCE_MAX_CONNECTIONS, DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS};
use duckdb::Connection;
use robin_sparkless_core::EngineError;
use robin_sparkless_core::engine::CancellationToken;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

pub(crate) struct DuckDbSnapshotState {
    pub(crate) connection: Option<Connection>,
    pub(crate) owner_identity: Option<String>,
    pub(crate) permit: Option<SourceConnectionPermit>,
}

#[derive(Default)]
pub(crate) struct SourceConnectionPool {
    pub(crate) state: Mutex<SourceConnectionPoolState>,
    pub(crate) available: Condvar,
}

#[derive(Default)]
pub(crate) struct SourceConnectionPoolState {
    pub(crate) active: usize,
    pub(crate) waiting: usize,
}

impl SourceConnectionPool {
    pub(crate) fn acquire(
        self: &Arc<Self>,
        token: &CancellationToken,
        slots: usize,
    ) -> Result<SourceConnectionPermit, EngineError> {
        token.check_cancelled()?;
        if slots == 0 || slots > DUCKDB_SOURCE_MAX_CONNECTIONS {
            return Err(EngineError::internal(
                "invalid DuckDB source connection reservation".into(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| EngineError::internal("DuckDB source pool lock poisoned".into()))?;
        if state.active + slots > DUCKDB_SOURCE_MAX_CONNECTIONS {
            if state.waiting >= DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS {
                return Err(EngineError::invalid_input(format!(
                    "DuckDB source connection queue is full ({DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS} waiting operations)"
                )));
            }
            state.waiting += 1;
            while state.active + slots > DUCKDB_SOURCE_MAX_CONNECTIONS {
                if token.is_cancelled() {
                    state.waiting -= 1;
                    self.available.notify_all();
                    return Err(EngineError::cancelled("operation cancelled".into()));
                }
                let (next_state, _) = self
                    .available
                    .wait_timeout(state, Duration::from_millis(10))
                    .map_err(|_| {
                        EngineError::internal("DuckDB source pool lock poisoned".into())
                    })?;
                state = next_state;
            }
            state.waiting -= 1;
        }
        state.active += slots;
        Ok(SourceConnectionPermit {
            pool: Arc::clone(self),
            slots,
        })
    }
}

pub(crate) struct SourceConnectionPermit {
    pool: Arc<SourceConnectionPool>,
    slots: usize,
}

impl Drop for SourceConnectionPermit {
    fn drop(&mut self) {
        let mut state = self
            .pool
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active = state.active.saturating_sub(self.slots);
        // Waiters can reserve different slot counts (Quack scans reserve a second control slot),
        // so wake all of them to let any request that now fits claim the released capacity.
        self.pool.available.notify_all();
    }
}
