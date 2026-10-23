use robin_sparkless::{CancellationToken, SparkSession};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

use crate::config::Limits;

pub const WAREHOUSE_SESSION_ID: &str = "sql-warehouse";

pub struct SessionManager {
    seed: SparkSession,
    sessions: Mutex<HashMap<String, Arc<ManagedSession>>>,
    generation: AtomicU64,
    max_sessions: usize,
    per_session_capacity: usize,
    idle_ttl_secs: u64,
}

pub struct ManagedSession {
    pub id: String,
    pub generation: u64,
    pub engine: SparkSession,
    pub serial: Arc<AsyncMutex<()>>,
    pub request_slots: Arc<Semaphore>,
    closing: AtomicBool,
    last_used: Mutex<Instant>,
    operations: Mutex<HashMap<String, CancellationToken>>,
}

#[derive(Debug)]
pub enum SessionError {
    Capacity,
    Closing,
    Poisoned,
}

impl SessionManager {
    pub fn new(limits: &Limits) -> Self {
        let seed = SparkSession::builder()
            .app_name("sparkless-server")
            .config("sparkless.pyspark.compat", "4.0")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.caseSensitive", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .get_or_create();
        Self {
            seed,
            sessions: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(1),
            max_sessions: limits.max_sessions,
            per_session_capacity: 1 + limits.max_pending_per_session,
            idle_ttl_secs: limits.session_idle_ttl_secs,
        }
    }

    pub fn get_or_create(&self, id: &str) -> Result<Arc<ManagedSession>, SessionError> {
        let mut sessions = self.sessions.lock().map_err(|_| SessionError::Poisoned)?;
        if let Some(session) = sessions.get(id) {
            if session.closing.load(Ordering::Acquire) {
                if Arc::strong_count(session) == 1 {
                    sessions.remove(id);
                } else {
                    return Err(SessionError::Closing);
                }
            } else {
                session.touch()?;
                return Ok(Arc::clone(session));
            }
        }
        let capacity_limit =
            if id == WAREHOUSE_SESSION_ID || sessions.contains_key(WAREHOUSE_SESSION_ID) {
                self.max_sessions
            } else {
                self.max_sessions.saturating_sub(1)
            };
        if sessions.len() >= capacity_limit {
            return Err(SessionError::Capacity);
        }
        let generation = self.generation.fetch_add(1, Ordering::Relaxed);
        let session = Arc::new(ManagedSession {
            id: id.to_owned(),
            generation,
            engine: self.seed.new_session(),
            serial: Arc::new(AsyncMutex::new(())),
            request_slots: Arc::new(Semaphore::new(self.per_session_capacity)),
            closing: AtomicBool::new(false),
            last_used: Mutex::new(Instant::now()),
            operations: Mutex::new(HashMap::new()),
        });
        sessions.insert(id.to_owned(), Arc::clone(&session));
        Ok(session)
    }

    pub fn close(&self, id: &str) -> Result<(), SessionError> {
        let mut sessions = self.sessions.lock().map_err(|_| SessionError::Poisoned)?;
        let Some(session) = sessions.get(id).cloned() else {
            return Ok(());
        };
        session.closing.store(true, Ordering::Release);
        session.cancel_operations()?;
        // `session` is a local reference in addition to the registry reference.
        if Arc::strong_count(&session) == 2 {
            sessions.remove(id);
        }
        Ok(())
    }

    pub fn close_all(&self) {
        if let Ok(sessions) = self.sessions.lock() {
            for session in sessions.values() {
                session.closing.store(true, Ordering::Release);
                let _ = session.cancel_operations();
            }
        }
    }

    pub fn verify_engine(&self) -> Result<(), String> {
        let session = self.seed.new_session();
        let result = session
            .sql("SELECT 1 AS result")
            .map_err(|error| error.to_string())?;
        let names = result.columns().map_err(|error| error.to_string())?;
        if names.len() != 1 || names[0] != "result" {
            return Err("startup query returned an unexpected schema".into());
        }
        let collected = result.collect().map_err(|error| error.to_string())?;
        let column = collected
            .columns()
            .first()
            .ok_or_else(|| "startup query returned no result column".to_owned())?;
        let value = column.get(0).map_err(|error| error.to_string())?;
        if value.to_string() != "1" {
            return Err("startup query returned an unexpected value".into());
        }
        Ok(())
    }

    pub fn reap_idle(&self) {
        let Ok(mut sessions) = self.sessions.lock() else {
            return;
        };
        let idle_ttl = std::time::Duration::from_secs(self.idle_ttl_secs);
        sessions.retain(|id, session| {
            if id == WAREHOUSE_SESSION_ID || Arc::strong_count(session) != 1 {
                return true;
            }
            if session.is_closing() {
                return false;
            }
            session
                .last_used
                .lock()
                .map(|last_used| last_used.elapsed() < idle_ttl)
                .unwrap_or(true)
        });
    }
}

impl ManagedSession {
    pub fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    pub fn touch(&self) -> Result<(), SessionError> {
        *self.last_used.lock().map_err(|_| SessionError::Poisoned)? = Instant::now();
        Ok(())
    }

    pub fn register_operation(
        self: &Arc<Self>,
        operation_id: String,
        token: CancellationToken,
    ) -> Result<OperationRegistration, SessionError> {
        if self.is_closing() {
            return Err(SessionError::Closing);
        }
        self.operations
            .lock()
            .map_err(|_| SessionError::Poisoned)?
            .insert(operation_id.clone(), token);
        if self.is_closing()
            && let Ok(operations) = self.operations.lock()
            && let Some(token) = operations.get(&operation_id)
        {
            token.cancel();
        }
        Ok(OperationRegistration {
            session: Arc::clone(self),
            operation_id,
        })
    }

    fn cancel_operations(&self) -> Result<(), SessionError> {
        let operations = self.operations.lock().map_err(|_| SessionError::Poisoned)?;
        for token in operations.values() {
            token.cancel();
        }
        Ok(())
    }
}

pub struct OperationRegistration {
    session: Arc<ManagedSession>,
    operation_id: String,
}

impl OperationRegistration {}

impl Drop for OperationRegistration {
    fn drop(&mut self) {
        if let Ok(mut operations) = self.session.operations.lock() {
            operations.remove(&self.operation_id);
        }
        let _ = self.session.touch();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Limits;
    use std::time::Duration;

    #[test]
    fn close_cancels_active_work_and_keeps_shared_session_alive() {
        let manager = SessionManager::new(&Limits::default());
        let session = manager.get_or_create("close-race").unwrap();
        let generation = session.generation;
        let token = CancellationToken::new();
        let registration = session
            .register_operation("active-operation".into(), token.clone())
            .unwrap();

        manager.close("close-race").unwrap();
        assert!(token.is_cancelled());
        assert!(session.is_closing());
        assert!(matches!(
            manager.get_or_create("close-race"),
            Err(SessionError::Closing)
        ));

        drop(registration);
        drop(session);
        let reopened = manager.get_or_create("close-race").unwrap();
        assert!(!reopened.is_closing());
        assert!(reopened.generation > generation);
    }

    #[test]
    fn idle_reaper_removes_only_unreferenced_expired_sessions() {
        let limits = Limits {
            session_idle_ttl_secs: 1,
            ..Limits::default()
        };
        let manager = SessionManager::new(&limits);
        let session = manager.get_or_create("idle-session").unwrap();
        let generation = session.generation;
        *session.last_used.lock().unwrap() = Instant::now() - Duration::from_secs(2);

        manager.reap_idle();
        assert!(!session.is_closing());
        assert!(Arc::ptr_eq(
            manager
                .sessions
                .lock()
                .unwrap()
                .get("idle-session")
                .unwrap(),
            &session
        ));

        drop(session);
        manager.reap_idle();
        assert!(
            !manager
                .sessions
                .lock()
                .unwrap()
                .contains_key("idle-session")
        );
        let reopened = manager.get_or_create("idle-session").unwrap();
        assert!(reopened.generation > generation);
    }
}
