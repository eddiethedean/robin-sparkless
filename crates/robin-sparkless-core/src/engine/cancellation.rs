//! Engine-neutral cooperative cancellation primitives.

use crate::error::EngineError;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// A thread-safe cancellation signal that can be shared with an active action.
///
/// Backends check this token at documented safe points. Cancellation is cooperative:
/// an operation that is inside an engine call without cancellation support observes the
/// request when that call returns. A cancelled result is only reported after the backend
/// reaches a safe point, so callers can treat it as a terminal outcome.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Create a token in the active state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation. Repeated calls are harmless.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Return whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Return the specific cancellation outcome when this token has been cancelled.
    pub fn check_cancelled(&self) -> Result<(), EngineError> {
        if self.is_cancelled() {
            Err(EngineError::Cancelled("operation cancelled".into()))
        } else {
            Ok(())
        }
    }
}
