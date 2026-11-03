//! Typed reader capability.

use crate::engine::{FrameActions, FrameSchema, FrameTransform};
use crate::error::EngineError;
use std::path::Path;

/// Reads files or catalog tables into a frame owned by the same backend.
pub trait DataFrameReaderBackend: Send + Sync {
    type Frame: FrameTransform + FrameActions + FrameSchema;

    fn csv(&self, path: &Path) -> Result<Self::Frame, EngineError>;
    fn parquet(&self, path: &Path) -> Result<Self::Frame, EngineError>;
    fn json(&self, path: &Path) -> Result<Self::Frame, EngineError>;
    fn table(&self, name: &str) -> Result<Self::Frame, EngineError>;
}
