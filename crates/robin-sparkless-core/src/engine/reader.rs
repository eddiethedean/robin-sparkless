//! Engine-agnostic DataFrameReader backend trait.

use crate::engine::DataFrameBackend;
use crate::error::EngineError;
use std::path::Path;

/// Backend for reading files into a DataFrame.
pub trait DataFrameReaderBackend: Send + Sync {
    fn csv(&self, path: &Path) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn parquet(&self, path: &Path) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn json(&self, path: &Path) -> Result<Box<dyn DataFrameBackend>, EngineError>;
    fn table(&self, name: &str) -> Result<Box<dyn DataFrameBackend>, EngineError>;
}
