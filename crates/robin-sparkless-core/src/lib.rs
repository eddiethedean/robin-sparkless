//! Robin Sparkless core: shared types, config, and error (no Polars dependency).

pub mod config;
pub mod date_utils;
pub mod error;
pub mod schema;

pub use config::SparklessConfig;
pub use error::EngineError;
pub use schema::{DataType, StructField, StructType};
