//! Robin Sparkless - A Rust DataFrame library with PySpark-like API
//!
//! This library provides a PySpark-compatible API built on top of Polars,
//! offering high-performance data processing in pure Rust.

pub mod column;
pub mod dataframe;
pub mod expression;
pub mod functions;
pub mod schema;
pub mod session;
pub mod type_coercion;
pub(crate) mod udfs;

#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "delta")]
pub mod delta;

#[cfg(feature = "pyo3")]
pub mod python;

pub use column::Column;
pub use dataframe::{DataFrame, GroupedData, JoinType};
pub use functions::*;
pub use schema::{StructField, StructType};
pub use session::{DataFrameReader, SparkSession, SparkSessionBuilder};
