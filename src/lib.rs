//! Robin Sparkless - A Rust DataFrame library with PySpark-like API
//! 
//! This library provides a PySpark-compatible API built on top of Polars,
//! offering high-performance data processing in pure Rust.

pub mod dataframe;
pub mod session;
pub mod column;
pub mod schema;
pub mod functions;
pub mod expression;
pub mod type_coercion;

pub use session::{SparkSession, SparkSessionBuilder, DataFrameReader};
pub use dataframe::{DataFrame, GroupedData};
pub use column::Column;
pub use schema::{StructType, StructField};
pub use functions::*;
