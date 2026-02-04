//! Robin Sparkless - A Rust DataFrame library with PySpark-like API
//!
//! This library provides a PySpark-compatible API built on top of Polars,
//! offering high-performance data processing in pure Rust.
//!
//! # API stability
//!
//! While the crate is in the 0.x series, we follow [semver](https://semver.org/) but may introduce
//! breaking changes in minor releases (e.g. 0.1 → 0.2) until 1.0. For behavioral caveats and
//! intentional differences from PySpark, see the [repository documentation](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md).

pub mod column;
pub mod dataframe;
pub mod expression;
pub mod functions;
pub mod plan;
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
pub use dataframe::{CubeRollupData, DataFrame, GroupedData, JoinType, WriteFormat, WriteMode};
pub use functions::{SortOrder, *};
pub use schema::{StructField, StructType};
pub use session::{DataFrameReader, SparkSession, SparkSessionBuilder};
