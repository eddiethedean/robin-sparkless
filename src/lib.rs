//! Robin Sparkless - A Rust DataFrame library with PySpark-like API
//!
//! This library provides a PySpark-compatible API built on top of Polars,
//! offering high-performance data processing in pure Rust.
//!
//! # Panics and errors
//!
//! Some functions panic when used with invalid or empty inputs (e.g. calling
//! `when(cond).otherwise(val)` without `.then()`, or passing no columns to
//! `format_string`, `elt`, `concat`, `coalesce`, or `named_struct` in Rust).
//! In Rust, `create_map` and `array` return `Result` for empty input instead of
//! panicking. From Python, empty columns for `coalesce`, `format_string`,
//! `printf`, and `named_struct` raise `ValueError`. See the documentation for
//! each function for details.
//!
//! # API stability
//!
//! While the crate is in the 0.x series, we follow [semver](https://semver.org/) but may introduce
//! breaking changes in minor releases (e.g. 0.1 → 0.2) until 1.0. For behavioral caveats and
//! intentional differences from PySpark, see the [repository documentation](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md).

pub mod column;
pub mod dataframe;
pub(crate) mod date_utils;
pub mod expression;
pub mod functions;
pub mod plan;
pub mod schema;
pub mod session;
pub mod type_coercion;
pub(crate) mod udf_registry;
pub(crate) mod udfs;

#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "delta")]
pub mod delta;

#[cfg(feature = "pyo3")]
pub mod python;

pub use column::Column;
pub use dataframe::{
    CubeRollupData, DataFrame, GroupedData, JoinType, PivotedGroupedData, SaveMode, WriteFormat,
    WriteMode,
};
pub use functions::{SortOrder, *};
pub use schema::{StructField, StructType};
pub use session::{DataFrameReader, SparkSession, SparkSessionBuilder};
