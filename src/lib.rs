//! Robin Sparkless - A Rust DataFrame library with PySpark-like API
//!
//! This library provides a PySpark-compatible API built on top of Polars,
//! offering high-performance data processing in pure Rust.
//!
//! # Getting started and embedding
//!
//! For application code and embedding, use the [prelude]: `use robin_sparkless::prelude::*`.
//! For a minimal FFI surface, use [prelude::embed].
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
pub mod config;
pub mod dataframe;
pub(crate) mod date_utils;
pub mod error;
pub mod expression;
pub mod functions;
pub mod plan;
pub mod prelude;
pub mod schema;
pub mod session;
pub mod traits;
pub mod type_coercion;
pub(crate) mod udf_registry;
pub(crate) mod udfs;

/// Re-export the underlying expression and literal types so downstream
/// bindings can depend on `robin_sparkless::Expr` / `LiteralValue`
/// instead of importing Polars directly.
pub type Expr = polars::prelude::Expr;
pub type LiteralValue = polars::prelude::LiteralValue;

#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "delta")]
pub mod delta;

pub use column::Column;
pub use config::SparklessConfig;
pub use dataframe::{
    CubeRollupData, DataFrame, GroupedData, JoinType, PivotedGroupedData, SaveMode, WriteFormat,
    WriteMode,
};
pub use error::EngineError;
pub use functions::{SortOrder, *};
pub use schema::{schema_from_json, DataType, StructField, StructType};
pub use session::{DataFrameReader, SparkSession, SparkSessionBuilder};
pub use traits::{FromRobinDf, IntoRobinDf};
