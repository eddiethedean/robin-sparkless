//! Robin Sparkless Polars: DataFrame, Session, Column, and expression layer (single crate that depends on Polars).

#![allow(clippy::collapsible_if)]
#![allow(clippy::let_and_return)]

pub mod column;
pub mod dataframe;
pub mod error;
pub mod engine_backend;
pub mod expr_ir;
pub mod expression;
pub mod functions;
pub mod plan;
pub mod schema;
pub(crate) mod schema_conv;
pub mod session;
pub mod traits;

pub mod type_coercion;
pub mod udf_context;
pub mod udf_registry;
pub mod udfs;

#[cfg(feature = "delta")]
pub mod delta;
#[cfg(feature = "sql")]
pub mod sql;

pub type Expr = polars::prelude::Expr;
pub type LiteralValue = polars::prelude::LiteralValue;
/// Re-export for root crate and API that returns Polars errors.
pub use polars::error::PolarsError;
/// Re-export for root DataFrame::from_polars.
pub use polars::prelude::DataFrame as PlDataFrame;
/// Re-export for root DataFrame::get_column_dtype return type.
pub use polars::prelude::DataType as PlDataType;
/// Re-export for root DataFrame::from_lazy.
pub use polars::prelude::LazyFrame;
/// Re-export for root SparkSession::register_udf.
pub use polars::prelude::Series;

pub use column::Column;
pub use dataframe::{
    CubeRollupData, DataFrame, GroupedData, JoinType, PivotedGroupedData, SaveMode, SelectItem,
    WriteFormat, WriteMode, broadcast,
};
pub use error::EngineError;
pub use expression::{column_to_expr, lit_bool, lit_f64, lit_i32, lit_i64, lit_str};
pub use functions::*;
pub use schema::{DataType, StructField, StructType, StructTypePolarsExt, schema_from_json};
pub use session::{DataFrameReader, SparkSession, SparkSessionBuilder};
pub use traits::{FromRobinDf, IntoRobinDf};
pub use type_coercion::{
    CompareOp, coerce_for_pyspark_comparison, coerce_for_pyspark_eq_null_safe, find_common_type,
};
pub use udf_context::{get_thread_udf_context, set_thread_udf_context};
pub use udf_registry::{RustUdf, UdfRegistry};
