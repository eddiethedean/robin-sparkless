//! Robin Sparkless core: shared types, config, error, and engine-agnostic expression IR (no Polars dependency).

pub mod config;
pub mod date_utils;
pub mod engine;
pub mod error;
pub mod expr;
pub mod schema;

pub use config::SparklessConfig;
pub use engine::{
    CollectedRows, DataFrameBackend, DataFrameReaderBackend, GroupedDataBackend, JoinType,
    PlanExecutor, SparkSessionBackend,
};
pub use error::EngineError;
pub use expr::{
    alias, between, call, col, count, eq, ge, gt, le, lit_bool, lit_f64, lit_i32, lit_i64,
    lit_null, lit_str, lt, max, mean, min, ne, not_, or_, and_, is_in, is_null, sum, ExprIr,
    LiteralValue, WhenBuilder, WhenThenBuilder, when,
};
pub use schema::{DataType, StructField, StructType};
