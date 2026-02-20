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
    ExprIr, LiteralValue, WhenBuilder, WhenThenBuilder, alias, and_, between, call, col, count, eq,
    ge, gt, is_in, is_null, le, lit_bool, lit_f64, lit_i32, lit_i64, lit_null, lit_str, lt, max,
    mean, min, ne, not_, or_, sum, when,
};
pub use schema::{DataType, StructField, StructType};
