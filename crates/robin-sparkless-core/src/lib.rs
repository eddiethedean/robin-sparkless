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
    ExprIr, LiteralValue, WhenBuilder, WhenThenBuilder, alias, and_, approx_count_distinct,
    between, bool_and, call, col, collect_list, collect_set, count, count_distinct, count_if, eq,
    every, first, ge, gt, is_in, is_null, kurtosis, le, lit_bool, lit_f64, lit_i32, lit_i64,
    lit_null, lit_str, lt, max, mean, median, min, mode, ne, not_, or_, skewness, std, stddev,
    stddev_pop, stddev_samp, sum, try_avg, try_sum, var_pop, var_samp, variance, when,
};
pub use schema::{DataType, StructField, StructType};
