//! Robin Sparkless core: shared types, config, error, and engine-agnostic expression IR (no Polars dependency).

pub mod config;
pub mod date_utils;
pub mod engine;
pub mod error;
pub mod expr;
pub mod jdbc_mapping;
pub mod path_security;
pub mod runtime_config;
pub mod schema;

pub use config::SparklessConfig;
pub use engine::{
    ArrowIpcBatchStream, ArrowIpcStream, CancellationToken, CatalogBatchWriter,
    CatalogCapabilities, CatalogOwnership, CatalogReadSnapshot, CatalogReader, CatalogResolution,
    CatalogResolver, CatalogWriter, CollectedRows, DataFrameReaderBackend, ExecutionConfigSnapshot,
    ExecutionContext, ExternalCatalog, ExternalTableDescriptor, ExternalTableFormat,
    ExternalTableSaveMode, ExternalTableWriteBatchRequest, ExternalTableWriteOutcome,
    ExternalTableWriteRequest, ExternalTableWriteResult, FrameActions, FrameSchema, FrameTransform,
    GroupedData, JoinType, SparkSessionBackend, begin_catalog_read_snapshot,
    resolve_catalog_identifier,
};
pub use error::{
    EngineError, ErrorCategory, ErrorDetail, ErrorRetryability, normalize_unresolved_column_message,
};
pub use expr::{
    ExprIr, LiteralValue, WhenBuilder, WhenThenBuilder, alias, and_, approx_count_distinct,
    between, bool_and, call, col, collect_list, collect_set, count, count_distinct, count_if, eq,
    every, first, ge, gt, is_in, is_null, kurtosis, le, lit_bool, lit_f64, lit_i32, lit_i64,
    lit_null, lit_str, lt, max, mean, median, min, mode, ne, not_, or_, skewness, std, stddev,
    stddev_pop, stddev_samp, sum, try_avg, try_sum, var_pop, var_samp, variance, when,
};
pub use jdbc_mapping::{
    db2_v4_type_mapping, jdbc_v4_type_mappings, mssql_v4_type_mapping, mysql_v4_type_mapping,
    oracle_v4_timestamp_mapping, postgres_v4_datetime_mapping,
};
pub use path_security::{
    confine_user_path_under_base, maybe_confine_files_path, resolve_path_under_base,
    validate_table_name,
};
pub use runtime_config::{
    PysparkCompat, SessionRuntimeConfig, decimal_allow_precision_loss_from_session_map,
};
pub use schema::{DataType, LogicalField, StructField, StructType};
