//! SparkSession and catalog (temp views, tables, databases).
//!
//! Catalog and table operations use mutexes. If a lock is poisoned (e.g. a thread panicked while
//! holding it), mutations may no-op and lookups may return empty/None; a message is emitted to
//! stderr in that case. These methods are best-effort when the process is in a bad state.

use crate::dataframe::DataFrame;
use crate::error::{EngineError, polars_to_core_error};
use crate::schema_conv::StructTypePolarsExt;
use crate::udf_registry::UdfRegistry;
use base64::Engine;
use polars::chunked_array::StructChunked;
use polars::chunked_array::builder::get_list_builder;
use polars::chunked_array::cast::CastOptions;
use polars::polars_utils;
use polars::prelude::{
    AnonymousScan, AnonymousScanArgs, DataFrame as PlDataFrame, DataType, Field, IntoSeries,
    LazyFrame, NamedFrom, PlSmallStr, PolarsError, ScanArgsAnonymous, Schema, SerReader, Series,
    TimeUnit, TimeZone,
};
use polars_arrow::array::*;
use polars_arrow::bitmap::Bitmap;
use polars_arrow::datatypes::{ArrowDataType, PhysicalType, PrimitiveType};
use polars_arrow::io::ipc::format::ipc::planus::ReadAsRoot;
use polars_arrow::io::ipc::format::ipc::{MessageHeaderRef, MessageRef};
use polars_arrow::io::ipc::read::read_stream_metadata;
use polars_arrow::match_integer_type;
use polars_arrow::with_match_primitive_type_full;
use robin_sparkless_core::engine::{
    CatalogOwnership, CatalogReadSnapshot, ExecutionConfigSnapshot, ExecutionContext,
    ExternalCatalog, ExternalTableDescriptor, ExternalTableFormat, begin_catalog_read_snapshot,
};
use robin_sparkless_core::schema::{
    DataType as CoreDataType, LogicalField as StructField, StructType,
};
use robin_sparkless_core::{
    CancellationToken, PysparkCompat, SessionRuntimeConfig, SparklessConfig, date_utils,
    decimal_allow_precision_loss_from_session_map,
};
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread_local;

mod catalog;
use catalog::{
    DatabaseCatalog, SharedCatalogMutationWriterGuards, TableCatalog, deduplicated_catalog_bytes,
    global_temp_catalog, next_temp_view_scope_id,
};
pub use catalog::{SharedCatalog, SharedCatalogReadLease, TempViewCatalog};
mod input;
use input::*;
mod builder;
pub use builder::SparkSessionBuilder;
mod reader;
pub use reader::DataFrameReader;

type ResolvedExternalTable = (
    Arc<ExternalCatalog>,
    CatalogOwnership,
    ExternalTableDescriptor,
);

const MAX_CONNECT_ARROW_SCHEMA_DEPTH: usize = 64;
const MAX_CONNECT_ARROW_SCHEMA_FIELDS: usize = 256;

/// When `SPARKLESS_FILES_BASE` is set, confine read/write paths under that directory.
pub(crate) fn confine_io_path(path: &Path) -> Result<PathBuf, PolarsError> {
    robin_sparkless_core::maybe_confine_files_path(path)
        .map_err(|e| PolarsError::InvalidOperation(e.to_string().into()))
}

fn max_range_rows() -> u64 {
    std::env::var("SPARKLESS_MAX_RANGE_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000_000)
}

fn range_row_count(start: i64, end: i64, step: i64) -> Result<u64, PolarsError> {
    if step > 0 {
        if end <= start {
            return Ok(0);
        }
        let span = (end as i128 - start as i128) as u128;
        let s = step as u128;
        u64::try_from(span.div_ceil(s))
            .map_err(|_| PolarsError::ComputeError("range cardinality exceeds UInt64".into()))
    } else {
        if end >= start {
            return Ok(0);
        }
        let span = (start as i128 - end as i128) as u128;
        let s = (-(step as i128)) as u128;
        u64::try_from(span.div_ceil(s))
            .map_err(|_| PolarsError::ComputeError("range cardinality exceeds UInt64".into()))
    }
}

#[derive(Debug)]
struct RangeScan {
    start: i64,
    step: i64,
    row_count: u64,
}

impl AnonymousScan for RangeScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, scan_opts: AnonymousScanArgs) -> Result<PlDataFrame, PolarsError> {
        let rows = scan_opts
            .n_rows
            .map(|requested| self.row_count.min(requested as u64))
            .unwrap_or(self.row_count);
        let capacity = usize::try_from(rows).map_err(|_| {
            PolarsError::ComputeError("range scan exceeds this platform's addressable rows".into())
        })?;
        let mut values = Vec::with_capacity(capacity);
        for index in 0..rows {
            let value = self.start as i128 + index as i128 * self.step as i128;
            values.push(i64::try_from(value).map_err(|_| {
                PolarsError::ComputeError("range value exceeds Int64 bounds".into())
            })?);
        }
        let id = Series::new("id".into(), values);
        PlDataFrame::new_infer_height(vec![id.into()])
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> Result<Arc<Schema>, PolarsError> {
        Ok(Arc::new(Schema::from_iter([Field::new(
            "id".into(),
            DataType::Int64,
        )])))
    }
}

/// Read a disk-backed warehouse table directory if it exists under the warehouse.
fn read_warehouse_table_at(
    spark: &SparkSession,
    warehouse: &str,
    table_name: &str,
) -> Result<Option<DataFrame>, PolarsError> {
    let dir =
        robin_sparkless_core::resolve_path_under_base(warehouse, table_name).map_err(|e| {
            PolarsError::InvalidOperation(format!("warehouse table lookup: {e}").into())
        })?;
    if !dir.is_dir() {
        return Ok(None);
    }
    let data_file = dir.join("data.parquet");
    let read_path = if data_file.is_file() { data_file } else { dir };
    Ok(Some(spark.read_parquet_unchecked(&read_path)?))
}

thread_local! {
    /// Thread-local SparkSession for UDF resolution in call_udf. Set by get_or_create.
    static THREAD_UDF_SESSION: RefCell<Option<SparkSession>> = const { RefCell::new(None) };
    static ACTIVE_EXECUTION_CONTEXT: RefCell<Option<ExecutionContext>> = const { RefCell::new(None) };
}

struct ExecutionContextGuard(Option<ExecutionContext>);

impl ExecutionContextGuard {
    fn install(context: ExecutionContext) -> Self {
        let previous = ACTIVE_EXECUTION_CONTEXT.with(|cell| cell.replace(Some(context)));
        Self(previous)
    }
}

impl Drop for ExecutionContextGuard {
    fn drop(&mut self) {
        ACTIVE_EXECUTION_CONTEXT.with(|cell| *cell.borrow_mut() = self.0.take());
    }
}

pub(crate) fn active_operation_id() -> Option<String> {
    ACTIVE_EXECUTION_CONTEXT.with(|cell| {
        cell.borrow()
            .as_ref()
            .map(|context| context.operation_id().to_string())
    })
}

pub(crate) fn has_active_execution_context() -> bool {
    ACTIVE_EXECUTION_CONTEXT.with(|cell| cell.borrow().is_some())
}

/// Set the thread-local session for UDF resolution (call_udf). Used by get_or_create.
pub(crate) fn set_thread_udf_session(session: SparkSession) {
    session.sync_thread_context();
    THREAD_UDF_SESSION.with(|cell| *cell.borrow_mut() = Some(session));
}

/// Get the thread-local session for UDF resolution. (call_udf uses expr's thread context; this is kept for compatibility.)
#[allow(dead_code)]
pub(crate) fn get_thread_udf_session() -> Option<SparkSession> {
    THREAD_UDF_SESSION.with(|cell| cell.borrow().clone())
}

/// Clear the thread-local session used for UDF resolution.
pub(crate) fn clear_thread_udf_session() {
    THREAD_UDF_SESSION.with(|cell| *cell.borrow_mut() = None);
}

/// Catalog of global temporary views (process-scoped). Persists across sessions within the same process.
/// PySpark: createOrReplaceGlobalTempView / spark.table("global_temp.name").
static GLOBAL_TEMP_CATALOG: OnceLock<Arc<Mutex<HashMap<String, DataFrame>>>> = OnceLock::new();
static NEXT_TEMP_VIEW_SCOPE_ID: AtomicU64 = AtomicU64::new(1);

/// Main entry point for creating DataFrames and executing queries
/// Similar to PySpark's SparkSession but using Polars as the backend
#[derive(Clone)]
pub struct SparkSession {
    app_name: Option<String>,
    master: Option<String>,
    config: HashMap<String, String>,
    /// Derived execution semantics (ANSI, map inference, compat profile).
    runtime_config: SessionRuntimeConfig,
    /// Temporary views: name -> DataFrame. Session-scoped; cleared when session is dropped.
    pub(crate) catalog: TempViewCatalog,
    /// Saved tables (saveAsTable): name -> DataFrame. Session-scoped; separate namespace from temp views.
    pub(crate) tables: TableCatalog,
    /// Databases/schemas created via CREATE DATABASE / CREATE SCHEMA. Session-scoped; used by listDatabases/databaseExists.
    pub(crate) databases: DatabaseCatalog,
    /// UDF registry: Rust UDFs. Session-scoped.
    pub(crate) udf_registry: UdfRegistry,
    /// Current database/schema name for this session (PySpark: catalog.currentDatabase()).
    pub(crate) current_database: Arc<Mutex<String>>,
    /// Best-effort cache flags (PySpark: catalog.cacheTable / isCached). No execution impact today.
    pub(crate) cached_tables: Arc<Mutex<HashSet<String>>>,
    /// External catalog resolver attached to this logical session.
    external_catalog: Arc<RwLock<Option<Arc<ExternalCatalog>>>>,
    /// Optional application catalog shared by sessions created from this session.
    shared_catalog: Option<SharedCatalog>,
    /// Stable identifier shared by clones of one logical session, but unique for new sessions.
    temp_view_scope_id: u64,
    execution_identity: Arc<()>,
}

impl SparkSession {
    pub fn new(
        app_name: Option<String>,
        master: Option<String>,
        config: HashMap<String, String>,
    ) -> Self {
        let mut config = config;
        if !config.contains_key("sparkless.pyspark.compat")
            && let Some(compat) = SessionRuntimeConfig::compat_from_env()
        {
            SessionRuntimeConfig::set_compat_profile(&mut config, compat);
        }
        let runtime_config = SessionRuntimeConfig::from_session_map(&config);
        SparkSession {
            app_name,
            master,
            config,
            runtime_config,
            catalog: Arc::new(Mutex::new(HashMap::new())),
            tables: Arc::new(Mutex::new(HashMap::new())),
            databases: Arc::new(Mutex::new(HashSet::new())),
            udf_registry: UdfRegistry::new(),
            current_database: Arc::new(Mutex::new("default".to_string())),
            cached_tables: Arc::new(Mutex::new(HashSet::new())),
            external_catalog: Arc::new(RwLock::new(None)),
            shared_catalog: None,
            temp_view_scope_id: next_temp_view_scope_id(),
            execution_identity: Arc::new(()),
        }
    }

    /// Return the application name for this session (if set).
    pub fn app_name(&self) -> Option<String> {
        self.app_name.clone()
    }

    /// Create a logical session with isolated temporary views/configuration (PySpark: `newSession`).
    /// Saved tables and databases remain isolated for ordinary embedded sessions. When this
    /// session was explicitly attached to a [`SharedCatalog`], the child shares that catalog
    /// while retaining its own temporary views and session configuration.
    pub fn new_session(&self) -> Self {
        let current_db = self.current_database();
        let (tables, databases) = match &self.shared_catalog {
            Some(catalog) => (catalog.tables.clone(), catalog.databases.clone()),
            None => (
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashSet::new())),
            ),
        };
        SparkSession {
            app_name: self.app_name.clone(),
            master: self.master.clone(),
            config: self.config.clone(),
            runtime_config: self.runtime_config.clone(),
            catalog: Arc::new(Mutex::new(HashMap::new())),
            tables,
            databases,
            udf_registry: UdfRegistry::new(),
            current_database: Arc::new(Mutex::new(current_db)),
            cached_tables: Arc::new(Mutex::new(HashSet::new())),
            // A new logical session never inherits external catalog callbacks.
            external_catalog: Arc::new(RwLock::new(None)),
            shared_catalog: self.shared_catalog.clone(),
            temp_view_scope_id: next_temp_view_scope_id(),
            execution_identity: Arc::new(()),
        }
    }

    /// Attach this session to an explicitly owned catalog scope.
    ///
    /// Use on a newly created session before registering saved tables. Sessions created later
    /// with [`Self::new_session`] share the catalog but get fresh temporary views and local
    /// settings. The catalog value, rather than process-global statics, defines service scope.
    pub fn with_shared_catalog(mut self, catalog: SharedCatalog) -> Self {
        self.tables = catalog.tables.clone();
        self.databases = catalog.databases.clone();
        self.shared_catalog = Some(catalog);
        self.temp_view_scope_id = next_temp_view_scope_id();
        self
    }

    /// Begin a statement-wide read snapshot for a shared catalog. The returned lease must live
    /// through execution and result encoding so concurrent drops/replacements remain accounted.
    pub fn begin_catalog_read_snapshot(
        &self,
    ) -> Result<Option<SharedCatalogReadLease>, EngineError> {
        let Some(catalog) = self.shared_catalog.clone() else {
            return Ok(None);
        };
        let _writer = catalog.writer.lock().map_err(|_| {
            EngineError::invalid_input("shared catalog writer lock is poisoned".into())
        })?;
        catalog.active_readers.fetch_add(1, Ordering::AcqRel);
        drop(_writer);
        Ok(Some(SharedCatalogReadLease { catalog }))
    }

    #[cfg(feature = "sql")]
    pub(crate) fn uses_shared_catalog(&self) -> bool {
        self.shared_catalog.is_some()
    }

    /// Serialize SQL catalog resolution/publication for sessions in one shared scope.
    ///
    /// The guard covers SQL translation, where table references are resolved into immutable
    /// DataFrame plans. Once returned, those plans keep their resolved table versions without
    /// holding the catalog lock through collection or result encoding.
    pub(crate) fn shared_catalog_guard(
        &self,
    ) -> Result<Option<std::sync::MutexGuard<'_, ()>>, EngineError> {
        self.shared_catalog
            .as_ref()
            .map(|catalog| {
                #[cfg(feature = "test-hooks")]
                crate::sql::test_hooks::before_catalog_writer_lock();
                catalog.writer.lock().map_err(|_| {
                    EngineError::invalid_input("shared catalog writer lock is poisoned".to_string())
                })
            })
            .transpose()
    }

    #[cfg(test)]
    pub(crate) fn shared_catalog_writer_lock_is_contended_for_test(&self) -> bool {
        self.shared_catalog.as_ref().is_some_and(|catalog| {
            matches!(
                catalog.writer.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            )
        })
    }

    /// Serialize shared-catalog mutations while allowing readers to resolve snapshots during
    /// staged data work. The catalog writer mutex remains the short publication/resolution lock.
    pub(crate) fn shared_catalog_mutation_guard(
        &self,
    ) -> Result<Option<std::sync::MutexGuard<'_, ()>>, EngineError> {
        self.shared_catalog
            .as_ref()
            .map(|catalog| {
                catalog.mutation_lane.lock().map_err(|_| {
                    EngineError::invalid_input(
                        "shared catalog mutation lane is poisoned".to_string(),
                    )
                })
            })
            .transpose()
    }

    fn shared_catalog_mutation_and_writer_guards(
        &self,
    ) -> Result<SharedCatalogMutationWriterGuards<'_>, EngineError> {
        let mutation = self.shared_catalog_mutation_guard()?;
        let writer = self.shared_catalog_guard()?;
        Ok((mutation, writer))
    }

    fn global_temp_views(&self) -> TempViewCatalog {
        self.shared_catalog
            .as_ref()
            .map(|catalog| catalog.global_temp_views.clone())
            .unwrap_or_else(global_temp_catalog)
    }

    /// Attach or remove this logical session's external catalog capabilities.
    ///
    /// Clones of this session share the resolver slot. `new_session()` creates a fresh slot.
    pub fn set_external_catalog(
        &self,
        catalog: Option<Arc<ExternalCatalog>>,
    ) -> Result<(), EngineError> {
        let mut current = self
            .external_catalog
            .write()
            .map_err(|_| EngineError::internal("external catalog lock poisoned".into()))?;
        let previous = std::mem::replace(&mut *current, catalog);
        drop(current);
        // Provider destruction may execute Python finalizers that re-enter this session.
        drop(previous);
        Ok(())
    }

    /// Begin one external-source snapshot for a Connect operation, if a reader is configured.
    pub fn begin_external_catalog_read_snapshot(
        &self,
        token: &CancellationToken,
    ) -> Result<Option<Arc<dyn CatalogReadSnapshot>>, EngineError> {
        let Some(provider) = self
            .external_catalog
            .read()
            .map_err(|_| EngineError::internal("external catalog lock poisoned".into()))?
            .clone()
        else {
            return Ok(None);
        };
        if !provider.has_reader() {
            return Ok(None);
        }
        Ok(Some(begin_catalog_read_snapshot(&provider, token)?))
    }

    pub(crate) fn external_catalog(&self) -> Result<Option<Arc<ExternalCatalog>>, PolarsError> {
        self.external_catalog
            .read()
            .map(|provider| provider.clone())
            .map_err(|_| PolarsError::InvalidOperation("external catalog lock poisoned".into()))
    }

    pub(crate) fn reject_read_only_external_target(&self, name: &str) -> Result<(), PolarsError> {
        let Some(catalog) = self.external_catalog()? else {
            return Ok(());
        };
        let ownership = catalog.resolve_ownership(name).map_err(|error| {
            PolarsError::InvalidOperation(
                format!("external catalog failed to resolve write owner for '{name}': {error}")
                    .into(),
            )
        })?;
        self.reject_read_only_external_owner(&ownership)
    }

    pub(crate) fn reject_read_only_external_owner(
        &self,
        ownership: &CatalogOwnership,
    ) -> Result<(), PolarsError> {
        let Some(catalog) = self.external_catalog()? else {
            return Ok(());
        };
        let Some(capabilities) = ownership.capabilities() else {
            return Ok(());
        };
        if !capabilities.write
            || (!catalog.has_materialized_writer_for(ownership)
                && !catalog.has_batch_writer_for(ownership))
        {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "WRITE_CAPABILITY_DENIED: external namespace '{}' has no write capability",
                    ownership.owner_id().unwrap_or_default()
                )
                .into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn reject_external_namespace_target(&self, name: &str) -> Result<(), PolarsError> {
        if self.saved_table_exists(name)
            || self.has_exact_temp_view(name)
            || Self::is_global_temp_name(name)
        {
            return Ok(());
        }
        let Some(catalog) = self.external_catalog()? else {
            return Ok(());
        };
        let ownership = catalog.resolve_ownership(name).map_err(|error| {
            PolarsError::InvalidOperation(
                format!("external catalog failed to resolve mutation owner for '{name}': {error}")
                    .into(),
            )
        })?;
        if let CatalogOwnership::External { owner_id, .. } = ownership {
            return Err(PolarsError::InvalidOperation(
                format!("external namespace '{owner_id}' does not support this SQL mutation")
                    .into(),
            ));
        }
        Ok(())
    }

    /// Return the current database for this session. Defaults to "default".
    pub fn current_database(&self) -> String {
        match self.current_database.lock() {
            Ok(guard) => guard.clone(),
            Err(_) => "default".to_string(),
        }
    }

    /// Set the current database for this session. Errors if the name does not exist.
    pub fn set_current_database(&self, name: &str) -> Result<(), EngineError> {
        if name.is_empty() {
            return Err(EngineError::invalid_input(
                "database name cannot be empty".to_string(),
            ));
        }
        if !self.database_exists(name) {
            return Err(EngineError::not_found(format!(
                "Database '{name}' does not exist"
            )));
        }
        match self.current_database.lock() {
            Ok(mut guard) => {
                *guard = if self.shared_catalog.is_some() {
                    name.to_ascii_lowercase()
                } else {
                    name.to_string()
                };
                Ok(())
            }
            Err(_) => Ok(()),
        }
    }

    /// Resolve an unqualified saved-table name in a shared service catalog.
    fn saved_table_key(&self, name: &str) -> String {
        if self.shared_catalog.is_none() {
            return name.to_string();
        }
        let name = name.to_ascii_lowercase();
        if name.contains('.') {
            name
        } else {
            format!("{}.{}", self.current_database().to_ascii_lowercase(), name)
        }
    }

    fn temp_view_accounting_key(&self, name: &str) -> String {
        if self.is_case_sensitive() {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        }
    }

    pub(crate) fn check_saved_table_database(&self, name: &str) -> Result<(), PolarsError> {
        if self.shared_catalog.is_none() {
            return Ok(());
        }
        let current = self.current_database();
        if !self.database_exists(&current) {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "Current database '{current}' does not exist; use USE to select an existing database"
                )
                .into(),
            ));
        }
        if let Some((database, _)) = name.split_once('.')
            && !self.database_exists(database)
        {
            return Err(PolarsError::InvalidOperation(
                format!("Database '{database}' does not exist").into(),
            ));
        }
        Ok(())
    }

    /// Whether a database still owns saved tables. A poisoned catalog is treated as non-empty.
    pub(crate) fn database_has_saved_tables(&self, name: &str) -> bool {
        let prefix = format!("{}.", name.to_ascii_lowercase());
        self.tables
            .lock()
            .map(|tables| {
                tables
                    .keys()
                    .any(|table| table.to_ascii_lowercase().starts_with(&prefix))
            })
            .unwrap_or(true)
    }

    /// Validate that a staged saved-table version fits configured shared-catalog limits.
    /// The caller must hold the shared catalog writer guard through validation and publication.
    #[cfg(feature = "sql")]
    pub(crate) fn check_catalog_table_growth(
        &self,
        name: &str,
        candidate: &DataFrame,
    ) -> Result<(), PolarsError> {
        if self.shared_catalog.is_none() {
            return Ok(());
        }
        let Some(max_table_bytes) =
            self.configured_catalog_limit("sparkless.server.max_table_bytes")
        else {
            return Ok(());
        };
        let max_catalog_bytes = self
            .configured_catalog_limit("sparkless.server.max_catalog_bytes")
            .unwrap_or(usize::MAX);
        let max_tables = self
            .configured_catalog_limit("sparkless.server.max_tables")
            .unwrap_or(usize::MAX);
        let candidate_bytes = candidate.estimated_size()?;
        if candidate_bytes > max_table_bytes {
            return Err(PolarsError::ComputeError(
                format!(
                    "CatalogCapacityExceeded: staged table is {candidate_bytes} bytes; the per-table limit is {max_table_bytes}"
                )
                .into(),
            ));
        }
        let target_key = self.saved_table_key(name);
        let tables = self
            .tables
            .lock()
            .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
            .iter()
            .map(|(key, frame)| (key.clone(), frame.clone()))
            .collect::<Vec<_>>();
        if !tables.iter().any(|(key, _)| key == &target_key) && tables.len() >= max_tables {
            return Err(PolarsError::ComputeError(
                format!("CatalogCapacityExceeded: saved-table limit is {max_tables}").into(),
            ));
        }
        // Count the staged version alongside committed and pinned versions. Shared Arrow chunks
        // count once, while a replacement that needs new buffers must fit before publication.
        let mut frames = tables
            .into_iter()
            .map(|(_, frame)| frame)
            .collect::<Vec<_>>();
        frames.extend(self.catalog_auxiliary_frames()?);
        frames.push(candidate.clone());
        let staged_total = deduplicated_catalog_bytes(&frames)?;
        if staged_total > max_catalog_bytes {
            return Err(PolarsError::ComputeError(
                format!(
                    "CatalogCapacityExceeded: staged catalog would exceed {max_catalog_bytes} bytes"
                )
                .into(),
            ));
        }
        Ok(())
    }

    /// Validate a materialized session-view snapshot against the shared catalog byte budget.
    /// The caller holds the catalog writer guard through validation and registration.
    #[cfg(feature = "sql")]
    pub(crate) fn check_catalog_temp_view_growth(
        &self,
        name: &str,
        candidate: &DataFrame,
    ) -> Result<(), PolarsError> {
        if self.shared_catalog.is_none() {
            return Ok(());
        }
        let max_catalog_bytes = self
            .configured_catalog_limit("sparkless.server.max_catalog_bytes")
            .unwrap_or(usize::MAX);
        let target = self.temp_view_accounting_key(name);
        let replacement_remains_pinned = self
            .shared_catalog
            .as_ref()
            .expect("checked above")
            .active_readers
            .load(Ordering::Acquire)
            > 0;
        let mut frames = self
            .tables
            .lock()
            .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let shared = self.shared_catalog.as_ref().expect("checked above");
        let views = shared
            .view_accounting
            .lock()
            .map_err(|_| PolarsError::InvalidOperation("view accounting lock poisoned".into()))?;
        for (scope_id, scoped_views) in views.iter() {
            for (view_name, frame) in scoped_views {
                if !replacement_remains_pinned
                    && *scope_id == self.temp_view_scope_id
                    && view_name == &target
                {
                    continue;
                }
                frames.push(frame.clone());
            }
        }
        drop(views);
        frames.extend(
            shared
                .pinned_versions
                .lock()
                .map_err(|_| PolarsError::InvalidOperation("pinned-version lock poisoned".into()))?
                .iter()
                .cloned(),
        );
        frames.push(candidate.clone());
        let staged_total = deduplicated_catalog_bytes(&frames)?;
        if staged_total > max_catalog_bytes {
            return Err(PolarsError::ComputeError(
                format!(
                    "CatalogCapacityExceeded: staged catalog would exceed {max_catalog_bytes} bytes"
                )
                .into(),
            ));
        }
        Ok(())
    }

    fn catalog_auxiliary_frames(&self) -> Result<Vec<DataFrame>, PolarsError> {
        let Some(catalog) = &self.shared_catalog else {
            return Ok(Vec::new());
        };
        let mut frames = catalog
            .view_accounting
            .lock()
            .map_err(|_| PolarsError::InvalidOperation("view accounting lock poisoned".into()))?
            .values()
            .flat_map(|views| views.values().cloned())
            .collect::<Vec<_>>();
        frames.extend(
            catalog
                .pinned_versions
                .lock()
                .map_err(|_| PolarsError::InvalidOperation("pinned-version lock poisoned".into()))?
                .iter()
                .cloned(),
        );
        Ok(frames)
    }

    #[cfg(test)]
    fn catalog_auxiliary_bytes(&self) -> Result<usize, PolarsError> {
        deduplicated_catalog_bytes(&self.catalog_auxiliary_frames()?)
    }

    fn retain_catalog_version_if_readers(&self, frame: DataFrame, _accounted_bytes: Option<usize>) {
        let Some(catalog) = &self.shared_catalog else {
            return;
        };
        if catalog.active_readers.load(Ordering::Acquire) == 0 {
            return;
        }
        if let Ok(mut pinned) = catalog.pinned_versions.lock() {
            pinned.push(frame);
        }
    }

    #[cfg(feature = "sql")]
    pub(crate) fn check_database_capacity(&self) -> Result<(), PolarsError> {
        let Some(max_databases) = self.configured_catalog_limit("sparkless.server.max_databases")
        else {
            return Ok(());
        };
        if self.shared_catalog.is_some()
            && self
                .databases
                .lock()
                .map_err(|_| {
                    PolarsError::InvalidOperation("database catalog lock poisoned".into())
                })?
                .len()
                .saturating_add(1)
                >= max_databases
        {
            return Err(PolarsError::ComputeError(
                format!("CatalogCapacityExceeded: database limit is {max_databases}").into(),
            ));
        }
        Ok(())
    }

    #[cfg(feature = "sql")]
    fn configured_catalog_limit(&self, key: &str) -> Option<usize> {
        self.config
            .get(key)
            .and_then(|value| value.parse::<usize>().ok())
    }

    pub fn cache_table(&self, name: &str) {
        if name.is_empty() {
            return;
        }
        if let Ok(mut guard) = self.cached_tables.lock() {
            guard.insert(name.to_string());
        }
    }

    pub fn uncache_table(&self, name: &str) {
        if name.is_empty() {
            return;
        }
        if let Ok(mut guard) = self.cached_tables.lock() {
            guard.remove(name);
        }
    }

    pub fn is_cached(&self, name: &str) -> bool {
        if name.is_empty() {
            return false;
        }
        self.cached_tables
            .lock()
            .map(|g| g.contains(name))
            .unwrap_or(false)
    }

    /// Register a DataFrame as a temporary view (PySpark: createOrReplaceTempView).
    /// The view is session-scoped and is dropped when the session is dropped.
    /// If the catalog lock is poisoned (e.g. a thread panicked while holding it), the operation is best-effort and a message is emitted to stderr.
    pub fn create_or_replace_temp_view(&self, name: &str, df: DataFrame) {
        #[cfg(feature = "sql")]
        if self.shared_catalog.is_some() {
            let (_mutation, _writer) = match self.shared_catalog_mutation_and_writer_guards() {
                Ok(guards) => guards,
                Err(error) => {
                    eprintln!("robin-sparkless-polars: {error}");
                    return;
                }
            };
            if let Err(error) = self.register_temp_view_during_sql(name, df) {
                eprintln!("robin-sparkless-polars: {error}");
            }
            return;
        }
        match self.catalog.lock() {
            Ok(mut m) => {
                if self.is_case_sensitive() {
                    m.insert(name.to_string(), df);
                } else if let Some(existing) = m
                    .keys()
                    .find(|existing| existing.eq_ignore_ascii_case(name))
                    .cloned()
                {
                    m.insert(existing, df);
                } else {
                    m.insert(name.to_string(), df);
                }
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: catalog lock poisoned, create_or_replace_temp_view may have failed"
                );
            }
        }
    }

    /// Register a session-local view while the caller holds the shared catalog writer guard.
    #[cfg(feature = "sql")]
    pub(crate) fn register_temp_view_during_sql(
        &self,
        name: &str,
        df: DataFrame,
    ) -> Result<(), PolarsError> {
        self.check_catalog_temp_view_growth(name, &df)?;
        let Some(shared) = &self.shared_catalog else {
            self.create_or_replace_temp_view(name, df);
            return Ok(());
        };
        let normalized = self.temp_view_accounting_key(name);
        let mut accounting = shared
            .view_accounting
            .lock()
            .map_err(|_| PolarsError::InvalidOperation("view accounting lock poisoned".into()))?;
        let mut views = self
            .catalog
            .lock()
            .map_err(|_| PolarsError::InvalidOperation("session view lock poisoned".into()))?;
        let key = if self.is_case_sensitive() {
            views.contains_key(name).then(|| name.to_string())
        } else {
            views
                .keys()
                .find(|existing| existing.eq_ignore_ascii_case(name))
                .cloned()
        };
        if key.is_none()
            && self
                .configured_catalog_limit("sparkless.server.max_temp_views")
                .is_some_and(|limit| views.len() >= limit)
        {
            return Err(PolarsError::ComputeError(
                "CatalogCapacityExceeded: session temporary-view limit reached".into(),
            ));
        }
        let key = key.unwrap_or_else(|| name.to_string());
        let previous = views.insert(key, df.clone());
        let session_sizes = accounting.entry(self.temp_view_scope_id).or_default();
        let previous_snapshot = session_sizes.remove(&normalized);
        session_sizes.insert(normalized, df);
        drop(views);
        drop(accounting);
        if let Some(previous) = previous {
            self.retain_catalog_version_if_readers(previous, None);
        }
        drop(previous_snapshot);
        Ok(())
    }

    /// Global temp view (PySpark: createGlobalTempView). Persists across sessions within the same process.
    /// For an explicitly shared catalog, the view is scoped to that catalog instead of the process.
    /// If the global catalog lock is poisoned, the operation is best-effort and a message is emitted to stderr.
    pub fn create_global_temp_view(&self, name: &str, df: DataFrame) {
        let (_mutation, _writer) = match self.shared_catalog_mutation_and_writer_guards() {
            Ok(guards) => guards,
            Err(error) => {
                eprintln!("robin-sparkless-polars: {error}");
                return;
            }
        };
        match self.global_temp_views().lock() {
            Ok(mut m) => {
                m.insert(name.to_string(), df);
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: global temp catalog lock poisoned, create_global_temp_view may have failed"
                );
            }
        }
    }

    /// Global temp view (PySpark: createOrReplaceGlobalTempView). Persists across sessions within the same process.
    /// If the global catalog lock is poisoned, the operation is best-effort and a message is emitted to stderr.
    pub fn create_or_replace_global_temp_view(&self, name: &str, df: DataFrame) {
        let (_mutation, _writer) = match self.shared_catalog_mutation_and_writer_guards() {
            Ok(guards) => guards,
            Err(error) => {
                eprintln!("robin-sparkless-polars: {error}");
                return;
            }
        };
        match self.global_temp_views().lock() {
            Ok(mut m) => {
                m.insert(name.to_string(), df);
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: global temp catalog lock poisoned, create_or_replace_global_temp_view may have failed"
                );
            }
        }
    }

    /// Returns true when `name` is registered as a session temp view (same resolution as [`Self::table`]).
    pub fn has_temp_view(&self, name: &str) -> bool {
        if self.shared_catalog.is_some() && name.contains('.') {
            return false;
        }
        if self.temp_view(name).is_some() {
            return true;
        }
        if name.contains('.') {
            let unqualified = name.rsplit_once('.').map(|(_, t)| t).unwrap_or(name);
            return self.temp_view(unqualified).is_some();
        }
        false
    }

    fn temp_view(&self, name: &str) -> Option<DataFrame> {
        self.catalog.lock().ok().and_then(|views| {
            if self.is_case_sensitive() {
                views.get(name).cloned()
            } else {
                views
                    .iter()
                    .find(|(view_name, _)| view_name.eq_ignore_ascii_case(name))
                    .map(|(_, frame)| frame.clone())
            }
        })
    }

    /// Returns whether a temp view is registered under exactly this identifier.
    pub(crate) fn has_exact_temp_view(&self, name: &str) -> bool {
        self.catalog.lock().ok().is_some_and(|views| {
            if self.is_case_sensitive() {
                views.contains_key(name)
            } else {
                views
                    .keys()
                    .any(|view_name| view_name.eq_ignore_ascii_case(name))
            }
        })
    }

    /// Drop a temporary view by name (PySpark: catalog.dropTempView).
    /// Returns true if a view was removed; false if not present.
    /// If the catalog lock is poisoned, returns false and a message is emitted to stderr.
    pub fn drop_temp_view(&self, name: &str) -> bool {
        if self.shared_catalog.is_some() {
            let (_mutation, _writer) = match self.shared_catalog_mutation_and_writer_guards() {
                Ok(guards) => guards,
                Err(error) => {
                    eprintln!("robin-sparkless-polars: {error}");
                    return false;
                }
            };
            return self.drop_temp_view_during_sql(name);
        }
        self.drop_temp_view_during_sql(name)
    }

    /// Drop a temporary view while the caller already owns the shared catalog mutation lane.
    pub(crate) fn drop_temp_view_during_sql(&self, name: &str) -> bool {
        let normalized = self.temp_view_accounting_key(name);
        let removed = if let Some(shared) = &self.shared_catalog {
            let mut accounting = match shared.view_accounting.lock() {
                Ok(accounting) => accounting,
                Err(_) => return false,
            };
            let mut views = match self.catalog.lock() {
                Ok(views) => views,
                Err(_) => return false,
            };
            let key = if self.is_case_sensitive() {
                name.to_string()
            } else {
                match views
                    .keys()
                    .find(|view_name| view_name.eq_ignore_ascii_case(name))
                    .cloned()
                {
                    Some(key) => key,
                    None => return false,
                }
            };
            let Some(frame) = views.remove(&key) else {
                return false;
            };
            let previous_snapshot = accounting
                .get_mut(&self.temp_view_scope_id)
                .and_then(|views| views.remove(&normalized));
            if accounting
                .get(&self.temp_view_scope_id)
                .is_some_and(HashMap::is_empty)
            {
                accounting.remove(&self.temp_view_scope_id);
            }
            drop(views);
            drop(accounting);
            self.retain_catalog_version_if_readers(frame, None);
            drop(previous_snapshot);
            true
        } else {
            match self.catalog.lock() {
                Ok(mut views) => {
                    if self.is_case_sensitive() {
                        views.remove(name)
                    } else {
                        let key = views
                            .keys()
                            .find(|view_name| view_name.eq_ignore_ascii_case(name))
                            .cloned();
                        key.and_then(|key| views.remove(&key))
                    }
                }
                Err(_) => None,
            }
            .is_some()
        };
        removed
    }

    /// Drop a global temporary view (PySpark: catalog.dropGlobalTempView). Removes from process-wide catalog.
    /// If the global catalog lock is poisoned, returns false and a message is emitted to stderr.
    pub fn drop_global_temp_view(&self, name: &str) -> bool {
        let (_mutation, _writer) = match self.shared_catalog_mutation_and_writer_guards() {
            Ok(guards) => guards,
            Err(error) => {
                eprintln!("robin-sparkless-polars: {error}");
                return false;
            }
        };
        self.drop_global_temp_view_during_sql(name)
    }

    /// Drop a global temporary view while the caller already holds the catalog writer guard.
    pub(crate) fn drop_global_temp_view_during_sql(&self, name: &str) -> bool {
        match self.global_temp_views().lock() {
            Ok(mut m) => m.remove(name).is_some(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: global temp catalog lock poisoned, drop_global_temp_view may have failed"
                );
                false
            }
        }
    }

    /// Register a DataFrame as a saved table (PySpark: saveAsTable). Inserts into the tables catalog only.
    /// If the tables lock is poisoned, the operation is best-effort and a message is emitted to stderr.
    pub fn register_table(&self, name: &str, df: DataFrame) {
        let (_mutation, _writer) = match self.shared_catalog_mutation_and_writer_guards() {
            Ok(guards) => guards,
            Err(error) => {
                eprintln!("robin-sparkless-polars: {error}");
                return;
            }
        };
        self.register_table_during_sql(name, df);
    }

    /// Register a table while the caller already holds the catalog writer guard.
    pub(crate) fn register_table_during_sql(&self, name: &str, df: DataFrame) {
        let key = self.saved_table_key(name);
        let previous = match self.tables.lock() {
            Ok(mut m) => m.insert(key, df),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, register_table may have failed"
                );
                None
            }
        };
        if let Some(previous) = previous {
            self.retain_catalog_version_if_readers(previous, None);
        }
    }

    /// Register a database/schema name (from CREATE DATABASE / CREATE SCHEMA). Persisted in session for listDatabases/databaseExists.
    /// If the databases lock is poisoned, the operation is best-effort and a message is emitted to stderr.
    pub fn register_database(&self, name: &str) {
        let (_mutation, _writer) = match self.shared_catalog_mutation_and_writer_guards() {
            Ok(guards) => guards,
            Err(error) => {
                eprintln!("robin-sparkless-polars: {error}");
                return;
            }
        };
        self.register_database_during_sql(name);
    }

    /// Register a database while the caller already holds the catalog writer guard.
    pub(crate) fn register_database_during_sql(&self, name: &str) {
        if name.eq_ignore_ascii_case("default") || name.eq_ignore_ascii_case("global_temp") {
            return;
        }
        let key = if self.shared_catalog.is_some() {
            name.to_ascii_lowercase()
        } else {
            name.to_string()
        };
        match self.databases.lock() {
            Ok(mut s) => {
                s.insert(key);
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: databases lock poisoned, register_database may have failed"
                );
            }
        }
    }

    /// List database names: built-in "default", "global_temp", plus any created via CREATE DATABASE / CREATE SCHEMA.
    /// If the databases lock is poisoned, returns only the built-in names and a message is emitted to stderr.
    pub fn list_database_names(&self) -> Vec<String> {
        let mut names: Vec<String> = vec!["default".to_string(), "global_temp".to_string()];
        match self.databases.lock() {
            Ok(guard) => {
                let mut created: Vec<String> = guard
                    .iter()
                    .filter(|name| {
                        !name.eq_ignore_ascii_case("default")
                            && !name.eq_ignore_ascii_case("global_temp")
                    })
                    .cloned()
                    .collect();
                created.sort();
                names.extend(created);
            }
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: databases lock poisoned, list_database_names may be incomplete"
                );
            }
        }
        names
    }

    /// True if the database name exists (default, global_temp, or created via CREATE DATABASE / CREATE SCHEMA).
    /// If the databases lock is poisoned, returns false for custom names and a message is emitted to stderr.
    pub fn database_exists(&self, name: &str) -> bool {
        if name.eq_ignore_ascii_case("default") || name.eq_ignore_ascii_case("global_temp") {
            return true;
        }
        match self.databases.lock() {
            Ok(s) if self.shared_catalog.is_some() => s.contains(&name.to_ascii_lowercase()),
            Ok(s) => s.contains(name),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: databases lock poisoned, database_exists may be wrong"
                );
                false
            }
        }
    }

    /// Get a saved table by name (tables map only). Returns None if not in saved tables (temp views not checked).
    /// If the tables lock is poisoned, returns None and a message is emitted to stderr.
    pub fn get_saved_table(&self, name: &str) -> Option<DataFrame> {
        match self.tables.lock() {
            Ok(m) => m.get(&self.saved_table_key(name)).cloned(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, get_saved_table may have failed"
                );
                None
            }
        }
    }

    /// Return a DataFrame of table names (and optional database) in this session. PySpark: catalog.listTables().
    /// Columns: "name" (string). Schema-qualified names like "my_schema.my_table" are listed as-is.
    /// If the tables lock is poisoned, returns an empty DataFrame and a message is emitted to stderr.
    pub fn list_tables(&self) -> Result<DataFrame, PolarsError> {
        let names = self.list_table_names();
        let rows: Vec<Vec<JsonValue>> = names
            .into_iter()
            .map(|n| vec![JsonValue::String(n)])
            .collect();
        let schema = vec![("name".to_string(), "string".to_string())];
        self.create_dataframe_from_rows(rows, schema, false, false)
    }

    /// Return a DataFrame of database names. PySpark: catalog.listDatabases().
    /// Column: "name" (string). Includes "default", "global_temp", and any created via CREATE DATABASE / CREATE SCHEMA.
    /// PR-C/#799,#798,#797,#796: Tests that assert 'test_schema' or 'test_db' in list expect this DataFrame.
    pub fn list_databases(&self) -> Result<DataFrame, PolarsError> {
        let names = self.list_database_names();
        let rows: Vec<Vec<JsonValue>> = names
            .into_iter()
            .map(|n| vec![JsonValue::String(n)])
            .collect();
        let schema = vec![("name".to_string(), "string".to_string())];
        self.create_dataframe_from_rows(rows, schema, false, false)
    }

    /// True if the name exists in the saved-tables map (not temp views).
    /// If the tables lock is poisoned, returns false and a message is emitted to stderr.
    pub fn saved_table_exists(&self, name: &str) -> bool {
        match self.tables.lock() {
            Ok(m) => m.contains_key(&self.saved_table_key(name)),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, saved_table_exists may be wrong"
                );
                false
            }
        }
    }

    /// Check if a table or temp view exists (PySpark: catalog.tableExists). True if name is in temp views, saved tables, global temp, or warehouse.
    /// If a catalog lock is poisoned, returns false for that source and a message is emitted to stderr.
    pub(crate) fn local_table_exists(&self, name: &str) -> bool {
        // global_temp.xyz
        if let Some((_db, tbl)) = Self::parse_global_temp_name(name) {
            return match self.global_temp_views().lock() {
                Ok(m) => m.contains_key(tbl),
                Err(_) => {
                    eprintln!(
                        "robin-sparkless-polars: global temp catalog lock poisoned, table_exists may be wrong"
                    );
                    false
                }
            };
        }
        if self.shared_catalog.is_none() || !name.contains('.') {
            let exact_only = name.contains('.') && self.external_catalog().ok().flatten().is_some();
            if if exact_only {
                self.has_exact_temp_view(name)
            } else {
                self.has_temp_view(name)
            } {
                return true;
            }
        }
        if match self.tables.lock() {
            Ok(m) => m.contains_key(&self.saved_table_key(name)),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, table_exists may be wrong"
                );
                false
            }
        } {
            return true;
        }
        // #984: schema-qualified name — try unqualified table name only for the
        // legacy local catalog. Once an external provider is attached, a miss for
        // the full identifier must not be shadowed by an unrelated local suffix.
        if self.shared_catalog.is_none()
            && name.contains('.')
            && self.external_catalog().ok().flatten().is_none()
        {
            let unqualified = name.rsplit_once('.').map(|(_, tbl)| tbl).unwrap_or(name);
            if match self.catalog.lock() {
                Ok(m) => m.contains_key(unqualified),
                Err(_) => false,
            } || match self.tables.lock() {
                Ok(m) => m.contains_key(unqualified),
                Err(_) => false,
            } {
                return true;
            }
        }
        // Warehouse fallback
        if let Some(warehouse) = self.warehouse_dir() {
            let path = Path::new(warehouse).join(name);
            if path.is_dir() {
                return true;
            }
            if name.contains('.') {
                let path_qualified =
                    Path::new(warehouse).join(name.replace('.', std::path::MAIN_SEPARATOR_STR));
                if path_qualified.is_dir() {
                    return true;
                }
            }
        }
        false
    }

    /// Check local catalogs, the warehouse, and the configured external catalog.
    pub fn try_table_exists(&self, name: &str) -> Result<bool, PolarsError> {
        if self.local_table_exists(name) {
            return Ok(true);
        }
        let (resolved, _) = self.resolve_external_table_descriptor(name)?;
        Ok(resolved.is_some())
    }

    /// Best-effort existence check. Use [`Self::try_table_exists`] when provider errors
    /// need to be distinguished from a genuine catalog miss.
    pub fn table_exists(&self, name: &str) -> bool {
        self.try_table_exists(name).unwrap_or(false)
    }

    pub(crate) fn is_global_temp_name(name: &str) -> bool {
        Self::parse_global_temp_name(name).is_some()
    }

    /// Return global temp view names (process-scoped). PySpark: catalog.listTables(dbName="global_temp").
    /// If the global catalog lock is poisoned, returns empty and a message is emitted to stderr.
    pub fn list_global_temp_view_names(&self) -> Vec<String> {
        match self.global_temp_views().lock() {
            Ok(m) => m.keys().cloned().collect(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: global temp catalog lock poisoned, list_global_temp_view_names may be incomplete"
                );
                Vec::new()
            }
        }
    }

    /// Return temporary view names in this session.
    /// If the catalog lock is poisoned, returns empty and a message is emitted to stderr.
    pub fn list_temp_view_names(&self) -> Vec<String> {
        match self.catalog.lock() {
            Ok(m) => m.keys().cloned().collect(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: catalog lock poisoned, list_temp_view_names may be incomplete"
                );
                Vec::new()
            }
        }
    }

    /// Return saved table names in this session (saveAsTable / write_delta_table).
    /// If the tables lock is poisoned, returns empty and a message is emitted to stderr.
    pub fn list_table_names(&self) -> Vec<String> {
        match self.tables.lock() {
            Ok(m) => m.keys().cloned().collect(),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, list_table_names may be incomplete"
                );
                Vec::new()
            }
        }
    }

    /// Drop a saved table by name (removes from tables catalog only). No-op if not present.
    /// If the tables lock is poisoned, returns false and a message is emitted to stderr.
    pub fn drop_table(&self, name: &str) -> bool {
        let (_mutation, _writer) = match self.shared_catalog_mutation_and_writer_guards() {
            Ok(guards) => guards,
            Err(error) => {
                eprintln!("robin-sparkless-polars: {error}");
                return false;
            }
        };
        self.drop_table_during_sql(name)
    }

    /// Drop a saved table while the caller already holds the catalog writer guard.
    pub(crate) fn drop_table_during_sql(&self, name: &str) -> bool {
        let key = self.saved_table_key(name);
        let removed = match self.tables.lock() {
            Ok(mut m) => m.remove(&key),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, drop_table may have failed"
                );
                None
            }
        };
        if let Some(frame) = removed {
            self.retain_catalog_version_if_readers(frame, None);
            true
        } else {
            false
        }
    }

    /// Drop a database/schema by name (from DROP SCHEMA / DROP DATABASE). Removes from registered databases only.
    /// Does not drop "default" or "global_temp". No-op if not present (or if_exists). Returns true if removed.
    /// If the databases lock is poisoned, returns false and a message is emitted to stderr.
    pub fn drop_database(&self, name: &str) -> bool {
        let (_mutation, _writer) = match self.shared_catalog_mutation_and_writer_guards() {
            Ok(guards) => guards,
            Err(error) => {
                eprintln!("robin-sparkless-polars: {error}");
                return false;
            }
        };
        self.drop_database_during_sql(name)
    }

    /// Drop a database while the caller already holds the catalog writer guard.
    pub(crate) fn drop_database_during_sql(&self, name: &str) -> bool {
        if name.eq_ignore_ascii_case("default") || name.eq_ignore_ascii_case("global_temp") {
            return false;
        }
        if self.shared_catalog.is_some() && self.database_has_saved_tables(name) {
            return false;
        }
        // Preserve the historical embedded behavior. A service session keeps its current
        // database after a drop so the next statement reports that the database is missing.
        if self.shared_catalog.is_none() && self.current_database().eq(name) {
            let _ = self.set_current_database("default");
        }
        match self.databases.lock() {
            Ok(mut s) if self.shared_catalog.is_some() => s.remove(&name.to_ascii_lowercase()),
            Ok(mut s) => s.remove(name),
            Err(_) => {
                eprintln!(
                    "robin-sparkless-polars: databases lock poisoned, drop_database may have failed"
                );
                false
            }
        }
    }

    /// Parse "global_temp.xyz" into ("global_temp", "xyz"). Returns None for plain names.
    fn parse_global_temp_name(name: &str) -> Option<(&str, &str)> {
        if let Some(dot) = name.find('.') {
            let (db, tbl) = name.split_at(dot);
            if db.eq_ignore_ascii_case("global_temp") {
                return Some((db, tbl.strip_prefix('.').unwrap_or(tbl)));
            }
        }
        None
    }

    /// Return spark.sql.warehouse.dir from config if set. Enables disk-backed saveAsTable.
    pub fn warehouse_dir(&self) -> Option<&str> {
        self.config
            .get("spark.sql.warehouse.dir")
            .map(|s| s.as_str())
            .filter(|s| !s.is_empty())
    }

    /// Resolve a table name to a Delta table path (warehouse/name if it contains _delta_log).
    /// Returns None if the table is not a warehouse-backed Delta table (DESCRIBE DETAIL support).
    #[cfg(feature = "delta")]
    pub fn resolve_delta_table_path(&self, table_name: &str) -> Option<std::path::PathBuf> {
        let warehouse = self.warehouse_dir()?;
        let path = robin_sparkless_core::resolve_path_under_base(warehouse, table_name).ok()?;
        if path.is_dir() && path.join("_delta_log").is_dir() {
            Some(path)
        } else {
            None
        }
    }

    /// Look up a table or temp view by name (PySpark: table(name)).
    /// Resolution order: (1) global_temp.xyz from global catalog, (2) temp view, (3) saved table, (4) warehouse.
    pub fn table(&self, name: &str) -> Result<DataFrame, PolarsError> {
        self.table_impl(name, true, false)
    }

    /// Resolve local tables normally and external table schemas without scanning source rows.
    /// Connect analysis uses this to report names/types before execution begins.
    pub fn table_schema_only(&self, name: &str) -> Result<DataFrame, PolarsError> {
        self.table_schema_only_with_cancellation(name, CancellationToken::new())
    }

    /// Resolve a schema-only table lookup while honoring the caller's request cancellation.
    pub fn table_schema_only_with_cancellation(
        &self,
        name: &str,
        token: CancellationToken,
    ) -> Result<DataFrame, PolarsError> {
        self.table_impl_with_source(name, true, false, None, token, false, &mut None)
    }

    fn empty_dataframe_with_schema(&self, schema: StructType) -> Result<DataFrame, PolarsError> {
        let empty = PlDataFrame::empty_with_schema(&schema.to_polars_schema());
        DataFrame::from_eager_with_options(empty, self.is_case_sensitive())
            .with_logical_schema(schema)
    }

    /// Resolve and scan a table in the caller's statement-wide external source snapshot.
    pub fn table_in_read_snapshot(
        &self,
        name: &str,
        snapshot: Arc<dyn CatalogReadSnapshot>,
        token: CancellationToken,
    ) -> Result<DataFrame, PolarsError> {
        self.table_impl_with_source(name, true, true, Some(snapshot), token, true, &mut None)
    }

    /// Resolve a Connect table and report the DuckDB owner selected by the same lookup.
    /// Local views and managed tables retain their normal precedence over external tables.
    pub fn table_for_connect(
        &self,
        name: &str,
        snapshot: Option<Arc<dyn CatalogReadSnapshot>>,
        token: CancellationToken,
        materialize_external: bool,
    ) -> Result<(DataFrame, Option<String>), PolarsError> {
        let mut source_owner = None;
        let frame = self.table_impl_with_source(
            name,
            true,
            snapshot.is_some(),
            snapshot,
            token,
            materialize_external,
            &mut source_owner,
        )?;
        Ok((frame, source_owner))
    }

    #[cfg(any(feature = "sql", feature = "delta"))]
    pub(crate) fn table_after_external_miss(
        &self,
        name: &str,
        provider_configured: bool,
    ) -> Result<DataFrame, PolarsError> {
        self.table_impl(name, false, provider_configured)
    }

    fn table_impl(
        &self,
        name: &str,
        resolve_external: bool,
        provider_configured: bool,
    ) -> Result<DataFrame, PolarsError> {
        let active_context = ACTIVE_EXECUTION_CONTEXT.with(|cell| cell.borrow().clone());
        let source_snapshot = active_context
            .as_ref()
            .and_then(|context| context.source_snapshot().cloned());
        let token = active_context
            .as_ref()
            .map(|context| context.cancellation().clone())
            .unwrap_or_default();
        self.table_impl_with_source(
            name,
            resolve_external,
            provider_configured || source_snapshot.is_some(),
            source_snapshot,
            token,
            true,
            &mut None,
        )
    }

    fn table_impl_with_source(
        &self,
        name: &str,
        resolve_external: bool,
        provider_configured: bool,
        source_snapshot: Option<Arc<dyn CatalogReadSnapshot>>,
        token: CancellationToken,
        materialize_external: bool,
        source_owner: &mut Option<String>,
    ) -> Result<DataFrame, PolarsError> {
        // global_temp.xyz -> global catalog only
        if let Some((_db, tbl)) = Self::parse_global_temp_name(name) {
            if let Some(df) = self
                .global_temp_views()
                .lock()
                .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
                .get(tbl)
                .cloned()
            {
                return Ok(df);
            }
            return Err(PolarsError::InvalidOperation(
                format!(
                    "Global temp view '{tbl}' not found. Register it with createOrReplaceGlobalTempView."
                )
                .into(),
            ));
        }
        // Session: temp view, saved table
        if let Some(df) = if self.shared_catalog.is_some() && name.contains('.') {
            None
        } else {
            self.temp_view(name)
        } {
            return Ok(df);
        }
        // An exact session-local saved table shadows an external catalog entry. Check it
        // before resolving multipart aliases so an unrelated provider row cannot capture it.
        let saved_table_key = self.saved_table_key(name);
        if let Some(df) = self
            .tables
            .lock()
            .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
            .get(&saved_table_key)
            .cloned()
        {
            return Ok(df);
        }
        let (mut external_df, mut has_external_provider) = (None, provider_configured);
        // External aliases are qualified namespaces. Resolve them before the managed catalog
        // validates the first component as a registered database (for example, `warehouse`).
        if resolve_external && name.contains('.') {
            (external_df, has_external_provider) = self.resolve_external_table_for_read(
                name,
                source_snapshot.clone(),
                &token,
                materialize_external,
                source_owner,
            )?;
            if let Some(df) = external_df.as_ref() {
                return Ok(df.clone());
            }
        }
        let (external_df, has_external_provider) = if resolve_external && !name.contains('.') {
            self.resolve_external_table_for_read(
                name,
                source_snapshot,
                &token,
                materialize_external,
                source_owner,
            )?
        } else {
            (external_df, has_external_provider)
        };
        if let Some(df) = external_df {
            return Ok(df);
        }
        if has_external_provider {
            return Err(PolarsError::InvalidOperation(
                format!("Externally owned table or view '{name}' was not found.").into(),
            ));
        }
        self.check_saved_table_database(name)?;
        // #984: schema-qualified name (e.g. test_schema.test_table): try unqualified table name so table("test_schema.test_table") finds a table registered as "test_table".
        // Preserve this historical fallback only when there is no external resolver. If a
        // configured catalog misses a multipart name, returning a local table with the same
        // final component would bind an unrelated relation.
        if self.shared_catalog.is_none()
            && name.contains('.')
            && !has_external_provider
            && self.external_catalog()?.is_none()
        {
            let unqualified = name.rsplit_once('.').map(|(_, tbl)| tbl).unwrap_or(name);
            if let Some(df) = self
                .catalog
                .lock()
                .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
                .get(unqualified)
                .cloned()
            {
                return Ok(df);
            }
            if let Some(df) = self
                .tables
                .lock()
                .map_err(|_| PolarsError::InvalidOperation("catalog lock poisoned".into()))?
                .get(unqualified)
                .cloned()
            {
                return Ok(df);
            }
        }
        // Warehouse fallback (disk-backed saveAsTable). #760: schema-qualified name try both
        // "schema.table" as single dir and "schema/table" (schema as subdir).
        if let Some(warehouse) = self.warehouse_dir() {
            if let Some(df) = read_warehouse_table_at(self, warehouse, name)? {
                return Ok(df);
            }
            if name.contains('.') {
                let alt = name.replace('.', std::path::MAIN_SEPARATOR_STR);
                if let Some(df) = read_warehouse_table_at(self, warehouse, &alt)? {
                    return Ok(df);
                }
            }
        }
        Err(PolarsError::InvalidOperation(
            format!(
                "Table or view '{name}' not found. Register it with create_or_replace_temp_view or saveAsTable. \
                (Schema-qualified names like 'schema.table' are supported; use the same name when saving and when calling table().)"
            )
            .into(),
        ))
    }

    fn resolve_external_table_for_read(
        &self,
        name: &str,
        snapshot: Option<Arc<dyn CatalogReadSnapshot>>,
        token: &CancellationToken,
        materialize: bool,
        source_owner: &mut Option<String>,
    ) -> Result<(Option<DataFrame>, bool), PolarsError> {
        let catalog = self.external_catalog()?;
        let Some(catalog) = catalog else {
            return if snapshot.is_some() {
                Err(PolarsError::InvalidOperation(
                    "external source snapshot is not attached to this SparkSession".into(),
                ))
            } else {
                Ok((None, false))
            };
        };
        let snapshot = match snapshot {
            Some(snapshot) => Some(snapshot),
            None if catalog.has_reader() => Some(
                begin_catalog_read_snapshot(&catalog, token)
                    .map_err(|error| PolarsError::InvalidOperation(error.to_string().into()))?,
            ),
            None => None,
        };
        let ownership = match snapshot.as_ref() {
            Some(snapshot) => snapshot.resolve_ownership(name),
            None => catalog.resolve_ownership(name),
        }
        .map_err(|error| {
            PolarsError::InvalidOperation(
                format!("external catalog failed to resolve owner for '{name}': {error}").into(),
            )
        })?;
        if matches!(ownership, CatalogOwnership::Local) {
            return Ok((None, false));
        }
        catalog
            .require_read_capability(&ownership)
            .map_err(|error| {
                PolarsError::InvalidOperation(format!("READ_CAPABILITY_DENIED: {error}").into())
            })?;
        let descriptor = match snapshot.as_ref() {
            Some(snapshot) => snapshot.resolve_table(name),
            None => catalog.resolve_table(&ownership, name),
        }
        .map_err(|error| {
            PolarsError::InvalidOperation(
                format!("external catalog failed to resolve '{name}': {error}").into(),
            )
        })?;
        let Some(descriptor) = descriptor else {
            return Ok((None, true));
        };
        descriptor.validate().map_err(|error| {
            PolarsError::InvalidOperation(
                format!("external catalog returned an invalid descriptor for '{name}': {error}")
                    .into(),
            )
        })?;
        if descriptor.format == ExternalTableFormat::DuckDb {
            *source_owner = Some(descriptor.canonical_location.clone());
        }
        if !materialize {
            let empty = self.empty_dataframe_with_schema(descriptor.schema.clone())?;
            return Ok((Some(empty), true));
        }
        let frame =
            self.read_external_table_in_snapshot(name, &descriptor, snapshot, token, true)?;
        Ok((Some(frame), true))
    }

    pub(crate) fn resolve_external_table_descriptor(
        &self,
        name: &str,
    ) -> Result<(Option<ResolvedExternalTable>, bool), PolarsError> {
        let Some(catalog) = self.external_catalog()? else {
            return Ok((None, false));
        };
        let snapshot = ACTIVE_EXECUTION_CONTEXT.with(|cell| {
            cell.borrow()
                .as_ref()
                .and_then(|context| context.source_snapshot().cloned())
        });
        let ownership = match snapshot.as_ref() {
            Some(snapshot) => snapshot.resolve_ownership(name),
            None => catalog.resolve_ownership(name),
        }
        .map_err(|error| {
            PolarsError::InvalidOperation(
                format!("external catalog failed to resolve owner for '{name}': {error}").into(),
            )
        })?;
        if matches!(ownership, CatalogOwnership::Local) {
            return Ok((None, false));
        }
        catalog
            .require_read_capability(&ownership)
            .map_err(|error| {
                PolarsError::InvalidOperation(format!("READ_CAPABILITY_DENIED: {error}").into())
            })?;
        let descriptor_result = match snapshot.as_ref() {
            Some(snapshot) => snapshot.resolve_table(name),
            None => catalog.resolve_table(&ownership, name),
        };
        let Some(descriptor) = descriptor_result.map_err(|error| {
            PolarsError::InvalidOperation(
                format!("external catalog failed to resolve '{name}': {error}").into(),
            )
        })?
        else {
            return Ok((None, true));
        };
        descriptor.validate().map_err(|error| {
            PolarsError::InvalidOperation(
                format!("external catalog returned an invalid descriptor for '{name}': {error}")
                    .into(),
            )
        })?;
        Ok((Some((catalog, ownership, descriptor)), true))
    }

    pub(crate) fn read_external_table_descriptor(
        &self,
        name: &str,
        descriptor: &ExternalTableDescriptor,
    ) -> Result<DataFrame, PolarsError> {
        self.read_external_table_descriptor_with_schema_validation(name, descriptor, true)
    }

    fn read_external_table_descriptor_with_schema_validation(
        &self,
        name: &str,
        descriptor: &ExternalTableDescriptor,
        validate_schema: bool,
    ) -> Result<DataFrame, PolarsError> {
        let token = CancellationToken::new();
        let active_snapshot = ACTIVE_EXECUTION_CONTEXT.with(|cell| {
            cell.borrow()
                .as_ref()
                .and_then(|context| context.source_snapshot().cloned())
        });
        let snapshot = if active_snapshot.is_some() {
            active_snapshot
        } else if descriptor.format == ExternalTableFormat::DuckDb {
            let catalog = self.external_catalog()?.ok_or_else(|| {
                PolarsError::InvalidOperation("DuckDB source is not configured".into())
            })?;
            let ownership = catalog
                .resolve_ownership(name)
                .map_err(|error| PolarsError::InvalidOperation(error.to_string().into()))?;
            Some(
                catalog
                    .begin_snapshot(&ownership, &token)
                    .map_err(|error| PolarsError::InvalidOperation(error.to_string().into()))?,
            )
        } else {
            None
        };
        self.read_external_table_in_snapshot(name, descriptor, snapshot, &token, validate_schema)
    }

    fn read_external_table_in_snapshot(
        &self,
        name: &str,
        descriptor: &ExternalTableDescriptor,
        snapshot: Option<Arc<dyn CatalogReadSnapshot>>,
        token: &CancellationToken,
        validate_schema: bool,
    ) -> Result<DataFrame, PolarsError> {
        #[cfg(not(feature = "duckdb-source"))]
        let _ = (&snapshot, token);
        let frame = if descriptor.format == ExternalTableFormat::DuckDb {
            #[cfg(feature = "duckdb-source")]
            {
                self.read_duckdb_table_batches(name, descriptor, snapshot.as_deref(), token)?
            }
            #[cfg(not(feature = "duckdb-source"))]
            {
                return Err(PolarsError::InvalidOperation(
                    "DuckDB source reads require the 'duckdb-source' feature".into(),
                ));
            }
        } else {
            self.read_external_descriptor(descriptor)?
        };

        if !validate_schema {
            return Ok(frame);
        }
        let actual_schema = frame.schema()?;
        #[cfg(feature = "delta")]
        let logical_map_paths = crate::delta::logical_map_paths_with_version(
            &descriptor.canonical_location,
            descriptor.version,
        )?;
        #[cfg(not(feature = "delta"))]
        let logical_map_paths = Vec::new();
        if !external_schemas_match(&descriptor.schema, &actual_schema, &logical_map_paths) {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "external table schema mismatch for '{}' (table_id '{}')",
                    name, descriptor.table_id
                )
                .into(),
            ));
        }
        Ok(frame)
    }

    #[cfg(feature = "duckdb-source")]
    fn read_duckdb_table_batches(
        &self,
        name: &str,
        descriptor: &ExternalTableDescriptor,
        snapshot: Option<&dyn CatalogReadSnapshot>,
        token: &CancellationToken,
    ) -> Result<DataFrame, PolarsError> {
        let snapshot = snapshot.ok_or_else(|| {
            PolarsError::InvalidOperation(
                "DuckDB source read requires an operation snapshot".into(),
            )
        })?;
        let mut frame: Option<PlDataFrame> = None;
        let mut row_count = 0u64;
        snapshot
            .scan_table(descriptor, token, &mut |batch| {
                if batch.bytes.len() > crate::duckdb_source::DUCKDB_SOURCE_MAX_BATCH_BYTES {
                    return Err(EngineError::invalid_input(
                        "DuckDB source IPC batch exceeded its configured byte limit".into(),
                    ));
                }
                row_count = row_count.checked_add(batch.row_count).ok_or_else(|| {
                    EngineError::invalid_input("DuckDB source row count overflow".into())
                })?;
                if row_count > crate::duckdb_source::DUCKDB_SOURCE_MAX_ROWS {
                    return Err(EngineError::invalid_input(
                        "DuckDB source exceeded its configured row limit".into(),
                    ));
                }
                let (decoded, _) = self.create_dataframe_from_arrow_ipc(
                    Some(&batch.bytes),
                    Some(descriptor.schema.clone()),
                    crate::duckdb_source::DUCKDB_SOURCE_MAX_BATCH_BYTES,
                    crate::duckdb_source::DUCKDB_SOURCE_MAX_BATCH_ROWS,
                    64 * 1024 * 1024,
                    512 * 1024,
                )?;
                let mut decoded = decoded
                    .collect()
                    .map_err(|error| EngineError::invalid_input(error.to_string()))?
                    .as_ref()
                    .clone();
                let source_names = descriptor
                    .schema
                    .logical_fields()
                    .iter()
                    .map(|field| field.name.clone())
                    .collect::<Vec<_>>();
                decoded
                    .set_column_names(&source_names)
                    .map_err(|error| EngineError::invalid_input(error.to_string()))?;
                snapshot.charge_decoded_bytes(decoded.estimated_size())?;
                match frame.as_mut() {
                    Some(frame) => {
                        frame
                            .vstack_mut(&decoded)
                            .map_err(|error| EngineError::invalid_input(error.to_string()))?;
                    }
                    None => frame = Some(decoded),
                }
                Ok(())
            })
            .map_err(|error| PolarsError::InvalidOperation(error.to_string().into()))?;
        token
            .check_cancelled()
            .map_err(|error| PolarsError::InvalidOperation(error.to_string().into()))?;
        let dataframe = match frame {
            Some(frame) => DataFrame::from_polars_with_options(frame, self.is_case_sensitive())
                .with_logical_schema(descriptor.schema.clone())?,
            None => self.empty_dataframe_with_schema(descriptor.schema.clone())?,
        };
        let _ = name;
        Ok(dataframe)
    }

    fn read_external_descriptor(
        &self,
        descriptor: &ExternalTableDescriptor,
    ) -> Result<DataFrame, PolarsError> {
        match descriptor.format {
            #[cfg(feature = "delta")]
            ExternalTableFormat::Delta => {
                let normalized = crate::delta::normalize_delta_path(std::path::Path::new(
                    &descriptor.canonical_location,
                ))?;
                let path = confine_io_path(&normalized)?;
                crate::delta::read_delta_snapshot_with_version(
                    path,
                    descriptor.version,
                    self.is_case_sensitive(),
                )
            }
            #[cfg(not(feature = "delta"))]
            ExternalTableFormat::Delta => Err(PolarsError::InvalidOperation(
                "external Delta table reads require the 'delta' feature".into(),
            )),
            ExternalTableFormat::DuckDb => Err(PolarsError::InvalidOperation(
                "DuckDB table reads require an operation-scoped source snapshot".into(),
            )),
            _ => Err(PolarsError::InvalidOperation(
                "external table format is not supported by this Sparkless backend".into(),
            )),
        }
    }

    pub fn builder() -> SparkSessionBuilder {
        SparkSessionBuilder::new()
    }

    /// Create a session from a [`SparklessConfig`](SparklessConfig).
    /// Equivalent to `SparkSession::builder().with_config(config).get_or_create()`.
    pub fn from_config(config: &SparklessConfig) -> SparkSession {
        Self::builder().with_config(config).get_or_create()
    }

    /// Return a reference to the session config (for catalog/conf compatibility).
    pub fn get_config(&self) -> &HashMap<String, String> {
        &self.config
    }

    /// Return a reference to the UDF registry. Used for thread context management.
    pub fn udf_registry(&self) -> &UdfRegistry {
        &self.udf_registry
    }

    /// Set a config key at runtime (PySpark: spark.conf.set(key, value)).
    /// When key is spark.sql.session.timeZone, also updates the thread UDF context so hour/minute/second use it (#1154).
    pub fn set_config(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();
        if key == "sparkless.pyspark.compat" {
            if let Some(compat) = PysparkCompat::parse(&value) {
                SessionRuntimeConfig::set_compat_profile(&mut self.config, compat);
                self.runtime_config = SessionRuntimeConfig::from_session_map(&self.config);
                self.sync_thread_context();
                return;
            }
        }
        self.config.insert(key.clone(), value.clone());
        if key == "spark.sql.session.timeZone" {
            crate::update_thread_session_time_zone(Some(value.clone()));
        }
        self.runtime_config = SessionRuntimeConfig::from_session_map(&self.config);
        self.sync_thread_context();
    }

    /// Current runtime semantics (ANSI, map inference, compat profile).
    pub fn runtime_config(&self) -> &SessionRuntimeConfig {
        &self.runtime_config
    }

    /// Capture the session settings, cancellation signal, and one source snapshot for an action.
    pub fn execution_context(
        &self,
        operation_id: impl Into<Arc<str>>,
        cancellation: CancellationToken,
    ) -> Result<ExecutionContext, EngineError> {
        let configuration = ExecutionConfigSnapshot::new(
            self.runtime_config.clone(),
            self.is_case_sensitive(),
            self.config.get("spark.sql.session.timeZone").cloned(),
            decimal_allow_precision_loss_from_session_map(&self.config),
        );
        let mut context = ExecutionContext::new(operation_id, configuration, cancellation.clone())
            .bind_to_session(&self.execution_identity);
        if let Some(snapshot) = self.begin_external_catalog_read_snapshot(&cancellation)? {
            context = context.with_source_snapshot(snapshot);
        }
        Ok(context)
    }

    /// Run SQL with an immutable operation context and restore worker thread settings after
    /// completion, including when planning or execution returns an error.
    pub fn sql_with_context(
        &self,
        query: &str,
        context: &ExecutionContext,
    ) -> Result<DataFrame, EngineError> {
        if !context.belongs_to_session(&self.execution_identity) {
            return Err(EngineError::invalid_input(
                "execution context belongs to a different SparkSession".into(),
            ));
        }
        context.check_cancelled()?;
        let _active_context = ExecutionContextGuard::install(context.clone());
        let _thread_context = crate::udf_context::ThreadContextGuard::capture();
        let configuration = context.configuration();
        crate::set_thread_udf_context_full(
            Arc::new(self.udf_registry.clone()),
            configuration.case_sensitive(),
            configuration.session_time_zone().map(str::to_owned),
            configuration.runtime().clone(),
        );
        crate::set_thread_decimal_allow_precision_loss(
            configuration.decimal_allow_precision_loss(),
        );
        self.sql_with_cancellation(query, context.cancellation())
    }

    pub fn sync_thread_context(&self) {
        let session_tz = self.config.get("spark.sql.session.timeZone").cloned();
        crate::set_thread_udf_context_full(
            Arc::new(self.udf_registry.clone()),
            self.is_case_sensitive(),
            session_tz,
            self.runtime_config.clone(),
        );
        crate::set_thread_decimal_allow_precision_loss(
            decimal_allow_precision_loss_from_session_map(&self.config),
        );
    }

    /// Whether column names are case-sensitive (PySpark: spark.sql.caseSensitive).
    /// Default is false (case-insensitive matching).
    pub fn is_case_sensitive(&self) -> bool {
        if let Some(value) = ACTIVE_EXECUTION_CONTEXT.with(|cell| {
            cell.borrow()
                .as_ref()
                .map(|context| context.configuration().case_sensitive())
        }) {
            return value;
        }
        self.config
            .get("spark.sql.caseSensitive")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Register a Rust UDF. Session-scoped. Use with call_udf. PySpark: spark.udf.register (Python) or equivalent.
    pub fn register_udf<F>(&self, name: &str, f: F) -> Result<(), PolarsError>
    where
        F: Fn(&[Series]) -> Result<Series, PolarsError> + Send + Sync + 'static,
    {
        self.udf_registry.register_rust_udf(name, f)
    }

    /// Create a DataFrame from a vector of tuples (i64, i64, String)
    ///
    /// # Example
    /// ```
    /// use robin_sparkless_polars::SparkSession;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df = spark.create_dataframe(
    ///     vec![
    ///         (1, 25, "Alice".to_string()),
    ///         (2, 30, "Bob".to_string()),
    ///     ],
    ///     vec!["id", "age", "name"],
    /// )?;
    /// #     let _ = df;
    /// #     Ok(())
    /// # }
    /// ```
    pub fn create_dataframe(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<DataFrame, PolarsError> {
        if column_names.len() != 3 {
            return Err(PolarsError::ComputeError(
                format!(
                    "create_dataframe: expected 3 column names for (i64, i64, String) tuples, got {}. Hint: provide exactly 3 names, e.g. [\"id\", \"age\", \"name\"].",
                    column_names.len()
                )
                .into(),
            ));
        }

        let mut cols: Vec<Series> = Vec::with_capacity(3);

        // First column: i64
        let col0: Vec<i64> = data.iter().map(|t| t.0).collect();
        cols.push(Series::new(column_names[0].into(), col0));

        // Second column: i64
        let col1: Vec<i64> = data.iter().map(|t| t.1).collect();
        cols.push(Series::new(column_names[1].into(), col1));

        // Third column: String
        let col2: Vec<String> = data.iter().map(|t| t.2.clone()).collect();
        cols.push(Series::new(column_names[2].into(), col2));

        let pl_df = PlDataFrame::new_infer_height(cols.iter().map(|s| s.clone().into()).collect())?;
        Ok(DataFrame::from_polars_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// Same as [`create_dataframe`](Self::create_dataframe) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn create_dataframe_engine(
        &self,
        data: Vec<(i64, i64, String)>,
        column_names: Vec<&str>,
    ) -> Result<DataFrame, EngineError> {
        self.create_dataframe(data, column_names)
            .map_err(polars_to_core_error)
    }

    /// Create a DataFrame from a Polars DataFrame
    pub fn create_dataframe_from_polars(&self, df: PlDataFrame) -> DataFrame {
        DataFrame::from_polars_with_options(df, self.is_case_sensitive())
    }

    /// Create a one-column Long result for Spark Connect's deferred global count node.
    pub fn create_dataframe_from_i64_column(
        &self,
        name: &str,
        values: Vec<i64>,
    ) -> Result<DataFrame, EngineError> {
        let column = Series::new(name.into(), values);
        let frame =
            PlDataFrame::new_infer_height(vec![column.into()]).map_err(polars_to_core_error)?;
        Ok(self.create_dataframe_from_polars(frame))
    }

    /// Decode a bounded Arrow IPC stream as a Connect local relation, applying the declared
    /// Spark schema by position while retaining public labels and logical field metadata.
    ///
    /// The input is preflighted before Polars materializes any record batch: IPC compression,
    /// dictionary encoding, nested Arrow fields, excessive rows, and oversized scalar values are
    /// rejected at this boundary. A missing payload creates a typed empty relation.
    pub fn create_dataframe_from_arrow_ipc(
        &self,
        data: Option<&[u8]>,
        schema: Option<StructType>,
        max_ipc_bytes: usize,
        max_rows: usize,
        max_decoded_bytes: usize,
        max_scalar_bytes: usize,
    ) -> Result<(DataFrame, StructType), EngineError> {
        match data {
            Some(data) => {
                if data.len() > max_ipc_bytes {
                    return Err(EngineError::invalid_input(format!(
                        "Connect local Arrow IPC data exceeds {max_ipc_bytes} bytes"
                    )));
                }
                let row_count = preflight_connect_arrow_stream(data, max_rows, max_decoded_bytes)?;
                let (normalized_data, public_names) = rewrite_connect_arrow_input_schema(data)?;
                let mut reader =
                    polars::io::ipc::IpcStreamReader::new(Cursor::new(normalized_data.as_slice()));
                let arrow_schema = reader.arrow_schema().map_err(|error| {
                    EngineError::invalid_input(format!("invalid Connect Arrow schema: {error}"))
                })?;
                let source_schema = match schema {
                    Some(schema) => schema,
                    None => infer_connect_schema(&arrow_schema, &public_names)?,
                };
                if source_schema.logical_fields().len() != arrow_schema.len() {
                    return Err(EngineError::invalid_input(format!(
                        "Connect local schema has {} fields but Arrow data has {}",
                        source_schema.logical_fields().len(),
                        arrow_schema.len()
                    )));
                }
                validate_connect_arrow_source_schema(&arrow_schema)?;
                let frame = reader
                    .with_n_rows(Some(max_rows.saturating_add(1)))
                    .set_rechunk(false)
                    .finish()
                    .map_err(|error| {
                        EngineError::invalid_input(format!(
                            "invalid Connect Arrow IPC data: {error}"
                        ))
                    })?;
                if frame.height() > max_rows {
                    return Err(EngineError::invalid_input(format!(
                        "Connect local relation exceeds {max_rows} rows"
                    )));
                }
                if frame.height() as u64 != row_count {
                    return Err(EngineError::invalid_input(format!(
                        "Connect Arrow IPC row count mismatch: metadata declares {row_count}, decoder produced {}",
                        frame.height()
                    )));
                }
                if frame.estimated_size() > max_decoded_bytes {
                    return Err(EngineError::invalid_input(format!(
                        "Connect local decoded buffers exceed {max_decoded_bytes} bytes"
                    )));
                }
                let physical_schema = connect_physical_schema(&source_schema)?;
                let mut columns = Vec::with_capacity(frame.width());
                for (index, (column, logical_field)) in frame
                    .columns()
                    .iter()
                    .zip(source_schema.logical_fields())
                    .enumerate()
                {
                    let internal_name = connect_local_column_name(index);
                    let target_type = crate::schema_conv::checked_data_type_to_polars_type(
                        &logical_field.data_type,
                        &logical_field.name,
                    )
                    .map_err(|error| {
                        EngineError::invalid_input(format!("invalid Connect local schema: {error}"))
                    })?;
                    let source = column.as_materialized_series();
                    validate_connect_scalar_values(source, max_scalar_bytes, &logical_field.name)?;
                    let mut converted = source
                        .cast_with_options(&target_type, CastOptions::Strict)
                        .map_err(|error| EngineError::invalid_input(format!(
                            "Connect local column {:?} cannot be cast to its declared type: {error}",
                            logical_field.name
                        )))?;
                    converted.rename(internal_name.into());
                    if !logical_field.nullable && converted.null_count() != 0 {
                        return Err(EngineError::invalid_input(format!(
                            "Connect local non-nullable column {:?} contains null values",
                            logical_field.name
                        )));
                    }
                    validate_connect_scalar_values(
                        &converted,
                        max_scalar_bytes,
                        &logical_field.name,
                    )?;
                    columns.push(converted.into());
                }
                let imported = PlDataFrame::new_infer_height(columns).map_err(|error| {
                    EngineError::invalid_input(format!(
                        "cannot stage Connect local relation: {error}"
                    ))
                })?;
                let decoded_bytes = imported.estimated_size();
                if decoded_bytes > max_decoded_bytes {
                    return Err(EngineError::invalid_input(format!(
                        "Connect local decoded buffers exceed {max_decoded_bytes} bytes"
                    )));
                }
                let dataframe =
                    DataFrame::from_polars_with_options(imported, self.is_case_sensitive())
                        .with_logical_schema(physical_schema.clone())
                        .map_err(|error| {
                            EngineError::invalid_input(format!(
                                "invalid Connect local logical schema: {error}"
                            ))
                        })?;
                Ok((dataframe, source_schema))
            }
            None => {
                let source_schema = schema.ok_or_else(|| {
                    EngineError::invalid_input(
                        "Connect LocalRelation requires data or a schema".into(),
                    )
                })?;
                source_schema.validate_metadata().map_err(|error| {
                    EngineError::invalid_input(format!(
                        "invalid Connect local schema metadata: {error}"
                    ))
                })?;
                let physical_schema = connect_physical_schema(&source_schema)?;
                let empty_schema = physical_schema.to_polars_schema();
                let empty = PlDataFrame::empty_with_schema(&empty_schema);
                let dataframe = DataFrame::from_eager_with_options(empty, self.is_case_sensitive())
                    .with_logical_schema(physical_schema)
                    .map_err(|error| {
                        EngineError::invalid_input(format!("invalid Connect empty schema: {error}"))
                    })?;
                Ok((dataframe, source_schema))
            }
        }
    }

    /// Infer dtype string from a single JSON value (for schema inference). Returns None for Null.
    fn infer_dtype_from_json_value(v: &JsonValue) -> Option<String> {
        match v {
            JsonValue::Null => None,
            JsonValue::Bool(_) => Some("boolean".to_string()),
            JsonValue::Number(n) => {
                if n.is_i64() {
                    Some("long".to_string())
                } else {
                    Some("double".to_string())
                }
            }
            JsonValue::String(_) => {
                // #1103: Keep string columns as string in createDataFrame so that e.g.
                // filter(date_col > string_col) only casts the string in the predicate and the
                // result row still has the string column as string (PySpark parity). Do not infer
                // date/timestamp from date-like strings; user can pass explicit schema if needed.
                Some("string".to_string())
            }
            JsonValue::Array(arr) => Some(infer_array_type_from_json_array(arr)),
            JsonValue::Object(_) => None, // struct type is inferred in infer_schema_from_json_rows from object keys
        }
    }

    /// Infer a struct type string from a JSON object (for nested struct inference in createDataFrame).
    fn infer_struct_dtype_from_json_object(obj: &serde_json::Map<String, JsonValue>) -> String {
        let mut keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
        keys.sort();
        keys.iter()
            .map(|k| {
                let field_typ = obj
                    .get(*k)
                    .map(|v| match v {
                        JsonValue::Object(inner_obj) => {
                            format!(
                                "struct<{}>",
                                Self::infer_struct_dtype_from_json_object(inner_obj)
                            )
                        }
                        _ => Self::infer_dtype_from_json_value(v)
                            .unwrap_or_else(|| "string".to_string()),
                    })
                    .unwrap_or_else(|| "string".to_string());
                format!("{}:{}", k, field_typ)
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Infer struct type from a column of objects by using the first non-null value per key across all rows.
    /// Fixes E2 when row0 has E2=null and row1 has E2=4 so E2 is inferred as bigint (#339 null_field).
    fn infer_struct_dtype_from_json_rows(
        rows: &[Vec<JsonValue>],
        col_idx: usize,
    ) -> Option<String> {
        let mut all_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
        for row in rows {
            if let Some(JsonValue::Object(obj)) = row.get(col_idx) {
                for k in obj.keys() {
                    all_keys.insert(k.clone());
                }
            }
        }
        let mut key_to_first_non_null: std::collections::HashMap<String, JsonValue> =
            std::collections::HashMap::new();
        for row in rows {
            let Some(JsonValue::Object(obj)) = row.get(col_idx) else {
                continue; // skip null or non-object rows so struct type is inferred from object rows
            };
            for k in &all_keys {
                if key_to_first_non_null.contains_key(k) {
                    continue;
                }
                if let Some(val) = obj.get(k) {
                    if !matches!(val, JsonValue::Null) {
                        key_to_first_non_null.insert(k.clone(), val.clone());
                    }
                }
            }
        }
        let mut keys: Vec<&String> = all_keys.iter().collect();
        keys.sort();
        fn is_int_like(typ: &str) -> bool {
            let t = typ.trim().to_lowercase();
            matches!(
                t.as_str(),
                "bigint" | "int" | "integer" | "long" | "smallint" | "tinyint"
            )
        }
        // #1150: When struct has any int-like field, only expose int-like and nested-struct so
        // getField("E2") yields null for string fields (PySpark parity, test_issue_330_struct_field_alias).
        fn strict_visible(typ: &str) -> bool {
            let t = typ.trim().to_lowercase();
            is_int_like(&t) || t.starts_with("struct<")
        }
        // #1219: When struct has no nested struct fields (e.g. simple flat struct) we want broader
        // visibility so that string/double/float/bool fields are available for getField/dot access.
        // This ensures simple structs like {name: string, age: int} expose both fields, while nested
        // alias scenarios continue to use strict visibility.
        fn extended_visible(typ: &str) -> bool {
            let t = typ.trim().to_lowercase();
            strict_visible(typ)
                || matches!(
                    t.as_str(),
                    "string" | "str" | "varchar" | "double" | "float" | "boolean" | "bool"
                )
        }
        let field_types: Vec<(String, String)> = keys
            .iter()
            .filter_map(|k| {
                let field_typ = key_to_first_non_null.get(*k).map(|val| match val {
                    JsonValue::Object(inner_obj) => {
                        format!(
                            "struct<{}>",
                            Self::infer_struct_dtype_from_json_object(inner_obj)
                        )
                    }
                    _ => Self::infer_dtype_from_json_value(val)
                        .unwrap_or_else(|| "string".to_string()),
                })?;
                Some(((*k).to_string(), field_typ))
            })
            .collect();
        let has_nested_struct = field_types
            .iter()
            .any(|(_, typ)| typ.trim().to_lowercase().starts_with("struct<"));

        // Heuristic: Spark/PySpark's dict-of-dicts inference for synthetic E1/E2/... shaped
        // structs is observed to *not* reliably expose inner string/float/bool fields in some
        // parity scenarios (see tests for issue #330). Keep strict visibility for those shapes,
        // but allow natural-name structs like {"name": "...", "age": 30} (issue #1473) to expose
        // all primitive fields.
        fn is_spark_e_field(name: &str) -> bool {
            let s = name.trim();
            if s.len() < 2 {
                return false;
            }
            let mut chars = s.chars();
            if chars.next() != Some('E') {
                return false;
            }
            chars.all(|c| c.is_ascii_digit())
        }
        let has_int_like = field_types.iter().any(|(_, typ)| is_int_like(typ));
        let has_spark_e_fields = field_types.iter().any(|(k, _)| is_spark_e_field(k));
        let use_strict_visibility = has_nested_struct || (has_int_like && has_spark_e_fields);
        let inner = field_types
            .into_iter()
            .filter_map(|(k, field_typ)| {
                let ok = if use_strict_visibility {
                    strict_visible(&field_typ)
                } else {
                    extended_visible(&field_typ)
                };
                if !ok {
                    return None;
                }
                Some(format!("{}:{}", k.as_str(), field_typ))
            })
            .collect::<Vec<_>>()
            .join(",");
        if inner.is_empty() {
            return None;
        }
        Some(format!("struct<{}>", inner))
    }

    /// Infer schema (name, dtype_str) from JSON rows by scanning values per column.
    /// Used by createDataFrame(data, schema=None) when schema is omitted or only column names given.
    /// When the first non-null value is a JSON object, infers struct (PR13 / struct parity).
    /// #1084: If any value in a column infers as string (e.g. "2024-01-2" with single-digit day),
    /// use string for the column so date-like strings stay string and comparisons remain lexicographic.
    pub fn infer_schema_from_json_rows(
        rows: &[Vec<JsonValue>],
        names: &[String],
    ) -> Vec<(String, String)> {
        if names.is_empty() {
            return Vec::new();
        }
        let mut schema: Vec<(String, String)> = names
            .iter()
            .map(|n| (n.clone(), "string".to_string()))
            .collect();
        for (col_idx, (_, dtype_str)) in schema.iter_mut().enumerate() {
            if let Some(ty) = Self::infer_struct_dtype_from_json_rows(rows, col_idx) {
                for row in rows {
                    if row
                        .get(col_idx)
                        .is_some_and(|v| matches!(v, JsonValue::Object(_)))
                    {
                        *dtype_str = ty.clone();
                        break;
                    }
                }
                continue;
            }
            let mut has_plain_string = false;
            let mut merged = String::new();
            for row in rows {
                let v = row.get(col_idx).unwrap_or(&JsonValue::Null);
                if matches!(v, JsonValue::Null) {
                    continue;
                }
                if let JsonValue::String(_) = v {
                    has_plain_string = true;
                    break;
                }
                if let Some(dtype) = Self::infer_dtype_from_json_value(v) {
                    merged = if merged.is_empty() {
                        dtype
                    } else {
                        merge_spark_type_names(&merged, &dtype)
                    };
                }
            }
            if has_plain_string {
                *dtype_str = "string".to_string();
            } else if !merged.is_empty() {
                *dtype_str = merged;
            }
        }
        schema
    }

    /// Create a DataFrame from rows and a schema (arbitrary column count and types).
    ///
    /// `rows`: each inner vec is one row; length must match schema length. Values are JSON-like (i64, f64, string, bool, null, object, array).
    /// `schema`: list of (column_name, dtype_string), e.g. `[("id", "bigint"), ("name", "string")]`.
    /// `verify_schema`: when true (#420), validate each cell type against schema and return a clear error (e.g. "Row 1: column 'age' expected bigint, got string").
    /// `schema_was_inferred`: when true (Python passed no schema), run Phase 6 validation: all-null column and mixed int/float raise.
    /// Supported dtype strings: bigint, int, long, double, float, string, str, varchar, boolean, bool, date, timestamp, datetime, list, array, array<element_type>, struct<field:type,...>.
    /// When `rows` is empty and `schema` is non-empty, returns an empty DataFrame with that schema (issue #519). Use with `write.format("parquet").saveAsTable(...)` then append; PySpark would fail with "can not infer schema from empty dataset".
    pub fn create_dataframe_from_rows(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
        verify_schema: bool,
        schema_was_inferred: bool,
    ) -> Result<DataFrame, PolarsError> {
        self.create_dataframe_from_rows_impl(rows, schema, verify_schema, schema_was_inferred, true)
    }

    /// Build rows against a caller-supplied schema without the all-string schema inference used
    /// by the Python-compatible `create_dataframe_from_rows` entry point.
    pub(crate) fn create_dataframe_from_rows_explicit_schema(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
        verify_schema: bool,
    ) -> Result<DataFrame, PolarsError> {
        self.create_dataframe_from_rows_impl(rows, schema, verify_schema, false, false)
    }

    fn create_dataframe_from_rows_impl(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
        verify_schema: bool,
        schema_was_inferred: bool,
        allow_schema_inference: bool,
    ) -> Result<DataFrame, PolarsError> {
        // #1345: Empty schema + rows with 0 fields each is valid (PySpark: createDataFrame([{}], StructType([])) -> 1 row, 0 cols).
        // Only fail when a row has more than 0 fields (LENGTH_SHOULD_BE_THE_SAME).
        if schema.is_empty() && !rows.is_empty() {
            for (row_idx, row) in rows.iter().enumerate() {
                if !row.is_empty() {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "create_dataframe_from_rows: LENGTH_SHOULD_BE_THE_SAME. Expected 0 fields, got {} (row index {}).",
                            row.len(), row_idx
                        )
                            .into(),
                    ));
                }
            }
        }
        // #624: When schema is only column names (all "string"), infer from data (PySpark parity).
        // #731, #769, #772: createDataFrame(rows, ["name", "age"]) yields int/double/bool columns.
        let schema_inferred_in_rust = allow_schema_inference
            && !schema.is_empty()
            && !rows.is_empty()
            && schema
                .iter()
                .all(|(_, t)| t.trim().eq_ignore_ascii_case("string"));
        let mut schema = if schema_inferred_in_rust {
            let names: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();
            Self::infer_schema_from_json_rows(&rows, &names)
        } else {
            schema
        };
        // When schema was not all-string (e.g. ID:bigint, StructValue:string), still upgrade any "string"
        // column that has Object in the data to struct (so createDataFrame([{"ID":1,"StructValue":{...}}]) gets struct).
        // #1149: When schema_was_inferred (e.g. names-only from Python), do not upgrade numeric/string
        // so Python's first-col Long and later String stay (PySpark parity). But when Python inferred
        // "string" for a dict column (infer_type_from_py_value has no dict case), upgrade to struct
        // so getField("E1") works (#1216).
        if allow_schema_inference && !rows.is_empty() {
            let names: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();
            let inferred = Self::infer_schema_from_json_rows(&rows, &names);
            for (col_idx, (_, dtype_str)) in schema.iter_mut().enumerate() {
                if dtype_str.trim().eq_ignore_ascii_case("string")
                    && inferred.get(col_idx).map(|(_, t)| t) != Some(&"string".to_string())
                {
                    if let Some((_, inferred_type)) = inferred.get(col_idx) {
                        // Only upgrade when Rust inference says struct/map: avoid overwriting
                        // Python's intentional string for non-dict columns (#1149).
                        let inferred_trimmed = inferred_type.trim();
                        if inferred_trimmed.to_lowercase().starts_with("struct<")
                            || inferred_trimmed.to_lowercase().starts_with("map<")
                        {
                            *dtype_str = inferred_type.clone();
                        }
                    }
                }
            }
        }

        // Phase 6 validation: reject all-null columns and mixed int/float (PySpark parity). Run when schema was inferred (Python or Rust).
        let run_inferred_validation = schema_was_inferred || schema_inferred_in_rust;
        if run_inferred_validation && !rows.is_empty() {
            for (col_idx, (_name, dtype_str)) in schema.iter().enumerate() {
                let mut has_null_only = true;
                let mut has_int = false;
                let mut has_float = false;
                let mut has_non_number = false;
                let mut first_non_number: Option<&JsonValue> = None;
                for row in &rows {
                    let v = row.get(col_idx).unwrap_or(&JsonValue::Null);
                    match v {
                        JsonValue::Null => {}
                        JsonValue::Number(n) => {
                            has_null_only = false;
                            if n.is_i64() {
                                has_int = true;
                            } else {
                                has_float = true;
                            }
                        }
                        JsonValue::String(s) => {
                            has_null_only = false;
                            // Treat textual inf/-inf/nan/NaN/Infinity as numeric (DoubleType)
                            // for validation, so createDataFrame with float("inf") / NaN passes.
                            let lower = s.to_ascii_lowercase();
                            if lower == "inf"
                                || lower == "+inf"
                                || lower == "-inf"
                                || lower == "infinity"
                                || lower == "+infinity"
                                || lower == "-infinity"
                                || lower == "nan"
                            {
                                has_float = true;
                            } else if !has_non_number {
                                has_non_number = true;
                                first_non_number = Some(v);
                            }
                        }
                        _ => {
                            has_null_only = false;
                            if !has_non_number {
                                has_non_number = true;
                                first_non_number = Some(v);
                            }
                        }
                    }
                }
                if has_null_only {
                    return Err(PolarsError::InvalidOperation(
                        "Some of types cannot be determined because the column is all null. Use explicit schema.".into(),
                    ));
                }
                let type_lower = dtype_str.trim().to_lowercase();
                let is_numeric = type_lower.as_str() == "bigint"
                    || type_lower.as_str() == "long"
                    || type_lower.as_str() == "double"
                    || type_lower.as_str() == "float"
                    || type_lower.as_str() == "int"
                    || type_lower.as_str() == "integer";
                if is_numeric && has_int && has_float {
                    return Err(PolarsError::InvalidOperation(
                        "Can not merge type DoubleType and LongType. Use explicit schema or consistent types.".into(),
                    ));
                }
                // PySpark parity: when schema is inferred as numeric but data mixes numeric
                // values with non-numeric (e.g. int and string), raise a TypeError-style
                // message "Can not merge type ..." instead of silently coercing.
                if is_numeric && (has_int || has_float) && has_non_number {
                    let numeric_type =
                        if type_lower.as_str() == "double" || type_lower.as_str() == "float" {
                            "DoubleType"
                        } else {
                            "LongType"
                        };
                    let other_type = first_non_number
                        .map(value_type_name)
                        .unwrap_or("non-numeric");
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "Can not merge type {numeric} and {other}. Use explicit schema or consistent types.",
                            numeric = numeric_type,
                            other = match other_type {
                                "string" => "StringType",
                                "boolean" => "BooleanType",
                                "array" => "ArrayType",
                                "object" => "StructType",
                                "null" => "NullType",
                                other => other,
                            }
                        )
                        .into(),
                    ));
                }
            }
        }

        if schema.is_empty() {
            if rows.is_empty() {
                return Ok(DataFrame::from_eager_with_options(
                    PlDataFrame::new(0, vec![])?,
                    self.is_case_sensitive(),
                ));
            }
            // #1345: PySpark allows createDataFrame([{}], StructType([])) -> 1 row, 0 cols.
            if rows.iter().all(|r| r.is_empty()) {
                return Ok(DataFrame::from_eager_with_options(
                    PlDataFrame::new(rows.len(), vec![])?,
                    self.is_case_sensitive(),
                ));
            }
            return Err(PolarsError::InvalidOperation(
                "create_dataframe_from_rows: schema must not be empty when rows are not empty"
                    .into(),
            ));
        }
        // #1347: Robin-sparkless rejects duplicate field names in schema; PySpark allows them. See PYSPARK_DIFFERENCES.md.
        let mut seen = std::collections::HashSet::new();
        for (name, _) in &schema {
            if !seen.insert(name.clone()) {
                return Err(PolarsError::InvalidOperation(
                    format!(
                        "create_dataframe_from_rows: duplicate column name '{name}' in schema (robin-sparkless rejects duplicate field names; see PYSPARK_DIFFERENCES.md)"
                    )
                    .into(),
                ));
            }
        }
        // #711: PySpark raises when row length differs from schema (LENGTH_SHOULD_BE_THE_SAME).
        let expected_len = schema.len();
        for (row_idx, row) in rows.iter().enumerate() {
            let got = row.len();
            if got != expected_len {
                return Err(PolarsError::InvalidOperation(
                    format!(
                        "create_dataframe_from_rows: LENGTH_SHOULD_BE_THE_SAME. length should be the same. Expected {} fields, got {} (row index {}).",
                        expected_len, got, row_idx
                    )
                    .into(),
                ));
            }
        }
        // #420: When verify_schema is true, validate each cell type before building.
        // #1149: When schema was inferred (e.g. names-only), skip verification so string-in-numeric
        // columns are coerced to null during build (PySpark parity for createDataFrame(data, [names])).
        if verify_schema && !schema_was_inferred {
            for (row_idx, row) in rows.iter().enumerate() {
                for (col_idx, (name, type_str)) in schema.iter().enumerate() {
                    let v = row.get(col_idx).unwrap_or(&JsonValue::Null);
                    if let Err(msg) = verify_json_value_for_type(v, type_str) {
                        return Err(PolarsError::InvalidOperation(
                            format!(
                                "Row {}: column '{}' expected type {} but {}",
                                row_idx, name, type_str, msg
                            )
                            .into(),
                        ));
                    }
                }
            }
        }
        use chrono::{NaiveDate, NaiveDateTime};

        let mut cols: Vec<Series> = Vec::with_capacity(schema.len());

        for (col_idx, (name, type_str)) in schema.iter().enumerate() {
            let type_lower = type_str.trim().to_lowercase();
            let s = match type_lower.as_str() {
                "int32" => {
                    let vals: Vec<Option<i32>> = rows
                        .iter()
                        .map(
                            |row| match row.get(col_idx).cloned().unwrap_or(JsonValue::Null) {
                                JsonValue::Number(n) => {
                                    n.as_i64().and_then(|i| i32::try_from(i).ok())
                                }
                                JsonValue::String(s) => {
                                    decimal_transport_value(&s).parse::<i32>().ok()
                                }
                                JsonValue::Null => None,
                                _ => None,
                            },
                        )
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "smallint" | "short" => {
                    let vals: Vec<Option<i16>> = rows
                        .iter()
                        .map(|row| {
                            let value = row.get(col_idx).unwrap_or(&JsonValue::Null);
                            match value {
                                JsonValue::Number(number) => {
                                    number.as_i64().and_then(|value| i16::try_from(value).ok())
                                }
                                JsonValue::String(value) => {
                                    decimal_transport_value(value).parse::<i16>().ok()
                                }
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "int" | "integer" | "bigint" | "long" | "tinyint" => {
                    // #1265: values may exceed i64 (e.g. UDF returns 10^18); use Float64 when any value doesn't fit.
                    let mut needs_f64 = false;
                    let i64_vals: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Number(ref n) => {
                                    if let Some(i) = n.as_i64() {
                                        Some(i)
                                    } else {
                                        needs_f64 = true;
                                        n.as_f64().map(|f| f as i64) // placeholder; we'll use f64 column
                                    }
                                }
                                JsonValue::String(s) => {
                                    decimal_transport_value(&s).parse::<i64>().ok()
                                }
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    if needs_f64 {
                        let f64_vals: Vec<Option<f64>> = rows
                            .iter()
                            .map(|row| {
                                let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                                match v {
                                    JsonValue::Number(n) => {
                                        n.as_i64().map(|i| i as f64).or_else(|| n.as_f64())
                                    }
                                    JsonValue::String(s) => decimal_transport_value(&s)
                                        .parse::<i64>()
                                        .ok()
                                        .map(|i| i as f64),
                                    JsonValue::Null => None,
                                    _ => None,
                                }
                            })
                            .collect();
                        Series::new(name.as_str().into(), f64_vals)
                    } else {
                        Series::new(name.as_str().into(), i64_vals)
                    }
                }
                "double" | "double_precision" => {
                    let vals: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Number(n) => n.as_f64(),
                                JsonValue::String(s) => {
                                    decimal_transport_value(&s).parse::<f64>().ok()
                                }
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "float" => {
                    let vals: Vec<Option<f32>> = rows
                        .iter()
                        .map(|row| {
                            let value = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match value {
                                JsonValue::Number(number) => {
                                    number.as_f64().map(|value| value as f32)
                                }
                                JsonValue::String(value) => {
                                    decimal_transport_value(&value).parse::<f32>().ok()
                                }
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                _ if is_decimal_type_str(&type_lower) => {
                    let (precision, scale) = parse_decimal_precision_scale(&type_lower)
                        .ok_or_else(|| {
                            PolarsError::ComputeError(
                                format!("invalid decimal type {type_lower}").into(),
                            )
                        })?;
                    let vals: Vec<Option<i128>> = rows
                        .iter()
                        .map(|row| {
                            json_value_to_decimal_scaled(
                                row.get(col_idx).unwrap_or(&JsonValue::Null),
                                precision,
                                scale,
                            )
                        })
                        .collect::<Result<_, PolarsError>>()?;
                    Series::new(name.as_str().into(), vals)
                        .into_decimal(precision, scale)?
                        .into_series()
                }
                "string" | "str" | "varchar" => {
                    let vals: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::String(s) => {
                                    Some(decimal_transport_value(&s).to_string())
                                }
                                JsonValue::Null => None,
                                other => Some(other.to_string()),
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "boolean" | "bool" => {
                    let vals: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::Bool(b) => Some(b),
                                JsonValue::String(s) => {
                                    if s.eq_ignore_ascii_case("true") {
                                        Some(true)
                                    } else if s.eq_ignore_ascii_case("false") {
                                        Some(false)
                                    } else {
                                        None
                                    }
                                }
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    Series::new(name.as_str().into(), vals)
                }
                "date" => {
                    let epoch = date_utils::epoch_naive_date();
                    let vals: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| {
                            let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                            match v {
                                JsonValue::String(s) => NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                                    .ok()
                                    .map(|d| (d - epoch).num_days() as i32),
                                JsonValue::Object(obj) => parse_date_from_json_object(&obj),
                                JsonValue::Null => None,
                                _ => None,
                            }
                        })
                        .collect();
                    let series = Series::new(name.as_str().into(), vals);
                    series
                        .cast(&DataType::Date)
                        .map_err(|e| PolarsError::ComputeError(format!("date cast: {e}").into()))?
                }
                "timestamp" | "datetime" | "timestamp_ntz" => {
                    let vals: Vec<Option<i64>> =
                        rows.iter()
                            .map(|row| {
                                let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                                match v {
                                    JsonValue::String(s) => {
                                        let parsed = NaiveDateTime::parse_from_str(
                                            &s,
                                            "%Y-%m-%dT%H:%M:%S%.f",
                                        )
                                        .map_err(|e| {
                                            PolarsError::ComputeError(e.to_string().into())
                                        })
                                        .or_else(|_| {
                                            NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S")
                                                .map_err(|e| {
                                                    PolarsError::ComputeError(e.to_string().into())
                                                })
                                        })
                                        .or_else(|_| {
                                            NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                                                .map_err(|e| {
                                                    PolarsError::ComputeError(e.to_string().into())
                                                })
                                        })
                                        .or_else(|_| {
                                            NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                                                .map_err(|e| {
                                                    PolarsError::ComputeError(e.to_string().into())
                                                })
                                                .and_then(|d| {
                                                    d.and_hms_opt(0, 0, 0).ok_or_else(|| {
                                                        PolarsError::ComputeError(
                                                            "date to datetime (0:0:0)".into(),
                                                        )
                                                    })
                                                })
                                        });
                                        parsed.ok().map(|dt| dt.and_utc().timestamp_micros())
                                    }
                                    JsonValue::Object(obj) => parse_datetime_from_json_object(&obj),
                                    JsonValue::Number(n) => n.as_i64(),
                                    JsonValue::Null => None,
                                    _ => None,
                                }
                            })
                            .collect();
                    let series = Series::new(name.as_str().into(), vals);
                    series.cast(&timestamp_data_type(type_str)).map_err(|e| {
                        PolarsError::ComputeError(format!("datetime cast: {e}").into())
                    })?
                }
                "list" | "array" => {
                    // PySpark parity: ("col", "list") or ("col", "array"); infer element type from first non-null array.
                    let (elem_type, inner_dtype) = infer_list_element_type(&rows, col_idx)
                        .unwrap_or(("bigint".to_string(), DataType::Int64));
                    let n = rows.len();
                    let mut builder = get_list_builder(&inner_dtype, 64, n, name.as_str().into());
                    for row in rows.iter() {
                        let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                        if let JsonValue::Null = &v {
                            builder.append_null();
                        } else if let Some(arr) = json_value_to_array(&v) {
                            // #625: Array, Object with "0","1",..., or string that parses as JSON array (PySpark list parity).
                            let elem_series: Vec<Series> = arr
                                .iter()
                                .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                                .collect::<Result<Vec<_>, _>>()?;
                            let vals: Vec<_> =
                                elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                            let s = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &vals,
                                &inner_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("array elem: {e}").into())
                            })?;
                            builder.append_series(&s)?;
                        } else {
                            // #611: PySpark accepts single value as one-element list.
                            let single_arr = [v];
                            let elem_series: Vec<Series> = single_arr
                                .iter()
                                .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                                .collect::<Result<Vec<_>, _>>()?;
                            let vals: Vec<_> =
                                elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                            let s = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &vals,
                                &inner_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("array elem: {e}").into())
                            })?;
                            builder.append_series(&s)?;
                        }
                    }
                    builder.finish().into_series()
                }
                _ if parse_array_element_type(type_str).is_some() => {
                    let elem_type = parse_array_element_type(type_str).unwrap_or_else(|| {
                        unreachable!("guard above ensures parse_array_element_type returned Some")
                    });
                    // Special case: array<map<k,v>> – inner dtype is List(Struct{key,value})
                    // so each array element is its own map (list-of-{key,value} pairs).
                    let inner_dtype = if let Some((key_type, value_type)) =
                        parse_map_key_value_types(&elem_type)
                    {
                        let key_dtype = json_type_str_to_polars(key_type.trim()).ok_or_else(|| {
                            PolarsError::ComputeError(
                                format!(
                                    "create_dataframe_from_rows: array map key type '{key_type}' not supported"
                                )
                                .into(),
                            )
                        })?;
                        let value_dtype =
                            json_type_str_to_polars(value_type.trim()).ok_or_else(|| {
                                PolarsError::ComputeError(
                                    format!(
                                        "create_dataframe_from_rows: array map value type '{value_type}' not supported"
                                    )
                                    .into(),
                                )
                            })?;
                        DataType::List(Box::new(DataType::Struct(vec![
                            Field::new("key".into(), key_dtype),
                            Field::new("value".into(), value_dtype),
                        ])))
                    } else {
                        json_type_str_to_polars(&elem_type).ok_or_else(|| {
                            PolarsError::ComputeError(
                                format!(
                                    "create_dataframe_from_rows: array element type '{elem_type}' not supported"
                                )
                                .into(),
                            )
                        })?
                    };
                    let n = rows.len();
                    let mut builder = get_list_builder(&inner_dtype, 64, n, name.as_str().into());
                    for row in rows.iter() {
                        let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                        if let JsonValue::Null = &v {
                            builder.append_null();
                        } else if let Some(arr) = json_value_to_array(&v) {
                            // #625: Array, Object with "0","1",..., or string that parses as JSON array (PySpark list parity).
                            // #710/PR-G: Nested array: one list series per element, then vals + from_any_values.
                            let elem_series: Vec<Series> =
                                if parse_array_element_type(&elem_type).is_some() {
                                    arr.iter()
                                        .map(|e| {
                                            json_values_to_series(
                                                &[Some(e.clone())],
                                                &elem_type,
                                                "elem",
                                            )
                                        })
                                        .collect::<Result<Vec<_>, _>>()?
                                } else {
                                    arr.iter()
                                        .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                                        .collect::<Result<Vec<_>, _>>()?
                                };
                            let vals: Vec<_> =
                                elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                            let s = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &vals,
                                &inner_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("array elem: {e}").into())
                            })?;
                            builder.append_series(&s)?;
                        } else {
                            // #611: PySpark accepts single value as one-element list.
                            let single_arr = [v];
                            let elem_series: Vec<Series> =
                                if parse_array_element_type(&elem_type).is_some() {
                                    single_arr
                                        .iter()
                                        .map(|e| {
                                            json_values_to_series(
                                                &[Some(e.clone())],
                                                &elem_type,
                                                "elem",
                                            )
                                        })
                                        .collect::<Result<Vec<_>, _>>()?
                                } else {
                                    single_arr
                                        .iter()
                                        .map(|e| json_value_to_series_single(e, &elem_type, "elem"))
                                        .collect::<Result<Vec<_>, _>>()?
                                };
                            let vals: Vec<_> =
                                elem_series.iter().filter_map(|s| s.get(0).ok()).collect();
                            let s = Series::from_any_values_and_dtype(
                                PlSmallStr::EMPTY,
                                &vals,
                                &inner_dtype,
                                false,
                            )
                            .map_err(|e| {
                                PolarsError::ComputeError(format!("array elem: {e}").into())
                            })?;
                            builder.append_series(&s)?;
                        }
                    }
                    builder.finish().into_series()
                }
                _ if parse_map_key_value_types(type_str).is_some() => {
                    let (key_type, value_type) = parse_map_key_value_types(type_str)
                        .unwrap_or_else(|| unreachable!("guard ensures Some"));
                    let key_dtype = json_type_str_to_polars(&key_type).ok_or_else(|| {
                        PolarsError::ComputeError(
                            format!(
                                "create_dataframe_from_rows: map key type '{key_type}' not supported"
                            )
                            .into(),
                        )
                    })?;
                    let value_dtype = json_type_str_to_polars(&value_type).ok_or_else(|| {
                        PolarsError::ComputeError(
                            format!(
                                "create_dataframe_from_rows: map value type '{value_type}' not supported"
                            )
                            .into(),
                        )
                    })?;
                    let struct_dtype = DataType::Struct(vec![
                        Field::new("key".into(), key_dtype.clone()),
                        Field::new("value".into(), value_dtype.clone()),
                    ]);
                    let n = rows.len();
                    let mut builder = get_list_builder(&struct_dtype, 64, n, name.as_str().into());
                    for row in rows.iter() {
                        let v = row.get(col_idx).cloned().unwrap_or(JsonValue::Null);
                        if matches!(v, JsonValue::Null) {
                            builder.append_null();
                        } else if let Some(obj) = v.as_object() {
                            let st = json_object_to_map_struct_series(
                                obj,
                                &key_type,
                                &value_type,
                                &key_dtype,
                                &value_dtype,
                                name,
                            )?;
                            builder.append_series(&st)?;
                        } else if let JsonValue::String(s) = &v {
                            // #691/PR-F: Map value as string (e.g. Python dict repr "{'k': 'v'}"). #971: also double-encoded or Python repr.
                            let parsed = string_to_json_object(s);
                            if let Some(parsed) = parsed {
                                let st = json_object_to_map_struct_series(
                                    &parsed,
                                    &key_type,
                                    &value_type,
                                    &key_dtype,
                                    &value_dtype,
                                    name,
                                )?;
                                builder.append_series(&st)?;
                            } else {
                                return Err(PolarsError::ComputeError(
                                    format!(
                                        "create_dataframe_from_rows: map column '{name}' expects JSON object (dict), got {:?}",
                                        v
                                    )
                                    .into(),
                                ));
                            }
                        } else {
                            return Err(PolarsError::ComputeError(
                                format!(
                                    "create_dataframe_from_rows: map column '{name}' expects JSON object (dict), got {:?}",
                                    v
                                )
                                .into(),
                            ));
                        }
                    }
                    builder.finish().into_series()
                }
                _ if parse_struct_fields(&type_lower).is_some() => {
                    let values: Vec<Option<JsonValue>> =
                        rows.iter().map(|row| row.get(col_idx).cloned()).collect();
                    json_values_to_series(&values, type_str, name)?
                }
                _ if type_lower == "binary" => {
                    let values: Vec<Option<JsonValue>> =
                        rows.iter().map(|row| row.get(col_idx).cloned()).collect();
                    json_values_to_series(&values, &type_lower, name)?
                }
                _ => {
                    return Err(PolarsError::ComputeError(
                        format!(
                            "create_dataframe_from_rows: unsupported type '{type_str}' for column '{name}'"
                        )
                        .into(),
                    ));
                }
            };
            cols.push(s);
        }

        let pl_df = PlDataFrame::new_infer_height(cols.iter().map(|s| s.clone().into()).collect())?;
        // Keep eager so schema (including struct types) is available for dotted select (#1076).
        Ok(DataFrame::from_eager_with_options(
            pl_df,
            self.is_case_sensitive(),
        ))
    }

    /// #419: Create a DataFrame with a single column named "value" from a list of scalar values (PySpark createDataFrame([1,2,3], "bigint")).
    /// Each element of `values` becomes one row. Use from Python/bindings when schema is a single type string.
    pub fn create_dataframe_from_single_column(
        &self,
        values: Vec<JsonValue>,
        type_str: &str,
    ) -> Result<DataFrame, PolarsError> {
        let schema = vec![("value".to_string(), type_str.to_string())];
        let rows: Vec<Vec<JsonValue>> = values.into_iter().map(|v| vec![v]).collect();
        self.create_dataframe_from_rows(rows, schema, false, false)
    }

    /// Same as [`create_dataframe_from_rows`](Self::create_dataframe_from_rows) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn create_dataframe_from_rows_engine(
        &self,
        rows: Vec<Vec<JsonValue>>,
        schema: Vec<(String, String)>,
        verify_schema: bool,
        schema_was_inferred: bool,
    ) -> Result<DataFrame, EngineError> {
        self.create_dataframe_from_rows(rows, schema, verify_schema, schema_was_inferred)
            .map_err(polars_to_core_error)
    }

    /// Create a DataFrame with a single column `id` (bigint) containing values from start to end (exclusive) with step.
    /// PySpark: spark.range(end) or spark.range(start, end, step).
    ///
    /// - `range(end)` → 0 to end-1, step 1
    /// - `range(start, end)` → start to end-1, step 1
    /// - `range(start, end, step)` → start, start+step, ... up to but not including end
    pub fn range(&self, start: i64, end: i64, step: i64) -> Result<DataFrame, PolarsError> {
        if step == 0 {
            return Err(PolarsError::InvalidOperation(
                "range: step must not be 0".into(),
            ));
        }
        let row_count = range_row_count(start, end, step)?;
        let max_rows = max_range_rows();
        if row_count > max_rows {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "range: would create {row_count} rows, exceeding limit of {max_rows} \
                     (set SPARKLESS_MAX_RANGE_ROWS to override)"
                )
                .into(),
            ));
        }
        let source = Arc::new(RangeScan {
            start,
            step,
            row_count,
        });
        let schema = Arc::new(Schema::from_iter([Field::new(
            "id".into(),
            DataType::Int64,
        )]));
        let lazy = LazyFrame::anonymous_scan(
            source,
            ScanArgsAnonymous {
                schema: Some(schema),
                ..Default::default()
            },
        )?;
        DataFrame::from_lazy_with_options(lazy, self.is_case_sensitive())
            .with_range_cardinality(start, step, row_count)
            .with_logical_schema(StructType::from_logical_fields(vec![StructField::new(
                "id".into(),
                CoreDataType::Long,
                false,
            )]))
    }

    /// Read a CSV file.
    ///
    /// Uses Polars' CSV reader with default options:
    /// - Header row is inferred (default: true)
    /// - Schema is inferred from first 100 rows
    ///
    /// # Example
    /// ```
    /// use robin_sparkless_polars::SparkSession;
    ///
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df_result = spark.read_csv("data.csv");
    /// // Handle the Result as appropriate in your application
    /// ```
    pub fn read_csv(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = confine_io_path(path.as_ref())?;
        if !path.exists() {
            return Err(PolarsError::ComputeError(
                format!("read_csv: file not found: {}", path.display()).into(),
            ));
        }
        let path_display = path.display();
        // Use LazyCsvReader - call finish() to get LazyFrame, then collect
        let pl_path = PlRefPath::try_from_path(&path).map_err(|e| {
            PolarsError::ComputeError(format!("read_csv({path_display}): path: {e}").into())
        })?;
        let lf = LazyCsvReader::new(pl_path)
            .with_has_header(true)
            .with_infer_schema_length(Some(100))
            .finish()
            .map_err(|e| {
                PolarsError::ComputeError(
                    format!(
                        "read_csv({path_display}): {e} Hint: check that the file exists and is valid CSV."
                    )
                    .into(),
                )
            })?;
        Ok(crate::dataframe::DataFrame::from_lazy_with_options(
            lf,
            self.is_case_sensitive(),
        ))
    }

    /// Same as [`read_csv`](Self::read_csv) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn read_csv_engine(&self, path: impl AsRef<Path>) -> Result<DataFrame, EngineError> {
        self.read_csv(path).map_err(polars_to_core_error)
    }

    /// Read a Parquet file.
    ///
    /// Uses Polars' Parquet reader. Parquet files have embedded schema, so
    /// schema inference is automatic.
    ///
    /// # Example
    /// ```
    /// use robin_sparkless_polars::SparkSession;
    ///
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df_result = spark.read_parquet("data.parquet");
    /// // Handle the Result as appropriate in your application
    /// ```
    pub fn read_parquet(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        let path = confine_io_path(path.as_ref())?;
        self.read_parquet_unchecked(&path)
    }

    /// Read parquet without `SPARKLESS_FILES_BASE` confinement (warehouse-internal use).
    pub(crate) fn read_parquet_unchecked(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        let path = path.as_ref();
        if !path.exists() {
            return Err(PolarsError::ComputeError(
                format!("read_parquet: file not found: {}", path.display()).into(),
            ));
        }
        // Use LazyFrame::scan_parquet
        let pl_path = PlRefPath::try_from_path(path)
            .map_err(|e| PolarsError::ComputeError(format!("read_parquet: path: {e}").into()))?;
        let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?;
        Ok(crate::dataframe::DataFrame::from_lazy_with_options(
            lf,
            self.is_case_sensitive(),
        ))
    }

    /// Same as [`read_parquet`](Self::read_parquet) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn read_parquet_engine(&self, path: impl AsRef<Path>) -> Result<DataFrame, EngineError> {
        self.read_parquet(path).map_err(polars_to_core_error)
    }

    /// Read a JSON file (JSONL format - one JSON object per line).
    ///
    /// Uses Polars' JSONL reader with default options:
    /// - Schema is inferred from first 100 rows
    ///
    /// # Example
    /// ```
    /// use robin_sparkless_polars::SparkSession;
    ///
    /// let spark = SparkSession::builder().app_name("test").get_or_create();
    /// let df_result = spark.read_json("data.json");
    /// // Handle the Result as appropriate in your application
    /// ```
    pub fn read_json(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        use polars::prelude::*;
        use std::num::NonZeroUsize;
        let path = confine_io_path(path.as_ref())?;
        if !path.exists() {
            return Err(PolarsError::ComputeError(
                format!("read_json: file not found: {}", path.display()).into(),
            ));
        }
        // Use LazyJsonLineReader - call finish() to get LazyFrame, then collect
        let pl_path = PlRefPath::try_from_path(&path)
            .map_err(|e| PolarsError::ComputeError(format!("read_json: path: {e}").into()))?;
        let lf = LazyJsonLineReader::new(pl_path)
            .with_infer_schema_length(NonZeroUsize::new(100))
            .finish()?;
        Ok(crate::dataframe::DataFrame::from_lazy_with_options(
            lf,
            self.is_case_sensitive(),
        ))
    }

    /// Same as [`read_json`](Self::read_json) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn read_json_engine(&self, path: impl AsRef<Path>) -> Result<DataFrame, EngineError> {
        self.read_json(path).map_err(polars_to_core_error)
    }

    /// Execute a SQL query (SELECT only). Tables must be registered with `create_or_replace_temp_view`.
    /// Requires the `sql` feature. Supports: SELECT (columns or *), FROM (single table or JOIN),
    /// WHERE (basic predicates), GROUP BY + aggregates, ORDER BY, LIMIT.
    #[cfg(feature = "sql")]
    pub fn sql(&self, query: &str) -> Result<DataFrame, PolarsError> {
        crate::sql::execute_sql(self, query)
    }

    /// Execute SQL while sharing a cancellation signal with eager statement work such as writes.
    #[cfg(feature = "sql")]
    pub fn sql_with_cancellation(
        &self,
        query: &str,
        token: &robin_sparkless_core::CancellationToken,
    ) -> Result<DataFrame, EngineError> {
        crate::sql::execute_sql_with_cancellation(self, query, token)
    }

    /// Execute a SQL query (stub when `sql` feature is disabled).
    #[cfg(not(feature = "sql"))]
    pub fn sql(&self, _query: &str) -> Result<DataFrame, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "SQL queries require the 'sql' feature. Build with --features sql.".into(),
        ))
    }

    /// SQL is unavailable when the `sql` feature is disabled.
    #[cfg(not(feature = "sql"))]
    pub fn sql_with_cancellation(
        &self,
        _query: &str,
        _token: &robin_sparkless_core::CancellationToken,
    ) -> Result<DataFrame, EngineError> {
        Err(EngineError::sql(
            "SQL queries require the 'sql' feature. Build with --features sql.".into(),
        ))
    }

    /// Same as [`table`](Self::table) but returns [`EngineError`]. Use in bindings to avoid Polars.
    pub fn table_engine(&self, name: &str) -> Result<DataFrame, EngineError> {
        self.table(name).map_err(polars_to_core_error)
    }

    /// Returns true if the string looks like a filesystem path (has separators or path exists).
    fn looks_like_path(s: &str) -> bool {
        s.contains('/') || s.contains('\\') || Path::new(s).exists()
    }

    /// Read a Delta table from path (latest version). Internal; use read_delta(name_or_path: &str) for dispatch.
    #[cfg(feature = "delta")]
    pub fn read_delta_path(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        let path = confine_io_path(path.as_ref())?;
        crate::delta::read_delta(path, self.is_case_sensitive())
    }

    /// Read Delta table at path, optional version. Internal; use read_delta_str for dispatch.
    #[cfg(feature = "delta")]
    pub fn read_delta_path_with_version(
        &self,
        path: impl AsRef<Path>,
        version: Option<i64>,
    ) -> Result<DataFrame, PolarsError> {
        let path = confine_io_path(path.as_ref())?;
        crate::delta::read_delta_with_version(path, version, self.is_case_sensitive())
    }

    /// Read a Delta table or in-memory table by name/path. If name_or_path looks like a path, reads from Delta on disk; else resolves as table name (temp view then saved table).
    #[cfg(feature = "delta")]
    pub fn read_delta(&self, name_or_path: &str) -> Result<DataFrame, PolarsError> {
        if !Self::looks_like_path(name_or_path) {
            return self.table(name_or_path);
        }
        let path = name_or_path.strip_prefix("file://").unwrap_or(name_or_path);
        self.read_delta_path(Path::new(path))
    }

    #[cfg(feature = "delta")]
    pub fn read_delta_with_version(
        &self,
        name_or_path: &str,
        version: Option<i64>,
    ) -> Result<DataFrame, PolarsError> {
        if !Self::looks_like_path(name_or_path) {
            if Self::parse_global_temp_name(name_or_path).is_some()
                || self.has_exact_temp_view(name_or_path)
                || self.saved_table_exists(name_or_path)
            {
                return self.table(name_or_path);
            }
            let (resolved, has_provider) = self.resolve_external_table_descriptor(name_or_path)?;
            if let Some((_, _, mut descriptor)) = resolved {
                let validate_schema = version.is_none() || descriptor.version == version;
                if version.is_some() {
                    descriptor.version = version;
                }
                return self.read_external_table_descriptor_with_schema_validation(
                    name_or_path,
                    &descriptor,
                    validate_schema,
                );
            }
            if has_provider {
                return self.table_after_external_miss(name_or_path, true);
            }
            if let Ok(table) = self.table(name_or_path) {
                return Ok(table);
            }
            return self.table(name_or_path);
        }
        if Self::looks_like_path(name_or_path) {
            let path = name_or_path.strip_prefix("file://").unwrap_or(name_or_path);
            return self.read_delta_path_with_version(Path::new(path), version);
        }
        unreachable!("non-path Delta reads are handled above")
    }

    /// Stub when `delta` feature is disabled. Still supports reading by table name.
    #[cfg(not(feature = "delta"))]
    pub fn read_delta(&self, name_or_path: &str) -> Result<DataFrame, PolarsError> {
        if !Self::looks_like_path(name_or_path) {
            return self.table(name_or_path);
        }
        Err(PolarsError::InvalidOperation(
            "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
        ))
    }

    #[cfg(not(feature = "delta"))]
    pub fn read_delta_with_version(
        &self,
        name_or_path: &str,
        version: Option<i64>,
    ) -> Result<DataFrame, PolarsError> {
        let _ = version;
        self.read_delta(name_or_path)
    }

    /// Path-only read_delta (for DataFrameReader.load/format delta). Requires delta feature.
    #[cfg(feature = "delta")]
    pub fn read_delta_from_path(&self, path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        self.read_delta_path(path)
    }

    #[cfg(not(feature = "delta"))]
    pub fn read_delta_from_path(&self, _path: impl AsRef<Path>) -> Result<DataFrame, PolarsError> {
        Err(PolarsError::InvalidOperation(
            "Delta Lake requires the 'delta' feature. Build with --features delta.".into(),
        ))
    }

    /// Stop the session (cleanup resources)
    /// Best-effort cleanup. If any catalog lock is poisoned, a message is emitted to stderr and cleanup may be partial.
    pub fn stop(&self) {
        let views = if let Some(shared) = &self.shared_catalog {
            let mut accounting = shared.view_accounting.lock().ok();
            let snapshots = accounting
                .as_mut()
                .and_then(|accounting| accounting.remove(&self.temp_view_scope_id))
                .unwrap_or_default();
            let frames = match self.catalog.lock() {
                Ok(mut views) => std::mem::take(&mut *views)
                    .into_values()
                    .collect::<Vec<_>>(),
                Err(_) => {
                    eprintln!(
                        "robin-sparkless-polars: catalog lock poisoned, stop() cleanup may be partial"
                    );
                    Vec::new()
                }
            };
            drop(snapshots);
            drop(accounting);
            frames
        } else {
            match self.catalog.lock() {
                Ok(mut m) => {
                    m.clear();
                    Vec::new()
                }
                Err(_) => {
                    eprintln!(
                        "robin-sparkless-polars: catalog lock poisoned, stop() cleanup may be partial"
                    );
                    Vec::new()
                }
            }
        };
        for frame in views {
            self.retain_catalog_version_if_readers(frame, None);
        }
        if self.shared_catalog.is_none() {
            match self.tables.lock() {
                Ok(mut m) => m.clear(),
                Err(_) => eprintln!(
                    "robin-sparkless-polars: tables lock poisoned, stop() cleanup may be partial"
                ),
            }
            match self.databases.lock() {
                Ok(mut s) => s.clear(),
                Err(_) => eprintln!(
                    "robin-sparkless-polars: databases lock poisoned, stop() cleanup may be partial"
                ),
            }
        }
        let _ = self.udf_registry.clear();
        clear_thread_udf_session();
        crate::clear_thread_udf_context();
    }
}

impl SparkSession {
    /// Get a DataFrameReader for reading files
    pub fn read(&self) -> DataFrameReader {
        DataFrameReader::new(SparkSession {
            app_name: self.app_name.clone(),
            master: self.master.clone(),
            config: self.config.clone(),
            runtime_config: self.runtime_config.clone(),
            catalog: self.catalog.clone(),
            tables: self.tables.clone(),
            databases: self.databases.clone(),
            udf_registry: self.udf_registry.clone(),
            current_database: self.current_database.clone(),
            cached_tables: self.cached_tables.clone(),
            external_catalog: self.external_catalog.clone(),
            shared_catalog: self.shared_catalog.clone(),
            temp_view_scope_id: self.temp_view_scope_id,
            execution_identity: self.execution_identity.clone(),
        })
    }
}

/// Compare names and data types while ignoring nullability. The current Polars schema adapter
/// reports fields as nullable because Polars does not retain Delta's nullability metadata.
fn external_schemas_match(
    expected: &StructType,
    actual: &StructType,
    logical_map_paths: &[String],
) -> bool {
    let expected_fields = expected.logical_fields();
    let actual_fields = actual.logical_fields();
    expected_fields.len() == actual_fields.len()
        && expected_fields
            .iter()
            .zip(actual_fields)
            .all(|(expected, actual)| {
                expected.name == actual.name
                    && external_data_types_match(
                        &expected.data_type,
                        &actual.data_type,
                        &format!(
                            "\"{}\"",
                            expected.name.replace('\\', "\\\\").replace('"', "\\\"")
                        ),
                        logical_map_paths,
                    )
            })
}

fn external_data_types_match(
    expected: &CoreDataType,
    actual: &CoreDataType,
    path: &str,
    logical_map_paths: &[String],
) -> bool {
    use CoreDataType::*;

    match (expected, actual) {
        (String, String)
        | (Integer, Integer)
        | (Short, Short)
        | (Long, Long)
        | (Double, Double)
        | (Boolean, Boolean)
        | (Date, Date)
        | (Timestamp, Timestamp)
        | (Binary, Binary) => true,
        (Decimal(expected_precision, expected_scale), Decimal(actual_precision, actual_scale)) => {
            expected_precision == actual_precision && expected_scale == actual_scale
        }
        (Array(expected), Array(actual)) => {
            external_data_types_match(expected, actual, &format!("{path}[]"), logical_map_paths)
        }
        (Map(expected_key, expected_value), Map(actual_key, actual_value)) => {
            external_data_types_match(expected_key, actual_key, path, logical_map_paths)
                && external_data_types_match(expected_value, actual_value, path, logical_map_paths)
        }
        (Map(expected_key, expected_value), Array(actual_entries))
            if logical_map_paths.iter().any(|p| p == path) =>
        {
            let LogicalStruct(actual_fields) = actual_entries.as_ref() else {
                return false;
            };
            actual_fields.len() == 2
                && actual_fields[0].name == "key"
                && actual_fields[1].name == "value"
                && external_data_types_match(
                    expected_key,
                    &actual_fields[0].data_type,
                    &format!("{path}{{key}}"),
                    logical_map_paths,
                )
                && external_data_types_match(
                    expected_value,
                    &actual_fields[1].data_type,
                    &format!("{path}{{value}}"),
                    logical_map_paths,
                )
        }
        (LogicalStruct(expected_fields), LogicalStruct(actual_fields)) => {
            expected_fields.len() == actual_fields.len()
                && expected_fields
                    .iter()
                    .zip(actual_fields)
                    .all(|(expected, actual)| {
                        expected.name == actual.name
                            && external_data_types_match(
                                &expected.data_type,
                                &actual.data_type,
                                &format!(
                                    "{path}.\"{}\"",
                                    expected.name.replace('\\', "\\\\").replace('"', "\\\"")
                                ),
                                logical_map_paths,
                            )
                    })
        }
        _ => false,
    }
}

impl Default for SparkSession {
    fn default() -> Self {
        Self::builder().get_or_create()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "delta")]
    use robin_sparkless_core::engine::{CatalogResolver, ExternalTableSaveMode};
    use serde_json::json;

    #[test]
    fn explicit_double_schema_accepts_non_finite_float_transport_values() {
        for value in ["nan", "NaN", "inf", "+Infinity", "-inf"] {
            assert!(
                verify_json_value_for_type(&JsonValue::String(value.into()), "double").is_ok(),
                "expected {value:?} to be accepted as a transported Python float"
            );
        }
        assert!(verify_json_value_for_type(&json!("not a number"), "double").is_err());
    }

    #[test]
    fn timestamp_verification_accepts_python_datetime_microseconds() {
        let value = json!("2026-10-24 07:38:40.237647");
        assert!(verify_json_value_for_type(&value, "timestamp").is_ok());
    }

    #[test]
    fn explicit_float_schema_builds_float32_series() {
        let session = SparkSession::builder()
            .app_name("explicit-float32-schema")
            .get_or_create();
        let frame = session
            .create_dataframe_from_rows(
                vec![vec![json!(1.25)]],
                vec![("score".into(), "float".into())],
                false,
                false,
            )
            .unwrap();

        assert_eq!(frame.get_column_dtype("score"), Some(DataType::Float32));
        let materialized = frame.collect().unwrap();
        assert_eq!(
            materialized.column("score").unwrap().dtype(),
            &DataType::Float32
        );
    }

    fn catalog_test_frame() -> DataFrame {
        let values = vec!["x".repeat(128); 64];
        let physical = PlDataFrame::new(
            values.len(),
            vec![Series::new("payload".into(), values).into()],
        )
        .unwrap();
        DataFrame::from_eager_with_options(physical, false)
    }

    fn catalog_test_session_with_limits(
        max_catalog_bytes: usize,
        max_table_bytes: usize,
    ) -> SparkSession {
        let mut config = HashMap::new();
        config.insert(
            "sparkless.server.max_catalog_bytes".to_string(),
            max_catalog_bytes.to_string(),
        );
        config.insert(
            "sparkless.server.max_table_bytes".to_string(),
            max_table_bytes.to_string(),
        );
        SparkSession::new(Some("catalog-accounting-test".into()), None, config)
            .with_shared_catalog(SharedCatalog::new())
    }

    fn catalog_test_session(max_catalog_bytes: usize) -> SparkSession {
        catalog_test_session_with_limits(max_catalog_bytes, usize::MAX)
    }

    /// Manual resource witness for staged and snapshot-pinned table versions.
    /// Run with SPARKLESS_CATALOG_BENCH_ROWS set for separate increasing-cardinality processes.
    #[test]
    #[ignore = "manual V17 retained/staged catalog memory measurement"]
    fn shared_catalog_retention_and_staging_memory_measurement() {
        let rows = std::env::var("SPARKLESS_CATALOG_BENCH_ROWS")
            .expect("set SPARKLESS_CATALOG_BENCH_ROWS")
            .parse::<usize>()
            .expect("SPARKLESS_CATALOG_BENCH_ROWS must be a positive integer");
        assert!(rows > 0);
        let row_count = i64::try_from(rows).expect("row count must fit i64");
        let make_frame = |start: i64| {
            let end = start
                .checked_add(row_count)
                .expect("benchmark range end must fit i64");
            let values = (start..end).collect::<Vec<_>>();
            let physical =
                PlDataFrame::new_infer_height(vec![Series::new("id".into(), values).into()])
                    .unwrap();
            DataFrame::from_polars(physical)
        };

        let construction_started = std::time::Instant::now();
        let original = make_frame(0);
        let candidate = make_frame(row_count);
        let construction_micros = construction_started.elapsed().as_micros();
        let original_bytes = deduplicated_catalog_bytes(std::slice::from_ref(&original)).unwrap();
        let candidate_bytes = deduplicated_catalog_bytes(std::slice::from_ref(&candidate)).unwrap();

        let session = catalog_test_session(usize::MAX);
        session.register_table("phase1_memory", original.clone());
        let read_lease = session.begin_catalog_read_snapshot().unwrap().unwrap();
        let resolved_reader = session.table("phase1_memory").unwrap();

        let staging_started = std::time::Instant::now();
        let staged_accounted_bytes =
            deduplicated_catalog_bytes(&[original.clone(), candidate.clone()]).unwrap();
        {
            let _mutation = session.shared_catalog_mutation_guard().unwrap();
            let _writer = session.shared_catalog_guard().unwrap();
            session
                .check_catalog_table_growth("phase1_memory", &candidate)
                .unwrap();
            session.register_table_during_sql("phase1_memory", candidate.clone());
        }
        let staging_micros = staging_started.elapsed().as_micros();
        let retained_pinned_bytes = session.catalog_auxiliary_bytes().unwrap();
        assert_eq!(retained_pinned_bytes, original_bytes);
        let read_started = std::time::Instant::now();
        let read = resolved_reader.collect().unwrap();
        let read_micros = read_started.elapsed().as_micros();
        let old_values = read.column("id").unwrap().i64().unwrap();
        assert_eq!(old_values.len(), rows);
        assert_eq!(old_values.get(0), Some(0));
        assert_eq!(old_values.get(rows - 1), Some(row_count - 1));
        let current = session.table("phase1_memory").unwrap().collect().unwrap();
        let current_values = current.column("id").unwrap().i64().unwrap();
        assert_eq!(current_values.get(0), Some(row_count));
        assert_eq!(current_values.get(rows - 1), Some(row_count * 2 - 1));

        println!(
            "catalog_memory rows={rows} strategy=eager_i64_table_replacement \
             construction_us={construction_micros} original_bytes={original_bytes} \
             candidate_bytes={candidate_bytes} staged_accounted_bytes={staged_accounted_bytes} \
             staging_us={staging_micros} retained_pinned_bytes={retained_pinned_bytes} \
             read_us={read_micros} read_rows={rows} read_bytes={original_bytes}",
        );

        drop(read_lease);
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), 0);
    }

    #[test]
    fn shared_catalog_cancelled_insert_before_execution_has_no_effect() {
        let session = SparkSession::new(Some("cancelled-insert-test".into()), None, HashMap::new())
            .with_shared_catalog(SharedCatalog::new());
        session.register_database("default");
        session
            .sql("CREATE TABLE default.cancel_before_publish (id BIGINT)")
            .unwrap();

        let token = robin_sparkless_core::CancellationToken::new();
        token.cancel();
        let error = match session.sql_with_cancellation(
            "INSERT INTO default.cancel_before_publish VALUES (1)",
            &token,
        ) {
            Err(error) => error,
            Ok(_) => panic!("pre-cancelled insert unexpectedly returned a result"),
        };
        assert_eq!(
            error.category(),
            robin_sparkless_core::ErrorCategory::Cancelled,
            "{error}"
        );
        assert_eq!(
            session
                .table("default.cancel_before_publish")
                .unwrap()
                .count()
                .unwrap(),
            0
        );
    }

    #[test]
    fn shared_catalog_database_limit_includes_default_without_losing_last_slot() {
        let mut config = HashMap::new();
        config.insert(
            "sparkless.server.max_databases".to_string(),
            "2".to_string(),
        );
        let session = SparkSession::new(Some("database-limit-test".into()), None, config)
            .with_shared_catalog(SharedCatalog::new());

        assert!(session.database_exists("default"));
        assert!(session.check_database_capacity().is_ok());
        session.register_database("last_slot");
        assert_eq!(session.databases.lock().unwrap().len(), 1);

        let error = session.check_database_capacity().unwrap_err();
        assert!(error.to_string().contains("CatalogCapacityExceeded"));
        assert!(!session.database_exists("next_database"));
    }

    #[test]
    fn shared_catalog_saved_table_limit_rejects_new_tables_but_allows_replacement() {
        let mut config = HashMap::new();
        config.insert("sparkless.server.max_tables".to_string(), "1".to_string());
        config.insert(
            "sparkless.server.max_catalog_bytes".to_string(),
            usize::MAX.to_string(),
        );
        config.insert(
            "sparkless.server.max_table_bytes".to_string(),
            usize::MAX.to_string(),
        );
        let session = SparkSession::new(Some("table-limit-test".into()), None, config)
            .with_shared_catalog(SharedCatalog::new());
        let first = catalog_test_frame();

        {
            let _mutation = session.shared_catalog_mutation_guard().unwrap();
            let _writer = session.shared_catalog_guard().unwrap();
            session.check_catalog_table_growth("first", &first).unwrap();
            session.register_table_during_sql("first", first.clone());

            let error = session
                .check_catalog_table_growth("second", &catalog_test_frame())
                .unwrap_err();
            assert!(error.to_string().contains("CatalogCapacityExceeded"));
            assert_eq!(session.list_table_names(), vec!["default.first"]);

            let replacement = catalog_test_frame();
            session
                .check_catalog_table_growth("first", &replacement)
                .unwrap();
            session.register_table_during_sql("first", replacement);
            assert_eq!(session.list_table_names(), vec!["default.first"]);
        }
    }

    #[test]
    fn shared_catalog_per_table_byte_limit_accepts_exact_boundary_and_rejects_before_publish() {
        let candidate = catalog_test_frame();
        let candidate_bytes = candidate.estimated_size().unwrap();
        let over_limit = catalog_test_session_with_limits(usize::MAX, candidate_bytes - 1);
        {
            let _mutation = over_limit.shared_catalog_mutation_guard().unwrap();
            let _writer = over_limit.shared_catalog_guard().unwrap();
            let error = over_limit
                .check_catalog_table_growth("oversized", &candidate)
                .unwrap_err();
            assert!(error.to_string().contains("CatalogCapacityExceeded"));
            assert!(error.to_string().contains("staged table"));
            assert!(!over_limit.saved_table_exists("oversized"));
        }

        let exact_limit = catalog_test_session_with_limits(usize::MAX, candidate_bytes);
        {
            let _mutation = exact_limit.shared_catalog_mutation_guard().unwrap();
            let _writer = exact_limit.shared_catalog_guard().unwrap();
            exact_limit
                .check_catalog_table_growth("exact", &candidate)
                .unwrap();
            exact_limit.register_table_during_sql("exact", candidate);
            assert!(exact_limit.saved_table_exists("exact"));
        }
    }

    #[test]
    fn shared_catalog_temp_view_limit_is_per_session_and_allows_replacement() {
        let mut config = HashMap::new();
        config.insert(
            "sparkless.server.max_temp_views".to_string(),
            "1".to_string(),
        );
        config.insert(
            "sparkless.server.max_catalog_bytes".to_string(),
            usize::MAX.to_string(),
        );
        let session = SparkSession::new(Some("temp-view-limit-test".into()), None, config)
            .with_shared_catalog(SharedCatalog::new());
        let sibling = session.new_session();

        {
            let _mutation = session.shared_catalog_mutation_guard().unwrap();
            let _writer = session.shared_catalog_guard().unwrap();
            session
                .register_temp_view_during_sql("first", catalog_test_frame())
                .unwrap();
            let error = session
                .register_temp_view_during_sql("second", catalog_test_frame())
                .unwrap_err();
            assert!(error.to_string().contains("CatalogCapacityExceeded"));

            // Replacing an existing binding consumes no additional view slot.
            session
                .register_temp_view_during_sql("first", catalog_test_frame())
                .unwrap();
        }

        // The sibling shares saved catalog data, but its temporary-view quota is independent.
        let _mutation = sibling.shared_catalog_mutation_guard().unwrap();
        let _writer = sibling.shared_catalog_guard().unwrap();
        sibling
            .register_temp_view_during_sql("sibling_view", catalog_test_frame())
            .unwrap();
        assert_eq!(session.list_temp_view_names(), vec!["first"]);
        assert_eq!(sibling.list_temp_view_names(), vec!["sibling_view"]);
    }

    #[test]
    fn shared_catalog_temp_view_limit_serializes_competing_publications() {
        let mut config = HashMap::new();
        config.insert(
            "sparkless.server.max_temp_views".to_string(),
            "1".to_string(),
        );
        config.insert(
            "sparkless.server.max_catalog_bytes".to_string(),
            usize::MAX.to_string(),
        );
        let session = SparkSession::new(Some("temp-view-race-test".into()), None, config)
            .with_shared_catalog(SharedCatalog::new());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let joins = ["first", "second"].map(|name| {
            let session = session.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let candidate = catalog_test_frame();
                barrier.wait();
                let _mutation = session.shared_catalog_mutation_guard().unwrap();
                let _writer = session.shared_catalog_guard().unwrap();
                session
                    .register_temp_view_during_sql(name, candidate)
                    .is_ok()
            })
        });
        barrier.wait();
        let accepted = joins
            .into_iter()
            .map(|join| join.join().unwrap())
            .filter(|accepted| *accepted)
            .count();
        assert_eq!(accepted, 1);
        assert_eq!(session.list_temp_view_names().len(), 1);
    }

    #[test]
    fn shared_catalog_reader_keeps_resolved_table_version_across_publication() {
        let session = catalog_test_session(usize::MAX);
        let make_frame = |values: Vec<i64>| {
            let id = Series::new("id".into(), values);
            let physical = PlDataFrame::new_infer_height(vec![id.into()]).unwrap();
            DataFrame::from_polars(physical)
        };
        let original = make_frame(vec![1]);
        let original_bytes = original.estimated_size().unwrap();
        session.register_table("items", original);

        let read_lease = session.begin_catalog_read_snapshot().unwrap().unwrap();
        let resolved_reader = session.table("items").unwrap();
        let replacement = make_frame(vec![1, 2]);
        {
            let _mutation = session.shared_catalog_mutation_guard().unwrap();
            let _writer = session.shared_catalog_guard().unwrap();
            session
                .check_catalog_table_growth("items", &replacement)
                .unwrap();
            session.register_table_during_sql("items", replacement);
        }

        let old_values = resolved_reader
            .collect()
            .unwrap()
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(old_values, vec![1]);
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), original_bytes);
        let new_values = session
            .table("items")
            .unwrap()
            .collect()
            .unwrap()
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(new_values, vec![1, 2]);

        drop(read_lease);
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), 0);
    }

    #[test]
    fn shared_catalog_join_keeps_one_snapshot_across_table_resolutions() {
        let session = catalog_test_session(usize::MAX);
        let make_frame = |id| {
            let id = Series::new("id".into(), [id]);
            let physical = PlDataFrame::new_infer_height(vec![id.into()]).unwrap();
            DataFrame::from_polars(physical)
        };
        session.register_table("left_items", make_frame(1_i64));
        session.register_table("right_items", make_frame(10_i64));

        let read_lease = session.begin_catalog_read_snapshot().unwrap().unwrap();
        let resolved_join = session
            .sql(
                "SELECT l.id AS left_id, r.id AS right_id \
                 FROM left_items l CROSS JOIN right_items r",
            )
            .unwrap();
        {
            let _mutation = session.shared_catalog_mutation_guard().unwrap();
            let _writer = session.shared_catalog_guard().unwrap();
            let left_replacement = make_frame(2_i64);
            session
                .check_catalog_table_growth("left_items", &left_replacement)
                .unwrap();
            session.register_table_during_sql("left_items", left_replacement);
            let right_replacement = make_frame(20_i64);
            session
                .check_catalog_table_growth("right_items", &right_replacement)
                .unwrap();
            session.register_table_during_sql("right_items", right_replacement);
        }

        let old_rows = resolved_join.collect_as_json_rows().unwrap();
        assert_eq!(old_rows.len(), 1);
        assert_eq!(old_rows[0].get("left_id"), Some(&serde_json::json!(1)));
        assert_eq!(old_rows[0].get("right_id"), Some(&serde_json::json!(10)));
        let new_rows = session
            .sql(
                "SELECT l.id AS left_id, r.id AS right_id \
                 FROM left_items l CROSS JOIN right_items r",
            )
            .unwrap()
            .collect_as_json_rows()
            .unwrap();
        assert_eq!(new_rows.len(), 1);
        assert_eq!(new_rows[0].get("left_id"), Some(&serde_json::json!(2)));
        assert_eq!(new_rows[0].get("right_id"), Some(&serde_json::json!(20)));

        drop(read_lease);
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), 0);
    }

    #[test]
    fn shared_catalog_drop_keeps_resolved_reader_alive_until_lease_release() {
        let session = catalog_test_session(usize::MAX);
        let id = Series::new("id".into(), [4_i64, 5_i64]);
        let physical = PlDataFrame::new_infer_height(vec![id.into()]).unwrap();
        let original = DataFrame::from_polars(physical);
        let original_bytes = original.estimated_size().unwrap();
        session.register_table("drop_snapshot", original);

        let read_lease = session.begin_catalog_read_snapshot().unwrap().unwrap();
        let resolved_reader = session
            .sql("SELECT id FROM drop_snapshot ORDER BY id")
            .unwrap();
        session.sql("DROP TABLE drop_snapshot").unwrap();
        assert!(!session.saved_table_exists("drop_snapshot"));
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), original_bytes);

        let rows = resolved_reader.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&serde_json::json!(4)));
        assert_eq!(rows[1].get("id"), Some(&serde_json::json!(5)));

        let missing = session.sql("SELECT id FROM drop_snapshot");
        assert!(missing.is_err());
        drop(read_lease);
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), 0);
    }

    #[test]
    fn shared_catalog_charges_pinned_versions_until_last_read_finishes() {
        let original = catalog_test_frame();
        let bytes = original.estimated_size().unwrap();
        let session = catalog_test_session(bytes.saturating_mul(2));
        session.register_table("t", original.clone());
        let read_lease = session.begin_catalog_read_snapshot().unwrap().unwrap();

        let replacement = catalog_test_frame();
        let _mutation = session.shared_catalog_mutation_guard().unwrap();
        let _writer = session.shared_catalog_guard().unwrap();
        session
            .check_catalog_table_growth("t", &replacement)
            .unwrap();
        session.register_table_during_sql("t", replacement.clone());
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), bytes);
        assert!(
            session
                .check_catalog_table_growth("t", &catalog_test_frame())
                .unwrap_err()
                .to_string()
                .contains("CatalogCapacityExceeded")
        );

        drop(_writer);
        drop(_mutation);
        drop(read_lease);
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), 0);
        let next = catalog_test_frame();
        let _mutation = session.shared_catalog_mutation_guard().unwrap();
        let _writer = session.shared_catalog_guard().unwrap();
        session.check_catalog_table_growth("t", &next).unwrap();
    }

    #[test]
    fn shared_catalog_charges_temp_view_snapshots_and_releases_dropped_views() {
        let table = catalog_test_frame();
        let bytes = table.estimated_size().unwrap();
        let session = catalog_test_session(bytes.saturating_mul(2));
        session.register_table("t", table.clone());

        let _mutation = session.shared_catalog_mutation_guard().unwrap();
        let _writer = session.shared_catalog_guard().unwrap();
        session
            .register_temp_view_during_sql("first", catalog_test_frame())
            .unwrap();
        assert!(
            session
                .register_temp_view_during_sql("second", catalog_test_frame())
                .unwrap_err()
                .to_string()
                .contains("CatalogCapacityExceeded")
        );
        assert!(session.drop_temp_view_during_sql("first"));
        session
            .register_temp_view_during_sql("second", catalog_test_frame())
            .unwrap();
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), bytes);
        session.stop();
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), 0);
    }

    #[test]
    fn shared_catalog_temp_view_replacement_reserves_a_pinned_version() {
        let table = catalog_test_frame();
        let bytes = table.estimated_size().unwrap();
        let session = catalog_test_session(bytes.saturating_mul(2));
        session.register_table("t", table.clone());

        let _mutation = session.shared_catalog_mutation_guard().unwrap();
        let _writer = session.shared_catalog_guard().unwrap();
        session
            .register_temp_view_during_sql("snapshot", catalog_test_frame())
            .unwrap();
        drop(_writer);

        let read_lease = session.begin_catalog_read_snapshot().unwrap().unwrap();
        let _writer = session.shared_catalog_guard().unwrap();
        let replacement = catalog_test_frame();
        let error = session
            .register_temp_view_during_sql("snapshot", replacement.clone())
            .unwrap_err();
        assert!(error.to_string().contains("CatalogCapacityExceeded"));
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), bytes);
        assert_eq!(session.temp_view("snapshot").unwrap().count().unwrap(), 64);

        drop(_writer);
        drop(read_lease);
        let _writer = session.shared_catalog_guard().unwrap();
        session
            .register_temp_view_during_sql("snapshot", replacement)
            .unwrap();
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), bytes);
    }

    #[test]
    fn shared_catalog_deduplicates_shared_arrow_chunks_across_views() {
        let table = catalog_test_frame();
        let bytes = table.estimated_size().unwrap();
        let session = catalog_test_session(bytes);
        session.register_table("t", table.clone());

        let _mutation = session.shared_catalog_mutation_guard().unwrap();
        let _writer = session.shared_catalog_guard().unwrap();
        session
            .register_temp_view_during_sql("alias", table.clone())
            .unwrap();
        session
            .register_temp_view_during_sql("second_alias", table)
            .unwrap();
        assert_eq!(session.catalog_auxiliary_bytes().unwrap(), bytes);
        assert!(
            session
                .register_temp_view_during_sql("independent", catalog_test_frame())
                .unwrap_err()
                .to_string()
                .contains("CatalogCapacityExceeded")
        );
    }

    #[test]
    fn shared_catalog_serializes_concurrent_growth_at_the_byte_limit() {
        let frame = catalog_test_frame();
        let bytes = frame.estimated_size().unwrap();
        let session = catalog_test_session(bytes);
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut joins = Vec::new();
        for name in ["first", "second"] {
            let session = session.clone();
            let barrier = barrier.clone();
            joins.push(std::thread::spawn(move || {
                let candidate = catalog_test_frame();
                barrier.wait();
                let _mutation = session.shared_catalog_mutation_guard().unwrap();
                let _writer = session.shared_catalog_guard().unwrap();
                if session
                    .check_catalog_table_growth(name, &candidate)
                    .is_err()
                {
                    return false;
                }
                session.register_table_during_sql(name, candidate);
                true
            }));
        }
        barrier.wait();
        let accepted = joins
            .into_iter()
            .map(|join| join.join().unwrap())
            .filter(|accepted| *accepted)
            .count();
        assert_eq!(accepted, 1);
        assert_eq!(session.tables.lock().unwrap().len(), 1);
    }

    #[test]
    fn range_uses_lazy_source_with_checked_signed_cardinality() {
        let session = SparkSession::builder()
            .app_name("lazy-range-source")
            .get_or_create();

        let positive = session.range(1, 8, 3).unwrap();
        assert_eq!(positive.count().unwrap(), 3);
        let positive_values = positive
            .limit(3)
            .unwrap()
            .collect()
            .unwrap()
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(positive_values, vec![1, 4, 7]);

        let negative = session.range(7, -2, -3).unwrap();
        let negative_values = negative
            .collect()
            .unwrap()
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(negative_values, vec![7, 4, 1]);

        let near_max = session
            .range(i64::MAX - 2, i64::MAX, 1)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            near_max
                .column("id")
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            vec![i64::MAX - 2, i64::MAX - 1]
        );
        let near_min = session
            .range(i64::MIN + 2, i64::MIN, -1)
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(
            near_min
                .column("id")
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            vec![i64::MIN + 2, i64::MIN + 1]
        );

        assert_eq!(session.range(4, 4, 1).unwrap().count().unwrap(), 0);
        assert!(session.range(0, 1, 0).is_err());
        assert_eq!(range_row_count(i64::MIN, i64::MAX, 1).unwrap(), u64::MAX);
        assert_eq!(range_row_count(i64::MIN, i64::MAX, i64::MAX).unwrap(), 3);
        assert_eq!(range_row_count(i64::MAX, i64::MIN, i64::MIN).unwrap(), 2);
    }

    #[test]
    fn range_limit_and_filtered_count_keep_their_semantics() {
        let session = SparkSession::builder()
            .app_name("lazy-range-limit-count")
            .get_or_create();

        let large = session.range(0, 1_000_000, 1).unwrap();
        let first_two = large.limit(2).unwrap();
        assert_eq!(first_two.count().unwrap(), 2);
        let values = first_two
            .collect()
            .unwrap()
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(values, vec![0, 1]);

        let filtered = session
            .range(0, 10, 1)
            .unwrap()
            .filter(polars::prelude::col("id").gt(5i64))
            .unwrap();
        assert_eq!(filtered.count().unwrap(), 4);
    }

    #[test]
    fn range_chained_filters_and_interleaved_limits_execute_in_bounded_batches() {
        let session = SparkSession::builder()
            .app_name("lazy-range-batched-count")
            .get_or_create();

        let filtered = session
            .range(0, 100_000, 1)
            .unwrap()
            .filter(polars::prelude::col("id").gt(10i64))
            .unwrap()
            .filter(polars::prelude::col("id").lt(90_000i64))
            .unwrap();
        assert_eq!(filtered.count().unwrap(), 89_989);

        let limited = session
            .range(0, 100_000, 1)
            .unwrap()
            .filter(polars::prelude::col("id").gt(10i64))
            .unwrap()
            .limit(4)
            .unwrap()
            .filter(polars::prelude::col("id").gt(12i64))
            .unwrap();
        assert_eq!(limited.count().unwrap(), 2);
        let ids = limited
            .collect()
            .unwrap()
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![13, 14]);
    }

    #[derive(Clone)]
    struct TestExternalCatalog {
        identifier: String,
        descriptor: Arc<Mutex<Option<ExternalTableDescriptor>>>,
        error: Option<String>,
        write_error: Arc<Mutex<Option<String>>>,
        resolutions: Arc<Mutex<Vec<String>>>,
        writes: Arc<Mutex<Vec<robin_sparkless_core::engine::ExternalTableWriteRequest>>>,
    }

    impl CatalogResolver for TestExternalCatalog {
        fn resolve_ownership(&self, identifier: &str) -> Result<CatalogOwnership, EngineError> {
            let namespace = |value: &str| {
                let parts = value.split('.').collect::<Vec<_>>();
                if parts.len() >= 2 {
                    parts[..parts.len() - 1].join(".")
                } else {
                    value.to_string()
                }
            };
            if namespace(identifier) == namespace(&self.identifier) {
                Ok(CatalogOwnership::external(
                    namespace(&self.identifier),
                    robin_sparkless_core::engine::CatalogCapabilities::READ_WRITE,
                ))
            } else {
                Ok(CatalogOwnership::Local)
            }
        }

        fn resolve_table(
            &self,
            identifier: &str,
        ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
            self.resolutions
                .lock()
                .expect("test resolution lock")
                .push(identifier.to_string());
            if let Some(error) = &self.error {
                return Err(EngineError::invalid_input(error.clone()));
            }
            if identifier == self.identifier {
                Ok(self
                    .descriptor
                    .lock()
                    .expect("test descriptor lock")
                    .clone())
            } else {
                Ok(None)
            }
        }
    }

    impl TestExternalCatalog {
        fn write_table_request(
            &self,
            request: robin_sparkless_core::engine::ExternalTableWriteRequest,
        ) -> Result<robin_sparkless_core::engine::ExternalTableWriteResult, EngineError> {
            self.writes
                .lock()
                .expect("test write lock")
                .push(request.clone());
            if let Some(error) = self
                .write_error
                .lock()
                .expect("test write error lock")
                .clone()
            {
                return Err(EngineError::invalid_input(error));
            }

            #[cfg(feature = "delta")]
            {
                use polars::io::ipc::IpcStreamReader;
                use polars::prelude::SerReader;

                let input = IpcStreamReader::new(std::io::Cursor::new(request.data.bytes))
                    .finish()
                    .map_err(|error| {
                        EngineError::invalid_input(format!("decode Arrow IPC: {error}"))
                    })?;
                let mode = match request.mode {
                    robin_sparkless_core::engine::ExternalTableSaveMode::Append => {
                        crate::dataframe::SaveMode::Append
                    }
                    robin_sparkless_core::engine::ExternalTableSaveMode::Overwrite => {
                        crate::dataframe::SaveMode::Overwrite
                    }
                    robin_sparkless_core::engine::ExternalTableSaveMode::ErrorIfExists
                    | robin_sparkless_core::engine::ExternalTableSaveMode::Ignore => {
                        return Err(EngineError::invalid_input(
                            "test provider received a non-mutating save mode".into(),
                        ));
                    }
                };
                let option_is_true = |name: &str| {
                    request.options.iter().rev().any(|(key, value)| {
                        key.eq_ignore_ascii_case(name) && value.eq_ignore_ascii_case("true")
                    })
                };
                crate::delta::write_delta_with_save_options_and_logical_map_paths_for_catalog(
                    &input,
                    &request.table.canonical_location,
                    mode,
                    option_is_true("mergeSchema"),
                    option_is_true("overwriteSchema"),
                    &request.partition_by,
                    &[],
                )
                .map_err(|error| {
                    EngineError::invalid_input(format!("write local Delta fixture: {error}"))
                })?;

                let committed_version = request.table.version.unwrap_or(0) + 1;
                let mut table = request.table;
                table.version = Some(committed_version);
                *self.descriptor.lock().expect("test descriptor lock") = Some(table.clone());
                Ok(robin_sparkless_core::engine::ExternalTableWriteResult {
                    table,
                    committed_version: Some(committed_version),
                    provider_commit_id: None,
                })
            }
            #[cfg(not(feature = "delta"))]
            {
                let _ = request;
                Err(EngineError::invalid_input(
                    "test provider requires the delta feature".into(),
                ))
            }
        }
    }

    struct TestPreparedWrite {
        provider: TestExternalCatalog,
        request: robin_sparkless_core::engine::ExternalTableWriteRequest,
    }

    impl robin_sparkless_core::engine::PreparedCatalogWrite for TestPreparedWrite {
        fn commit(
            self: Box<Self>,
            token: &CancellationToken,
        ) -> robin_sparkless_core::engine::ExternalTableWriteOutcome {
            if let Err(error) = token.check_cancelled() {
                return robin_sparkless_core::engine::ExternalTableWriteOutcome::NotCommitted(
                    error,
                );
            }
            match self.provider.write_table_request(self.request) {
                Ok(receipt) => {
                    robin_sparkless_core::engine::ExternalTableWriteOutcome::Committed(receipt)
                }
                Err(error) => {
                    robin_sparkless_core::engine::ExternalTableWriteOutcome::NotCommitted(error)
                }
            }
        }
    }

    impl robin_sparkless_core::engine::CatalogWriter for TestExternalCatalog {
        fn prepare_write(
            &self,
            request: robin_sparkless_core::engine::ExternalTableWriteRequest,
        ) -> Result<Box<dyn robin_sparkless_core::engine::PreparedCatalogWrite>, EngineError>
        {
            Ok(Box::new(TestPreparedWrite {
                provider: self.clone(),
                request,
            }))
        }
    }

    fn catalog_for_test(provider: Arc<TestExternalCatalog>) -> Arc<ExternalCatalog> {
        let resolver: Arc<dyn CatalogResolver> = provider.clone();
        let writer: Arc<dyn robin_sparkless_core::engine::CatalogWriter> = provider;
        Arc::new(ExternalCatalog::new(resolver, None, Some(writer), None))
    }

    fn external_descriptor(path: &std::path::Path, schema: StructType) -> ExternalTableDescriptor {
        ExternalTableDescriptor {
            table_id: "table-123".into(),
            canonical_location: path.to_string_lossy().into_owned(),
            schema,
            version: None,
            provider: "delta-rs".into(),
            permissions_context: Some("test-principal".into()),
            format: ExternalTableFormat::Delta,
        }
    }

    #[cfg(feature = "delta")]
    fn write_test_delta_table(path: &std::path::Path) -> StructType {
        let session = SparkSession::builder()
            .app_name("external-catalog-fixture")
            .get_or_create();
        let frame = session
            .create_dataframe(
                vec![(7, 10, "value".to_string())],
                vec!["id", "age", "name"],
            )
            .expect("create fixture DataFrame");
        let schema = frame.schema().expect("read fixture schema");
        frame
            .write_delta(path, true, false)
            .expect("write real local Delta fixture");
        schema
    }

    #[cfg(feature = "delta")]
    fn typed_external_frame(session: &SparkSession, id: i64, amount: &str) -> DataFrame {
        session
            .create_dataframe_from_rows(
                vec![vec![
                    json!(id),
                    json!(amount),
                    json!("2026-10-19"),
                    json!("2026-10-19 12:34:56"),
                    json!([id, id + 1]),
                    json!({"label": "sample", "count": id}),
                ]],
                vec![
                    ("id".into(), "long".into()),
                    ("amount".into(), "decimal(10,2)".into()),
                    ("day".into(), "date".into()),
                    ("created".into(), "timestamp".into()),
                    ("items".into(), "array<long>".into()),
                    ("payload".into(), "struct<label:string,count:long>".into()),
                ],
                true,
                false,
            )
            .expect("create typed external table fixture")
    }

    fn test_external_catalog(
        identifier: &str,
        descriptor: Option<ExternalTableDescriptor>,
        error: Option<&str>,
    ) -> (Arc<TestExternalCatalog>, Arc<Mutex<Vec<String>>>) {
        let resolutions = Arc::new(Mutex::new(Vec::new()));
        let provider = Arc::new(TestExternalCatalog {
            identifier: identifier.to_string(),
            descriptor: Arc::new(Mutex::new(descriptor)),
            error: error.map(str::to_string),
            write_error: Arc::new(Mutex::new(None)),
            resolutions: resolutions.clone(),
            writes: Arc::new(Mutex::new(Vec::new())),
        });
        (provider, resolutions)
    }

    #[test]
    fn decimal_schema_enforces_declared_precision() {
        let valid = vec![Some(json!("99.99"))];
        let series = json_values_to_series(&valid, "decimal(4,2)", "amount")
            .expect("99.99 fits decimal(4,2)");
        assert_eq!(series.dtype(), &DataType::Decimal(4, 2));

        let too_large = json!("100.00");
        assert!(verify_json_value_for_type(&too_large, "decimal(4,2)").is_err());
        assert!(json_values_to_series(&[Some(too_large)], "decimal(4,2)", "amount").is_err());
        assert!(parse_decimal_precision_scale("decimal(2,3)").is_none());
    }

    #[test]
    fn managed_int32_validation_is_strict_without_changing_legacy_int_aliases() {
        let outside_int32 = json!(2147483648_i64);
        assert!(verify_json_value_for_type(&outside_int32, "int").is_ok());
        assert!(verify_json_value_for_type(&outside_int32, "int32").is_err());
        assert!(verify_json_value_for_type(&json!(i32::MAX), "int32").is_ok());
    }

    #[test]
    fn test_spark_session_builder_basic() {
        let spark = SparkSession::builder().app_name("test_app").get_or_create();

        assert_eq!(spark.app_name, Some("test_app".to_string()));
    }

    #[test]
    fn test_spark_session_builder_with_master() {
        let spark = SparkSession::builder()
            .app_name("test_app")
            .master("local[*]")
            .get_or_create();

        assert_eq!(spark.app_name, Some("test_app".to_string()));
        assert_eq!(spark.master, Some("local[*]".to_string()));
    }

    #[test]
    fn test_spark_session_builder_with_config() {
        let spark = SparkSession::builder()
            .app_name("test_app")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "2g")
            .get_or_create();

        assert_eq!(
            spark.config.get("spark.executor.memory"),
            Some(&"4g".to_string())
        );
        assert_eq!(
            spark.config.get("spark.driver.memory"),
            Some(&"2g".to_string())
        );
    }

    /// Batch 8 / #789: spark.conf().get("spark.app.name") returns the app name set in builder.
    #[test]
    fn test_spark_session_conf_returns_app_name() {
        let spark = SparkSession::builder()
            .app_name("test_fixture")
            .get_or_create();
        let conf = spark.get_config();
        assert_eq!(
            conf.get("spark.app.name"),
            Some(&"test_fixture".to_string()),
            "conf should expose spark.app.name for PySpark parity"
        );
    }

    #[test]
    fn test_spark_session_default() {
        let spark = SparkSession::default();
        assert!(spark.app_name.is_none());
        assert!(spark.master.is_none());
        assert!(spark.config.is_empty());
    }

    #[test]
    fn test_create_dataframe_success() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let data = vec![
            (1i64, 25i64, "Alice".to_string()),
            (2i64, 30i64, "Bob".to_string()),
        ];

        let result = spark.create_dataframe(data, vec!["id", "age", "name"]);

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 2);

        let columns = df.columns().unwrap();
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"age".to_string()));
        assert!(columns.contains(&"name".to_string()));
    }

    #[test]
    fn test_create_dataframe_wrong_column_count() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let data = vec![(1i64, 25i64, "Alice".to_string())];

        // Too few columns
        let result = spark.create_dataframe(data.clone(), vec!["id", "age"]);
        assert!(result.is_err());

        // Too many columns
        let result = spark.create_dataframe(data, vec!["id", "age", "name", "extra"]);
        assert!(result.is_err());
    }

    /// #1345: Empty schema + rows with 0 fields each succeeds (PySpark: createDataFrame([{}], StructType([]))).
    #[test]
    fn test_create_dataframe_from_rows_empty_schema_with_rows_returns_error() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![vec![]];
        let schema: Vec<(String, String)> = vec![];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        let df = result.expect("#1345: empty schema + empty row(s) should succeed");
        assert_eq!(df.count().unwrap(), 1);
        assert_eq!(df.columns().unwrap().len(), 0);
        // Empty schema + row with >0 fields still errors
        let rows_bad: Vec<Vec<JsonValue>> = vec![vec![JsonValue::Null]];
        let result_bad = spark.create_dataframe_from_rows(rows_bad, vec![], false, false);
        let err_msg = match &result_bad {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected error for empty schema with row containing 1 field"),
        };
        assert!(
            err_msg.contains("LENGTH_SHOULD_BE_THE_SAME") || err_msg.contains("Expected 0 fields")
        );
    }

    #[test]
    fn test_create_dataframe_from_rows_empty_data_with_schema() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![];
        let schema = vec![
            ("a".to_string(), "int".to_string()),
            ("b".to_string(), "string".to_string()),
        ];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
        assert_eq!(df.collect_inner().unwrap().get_column_names(), &["a", "b"]);
    }

    /// #419: Single column "value" from scalar values (createDataFrame([1,2,3], "bigint") parity).
    #[test]
    fn test_create_dataframe_from_single_column() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe_from_single_column(vec![json!(1), json!(2), json!(3)], "bigint")
            .unwrap();
        assert_eq!(df.count().unwrap(), 3);
        let rows = df.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("value").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(rows[1].get("value").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(rows[2].get("value").and_then(|v| v.as_i64()), Some(3));
        let df2 = spark
            .create_dataframe_from_single_column(vec![json!("a"), json!("b")], "string")
            .unwrap();
        assert_eq!(df2.count().unwrap(), 2);
        let rows2 = df2.collect_as_json_rows().unwrap();
        assert_eq!(rows2[0].get("value").and_then(|v| v.as_str()), Some("a"));
    }

    /// #420: When verify_schema is true, type mismatch returns clear error with row index and column.
    #[test]
    fn test_create_dataframe_from_rows_verify_schema_raises_on_type_mismatch() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows = vec![
            vec![json!("Alice"), json!(25)],
            vec![json!("Bob"), json!("thirty")], // age as string, schema says bigint
        ];
        let schema = vec![
            ("name".to_string(), "string".to_string()),
            ("age".to_string(), "bigint".to_string()),
        ];
        let result = spark.create_dataframe_from_rows(rows, schema, true, false);
        let err = match &result {
            Ok(_) => panic!("expected error when verify_schema=true and type mismatch"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("Row 1") || err.to_lowercase().contains("row 1"),
            "error should mention row index: {}",
            err
        );
        assert!(
            err.contains("age") || err.to_lowercase().contains("column"),
            "error should mention column: {}",
            err
        );
        assert!(
            err.contains("bigint") || err.to_lowercase().contains("number"),
            "error should mention expected type: {}",
            err
        );
    }

    #[test]
    fn test_create_dataframe_from_rows_empty_schema_empty_data() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![];
        let schema: Vec<(String, String)> = vec![];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
        assert_eq!(df.collect_inner().unwrap().get_column_names().len(), 0);
    }

    /// create_dataframe_from_rows: struct column as JSON object (by field name). PySpark parity #600.
    #[test]
    fn test_create_dataframe_from_rows_struct_as_object() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!({"a": 1, "b": "y"})],
            vec![json!("z"), json!({"a": 2, "b": "w"})],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "nested"]);
    }

    /// create_dataframe_from_rows: struct column as JSON array (by position). PySpark parity #600.
    #[test]
    fn test_create_dataframe_from_rows_struct_as_array() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!([1, "y"])],
            vec![json!("z"), json!([2, "w"])],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "nested"]);
    }

    /// #634: create_dataframe_from_rows accepts struct as object with "0","1",... keys (Python tuple serialization).
    #[test]
    fn test_create_dataframe_from_rows_struct_as_object_numeric_keys() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!({"0": 1, "1": "y"})],
            vec![json!("z"), json!({"0": 2, "1": "w"})],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "nested"]);
    }

    /// #1015: When row data has E1/E2 keys (Sparkless serialization), struct is built with schema names (a, b) so collect() yields row["nested"]["a"].
    #[test]
    fn test_issue_1015_collect_struct_field_names() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![vec![json!("x"), json!({"E1": 1, "E2": "y"})]];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        let rows_out = df.collect_as_json_rows().unwrap();
        assert_eq!(rows_out.len(), 1);
        let nested = rows_out[0]
            .get("nested")
            .and_then(|v| v.as_object())
            .expect("nested");
        assert!(
            nested.contains_key("a"),
            "#1015: collect should use schema field name 'a', not E1"
        );
        assert!(
            nested.contains_key("b"),
            "#1015: collect should use schema field name 'b', not E2"
        );
        assert_eq!(nested.get("a"), Some(&json!(1)));
        assert_eq!(nested.get("b"), Some(&json!("y")));
    }

    /// #1347: create_dataframe_from_rows raises when schema has duplicate column names (stricter than PySpark).
    #[test]
    fn test_create_dataframe_from_rows_duplicate_column_names_raises() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows = vec![vec![json!(1), json!("a"), json!(true)]];
        let schema = vec![
            ("a".to_string(), "bigint".to_string()),
            ("b".to_string(), "string".to_string()),
            ("a".to_string(), "boolean".to_string()),
        ];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        match &result {
            Err(e) => assert!(
                e.to_string().contains("duplicate column name"),
                "expected duplicate column error, got: {}",
                e
            ),
            Ok(_) => panic!("expected error for duplicate column names in schema"),
        }
    }

    /// #711: create_dataframe_from_rows raises when row length differs from schema (PySpark LENGTH_SHOULD_BE_THE_SAME).
    #[test]
    fn test_create_dataframe_from_rows_mismatched_row_length_raises() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("Name".to_string(), "string".to_string()),
            ("Value".to_string(), "integer".to_string()),
        ];
        // Row 0 has 1 element, schema has 2
        let rows_short = vec![vec![json!("Alice")], vec![json!("Bob"), json!(2)]];
        let result = spark.create_dataframe_from_rows(rows_short, schema.clone(), false, false);
        match &result {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.to_lowercase().contains("length"),
                    "expected length error, got: {}",
                    msg
                );
                assert!(
                    msg.contains("1") && msg.contains("2"),
                    "expected 1 and 2 in message, got: {}",
                    msg
                );
            }
            Ok(_) => panic!("expected error for mismatched row length"),
        }
        // Row 1 has 3 elements, schema has 2
        let rows_long = vec![
            vec![json!("Alice"), json!(1)],
            vec![json!("Bob"), json!(2), json!(100)],
        ];
        let result2 = spark.create_dataframe_from_rows(rows_long, schema, false, false);
        match &result2 {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.to_lowercase().contains("length"),
                    "expected length error, got: {}",
                    msg
                );
                assert!(
                    msg.contains("3") && msg.contains("2"),
                    "expected 3 and 2 in message, got: {}",
                    msg
                );
            }
            Ok(_) => panic!("expected error for mismatched row length (too long)"),
        }
    }

    /// #731, #769, #772: create_dataframe_from_rows with schema that is only column names (all "string")
    /// infers types from data; collect returns int/double/bool not strings.
    #[test]
    fn test_create_dataframe_from_rows_all_string_schema_infers_types() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("Alice"), json!(25), json!(50000.5), json!(true)],
            vec![json!("Bob"), json!(30), json!(60000.0), json!(false)],
        ];
        let schema = vec![
            ("name".to_string(), "string".to_string()),
            ("age".to_string(), "string".to_string()),
            ("salary".to_string(), "string".to_string()),
            ("active".to_string(), "string".to_string()),
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let rows_out = df.collect_as_json_rows().unwrap();
        assert_eq!(rows_out.len(), 2);
        // age/salary/active must be number/bool in JSON, not string
        let r0 = &rows_out[0];
        assert_eq!(r0.get("name").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(r0.get("age").and_then(|v| v.as_i64()), Some(25));
        assert_eq!(r0.get("salary").and_then(|v| v.as_f64()), Some(50000.5));
        assert_eq!(r0.get("active").and_then(|v| v.as_bool()), Some(true));
    }

    /// PR13: create_dataframe_from_rows with empty schema + non-empty rows returns error (LENGTH_SHOULD_BE_THE_SAME).
    #[test]
    fn test_create_dataframe_from_rows_infer_struct_from_objects() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("id1"), json!({"a": 1, "b": "x"})],
            vec![json!("id2"), json!({"a": 2, "b": "y"})],
        ];
        let schema = vec![];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        match &result {
            Err(e) => assert!(
                e.to_string().contains("LENGTH_SHOULD_BE_THE_SAME")
                    || e.to_string().contains("Expected 0 fields"),
                "expected LENGTH_SHOULD_BE_THE_SAME: {}",
                e
            ),
            Ok(_) => panic!("expected error for empty schema with non-empty rows"),
        }
    }

    /// #610: create_dataframe_from_rows accepts struct as string that parses to object or array (Sparkless/Python serialization).
    #[test]
    fn test_issue_610_struct_value_as_string_object_or_array() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        // Struct as string that parses to JSON object (e.g. Python dict serialized as string).
        let rows_object: Vec<Vec<JsonValue>> =
            vec![vec![json!("A"), json!(r#"{"a": 1, "b": "x"}"#)]];
        let df1 = spark
            .create_dataframe_from_rows(rows_object, schema.clone(), false, false)
            .unwrap();
        assert_eq!(df1.count().unwrap(), 1);

        // Struct as string that parses to JSON array (e.g. Python tuple (1, "y") serialized as "[1, \"y\"]").
        let rows_array: Vec<Vec<JsonValue>> = vec![vec![json!("B"), json!(r#"[1, "y"]"#)]];
        let df2 = spark
            .create_dataframe_from_rows(rows_array, schema, false, false)
            .unwrap();
        assert_eq!(df2.count().unwrap(), 1);
    }

    /// #691/PR-F: create_dataframe_from_rows accepts struct column as Python dict repr string (e.g. "{'a': 1, 'b': 'x'}").
    #[test]
    fn test_issue_691_struct_value_as_python_dict_repr_string() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            (
                "nested".to_string(),
                "struct<a:bigint,b:string>".to_string(),
            ),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![vec![json!("x"), json!("{'a': 1, 'b': 'x'}")]];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 1);
    }

    /// #710/PR-G: create_dataframe_from_rows with nested array type (array<array<bigint>>).
    #[test]
    fn test_issue_710_nested_array_type() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("arr".to_string(), "array<array<bigint>>".to_string()),
        ];
        // One row: id="x", arr=[[1,2],[3,4]]
        let rows: Vec<Vec<JsonValue>> = vec![vec![json!("x"), json!([[1, 2], [3, 4]])]];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 1);
        let collected = df.collect_inner().unwrap();
        let arr_col = collected.column("arr").unwrap();
        assert!(arr_col.dtype().is_list());
        let list = arr_col.list().unwrap();
        assert_eq!(list.len(), 1);
    }

    /// #611: create_dataframe_from_rows accepts single value as one-element array (PySpark parity).
    #[test]
    fn test_issue_611_array_column_single_value_as_one_element() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("arr".to_string(), "array<bigint>".to_string()),
        ];
        // Single number as one-element list (PySpark accepts this).
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!(42)],
            vec![json!("y"), json!([1, 2, 3])],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        let arr_col = collected.column("arr").unwrap();
        let list = arr_col.list().unwrap();
        let row0 = list.get(0).unwrap();
        assert_eq!(
            row0.len(),
            1,
            "#611: single value should become one-element list"
        );
        let row1 = list.get(1).unwrap();
        assert_eq!(row1.len(), 3);
    }

    /// create_dataframe_from_rows: array column with JSON array and null. PySpark parity #601.
    #[test]
    fn test_create_dataframe_from_rows_array_column() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("arr".to_string(), "array<bigint>".to_string()),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!([1, 2, 3])],
            vec![json!("y"), json!([4, 5])],
            vec![json!("z"), json!(null)],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 3);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "arr"]);

        // Issue #601: verify array data round-trips correctly (not just no error).
        let arr_col = collected.column("arr").unwrap();
        let list = arr_col.list().unwrap();
        // Row 0: [1, 2, 3]
        let row0 = list.get(0).unwrap();
        assert_eq!(row0.len(), 3, "row 0 arr should have 3 elements");
        // Row 1: [4, 5]
        let row1 = list.get(1).unwrap();
        assert_eq!(row1.len(), 2);
        // Row 2: null list (representation may be None or empty)
        let row2 = list.get(2);
        assert!(
            row2.is_none() || row2.as_ref().map(|a| a.is_empty()).unwrap_or(false),
            "row 2 arr should be null or empty"
        );
    }

    /// create_dataframe_from_rows: array<timestamp> and array<date> element types (PySpark parity).
    #[test]
    fn test_create_dataframe_from_rows_array_timestamp() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("ts_arr".to_string(), "array<timestamp>".to_string()),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![
                json!("a"),
                json!(["2025-01-15T10:00:00Z", "2025-01-16T12:00:00"]),
            ],
            vec![json!("b"), json!(null)],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .expect("array<timestamp> should be supported");
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "ts_arr"]);
    }

    /// Issue #601: PySpark createDataFrame([(\"x\", [1,2,3]), (\"y\", [4,5])], schema) with ArrayType.
    /// Must not fail with \"array column value must be null or array\" and must produce correct structure.
    #[test]
    fn test_issue_601_array_column_pyspark_parity() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("arr".to_string(), "array<bigint>".to_string()),
        ];
        // Exact PySpark example: rows with string id and list of ints.
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!([1, 2, 3])],
            vec![json!("y"), json!([4, 5])],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .expect("issue #601: create_dataframe_from_rows must accept array column (JSON array)");
        let n = df.count().unwrap();
        assert_eq!(n, 2, "issue #601: expected 2 rows");
        let collected = df.collect_inner().unwrap();
        let arr_col = collected.column("arr").unwrap();
        let list = arr_col.list().unwrap();
        // Verify list lengths match PySpark [1,2,3] and [4,5]
        let row0 = list.get(0).unwrap();
        assert_eq!(
            row0.len(),
            3,
            "issue #601: first row arr must have 3 elements [1,2,3]"
        );
        let row1 = list.get(1).unwrap();
        assert_eq!(
            row1.len(),
            2,
            "issue #601: second row arr must have 2 elements [4,5]"
        );
    }

    /// #1016: create_dataframe_from_rows accepts array<boolean> as Python repr string "[True, False, True]".
    #[test]
    fn test_issue_1016_array_boolean_python_repr() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("flags".to_string(), "array<boolean>".to_string()),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![
                JsonValue::String("a".into()),
                JsonValue::String("[True, False, True]".into()),
            ],
            vec![
                JsonValue::String("b".into()),
                JsonValue::String("[False, True]".into()),
            ],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .expect("#1016: array<boolean> from Python repr string must parse");
        let collected = df.collect_inner().unwrap();
        let flags_col = collected.column("flags").unwrap();
        let list = flags_col.list().unwrap();
        let row0 = list.get(0).unwrap();
        assert_eq!(row0.len(), 3, "#1016: [True, False, True] -> 3 elements");
        let row1 = list.get(1).unwrap();
        assert_eq!(row1.len(), 2, "#1016: [False, True] -> 2 elements");
    }

    /// #624: When schema is empty but rows are not, backend returns LENGTH_SHOULD_BE_THE_SAME (no inference).
    #[test]
    fn test_issue_624_empty_schema_inferred_from_rows() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema: Vec<(String, String)> = vec![];
        let rows: Vec<Vec<JsonValue>> =
            vec![vec![json!("a"), json!(1)], vec![json!("b"), json!(2)]];
        let result = spark.create_dataframe_from_rows(rows, schema, false, false);
        match &result {
            Err(e) => assert!(
                e.to_string().contains("LENGTH_SHOULD_BE_THE_SAME")
                    || e.to_string().contains("Expected 0 fields"),
                "expected LENGTH_SHOULD_BE_THE_SAME: {}",
                e
            ),
            Ok(_) => panic!("expected error for empty schema with non-empty rows"),
        }
    }

    /// #1345: Empty schema + rows with 0 fields each is valid (PySpark: createDataFrame([{}], StructType([])) -> 1 row, 0 cols).
    #[test]
    fn test_issue_1345_empty_schema_empty_rows_parity() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema: Vec<(String, String)> = vec![];
        let rows: Vec<Vec<JsonValue>> = vec![vec![]];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .expect("#1345: empty schema + empty row should succeed");
        assert_eq!(df.count().unwrap(), 1);
        assert_eq!(df.columns().unwrap().len(), 0);
    }

    /// #627: create_dataframe_from_rows accepts map column (dict/object). PySpark MapType parity.
    #[test]
    fn test_create_dataframe_from_rows_map_column() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "integer".to_string()),
            ("m".to_string(), "map<string,string>".to_string()),
        ];
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!(1), json!({"a": "x", "b": "y"})],
            vec![json!(2), json!({"c": "z"})],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        assert_eq!(collected.get_column_names(), &["id", "m"]);
        let m_col = collected.column("m").unwrap();
        let list = m_col.list().unwrap();
        let row0 = list.get(0).unwrap();
        assert_eq!(row0.len(), 2, "row 0 map should have 2 entries");
        let row1 = list.get(1).unwrap();
        assert_eq!(row1.len(), 1, "row 1 map should have 1 entry");
    }

    /// #625: create_dataframe_from_rows accepts array column as JSON array or Object (Python list parity).
    #[test]
    fn test_issue_625_array_column_list_or_object() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("arr".to_string(), "array<bigint>".to_string()),
        ];
        // JSON array (Python list) and Object with "0","1","2" keys (some serializations).
        let rows: Vec<Vec<JsonValue>> = vec![
            vec![json!("x"), json!([1, 2, 3])],
            vec![json!("y"), json!({"0": 4, "1": 5})],
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .expect("#625: array column must accept list/array or object representation");
        assert_eq!(df.count().unwrap(), 2);
        let collected = df.collect_inner().unwrap();
        let list = collected.column("arr").unwrap().list().unwrap();
        assert_eq!(list.get(0).unwrap().len(), 3);
        assert_eq!(list.get(1).unwrap().len(), 2);
    }

    #[test]
    fn test_create_dataframe_empty() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let data: Vec<(i64, i64, String)> = vec![];

        let result = spark.create_dataframe(data, vec!["id", "age", "name"]);

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
    }

    #[test]
    fn test_create_dataframe_from_polars() {
        use polars::prelude::df;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let polars_df = df!(
            "x" => &[1, 2, 3],
            "y" => &[4, 5, 6]
        )
        .unwrap();

        let df = spark.create_dataframe_from_polars(polars_df);

        assert_eq!(df.count().unwrap(), 3);
        let columns = df.columns().unwrap();
        assert!(columns.contains(&"x".to_string()));
        assert!(columns.contains(&"y".to_string()));
    }

    #[test]
    fn test_read_csv_file_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.read_csv("nonexistent_file.csv");

        assert!(result.is_err());
    }

    #[test]
    fn test_read_parquet_file_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.read_parquet("nonexistent_file.parquet");

        assert!(result.is_err());
    }

    #[test]
    fn test_read_json_file_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.read_json("nonexistent_file.json");

        assert!(result.is_err());
    }

    #[test]
    fn test_rust_udf_dataframe() {
        use crate::functions::{call_udf, col};
        use polars::prelude::DataType;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        spark
            .register_udf("to_str", |cols| cols[0].cast(&DataType::String))
            .unwrap();
        let df = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string()), (2, 30, "Bob".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let col = call_udf("to_str", &[col("id")]).unwrap();
        let df2 = df.with_column("id_str", &col).unwrap();
        let cols = df2.columns().unwrap();
        assert!(cols.contains(&"id_str".to_string()));
        let rows = df2.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("id_str").and_then(|v| v.as_str()), Some("1"));
        assert_eq!(rows[1].get("id_str").and_then(|v| v.as_str()), Some("2"));
    }

    #[test]
    fn test_case_insensitive_filter_select() {
        use crate::expression::lit_i64;
        use crate::functions::col;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 25, "Alice".to_string()),
                    (2, 30, "Bob".to_string()),
                    (3, 35, "Charlie".to_string()),
                ],
                vec!["Id", "Age", "Name"],
            )
            .unwrap();
        // Filter with lowercase column names (PySpark default: case-insensitive)
        let filtered = df
            .filter(col("age").gt(lit_i64(26)).expr().clone())
            .unwrap()
            .select(vec!["name"])
            .unwrap();
        assert_eq!(filtered.count().unwrap(), 2);
        let rows = filtered.collect_as_json_rows().unwrap();
        let names: Vec<&str> = rows
            .iter()
            .map(|r| r.get("name").and_then(|v| v.as_str()).unwrap())
            .collect();
        assert!(names.contains(&"Bob"));
        assert!(names.contains(&"Charlie"));
    }

    #[test]
    fn test_sql_returns_error_without_feature_or_unknown_table() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        let result = spark.sql("SELECT * FROM table");

        assert!(result.is_err());
        match result {
            Err(PolarsError::InvalidOperation(msg)) => {
                let s = msg.to_string();
                // Without sql feature: "SQL queries require the 'sql' feature"
                // With sql feature but no table: "Table or view 'table' not found" or parse error
                assert!(
                    s.contains("SQL") || s.contains("Table") || s.contains("feature"),
                    "unexpected message: {s}"
                );
            }
            _ => panic!("Expected InvalidOperation error"),
        }
    }

    #[test]
    fn test_spark_session_stop() {
        let spark = SparkSession::builder().app_name("test").get_or_create();

        // stop() should complete without error
        spark.stop();
    }

    #[test]
    fn test_dataframe_reader_api() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let reader = spark.read();

        // All readers should return errors for non-existent files
        assert!(reader.csv("nonexistent.csv").is_err());
        assert!(reader.parquet("nonexistent.parquet").is_err());
        assert!(reader.json("nonexistent.json").is_err());
    }

    #[test]
    fn test_read_csv_with_valid_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let spark = SparkSession::builder().app_name("test").get_or_create();

        // Create a temporary CSV file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name,age").unwrap();
        writeln!(temp_file, "1,Alice,25").unwrap();
        writeln!(temp_file, "2,Bob,30").unwrap();
        temp_file.flush().unwrap();

        let result = spark.read_csv(temp_file.path());

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 2);

        let columns = df.columns().unwrap();
        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"name".to_string()));
        assert!(columns.contains(&"age".to_string()));
    }

    #[test]
    fn test_read_json_with_valid_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let spark = SparkSession::builder().app_name("test").get_or_create();

        // Create a temporary JSONL file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#"{{"id":1,"name":"Alice"}}"#).unwrap();
        writeln!(temp_file, r#"{{"id":2,"name":"Bob"}}"#).unwrap();
        temp_file.flush().unwrap();

        let result = spark.read_json(temp_file.path());

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 2);
    }

    #[test]
    fn test_read_csv_empty_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let spark = SparkSession::builder().app_name("test").get_or_create();

        // Create an empty CSV file (just header)
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name").unwrap();
        temp_file.flush().unwrap();

        let result = spark.read_csv(temp_file.path());

        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.count().unwrap(), 0);
    }

    #[test]
    fn test_write_partitioned_parquet() {
        use crate::dataframe::{WriteFormat, WriteMode};
        use std::fs;
        use tempfile::TempDir;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 25, "Alice".to_string()),
                    (2, 30, "Bob".to_string()),
                    (3, 25, "Carol".to_string()),
                ],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("out");
        df.write()
            .mode(WriteMode::Overwrite)
            .format(WriteFormat::Parquet)
            .partition_by(["age"])
            .save(&path)
            .unwrap();
        assert!(path.is_dir());
        let entries: Vec<_> = fs::read_dir(&path).unwrap().collect();
        assert_eq!(
            entries.len(),
            2,
            "expected two partition dirs (age=25, age=30)"
        );
        let names: Vec<String> = entries
            .iter()
            .filter_map(|e| e.as_ref().ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().any(|n| n.starts_with("age=")));
        let df_read = spark.read_parquet(&path).unwrap();
        assert_eq!(df_read.count().unwrap(), 3);
    }

    #[test]
    fn test_save_as_table_error_if_exists() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        // First call succeeds
        df.write()
            .save_as_table(&spark, "t1", SaveMode::ErrorIfExists)
            .unwrap();
        assert!(spark.table("t1").is_ok());
        assert_eq!(spark.table("t1").unwrap().count().unwrap(), 1);
        // Second call with ErrorIfExists fails
        let err = df
            .write()
            .save_as_table(&spark, "t1", SaveMode::ErrorIfExists)
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn test_save_as_table_overwrite() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df1 = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let df2 = spark
            .create_dataframe(
                vec![(2, 30, "Bob".to_string()), (3, 35, "Carol".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        df1.write()
            .save_as_table(&spark, "t_over", SaveMode::ErrorIfExists)
            .unwrap();
        assert_eq!(spark.table("t_over").unwrap().count().unwrap(), 1);
        df2.write()
            .save_as_table(&spark, "t_over", SaveMode::Overwrite)
            .unwrap();
        assert_eq!(spark.table("t_over").unwrap().count().unwrap(), 2);
    }

    #[test]
    fn test_save_as_table_append() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df1 = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let df2 = spark
            .create_dataframe(vec![(2, 30, "Bob".to_string())], vec!["id", "age", "name"])
            .unwrap();
        df1.write()
            .save_as_table(&spark, "t_append", SaveMode::ErrorIfExists)
            .unwrap();
        df2.write()
            .save_as_table(&spark, "t_append", SaveMode::Append)
            .unwrap();
        assert_eq!(spark.table("t_append").unwrap().count().unwrap(), 2);
    }

    /// Empty DataFrame with explicit schema: saveAsTable(Overwrite) then append one row (issue #495).
    #[test]
    fn test_save_as_table_empty_df_then_append() {
        use crate::dataframe::SaveMode;
        use serde_json::json;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let empty_df = spark
            .create_dataframe_from_rows(vec![], schema.clone(), false, false)
            .unwrap();
        assert_eq!(empty_df.count().unwrap(), 0);

        empty_df
            .write()
            .save_as_table(&spark, "t_empty_append", SaveMode::Overwrite)
            .unwrap();
        let r1 = spark.table("t_empty_append").unwrap();
        assert_eq!(r1.count().unwrap(), 0);
        let cols = r1.columns().unwrap();
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"name".to_string()));

        let one_row = spark
            .create_dataframe_from_rows(vec![vec![json!(1), json!("a")]], schema, false, false)
            .unwrap();
        one_row
            .write()
            .save_as_table(&spark, "t_empty_append", SaveMode::Append)
            .unwrap();
        let r2 = spark.table("t_empty_append").unwrap();
        assert_eq!(r2.count().unwrap(), 1);
    }

    /// Empty DataFrame with schema: write.format("parquet").save(path) must not fail (issue #519).
    /// PySpark fails with "can not infer schema from empty dataset"; robin-sparkless uses explicit schema.
    #[test]
    fn test_write_parquet_empty_df_with_schema() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let empty_df = spark
            .create_dataframe_from_rows(vec![], schema, false, false)
            .unwrap();
        assert_eq!(empty_df.count().unwrap(), 0);

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("empty.parquet");
        empty_df
            .write()
            .format(crate::dataframe::WriteFormat::Parquet)
            .mode(crate::dataframe::WriteMode::Overwrite)
            .save(&path)
            .unwrap();
        assert!(path.is_file());

        // Read back and verify schema preserved
        let read_df = spark.read().parquet(path.to_str().unwrap()).unwrap();
        assert_eq!(read_df.count().unwrap(), 0);
        let cols = read_df.columns().unwrap();
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"name".to_string()));
    }

    /// Empty DataFrame with schema + warehouse: saveAsTable(Overwrite) then append (issue #495 disk path).
    #[test]
    fn test_save_as_table_empty_df_warehouse_then_append() {
        use crate::dataframe::SaveMode;
        use serde_json::json;
        use std::sync::atomic::{AtomicU64, Ordering};
        use tempfile::TempDir;

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = TempDir::new().unwrap();
        let warehouse = dir.path().join(format!("wh_{n}"));
        std::fs::create_dir_all(&warehouse).unwrap();
        let spark = SparkSession::builder()
            .app_name("test")
            .config(
                "spark.sql.warehouse.dir",
                warehouse.as_os_str().to_str().unwrap(),
            )
            .get_or_create();

        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let empty_df = spark
            .create_dataframe_from_rows(vec![], schema.clone(), false, false)
            .unwrap();
        empty_df
            .write()
            .save_as_table(&spark, "t_empty_wh", SaveMode::Overwrite)
            .unwrap();
        let r1 = spark.table("t_empty_wh").unwrap();
        assert_eq!(r1.count().unwrap(), 0);

        let one_row = spark
            .create_dataframe_from_rows(vec![vec![json!(1), json!("a")]], schema, false, false)
            .unwrap();
        one_row
            .write()
            .save_as_table(&spark, "t_empty_wh", SaveMode::Append)
            .unwrap();
        let r2 = spark.table("t_empty_wh").unwrap();
        assert_eq!(r2.count().unwrap(), 1);
    }

    #[test]
    fn test_save_as_table_ignore() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df1 = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let df2 = spark
            .create_dataframe(vec![(2, 30, "Bob".to_string())], vec!["id", "age", "name"])
            .unwrap();
        df1.write()
            .save_as_table(&spark, "t_ignore", SaveMode::ErrorIfExists)
            .unwrap();
        df2.write()
            .save_as_table(&spark, "t_ignore", SaveMode::Ignore)
            .unwrap();
        // Still 1 row (ignore did not replace)
        assert_eq!(spark.table("t_ignore").unwrap().count().unwrap(), 1);
    }

    #[test]
    fn test_table_resolution_temp_view_first() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df_saved = spark
            .create_dataframe(
                vec![(1, 25, "Saved".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let df_temp = spark
            .create_dataframe(vec![(2, 30, "Temp".to_string())], vec!["id", "age", "name"])
            .unwrap();
        df_saved
            .write()
            .save_as_table(&spark, "x", SaveMode::ErrorIfExists)
            .unwrap();
        spark.create_or_replace_temp_view("x", df_temp);
        // table("x") must return temp view (PySpark order)
        let t = spark.table("x").unwrap();
        let rows = t.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Temp"));
    }

    /// #629: Exact reproduction – createDataFrame, createOrReplaceTempView, then table() must resolve.
    #[test]
    fn test_issue_629_temp_view_visible_after_create() {
        use serde_json::json;

        let spark = SparkSession::builder().app_name("repro").get_or_create();
        let schema = vec![
            ("id".to_string(), "long".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let rows: Vec<Vec<JsonValue>> =
            vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        spark.create_or_replace_temp_view("my_view", df);
        let result = spark
            .table("my_view")
            .unwrap()
            .collect_as_json_rows()
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get("id").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(result[0].get("name").and_then(|v| v.as_str()), Some("a"));
        assert_eq!(result[1].get("id").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(result[1].get("name").and_then(|v| v.as_str()), Some("b"));
    }

    #[test]
    fn test_drop_table() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        df.write()
            .save_as_table(&spark, "t_drop", SaveMode::ErrorIfExists)
            .unwrap();
        assert!(spark.table("t_drop").is_ok());
        assert!(spark.drop_table("t_drop"));
        assert!(spark.table("t_drop").is_err());
        // drop again is no-op, returns false
        assert!(!spark.drop_table("t_drop"));
    }

    /// PR15: list_tables returns DataFrame with "name" column; schema-qualified table names work.
    #[test]
    fn test_list_tables_and_schema_qualified_name() {
        use crate::dataframe::SaveMode;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(vec![(1, 10, "a".to_string())], vec!["id", "x", "name"])
            .unwrap();
        df.write()
            .save_as_table(&spark, "t_plain", SaveMode::ErrorIfExists)
            .unwrap();
        df.write()
            .save_as_table(&spark, "my_schema.my_table", SaveMode::ErrorIfExists)
            .unwrap();

        let list_df = spark.list_tables().unwrap();
        assert_eq!(list_df.count().unwrap(), 2);
        let names: Vec<String> = list_df
            .collect_as_json_rows()
            .unwrap()
            .into_iter()
            .map(|r| {
                r.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        assert!(names.contains(&"t_plain".to_string()));
        assert!(names.contains(&"my_schema.my_table".to_string()));

        assert_eq!(spark.table("t_plain").unwrap().count().unwrap(), 1);
        assert_eq!(
            spark.table("my_schema.my_table").unwrap().count().unwrap(),
            1,
            "schema-qualified table name must resolve"
        );
    }

    /// PR-C: list_databases() returns DataFrame with "name" column; CREATE SCHEMA adds to list.
    /// Only runs when the `sql` feature is enabled (spark.sql() is required).
    #[test]
    #[cfg(feature = "sql")]
    fn test_list_databases_returns_dataframe() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let db_df = spark.list_databases().unwrap();
        let names: Vec<String> = db_df
            .collect_as_json_rows()
            .unwrap()
            .into_iter()
            .map(|r| {
                r.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        assert!(names.contains(&"default".to_string()));
        assert!(names.contains(&"global_temp".to_string()));
        // CREATE SCHEMA adds to list (no IF NOT EXISTS: sqlparser may not support it for CREATE SCHEMA)
        spark
            .sql("CREATE SCHEMA test_schema_for_list_db")
            .expect("CREATE SCHEMA must succeed");
        let db_df2 = spark.list_databases().unwrap();
        let names2: Vec<String> = db_df2
            .collect_as_json_rows()
            .unwrap()
            .into_iter()
            .map(|r| {
                r.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        assert!(
            names2.contains(&"test_schema_for_list_db".to_string()),
            "list_databases() should include schema created by CREATE SCHEMA; got: {:?}",
            names2
        );
    }

    #[test]
    fn test_global_temp_view_persists_across_sessions() {
        // Session 1: create global temp view
        let spark1 = SparkSession::builder().app_name("s1").get_or_create();
        let df1 = spark1
            .create_dataframe(
                vec![(1, 25, "Alice".to_string()), (2, 30, "Bob".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark1.create_or_replace_global_temp_view("people", df1);
        assert_eq!(
            spark1.table("global_temp.people").unwrap().count().unwrap(),
            2
        );

        // Session 2: different session can see global temp view
        let spark2 = SparkSession::builder().app_name("s2").get_or_create();
        let df2 = spark2.table("global_temp.people").unwrap();
        assert_eq!(df2.count().unwrap(), 2);
        let rows = df2.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Alice"));

        // Local temp view in spark2 does not shadow global_temp
        let df_local = spark2
            .create_dataframe(
                vec![(3, 35, "Carol".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark2.create_or_replace_temp_view("people", df_local);
        // table("people") = local temp view (session resolution)
        assert_eq!(spark2.table("people").unwrap().count().unwrap(), 1);
        // table("global_temp.people") = global temp view (unchanged)
        assert_eq!(
            spark2.table("global_temp.people").unwrap().count().unwrap(),
            2
        );

        // Drop global temp view
        assert!(spark2.drop_global_temp_view("people"));
        assert!(spark2.table("global_temp.people").is_err());
    }

    #[test]
    fn test_warehouse_persistence_between_sessions() {
        use crate::dataframe::SaveMode;
        use std::fs;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let warehouse = dir.path().to_str().unwrap();

        // Session 1: save to warehouse
        let spark1 = SparkSession::builder()
            .app_name("w1")
            .config("spark.sql.warehouse.dir", warehouse)
            .get_or_create();
        let df1 = spark1
            .create_dataframe(
                vec![(1, 25, "Alice".to_string()), (2, 30, "Bob".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        df1.write()
            .save_as_table(&spark1, "users", SaveMode::ErrorIfExists)
            .unwrap();
        assert_eq!(spark1.table("users").unwrap().count().unwrap(), 2);

        // Session 2: new session reads from warehouse
        let spark2 = SparkSession::builder()
            .app_name("w2")
            .config("spark.sql.warehouse.dir", warehouse)
            .get_or_create();
        let df2 = spark2.table("users").unwrap();
        assert_eq!(df2.count().unwrap(), 2);
        let rows = df2.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Alice"));

        // Verify parquet was written
        let table_path = dir.path().join("users");
        assert!(table_path.is_dir());
        let entries: Vec<_> = fs::read_dir(&table_path).unwrap().collect();
        assert!(!entries.is_empty());
    }

    #[test]
    #[cfg(feature = "sql")]
    fn test_sql_union_default_distinct() {
        use serde_json::json;
        let spark = SparkSession::builder()
            .app_name("union_distinct")
            .get_or_create();
        let df1 = spark
            .create_dataframe_from_single_column(vec![json!(1), json!(1)], "long")
            .unwrap();
        let df2 = spark
            .create_dataframe_from_single_column(vec![json!(1)], "long")
            .unwrap();
        spark.create_or_replace_temp_view("u1", df1);
        spark.create_or_replace_temp_view("u2", df2);
        let out = spark
            .sql("SELECT value AS x FROM u1 UNION SELECT value AS x FROM u2")
            .unwrap();
        assert_eq!(out.count().unwrap(), 1);
    }

    #[test]
    #[cfg(feature = "sql")]
    fn test_sql_union_all_keeps_duplicates() {
        use serde_json::json;
        let spark = SparkSession::builder()
            .app_name("union_all")
            .get_or_create();
        let df1 = spark
            .create_dataframe_from_single_column(vec![json!(1)], "long")
            .unwrap();
        let df2 = spark
            .create_dataframe_from_single_column(vec![json!(1)], "long")
            .unwrap();
        spark.create_or_replace_temp_view("a1", df1);
        spark.create_or_replace_temp_view("a2", df2);
        let out = spark
            .sql("SELECT value AS x FROM a1 UNION ALL SELECT value AS x FROM a2")
            .unwrap();
        assert_eq!(out.count().unwrap(), 2);
    }

    #[test]
    #[cfg(feature = "delta")]
    fn external_catalog_table_and_reader_resolve_real_delta_snapshot() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("orders");
        let schema = write_test_delta_table(&path);
        let descriptor = external_descriptor(&path, schema);
        let identifier = "main.analytics.orders";
        let (provider, resolutions) = test_external_catalog(identifier, Some(descriptor), None);
        let session = SparkSession::builder()
            .app_name("external-catalog-read")
            .get_or_create();
        session
            .set_external_catalog(Some(catalog_for_test(provider)))
            .expect("set external provider");

        let direct = session
            .table(identifier)
            .expect("resolve via SparkSession.table");
        assert_eq!(direct.count().unwrap(), 1);
        assert_eq!(direct.collect_as_json_rows().unwrap()[0]["name"], "value");

        let reader = session
            .read()
            .table(identifier)
            .expect("resolve via DataFrameReader.table");
        assert_eq!(reader.count().unwrap(), 1);
        assert_eq!(
            *resolutions.lock().expect("test resolution lock"),
            vec![identifier.to_string(), identifier.to_string()]
        );
    }

    #[test]
    #[cfg(feature = "delta")]
    fn external_catalog_table_writes_delegate_typed_ipc_with_modes_and_options() {
        use crate::dataframe::SaveMode;
        use polars::io::ipc::IpcStreamReader;
        use polars::prelude::SerReader;
        use std::io::Cursor;

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("orders");
        let seed_session = SparkSession::builder()
            .app_name("external-catalog-write-seed")
            .get_or_create();
        let seed = typed_external_frame(&seed_session, 1, "1.25");
        let schema = seed.schema().unwrap();
        seed.write_delta(&path, true, false)
            .expect("write real local Delta seed");
        let mut descriptor = external_descriptor(&path, schema);
        descriptor.version = Some(0);

        let identifier = "main.analytics.orders";
        let (provider, resolutions) = test_external_catalog(identifier, Some(descriptor), None);
        let session = SparkSession::builder()
            .app_name("external-catalog-write")
            .get_or_create();
        session
            .set_external_catalog(Some(catalog_for_test(provider.clone())))
            .expect("set external provider");

        let input = typed_external_frame(&session, 2, "12.34");
        let expected = input.collect_inner().unwrap().as_ref().clone();
        input
            .write()
            .option("mergeSchema", "true")
            .option("user.option", "kept")
            .option("sparkless.writer.format", "delta")
            .save_as_table(&session, identifier, SaveMode::Append)
            .expect("delegate append to the external table provider");

        let writes = provider.writes.lock().expect("test write lock");
        assert_eq!(writes.len(), 1);
        let request = &writes[0];
        assert_eq!(request.identifier, identifier);
        assert_eq!(request.table.table_id, "table-123");
        assert_eq!(request.mode, ExternalTableSaveMode::Append);
        assert_eq!(request.format.as_deref(), Some("delta"));
        assert!(
            request
                .options
                .iter()
                .any(|(key, value)| { key.eq_ignore_ascii_case("mergeSchema") && value == "true" })
        );
        assert!(
            request
                .options
                .iter()
                .any(|(key, value)| { key == "user.option" && value == "kept" })
        );
        assert!(
            request
                .options
                .iter()
                .all(|(key, _)| !key.to_ascii_lowercase().starts_with("sparkless."))
        );
        assert!(request.partition_by.is_empty());

        let transmitted = IpcStreamReader::new(Cursor::new(request.data.bytes.clone()))
            .finish()
            .expect("decode captured Arrow IPC stream");
        assert_eq!(transmitted.schema(), expected.schema());
        assert_eq!(expected.column("created").unwrap().null_count(), 0);
        for name in ["id", "amount", "day", "created", "items", "payload"] {
            assert!(
                transmitted
                    .column(name)
                    .unwrap()
                    .equals_missing(expected.column(name).unwrap())
            );
        }
        drop(writes);

        let committed = session
            .table(identifier)
            .expect("read the committed external snapshot");
        assert_eq!(committed.count().unwrap(), 2);
        assert_eq!(
            provider
                .descriptor
                .lock()
                .expect("test descriptor lock")
                .as_ref()
                .and_then(|table| table.version),
            Some(1)
        );
        assert_eq!(
            *resolutions.lock().expect("test resolution lock"),
            vec![identifier.to_string(), identifier.to_string()]
        );

        let overwrite_input = typed_external_frame(&session, 3, "30.00");
        overwrite_input
            .write()
            .option("sparkless.writer.format", "delta")
            .save_as_table(&session, identifier, SaveMode::Overwrite)
            .expect("delegate overwrite to the external table provider");
        {
            let writes = provider.writes.lock().expect("test write lock");
            assert_eq!(writes.len(), 2);
            assert_eq!(writes[1].mode, ExternalTableSaveMode::Overwrite);
            assert_eq!(writes[1].table.version, Some(1));
        }
        assert_eq!(session.table(identifier).unwrap().count().unwrap(), 1);

        *provider.write_error.lock().expect("test write error lock") =
            Some("native commit failed".into());
        let failing_input = typed_external_frame(&session, 4, "99.99");
        let error = failing_input
            .write()
            .mode(crate::dataframe::WriteMode::Append)
            .save_as_table(&session, identifier, SaveMode::Append)
            .expect_err("provider error must stop the write without a local fallback");
        assert!(error.to_string().contains("native commit failed"));
        assert_eq!(
            session.table(identifier).unwrap().count().unwrap(),
            1,
            "failed delegated write must leave the committed table unchanged"
        );
    }

    #[test]
    fn external_catalog_save_modes_do_not_delegate_non_mutating_modes() {
        use crate::dataframe::SaveMode;

        let session = SparkSession::builder()
            .app_name("external-catalog-save-modes")
            .get_or_create();
        let frame = session
            .create_dataframe(vec![(9, 1, "value".into())], vec!["id", "age", "name"])
            .unwrap();
        let schema = frame.schema().unwrap();
        let descriptor =
            external_descriptor(std::path::Path::new("/unused/external-table"), schema);
        let (provider, resolutions) =
            test_external_catalog("main.analytics.orders", Some(descriptor), None);
        session
            .set_external_catalog(Some(catalog_for_test(provider.clone())))
            .expect("set external provider");

        let error = frame
            .write()
            .save_as_table(&session, "main.analytics.orders", SaveMode::ErrorIfExists)
            .expect_err("ErrorIfExists must reject an existing external table");
        assert!(error.to_string().contains("SaveMode is ErrorIfExists"));
        frame
            .write()
            .save_as_table(&session, "main.analytics.orders", SaveMode::Ignore)
            .expect("Ignore leaves an existing external table unchanged");
        assert!(provider.writes.lock().expect("test write lock").is_empty());
        assert_eq!(
            *resolutions.lock().expect("test resolution lock"),
            vec![
                "main.analytics.orders".to_string(),
                "main.analytics.orders".to_string()
            ]
        );
    }

    #[test]
    #[cfg(all(feature = "delta", feature = "sql"))]
    fn sql_reads_external_catalog_relation_through_session_table() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("orders");
        let schema = write_test_delta_table(&path);
        let descriptor = external_descriptor(&path, schema);
        let identifier = "main.analytics.orders";
        let (provider, resolutions) = test_external_catalog(identifier, Some(descriptor), None);
        let session = SparkSession::builder()
            .app_name("external-catalog-sql")
            .get_or_create();
        session
            .set_external_catalog(Some(catalog_for_test(provider)))
            .expect("set external provider");

        let result = session
            .sql("SELECT id, name FROM main.analytics.orders")
            .expect("SQL external table read");
        assert_eq!(result.count().unwrap(), 1);
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows[0]["id"], 7);
        assert_eq!(rows[0]["name"], "value");
        assert_eq!(
            *resolutions.lock().expect("test resolution lock"),
            vec![identifier.to_string()]
        );
    }

    #[test]
    #[cfg(all(feature = "delta", feature = "sql"))]
    fn sql_insert_into_external_catalog_delegates_a_native_append() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("orders");
        let seed_session = SparkSession::builder()
            .app_name("external-catalog-insert-seed")
            .get_or_create();
        let seed = seed_session
            .create_dataframe(
                vec![(7, 10, "value".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        let schema = seed.schema().unwrap();
        seed.write_delta(&path, true, false)
            .expect("write real local Delta seed");
        let mut descriptor = external_descriptor(&path, schema);
        descriptor.version = Some(0);

        let identifier = "main.analytics.orders";
        let (provider, _) = test_external_catalog(identifier, Some(descriptor), None);
        let session = SparkSession::builder()
            .app_name("external-catalog-insert")
            .get_or_create();
        session
            .set_external_catalog(Some(catalog_for_test(provider.clone())))
            .expect("set external provider");

        let statement = session
            .sql("INSERT INTO main.analytics.orders VALUES (8, 20, 'added')")
            .expect("insert into external Delta table");
        assert_eq!(statement.count().unwrap(), 0);
        assert_eq!(session.table(identifier).unwrap().count().unwrap(), 2);

        let writes = provider.writes.lock().expect("test write lock");
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].identifier, identifier);
        assert_eq!(writes[0].mode, ExternalTableSaveMode::Append);
        assert_eq!(writes[0].table.version, Some(0));
        assert!(writes[0].options.is_empty());
    }

    #[test]
    fn external_catalog_provider_errors_are_not_treated_as_misses() {
        let session = SparkSession::builder()
            .app_name("external-catalog-error")
            .get_or_create();
        let local = session
            .create_dataframe(
                vec![(99, 0, "local".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        session.register_table("orders", local);
        let (provider, resolutions) = test_external_catalog(
            "main.analytics.orders",
            None,
            Some("catalog service unavailable"),
        );
        session
            .set_external_catalog(Some(catalog_for_test(provider)))
            .expect("set external provider");

        let error = session
            .table("main.analytics.orders")
            .err()
            .expect("provider failure must surface");
        assert!(error.to_string().contains("catalog service unavailable"));
        assert_eq!(
            *resolutions.lock().expect("test resolution lock"),
            vec!["main.analytics.orders".to_string()]
        );
    }

    #[test]
    fn exact_local_saved_table_shadows_external_catalog_entry() {
        let session = SparkSession::builder()
            .app_name("external-catalog-local-shadow")
            .get_or_create();
        let identifier = "main.analytics.orders";
        let local = session
            .create_dataframe(
                vec![(99, 0, "local".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        local
            .write()
            .save_as_table(
                &session,
                identifier,
                crate::dataframe::SaveMode::ErrorIfExists,
            )
            .expect("save local table");
        let descriptor = external_descriptor(
            std::path::Path::new("/unused/external-table"),
            local.schema().unwrap(),
        );
        let (provider, resolutions) = test_external_catalog(identifier, Some(descriptor), None);
        session
            .set_external_catalog(Some(catalog_for_test(provider)))
            .expect("set external provider");

        let rows = session
            .table(identifier)
            .expect("local saved table should shadow external catalog")
            .collect_as_json_rows()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], 99);
        assert!(
            resolutions.lock().expect("resolution lock").is_empty(),
            "the external provider must not be consulted for an exact local table"
        );
    }

    #[test]
    fn external_catalog_miss_does_not_strip_multipart_name_to_local_table() {
        let session = SparkSession::builder()
            .app_name("external-catalog-miss")
            .get_or_create();
        let local = session
            .create_dataframe(
                vec![(99, 0, "local".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        session.register_table("orders", local);
        let (provider, _) = test_external_catalog("another.catalog.table", None, None);
        session
            .set_external_catalog(Some(catalog_for_test(provider)))
            .expect("set external provider");

        let error = session
            .table("main.analytics.orders")
            .err()
            .expect("multipart miss must not bind unrelated local table");
        assert!(error.to_string().contains("main.analytics.orders"));
    }

    #[test]
    #[cfg(feature = "delta")]
    fn external_catalog_descriptor_schema_mismatch_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("orders");
        let _actual_schema = write_test_delta_table(&path);
        let wrong_schema =
            StructType::from_logical_fields(vec![robin_sparkless_core::LogicalField::new(
                "id".into(),
                CoreDataType::String,
                true,
            )]);
        let descriptor = external_descriptor(&path, wrong_schema);
        let (provider, _) = test_external_catalog("main.analytics.orders", Some(descriptor), None);
        let session = SparkSession::builder()
            .app_name("external-catalog-schema")
            .get_or_create();
        session
            .set_external_catalog(Some(catalog_for_test(provider)))
            .expect("set external provider");

        let error = session
            .table("main.analytics.orders")
            .err()
            .expect("schema mismatch must fail");
        assert!(error.to_string().contains("external table schema mismatch"));
    }

    #[test]
    #[cfg(not(feature = "delta"))]
    fn external_catalog_requires_delta_feature_for_delta_descriptors() {
        let path = std::path::Path::new("/tmp/external-catalog-delta-feature-required");
        let schema =
            StructType::from_logical_fields(vec![robin_sparkless_core::LogicalField::new(
                "id".into(),
                CoreDataType::Long,
                true,
            )]);
        let descriptor = external_descriptor(path, schema);
        let (provider, _) = test_external_catalog("main.analytics.orders", Some(descriptor), None);
        let session = SparkSession::builder()
            .app_name("external-catalog-no-delta-feature")
            .get_or_create();
        session
            .set_external_catalog(Some(catalog_for_test(provider)))
            .expect("set external provider");

        let error = session
            .table("main.analytics.orders")
            .err()
            .expect("Delta descriptor must fail without the optional feature");
        assert!(
            error
                .to_string()
                .contains("external Delta table reads require the 'delta' feature")
        );
    }

    #[test]
    #[cfg(feature = "delta")]
    fn external_catalog_provider_is_not_inherited_by_new_session() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("orders");
        let schema = write_test_delta_table(&path);
        let descriptor = external_descriptor(&path, schema);
        let (provider, resolutions) =
            test_external_catalog("main.analytics.orders", Some(descriptor), None);
        let parent = SparkSession::builder()
            .app_name("external-catalog-parent")
            .get_or_create();
        parent
            .set_external_catalog(Some(catalog_for_test(provider)))
            .expect("set external provider");
        assert_eq!(
            parent
                .table("main.analytics.orders")
                .unwrap()
                .count()
                .unwrap(),
            1
        );

        let child = parent.new_session();
        assert!(child.table("main.analytics.orders").is_err());
        assert_eq!(
            *resolutions.lock().expect("test resolution lock"),
            vec!["main.analytics.orders".to_string()]
        );
    }
}

#[cfg(test)]
mod connect_arrow_tests {
    use super::*;
    use polars_arrow::io::ipc::format::ipc;

    fn shared_schema_stream(
        name: &str,
        field_count: usize,
        metadata_value: &str,
        metadata_count: usize,
    ) -> Vec<u8> {
        let mut builder = ipc::planus::Builder::new();
        let entry = ipc::KeyValue::create(&mut builder, "key", metadata_value);
        let metadata = builder.create_vector(vec![entry; metadata_count].as_slice());
        let dtype = ipc::Type::create_int(
            &mut builder,
            &ipc::Int {
                bit_width: 64,
                is_signed: true,
            },
        );
        let field = ipc::Field::create(&mut builder, name, true, dtype, (), (), ());
        let schema = ipc::Schema::create(
            &mut builder,
            ipc::Endianness::Little,
            vec![field; field_count].as_slice(),
            metadata,
            (),
        );
        let header = ipc::MessageHeader::create_schema(&mut builder, schema);
        let message = ipc::Message::create(&mut builder, ipc::MetadataVersion::V5, header, 0, ());
        let metadata = builder.finish(message, None);
        let padded_len = (metadata.len() + 7) & !7;
        let mut stream = vec![0xff; 4];
        stream.extend_from_slice(&(padded_len as i32).to_le_bytes());
        stream.extend_from_slice(metadata);
        stream.resize(8 + padded_len, 0);
        stream.extend_from_slice(&[0xff; 4]);
        stream.extend_from_slice(&0i32.to_le_bytes());
        stream
    }

    #[test]
    fn connect_arrow_preflight_bounds_shared_metadata_before_owning_the_schema() {
        let stream = shared_schema_stream("value", 1, &"v".repeat(1024), 64);
        assert!(stream.len() < 4096, "wire data shares the metadata entry");
        let error = preflight_connect_arrow_stream(&stream, 10_000, 8 * 1024 * 1024)
            .unwrap_err()
            .to_string();
        assert!(error.contains("metadata exceeds"), "{error}");
    }

    #[test]
    fn connect_arrow_preflight_bounds_shared_field_names_before_owning_the_schema() {
        let stream = shared_schema_stream(&"x".repeat(65_537), 128, "", 0);
        assert!(stream.len() < 70_000, "wire data shares the field table");
        let error = preflight_connect_arrow_stream(&stream, 10_000, 8 * 1024 * 1024)
            .unwrap_err()
            .to_string();
        assert!(error.contains("expanded schema exceeds"), "{error}");
    }

    #[test]
    fn connect_arrow_preflight_rejects_buffer_ranges_and_decoded_byte_overflow() {
        for (buffer_length, budget, expected_error) in [
            (16 * 1024 * 1024, 8 * 1024 * 1024, "extends past"),
            (8, 4, "decoded bytes"),
        ] {
            let mut stream = shared_schema_stream("x", 1, "", 0);
            stream.truncate(stream.len() - 8);
            let message = ipc::Message {
                version: ipc::MetadataVersion::V5,
                header: Some(ipc::MessageHeader::RecordBatch(Box::new(
                    ipc::RecordBatch {
                        length: 1,
                        nodes: Some(vec![ipc::FieldNode {
                            length: 1,
                            null_count: 0,
                        }]),
                        buffers: Some(vec![
                            ipc::Buffer {
                                offset: 0,
                                length: 0,
                            },
                            ipc::Buffer {
                                offset: 0,
                                length: buffer_length,
                            },
                        ]),
                        ..Default::default()
                    },
                ))),
                body_length: 8,
                ..Default::default()
            };
            let mut builder = ipc::planus::Builder::new();
            let metadata = builder.finish(&message, None);
            let padded_len = (metadata.len() + 7) & !7;
            let metadata_start = stream.len();
            stream.extend_from_slice(&[0xff; 4]);
            stream.extend_from_slice(&(padded_len as i32).to_le_bytes());
            stream.extend_from_slice(metadata);
            stream.resize(metadata_start + 8 + padded_len, 0);
            stream.extend_from_slice(&7i64.to_le_bytes());
            stream.extend_from_slice(&[0xff; 4]);
            stream.extend_from_slice(&0i32.to_le_bytes());
            let error = preflight_connect_arrow_stream(&stream, 10_000, budget)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected_error), "{error}");
        }
    }

    #[test]
    fn empty_connect_local_relation_streams_typed_schema_without_lazy_scan() {
        use polars::io::ipc::IpcStreamReader;

        let session = SparkSession::builder().get_or_create();
        let schema = StructType::from_logical_fields(vec![
            StructField::new("id".into(), CoreDataType::Long, false),
            StructField::new("label".into(), CoreDataType::String, true),
        ]);
        let (frame, imported_schema) = session
            .create_dataframe_from_arrow_ipc(None, Some(schema.clone()), 1024, 10, 1024, 512)
            .unwrap();
        assert!(frame.is_eager());
        assert_eq!(imported_schema.logical_fields().len(), 2);
        assert_eq!(imported_schema.logical_fields()[0].name, "id");
        assert_eq!(imported_schema.logical_fields()[1].name, "label");

        let mut batches = frame
            .to_arrow_ipc_batches_with_schema_and_cancellation(
                10,
                1024 * 1024,
                &robin_sparkless_core::engine::CancellationToken::new(),
                &schema,
            )
            .unwrap();
        let batch = batches.next().unwrap().unwrap();
        assert_eq!(batch.row_count, 0);
        let decoded = IpcStreamReader::new(Cursor::new(batch.bytes))
            .finish()
            .expect("decode empty Connect Arrow batch");
        assert_eq!(decoded.height(), 0);
        let column_names = decoded
            .get_column_names()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        assert_eq!(column_names, ["id", "label"]);
        assert!(batches.next().is_none());
    }

    #[test]
    fn connect_arrow_import_preserves_duplicate_fields_with_and_without_a_schema() {
        let session = SparkSession::builder().get_or_create();
        let native = PlDataFrame::new_infer_height(vec![
            Series::new("first".into(), vec![7i64]).into(),
            Series::new("second".into(), vec![11i64]).into(),
        ])
        .unwrap();
        let source = session.create_dataframe_from_polars(native);
        let schema = StructType::from_logical_fields(vec![
            StructField::new("duplicate".into(), CoreDataType::Long, true),
            StructField::new("duplicate".into(), CoreDataType::Long, true),
        ]);
        let mut batches = source
            .to_arrow_ipc_batches_with_schema_and_cancellation(
                10,
                1024 * 1024,
                &robin_sparkless_core::engine::CancellationToken::new(),
                &schema,
            )
            .unwrap();
        let data = batches.next().unwrap().unwrap().bytes;
        for declared_schema in [None, Some(schema)] {
            let (frame, imported_schema) = session
                .create_dataframe_from_arrow_ipc(
                    Some(&data),
                    declared_schema,
                    1024 * 1024,
                    10_000,
                    8 * 1024 * 1024,
                    512 * 1024,
                )
                .unwrap();
            let frame = frame.collect().unwrap();
            assert_eq!(imported_schema.logical_fields().len(), 2);
            assert!(
                imported_schema
                    .logical_fields()
                    .iter()
                    .all(|field| field.name == "duplicate")
            );
            assert_eq!(frame.width(), 2);
            assert_eq!(
                frame.columns()[0]
                    .as_materialized_series()
                    .i64()
                    .unwrap()
                    .get(0),
                Some(7)
            );
            assert_eq!(
                frame.columns()[1]
                    .as_materialized_series()
                    .i64()
                    .unwrap()
                    .get(0),
                Some(11)
            );
        }
    }
}
