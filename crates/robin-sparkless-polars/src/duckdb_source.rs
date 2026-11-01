//! Optional, read-only DuckDB catalog integration for Polars.
//!
//! The native source currently supports local DuckDB files. The catalog never accepts a path
//! from a table identifier: the server supplies a trusted alias-to-path map. A statement uses
//! one DuckDB read transaction and exports typed Arrow IPC batches before Polars evaluates its
//! relational plan.

use duckdb::{AccessMode, Config, Connection, InterruptHandle, params};
use robin_sparkless_core::engine::{
    ArrowIpcStream, CancellationToken, ExternalCatalogProvider, ExternalCatalogReadSnapshot,
    ExternalTableDescriptor, ExternalTableFormat,
};
use robin_sparkless_core::{DataType, EngineError, LogicalField, StructType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Maximum number of source rows admitted by one DuckDB-backed Connect operation.
pub const DUCKDB_SOURCE_MAX_ROWS: u64 = 1_000_000;
/// Maximum combined Arrow IPC input bytes admitted by one source operation.
pub const DUCKDB_SOURCE_MAX_IPC_BYTES: usize = 32 * 1024 * 1024;
/// Maximum decoded source data retained across all tables in one statement.
pub const DUCKDB_SOURCE_MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;
/// Maximum size of one independently readable Arrow IPC source batch.
pub const DUCKDB_SOURCE_MAX_BATCH_BYTES: usize = 1024 * 1024;
/// Maximum rows accepted in one DuckDB-produced source batch.
pub const DUCKDB_SOURCE_MAX_BATCH_ROWS: usize = 16_384;

/// Trusted server configuration for a read-only DuckDB database alias.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbSourceConfig {
    /// Prefix clients use in table names, such as `warehouse` in `warehouse.main.events`.
    pub alias: String,
    /// Path to an existing DuckDB database file. It is opened read-only.
    pub database_path: PathBuf,
}

#[derive(Debug, Clone)]
struct ConfiguredSource {
    alias: String,
    database_path: PathBuf,
}

/// Read-only DuckDB table resolver. This source is opt-in through the `duckdb-source` feature.
pub struct DuckDbCatalog {
    sources: HashMap<String, Arc<ConfiguredSource>>,
}

impl DuckDbCatalog {
    /// Validate and freeze trusted source aliases and paths.
    pub fn new(configs: impl IntoIterator<Item = DuckDbSourceConfig>) -> Result<Self, EngineError> {
        let mut sources = HashMap::new();
        for config in configs {
            if !valid_identifier(&config.alias) {
                return Err(EngineError::User(format!(
                    "DuckDB source alias {:?} must be a simple identifier",
                    config.alias
                )));
            }
            let alias = config.alias.to_ascii_lowercase();
            if sources.contains_key(&alias) {
                return Err(EngineError::User(format!(
                    "duplicate DuckDB source alias '{alias}'"
                )));
            }
            let database_path = std::fs::canonicalize(&config.database_path).map_err(|error| {
                EngineError::User(format!(
                    "DuckDB source '{}' database path is unavailable: {error}",
                    config.alias
                ))
            })?;
            if !database_path.is_file() {
                return Err(EngineError::User(format!(
                    "DuckDB source '{}' path is not a database file",
                    config.alias
                )));
            }
            sources.insert(
                alias.clone(),
                Arc::new(ConfiguredSource {
                    alias,
                    database_path,
                }),
            );
        }
        if sources.is_empty() {
            return Err(EngineError::User(
                "at least one DuckDB source alias is required".into(),
            ));
        }
        Ok(Self { sources })
    }

    /// Parse the JSON value used by `SPARKLESS_DUCKDB_SOURCES`.
    pub fn from_json(json: &str) -> Result<Self, EngineError> {
        let configs: Vec<DuckDbSourceConfig> = serde_json::from_str(json).map_err(|error| {
            EngineError::User(format!("invalid SPARKLESS_DUCKDB_SOURCES JSON: {error}"))
        })?;
        Self::new(configs)
    }

    fn source_for(&self, alias: &str) -> Result<Arc<ConfiguredSource>, EngineError> {
        self.sources
            .get(&alias.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| {
                EngineError::User(format!("DuckDB source alias '{alias}' is not configured"))
            })
    }
}

impl ExternalCatalogProvider for DuckDbCatalog {
    fn resolve_table(
        &self,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
        let Some((source, schema_name, table_name)) = self.parse_identifier(identifier)? else {
            return Ok(None);
        };
        let snapshot = DuckDbReadSnapshot::new(self.sources.clone());
        let mut state = snapshot
            .state
            .lock()
            .map_err(|_| EngineError::Internal("DuckDB snapshot lock poisoned".into()))?;
        snapshot.resolve_in_source(&mut state, source, &schema_name, &table_name)
    }

    fn begin_read_snapshot(
        &self,
        token: &CancellationToken,
    ) -> Result<Option<Arc<dyn ExternalCatalogReadSnapshot>>, EngineError> {
        token.check_cancelled()?;
        Ok(Some(Arc::new(DuckDbReadSnapshot::new(
            self.sources.clone(),
        ))))
    }

    fn scan_table(
        &self,
        table: &ExternalTableDescriptor,
        token: &CancellationToken,
        consume: &mut dyn FnMut(ArrowIpcStream) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let snapshot = DuckDbReadSnapshot::new(self.sources.clone());
        snapshot.scan_table(table, token, consume)
    }
}

impl DuckDbCatalog {
    fn parse_identifier(
        &self,
        identifier: &str,
    ) -> Result<Option<(Arc<ConfiguredSource>, String, String)>, EngineError> {
        let parts = identifier.split('.').collect::<Vec<_>>();
        let (alias, schema, table) = match parts.as_slice() {
            [alias, table] => (*alias, "main", *table),
            [alias, schema, table] => (*alias, *schema, *table),
            _ => return Ok(None),
        };
        if !valid_identifier(alias) || !valid_identifier(schema) || !valid_identifier(table) {
            return Ok(None);
        }
        let source = self.source_for(alias)?;
        Ok(Some((source, schema.to_string(), table.to_string())))
    }
}

struct DuckDbSnapshotState {
    connection: Option<Connection>,
    database_path: Option<PathBuf>,
}

struct DuckDbReadSnapshot {
    sources: HashMap<String, Arc<ConfiguredSource>>,
    state: Mutex<DuckDbSnapshotState>,
    rows: AtomicU64,
    ipc_bytes: AtomicUsize,
    decoded_bytes: AtomicUsize,
    failed: AtomicBool,
}

impl DuckDbReadSnapshot {
    fn new(sources: HashMap<String, Arc<ConfiguredSource>>) -> Self {
        Self {
            sources,
            state: Mutex::new(DuckDbSnapshotState {
                connection: None,
                database_path: None,
            }),
            rows: AtomicU64::new(0),
            ipc_bytes: AtomicUsize::new(0),
            decoded_bytes: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
        }
    }

    fn source_for(&self, alias: &str) -> Result<Arc<ConfiguredSource>, EngineError> {
        self.sources
            .get(&alias.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| {
                EngineError::User(format!("DuckDB source alias '{alias}' is not configured"))
            })
    }

    fn ensure_connection<'a>(
        &self,
        state: &'a mut DuckDbSnapshotState,
        source: &ConfiguredSource,
    ) -> Result<&'a Connection, EngineError> {
        if let Some(path) = state.database_path.as_ref() {
            if path != &source.database_path {
                return Err(EngineError::User(
                    "one Connect statement cannot read DuckDB tables from different database owners"
                        .into(),
                ));
            }
            return Ok(state
                .connection
                .as_ref()
                .expect("path and connection match"));
        }
        let connection = open_read_only(&source.database_path)?;
        connection
            .execute_batch("BEGIN TRANSACTION")
            .map_err(duckdb_failure)?;
        state.database_path = Some(source.database_path.clone());
        state.connection = Some(connection);
        Ok(state.connection.as_ref().expect("connection just inserted"))
    }

    fn resolve_in_source(
        &self,
        state: &mut DuckDbSnapshotState,
        source: Arc<ConfiguredSource>,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
        let connection = self.ensure_connection(state, &source)?;
        let sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
                   WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?) \
                   ORDER BY ordinal_position";
        let mut statement = connection.prepare(&sql).map_err(duckdb_failure)?;
        let described = statement
            .query(params![schema_name, table_name])
            .map_err(duckdb_failure)?;
        let mut rows = described;
        let mut fields = Vec::new();
        while let Some(row) = rows.next().map_err(duckdb_failure)? {
            let name: String = row.get(0).map_err(duckdb_failure)?;
            let type_name: String = row.get(1).map_err(duckdb_failure)?;
            let nullability: String = row.get(2).map_err(duckdb_failure)?;
            fields.push(LogicalField::new(
                name,
                duckdb_type_to_spark(&type_name)?,
                !nullability.eq_ignore_ascii_case("NO"),
            ));
        }
        if fields.is_empty() {
            return Ok(None);
        }
        let path = source
            .database_path
            .to_str()
            .ok_or_else(|| EngineError::User("DuckDB database paths must be valid UTF-8".into()))?;
        let descriptor = ExternalTableDescriptor {
            table_id: format!("duckdb:{}:{schema_name}:{table_name}", source.alias),
            canonical_location: path.to_string(),
            schema: StructType::from_logical_fields(fields),
            version: None,
            provider: "duckdb-rs-1.20000.1".into(),
            permissions_context: Some(
                serde_json::json!({"alias": source.alias, "schema": schema_name, "table": table_name})
                    .to_string(),
            ),
            format: ExternalTableFormat::DuckDb,
        };
        descriptor.validate()?;
        Ok(Some(descriptor))
    }
}

impl ExternalCatalogReadSnapshot for DuckDbReadSnapshot {
    fn resolve_table(
        &self,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
        let parts = identifier.split('.').collect::<Vec<_>>();
        let (alias, schema, table) = match parts.as_slice() {
            [alias, table] => (*alias, "main", *table),
            [alias, schema, table] => (*alias, *schema, *table),
            _ => return Ok(None),
        };
        if !valid_identifier(alias) || !valid_identifier(schema) || !valid_identifier(table) {
            return Ok(None);
        }
        let source = self.source_for(alias)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| EngineError::Internal("DuckDB snapshot lock poisoned".into()))?;
        self.resolve_in_source(&mut state, source, schema, table)
    }

    fn scan_table(
        &self,
        table: &ExternalTableDescriptor,
        token: &CancellationToken,
        consume: &mut dyn FnMut(ArrowIpcStream) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let result = (|| {
            table.validate()?;
            if table.format != ExternalTableFormat::DuckDb {
                return Err(EngineError::User(
                    "DuckDB source received a descriptor for another table format".into(),
                ));
            }
            let context: serde_json::Value =
                serde_json::from_str(table.permissions_context.as_deref().ok_or_else(|| {
                    EngineError::User("DuckDB table descriptor is incomplete".into())
                })?)
                .map_err(|error| {
                    EngineError::User(format!("invalid DuckDB table context: {error}"))
                })?;
            let alias = context["alias"].as_str().ok_or_else(|| {
                EngineError::User("DuckDB table descriptor is missing its source alias".into())
            })?;
            let schema = context["schema"].as_str().ok_or_else(|| {
                EngineError::User("DuckDB table descriptor is missing its schema".into())
            })?;
            let table_name = context["table"].as_str().ok_or_else(|| {
                EngineError::User("DuckDB table descriptor is missing its table name".into())
            })?;
            if !valid_identifier(alias)
                || !valid_identifier(schema)
                || !valid_identifier(table_name)
            {
                return Err(EngineError::User(
                    "DuckDB table descriptor contains an invalid identifier".into(),
                ));
            }
            let source = self.source_for(alias)?;
            if table.canonical_location != source.database_path.to_string_lossy() {
                return Err(EngineError::User(
                    "DuckDB table descriptor does not match its configured source path".into(),
                ));
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| EngineError::Internal("DuckDB snapshot lock poisoned".into()))?;
            let connection = self.ensure_connection(&mut state, &source)?;
            token.check_cancelled()?;
            let _watchdog =
                CancellationWatchdog::new(token.clone(), connection.interrupt_handle())?;
            let sql = format!(
                "SELECT * FROM {}.{}",
                quote_identifier(schema),
                quote_identifier(table_name)
            );
            let mut statement = connection.prepare(&sql).map_err(duckdb_failure)?;
            let mut batches = statement.stream_arrow([]).map_err(duckdb_failure)?;
            while let Some(batch) = batches.next() {
                token.check_cancelled()?;
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(error) => {
                        token.check_cancelled()?;
                        return Err(duckdb_failure(error));
                    }
                };
                if batch.num_rows() > DUCKDB_SOURCE_MAX_BATCH_ROWS {
                    self.failed.store(true, Ordering::Release);
                    return Err(EngineError::User(format!(
                        "DuckDB source batch exceeds {DUCKDB_SOURCE_MAX_BATCH_ROWS} rows"
                    )));
                }
                validate_batch_scalars(&batch)?;
                let bytes = encode_batch(&batch)?;
                if bytes.len() > DUCKDB_SOURCE_MAX_BATCH_BYTES {
                    self.failed.store(true, Ordering::Release);
                    return Err(EngineError::User(format!(
                        "DuckDB source batch exceeds {DUCKDB_SOURCE_MAX_BATCH_BYTES} IPC bytes"
                    )));
                }
                let next_rows = self
                    .rows
                    .load(Ordering::Acquire)
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| EngineError::User("DuckDB source row count overflow".into()))?;
                let next_bytes = self
                    .ipc_bytes
                    .load(Ordering::Acquire)
                    .checked_add(bytes.len())
                    .ok_or_else(|| {
                        EngineError::User("DuckDB source IPC byte count overflow".into())
                    })?;
                if next_rows > DUCKDB_SOURCE_MAX_ROWS {
                    self.failed.store(true, Ordering::Release);
                    return Err(EngineError::User(format!(
                        "DuckDB source exceeds the {DUCKDB_SOURCE_MAX_ROWS}-row operation limit"
                    )));
                }
                if next_bytes > DUCKDB_SOURCE_MAX_IPC_BYTES {
                    self.failed.store(true, Ordering::Release);
                    return Err(EngineError::User(format!(
                        "DuckDB source exceeds the {DUCKDB_SOURCE_MAX_IPC_BYTES}-byte operation limit"
                    )));
                }
                self.rows.store(next_rows, Ordering::Release);
                self.ipc_bytes.store(next_bytes, Ordering::Release);
                consume(ArrowIpcStream {
                    bytes,
                    row_count: batch.num_rows() as u64,
                })?;
            }
            token.check_cancelled()?;
            Ok(())
        })();
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }
    fn charge_decoded_bytes(&self, bytes: usize) -> Result<(), EngineError> {
        match self
            .decoded_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= DUCKDB_SOURCE_MAX_DECODED_BYTES)
            }) {
            Ok(_) => Ok(()),
            Err(_) => {
                self.failed.store(true, Ordering::Release);
                Err(EngineError::User(format!(
                    "DuckDB source decoded data exceeds the {DUCKDB_SOURCE_MAX_DECODED_BYTES}-byte statement limit"
                )))
            }
        }
    }
}

/// Interrupts an in-flight DuckDB query promptly when its operation token is cancelled.
struct CancellationWatchdog {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl CancellationWatchdog {
    fn new(token: CancellationToken, interrupt: Arc<InterruptHandle>) -> Result<Self, EngineError> {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker = std::thread::Builder::new()
            .name("sparkless-duckdb-cancel".into())
            .spawn(move || {
                while !worker_stop.load(Ordering::Acquire) {
                    if token.is_cancelled() {
                        interrupt.interrupt();
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
            .map_err(|error| {
                EngineError::Internal(format!("cannot start DuckDB cancellation watcher: {error}"))
            })?;
        Ok(Self {
            stop,
            worker: Some(worker),
        })
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for CancellationWatchdog {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Drop for DuckDbReadSnapshot {
    fn drop(&mut self) {
        let Ok(state) = self.state.get_mut() else {
            return;
        };
        if let Some(connection) = state.connection.as_ref() {
            let command = if self.failed.load(Ordering::Acquire) {
                "ROLLBACK"
            } else {
                "COMMIT"
            };
            if let Err(error) = connection.execute_batch(command) {
                eprintln!("sparkless DuckDB source {command} failed: {error}");
            }
        }
    }
}

fn open_read_only(path: &Path) -> Result<Connection, EngineError> {
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(duckdb_failure)?;
    let connection = Connection::open_with_flags(path, config).map_err(duckdb_failure)?;
    connection
        .execute_batch("SET TimeZone = 'UTC'; SET threads = 1; SET memory_limit = '256MB';")
        .map_err(duckdb_failure)?;
    Ok(connection)
}

fn validate_batch_scalars(
    batch: &duckdb::arrow::record_batch::RecordBatch,
) -> Result<(), EngineError> {
    use arrow60::array::{Array, Float64Array, StringArray};
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        match field.data_type() {
            arrow60::datatypes::DataType::Float64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        EngineError::User(format!(
                            "DuckDB DOUBLE column {:?} has an unexpected Arrow representation",
                            field.name()
                        ))
                    })?;
                if (0..values.len())
                    .any(|index| !values.is_null(index) && !values.value(index).is_finite())
                {
                    return Err(EngineError::User(format!(
                        "DuckDB DOUBLE column {:?} contains a non-finite value",
                        field.name()
                    )));
                }
            }
            arrow60::datatypes::DataType::Utf8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        EngineError::User(format!(
                            "DuckDB VARCHAR column {:?} has an unexpected Arrow representation",
                            field.name()
                        ))
                    })?;
                if (0..values.len())
                    .any(|index| !values.is_null(index) && values.value(index).len() > 512 * 1024)
                {
                    return Err(EngineError::User(format!(
                        "DuckDB VARCHAR column {:?} contains a value larger than 512 KiB",
                        field.name()
                    )));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn duckdb_type_to_spark(type_name: &str) -> Result<DataType, EngineError> {
    let normalized = type_name.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "BOOLEAN" => Ok(DataType::Boolean),
        "SMALLINT" | "INT2" => Ok(DataType::Short),
        "INTEGER" | "INT" | "INT4" | "SIGNED" => Ok(DataType::Integer),
        "BIGINT" | "INT8" | "LONG" => Ok(DataType::Long),
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => Ok(DataType::Double),
        "VARCHAR" | "STRING" | "TEXT" => Ok(DataType::String),
        "DATE" => Ok(DataType::Date),
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => Ok(DataType::Timestamp),
        _ if normalized.starts_with("DECIMAL(") && normalized.ends_with(')') => {
            let inner = &normalized[8..normalized.len() - 1];
            let (precision, scale) = inner.split_once(',').ok_or_else(|| {
                EngineError::User(format!("unsupported DuckDB decimal type '{type_name}'"))
            })?;
            let precision: u8 = precision.trim().parse().map_err(|_| {
                EngineError::User(format!("unsupported DuckDB decimal type '{type_name}'"))
            })?;
            let scale: u8 = scale.trim().parse().map_err(|_| {
                EngineError::User(format!("unsupported DuckDB decimal type '{type_name}'"))
            })?;
            if precision == 0 || precision > 38 || scale > precision {
                return Err(EngineError::User(format!(
                    "DuckDB decimal type '{type_name}' is outside the supported precision/scale range"
                )));
            }
            Ok(DataType::Decimal(precision, scale))
        }
        _ => Err(EngineError::User(format!(
            "DuckDB type '{type_name}' is not in the supported source type set"
        ))),
    }
}

fn encode_batch(batch: &duckdb::arrow::record_batch::RecordBatch) -> Result<Vec<u8>, EngineError> {
    use arrow60::ipc::writer::StreamWriter;
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).map_err(|error| {
            EngineError::User(format!("cannot encode DuckDB Arrow schema: {error}"))
        })?;
        writer.write(batch).map_err(|error| {
            EngineError::User(format!("cannot encode DuckDB Arrow batch: {error}"))
        })?;
        writer.finish().map_err(|error| {
            EngineError::User(format!("cannot finish DuckDB Arrow batch: {error}"))
        })?;
    }
    Ok(bytes)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn valid_identifier(identifier: &str) -> bool {
    let mut chars = identifier.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn duckdb_failure(error: impl std::fmt::Display) -> EngineError {
    EngineError::User(format!("DuckDB source operation failed: {error}"))
}
