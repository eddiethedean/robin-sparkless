//! One writable DuckDB owner used by the optional Spark Connect execution adapter.

use crate::config::{
    ConfiguredOwner, resolve_quack_extension_path, valid_identifier, validate_quack_uri,
};
use crate::connection::{configure_connection, duckdb_failure, quack_connection_id_for};
use crate::scan::{
    encode_batch, quote_identifier, quote_literal, validate_execution_batch_scalars,
};
use crate::snapshot::{CancellationWatchdog, QuackCancellation};
use arrow60::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, TimeUnit};
use duckdb::Connection;
use duckdb::ToSql;
use duckdb::arrow::ipc::reader::StreamReader;
use duckdb::vtab::arrow::{ArrowBatchRegistration, ArrowVTab};
use robin_sparkless_core::engine::{ArrowIpcStream, CancellationToken};
use robin_sparkless_core::{DataType, EngineError, LogicalField, StructType};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::io::Cursor;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

const EXECUTION_CATALOG: &str = "__sparkless_execution";
const EXECUTION_RECEIPTS_SCHEMA: &str = "__sparkless_meta";
const EXECUTION_RECEIPTS_TABLE: &str = "connect_operation_receipts";
pub const DUCKDB_EXECUTION_MAX_BATCH_ROWS: usize = 16_384;
pub const DUCKDB_EXECUTION_MAX_BATCH_BYTES: usize = 1024 * 1024;
/// Persistent idempotency receipts are bounded; new writes are rejected after this count.
pub const DUCKDB_EXECUTION_MAX_RECEIPTS: u64 = 1_000_000;
/// Maximum simultaneous read connections used by the DuckDB Connect execution owner.
pub const DUCKDB_EXECUTION_MAX_READ_CONNECTIONS: usize = 4;
/// Maximum read operations queued while all execution query connections are busy.
pub const DUCKDB_EXECUTION_MAX_QUEUED_READS: usize = 16;

/// Limits and schema expectations for one Arrow IPC query operation.
pub struct ArrowIpcQueryOptions<'a> {
    /// Validated SQL submitted to the execution owner.
    pub sql: &'a str,
    /// Arrow IPC inputs referenced by `arrow(?)` in the query.
    pub inputs: &'a [Vec<u8>],
    /// Optional Spark schemas paired with the Arrow IPC inputs.
    pub expected_input_schemas: &'a [Option<StructType>],
    /// Optional analyzed output schema that the query must match.
    pub expected_output_schema: Option<&'a StructType>,
    /// Maximum combined bytes accepted across all encoded inputs.
    pub max_input_bytes: usize,
    /// Maximum combined rows decoded from all inputs.
    pub max_input_rows: usize,
    /// Maximum decoded bytes retained across all input batches.
    pub max_input_decoded_bytes: usize,
    /// Cancellation token checked throughout execution.
    pub token: &'a CancellationToken,
    /// Maximum rows in an independently readable output batch.
    pub max_batch_rows: usize,
    /// Maximum encoded bytes in an output batch.
    pub max_batch_bytes: usize,
}

/// Receipt result returned by one persistent DuckDB transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuckDbTransactionOutcome {
    /// The statement and its durable request receipt committed together.
    Committed,
    /// The matching operation receipt already existed; the statement was not run again.
    Replayed,
}

/// Trusted, process-startup configuration for the database that owns Connect writes.
#[derive(Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum DuckDbExecutionOwnerConfig {
    /// A single process-owned persistent database. Do not open this path for writing from another
    /// process; use a shared Quack owner when independent clients must write concurrently.
    Local { database_path: PathBuf },
    /// A Quack endpoint that is the authoritative shared owner for both Sparkless and clients.
    Quack {
        quack_uri: String,
        token: String,
        #[serde(default)]
        extension_path: PathBuf,
        #[serde(default)]
        disable_ssl: Option<bool>,
        #[serde(default)]
        ssl_fingerprint: Option<String>,
    },
}

impl std::fmt::Debug for DuckDbExecutionOwnerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { database_path } => formatter
                .debug_struct("DuckDbExecutionOwnerConfig::Local")
                .field("database_path", database_path)
                .finish(),
            Self::Quack {
                quack_uri,
                token: _,
                extension_path,
                disable_ssl,
                ssl_fingerprint,
            } => formatter
                .debug_struct("DuckDbExecutionOwnerConfig::Quack")
                .field("quack_uri", quack_uri)
                .field("token", &"[redacted]")
                .field("extension_path", extension_path)
                .field("disable_ssl", disable_ssl)
                .field(
                    "ssl_fingerprint",
                    &ssl_fingerprint.as_ref().map(|_| "[redacted]"),
                )
                .finish(),
        }
    }
}

/// A serialized DuckDB connection with bounded Arrow query streaming and transactional commands.
/// The mutex makes connection ownership explicit: only one operation uses this connection at a
/// time, and the connection is never shared as a writable file across processes.
pub struct DuckDbExecutionOwner {
    connection: Mutex<Connection>,
    read_connections: Arc<ExecutionReadConnectionPool>,
    quack_connection_needs_reopen: AtomicBool,
    owner: ConfiguredOwner,
    local_database: Option<Arc<SharedLocalDatabase>>,
}

impl DuckDbExecutionOwner {
    pub fn from_json(json: &str) -> Result<Self, EngineError> {
        let config: DuckDbExecutionOwnerConfig = serde_json::from_str(json).map_err(|error| {
            EngineError::invalid_input(format!("invalid DuckDB execution owner JSON: {error}"))
        })?;
        Self::open(config)
    }

    pub fn open(config: DuckDbExecutionOwnerConfig) -> Result<Self, EngineError> {
        let owner = match config {
            DuckDbExecutionOwnerConfig::Local { database_path } => ConfiguredOwner::Local {
                database_path: prepare_database_path(&database_path)?,
            },
            DuckDbExecutionOwnerConfig::Quack {
                quack_uri,
                token,
                extension_path,
                disable_ssl,
                ssl_fingerprint,
            } => {
                validate_quack_uri(&quack_uri)?;
                if token.trim().len() < 4 {
                    return Err(EngineError::invalid_input(
                        "DuckDB execution Quack token must contain at least four characters".into(),
                    ));
                }
                if disable_ssl == Some(true) && ssl_fingerprint.is_some() {
                    return Err(EngineError::invalid_input(
                        "DuckDB execution Quack owner cannot set an SSL fingerprint when SSL is disabled"
                            .into(),
                    ));
                }
                let extension_path =
                    resolve_quack_extension_path(&extension_path).map_err(|error| {
                        EngineError::invalid_input(format!(
                            "DuckDB execution Quack extension is unavailable: {error}"
                        ))
                    })?;
                if !extension_path.is_file()
                    || extension_path.file_name().and_then(|name| name.to_str())
                        != Some("quack.duckdb_extension")
                {
                    return Err(EngineError::invalid_input(
                        "DuckDB execution Quack extension path must name quack.duckdb_extension"
                            .into(),
                    ));
                }
                ConfiguredOwner::Quack {
                    uri: quack_uri,
                    token,
                    extension_path,
                    disable_ssl,
                    ssl_fingerprint,
                }
            }
        };
        let local_database = match &owner {
            ConfiguredOwner::Local { database_path } => {
                Some(Arc::new(SharedLocalDatabase::open(database_path)?))
            }
            ConfiguredOwner::Quack { .. } => None,
        };
        let connection = open_execution_connection(&owner, local_database.as_ref())?;
        initialize_receipt_ledger(&connection, &owner)?;
        let read_connections = Arc::new(ExecutionReadConnectionPool::new(
            owner.clone(),
            local_database.clone(),
        ));
        Ok(Self {
            connection: Mutex::new(connection),
            read_connections,
            quack_connection_needs_reopen: AtomicBool::new(false),
            owner,
            local_database,
        })
    }

    pub fn is_shared_owner(&self) -> bool {
        matches!(self.owner, ConfiguredOwner::Quack { .. })
    }

    /// Translate a Connect table name into a fixed-catalog DuckDB reference. Client-supplied
    /// paths, URI values, catalog names, and extension names are rejected.
    pub fn table_reference(&self, identifier: &str) -> Result<String, EngineError> {
        let parts = identifier
            .split('.')
            .map(|part| part.trim().trim_matches('`').trim_matches('"'))
            .collect::<Vec<_>>();
        let (schema, table) = match parts.as_slice() {
            [table] => ("main", *table),
            [schema, table] => (*schema, *table),
            _ => {
                return Err(EngineError::invalid_input(
                    "DuckDB execution accepts table names with one or two parts".into(),
                ));
            }
        };
        if !valid_identifier(schema) || !valid_identifier(table) {
            return Err(EngineError::invalid_input(
                "DuckDB execution table identifiers must be simple ASCII identifiers".into(),
            ));
        }
        if schema.eq_ignore_ascii_case(EXECUTION_RECEIPTS_SCHEMA) {
            return Err(EngineError::invalid_input(
                "DuckDB execution table identifiers cannot use the reserved receipt schema".into(),
            ));
        }
        let reference = if self.is_shared_owner() {
            format!(
                "{}.{}.{}",
                quote_identifier(EXECUTION_CATALOG),
                quote_identifier(schema),
                quote_identifier(table)
            )
        } else {
            format!("{}.{}", quote_identifier(schema), quote_identifier(table))
        };
        Ok(reference)
    }

    /// Resolve the output schema without retaining result rows.
    pub fn describe_query(
        &self,
        sql: &str,
        token: &CancellationToken,
    ) -> Result<StructType, EngineError> {
        token.check_cancelled()?;
        let mut connection = self.read_connections.acquire(token)?;
        let result = (|| {
            let _watchdog = self.cancellation_watchdog(&mut connection, token, false)?;
            let mut statement = connection
                .prepare(sql)
                .map_err(|error| self.failure(error))?;
            let batches = statement
                .stream_arrow([])
                .map_err(|error| self.failure(error))?;
            let schema = arrow_schema_to_spark(&batches.get_schema())?;
            token.check_cancelled()?;
            Ok(schema)
        })();
        if result.is_err() {
            connection.discard();
        }
        result
    }

    /// Stream one DuckDB query through independently readable, size-bounded Arrow IPC batches.
    /// `on_schema` runs before any batch is sent so Connect can emit its schema response first.
    pub fn stream_query<S, B>(
        &self,
        sql: &str,
        token: &CancellationToken,
        max_batch_rows: usize,
        max_batch_bytes: usize,
        on_schema: S,
        consume: B,
    ) -> Result<(), EngineError>
    where
        S: FnOnce(StructType) -> Result<(), EngineError>,
        B: FnMut(ArrowIpcStream) -> Result<(), EngineError>,
    {
        self.stream_query_with_arrow_ipc(
            ArrowIpcQueryOptions {
                sql,
                inputs: &[],
                expected_input_schemas: &[],
                expected_output_schema: None,
                max_input_bytes: 0,
                max_input_rows: 0,
                max_input_decoded_bytes: 0,
                token,
                max_batch_rows,
                max_batch_bytes,
            },
            on_schema,
            consume,
        )
    }

    /// Stream a query containing one `arrow(?)` source per supplied Spark Connect LocalRelation.
    /// Inputs are decoded and bounded before the shared DuckDB connection is acquired.
    pub fn stream_query_with_arrow_ipc<S, B>(
        &self,
        options: ArrowIpcQueryOptions<'_>,
        on_schema: S,
        mut consume: B,
    ) -> Result<(), EngineError>
    where
        S: FnOnce(StructType) -> Result<(), EngineError>,
        B: FnMut(ArrowIpcStream) -> Result<(), EngineError>,
    {
        let ArrowIpcQueryOptions {
            sql,
            inputs,
            expected_input_schemas,
            expected_output_schema,
            max_input_bytes,
            max_input_rows,
            max_input_decoded_bytes,
            token,
            max_batch_rows,
            max_batch_bytes,
        } = options;
        if expected_input_schemas.len() != inputs.len() {
            return Err(EngineError::invalid_input(
                "Connect local Arrow schema metadata does not match its input streams".into(),
            ));
        }
        let mut input_bytes = 0usize;
        let mut input_rows = 0usize;
        let mut decoded_bytes = 0usize;
        let mut input_batches = Vec::new();
        for (index, input) in inputs.iter().enumerate() {
            input_bytes = input_bytes.saturating_add(input.len());
            if input_bytes > max_input_bytes {
                return Err(EngineError::invalid_input(
                    "Connect local Arrow IPC inputs exceed their combined byte limit".into(),
                ));
            }
            let mut reader = StreamReader::try_new(Cursor::new(input), None).map_err(|error| {
                EngineError::invalid_input(format!(
                    "invalid Connect local Arrow IPC stream: {error}"
                ))
            })?;
            let schema = reader.schema();
            if schema.fields().len() > 512 {
                return Err(EngineError::invalid_input(
                    "Connect local Arrow schema exceeds the 512-field limit".into(),
                ));
            }
            let input_schema = arrow_schema_to_spark(&schema)?;
            if let Some(expected_schema) = &expected_input_schemas[index] {
                let actual = serde_json::to_value(&input_schema).map_err(|error| {
                    EngineError::internal(format!(
                        "cannot encode decoded Connect input schema: {error}"
                    ))
                })?;
                let expected = serde_json::to_value(expected_schema).map_err(|error| {
                    EngineError::internal(format!(
                        "cannot encode declared Connect input schema: {error}"
                    ))
                })?;
                if actual != expected {
                    return Err(EngineError::invalid_input(
                        "Connect LocalRelation Arrow schema does not match its declared schema"
                            .into(),
                    ));
                }
            }
            let mut stream_batches = Vec::new();
            for batch in &mut reader {
                token.check_cancelled()?;
                let batch = batch.map_err(|error| {
                    EngineError::invalid_input(format!(
                        "invalid Connect local Arrow batch: {error}"
                    ))
                })?;
                input_rows = input_rows.saturating_add(batch.num_rows());
                decoded_bytes = decoded_bytes.saturating_add(batch.get_array_memory_size());
                if input_rows > max_input_rows || decoded_bytes > max_input_decoded_bytes {
                    return Err(EngineError::invalid_input(
                        "Connect local Arrow inputs exceed their decoded row or byte limit".into(),
                    ));
                }
                stream_batches.push(batch);
            }
            let batch = if stream_batches.is_empty() {
                duckdb::arrow::record_batch::RecordBatch::new_empty(schema)
            } else if stream_batches.len() == 1 {
                stream_batches.pop().expect("one local Arrow batch exists")
            } else {
                duckdb::arrow::compute::concat_batches(&schema, &stream_batches).map_err(
                    |error| {
                        EngineError::invalid_input(format!(
                            "cannot combine Connect local Arrow batches: {error}"
                        ))
                    },
                )?
            };
            input_batches.push(batch);
        }
        token.check_cancelled()?;
        let mut connection = self.read_connections.acquire(token)?;
        let result = (|| {
            let _watchdog = self.cancellation_watchdog(&mut connection, token, false)?;
            let mut statement = connection
                .prepare(sql)
                .map_err(|error| self.failure(error))?;
            let registrations = input_batches
                .into_iter()
                .map(ArrowBatchRegistration::new)
                .collect::<Vec<_>>();
            let mut batches = if registrations.is_empty() {
                statement.stream_arrow([])
            } else {
                let registration_refs = registrations.iter().collect::<Vec<_>>();
                let bindings = registrations
                    .iter()
                    .zip(registration_refs.iter())
                    .map(|(_, registration)| registration as &dyn ToSql)
                    .collect::<Vec<_>>();
                statement.stream_arrow(bindings.as_slice())
            }
            .map_err(|error| self.failure(error))?;
            let mut arrow_schema = batches.get_schema().clone();
            let schema = if let Some(expected_schema) = expected_output_schema {
                validate_output_data_types(&arrow_schema, expected_schema)?;
                arrow_schema = rename_output_fields(&arrow_schema, expected_schema)?;
                expected_schema.clone()
            } else {
                arrow_schema_to_spark(&arrow_schema)?
            };
            on_schema(schema)?;
            let mut emitted_batch = false;
            let mut witness_rows = 0usize;
            let mut witness_batches = 0usize;
            for batch in &mut batches {
                token.check_cancelled()?;
                let batch = batch.map_err(|error| self.failure(error))?;
                if batch.num_rows() > max_batch_rows {
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB execution batch has {} rows, exceeding the {max_batch_rows}-row limit",
                        batch.num_rows()
                    )));
                }
                let batch = if expected_output_schema.is_some() {
                    duckdb::arrow::record_batch::RecordBatch::try_new(
                        arrow_schema.clone(),
                        batch.columns().to_vec(),
                    )
                    .map_err(|error| {
                        EngineError::internal(format!(
                            "cannot apply Spark Connect output schema to DuckDB Arrow batch: {error}"
                        ))
                    })?
                } else {
                    batch
                };
                validate_execution_batch_scalars(&batch)?;
                let bytes = encode_batch(&batch)?;
                if bytes.len() > max_batch_bytes {
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB execution batch exceeds the {max_batch_bytes}-byte Arrow IPC limit"
                    )));
                }
                consume(ArrowIpcStream {
                    bytes,
                    row_count: batch.num_rows() as u64,
                })?;
                witness_rows = witness_rows.saturating_add(batch.num_rows());
                witness_batches = witness_batches.saturating_add(1);
                emitted_batch = true;
            }
            if !emitted_batch {
                let batch = duckdb::arrow::record_batch::RecordBatch::new_empty(arrow_schema);
                let bytes = encode_batch(&batch)?;
                if bytes.len() > max_batch_bytes {
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB empty result schema exceeds the {max_batch_bytes}-byte Arrow IPC limit"
                    )));
                }
                consume(ArrowIpcStream {
                    bytes,
                    row_count: 0,
                })?;
                witness_batches = 1;
            }
            token.check_cancelled()?;
            trace_execution_witness("query", "completed", witness_rows, witness_batches);
            Ok(())
        })();
        if result.is_err() {
            connection.discard();
        }
        result
    }

    /// Execute one previously validated write statement in an explicit transaction. Once
    /// COMMIT is sent, an error is reported as unknown because the owner may have committed
    /// before the connection failed.
    pub fn execute_transaction(
        &self,
        sql: &str,
        operation_key: &str,
        request_fingerprint: &[u8; 32],
        token: &CancellationToken,
    ) -> Result<DuckDbTransactionOutcome, EngineError> {
        self.execute_transaction_with_commit(
            sql,
            operation_key,
            request_fingerprint,
            token,
            |connection| {
                connection
                    .execute_batch("COMMIT")
                    .map_err(|error| error.to_string())
            },
        )
    }

    fn execute_transaction_with_commit(
        &self,
        sql: &str,
        operation_key: &str,
        request_fingerprint: &[u8; 32],
        token: &CancellationToken,
        commit: impl FnOnce(&Connection) -> Result<(), String>,
    ) -> Result<DuckDbTransactionOutcome, EngineError> {
        token.check_cancelled()?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| EngineError::internal("DuckDB execution owner is unavailable".into()))?;
        let _watchdog = self.cancellation_watchdog(&mut connection, token, true)?;
        connection
            .execute_batch("BEGIN TRANSACTION")
            .map_err(|error| self.failure(error))?;
        let ledger = receipt_ledger_reference(&self.owner);
        let fingerprint = hex_digest(request_fingerprint);
        let operation_key_sql = quote_literal(operation_key);
        let existing = match connection.query_row(
            &format!(
                "SELECT (SELECT request_fingerprint FROM {ledger} WHERE operation_key = {operation_key_sql})"
            ),
            [],
            |row| row.get::<_, Option<String>>(0),
        ) {
            Ok(existing) => existing,
            Err(error) => {
                return Err(self.rollback_before_write(
                    &connection,
                    "operation receipt lookup",
                    error,
                ));
            }
        };
        if let Some(existing) = existing {
            connection
                .execute_batch("ROLLBACK")
                .map_err(|error| self.failure(error))?;
            if existing != fingerprint {
                return Err(EngineError::invalid_input(
                    "DuckDB operation identity is already committed for different request content"
                        .into(),
                ));
            }
            trace_execution_witness("write", "replayed", 0, 0);
            return Ok(DuckDbTransactionOutcome::Replayed);
        }
        let receipt_count =
            match connection.query_row(&format!("SELECT COUNT(*) FROM {ledger}"), [], |row| {
                row.get::<_, u64>(0)
            }) {
                Ok(receipt_count) => receipt_count,
                Err(error) => {
                    return Err(self.rollback_before_write(
                        &connection,
                        "operation receipt capacity check",
                        error,
                    ));
                }
            };
        if receipt_count >= DUCKDB_EXECUTION_MAX_RECEIPTS {
            connection.execute_batch("ROLLBACK").map_err(|error| {
                self.mark_quack_connection_for_reopen();
                EngineError::internal(format!(
                    "DuckDB transaction cleanup failed before statement execution: {}",
                    self.redact(&error.to_string())
                ))
            })?;
            return Err(EngineError::invalid_input(format!(
                "DuckDB durable operation receipt capacity is full ({DUCKDB_EXECUTION_MAX_RECEIPTS})"
            )));
        }
        if let Err(error) = connection.execute_batch(sql) {
            if connection.execute_batch("ROLLBACK").is_err() {
                self.mark_quack_connection_for_reopen();
                return Err(EngineError::internal(format!(
                    "DuckDB transaction outcome is unknown after statement failure: {}",
                    self.redact(&error.to_string())
                )));
            }
            return Err(self.failure(error));
        }
        if token.is_cancelled() {
            connection.execute_batch("ROLLBACK").map_err(|error| {
                self.mark_quack_connection_for_reopen();
                EngineError::internal(format!(
                    "DuckDB transaction outcome is unknown after cancellation: {}",
                    self.redact(&error.to_string())
                ))
            })?;
            return Err(EngineError::invalid_input(
                "DuckDB write was cancelled before commit".into(),
            ));
        }
        let fingerprint_sql = quote_literal(&fingerprint);
        connection
            .execute_batch(&format!(
                "INSERT INTO {ledger} (operation_key, request_fingerprint) VALUES ({operation_key_sql}, {fingerprint_sql})"
            ))
            .map_err(|error| {
                if connection.execute_batch("ROLLBACK").is_err() {
                    self.mark_quack_connection_for_reopen();
                    EngineError::internal(format!(
                        "DuckDB transaction outcome is unknown after receipt failure: {}",
                        self.redact(&error.to_string())
                    ))
                } else {
                    self.failure(error)
                }
            })?;
        if let Err(error) = commit(&connection) {
            // A COMMIT error is ambiguous: the transaction may still be open, or it may
            // already be durable. A best-effort rollback cleans up the former without
            // changing the unknown outcome of the latter.
            let _ = connection.execute_batch("ROLLBACK");
            self.mark_quack_connection_for_reopen();
            return Err(EngineError::internal(format!(
                "DuckDB transaction outcome is unknown after commit was requested: {}",
                self.redact(&error)
            )));
        }
        self.read_connections.invalidate();
        trace_execution_witness("write", "committed", 0, 0);
        Ok(DuckDbTransactionOutcome::Committed)
    }

    fn cancellation_watchdog(
        &self,
        connection: &mut Connection,
        token: &CancellationToken,
        primary_connection: bool,
    ) -> Result<CancellationWatchdog, EngineError> {
        let quack_cancel = if matches!(self.owner, ConfiguredOwner::Quack { .. }) {
            if primary_connection && self.quack_connection_needs_reopen.load(Ordering::Acquire) {
                self.reopen_quack_connection(connection)?;
            }
            let connection_id = match quack_connection_id_for(connection, EXECUTION_CATALOG) {
                Ok(connection_id) => connection_id,
                Err(_) if primary_connection => {
                    self.mark_quack_connection_for_reopen();
                    self.reopen_quack_connection(connection)?;
                    quack_connection_id_for(connection, EXECUTION_CATALOG).map_err(|error| {
                        self.quack_connection_needs_reopen
                            .store(true, Ordering::Release);
                        EngineError::invalid_input(format!(
                            "DuckDB execution failed: {}",
                            self.redact(&error.to_string())
                        ))
                    })?
                }
                Err(error) => {
                    self.mark_quack_connection_for_reopen();
                    return Err(self.failure(error));
                }
            };
            Some(QuackCancellation {
                owner: self.owner.clone(),
                connection_id,
            })
        } else {
            None
        };
        CancellationWatchdog::new(token.clone(), connection.interrupt_handle(), quack_cancel)
    }

    fn failure(&self, error: impl std::fmt::Display) -> EngineError {
        self.mark_quack_connection_for_reopen();
        EngineError::invalid_input(format!(
            "DuckDB execution failed: {}",
            self.redact(&error.to_string())
        ))
    }

    fn rollback_before_write(
        &self,
        connection: &Connection,
        phase: &str,
        error: impl std::fmt::Display,
    ) -> EngineError {
        let error = self.redact(&error.to_string());
        match connection.execute_batch("ROLLBACK") {
            Ok(()) => self.failure(format!("{phase} failed: {error}")),
            Err(rollback_error) => {
                self.mark_quack_connection_for_reopen();
                EngineError::internal(format!(
                    "DuckDB transaction cleanup failed before statement execution during {phase}: {error}; rollback failed: {}",
                    self.redact(&rollback_error.to_string())
                ))
            }
        }
    }

    fn mark_quack_connection_for_reopen(&self) {
        if matches!(self.owner, ConfiguredOwner::Quack { .. }) {
            self.quack_connection_needs_reopen
                .store(true, Ordering::Release);
            self.read_connections.invalidate();
        }
    }

    fn redact(&self, message: &str) -> String {
        match &self.owner {
            ConfiguredOwner::Quack { token, .. } => message.replace(token, "[redacted]"),
            ConfiguredOwner::Local { .. } => message.to_string(),
        }
    }

    fn reopen_quack_connection(&self, connection: &mut Connection) -> Result<(), EngineError> {
        let replacement = open_execution_connection(&self.owner, self.local_database.as_ref())?;
        *connection = replacement;
        self.quack_connection_needs_reopen
            .store(false, Ordering::Release);
        Ok(())
    }
}

struct ExecutionReadConnectionPool {
    owner: ConfiguredOwner,
    state: Mutex<ExecutionReadConnectionPoolState>,
    available: Condvar,
    local_database: Option<Arc<SharedLocalDatabase>>,
}

#[derive(Default)]
struct ExecutionReadConnectionPoolState {
    connections: Vec<Connection>,
    opened: usize,
    waiting: usize,
    generation: u64,
}

impl ExecutionReadConnectionPool {
    fn new(owner: ConfiguredOwner, local_database: Option<Arc<SharedLocalDatabase>>) -> Self {
        Self {
            owner,
            state: Mutex::new(ExecutionReadConnectionPoolState::default()),
            available: Condvar::new(),
            local_database,
        }
    }

    fn acquire(
        self: &Arc<Self>,
        token: &CancellationToken,
    ) -> Result<ExecutionReadConnection, EngineError> {
        let mut state = self.state.lock().map_err(|_| {
            EngineError::internal("DuckDB execution read pool is unavailable".into())
        })?;
        let mut queued = false;
        loop {
            if token.is_cancelled() {
                if queued {
                    state.waiting = state.waiting.saturating_sub(1);
                    self.available.notify_all();
                }
                return Err(EngineError::cancelled("operation cancelled".into()));
            }
            if let Some(connection) = state.connections.pop() {
                if queued {
                    state.waiting = state.waiting.saturating_sub(1);
                }
                return Ok(ExecutionReadConnection {
                    pool: Arc::clone(self),
                    connection: Some(connection),
                    generation: state.generation,
                    discard: false,
                });
            }
            if state.opened < DUCKDB_EXECUTION_MAX_READ_CONNECTIONS {
                if queued {
                    state.waiting = state.waiting.saturating_sub(1);
                }
                state.opened += 1;
                let generation = state.generation;
                drop(state);
                return match open_execution_read_connection(
                    &self.owner,
                    self.local_database.as_ref(),
                ) {
                    Ok(connection) => Ok(ExecutionReadConnection {
                        pool: Arc::clone(self),
                        connection: Some(connection),
                        generation,
                        discard: false,
                    }),
                    Err(error) => {
                        let mut state = self.state.lock().map_err(|_| {
                            EngineError::internal(
                                "DuckDB execution read pool is unavailable".into(),
                            )
                        })?;
                        state.opened = state.opened.saturating_sub(1);
                        self.available.notify_all();
                        Err(error)
                    }
                };
            }
            if !queued {
                if state.waiting >= DUCKDB_EXECUTION_MAX_QUEUED_READS {
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB execution read queue is full ({DUCKDB_EXECUTION_MAX_QUEUED_READS} waiting operations)"
                    )));
                }
                state.waiting += 1;
                queued = true;
            }
            let (next_state, _) = self
                .available
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| {
                    EngineError::internal("DuckDB execution read pool is unavailable".into())
                })?;
            state = next_state;
        }
    }

    fn invalidate(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.generation = state.generation.wrapping_add(1);
        let idle = std::mem::take(&mut state.connections);
        let idle_count = idle.len();
        state.opened = state.opened.saturating_sub(idle_count);
        self.available.notify_all();
        drop(state);
        drop(idle);
    }
}

struct ExecutionReadConnection {
    pool: Arc<ExecutionReadConnectionPool>,
    connection: Option<Connection>,
    generation: u64,
    discard: bool,
}

impl ExecutionReadConnection {
    fn discard(&mut self) {
        self.discard = true;
    }
}

impl Deref for ExecutionReadConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.connection
            .as_ref()
            .expect("execution read connection is checked out")
    }
}

impl DerefMut for ExecutionReadConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection
            .as_mut()
            .expect("execution read connection is checked out")
    }
}

impl Drop for ExecutionReadConnection {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        let Ok(mut state) = self.pool.state.lock() else {
            return;
        };
        if self.discard || self.generation != state.generation {
            state.opened = state.opened.saturating_sub(1);
            self.pool.available.notify_all();
            drop(state);
            drop(connection);
        } else {
            state.connections.push(connection);
            self.pool.available.notify_all();
        }
    }
}

fn open_execution_read_connection(
    owner: &ConfiguredOwner,
    local_database: Option<&Arc<SharedLocalDatabase>>,
) -> Result<Connection, EngineError> {
    open_execution_connection(owner, local_database)
}

fn prepare_database_path(path: &Path) -> Result<PathBuf, EngineError> {
    if path.as_os_str().is_empty() {
        return Err(EngineError::invalid_input(
            "DuckDB execution database path must not be empty".into(),
        ));
    }
    if path.exists() {
        let canonical = std::fs::canonicalize(path).map_err(|error| {
            EngineError::invalid_input(format!(
                "DuckDB execution database path is unavailable: {error}"
            ))
        })?;
        if !canonical.is_file() {
            return Err(EngineError::invalid_input(
                "DuckDB execution database path must name a file".into(),
            ));
        }
        return Ok(canonical);
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = std::fs::canonicalize(parent).map_err(|error| {
        EngineError::invalid_input(format!(
            "DuckDB execution database parent directory is unavailable: {error}"
        ))
    })?;
    let file_name = path.file_name().ok_or_else(|| {
        EngineError::invalid_input("DuckDB execution database path must include a file name".into())
    })?;
    Ok(parent.join(file_name))
}

fn open_execution_connection(
    owner: &ConfiguredOwner,
    local_database: Option<&Arc<SharedLocalDatabase>>,
) -> Result<Connection, EngineError> {
    let connection = match owner {
        ConfiguredOwner::Local { .. } => local_database
            .ok_or_else(|| {
                EngineError::internal(
                    "local DuckDB execution database handle is unavailable".into(),
                )
            })?
            .connect()?,
        ConfiguredOwner::Quack {
            uri,
            token,
            extension_path,
            disable_ssl,
            ssl_fingerprint,
        } => {
            let connection = Connection::open_in_memory().map_err(duckdb_failure)?;
            configure_connection(&connection)?;
            let extension_path = extension_path.to_str().ok_or_else(|| {
                EngineError::invalid_input(
                    "DuckDB Quack extension paths must be valid UTF-8".into(),
                )
            })?;
            connection
                .execute_batch(&format!("LOAD {}", quote_literal(extension_path)))
                .map_err(duckdb_failure)?;
            let mut options = vec![format!("TOKEN {}", quote_literal(token))];
            if let Some(disable_ssl) = disable_ssl {
                options.push(format!("DISABLE_SSL {disable_ssl}"));
            }
            if let Some(fingerprint) = ssl_fingerprint {
                options.push(format!("SSL_FINGERPRINT {}", quote_literal(fingerprint)));
            }
            connection
                .execute_batch(&format!(
                    "ATTACH {} AS {} ({})",
                    quote_literal(uri),
                    quote_identifier(EXECUTION_CATALOG),
                    options.join(", ")
                ))
                .map_err(|error| {
                    EngineError::invalid_input(format!(
                        "DuckDB execution Quack attach failed: {}",
                        error.to_string().replace(token, "[redacted]")
                    ))
                })?;
            connection
        }
    };
    configure_connection(&connection)?;
    connection
        .register_table_function::<ArrowVTab>("arrow")
        .map_err(duckdb_failure)?;
    Ok(connection)
}

struct SharedLocalDatabase {
    handle: duckdb::ffi::duckdb_database,
}

// The shared DuckDB database handle is used only to create independent connections. Each
// connection is exclusively owned, and the Arc-held database handle outlives all of them.
unsafe impl Send for SharedLocalDatabase {}
unsafe impl Sync for SharedLocalDatabase {}

impl SharedLocalDatabase {
    fn open(path: &Path) -> Result<Self, EngineError> {
        let path = local_database_path_cstring(path)?;
        let mut handle = ptr::null_mut();
        let mut error = ptr::null_mut();
        let status = unsafe {
            duckdb::ffi::duckdb_open_ext(path.as_ptr(), &mut handle, ptr::null_mut(), &mut error)
        };
        let error_message = if error.is_null() {
            None
        } else {
            let message = unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned();
            unsafe { duckdb::ffi::duckdb_free(error.cast()) };
            Some(message)
        };
        if status != duckdb::ffi::DuckDBSuccess || handle.is_null() {
            if !handle.is_null() {
                unsafe { duckdb::ffi::duckdb_close(&mut handle) };
            }
            return Err(EngineError::invalid_input(format!(
                "DuckDB execution database could not open: {}",
                error_message.unwrap_or_else(|| "DuckDB open failed".into())
            )));
        }
        Ok(Self { handle })
    }

    fn connect(&self) -> Result<Connection, EngineError> {
        // SAFETY: this handle is owned by the shared database wrapper and remains alive through
        // the Arc held by the server and query pool. Connections close only their connection
        // handle; the wrapper closes the database after its last owner is dropped.
        unsafe { Connection::open_from_raw(self.handle) }.map_err(duckdb_failure)
    }
}

impl Drop for SharedLocalDatabase {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { duckdb::ffi::duckdb_close(&mut self.handle) };
        }
    }
}

fn local_database_path_cstring(path: &Path) -> Result<CString, EngineError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        CString::new(path.as_os_str().as_bytes()).map_err(|error| {
            EngineError::invalid_input(format!("DuckDB database path contains a NUL byte: {error}"))
        })
    }
    #[cfg(not(unix))]
    {
        let path = path.to_str().ok_or_else(|| {
            EngineError::invalid_input("DuckDB database path must be valid UTF-8".into())
        })?;
        CString::new(path).map_err(|error| {
            EngineError::invalid_input(format!("DuckDB database path contains a NUL byte: {error}"))
        })
    }
}

fn trace_execution_witness(operation: &str, outcome: &str, rows: usize, batches: usize) {
    if std::env::var_os("SPARKLESS_CONNECT_TRACE").as_deref() == Some(std::ffi::OsStr::new("1")) {
        eprintln!(
            "connect_trace event=duckdb_execution_witness engine=duckdb operation={operation} outcome={outcome} rows={rows} batches={batches}"
        );
    }
}

fn initialize_receipt_ledger(
    connection: &Connection,
    owner: &ConfiguredOwner,
) -> Result<(), EngineError> {
    let catalog = if matches!(owner, ConfiguredOwner::Quack { .. }) {
        format!("{}.", quote_identifier(EXECUTION_CATALOG))
    } else {
        String::new()
    };
    let schema = format!("{catalog}{}", quote_identifier(EXECUTION_RECEIPTS_SCHEMA));
    let table = format!("{schema}.{}", quote_identifier(EXECUTION_RECEIPTS_TABLE));
    connection
        .execute_batch(&format!(
            "CREATE SCHEMA IF NOT EXISTS {schema}; CREATE TABLE IF NOT EXISTS {table} (operation_key VARCHAR PRIMARY KEY, request_fingerprint VARCHAR NOT NULL, committed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);"
        ))
        .map_err(|error| match owner {
            ConfiguredOwner::Quack { token, .. } => EngineError::invalid_input(format!(
                "DuckDB execution receipt ledger initialization failed: {}",
                error.to_string().replace(token, "[redacted]")
            )),
            ConfiguredOwner::Local { .. } => duckdb_failure(error),
        })
}

fn receipt_ledger_reference(owner: &ConfiguredOwner) -> String {
    if matches!(owner, ConfiguredOwner::Quack { .. }) {
        format!(
            "{}.{}.{}",
            quote_identifier(EXECUTION_CATALOG),
            quote_identifier(EXECUTION_RECEIPTS_SCHEMA),
            quote_identifier(EXECUTION_RECEIPTS_TABLE)
        )
    } else {
        format!(
            "{}.{}",
            quote_identifier(EXECUTION_RECEIPTS_SCHEMA),
            quote_identifier(EXECUTION_RECEIPTS_TABLE)
        )
    }
}

fn hex_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn validate_output_data_types(
    actual: &arrow60::datatypes::SchemaRef,
    expected: &StructType,
) -> Result<(), EngineError> {
    if actual.fields().len() != expected.logical_fields().len() {
        return Err(EngineError::internal(
            "DuckDB output width does not match the validated Spark Connect schema".into(),
        ));
    }
    for (actual_field, expected_field) in actual.fields().iter().zip(expected.logical_fields()) {
        let actual_type =
            serde_json::to_value(arrow_field_to_spark_type(actual_field.data_type())?).map_err(
                |error| EngineError::internal(format!("cannot encode DuckDB output type: {error}")),
            )?;
        let expected_type = serde_json::to_value(&expected_field.data_type).map_err(|error| {
            EngineError::internal(format!("cannot encode Spark Connect output type: {error}"))
        })?;
        if actual_type != expected_type {
            return Err(EngineError::internal(
                "DuckDB output type does not match the validated Spark Connect schema".into(),
            ));
        }
    }
    Ok(())
}

fn rename_output_fields(
    actual: &arrow60::datatypes::SchemaRef,
    expected: &StructType,
) -> Result<arrow60::datatypes::SchemaRef, EngineError> {
    if actual.fields().len() != expected.logical_fields().len() {
        return Err(EngineError::internal(
            "DuckDB output width does not match the validated Spark Connect schema".into(),
        ));
    }
    let fields = actual
        .fields()
        .iter()
        .zip(expected.logical_fields())
        .map(|(field, expected)| {
            let metadata = expected
                .metadata
                .iter()
                .map(|(key, value)| {
                    serde_json::to_string(value)
                        .map(|value| (key.clone(), value))
                        .map_err(|error| {
                            EngineError::internal(format!(
                                "cannot encode Spark Connect field metadata: {error}"
                            ))
                        })
                })
                .collect::<Result<std::collections::HashMap<_, _>, _>>()?;
            Ok(field
                .as_ref()
                .clone()
                .with_name(expected.name.clone())
                .with_nullable(expected.nullable)
                .with_metadata(metadata))
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields,
        actual.metadata().clone(),
    )))
}

fn arrow_schema_to_spark(
    schema: &arrow60::datatypes::SchemaRef,
) -> Result<StructType, EngineError> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = arrow_field_to_spark_type(field.data_type())?;
            let metadata = field
                .metadata()
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        serde_json::from_str(value)
                            .unwrap_or_else(|_| serde_json::Value::String(value.clone())),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            Ok(
                LogicalField::new(field.name().clone(), data_type, field.is_nullable())
                    .with_metadata(metadata),
            )
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    Ok(StructType::from_logical_fields(fields))
}

fn arrow_field_to_spark_type(arrow_type: &ArrowDataType) -> Result<DataType, EngineError> {
    Ok(match arrow_type {
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::String,
        ArrowDataType::Boolean => DataType::Boolean,
        ArrowDataType::Int8 => DataType::Byte,
        ArrowDataType::Int16 => DataType::Short,
        ArrowDataType::Int32 => DataType::Integer,
        ArrowDataType::Int64 => DataType::Long,
        ArrowDataType::Float32 => DataType::Float,
        ArrowDataType::Float64 => DataType::Double,
        ArrowDataType::Date32 => DataType::Date,
        ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_)) => DataType::Timestamp,
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => DataType::TimestampNtz,
        ArrowDataType::Decimal128(precision, scale) => {
            let precision = *precision;
            let scale = u8::try_from(*scale).map_err(|_| {
                EngineError::invalid_input("DuckDB decimal scale is out of range".into())
            })?;
            if precision == 0 || precision > 38 || scale > precision {
                return Err(EngineError::invalid_input(
                    "DuckDB decimal precision/scale is outside the Spark range".into(),
                ));
            }
            DataType::Decimal(precision, scale)
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary => DataType::Binary,
        other => {
            return Err(EngineError::invalid_input(format!(
                "DuckDB result type {other} is outside the initial Connect type set"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DUCKDB_EXECUTION_MAX_BATCH_BYTES, DUCKDB_EXECUTION_MAX_BATCH_ROWS,
        DUCKDB_EXECUTION_MAX_READ_CONNECTIONS, DuckDbExecutionOwner, DuckDbExecutionOwnerConfig,
        DuckDbTransactionOutcome,
    };
    use robin_sparkless_core::engine::CancellationToken;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier, mpsc};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    static NEXT_DATABASE_ID: AtomicU64 = AtomicU64::new(0);

    struct TempDatabase(PathBuf);

    impl TempDatabase {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock must be after Unix epoch")
                .as_nanos();
            Self(std::env::temp_dir().join(format!(
                "sparkless-duckdb-execution-{}-{unique}-{}.duckdb",
                std::process::id(),
                NEXT_DATABASE_ID.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }

    impl Drop for TempDatabase {
        fn drop(&mut self) {
            let wal_path = PathBuf::from(format!("{}.wal", self.0.display()));
            let _ = std::fs::remove_file(&self.0);
            let _ = std::fs::remove_file(wal_path);
        }
    }

    fn local_owner(database: &TempDatabase) -> DuckDbExecutionOwner {
        DuckDbExecutionOwner::open(DuckDbExecutionOwnerConfig::Local {
            database_path: database.0.clone(),
        })
        .expect("local DuckDB owner should open")
    }

    #[test]
    fn local_writes_commit_with_receipts_and_replay_after_reopen() {
        let database = TempDatabase::new();
        let token = CancellationToken::new();
        {
            let owner = local_owner(&database);
            assert_eq!(
                owner
                    .execute_transaction(
                        "CREATE TABLE main.items (id BIGINT)",
                        "create-items",
                        &[1; 32],
                        &token,
                    )
                    .expect("create table should commit"),
                DuckDbTransactionOutcome::Committed
            );
            assert_eq!(
                owner
                    .execute_transaction(
                        "CREATE TABLE main.items (id BIGINT)",
                        "create-items",
                        &[1; 32],
                        &token,
                    )
                    .expect("matching create should replay"),
                DuckDbTransactionOutcome::Replayed
            );
            assert!(
                owner
                    .execute_transaction(
                        "CREATE TABLE main.items (id BIGINT)",
                        "create-items",
                        &[2; 32],
                        &token,
                    )
                    .is_err()
            );
            assert_eq!(
                owner
                    .execute_transaction(
                        "INSERT INTO main.items VALUES (1), (2)",
                        "append-items",
                        &[3; 32],
                        &token,
                    )
                    .expect("insert should commit"),
                DuckDbTransactionOutcome::Committed
            );
            assert_eq!(
                owner
                    .execute_transaction(
                        "INSERT INTO main.items VALUES (1), (2)",
                        "append-items",
                        &[3; 32],
                        &token,
                    )
                    .expect("matching insert should replay"),
                DuckDbTransactionOutcome::Replayed
            );
            let rows = owner
                .connection
                .lock()
                .expect("owner connection should be available")
                .query_row("SELECT COUNT(*) FROM main.items", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("rows should be visible before closing the owner");
            assert_eq!(rows, 2);
        }

        assert!(
            database.0.is_file(),
            "database file should exist at {:?}",
            database.0
        );
        let owner = local_owner(&database);
        let reopened_rows = owner
            .connection
            .lock()
            .expect("owner connection should be available")
            .query_row("SELECT COUNT(*) FROM main.items", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("rows should be persisted when reopening the owner");
        assert_eq!(reopened_rows, 2);
        assert_eq!(
            owner
                .execute_transaction(
                    "INSERT INTO main.items VALUES (1), (2)",
                    "append-items",
                    &[3; 32],
                    &token,
                )
                .expect("durable receipt should replay after reopening"),
            DuckDbTransactionOutcome::Replayed
        );
        let connection = owner
            .connection
            .lock()
            .expect("owner connection should be available");
        let rows = connection
            .query_row("SELECT COUNT(*) FROM main.items", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("table should remain queryable after reopening");
        assert_eq!(rows, 2, "replay must not apply the insert twice");
    }

    #[test]
    fn lost_commit_acknowledgement_is_unknown_and_replays_without_reapplying() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let token = CancellationToken::new();
        owner
            .execute_transaction(
                "CREATE TABLE main.commit_ack_items (id BIGINT)",
                "create-commit-ack-items",
                &[1; 32],
                &token,
            )
            .expect("fixture table should be created");

        let failure = owner
            .execute_transaction_with_commit(
                "INSERT INTO main.commit_ack_items VALUES (7)",
                "lost-commit-acknowledgement",
                &[2; 32],
                &token,
                |connection| {
                    connection
                        .execute_batch("COMMIT")
                        .map_err(|error| error.to_string())?;
                    Err("simulated lost COMMIT acknowledgement".into())
                },
            )
            .expect_err("the injected acknowledgement loss should be unknown");
        assert!(
            failure
                .to_string()
                .contains("transaction outcome is unknown after commit was requested")
        );

        let connection = owner
            .connection
            .lock()
            .expect("owner connection should remain available");
        let rows = connection
            .query_row("SELECT COUNT(*) FROM main.commit_ack_items", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("the committed row should be visible after lost acknowledgement");
        assert_eq!(rows, 1);
        let receipts = connection
            .query_row(
                "SELECT COUNT(*) FROM \"__sparkless_meta\".\"connect_operation_receipts\" WHERE operation_key = 'lost-commit-acknowledgement'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("the durable receipt should be visible after lost acknowledgement");
        assert_eq!(receipts, 1);
        drop(connection);

        assert_eq!(
            owner
                .execute_transaction(
                    "INSERT INTO main.commit_ack_items VALUES (7)",
                    "lost-commit-acknowledgement",
                    &[2; 32],
                    &token,
                )
                .expect("retry should replay the durable receipt"),
            DuckDbTransactionOutcome::Replayed
        );
        let rows = owner
            .connection
            .lock()
            .expect("owner connection should remain available")
            .query_row("SELECT COUNT(*) FROM main.commit_ack_items", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("the table should remain queryable after replay");
        assert_eq!(rows, 1, "retry must not apply the committed insert twice");
    }

    #[test]
    fn duckdb_commit_error_returns_unknown_and_allows_retry_after_test_rollback() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let token = CancellationToken::new();
        owner
            .execute_transaction(
                "CREATE TABLE main.commit_error_items (id BIGINT)",
                "create-commit-error-items",
                &[1; 32],
                &token,
            )
            .expect("fixture table should be created");

        let failure = owner
            .execute_transaction_with_commit(
                "INSERT INTO main.commit_error_items VALUES (9)",
                "failed-commit-boundary",
                &[2; 32],
                &token,
                |connection| {
                    // Force DuckDB itself to return an error for COMMIT. This exercises the
                    // database-error path, but is not a storage or I/O failure injection.
                    connection
                        .execute_batch("ROLLBACK")
                        .map_err(|error| error.to_string())?;
                    connection
                        .execute_batch("COMMIT")
                        .map_err(|error| error.to_string())
                },
            )
            .expect_err("the injected COMMIT failure should have an unknown outcome");
        assert!(
            failure
                .to_string()
                .contains("transaction outcome is unknown after commit was requested")
        );

        {
            let connection = owner
                .connection
                .lock()
                .expect("owner connection should remain available");
            let rows = connection
                .query_row("SELECT COUNT(*) FROM main.commit_error_items", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("table should remain queryable after rollback");
            assert_eq!(rows, 0);
            let receipts = connection
                .query_row(
                    "SELECT COUNT(*) FROM \"__sparkless_meta\".\"connect_operation_receipts\" WHERE operation_key = 'failed-commit-boundary'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("receipt ledger should remain queryable after rollback");
            assert_eq!(receipts, 0);
        }

        assert_eq!(
            owner
                .execute_transaction(
                    "INSERT INTO main.commit_error_items VALUES (9)",
                    "failed-commit-boundary",
                    &[2; 32],
                    &token,
                )
                .expect("the operation should be retryable when no receipt committed"),
            DuckDbTransactionOutcome::Committed
        );
        let rows = owner
            .connection
            .lock()
            .expect("owner connection should remain available")
            .query_row("SELECT COUNT(*) FROM main.commit_error_items", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("table should remain queryable after retry");
        assert_eq!(rows, 1);
    }

    #[test]
    #[ignore = "run alone with SIGXFSZ ignored and RLIMIT_FSIZE below the inserted value"]
    fn storage_write_failure_during_commit_returns_unknown() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let token = CancellationToken::new();
        owner
            .execute_transaction(
                "CREATE TABLE main.storage_failure_items (payload VARCHAR)",
                "create-storage-failure-items",
                &[1; 32],
                &token,
            )
            .expect("fixture table should be created before the file-size limit applies");

        let failure = owner
            .execute_transaction(
                "INSERT INTO main.storage_failure_items \
                 SELECT string_agg(sha256(i::VARCHAR), '') \
                 FROM range(524288) AS source_rows(i)",
                "storage-write-failure-at-commit",
                &[2; 32],
                &token,
            )
            .expect_err("the operating-system file-size limit should reject COMMIT");
        eprintln!("commit error under RLIMIT_FSIZE: {failure}");
        assert!(
            failure
                .to_string()
                .contains("transaction outcome is unknown after commit was requested")
        );

        // Reopen the temporary database after the failed WAL write. The statement and
        // its receipt share a transaction, so recovery must expose both or neither.
        drop(owner);
        let owner = local_owner(&database);
        let connection = owner
            .connection
            .lock()
            .expect("owner connection should be available after recovery");
        let (rows, payload_bytes) = connection
            .query_row(
                "SELECT COUNT(*), COALESCE(MAX(length(payload)), 0) \
                 FROM main.storage_failure_items",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .expect("the database should reopen and query after the WAL write fault");
        let receipts = connection
            .query_row(
                "SELECT COUNT(*) FROM \"__sparkless_meta\".\"connect_operation_receipts\" \
                 WHERE operation_key = 'storage-write-failure-at-commit'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("the receipt ledger should be readable after recovery");
        assert!(rows == 0 || rows == 1, "the write must be all or nothing");
        assert_eq!(
            rows, receipts,
            "the write and receipt must recover atomically"
        );
        assert_eq!(
            payload_bytes,
            if rows == 1 { 32 * 1024 * 1024 } else { 0 },
            "a recovered row must contain the complete transaction value"
        );
        eprintln!(
            "post-fault recovery: rows={rows} receipts={receipts} payload_bytes={payload_bytes}"
        );
    }

    #[test]
    fn failed_statement_rolls_back_effects_and_does_not_reserve_the_operation_identity() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let token = CancellationToken::new();

        let failure = owner
            .execute_transaction(
                "CREATE TABLE main.rollback_items (id BIGINT); INSERT INTO main.rollback_items VALUES (1); INSERT INTO main.table_that_does_not_exist VALUES (2)",
                "retry-after-statement-failure",
                &[1; 32],
                &token,
            )
            .expect_err("the final statement should fail");
        assert!(failure.to_string().contains("does not exist"));

        let connection = owner
            .connection
            .lock()
            .expect("owner connection should be available");
        let created_tables = connection
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'rollback_items'",
                [],
                |row| row.get::<_, u64>(0),
            )
            .expect("catalog should be queryable after rollback");
        assert_eq!(created_tables, 0, "failed transaction must remove its DDL");
        drop(connection);

        assert_eq!(
            owner
                .execute_transaction(
                    "CREATE TABLE main.rollback_items (id BIGINT)",
                    "retry-after-statement-failure",
                    &[2; 32],
                    &token,
                )
                .expect("the identity should be reusable when no receipt committed"),
            DuckDbTransactionOutcome::Committed
        );
    }

    #[test]
    fn cancelled_before_admission_has_no_effect_and_reserves_no_receipt() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let token = CancellationToken::new();
        token.cancel();

        let error = owner
            .execute_transaction(
                "CREATE TABLE main.cancelled_items (id BIGINT)",
                "cancelled-before-admission",
                &[1; 32],
                &token,
            )
            .expect_err("a cancelled write should be rejected before execution");
        assert!(error.to_string().contains("operation cancelled"));

        let connection = owner
            .connection
            .lock()
            .expect("owner connection should remain available");
        let created_tables = connection
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'cancelled_items'",
                [],
                |row| row.get::<_, u64>(0),
            )
            .expect("catalog should be queryable after cancellation");
        assert_eq!(created_tables, 0);
        let receipts = connection
            .query_row(
                "SELECT COUNT(*) FROM \"__sparkless_meta\".\"connect_operation_receipts\" WHERE operation_key = 'cancelled-before-admission'",
                [],
                |row| row.get::<_, u64>(0),
            )
            .expect("receipt ledger should be queryable after cancellation");
        assert_eq!(receipts, 0);
    }

    #[test]
    fn receipt_operation_keys_escape_quotes_before_interpolation() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let token = CancellationToken::new();
        let operation_key = "retry' OR 1=1 --";
        owner
            .execute_transaction(
                "CREATE TABLE main.quoted_key_items (id BIGINT)",
                "create-quoted-key-items",
                &[1; 32],
                &token,
            )
            .expect("table should be created");

        assert_eq!(
            owner
                .execute_transaction(
                    "INSERT INTO main.quoted_key_items VALUES (42)",
                    operation_key,
                    &[2; 32],
                    &token,
                )
                .expect("quoted operation key should commit"),
            DuckDbTransactionOutcome::Committed
        );
        assert_eq!(
            owner
                .execute_transaction(
                    "INSERT INTO main.quoted_key_items VALUES (42)",
                    operation_key,
                    &[2; 32],
                    &token,
                )
                .expect("quoted operation key should replay"),
            DuckDbTransactionOutcome::Replayed
        );
        let rows = owner
            .connection
            .lock()
            .expect("owner connection should be available")
            .query_row("SELECT COUNT(*) FROM main.quoted_key_items", [], |row| {
                row.get::<_, u64>(0)
            })
            .expect("quoted-key table should remain queryable");
        assert_eq!(rows, 1, "replay must not apply the write twice");
    }

    #[test]
    fn concurrent_retries_with_the_same_identity_apply_one_write() {
        let database = TempDatabase::new();
        let owner = Arc::new(local_owner(&database));
        let token = CancellationToken::new();
        owner
            .execute_transaction(
                "CREATE TABLE main.concurrent_items (id BIGINT)",
                "create-concurrent-items",
                &[3; 32],
                &token,
            )
            .expect("table should be created");

        let workers = 8;
        let barrier = Arc::new(Barrier::new(workers));
        let handles = (0..workers)
            .map(|_| {
                let owner = Arc::clone(&owner);
                let barrier = Arc::clone(&barrier);
                let token = token.clone();
                thread::spawn(move || {
                    barrier.wait();
                    owner.execute_transaction(
                        "INSERT INTO main.concurrent_items VALUES (42)",
                        "append-concurrent-item",
                        &[4; 32],
                        &token,
                    )
                })
            })
            .collect::<Vec<_>>();

        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().expect("write worker should not panic"))
            .collect::<Result<Vec<_>, _>>()
            .expect("all identical retries should commit or replay");
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == DuckDbTransactionOutcome::Committed)
                .count(),
            1,
            "exactly one request should apply the write"
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == DuckDbTransactionOutcome::Replayed)
                .count(),
            workers - 1,
            "all remaining requests should use the committed receipt"
        );

        let connection = owner
            .connection
            .lock()
            .expect("owner connection should be available");
        let rows = connection
            .query_row("SELECT COUNT(*) FROM main.concurrent_items", [], |row| {
                row.get::<_, u64>(0)
            })
            .expect("table should remain queryable");
        assert_eq!(rows, 1, "concurrent replay must not duplicate the row");
    }

    #[test]
    fn local_query_stream_returns_schema_and_bounded_arrow_batches() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let token = CancellationToken::new();
        let mut output_schema = None;
        let mut row_counts = Vec::new();

        owner
            .stream_query(
                "SELECT 1::BIGINT AS id UNION ALL SELECT 2::BIGINT AS id",
                &token,
                DUCKDB_EXECUTION_MAX_BATCH_ROWS,
                DUCKDB_EXECUTION_MAX_BATCH_BYTES,
                |schema| {
                    output_schema = Some(schema);
                    Ok(())
                },
                |batch| {
                    row_counts.push(batch.row_count);
                    Ok(())
                },
            )
            .expect("small query should stream");

        let schema = output_schema.expect("query should emit its schema");
        assert_eq!(schema.logical_fields()[0].name, "id");
        assert_eq!(row_counts.iter().sum::<u64>(), 2);
        assert!(
            row_counts
                .iter()
                .all(|rows| *rows <= DUCKDB_EXECUTION_MAX_BATCH_ROWS as u64)
        );
    }

    #[test]
    fn slow_result_consumer_does_not_block_an_independent_read_query() {
        let database = TempDatabase::new();
        let owner = Arc::new(local_owner(&database));
        let (batch_started_tx, batch_started_rx) = mpsc::sync_channel(1);
        let (release_stream_tx, release_stream_rx) = mpsc::sync_channel(1);
        let slow_owner = Arc::clone(&owner);
        let slow_stream = thread::spawn(move || {
            let mut first_batch = true;
            slow_owner.stream_query(
                "SELECT value FROM range(1000) AS values(value)",
                &CancellationToken::new(),
                DUCKDB_EXECUTION_MAX_BATCH_ROWS,
                DUCKDB_EXECUTION_MAX_BATCH_BYTES,
                |_| Ok(()),
                |_| {
                    if first_batch {
                        first_batch = false;
                        batch_started_tx.send(()).map_err(|error| {
                            robin_sparkless_core::EngineError::internal(error.to_string())
                        })?;
                        release_stream_rx
                            .recv_timeout(Duration::from_secs(5))
                            .map_err(|error| {
                                robin_sparkless_core::EngineError::internal(error.to_string())
                            })?;
                    }
                    Ok(())
                },
            )
        });
        batch_started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the slow stream should hold its read connection");

        let (fast_query_tx, fast_query_rx) = mpsc::sync_channel(1);
        let fast_owner = Arc::clone(&owner);
        let fast_query = thread::spawn(move || {
            fast_owner.stream_query(
                "SELECT COUNT(*) AS rows FROM range(10)",
                &CancellationToken::new(),
                DUCKDB_EXECUTION_MAX_BATCH_ROWS,
                DUCKDB_EXECUTION_MAX_BATCH_BYTES,
                |_| Ok(()),
                |_| {
                    fast_query_tx.send(()).map_err(|error| {
                        robin_sparkless_core::EngineError::internal(error.to_string())
                    })
                },
            )
        });
        let fast_query_completed = fast_query_rx.recv_timeout(Duration::from_secs(2));
        release_stream_tx
            .send(())
            .expect("the slow stream should still be waiting");
        slow_stream
            .join()
            .expect("the slow stream worker should not panic")
            .expect("the slow stream should complete");
        fast_query
            .join()
            .expect("the independent query worker should not panic")
            .expect("the independent query should complete");
        fast_query_completed
            .expect("an independent read should complete while another result consumer is blocked");
    }

    #[test]
    fn pooled_local_read_connections_observe_committed_writes() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let token = CancellationToken::new();
        owner
            .stream_query(
                "SELECT 1 AS warmup",
                &token,
                DUCKDB_EXECUTION_MAX_BATCH_ROWS,
                DUCKDB_EXECUTION_MAX_BATCH_BYTES,
                |_| Ok(()),
                |_| Ok(()),
            )
            .expect("the first read should populate the query pool");
        owner
            .execute_transaction(
                "CREATE TABLE main.after_read_pool (id BIGINT)",
                "create-after-read-pool",
                &[1; 32],
                &token,
            )
            .expect("a write should commit after a pooled read");
        owner
            .execute_transaction(
                "INSERT INTO main.after_read_pool VALUES (42)",
                "append-after-read-pool",
                &[2; 32],
                &token,
            )
            .expect("the append should commit after a pooled read");

        let mut rows = 0;
        owner
            .stream_query(
                "SELECT id FROM main.after_read_pool",
                &token,
                DUCKDB_EXECUTION_MAX_BATCH_ROWS,
                DUCKDB_EXECUTION_MAX_BATCH_BYTES,
                |_| Ok(()),
                |batch| {
                    rows += batch.row_count;
                    Ok(())
                },
            )
            .expect("an existing pooled read connection should see committed writes");
        assert_eq!(rows, 1);
    }

    #[test]
    fn cancelled_read_pool_waiter_releases_queue_slot_and_connections_return() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let pool = Arc::clone(&owner.read_connections);
        let active = CancellationToken::new();
        let checked_out = (0..DUCKDB_EXECUTION_MAX_READ_CONNECTIONS)
            .map(|_| {
                pool.acquire(&active)
                    .expect("the pool should open each bounded read connection")
            })
            .collect::<Vec<_>>();

        let waiting_token = CancellationToken::new();
        let thread_token = waiting_token.clone();
        let waiter_pool = Arc::clone(&pool);
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let waiter = thread::spawn(move || {
            started_tx
                .send(())
                .expect("the test should still be waiting for the pool");
            waiter_pool.acquire(&thread_token)
        });
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the extra read should start waiting");

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let waiting = pool
                .state
                .lock()
                .expect("read pool state should be available")
                .waiting;
            if waiting == 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the extra read should enter the bounded wait queue"
            );
            thread::sleep(Duration::from_millis(5));
        }

        waiting_token.cancel();
        let error = match waiter
            .join()
            .expect("the waiting read worker should not panic")
        {
            Ok(_) => panic!("a cancelled read should leave the pool wait queue"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("operation cancelled"));
        assert_eq!(
            pool.state
                .lock()
                .expect("read pool state should be available")
                .waiting,
            0,
            "cancellation must release its queue slot"
        );

        drop(checked_out);
        let state = pool
            .state
            .lock()
            .expect("read pool state should be available");
        assert_eq!(state.opened, DUCKDB_EXECUTION_MAX_READ_CONNECTIONS);
        assert_eq!(
            state.connections.len(),
            DUCKDB_EXECUTION_MAX_READ_CONNECTIONS
        );
        assert_eq!(state.waiting, 0);
    }

    #[test]
    fn invalidated_checked_out_read_connection_is_closed_when_returned() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let pool = Arc::clone(&owner.read_connections);
        let checked_out = pool
            .acquire(&CancellationToken::new())
            .expect("the pool should open a read connection");
        assert_eq!(
            pool.state
                .lock()
                .expect("read pool state should be available")
                .opened,
            1
        );

        pool.invalidate();
        assert_eq!(
            pool.state
                .lock()
                .expect("read pool state should be available")
                .opened,
            1,
            "checked out connections remain accounted for until they return"
        );
        drop(checked_out);

        {
            let state = pool
                .state
                .lock()
                .expect("read pool state should be available");
            assert_eq!(state.opened, 0);
            assert!(state.connections.is_empty());
        }
        let replacement = pool
            .acquire(&CancellationToken::new())
            .expect("the pool should recover after invalidating an active connection");
        assert_eq!(
            pool.state
                .lock()
                .expect("read pool state should be available")
                .opened,
            1
        );
        drop(replacement);
    }

    #[test]
    fn query_stream_rejects_non_finite_and_infinite_scalar_values_before_emitting() {
        let database = TempDatabase::new();
        let owner = local_owner(&database);
        let cases = [
            ("SELECT CAST('NaN' AS DOUBLE) AS score", "non-finite"),
            ("SELECT DATE 'infinity' AS day", "infinite value"),
            ("SELECT TIMESTAMP 'infinity' AS seen_at", "infinite value"),
        ];

        for (sql, expected_error) in cases {
            let mut emitted_batches = 0;
            let error = owner
                .stream_query(
                    sql,
                    &CancellationToken::new(),
                    DUCKDB_EXECUTION_MAX_BATCH_ROWS,
                    DUCKDB_EXECUTION_MAX_BATCH_BYTES,
                    |_| Ok(()),
                    |_| {
                        emitted_batches += 1;
                        Ok(())
                    },
                )
                .expect_err("unsupported scalar values should be rejected before Arrow emission");
            assert!(error.to_string().contains(expected_error), "{error}");
            assert_eq!(emitted_batches, 0);
        }
    }
}
