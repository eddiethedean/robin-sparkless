//! One writable DuckDB owner used by the optional Spark Connect execution adapter.

use crate::config::{
    ConfiguredOwner, resolve_quack_extension_path, valid_identifier, validate_quack_uri,
};
use crate::connection::{configure_connection, duckdb_failure, quack_connection_id};
use crate::scan::{encode_batch, quote_identifier, quote_literal};
use crate::snapshot::{CancellationWatchdog, QuackCancellation};
use arrow60::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, TimeUnit};
use duckdb::Connection;
use duckdb::arrow::ipc::reader::StreamReader;
use duckdb::vtab::arrow::{ArrowBatchRegistration, ArrowVTab};
use duckdb::{ToSql, params};
use robin_sparkless_core::engine::{ArrowIpcStream, CancellationToken};
use robin_sparkless_core::{DataType, EngineError, LogicalField, StructType};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const EXECUTION_CATALOG: &str = "__sparkless_execution";
const EXECUTION_RECEIPTS_SCHEMA: &str = "__sparkless_meta";
const EXECUTION_RECEIPTS_TABLE: &str = "connect_operation_receipts";
pub const DUCKDB_EXECUTION_MAX_BATCH_ROWS: usize = 16_384;
pub const DUCKDB_EXECUTION_MAX_BATCH_BYTES: usize = 1024 * 1024;
/// Persistent idempotency receipts are bounded; new writes are rejected after this count.
pub const DUCKDB_EXECUTION_MAX_RECEIPTS: u64 = 1_000_000;

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
    owner: ConfiguredOwner,
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
        let connection = open_execution_connection(&owner)?;
        initialize_receipt_ledger(&connection, &owner)?;
        Ok(Self {
            connection: Mutex::new(connection),
            owner,
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
        let connection = self
            .connection
            .lock()
            .map_err(|_| EngineError::internal("DuckDB execution owner is unavailable".into()))?;
        let _watchdog = self.cancellation_watchdog(&connection, token)?;
        let mut statement = connection
            .prepare(sql)
            .map_err(|error| self.failure(error))?;
        let batches = statement
            .stream_arrow([])
            .map_err(|error| self.failure(error))?;
        let schema = arrow_schema_to_spark(&batches.get_schema())?;
        token.check_cancelled()?;
        Ok(schema)
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
            sql,
            &[],
            &[],
            None,
            0,
            0,
            0,
            token,
            max_batch_rows,
            max_batch_bytes,
            on_schema,
            consume,
        )
    }

    /// Stream a query containing one `arrow(?)` source per supplied Spark Connect LocalRelation.
    /// Inputs are decoded and bounded before the shared DuckDB connection is acquired.
    pub fn stream_query_with_arrow_ipc<S, B>(
        &self,
        sql: &str,
        inputs: &[Vec<u8>],
        expected_input_schemas: &[Option<StructType>],
        expected_output_schema: Option<&StructType>,
        max_input_bytes: usize,
        max_input_rows: usize,
        max_input_decoded_bytes: usize,
        token: &CancellationToken,
        max_batch_rows: usize,
        max_batch_bytes: usize,
        on_schema: S,
        mut consume: B,
    ) -> Result<(), EngineError>
    where
        S: FnOnce(StructType) -> Result<(), EngineError>,
        B: FnMut(ArrowIpcStream) -> Result<(), EngineError>,
    {
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
        let connection = self
            .connection
            .lock()
            .map_err(|_| EngineError::internal("DuckDB execution owner is unavailable".into()))?;
        let _watchdog = self.cancellation_watchdog(&connection, token)?;
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
        token.check_cancelled()?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| EngineError::internal("DuckDB execution owner is unavailable".into()))?;
        let _watchdog = self.cancellation_watchdog(&connection, token)?;
        connection
            .execute_batch("BEGIN TRANSACTION")
            .map_err(|error| self.failure(error))?;
        let ledger = receipt_ledger_reference(&self.owner);
        let fingerprint = hex_digest(request_fingerprint);
        let existing = match connection.query_row(
            &format!("SELECT (SELECT request_fingerprint FROM {ledger} WHERE operation_key = ?1)"),
            [operation_key],
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
                return Err(EngineError::internal(format!(
                    "DuckDB transaction outcome is unknown after statement failure: {}",
                    self.redact(&error.to_string())
                )));
            }
            return Err(self.failure(error));
        }
        if token.is_cancelled() {
            connection.execute_batch("ROLLBACK").map_err(|error| {
                EngineError::internal(format!(
                    "DuckDB transaction outcome is unknown after cancellation: {}",
                    self.redact(&error.to_string())
                ))
            })?;
            return Err(EngineError::invalid_input(
                "DuckDB write was cancelled before commit".into(),
            ));
        }
        connection
            .execute(
                &format!(
                    "INSERT INTO {ledger} (operation_key, request_fingerprint) VALUES (?1, ?2)"
                ),
                params![operation_key, fingerprint],
            )
            .map_err(|error| {
                if connection.execute_batch("ROLLBACK").is_err() {
                    EngineError::internal(format!(
                        "DuckDB transaction outcome is unknown after receipt failure: {}",
                        self.redact(&error.to_string())
                    ))
                } else {
                    self.failure(error)
                }
            })?;
        connection.execute_batch("COMMIT").map_err(|error| {
            EngineError::internal(format!(
                "DuckDB transaction outcome is unknown after commit was requested: {}",
                self.redact(&error.to_string())
            ))
        })?;
        trace_execution_witness("write", "committed", 0, 0);
        Ok(DuckDbTransactionOutcome::Committed)
    }

    fn cancellation_watchdog(
        &self,
        connection: &Connection,
        token: &CancellationToken,
    ) -> Result<CancellationWatchdog, EngineError> {
        let quack_cancel = if matches!(self.owner, ConfiguredOwner::Quack { .. }) {
            Some(QuackCancellation {
                owner: self.owner.clone(),
                connection_id: quack_connection_id(connection)?,
            })
        } else {
            None
        };
        CancellationWatchdog::new(token.clone(), connection.interrupt_handle(), quack_cancel)
    }

    fn failure(&self, error: impl std::fmt::Display) -> EngineError {
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
            Err(rollback_error) => EngineError::internal(format!(
                "DuckDB transaction cleanup failed before statement execution during {phase}: {error}; rollback failed: {}",
                self.redact(&rollback_error.to_string())
            )),
        }
    }

    fn redact(&self, message: &str) -> String {
        match &self.owner {
            ConfiguredOwner::Quack { token, .. } => message.replace(token, "[redacted]"),
            ConfiguredOwner::Local { .. } => message.to_string(),
        }
    }
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

fn open_execution_connection(owner: &ConfiguredOwner) -> Result<Connection, EngineError> {
    let connection = match owner {
        ConfiguredOwner::Local { database_path } => {
            Connection::open(database_path).map_err(duckdb_failure)?
        }
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

#[cfg(test)]
mod tests {
    use super::{
        DUCKDB_EXECUTION_MAX_BATCH_BYTES, DUCKDB_EXECUTION_MAX_BATCH_ROWS, DuckDbExecutionOwner,
        DuckDbExecutionOwnerConfig, DuckDbTransactionOutcome,
    };
    use robin_sparkless_core::engine::CancellationToken;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

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
            let precision = u8::try_from(*precision).map_err(|_| {
                EngineError::invalid_input("DuckDB decimal precision is out of range".into())
            })?;
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
