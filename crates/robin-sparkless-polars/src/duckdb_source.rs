//! Optional, read-only DuckDB catalog integration for Polars.
//!
//! Sources can be standalone local files or DuckDB owners served over Quack. The catalog never
//! accepts a path, URI, extension, or credential from a table identifier: the server supplies a
//! trusted alias-to-source map. A statement uses one DuckDB read transaction and exports typed
//! Arrow IPC batches before Polars evaluates its relational plan.

use duckdb::{AccessMode, Config, Connection, InterruptHandle};
use robin_sparkless_core::engine::{
    ArrowIpcStream, CancellationToken, ExternalCatalogProvider, ExternalCatalogReadSnapshot,
    ExternalTableDescriptor, ExternalTableFormat,
};
use robin_sparkless_core::{DataType, EngineError, LogicalField, StructType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
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
/// Maximum simultaneously open DuckDB source connections per configured catalog.
pub const DUCKDB_SOURCE_MAX_CONNECTIONS: usize = 4;
/// Maximum source operations queued while waiting for a DuckDB connection.
pub const DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS: usize = 16;

/// Trusted server configuration for a read-only standalone DuckDB database alias.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbSourceConfig {
    /// Prefix clients use in table names, such as `warehouse` in `warehouse.main.events`.
    pub alias: String,
    /// Path to an existing DuckDB database file. It is opened read-only.
    pub database_path: PathBuf,
}

/// Trusted server configuration for a read-only Quack-backed DuckDB alias.
///
/// `extension_path` must point to the Quack extension built for the exact DuckDB version and
/// platform bundled with this Sparkless build. Keeping the extension path explicit prevents a
/// source connection from downloading executable code at runtime.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbQuackSourceConfig {
    /// Prefix clients use in table names, such as `warehouse` in `warehouse.main.events`.
    pub alias: String,
    /// Quack URI such as `quack:localhost:9494` or `quack://localhost:9494`.
    pub quack_uri: String,
    /// Quack authentication token supplied by the trusted deployment configuration.
    pub token: String,
    /// Exact local DuckDB 2.0.0 Quack extension binary to load.
    pub extension_path: PathBuf,
    /// Optional transport override. Omit to use Quack's local/remote defaults.
    #[serde(default)]
    pub disable_ssl: Option<bool>,
    /// Optional expected TLS certificate fingerprint for a self-signed remote certificate.
    #[serde(default)]
    pub ssl_fingerprint: Option<String>,
}

impl std::fmt::Debug for DuckDbQuackSourceConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DuckDbQuackSourceConfig")
            .field("alias", &self.alias)
            .field("quack_uri", &self.quack_uri)
            .field("token", &"[redacted]")
            .field("extension_path", &self.extension_path)
            .field("disable_ssl", &self.disable_ssl)
            .field(
                "ssl_fingerprint",
                &self.ssl_fingerprint.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

/// One trusted source definition accepted by [`DuckDbCatalog::new_with_sources`].
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum DuckDbSourceSpec {
    /// Standalone read-only file mode.
    Local(DuckDbSourceConfig),
    /// Shared-owner Quack mode.
    Quack(DuckDbQuackSourceConfig),
}

#[derive(Clone)]
enum ConfiguredOwner {
    Local {
        database_path: PathBuf,
    },
    Quack {
        uri: String,
        token: String,
        extension_path: PathBuf,
        disable_ssl: Option<bool>,
        ssl_fingerprint: Option<String>,
    },
}

impl ConfiguredOwner {
    fn identity(&self) -> String {
        match self {
            Self::Local { database_path } => database_path.to_string_lossy().into_owned(),
            Self::Quack { uri, .. } => uri.clone(),
        }
    }

    fn quack_endpoint(&self) -> Option<&str> {
        match self {
            Self::Quack { uri, .. } => Some(uri),
            Self::Local { .. } => None,
        }
    }

    fn same_quack_connection(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Quack {
                    uri: left_uri,
                    token: left_token,
                    extension_path: left_extension,
                    disable_ssl: left_disable_ssl,
                    ssl_fingerprint: left_fingerprint,
                },
                Self::Quack {
                    uri: right_uri,
                    token: right_token,
                    extension_path: right_extension,
                    disable_ssl: right_disable_ssl,
                    ssl_fingerprint: right_fingerprint,
                },
            ) => {
                left_uri == right_uri
                    && left_token == right_token
                    && left_extension == right_extension
                    && left_disable_ssl == right_disable_ssl
                    && left_fingerprint == right_fingerprint
            }
            _ => false,
        }
    }
}

const QUACK_CATALOG: &str = "__sparkless_quack_source";

#[derive(Clone)]
struct ConfiguredSource {
    alias: String,
    owner: ConfiguredOwner,
}

/// Read-only DuckDB table resolver. This source is opt-in through the `duckdb-source` feature.
pub struct DuckDbCatalog {
    sources: HashMap<String, Arc<ConfiguredSource>>,
    pool: Arc<SourceConnectionPool>,
}

impl DuckDbCatalog {
    /// Validate and freeze trusted source aliases and paths.
    pub fn new(configs: impl IntoIterator<Item = DuckDbSourceConfig>) -> Result<Self, EngineError> {
        Self::new_with_sources(configs.into_iter().map(DuckDbSourceSpec::Local))
    }

    /// Validate and freeze trusted local-file and Quack source definitions.
    pub fn new_with_sources(
        configs: impl IntoIterator<Item = DuckDbSourceSpec>,
    ) -> Result<Self, EngineError> {
        let mut sources = HashMap::new();
        for spec in configs {
            let (alias_value, owner) = match spec {
                DuckDbSourceSpec::Local(config) => {
                    let database_path =
                        std::fs::canonicalize(&config.database_path).map_err(|error| {
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
                    (config.alias, ConfiguredOwner::Local { database_path })
                }
                DuckDbSourceSpec::Quack(config) => {
                    validate_quack_uri(&config.quack_uri)?;
                    if config.token.trim().len() < 4 {
                        return Err(EngineError::User(format!(
                            "DuckDB Quack source '{}' requires a token of at least four characters",
                            config.alias
                        )));
                    }
                    if config.disable_ssl == Some(true) && config.ssl_fingerprint.is_some() {
                        return Err(EngineError::User(format!(
                            "DuckDB Quack source '{}' cannot set an SSL fingerprint when SSL is disabled",
                            config.alias
                        )));
                    }
                    let extension_path =
                        std::fs::canonicalize(&config.extension_path).map_err(|error| {
                            EngineError::User(format!(
                                "DuckDB Quack source '{}' extension path is unavailable: {error}",
                                config.alias
                            ))
                        })?;
                    if !extension_path.is_file()
                        || extension_path.file_name().and_then(|name| name.to_str())
                            != Some("quack.duckdb_extension")
                    {
                        return Err(EngineError::User(format!(
                            "DuckDB Quack source '{}' extension path must name quack.duckdb_extension",
                            config.alias
                        )));
                    }
                    (
                        config.alias,
                        ConfiguredOwner::Quack {
                            uri: config.quack_uri,
                            token: config.token,
                            extension_path,
                            disable_ssl: config.disable_ssl,
                            ssl_fingerprint: config.ssl_fingerprint,
                        },
                    )
                }
            };
            if let Some(endpoint) = owner.quack_endpoint()
                && sources.values().any(|configured: &Arc<ConfiguredSource>| {
                    configured.owner.quack_endpoint() == Some(endpoint)
                        && !configured.owner.same_quack_connection(&owner)
                })
            {
                return Err(EngineError::User(
                    "DuckDB Quack aliases for one endpoint must use the same token and transport settings"
                        .into(),
                ));
            }
            if !valid_identifier(&alias_value) {
                return Err(EngineError::User(format!(
                    "DuckDB source alias {:?} must be a simple identifier",
                    alias_value
                )));
            }
            let alias = alias_value.to_ascii_lowercase();
            if sources.contains_key(&alias) {
                return Err(EngineError::User(format!(
                    "duplicate DuckDB source alias '{alias}'"
                )));
            }
            sources.insert(alias.clone(), Arc::new(ConfiguredSource { alias, owner }));
        }
        if sources.is_empty() {
            return Err(EngineError::User(
                "at least one DuckDB source alias is required".into(),
            ));
        }
        Ok(Self {
            sources,
            pool: Arc::new(SourceConnectionPool::default()),
        })
    }

    /// Parse the JSON value used by `SPARKLESS_DUCKDB_SOURCES`.
    pub fn from_json(json: &str) -> Result<Self, EngineError> {
        let configs: Vec<DuckDbSourceSpec> = serde_json::from_str(json).map_err(|error| {
            EngineError::User(format!("invalid SPARKLESS_DUCKDB_SOURCES JSON: {error}"))
        })?;
        Self::new_with_sources(configs)
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
        let snapshot = DuckDbReadSnapshot::new(
            self.sources.clone(),
            Arc::clone(&self.pool),
            CancellationToken::new(),
        );
        let mut state = snapshot
            .state
            .lock()
            .map_err(|_| EngineError::Internal("DuckDB snapshot lock poisoned".into()))?;
        snapshot.resolve_in_source(&mut state, source, &schema_name, &table_name)
    }

    fn is_read_only_target(&self, identifier: &str) -> Result<bool, EngineError> {
        let parts = identifier.split('.').collect::<Vec<_>>();
        if !(1..=3).contains(&parts.len()) || !parts.iter().all(|part| valid_identifier(part)) {
            return Ok(false);
        }
        Ok(self.sources.contains_key(&parts[0].to_ascii_lowercase()))
    }

    fn begin_read_snapshot(
        &self,
        token: &CancellationToken,
    ) -> Result<Option<Arc<dyn ExternalCatalogReadSnapshot>>, EngineError> {
        token.check_cancelled()?;
        Ok(Some(Arc::new(DuckDbReadSnapshot::new(
            self.sources.clone(),
            Arc::clone(&self.pool),
            token.clone(),
        ))))
    }

    fn scan_table(
        &self,
        table: &ExternalTableDescriptor,
        token: &CancellationToken,
        consume: &mut dyn FnMut(ArrowIpcStream) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let snapshot =
            DuckDbReadSnapshot::new(self.sources.clone(), Arc::clone(&self.pool), token.clone());
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
        let Some(source) = self.sources.get(&alias.to_ascii_lowercase()).cloned() else {
            return Ok(None);
        };
        Ok(Some((source, schema.to_string(), table.to_string())))
    }
}

/*
 * Source ownership, lookup, and scanning are deliberately implemented below the public
 * configuration surface so table identifiers never carry connection details.
 */

struct DuckDbSnapshotState {
    connection: Option<Connection>,
    owner_identity: Option<String>,
    permit: Option<SourceConnectionPermit>,
}

#[derive(Default)]
struct SourceConnectionPool {
    state: Mutex<SourceConnectionPoolState>,
    available: Condvar,
}

#[derive(Default)]
struct SourceConnectionPoolState {
    active: usize,
    waiting: usize,
}

impl SourceConnectionPool {
    fn acquire(
        self: &Arc<Self>,
        token: &CancellationToken,
        slots: usize,
    ) -> Result<SourceConnectionPermit, EngineError> {
        token.check_cancelled()?;
        if slots == 0 || slots > DUCKDB_SOURCE_MAX_CONNECTIONS {
            return Err(EngineError::Internal(
                "invalid DuckDB source connection reservation".into(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| EngineError::Internal("DuckDB source pool lock poisoned".into()))?;
        if state.active + slots > DUCKDB_SOURCE_MAX_CONNECTIONS {
            if state.waiting >= DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS {
                return Err(EngineError::User(format!(
                    "DuckDB source connection queue is full ({DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS} waiting operations)"
                )));
            }
            state.waiting += 1;
            while state.active + slots > DUCKDB_SOURCE_MAX_CONNECTIONS {
                if token.is_cancelled() {
                    state.waiting -= 1;
                    self.available.notify_all();
                    return Err(EngineError::Cancelled("operation cancelled".into()));
                }
                let (next_state, _) = self
                    .available
                    .wait_timeout(state, Duration::from_millis(10))
                    .map_err(|_| {
                        EngineError::Internal("DuckDB source pool lock poisoned".into())
                    })?;
                state = next_state;
            }
            state.waiting -= 1;
        }
        state.active += slots;
        Ok(SourceConnectionPermit {
            pool: Arc::clone(self),
            slots,
        })
    }
}

struct SourceConnectionPermit {
    pool: Arc<SourceConnectionPool>,
    slots: usize,
}

impl Drop for SourceConnectionPermit {
    fn drop(&mut self) {
        let mut state = self
            .pool
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active = state.active.saturating_sub(self.slots);
        // Waiters can reserve different slot counts (Quack scans reserve a second control slot),
        // so wake all of them to let any request that now fits claim the released capacity.
        self.pool.available.notify_all();
    }
}

struct DuckDbReadSnapshot {
    sources: HashMap<String, Arc<ConfiguredSource>>,
    pool: Arc<SourceConnectionPool>,
    token: CancellationToken,
    state: Mutex<DuckDbSnapshotState>,
    rows: AtomicU64,
    ipc_bytes: AtomicUsize,
    decoded_bytes: AtomicUsize,
    failed: AtomicBool,
}

impl DuckDbReadSnapshot {
    fn new(
        sources: HashMap<String, Arc<ConfiguredSource>>,
        pool: Arc<SourceConnectionPool>,
        token: CancellationToken,
    ) -> Self {
        Self {
            sources,
            pool,
            token,
            state: Mutex::new(DuckDbSnapshotState {
                connection: None,
                owner_identity: None,
                permit: None,
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
        let owner_identity = source.owner.identity();
        if let Some(existing_owner) = state.owner_identity.as_ref() {
            if existing_owner != &owner_identity {
                return Err(EngineError::User(
                    "one Connect statement cannot read DuckDB tables from different database owners"
                        .into(),
                ));
            }
            return Ok(state
                .connection
                .as_ref()
                .expect("owner identity and connection match"));
        }
        // Quack cancellation is a separate client request. Reserve its connection alongside
        // the scan connection so cancellation never exceeds the catalog's four-connection cap.
        let slots = if matches!(&source.owner, ConfiguredOwner::Quack { .. }) {
            2
        } else {
            1
        };
        let permit = self.pool.acquire(&self.token, slots)?;
        let connection = open_source(&source.owner)?;
        connection
            .execute_batch("BEGIN TRANSACTION")
            .map_err(duckdb_failure)?;
        state.owner_identity = Some(owner_identity);
        state.connection = Some(connection);
        state.permit = Some(permit);
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
        let sql = match &source.owner {
            ConfiguredOwner::Local { .. } => format!(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
                 WHERE lower(table_schema) = lower({}) AND lower(table_name) = lower({}) \
                 ORDER BY ordinal_position",
                quote_literal(schema_name),
                quote_literal(table_name)
            ),
            ConfiguredOwner::Quack { .. } => {
                let remote_query = format!(
                    "SELECT column_name, data_type, is_nullable \
                     FROM information_schema.columns \
                     WHERE lower(table_schema) = lower({}) AND lower(table_name) = lower({}) \
                     ORDER BY ordinal_position",
                    quote_literal(schema_name),
                    quote_literal(table_name)
                );
                format!(
                    "SELECT column_name, data_type, is_nullable FROM quack_query_by_name({}, {}, use_transaction := true)",
                    quote_literal(QUACK_CATALOG),
                    quote_literal(&remote_query)
                )
            }
        };
        let mut statement = connection.prepare(&sql).map_err(duckdb_failure)?;
        let mut rows = statement.query([]).map_err(duckdb_failure)?;
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
            return Err(EngineError::NotFound(format!(
                "DuckDB table '{}.{}.{}' was not found",
                source.alias, schema_name, table_name
            )));
        }
        let descriptor = ExternalTableDescriptor {
            table_id: format!("duckdb:{}:{schema_name}:{table_name}", source.alias),
            canonical_location: source.owner.identity(),
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
        let Some(source) = self.sources.get(&alias.to_ascii_lowercase()).cloned() else {
            return Ok(None);
        };
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
            if table.canonical_location != source.owner.identity() {
                return Err(EngineError::User(
                    "DuckDB table descriptor does not match its configured source owner".into(),
                ));
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| EngineError::Internal("DuckDB snapshot lock poisoned".into()))?;
            let connection = self.ensure_connection(&mut state, &source)?;
            token.check_cancelled()?;
            let quack_cancel = match &source.owner {
                ConfiguredOwner::Local { .. } => None,
                ConfiguredOwner::Quack { .. } => Some(QuackCancellation {
                    owner: source.owner.clone(),
                    connection_id: quack_connection_id(connection)?,
                }),
            };
            let _watchdog = CancellationWatchdog::new(
                token.clone(),
                connection.interrupt_handle(),
                quack_cancel,
            )?;
            let table_reference = match &source.owner {
                ConfiguredOwner::Local { .. } => format!(
                    "{}.{}",
                    quote_identifier(schema),
                    quote_identifier(table_name)
                ),
                ConfiguredOwner::Quack { .. } => format!(
                    "{}.{}.{}",
                    quote_identifier(QUACK_CATALOG),
                    quote_identifier(schema),
                    quote_identifier(table_name)
                ),
            };
            let sql = format!("SELECT * FROM {table_reference}");
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

struct QuackCancellation {
    owner: ConfiguredOwner,
    connection_id: String,
}

impl CancellationWatchdog {
    fn new(
        token: CancellationToken,
        interrupt: Arc<InterruptHandle>,
        quack: Option<QuackCancellation>,
    ) -> Result<Self, EngineError> {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker = std::thread::Builder::new()
            .name("sparkless-duckdb-cancel".into())
            .spawn(move || {
                let mut quack = quack;
                while !worker_stop.load(Ordering::Acquire) {
                    if token.is_cancelled() {
                        interrupt.interrupt();
                        // The local interrupt stops DuckDB's HTTP scan. Quack also needs an
                        // explicit cancellation request to stop work on the shared owner. The
                        // scan lease remains held until this request finishes.
                        if let Some(quack) = quack.take() {
                            match cancel_quack_query(&quack.owner, &quack.connection_id) {
                                Ok(true) => {}
                                Ok(false) => {
                                    eprintln!(
                                        "sparkless DuckDB Quack cancellation was not accepted"
                                    );
                                }
                                Err(_) => {
                                    eprintln!("sparkless DuckDB Quack cancellation request failed");
                                }
                            }
                        }
                        break;
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
    configure_connection(&connection)?;
    Ok(connection)
}

fn open_source(owner: &ConfiguredOwner) -> Result<Connection, EngineError> {
    open_source_with_http_timeout(owner, None)
}

fn open_source_with_http_timeout(
    owner: &ConfiguredOwner,
    http_timeout_seconds: Option<u64>,
) -> Result<Connection, EngineError> {
    match owner {
        ConfiguredOwner::Local { database_path } => open_read_only(database_path),
        ConfiguredOwner::Quack {
            uri,
            token,
            extension_path,
            disable_ssl,
            ssl_fingerprint,
        } => {
            let connection = Connection::open_in_memory().map_err(duckdb_failure)?;
            configure_connection(&connection)?;
            if let Some(timeout) = http_timeout_seconds {
                connection
                    .execute_batch(&format!("SET http_timeout = {timeout}"))
                    .map_err(duckdb_failure)?;
            }
            let extension_path = extension_path.to_str().ok_or_else(|| {
                EngineError::User("DuckDB Quack extension paths must be valid UTF-8".into())
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
            let attach = format!(
                "ATTACH {} AS {} ({})",
                quote_literal(uri),
                quote_identifier(QUACK_CATALOG),
                options.join(", ")
            );
            connection
                .execute_batch(&attach)
                .map_err(|error| duckdb_quack_failure(error, token))?;
            Ok(connection)
        }
    }
}

fn quack_connection_id(connection: &Connection) -> Result<String, EngineError> {
    connection
        .query_row(
            &format!(
                "SELECT quack_connection_id({})",
                quote_literal(QUACK_CATALOG)
            ),
            [],
            |row| row.get(0),
        )
        .map_err(duckdb_failure)
}

fn cancel_quack_query(owner: &ConfiguredOwner, connection_id: &str) -> Result<bool, EngineError> {
    // Cancellation must not wait forever if the owner is unreachable. This timeout applies only
    // to the short lived control connection; it does not cap source query duration.
    let connection = open_source_with_http_timeout(owner, Some(3))?;
    let sql = format!(
        "SELECT * FROM quack_cancel({}, {})",
        quote_literal(QUACK_CATALOG),
        quote_literal(connection_id)
    );
    let mut statement = connection.prepare(&sql).map_err(duckdb_failure)?;
    let mut rows = statement.query([]).map_err(duckdb_failure)?;
    let row = rows
        .next()
        .map_err(duckdb_failure)?
        .ok_or_else(|| EngineError::Internal("DuckDB Quack cancel returned no result".into()))?;
    row.get(1).map_err(duckdb_failure)
}

fn configure_connection(connection: &Connection) -> Result<(), EngineError> {
    connection
        .execute_batch("SET TimeZone = 'UTC'; SET threads = 1; SET memory_limit = '256MB';")
        .map_err(duckdb_failure)?;
    Ok(())
}

fn validate_quack_uri(uri: &str) -> Result<(), EngineError> {
    let address = uri
        .strip_prefix("quack://")
        .or_else(|| uri.strip_prefix("quack:"));
    let Some(address) = address else {
        return Err(EngineError::User(
            "DuckDB Quack source URI must use the quack: scheme".into(),
        ));
    };
    let invalid = address.is_empty()
        || address.chars().any(|character| {
            character.is_whitespace()
                || matches!(character, '\'' | '"' | '@' | '/' | '?' | '#' | '\\' | '\0')
        });
    if invalid {
        return Err(EngineError::User(
            "DuckDB Quack source URI must contain only a host and optional port".into(),
        ));
    }
    Ok(())
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

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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

fn duckdb_quack_failure(error: impl std::fmt::Display, token: &str) -> EngineError {
    let message = error.to_string().replace(token, "[redacted]");
    EngineError::User(format!("DuckDB Quack source operation failed: {message}"))
}

#[cfg(test)]
mod source_pool_tests {
    use super::*;
    use std::thread;

    fn wait_for_waiters(pool: &SourceConnectionPool, expected: usize) {
        for _ in 0..10_000 {
            if pool.state.lock().unwrap().waiting == expected {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("source pool did not reach {expected} waiters");
    }

    #[test]
    fn source_pool_enforces_connection_and_queue_caps() {
        let pool = Arc::new(SourceConnectionPool::default());
        let token = CancellationToken::new();
        let active = (0..DUCKDB_SOURCE_MAX_CONNECTIONS)
            .map(|_| pool.acquire(&token, 1).unwrap())
            .collect::<Vec<_>>();
        let waiters = (0..DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS)
            .map(|_| {
                let pool = Arc::clone(&pool);
                let token = token.clone();
                thread::spawn(move || {
                    let _permit = pool.acquire(&token, 1).unwrap();
                })
            })
            .collect::<Vec<_>>();
        wait_for_waiters(&pool, DUCKDB_SOURCE_MAX_QUEUED_CONNECTIONS);

        assert!(matches!(
            pool.acquire(&token, 1),
            Err(EngineError::User(message)) if message.contains("queue is full")
        ));
        drop(active);
        for waiter in waiters {
            waiter.join().unwrap();
        }
        assert_eq!(pool.state.lock().unwrap().active, 0);
    }

    #[test]
    fn source_pool_cancellation_removes_a_waiter() {
        let pool = Arc::new(SourceConnectionPool::default());
        let token = CancellationToken::new();
        let active = (0..DUCKDB_SOURCE_MAX_CONNECTIONS)
            .map(|_| pool.acquire(&token, 1).unwrap())
            .collect::<Vec<_>>();
        let waiter_token = token.clone();
        let waiter_pool = Arc::clone(&pool);
        let waiter = thread::spawn(move || waiter_pool.acquire(&waiter_token, 1));
        wait_for_waiters(&pool, 1);
        token.cancel();
        assert!(matches!(
            waiter.join().unwrap(),
            Err(EngineError::Cancelled(_))
        ));
        assert_eq!(pool.state.lock().unwrap().waiting, 0);
        drop(active);
    }

    #[test]
    fn source_pool_reserves_quack_data_and_cancel_connections_together() {
        let pool = Arc::new(SourceConnectionPool::default());
        let token = CancellationToken::new();
        let first = pool.acquire(&token, 2).unwrap();
        let second = pool.acquire(&token, 2).unwrap();
        assert_eq!(pool.state.lock().unwrap().active, 4);

        let waiter_pool = Arc::clone(&pool);
        let waiter_token = token.clone();
        let waiter = thread::spawn(move || waiter_pool.acquire(&waiter_token, 2));
        wait_for_waiters(&pool, 1);
        drop(first);
        let third = waiter.join().unwrap().unwrap();
        assert_eq!(pool.state.lock().unwrap().active, 4);

        drop((second, third));
        assert_eq!(pool.state.lock().unwrap().active, 0);
    }

    #[test]
    fn source_pool_wakes_waiters_whose_weight_fits_released_capacity() {
        use std::sync::mpsc;

        let pool = Arc::new(SourceConnectionPool::default());
        let token = CancellationToken::new();
        let pair = pool.acquire(&token, 2).unwrap();
        let first_single = pool.acquire(&token, 1).unwrap();
        let second_single = pool.acquire(&token, 1).unwrap();

        let (large_tx, large_rx) = mpsc::channel();
        let large_pool = Arc::clone(&pool);
        let large_token = token.clone();
        let large = thread::spawn(move || {
            let permit = large_pool.acquire(&large_token, 2).unwrap();
            large_tx.send(()).unwrap();
            permit
        });
        wait_for_waiters(&pool, 1);

        let (small_tx, small_rx) = mpsc::channel();
        let small_pool = Arc::clone(&pool);
        let small_token = token.clone();
        let small = thread::spawn(move || {
            let permit = small_pool.acquire(&small_token, 1).unwrap();
            small_tx.send(()).unwrap();
            permit
        });
        wait_for_waiters(&pool, 2);

        drop(first_single);
        small_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the one-slot waiter fits the released capacity");
        let small_permit = small.join().unwrap();
        drop((pair, second_single, small_permit));
        large_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the two-slot waiter fits after the larger reservation is released");
        let large_permit = large.join().unwrap();
        drop(large_permit);
        assert_eq!(pool.state.lock().unwrap().active, 0);
    }
}
