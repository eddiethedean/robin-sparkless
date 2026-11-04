use crate::config::{ConfiguredOwner, ConfiguredSource, QUACK_CATALOG};
use crate::connection::{
    cancel_quack_query, duckdb_failure, open_source_for_snapshot, quack_connection_id,
};
use crate::connection_pool::{DuckDbSnapshotState, SourceConnectionPool};
use crate::scan::{duckdb_type_to_spark, quote_literal};
use duckdb::{Connection, InterruptHandle};
use robin_sparkless_core::engine::{
    CancellationToken, ExternalTableDescriptor, ExternalTableFormat,
};
use robin_sparkless_core::{EngineError, LogicalField, StructType};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

pub(crate) struct DuckDbReadSnapshot {
    pub(crate) sources: HashMap<String, Arc<ConfiguredSource>>,
    pub(crate) pool: Arc<SourceConnectionPool>,
    pub(crate) token: CancellationToken,
    pub(crate) state: Mutex<DuckDbSnapshotState>,
    pub(crate) rows: AtomicU64,
    pub(crate) ipc_bytes: AtomicUsize,
    pub(crate) decoded_bytes: AtomicUsize,
    pub(crate) failed: AtomicBool,
}

impl DuckDbReadSnapshot {
    pub(crate) fn new(
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

    pub(crate) fn source_for(&self, alias: &str) -> Result<Arc<ConfiguredSource>, EngineError> {
        self.sources
            .get(&alias.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| {
                EngineError::invalid_input(format!(
                    "DuckDB source alias '{alias}' is not configured"
                ))
            })
    }

    pub(crate) fn ensure_connection<'a>(
        &self,
        state: &'a mut DuckDbSnapshotState,
        source: &ConfiguredSource,
    ) -> Result<&'a Connection, EngineError> {
        let owner_identity = source.owner.identity();
        if let Some(existing_owner) = state.owner_identity.as_ref() {
            if existing_owner != &owner_identity {
                return Err(EngineError::invalid_input(
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
        self.token.check_cancelled()?;
        let connection = open_source_for_snapshot(&source.owner)?;
        self.token.check_cancelled()?;
        connection
            .execute_batch("BEGIN TRANSACTION")
            .map_err(duckdb_failure)?;
        state.owner_identity = Some(owner_identity);
        state.connection = Some(connection);
        state.permit = Some(permit);
        Ok(state.connection.as_ref().expect("connection just inserted"))
    }

    pub(crate) fn resolve_in_source(
        &self,
        state: &mut DuckDbSnapshotState,
        source: Arc<ConfiguredSource>,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
        self.token.check_cancelled()?;
        let connection = self.ensure_connection(state, &source)?;
        let quack_cancel = match &source.owner {
            ConfiguredOwner::Local { .. } => None,
            ConfiguredOwner::Quack { .. } => Some(QuackCancellation {
                owner: source.owner.clone(),
                connection_id: quack_connection_id(connection)?,
            }),
        };
        let _watchdog = CancellationWatchdog::new(
            self.token.clone(),
            connection.interrupt_handle(),
            quack_cancel,
        )?;
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
            self.token.check_cancelled()?;
            let name: String = row.get(0).map_err(duckdb_failure)?;
            let type_name: String = row.get(1).map_err(duckdb_failure)?;
            let nullability: String = row.get(2).map_err(duckdb_failure)?;
            fields.push(LogicalField::new(
                name,
                duckdb_type_to_spark(&type_name)?,
                !nullability.eq_ignore_ascii_case("NO"),
            ));
        }
        self.token.check_cancelled()?;
        if fields.is_empty() {
            return Ok(None);
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

pub(crate) struct CancellationWatchdog {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

pub(crate) struct QuackCancellation {
    pub(crate) owner: ConfiguredOwner,
    pub(crate) connection_id: String,
}

impl CancellationWatchdog {
    pub(crate) fn new(
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
                            let trace_enabled = std::env::var_os("SPARKLESS_CONNECT_TRACE")
                                .as_deref()
                                == Some(std::ffi::OsStr::new("1"));
                            if trace_enabled {
                                eprintln!("connect_trace event=duckdb_source_cancel_requested");
                            }
                            match cancel_quack_query(&quack.owner, &quack.connection_id) {
                                Ok(accepted) => {
                                    if trace_enabled {
                                        eprintln!(
                                            "connect_trace event=duckdb_source_remote_cancel accepted={accepted}"
                                        );
                                    }
                                    if !accepted {
                                        eprintln!(
                                            "sparkless DuckDB Quack cancellation was not accepted"
                                        );
                                    }
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
                EngineError::internal(format!("cannot start DuckDB cancellation watcher: {error}"))
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
