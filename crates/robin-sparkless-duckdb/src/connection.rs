use crate::config::{ConfiguredOwner, QUACK_CATALOG};
use crate::scan::{quote_identifier, quote_literal};
use duckdb::{AccessMode, Config, Connection};
use robin_sparkless_core::EngineError;
use std::path::Path;

pub(crate) fn open_read_only(path: &Path) -> Result<Connection, EngineError> {
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(duckdb_failure)?;
    let connection = Connection::open_with_flags(path, config).map_err(duckdb_failure)?;
    configure_connection(&connection)?;
    Ok(connection)
}

pub(crate) fn open_source(owner: &ConfiguredOwner) -> Result<Connection, EngineError> {
    open_source_with_http_timeout(owner, None)
}

pub(crate) fn open_source_for_snapshot(owner: &ConfiguredOwner) -> Result<Connection, EngineError> {
    match owner {
        ConfiguredOwner::Local { .. } => open_source(owner),
        ConfiguredOwner::Quack { .. } => {
            // Bound extension attachment while opening a snapshot, then restore DuckDB's
            // configured default so long-running remote scans are not capped by startup time.
            let connection = open_source_with_http_timeout(owner, Some(3))?;
            connection
                .execute_batch("RESET http_timeout")
                .map_err(duckdb_failure)?;
            Ok(connection)
        }
    }
}

pub(crate) fn open_source_with_http_timeout(
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

pub(crate) fn quack_connection_id(connection: &Connection) -> Result<String, EngineError> {
    quack_connection_id_for(connection, QUACK_CATALOG)
}

pub(crate) fn quack_connection_id_for(
    connection: &Connection,
    catalog: &str,
) -> Result<String, EngineError> {
    connection
        .query_row(
            &format!("SELECT quack_connection_id({})", quote_literal(catalog)),
            [],
            |row| row.get(0),
        )
        .map_err(duckdb_failure)
}

pub(crate) fn cancel_quack_query(
    owner: &ConfiguredOwner,
    connection_id: &str,
) -> Result<bool, EngineError> {
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
        .ok_or_else(|| EngineError::internal("DuckDB Quack cancel returned no result".into()))?;
    row.get(1).map_err(duckdb_failure)
}

pub(crate) fn configure_connection(connection: &Connection) -> Result<(), EngineError> {
    connection
        .execute_batch("SET TimeZone = 'UTC'; SET threads = 1; SET memory_limit = '256MB';")
        .map_err(duckdb_failure)?;
    Ok(())
}

pub(crate) fn duckdb_failure(error: impl std::fmt::Display) -> EngineError {
    EngineError::invalid_input(format!("DuckDB source operation failed: {error}"))
}

pub(crate) fn duckdb_quack_failure(error: impl std::fmt::Display, token: &str) -> EngineError {
    let message = error.to_string().replace(token, "[redacted]");
    EngineError::invalid_input(format!("DuckDB Quack source operation failed: {message}"))
}
