//! Polars-backed JDBC database adapters.
//!
//! The driver-neutral `robin-sparkless-jdbc` crate owns option parsing, URL policy, and SQL
//! identifier rules. This crate owns concrete database connections, reads/writes, and conversion
//! between database values and Polars frames. The Polars engine selects this adapter through its
//! feature graph and passes the session's immutable runtime configuration explicitly.

use polars::prelude::DataFrame as PlDataFrame;
use robin_sparkless_core::{EngineError, SessionRuntimeConfig};
use robin_sparkless_jdbc::{
    JdbcOptions, driver_hint_matches, is_db2_url, is_mariadb_url, is_mssql_url, is_mysql_url,
    is_oracle_url, is_postgres_url, is_sqlite_url,
};

#[cfg(feature = "db2")]
mod db2;
#[cfg(feature = "mariadb")]
mod mariadb;
#[cfg(feature = "mssql")]
mod mssql;
#[cfg(any(feature = "mysql", feature = "mariadb"))]
mod mysql;
#[cfg(feature = "oracle")]
mod oracle;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

/// Save behavior used by JDBC drivers without exposing the Polars facade's writer type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JdbcSaveMode {
    ErrorIfExists,
    Overwrite,
    Append,
    Ignore,
}

/// Write a Polars frame to the selected JDBC destination.
pub fn write_jdbc_from_polars(
    frame: &PlDataFrame,
    options: &JdbcOptions,
    mode: JdbcSaveMode,
    _runtime: &SessionRuntimeConfig,
) -> Result<(), EngineError> {
    if is_sqlite_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "org.sqlite.JDBC")
    {
        #[cfg(feature = "sqlite")]
        return sqlite::write_jdbc_sqlite(frame, options, mode);
        #[cfg(not(feature = "sqlite"))]
        return Err(missing_feature("write", "SQLite", "sqlite"));
    }
    if is_postgres_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "org.postgresql.Driver")
    {
        #[cfg(feature = "postgres")]
        return postgres::write_jdbc_postgres(frame, options, mode);
        #[cfg(not(feature = "postgres"))]
        return Err(missing_feature("write", "PostgreSQL", "postgres"));
    }
    if is_mysql_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "com.mysql.cj.jdbc.Driver")
    {
        #[cfg(feature = "mysql")]
        return mysql::write_jdbc_mysql(frame, options, mode, _runtime);
        #[cfg(not(feature = "mysql"))]
        return Err(missing_feature("write", "MySQL", "mysql"));
    }
    if is_mariadb_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "org.mariadb.jdbc.Driver")
    {
        #[cfg(feature = "mariadb")]
        return mariadb::write_jdbc_mariadb(frame, options, mode, _runtime);
        #[cfg(not(feature = "mariadb"))]
        return Err(missing_feature("write", "MariaDB", "mariadb"));
    }
    if is_mssql_url(&options.url)
        || driver_hint_matches(
            options.driver.as_deref(),
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        )
    {
        #[cfg(feature = "mssql")]
        return mssql::write_jdbc_mssql(frame, options, mode);
        #[cfg(not(feature = "mssql"))]
        return Err(missing_feature("write", "SQL Server", "mssql"));
    }
    if is_oracle_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "oracle.jdbc.OracleDriver")
    {
        #[cfg(feature = "oracle")]
        return oracle::write_jdbc_oracle(frame, options, mode);
        #[cfg(not(feature = "oracle"))]
        return Err(missing_feature("write", "Oracle", "oracle"));
    }
    if is_db2_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "com.ibm.db2.jcc.DB2Driver")
    {
        #[cfg(feature = "db2")]
        return db2::write_jdbc_db2(frame, options, mode);
        #[cfg(not(feature = "db2"))]
        return Err(missing_feature("write", "DB2", "db2"));
    }

    Err(EngineError::invalid_input(format!(
        "JDBC write: unsupported URL scheme in '{}'",
        robin_sparkless_jdbc::sql_ident::redact_jdbc_url(&options.url)
    )))
}

/// Read a JDBC source into a Polars frame using the supplied session compatibility settings.
pub fn read_jdbc_to_polars(
    options: &JdbcOptions,
    _runtime: &SessionRuntimeConfig,
) -> Result<PlDataFrame, EngineError> {
    options.enforce_sql_trust_boundary()?;
    if is_sqlite_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "org.sqlite.JDBC")
    {
        #[cfg(feature = "sqlite")]
        return sqlite::read_jdbc_sqlite(options);
        #[cfg(not(feature = "sqlite"))]
        return Err(missing_feature("read", "SQLite", "sqlite"));
    }
    if is_postgres_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "org.postgresql.Driver")
    {
        #[cfg(feature = "postgres")]
        return postgres::read_jdbc_postgres(options, _runtime);
        #[cfg(not(feature = "postgres"))]
        return Err(missing_feature("read", "PostgreSQL", "postgres"));
    }
    if is_mysql_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "com.mysql.cj.jdbc.Driver")
    {
        #[cfg(feature = "mysql")]
        return mysql::read_jdbc_mysql(options, _runtime);
        #[cfg(all(feature = "mariadb", not(feature = "mysql")))]
        return Err(missing_feature("read", "MySQL", "mysql"));
        #[cfg(not(any(feature = "mysql", feature = "mariadb")))]
        return Err(missing_feature("read", "MySQL", "mysql"));
    }
    if is_mariadb_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "org.mariadb.jdbc.Driver")
    {
        #[cfg(feature = "mariadb")]
        return mariadb::read_jdbc_mariadb(options, _runtime);
        #[cfg(not(feature = "mariadb"))]
        return Err(missing_feature("read", "MariaDB", "mariadb"));
    }
    if is_mssql_url(&options.url)
        || driver_hint_matches(
            options.driver.as_deref(),
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        )
    {
        #[cfg(feature = "mssql")]
        return mssql::read_jdbc_mssql(options, _runtime);
        #[cfg(not(feature = "mssql"))]
        return Err(missing_feature("read", "SQL Server", "mssql"));
    }
    if is_oracle_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "oracle.jdbc.OracleDriver")
    {
        #[cfg(feature = "oracle")]
        return oracle::read_jdbc_oracle(options);
        #[cfg(not(feature = "oracle"))]
        return Err(missing_feature("read", "Oracle", "oracle"));
    }
    if is_db2_url(&options.url)
        || driver_hint_matches(options.driver.as_deref(), "com.ibm.db2.jcc.DB2Driver")
    {
        #[cfg(feature = "db2")]
        return db2::read_jdbc_db2(options);
        #[cfg(not(feature = "db2"))]
        return Err(missing_feature("read", "DB2", "db2"));
    }

    Err(EngineError::invalid_input(format!(
        "JDBC read: unsupported URL scheme in '{}'",
        robin_sparkless_jdbc::sql_ident::redact_jdbc_url(&options.url)
    )))
}

#[allow(dead_code)] // Used by builds that enable only a subset of database drivers.
fn missing_feature(operation: &str, driver: &str, feature: &str) -> EngineError {
    EngineError::invalid_input(format!(
        "JDBC {operation}: {driver} URL/driver requires the '{feature}' feature"
    ))
}
