//! Driver-neutral JDBC configuration, identifier policy, and dialect utilities.
//!
//! Database execution and Polars conversion remain in `robin-sparkless-polars`; this crate owns
//! the validated storage options and SQL identifier rules shared by those adapters.

use robin_sparkless_core::EngineError;
use std::collections::HashMap;

pub mod sql_ident;

/// Match an optional JDBC driver class name without case sensitivity.
pub fn driver_hint_matches(driver: Option<&str>, expected: &str) -> bool {
    driver.is_some_and(|driver| driver.eq_ignore_ascii_case(expected))
}

/// JDBC-style options modeled after PySpark's JDBC DataSource V1 options.
#[derive(Debug, Clone)]
pub struct JdbcOptions {
    /// Connection URL, typically a JDBC URL.
    pub url: String,

    /// Table name or subquery (PySpark: `dbtable`).
    pub dbtable: Option<String>,

    /// Explicit SQL query (PySpark: `query`). Mutually exclusive with `dbtable`
    /// for reads; writers generally require `dbtable`.
    pub query: Option<String>,

    /// Username for authentication (PySpark: `user`).
    pub user: Option<String>,

    /// Password for authentication (PySpark: `password`).
    pub password: Option<String>,

    /// Optional driver class / dialect hint (PySpark: `driver`).
    pub driver: Option<String>,

    /// Compatibility field (`partitionColumn`); adapters do not implement partitioned reads.
    pub partition_column: Option<String>,

    /// Compatibility-only partition bound (`lowerBound`); no partitioned execution effect.
    pub lower_bound: Option<i64>,

    /// Compatibility-only partition bound (`upperBound`); no partitioned execution effect.
    pub upper_bound: Option<i64>,

    /// Compatibility-only partition count (`numPartitions`); no partitioned execution effect.
    pub num_partitions: Option<i32>,

    /// Requested fetch size (`fetchsize`); only PostgreSQL attempts a setting, not a verified fetch bound.
    pub fetch_size: Option<i32>,

    /// Driver-specific write batch size (`batchsize`); SQL Server uses it for progress reporting.
    pub batch_size: Option<i32>,

    // --- New PySpark JDBC options ---
    /// SQL to execute after opening connection (PySpark: `sessionInitStatement`).
    pub session_init_statement: Option<String>,

    /// Driver-specific timeout in seconds (`queryTimeout`); SQLite uses a busy timeout, DB2 ignores it.
    pub query_timeout: Option<i32>,

    /// SQL to execute before the main query, e.g. CTEs or temp tables (PySpark: `prepareQuery`).
    pub prepare_query: Option<String>,

    /// Custom schema for reads, e.g. "id DECIMAL(38,0), name STRING" (PySpark: `customSchema`).
    pub custom_schema: Option<String>,

    /// Use TRUNCATE instead of DELETE for Overwrite mode (PySpark: `truncate`).
    pub truncate: Option<bool>,

    /// Use CASCADE with TRUNCATE (PySpark: `cascadeTruncate`).
    pub cascade_truncate: Option<bool>,

    /// Transaction isolation level: NONE, READ_UNCOMMITTED, READ_COMMITTED,
    /// REPEATABLE_READ, SERIALIZABLE (PySpark: `isolationLevel`).
    pub isolation_level: Option<String>,

    /// Extra options for CREATE TABLE, e.g. "ENGINE=InnoDB" (PySpark: `createTableOptions`).
    pub create_table_options: Option<String>,

    /// Column types for CREATE TABLE, e.g. "name CHAR(64)" (PySpark: `createTableColumnTypes`).
    pub create_table_column_types: Option<String>,

    /// Raw options map so implementations can inspect additional keys without
    /// round-tripping through this struct.
    pub raw_options: HashMap<String, String>,
}

impl JdbcOptions {
    /// Construct `JdbcOptions` from a generic options map, following PySpark's
    /// JDBC conventions where possible.
    ///
    /// Required keys:
    /// - `url`
    ///
    /// For reads at least one of `dbtable` or `query` is typically required;
    /// writers require `dbtable`. That validation is performed by the
    /// higher-level read/write helpers so this function only enforces the
    /// presence of `url`.
    pub fn from_options_map(options: &HashMap<String, String>) -> Result<JdbcOptions, EngineError> {
        let url = options.get("url").cloned().ok_or_else(|| {
            EngineError::invalid_input("JDBC options: missing required option 'url'".to_string())
        })?;

        // Helper to parse an integer option with a clear error message.
        fn parse_i64_opt(
            options: &HashMap<String, String>,
            key: &str,
        ) -> Result<Option<i64>, EngineError> {
            if let Some(v) = options.get(key) {
                v.parse::<i64>().map(Some).map_err(|e| {
                    EngineError::invalid_input(format!(
                        "JDBC options: could not parse '{key}'='{}' as i64: {e}",
                        v
                    ))
                })
            } else {
                Ok(None)
            }
        }

        fn parse_i32_opt(
            options: &HashMap<String, String>,
            key: &str,
        ) -> Result<Option<i32>, EngineError> {
            if let Some(v) = options.get(key) {
                v.parse::<i32>().map(Some).map_err(|e| {
                    EngineError::invalid_input(format!(
                        "JDBC options: could not parse '{key}'='{}' as i32: {e}",
                        v
                    ))
                })
            } else {
                Ok(None)
            }
        }

        fn parse_bool_opt(options: &HashMap<String, String>, key: &str) -> Option<bool> {
            options.get(key).map(|v| v.eq_ignore_ascii_case("true"))
        }

        let dbtable = options.get("dbtable").cloned();
        let query = options.get("query").cloned();
        let user = options.get("user").cloned();
        let password = options.get("password").cloned();
        let driver = options.get("driver").cloned();

        let partition_column = options.get("partitionColumn").cloned();
        let lower_bound = parse_i64_opt(options, "lowerBound")?;
        let upper_bound = parse_i64_opt(options, "upperBound")?;
        let num_partitions = parse_i32_opt(options, "numPartitions")?;
        let fetch_size = parse_i32_opt(options, "fetchsize")?;
        let batch_size = parse_i32_opt(options, "batchsize")?;

        // New PySpark JDBC options
        let session_init_statement = options.get("sessionInitStatement").cloned();
        let query_timeout = parse_i32_opt(options, "queryTimeout")?;
        let prepare_query = options.get("prepareQuery").cloned();
        let custom_schema = options.get("customSchema").cloned();
        let truncate = parse_bool_opt(options, "truncate");
        let cascade_truncate = parse_bool_opt(options, "cascadeTruncate");
        let isolation_level = options.get("isolationLevel").cloned();
        let create_table_options = options.get("createTableOptions").cloned();
        let create_table_column_types = options.get("createTableColumnTypes").cloned();

        Ok(JdbcOptions {
            url,
            dbtable,
            query,
            user,
            password,
            driver,
            partition_column,
            lower_bound,
            upper_bound,
            num_partitions,
            fetch_size,
            batch_size,
            session_init_statement,
            query_timeout,
            prepare_query,
            custom_schema,
            truncate,
            cascade_truncate,
            isolation_level,
            create_table_options,
            create_table_column_types,
            raw_options: options.clone(),
        })
    }

    /// Convenience constructor from the common `.jdbc(url, table, properties)`
    /// signature used by PySpark.
    pub fn from_url_dbtable_and_properties(
        url: String,
        dbtable: String,
        properties: &HashMap<String, String>,
    ) -> Result<JdbcOptions, EngineError> {
        let mut opts = properties.clone();
        opts.insert("url".to_string(), url);
        opts.insert("dbtable".to_string(), dbtable);
        JdbcOptions::from_options_map(&opts)
    }

    /// Whether this options set looks like a "query"-based read (has `query`
    /// but no `dbtable`).
    pub fn is_query_based_read(&self) -> bool {
        self.query.is_some() && self.dbtable.is_none()
    }

    /// Whether this options set has a `dbtable` specified.
    pub fn has_dbtable(&self) -> bool {
        self.dbtable.is_some()
    }

    /// Reject arbitrary SQL options when hardened (see `SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL`).
    pub fn enforce_sql_trust_boundary(&self) -> Result<(), EngineError> {
        if jdbc_allow_arbitrary_sql() {
            return Ok(());
        }
        if self.query.is_some()
            || self.session_init_statement.is_some()
            || self.prepare_query.is_some()
        {
            return Err(EngineError::invalid_input(
                "JDBC query/sessionInitStatement/prepareQuery disabled. \
                 Set SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL=true or use dbtable only."
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Whether JDBC `query` / `sessionInitStatement` / `prepareQuery` are permitted.
pub fn jdbc_allow_arbitrary_sql() -> bool {
    if std::env::var("SPARKLESS_HARDENED")
        .ok()
        .is_some_and(|v| v == "1")
    {
        return std::env::var("SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL")
            .ok()
            .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    }
    std::env::var("SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL")
        .ok()
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true)
}

/// True if the URL is for SQLite (e.g. `jdbc:sqlite:/path` or `jdbc:sqlite:file:path`).
pub fn is_sqlite_url(url: &str) -> bool {
    url.starts_with("jdbc:sqlite") || url.starts_with("sqlite:")
}

/// True if the URL is for PostgreSQL (e.g. `jdbc:postgresql://...` or `postgres://...`).
pub fn is_postgres_url(url: &str) -> bool {
    url.starts_with("jdbc:postgresql")
        || url.starts_with("postgres://")
        || url.starts_with("postgresql://")
}

pub fn is_mysql_url(url: &str) -> bool {
    url.starts_with("jdbc:mysql:")
}

pub fn is_mariadb_url(url: &str) -> bool {
    url.starts_with("jdbc:mariadb:")
}

pub fn is_mssql_url(url: &str) -> bool {
    url.starts_with("jdbc:sqlserver:")
}

pub fn is_oracle_url(url: &str) -> bool {
    url.starts_with("jdbc:oracle:")
}

pub fn is_db2_url(url: &str) -> bool {
    url.starts_with("jdbc:db2:")
}
