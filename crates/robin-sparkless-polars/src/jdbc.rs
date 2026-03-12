//! JDBC-style options and helpers for database IO.
//!
//! This module parses JDBC-like option maps (as used by
//! `DataFrameReader::format("jdbc").options(...)` and `DataFrameWriter::options(...)`)
//! and dispatches read/write to PostgreSQL (feature `jdbc`) or SQLite (feature `sqlite`)
//! based on the connection URL.

use std::collections::HashMap;

use crate::error::EngineError;

#[cfg(any(feature = "jdbc", feature = "sqlite"))]
use polars::prelude::{DataFrame as PlDataFrame, NamedFrom, Series};

#[cfg(feature = "jdbc")]
use postgres::{Client, NoTls};
#[cfg(feature = "jdbc")]
use postgres::types::Type as PgType;

/// JDBC-style options modeled after PySpark's JDBC DataSource V1 options.
///
/// In v1 we focus on PostgreSQL, but the struct is intentionally generic so it
/// can be reused for other databases later.
#[derive(Debug, Clone)]
pub struct JdbcOptions {
    /// Connection URL, typically a JDBC or PostgreSQL connection string.
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

    /// Optional driver class / dialect hint (PySpark: `driver`). For Rust we
    /// treat this as a hint to select an underlying driver implementation,
    /// defaulting to PostgreSQL when omitted.
    pub driver: Option<String>,

    /// Column used for partitioned reads (PySpark: `partitionColumn`).
    pub partition_column: Option<String>,

    /// Lower bound for partitioning (PySpark: `lowerBound`).
    pub lower_bound: Option<i64>,

    /// Upper bound for partitioning (PySpark: `upperBound`).
    pub upper_bound: Option<i64>,

    /// Number of partitions (PySpark: `numPartitions`).
    pub num_partitions: Option<i32>,

    /// JDBC fetch size (PySpark: `fetchsize`).
    pub fetch_size: Option<i32>,

    /// Batch size for writes (PySpark: `batchsize`).
    pub batch_size: Option<i32>,

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
            EngineError::User("JDBC options: missing required option 'url'".to_string())
        })?;

        // Helper to parse an integer option with a clear error message.
        fn parse_i64_opt(
            options: &HashMap<String, String>,
            key: &str,
        ) -> Result<Option<i64>, EngineError> {
            if let Some(v) = options.get(key) {
                v.parse::<i64>()
                    .map(Some)
                    .map_err(|e| {
                        EngineError::User(format!(
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
                v.parse::<i32>()
                    .map(Some)
                    .map_err(|e| {
                        EngineError::User(format!(
                            "JDBC options: could not parse '{key}'='{}' as i32: {e}",
                            v
                        ))
                    })
            } else {
                Ok(None)
            }
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

    /// Whether this options set looks like a \"query\"-based read (has `query`
    /// but no `dbtable`).
    pub fn is_query_based_read(&self) -> bool {
        self.query.is_some() && self.dbtable.is_none()
    }

    /// Whether this options set has a `dbtable` specified.
    pub fn has_dbtable(&self) -> bool {
        self.dbtable.is_some()
    }
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

/// Write a Polars `DataFrame` to a JDBC destination (PostgreSQL or SQLite) using
/// the given options and save mode.
#[cfg(any(feature = "jdbc", feature = "sqlite"))]
pub fn write_jdbc_from_polars(
    df: &PlDataFrame,
    opts: &JdbcOptions,
    mode: crate::dataframe::SaveMode,
) -> Result<(), EngineError> {
    if is_sqlite_url(&opts.url) {
        #[cfg(feature = "sqlite")]
        return write_jdbc_sqlite(df, opts, mode);
        #[cfg(not(feature = "sqlite"))]
        return Err(EngineError::User(
            "JDBC write: SQLite URL requires the 'sqlite' feature".to_string(),
        ));
    }
    if is_postgres_url(&opts.url) {
        #[cfg(feature = "jdbc")]
        return write_jdbc_postgres(df, opts, mode);
        #[cfg(not(feature = "jdbc"))]
        return Err(EngineError::User(
            "JDBC write: PostgreSQL URL requires the 'jdbc' feature".to_string(),
        ));
    }
    Err(EngineError::User(format!(
        "JDBC write: unsupported URL scheme in '{}'; supported: jdbc:sqlite:, jdbc:postgresql:, postgres://",
        opts.url
    )))
}

#[cfg(feature = "jdbc")]
fn write_jdbc_postgres(
    df: &PlDataFrame,
    opts: &JdbcOptions,
    mode: crate::dataframe::SaveMode,
) -> Result<(), EngineError> {
    use crate::dataframe::SaveMode as Sm;

    let mut url = opts.url.clone();
    if let Some(stripped) = url.strip_prefix("jdbc:") {
        url = stripped.to_string();
    }

    let table = opts.dbtable.as_deref().ok_or_else(|| {
        EngineError::User(
            "JDBC write: 'dbtable' option is required for writes (target table name)".to_string(),
        )
    })?;

    let mut client = Client::connect(&url, NoTls)
        .map_err(|e| EngineError::Io(format!("JDBC write: failed to connect: {e}")))?;

    match mode {
        Sm::Overwrite => {
            let _ = client.execute(&format!("TRUNCATE TABLE {table}"), &[]);
        }
        Sm::ErrorIfExists | Sm::Append | Sm::Ignore => {}
    }

    if df.height() == 0 {
        return Ok(());
    }

    let col_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|n| n.as_str().to_string())
        .collect();
    let placeholders: Vec<String> = (1..=col_names.len())
        .map(|i| format!("${i}"))
        .collect();
    let insert_sql = format!(
        "INSERT INTO {table} ({cols}) VALUES ({vals})",
        cols = col_names.join(", "),
        vals = placeholders.join(", ")
    );

    for row_idx in 0..df.height() {
        let mut params: Vec<Box<dyn postgres::types::ToSql + Sync>> =
            Vec::with_capacity(col_names.len());
        for col in df.columns() {
            let v = col.get(row_idx).map_err(|e| {
                EngineError::Internal(format!("JDBC write: get cell: {e}"))
            })?;
            let boxed: Box<dyn postgres::types::ToSql + Sync> = match v {
                polars::prelude::AnyValue::Null => Box::new(Option::<i32>::None),
                polars::prelude::AnyValue::Boolean(b) => Box::new(Some(b)),
                polars::prelude::AnyValue::Int64(i) => Box::new(Some(i)),
                polars::prelude::AnyValue::Int32(i) => Box::new(Some(i)),
                polars::prelude::AnyValue::Float64(f) => Box::new(Some(f)),
                polars::prelude::AnyValue::Float32(f) => Box::new(Some(f as f64)),
                polars::prelude::AnyValue::String(s) => Box::new(Some(s.to_string())),
                polars::prelude::AnyValue::StringOwned(ref s) => Box::new(Some(s.as_str().to_string())),
                other => Box::new(Some(other.to_string())),
            };
            params.push(boxed);
        }
        let mut param_refs: Vec<&(dyn postgres::types::ToSql + Sync)> =
            Vec::with_capacity(params.len());
        for p in &params {
            param_refs.push(p.as_ref());
        }
        client
            .execute(&insert_sql, &param_refs)
            .map_err(|e| EngineError::Sql(format!("JDBC write: insert failed: {e}")))?;
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
fn write_jdbc_sqlite(
    df: &PlDataFrame,
    opts: &JdbcOptions,
    mode: crate::dataframe::SaveMode,
) -> Result<(), EngineError> {
    use crate::dataframe::SaveMode as Sm;

    let path = sqlite_url_to_path(&opts.url)?;
    let table = opts.dbtable.as_deref().ok_or_else(|| {
        EngineError::User(
            "JDBC write: 'dbtable' option is required for writes (target table name)".to_string(),
        )
    })?;

    let conn = rusqlite::Connection::open(&path)
        .map_err(|e| EngineError::Io(format!("JDBC write (SQLite): failed to open: {e}")))?;

    match mode {
        Sm::Overwrite => {
            let _ = conn.execute(&format!("DELETE FROM {table}"), []);
        }
        Sm::ErrorIfExists | Sm::Append | Sm::Ignore => {}
    }

    if df.height() == 0 {
        return Ok(());
    }

    let col_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|n| n.as_str().to_string())
        .collect();
    let placeholders = (0..col_names.len()).map(|_| "?").collect::<Vec<_>>().join(", ");
    let insert_sql = format!(
        "INSERT INTO {table} ({cols}) VALUES ({vals})",
        cols = col_names.join(", "),
        vals = placeholders
    );

    let mut stmt = conn
        .prepare(&insert_sql)
        .map_err(|e| EngineError::Sql(format!("JDBC write (SQLite): prepare failed: {e}")))?;

    for row_idx in 0..df.height() {
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(col_names.len());
        for col in df.columns() {
            let v = col.get(row_idx).map_err(|e| {
                EngineError::Internal(format!("JDBC write: get cell: {e}"))
            })?;
            let boxed: Box<dyn rusqlite::ToSql> = match v {
                polars::prelude::AnyValue::Null => Box::new(rusqlite::types::Value::Null),
                polars::prelude::AnyValue::Boolean(b) => Box::new(if b { 1i32 } else { 0 }),
                polars::prelude::AnyValue::Int64(i) => Box::new(i),
                polars::prelude::AnyValue::Int32(i) => Box::new(i),
                polars::prelude::AnyValue::Float64(f) => Box::new(f),
                polars::prelude::AnyValue::Float32(f) => Box::new(f as f64),
                polars::prelude::AnyValue::String(s) => Box::new(s.to_string()),
                polars::prelude::AnyValue::StringOwned(ref s) => Box::new(s.as_str().to_string()),
                other => Box::new(other.to_string()),
            };
            params.push(boxed);
        }
        let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        stmt.execute(refs.as_slice())
            .map_err(|e| EngineError::Sql(format!("JDBC write (SQLite): insert failed: {e}")))?;
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
fn sqlite_url_to_path(url: &str) -> Result<std::path::PathBuf, EngineError> {
    let rest = url
        .strip_prefix("jdbc:sqlite:")
        .or_else(|| url.strip_prefix("sqlite:"))
        .ok_or_else(|| EngineError::User("SQLite URL must start with jdbc:sqlite: or sqlite:".to_string()))?;
    let path = rest
        .strip_prefix("file:")
        .unwrap_or(rest);
    Ok(std::path::PathBuf::from(path))
}

/// Read from a JDBC source (PostgreSQL or SQLite) into a Polars `DataFrame`.
///
/// This is a low-level helper; callers are expected to wrap the resulting
/// `PlDataFrame` in the crate's `DataFrame` type and apply case-sensitivity
/// behavior as needed.
#[cfg(any(feature = "jdbc", feature = "sqlite"))]
pub fn read_jdbc_to_polars(opts: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
    if is_sqlite_url(&opts.url) {
        #[cfg(feature = "sqlite")]
        return read_jdbc_sqlite(opts);
        #[cfg(not(feature = "sqlite"))]
        return Err(EngineError::User(
            "JDBC read: SQLite URL requires the 'sqlite' feature".to_string(),
        ));
    }
    if is_postgres_url(&opts.url) {
        #[cfg(feature = "jdbc")]
        return read_jdbc_postgres(opts);
        #[cfg(not(feature = "jdbc"))]
        return Err(EngineError::User(
            "JDBC read: PostgreSQL URL requires the 'jdbc' feature".to_string(),
        ));
    }
    Err(EngineError::User(format!(
        "JDBC read: unsupported URL scheme in '{}'; supported: jdbc:sqlite:, jdbc:postgresql:, postgres://",
        opts.url
    )))
}

#[cfg(feature = "jdbc")]
fn read_jdbc_postgres(opts: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
    let mut url = opts.url.clone();
    if let Some(stripped) = url.strip_prefix("jdbc:") {
        url = stripped.to_string();
    }

    let sql = if let Some(query) = &opts.query {
        query.clone()
    } else if let Some(table) = &opts.dbtable {
        format!("SELECT * FROM {table}")
    } else {
        return Err(EngineError::User(
            "JDBC read: either 'dbtable' or 'query' option is required".to_string(),
        ));
    };

    let mut client = Client::connect(&url, NoTls)
        .map_err(|e| EngineError::Io(format!("JDBC read: failed to connect: {e}")))?;

    if let Some(fetch) = opts.fetch_size {
        client
            .simple_query(&format!("SET SESSION FETCH_COUNT = {fetch}"))
            .map_err(|e| EngineError::Other(format!("JDBC read: failed to set fetchsize: {e}")))?;
    }

    let rows = client
        .query(&sql, &[])
        .map_err(|e| EngineError::Sql(format!("JDBC read: query failed: {e}")))?;

    if rows.is_empty() {
        return Ok(PlDataFrame::empty());
    }

    let columns = rows[0].columns();
    let mut series_vec: Vec<Series> = Vec::with_capacity(columns.len());
    for (idx, col) in columns.iter().enumerate() {
        let name = col.name().to_string();
        let ty = col.type_();
        let s = build_series_for_column(&name, idx, ty, &rows)?;
        series_vec.push(s);
    }
    let cols: Vec<polars::prelude::Column> = series_vec.into_iter().map(|s| s.into()).collect();
    PlDataFrame::new_infer_height(cols)
        .map_err(|e| EngineError::Internal(format!("JDBC read: failed to build DataFrame: {e}")))
}

#[cfg(feature = "sqlite")]
fn read_jdbc_sqlite(opts: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
    use rusqlite::types::Value;

    let path = sqlite_url_to_path(&opts.url)?;
    let sql = if let Some(query) = &opts.query {
        query.clone()
    } else if let Some(table) = &opts.dbtable {
        format!("SELECT * FROM {table}")
    } else {
        return Err(EngineError::User(
            "JDBC read: either 'dbtable' or 'query' option is required".to_string(),
        ));
    };

    let conn = rusqlite::Connection::open(&path)
        .map_err(|e| EngineError::Io(format!("JDBC read (SQLite): failed to open: {e}")))?;

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| EngineError::Sql(format!("JDBC read (SQLite): prepare failed: {e}")))?;

    let column_names: Vec<String> = stmt.column_names().iter().map(|s| (*s).to_string()).collect();
    let ncols = column_names.len();
    let mut columns_data: Vec<Vec<Option<Value>>> = (0..ncols).map(|_| Vec::new()).collect();

    let mut rows = stmt
        .query([])
        .map_err(|e| EngineError::Sql(format!("JDBC read (SQLite): query failed: {e}")))?;

    while let Some(row) = rows
        .next()
        .map_err(|e| EngineError::Other(format!("JDBC read (SQLite): row: {e}")))?
    {
        for (c, col_data) in columns_data.iter_mut().enumerate() {
            let v: Option<Value> = row.get(c).map_err(|e| {
                EngineError::Other(format!("JDBC read (SQLite): column {}: {e}", column_names[c]))
            })?;
            col_data.push(v);
        }
    }

    if columns_data.iter().all(|c| c.is_empty()) {
        return Ok(PlDataFrame::empty());
    }

    let mut series_vec: Vec<Series> = Vec::with_capacity(ncols);
    for (name, col_data) in column_names.iter().zip(columns_data.iter()) {
        let s = sqlite_values_to_series(name, col_data)?;
        series_vec.push(s);
    }
    let cols: Vec<polars::prelude::Column> = series_vec.into_iter().map(|s| s.into()).collect();
    PlDataFrame::new_infer_height(cols)
        .map_err(|e| EngineError::Internal(format!("JDBC read (SQLite): build DataFrame: {e}")))
}

#[cfg(feature = "sqlite")]
fn sqlite_values_to_series(
    name: &str,
    values: &[Option<rusqlite::types::Value>],
) -> Result<Series, EngineError> {
    use rusqlite::types::Value;

    if values.is_empty() {
        return Ok(Series::new(name.into(), Vec::<Option<i64>>::new()));
    }
    let mut has_int = false;
    let mut has_real = false;
    let mut has_text = false;
    for v in values {
        match v {
            None | Some(Value::Null) => {}
            Some(Value::Integer(_)) => has_int = true,
            Some(Value::Real(_)) => has_real = true,
            Some(Value::Text(_)) => has_text = true,
            Some(Value::Blob(_)) => {}
        }
    }
    if has_int && !has_real && !has_text {
        let vals: Vec<Option<i64>> = values
            .iter()
            .map(|v| match v {
                None | Some(Value::Null) => None,
                Some(Value::Integer(i)) => Some(*i),
                _ => None,
            })
            .collect();
        return Ok(Series::new(name.into(), vals));
    }
    if has_real && !has_text {
        let vals: Vec<Option<f64>> = values
            .iter()
            .map(|v| match v {
                None | Some(Value::Null) => None,
                Some(Value::Integer(i)) => Some(*i as f64),
                Some(Value::Real(f)) => Some(*f),
                _ => None,
            })
            .collect();
        return Ok(Series::new(name.into(), vals));
    }
    let vals: Vec<Option<String>> = values
        .iter()
        .map(|v| match v {
            None | Some(Value::Null) => None,
            Some(Value::Integer(i)) => Some(i.to_string()),
            Some(Value::Real(f)) => Some(f.to_string()),
            Some(Value::Text(s)) => Some(s.clone()),
            Some(Value::Blob(b)) => Some(format!("<{} bytes>", b.len())),
        })
        .collect();
    Ok(Series::new(name.into(), vals))
}

#[cfg(feature = "jdbc")]
fn build_series_for_column(
    name: &str,
    index: usize,
    ty: &PgType,
    rows: &[postgres::Row],
) -> Result<Series, EngineError> {
    use EngineError::*;

    match *ty {
        PgType::BOOL => {
            let mut vals: Vec<Option<bool>> = Vec::with_capacity(rows.len());
            for row in rows {
                let v: Option<bool> = row
                    .try_get(index)
                    .map_err(|e| Other(format!("JDBC read: bool column '{name}': {e}")))?;
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::INT2 => {
            let mut vals: Vec<Option<i16>> = Vec::with_capacity(rows.len());
            for row in rows {
                let v: Option<i16> = row
                    .try_get(index)
                    .map_err(|e| Other(format!("JDBC read: int2 column '{name}': {e}")))?;
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::INT4 => {
            let mut vals: Vec<Option<i32>> = Vec::with_capacity(rows.len());
            for row in rows {
                let v: Option<i32> = row
                    .try_get(index)
                    .map_err(|e| Other(format!("JDBC read: int4 column '{name}': {e}")))?;
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::INT8 => {
            let mut vals: Vec<Option<i64>> = Vec::with_capacity(rows.len());
            for row in rows {
                let v: Option<i64> = row
                    .try_get(index)
                    .map_err(|e| Other(format!("JDBC read: int8 column '{name}': {e}")))?;
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::FLOAT4 => {
            let mut vals: Vec<Option<f32>> = Vec::with_capacity(rows.len());
            for row in rows {
                let v: Option<f32> = row
                    .try_get(index)
                    .map_err(|e| Other(format!("JDBC read: float4 column '{name}': {e}")))?;
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::FLOAT8 => {
            let mut vals: Vec<Option<f64>> = Vec::with_capacity(rows.len());
            for row in rows {
                let v: Option<f64> = row
                    .try_get(index)
                    .map_err(|e| Other(format!("JDBC read: float8 column '{name}': {e}")))?;
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::TEXT | PgType::VARCHAR | PgType::BPCHAR | PgType::NAME => {
            let mut vals: Vec<Option<String>> = Vec::with_capacity(rows.len());
            for row in rows {
                let v: Option<String> = row
                    .try_get(index)
                    .map_err(|e| Other(format!("JDBC read: text-like column '{name}': {e}")))?;
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::TIMESTAMP | PgType::TIMESTAMPTZ => {
            // Read as string and parse; postgres may not have chrono FromSql by default.
            let mut vals: Vec<Option<chrono::NaiveDateTime>> = Vec::with_capacity(rows.len());
            for row in rows {
                let s: Option<String> = row
                    .try_get(index)
                    .map_err(|e| Other(format!("JDBC read: timestamp column '{name}': {e}")))?;
                let v = s.and_then(|s| {
                    chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f").ok()
                        .or_else(|| chrono::DateTime::parse_from_rfc3339(&s).ok().map(|dt| dt.naive_local()))
                });
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::DATE => {
            // Read as string and parse; postgres crate may not have chrono FromSql by default.
            let mut vals: Vec<Option<chrono::NaiveDate>> = Vec::with_capacity(rows.len());
            for row in rows {
                let s: Option<String> = row
                    .try_get(index)
                    .map_err(|e| Other(format!("JDBC read: date column '{name}': {e}")))?;
                let v = s.and_then(|s| chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok());
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::NUMERIC => {
            // Map NUMERIC to f64; read as string to avoid postgres_types::Numeric API.
            let mut vals: Vec<Option<f64>> = Vec::with_capacity(rows.len());
            for row in rows {
                let s: Option<String> = row
                    .try_get(index)
                    .map_err(|e| Other(format!("JDBC read: numeric column '{name}': {e}")))?;
                let f = s.and_then(|s| s.parse::<f64>().ok());
                vals.push(f);
            }
            Ok(Series::new(name.into(), vals))
        }
        _ => {
            // Fallback: convert values to string for unsupported types.
            let mut vals: Vec<Option<String>> = Vec::with_capacity(rows.len());
            for row in rows {
                let cell: Result<Option<String>, _> = row.try_get(index);
                match cell {
                    Ok(v) => vals.push(v),
                    Err(e) => {
                        return Err(Other(format!(
                            "JDBC read: unsupported column '{name}' type {ty:?}: {e}"
                        )))
                    }
                }
            }
            Ok(Series::new(name.into(), vals))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jdbc_options_from_map_requires_url() {
        let mut m = HashMap::new();
        m.insert("dbtable".to_string(), "t".to_string());
        let r = JdbcOptions::from_options_map(&m);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().to_lowercase().contains("url"));
    }

    #[test]
    fn jdbc_options_from_map_empty_url_key_not_present() {
        let m = HashMap::new();
        let r = JdbcOptions::from_options_map(&m);
        assert!(r.is_err());
    }

    #[test]
    fn jdbc_options_from_map_parses_options() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("dbtable".to_string(), "mytable".to_string());
        m.insert("user".to_string(), "u".to_string());
        m.insert("fetchsize".to_string(), "1000".to_string());
        let r = JdbcOptions::from_options_map(&m).unwrap();
        assert_eq!(r.url, "postgres://localhost/db");
        assert_eq!(r.dbtable.as_deref(), Some("mytable"));
        assert_eq!(r.user.as_deref(), Some("u"));
        assert_eq!(r.fetch_size, Some(1000));
    }

    #[test]
    fn jdbc_options_from_map_parses_pyspark_camel_case_keys() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "jdbc:postgresql://host/db".to_string());
        m.insert("dbtable".to_string(), "t".to_string());
        m.insert("partitionColumn".to_string(), "id".to_string());
        m.insert("lowerBound".to_string(), "0".to_string());
        m.insert("upperBound".to_string(), "10000".to_string());
        m.insert("numPartitions".to_string(), "4".to_string());
        m.insert("batchsize".to_string(), "500".to_string());
        let r = JdbcOptions::from_options_map(&m).unwrap();
        assert_eq!(r.partition_column.as_deref(), Some("id"));
        assert_eq!(r.lower_bound, Some(0));
        assert_eq!(r.upper_bound, Some(10000));
        assert_eq!(r.num_partitions, Some(4));
        assert_eq!(r.batch_size, Some(500));
    }

    #[test]
    fn jdbc_options_invalid_lower_bound_returns_error() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("lowerBound".to_string(), "not_a_number".to_string());
        let r = JdbcOptions::from_options_map(&m);
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(err.to_lowercase().contains("lowerbound"), "error should mention lowerBound: {}", err);
        assert!(err.contains("not_a_number"));
    }

    #[test]
    fn jdbc_options_invalid_num_partitions_returns_error() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("numPartitions".to_string(), "abc".to_string());
        let r = JdbcOptions::from_options_map(&m);
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(err.to_lowercase().contains("numpartitions"), "error should mention numPartitions: {}", err);
    }

    #[test]
    fn jdbc_options_query_and_helpers() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("query".to_string(), "SELECT 1 AS x".to_string());
        let r = JdbcOptions::from_options_map(&m).unwrap();
        assert!(r.is_query_based_read());
        assert!(!r.has_dbtable());
        assert_eq!(r.query.as_deref(), Some("SELECT 1 AS x"));
    }

    #[test]
    fn jdbc_options_dbtable_has_dbtable() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://localhost/db".to_string());
        m.insert("dbtable".to_string(), "mytable".to_string());
        let r = JdbcOptions::from_options_map(&m).unwrap();
        assert!(!r.is_query_based_read());
        assert!(r.has_dbtable());
    }

    #[test]
    fn jdbc_options_raw_options_preserves_all_keys() {
        let mut m = HashMap::new();
        m.insert("url".to_string(), "postgres://x/y".to_string());
        m.insert("dbtable".to_string(), "t".to_string());
        m.insert("customKey".to_string(), "customVal".to_string());
        let r = JdbcOptions::from_options_map(&m).unwrap();
        assert_eq!(r.raw_options.get("url").map(String::as_str), Some("postgres://x/y"));
        assert_eq!(r.raw_options.get("customKey").map(String::as_str), Some("customVal"));
    }

    #[test]
    fn jdbc_options_from_url_dbtable_and_properties() {
        let mut props = HashMap::new();
        props.insert("password".to_string(), "secret".to_string());
        let r = JdbcOptions::from_url_dbtable_and_properties(
            "postgres://h/d".to_string(),
            "tbl".to_string(),
            &props,
        )
        .unwrap();
        assert_eq!(r.url, "postgres://h/d");
        assert_eq!(r.dbtable.as_deref(), Some("tbl"));
        assert_eq!(r.password.as_deref(), Some("secret"));
    }

    #[test]
    fn jdbc_options_sqlite_and_postgres_url_detection() {
        assert!(super::is_sqlite_url("jdbc:sqlite:/tmp/db.db"));
        assert!(super::is_sqlite_url("jdbc:sqlite:file:foo.db"));
        assert!(super::is_sqlite_url("sqlite:/path/to/db"));
        assert!(!super::is_sqlite_url("jdbc:postgresql://localhost/db"));
        assert!(super::is_postgres_url("jdbc:postgresql://host/db"));
        assert!(super::is_postgres_url("postgres://localhost/db"));
        assert!(super::is_postgres_url("postgresql://localhost/db"));
        assert!(!super::is_postgres_url("jdbc:sqlite:mem"));
    }

    #[test]
    fn jdbc_options_from_url_dbtable_empty_properties() {
        let props = HashMap::new();
        let r = JdbcOptions::from_url_dbtable_and_properties(
            "postgresql://localhost/mydb".to_string(),
            "public.schema_table".to_string(),
            &props,
        )
        .unwrap();
        assert_eq!(r.url, "postgresql://localhost/mydb");
        assert_eq!(r.dbtable.as_deref(), Some("public.schema_table"));
        assert!(r.user.is_none());
        assert!(r.password.is_none());
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_read_write_roundtrip() {
        use crate::dataframe::SaveMode;
        use polars::prelude::{NamedFrom, Series};

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", []).unwrap();
        drop(conn);

        let id_series = Series::new("id".into(), vec![1i64, 2i64]);
        let name_series = Series::new("name".into(), vec!["a", "b"]);
        let df = PlDataFrame::new_infer_height(vec![id_series.into(), name_series.into()]).unwrap();

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            raw_options: HashMap::new(),
        };
        super::write_jdbc_from_polars(&df, &opts, SaveMode::Append).unwrap();

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 2);
        let names: Vec<&str> = read_df.get_column_names().iter().map(|n| n.as_str()).collect();
        assert_eq!(names, ["id", "name"]);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_overwrite_replaces_content() {
        use crate::dataframe::SaveMode;
        use polars::prelude::Series;

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", []).unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            raw_options: HashMap::new(),
        };

        let df1 = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), vec![1i64, 2i64]).into(),
            Series::new("name".into(), vec!["a", "b"]).into(),
        ])
        .unwrap();
        super::write_jdbc_from_polars(&df1, &opts, SaveMode::Append).unwrap();

        let df2 = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), vec![10i64]).into(),
            Series::new("name".into(), vec!["overwritten"]).into(),
        ])
        .unwrap();
        super::write_jdbc_from_polars(&df2, &opts, SaveMode::Overwrite).unwrap();

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 1);
        let names: Vec<&str> = read_df.get_column_names().iter().map(|n| n.as_str()).collect();
        assert_eq!(names, ["id", "name"]);
        let id_val: i64 = read_df.column("id").unwrap().get(0).unwrap().try_extract::<i64>().unwrap();
        assert_eq!(id_val, 10);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_read_with_query_option() {

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)", []).unwrap();
        conn.execute("INSERT INTO t (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three')", [])
            .unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url,
            dbtable: None,
            query: Some("SELECT id, name FROM t WHERE id = 2".to_string()),
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            raw_options: HashMap::new(),
        };
        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 1);
        let names: Vec<&str> = read_df.get_column_names().iter().map(|n| n.as_str()).collect();
        assert_eq!(names, ["id", "name"]);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_read_empty_table() {
        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE empty_t (a INTEGER, b TEXT)", []).unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url,
            dbtable: Some("empty_t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            raw_options: HashMap::new(),
        };
        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 0);
        // Empty result may be empty() (no columns) or 0-row with schema depending on backend
        if read_df.get_column_names().len() >= 2 {
            let names: Vec<&str> = read_df.get_column_names().iter().map(|n| n.as_str()).collect();
            assert_eq!(names, ["a", "b"]);
        }
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_multiple_types_and_nulls() {

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute(
            "CREATE TABLE types_t (id INTEGER, score REAL, label TEXT)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO types_t (id, score, label) VALUES (1, 3.14, 'pi'), (2, NULL, 'no_score'), (3, 0.5, NULL)",
            [],
        )
        .unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url,
            dbtable: Some("types_t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            raw_options: HashMap::new(),
        };
        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 3);
        let names: Vec<&str> = read_df.get_column_names().iter().map(|n| n.as_str()).collect();
        assert_eq!(names, ["id", "score", "label"]);
        // id: integers
        let id_col = read_df.column("id").unwrap();
        assert_eq!(id_col.get(0).unwrap().try_extract::<i64>().unwrap(), 1);
        assert_eq!(id_col.get(1).unwrap().try_extract::<i64>().unwrap(), 2);
        assert_eq!(id_col.get(2).unwrap().try_extract::<i64>().unwrap(), 3);
        // score: real with one NULL
        let score_col = read_df.column("score").unwrap();
        assert!((score_col.get(0).unwrap().try_extract::<f64>().unwrap() - 3.14).abs() < 1e-6);
        assert!(score_col.get(1).unwrap().is_null());
        assert!((score_col.get(2).unwrap().try_extract::<f64>().unwrap() - 0.5).abs() < 1e-6);
        // label: text with one NULL (AnyValue::to_string() quotes strings)
        let label_col = read_df.column("label").unwrap();
        assert_eq!(label_col.get(0).unwrap().to_string(), "\"pi\"");
        assert_eq!(label_col.get(1).unwrap().to_string(), "\"no_score\"");
        assert!(label_col.get(2).unwrap().is_null());
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_write_empty_dataframe_no_op() {
        use crate::dataframe::SaveMode;
        use polars::prelude::Series;

        let tmp = tempfile::Builder::new().suffix(".db").tempfile().unwrap();
        let db_path = tmp.path();
        let url = format!("jdbc:sqlite:{}", db_path.display());

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE t (id INTEGER)", []).unwrap();
        conn.execute("INSERT INTO t (id) VALUES (1)", []).unwrap();
        drop(conn);

        let opts = JdbcOptions {
            url: url.clone(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            raw_options: HashMap::new(),
        };

        let empty_df = PlDataFrame::new_infer_height(vec![Series::new("id".into(), Vec::<i64>::new()).into()]).unwrap();
        super::write_jdbc_from_polars(&empty_df, &opts, SaveMode::Append).unwrap();

        let read_df = super::read_jdbc_to_polars(&opts).unwrap();
        assert_eq!(read_df.height(), 1, "empty write should not remove existing row");
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_unsupported_url_returns_clear_error() {
        let opts = JdbcOptions {
            url: "jdbc:mysql://localhost/db".to_string(),
            dbtable: Some("t".to_string()),
            query: None,
            user: None,
            password: None,
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            raw_options: HashMap::new(),
        };
        let r = super::read_jdbc_to_polars(&opts);
        assert!(r.is_err());
        let err = r.unwrap_err().to_string();
        assert!(err.to_lowercase().contains("unsupported") || err.to_lowercase().contains("url"), "{}", err);
    }
}
