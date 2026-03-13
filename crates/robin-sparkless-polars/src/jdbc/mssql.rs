use crate::error::EngineError;
use crate::jdbc::JdbcOptions;

use futures_util::TryStreamExt;
use polars::prelude::{DataFrame as PlDataFrame, NamedFrom, Series};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use tiberius::{AuthMethod, Client, ColumnType, Config, Query, QueryItem};

#[derive(Debug, Clone, Copy)]
enum ColKind {
    I64,
    F64,
    Bool,
    Str,
}

#[derive(Debug, Clone)]
enum Cell {
    I64(Option<i64>),
    F64(Option<f64>),
    Bool(Option<bool>),
    Str(Option<String>),
}

fn parse_sqlserver_jdbc_url(url: &str) -> Result<(String, u16, Option<String>), EngineError> {
    // Support the common PySpark JDBC URL shape:
    //   jdbc:sqlserver://host:1433;databaseName=db;encrypt=false;trustServerCertificate=true
    let mut u = url.to_string();
    if let Some(stripped) = u.strip_prefix("jdbc:") {
        u = stripped.to_string();
    }
    let rest = u.strip_prefix("sqlserver://").ok_or_else(|| {
        EngineError::User(format!(
            "JDBC SQL Server: URL must start with jdbc:sqlserver:// (got '{url}')"
        ))
    })?;

    let (host_port, params) = rest.split_once(';').unwrap_or((rest, ""));
    let (host, port) = if let Some((h, p)) = host_port.rsplit_once(':') {
        let port = p.parse::<u16>().map_err(|e| {
            EngineError::User(format!("JDBC SQL Server: invalid port '{p}': {e}"))
        })?;
        (h.to_string(), port)
    } else {
        (host_port.to_string(), 1433)
    };

    let mut db_name: Option<String> = None;
    for part in params.split(';').filter(|s| !s.is_empty()) {
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        if k.eq_ignore_ascii_case("databaseName") && !v.is_empty() {
            db_name = Some(v.to_string());
        }
    }

    Ok((host, port, db_name))
}

async fn connect_async(
    opts: &JdbcOptions,
) -> Result<Client<tokio_util::compat::Compat<TcpStream>>, EngineError> {
    let (host, port, db_name) = parse_sqlserver_jdbc_url(&opts.url)?;

    let user = opts.user.clone().unwrap_or_default();
    let password = opts.password.clone().unwrap_or_default();
    if user.is_empty() {
        return Err(EngineError::User(
            "JDBC SQL Server: missing 'user' option".to_string(),
        ));
    }

    let mut config = Config::new();
    config.host(host);
    config.port(port);
    if let Some(db) = db_name {
        config.database(db);
    }
    config.authentication(AuthMethod::sql_server(user, password));
    // Local dev defaults: allow self-signed certs.
    config.trust_cert();

    let addr = config.get_addr();
    let tcp = TcpStream::connect(addr)
        .await
        .map_err(|e| EngineError::Io(format!("JDBC SQL Server: connect tcp failed: {e}")))?;
    tcp.set_nodelay(true).ok();

    Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| EngineError::Io(format!("JDBC SQL Server: connect failed: {e}")))
}

fn with_runtime<T>(
    fut: impl std::future::Future<Output = Result<T, EngineError>>,
) -> Result<T, EngineError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| {
            EngineError::Internal(format!("JDBC SQL Server: failed to create runtime: {e}"))
        })?
        .block_on(fut)
}

fn kind_from_column_type(ty: ColumnType) -> ColKind {
    match ty {
        ColumnType::Bit => ColKind::Bool,
        ColumnType::Int1
        | ColumnType::Int2
        | ColumnType::Int4
        | ColumnType::Int8
        | ColumnType::Intn => ColKind::I64,
        ColumnType::Float4 | ColumnType::Float8 | ColumnType::Floatn => ColKind::F64,
        _ => ColKind::Str,
    }
}

pub(crate) fn read_jdbc_mssql(opts: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
    let sql = if let Some(query) = &opts.query {
        query.clone()
    } else if let Some(table) = &opts.dbtable {
        format!("SELECT * FROM {table}")
    } else {
        return Err(EngineError::User(
            "JDBC read: either 'dbtable' or 'query' option is required".to_string(),
        ));
    };

    with_runtime(async move {
        let mut client = connect_async(opts).await?;

        // Execute session initialization statement if provided
        if let Some(init_sql) = &opts.session_init_statement {
            client
                .execute(init_sql.as_str(), &[])
                .await
                .map_err(|e| EngineError::Sql(format!("JDBC read (SQL Server): sessionInitStatement failed: {e}")))?;
        }

        // Set query timeout if provided (SQL Server uses SET LOCK_TIMEOUT in milliseconds)
        if let Some(timeout_secs) = opts.query_timeout {
            let timeout_ms = timeout_secs * 1000;
            client
                .execute(format!("SET LOCK_TIMEOUT {timeout_ms}").as_str(), &[])
                .await
                .map_err(|e| EngineError::Sql(format!("JDBC read (SQL Server): failed to set queryTimeout: {e}")))?;
        }

        // Execute prepare query if provided
        if let Some(prep_sql) = &opts.prepare_query {
            client
                .execute(prep_sql.as_str(), &[])
                .await
                .map_err(|e| EngineError::Sql(format!("JDBC read (SQL Server): prepareQuery failed: {e}")))?;
        }

        let mut stream = client
            .query(sql, &[])
            .await
            .map_err(|e| EngineError::Sql(format!("JDBC read (SQL Server): query failed: {e}")))?;

        let columns_opt = stream
            .columns()
            .await
            .map_err(|e| EngineError::Other(format!("JDBC read (SQL Server): columns: {e}")))?;
        let columns = columns_opt.ok_or_else(|| {
            EngineError::Internal("JDBC read (SQL Server): missing columns metadata".to_string())
        })?;

        let column_names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();
        let column_kinds: Vec<ColKind> = columns
            .iter()
            .map(|c| kind_from_column_type(c.column_type()))
            .collect();
        let ncols = column_names.len();

        let mut columns_data: Vec<Vec<Cell>> = (0..ncols).map(|_| Vec::new()).collect();

        while let Some(item) = stream
            .try_next()
            .await
            .map_err(|e| EngineError::Other(format!("JDBC read (SQL Server): stream: {e}")))?
        {
            if let QueryItem::Row(row) = item {
                for c in 0..ncols {
                    let cell = match column_kinds[c] {
                        ColKind::I64 => Cell::I64(row.get::<i64, _>(c)),
                        ColKind::F64 => Cell::F64(row.get::<f64, _>(c)),
                        ColKind::Bool => Cell::Bool(row.get::<bool, _>(c)),
                        ColKind::Str => {
                            let s: Option<&str> = row.get(c);
                            Cell::Str(s.map(|s| s.to_string()))
                        }
                    };
                    columns_data[c].push(cell);
                }
            }
        }

        if columns_data.iter().all(|c| c.is_empty()) {
            return Ok(PlDataFrame::empty());
        }

        let mut series_vec: Vec<Series> = Vec::with_capacity(ncols);
        for (idx, name) in column_names.iter().enumerate() {
            series_vec.push(cells_to_series(name, &columns_data[idx]));
        }
        let cols: Vec<polars::prelude::Column> = series_vec.into_iter().map(|s| s.into()).collect();
        PlDataFrame::new_infer_height(cols)
            .map_err(|e| EngineError::Internal(format!("JDBC read (SQL Server): build DataFrame: {e}")))
    })
}

fn cells_to_series(name: &str, cells: &[Cell]) -> Series {
    if cells.iter().any(|c| matches!(c, Cell::Str(_))) {
        let vals: Vec<Option<String>> = cells
            .iter()
            .map(|c| match c {
                Cell::Str(s) => s.clone(),
                Cell::I64(i) => i.map(|v| v.to_string()),
                Cell::F64(f) => f.map(|v| v.to_string()),
                Cell::Bool(b) => b.map(|v| if v { "true" } else { "false" }.to_string()),
            })
            .collect();
        return Series::new(name.into(), vals);
    }
    if cells.iter().any(|c| matches!(c, Cell::F64(_))) {
        let vals: Vec<Option<f64>> = cells
            .iter()
            .map(|c| match c {
                Cell::F64(f) => *f,
                Cell::I64(i) => i.map(|v| v as f64),
                Cell::Bool(b) => b.map(|v| if v { 1.0 } else { 0.0 }),
                Cell::Str(_) => None,
            })
            .collect();
        return Series::new(name.into(), vals);
    }
    if cells.iter().any(|c| matches!(c, Cell::Bool(_))) {
        let vals: Vec<Option<bool>> = cells
            .iter()
            .map(|c| match c {
                Cell::Bool(b) => *b,
                _ => None,
            })
            .collect();
        return Series::new(name.into(), vals);
    }
    let vals: Vec<Option<i64>> = cells
        .iter()
        .map(|c| match c {
            Cell::I64(i) => *i,
            _ => None,
        })
        .collect();
    Series::new(name.into(), vals)
}

pub(crate) fn write_jdbc_mssql(
    df: &PlDataFrame,
    opts: &JdbcOptions,
    mode: crate::dataframe::SaveMode,
) -> Result<(), EngineError> {
    use crate::dataframe::SaveMode as Sm;

    let table = opts.dbtable.as_deref().ok_or_else(|| {
        EngineError::User(
            "JDBC write: 'dbtable' option is required for writes (target table name)".to_string(),
        )
    })?;

    let col_names: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|n| n.as_str().to_string())
        .collect();
    let placeholders = (1..=col_names.len())
        .map(|i| format!("@P{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "INSERT INTO {table} ({cols}) VALUES ({vals})",
        cols = col_names.join(", "),
        vals = placeholders
    );

    with_runtime(async move {
        let mut client = connect_async(opts).await?;

        // Execute session initialization statement if provided
        if let Some(init_sql) = &opts.session_init_statement {
            client
                .execute(init_sql.as_str(), &[])
                .await
                .map_err(|e| EngineError::Sql(format!("JDBC write (SQL Server): sessionInitStatement failed: {e}")))?;
        }

        match mode {
            Sm::ErrorIfExists => {
                let mut stream = client
                    .query(format!("SELECT COUNT(*) FROM {table}"), &[])
                    .await
                    .map_err(|e| EngineError::Sql(format!("JDBC write (SQL Server): check table: {e}")))?;
                let mut count: i64 = 0;
                while let Some(item) = stream.try_next().await.ok().flatten() {
                    if let QueryItem::Row(row) = item {
                        count = row.get::<i64, _>(0).unwrap_or(0);
                    }
                }
                if count > 0 {
                    return Err(EngineError::User(format!(
                        "Table '{table}' already has data. SaveMode is ErrorIfExists."
                    )));
                }
            }
            Sm::Ignore => {
                let mut stream = client
                    .query(format!("SELECT COUNT(*) FROM {table}"), &[])
                    .await
                    .map_err(|e| EngineError::Sql(format!("JDBC write (SQL Server): check table: {e}")))?;
                let mut count: i64 = 0;
                while let Some(item) = stream.try_next().await.ok().flatten() {
                    if let QueryItem::Row(row) = item {
                        count = row.get::<i64, _>(0).unwrap_or(0);
                    }
                }
                if count > 0 {
                    return Ok(());
                }
            }
            Sm::Overwrite => {
                let use_truncate = opts.truncate.unwrap_or(true);
                if use_truncate {
                    let _ = client.execute(format!("TRUNCATE TABLE {table}"), &[]).await;
                } else {
                    let _ = client.execute(format!("DELETE FROM {table}"), &[]).await;
                }
            }
            Sm::Append => {}
        }

        if df.height() == 0 {
            return Ok(());
        }

        let batch_size = opts.batch_size.unwrap_or(1000) as usize;
        let total_rows = df.height();

        for batch_start in (0..total_rows).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(total_rows);
            
            // Start a transaction for this batch
            client.execute("BEGIN TRANSACTION", &[]).await
                .map_err(|e| EngineError::Sql(format!("JDBC write (SQL Server): begin transaction: {e}")))?;

            for row_idx in batch_start..batch_end {
                let mut q = Query::new(insert_sql.as_str());
                for col in df.columns() {
                    let v = col
                        .get(row_idx)
                        .map_err(|e| EngineError::Internal(format!("JDBC write: get cell: {e}")))?;
                    match v {
                        polars::prelude::AnyValue::Null => q.bind(Option::<String>::None),
                        polars::prelude::AnyValue::Boolean(b) => q.bind(b),
                        polars::prelude::AnyValue::Int64(i) => q.bind(i),
                        polars::prelude::AnyValue::Int32(i) => q.bind(i as i64),
                        polars::prelude::AnyValue::Float64(f) => q.bind(f),
                        polars::prelude::AnyValue::Float32(f) => q.bind(f as f64),
                        polars::prelude::AnyValue::String(s) => q.bind(s.to_string()),
                        polars::prelude::AnyValue::StringOwned(ref s) => q.bind(s.as_str().to_string()),
                        other => q.bind(other.to_string()),
                    };
                }
                q.execute(&mut client).await.map_err(|e| {
                    EngineError::Sql(format!("JDBC write (SQL Server): insert failed: {e}"))
                })?;
            }
            
            // Commit this batch
            client.execute("COMMIT", &[]).await
                .map_err(|e| EngineError::Sql(format!("JDBC write (SQL Server): commit batch: {e}")))?;
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn mssql_roundtrip_if_env() {
        let url = match std::env::var("ROBIN_SPARKLESS_TEST_JDBC_MSSQL_URL") {
            Ok(v) => v,
            Err(_) => return,
        };
        let user = std::env::var("ROBIN_SPARKLESS_TEST_JDBC_MSSQL_USER").unwrap_or_default();
        let password = std::env::var("ROBIN_SPARKLESS_TEST_JDBC_MSSQL_PASSWORD").unwrap_or_default();

        let mut opts = JdbcOptions {
            url,
            dbtable: None,
            query: None,
            user: Some(user),
            password: Some(password),
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            session_init_statement: None,
            query_timeout: None,
            prepare_query: None,
            custom_schema: None,
            truncate: None,
            cascade_truncate: None,
            isolation_level: None,
            create_table_options: None,
            create_table_column_types: None,
            raw_options: HashMap::new(),
        };

        // Create tables for the test (idempotent).
        with_runtime(async {
            let mut client = connect_async(&opts).await?;
            let _ = client.execute("IF OBJECT_ID('sparkless_jdbc_test','U') IS NOT NULL DROP TABLE sparkless_jdbc_test", &[]).await;
            client.execute("CREATE TABLE sparkless_jdbc_test (id BIGINT PRIMARY KEY, name NVARCHAR(255))", &[]).await
                .map_err(|e| EngineError::Sql(format!("create table: {e}")))?;
            client.execute("INSERT INTO sparkless_jdbc_test (id, name) VALUES (1, N'a'), (2, N'b')", &[]).await
                .map_err(|e| EngineError::Sql(format!("seed: {e}")))?;

            let _ = client.execute("IF OBJECT_ID('sparkless_jdbc_writeread_test','U') IS NOT NULL DROP TABLE sparkless_jdbc_writeread_test", &[]).await;
            client.execute("CREATE TABLE sparkless_jdbc_writeread_test (id BIGINT, name NVARCHAR(255))", &[]).await
                .map_err(|e| EngineError::Sql(format!("create table 2: {e}")))?;
            Ok(())
        }).unwrap();

        opts.dbtable = Some("sparkless_jdbc_test".to_string());
        let df = read_jdbc_mssql(&opts).unwrap();
        assert!(df.height() >= 2);

        let write_df = PlDataFrame::new_infer_height(vec![
            Series::new("id".into(), vec![10i64, 20i64]).into(),
            Series::new("name".into(), vec!["ten", "twenty"]).into(),
        ])
        .unwrap();
        let mut write_opts = opts.clone();
        write_opts.dbtable = Some("sparkless_jdbc_writeread_test".to_string());
        crate::jdbc::write_jdbc_from_polars(&write_df, &write_opts, crate::dataframe::SaveMode::Overwrite)
            .unwrap();

        let read_df = crate::jdbc::read_jdbc_to_polars(&write_opts).unwrap();
        assert_eq!(read_df.height(), 2);
    }
}

