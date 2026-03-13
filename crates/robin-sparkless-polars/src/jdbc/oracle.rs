use crate::error::EngineError;
use crate::jdbc::JdbcOptions;

use oracle_rs::{Connection, Value};
use polars::prelude::{DataFrame as PlDataFrame, NamedFrom, Series};

fn parse_oracle_jdbc_url(url: &str) -> Result<String, EngineError> {
    // Support a common EZConnect-style JDBC URL:
    //   jdbc:oracle:thin:@//host:1521/SERVICE
    //   jdbc:oracle:thin:@host:1521:SID  (treated as host:port/SID)
    let u = url.strip_prefix("jdbc:").unwrap_or(url);
    let rest = u.strip_prefix("oracle:thin:@").ok_or_else(|| {
        EngineError::User(format!(
            "JDBC Oracle: URL must start with jdbc:oracle:thin:@ (got '{url}')"
        ))
    })?;

    let rest = rest.strip_prefix("//").unwrap_or(rest);
    if let Some((host_port, service)) = rest.split_once('/') {
        return Ok(format!("{host_port}/{service}"));
    }
    if let Some((host_port, sid)) = rest.split_once(':') {
        // If we got host:port:SID, host_port is host, sid contains port:SID; handle that.
        if let Some((port, sid2)) = sid.split_once(':') {
            return Ok(format!("{host_port}:{port}/{sid2}"));
        }
    }
    Ok(rest.to_string())
}

fn with_runtime<T>(
    fut: impl std::future::Future<Output = Result<T, EngineError>>,
) -> Result<T, EngineError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| EngineError::Internal(format!("JDBC Oracle: failed to create runtime: {e}")))?
        .block_on(fut)
}

async fn connect_async(opts: &JdbcOptions) -> Result<Connection, EngineError> {
    let connect_string = parse_oracle_jdbc_url(&opts.url)?;
    let user = opts.user.clone().unwrap_or_default();
    let password = opts.password.clone().unwrap_or_default();
    if user.is_empty() {
        return Err(EngineError::User("JDBC Oracle: missing 'user' option".to_string()));
    }
    Connection::connect(&connect_string, &user, &password)
        .await
        .map_err(|e| EngineError::Io(format!("JDBC Oracle: connect failed: {e}")))
}

pub(crate) fn read_jdbc_oracle(opts: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
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
        let conn = connect_async(opts).await?;

        // Execute session initialization statement if provided
        if let Some(init_sql) = &opts.session_init_statement {
            conn.execute(init_sql, &[])
                .await
                .map_err(|e| EngineError::Sql(format!("JDBC read (Oracle): sessionInitStatement failed: {e}")))?;
        }

        // Set query timeout if provided (Oracle uses DBMS_SESSION.SET_SQL_TIMEOUT in seconds)
        // Note: This requires elevated privileges; fall back to doing nothing if it fails.
        if let Some(timeout_secs) = opts.query_timeout {
            let _ = conn
                .execute(&format!("BEGIN DBMS_SESSION.SET_SQL_TIMEOUT({timeout_secs}); END;"), &[])
                .await;
        }

        // Execute prepare query if provided
        if let Some(prep_sql) = &opts.prepare_query {
            conn.execute(prep_sql, &[])
                .await
                .map_err(|e| EngineError::Sql(format!("JDBC read (Oracle): prepareQuery failed: {e}")))?;
        }

        let result = conn
            .query(&sql, &[])
            .await
            .map_err(|e| EngineError::Sql(format!("JDBC read (Oracle): query failed: {e}")))?;

        if result.rows.is_empty() || (result.rows.len() == 1 && result.rows[0].len() == 0) {
            return Ok(PlDataFrame::empty());
        }

        // Best-effort column naming: fall back to c0..cN (oracle-rs Row doesn't expose
        // column names directly in query results at the moment).
        let first = &result.rows[0];
        let n = first.len();
        let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();

        let ncols = names.len();
        let mut columns: Vec<Vec<Option<Value>>> = (0..ncols).map(|_| Vec::new()).collect();
        for row in &result.rows {
            for idx in 0..ncols {
                let v = row.get(idx).cloned();
                columns[idx].push(v);
            }
        }

        let mut series_vec: Vec<Series> = Vec::with_capacity(ncols);
        for (name, vals) in names.iter().zip(columns.iter()) {
            series_vec.push(oracle_values_to_series(name, vals));
        }
        let cols: Vec<polars::prelude::Column> = series_vec.into_iter().map(|s| s.into()).collect();
        PlDataFrame::new_infer_height(cols)
            .map_err(|e| EngineError::Internal(format!("JDBC read (Oracle): build DataFrame: {e}")))
    })
}

fn oracle_values_to_series(name: &str, values: &[Option<Value>]) -> Series {
    let mut has_i64 = false;
    let mut has_f64 = false;
    let mut has_bool = false;
    let mut has_str = false;
    for v in values {
        match v {
            None => {}
            Some(v) if v.as_i64().is_some() => has_i64 = true,
            Some(v) if v.as_f64().is_some() => has_f64 = true,
            Some(v) if v.as_bool().is_some() => has_bool = true,
            Some(v) if v.as_str().is_some() => has_str = true,
            Some(_) => has_str = true,
        }
    }
    if has_bool && !has_i64 && !has_f64 && !has_str {
        let vals: Vec<Option<bool>> = values.iter().map(|v| v.as_ref().and_then(|v| v.as_bool())).collect();
        return Series::new(name.into(), vals);
    }
    if has_i64 && !has_f64 && !has_str {
        let vals: Vec<Option<i64>> = values.iter().map(|v| v.as_ref().and_then(|v| v.as_i64())).collect();
        return Series::new(name.into(), vals);
    }
    if has_f64 && !has_str {
        let vals: Vec<Option<f64>> = values
            .iter()
            .map(|v| v.as_ref().and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))))
            .collect();
        return Series::new(name.into(), vals);
    }
    let vals: Vec<Option<String>> = values
        .iter()
        .map(|v| v.as_ref().and_then(|v| v.as_str().map(|s| s.to_string())).or_else(|| v.as_ref().map(|v| v.to_string())))
        .collect();
    Series::new(name.into(), vals)
}

pub(crate) fn write_jdbc_oracle(
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

    with_runtime(async move {
        let conn = connect_async(opts).await?;

        // Execute session initialization statement if provided
        if let Some(init_sql) = &opts.session_init_statement {
            conn.execute(init_sql, &[])
                .await
                .map_err(|e| EngineError::Sql(format!("JDBC write (Oracle): sessionInitStatement failed: {e}")))?;
        }

        match mode {
            Sm::ErrorIfExists => {
                let result = conn
                    .query(&format!("SELECT COUNT(*) FROM {table}"), &[])
                    .await
                    .map_err(|e| EngineError::Sql(format!("JDBC write (Oracle): check table: {e}")))?;
                let count = result.rows.first().and_then(|r| r.get(0)).and_then(|v| v.as_i64()).unwrap_or(0);
                if count > 0 {
                    return Err(EngineError::User(format!(
                        "Table '{table}' already has data. SaveMode is ErrorIfExists."
                    )));
                }
            }
            Sm::Ignore => {
                let result = conn
                    .query(&format!("SELECT COUNT(*) FROM {table}"), &[])
                    .await
                    .map_err(|e| EngineError::Sql(format!("JDBC write (Oracle): check table: {e}")))?;
                let count = result.rows.first().and_then(|r| r.get(0)).and_then(|v| v.as_i64()).unwrap_or(0);
                if count > 0 {
                    return Ok(());
                }
            }
            Sm::Overwrite => {
                let use_truncate = opts.truncate.unwrap_or(false); // Oracle default: use DELETE
                if use_truncate {
                    if opts.cascade_truncate.unwrap_or(false) {
                        let _ = conn.execute(&format!("TRUNCATE TABLE {table} CASCADE"), &[]).await;
                    } else {
                        let _ = conn.execute(&format!("TRUNCATE TABLE {table}"), &[]).await;
                    }
                } else {
                    let _ = conn.execute(&format!("DELETE FROM {table}"), &[]).await;
                }
                conn.commit().await.ok();
            }
            Sm::Append => {}
        }

        if df.height() == 0 {
            return Ok(());
        }

        let col_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|n| n.as_str().to_string())
            .collect();
        let placeholders = (1..=col_names.len())
            .map(|i| format!(":{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql = format!(
            "INSERT INTO {table} ({cols}) VALUES ({vals})",
            cols = col_names.join(", "),
            vals = placeholders
        );

        let batch_size = opts.batch_size.unwrap_or(1000) as usize;
        let total_rows = df.height();

        for batch_start in (0..total_rows).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(total_rows);

            for row_idx in batch_start..batch_end {
                let mut params: Vec<Value> = Vec::with_capacity(col_names.len());
                for col in df.columns() {
                    let v = col
                        .get(row_idx)
                        .map_err(|e| EngineError::Internal(format!("JDBC write: get cell: {e}")))?;
                    let oracle_v = match v {
                        polars::prelude::AnyValue::Null => Value::Null,
                        polars::prelude::AnyValue::Boolean(b) => b.into(),
                        polars::prelude::AnyValue::Int64(i) => i.into(),
                        polars::prelude::AnyValue::Int32(i) => (i as i64).into(),
                        polars::prelude::AnyValue::Float64(f) => f.into(),
                        polars::prelude::AnyValue::Float32(f) => (f as f64).into(),
                        polars::prelude::AnyValue::String(s) => s.to_string().into(),
                        polars::prelude::AnyValue::StringOwned(ref s) => s.as_str().to_string().into(),
                        other => other.to_string().into(),
                    };
                    params.push(oracle_v);
                }
                conn.execute(&insert_sql, &params)
                    .await
                    .map_err(|e| EngineError::Sql(format!("JDBC write (Oracle): insert failed: {e}")))?;
            }
            
            // Commit this batch
            conn.commit().await.ok();
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_smoke_if_env() {
        let url = match std::env::var("ROBIN_SPARKLESS_TEST_JDBC_ORACLE_URL") {
            Ok(v) => v,
            Err(_) => return,
        };
        let user = std::env::var("ROBIN_SPARKLESS_TEST_JDBC_ORACLE_USER").unwrap_or_default();
        let password =
            std::env::var("ROBIN_SPARKLESS_TEST_JDBC_ORACLE_PASSWORD").unwrap_or_default();

        let opts = JdbcOptions {
            url,
            dbtable: Some("sparkless_jdbc_test".to_string()),
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
            raw_options: std::collections::HashMap::new(),
        };

        // This will succeed if the server/table exists; otherwise it will return an error.
        let _ = read_jdbc_oracle(&opts);
    }
}

