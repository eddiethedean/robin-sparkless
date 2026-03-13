use crate::error::EngineError;
use crate::jdbc::JdbcOptions;

use odbc_api::{
    ConnectionOptions, Cursor, Environment, IntoParameter, ResultSetMetadata, buffers::TextRowSet,
};
use polars::prelude::{DataFrame as PlDataFrame, NamedFrom, Series};

fn normalize_db2_dsn(url: &str, opts: &JdbcOptions) -> Result<String, EngineError> {
    // The url can be `jdbc:db2://host:port/db` which we translate into an ODBC connection string.
    let url = url.strip_prefix("jdbc:").unwrap_or(url);
    let rest = url.strip_prefix("db2://").ok_or_else(|| {
        EngineError::User(format!(
            "JDBC DB2: URL must start with jdbc:db2:// (got '{url}')"
        ))
    })?;

    let (host_port, db_name) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = if let Some((h, p)) = host_port.rsplit_once(':') {
        (h, p)
    } else {
        (host_port, "50000")
    };

    let user = opts.user.clone().unwrap_or_default();
    let password = opts.password.clone().unwrap_or_default();

    Ok(format!(
        "DRIVER={{IBM DB2 ODBC DRIVER}};SERVER={host};PORT={port};DATABASE={db_name};UID={user};PWD={password};"
    ))
}

pub(crate) fn read_jdbc_db2(opts: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
    let dsn = normalize_db2_dsn(&opts.url, opts)?;

    let sql = if let Some(query) = &opts.query {
        query.clone()
    } else if let Some(table) = &opts.dbtable {
        format!("SELECT * FROM {table}")
    } else {
        return Err(EngineError::User(
            "JDBC read: either 'dbtable' or 'query' option is required".to_string(),
        ));
    };

    let env = Environment::new()
        .map_err(|e| EngineError::Internal(format!("JDBC DB2: ODBC env: {e}")))?;
    let conn = env
        .connect_with_connection_string(&dsn, ConnectionOptions::default())
        .map_err(|e| EngineError::Io(format!("JDBC DB2: connect failed: {e}")))?;

    // Execute session initialization statement if provided
    if let Some(init_sql) = &opts.session_init_statement {
        conn.execute(init_sql, (), None).map_err(|e| {
            EngineError::Sql(format!("JDBC read (DB2): sessionInitStatement failed: {e}"))
        })?;
    }

    // Note: DB2 query timeout would require setting SQL_ATTR_QUERY_TIMEOUT on the statement,
    // which odbc-api supports differently. For now, we skip this.

    // Execute prepare query if provided
    if let Some(prep_sql) = &opts.prepare_query {
        conn.execute(prep_sql, (), None)
            .map_err(|e| EngineError::Sql(format!("JDBC read (DB2): prepareQuery failed: {e}")))?;
    }

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| EngineError::Sql(format!("JDBC DB2: prepare failed: {e}")))?;

    let mut cursor = stmt
        .execute(())
        .map_err(|e| EngineError::Sql(format!("JDBC DB2: execute failed: {e}")))?
        .ok_or_else(|| {
            EngineError::Internal("JDBC DB2: statement did not return a result set".to_string())
        })?;

    let ncols = cursor
        .num_result_cols()
        .map_err(|e| EngineError::Other(format!("JDBC DB2: ncols: {e}")))? as usize;

    let mut column_names: Vec<String> = Vec::with_capacity(ncols);
    for idx in 1..=ncols as u16 {
        let desc = cursor
            .col_name(idx)
            .map_err(|e| EngineError::Other(format!("JDBC DB2: col_name: {e}")))?;
        column_names.push(desc);
    }

    const BATCH_SIZE: usize = 1024;
    let buffers = TextRowSet::for_cursor(BATCH_SIZE, &mut cursor, Some(4096))
        .map_err(|e| EngineError::Other(format!("JDBC DB2: buffer: {e}")))?;
    let mut row_cursor = cursor
        .bind_buffer(buffers)
        .map_err(|e| EngineError::Other(format!("JDBC DB2: bind_buffer: {e}")))?;

    let mut columns: Vec<Vec<Option<String>>> = (0..ncols).map(|_| Vec::new()).collect();

    while let Some(batch) = row_cursor
        .fetch()
        .map_err(|e| EngineError::Other(format!("JDBC DB2: fetch: {e}")))?
    {
        for row_idx in 0..batch.num_rows() {
            for (col_idx, col) in columns.iter_mut().enumerate().take(ncols) {
                let v = batch
                    .at(col_idx, row_idx)
                    .and_then(|s| std::str::from_utf8(s).ok().map(|s| s.to_string()));
                col.push(v);
            }
        }
    }

    if columns.iter().all(|c| c.is_empty()) {
        return Ok(PlDataFrame::empty());
    }

    let mut series_vec: Vec<Series> = Vec::with_capacity(ncols);
    for (name, col_data) in column_names.iter().zip(columns.iter()) {
        series_vec.push(db2_values_to_series(name, col_data));
    }
    let cols: Vec<polars::prelude::Column> = series_vec.into_iter().map(|s| s.into()).collect();
    PlDataFrame::new_infer_height(cols)
        .map_err(|e| EngineError::Internal(format!("JDBC read (DB2): build DataFrame: {e}")))
}

fn db2_values_to_series(name: &str, values: &[Option<String>]) -> Series {
    let mut has_int = true;
    let mut has_float = false;
    for v in values.iter().filter_map(|v| v.as_ref()) {
        if v.parse::<i64>().is_err() {
            has_int = false;
            if v.parse::<f64>().is_ok() {
                has_float = true;
            } else {
                has_float = false;
                break;
            }
        }
    }
    if has_int {
        let vals: Vec<Option<i64>> = values
            .iter()
            .map(|v| v.as_ref().and_then(|s| s.parse().ok()))
            .collect();
        return Series::new(name.into(), vals);
    }
    if has_float {
        let vals: Vec<Option<f64>> = values
            .iter()
            .map(|v| v.as_ref().and_then(|s| s.parse().ok()))
            .collect();
        return Series::new(name.into(), vals);
    }
    let vals: Vec<Option<String>> = values.to_vec();
    Series::new(name.into(), vals)
}

pub(crate) fn write_jdbc_db2(
    df: &PlDataFrame,
    opts: &JdbcOptions,
    mode: crate::dataframe::SaveMode,
) -> Result<(), EngineError> {
    use crate::dataframe::SaveMode as Sm;

    let dsn = normalize_db2_dsn(&opts.url, opts)?;

    let table = opts.dbtable.as_deref().ok_or_else(|| {
        EngineError::User(
            "JDBC write: 'dbtable' option is required for writes (target table name)".to_string(),
        )
    })?;

    let env = Environment::new()
        .map_err(|e| EngineError::Internal(format!("JDBC DB2: ODBC env: {e}")))?;
    let conn = env
        .connect_with_connection_string(&dsn, ConnectionOptions::default())
        .map_err(|e| EngineError::Io(format!("JDBC DB2: connect failed: {e}")))?;

    // Execute session initialization statement if provided
    if let Some(init_sql) = &opts.session_init_statement {
        conn.execute(init_sql, (), None).map_err(|e| {
            EngineError::Sql(format!(
                "JDBC write (DB2): sessionInitStatement failed: {e}"
            ))
        })?;
    }

    match mode {
        Sm::ErrorIfExists => {
            // DB2: check if table has data
            let check_sql = format!("SELECT COUNT(*) FROM {table}");
            let mut stmt = conn
                .prepare(&check_sql)
                .map_err(|e| EngineError::Sql(format!("JDBC write (DB2): check table: {e}")))?;
            let mut cursor = stmt
                .execute(())
                .map_err(|e| EngineError::Sql(format!("JDBC write (DB2): check table: {e}")))?
                .ok_or_else(|| {
                    EngineError::Internal("DB2: count query returned no result".to_string())
                })?;
            let buffers = TextRowSet::for_cursor(1, &mut cursor, Some(256))
                .map_err(|e| EngineError::Other(format!("JDBC write (DB2): buffer: {e}")))?;
            let mut row_cursor = cursor
                .bind_buffer(buffers)
                .map_err(|e| EngineError::Other(format!("JDBC write (DB2): bind: {e}")))?;
            let mut count: i64 = 0;
            if let Some(batch) = row_cursor
                .fetch()
                .map_err(|e| EngineError::Other(format!("JDBC write (DB2): fetch: {e}")))?
            {
                if batch.num_rows() > 0 {
                    if let Some(v) = batch.at(0, 0) {
                        if let Ok(s) = std::str::from_utf8(v) {
                            count = s.parse().unwrap_or(0);
                        }
                    }
                }
            }
            if count > 0 {
                return Err(EngineError::User(format!(
                    "Table '{table}' already has data. SaveMode is ErrorIfExists."
                )));
            }
        }
        Sm::Ignore => {
            let check_sql = format!("SELECT COUNT(*) FROM {table}");
            let mut stmt = conn
                .prepare(&check_sql)
                .map_err(|e| EngineError::Sql(format!("JDBC write (DB2): check table: {e}")))?;
            let mut cursor = stmt
                .execute(())
                .map_err(|e| EngineError::Sql(format!("JDBC write (DB2): check table: {e}")))?
                .ok_or_else(|| {
                    EngineError::Internal("DB2: count query returned no result".to_string())
                })?;
            let buffers = TextRowSet::for_cursor(1, &mut cursor, Some(256))
                .map_err(|e| EngineError::Other(format!("JDBC write (DB2): buffer: {e}")))?;
            let mut row_cursor = cursor
                .bind_buffer(buffers)
                .map_err(|e| EngineError::Other(format!("JDBC write (DB2): bind: {e}")))?;
            let mut count: i64 = 0;
            if let Some(batch) = row_cursor
                .fetch()
                .map_err(|e| EngineError::Other(format!("JDBC write (DB2): fetch: {e}")))?
            {
                if batch.num_rows() > 0 {
                    if let Some(v) = batch.at(0, 0) {
                        if let Ok(s) = std::str::from_utf8(v) {
                            count = s.parse().unwrap_or(0);
                        }
                    }
                }
            }
            if count > 0 {
                return Ok(());
            }
        }
        Sm::Overwrite => {
            let use_truncate = opts.truncate.unwrap_or(true);
            if use_truncate {
                let _ = conn.execute(&format!("TRUNCATE TABLE {table} IMMEDIATE"), (), None);
            } else {
                let _ = conn.execute(&format!("DELETE FROM {table}"), (), None);
            }
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
    let placeholders = (0..col_names.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "INSERT INTO {table} ({cols}) VALUES ({vals})",
        cols = col_names.join(", "),
        vals = placeholders
    );

    let batch_size = opts.batch_size.unwrap_or(1000) as usize;
    let total_rows = df.height();

    let mut prepared = conn
        .prepare(&insert_sql)
        .map_err(|e| EngineError::Sql(format!("JDBC write (DB2): prepare: {e}")))?;

    for batch_start in (0..total_rows).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(total_rows);

        for row_idx in batch_start..batch_end {
            let mut params: Vec<Option<String>> = Vec::with_capacity(col_names.len());
            for col in df.columns() {
                let v = col
                    .get(row_idx)
                    .map_err(|e| EngineError::Internal(format!("JDBC write: get cell: {e}")))?;
                let s = match v {
                    polars::prelude::AnyValue::Null => None,
                    other => Some(other.to_string()),
                };
                params.push(s);
            }

            let param_refs: Vec<_> = params
                .iter()
                .map(|s| s.as_deref().into_parameter())
                .collect();
            prepared
                .execute(&param_refs[..])
                .map_err(|e| EngineError::Sql(format!("JDBC write (DB2): insert failed: {e}")))?;
        }

        // Note: DB2 auto-commits by default in ODBC unless autocommit is disabled
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn db2_smoke_if_env() {
        let url = match std::env::var("ROBIN_SPARKLESS_TEST_JDBC_DB2_URL") {
            Ok(v) => v,
            Err(_) => return,
        };
        let user = std::env::var("ROBIN_SPARKLESS_TEST_JDBC_DB2_USER").unwrap_or_default();
        let password = std::env::var("ROBIN_SPARKLESS_TEST_JDBC_DB2_PASSWORD").unwrap_or_default();

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
            raw_options: HashMap::new(),
        };

        let _ = read_jdbc_db2(&opts);
    }
}
