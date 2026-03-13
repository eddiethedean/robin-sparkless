use crate::error::EngineError;
use crate::jdbc::JdbcOptions;

use mysql::prelude::Queryable;
use mysql::{Conn, Opts, OptsBuilder, Params, Value};
use polars::prelude::{DataFrame as PlDataFrame, NamedFrom, Series};

fn normalize_mysql_url(url: &str) -> Result<String, EngineError> {
    // Accept JDBC form `jdbc:mysql://...` and turn into a mysql crate URL.
    let mut u = url.to_string();
    if let Some(stripped) = u.strip_prefix("jdbc:") {
        u = stripped.to_string();
    }
    if u.starts_with("mysql://") {
        Ok(u)
    } else if u.starts_with("mariadb://") {
        // Treat MariaDB protocol as MySQL for the mysql crate.
        Ok(format!("mysql://{}", u.trim_start_matches("mariadb://")))
    } else {
        Err(EngineError::User(format!(
            "JDBC MySQL: URL must start with jdbc:mysql: or mysql:// (got '{url}')"
        )))
    }
}

fn connect(opts: &JdbcOptions) -> Result<Conn, EngineError> {
    let url = normalize_mysql_url(&opts.url)?;

    // Prefer explicit user/password properties when provided; otherwise rely on URL.
    // The mysql crate supports `mysql://user:pass@host:port/db`.
    let base = Opts::from_url(&url)
        .map_err(|e| EngineError::User(format!("JDBC MySQL: invalid url: {e}")))?;
    let mut builder = OptsBuilder::from_opts(base);
    if let Some(user) = &opts.user {
        if !user.is_empty() {
            builder = builder.user(Some(user.as_str()));
        }
    }
    if let Some(password) = &opts.password {
        if !password.is_empty() {
            builder = builder.pass(Some(password.as_str()));
        }
    }

    Conn::new(builder).map_err(|e| EngineError::Io(format!("JDBC MySQL: connect failed: {e}")))
}

pub(crate) fn write_jdbc_mysql(
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

    let mut conn = connect(opts)?;

    match mode {
        Sm::Overwrite => {
            let _ = conn.query_drop(format!("TRUNCATE TABLE {table}"));
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
    let placeholders = (0..col_names.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!(
        "INSERT INTO {table} ({cols}) VALUES ({vals})",
        cols = col_names.join(", "),
        vals = placeholders
    );

    for row_idx in 0..df.height() {
        let mut params: Vec<Value> = Vec::with_capacity(col_names.len());
        for col in df.columns() {
            let v = col
                .get(row_idx)
                .map_err(|e| EngineError::Internal(format!("JDBC write: get cell: {e}")))?;
            let mysql_v = match v {
                polars::prelude::AnyValue::Null => Value::NULL,
                polars::prelude::AnyValue::Boolean(b) => Value::Int(if b { 1 } else { 0 }),
                polars::prelude::AnyValue::Int64(i) => Value::Int(i),
                polars::prelude::AnyValue::Int32(i) => Value::Int(i as i64),
                polars::prelude::AnyValue::Float64(f) => Value::Double(f),
                polars::prelude::AnyValue::Float32(f) => Value::Double(f as f64),
                polars::prelude::AnyValue::String(s) => Value::Bytes(s.as_bytes().to_vec()),
                polars::prelude::AnyValue::StringOwned(ref s) => {
                    Value::Bytes(s.as_bytes().to_vec())
                }
                other => Value::Bytes(other.to_string().into_bytes()),
            };
            params.push(mysql_v);
        }
        conn.exec_drop(&insert_sql, Params::Positional(params))
            .map_err(|e| EngineError::Sql(format!("JDBC write (MySQL): insert failed: {e}")))?;
    }
    Ok(())
}

pub(crate) fn read_jdbc_mysql(opts: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
    let sql = if let Some(query) = &opts.query {
        query.clone()
    } else if let Some(table) = &opts.dbtable {
        format!("SELECT * FROM {table}")
    } else {
        return Err(EngineError::User(
            "JDBC read: either 'dbtable' or 'query' option is required".to_string(),
        ));
    };

    let mut conn = connect(opts)?;

    let result = conn
        .query_iter(sql)
        .map_err(|e| EngineError::Sql(format!("JDBC read (MySQL): query failed: {e}")))?;

    let column_names: Vec<String> = result
        .columns()
        .as_ref()
        .iter()
        .map(|c| c.name_str().to_string())
        .collect();
    let ncols = column_names.len();

    let mut columns_data: Vec<Vec<Option<Value>>> = (0..ncols).map(|_| Vec::new()).collect();

    for row_res in result {
        let row: mysql::Row = row_res
            .map_err(|e| EngineError::Other(format!("JDBC read (MySQL): row: {e}")))?;
        for (idx, col_data) in columns_data.iter_mut().enumerate() {
            let v: Option<Value> = row.get(idx);
            col_data.push(v);
        }
    }

    if columns_data.iter().all(|c| c.is_empty()) {
        return Ok(PlDataFrame::empty());
    }

    let mut series_vec: Vec<Series> = Vec::with_capacity(ncols);
    for (name, col_data) in column_names.iter().zip(columns_data.iter()) {
        series_vec.push(mysql_values_to_series(name, col_data)?);
    }
    let cols: Vec<polars::prelude::Column> = series_vec.into_iter().map(|s| s.into()).collect();
    PlDataFrame::new_infer_height(cols)
        .map_err(|e| EngineError::Internal(format!("JDBC read (MySQL): build DataFrame: {e}")))
}

fn mysql_values_to_series(name: &str, values: &[Option<Value>]) -> Result<Series, EngineError> {
    if values.is_empty() {
        return Ok(Series::new(name.into(), Vec::<Option<i64>>::new()));
    }

    let mut has_int = false;
    let mut has_float = false;
    let mut has_text = false;
    for v in values {
        match v {
            None | Some(Value::NULL) => {}
            Some(Value::Int(_)) | Some(Value::UInt(_)) => has_int = true,
            Some(Value::Float(_)) | Some(Value::Double(_)) => has_float = true,
            Some(Value::Bytes(_)) => has_text = true,
            Some(Value::Date(..)) | Some(Value::Time(..)) => has_text = true,
        }
    }

    if has_int && !has_float && !has_text {
        let vals: Vec<Option<i64>> = values
            .iter()
            .map(|v| match v {
                None | Some(Value::NULL) => None,
                Some(Value::Int(i)) => Some(*i),
                Some(Value::UInt(u)) => i64::try_from(*u).ok(),
                _ => None,
            })
            .collect();
        return Ok(Series::new(name.into(), vals));
    }

    if has_float && !has_text {
        let vals: Vec<Option<f64>> = values
            .iter()
            .map(|v| match v {
                None | Some(Value::NULL) => None,
                Some(Value::Int(i)) => Some(*i as f64),
                Some(Value::UInt(u)) => Some(*u as f64),
                Some(Value::Float(f)) => Some(*f as f64),
                Some(Value::Double(f)) => Some(*f),
                _ => None,
            })
            .collect();
        return Ok(Series::new(name.into(), vals));
    }

    let vals: Vec<Option<String>> = values
        .iter()
        .map(|v| match v {
            None | Some(Value::NULL) => None,
            Some(Value::Int(i)) => Some(i.to_string()),
            Some(Value::UInt(u)) => Some(u.to_string()),
            Some(Value::Float(f)) => Some(f.to_string()),
            Some(Value::Double(f)) => Some(f.to_string()),
            Some(Value::Bytes(b)) => Some(String::from_utf8_lossy(b).to_string()),
            Some(Value::Date(y, m, d, hh, mm, ss, micros)) => Some(format!(
                "{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{micros:06}"
            )),
            Some(Value::Time(neg, days, hours, mins, secs, micros)) => Some(format!(
                "{}{}d {:02}:{:02}:{:02}.{:06}",
                if *neg { "-" } else { "" },
                days,
                hours,
                mins,
                secs,
                micros
            )),
        })
        .collect();
    Ok(Series::new(name.into(), vals))
}

#[cfg(test)]
mod tests {
    use super::*;

    // These are runtime-gated integration tests. They pass (no-op) when the env var isn't set.
    // Set ROBIN_SPARKLESS_TEST_JDBC_MYSQL_URL to run against a live server.
    #[test]
    fn mysql_roundtrip_if_env() {
        let url = match std::env::var("ROBIN_SPARKLESS_TEST_JDBC_MYSQL_URL") {
            Ok(v) => v,
            Err(_) => return,
        };

        let mut opts = JdbcOptions {
            url,
            dbtable: None,
            query: None,
            user: std::env::var("ROBIN_SPARKLESS_TEST_JDBC_MYSQL_USER").ok(),
            password: std::env::var("ROBIN_SPARKLESS_TEST_JDBC_MYSQL_PASSWORD").ok(),
            driver: None,
            partition_column: None,
            lower_bound: None,
            upper_bound: None,
            num_partitions: None,
            fetch_size: None,
            batch_size: None,
            raw_options: std::collections::HashMap::new(),
        };

        let mut conn = connect(&opts).unwrap();
        let _ = conn.query_drop("DROP TABLE IF EXISTS sparkless_jdbc_test");
        conn.query_drop("CREATE TABLE sparkless_jdbc_test (id BIGINT PRIMARY KEY, name TEXT)")
            .unwrap();
        conn.query_drop("INSERT INTO sparkless_jdbc_test (id, name) VALUES (1,'a'),(2,'b')")
            .unwrap();

        // Read (dbtable)
        opts.dbtable = Some("sparkless_jdbc_test".to_string());
        let df = read_jdbc_mysql(&opts).unwrap();
        assert!(df.height() >= 2);

        // Write overwrite + read back
        let _ = conn.query_drop("DROP TABLE IF EXISTS sparkless_jdbc_writeread_test");
        conn.query_drop("CREATE TABLE sparkless_jdbc_writeread_test (id BIGINT, name TEXT)")
            .unwrap();
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

