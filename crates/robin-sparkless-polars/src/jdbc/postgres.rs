use crate::error::EngineError;
use crate::jdbc::JdbcOptions;

use polars::prelude::{DataFrame as PlDataFrame, NamedFrom, Series};

use postgres::types::Type as PgType;
use postgres::{Client, NoTls};

pub(crate) fn write_jdbc_postgres(
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

    // Execute session initialization statement if provided
    if let Some(init_sql) = &opts.session_init_statement {
        client
            .batch_execute(init_sql)
            .map_err(|e| EngineError::Sql(format!("JDBC write: sessionInitStatement failed: {e}")))?;
    }

    match mode {
        Sm::ErrorIfExists => {
            let row = client
                .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
                .map_err(|e| EngineError::Sql(format!("JDBC write: check table: {e}")))?;
            let count: i64 = row.get(0);
            if count > 0 {
                return Err(EngineError::User(format!(
                    "Table '{table}' already has data. SaveMode is ErrorIfExists."
                )));
            }
        }
        Sm::Ignore => {
            let row = client
                .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
                .map_err(|e| EngineError::Sql(format!("JDBC write: check table: {e}")))?;
            let count: i64 = row.get(0);
            if count > 0 {
                return Ok(());
            }
        }
        Sm::Overwrite => {
            let use_truncate = opts.truncate.unwrap_or(true);
            if use_truncate {
                if opts.cascade_truncate.unwrap_or(false) {
                    let _ = client.execute(&format!("TRUNCATE TABLE ONLY {table} CASCADE"), &[]);
                } else {
                    let _ = client.execute(&format!("TRUNCATE TABLE {table}"), &[]);
                }
            } else {
                let _ = client.execute(&format!("DELETE FROM {table}"), &[]);
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
    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("${i}")).collect();
    let insert_sql = format!(
        "INSERT INTO {table} ({cols}) VALUES ({vals})",
        cols = col_names.join(", "),
        vals = placeholders.join(", ")
    );

    let batch_size = opts.batch_size.unwrap_or(1000) as usize;
    let total_rows = df.height();
    
    for batch_start in (0..total_rows).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(total_rows);
        
        // Start a transaction for this batch
        client
            .batch_execute("BEGIN")
            .map_err(|e| EngineError::Sql(format!("JDBC write: begin transaction: {e}")))?;
        
        for row_idx in batch_start..batch_end {
            let mut params: Vec<Box<dyn postgres::types::ToSql + Sync>> =
                Vec::with_capacity(col_names.len());
            for col in df.columns() {
                let v = col
                    .get(row_idx)
                    .map_err(|e| EngineError::Internal(format!("JDBC write: get cell: {e}")))?;
                let boxed: Box<dyn postgres::types::ToSql + Sync> = match v {
                    polars::prelude::AnyValue::Null => Box::new(Option::<i32>::None),
                    polars::prelude::AnyValue::Boolean(b) => Box::new(Some(b)),
                    polars::prelude::AnyValue::Int64(i) => Box::new(Some(i)),
                    polars::prelude::AnyValue::Int32(i) => Box::new(Some(i)),
                    polars::prelude::AnyValue::Float64(f) => Box::new(Some(f)),
                    polars::prelude::AnyValue::Float32(f) => Box::new(Some(f as f64)),
                    polars::prelude::AnyValue::String(s) => Box::new(Some(s.to_string())),
                    polars::prelude::AnyValue::StringOwned(ref s) => {
                        Box::new(Some(s.as_str().to_string()))
                    }
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
        
        // Commit this batch
        client
            .batch_execute("COMMIT")
            .map_err(|e| EngineError::Sql(format!("JDBC write: commit batch: {e}")))?;
    }
    Ok(())
}

pub(crate) fn read_jdbc_postgres(opts: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
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

    // Execute session initialization statement if provided
    if let Some(init_sql) = &opts.session_init_statement {
        client
            .batch_execute(init_sql)
            .map_err(|e| EngineError::Sql(format!("JDBC read: sessionInitStatement failed: {e}")))?;
    }

    // Set query timeout if provided (PostgreSQL uses milliseconds)
    if let Some(timeout_secs) = opts.query_timeout {
        let timeout_ms = timeout_secs * 1000;
        client
            .batch_execute(&format!("SET statement_timeout = {timeout_ms}"))
            .map_err(|e| EngineError::Sql(format!("JDBC read: failed to set queryTimeout: {e}")))?;
    }

    if let Some(fetch) = opts.fetch_size {
        client
            .simple_query(&format!("SET SESSION FETCH_COUNT = {fetch}"))
            .map_err(|e| {
                EngineError::Other(format!("JDBC read: failed to set fetchsize: {e}"))
            })?;
    }

    // Execute prepare query if provided (for CTEs, temp tables, etc.)
    if let Some(prep_sql) = &opts.prepare_query {
        client
            .batch_execute(prep_sql)
            .map_err(|e| EngineError::Sql(format!("JDBC read: prepareQuery failed: {e}")))?;
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
                let v: Option<String> = row.try_get(index).map_err(|e| {
                    Other(format!("JDBC read: text-like column '{name}': {e}"))
                })?;
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::TIMESTAMP | PgType::TIMESTAMPTZ => {
            let mut vals: Vec<Option<chrono::NaiveDateTime>> = Vec::with_capacity(rows.len());
            for row in rows {
                let s: Option<String> = row.try_get(index).map_err(|e| {
                    Other(format!("JDBC read: timestamp column '{name}': {e}"))
                })?;
                let v = s.and_then(|s| {
                    chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
                        .ok()
                        .or_else(|| {
                            chrono::DateTime::parse_from_rfc3339(&s)
                                .ok()
                                .map(|dt| dt.naive_local())
                        })
                });
                vals.push(v);
            }
            Ok(Series::new(name.into(), vals))
        }
        PgType::DATE => {
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
            let mut vals: Vec<Option<f64>> = Vec::with_capacity(rows.len());
            for row in rows {
                let s: Option<String> = row.try_get(index).map_err(|e| {
                    Other(format!("JDBC read: numeric column '{name}': {e}"))
                })?;
                let f = s.and_then(|s| s.parse::<f64>().ok());
                vals.push(f);
            }
            Ok(Series::new(name.into(), vals))
        }
        _ => {
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

