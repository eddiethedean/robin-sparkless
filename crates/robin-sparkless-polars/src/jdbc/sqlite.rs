use crate::error::EngineError;
use crate::jdbc::JdbcOptions;

use polars::prelude::{DataFrame as PlDataFrame, NamedFrom, Series};

pub(crate) fn write_jdbc_sqlite(
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
    let placeholders = (0..col_names.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
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
            let v = col
                .get(row_idx)
                .map_err(|e| EngineError::Internal(format!("JDBC write: get cell: {e}")))?;
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

pub(crate) fn read_jdbc_sqlite(opts: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
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

    let column_names: Vec<String> = stmt
        .column_names()
        .iter()
        .map(|s| (*s).to_string())
        .collect();
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
                EngineError::Other(format!(
                    "JDBC read (SQLite): column {}: {e}",
                    column_names[c]
                ))
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

fn sqlite_url_to_path(url: &str) -> Result<std::path::PathBuf, EngineError> {
    let rest = url
        .strip_prefix("jdbc:sqlite:")
        .or_else(|| url.strip_prefix("sqlite:"))
        .ok_or_else(|| {
            EngineError::User("SQLite URL must start with jdbc:sqlite: or sqlite:".to_string())
        })?;
    let path = rest.strip_prefix("file:").unwrap_or(rest);
    Ok(std::path::PathBuf::from(path))
}

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

