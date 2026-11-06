use duckdb::Connection;
use std::io::{self, BufRead, Write};
use std::net::TcpListener;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let extension_path = args.next().map(PathBuf::from).ok_or(
        "usage: duckdb_quack_fixture_owner <quack.duckdb_extension> [--writer|--reader|--catalog-update <quack-uri> <token>|--receipt-reader <quack-uri> <token> <operation-key>|--persistent <database-path> <port>]",
    )?;
    let extension_path = std::fs::canonicalize(extension_path)?;
    let extension_path = extension_path
        .to_str()
        .ok_or("Quack extension path must be valid UTF-8")?;
    let mode = args.next().map(|mode| mode.to_string_lossy().into_owned());
    if mode.as_deref() == Some("--writer") {
        let uri = args
            .next()
            .map(|value| value.to_string_lossy().into_owned())
            .ok_or("writer mode requires a Quack URI")?;
        let token = args
            .next()
            .map(|value| value.to_string_lossy().into_owned())
            .ok_or("writer mode requires an authentication token")?;
        if args.next().is_some() {
            return Err("writer mode received unexpected arguments".into());
        }
        return run_writer(extension_path, &uri, &token);
    }
    if mode.as_deref() == Some("--reader") {
        let uri = args
            .next()
            .map(|value| value.to_string_lossy().into_owned())
            .ok_or("reader mode requires a Quack URI")?;
        let token = args
            .next()
            .map(|value| value.to_string_lossy().into_owned())
            .ok_or("reader mode requires an authentication token")?;
        if args.next().is_some() {
            return Err("reader mode received unexpected arguments".into());
        }
        return run_reader(extension_path, &uri, &token);
    }
    if mode.as_deref() == Some("--catalog-update") {
        let uri = args
            .next()
            .map(|value| value.to_string_lossy().into_owned())
            .ok_or("catalog-update mode requires a Quack URI")?;
        let token = args
            .next()
            .map(|value| value.to_string_lossy().into_owned())
            .ok_or("catalog-update mode requires an authentication token")?;
        if args.next().is_some() {
            return Err("catalog-update mode received unexpected arguments".into());
        }
        return run_catalog_update(extension_path, &uri, &token);
    }
    if mode.as_deref() == Some("--receipt-reader") {
        let uri = args
            .next()
            .map(|value| value.to_string_lossy().into_owned())
            .ok_or("receipt-reader mode requires a Quack URI")?;
        let token = args
            .next()
            .map(|value| value.to_string_lossy().into_owned())
            .ok_or("receipt-reader mode requires an authentication token")?;
        let operation_key = args
            .next()
            .map(|value| value.to_string_lossy().into_owned())
            .ok_or("receipt-reader mode requires an operation key")?;
        if args.next().is_some() {
            return Err("receipt-reader mode received unexpected arguments".into());
        }
        return run_receipt_reader(extension_path, &uri, &token, &operation_key);
    }
    let persistent_owner = if mode.as_deref() == Some("--persistent") {
        let database_path = args
            .next()
            .map(PathBuf::from)
            .ok_or("persistent mode requires a database path")?;
        let port = args
            .next()
            .map(|value| value.to_string_lossy().parse::<u16>())
            .transpose()?
            .ok_or("persistent mode requires a Quack port")?;
        if args.next().is_some() {
            return Err("persistent mode received unexpected arguments".into());
        }
        Some((database_path, port))
    } else {
        if mode.is_some() || args.next().is_some() {
            return Err("unexpected fixture owner argument".into());
        }
        None
    };

    let (connection, port) = if let Some((database_path, port)) = persistent_owner {
        (Connection::open(database_path)?, port)
    } else {
        let port_listener = TcpListener::bind(("127.0.0.1", 0))?;
        let port = port_listener.local_addr()?.port();
        drop(port_listener);
        (Connection::open_in_memory()?, port)
    };
    let uri = format!("quack:localhost:{port}");
    let token = "sparkless-4.19-fixture-token";

    connection.execute_batch(&format!(
        "LOAD {};
         SET TimeZone = 'UTC';",
        sql_literal(extension_path)
    ))?;
    let fixture_exists = connection.query_row(
        "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'events'",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !fixture_exists {
        connection.execute_batch(
            "CREATE TABLE events (
             id BIGINT NOT NULL,
             bucket INTEGER,
             label VARCHAR,
             amount DECIMAL(12, 2),
             active BOOLEAN
         );
         INSERT INTO events VALUES
             (1, 10, 'alpha', 12.50, true),
             (2, 10, 'beta', NULL, false),
             (3, 20, 'gamma', 7.25, true);
         CREATE TABLE catalog_change_events (id INTEGER, label VARCHAR);
         INSERT INTO catalog_change_events VALUES (1, 'before-analysis');
         CREATE TABLE right_events (id BIGINT, note VARCHAR);
         INSERT INTO right_events VALUES (1, 'one'), (2, 'two'), (4, 'four');
         CREATE TABLE empty_events (id BIGINT NOT NULL, label VARCHAR);
         CREATE TABLE all_null_events (id INTEGER, label VARCHAR);
         INSERT INTO all_null_events VALUES (1, NULL), (2, NULL);
         CREATE TABLE typed_events (
             small SMALLINT,
             score DOUBLE,
             day DATE,
             seen_at TIMESTAMPTZ
         );
         INSERT INTO typed_events VALUES
             (-32768, 3.5, DATE '2026-10-30', TIMESTAMPTZ '2026-10-30 12:34:56.123456+00');
         CREATE TABLE integer_boundary_events (
             small_min SMALLINT NOT NULL,
             small_max SMALLINT NOT NULL,
             int_min INTEGER NOT NULL,
             int_max INTEGER NOT NULL,
             bigint_min BIGINT NOT NULL,
             bigint_max BIGINT NOT NULL
         );
         INSERT INTO integer_boundary_events VALUES
             (-32768, 32767, -2147483648, 2147483647,
              CAST('-9223372036854775808' AS BIGINT),
              CAST('9223372036854775807' AS BIGINT));
         CREATE TABLE decimal_boundary_events (
             d1 DECIMAL(1, 0),
             d9 DECIMAL(9, 0),
             d18 DECIMAL(18, 6),
             d38 DECIMAL(38, 0),
             d38s38 DECIMAL(38, 38)
         );
         INSERT INTO decimal_boundary_events VALUES
             (9, 999999999, CAST('999999999999.999999' AS DECIMAL(18, 6)),
              CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0)),
              CAST('0.' || repeat('9', 38) AS DECIMAL(38, 38))),
             (-9, -999999999, CAST('-999999999999.999999' AS DECIMAL(18, 6)),
              CAST('-99999999999999999999999999999999999999' AS DECIMAL(38, 0)),
              CAST('-0.' || repeat('9', 38) AS DECIMAL(38, 38)));
         CREATE TABLE non_finite_events (score DOUBLE);
         INSERT INTO non_finite_events VALUES (CAST('NaN' AS DOUBLE));
         CREATE TABLE infinite_date_events (day DATE);
         INSERT INTO infinite_date_events VALUES (DATE 'infinity');
         CREATE TABLE infinite_timestamp_events (seen_at TIMESTAMP);
         INSERT INTO infinite_timestamp_events VALUES (TIMESTAMP 'infinity');
         CREATE TABLE nested_events (items INTEGER[]);
         INSERT INTO nested_events VALUES ([1, 2]);
         CREATE TABLE oversized_events (payload VARCHAR);
         INSERT INTO oversized_events VALUES (repeat('x', 524289));
         CREATE TABLE replay_events AS
             SELECT i::BIGINT AS id FROM range(50000) AS source_rows(i);
         CREATE VIEW shutdown_events AS
             SELECT i::BIGINT AS id FROM range(1000000000) AS source_rows(i);",
        )?;
    }
    connection.query_row(&serve_sql(&uri, token), [], |row| row.get::<_, String>(0))?;

    println!("{}", serde_json::json!({ "uri": uri, "token": token }));
    io::stdout().flush()?;
    let mut serving = true;
    for command in io::stdin().lock().lines() {
        match command?.trim() {
            "stop" if serving => {
                connection.execute_batch(&format!("CALL quack_stop({})", sql_literal(&uri)))?;
                serving = false;
                println!("stopped");
                io::stdout().flush()?;
            }
            "start" if !serving => {
                connection.query_row(&serve_sql(&uri, token), [], |row| row.get::<_, String>(0))?;
                serving = true;
                println!("started");
                io::stdout().flush()?;
            }
            "states" => {
                let states: Option<String> = connection.query_row(
                    "SELECT string_agg(DISTINCT state, ',') FROM quack_active_connections()",
                    [],
                    |row| row.get(0),
                )?;
                println!("{}", serde_json::json!({ "states": states }));
                io::stdout().flush()?;
            }
            "quit" => {
                if serving {
                    connection.execute_batch(&format!("CALL quack_stop({})", sql_literal(&uri)))?;
                }
                break;
            }
            "" => {}
            other => return Err(format!("unexpected fixture owner command: {other}").into()),
        }
    }
    Ok(())
}

fn serve_sql(uri: &str, token: &str) -> String {
    format!(
        "CALL quack_serve({}, token = {}, disable_ssl = true)",
        sql_literal(uri),
        sql_literal(token)
    )
}

fn run_writer(
    extension_path: &str,
    uri: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let writer = Connection::open_in_memory()?;
    writer.execute_batch(&format!(
        "LOAD {};
         SET TimeZone = 'UTC';
         ATTACH {} AS writer (TOKEN {}, DISABLE_SSL true);
         INSERT INTO writer.main.events VALUES (4, 30, 'writer', 9.99, true);",
        sql_literal(extension_path),
        sql_literal(uri),
        sql_literal(token)
    ))?;
    Ok(())
}

fn run_reader(
    extension_path: &str,
    uri: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = Connection::open_in_memory()?;
    reader.execute_batch(&format!(
        "LOAD {};
         SET TimeZone = 'UTC';
         ATTACH {} AS reader (TOKEN {}, DISABLE_SSL true);",
        sql_literal(extension_path),
        sql_literal(uri),
        sql_literal(token)
    ))?;
    let mut statement = reader.prepare("SELECT id FROM reader.main.events ORDER BY id")?;
    let ids = statement
        .query_map([], |row| row.get::<_, i64>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    println!("{}", serde_json::json!({ "event_ids": ids }));
    Ok(())
}

fn run_catalog_update(
    extension_path: &str,
    uri: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let writer = Connection::open_in_memory()?;
    writer.execute_batch(&format!(
        "LOAD {};
         SET TimeZone = 'UTC';
         ATTACH {} AS writer (TOKEN {}, DISABLE_SSL true);
         INSERT INTO writer.main.catalog_change_events VALUES (2, 'after-analysis');",
        sql_literal(extension_path),
        sql_literal(uri),
        sql_literal(token)
    ))?;
    Ok(())
}

fn run_receipt_reader(
    extension_path: &str,
    uri: &str,
    token: &str,
    operation_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let reader = Connection::open_in_memory()?;
    reader.execute_batch(&format!(
        "LOAD {};
         SET TimeZone = 'UTC';
         ATTACH {} AS reader (TOKEN {}, DISABLE_SSL true);",
        sql_literal(extension_path),
        sql_literal(uri),
        sql_literal(token)
    ))?;
    let statement = format!(
        "SELECT operation_key, request_fingerprint FROM reader.__sparkless_meta.connect_operation_receipts WHERE operation_key = {}",
        sql_literal(operation_key)
    );
    let mut query = reader.prepare(&statement)?;
    let receipts = query
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    println!("{}", serde_json::json!({ "receipts": receipts }));
    Ok(())
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
