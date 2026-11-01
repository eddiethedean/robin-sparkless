use duckdb::Connection;
use std::io::{self, BufRead, Write};
use std::net::TcpListener;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let extension_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: duckdb_quack_fixture_owner <quack.duckdb_extension>")?;
    let extension_path = std::fs::canonicalize(extension_path)?;
    let extension_path = extension_path
        .to_str()
        .ok_or("Quack extension path must be valid UTF-8")?;

    let port_listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = port_listener.local_addr()?.port();
    drop(port_listener);
    let uri = format!("quack:localhost:{port}");
    let token = "sparkless-4.19-fixture-token";

    let connection = Connection::open_in_memory()?;
    connection.execute_batch(&format!(
        "LOAD {};
         SET TimeZone = 'UTC';
         CREATE TABLE events (
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
             (-32768, 3.5, DATE '2026-10-30', TIMESTAMPTZ '2026-10-30 12:34:56+00');
         CREATE TABLE non_finite_events (score DOUBLE);
         INSERT INTO non_finite_events VALUES (CAST('NaN' AS DOUBLE));
         CREATE TABLE nested_events (items INTEGER[]);
         INSERT INTO nested_events VALUES ([1, 2]);",
        sql_literal(extension_path)
    ))?;
    connection.query_row(
        &format!(
            "CALL quack_serve({}, token = {})",
            sql_literal(&uri),
            sql_literal(token)
        ),
        [],
        |row| row.get::<_, String>(0),
    )?;

    println!("{}", serde_json::json!({ "uri": uri, "token": token }));
    io::stdout().flush()?;
    let mut command = String::new();
    io::stdin().lock().read_line(&mut command)?;
    connection.execute_batch(&format!("CALL quack_stop({})", sql_literal(&uri)))?;
    Ok(())
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
