#![cfg(feature = "duckdb-source")]

use robin_sparkless_core::engine::{CancellationToken, begin_external_catalog_read_snapshot};
use robin_sparkless_polars::{
    DataType, DuckDbCatalog, DuckDbQuackSourceConfig, DuckDbSourceConfig, DuckDbSourceSpec,
    ExternalCatalogProvider, SparkSession,
};
use std::sync::Arc;

fn fixture_database() -> (tempfile::TempDir, std::path::PathBuf) {
    let directory = tempfile::tempdir().expect("create DuckDB test directory");
    let database = directory.path().join("source.duckdb");
    let connection = duckdb::Connection::open(&database).expect("create DuckDB fixture");
    connection
        .execute_batch(
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
            INSERT INTO nested_events VALUES ([1, 2]);
            CREATE TABLE empty_events (id BIGINT NOT NULL, label VARCHAR);",
        )
        .expect("populate DuckDB fixture");
    drop(connection);
    (directory, database)
}

fn source(database: &std::path::Path) -> Arc<DuckDbCatalog> {
    Arc::new(
        DuckDbCatalog::new([DuckDbSourceConfig {
            alias: "warehouse".into(),
            database_path: database.to_path_buf(),
        }])
        .expect("configure DuckDB catalog"),
    )
}

#[test]
fn duckdb_read_only_catalog_resolves_schema_and_streams_typed_batches() {
    let (_directory, database) = fixture_database();
    let provider = source(&database);
    let session = SparkSession::builder().get_or_create();
    session
        .set_external_catalog_provider(Some(provider))
        .expect("attach external catalog");

    // Schema analysis returns a schema-bearing empty relation without reading table rows.
    let analyzed = session
        .table_schema_only("warehouse.main.events")
        .expect("analyze table schema");
    assert!(analyzed.collect_as_json_rows().unwrap().is_empty());
    let schema = analyzed.schema_engine().unwrap();
    let fields = schema.logical_fields();
    assert_eq!(
        fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "bucket", "label", "amount", "active"]
    );
    assert!(matches!(fields[0].data_type, DataType::Long));
    assert!(!fields[0].nullable);
    assert!(matches!(fields[1].data_type, DataType::Integer));
    assert!(matches!(fields[3].data_type, DataType::Decimal(12, 2)));
    assert!(matches!(fields[4].data_type, DataType::Boolean));

    let loaded = session
        .table("warehouse.events")
        .expect("read DuckDB table");
    let rows = loaded.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["label"], serde_json::json!("alpha"));
    assert_eq!(rows[0]["amount"], serde_json::json!("12.50"));
    assert_eq!(rows[1]["amount"], serde_json::Value::Null);
    assert_eq!(rows[2]["id"], serde_json::json!(3));

    let typed = session
        .table("warehouse.typed_events")
        .expect("read typed scalar table");
    let typed_schema = typed.schema_engine().unwrap();
    let typed_fields = typed_schema.logical_fields();
    assert!(matches!(typed_fields[0].data_type, DataType::Short));
    assert!(matches!(typed_fields[1].data_type, DataType::Double));
    assert!(matches!(typed_fields[2].data_type, DataType::Date));
    assert!(matches!(typed_fields[3].data_type, DataType::Timestamp));
    assert_eq!(typed.collect_as_json_rows().unwrap().len(), 1);
    assert!(session.table("warehouse.non_finite_events").is_err());
    assert!(session.table("warehouse.nested_events").is_err());
}

#[test]
fn duckdb_empty_tables_keep_their_declared_schema_and_unknown_aliases_fail() {
    let (_directory, database) = fixture_database();
    let session = SparkSession::builder().get_or_create();
    session
        .set_external_catalog_provider(Some(source(&database)))
        .expect("attach external catalog");

    let empty = session
        .table("warehouse.main.empty_events")
        .expect("read empty table");
    assert!(empty.collect_as_json_rows().unwrap().is_empty());
    assert_eq!(
        empty
            .schema_engine()
            .unwrap()
            .logical_fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "label"]
    );
    assert!(session.table("unconfigured.events").is_err());
}

#[test]
fn duckdb_catalog_rejects_duplicate_aliases_and_missing_database_files() {
    let (_directory, database) = fixture_database();
    let duplicate = DuckDbCatalog::new([
        DuckDbSourceConfig {
            alias: "warehouse".into(),
            database_path: database.clone(),
        },
        DuckDbSourceConfig {
            alias: "WAREHOUSE".into(),
            database_path: database,
        },
    ]);
    assert!(duplicate.is_err());
    assert!(
        DuckDbCatalog::new([DuckDbSourceConfig {
            alias: "missing".into(),
            database_path: "/nonexistent/sparkless.duckdb".into(),
        }])
        .is_err()
    );
}

#[test]
fn duckdb_quack_configuration_requires_one_consistent_connection_per_endpoint() {
    let directory = tempfile::tempdir().expect("create extension fixture directory");
    let extension_path = directory.path().join("quack.duckdb_extension");
    std::fs::write(&extension_path, b"fixture").expect("create extension path fixture");
    let source = |alias: &str, token: &str| {
        DuckDbSourceSpec::Quack(DuckDbQuackSourceConfig {
            alias: alias.into(),
            quack_uri: "quack:localhost:9494".into(),
            token: token.into(),
            extension_path: extension_path.clone(),
            disable_ssl: None,
            ssl_fingerprint: None,
        })
    };

    assert!(
        DuckDbCatalog::new_with_sources([
            source("first", "shared-token"),
            source("second", "shared-token")
        ])
        .is_ok()
    );
    assert!(
        DuckDbCatalog::new_with_sources([
            source("first", "first-token"),
            source("second", "second-token")
        ])
        .is_err()
    );
    assert!(
        !format!(
            "{:?}",
            DuckDbQuackSourceConfig {
                alias: "warehouse".into(),
                quack_uri: "quack:localhost:9494".into(),
                token: "secret-token".into(),
                extension_path,
                disable_ssl: None,
                ssl_fingerprint: None,
            }
        )
        .contains("secret-token")
    );
}

#[test]
fn duckdb_read_snapshot_reuses_one_database_owner_for_multiple_tables() {
    let (_directory, database) = fixture_database();
    let provider: Arc<dyn ExternalCatalogProvider> = source(&database);
    let token = CancellationToken::new();
    let snapshot = begin_external_catalog_read_snapshot(Arc::clone(&provider), &token)
        .expect("begin source snapshot");
    assert!(
        snapshot
            .resolve_table("warehouse.events")
            .unwrap()
            .is_some()
    );
    assert!(
        snapshot
            .resolve_table("warehouse.empty_events")
            .unwrap()
            .is_some()
    );
    assert!(
        snapshot
            .resolve_table("other_database.events")
            .unwrap()
            .is_none()
    );
}

#[test]
#[ignore = "requires SPARKLESS_TEST_QUACK_EXTENSION_PATH for DuckDB 2.0.0"]
fn quack_shared_owner_pins_read_snapshot_and_observes_writer_commit_next_statement() {
    use std::net::TcpListener;

    let extension_path = std::env::var("SPARKLESS_TEST_QUACK_EXTENSION_PATH")
        .expect("set the path to the DuckDB 2.0.0 quack.duckdb_extension");
    let extension_literal = sql_literal(&extension_path);
    let port_listener = TcpListener::bind(("127.0.0.1", 0)).expect("allocate free Quack port");
    let port = port_listener.local_addr().unwrap().port();
    drop(port_listener);

    let uri = format!("quack:localhost:{port}");
    let token = "sparkless-phase419-test-token";
    let server = duckdb::Connection::open_in_memory().expect("open Quack owner");
    server
        .execute_batch(&format!(
            "LOAD {extension_literal};
             SET TimeZone = 'UTC';
             CREATE TABLE events (id BIGINT, note VARCHAR);
             INSERT INTO events VALUES (1, 'one');
             CREATE VIEW slow_events AS
                 SELECT i::BIGINT AS id FROM range(1000000000) AS source_rows(i);"
        ))
        .expect("load Quack and create owner fixture");
    server
        .query_row(
            &format!(
                "CALL quack_serve({}, token = {})",
                sql_literal(&uri),
                sql_literal(token)
            ),
            [],
            |row| row.get::<_, String>(0),
        )
        .expect("start Quack service");

    let writer = duckdb::Connection::open_in_memory().expect("open independent writer client");
    writer
        .execute_batch(&format!(
            "LOAD {extension_literal};
             ATTACH {} AS writer (TOKEN {});",
            sql_literal(&uri),
            sql_literal(token)
        ))
        .expect("attach writer to shared owner");

    let catalog_json = serde_json::json!([{
        "alias": "warehouse",
        "quack_uri": uri,
        "token": token,
        "extension_path": extension_path,
        "disable_ssl": true,
    }]);
    let provider: Arc<dyn ExternalCatalogProvider> = Arc::new(
        DuckDbCatalog::from_json(&catalog_json.to_string()).expect("configure Quack source"),
    );
    let cancellation = CancellationToken::new();
    let first_snapshot = begin_external_catalog_read_snapshot(Arc::clone(&provider), &cancellation)
        .expect("begin first shared-owner snapshot");
    let descriptor = first_snapshot
        .resolve_table("warehouse.events")
        .expect("resolve table in first snapshot")
        .expect("table exists");

    writer
        .execute_batch("INSERT INTO writer.main.events VALUES (2, 'two');")
        .expect("commit independent writer change");

    let mut first_snapshot_rows = 0;
    first_snapshot
        .scan_table(&descriptor, &cancellation, &mut |batch| {
            first_snapshot_rows += batch.row_count;
            Ok(())
        })
        .expect("scan from pinned snapshot");
    assert_eq!(first_snapshot_rows, 1);
    drop(first_snapshot);

    let second_snapshot =
        begin_external_catalog_read_snapshot(Arc::clone(&provider), &cancellation)
            .expect("begin second shared-owner snapshot");
    let descriptor = second_snapshot
        .resolve_table("warehouse.events")
        .expect("resolve table in second snapshot")
        .expect("table exists");
    let mut second_snapshot_rows = 0;
    second_snapshot
        .scan_table(&descriptor, &cancellation, &mut |batch| {
            second_snapshot_rows += batch.row_count;
            Ok(())
        })
        .expect("scan after writer commit");
    assert_eq!(second_snapshot_rows, 2);
    drop(second_snapshot);

    // Cancelling after the first remote batch must stop work on the shared DuckDB owner, not
    // only interrupt DuckDB's local HTTP client connection.
    let cancel_token = CancellationToken::new();
    let cancel_snapshot = begin_external_catalog_read_snapshot(provider, &cancel_token)
        .expect("begin cancellable shared-owner snapshot");
    let slow_table = cancel_snapshot
        .resolve_table("warehouse.slow_events")
        .expect("resolve slow view")
        .expect("slow view exists");
    let cancel_signal = cancel_token.clone();
    let cancel_started = std::time::Instant::now();
    let cancelled = cancel_snapshot.scan_table(&slow_table, &cancel_token, &mut |batch| {
        assert!(batch.row_count > 0);
        cancel_signal.cancel();
        Ok(())
    });
    assert!(matches!(
        cancelled,
        Err(robin_sparkless_core::EngineError::Cancelled(_))
    ));
    assert!(cancel_started.elapsed() < std::time::Duration::from_secs(8));
    let owner_states_after_cancel: String = server
        .query_row(
            "SELECT string_agg(DISTINCT state, ',') FROM quack_active_connections()",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .expect("inspect Quack owner after cancellation")
        .expect("Quack owner reported connection states");
    assert!(
        owner_states_after_cancel
            .split(',')
            .any(|state| state.trim() == "cancelled"),
        "owner did not record the cancelled remote query: {owner_states_after_cancel}"
    );
    drop(cancel_snapshot);
    drop(writer);
    server
        .execute_batch(&format!("CALL quack_stop({})", sql_literal(&uri)))
        .expect("stop Quack service");
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
