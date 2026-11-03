#![cfg(feature = "duckdb-source")]

use robin_sparkless_core::engine::{
    CancellationToken, CatalogOwnership, CatalogResolver, ExternalCatalog,
    begin_catalog_read_snapshot,
};
use robin_sparkless_polars::{
    DataType, DuckDbCatalog, DuckDbQuackSourceConfig, DuckDbSourceConfig, DuckDbSourceSpec,
    SparkSession,
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
        .set_external_catalog(Some(provider.into_external_catalog()))
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
        .set_external_catalog(Some(source(&database).into_external_catalog()))
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
fn duckdb_decimal_precision_and_scale_boundaries_keep_exact_values() {
    let (_directory, database) = fixture_database();
    let session = SparkSession::builder().get_or_create();
    session
        .set_external_catalog(Some(source(&database).into_external_catalog()))
        .expect("attach external catalog");

    let decimals = session
        .table("warehouse.decimal_boundary_events")
        .expect("read decimal boundary table");
    let schema = decimals.schema_engine().unwrap();
    let fields = schema.logical_fields();
    assert_eq!(fields.len(), 5);
    assert!(matches!(fields[0].data_type, DataType::Decimal(1, 0)));
    assert!(matches!(fields[1].data_type, DataType::Decimal(9, 0)));
    assert!(matches!(fields[2].data_type, DataType::Decimal(18, 6)));
    assert!(matches!(fields[3].data_type, DataType::Decimal(38, 0)));
    assert!(matches!(fields[4].data_type, DataType::Decimal(38, 38)));

    let rows = decimals.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    let mut rendered = rows
        .iter()
        .map(|row| {
            ["d1", "d9", "d18", "d38", "d38s38"].map(|name| row[name].as_str().unwrap().to_string())
        })
        .collect::<Vec<_>>();
    rendered.sort_by(|left, right| left[0].cmp(&right[0]));
    let nines_38 = "9".repeat(38);
    let fractional_38 = format!("0.{}", "9".repeat(38));
    assert_eq!(
        rendered,
        [
            [
                "-9".to_string(),
                "-999999999".to_string(),
                "-999999999999.999999".to_string(),
                format!("-{nines_38}"),
                format!("-{}", fractional_38),
            ],
            [
                "9".to_string(),
                "999999999".to_string(),
                "999999999999.999999".to_string(),
                nines_38,
                fractional_38,
            ],
        ]
    );
}

#[test]
fn duckdb_integer_extrema_keep_exact_values_and_non_null_schema() {
    let (_directory, database) = fixture_database();
    let session = SparkSession::builder().get_or_create();
    session
        .set_external_catalog(Some(source(&database).into_external_catalog()))
        .expect("attach external catalog");

    let extrema = session
        .table("warehouse.integer_boundary_events")
        .expect("read integer boundary table");
    let schema = extrema.schema_engine().unwrap();
    let fields = schema.logical_fields();
    assert_eq!(fields.len(), 6);
    assert!(matches!(fields[0].data_type, DataType::Short));
    assert!(matches!(fields[1].data_type, DataType::Short));
    assert!(matches!(fields[2].data_type, DataType::Integer));
    assert!(matches!(fields[3].data_type, DataType::Integer));
    assert!(matches!(fields[4].data_type, DataType::Long));
    assert!(matches!(fields[5].data_type, DataType::Long));
    assert!(fields.iter().all(|field| !field.nullable));
    assert_eq!(
        fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        [
            "small_min",
            "small_max",
            "int_min",
            "int_max",
            "bigint_min",
            "bigint_max"
        ]
    );

    let rows = extrema.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["small_min"].as_i64(), Some(i16::MIN as i64));
    assert_eq!(row["small_max"].as_i64(), Some(i16::MAX as i64));
    assert_eq!(row["int_min"].as_i64(), Some(i32::MIN as i64));
    assert_eq!(row["int_max"].as_i64(), Some(i32::MAX as i64));
    assert_eq!(row["bigint_min"].as_i64(), Some(i64::MIN));
    assert_eq!(row["bigint_max"].as_i64(), Some(i64::MAX));
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
fn duckdb_source_write_targets_are_read_only_even_when_the_table_is_missing() {
    let (_directory, database) = fixture_database();
    let provider = source(&database);

    let is_read_only_owner = |identifier: &str| {
        matches!(
            provider.resolve_ownership(identifier).unwrap(),
            CatalogOwnership::External { capabilities, .. }
                if capabilities.read && !capabilities.write && !capabilities.batch_write
        )
    };
    assert!(is_read_only_owner("warehouse.main.events"));
    assert!(is_read_only_owner("WAREHOUSE.main.not_created"));
    assert!(is_read_only_owner("warehouse.main"));
    assert!(!is_read_only_owner("unconfigured.main.events"));
    assert!(!is_read_only_owner("warehouse.bad-name"));
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
fn duckdb_quack_configuration_can_omit_the_packaged_extension_path() {
    let error = match DuckDbCatalog::from_json(
        r#"[{"alias":"warehouse","quack_uri":"quack:localhost:9494","token":"fixture-token"}]"#,
    ) {
        Ok(_) => panic!("the unbundled test executable has no adjacent Quack extension"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("extension path is unavailable"));
    assert!(!error.to_string().contains("missing field `extension_path`"));
}

#[test]
fn duckdb_read_snapshot_reuses_one_database_owner_for_multiple_tables() {
    let (_directory, database) = fixture_database();
    let provider: Arc<ExternalCatalog> = source(&database).into_external_catalog();
    let token = CancellationToken::new();
    let snapshot = begin_catalog_read_snapshot(&provider, &token).expect("begin source snapshot");
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
             CREATE TABLE labels (id BIGINT, note VARCHAR);
             INSERT INTO labels VALUES (1, 'one');
             CREATE VIEW slow_events AS
                 SELECT i::BIGINT AS id FROM range(1000000000) AS source_rows(i);"
        ))
        .expect("load Quack and create owner fixture");
    server
        .query_row(
            &format!(
                "CALL quack_serve({}, token = {}, disable_ssl = true)",
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
    let provider: Arc<ExternalCatalog> = Arc::new(
        DuckDbCatalog::from_json(&catalog_json.to_string()).expect("configure Quack source"),
    )
    .into_external_catalog();
    let cancellation = CancellationToken::new();
    let first_snapshot = begin_catalog_read_snapshot(&provider, &cancellation)
        .expect("begin first shared-owner snapshot");
    let descriptor = first_snapshot
        .resolve_table("warehouse.events")
        .expect("resolve table in first snapshot")
        .expect("table exists");
    let labels_descriptor = first_snapshot
        .resolve_table("warehouse.labels")
        .expect("resolve second table in first snapshot")
        .expect("second table exists");

    writer
        .execute_batch(
            "BEGIN TRANSACTION;
             INSERT INTO writer.main.events VALUES (2, 'two');
             INSERT INTO writer.main.labels VALUES (2, 'two');
             COMMIT;",
        )
        .expect("commit independent multi-table writer change");

    let mut first_snapshot_rows = 0;
    first_snapshot
        .scan_table(&descriptor, &cancellation, &mut |batch| {
            first_snapshot_rows += batch.row_count;
            Ok(())
        })
        .expect("scan from pinned snapshot");
    assert_eq!(first_snapshot_rows, 1);
    let mut first_snapshot_label_rows = 0;
    first_snapshot
        .scan_table(&labels_descriptor, &cancellation, &mut |batch| {
            first_snapshot_label_rows += batch.row_count;
            Ok(())
        })
        .expect("scan second table from the same pinned snapshot");
    assert_eq!(first_snapshot_label_rows, 1);
    drop(first_snapshot);

    let second_snapshot = begin_catalog_read_snapshot(&provider, &cancellation)
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
    let labels_descriptor = second_snapshot
        .resolve_table("warehouse.labels")
        .expect("resolve second table in second snapshot")
        .expect("second table exists");
    let mut second_snapshot_label_rows = 0;
    second_snapshot
        .scan_table(&labels_descriptor, &cancellation, &mut |batch| {
            second_snapshot_label_rows += batch.row_count;
            Ok(())
        })
        .expect("scan second table after writer commit");
    assert_eq!(second_snapshot_label_rows, 2);
    drop(second_snapshot);

    // Cancelling after the first remote batch must stop work on the shared DuckDB owner, not
    // only interrupt DuckDB's local HTTP client connection.
    let cancel_token = CancellationToken::new();
    let cancel_snapshot = begin_catalog_read_snapshot(&provider, &cancel_token)
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
        Err(error) if error.category() == robin_sparkless_core::ErrorCategory::Cancelled
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
