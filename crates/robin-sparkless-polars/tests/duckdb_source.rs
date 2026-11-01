#![cfg(feature = "duckdb-source")]

use robin_sparkless_core::engine::{CancellationToken, begin_external_catalog_read_snapshot};
use robin_sparkless_polars::{
    DataType, DuckDbCatalog, DuckDbSourceConfig, ExternalCatalogProvider, SparkSession,
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
    assert!(analyzed.collect_rows().unwrap().is_empty());
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
    let rows = loaded.collect_rows().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["label"], serde_json::json!("alpha"));
    assert_eq!(rows[0]["amount"], serde_json::json!(12.5));
    assert_eq!(rows[1]["amount"], serde_json::Value::Null);
    assert_eq!(rows[2]["id"], serde_json::json!(3));
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
    assert!(empty.collect_rows().unwrap().is_empty());
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
    assert!(snapshot.resolve_table("other_database.events").is_err());
}
