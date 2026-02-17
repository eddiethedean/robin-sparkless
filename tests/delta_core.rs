//! Delta Lake behavior tests migrated from `tests/python/test_robin_sparkless.py`.
//!
//! These validate that `write_delta` and `read_delta` round-trip data when the
//! `delta` feature is enabled.

use std::fs;
use std::path::PathBuf;

use polars::prelude::df;
use robin_sparkless::{DataFrame, SparkSession};

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("delta_core_tests")
        .get_or_create()
}

#[test]
fn write_and_read_delta_round_trip_core() {
    // Skip gracefully when delta feature is not built by checking error message.
    let spark = spark();
    let tmp_dir = tempfile::tempdir().unwrap();
    let path: PathBuf = tmp_dir.path().join("delta_table");

    let pl = df![
        "id" => &[1i64, 2i64, 3i64],
        "v" => &[10i64, 20i64, 30i64],
        "name" => &["a", "b", "c"],
    ]
    .unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);

    let write_result = df.write_delta(&path, true);
    if let Err(e) = write_result {
        let msg = format!("{e}");
        if msg.contains("requires the 'delta' feature") {
            // Feature not built; skip this test.
            return;
        }
        panic!("unexpected error from write_delta: {msg}");
    }

    assert!(
        fs::metadata(&path).is_ok(),
        "delta path should exist after write"
    );

    let back = spark.read_delta_from_path(&path);
    let back = match back {
        Ok(df) => df,
        Err(e) => {
            let msg = format!("{e}");
            if msg.contains("requires the 'delta' feature") {
                // Feature not built; skip.
                return;
            }
            panic!("unexpected error from read_delta_from_path: {msg}");
        }
    };

    let rows = back.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["id"].as_i64().unwrap(), 1);
    assert_eq!(rows[1]["id"].as_i64().unwrap(), 2);
    assert_eq!(rows[2]["id"].as_i64().unwrap(), 3);
}

/// Verify append vs overwrite semantics for Delta writes when the `delta`
/// feature is enabled.
#[test]
fn write_delta_append_and_overwrite_core() {
    let spark = spark();
    let tmp_dir = tempfile::tempdir().unwrap();
    let path: PathBuf = tmp_dir.path().join("delta_table_append");

    let pl1 = df![
        "id" => &[1i64, 2i64],
        "v" => &[10i64, 20i64],
    ]
    .unwrap();
    let pl2 = df![
        "id" => &[3i64],
        "v" => &[30i64],
    ]
    .unwrap();

    let df1: DataFrame = spark.create_dataframe_from_polars(pl1);
    let df2: DataFrame = spark.create_dataframe_from_polars(pl2);

    // First write: overwrite = true (create table).
    if let Err(e) = df1.write_delta(&path, true) {
        let msg = format!("{e}");
        if msg.contains("requires the 'delta' feature") {
            return;
        }
        panic!("unexpected error from first write_delta: {msg}");
    }

    // Second write: append a new row.
    if let Err(e) = df2.write_delta(&path, false) {
        let msg = format!("{e}");
        if msg.contains("requires the 'delta' feature") {
            return;
        }
        panic!("unexpected error from append write_delta: {msg}");
    }

    // Read back and expect three rows total.
    let back = match spark.read_delta_from_path(&path) {
        Ok(df) => df,
        Err(e) => {
            let msg = format!("{e}");
            if msg.contains("requires the 'delta' feature") {
                return;
            }
            panic!("unexpected error from read_delta_from_path: {msg}");
        }
    };

    let rows = back.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 3);
}
