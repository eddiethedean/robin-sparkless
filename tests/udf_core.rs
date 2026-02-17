//! UDF and error-handling tests migrated from Python, focused on
//! Rust-level behavior (registry, call_udf, and robustness).
//!
//! These do not exercise Python-defined functions; instead they verify
//! that the Rust side correctly handles registration and lookup of
//! UDFs exposed via `call_udf`, which underpins the Python API.

use polars::prelude::{df, Series};
use robin_sparkless::{col, DataFrame, SparkSession};

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("udf_core_tests")
        .get_or_create()
}

#[test]
fn call_udf_missing_name_returns_error() {
    let spark = spark();
    let pl = df!["id" => &[1i64, 2i64]].unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);
    // Ensure session is initialized for UDF registry
    let _ = df;

    let col_id = col("id");
    let result = robin_sparkless::call_udf("missing_udf", &[col_id]);
    assert!(
        result.is_err(),
        "calling unknown UDF via call_udf should return an error"
    );
}

/// Register a simple Rust UDF that adds two integer columns and ensure it can
/// be invoked via `call_udf` and used in a with_column_expr pipeline.
#[test]
fn register_and_call_rust_udf_success() {
    use polars::prelude::PolarsError;

    let spark = spark();
    let pl = df![
        "a" => &[1i64, 2i64, 3i64],
        "b" => &[10i64, 20i64, 30i64],
    ]
    .unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);

    // Register a Rust UDF that computes a + b.
    spark
        .register_udf("add_ab", |cols: &[Series]| -> Result<Series, PolarsError> {
            let a = &cols[0];
            let b = &cols[1];
            let sum = a + b;
            Ok(sum?)
        })
        .expect("register_udf should succeed");

    // Build a column invoking the UDF and add it to the DataFrame.
    let c_add = robin_sparkless::call_udf("add_ab", &[col("a"), col("b")])
        .expect("registered UDF should resolve");
    let df2 = df
        .with_column_expr("c", c_add.into_expr())
        .expect("with_column_expr should succeed");

    let rows = df2.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["c"].as_i64().unwrap(), 11);
    assert_eq!(rows[1]["c"].as_i64().unwrap(), 22);
    assert_eq!(rows[2]["c"].as_i64().unwrap(), 33);
}
