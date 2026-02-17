//! UDF and error-handling tests migrated from Python, focused on
//! Rust-level behavior (registry, call_udf, and robustness).
//!
//! These do not exercise Python-defined functions; instead they verify
//! that the Rust side correctly handles registration and lookup of
//! UDFs exposed via `call_udf`, which underpins the Python API.

use polars::prelude::df;
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
