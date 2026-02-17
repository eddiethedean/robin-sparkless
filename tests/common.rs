//! Shared helpers for integration tests (SparkSession and DataFrame setup).

use polars::prelude::df;
use robin_sparkless::{DataFrame, SparkSession};

/// Create a SparkSession with a descriptive app name for tests.
pub fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("robin_sparkless_tests")
        .get_or_create()
}

/// Convenience helper for a small (id, age, name) test DataFrame.
pub fn small_people_df() -> DataFrame {
    let spark = spark();
    let pl = df![
        "id" => &[1i64, 2i64, 3i64],
        "age" => &[25i64, 30i64, 35i64],
        "name" => &["Alice", "Bob", "Carol"],
    ]
    .unwrap();
    spark.create_dataframe_from_polars(pl)
}
