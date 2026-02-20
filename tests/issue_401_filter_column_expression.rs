//! Regression tests for issue #401 – filter with expression columns.
//!
//! These tests ensure that boolean expressions built via Column methods
//! can be used reliably in DataFrame::filter.

use polars::prelude::df;
use robin_sparkless::functions::{col, lit_i64};
use robin_sparkless::SparkSession;

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("issue_401_filter_column_expression")
        .get_or_create()
}

#[test]
fn issue_401_filter_with_boolean_column_expression() {
    let spark = spark();
    let pl = df![
        "id" => &[1i64, 2i64, 3i64],
        "v" => &[10i64, 20i64, 30i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // Build a boolean expression column and use it in filter.
    let expr = col("v")
        .gt_eq(lit_i64(20).into_expr())
        .into_expr();
    let filtered = df.filter(expr).unwrap();
    let rows = filtered.collect_as_json_rows().unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"].as_i64().unwrap(), 2);
    assert_eq!(rows[1]["id"].as_i64().unwrap(), 3);
}
