//! Regression tests for issue #557 – substr/substring semantics (PySpark parity).

mod common;

use common::spark;
use robin_sparkless::functions::{col, substring};
use serde_json::json;

#[test]
fn issue_557_substring_1based_positive_start() {
    // PySpark: substring("Hello", 2, 3) -> "ell" (1-based start, length 3)
    let session = spark();
    let data = vec![vec![json!("Hello")]];
    let schema = vec![("s".to_string(), "string".to_string())];
    let df = session
        .create_dataframe_from_rows(data, schema)
        .unwrap()
        .select_exprs(vec![
            substring(&col("s"), 2, Some(3)).alias("out").into_expr(),
        ])
        .unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("out").and_then(|v| v.as_str()), Some("ell"));
}

#[test]
fn issue_557_substring_negative_start_from_end() {
    // PySpark: substring("Spark SQL", -3) -> "SQL" (3 chars from end)
    let session = spark();
    let data = vec![vec![json!("Spark SQL")]];
    let schema = vec![("s".to_string(), "string".to_string())];
    let df = session
        .create_dataframe_from_rows(data, schema)
        .unwrap()
        .select_exprs(vec![
            substring(&col("s"), -3, None).alias("out").into_expr(),
        ])
        .unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("out").and_then(|v| v.as_str()), Some("SQL"));
}

#[test]
fn issue_557_substring_zero_length_empty_string() {
    // PySpark: length < 1 -> empty string
    let session = spark();
    let data = vec![vec![json!("Hello")]];
    let schema = vec![("s".to_string(), "string".to_string())];
    let df = session
        .create_dataframe_from_rows(data, schema)
        .unwrap()
        .select_exprs(vec![
            substring(&col("s"), 1, Some(0)).alias("out").into_expr(),
        ])
        .unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("out").and_then(|v| v.as_str()), Some(""));
}

#[test]
fn issue_557_substring_negative_length_empty_string() {
    let session = spark();
    let data = vec![vec![json!("Hello")]];
    let schema = vec![("s".to_string(), "string".to_string())];
    let df = session
        .create_dataframe_from_rows(data, schema)
        .unwrap()
        .select_exprs(vec![
            substring(&col("s"), 1, Some(-1)).alias("out").into_expr(),
        ])
        .unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("out").and_then(|v| v.as_str()), Some(""));
}
