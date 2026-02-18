//! Regression tests for issue #556 – Reverse-operator arithmetic (PySpark parity).
//! (1 - col("x")) and (100 * col("x")) with literal on left.

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_556_plan_lit_minus_col() {
    let session = spark();
    let data = vec![vec![json!(10)], vec![json!(20)]];
    let schema = vec![("x".to_string(), "bigint".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "rev",
            "expr": {"op": "sub", "left": {"lit": 1}, "right": {"col": "x"}}
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("rev").and_then(|v| v.as_i64()), Some(-9));
    assert_eq!(rows[1].get("rev").and_then(|v| v.as_i64()), Some(-19));
}

#[test]
fn issue_556_plan_lit_times_col() {
    let session = spark();
    let data = vec![vec![json!(3)], vec![json!(5)]];
    let schema = vec![("x".to_string(), "bigint".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "scaled",
            "expr": {"op": "mul", "left": {"lit": 100}, "right": {"col": "x"}}
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("scaled").and_then(|v| v.as_i64()), Some(300));
    assert_eq!(rows[1].get("scaled").and_then(|v| v.as_i64()), Some(500));
}
