//! Regression tests for issue #548 – date_trunc and to_date as expression op (PySpark parity).

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_548_plan_op_to_date() {
    let session = spark();
    let data = vec![vec![json!("2024-03-15")]];
    let schema = vec![("d".to_string(), "string".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "dt",
            "expr": {"op": "to_date", "args": [{"col": "d"}]}
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get("dt").is_some());
}

#[test]
fn issue_548_plan_op_date_trunc() {
    let session = spark();
    let data = vec![vec![json!("2024-03-15 12:30:45")]];
    let schema = vec![("ts".to_string(), "string".to_string())];
    // to_date then date_trunc with Polars duration "1d" (day)
    let plan_ops = vec![
        json!({
            "op": "withColumn",
            "payload": {
                "name": "dt",
                "expr": {"op": "to_date", "args": [{"col": "ts"}]}
            }
        }),
        json!({
            "op": "withColumn",
            "payload": {
                "name": "truncated",
                "expr": {"op": "date_trunc", "args": [{"lit": "1d"}, {"col": "dt"}]}
            }
        }),
    ];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get("truncated").is_some());
}
