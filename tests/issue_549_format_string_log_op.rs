//! Regression tests for issue #549 – format_string and log() as expression op.

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_549_plan_op_format_string() {
    let session = spark();
    let data = vec![vec![json!("Alice"), json!(123)]];
    let schema = vec![
        ("Name".to_string(), "string".to_string()),
        ("IntegerValue".to_string(), "bigint".to_string()),
    ];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "formatted",
            "expr": {
                "op": "format_string",
                "args": [{"lit": "%s: %d"}, {"col": "Name"}, {"col": "IntegerValue"}]
            }
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("formatted").and_then(|v| v.as_str()),
        Some("Alice: 123")
    );
}

#[test]
fn issue_549_plan_op_log_one_arg() {
    let session = spark();
    // log(col) = natural log; op "log" is accepted
    let data = vec![vec![json!(1.0)]];
    let schema = vec![("x".to_string(), "double".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "ln_x",
            "expr": {"op": "log", "args": [{"col": "x"}]}
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    let v = rows[0].get("ln_x").and_then(|v| v.as_f64()).unwrap();
    assert!((v - 0.0).abs() < 1e-10, "ln(1) = 0, got {}", v);
}
