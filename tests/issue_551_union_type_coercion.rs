//! Regression tests for issue #551 – Union type coercion (PySpark parity).

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_551_union_string_and_int64_coerces_to_string() {
    let session = spark();
    // Left: one row, x as string "a"
    let data = vec![vec![json!("a")]];
    let schema = vec![("x".to_string(), "string".to_string())];
    // Union with right: one row, x as int 1 → common type String, so 1 becomes "1"
    let plan_ops = vec![json!({
        "op": "union",
        "payload": {
            "other_data": [[1]],
            "other_schema": [{"name": "x", "type": "long"}]
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    // First row from left: x = "a"
    assert_eq!(rows[0].get("x").and_then(|v| v.as_str()), Some("a"));
    // Second row from right: x was 1, coerced to string "1"
    assert_eq!(rows[1].get("x").and_then(|v| v.as_str()), Some("1"));
}
