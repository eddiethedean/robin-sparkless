//! Regression tests for issue #554 – Array column and explode (PySpark parity).
//! Plan interpreter accepts {"op": "explode", "args": [{"col": "arr"}]}.

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_554_plan_op_explode() {
    let session = spark();
    let data = vec![vec![json!([1, 2, 3])], vec![json!([10, 20])]];
    let schema = vec![("arr".to_string(), "array".to_string())];
    let plan_ops = vec![json!({
        "op": "select",
        "payload": {
            "columns": [
                {
                    "name": "x",
                    "expr": {"op": "explode", "args": [{"col": "arr"}]}
                }
            ]
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 5); // 3 + 2 elements
    let x_vals: Vec<i64> = rows
        .iter()
        .filter_map(|r| r.get("x").and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(x_vals, vec![1, 2, 3, 10, 20]);
}
