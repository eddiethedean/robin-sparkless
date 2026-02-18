//! Regression tests for issue #558 – Join type coercion (PySpark parity).
//! Join keys with different types (e.g. string vs int) are coerced to a common type.

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_558_join_string_and_long_coerces() {
    let session = spark();
    // Left: id as string "1", "2"
    let left_data = vec![vec![json!("1"), json!("a")], vec![json!("2"), json!("b")]];
    let left_schema = vec![
        ("id".to_string(), "string".to_string()),
        ("label".to_string(), "string".to_string()),
    ];
    // Right: id as long 1, 2
    let right_schema_plan = vec![
        json!({"name": "id", "type": "long"}),
        json!({"name": "name", "type": "string"}),
    ];
    let plan_ops = vec![json!({
        "op": "join",
        "payload": {
            "other_data": [[1, "x"], [2, "y"]],
            "other_schema": right_schema_plan,
            "on": ["id"],
            "how": "inner"
        }
    })];
    let df = plan::execute_plan(&session, left_data, left_schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(
        rows.len(),
        2,
        "inner join on id: string 1,2 vs long 1,2 -> 2 rows"
    );
}
