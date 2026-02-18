//! Regression tests for issue #559 – unionByName diamond duplicate rows (PySpark parity).
//! (A unionByName B) unionByName A must preserve duplicate rows from A (no accidental dedup).

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_559_union_by_name_diamond_preserves_duplicate_rows() {
    let session = spark();
    // A: 2 rows
    let data = vec![vec![json!(1), json!("x")], vec![json!(2), json!("y")]];
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    // Plan: A unionByName B (1 row), then result unionByName A again (diamond)
    let plan_ops = vec![
        json!({
            "op": "unionByName",
            "payload": {
                "other_data": [[3, "z"]],
                "other_schema": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}]
            }
        }),
        json!({
            "op": "unionByName",
            "payload": {
                "other_data": [[1, "x"], [2, "y"]],
                "other_schema": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}]
            }
        }),
    ];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    // Expected: A (2) + B (1) + A (2) = 5 rows
    assert_eq!(
        rows.len(),
        5,
        "diamond (A u B) u A must preserve duplicates: 2 + 1 + 2 = 5 rows"
    );
}
