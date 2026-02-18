//! Regression tests for issue #550 – Window function approx_count_distinct (PySpark parity).

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_550_plan_window_approx_count_distinct() {
    let session = spark();
    let data = vec![
        vec![json!("A"), json!(1)],
        vec![json!("A"), json!(10)],
        vec![json!("B"), json!(5)],
    ];
    let schema = vec![
        ("type".to_string(), "string".to_string()),
        ("value".to_string(), "bigint".to_string()),
    ];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "approx_distinct",
            "expr": {
                "fn": "approx_count_distinct",
                "args": [{"col": "value"}],
                "window": {"partition_by": ["type"]}
            }
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 3);
    // Partition A: two distinct values (1, 10) -> 2; partition B: one value -> 1
    let a_rows: Vec<_> = rows
        .iter()
        .filter(|r| r.get("type").and_then(|v| v.as_str()) == Some("A"))
        .collect();
    let b_rows: Vec<_> = rows
        .iter()
        .filter(|r| r.get("type").and_then(|v| v.as_str()) == Some("B"))
        .collect();
    assert_eq!(a_rows.len(), 2);
    assert_eq!(b_rows.len(), 1);
    assert_eq!(
        a_rows[0].get("approx_distinct").and_then(|v| v.as_i64()),
        Some(2)
    );
    assert_eq!(
        b_rows[0].get("approx_distinct").and_then(|v| v.as_i64()),
        Some(1)
    );
}
