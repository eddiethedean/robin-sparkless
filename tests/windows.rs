//! Window functions.
//!
//! Merged from: issue_550, issue_642.

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

#[test]
fn plan_with_column_row_number_window() {
    let spark = spark();
    let schema = vec![
        ("dept".to_string(), "string".to_string()),
        ("salary".to_string(), "bigint".to_string()),
    ];
    let rows = vec![
        vec![json!("A"), json!(10)],
        vec![json!("A"), json!(20)],
        vec![json!("B"), json!(30)],
    ];
    let plan_steps = vec![
        json!({
            "op": "withColumn",
            "payload": {
                "name": "rn",
                "expr": {"fn": "row_number", "window": {"partition_by": ["dept"]}}
            }
        }),
        json!({"op": "select", "payload": ["dept", "salary", "rn"]}),
    ];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].get("rn").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(out[1].get("rn").and_then(|v| v.as_i64()), Some(2));
    assert_eq!(out[2].get("rn").and_then(|v| v.as_i64()), Some(1));
}
