//! Tests for PR7: Window row_number in plan (Fixes #642).

use robin_sparkless::plan;
use robin_sparkless::SparkSession;
use serde_json::json;

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("issue_642_window_row_number")
        .get_or_create()
}

/// #642: plan withColumn row_number() over partition.
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
