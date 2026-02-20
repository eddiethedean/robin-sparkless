//! Tests for PR6: Joins row count (Fixes #639).
//!
//! Plan join (inner, left, right, outer) must return correct row counts.

use robin_sparkless::plan;
use robin_sparkless::SparkSession;
use serde_json::json;

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("issue_639_joins_row_count")
        .get_or_create()
}

/// #639: inner join row count — only matching rows.
#[test]
fn plan_join_inner_row_count() {
    let spark = spark();
    let left_data = vec![
        vec![json!(1), json!(10)],
        vec![json!(2), json!(20)],
        vec![json!(3), json!(10)],
    ];
    let left_schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("fk".to_string(), "bigint".to_string()),
    ];
    let other_schema = vec![
        json!({"name": "fk", "type": "long"}),
        json!({"name": "label", "type": "string"}),
    ];
    let plan_steps = vec![json!({
        "op": "join",
        "payload": {
            "other_data": [[10, "A"], [20, "B"]],
            "other_schema": other_schema,
            "on": ["fk"],
            "how": "inner"
        }
    })];
    let df = plan::execute_plan(&spark, left_data, left_schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 3, "inner join: 3 left rows match (10,20,10)");
}

/// #639: left join row count — all left rows preserved.
#[test]
fn plan_join_left_row_count() {
    let spark = spark();
    let left_data = vec![
        vec![json!(1), json!(10)],
        vec![json!(2), json!(99)],
    ];
    let left_schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("fk".to_string(), "bigint".to_string()),
    ];
    let other_schema = vec![
        json!({"name": "fk", "type": "long"}),
        json!({"name": "label", "type": "string"}),
    ];
    let plan_steps = vec![json!({
        "op": "join",
        "payload": {
            "other_data": [[10, "X"]],
            "other_schema": other_schema,
            "on": ["fk"],
            "how": "left"
        }
    })];
    let df = plan::execute_plan(&spark, left_data, left_schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2, "left join: 2 left rows (fk=99 has null right)");
}
