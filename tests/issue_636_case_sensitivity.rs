//! Tests for PR9: Case sensitivity (Fixes #636).
//!
//! Column names in plan resolve case-insensitively (e.g. col("ID") when schema has "id").

use robin_sparkless::plan;
use robin_sparkless::SparkSession;
use serde_json::json;

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("issue_636_case_sensitivity")
        .get_or_create()
}

/// #636: plan filter with uppercase column ref resolves to lowercase schema column.
#[test]
fn plan_filter_case_insensitive_column_ref() {
    let spark = spark();
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let rows = vec![
        vec![json!(1), json!("a")],
        vec![json!(2), json!("b")],
    ];
    let plan_steps = vec![json!({
        "op": "filter",
        "payload": {"op": "gt", "left": {"col": "ID"}, "right": {"lit": 1}}
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("id").and_then(|v| v.as_i64()), Some(2));
}

/// #636: plan select with mixed-case column name resolves to schema column.
#[test]
fn plan_select_case_insensitive_column_name() {
    let spark = spark();
    let schema = vec![("Name".to_string(), "string".to_string())];
    let rows = vec![vec![json!("Alice")]];
    let plan_steps = vec![json!({
        "op": "select",
        "payload": ["name"]
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("Name").and_then(|v| v.as_str()), Some("Alice"));
}
