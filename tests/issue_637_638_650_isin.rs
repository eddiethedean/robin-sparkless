//! Tests for PR2: isin plan op/fn and filter behavior (Fixes #637, #638, #650).
//!
//! Covers: plan "filter" with isin (op and fn forms), empty list → 0 rows,
//! Int64 column isin(lit values), string column isin(lit values).

use robin_sparkless::SparkSession;
use robin_sparkless::plan;
use serde_json::json;

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("issue_637_638_650_isin")
        .get_or_create()
}

#[test]
fn plan_filter_isin_op_int64() {
    let spark = spark();
    let schema = vec![("value".to_string(), "bigint".to_string())];
    let rows = vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]];
    let plan = vec![json!({
        "op": "filter",
        "payload": {"op": "isin", "left": {"col": "value"}, "right": {"lit": [1, 3]}}
    })];
    let result = plan::execute_plan(&spark, rows, schema, &plan).unwrap();
    let rows_out = result.collect_as_json_rows_engine().unwrap();
    assert_eq!(rows_out.len(), 2, "value.isin(1, 3) should match 2 rows");
    let values: std::collections::HashSet<i64> = rows_out
        .iter()
        .map(|r| r.get("value").and_then(|v| v.as_i64()).unwrap())
        .collect();
    assert_eq!(values, [1, 3].into_iter().collect());
}

#[test]
fn plan_filter_isin_fn_int64() {
    let spark = spark();
    let schema = vec![("id".to_string(), "bigint".to_string())];
    let rows = vec![vec![json!(10)], vec![json!(20)], vec![json!(30)]];
    let plan = vec![json!({
        "op": "filter",
        "payload": {"fn": "isin", "args": [{"col": "id"}, {"lit": 10}, {"lit": 30}]}
    })];
    let result = plan::execute_plan(&spark, rows, schema, &plan).unwrap();
    let rows_out = result.collect_as_json_rows_engine().unwrap();
    assert_eq!(rows_out.len(), 2);
    let ids: std::collections::HashSet<i64> = rows_out
        .iter()
        .map(|r| r.get("id").and_then(|v| v.as_i64()).unwrap())
        .collect();
    assert_eq!(ids, [10, 30].into_iter().collect());
}

#[test]
fn plan_filter_isin_empty_yields_zero_rows() {
    let spark = spark();
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let rows = vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
    let plan = vec![json!({
        "op": "filter",
        "payload": {"op": "isin", "left": {"col": "id"}, "right": {"lit": []}}
    })];
    let result = plan::execute_plan(&spark, rows, schema, &plan).unwrap();
    let rows_out = result.collect_as_json_rows_engine().unwrap();
    assert_eq!(rows_out.len(), 0, "col.isin([]) should match 0 rows");
}

#[test]
fn plan_filter_isin_op_string() {
    let spark = spark();
    let schema = vec![("name".to_string(), "string".to_string())];
    let rows = vec![
        vec![json!("alice")],
        vec![json!("bob")],
        vec![json!("carol")],
    ];
    let plan = vec![json!({
        "op": "filter",
        "payload": {"op": "isin", "left": {"col": "name"}, "right": {"lit": ["alice", "carol"]}}
    })];
    let result = plan::execute_plan(&spark, rows, schema, &plan).unwrap();
    let rows_out = result.collect_as_json_rows_engine().unwrap();
    assert_eq!(rows_out.len(), 2);
    let names: std::collections::HashSet<String> = rows_out
        .iter()
        .filter_map(|r| r.get("name").and_then(|v| v.as_str()).map(String::from))
        .collect();
    assert_eq!(
        names,
        ["alice".into(), "carol".into()].into_iter().collect()
    );
}
