//! Tests for PR5: Plan between, power, groupBy (Fixes #640, #641).
//!
//! Covers: filter with between(lower, upper), withColumn with power/**,
//! groupBy with aggs (sum, count).

use robin_sparkless::plan;
use robin_sparkless::SparkSession;
use serde_json::json;

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("issue_640_641_between_power_groupby")
        .get_or_create()
}

/// #640: plan filter with between(col, lower, upper).
#[test]
fn plan_filter_between() {
    let spark = spark();
    let schema = vec![
        ("a".to_string(), "bigint".to_string()),
        ("b".to_string(), "bigint".to_string()),
    ];
    let rows = vec![
        vec![json!(2), json!(10)],
        vec![json!(5), json!(20)],
        vec![json!(8), json!(30)],
    ];
    let plan_steps = vec![json!({
        "op": "filter",
        "payload": {"op": "between", "left": {"col": "a"}, "lower": {"lit": 3}, "upper": {"lit": 7}}
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("a").and_then(|v| v.as_i64()), Some(5));
    assert_eq!(out[0].get("b").and_then(|v| v.as_i64()), Some(20));
}

/// #641: plan withColumn with power (op **).
#[test]
fn plan_with_column_power_op() {
    let spark = spark();
    let schema = vec![("a".to_string(), "bigint".to_string())];
    let rows = vec![vec![json!(5)], vec![json!(10)]];
    let plan_steps = vec![json!({
        "op": "withColumn",
        "payload": {"name": "squared", "expr": {"op": "**", "left": {"col": "a"}, "right": {"lit": 2}}}
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get("squared").and_then(|v| v.as_f64()), Some(25.0));
    assert_eq!(out[1].get("squared").and_then(|v| v.as_f64()), Some(100.0));
}

/// Plan groupBy with sum agg.
#[test]
fn plan_groupby_sum() {
    let spark = spark();
    let schema = vec![
        ("grp".to_string(), "string".to_string()),
        ("n".to_string(), "bigint".to_string()),
    ];
    let rows = vec![
        vec![json!("G1"), json!(10)],
        vec![json!("G1"), json!(20)],
        vec![json!("G2"), json!(30)],
    ];
    let plan_steps = vec![json!({
        "op": "groupBy",
        "payload": {
            "group_by": ["grp"],
            "aggs": [{"agg": "sum", "column": "n", "alias": "total"}]
        }
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2);
    let g1 = out.iter().find(|r| r.get("grp").and_then(|v| v.as_str()) == Some("G1")).unwrap();
    let g2 = out.iter().find(|r| r.get("grp").and_then(|v| v.as_str()) == Some("G2")).unwrap();
    assert_eq!(g1.get("total").and_then(|v| v.as_i64()), Some(30));
    assert_eq!(g2.get("total").and_then(|v| v.as_i64()), Some(30));
}
