//! Tests for PR4: Cast/conversion in plan (Fixes #649, #635).
//!
//! Covers: plan select/withColumn with cast and try_cast expressions,
//! int->string, string->bigint (try_cast null on invalid).

use robin_sparkless::SparkSession;
use robin_sparkless::plan;
use serde_json::json;

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("issue_649_635_cast_conversion")
        .get_or_create()
}

/// #649: plan select with cast(col, "string") — int to string.
#[test]
fn plan_select_cast_int_to_string() {
    let spark = spark();
    let schema = vec![("n".to_string(), "bigint".to_string())];
    let rows = vec![vec![json!(10)], vec![json!(20)]];
    let plan_steps = vec![json!({
        "op": "select",
        "payload": [{"name": "n", "expr": {"fn": "cast", "args": [{"col": "n"}, {"lit": "string"}]}}]
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get("n").and_then(|v| v.as_str()), Some("10"));
    assert_eq!(out[1].get("n").and_then(|v| v.as_str()), Some("20"));
}

/// #635: plan withColumn try_cast(string, "bigint") — invalid string becomes null.
#[test]
fn plan_with_column_try_cast_string_to_bigint() {
    let spark = spark();
    let schema = vec![("x".to_string(), "string".to_string())];
    let rows = vec![
        vec![json!("1")],
        vec![json!("2")],
        vec![json!("not_a_number")],
    ];
    let plan_steps = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "y",
            "expr": {"fn": "try_cast", "args": [{"col": "x"}, {"lit": "bigint"}]}
        }
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].get("y").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(out[1].get("y").and_then(|v| v.as_i64()), Some(2));
    assert!(out[2].get("y").map(|v| v.is_null()).unwrap_or(false));
}

/// Plan select with cast and alias via {name, expr} items.
#[test]
fn plan_select_cast_with_alias() {
    let spark = spark();
    let schema = vec![
        ("a".to_string(), "bigint".to_string()),
        ("b".to_string(), "bigint".to_string()),
    ];
    let rows = vec![vec![json!(1), json!(2)], vec![json!(3), json!(4)]];
    let plan_steps = vec![json!({
        "op": "select",
        "payload": [
            {"name": "a_str", "expr": {"fn": "cast", "args": [{"col": "a"}, {"lit": "string"}]}},
            {"name": "b", "expr": {"col": "b"}}
        ]
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get("a_str").and_then(|v| v.as_str()), Some("1"));
    assert_eq!(out[0].get("b").and_then(|v| v.as_i64()), Some(2));
}
