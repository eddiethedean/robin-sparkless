//! Tests for PR3: create_dataframe_from_rows and JSON (Fixes #633, #634, #647).
//!
//! Covers: empty schema + non-empty rows (infer schema from JSON rows),
//! create_dataframe_from_rows round-trip with collect_as_json_rows,
//! and schema_from_json + create_dataframe_from_rows workflow.

use robin_sparkless::SparkSession;
use robin_sparkless::schema::schema_from_json;
use robin_sparkless::{plan, schema::StructType};
use serde_json::json;

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("issue_633_634_647_create_dataframe_json")
        .get_or_create()
}

/// #633/#624-style: empty schema with non-empty rows infers schema from JSON row values.
#[test]
fn create_dataframe_from_rows_empty_schema_infers_from_json_rows() {
    let spark = spark();
    let schema: Vec<(String, String)> = vec![];
    let rows: Vec<Vec<serde_json::Value>> = vec![
        vec![json!("hello"), json!(42), json!(true)],
        vec![json!("world"), json!(0), json!(false)],
    ];
    let df = spark
        .create_dataframe_from_rows_engine(rows, schema)
        .expect("empty schema + non-empty rows should infer schema");
    assert_eq!(df.count_engine().unwrap(), 2);
    let names = df.columns_engine().unwrap();
    assert_eq!(names, vec!["c0", "c1", "c2"]);
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].get("c0").and_then(|v| v.as_str()), Some("hello"));
    assert_eq!(out[0].get("c1").and_then(|v| v.as_i64()), Some(42));
    assert_eq!(out[0].get("c2").and_then(|v| v.as_bool()), Some(true));
}

/// #634: create_dataframe_from_rows then collect as JSON rows (round-trip).
#[test]
fn create_dataframe_from_rows_collect_as_json_roundtrip() {
    let spark = spark();
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("label".to_string(), "string".to_string()),
    ];
    let rows: Vec<Vec<serde_json::Value>> =
        vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
    let df = spark
        .create_dataframe_from_rows_engine(rows.clone(), schema)
        .unwrap();
    let collected = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0].get("id").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(
        collected[0].get("label").and_then(|v| v.as_str()),
        Some("a")
    );
    assert_eq!(collected[1].get("id").and_then(|v| v.as_i64()), Some(2));
    assert_eq!(
        collected[1].get("label").and_then(|v| v.as_str()),
        Some("b")
    );
}

/// #647: execute_plan with data from create_dataframe_from_rows-style JSON rows.
#[test]
fn execute_plan_with_json_rows_data() {
    let spark = spark();
    let schema = vec![
        ("x".to_string(), "bigint".to_string()),
        ("y".to_string(), "string".to_string()),
    ];
    let rows: Vec<Vec<serde_json::Value>> =
        vec![vec![json!(10), json!("p")], vec![json!(20), json!("q")]];
    let plan = vec![
        json!({"op": "filter", "payload": {"op": "gt", "left": {"col": "x"}, "right": {"lit": 15}}}),
        json!({"op": "select", "payload": ["y", "x"]}),
    ];
    let df = plan::execute_plan(&spark, rows, schema, &plan).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("y").and_then(|v| v.as_str()), Some("q"));
    assert_eq!(out[0].get("x").and_then(|v| v.as_i64()), Some(20));
}

/// schema_from_json then create_dataframe_from_rows (embedding workflow).
#[test]
fn schema_from_json_and_create_dataframe_from_rows() {
    // StructType JSON from host (matches StructType serialization: fields with name, data_type, nullable).
    let schema_json = r#"{"fields":[{"name":"id","data_type":"Long","nullable":true},{"name":"name","data_type":"String","nullable":true}]}"#;
    let _struct_type: StructType =
        schema_from_json(schema_json).expect("schema_from_json should parse");
    let spark = spark();
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let rows: Vec<Vec<serde_json::Value>> =
        vec![vec![json!(1), json!("alice")], vec![json!(2), json!("bob")]];
    let df = spark
        .create_dataframe_from_rows_engine(rows, schema)
        .unwrap();
    assert_eq!(df.count_engine().unwrap(), 2);
}
