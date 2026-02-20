//! Regression tests for issue #542 – create_map semantics / support (PySpark parity).
//!
//! PySpark: F.create_map(key1, val1, key2, val2, ...) builds a MapType column.

mod common;

use common::spark;
use robin_sparkless::functions::{col, create_map, lit_str};
use serde_json::json;

#[test]
fn issue_542_rust_api_create_map() {
    let spark = spark();
    let pl = polars::prelude::df![
        "val1" => &["a"],
        "val2" => &[1i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);
    let map_col = create_map(&[
        &lit_str("key1"),
        &col("val1"),
        &lit_str("key2"),
        &col("val2"),
    ])
    .unwrap();
    let out = df
        .with_column("map_col", &map_col)
        .unwrap()
        .select(vec!["map_col"])
        .unwrap();
    let rows = out.count().unwrap();
    assert_eq!(rows, 1, "create_map should produce one row");
    // Map is List(Struct{key,value}); get first key via map_keys then element_at
    let with_keys = out
        .with_column("keys", &robin_sparkless::map_keys(&col("map_col")))
        .unwrap();
    let keys_row = with_keys.collect_as_json_rows().unwrap();
    assert_eq!(keys_row.len(), 1, "one row");
}

#[test]
fn issue_542_plan_create_map_op() {
    let session = spark();
    let data = vec![vec![json!("a"), json!(1)]];
    let schema = vec![
        ("val1".to_string(), "string".to_string()),
        ("val2".to_string(), "bigint".to_string()),
    ];
    let plan = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "map_col",
            "expr": {
                "op": "createMap",
                "args": [
                    {"lit": "key1"},
                    {"col": "val1"},
                    {"lit": "key2"},
                    {"col": "val2"}
                ]
            }
        }
    })];
    let df = robin_sparkless::plan::execute_plan(&session, data, schema, &plan).unwrap();
    assert_eq!(
        df.count().unwrap(),
        1,
        "createMap plan should produce one row"
    );
}

#[test]
fn issue_542_plan_create_map_fn() {
    let session = spark();
    let data = vec![vec![json!("a"), json!(1)]];
    let schema = vec![
        ("val1".to_string(), "string".to_string()),
        ("val2".to_string(), "bigint".to_string()),
    ];
    let plan = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "map_col",
            "expr": {
                "fn": "create_map",
                "args": [
                    {"lit": "key1"},
                    {"col": "val1"},
                    {"lit": "key2"},
                    {"col": "val2"}
                ]
            }
        }
    })];
    let df = robin_sparkless::plan::execute_plan(&session, data, schema, &plan).unwrap();
    assert_eq!(
        df.count().unwrap(),
        1,
        "create_map fn plan should produce one row"
    );
}

/// #578: create_map() with no args must return empty map {} per row, not null (PySpark parity).
#[test]
fn issue_578_create_map_empty_returns_empty_object_not_null() {
    let spark = spark();
    let df = spark
        .create_dataframe_from_rows(
            vec![vec![json!(1)]],
            vec![("id".to_string(), "bigint".to_string())],
        )
        .unwrap();
    let empty_map = create_map(&[]).unwrap();
    let out = df.with_column("m", &empty_map).unwrap();
    let rows = out.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    let m = rows[0].get("m").expect("column m");
    assert!(
        !m.is_null(),
        "create_map() with no args must yield empty object not null (#578)"
    );
    let obj = m.as_object().expect("m must be JSON object (empty map)");
    assert!(obj.is_empty(), "empty create_map must be empty object");
}
