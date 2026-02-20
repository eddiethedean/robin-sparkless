//! Union, unionByName, UDF, explode, property coercion.
//!
//! Merged from: issue_551, issue_559, udf_core, issue_545, issue_554, property_type_coercion.

mod common;

use common::spark;
use polars::prelude::{PolarsError, Series, df};
use rand::Rng;
use robin_sparkless::DataFrame;
use robin_sparkless::functions::{col, lit_i64};
use robin_sparkless::plan;
use serde_json::json;

// ---------- issue_551 ----------

#[test]
fn issue_551_union_string_and_int64_coerces_to_string() {
    let session = spark();
    let data = vec![vec![json!("a")]];
    let schema = vec![("x".to_string(), "string".to_string())];
    let plan_ops = vec![json!({
        "op": "union",
        "payload": {
            "other_data": [[1]],
            "other_schema": [{"name": "x", "type": "long"}]
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("x").and_then(|v| v.as_str()), Some("a"));
    assert_eq!(rows[1].get("x").and_then(|v| v.as_str()), Some("1"));
}

// ---------- issue_559 ----------

#[test]
fn issue_559_union_by_name_diamond_preserves_duplicate_rows() {
    let session = spark();
    let data = vec![vec![json!(1), json!("x")], vec![json!(2), json!("y")]];
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let plan_ops = vec![
        json!({
            "op": "unionByName",
            "payload": {
                "other_data": [[3, "z"]],
                "other_schema": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}]
            }
        }),
        json!({
            "op": "unionByName",
            "payload": {
                "other_data": [[1, "x"], [2, "y"]],
                "other_schema": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}]
            }
        }),
    ];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(
        rows.len(),
        5,
        "diamond (A u B) u A must preserve duplicates: 2 + 1 + 2 = 5 rows"
    );
}

// ---------- udf_core ----------

#[test]
fn call_udf_missing_name_returns_error() {
    let spark = spark();
    let pl = df!["id" => &[1i64, 2i64]].unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);
    let _ = df;

    let col_id = col("id");
    let result = robin_sparkless::call_udf("missing_udf", &[col_id]);
    assert!(
        result.is_err(),
        "calling unknown UDF via call_udf should return an error"
    );
}

#[test]
fn register_and_call_rust_udf_success() {
    let spark = spark();
    let pl = df![
        "a" => &[1i64, 2i64, 3i64],
        "b" => &[10i64, 20i64, 30i64],
    ]
    .unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);

    spark
        .register_udf("add_ab", |cols: &[Series]| -> Result<Series, PolarsError> {
            let a = &cols[0];
            let b = &cols[1];
            a + b
        })
        .expect("register_udf should succeed");

    let c_add = robin_sparkless::call_udf("add_ab", &[col("a"), col("b")])
        .expect("registered UDF should resolve");
    let df2 = df
        .with_column_expr("c", c_add.into_expr())
        .expect("with_column_expr should succeed");

    let rows = df2.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["c"].as_i64().unwrap(), 11);
    assert_eq!(rows[1]["c"].as_i64().unwrap(), 22);
    assert_eq!(rows[2]["c"].as_i64().unwrap(), 33);
}

// ---------- issue_545 ----------

#[test]
fn issue_545_plan_udf_op_rust_udf() {
    let session = spark();
    session
        .register_udf("inc", |cols: &[Series]| Ok(&cols[0] + 1))
        .expect("register_udf should succeed");

    let data = vec![vec![json!(1)], vec![json!(2)]];
    let schema = vec![("x".to_string(), "bigint".to_string())];
    let plan_ops = vec![json!({
        "op": "withColumn",
        "payload": {
            "name": "x_plus_one",
            "expr": {
                "op": "udf",
                "udf": "inc",
                "args": [{"col": "x"}]
            }
        }
    })];

    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("x_plus_one").and_then(|v| v.as_i64()),
        Some(2),
        "op 'udf' with Rust UDF should be applied"
    );
    assert_eq!(rows[1].get("x_plus_one").and_then(|v| v.as_i64()), Some(3));
}

// ---------- issue_554 ----------

#[test]
fn issue_554_plan_op_explode() {
    let session = spark();
    let data = vec![vec![json!([1, 2, 3])], vec![json!([10, 20])]];
    let schema = vec![("arr".to_string(), "array".to_string())];
    let plan_ops = vec![json!({
        "op": "select",
        "payload": {
            "columns": [
                {
                    "name": "x",
                    "expr": {"op": "explode", "args": [{"col": "arr"}]}
                }
            ]
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 5);
    let x_vals: Vec<i64> = rows
        .iter()
        .filter_map(|r| r.get("x").and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(x_vals, vec![1, 2, 3, 10, 20]);
}

// ---------- property_type_coercion ----------

#[test]
fn property_string_numeric_comparison_behaves_consistently() {
    let spark = spark();
    let mut rng = rand::thread_rng();

    let mut ints = Vec::new();
    let mut strs = Vec::new();
    for _ in 0..10 {
        let v: i64 = rng.gen_range(-10..=10);
        ints.push(v);
        strs.push(v.to_string());
    }

    let pl = df![
        "num" => &ints,
        "num_str" => &strs,
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let cmp = df
        .filter(col("num_str").gt(lit_i64(0).into_expr()).into_expr())
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    for row in cmp {
        let n = row["num"].as_i64().unwrap();
        assert!(n > 0);
    }
}
