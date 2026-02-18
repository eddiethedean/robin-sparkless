//! Regression tests for issue #541 – withField struct update (PySpark parity).
//!
//! PySpark: F.col("struct_col").withField("field_name", value) adds or replaces a struct field.

mod common;

use common::spark;
use robin_sparkless::{col, lit_i64, named_struct};
use serde_json::json;

#[test]
fn issue_541_rust_api_with_field() {
    let spark = spark();
    let pl = polars::prelude::df![
        "id" => &["1"],
        "value_1" => &[10i64],
        "value_2" => &["x"],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);
    let struct_df = df
        .select_exprs(vec![
            col("id").clone().into_expr(),
            named_struct(&[("value_1", &col("value_1")), ("value_2", &col("value_2"))])
                .alias("my_struct")
                .into_expr(),
        ])
        .unwrap();
    let updated = struct_df
        .with_column(
            "my_struct",
            &col("my_struct").with_field("value_1", &lit_i64(99)),
        )
        .unwrap();
    // Verify by selecting the struct field (collect_as_json_rows does not serialize struct as object yet).
    let extracted = updated
        .with_column("value_1_out", &col("my_struct").get_field("value_1"))
        .unwrap();
    let rows = extracted.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("value_1_out").and_then(|v| v.as_i64()),
        Some(99),
        "with_field should replace value_1 with 99"
    );
}

#[test]
fn issue_541_plan_with_field() {
    let session = spark();
    let data = vec![vec![json!("1"), json!({"value_1": 10, "value_2": "x"})]];
    let schema = vec![
        ("id".to_string(), "string".to_string()),
        (
            "my_struct".to_string(),
            "struct<value_1:bigint,value_2:string>".to_string(),
        ),
    ];
    let plan = vec![
        json!({
            "op": "withColumn",
            "payload": {
                "name": "my_struct",
                "expr": {
                    "fn": "withField",
                    "args": [
                        {"col": "my_struct"},
                        {"lit": "value_1"},
                        {"lit": 99}
                    ]
                }
            }
        }),
        // Add column that extracts the updated field so we can assert (struct not serialized in JSON rows).
        json!({
            "op": "withColumn",
            "payload": {
                "name": "value_1_out",
                "expr": {"fn": "get_field", "args": [{"col": "my_struct"}, {"lit": "value_1"}]}
            }
        }),
    ];
    let df = robin_sparkless::plan::execute_plan(&session, data, schema, &plan).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("value_1_out").and_then(|v| v.as_i64()),
        Some(99),
        "plan withField should replace value_1 with 99"
    );
}
