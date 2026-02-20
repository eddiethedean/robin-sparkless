//! Tests for PR1: Column/select/filter parity (Fixes #644, #645, #646).
//!
//! - #644: cannot convert to Column — plan expr_from_value accepts bare string as column ref.
//! - #645: select expects Column or str — select_items accepts mix of names and exprs.
//! - #646: filter predicate must be Boolean — clearer error when predicate is non-Boolean.

use robin_sparkless::SparkSession;
use robin_sparkless::dataframe::SelectItem;
use robin_sparkless::functions::col;
use robin_sparkless::plan;
use serde_json::Value as JsonValue;
use serde_json::json;

#[test]
fn test_plan_filter_with_bare_string_column_ref() {
    // #644: plan "filter" with payload as bare column name (string) should be accepted as column ref.
    let spark = SparkSession::builder().get_or_create();
    let rows = vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let plan_steps = vec![
        json!({"op": "filter", "payload": "id"}), // bare string = column ref (non-boolean will fail at collect or we coerce)
    ];
    // Note: filter with just column "id" (integer) is not boolean; Polars may error at collect.
    // Here we only check that the plan parser accepts bare string (no "expression must be a JSON object").
    let result = plan::execute_plan(&spark, rows, schema, &plan_steps);
    // Either succeeds (if we coerce) or fails with a type/boolean error, not "expression must be a JSON object".
    if let Err(e) = &result {
        let msg = e.to_string();
        assert!(
            !msg.contains("expression must be a JSON object"),
            "plan should accept bare string as column ref, got: {}",
            msg
        );
    }
}

#[test]
fn test_plan_filter_with_object_column_ref() {
    // Filter with proper boolean expression.
    let spark = SparkSession::builder().get_or_create();
    let rows = vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
    let schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let plan_steps = vec![json!({
        "op": "filter",
        "payload": {"op": "gt", "left": {"col": "id"}, "right": {"lit": 1}}
    })];
    let df = plan::execute_plan(&spark, rows, schema, &plan_steps).unwrap();
    let out = df.collect_as_json_rows_engine().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get("id").and_then(|v| v.as_i64()), Some(2));
}

#[test]
fn test_select_items_mixed_names_and_exprs() {
    // #645: select with mix of column names and expressions.
    let spark = SparkSession::builder().get_or_create();
    let rows = vec![vec![json!(1), json!(10)], vec![json!(2), json!(20)]];
    let schema = vec![
        ("a".to_string(), "bigint".to_string()),
        ("b".to_string(), "bigint".to_string()),
    ];
    let df = spark
        .create_dataframe_from_rows_engine(rows, schema)
        .unwrap();
    let items = vec![
        SelectItem::ColumnName("a"),
        SelectItem::Expr(col("b").alias("b_doubled").into_expr()),
    ];
    let out = df.select_items(items).unwrap();
    let names = out.columns_engine().unwrap();
    assert_eq!(names, vec!["a", "b_doubled"]);
    let rows = out.collect_as_json_rows_engine().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("a").and_then(|v: &JsonValue| v.as_i64()),
        Some(1)
    );
    assert_eq!(
        rows[0]
            .get("b_doubled")
            .and_then(|v: &JsonValue| v.as_i64()),
        Some(10)
    );
}
