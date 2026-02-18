//! Regression tests for issue #555 – Case-insensitive column resolution (PySpark parity).
//! F.col("age") should resolve to column "Age" when schema has "Age".

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_555_select_col_lowercase_resolves_to_mixed_case_schema() {
    let session = spark();
    // Schema has "Name" and "Age"; select using lowercase "age" should resolve to "Age"
    let data = vec![vec![json!("Alice"), json!(25)]];
    let schema = vec![
        ("Name".to_string(), "string".to_string()),
        ("Age".to_string(), "bigint".to_string()),
    ];
    let plan_ops = vec![json!({
        "op": "select",
        "payload": {
            "columns": [{"name": "age_out", "expr": {"col": "age"}}]
        }
    })];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("age_out").and_then(|v| v.as_i64()),
        Some(25),
        "col(\"age\") must resolve to column \"Age\""
    );
}
