//! Regression tests for issue #560 – first() and orderBy semantics (PySpark parity).
//! groupBy + first/last aggregations + orderBy on result.

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_560_groupby_first_last_then_orderby() {
    let session = spark();
    let data = vec![
        vec![json!("Sales"), json!("Alice"), json!(1000)],
        vec![json!("Sales"), json!("Bob"), json!(1500)],
        vec![json!("Eng"), json!("Charlie"), json!(2000)],
        vec![json!("Eng"), json!("Dave"), json!(2500)],
    ];
    let schema = vec![
        ("dept".to_string(), "string".to_string()),
        ("name".to_string(), "string".to_string()),
        ("salary".to_string(), "bigint".to_string()),
    ];
    let plan_ops = vec![
        json!({
            "op": "groupBy",
            "payload": {
                "group_by": ["dept"],
                "aggs": [
                    {"agg": "first", "column": "name", "alias": "first(name)"},
                    {"agg": "last", "column": "name", "alias": "last(name)"}
                ]
            }
        }),
        json!({
            "op": "orderBy",
            "payload": { "columns": ["dept"], "ascending": [true] }
        }),
    ];
    let df = plan::execute_plan(&session, data, schema, &plan_ops).unwrap();
    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2, "2 groups: Eng, Sales");
    // orderBy dept asc -> Eng first, then Sales
    let first_row = &rows[0];
    let second_row = &rows[1];
    assert_eq!(first_row.get("dept").and_then(|v| v.as_str()), Some("Eng"));
    assert_eq!(
        second_row.get("dept").and_then(|v| v.as_str()),
        Some("Sales")
    );
    assert!(first_row.contains_key("first(name)"));
    assert!(first_row.contains_key("last(name)"));
}
