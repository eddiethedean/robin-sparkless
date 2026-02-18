//! Regression tests for issue #552 – Inner/left join return correct row counts (PySpark parity).

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

/// Emp 4 rows (dept_id 10,20,10,30), dept 3 rows (10,20,40). Inner on dept_id -> 3 rows, left -> 4 rows.
#[test]
fn issue_552_plan_join_inner_and_left_row_counts() {
    let session = spark();
    // Left: emp (id, name, dept_id)
    let emp_data = vec![
        vec![json!(1), json!("Alice"), json!(10)],
        vec![json!(2), json!("Bob"), json!(20)],
        vec![json!(3), json!("Charlie"), json!(10)],
        vec![json!(4), json!("David"), json!(30)],
    ];
    let emp_schema = vec![
        ("id".to_string(), "bigint".to_string()),
        ("name".to_string(), "string".to_string()),
        ("dept_id".to_string(), "bigint".to_string()),
    ];
    // Right: dept (dept_id, name) — embedded in plan payload
    let _dept_data = vec![
        vec![json!(10), json!("IT")],
        vec![json!(20), json!("HR")],
        vec![json!(40), json!("Finance")],
    ];
    let dept_schema_plan = vec![
        json!({"name": "dept_id", "type": "long"}),
        json!({"name": "name", "type": "string"}),
    ];

    let plan_inner = vec![json!({
        "op": "join",
        "payload": {
            "other_data": [[10, "IT"], [20, "HR"], [40, "Finance"]],
            "other_schema": dept_schema_plan,
            "on": ["dept_id"],
            "how": "inner"
        }
    })];
    let df_inner =
        plan::execute_plan(&session, emp_data.clone(), emp_schema.clone(), &plan_inner).unwrap();
    let rows_inner = df_inner.collect_as_json_rows().unwrap();
    assert_eq!(
        rows_inner.len(),
        3,
        "inner join must return 3 rows (dept_id 10,20,10)"
    );

    let plan_left = vec![json!({
        "op": "join",
        "payload": {
            "other_data": [[10, "IT"], [20, "HR"], [40, "Finance"]],
            "other_schema": dept_schema_plan,
            "on": ["dept_id"],
            "how": "left"
        }
    })];
    let df_left = plan::execute_plan(&session, emp_data, emp_schema, &plan_left).unwrap();
    let rows_left = df_left.collect_as_json_rows().unwrap();
    assert_eq!(rows_left.len(), 4, "left join must return 4 rows");
}
