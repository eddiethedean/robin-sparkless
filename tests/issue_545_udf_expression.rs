//! Regression tests for issue #545 – UDF expression not supported (PySpark parity).
//!
//! Plan interpreter previously reported "unsupported expression op: udf".
//! We now accept {"op": "udf", "udf"|"name": "name", "args": [<expr>, ...]}.

mod common;

use common::spark;
use robin_sparkless::plan;
use serde_json::json;

#[test]
fn issue_545_plan_udf_op_rust_udf() {
    let session = spark();
    session
        .register_udf("inc", |cols: &[polars::prelude::Series]| Ok(&cols[0] + 1))
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
