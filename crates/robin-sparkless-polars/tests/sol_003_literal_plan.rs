use robin_sparkless_polars::{SparkSession, plan::execute_plan};
use serde_json::json;

#[test]
fn sol_003_plan_fractional_literal_has_the_runtime_schema() {
    let session = SparkSession::builder().get_or_create();
    let frame = execute_plan(
        &session,
        vec![vec![json!(1)], vec![json!(2)]],
        vec![("a".into(), "bigint".into())],
        &[json!({"op": "select", "payload": [{"name": "r", "expr": {
            "fn": "try_add", "args": [{"col": "a"}, {"lit": 0.5}]
        }}]})],
    )
    .unwrap();
    let planned = frame.get_column_dtype("r").unwrap();
    let actual = frame.collect().unwrap();
    assert_eq!(
        actual
            .column("r")
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![Some(1.5), Some(2.5)]
    );
    assert_eq!(
        &planned,
        actual.column("r").unwrap().dtype(),
        "SOL-003: planned and collected arithmetic schemas must agree"
    );
}
