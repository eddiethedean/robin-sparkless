use robin_sparkless_polars::{SparkSession, plan::execute_plan};
use serde_json::json;

#[test]
fn sol_009_plan_row_number_uses_the_descending_string_tiebreaker() {
    let session = SparkSession::builder().get_or_create();
    let frame = execute_plan(
        &session,
        vec![vec![json!(1), json!("a")], vec![json!(1), json!("z")]],
        vec![("a".into(), "bigint".into()), ("b".into(), "string".into())],
        &[json!({"op": "select", "payload": [{"name": "r", "expr": {
            "fn": "row_number", "args": [], "window": {"order_by": ["a", "-b"]}
        }}]})],
    )
    .unwrap();
    let actual = frame.collect().unwrap();
    assert_eq!(
        actual
            .column("r")
            .unwrap()
            .u32()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![Some(2), Some(1)],
        "SOL-009: every encoded order key and direction must affect plan row numbers"
    );
}
