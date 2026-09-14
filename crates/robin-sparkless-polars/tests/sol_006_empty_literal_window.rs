use polars::prelude::*;
use robin_sparkless_polars::Column as SparkColumn;

#[test]
fn sol_006_literal_row_number_does_not_create_rows_for_empty_input() {
    let expr = SparkColumn::row_number_over(&[], &["<expr>".into()])
        .unwrap()
        .into_expr();
    let frame =
        DataFrame::new_infer_height(vec![Series::new("a".into(), Vec::<i64>::new()).into()])
            .unwrap();
    let out = frame.lazy().select([expr.alias("r")]).collect().expect(
        "SOL-006: literal row_number must execute on empty inputs without a height mismatch",
    );
    assert_eq!(
        out.height(),
        0,
        "SOL-006: literal ordering must preserve empty-input cardinality"
    );
}
