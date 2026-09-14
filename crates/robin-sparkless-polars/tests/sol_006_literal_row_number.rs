use polars::prelude::*;
use robin_sparkless_polars::Column as SparkColumn;

#[test]
fn sol_006_literal_order_produces_one_row_number_per_row() {
    let expr = SparkColumn::row_number_over(&[], &["<expr>".into()])
        .unwrap()
        .into_expr();
    let out = df!("a" => [1i64, 2, 3])
        .unwrap()
        .lazy()
        .select([col("a"), expr.alias("r")])
        .collect()
        .expect("SOL-006: a literal order key must not be resolved as a source column");
    assert_eq!(
        out.column("r")
            .unwrap()
            .u32()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(3)]
    );
}
