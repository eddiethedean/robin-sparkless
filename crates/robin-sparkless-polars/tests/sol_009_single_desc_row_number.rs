use polars::prelude::*;
use robin_sparkless_polars::Column as SparkColumn;

#[test]
fn sol_009_single_descending_key_is_honored() {
    let expr = SparkColumn::row_number_over(&[], &["-a".into()])
        .unwrap()
        .into_expr();
    let out = df!("a" => [1i64, 2, 3])
        .unwrap()
        .lazy()
        .select([col("a"), expr.alias("r")])
        .collect()
        .unwrap();
    assert_eq!(
        out.column("r")
            .unwrap()
            .u32()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![Some(3), Some(2), Some(1)],
        "SOL-009: row_number must retain the encoded direction for a single key"
    );
}
