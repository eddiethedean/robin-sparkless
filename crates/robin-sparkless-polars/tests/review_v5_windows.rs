use polars::prelude::*;
use robin_sparkless_polars::Column as SparkColumn;

#[test]
fn multi_key_rank_resets_for_each_partition() {
    let expr = SparkColumn::new("a".into())
        .rank_over(&["p"], &["a".into(), "-b".into()], false)
        .into_expr();
    let out = df!("p" => ["x","y","x","y"], "a" => [1i64, 1, 1, 1], "b" => ["a","z","z","a"])
        .unwrap()
        .lazy()
        .select([expr.alias("r")])
        .collect()
        .unwrap();
    let ranks = out
        .column("r")
        .unwrap()
        .u32()
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(ranks, vec![Some(2), Some(1), Some(1), Some(2)]);
}

#[test]
fn single_key_null_rank_resets_for_each_partition() {
    let expr = SparkColumn::new("a".into())
        .rank_over(&["p"], &["a".into()], false)
        .into_expr();
    let out = df!("p" => ["x","y","x","y","x"], "a" => [None,Some(1i64),Some(1),None,None])
        .unwrap()
        .lazy()
        .select([expr.alias("r")])
        .collect()
        .unwrap();
    let ranks = out
        .column("r")
        .unwrap()
        .u32()
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    assert_eq!(ranks, vec![Some(1), Some(2), Some(3), Some(1), Some(1)]);
}

#[test]
fn cume_dist_with_literal_order_is_one() {
    let expr = robin_sparkless_polars::functions::lit_i32(1)
        .cume_dist_over(&[], &["<expr>".into()], false)
        .into_expr();
    let out = df!("a" => [1i64, 2, 3])
        .unwrap()
        .lazy()
        .select([col("a"), expr.alias("r")])
        .collect()
        .unwrap();
    println!("literal cume_dist={out:?}");
    assert_eq!(
        out.column("r")
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![Some(1.0); 3]
    );
}

#[test]
fn ntile_with_literal_order_distributes_rows() {
    let expr = robin_sparkless_polars::functions::lit_i32(1)
        .ntile_over(3, &[], &["<expr>".into()], false)
        .into_expr();
    let out = df!("a" => [1i64, 2, 3])
        .unwrap()
        .lazy()
        .select([col("a"), expr.alias("r")])
        .collect()
        .unwrap();
    println!("literal ntile={out:?}");
    assert_eq!(
        out.column("r")
            .unwrap()
            .i32()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(3)]
    );
}
