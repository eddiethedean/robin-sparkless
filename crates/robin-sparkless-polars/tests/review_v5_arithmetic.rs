use polars::prelude::*;
use robin_sparkless_polars::ansi;

#[test]
fn float32_subtraction_schema_matches_values() {
    let mut plan = df!("a" => [1.25f32, 2.5], "b" => [0.5f32, 0.75])
        .unwrap()
        .lazy()
        .select([ansi::sub_expr(col("a"), col("b")).alias("r")]);
    let schema = plan.collect_schema().unwrap();
    let out = plan.collect().unwrap();
    println!("planned={schema:?}; actual={out:?}");
    assert_eq!(schema.get("r").unwrap(), out.column("r").unwrap().dtype());
}

#[test]
fn integer_plus_fractional_literal_preserves_fraction() {
    let mut plan = df!("a" => [1i64, 2])
        .unwrap()
        .lazy()
        .select([ansi::add_expr(
            col("a"),
            robin_sparkless_polars::functions::lit_f64(0.5).into_expr(),
        )
        .alias("r")]);
    let schema = plan.collect_schema().unwrap();
    let out = plan.collect().unwrap();
    println!("planned={schema:?}; actual={out:?}");
    assert_eq!(schema.get("r").unwrap(), out.column("r").unwrap().dtype());
}

#[test]
fn float32_subtraction_can_join_float32_keys() {
    let left = df!("a" => [1.25f32, 2.5], "b" => [0.5f32, 0.75])
        .unwrap()
        .lazy()
        .select([ansi::sub_expr(col("a"), col("b")).alias("r")]);
    let right = df!("r" => [0.75f32, 1.75]).unwrap().lazy();
    let out = left
        .join(
            right,
            [col("r")],
            [col("r")],
            JoinArgs::new(JoinType::Inner),
        )
        .collect();
    println!("join={out:?}");
    assert!(out.is_ok());
}
