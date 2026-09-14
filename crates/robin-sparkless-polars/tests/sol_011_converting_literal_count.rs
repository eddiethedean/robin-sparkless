use polars::prelude::*;
use robin_sparkless_polars::{Column, functions::count};

#[test]
fn sol_011_non_null_converting_literal_counts_each_input_row() {
    let input = df!("a" => [1i64, 2, 3, 4]).unwrap();
    let converted = Column::from_expr(lit(1.1f64).cast(DataType::Float32), None);
    let evaluated = input
        .clone()
        .lazy()
        .select([converted.expr().clone().alias("value")])
        .collect()
        .unwrap();
    let value = evaluated.column("value").unwrap().f32().unwrap().get(0);
    assert_eq!(value, Some(1.1f32));
    assert_ne!(f64::from(value.unwrap()), 1.1f64);

    let result = input
        .lazy()
        .select([count(&converted).into_expr().alias("n")])
        .collect()
        .unwrap();
    assert_eq!(
        result.column("n").unwrap().i64().unwrap().get(0),
        Some(4),
        "SOL-011: evaluating a non-null converting literal must preserve input-row count semantics"
    );
}
