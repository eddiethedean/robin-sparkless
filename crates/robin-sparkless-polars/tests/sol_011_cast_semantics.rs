use polars::prelude::*;
use robin_sparkless_polars::DataFrame;

#[test]
fn sol_011_comparison_preserves_explicit_literal_cast_value() {
    let frame = DataFrame::from_polars(df!("a" => [1i64, 2]).unwrap());
    let predicate = col("a").eq(lit(1.9f64).cast(DataType::Int64));
    let result = frame.filter(predicate).unwrap().collect().unwrap();
    assert_eq!(
        result
            .column("a")
            .unwrap()
            .i64()
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![Some(1)],
        "SOL-011: coercion must retain an explicit literal cast's converted value"
    );
}
