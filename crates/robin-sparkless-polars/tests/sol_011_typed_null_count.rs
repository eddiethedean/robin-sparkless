use polars::prelude::*;
use robin_sparkless_polars::{Column, functions::count};

#[test]
fn sol_011_count_typed_null_excludes_every_row() {
    let input = df!("a" => [1i64, 2, 3, 4]).unwrap();
    let null = Column::lit_null("bigint").unwrap();
    let result = input
        .lazy()
        .select([count(&null).into_expr().alias("n")])
        .collect()
        .unwrap();
    assert_eq!(
        result.column("n").unwrap().i64().unwrap().get(0),
        Some(0),
        "SOL-011: a typed NULL is still NULL; count must not count its rows"
    );
}

#[test]
fn sol_011_literal_consumer_preserves_converted_value() {
    let converted = Column::from_expr(lit(1.9f64).cast(DataType::Int64), None);
    // Declining literal classification is permitted. Emitting a value must not
    // bypass the actual cast and silently supply a different argument value.
    if let Some(json) = converted.literal_as_json_string() {
        assert_eq!(
            json, "1",
            "SOL-011: serialized literal arguments must retain their evaluated cast value"
        );
    }
}
