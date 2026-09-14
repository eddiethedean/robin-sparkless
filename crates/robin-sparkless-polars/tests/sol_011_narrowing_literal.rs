use polars::prelude::*;
use robin_sparkless_polars::{Column, functions::count};

#[test]
fn sol_011_unknown_integer_narrowing_preserves_evaluated_nullness() {
    let input = df!("a" => [1i64, 2, 3, 4]).unwrap();
    let narrowed = Column::from_expr(lit(300i64).cast(DataType::Int8), None);
    let evaluated = input
        .clone()
        .lazy()
        .select([narrowed.expr().clone().alias("value")])
        .collect()
        .unwrap();
    assert_eq!(
        evaluated.column("value").unwrap().null_count(),
        evaluated.height()
    );

    let result = input
        .lazy()
        .select([count(&narrowed).into_expr().alias("n")])
        .collect()
        .unwrap();
    assert_eq!(
        result.column("n").unwrap().i64().unwrap().get(0),
        Some(0),
        "SOL-011: an unknown integer's narrowing cast can produce NULL; count must preserve that evaluation"
    );
}

#[test]
fn sol_011_unknown_integer_narrowing_never_serializes_uncast_value() {
    let narrowed = Column::from_expr(lit(300i64).cast(DataType::Int8), None);
    let evaluated = df!("a" => [1i64])
        .unwrap()
        .lazy()
        .select([narrowed.expr().clone().alias("value")])
        .collect()
        .unwrap();
    assert_eq!(evaluated.column("value").unwrap().null_count(), 1);
    // Declining classification is allowed; any emitted literal must preserve
    // the executable expression's value, including a non-strict cast's NULL.
    if let Some(json) = narrowed.literal_as_json_string() {
        assert_eq!(
            json, "null",
            "SOL-011: serialization must not bypass a narrowing cast"
        );
    }
}
