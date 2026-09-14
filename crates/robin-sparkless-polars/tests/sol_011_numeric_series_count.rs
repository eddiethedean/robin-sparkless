use polars::prelude::*;
use robin_sparkless_polars::{Column, functions::count};

#[test]
fn sol_011_aliased_numeric_series_counts_its_non_null_values() {
    let input = df!("a" => [1i64, 2, 3, 4]).unwrap();
    let values = Series::new("values".into(), [Some(1i64), None, Some(3i64)]);
    let literal = Column::from_expr(lit(values).alias("values"), None);
    let result = input
        .lazy()
        .select([count(&literal).into_expr().alias("n")])
        .collect()
        .unwrap();

    assert_eq!(result.shape(), (1, 1));
    assert_eq!(
        result.column("n").unwrap().i64().unwrap().get(0),
        Some(2),
        "SOL-011: an aliased numeric Series literal must count its two non-null values, not all four input rows"
    );
}
