use polars::prelude::*;
use robin_sparkless_polars::{Column, functions::count};

#[test]
fn sol_011_vector_literal_count_remains_a_scalar_aggregation() {
    let input = df!("a" => [1i64, 2, 3, 4]).unwrap();
    let values = Series::new("values".into(), [Some("x"), None, Some("z")]);
    let literal = Column::from_expr(lit(values), None);
    let result = input
        .lazy()
        .select([count(&literal).into_expr().alias("n")])
        .collect()
        .unwrap();

    assert_eq!(
        result.shape(),
        (1, 1),
        "SOL-011: a vector literal is not a scalar constant; count must still aggregate to one row"
    );
    assert_eq!(result.column("n").unwrap().i64().unwrap().get(0), Some(2));
}
