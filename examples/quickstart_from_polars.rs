//! Run QUICKSTART Basic Usage: from_polars, columns(), filter, show.
use polars::prelude::*;
use robin_sparkless::{DataFrame, col, lit_i64};

fn main() -> PolarsResult<()> {
    let polars_df = df!(
        "id" => &[1, 2, 3],
        "age" => &[25, 30, 35],
        "name" => &["Alice", "Bob", "Charlie"],
    )?;

    let df = DataFrame::from_polars(polars_df);

    println!("{:?}", df.columns()?);

    let adults = df.filter(col("age").gt(lit_i64(18).into_expr()).into_expr())?;
    adults.show(Some(10))?;

    Ok(())
}
