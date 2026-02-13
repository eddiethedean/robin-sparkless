//! Demo matching README quick start: create_dataframe, filter, show.
use robin_sparkless::{col, lit_i64, SparkSession};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = SparkSession::builder().app_name("demo").get_or_create();

    let df = spark.create_dataframe(
        vec![
            (1, 25, "Alice".to_string()),
            (2, 30, "Bob".to_string()),
            (3, 35, "Charlie".to_string()),
        ],
        vec!["id", "age", "name"],
    )?;

    let adults = df.filter(col("age").gt(lit_i64(26).into_expr()).into_expr())?;
    adults.show(Some(10))?;

    Ok(())
}
