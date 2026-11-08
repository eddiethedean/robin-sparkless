//! Complete Column API tutorial, checked against docs/USER_GUIDE.md.
use robin_sparkless::SparkSession;
use robin_sparkless::functions::{col, lit_i64};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = SparkSession::builder()
        .app_name("rust-guide")
        .get_or_create();
    let people = spark.create_dataframe(
        vec![
            (1, 25, "Alice".to_string()),
            (2, 30, "Bob".to_string()),
            (3, 35, "Charlie".to_string()),
        ],
        vec!["id", "age", "name"],
    )?;
    let adults = people
        .filter(col("age").gt(lit_i64(25).into_expr()).into_expr())?
        .select(vec!["name", "age"])?
        .order_by(vec!["age"], vec![true])?
        .limit(10)?;
    assert_eq!(adults.count()?, 2);
    let rows = adults.collect_as_json_rows()?;
    assert_eq!(rows[0]["name"], "Bob");
    adults.group_by(vec!["name"])?.sum("age")?.show(Some(10))?;
    spark.create_or_replace_temp_view("adults", adults);
    assert_eq!(
        spark
            .sql("SELECT name FROM adults WHERE age > 30")?
            .count()?,
        1
    );
    Ok(())
}
