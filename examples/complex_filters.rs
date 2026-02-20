/// Example demonstrating complex filter expressions and logical operators
///
/// This example shows how to use complex boolean expressions with AND, OR, NOT
/// operators, nested conditions, and arithmetic expressions in filters and withColumn.
use polars::prelude::*;
use robin_sparkless::functions::col;
use robin_sparkless::SparkSession;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Note: SparkSession is created but not used in this example
    // as we're creating the DataFrame directly from Polars
    let _spark = SparkSession::builder()
        .app_name("complex_filters_example")
        .get_or_create();

    // Create a DataFrame with sample data using Polars directly
    // (create_dataframe currently supports 3-tuples, so we use Polars for 4 columns)
    let polars_df = df!(
        "id" => &[1, 2, 3, 4, 5],
        "age" => &[25, 35, 45, 55, 30],
        "score" => &[50, 60, 110, 70, 80],
        "vip" => &[0, 0, 1, 0, 1],
    )?;
    let df = robin_sparkless::DataFrame::from_polars(polars_df);

    println!("Original DataFrame:");
    df.show(Some(10))?;

    // Example 1: Simple filter with AND
    // Filter: age > 30 AND score < 100
    let filtered1 = df.filter(
        col("age")
            .into_expr()
            .gt(lit(30))
            .and(col("score").into_expr().lt(lit(100))),
    )?;
    println!("\nFiltered: age > 30 AND score < 100");
    filtered1.show(Some(10))?;

    // Example 2: Complex filter with nested OR
    // Filter: age > 30 AND (score < 100 OR vip == 1)
    let filtered2 = df.filter(
        col("age").into_expr().gt(lit(30)).and(
            col("score")
                .into_expr()
                .lt(lit(100))
                .or(col("vip").into_expr().eq(lit(1))),
        ),
    )?;
    println!("\nFiltered: age > 30 AND (score < 100 OR vip == 1)");
    filtered2.show(Some(10))?;

    // Example 3: Using NOT operator
    // Filter: NOT (vip == 0)
    let filtered3 = df.filter(col("vip").into_expr().eq(lit(0)).not())?;
    println!("\nFiltered: NOT (vip == 0)");
    filtered3.show(Some(10))?;

    // Example 4: withColumn with logical expression
    // Add a boolean column: age > 30 AND score < 100
    let df_with_flag = df.with_column_expr(
        "is_target",
        col("age")
            .into_expr()
            .gt(lit(30))
            .and(col("score").into_expr().lt(lit(100))),
    )?;
    println!("\nWith column 'is_target': age > 30 AND score < 100");
    df_with_flag.show(Some(10))?;

    // Example 5: withColumn with arithmetic and comparison
    // Add a column: (age + score) > 100
    let df_with_sum = df.with_column_expr(
        "above_threshold",
        (col("age").into_expr() + col("score").into_expr()).gt(lit(100)),
    )?;
    println!("\nWith column 'above_threshold': (age + score) > 100");
    df_with_sum.show(Some(10))?;

    Ok(())
}
