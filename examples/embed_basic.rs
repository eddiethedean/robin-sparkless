//! Example for embedding robin-sparkless in an app or binding.
//!
//! Uses the prelude and optional config from env; runs a simple pipeline
//! and prints results as JSON. Run with: `cargo run --example embed_basic`

use robin_sparkless::prelude::*;

fn main() -> Result<(), robin_sparkless::EngineError> {
    // Optional: configure from env (ROBIN_SPARKLESS_WAREHOUSE_DIR, ROBIN_SPARKLESS_CASE_SENSITIVE, etc.)
    let config = SparklessConfig::from_env();
    let spark = SparkSession::from_config(&config);

    let df = spark
        .create_dataframe(
            vec![
                (1i64, 100i64, "Alice".to_string()),
                (2i64, 200i64, "Bob".to_string()),
                (3i64, 300i64, "Charlie".to_string()),
            ],
            vec!["id", "score", "name"],
        )
        .map_err(robin_sparkless::EngineError::from)?;

    // Simple pipeline: filter, then aggregate
    let filtered = df
        .filter(col("id").gt(lit_i64(1).into_expr()).into_expr())
        .map_err(robin_sparkless::EngineError::from)?;
    let grouped = filtered
        .group_by(vec!["id"])
        .map_err(robin_sparkless::EngineError::from)?;
    let aggregated = grouped
        .agg_columns(vec![count(&col("score")), sum(&col("score"))])
        .map_err(robin_sparkless::EngineError::from)?;

    println!("Schema: {:?}", aggregated.schema());
    let json = aggregated.to_json_rows()?;
    println!("Rows (JSON): {}", json);

    Ok(())
}
