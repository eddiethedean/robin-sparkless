//! Example for embedding robin-sparkless in an app or binding.
//!
//! Uses the prelude and optional config from env; runs a simple pipeline
//! and prints results as JSON. Run with: `cargo run --example embed_basic`
//!
//! Uses the engine-agnostic ExprIr API (col, lit_i64, gt, count, sum from core).

use robin_sparkless::prelude::*;
use robin_sparkless::{col, count, gt, lit_i64, sum};

fn main() -> Result<(), robin_sparkless::EngineError> {
    // Optional: configure from env (ROBIN_SPARKLESS_WAREHOUSE_DIR, ROBIN_SPARKLESS_CASE_SENSITIVE, etc.)
    let config = SparklessConfig::from_env();
    let spark = SparkSession::from_config(&config);

    let df = spark
        .create_dataframe_engine(
            vec![
                (1i64, 100i64, "Alice".to_string()),
                (2i64, 200i64, "Bob".to_string()),
                (3i64, 300i64, "Charlie".to_string()),
            ],
            vec!["id", "score", "name"],
        )?;

    // Simple pipeline: filter (ExprIr), then aggregate (ExprIr)
    let filtered = df.filter_expr_ir(&gt(col("id"), lit_i64(1)))?;
    let grouped = filtered.group_by_engine(vec!["id"])?;
    let aggregated = grouped.agg_expr_ir(&[count(col("score")), sum(col("score"))])?;

    println!("Schema: {:?}", aggregated.schema_engine()?);
    let json = aggregated.to_json_rows()?;
    println!("Rows (JSON): {}", json);

    Ok(())
}
