//! Exact snippet from README "Embedding" section: run to capture real output.
use robin_sparkless::prelude::*;
use robin_sparkless::{col, gt, lit_i64};

fn main() -> Result<(), robin_sparkless::EngineError> {
    let config = SparklessConfig::from_env();
    let spark = SparkSession::from_config(&config);

    let df = spark.create_dataframe_engine(
        vec![
            (1i64, 10i64, "a".to_string()),
            (2i64, 20i64, "b".to_string()),
            (3i64, 30i64, "c".to_string()),
        ],
        vec!["id", "value", "label"],
    )?;
    let filtered = df.filter_expr_ir(&gt(col("id"), lit_i64(1)))?;
    let json = filtered.to_json_rows()?;
    println!("{}", json);
    Ok(())
}
