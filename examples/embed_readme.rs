//! Exact snippet from README "Embedding" section: run to capture real output.
use robin_sparkless::prelude::*;

fn main() -> Result<(), robin_sparkless::EngineError> {
    let config = SparklessConfig::from_env();
    let spark = SparkSession::from_config(&config);

    let df = spark
        .create_dataframe(
            vec![
                (1i64, 10i64, "a".to_string()),
                (2i64, 20i64, "b".to_string()),
                (3i64, 30i64, "c".to_string()),
            ],
            vec!["id", "value", "label"],
        )
        .map_err(robin_sparkless::EngineError::from)?;
    let filtered = df
        .filter(col("id").gt(lit_i64(1).into_expr()).into_expr())
        .map_err(robin_sparkless::EngineError::from)?;
    let json = filtered.to_json_rows()?;
    println!("{}", json);
    Ok(())
}
