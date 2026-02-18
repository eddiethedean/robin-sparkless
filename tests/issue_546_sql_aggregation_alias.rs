//! Regression tests for issue #546 – SQL alias in aggregation SELECT (PySpark parity).
//!
//! PySpark: SELECT grp, COUNT(v) AS cnt FROM t GROUP BY grp returns a column named "cnt".

use polars::prelude::df;
use robin_sparkless::{DataFrame, SparkSession};

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("issue_546_test")
        .get_or_create()
}

#[test]
fn issue_546_sql_count_as_cnt() {
    let spark = spark();
    if spark.sql("SELECT 1").is_err() {
        return; // SQL feature not enabled
    }
    let pl = df![
        "grp" => &[1i64, 1i64, 2i64],
        "v"   => &[10i64, 20i64, 30i64],
    ]
    .unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);
    spark.create_or_replace_temp_view("t", df);

    let result = spark
        .sql("SELECT grp, COUNT(v) AS cnt FROM t GROUP BY grp ORDER BY grp")
        .unwrap();
    let rows = result.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("grp").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(
        rows[0].get("cnt").and_then(|v| v.as_i64()),
        Some(2),
        "COUNT(v) AS cnt should produce column named cnt with value 2 for grp=1"
    );
    assert_eq!(rows[1].get("grp").and_then(|v| v.as_i64()), Some(2));
    assert_eq!(rows[1].get("cnt").and_then(|v| v.as_i64()), Some(1));
}
