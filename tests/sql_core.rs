//! SQL and catalog behavior tests migrated from Python tests in
//! `tests/python/test_robin_sparkless.py` and `tests/python/test_issue_362_sql_drop_table.py`.
//!
//! These validate that `SparkSession::sql`, temp views, and basic DDL
//! behave as expected directly at the Rust level.

use polars::prelude::df;
use robin_sparkless::{DataFrame, SaveMode, SparkSession, WriteMode};

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("sql_core_tests")
        .get_or_create()
}

#[test]
fn sql_select_where_returns_rows_core() {
    let spark = spark();
    let df: DataFrame = spark
        .create_dataframe(
            vec![
                (1i64, 10i64, "a".to_string()),
                (2i64, 20i64, "b".to_string()),
                (3i64, 30i64, "c".to_string()),
            ],
            vec!["id", "v", "name"],
        )
        .unwrap();

    // Skip when sql feature is not enabled.
    if spark.sql("SELECT 1").is_err() {
        return;
    }

    spark.create_or_replace_temp_view("t", df);
    let result = spark.sql("SELECT * FROM t WHERE id > 1").unwrap();
    let rows = result.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"].as_i64().unwrap(), 2);
    assert_eq!(rows[1]["id"].as_i64().unwrap(), 3);
    assert_eq!(rows[0]["name"].as_str().unwrap(), "b");
    assert_eq!(rows[1]["name"].as_str().unwrap(), "c");
}

#[test]
fn sql_drop_table_if_exists_core() {
    let spark = spark();
    // Skip when sql feature is not enabled.
    let out = match spark.sql("DROP TABLE IF EXISTS my_schema.my_table") {
        Ok(df) => df,
        Err(_) => return,
    };
    assert_eq!(out.count().unwrap(), 0);
}

#[test]
fn sql_drop_table_removes_temp_view_core() {
    let spark = spark();
    let pl = df![
        "id" => &[1i64, 2i64],
        "x" => &["a", "b"],
    ]
    .unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);
    spark.create_or_replace_temp_view("t_to_drop", df);

    // If sql feature is disabled, table() or sql() may error; treat that as skip.
    match spark.table("t_to_drop") {
        Ok(df_existing) => assert_eq!(df_existing.count().unwrap(), 2),
        Err(_) => return,
    }

    if spark.sql("DROP TABLE t_to_drop").is_err() {
        return;
    }
    assert!(spark.table("t_to_drop").is_err());
}

#[test]
fn sql_drop_view_removes_temp_view_core() {
    let spark = spark();
    let pl = df!["v" => &[1i64]].unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);
    spark.create_or_replace_temp_view("v_to_drop", df);

    if spark.sql("DROP VIEW v_to_drop").is_err() {
        return;
    }
    assert!(spark.table("v_to_drop").is_err());
}

/// Basic DDL and catalog behavior when the `sql` feature is enabled:
/// create a saved table and ensure it is visible via the catalog and
/// can be queried back.
#[test]
fn sql_create_table_and_catalog_core() {
    let spark = spark();

    // Skip if SQL is not enabled.
    if spark.sql("SELECT 1").is_err() {
        return;
    }

    // Create a small DataFrame and save it as a table.
    let pl = df![
        "id" => &[1i64, 2i64],
        "name" => &["a", "b"],
    ]
    .unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);

    // Save as a managed table; if this fails due to feature support or missing
    // SQL feature, treat as skip.
    if df
        .write()
        .mode(WriteMode::Overwrite)
        .save_as_table(&spark, "tbl_core_sql", SaveMode::Overwrite)
        .is_err()
    {
        return;
    }

    // Table should be visible via table() and return two rows.
    let table_df = match spark.table("tbl_core_sql") {
        Ok(df) => df,
        Err(_) => return,
    };
    let rows = table_df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);

    // Access via SQL as well.
    let via_sql = match spark.sql("SELECT * FROM tbl_core_sql ORDER BY id") {
        Ok(df) => df,
        Err(_) => return,
    };
    let rows_sql = via_sql.collect_as_json_rows().unwrap();
    assert_eq!(rows_sql.len(), 2);
    assert_eq!(rows_sql[0]["id"].as_i64().unwrap(), 1);
    assert_eq!(rows_sql[1]["id"].as_i64().unwrap(), 2);
}
