//! Core DataFrame and Column behavior tests migrated from the Python
//! `tests/python/test_robin_sparkless.py` smoke suite.
//!
//! These tests exercise high-value semantics directly against the Rust API
//! so the core engine is validated without going through the Python bindings.

use polars::prelude::{col as pl_col, df, lit as pl_lit};
use robin_sparkless::{col, lit_i64, DataFrame, SparkSession};

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("dataframe_core_tests")
        .get_or_create()
}

fn sample_df() -> DataFrame {
    spark()
        .create_dataframe(
            vec![
                (1i64, 25i64, "Alice".to_string()),
                (2i64, 30i64, "Bob".to_string()),
                (3i64, 35i64, "Carol".to_string()),
            ],
            vec!["id", "age", "name"],
        )
        .unwrap()
}

/// Equivalent to `test_create_dataframe_and_collect` in
/// `tests/python/test_robin_sparkless.py`.
#[test]
fn create_dataframe_and_collect_core() {
    let df = sample_df();
    let n = df.count().unwrap();
    assert_eq!(n, 3);

    let rows = df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["id"].as_i64().unwrap(), 1);
    assert_eq!(rows[0]["age"].as_i64().unwrap(), 25);
    assert_eq!(rows[0]["name"].as_str().unwrap(), "Alice");
}

/// Equivalent to `test_filter_and_select` from the Python test suite:
/// filter on a numeric predicate then select a subset of columns.
#[test]
fn filter_and_select_core() {
    let df = sample_df();

    // filter: age > 28
    let expr = col("age").gt(lit_i64(28).into_expr()).into_expr();
    let filtered = df.filter(expr).unwrap();
    assert_eq!(filtered.count().unwrap(), 2);

    let filtered_rows = filtered.collect_as_json_rows().unwrap();
    assert!(filtered_rows
        .iter()
        .all(|r| r["age"].as_i64().unwrap() > 28));

    // select columns
    let selected = df.select(vec!["id", "name"]).unwrap();
    assert_eq!(selected.count().unwrap(), 3);
    let selected_rows = selected.collect_as_json_rows().unwrap();
    let first = &selected_rows[0];
    assert!(first.get("id").is_some());
    assert!(first.get("name").is_some());
    assert!(first.get("age").is_none());
}

/// Column–column comparison semantics, adapted from
/// `test_filter_column_vs_column` and related tests in the Python suite.
#[test]
fn filter_column_vs_column_core() {
    let spark = spark();
    let pl = df![
        "a" => &[1i64, 2i64, 3i64, 4i64, 5i64],
        "b" => &[5i64, 4i64, 1i64, 2i64, 1i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // a > b via method-style comparison
    let gt_rows = df
        .filter(col("a").gt(col("b").into_expr()).into_expr())
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert_eq!(gt_rows.len(), 3);

    // Other operators: <, >=, <=, ==, != (check row counts)
    assert_eq!(
        df.filter(col("a").lt(col("b").into_expr()).into_expr())
            .unwrap()
            .count()
            .unwrap(),
        2
    );
    assert_eq!(
        df.filter(col("a").gt_eq(col("b").into_expr()).into_expr())
            .unwrap()
            .count()
            .unwrap(),
        3
    );
    assert_eq!(
        df.filter(col("a").lt_eq(col("b").into_expr()).into_expr())
            .unwrap()
            .count()
            .unwrap(),
        2
    );
    assert_eq!(
        df.filter(col("a").eq(col("b").into_expr()).into_expr())
            .unwrap()
            .count()
            .unwrap(),
        0
    );
    assert_eq!(
        df.filter(col("a").neq(col("b").into_expr()).into_expr())
            .unwrap()
            .count()
            .unwrap(),
        5
    );
}

/// Column–column comparison combined with literals, mirroring
/// `test_filter_column_vs_column_combined_with_literal` in Python.
#[test]
fn filter_column_vs_column_combined_with_literal_core() {
    let spark = spark();
    let pl = df![
        "a" => &[1i64, 2i64, 3i64, 4i64, 5i64],
        "b" => &[5i64, 4i64, 1i64, 2i64, 1i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // Use Polars expressions to mirror (a > b) & (a > 2)
    let expr_and = pl_col("a")
        .gt(pl_col("b"))
        .and(pl_col("a").gt(pl_lit(2_i64)));
    let out = df.filter(expr_and).unwrap().collect_as_json_rows().unwrap();
    assert_eq!(out.len(), 3);

    // (a < b) | (b >= 5)
    let expr_or = pl_col("a")
        .lt(pl_col("b"))
        .or(pl_col("b").gt_eq(pl_lit(5_i64)));
    let out2 = df.filter(expr_or).unwrap().collect_as_json_rows().unwrap();
    assert_eq!(out2.len(), 2);
}
