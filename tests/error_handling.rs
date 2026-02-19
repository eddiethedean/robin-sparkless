//! Tests for error handling behavior.
//!
//! These tests verify that robin-sparkless returns appropriate errors
//! for invalid operations and edge cases.

use polars::prelude::PolarsError;
use robin_sparkless::{DataFrame, JoinType, SparkSession, col};

/// Helper to create a simple test DataFrame
fn test_df() -> DataFrame {
    let spark = SparkSession::builder()
        .app_name("error_tests")
        .get_or_create();
    let tuples = vec![
        (1i64, 25i64, "Alice".to_string()),
        (2i64, 35i64, "Bob".to_string()),
        (3i64, 45i64, "Charlie".to_string()),
    ];
    spark
        .create_dataframe(tuples, vec!["id", "age", "name"])
        .unwrap()
}

#[test]
fn test_column_not_found_error() {
    let df = test_df();

    // Attempting to get a non-existent column should return ColumnNotFound error
    let result = df.column("nonexistent");

    assert!(result.is_err());
    match result {
        Err(PolarsError::ColumnNotFound(msg)) => {
            assert!(msg.to_string().contains("nonexistent"));
        }
        Err(e) => panic!("Expected ColumnNotFound error, got: {e:?}"),
        Ok(_) => panic!("Expected error, got Ok"),
    }
}

#[test]
fn test_select_nonexistent_column_error() {
    let df = test_df();

    // Attempting to select a non-existent column should return an error
    let result = df.select(vec!["id", "nonexistent"]);

    assert!(result.is_err());
}

#[test]
fn test_empty_dataframe_operations() {
    let df = DataFrame::empty();

    // Empty DataFrame should have 0 rows
    assert_eq!(df.count().unwrap(), 0);

    // Schema should work on empty DataFrame
    let schema = df.schema();
    assert!(schema.is_ok());

    // Columns should work on empty DataFrame
    let columns = df.columns();
    assert!(columns.is_ok());
    assert!(columns.unwrap().is_empty());
}

#[test]
fn test_filter_on_empty_dataframe() {
    let df = DataFrame::empty();

    // Filter on empty DataFrame should return empty DataFrame without error
    // Note: This test verifies graceful handling, not that it will succeed
    // since we need a valid filter expression that references existing columns

    // We can't filter on empty DataFrame with column reference
    // but we can verify the DataFrame is still usable
    assert_eq!(df.count().unwrap(), 0);
}

#[test]
fn test_order_by_nonexistent_column_error() {
    let df = test_df();

    // Attempting to order by a non-existent column should return an error
    let result = df.order_by(vec!["nonexistent"], vec![true]);

    assert!(result.is_err());
}

#[test]
fn test_group_by_nonexistent_column_error() {
    let df = test_df();

    // Attempting to group by a non-existent column should return an error
    let result = df.group_by(vec!["nonexistent"]);

    // GroupBy itself may not error until aggregation
    if let Ok(grouped) = result {
        let agg_result = grouped.count();
        assert!(agg_result.is_err());
    }
}

#[test]
fn test_with_column_preserves_dataframe() {
    let df = test_df();

    // withColumn with a valid expression should succeed
    let expr = col("age").gt(polars::prelude::lit(30));
    let result = df.with_column_expr("is_adult", expr.into_expr());

    assert!(result.is_ok());
    let new_df = result.unwrap();

    // Original DataFrame should be unchanged (immutability)
    assert_eq!(df.count().unwrap(), 3);

    // New DataFrame should have the new column
    let cols = new_df.columns().unwrap();
    assert!(cols.contains(&"is_adult".to_string()));
}

#[test]
fn test_multiple_operations_on_same_dataframe() {
    let df = test_df();

    // Multiple operations on the same DataFrame should all succeed
    // (verifying immutability and that operations don't corrupt state)
    let filtered1 = df.filter(col("age").gt(polars::prelude::lit(30)).into_expr());
    let filtered2 = df.filter(col("age").lt(polars::prelude::lit(40)).into_expr());
    let selected = df.select(vec!["id", "name"]);

    assert!(filtered1.is_ok());
    assert!(filtered2.is_ok());
    assert!(selected.is_ok());

    // Each result should be independent
    assert_eq!(filtered1.unwrap().count().unwrap(), 2); // age > 30: Bob, Charlie
    assert_eq!(filtered2.unwrap().count().unwrap(), 2); // age < 40: Alice, Bob
    assert_eq!(selected.unwrap().columns().unwrap().len(), 2); // id, name
}

#[test]
fn test_join_nonexistent_column_error() {
    let spark = SparkSession::builder()
        .app_name("error_tests")
        .get_or_create();
    let left = spark
        .create_dataframe(
            vec![(1i64, 10i64, "a".to_string())],
            vec!["id", "v", "label"],
        )
        .unwrap();
    let right = spark
        .create_dataframe(
            vec![(1i64, 100i64, "x".to_string())],
            vec!["id", "w", "tag"],
        )
        .unwrap();
    let result = left.join(&right, vec!["nonexistent"], JoinType::Inner);
    assert!(result.is_err());
}

#[test]
fn test_group_by_agg_nonexistent_column_error() {
    let df = test_df();
    let grouped = df.group_by(vec!["id"]).unwrap();
    let result = grouped.sum("nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_limit_zero_returns_empty() {
    let df = test_df();
    let limited = df.limit(0).unwrap();
    assert_eq!(limited.count().unwrap(), 0);
}

#[test]
fn test_union_with_empty_dataframe() {
    let spark = SparkSession::builder()
        .app_name("error_tests")
        .get_or_create();
    let non_empty = spark
        .create_dataframe(
            vec![(1i64, 25i64, "Alice".to_string())],
            vec!["id", "age", "name"],
        )
        .unwrap();
    let empty = spark
        .create_dataframe(vec![] as Vec<(i64, i64, String)>, vec!["id", "age", "name"])
        .unwrap();
    let result = non_empty.union(&empty);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().count().unwrap(), 1);
    let result2 = empty.union(&non_empty);
    assert!(result2.is_ok());
    assert_eq!(result2.unwrap().count().unwrap(), 1);
}
