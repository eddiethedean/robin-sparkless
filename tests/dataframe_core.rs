//! Core DataFrame and Column behavior tests migrated from the Python
//! `tests/python/test_robin_sparkless.py` smoke suite.
//!
//! These tests exercise high-value semantics directly against the Rust API
//! so the core engine is validated without going through the Python bindings.

mod common;

use common::{small_people_df, spark};
use polars::prelude::{col as pl_col, df, lit as pl_lit};
use robin_sparkless::functions::{col, lit_i64};
use robin_sparkless::JoinType;

/// Equivalent to `test_create_dataframe_and_collect` in
/// `tests/python/test_robin_sparkless.py`.
#[test]
fn create_dataframe_and_collect_core() {
    let df = small_people_df();
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
    let df = small_people_df();

    // filter: age > 28
    let expr = col("age").gt(lit_i64(28).into_expr()).into_expr();
    let filtered = df.filter(expr).unwrap();
    assert_eq!(filtered.count().unwrap(), 2);

    let filtered_rows = filtered.collect_as_json_rows().unwrap();
    assert!(
        filtered_rows
            .iter()
            .all(|r| r["age"].as_i64().unwrap() > 28)
    );

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

/// Basic join semantics on small in-memory DataFrames, mirroring
/// simple PySpark pipelines that join on a single key.
#[test]
fn join_inner_and_left_core() {
    let spark = spark();
    let left_pl = df![
        "id" => &[1i64, 2i64, 3i64],
        "v"  => &[10i64, 20i64, 30i64],
    ]
    .unwrap();
    let right_pl = df![
        "id" => &[2i64, 3i64, 4i64],
        "w"  => &[200i64, 300i64, 400i64],
    ]
    .unwrap();

    let left = spark.create_dataframe_from_polars(left_pl);
    let right = spark.create_dataframe_from_polars(right_pl);

    // Inner join: ids 2 and 3 are present on both sides.
    let inner = left
        .join(&right, vec!["id"], JoinType::Inner)
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    let inner_ids: Vec<i64> = inner.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(inner_ids, vec![2, 3]);

    // Left join: ids 1, 2, 3 from the left; id 1 has null `w`.
    let left_join = spark
        .create_dataframe_from_polars(
            df![
                "id" => &[1i64, 2i64, 3i64],
                "v"  => &[10i64, 20i64, 30i64],
            ]
            .unwrap(),
        )
        .join(&right, vec!["id"], JoinType::Left)
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(left_join.len(), 3);
    assert_eq!(left_join[0]["id"].as_i64().unwrap(), 1);
    assert!(left_join[0]["w"].is_null());
}

/// Window functions via `with_column` on a small DataFrame: row_number
/// partitioned by a grouping column.
#[test]
fn with_column_window_row_number_core() {
    let spark = spark();
    let pl = df![
        "dept" => &["A", "A", "B", "B"],
        "salary" => &[100i64, 200i64, 150i64, 250i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let with_rn = df
        .with_column("rn", &col("salary").row_number(true).over(&["dept"]))
        .unwrap();
    let rows = with_rn.collect_as_json_rows().unwrap();

    // Within each department, row numbers start at 1 and increase.
    let mut a_ranks = Vec::new();
    let mut b_ranks = Vec::new();
    for row in rows {
        let dept = row["dept"].as_str().unwrap();
        let rn = row["rn"].as_i64().unwrap();
        match dept {
            "A" => a_ranks.push(rn),
            "B" => b_ranks.push(rn),
            other => panic!("unexpected dept {other}"),
        }
    }
    a_ranks.sort();
    b_ranks.sort();
    assert_eq!(a_ranks, vec![1, 2]);
    assert_eq!(b_ranks, vec![1, 2]);
}

/// Simple with_column string transformation: uppercasing a name field.
#[test]
fn with_column_string_upper_core() {
    let df = small_people_df();
    let df2 = df.with_column("name_upper", &col("name").upper()).unwrap();
    let rows = df2.collect_as_json_rows().unwrap();

    let names: Vec<&str> = rows
        .iter()
        .map(|r| r["name_upper"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"ALICE"));
    assert!(names.contains(&"BOB"));
    assert!(names.contains(&"CAROL"));
}
