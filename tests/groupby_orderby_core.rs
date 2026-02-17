//! Core group_by and order_by behavior tests migrated from
//! `tests/python/test_issue_352_groupby_orderby_accept_column.py`.
//!
//! These tests focus on the underlying grouping and ordering semantics
//! using the Rust API (string-based group_by/order_by).

mod common;

use common::spark;
use polars::prelude::df;
use robin_sparkless::SparkSession;

#[test]
fn group_by_column_then_agg_sum_core() {
    let spark = spark();
    let pl = df![
        "dept" => &["A", "A", "B"],
        "salary" => &[100i64, 200i64, 150i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let gd = df.group_by(vec!["dept"]).unwrap();
    let out_df = gd.sum("salary").unwrap();
    let out = out_df.collect_as_json_rows().unwrap();

    assert_eq!(out.len(), 2);
    let mut a_total = None;
    let mut b_total = None;
    for row in out {
        match row["dept"].as_str().unwrap() {
            "A" => a_total = Some(row["sum(salary)"].as_i64().unwrap()),
            "B" => b_total = Some(row["sum(salary)"].as_i64().unwrap()),
            other => panic!("unexpected dept {other}"),
        }
    }
    assert_eq!(a_total, Some(300));
    assert_eq!(b_total, Some(150));
}

#[test]
fn group_by_list_of_columns_core() {
    let spark = spark();
    let pl = df![
        "a" => &[1i64, 1i64, 1i64],
        "b" => &[10i64, 10i64, 20i64],
        "v" => &[100i64, 200i64, 50i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let gd = df.group_by(vec!["a", "b"]).unwrap();
    let out_df = gd.sum("v").unwrap();
    let out = out_df.collect_as_json_rows().unwrap();

    assert_eq!(out.len(), 2);
    let mut totals = std::collections::HashMap::new();
    for row in out {
        let key = (row["a"].as_i64().unwrap(), row["b"].as_i64().unwrap());
        let total = row["sum(v)"].as_i64().unwrap();
        totals.insert(key, total);
    }
    assert_eq!(totals.get(&(1, 10)), Some(&300));
    assert_eq!(totals.get(&(1, 20)), Some(&50));
}

#[test]
fn group_by_single_str_and_list_core() {
    let spark = spark();
    let pl = df![
        "dept" => &["A", "A"],
        "n" => &[1i64, 2i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let gd1 = df.group_by(vec!["dept"]).unwrap();
    let out1_df = gd1.sum("n").unwrap();
    let out1 = out1_df.collect_as_json_rows().unwrap();
    assert_eq!(out1.len(), 1);
    assert_eq!(out1[0]["dept"].as_str().unwrap(), "A");
    assert_eq!(out1[0]["sum(n)"].as_i64().unwrap(), 3);

    let gd2 = df.group_by(vec!["dept"]).unwrap();
    let out2_df = gd2.sum("n").unwrap();
    let out2 = out2_df.collect_as_json_rows().unwrap();
    assert_eq!(out2.len(), 1);
    assert_eq!(out2[0]["dept"].as_str().unwrap(), "A");
    assert_eq!(out2[0]["sum(n)"].as_i64().unwrap(), 3);
}

#[test]
fn order_by_column_and_list_core() {
    let spark = spark();
    let pl = df!["x" => &[3i64, 1i64, 2i64]].unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let out = df
        .order_by(vec!["x"], vec![true])
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    let xs: Vec<i64> = out.iter().map(|r| r["x"].as_i64().unwrap()).collect();
    assert_eq!(xs, vec![1, 2, 3]);

    let pl2 = df![
        "a" => &[2i64, 1i64, 1i64],
        "b" => &[1i64, 2i64, 1i64],
    ]
    .unwrap();
    let df2 = spark.create_dataframe_from_polars(pl2);
    let out2 = df2
        .order_by(vec!["a", "b"], vec![true, true])
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(
        (
            out2[0]["a"].as_i64().unwrap(),
            out2[0]["b"].as_i64().unwrap()
        ),
        (1, 1)
    );
    assert_eq!(
        (
            out2[1]["a"].as_i64().unwrap(),
            out2[1]["b"].as_i64().unwrap()
        ),
        (1, 2)
    );
    assert_eq!(
        (
            out2[2]["a"].as_i64().unwrap(),
            out2[2]["b"].as_i64().unwrap()
        ),
        (2, 1)
    );
}

/// Group-by semantics when grouping keys contain NULL values. Mirrors
/// PySpark's behavior of treating NULL as its own group.
#[test]
fn group_by_with_null_keys_core() {
    let spark = spark();
    let pl = df![
        "dept" => &[Some("A"), Some("A"), None],
        "salary" => &[100i64, 200i64, 300i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let gd = df.group_by(vec!["dept"]).unwrap();
    let out_df = gd.sum("salary").unwrap();
    let out = out_df.collect_as_json_rows().unwrap();

    // Expect two groups: "A" and null.
    assert_eq!(out.len(), 2);
    let mut saw_a = false;
    let mut saw_null = false;
    for row in out {
        if row["dept"].is_null() {
            assert_eq!(row["sum(salary)"].as_i64().unwrap(), 300);
            saw_null = true;
        } else {
            assert_eq!(row["dept"].as_str().unwrap(), "A");
            assert_eq!(row["sum(salary)"].as_i64().unwrap(), 300);
            saw_a = true;
        }
    }
    assert!(saw_a && saw_null);
}
