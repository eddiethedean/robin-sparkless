//! Regression tests for issue #492 – case-insensitive orderBy on mixed-case column names.
//!
//! When spark.sql.caseSensitive is false (PySpark default), orderBy should resolve column
//! names case-insensitively so that df.order_by("value") and df.order_by("VALUE") both
//! work when the actual column is named "Value".

mod common;

use common::spark;
use polars::prelude::df;

fn expected_values_asc() -> Vec<i64> {
    vec![5, 10, 20]
}

#[test]
fn issue_492_order_by_exact_case() {
    let spark = spark();
    let pl = df![
        "Name" => &["Alice", "Bob", "Charlie"],
        "Value" => &[10i64, 5i64, 20i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let rows = df
        .order_by(vec!["Value"], vec![true])
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    let values: Vec<i64> = rows.iter().map(|r| r["Value"].as_i64().unwrap()).collect();
    assert_eq!(
        values,
        expected_values_asc(),
        "order_by(\"Value\") should sort ascending"
    );
}

#[test]
fn issue_492_order_by_lowercase() {
    let spark = spark();
    let pl = df![
        "Name" => &["Alice", "Bob", "Charlie"],
        "Value" => &[10i64, 5i64, 20i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // PySpark parity: when caseSensitive is false, "value" resolves to "Value"
    let rows = df
        .order_by(vec!["value"], vec![true])
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    let values: Vec<i64> = rows.iter().map(|r| r["Value"].as_i64().unwrap()).collect();
    assert_eq!(
        values,
        expected_values_asc(),
        "order_by(\"value\") should resolve to Value and sort ascending"
    );
}

#[test]
fn issue_492_order_by_uppercase() {
    let spark = spark();
    let pl = df![
        "Name" => &["Alice", "Bob", "Charlie"],
        "Value" => &[10i64, 5i64, 20i64],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // PySpark parity: when caseSensitive is false, "VALUE" resolves to "Value"
    let rows = df
        .order_by(vec!["VALUE"], vec![true])
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    let values: Vec<i64> = rows.iter().map(|r| r["Value"].as_i64().unwrap()).collect();
    assert_eq!(
        values,
        expected_values_asc(),
        "order_by(\"VALUE\") should resolve to Value and sort ascending"
    );
}
