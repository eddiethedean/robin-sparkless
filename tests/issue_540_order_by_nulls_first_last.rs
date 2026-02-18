//! Regression tests for issue #540 – orderBy nulls_first / nulls_last (PySpark parity).
//!
//! PySpark: asc_nulls_first(), asc_nulls_last(), desc_nulls_first(), desc_nulls_last().
//! Default: ASC → nulls first, DESC → nulls last.

mod common;

use common::spark;
use robin_sparkless::{col, desc_nulls_last, SparkSession};

#[test]
fn issue_540_desc_nulls_last() {
    let spark = spark();
    let pl = polars::prelude::df![
        "value" => &["A", "B", "C", "D"],
        "ord" => &[Some(1i64), Some(2i64), None, Some(4i64)],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // desc_nulls_last: descending order, nulls last. So non-null 4,2,1 then null.
    let sorted = df
        .order_by_exprs(vec![desc_nulls_last(&col("ord"))])
        .unwrap();
    let rows = sorted.collect_as_json_rows().unwrap();
    let ords: Vec<Option<i64>> = rows
        .iter()
        .map(|r| r.get("ord").and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(ords, vec![Some(4), Some(2), Some(1), None], "nulls last");
}

#[test]
fn issue_540_default_order_by_desc_null_order() {
    // Simple order_by(refs, ascending) should use PySpark default: DESC → nulls last.
    let spark = spark();
    let pl = polars::prelude::df![
        "value" => &["A", "B", "C", "D"],
        "ord" => &[Some(1i64), Some(2i64), None, Some(4i64)],
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let sorted = df.order_by(vec!["ord"], vec![false]).unwrap(); // descending
    let rows = sorted.collect_as_json_rows().unwrap();
    let ords: Vec<Option<i64>> = rows
        .iter()
        .map(|r| r.get("ord").and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(
        ords,
        vec![Some(4), Some(2), Some(1), None],
        "default DESC nulls last"
    );
}
