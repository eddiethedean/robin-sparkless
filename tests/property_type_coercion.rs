//! Simple property-style tests for type coercion: ensure that comparing
//! string-encoded integers against numeric literals has consistent semantics.
//!
//! This is intentionally lightweight (not using an external proptest
//! dependency) but exercises several randomly generated inputs.

use rand::Rng;
use robin_sparkless::functions::{col, lit_i64};
use robin_sparkless::SparkSession;

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("property_type_coercion")
        .get_or_create()
}

#[test]
fn property_string_numeric_comparison_behaves_consistently() {
    use polars::prelude::df;

    let spark = spark();
    let mut rng = rand::thread_rng();

    // Generate a small batch of random integer values and their string form.
    let mut ints = Vec::new();
    let mut strs = Vec::new();
    for _ in 0..10 {
        let v: i64 = rng.gen_range(-10..=10);
        ints.push(v);
        strs.push(v.to_string());
    }

    let pl = df![
        "num" => &ints,
        "num_str" => &strs,
    ]
    .unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // Compare string column to numeric literal and ensure results match
    // numeric comparison on the parsed ints.
    let cmp = df
        .filter(col("num_str").gt(lit_i64(0).into_expr()).into_expr())
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    for row in cmp {
        let n = row["num"].as_i64().unwrap();
        assert!(n > 0);
    }
}
