//! Regression tests for issue #544 – cast/astype semantics (PySpark parity).
//!
//! Focus on a few core cases:
//! - int -> string
//! - string -> int (cast vs try_cast)
//! - string -> boolean

mod common;

use common::spark;
use robin_sparkless::{cast, col, try_cast};

#[test]
fn issue_544_int_to_string_cast() {
    let spark = spark();
    let pl = polars::prelude::df!["x" => &[1i64, 2i64]].unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    let out = df
        .select_exprs(vec![
            cast(&col("x"), "string").unwrap().alias("s").into_expr(),
        ])
        .unwrap();
    let rows = out.collect_as_json_rows().unwrap();
    assert_eq!(rows[0].get("s").and_then(|v| v.as_str()), Some("1"));
    assert_eq!(rows[1].get("s").and_then(|v| v.as_str()), Some("2"));
}

#[test]
fn issue_544_string_to_int_cast_and_try_cast() {
    let spark = spark();
    let pl = polars::prelude::df!["x" => &["1", "2", "not_int"]].unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // cast should error on invalid string
    let cast_res = cast(&col("x"), "bigint").unwrap();
    let out = df
        .select_exprs(vec![cast_res.alias("y").into_expr()])
        .and_then(|d| d.collect_as_json_rows());
    assert!(
        out.is_err(),
        "cast to bigint should error on invalid string value"
    );

    // try_cast should yield null on invalid
    let try_col = try_cast(&col("x"), "bigint").unwrap();
    let out_ok = df
        .select_exprs(vec![try_col.alias("y").into_expr()])
        .unwrap();
    let rows = out_ok.collect_as_json_rows().unwrap();
    assert_eq!(rows[0].get("y").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(rows[1].get("y").and_then(|v| v.as_i64()), Some(2));
    assert!(rows[2].get("y").unwrap().is_null());
}

#[test]
fn issue_544_string_to_boolean_cast_and_try_cast() {
    let spark = spark();
    let pl = polars::prelude::df!["x" => &["true", "false", "1", "0", "invalid"]].unwrap();
    let df = spark.create_dataframe_from_polars(pl);

    // cast to boolean: invalid should error
    let cast_bool = cast(&col("x"), "boolean").unwrap();
    let out = df
        .select_exprs(vec![cast_bool.alias("b").into_expr()])
        .and_then(|d| d.collect_as_json_rows());
    assert!(
        out.is_err(),
        "cast to boolean should error on invalid value"
    );

    // try_cast to boolean: invalid should become null
    let try_bool = try_cast(&col("x"), "boolean").unwrap();
    let out_ok = df
        .select_exprs(vec![try_bool.alias("b").into_expr()])
        .unwrap();
    let rows = out_ok.collect_as_json_rows().unwrap();
    assert_eq!(rows[0].get("b").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(rows[1].get("b").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(rows[2].get("b").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(rows[3].get("b").and_then(|v| v.as_bool()), Some(false));
    assert!(rows[4].get("b").unwrap().is_null());
}
