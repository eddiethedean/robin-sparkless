//! Integration tests for issue #235: string–numeric comparison coercion in filter.
//! Issue #265: date/datetime vs string comparison (PySpark implicit cast).
//! Issue #602: filter predicate string vs numeric coercion (PySpark parity).
//!
//! These tests ensure that `df.filter(col("str_col") == lit(123))` (and symmetric form)
//! is coerced so that the string column is compared numerically (PySpark parity), and
//! that the filter runs without "cannot compare string with numeric type" or
//! "filter predicate was not of type boolean".
//! Issue #265: `df.filter(col("dt") == "2025-01-01")` on a date column is coerced so
//! the string literal is cast to date (PySpark parity).

use chrono::NaiveDate;
use polars::prelude::{DataType, NamedFrom, Series};
use robin_sparkless::functions::{col, lit_i64, lit_str};
use robin_sparkless::DataFrame;

fn df_with_string_column() -> DataFrame {
    let s = Series::new("str_col".into(), &["123", "456"]);
    let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
    DataFrame::from_polars(pl_df)
}

#[test]
fn issue_235_string_eq_numeric_literal_in_filter() {
    let df = df_with_string_column();
    let expr = col("str_col").eq(lit_i64(123).into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(
        out.count().unwrap(),
        1,
        "filter(str_col == 123) should return one row"
    );
}

/// Issue #602: PySpark df.filter(df["s"] == 123) on string column must work (coercion).
/// Must not fail with "cannot compare string with numeric type" or "filter predicate was not of type boolean".
#[test]
fn issue_602_filter_string_column_eq_numeric_literal() {
    let df = df_with_string_column();
    let expr = col("str_col").eq(lit_i64(123).into_expr()).into_expr();
    let out = df
        .filter(expr)
        .expect("issue #602: filter(string_col == 123) must succeed with coercion");
    assert_eq!(
        out.count().unwrap(),
        1,
        "issue #602: expect one row [Row(s='123')]"
    );
}

#[test]
fn issue_235_literal_eq_string_column_symmetric_form() {
    let df = df_with_string_column();
    let expr = lit_i64(123).eq(col("str_col").into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(
        out.count().unwrap(),
        1,
        "filter(123 == str_col) should return one row"
    );
}

#[test]
fn issue_235_string_gt_numeric_literal_uses_numeric_semantics() {
    let df = df_with_string_column();
    let expr = col("str_col").gt(lit_i64(200).into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(
        out.count().unwrap(),
        1,
        "filter(str_col > 200) should return one row (456)"
    );
}

#[test]
fn issue_235_string_eq_numeric_with_invalid_string_non_matching() {
    let s = Series::new("str_col".into(), &["abc", "123"]);
    let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
    let df = DataFrame::from_polars(pl_df);
    let expr = col("str_col").eq(lit_i64(123).into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(
        out.count().unwrap(),
        1,
        "invalid numeric string should not match"
    );
}

fn df_with_date_column() -> DataFrame {
    let d1 = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
    let s = Series::new("dt".into(), [d1, d2])
        .cast(&DataType::Date)
        .unwrap();
    let pl_df = polars::prelude::DataFrame::new_infer_height(vec![s.into()]).unwrap();
    DataFrame::from_polars(pl_df)
}

#[test]
fn issue_265_date_column_eq_string_literal() {
    let df = df_with_date_column();
    let expr = col("dt").eq(lit_str("2025-01-01").into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(
        out.count().unwrap(),
        1,
        "filter(dt == '2025-01-01') on date column should return one row"
    );
}

#[test]
fn issue_265_date_column_ne_string_literal() {
    let df = df_with_date_column();
    let expr = col("dt").neq(lit_str("2025-01-01").into_expr()).into_expr();
    let out = df.filter(expr).unwrap();
    assert_eq!(
        out.count().unwrap(),
        1,
        "filter(dt != '2025-01-01') on date column should return one row"
    );
}
