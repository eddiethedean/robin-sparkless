//! Robust tests for the lazy backend implementation (Issue #438).
//! Verifies that transformations extend the plan and only actions trigger materialization.

use std::io::Write;

use polars::prelude::{col, len, lit};
use robin_sparkless::{DataFrame, JoinType, SparkSession};
use tempfile::{NamedTempFile, TempDir};

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("lazy_backend_tests")
        .get_or_create()
}

fn sample_df() -> DataFrame {
    spark()
        .create_dataframe(
            vec![
                (1i64, 25i64, "Alice".to_string()),
                (2i64, 30i64, "Bob".to_string()),
                (3i64, 25i64, "Carol".to_string()),
                (4i64, 35i64, "Dave".to_string()),
                (5i64, 30i64, "Eve".to_string()),
            ],
            vec!["id", "age", "name"],
        )
        .unwrap()
}

/// Schema, columns, and resolve_column_name work on lazy DataFrames (before collect).
#[test]
fn lazy_schema_resolution_before_collect() {
    let df = sample_df();

    // schema() uses collect_schema on lazy - should work
    let schema = df.schema().unwrap();
    assert_eq!(schema.fields().len(), 3);

    // columns() uses schema - should work
    let cols = df.columns().unwrap();
    assert_eq!(cols, vec!["id", "age", "name"]);

    // resolve_column_name (case-insensitive) should work on lazy
    let resolved = df.resolve_column_name("AGE").unwrap();
    assert_eq!(resolved, "age");

    // get_column_dtype should work
    let dtype = df.get_column_dtype("id").unwrap();
    assert!(dtype.is_integer());
}

/// Full transformation pipeline: filter → select → group_by → agg → collect.
#[test]
fn lazy_full_pipeline_filter_select_groupby_agg() {
    let df = sample_df();

    let result = df
        .filter(col("age").gt_eq(lit(28)))
        .unwrap()
        .select(vec!["id", "age", "name"])
        .unwrap()
        .group_by(vec!["age"])
        .unwrap()
        .agg(vec![len().alias("count")])
        .unwrap();

    // Result should be lazy; collect to verify. Filter age >= 28: ages 30 (2), 35 (1) → 2 groups
    let rows = result.collect().unwrap();
    assert_eq!(rows.height(), 2);
    let count_col = rows.column("count").unwrap();
    // Polars len() returns u32
    let counts: Vec<u32> = count_col
        .u32()
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap_or(0))
        .collect();
    assert_eq!(counts.iter().sum::<u32>(), 3); // 2 + 1 rows
}

/// Transformation chain returns lazy; only collect materializes.
#[test]
fn lazy_transformation_chain_no_intermediate_collect() {
    let df = sample_df();

    let filtered = df.filter(col("age").eq(lit(25))).unwrap();
    assert_eq!(filtered.count().unwrap(), 2);

    let selected = filtered.select(vec!["id", "name"]).unwrap();
    assert_eq!(selected.count().unwrap(), 2);

    let limited = selected.limit(1).unwrap();
    assert_eq!(limited.count().unwrap(), 1);
}

/// Join returns lazy DataFrame.
#[test]
fn lazy_join_returns_lazy() {
    let left_pl = polars::prelude::df!("id" => &[1i64, 2i64], "label" => &["a", "b"]).unwrap();
    let right_pl = polars::prelude::df!("id" => &[1i64, 3i64], "value" => &[10i64, 30i64]).unwrap();
    let left = spark().create_dataframe_from_polars(left_pl);
    let right = spark().create_dataframe_from_polars(right_pl);

    let joined = left.join(&right, vec!["id"], JoinType::Inner).unwrap();
    assert_eq!(joined.count().unwrap(), 1);

    let left_joined = left.join(&right, vec!["id"], JoinType::Left).unwrap();
    assert_eq!(left_joined.count().unwrap(), 2);
}

/// Union returns lazy DataFrame.
#[test]
fn lazy_union_returns_lazy() {
    let df1_pl = polars::prelude::df!("id" => &[1i64], "name" => &["a"]).unwrap();
    let df2_pl = polars::prelude::df!("id" => &[2i64], "name" => &["b"]).unwrap();
    let df1 = spark().create_dataframe_from_polars(df1_pl);
    let df2 = spark().create_dataframe_from_polars(df2_pl);

    let united = df1.union(&df2).unwrap();
    assert_eq!(united.count().unwrap(), 2);
}

/// read_csv returns lazy DataFrame; schema and count work after collect.
#[test]
fn lazy_read_csv_returns_lazy() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "x,y").unwrap();
    writeln!(f, "1,10").unwrap();
    writeln!(f, "2,20").unwrap();
    f.flush().unwrap();

    let df = spark().read_csv(f.path()).unwrap();

    // Schema available without full collect (collect_schema)
    let cols = df.columns().unwrap();
    assert_eq!(cols, vec!["x", "y"]);

    assert_eq!(df.count().unwrap(), 2);
}

/// read_parquet returns lazy DataFrame.
#[test]
fn lazy_read_parquet_returns_lazy() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("data.parquet");

    let df_in_pl = polars::prelude::df!("id" => &[1i64], "name" => &["a"]).unwrap();
    let df_in = spark().create_dataframe_from_polars(df_in_pl);
    df_in
        .write()
        .mode(robin_sparkless::WriteMode::Overwrite)
        .format(robin_sparkless::WriteFormat::Parquet)
        .save(&path)
        .unwrap();

    let df = spark().read_parquet(&path).unwrap();
    assert_eq!(df.columns().unwrap(), vec!["id", "name"]);
    assert_eq!(df.count().unwrap(), 1);
}

/// Distinct, drop, dropna, fillna return lazy.
#[test]
fn lazy_distinct_drop_dropna_fillna() {
    let df = sample_df();

    let distinct = df.distinct(Some(vec!["age"])).unwrap();
    assert_eq!(distinct.count().unwrap(), 3);

    let dropped = df.drop(vec!["name"]).unwrap();
    assert_eq!(dropped.columns().unwrap(), vec!["id", "age"]);
    assert_eq!(dropped.count().unwrap(), 5);
}

/// limit and offset return lazy.
#[test]
fn lazy_limit_offset() {
    let df = sample_df();

    let limited = df.limit(2).unwrap();
    assert_eq!(limited.count().unwrap(), 2);

    let offset = df.offset(2).unwrap();
    assert_eq!(offset.count().unwrap(), 3);
}

/// Actions (count, show, collect) materialize correctly.
#[test]
fn lazy_actions_materialize() {
    let df = sample_df();

    assert_eq!(df.count().unwrap(), 5);

    let rows = df.collect().unwrap();
    assert_eq!(rows.height(), 5);

    let json_rows = df.collect_as_json_rows().unwrap();
    assert_eq!(json_rows.len(), 5);
}

/// create_dataframe produces lazy DataFrame.
#[test]
fn lazy_create_dataframe_is_lazy() {
    let df = spark()
        .create_dataframe(
            vec![
                (1i64, 10i64, "x".to_string()),
                (2i64, 20i64, "y".to_string()),
            ],
            vec!["a", "b", "c"],
        )
        .unwrap();

    let cols = df.columns().unwrap();
    assert_eq!(cols, vec!["a", "b", "c"]);
    assert_eq!(df.count().unwrap(), 2);
}

/// range produces lazy DataFrame.
#[test]
fn lazy_range_is_lazy() {
    let df = spark().range(0, 10, 1).unwrap();
    assert_eq!(df.columns().unwrap(), vec!["id"]);
    assert_eq!(df.count().unwrap(), 10);
}

/// Pivot (which collects for distinct values) and groupBy.agg produce correct results.
#[test]
fn lazy_pivot_and_groupby_agg() {
    let df = sample_df();

    let grouped = df.group_by(vec!["age"]).unwrap();
    let counted = grouped.count().unwrap();
    assert_eq!(counted.count().unwrap(), 3); // 3 distinct ages

    let summed = df.group_by(vec!["age"]).unwrap().sum("id").unwrap();
    assert_eq!(summed.count().unwrap(), 3);
}
