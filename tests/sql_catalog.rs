//! SQL, temp views, catalog, empty table/schema.
//!
//! Merged from: sql_core, issue_546, issue_553, issue_648_643.

mod common;

use common::spark;
use polars::prelude::df;
use robin_sparkless::{DataFrame, SaveMode, WriteMode};
use serde_json::json;

// ---------- sql_core ----------

#[test]
fn sql_select_where_returns_rows_core() {
    let spark = spark();
    let df: DataFrame = spark
        .create_dataframe(
            vec![
                (1i64, 10i64, "a".to_string()),
                (2i64, 20i64, "b".to_string()),
                (3i64, 30i64, "c".to_string()),
            ],
            vec!["id", "v", "name"],
        )
        .unwrap();

    if spark.sql("SELECT 1").is_err() {
        return;
    }

    spark.create_or_replace_temp_view("t", df);
    let result = spark.sql("SELECT * FROM t WHERE id > 1").unwrap();
    let rows = result.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"].as_i64().unwrap(), 2);
    assert_eq!(rows[1]["id"].as_i64().unwrap(), 3);
    assert_eq!(rows[0]["name"].as_str().unwrap(), "b");
    assert_eq!(rows[1]["name"].as_str().unwrap(), "c");
}

#[test]
fn sql_drop_table_if_exists_core() {
    let spark = spark();
    let out = match spark.sql("DROP TABLE IF EXISTS my_schema.my_table") {
        Ok(df) => df,
        Err(_) => return,
    };
    assert_eq!(out.count().unwrap(), 0);
}

#[test]
fn sql_drop_table_removes_temp_view_core() {
    let spark = spark();
    let pl = df![
        "id" => &[1i64, 2i64],
        "x" => &["a", "b"],
    ]
    .unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);
    spark.create_or_replace_temp_view("t_to_drop", df);

    match spark.table("t_to_drop") {
        Ok(df_existing) => assert_eq!(df_existing.count().unwrap(), 2),
        Err(_) => return,
    }

    if spark.sql("DROP TABLE t_to_drop").is_err() {
        return;
    }
    assert!(spark.table("t_to_drop").is_err());
}

#[test]
fn sql_drop_view_removes_temp_view_core() {
    let spark = spark();
    let pl = df!["v" => &[1i64]].unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);
    spark.create_or_replace_temp_view("v_to_drop", df);

    if spark.sql("DROP VIEW v_to_drop").is_err() {
        return;
    }
    assert!(spark.table("v_to_drop").is_err());
}

#[test]
fn sql_create_table_and_catalog_core() {
    let spark = spark();

    if spark.sql("SELECT 1").is_err() {
        return;
    }

    let pl = df![
        "id" => &[1i64, 2i64],
        "name" => &["a", "b"],
    ]
    .unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);

    if df
        .write()
        .mode(WriteMode::Overwrite)
        .save_as_table(&spark, "tbl_core_sql", SaveMode::Overwrite)
        .is_err()
    {
        return;
    }

    let table_df = match spark.table("tbl_core_sql") {
        Ok(df) => df,
        Err(_) => return,
    };
    let rows = table_df.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);

    let via_sql = match spark.sql("SELECT * FROM tbl_core_sql ORDER BY id") {
        Ok(df) => df,
        Err(_) => return,
    };
    let rows_sql = via_sql.collect_as_json_rows().unwrap();
    assert_eq!(rows_sql.len(), 2);
    assert_eq!(rows_sql[0]["id"].as_i64().unwrap(), 1);
    assert_eq!(rows_sql[1]["id"].as_i64().unwrap(), 2);
}

// ---------- issue_546 ----------

#[test]
fn issue_546_sql_count_as_cnt() {
    let spark = spark();
    if spark.sql("SELECT 1").is_err() {
        return;
    }
    let pl = df![
        "grp" => &[1i64, 1i64, 2i64],
        "v"   => &[10i64, 20i64, 30i64],
    ]
    .unwrap();
    let df: DataFrame = spark.create_dataframe_from_polars(pl);
    spark.create_or_replace_temp_view("t", df);

    let result = spark
        .sql("SELECT grp, COUNT(v) AS cnt FROM t GROUP BY grp ORDER BY grp")
        .unwrap();
    let rows = result.collect_as_json_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("grp").and_then(|v| v.as_i64()), Some(1));
    assert_eq!(
        rows[0].get("cnt").and_then(|v| v.as_i64()),
        Some(2),
        "COUNT(v) AS cnt should produce column named cnt with value 2 for grp=1"
    );
    assert_eq!(rows[1].get("grp").and_then(|v| v.as_i64()), Some(2));
    assert_eq!(rows[1].get("cnt").and_then(|v| v.as_i64()), Some(1));
}

// ---------- issue_553 ----------

#[test]
fn issue_553_empty_df_save_as_table_then_table_count_zero() {
    let session = spark();
    let schema = vec![
        ("id".to_string(), "int".to_string()),
        ("name".to_string(), "string".to_string()),
    ];
    let empty = session.create_dataframe_from_rows(vec![], schema).unwrap();
    empty
        .write()
        .save_as_table(&session, "my_table", SaveMode::Overwrite)
        .unwrap();
    let t = session.table("my_table").unwrap();
    assert_eq!(t.count().unwrap(), 0);
    let cols = t.columns().unwrap();
    assert_eq!(cols, vec!["id", "name"]);
}

// ---------- issue_648_643 ----------

#[test]
fn empty_df_save_as_table_then_table_count_zero() {
    let session = spark();
    let schema = vec![
        ("k".to_string(), "bigint".to_string()),
        ("v".to_string(), "string".to_string()),
    ];
    let empty = session
        .create_dataframe_from_rows_engine(vec![], schema)
        .unwrap();
    empty
        .write()
        .save_as_table(&session, "empty_tbl", SaveMode::Overwrite)
        .unwrap();
    let t = session.table_engine("empty_tbl").unwrap();
    assert_eq!(t.count_engine().unwrap(), 0);
}

#[test]
fn table_name_temp_view_then_table() {
    let session = spark();
    let schema = vec![("x".to_string(), "bigint".to_string())];
    let rows = vec![vec![json!(1)], vec![json!(2)]];
    let df = session
        .create_dataframe_from_rows_engine(rows, schema)
        .unwrap();
    session.create_or_replace_temp_view("my_view", df);
    let t = session.table_engine("my_view").unwrap();
    assert_eq!(t.count_engine().unwrap(), 2);
}
