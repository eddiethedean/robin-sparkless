//! Tests for PR8: Catalog/table name + empty DF (Fixes #648, #643).
//!
//! Empty DataFrame with schema: saveAsTable then table() returns 0 rows;
//! temp view name resolution.

use robin_sparkless::SparkSession;
use robin_sparkless::SaveMode;
use serde_json::json;

fn spark() -> SparkSession {
    SparkSession::builder()
        .app_name("issue_648_643_catalog_table_empty_df")
        .get_or_create()
}

/// #643: empty DataFrame with schema, saveAsTable, table() returns 0 rows.
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

/// #648: table name resolution — createDataFrame, createOrReplaceTempView, table().
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
