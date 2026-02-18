//! Regression tests for issue #553 – Empty DataFrame with explicit schema and table ops (PySpark parity).
//!
//! PySpark raises "can not infer schema from empty dataset"; robin-sparkless supports
//! createDataFrame([], schema), saveAsTable, and spark.table() on empty DF.

mod common;

use common::spark;
use robin_sparkless::SaveMode;

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
    assert_eq!(
        t.count().unwrap(),
        0,
        "spark.table() on empty saved table must return 0 rows"
    );
    let cols = t.columns().unwrap();
    assert_eq!(cols, vec!["id", "name"]);
}
