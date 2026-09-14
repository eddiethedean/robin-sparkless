#![cfg(feature = "sqlite")]

use polars::prelude::DataType;
use robin_sparkless_polars::jdbc::{JdbcOptions, read_jdbc_to_polars};
use std::collections::HashMap;

#[test]
fn sol_005_empty_dbtable_preserves_declared_string_type() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let conn = rusqlite::Connection::open(temp.path()).unwrap();
    conn.execute_batch("CREATE TABLE text_rows (name TEXT); INSERT INTO text_rows VALUES ('x');")
        .unwrap();
    let opts = JdbcOptions::from_options_map(&HashMap::from([
        (
            "url".into(),
            format!("jdbc:sqlite:{}", temp.path().display()),
        ),
        ("dbtable".into(), "text_rows".into()),
    ]))
    .unwrap();
    let full = read_jdbc_to_polars(&opts).unwrap();
    assert_eq!(full.column("name").unwrap().dtype(), &DataType::String);
    conn.execute("DELETE FROM text_rows", []).unwrap();
    let empty = read_jdbc_to_polars(&opts).unwrap();
    assert_eq!(empty.height(), 0);
    assert_eq!(
        empty.schema(),
        full.schema(),
        "SOL-005: dbtable output columns map directly to declared metadata even without a query projection"
    );
}
