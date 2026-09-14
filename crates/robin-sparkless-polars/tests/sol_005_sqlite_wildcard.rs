#![cfg(feature = "sqlite")]

use polars::prelude::*;
use robin_sparkless_polars::jdbc::{JdbcOptions, read_jdbc_to_polars};
use std::collections::HashMap;

#[test]
fn sol_005_empty_wildcard_query_preserves_text_type() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let conn = rusqlite::Connection::open(temp.path()).unwrap();
    conn.execute_batch(
        "CREATE TABLE review_text (name TEXT); INSERT INTO review_text VALUES ('hello');",
    )
    .unwrap();
    let read = |query: &str| {
        let opts = JdbcOptions::from_options_map(&HashMap::from([
            (
                "url".into(),
                format!("jdbc:sqlite:{}", temp.path().display()),
            ),
            ("query".into(), query.into()),
        ]))
        .unwrap();
        read_jdbc_to_polars(&opts).unwrap()
    };
    let full = read("SELECT * FROM review_text");
    let empty = read("SELECT * FROM review_text WHERE 0");
    assert_eq!(full.column("name").unwrap().dtype(), &DataType::String);
    assert_eq!(
        empty.schema(),
        full.schema(),
        "SOL-005: removing rows must not change the JDBC result schema"
    );
}
