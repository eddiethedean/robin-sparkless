#![cfg(feature = "sqlite")]

use polars::prelude::DataType;
use robin_sparkless_polars::jdbc::{JdbcOptions, read_jdbc_to_polars};
use std::collections::HashMap;

#[test]
fn sol_005_qualified_wildcard_uses_the_selected_tables_type() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let conn = rusqlite::Connection::open(temp.path()).unwrap();
    conn.execute_batch("CREATE TABLE left_text (name TEXT); INSERT INTO left_text VALUES ('x'); CREATE TABLE right_int (name INTEGER); INSERT INTO right_int VALUES (7);").unwrap();
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
    let query = "SELECT b.* FROM left_text a JOIN right_int b ON 1 = 1";
    let full = read(query);
    let empty = read(&format!("{query} WHERE 0"));
    assert_eq!(full.column("name").unwrap().dtype(), &DataType::Int64);
    assert_eq!(
        empty.schema(),
        full.schema(),
        "SOL-005: wildcard metadata must come from the selected table, not the first FROM table"
    );
}
