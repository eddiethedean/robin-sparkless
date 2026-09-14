use robin_sparkless_polars::jdbc::{JdbcOptions, read_jdbc_to_polars};
use std::collections::HashMap;

fn compare_query_schema(query: &str) {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let conn = rusqlite::Connection::open(temp.path()).unwrap();
    conn.execute_batch(
        "CREATE TABLE review_text (name TEXT); INSERT INTO review_text VALUES ('hello');",
    )
    .unwrap();
    let read = |q: &str| {
        let opts = JdbcOptions::from_options_map(&HashMap::from([
            (
                "url".into(),
                format!("jdbc:sqlite:{}", temp.path().display()),
            ),
            ("query".into(), q.into()),
        ]))
        .unwrap();
        read_jdbc_to_polars(&opts).unwrap()
    };
    let full = read(query);
    let empty = read(&format!("{query} WHERE 0"));
    println!("query={query}; full={full:?}; empty={empty:?}");
    assert_eq!(empty.schema(), full.schema());
}

#[test]
fn qualified_alias_empty_schema() {
    compare_query_schema("SELECT t.name AS alias FROM review_text t");
}

#[test]
fn computed_alias_is_not_mistaken_for_source_column() {
    compare_query_schema("SELECT length(name) AS name FROM review_text");
}
