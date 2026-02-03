//! SQL parsing and translation to DataFrame operations.
//! Compiled only when the `sql` feature is enabled.

mod parser;
mod translator;

use crate::dataframe::DataFrame;
use crate::session::SparkSession;
use polars::prelude::PolarsError;

/// Parse a SQL string and execute it using the session's catalog.
/// Supports: SELECT (columns or *), FROM single table or two-table JOIN,
/// WHERE (basic predicates), GROUP BY + aggregates, ORDER BY, LIMIT.
pub fn execute_sql(session: &SparkSession, query: &str) -> Result<DataFrame, PolarsError> {
    let stmt = parser::parse_sql(query)?;
    translator::translate(session, &stmt)
}

pub use parser::parse_sql;
pub use translator::translate;

#[cfg(test)]
mod tests {
    use crate::SparkSession;

    #[test]
    fn test_sql_select_from_temp_view() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 25, "Alice".to_string()),
                    (2, 30, "Bob".to_string()),
                    (3, 35, "Carol".to_string()),
                ],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark.sql("SELECT id, name FROM t WHERE age > 26").unwrap();
        let cols = result.columns().unwrap();
        assert_eq!(cols, vec!["id", "name"]);
        assert_eq!(result.count().unwrap(), 2);
    }

    #[test]
    fn test_sql_select_star() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![(1, 10, "a".to_string()), (2, 20, "b".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("v", df);
        let result = spark.sql("SELECT * FROM v").unwrap();
        assert_eq!(result.columns().unwrap(), vec!["id", "age", "name"]);
        assert_eq!(result.count().unwrap(), 2);
    }

    #[test]
    fn test_sql_group_by_count() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 1, "a".to_string()),
                    (2, 1, "b".to_string()),
                    (3, 2, "c".to_string()),
                ],
                vec!["id", "grp", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark
            .sql("SELECT grp, COUNT(id) FROM t GROUP BY grp ORDER BY grp")
            .unwrap();
        assert_eq!(result.count().unwrap(), 2);
    }

    #[test]
    fn test_sql_table_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let result = spark.sql("SELECT 1 FROM nonexistent");
        assert!(result.is_err());
    }
}
