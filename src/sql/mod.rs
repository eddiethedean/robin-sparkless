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
pub use translator::{expr_string_to_polars, translate};

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
    fn test_sql_group_by_expression() {
        // Issue #588: GROUP BY (age > 30) — expression instead of column name.
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 25, "a".to_string()),
                    (2, 35, "b".to_string()),
                    (3, 28, "c".to_string()),
                ],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark
            .sql("SELECT COUNT(*) as count FROM t GROUP BY (age > 30)")
            .unwrap();
        assert_eq!(result.count().unwrap(), 2);
    }

    #[test]
    fn test_sql_scalar_aggregate() {
        // Issue #587: SELECT AVG(salary) FROM t (no GROUP BY) — scalar aggregation.
        // create_dataframe takes (i64, i64, String) -> columns ["id", "salary", "name"]
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![(1, 100, "Alice".to_string()), (2, 200, "Bob".to_string())],
                vec!["id", "salary", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("test_table", df);
        let result = spark
            .sql("SELECT AVG(salary) as avg_salary FROM test_table")
            .unwrap();
        assert_eq!(result.count().unwrap(), 1);
        let rows = result.collect_as_json_rows().unwrap();
        let avg_val = rows[0].get("avg_salary").and_then(|v| v.as_f64()).unwrap();
        assert!((avg_val - 150.0).abs() < 1e-9);
    }

    #[test]
    fn test_sql_having() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 25, "a".to_string()),
                    (2, 25, "b".to_string()),
                    (3, 30, "c".to_string()),
                    (4, 35, "d".to_string()),
                ],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark
            .sql("SELECT age, COUNT(id) FROM t GROUP BY age HAVING age > 26")
            .unwrap();
        assert_eq!(result.count().unwrap(), 2);
        let rows = result.collect_as_json_rows().unwrap();
        let ages: Vec<i64> = rows
            .iter()
            .map(|r| r.get("age").and_then(|v| v.as_i64()).unwrap())
            .collect();
        assert!(ages.contains(&30));
        assert!(ages.contains(&35));
        assert!(!ages.contains(&25));
    }

    #[test]
    fn test_sql_having_agg() {
        // Issue #589: HAVING with aggregate expression (e.g. HAVING AVG(salary) > 55000).
        // create_dataframe takes (i64, i64, String) -> columns ["dummy", "salary", "dept"]
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (0, 50000, "A".to_string()),
                    (0, 60000, "A".to_string()),
                    (0, 40000, "B".to_string()),
                ],
                vec!["dummy", "salary", "dept"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark
            .sql("SELECT dept, AVG(salary) as avg_sal FROM t GROUP BY dept HAVING AVG(salary) >= 55000")
            .unwrap();
        assert_eq!(result.count().unwrap(), 1);
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("dept").and_then(|v| v.as_str()).unwrap(), "A");
    }

    #[test]
    fn test_sql_where_like_and_in() {
        // Issue #590: WHERE with LIKE and IN.
        // create_dataframe takes (i64, i64, String) -> columns ["id", "dummy", "name"]
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 0, "Alice".to_string()),
                    (2, 0, "Bob".to_string()),
                    (3, 0, "Carol".to_string()),
                ],
                vec!["id", "dummy", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let like_result = spark.sql("SELECT * FROM t WHERE name LIKE 'A%'").unwrap();
        assert_eq!(like_result.count().unwrap(), 1);
        let rows = like_result.collect_as_json_rows().unwrap();
        assert_eq!(
            rows[0].get("name").and_then(|v| v.as_str()).unwrap(),
            "Alice"
        );
        let in_result = spark.sql("SELECT * FROM t WHERE id IN (1, 2)").unwrap();
        assert_eq!(in_result.count().unwrap(), 2);
    }

    #[test]
    fn test_sql_table_not_found() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let result = spark.sql("SELECT 1 FROM nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_udf_select() {
        use polars::prelude::DataType;

        let spark = SparkSession::builder().app_name("test").get_or_create();
        spark
            .register_udf("to_str", |cols| cols[0].cast(&DataType::String))
            .unwrap();
        let df = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string()), (2, 30, "Bob".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark
            .sql("SELECT id, to_str(id) AS id_str, name FROM t")
            .unwrap();
        let cols = result.columns().unwrap();
        assert!(cols.contains(&"id_str".to_string()));
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("id_str").and_then(|v| v.as_str()), Some("1"));
    }

    #[test]
    fn test_sql_builtin_upper() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![(1, 25, "alice".to_string()), (2, 30, "bob".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark
            .sql("SELECT id, UPPER(name) AS upper_name FROM t ORDER BY id")
            .unwrap();
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(
            rows[0].get("upper_name").and_then(|v| v.as_str()),
            Some("ALICE")
        );
    }

    #[test]
    fn test_sql_from_global_temp_view() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![(1, 10, "a".to_string()), (2, 20, "b".to_string())],
                vec!["id", "v", "name"],
            )
            .unwrap();
        spark.create_or_replace_global_temp_view("gv", df);
        let result = spark
            .sql("SELECT * FROM global_temp.gv ORDER BY id")
            .unwrap();
        assert_eq!(result.count().unwrap(), 2);
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("a"));
        assert_eq!(rows[1].get("name").and_then(|v| v.as_str()), Some("b"));
    }

    /// Case-insensitive column resolution (PySpark default; issue #194).
    #[test]
    fn test_sql_create_schema_ddl() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        // CREATE SCHEMA persists name; returns empty DataFrame (issue #347).
        let out = spark.sql("CREATE SCHEMA my_schema").unwrap();
        assert_eq!(out.count().unwrap(), 0);
        assert!(out.columns().unwrap().is_empty());
        assert!(spark.database_exists("my_schema"));
        assert!(
            spark
                .list_database_names()
                .contains(&"my_schema".to_string())
        );
    }

    #[test]
    fn test_sql_create_database_ddl() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let out = spark.sql("CREATE DATABASE my_db").unwrap();
        assert_eq!(out.count().unwrap(), 0);
        assert!(out.columns().unwrap().is_empty());
        assert!(spark.database_exists("my_db"));
        assert!(spark.list_database_names().contains(&"my_db".to_string()));
    }

    #[test]
    fn test_sql_drop_table_ddl() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        // DROP TABLE IF EXISTS (no error when table does not exist)
        let out = spark
            .sql("DROP TABLE IF EXISTS my_schema.my_table")
            .unwrap();
        assert_eq!(out.count().unwrap(), 0);
        // Create a temp view then DROP TABLE
        let df = spark
            .create_dataframe(vec![(1i64, 10i64, "a".to_string())], vec!["id", "v", "x"])
            .unwrap();
        spark.create_or_replace_temp_view("t_drop_me", df.clone());
        assert!(spark.table("t_drop_me").is_ok());
        let _ = spark.sql("DROP TABLE t_drop_me").unwrap();
        assert!(spark.table("t_drop_me").is_err());
    }

    #[test]
    fn test_sql_drop_schema() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        // CREATE then DROP SCHEMA (issue #526; sqlparser 0.45 has no DROP DATABASE token)
        spark
            .sql("CREATE SCHEMA IF NOT EXISTS test_schema_to_drop")
            .unwrap();
        assert!(spark.database_exists("test_schema_to_drop"));
        spark
            .sql("DROP SCHEMA IF EXISTS test_schema_to_drop CASCADE")
            .unwrap();
        assert!(!spark.database_exists("test_schema_to_drop"));
    }

    #[test]
    fn test_sql_case_insensitive_columns() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1, 25, "Alice".to_string()),
                    (2, 30, "Bob".to_string()),
                    (3, 35, "Charlie".to_string()),
                ],
                vec!["Id", "Age", "Name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        // SQL with lowercase column names resolves to Id, Age, Name
        let result = spark
            .sql("SELECT name, age FROM t WHERE age > 26 ORDER BY age")
            .unwrap();
        assert_eq!(result.count().unwrap(), 2);
        let cols = result.columns().unwrap();
        assert_eq!(cols, vec!["name", "age"]);
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("Bob"));
        assert_eq!(rows[0].get("age").and_then(|v| v.as_i64()), Some(30));
    }
}
