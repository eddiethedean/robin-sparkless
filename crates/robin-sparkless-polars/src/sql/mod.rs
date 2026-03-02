//! SQL parsing and translation to DataFrame operations.
//! Parsing is provided by the `spark-sql-parser` crate; this module translates AST to DataFrame ops.
//! Compiled only when the `sql` feature is enabled.

mod translator;

use crate::dataframe::DataFrame;
use crate::session::SparkSession;
use polars::prelude::PolarsError;
/// SQL AST statement type; re-exported for API consumers (e.g. root crate).
pub use sqlparser::ast::Statement;

/// Parse a single SQL statement using [spark_sql_parser]. Returns PolarsError for compatibility with session/translator.
/// On parse failure, error message includes supported statements (#706, #701).
fn parse_sql_to_statement(query: &str) -> Result<Statement, PolarsError> {
    spark_sql_parser::parse_sql(query).map_err(|e| {
        let msg = e.to_string();
        let hint = " Only SELECT, CREATE SCHEMA/DATABASE, and DROP TABLE/VIEW/SCHEMA/DATABASE are supported for execution.";
        PolarsError::InvalidOperation(
            format!("{msg}{hint}").into(),
        )
    })
}

/// Parse a single SQL statement (SELECT or DDL: CREATE SCHEMA / CREATE DATABASE / DROP TABLE).
/// Delegates to the [spark-sql-parser](https://crates.io/crates/spark-sql-parser) crate.
pub fn parse_sql(query: &str) -> Result<Statement, PolarsError> {
    parse_sql_to_statement(query)
}

/// Parse a SQL string and execute it using the session's catalog.
/// Supports: SELECT (columns or *), FROM single table or two-table JOIN,
/// WHERE (basic predicates), GROUP BY + aggregates, ORDER BY, LIMIT,
/// DESCRIBE DETAIL table_name (Delta Lake; requires delta feature).
pub fn execute_sql(session: &SparkSession, query: &str) -> Result<DataFrame, PolarsError> {
    // Handle DESCRIBE DETAIL before parsing so we return Delta/table-not-found errors, not SQL parse error (#768, #773).
    const PREFIX: &str = "DESCRIBE DETAIL ";
    let q = query.trim();
    if q.len() > PREFIX.len()
        && q.get(..PREFIX.len())
            .map(|s| s.eq_ignore_ascii_case(PREFIX))
            == Some(true)
    {
        let table_name = q[PREFIX.len()..].trim();
        if table_name.is_empty() {
            return Err(PolarsError::InvalidOperation(
                "DESCRIBE DETAIL requires a table name.".into(),
            ));
        }
        #[cfg(feature = "delta")]
        {
            if let Some(path) = session.resolve_delta_table_path(table_name) {
                return crate::delta::describe_delta_detail(
                    path,
                    Some(table_name),
                    session.is_case_sensitive(),
                );
            }
            if session.table(table_name).is_ok() {
                return Err(PolarsError::InvalidOperation(
                    format!(
                        "DESCRIBE DETAIL: table '{table_name}' is not a Delta table in the warehouse. \
                         Set spark.sql.warehouse.dir and use a Delta table at {{warehouse}}/{table_name}."
                    )
                    .into(),
                ));
            }
            return Err(PolarsError::InvalidOperation(
                format!(
                    "Table or view '{table_name}' not found. Register it with create_or_replace_temp_view or saveAsTable. \
                    (Schema-qualified names like 'schema.table' are supported.)"
                )
                .into(),
            ));
        }
        #[cfg(not(feature = "delta"))]
        {
            return Err(PolarsError::InvalidOperation(
                "DESCRIBE DETAIL requires the delta feature.".into(),
            ));
        }
    }

    // SHOW DATABASES / SHOW TABLES [IN db] (PySpark parity; issue #1046).
    let q_upper = q.to_ascii_uppercase();
    const SHOW_DATABASES_PREFIX: &str = "SHOW DATABASES";
    if q_upper.len() >= SHOW_DATABASES_PREFIX.len()
        && q_upper.get(..SHOW_DATABASES_PREFIX.len()) == Some(SHOW_DATABASES_PREFIX)
    {
        return translator::translate_show_databases(session);
    }
    const SHOW_TABLES_PREFIX: &str = "SHOW TABLES";
    if q_upper.len() >= SHOW_TABLES_PREFIX.len()
        && q_upper.get(..SHOW_TABLES_PREFIX.len()) == Some(SHOW_TABLES_PREFIX)
    {
        let rest = q[SHOW_TABLES_PREFIX.len()..].trim();
        let mut db: Option<&str> = None;
        if !rest.is_empty() {
            let parts: Vec<&str> = rest.split_whitespace().collect();
            if parts.len() >= 2
                && (parts[0].eq_ignore_ascii_case("IN") || parts[0].eq_ignore_ascii_case("FROM"))
            {
                db = Some(parts[1]);
            }
        }
        return translator::translate_show_tables(session, db);
    }

    // DESCRIBE table_name [col_name] (PySpark 3.5: optional column). Parser only accepts "DESCRIBE t".
    const DESCRIBE_PREFIX: &str = "DESCRIBE ";
    const DESC_PREFIX: &str = "DESC ";
    const DESC_DETAIL_PREFIX: &str = "DESC DETAIL ";
    let is_describe = q.len() >= DESCRIBE_PREFIX.len()
        && q.get(..DESCRIBE_PREFIX.len())
            .map(|s| s.eq_ignore_ascii_case(DESCRIBE_PREFIX))
            == Some(true)
        && q.get(..PREFIX.len())
            .map(|s| s.eq_ignore_ascii_case(PREFIX))
            != Some(true);
    let is_desc = q.len() >= DESC_PREFIX.len()
        && q.get(..DESC_PREFIX.len())
            .map(|s| s.eq_ignore_ascii_case(DESC_PREFIX))
            == Some(true)
        && (q.len() < DESC_DETAIL_PREFIX.len()
            || q.get(..DESC_DETAIL_PREFIX.len())
                .map(|s| s.eq_ignore_ascii_case(DESC_DETAIL_PREFIX))
                != Some(true));
    let rest = if is_describe {
        q[DESCRIBE_PREFIX.len()..].trim()
    } else if is_desc {
        q[DESC_PREFIX.len()..].trim()
    } else {
        ""
    };
    if !rest.is_empty() && (is_describe || is_desc) {
        let parts: Vec<&str> = rest.split_whitespace().collect();
        // #1013: DESCRIBE [TABLE] [EXTENDED] table_name [col_name] — skip TABLE/EXTENDED to get table name.
        let idx = parts
            .iter()
            .position(|p| !p.eq_ignore_ascii_case("TABLE") && !p.eq_ignore_ascii_case("EXTENDED"));
        let table_name = idx.and_then(|i| parts.get(i)).copied().unwrap_or("");
        let col_name = idx.and_then(|i| parts.get(i + 1)).copied();
        if !table_name.is_empty() {
            return translator::translate_describe_table_optional_col(
                session, table_name, col_name,
            );
        }
    }

    let stmt = parse_sql_to_statement(query)?;
    translator::translate(session, &stmt)
}

/// Parse multiple selectExpr strings via SQL (e.g. "upper(Name) as u"). Registers df as __selectexpr_t, parses each expr, then drops the temp view. PySpark selectExpr parity.
pub fn parse_select_exprs(
    session: &SparkSession,
    df: &DataFrame,
    exprs: &[String],
) -> Result<Vec<polars::prelude::Expr>, PolarsError> {
    const TMP_VIEW: &str = "__selectexpr_t";
    session.create_or_replace_temp_view(TMP_VIEW, df.clone());
    let mut result = Vec::with_capacity(exprs.len());
    let ok = (|| {
        for e in exprs {
            result.push(translator::expr_string_to_polars(e, session, df)?);
        }
        Ok::<(), PolarsError>(())
    })();
    session.drop_temp_view(TMP_VIEW);
    ok?;
    Ok(result)
}

pub use translator::{expr_string_to_polars, translate};

#[cfg(test)]
mod tests {
    use super::Statement;
    use crate::sql::parse_sql;
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
    fn test_sql_show_statement_variants() {
        let stmt_db = parse_sql("SHOW DATABASES").unwrap();
        println!("SHOW DATABASES parsed as: {:?}", stmt_db);
        let stmt_tables = parse_sql("SHOW TABLES").unwrap();
        println!("SHOW TABLES parsed as: {:?}", stmt_tables);
    }

    /// Duplicate output names (e.g. two COUNT(*)) are disambiguated to count, count_1.
    #[test]
    fn test_sql_duplicate_agg_aliases() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1i64, 10i64, "a".to_string()),
                    (2i64, 20i64, "b".to_string()),
                ],
                vec!["id", "v", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark
            .sql("SELECT COUNT(*) AS count, COUNT(id) AS count FROM t")
            .unwrap();
        let cols = result.columns().unwrap();
        assert_eq!(cols.len(), 2);
        assert!(cols.contains(&"count".to_string()));
        assert!(cols.contains(&"count_1".to_string()));
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("count").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(rows[0].get("count_1").and_then(|v| v.as_i64()), Some(2));
    }

    /// #1019: SELECT with duplicate output names (e.g. self-join l.manager_id, r.manager_id) disambiguates to manager_id, manager_id_1.
    #[test]
    fn test_sql_duplicate_projection_aliases() {
        use serde_json::json;
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "string".to_string()),
            ("manager_id".to_string(), "bigint".to_string()),
        ];
        let rows = vec![
            vec![json!(1), json!("Alice"), json!(0)],
            vec![json!(2), json!("Bob"), json!(1)],
        ];
        let emp = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        spark.create_or_replace_temp_view("e", emp.clone());
        spark.create_or_replace_temp_view("m", emp);
        let result = spark
            .sql("SELECT e.manager_id, m.manager_id FROM e LEFT JOIN m ON e.manager_id = m.id")
            .unwrap();
        let cols = result.columns().unwrap();
        assert_eq!(
            cols.len(),
            2,
            "disambiguate duplicate manager_id to manager_id, manager_id_1"
        );
        assert!(cols.contains(&"manager_id".to_string()));
        assert!(cols.contains(&"manager_id_1".to_string()));
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

    /// DESCRIBE DETAIL for Delta Lake (issue #678).
    #[cfg(feature = "delta")]
    #[test]
    fn test_sql_describe_detail_delta() {
        let dir = tempfile::tempdir().unwrap();
        let warehouse = dir.path().join("wh");
        std::fs::create_dir_all(&warehouse).unwrap();
        let table_path = warehouse.join("test_detail_basic");

        let spark = SparkSession::builder()
            .app_name("test")
            .config(
                "spark.sql.warehouse.dir",
                warehouse.as_os_str().to_str().unwrap(),
            )
            .get_or_create();

        let df = spark
            .create_dataframe(
                vec![(1, 25, "Alice".to_string()), (2, 30, "Bob".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        crate::delta::write_delta(
            df.collect_inner().unwrap().as_ref(),
            &table_path,
            true,
            false,
        )
        .unwrap();

        let result = spark.sql("DESCRIBE DETAIL test_detail_basic").unwrap();
        assert_eq!(result.count().unwrap(), 1);
        let rows = result.collect_as_json_rows().unwrap();
        let row = &rows[0];
        assert_eq!(row.get("format").and_then(|v| v.as_str()), Some("delta"));
        assert!(
            row.get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.contains("test_detail_basic"))
                == Some(true)
        );
        assert!(row.get("numFiles").and_then(|v| v.as_i64()).unwrap_or(-1) >= 0);
        assert!(
            row.get("sizeInBytes")
                .and_then(|v| v.as_i64())
                .unwrap_or(-1)
                >= 0
        );
        assert!(row.get("minReaderVersion").is_some());
        assert!(row.get("minWriterVersion").is_some());
    }

    /// DESCRIBE table_name: returns col_name, data_type (PySpark DESCRIBE parity).
    #[test]
    fn test_sql_describe_table() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1i64, 25i64, "Alice".to_string()),
                    (2i64, 30i64, "Bob".to_string()),
                ],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark.sql("DESCRIBE t").unwrap();
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("col_name").and_then(|v| v.as_str()), Some("id"));
        assert_eq!(
            rows[0].get("data_type").and_then(|v| v.as_str()),
            Some("long")
        );
        assert_eq!(
            rows[1].get("col_name").and_then(|v| v.as_str()),
            Some("age")
        );
        assert_eq!(
            rows[1].get("data_type").and_then(|v| v.as_str()),
            Some("long")
        );
        assert_eq!(
            rows[2].get("col_name").and_then(|v| v.as_str()),
            Some("name")
        );
        assert_eq!(
            rows[2].get("data_type").and_then(|v| v.as_str()),
            Some("string")
        );
    }

    /// DESCRIBE table_name col_name: returns single column row (PySpark 3.5 parity).
    #[test]
    fn test_sql_describe_table_column() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1i64, 25i64, "Alice".to_string()),
                    (2i64, 30i64, "Bob".to_string()),
                ],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark.sql("DESCRIBE t age").unwrap();
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("col_name").and_then(|v| v.as_str()),
            Some("age")
        );
        assert_eq!(
            rows[0].get("data_type").and_then(|v| v.as_str()),
            Some("long")
        );
    }

    /// DESCRIBE TABLE EXTENDED table_name (#1013): TABLE/EXTENDED skipped to get table name.
    #[test]
    fn test_sql_describe_table_extended() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![(1i64, 25i64, "a".to_string())],
                vec!["id", "age", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark.sql("DESCRIBE TABLE EXTENDED t").unwrap();
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("col_name").and_then(|v| v.as_str()), Some("id"));
        assert_eq!(
            rows[1].get("col_name").and_then(|v| v.as_str()),
            Some("age")
        );
        assert_eq!(
            rows[2].get("col_name").and_then(|v| v.as_str()),
            Some("name")
        );
    }

    /// CREATE OR REPLACE TEMP VIEW ... AS SELECT (#1011).
    #[test]
    fn test_sql_create_or_replace_temp_view() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1i64, 10i64, "x".to_string()),
                    (2i64, 20i64, "y".to_string()),
                ],
                vec!["id", "age", "label"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("base", df);
        spark
            .sql("CREATE OR REPLACE TEMP VIEW v AS SELECT id, label FROM base WHERE id > 1")
            .unwrap();
        let result = spark.sql("SELECT * FROM v").unwrap();
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&serde_json::json!(2)));
        assert_eq!(rows[0].get("label"), Some(&serde_json::json!("y")));
    }

    #[test]
    fn test_sql_left_anti_join() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let left = spark
            .create_dataframe(
                vec![
                    (1i64, 10i64, "a".to_string()),
                    (2i64, 20i64, "b".to_string()),
                    (3i64, 30i64, "c".to_string()),
                ],
                vec!["id", "v", "name"],
            )
            .unwrap();
        let right = spark
            .create_dataframe(
                vec![(1i64, 100i64, "x".to_string())],
                vec!["id", "v", "label"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("l", left);
        spark.create_or_replace_temp_view("r", right);
        // LEFT ANTI: rows in l with no match in r on id -> id 2 and 3
        let result = spark
            .sql("SELECT l.id, l.name FROM l LEFT ANTI JOIN r ON l.id = r.id")
            .unwrap();
        assert_eq!(result.count().unwrap(), 2);
        let rows = result.collect_as_json_rows().unwrap();
        let ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.get("id").and_then(|v| v.as_i64()))
            .collect();
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
    }

    /// PR-2/#776: CASE ... END AS alias in SELECT.
    #[test]
    fn test_sql_case_with_alias() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1i64, 10i64, "".to_string()),
                    (2i64, 20i64, "".to_string()),
                    (3i64, 30i64, "".to_string()),
                ],
                vec!["id", "v", "x"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);
        let result = spark
            .sql("SELECT id, CASE WHEN id = 1 THEN 'one' WHEN id = 2 THEN 'two' ELSE 'other' END AS label FROM t")
            .unwrap();
        assert_eq!(result.count().unwrap(), 3);
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("label").and_then(|v| v.as_str()), Some("one"));
        assert_eq!(rows[1].get("label").and_then(|v| v.as_str()), Some("two"));
        assert_eq!(rows[2].get("label").and_then(|v| v.as_str()), Some("other"));
    }

    /// PR-3/#730,#744: UPDATE and DELETE are supported; they modify the table in the session catalog.
    #[test]
    fn test_sql_update_delete_supported() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let df = spark
            .create_dataframe(
                vec![
                    (1i64, 10i64, "a".to_string()),
                    (2i64, 20i64, "b".to_string()),
                ],
                vec!["id", "v", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("t", df);

        // UPDATE: set v = 2 where name = 'a'
        spark.sql("UPDATE t SET v = 2 WHERE name = 'a'").unwrap();
        let result = spark.sql("SELECT id, v, name FROM t ORDER BY id").unwrap();
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("v").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(rows[1].get("v").and_then(|v| v.as_i64()), Some(20));

        // DELETE: remove row where id = 1
        spark.sql("DELETE FROM t WHERE id = 1").unwrap();
        let result2 = spark.sql("SELECT id, v, name FROM t ORDER BY id").unwrap();
        let rows2 = result2.collect_as_json_rows().unwrap();
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0].get("id").and_then(|v| v.as_i64()), Some(2));
    }

    /// PR-2/#743: JOIN ON with different column names (e.g. a.id = b.other_id).
    #[test]
    fn test_sql_join_on_different_column_names() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let left = spark
            .create_dataframe(
                vec![(1i64, 0i64, "a".to_string()), (2i64, 0i64, "b".to_string())],
                vec!["id", "v", "name"],
            )
            .unwrap();
        let right = spark
            .create_dataframe(
                vec![(1i64, 0i64, "x".to_string()), (3i64, 0i64, "z".to_string())],
                vec!["other_id", "v", "tag"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("l", left);
        spark.create_or_replace_temp_view("r", right);
        let result = spark
            .sql("SELECT l.id, l.name, r.tag FROM l INNER JOIN r ON l.id = r.other_id")
            .unwrap();
        assert_eq!(result.count().unwrap(), 1);
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("id").and_then(|v| v.as_i64()), Some(1));
        assert_eq!(rows[0].get("tag").and_then(|v| v.as_str()), Some("x"));
    }

    /// Batch 4 / #708: LEFT JOIN supported; returns all left rows with nulls for non-matching right.
    #[test]
    fn test_sql_left_join_supported() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let left = spark
            .create_dataframe(
                vec![
                    (1i64, 0i64, "a".to_string()),
                    (2i64, 0i64, "b".to_string()),
                    (3i64, 0i64, "c".to_string()),
                ],
                vec!["id", "_v", "name"],
            )
            .unwrap();
        let right = spark
            .create_dataframe(
                vec![(1i64, 0i64, "X".to_string()), (3i64, 0i64, "Z".to_string())],
                vec!["id", "_v", "tag"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("l", left);
        spark.create_or_replace_temp_view("r", right);
        let result = spark
            .sql("SELECT l.id, l.name, r.tag FROM l LEFT JOIN r ON l.id = r.id")
            .unwrap();
        assert_eq!(result.count().unwrap(), 3);
        let rows = result.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("tag").and_then(|v| v.as_str()), Some("X"));
        assert_eq!(rows[1].get("tag"), Some(&serde_json::Value::Null));
        assert_eq!(rows[2].get("tag").and_then(|v| v.as_str()), Some("Z"));
    }

    /// PR-B/#774: SQL UNION and UNION ALL.
    #[test]
    fn test_sql_union_all() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let a = spark
            .create_dataframe(
                vec![
                    (1i64, 10i64, "x".to_string()),
                    (2i64, 20i64, "y".to_string()),
                ],
                vec!["id", "v", "name"],
            )
            .unwrap();
        let b = spark
            .create_dataframe(
                vec![(3i64, 30i64, "z".to_string())],
                vec!["id", "v", "name"],
            )
            .unwrap();
        spark.create_or_replace_temp_view("a", a);
        spark.create_or_replace_temp_view("b", b);
        let result = spark
            .sql("SELECT id, name FROM a UNION ALL SELECT id, name FROM b")
            .unwrap();
        assert_eq!(result.count().unwrap(), 3);
        let result_union = spark
            .sql("SELECT id FROM a UNION SELECT id FROM b")
            .unwrap();
        assert_eq!(result_union.count().unwrap(), 3);
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

    /// #706, #701: Unsupported SQL returns clear error mentioning supported statements.
    #[test]
    fn test_sql_unsupported_statement_clear_error() {
        let spark = SparkSession::builder().app_name("test").get_or_create();
        let err = match spark.sql("INSERT INTO t SELECT 1") {
            Ok(_) => panic!("INSERT should not be supported"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("supported") || msg.contains("SELECT"),
            "error should mention supported statements: {}",
            msg
        );
        let err2 = match spark.sql("COMMIT") {
            Ok(_) => panic!("COMMIT should not be supported"),
            Err(e) => e,
        };
        let msg2 = err2.to_string();
        assert!(
            msg2.contains("supported") || msg2.contains("not supported"),
            "error should mention supported/not supported: {}",
            msg2
        );
    }
}
