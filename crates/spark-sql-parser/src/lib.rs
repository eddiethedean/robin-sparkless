//! Parse SQL into [sqlparser] AST.
//!
//! Supports a Spark-style subset: single-statement SELECT, CREATE SCHEMA/DATABASE,
//! and DROP TABLE/VIEW/SCHEMA, plus many DDL and utility statements (CREATE/ALTER/DROP
//! TABLE/VIEW/FUNCTION/SCHEMA, SHOW, INSERT, DESCRIBE, SET, RESET, CACHE, EXPLAIN, etc.).
//!
//! # SELECT and query compatibility
//!
//! Any statement that [sqlparser] parses as a `Query` (e.g. `SELECT`, `WITH ... SELECT`)
//! is accepted. Clause support is determined by [sqlparser] and the dialect in use
//! (this crate uses [GenericDialect](sqlparser::dialect::GenericDialect)).
//!
//! ## Known gaps
//!
//! Spark-specific query clauses such as `DISTRIBUTE BY`, `CLUSTER BY`, `SORT BY`
//! may not be recognized by the parser or may be rejected; behavior depends on
//! the upstream dialect and parser. Use single-statement queries only (one statement
//! per call).

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

/// Error returned when SQL parsing or validation fails.
#[derive(Error, Debug)]
#[error("{0}")]
pub struct ParseError(String);

/// Parse a single SQL statement (SELECT or DDL: CREATE SCHEMA / CREATE DATABASE / DROP TABLE/VIEW/SCHEMA).
///
/// Returns the [sqlparser::ast::Statement] on success. Only one statement per call;
/// run one statement at a time.
pub fn parse_sql(query: &str) -> Result<Statement, ParseError> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, query).map_err(|e| {
        ParseError(format!(
            "SQL parse error: {}. Hint: supported statements include SELECT, CREATE TABLE/VIEW/FUNCTION/SCHEMA/DATABASE, DROP TABLE/VIEW/SCHEMA.",
            e
        ))
    })?;
    if stmts.len() != 1 {
        return Err(ParseError(format!(
            "SQL: expected exactly one statement, got {}. Hint: run one statement at a time.",
            stmts.len()
        )));
    }
    let stmt = stmts.into_iter().next().expect("len == 1");
    match &stmt {
        Statement::Query(_) => {}
        Statement::CreateSchema { .. } | Statement::CreateDatabase { .. } => {}
        Statement::CreateTable(_) | Statement::CreateView(_) | Statement::CreateFunction(_) => {}
        Statement::AlterTable(_) | Statement::AlterView { .. } | Statement::AlterSchema(_) => {}
        Statement::Drop {
            object_type:
                sqlparser::ast::ObjectType::Table
                | sqlparser::ast::ObjectType::View
                | sqlparser::ast::ObjectType::Schema,
            ..
        } => {}
        Statement::DropFunction(_) => {}
        Statement::Use(_) | Statement::Truncate(_) | Statement::Declare { .. } => {}
        Statement::ShowTables { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowViews { .. }
        | Statement::ShowCreate { .. } => {}
        Statement::Insert(_) | Statement::Directory { .. } | Statement::LoadData { .. } => {}
        Statement::ExplainTable { .. } => {}
        Statement::Set(_) | Statement::Reset(_) => {}
        Statement::Cache { .. } | Statement::UNCache { .. } => {}
        Statement::Explain { .. } => {}
        _ => {
            return Err(ParseError(format!(
                "SQL: statement type not supported, got {:?}.",
                stmt
            )));
        }
    }
    Ok(stmt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{ObjectType, Statement};

    /// Assert that `sql` parses to the given statement variant.
    fn assert_parses_to<F>(sql: &str, check: F)
    where
        F: FnOnce(&Statement) -> bool,
    {
        let stmt = parse_sql(sql).unwrap_or_else(|e| panic!("parse_sql failed: {e}"));
        assert!(check(&stmt), "expected match for: {sql}");
    }

    // --- Error handling ---

    #[test]
    fn error_multiple_statements() {
        let err = parse_sql("SELECT 1; SELECT 2").unwrap_err();
        assert!(err.0.contains("expected exactly one statement"));
        assert!(err.0.contains("2"));
    }

    #[test]
    fn error_zero_statements() {
        let err = parse_sql("").unwrap_err();
        assert!(err.0.contains("expected exactly one statement") || err.0.contains("parse error"));
    }

    #[test]
    fn error_unsupported_statement_type() {
        // UPDATE is parsed by sqlparser but not in our whitelist
        let err = parse_sql("UPDATE t SET x = 1").unwrap_err();
        assert!(err.0.contains("not supported"));
    }

    #[test]
    fn error_syntax() {
        let err = parse_sql("SELECT FROM").unwrap_err();
        assert!(!err.0.is_empty());
    }

    // --- Queries ---

    #[test]
    fn query_select_simple() {
        assert_parses_to("SELECT 1", |s| matches!(s, Statement::Query(_)));
    }

    #[test]
    fn query_select_with_from() {
        assert_parses_to("SELECT a FROM t", |s| matches!(s, Statement::Query(_)));
    }

    #[test]
    fn query_with_cte() {
        assert_parses_to("WITH cte AS (SELECT 1) SELECT * FROM cte", |s| {
            matches!(s, Statement::Query(_))
        });
    }

    #[test]
    fn query_create_schema() {
        assert_parses_to("CREATE SCHEMA s", |s| {
            matches!(s, Statement::CreateSchema { .. })
        });
    }

    #[test]
    fn query_create_database() {
        assert_parses_to("CREATE DATABASE d", |s| {
            matches!(s, Statement::CreateDatabase { .. })
        });
    }

    // --- DDL: CREATE (issue #652) ---

    #[test]
    fn test_issue_652_create_table() {
        assert_parses_to("CREATE TABLE t (a INT)", |s| {
            matches!(s, Statement::CreateTable(_))
        });
    }

    #[test]
    fn test_issue_652_create_view() {
        assert_parses_to("CREATE VIEW v AS SELECT 1", |s| {
            matches!(s, Statement::CreateView(_))
        });
    }

    #[test]
    fn test_issue_652_create_function() {
        assert_parses_to("CREATE FUNCTION f() AS 'com.example.UDF'", |s| {
            matches!(s, Statement::CreateFunction(_))
        });
    }

    // --- DDL: ALTER (issue #653) ---

    #[test]
    fn test_issue_653_alter_table() {
        assert_parses_to("ALTER TABLE t ADD COLUMN c INT", |s| {
            matches!(s, Statement::AlterTable(_))
        });
    }

    #[test]
    fn test_issue_653_alter_view() {
        assert_parses_to("ALTER VIEW v AS SELECT 1", |s| {
            matches!(s, Statement::AlterView { .. })
        });
    }

    #[test]
    fn test_issue_653_alter_schema() {
        assert_parses_to("ALTER SCHEMA db RENAME TO db2", |s| {
            matches!(s, Statement::AlterSchema(_))
        });
    }

    // --- DDL: DROP (issue #654) ---

    #[test]
    fn test_issue_654_drop_table() {
        let stmt = parse_sql("DROP TABLE t").unwrap();
        match &stmt {
            Statement::Drop {
                object_type: ObjectType::Table,
                ..
            } => {}
            _ => panic!("expected Drop Table: {stmt:?}"),
        }
    }

    #[test]
    fn test_issue_654_drop_view() {
        let stmt = parse_sql("DROP VIEW v").unwrap();
        match &stmt {
            Statement::Drop {
                object_type: ObjectType::View,
                ..
            } => {}
            _ => panic!("expected Drop View: {stmt:?}"),
        }
    }

    #[test]
    fn test_issue_654_drop_schema() {
        let stmt = parse_sql("DROP SCHEMA s").unwrap();
        match &stmt {
            Statement::Drop {
                object_type: ObjectType::Schema,
                ..
            } => {}
            _ => panic!("expected Drop Schema: {stmt:?}"),
        }
    }

    #[test]
    fn test_issue_654_drop_function() {
        assert_parses_to("DROP FUNCTION f", |s| {
            matches!(s, Statement::DropFunction(_))
        });
    }

    // --- Utility: USE, TRUNCATE, DECLARE (issue #655) ---

    #[test]
    fn test_issue_655_use() {
        assert_parses_to("USE db1", |s| matches!(s, Statement::Use(_)));
    }

    #[test]
    fn test_issue_655_truncate() {
        assert_parses_to("TRUNCATE TABLE t", |s| matches!(s, Statement::Truncate(_)));
    }

    #[test]
    fn test_issue_655_declare() {
        assert_parses_to("DECLARE c CURSOR FOR SELECT 1", |s| {
            matches!(s, Statement::Declare { .. })
        });
    }

    // --- SHOW (issue #656) ---

    #[test]
    fn test_issue_656_show_tables() {
        assert_parses_to("SHOW TABLES", |s| matches!(s, Statement::ShowTables { .. }));
    }

    #[test]
    fn test_issue_656_show_databases() {
        assert_parses_to("SHOW DATABASES", |s| {
            matches!(s, Statement::ShowDatabases { .. })
        });
    }

    #[test]
    fn test_issue_656_show_schemas() {
        assert_parses_to("SHOW SCHEMAS", |s| {
            matches!(s, Statement::ShowSchemas { .. })
        });
    }

    #[test]
    fn test_issue_656_show_functions() {
        assert_parses_to("SHOW FUNCTIONS", |s| {
            matches!(s, Statement::ShowFunctions { .. })
        });
    }

    #[test]
    fn test_issue_656_show_columns() {
        assert_parses_to("SHOW COLUMNS FROM t", |s| {
            matches!(s, Statement::ShowColumns { .. })
        });
    }

    #[test]
    fn test_issue_656_show_views() {
        assert_parses_to("SHOW VIEWS", |s| matches!(s, Statement::ShowViews { .. }));
    }

    #[test]
    fn test_issue_656_show_create_table() {
        assert_parses_to("SHOW CREATE TABLE t", |s| {
            matches!(s, Statement::ShowCreate { .. })
        });
    }

    // --- INSERT / DIRECTORY (issue #657) ---

    #[test]
    fn test_issue_657_insert() {
        assert_parses_to("INSERT INTO t SELECT 1", |s| {
            matches!(s, Statement::Insert(_))
        });
    }

    #[test]
    fn test_issue_657_directory() {
        assert_parses_to("INSERT OVERWRITE DIRECTORY '/path' SELECT 1", |s| {
            matches!(s, Statement::Directory { .. })
        });
    }

    // --- DESCRIBE (issue #658) ---

    #[test]
    fn test_issue_658_describe_table() {
        assert_parses_to("DESCRIBE t", |s| {
            matches!(s, Statement::ExplainTable { .. })
        });
    }

    // --- SET, RESET, CACHE, UNCACHE (issue #659) ---

    #[test]
    fn test_issue_659_set() {
        assert_parses_to("SET x = 1", |s| matches!(s, Statement::Set(_)));
    }

    #[test]
    fn test_issue_659_reset() {
        assert_parses_to("RESET x", |s| matches!(s, Statement::Reset(_)));
    }

    #[test]
    fn test_issue_659_cache() {
        assert_parses_to("CACHE TABLE t", |s| matches!(s, Statement::Cache { .. }));
    }

    #[test]
    fn test_issue_659_uncache() {
        assert_parses_to("UNCACHE TABLE t", |s| {
            matches!(s, Statement::UNCache { .. })
        });
    }

    #[test]
    fn test_issue_659_uncache_if_exists() {
        assert_parses_to("UNCACHE TABLE IF EXISTS t", |s| {
            matches!(s, Statement::UNCache { .. })
        });
    }

    // --- EXPLAIN (issue #660) ---

    #[test]
    fn test_issue_660_explain() {
        assert_parses_to("EXPLAIN SELECT 1", |s| {
            matches!(s, Statement::Explain { .. })
        });
    }
}
