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
    let stmt = stmts.into_iter().next().unwrap();
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
    use sqlparser::ast::Statement;

    #[test]
    fn test_issue_652_create_table() {
        let stmt = parse_sql("CREATE TABLE t (a INT)").unwrap();
        assert!(matches!(stmt, Statement::CreateTable(_)));
    }

    #[test]
    fn test_issue_652_create_view() {
        let stmt = parse_sql("CREATE VIEW v AS SELECT 1").unwrap();
        assert!(matches!(stmt, Statement::CreateView(_)));
    }

    #[test]
    fn test_issue_652_create_function() {
        // sqlparser expects parentheses for the parameter list (possibly empty)
        let stmt = parse_sql("CREATE FUNCTION f() AS 'com.example.UDF'").unwrap();
        assert!(matches!(stmt, Statement::CreateFunction(_)));
    }

    #[test]
    fn test_issue_653_alter_table() {
        let stmt = parse_sql("ALTER TABLE t ADD COLUMN c INT").unwrap();
        assert!(matches!(stmt, Statement::AlterTable(_)));
    }

    #[test]
    fn test_issue_653_alter_view() {
        let stmt = parse_sql("ALTER VIEW v AS SELECT 1").unwrap();
        assert!(matches!(stmt, Statement::AlterView { .. }));
    }

    #[test]
    fn test_issue_653_alter_schema() {
        // sqlparser expects an operation like RENAME; SET LOCATION may not be supported
        let stmt = parse_sql("ALTER SCHEMA db RENAME TO db2").unwrap();
        assert!(matches!(stmt, Statement::AlterSchema(_)));
    }

    #[test]
    fn test_issue_654_drop_function() {
        // DROP DATABASE is already supported via Drop(Schema)
        let stmt = parse_sql("DROP FUNCTION f").unwrap();
        assert!(matches!(stmt, Statement::DropFunction(_)));
    }

    #[test]
    fn test_issue_655_use() {
        let stmt = parse_sql("USE db1").unwrap();
        assert!(matches!(stmt, Statement::Use(_)));
    }

    #[test]
    fn test_issue_655_truncate() {
        let stmt = parse_sql("TRUNCATE TABLE t").unwrap();
        assert!(matches!(stmt, Statement::Truncate(_)));
    }

    #[test]
    fn test_issue_655_declare() {
        // sqlparser Declare is for cursor/statement list; variable declaration may differ
        let stmt = parse_sql("DECLARE c CURSOR FOR SELECT 1").unwrap();
        assert!(matches!(stmt, Statement::Declare { .. }));
    }

    #[test]
    fn test_issue_656_show_tables() {
        let stmt = parse_sql("SHOW TABLES").unwrap();
        assert!(matches!(stmt, Statement::ShowTables { .. }));
    }

    #[test]
    fn test_issue_656_show_databases() {
        let stmt = parse_sql("SHOW DATABASES").unwrap();
        assert!(matches!(stmt, Statement::ShowDatabases { .. }));
    }

    #[test]
    fn test_issue_656_show_functions() {
        let stmt = parse_sql("SHOW FUNCTIONS").unwrap();
        assert!(matches!(stmt, Statement::ShowFunctions { .. }));
    }

    #[test]
    fn test_issue_656_show_columns() {
        let stmt = parse_sql("SHOW COLUMNS FROM t").unwrap();
        assert!(matches!(stmt, Statement::ShowColumns { .. }));
    }

    #[test]
    fn test_issue_657_insert() {
        let stmt = parse_sql("INSERT INTO t SELECT 1").unwrap();
        assert!(matches!(stmt, Statement::Insert(_)));
    }

    #[test]
    fn test_issue_657_directory() {
        let stmt = parse_sql("INSERT OVERWRITE DIRECTORY '/path' SELECT 1").unwrap();
        assert!(matches!(stmt, Statement::Directory { .. }));
    }

    // LOAD DATA is only parsed by dialects that support it (e.g. HiveDialect), not GenericDialect.
    // Statement::LoadData is allowed in the whitelist for when such a dialect is used.
}
