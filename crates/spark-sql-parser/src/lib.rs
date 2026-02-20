//! Parse SQL into [sqlparser] AST.
//!
//! Supports a Spark-style subset: single-statement SELECT, CREATE SCHEMA/DATABASE,
//! and DROP TABLE/VIEW/SCHEMA.

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
        Statement::Drop {
            object_type:
                sqlparser::ast::ObjectType::Table
                | sqlparser::ast::ObjectType::View
                | sqlparser::ast::ObjectType::Schema,
            ..
        } => {}
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
}
