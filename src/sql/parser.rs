//! Parse SQL into sqlparser AST.

use polars::prelude::PolarsError;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parse a single SQL statement (SELECT or DDL: CREATE SCHEMA / CREATE DATABASE).
pub fn parse_sql(query: &str) -> Result<Statement, PolarsError> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, query).map_err(|e| {
        PolarsError::InvalidOperation(
            format!("SQL parse error: {}. Hint: only SELECT and CREATE SCHEMA/DATABASE are supported.", e).into(),
        )
    })?;
    if stmts.len() != 1 {
        return Err(PolarsError::InvalidOperation(
            format!(
                "SQL: expected exactly one statement, got {}. Hint: run one statement at a time.",
                stmts.len()
            )
            .into(),
        ));
    }
    let stmt = stmts.into_iter().next().unwrap();
    match &stmt {
        Statement::Query(_) => {}
        Statement::CreateSchema { .. } | Statement::CreateDatabase { .. } => {}
        _ => {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "SQL: only SELECT and CREATE SCHEMA/DATABASE are supported, got {:?}.",
                    stmt
                )
                .into(),
            ));
        }
    }
    Ok(stmt)
}
