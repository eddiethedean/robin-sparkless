//! SQL identifier validation and dialect-specific quoting for JDBC.

use crate::error::EngineError;

/// Safe unquoted identifier: letter/underscore start, then alphanumeric/underscore.
fn is_valid_ident_part(part: &str) -> bool {
    if part.is_empty() || part.len() > 128 {
        return false;
    }
    let mut chars = part.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Validate `dbtable` / schema-qualified table name (no SQL injection via identifiers).
pub fn validate_dbtable(table: &str) -> Result<(), EngineError> {
    if table.trim().is_empty() {
        return Err(EngineError::User(
            "JDBC dbtable must not be empty".to_string(),
        ));
    }
    if table.contains(';') || table.contains("--") || table.contains("/*") {
        return Err(EngineError::User(
            "JDBC dbtable contains disallowed SQL characters".to_string(),
        ));
    }
    for part in table.split('.') {
        if !is_valid_ident_part(part) {
            return Err(EngineError::User(format!(
                "JDBC dbtable '{table}' contains invalid identifier segment '{part}'"
            )));
        }
    }
    Ok(())
}

/// Validate column names used in INSERT statements.
pub fn validate_column_ident(name: &str) -> Result<(), EngineError> {
    if !is_valid_ident_part(name) {
        return Err(EngineError::User(format!(
            "JDBC column name '{name}' is not a safe SQL identifier"
        )));
    }
    Ok(())
}

/// Quote identifier for PostgreSQL / SQLite (double quotes).
pub fn quote_ident_double(name: &str) -> Result<String, EngineError> {
    validate_column_ident(name)?;
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

/// Quote identifier for MySQL / MariaDB (backticks).
pub fn quote_ident_backtick(name: &str) -> Result<String, EngineError> {
    validate_column_ident(name)?;
    Ok(format!("`{}`", name.replace('`', "``")))
}

/// Quote identifier for SQL Server (brackets).
pub fn quote_ident_bracket(name: &str) -> Result<String, EngineError> {
    validate_column_ident(name)?;
    Ok(format!("[{}]", name.replace(']', "]]")))
}

/// Quote a possibly schema-qualified table name using the given quote function per segment.
pub fn quote_qualified_table(
    table: &str,
    quote_part: fn(&str) -> Result<String, EngineError>,
) -> Result<String, EngineError> {
    validate_dbtable(table)?;
    table
        .split('.')
        .map(quote_part)
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("."))
}

#[derive(Copy, Clone, Debug)]
pub enum JdbcDialect {
    Sqlite,
    Postgres,
    Mysql,
    Mssql,
    Oracle,
    Db2,
}

/// Quote a schema-qualified table for the given JDBC dialect.
pub fn quoted_table(dialect: JdbcDialect, table: &str) -> Result<String, EngineError> {
    let quote_part = match dialect {
        JdbcDialect::Sqlite | JdbcDialect::Postgres | JdbcDialect::Oracle | JdbcDialect::Db2 => {
            quote_ident_double
        }
        JdbcDialect::Mysql => quote_ident_backtick,
        JdbcDialect::Mssql => quote_ident_bracket,
    };
    quote_qualified_table(table, quote_part)
}

/// Quote column names for INSERT column lists.
pub fn quoted_columns(dialect: JdbcDialect, names: &[String]) -> Result<Vec<String>, EngineError> {
    names
        .iter()
        .map(|n| match dialect {
            JdbcDialect::Sqlite
            | JdbcDialect::Postgres
            | JdbcDialect::Oracle
            | JdbcDialect::Db2 => quote_ident_double(n),
            JdbcDialect::Mysql => quote_ident_backtick(n),
            JdbcDialect::Mssql => quote_ident_bracket(n),
        })
        .collect()
}

/// Redact credentials from JDBC URLs in error messages.
pub fn redact_jdbc_url(url: &str) -> String {
    if let Some(at) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let scheme = &url[..scheme_end + 3];
            let rest = &url[at + 1..];
            return format!("{scheme}***:***@{rest}");
        }
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_injection_in_dbtable() {
        assert!(validate_dbtable("t; DROP TABLE t").is_err());
    }

    #[test]
    fn quotes_postgres_table() {
        let q = quote_qualified_table("schema.t", quote_ident_double).unwrap();
        assert_eq!(q, "\"schema\".\"t\"");
    }
}
