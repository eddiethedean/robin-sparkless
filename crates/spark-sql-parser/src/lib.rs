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

use sqlparser::ast::{
    Expr as SqlExpr, Ident, ObjectName, Query, Select, SelectItem, SetExpr, Statement,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

/// Error returned when SQL parsing or validation fails.
#[derive(Error, Debug)]
#[error("{0}")]
pub struct ParseError(String);

/// Re-export of [sqlparser::ast] so consumers can depend only on `spark-sql-parser` for SQL AST types.
pub use sqlparser::ast;

/// Spark-oriented statement variants that are not reliably represented by upstream `sqlparser` AST.
///
/// This enum is intended to capture Spark/PySpark command forms where upstream parsing either:
/// - does not accept the syntax, or
/// - accepts it but in a way that's inconvenient for execution-layer parity.
#[derive(Debug, Clone, PartialEq)]
pub enum SparkStatement {
    /// A statement parsed by upstream `sqlparser`.
    Sqlparser(Box<Statement>),
    /// `DESCRIBE DETAIL <table>` (Delta Lake; Spark command).
    DescribeDetail { table: ObjectName },
    /// `SHOW DATABASES` (Spark command).
    ShowDatabases,
    /// `SHOW TABLES` or `SHOW TABLES IN/FROM <db>` (Spark command).
    ShowTables { db: Option<ObjectName> },
    /// `DESCRIBE/DESC [TABLE] [EXTENDED] <table> [<col>]` (Spark/PySpark parity).
    Describe {
        table: ObjectName,
        col: Option<Ident>,
        extended: bool,
    },
}

fn parse_one_statement_raw(query: &str) -> Result<Statement, ParseError> {
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
    Ok(stmts.into_iter().next().expect("len == 1"))
}

fn parse_object_name(name: &str) -> Result<ObjectName, ParseError> {
    let s = name.trim();
    if s.is_empty() {
        return Err(ParseError(
            "SQL: expected an object name, got empty string.".to_string(),
        ));
    }
    // Minimal support for Spark-style qualified names like `schema.table` and `global_temp.gv`.
    // Backtick quoting is intentionally not handled yet (can be added later).
    let parts: Vec<Ident> = s
        .split('.')
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map(Ident::new)
        .collect();
    if parts.is_empty() {
        return Err(ParseError(format!(
            "SQL: expected an object name, got '{s}'."
        )));
    }
    Ok(ObjectName::from(parts))
}

fn tokenize_ws(s: &str) -> Vec<&str> {
    s.split_whitespace().collect()
}

/// Parse a Spark/PySpark-compatible SQL string.
///
/// - First, fast-path Spark-only command variants (e.g. `DESCRIBE DETAIL`, `SHOW TABLES IN db`,
///   `DESCRIBE t col`).
/// - Otherwise, fall back to upstream `sqlparser` and return `SparkStatement::Sqlparser`.
/// - Always enforces **exactly one statement** per call.
pub fn parse_spark_sql(query: &str) -> Result<SparkStatement, ParseError> {
    let q = query.trim();
    if q.is_empty() {
        // Let upstream parser produce the most specific error message.
        let _ = parse_one_statement_raw(q)?;
    }

    // Tokenize for Spark-only command matching.
    let toks = tokenize_ws(q);
    if toks.len() >= 2
        && toks[0].eq_ignore_ascii_case("SHOW")
        && toks[1].eq_ignore_ascii_case("DATABASES")
    {
        return Ok(SparkStatement::ShowDatabases);
    }

    // SHOW TABLES [IN|FROM db]
    if toks.len() >= 2
        && toks[0].eq_ignore_ascii_case("SHOW")
        && toks[1].eq_ignore_ascii_case("TABLES")
    {
        let db = if toks.len() >= 4
            && (toks[2].eq_ignore_ascii_case("IN") || toks[2].eq_ignore_ascii_case("FROM"))
        {
            Some(parse_object_name(toks[3])?)
        } else {
            None
        };
        return Ok(SparkStatement::ShowTables { db });
    }

    // DESCRIBE DETAIL <table>
    if toks.len() >= 3
        && toks[0].eq_ignore_ascii_case("DESCRIBE")
        && toks[1].eq_ignore_ascii_case("DETAIL")
    {
        let table = parse_object_name(&toks[2..].join(" "))?;
        return Ok(SparkStatement::DescribeDetail { table });
    }

    // DESC DETAIL is a synonym for DESCRIBE DETAIL in Spark; treat it the same.
    if toks.len() >= 3
        && toks[0].eq_ignore_ascii_case("DESC")
        && toks[1].eq_ignore_ascii_case("DETAIL")
    {
        let table = parse_object_name(&toks[2..].join(" "))?;
        return Ok(SparkStatement::DescribeDetail { table });
    }

    // DESCRIBE/DESC [TABLE] [EXTENDED] <table> [<col>]
    if !toks.is_empty()
        && (toks[0].eq_ignore_ascii_case("DESCRIBE") || toks[0].eq_ignore_ascii_case("DESC"))
    {
        // Exclude DETAIL which is handled above.
        if toks.len() >= 2 && toks[1].eq_ignore_ascii_case("DETAIL") {
            // already handled above; fallthrough defensive.
        } else {
            let rest = &toks[1..];
            if !rest.is_empty() {
                let extended = rest.iter().any(|t| t.eq_ignore_ascii_case("EXTENDED"));
                // Find the first token that is not TABLE/EXTENDED => table name token.
                let idx = rest.iter().position(|t| {
                    !t.eq_ignore_ascii_case("TABLE") && !t.eq_ignore_ascii_case("EXTENDED")
                });
                if let Some(i) = idx {
                    let table_tok = rest.get(i).copied().unwrap_or("");
                    if !table_tok.is_empty() {
                        let table = parse_object_name(table_tok)?;
                        let col = rest.get(i + 1).map(|c| Ident::new(*c));
                        return Ok(SparkStatement::Describe {
                            table,
                            col,
                            extended,
                        });
                    }
                }
            }
        }
    }

    // Fall back to upstream parsing for everything else.
    let stmt = parse_one_statement_raw(query)?;
    Ok(SparkStatement::Sqlparser(Box::new(stmt)))
}

/// Parse a single SQL expression string (optionally with an alias) into `sqlparser` expression AST.
///
/// This is intended for PySpark parity helpers like `selectExpr` and `expr()` where the input is
/// a *projection expression*, not a full SQL statement.
pub fn parse_select_expr(expr_str: &str) -> Result<(SqlExpr, Option<Ident>), ParseError> {
    let e = expr_str.trim();
    if e.is_empty() {
        return Err(ParseError(
            "SQL: expected an expression string, got empty.".to_string(),
        ));
    }
    // Parse by embedding into a query; keep the hack local to this crate.
    const TMP_TABLE: &str = "__spark_sql_parser_expr_t";
    let query = format!("SELECT {e} FROM {TMP_TABLE}");
    let stmt = parse_one_statement_raw(&query)?;
    let query_ast: &Query = match &stmt {
        Statement::Query(q) => q.as_ref(),
        other => {
            return Err(ParseError(format!(
                "SQL: expected SELECT when parsing expression, got {other:?}."
            )));
        }
    };
    let select: &Select = match query_ast.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        other => {
            return Err(ParseError(format!(
                "SQL: expected SELECT when parsing expression, got {other:?}."
            )));
        }
    };
    let first: &SelectItem = select.projection.first().ok_or_else(|| {
        ParseError("SQL: expected non-empty SELECT list when parsing expression.".to_string())
    })?;
    match first {
        SelectItem::UnnamedExpr(ex) => Ok((ex.clone(), None)),
        SelectItem::ExprWithAlias { expr, alias } => Ok((expr.clone(), Some(alias.clone()))),
        other => Err(ParseError(format!(
            "SQL: unsupported expression form in SELECT list: {other:?}."
        ))),
    }
}

/// Parse a single SQL statement (SELECT or DDL: CREATE SCHEMA / CREATE DATABASE / DROP TABLE/VIEW/SCHEMA).
///
/// Returns the [sqlparser::ast::Statement] on success. Only one statement per call;
/// run one statement at a time.
pub fn parse_sql(query: &str) -> Result<Statement, ParseError> {
    let stmt = parse_one_statement_raw(query)?;
    match &stmt {
        Statement::Query(_) => {}
        Statement::CreateSchema { .. } | Statement::CreateDatabase { .. } => {}
        Statement::CreateTable(_) | Statement::CreateView(_) | Statement::CreateFunction(_) => {}
        Statement::AlterTable(_) | Statement::AlterView { .. } | Statement::AlterSchema(_) => {}
        Statement::Drop {
            object_type:
                sqlparser::ast::ObjectType::Table
                | sqlparser::ast::ObjectType::View
                | sqlparser::ast::ObjectType::Schema
                | sqlparser::ast::ObjectType::Database,
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
        Statement::Update(_) | Statement::Delete(_) => {}
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
        // COMMIT is parsed by sqlparser but not in our whitelist
        let err = parse_sql("COMMIT").unwrap_err();
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

    // --- SparkStatement parsing (Spark/PySpark command variants) ---

    #[test]
    fn spark_show_databases() {
        let s = parse_spark_sql("SHOW DATABASES").unwrap();
        assert!(matches!(s, SparkStatement::ShowDatabases));
    }

    #[test]
    fn spark_show_tables_in_db() {
        let s = parse_spark_sql("SHOW TABLES IN my_db").unwrap();
        match s {
            SparkStatement::ShowTables { db: Some(db) } => {
                assert_eq!(db.to_string(), "my_db");
            }
            other => panic!("expected ShowTables with db, got {other:?}"),
        }
    }

    #[test]
    fn spark_describe_detail() {
        let s = parse_spark_sql("DESCRIBE DETAIL schema1.tbl1").unwrap();
        match s {
            SparkStatement::DescribeDetail { table } => {
                assert_eq!(table.to_string(), "schema1.tbl1");
            }
            other => panic!("expected DescribeDetail, got {other:?}"),
        }
    }

    #[test]
    fn spark_describe_optional_col() {
        let s = parse_spark_sql("DESCRIBE t age").unwrap();
        match s {
            SparkStatement::Describe {
                table,
                col: Some(c),
                extended: false,
            } => {
                assert_eq!(table.to_string(), "t");
                assert_eq!(c.value, "age");
            }
            other => panic!("expected Describe with col, got {other:?}"),
        }
    }

    #[test]
    fn spark_describe_table_extended() {
        let s = parse_spark_sql("DESCRIBE TABLE EXTENDED t").unwrap();
        match s {
            SparkStatement::Describe {
                table,
                col: None,
                extended: true,
            } => {
                assert_eq!(table.to_string(), "t");
            }
            other => panic!("expected Describe extended, got {other:?}"),
        }
    }

    // --- Expression parsing helper ---

    #[test]
    fn parse_select_expr_with_alias() {
        let (e, a) = parse_select_expr("upper(Name) AS u").unwrap();
        let _ = e; // structure validated by parse
        assert_eq!(a.unwrap().value, "u");
    }

    #[test]
    fn parse_select_expr_without_alias() {
        let (_e, a) = parse_select_expr("ltrim(rtrim(Value))").unwrap();
        assert!(a.is_none());
    }

    // ========== Robust parse_spark_sql tests ==========

    #[test]
    fn spark_show_databases_case_insensitive() {
        for sql in ["show databases", "Show Databases", "SHOW DATABASES"] {
            let s = parse_spark_sql(sql).unwrap();
            assert!(
                matches!(s, SparkStatement::ShowDatabases),
                "failed for: {sql}"
            );
        }
    }

    #[test]
    fn spark_show_tables_no_db() {
        let s = parse_spark_sql("SHOW TABLES").unwrap();
        match s {
            SparkStatement::ShowTables { db: None } => {}
            other => panic!("expected ShowTables with db=None, got {other:?}"),
        }
    }

    #[test]
    fn spark_show_tables_from_db() {
        let s = parse_spark_sql("SHOW TABLES FROM other_db").unwrap();
        match s {
            SparkStatement::ShowTables { db: Some(db) } => assert_eq!(db.to_string(), "other_db"),
            other => panic!("expected ShowTables with db, got {other:?}"),
        }
    }

    #[test]
    fn spark_show_tables_in_db_case_insensitive() {
        let s = parse_spark_sql("show tables in MySchema").unwrap();
        match s {
            SparkStatement::ShowTables { db: Some(db) } => assert_eq!(db.to_string(), "MySchema"),
            other => panic!("expected ShowTables with db, got {other:?}"),
        }
    }

    #[test]
    fn spark_describe_detail_single_table() {
        let s = parse_spark_sql("DESCRIBE DETAIL t").unwrap();
        match s {
            SparkStatement::DescribeDetail { table } => assert_eq!(table.to_string(), "t"),
            other => panic!("expected DescribeDetail, got {other:?}"),
        }
    }

    #[test]
    fn spark_describe_detail_case_insensitive() {
        let s = parse_spark_sql("describe detail my_table").unwrap();
        match s {
            SparkStatement::DescribeDetail { table } => assert_eq!(table.to_string(), "my_table"),
            other => panic!("expected DescribeDetail, got {other:?}"),
        }
    }

    #[test]
    fn spark_desc_detail_synonym() {
        let s = parse_spark_sql("DESC DETAIL catalog.schema.tbl").unwrap();
        match s {
            SparkStatement::DescribeDetail { table } => {
                assert_eq!(table.to_string(), "catalog.schema.tbl")
            }
            other => panic!("expected DescribeDetail, got {other:?}"),
        }
    }

    #[test]
    fn spark_describe_table_only() {
        let s = parse_spark_sql("DESCRIBE my_tbl").unwrap();
        match s {
            SparkStatement::Describe {
                table,
                col: None,
                extended: false,
            } => assert_eq!(table.to_string(), "my_tbl"),
            other => panic!("expected Describe table only, got {other:?}"),
        }
    }

    #[test]
    fn spark_describe_extended_only() {
        let s = parse_spark_sql("DESCRIBE EXTENDED t").unwrap();
        match s {
            SparkStatement::Describe {
                table,
                col: None,
                extended: true,
            } => assert_eq!(table.to_string(), "t"),
            other => panic!("expected Describe extended, got {other:?}"),
        }
    }

    #[test]
    fn spark_desc_short_form() {
        let s = parse_spark_sql("DESC t col_x").unwrap();
        match s {
            SparkStatement::Describe {
                table,
                col: Some(c),
                extended: false,
            } => {
                assert_eq!(table.to_string(), "t");
                assert_eq!(c.value, "col_x");
            }
            other => panic!("expected Describe with col, got {other:?}"),
        }
    }

    #[test]
    fn spark_describe_qualified_table_with_col() {
        let s = parse_spark_sql("DESCRIBE global_temp.v id").unwrap();
        match s {
            SparkStatement::Describe {
                table,
                col: Some(c),
                extended: false,
            } => {
                assert_eq!(table.to_string(), "global_temp.v");
                assert_eq!(c.value, "id");
            }
            other => panic!("expected Describe qualified table + col, got {other:?}"),
        }
    }

    #[test]
    fn spark_parse_spark_sql_empty_fails() {
        let err = parse_spark_sql("").unwrap_err();
        assert!(
            err.0.contains("expected exactly one statement") || err.0.contains("parse error"),
            "unexpected error: {}",
            err.0
        );
    }

    #[test]
    fn spark_parse_spark_sql_whitespace_only_fails() {
        let err = parse_spark_sql("   \t\n  ").unwrap_err();
        assert!(!err.0.is_empty(), "expected some error message");
    }

    #[test]
    fn spark_parse_spark_sql_multiple_statements_fails() {
        let err = parse_spark_sql("SELECT 1; SELECT 2").unwrap_err();
        assert!(err.0.contains("expected exactly one statement"));
    }

    #[test]
    fn spark_parse_spark_sql_fallback_select() {
        let s = parse_spark_sql("SELECT 1 AS x").unwrap();
        match s {
            SparkStatement::Sqlparser(stmt) if matches!(stmt.as_ref(), Statement::Query(_)) => {}
            other => panic!("expected Sqlparser(Query), got {other:?}"),
        }
    }

    #[test]
    fn spark_parse_spark_sql_fallback_create_schema() {
        let s = parse_spark_sql("CREATE SCHEMA foo").unwrap();
        match s {
            SparkStatement::Sqlparser(stmt)
                if matches!(stmt.as_ref(), Statement::CreateSchema { .. }) => {}
            other => panic!("expected Sqlparser(CreateSchema), got {other:?}"),
        }
    }

    #[test]
    fn spark_parse_spark_sql_fallback_drop_table() {
        let s = parse_spark_sql("DROP TABLE IF EXISTS t").unwrap();
        match s {
            SparkStatement::Sqlparser(stmt) if matches!(stmt.as_ref(), Statement::Drop { .. }) => {}
            other => panic!("expected Sqlparser(Drop), got {other:?}"),
        }
    }

    // ========== Robust parse_select_expr tests ==========

    #[test]
    fn parse_select_expr_empty_fails() {
        let err = parse_select_expr("").unwrap_err();
        assert!(err.0.contains("expected an expression"));
    }

    #[test]
    fn parse_select_expr_whitespace_only_fails() {
        let err = parse_select_expr("   \n\t  ").unwrap_err();
        assert!(err.0.contains("expected an expression"));
    }

    #[test]
    fn parse_select_expr_literal_number() {
        let (e, a) = parse_select_expr("42").unwrap();
        assert!(matches!(e, SqlExpr::Value(_)));
        assert!(a.is_none());
    }

    #[test]
    fn parse_select_expr_literal_string() {
        let (e, _) = parse_select_expr("'hello'").unwrap();
        assert!(matches!(e, SqlExpr::Value(_)));
    }

    #[test]
    fn parse_select_expr_literal_null() {
        let (e, _) = parse_select_expr("NULL").unwrap();
        assert!(matches!(e, SqlExpr::Value(_)));
    }

    #[test]
    fn parse_select_expr_identifier() {
        let (e, _) = parse_select_expr("column_name").unwrap();
        assert!(matches!(e, SqlExpr::Identifier(_)));
    }

    #[test]
    fn parse_select_expr_compound_identifier() {
        let (e, _) = parse_select_expr("t.id").unwrap();
        assert!(matches!(e, SqlExpr::CompoundIdentifier(_)));
    }

    #[test]
    fn parse_select_expr_binary_op() {
        let (e, _) = parse_select_expr("a + b").unwrap();
        assert!(matches!(e, SqlExpr::BinaryOp { .. }));
    }

    #[test]
    fn parse_select_expr_function_call() {
        let (e, a) = parse_select_expr("COUNT(*)").unwrap();
        assert!(matches!(e, SqlExpr::Function(_)));
        assert!(a.is_none());
    }

    #[test]
    fn parse_select_expr_function_with_alias() {
        let (e, a) = parse_select_expr("SUM(amount) AS total").unwrap();
        assert!(matches!(e, SqlExpr::Function(_)));
        assert_eq!(a.as_ref().map(|i| i.value.as_str()), Some("total"));
    }

    #[test]
    fn parse_select_expr_nested_function() {
        let (_e, a) = parse_select_expr("UPPER(TRIM(name))").unwrap();
        assert!(a.is_none());
    }

    #[test]
    fn parse_select_expr_case_when() {
        let (e, _) = parse_select_expr("CASE WHEN x > 0 THEN 1 ELSE 0 END").unwrap();
        assert!(matches!(e, SqlExpr::Case { .. }));
    }

    #[test]
    fn parse_select_expr_comparison() {
        let (e, _) = parse_select_expr("id = 1").unwrap();
        assert!(matches!(e, SqlExpr::BinaryOp { .. }));
    }

    #[test]
    fn parse_select_expr_invalid_syntax_fails() {
        // Unmatched parenthesis or invalid token sequence should yield a parse error.
        let err = parse_select_expr("( unclosed").unwrap_err();
        assert!(!err.0.is_empty());
    }
}
