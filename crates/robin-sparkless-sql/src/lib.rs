//! Shared Spark SQL entry points and identifier rules. Execution remains in backend adapters.

mod plan;

pub use plan::{
    CtePlan, ExpressionShape, FromPlan, GroupByPlan, JoinConstraintPlan, JoinKind, JoinPlan,
    Projection, RelationPlan, RelationalNode, SelectPlan, SetOperator, SetQuantifier, SortKey,
    SqlExpression, SqlPlanError, SqlQueryPlan, lower_query,
};
use serde::{Deserialize, Serialize};
pub use spark_sql_parser::ast;
use spark_sql_parser::ast::{Ident, ObjectName, ObjectNamePart};
pub use spark_sql_parser::{ParseError, SparkStatement};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatementEffect {
    ReadOnly,
    Mutation,
}

/// A normalized statement keeps relational queries separate from effectful commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatementPlan {
    Query(SqlQueryPlan),
    Metadata(MetadataCommand),
    Command(CommandPlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataCommand {
    DescribeDetail {
        table: String,
    },
    ShowDatabases,
    ShowTables {
        database: Option<String>,
    },
    Describe {
        table: String,
        column: Option<String>,
        extended: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandPlan {
    pub command: String,
    pub target: Option<String>,
    pub format: Option<String>,
    pub effect: StatementEffect,
    pub query: Option<SqlQueryPlan>,
}

impl StatementPlan {
    pub fn effect(&self) -> StatementEffect {
        match self {
            Self::Query(_) | Self::Metadata(_) => StatementEffect::ReadOnly,
            Self::Command(command) => command.effect,
        }
    }
}

pub fn parse_statement(sql: &str) -> Result<SparkStatement, ParseError> {
    spark_sql_parser::parse_spark_sql(sql)
}

pub fn parse_query(sql: &str) -> Result<ast::Statement, ParseError> {
    spark_sql_parser::parse_sql(sql)
}

pub fn parse_expression(sql: &str) -> Result<ast::Expr, ParseError> {
    spark_sql_parser::parse_single_expression(sql)
}

/// Lower a parsed statement to a relational plan or an explicitly effect-classified command.
pub fn lower_statement(statement: &SparkStatement) -> Result<StatementPlan, SqlPlanError> {
    Ok(match statement {
        SparkStatement::DescribeDetail { table } => {
            StatementPlan::Metadata(MetadataCommand::DescribeDetail {
                table: object_name(table),
            })
        }
        SparkStatement::ShowDatabases => StatementPlan::Metadata(MetadataCommand::ShowDatabases),
        SparkStatement::ShowTables { db } => StatementPlan::Metadata(MetadataCommand::ShowTables {
            database: db.as_ref().map(object_name),
        }),
        SparkStatement::Describe {
            table,
            col,
            extended,
        } => StatementPlan::Metadata(MetadataCommand::Describe {
            table: object_name(table),
            column: col.as_ref().map(|column| column.value.clone()),
            extended: *extended,
        }),
        SparkStatement::CreateOrReplaceTableAs {
            table,
            format,
            query,
        } => StatementPlan::Command(CommandPlan {
            command: "create_or_replace_table_as".into(),
            target: Some(object_name(table)),
            format: Some(format.clone()),
            effect: StatementEffect::Mutation,
            query: Some(lower_query(query)?),
        }),
        SparkStatement::Sqlparser(statement) => match statement.as_ref() {
            ast::Statement::Query(query) => StatementPlan::Query(lower_query(query)?),
            command => StatementPlan::Command(CommandPlan {
                command: command
                    .to_string()
                    .split_whitespace()
                    .next()
                    .unwrap_or("unknown")
                    .to_ascii_lowercase(),
                target: None,
                format: None,
                effect: statement_effect(&SparkStatement::Sqlparser(Box::new(command.clone()))),
                query: None,
            }),
        },
    })
}

/// Lower syntax variants to the shared effect class used by local and remote executors.
/// Unknown SQL-parser statements are mutations until an adapter explicitly supports them.
pub fn statement_effect(statement: &SparkStatement) -> StatementEffect {
    let read_only = match statement {
        SparkStatement::DescribeDetail { .. }
        | SparkStatement::ShowDatabases
        | SparkStatement::ShowTables { .. }
        | SparkStatement::Describe { .. } => true,
        SparkStatement::Sqlparser(statement) => {
            matches!(statement.as_ref(), ast::Statement::Query(_))
        }
        SparkStatement::CreateOrReplaceTableAs { .. } => false,
    };
    if read_only {
        StatementEffect::ReadOnly
    } else {
        StatementEffect::Mutation
    }
}

/// Convert a parsed multipart SQL identifier to the canonical user-facing dotted form.
pub fn object_name(name: &ObjectName) -> String {
    if name.0.len() >= 2 {
        name.0
            .iter()
            .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
            .collect::<Vec<_>>()
            .join(".")
    } else {
        name.0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .map(|ident| ident.value.clone())
            .unwrap_or_default()
    }
}

/// Compare identifier parts using the frontend's configured Spark name rules.
pub fn identifier_eq(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        left.eq_ignore_ascii_case(right)
    }
}

/// Resolve a column from a list of names, preserving duplicate-name ambiguity.
pub fn resolve_column<'a>(
    names: &'a [String],
    requested: &str,
    case_sensitive: bool,
) -> Result<&'a str, robin_sparkless_core::EngineError> {
    robin_sparkless_plan::resolve_name(names, requested, case_sensitive)
}

pub fn identifier_from_ident(identifier: &Ident) -> String {
    identifier.value.clone()
}
