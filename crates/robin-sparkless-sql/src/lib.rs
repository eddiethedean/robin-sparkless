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
pub enum CommandKind {
    CreateTable,
    CreateView,
    CreateSchema,
    CreateDatabase,
    CreateOrReplaceTableAs,
    Drop { object_type: String },
    Insert,
    Update,
    Delete,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandPlan {
    pub kind: CommandKind,
    pub targets: Vec<String>,
    pub format: Option<String>,
    pub effect: StatementEffect,
    pub query: Option<SqlQueryPlan>,
    /// Canonical AST spelling retains command options not modeled as separate fields.
    pub source_sql: String,
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

/// Parse canonical SQL emitted by the lowerer and restore its decoded string values.
pub fn parse_lowered_query(sql: &str) -> Result<ast::Statement, ParseError> {
    let mut statement = parse_query(sql)?;
    let _ = ast::VisitMut::visit(&mut statement, &mut plan::LoweredStrings::Decode);
    Ok(statement)
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
            kind: CommandKind::CreateOrReplaceTableAs,
            targets: vec![object_name(table)],
            format: Some(format.clone()),
            effect: StatementEffect::Mutation,
            query: Some(lower_query(query)?),
            source_sql: format!(
                "CREATE OR REPLACE TABLE {} USING {} AS {}",
                object_name(table),
                format,
                query
            ),
        }),
        SparkStatement::Sqlparser(statement) => match statement.as_ref() {
            ast::Statement::Query(query) => StatementPlan::Query(lower_query(query)?),
            ast::Statement::ShowDatabases { .. } => {
                StatementPlan::Metadata(MetadataCommand::ShowDatabases)
            }
            ast::Statement::ShowTables { .. } => {
                StatementPlan::Metadata(MetadataCommand::ShowTables { database: None })
            }
            ast::Statement::ExplainTable {
                table_name,
                hive_format,
                ..
            } => StatementPlan::Metadata(MetadataCommand::Describe {
                table: object_name(table_name),
                column: None,
                extended: hive_format.is_some(),
            }),
            command => lower_command(command)?,
        },
    })
}

fn lower_command(statement: &ast::Statement) -> Result<StatementPlan, SqlPlanError> {
    let (kind, targets, query) = match statement {
        ast::Statement::CreateTable(create) => (
            CommandKind::CreateTable,
            vec![object_name(&create.name)],
            create.query.as_deref().map(lower_query).transpose()?,
        ),
        ast::Statement::CreateView(create) => (
            CommandKind::CreateView,
            vec![object_name(&create.name)],
            Some(lower_query(create.query.as_ref())?),
        ),
        ast::Statement::CreateSchema { schema_name, .. } => (
            CommandKind::CreateSchema,
            vec![schema_name_string(schema_name)],
            None,
        ),
        ast::Statement::CreateDatabase { db_name, .. } => (
            CommandKind::CreateDatabase,
            vec![object_name(db_name)],
            None,
        ),
        ast::Statement::Drop {
            object_type, names, ..
        } => (
            CommandKind::Drop {
                object_type: object_type.to_string(),
            },
            names.iter().map(object_name).collect(),
            None,
        ),
        ast::Statement::Insert(insert) => (
            CommandKind::Insert,
            match &insert.table {
                ast::TableObject::TableName(name) => vec![object_name(name)],
                _ => Vec::new(),
            },
            insert.source.as_deref().map(lower_query).transpose()?,
        ),
        ast::Statement::Update(update) => (
            CommandKind::Update,
            table_factor_name(&update.table.relation)
                .into_iter()
                .collect(),
            None,
        ),
        ast::Statement::Delete(delete) => {
            let tables = match &delete.from {
                ast::FromTable::WithFromKeyword(tables)
                | ast::FromTable::WithoutKeyword(tables) => tables,
            };
            (
                CommandKind::Delete,
                tables
                    .iter()
                    .filter_map(|table| table_factor_name(&table.relation))
                    .collect(),
                None,
            )
        }
        other => (
            CommandKind::Other(
                other
                    .to_string()
                    .split_whitespace()
                    .next()
                    .unwrap_or("unknown")
                    .to_ascii_lowercase(),
            ),
            Vec::new(),
            None,
        ),
    };
    let effect = statement_effect(&SparkStatement::Sqlparser(Box::new(statement.clone())));
    Ok(StatementPlan::Command(CommandPlan {
        kind,
        targets,
        format: None,
        effect,
        query,
        source_sql: statement.to_string(),
    }))
}

fn schema_name_string(name: &ast::SchemaName) -> String {
    match name {
        ast::SchemaName::Simple(name) => object_name(name),
        ast::SchemaName::UnnamedAuthorization(identifier) => identifier.value.clone(),
        other => other.to_string(),
    }
}

fn table_factor_name(factor: &ast::TableFactor) -> Option<String> {
    match factor {
        ast::TableFactor::Table { name, .. } => Some(object_name(name)),
        _ => None,
    }
}

/// Lower syntax variants to the shared effect class used by local and remote executors.
/// Unknown SQL-parser statements are mutations until an adapter explicitly supports them.
pub fn statement_effect(statement: &SparkStatement) -> StatementEffect {
    let read_only = match statement {
        SparkStatement::DescribeDetail { .. }
        | SparkStatement::ShowDatabases
        | SparkStatement::ShowTables { .. }
        | SparkStatement::Describe { .. } => true,
        SparkStatement::Sqlparser(statement) => matches!(
            statement.as_ref(),
            ast::Statement::Query(_)
                | ast::Statement::ShowDatabases { .. }
                | ast::Statement::ShowTables { .. }
                | ast::Statement::ExplainTable { .. }
        ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowers_query_clauses_and_relations_to_the_shared_plan() {
        let statement = parse_statement(
            "SELECT id, upper(name) AS normalized FROM analytics.events WHERE active = true ORDER BY id DESC LIMIT 5 OFFSET 2",
        )
        .expect("query parses");
        let StatementPlan::Query(query) = lower_statement(&statement).expect("query lowers") else {
            panic!("SELECT should lower to a query plan");
        };

        assert_eq!(query.limit, Some(5));
        assert_eq!(query.offset, Some(2));
        assert_eq!(query.order_by.len(), 1);
        assert_eq!(query.order_by[0].ascending, Some(false));
        let RelationalNode::Select(select) = &query.body else {
            panic!("query body should be a SELECT");
        };
        assert_eq!(select.projection.len(), 2);
        assert!(matches!(
            &select.projection[1],
            Projection::Expression { alias: Some(alias), .. } if alias == "normalized"
        ));
        assert!(matches!(
            &select.from[0].relation,
            RelationPlan::Table { name, .. } if name == "analytics.events"
        ));
        assert!(select.filter.is_some());
    }

    #[test]
    fn classifies_metadata_and_mutation_commands_and_retains_their_query() {
        let show = parse_statement("SHOW TABLES IN analytics").expect("SHOW parses");
        assert!(matches!(
            lower_statement(&show).expect("SHOW lowers"),
            StatementPlan::Metadata(MetadataCommand::ShowTables { database: Some(database) })
                if database == "analytics"
        ));

        let create = parse_statement(
            "CREATE OR REPLACE TABLE analytics.snapshot USING parquet AS SELECT id FROM analytics.events",
        )
        .expect("CREATE parses");
        let StatementPlan::Command(command) = lower_statement(&create).expect("CREATE lowers")
        else {
            panic!("CREATE should lower to a command plan");
        };
        assert_eq!(command.kind, CommandKind::CreateOrReplaceTableAs);
        assert_eq!(command.targets, ["analytics.snapshot"]);
        assert_eq!(command.format.as_deref(), Some("parquet"));
        assert_eq!(command.effect, StatementEffect::Mutation);
        assert!(command.query.is_some());
        assert!(!command.source_sql.is_empty());
    }

    #[test]
    fn rejects_non_literal_and_negative_limits_during_shared_lowering() {
        for sql in [
            "SELECT id FROM events LIMIT id",
            "SELECT id FROM events LIMIT -1",
        ] {
            let statement = parse_statement(sql).expect("statement parses");
            let error = lower_statement(&statement).expect_err("invalid LIMIT is rejected");
            assert!(error.to_string().contains("LIMIT"));
        }
    }
}
