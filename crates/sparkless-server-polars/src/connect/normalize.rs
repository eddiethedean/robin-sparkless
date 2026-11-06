//! Convert Spark Connect wire messages into the server core's transport-neutral plan model.

use crate::connect_proto as proto;
use sparkless_server_core::{
    ConnectCommand, ConnectExecutionPlan, ConnectExpression, ConnectGroupType, ConnectJoinType,
    ConnectLiteral, ConnectNamedTable, ConnectRelation, ConnectRelationKind,
};
use tonic::Status;

const MAX_PLAN_DEPTH: usize = 64;

pub(super) fn normalize_plan(plan: &proto::Plan) -> Result<ConnectExecutionPlan, Status> {
    Ok(match plan.op_type.as_ref() {
        Some(proto::plan::OpType::Root(relation)) => {
            ConnectExecutionPlan::Root(normalize_relation(relation, 1)?)
        }
        Some(proto::plan::OpType::Command(command)) => {
            use proto::command::CommandType;
            match command.command_type.as_ref() {
                Some(CommandType::SqlCommand(sql)) => normalize_sql_command(sql)?,
                Some(other) => ConnectExecutionPlan::Command(ConnectCommand::Unsupported(format!(
                    "Connect command {other:?} is not enabled"
                ))),
                None => ConnectExecutionPlan::Command(ConnectCommand::Unsupported(
                    "Connect command type is missing".into(),
                )),
            }
        }
        None => ConnectExecutionPlan::Missing,
    })
}

#[allow(deprecated)]
fn normalize_sql_command(sql: &proto::SqlCommand) -> Result<ConnectExecutionPlan, Status> {
    if !sql.args.is_empty()
        || !sql.pos_args.is_empty()
        || !sql.named_arguments.is_empty()
        || !sql.pos_arguments.is_empty()
    {
        return Ok(ConnectExecutionPlan::Command(ConnectCommand::Unsupported(
            "parameterized SQL commands are not enabled".into(),
        )));
    }

    let query = match sql.input.as_ref() {
        Some(input) if sql.sql.is_empty() => {
            let Some(proto::relation::RelType::Sql(query)) = input.rel_type.as_ref() else {
                return Ok(ConnectExecutionPlan::Command(ConnectCommand::Unsupported(
                    "SQL commands require a direct SQL relation; relation-backed commands are not enabled".into(),
                )));
            };
            if !query.args.is_empty()
                || !query.pos_args.is_empty()
                || !query.named_arguments.is_empty()
                || !query.pos_arguments.is_empty()
            {
                return Ok(ConnectExecutionPlan::Command(ConnectCommand::Unsupported(
                    "parameterized SQL relations are not enabled".into(),
                )));
            }
            query.query.as_str()
        }
        Some(_) => {
            return Ok(ConnectExecutionPlan::Command(ConnectCommand::Unsupported(
                "SQL commands cannot combine the legacy SQL field with an input relation".into(),
            )));
        }
        // The legacy scalar SQL field is still part of the wire contract for older clients.
        None => sql.sql.as_str(),
    };

    if query.trim().is_empty() {
        return Ok(ConnectExecutionPlan::Command(ConnectCommand::Unsupported(
            "SQL command text is required".into(),
        )));
    }
    let statement = robin_sparkless_sql::parse_statement(query).map_err(|error| {
        Status::invalid_argument(format!("invalid Connect SQL command: {error}"))
    })?;
    match robin_sparkless_sql::lower_statement(&statement)
        .map_err(|error| Status::invalid_argument(format!("unsupported Connect SQL: {error}")))?
    {
        // Spark Connect uses a SQL command as a preflight request for SELECT. Leave the actual
        // query lazy so PySpark returns a DataFrame and executes the SQL relation on action.
        robin_sparkless_sql::StatementPlan::Query(_) => {
            Ok(ConnectExecutionPlan::Command(ConnectCommand::SqlQuery {
                sql: query.to_string(),
            }))
        }
        robin_sparkless_sql::StatementPlan::Command(_) => {
            Ok(ConnectExecutionPlan::Command(ConnectCommand::Sql {
                sql: query.to_string(),
            }))
        }
        robin_sparkless_sql::StatementPlan::Metadata(_) => Ok(ConnectExecutionPlan::Command(
            ConnectCommand::Unsupported("SQL metadata commands are not enabled".into()),
        )),
    }
}

#[allow(deprecated)]
fn normalize_relation(relation: &proto::Relation, depth: usize) -> Result<ConnectRelation, Status> {
    if depth > MAX_PLAN_DEPTH {
        return Err(Status::resource_exhausted(format!(
            "Connect relation depth exceeds {MAX_PLAN_DEPTH}"
        )));
    }
    let kind = match relation.rel_type.as_ref() {
        Some(proto::relation::RelType::Range(range)) => ConnectRelationKind::Range {
            start: range.start,
            end: range.end,
            step: range.step,
            partitions: range.num_partitions,
        },
        Some(proto::relation::RelType::LocalRelation(local)) => ConnectRelationKind::Local {
            data: local.data.clone(),
            schema: local.schema.clone(),
        },
        Some(proto::relation::RelType::CachedLocalRelation(_)) => ConnectRelationKind::CachedLocal,
        Some(proto::relation::RelType::Read(read)) => {
            let named_table = match read.read_type.as_ref() {
                Some(proto::read::ReadType::NamedTable(table)) => Some(ConnectNamedTable {
                    identifier: table.unparsed_identifier.clone(),
                    options: table
                        .options
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                }),
                _ => None,
            };
            ConnectRelationKind::Read {
                is_streaming: read.is_streaming,
                named_table,
            }
        }
        Some(proto::relation::RelType::SubqueryAlias(alias)) => ConnectRelationKind::Alias {
            input: normalize_optional_relation(alias.input.as_deref(), depth)?,
            alias: alias.alias.clone(),
            qualifier: alias.qualifier.clone(),
        },
        Some(proto::relation::RelType::Filter(filter)) => ConnectRelationKind::Filter {
            input: normalize_optional_relation(filter.input.as_deref(), depth)?,
            condition: filter.condition.as_ref().map(normalize_expression),
        },
        Some(proto::relation::RelType::Project(project)) => ConnectRelationKind::Project {
            input: normalize_optional_relation(project.input.as_deref(), depth)?,
            expressions: project
                .expressions
                .iter()
                .map(normalize_expression)
                .collect(),
        },
        Some(proto::relation::RelType::Limit(limit)) => ConnectRelationKind::Limit {
            input: normalize_optional_relation(limit.input.as_deref(), depth)?,
            limit: i64::from(limit.limit),
        },
        Some(proto::relation::RelType::Join(join)) => ConnectRelationKind::Join {
            left: normalize_optional_relation(join.left.as_deref(), depth)?,
            right: normalize_optional_relation(join.right.as_deref(), depth)?,
            condition: join.join_condition.as_ref().map(normalize_expression),
            join_type: normalize_join_type(join.join_type),
            using_columns: join.using_columns.clone(),
            has_join_data_type: join.join_data_type.is_some(),
        },
        Some(proto::relation::RelType::Aggregate(aggregate)) => ConnectRelationKind::Aggregate {
            input: normalize_optional_relation(aggregate.input.as_deref(), depth)?,
            group_type: normalize_group_type(aggregate.group_type),
            grouping_expressions: aggregate
                .grouping_expressions
                .iter()
                .map(normalize_expression)
                .collect(),
            aggregate_expressions: aggregate
                .aggregate_expressions
                .iter()
                .map(normalize_expression)
                .collect(),
            has_pivot: aggregate.pivot.is_some(),
            has_grouping_sets: !aggregate.grouping_sets.is_empty(),
        },
        Some(proto::relation::RelType::Sql(sql)) => ConnectRelationKind::Sql {
            query: sql.query.clone(),
            has_arguments: !sql.args.is_empty()
                || !sql.pos_args.is_empty()
                || !sql.named_arguments.is_empty()
                || !sql.pos_arguments.is_empty(),
        },
        Some(_) => ConnectRelationKind::Unsupported("relation type".into()),
        None => ConnectRelationKind::Missing,
    };
    Ok(ConnectRelation {
        plan_id: relation.common.as_ref().and_then(|common| common.plan_id),
        kind,
    })
}

fn normalize_optional_relation(
    relation: Option<&proto::Relation>,
    depth: usize,
) -> Result<Option<Box<ConnectRelation>>, Status> {
    relation
        .map(|relation| normalize_relation(relation, depth + 1).map(Box::new))
        .transpose()
}

fn normalize_expression(expression: &proto::Expression) -> ConnectExpression {
    match expression.expr_type.as_ref() {
        Some(proto::expression::ExprType::Literal(literal)) => {
            ConnectExpression::Literal(match literal.literal_type.as_ref() {
                Some(proto::expression::literal::LiteralType::Integer(value)) => {
                    ConnectLiteral::Integer(*value)
                }
                Some(proto::expression::literal::LiteralType::Long(value)) => {
                    ConnectLiteral::Long(*value)
                }
                Some(proto::expression::literal::LiteralType::Boolean(value)) => {
                    ConnectLiteral::Boolean(*value)
                }
                Some(proto::expression::literal::LiteralType::Double(value)) => {
                    ConnectLiteral::Double(*value)
                }
                Some(proto::expression::literal::LiteralType::String(value)) => {
                    ConnectLiteral::String(value.clone())
                }
                Some(proto::expression::literal::LiteralType::Null(_)) => ConnectLiteral::Null,
                Some(_) => ConnectLiteral::Unsupported("literal type".into()),
                None => ConnectLiteral::Missing,
            })
        }
        Some(proto::expression::ExprType::UnresolvedAttribute(attribute)) => {
            ConnectExpression::Attribute {
                identifier: attribute.unparsed_identifier.clone(),
                plan_id: attribute.plan_id,
                is_metadata_column: attribute.is_metadata_column.unwrap_or(false),
            }
        }
        Some(proto::expression::ExprType::UnresolvedFunction(function)) => {
            ConnectExpression::Function {
                name: function.function_name.clone(),
                arguments: function
                    .arguments
                    .iter()
                    .map(normalize_expression)
                    .collect(),
                is_distinct: function.is_distinct,
                is_user_defined: function.is_user_defined_function,
                is_internal: function.is_internal.unwrap_or(false),
            }
        }
        Some(proto::expression::ExprType::ExpressionString(expression)) => {
            ConnectExpression::ExpressionString(expression.expression.clone())
        }
        Some(proto::expression::ExprType::Alias(alias)) => ConnectExpression::Alias {
            expression: alias
                .expr
                .as_deref()
                .map(|expression| Box::new(normalize_expression(expression))),
            names: alias.name.clone(),
            metadata: alias.metadata.clone(),
        },
        Some(proto::expression::ExprType::UnresolvedStar(_)) => ConnectExpression::Star,
        Some(_) => ConnectExpression::Unsupported("expression type".into()),
        None => ConnectExpression::Missing,
    }
}

fn normalize_join_type(value: i32) -> ConnectJoinType {
    match proto::join::JoinType::try_from(value) {
        Ok(proto::join::JoinType::Unspecified) => ConnectJoinType::Unspecified,
        Ok(proto::join::JoinType::Inner) => ConnectJoinType::Inner,
        Ok(proto::join::JoinType::FullOuter) => ConnectJoinType::FullOuter,
        Ok(proto::join::JoinType::LeftOuter) => ConnectJoinType::LeftOuter,
        Ok(proto::join::JoinType::RightOuter) => ConnectJoinType::RightOuter,
        Ok(proto::join::JoinType::LeftAnti) => ConnectJoinType::LeftAnti,
        Ok(proto::join::JoinType::LeftSemi) => ConnectJoinType::LeftSemi,
        Ok(proto::join::JoinType::Cross) => ConnectJoinType::Cross,
        Err(_) => ConnectJoinType::Unknown(value),
    }
}

fn normalize_group_type(value: i32) -> ConnectGroupType {
    match proto::aggregate::GroupType::try_from(value) {
        Ok(proto::aggregate::GroupType::Groupby) => ConnectGroupType::GroupBy,
        Ok(_) | Err(_) => ConnectGroupType::Other(value),
    }
}

/// Rebuilds the adapter's private Connect representation from the transport-neutral core plan.
/// This keeps generated wire messages inside the Connect adapter implementation.
#[allow(deprecated)]
pub(super) fn materialize_plan(plan: &ConnectExecutionPlan) -> proto::Plan {
    match plan {
        ConnectExecutionPlan::Root(relation) => proto::Plan {
            op_type: Some(proto::plan::OpType::Root(materialize_relation(relation))),
        },
        ConnectExecutionPlan::Command(command) => proto::Plan {
            op_type: Some(proto::plan::OpType::Command(match command {
                ConnectCommand::Sql { sql } => proto::Command {
                    command_type: Some(proto::command::CommandType::SqlCommand(
                        proto::SqlCommand {
                            sql: sql.clone(),
                            ..Default::default()
                        },
                    )),
                },
                ConnectCommand::SqlQuery { sql } => proto::Command {
                    command_type: Some(proto::command::CommandType::SqlCommand(
                        proto::SqlCommand {
                            input: Some(proto::Relation {
                                rel_type: Some(proto::relation::RelType::Sql(proto::Sql {
                                    query: sql.clone(),
                                    ..Default::default()
                                })),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    )),
                },
                ConnectCommand::Unsupported(_) => proto::Command::default(),
            })),
        },
        ConnectExecutionPlan::Missing => proto::Plan { op_type: None },
    }
}

pub(super) fn materialize_relation(relation: &ConnectRelation) -> proto::Relation {
    use proto::relation::RelType;

    let rel_type = match &relation.kind {
        ConnectRelationKind::Range {
            start,
            end,
            step,
            partitions,
        } => Some(RelType::Range(proto::Range {
            start: *start,
            end: *end,
            step: *step,
            num_partitions: *partitions,
        })),
        ConnectRelationKind::Local { data, schema } => {
            Some(RelType::LocalRelation(proto::LocalRelation {
                data: data.clone(),
                schema: schema.clone(),
            }))
        }
        ConnectRelationKind::CachedLocal => Some(RelType::CachedLocalRelation(
            proto::CachedLocalRelation::default(),
        )),
        ConnectRelationKind::Read {
            is_streaming,
            named_table,
        } => Some(RelType::Read(proto::Read {
            read_type: named_table.as_ref().map(|table| {
                proto::read::ReadType::NamedTable(proto::read::NamedTable {
                    unparsed_identifier: table.identifier.clone(),
                    options: table.options.iter().cloned().collect(),
                })
            }),
            is_streaming: *is_streaming,
        })),
        ConnectRelationKind::Alias {
            input,
            alias,
            qualifier,
        } => Some(RelType::SubqueryAlias(Box::new(proto::SubqueryAlias {
            input: input.as_deref().map(materialize_relation).map(Box::new),
            alias: alias.clone(),
            qualifier: qualifier.clone(),
        }))),
        ConnectRelationKind::Filter { input, condition } => {
            Some(RelType::Filter(Box::new(proto::Filter {
                input: input.as_deref().map(materialize_relation).map(Box::new),
                condition: condition.as_ref().map(materialize_expression),
            })))
        }
        ConnectRelationKind::Project { input, expressions } => {
            Some(RelType::Project(Box::new(proto::Project {
                input: input.as_deref().map(materialize_relation).map(Box::new),
                expressions: expressions.iter().map(materialize_expression).collect(),
            })))
        }
        ConnectRelationKind::Limit { input, limit } => {
            Some(RelType::Limit(Box::new(proto::Limit {
                input: input.as_deref().map(materialize_relation).map(Box::new),
                limit: i32::try_from(*limit).unwrap_or(i32::MAX),
            })))
        }
        ConnectRelationKind::Join {
            left,
            right,
            condition,
            join_type,
            using_columns,
            has_join_data_type,
        } => Some(RelType::Join(Box::new(proto::Join {
            left: left.as_deref().map(materialize_relation).map(Box::new),
            right: right.as_deref().map(materialize_relation).map(Box::new),
            join_condition: condition.as_ref().map(materialize_expression),
            join_type: join_type_value(*join_type),
            using_columns: using_columns.clone(),
            join_data_type: has_join_data_type.then(proto::join::JoinDataType::default),
        }))),
        ConnectRelationKind::Aggregate {
            input,
            group_type,
            grouping_expressions,
            aggregate_expressions,
            has_pivot,
            has_grouping_sets,
        } => Some(RelType::Aggregate(Box::new(proto::Aggregate {
            input: input.as_deref().map(materialize_relation).map(Box::new),
            group_type: group_type_value(*group_type),
            grouping_expressions: grouping_expressions
                .iter()
                .map(materialize_expression)
                .collect(),
            aggregate_expressions: aggregate_expressions
                .iter()
                .map(materialize_expression)
                .collect(),
            pivot: has_pivot.then(proto::aggregate::Pivot::default),
            grouping_sets: has_grouping_sets
                .then(proto::aggregate::GroupingSets::default)
                .into_iter()
                .collect(),
        }))),
        ConnectRelationKind::Sql { query, .. } => Some(RelType::Sql(proto::Sql {
            query: query.clone(),
            ..Default::default()
        })),
        ConnectRelationKind::Unsupported(_) => Some(RelType::Unknown(proto::Unknown::default())),
        ConnectRelationKind::Missing => None,
    };
    proto::Relation {
        common: relation.plan_id.map(|plan_id| proto::RelationCommon {
            plan_id: Some(plan_id),
            ..Default::default()
        }),
        rel_type,
    }
}

fn materialize_expression(expression: &ConnectExpression) -> proto::Expression {
    use proto::expression::ExprType;

    let expr_type = match expression {
        ConnectExpression::Literal(literal) => {
            let literal_type = match literal {
                ConnectLiteral::Integer(value) => {
                    Some(proto::expression::literal::LiteralType::Integer(*value))
                }
                ConnectLiteral::Long(value) => {
                    Some(proto::expression::literal::LiteralType::Long(*value))
                }
                ConnectLiteral::Boolean(value) => {
                    Some(proto::expression::literal::LiteralType::Boolean(*value))
                }
                ConnectLiteral::Double(value) => {
                    Some(proto::expression::literal::LiteralType::Double(*value))
                }
                ConnectLiteral::String(value) => Some(
                    proto::expression::literal::LiteralType::String(value.clone()),
                ),
                ConnectLiteral::Null => None,
                ConnectLiteral::Unsupported(_) => None,
                ConnectLiteral::Missing => None,
            };
            Some(ExprType::Literal(proto::expression::Literal {
                literal_type,
            }))
        }
        ConnectExpression::Attribute {
            identifier,
            plan_id,
            is_metadata_column,
        } => Some(ExprType::UnresolvedAttribute(
            proto::expression::UnresolvedAttribute {
                unparsed_identifier: identifier.clone(),
                plan_id: *plan_id,
                is_metadata_column: Some(*is_metadata_column),
            },
        )),
        ConnectExpression::Function {
            name,
            arguments,
            is_distinct,
            is_user_defined,
            is_internal,
        } => Some(ExprType::UnresolvedFunction(
            proto::expression::UnresolvedFunction {
                function_name: name.clone(),
                arguments: arguments.iter().map(materialize_expression).collect(),
                is_distinct: *is_distinct,
                is_user_defined_function: *is_user_defined,
                is_internal: Some(*is_internal),
            },
        )),
        ConnectExpression::ExpressionString(value) => Some(ExprType::ExpressionString(
            proto::expression::ExpressionString {
                expression: value.clone(),
            },
        )),
        ConnectExpression::Alias {
            expression,
            names,
            metadata,
        } => Some(ExprType::Alias(Box::new(proto::expression::Alias {
            expr: expression
                .as_deref()
                .map(materialize_expression)
                .map(Box::new),
            name: names.clone(),
            metadata: metadata.clone(),
        }))),
        ConnectExpression::Star => Some(ExprType::UnresolvedStar(
            proto::expression::UnresolvedStar::default(),
        )),
        ConnectExpression::Unsupported(_) => None,
        ConnectExpression::Missing => None,
    };
    proto::Expression {
        expr_type,
        ..Default::default()
    }
}

fn join_type_value(join_type: ConnectJoinType) -> i32 {
    match join_type {
        ConnectJoinType::Unspecified => proto::join::JoinType::Unspecified as i32,
        ConnectJoinType::Inner => proto::join::JoinType::Inner as i32,
        ConnectJoinType::FullOuter => proto::join::JoinType::FullOuter as i32,
        ConnectJoinType::LeftOuter => proto::join::JoinType::LeftOuter as i32,
        ConnectJoinType::RightOuter => proto::join::JoinType::RightOuter as i32,
        ConnectJoinType::LeftAnti => proto::join::JoinType::LeftAnti as i32,
        ConnectJoinType::LeftSemi => proto::join::JoinType::LeftSemi as i32,
        ConnectJoinType::Cross => proto::join::JoinType::Cross as i32,
        ConnectJoinType::Unknown(value) => value,
    }
}

fn group_type_value(group_type: ConnectGroupType) -> i32 {
    match group_type {
        ConnectGroupType::GroupBy => proto::aggregate::GroupType::Groupby as i32,
        ConnectGroupType::Other(value) => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql_command(query: &str) -> proto::Plan {
        proto::Plan {
            op_type: Some(proto::plan::OpType::Command(proto::Command {
                command_type: Some(proto::command::CommandType::SqlCommand(proto::SqlCommand {
                    input: Some(proto::Relation {
                        rel_type: Some(proto::relation::RelType::Sql(proto::Sql {
                            query: query.into(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
            })),
        }
    }

    #[test]
    fn current_client_sql_command_input_routes_reads_and_writes_by_effect() {
        let read = normalize_plan(&sql_command("SELECT 7 AS value")).unwrap();
        assert!(matches!(
            read,
            ConnectExecutionPlan::Command(ConnectCommand::SqlQuery { ref sql })
                if sql == "SELECT 7 AS value"
        ));

        let write = normalize_plan(&sql_command("CREATE TABLE items (id BIGINT)")).unwrap();
        assert!(matches!(
            write,
            ConnectExecutionPlan::Command(ConnectCommand::Sql { ref sql })
                if sql == "CREATE TABLE items (id BIGINT)"
        ));
    }

    #[test]
    fn sql_command_rejects_parameterized_and_non_sql_inputs() {
        let mut parameterized = sql_command("SELECT ?");
        let Some(proto::plan::OpType::Command(command)) = parameterized.op_type.as_mut() else {
            unreachable!()
        };
        let Some(proto::command::CommandType::SqlCommand(sql)) = command.command_type.as_mut()
        else {
            unreachable!()
        };
        let Some(input) = sql.input.as_mut() else {
            unreachable!()
        };
        let Some(proto::relation::RelType::Sql(query)) = input.rel_type.as_mut() else {
            unreachable!()
        };
        query.pos_arguments.push(proto::Expression::default());
        assert!(matches!(
            normalize_plan(&parameterized).unwrap(),
            ConnectExecutionPlan::Command(ConnectCommand::Unsupported(_))
        ));

        let mut unrelated_input = sql_command("SELECT 7");
        let Some(proto::plan::OpType::Command(command)) = unrelated_input.op_type.as_mut() else {
            unreachable!()
        };
        let Some(proto::command::CommandType::SqlCommand(sql)) = command.command_type.as_mut()
        else {
            unreachable!()
        };
        sql.input = Some(proto::Relation {
            rel_type: Some(proto::relation::RelType::Range(proto::Range {
                end: 1,
                step: 1,
                ..Default::default()
            })),
            ..Default::default()
        });
        assert!(matches!(
            normalize_plan(&unrelated_input).unwrap(),
            ConnectExecutionPlan::Command(ConnectCommand::Unsupported(_))
        ));
    }
}
