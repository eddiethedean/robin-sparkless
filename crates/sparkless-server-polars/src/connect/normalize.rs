//! Convert Spark Connect wire messages into the server core's transport-neutral plan model.

use crate::connect_proto as proto;
use sparkless_server_core::{
    ConnectExecutionPlan, ConnectExpression, ConnectGroupType, ConnectJoinType, ConnectLiteral,
    ConnectNamedTable, ConnectRelation, ConnectRelationKind,
};
use tonic::Status;

const MAX_PLAN_DEPTH: usize = 64;

pub(super) fn normalize_plan(plan: &proto::Plan) -> Result<ConnectExecutionPlan, Status> {
    Ok(match plan.op_type.as_ref() {
        Some(proto::plan::OpType::Root(relation)) => {
            ConnectExecutionPlan::Root(normalize_relation(relation, 1)?)
        }
        Some(proto::plan::OpType::Command(_)) => ConnectExecutionPlan::Command,
        None => ConnectExecutionPlan::Missing,
    })
}

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
        Some(_) => ConnectRelationKind::Unsupported("relation type".into()),
        None => ConnectRelationKind::Unsupported("missing relation type".into()),
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
pub(super) fn materialize_plan(plan: &ConnectExecutionPlan) -> proto::Plan {
    match plan {
        ConnectExecutionPlan::Root(relation) => proto::Plan {
            op_type: Some(proto::plan::OpType::Root(materialize_relation(relation))),
        },
        ConnectExecutionPlan::Command => proto::Plan {
            op_type: Some(proto::plan::OpType::Command(proto::Command::default())),
        },
        ConnectExecutionPlan::Missing => proto::Plan { op_type: None },
    }
}

fn materialize_relation(relation: &ConnectRelation) -> proto::Relation {
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
        ConnectRelationKind::Unsupported(_) => Some(RelType::Unknown(proto::Unknown::default())),
    };
    let mut result = proto::Relation {
        common: relation.plan_id.map(|plan_id| proto::RelationCommon {
            plan_id: Some(plan_id),
            ..Default::default()
        }),
        rel_type,
    };
    if result.rel_type.is_none() {
        result.rel_type = Some(RelType::Unknown(proto::Unknown::default()));
    }
    result
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
                ConnectLiteral::Unsupported(_) | ConnectLiteral::Missing => None,
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
        ConnectExpression::Unsupported(_) | ConnectExpression::Missing => None,
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
