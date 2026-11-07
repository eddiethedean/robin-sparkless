//! DuckDB implementation of the frozen Spark Connect relation and command subset.

use super::DuckDbConnectSession;
use crate::connect::{
    MAX_CONNECT_LOCAL_DECODED_BYTES, MAX_CONNECT_LOCAL_IPC_BYTES, MAX_CONNECT_LOCAL_ROWS,
};
use robin_sparkless_core::engine::{ArrowIpcBatchStream, ArrowIpcStream, CancellationToken};
use robin_sparkless_core::{DataType, EngineError, LogicalField, StructType};
use robin_sparkless_duckdb::{
    ArrowIpcQueryOptions, DUCKDB_EXECUTION_MAX_BATCH_BYTES, DUCKDB_EXECUTION_MAX_BATCH_ROWS,
};
use robin_sparkless_sql::{
    ExpressionShape, FromPlan, GroupByPlan, JoinConstraintPlan, JoinKind, Projection, RelationPlan,
    RelationalNode, SelectPlan, SqlExpression, SqlQueryPlan, StatementPlan,
};
use sparkless_server_core::{
    ConnectCommand, ConnectExecutionPlan, ConnectExecutionPort, ConnectExecutionRequest,
    ConnectExecutionResult, ConnectExpression, ConnectGroupType, ConnectJoinType, ConnectLiteral,
    ConnectOperationIdentity, ConnectRelation, ConnectRelationKind, ExecutionError,
};
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, SyncSender};
use std::thread;

const MAX_RELATION_DEPTH: usize = 64;
const MAX_RANGE_ROWS: u128 = 1_000_000;

pub struct DuckDbConnectExecution;

impl ConnectExecutionPort for DuckDbConnectExecution {
    fn execute_connect_plan(
        &self,
        session: &sparkless_server_core::EngineSessionHandle,
        request: ConnectExecutionRequest,
    ) -> Result<ConnectExecutionResult, ExecutionError> {
        let session = session
            .downcast_ref::<DuckDbConnectSession>("duckdb")
            .ok_or_else(|| {
                failure(
                    "MismatchedExecutionSession",
                    "session is not owned by DuckDB execution",
                )
            })?;
        match &request.plan {
            ConnectExecutionPlan::Command(ConnectCommand::SqlQuery { .. }) => {
                // The Spark client sends SELECT through ExecuteCommand first to distinguish a
                // command result from a lazy DataFrame. The SQL relation runs later on action.
                Ok(ConnectExecutionResult {
                    schema: None,
                    batches: ArrowIpcBatchStream::new(std::iter::empty()),
                    sql_command_result: None,
                })
            }
            ConnectExecutionPlan::Command(command) => {
                execute_command(
                    session,
                    command,
                    &request.operation_identity,
                    &request.cancellation,
                )?;
                Ok(ConnectExecutionResult {
                    schema: None,
                    batches: ArrowIpcBatchStream::new(std::iter::empty()),
                    sql_command_result: Some(empty_sql_command_result()),
                })
            }
            ConnectExecutionPlan::Root(relation) => {
                let expected_schema =
                    describe_connect_plan(session, &request.plan, &request.cancellation)?;
                let mut renderer = PlanRenderer::new(session, relation, &request.cancellation)?;
                let sql = format!("SELECT * FROM {}", renderer.relation(relation, 1)?);
                stream_query(
                    session,
                    PreparedConnectQuery {
                        sql,
                        local_inputs: renderer.local_inputs,
                        expected_input_schemas: renderer.local_input_schemas,
                        expected_schema,
                    },
                    request.max_batch_rows,
                    request.max_response_bytes,
                    request.cancellation,
                )
            }
            ConnectExecutionPlan::Missing => Err(failure(
                "CONNECT_PLAN_MISSING",
                "Connect execution plan is missing",
            )),
        }
    }
}

pub(super) fn describe_connect_plan(
    session: &DuckDbConnectSession,
    plan: &ConnectExecutionPlan,
    cancellation: &CancellationToken,
) -> Result<StructType, ExecutionError> {
    let ConnectExecutionPlan::Root(relation) = plan else {
        return Err(failure(
            "CONNECT_ANALYSIS_UNSUPPORTED",
            "only relational plans can be analyzed by DuckDB",
        ));
    };
    let mut renderer = PlanRenderer::new(session, relation, cancellation)?;
    let sql = format!("SELECT * FROM {}", renderer.relation(relation, 1)?);
    if !renderer.local_inputs.is_empty() {
        // LocalRelation schema is encoded in its Arrow stream, so the same bounded owner path
        // validates it and obtains the output schema without collecting result rows.
        let (sender, receiver) = mpsc::sync_channel(1);
        let owner = session.owner.clone();
        let inputs = renderer.local_inputs;
        let expected_input_schemas = renderer.local_input_schemas;
        let worker_cancellation = cancellation.clone();
        thread::spawn(move || {
            if let Err(error) = owner.stream_query_with_arrow_ipc(
                ArrowIpcQueryOptions {
                    sql: &sql,
                    inputs: &inputs,
                    expected_input_schemas: &expected_input_schemas,
                    expected_output_schema: None,
                    max_input_bytes: MAX_CONNECT_LOCAL_IPC_BYTES,
                    max_input_rows: MAX_CONNECT_LOCAL_ROWS,
                    max_input_decoded_bytes: MAX_CONNECT_LOCAL_DECODED_BYTES,
                    token: &worker_cancellation,
                    max_batch_rows: DUCKDB_EXECUTION_MAX_BATCH_ROWS,
                    max_batch_bytes: DUCKDB_EXECUTION_MAX_BATCH_BYTES,
                },
                |schema| send_event(&sender, DuckDbEvent::Schema(schema)),
                |_| {
                    Err(EngineError::internal(
                        "schema-only analysis stopped Arrow result consumption".into(),
                    ))
                },
            ) {
                let _ = sender.send(DuckDbEvent::Error(error));
            }
        });
        let schema = match receiver.recv() {
            Ok(DuckDbEvent::Schema(schema)) => Ok(schema),
            Ok(DuckDbEvent::Error(error)) => Err(engine_failure(error)),
            _ => Err(failure(
                "CONNECT_ANALYSIS_FAILED",
                "DuckDB did not return an output schema",
            )),
        }?;
        return normalize_connect_schema(session, relation, schema, cancellation);
    }
    let schema = session
        .owner
        .describe_query(&sql, cancellation)
        .map_err(engine_failure)?;
    normalize_connect_schema(session, relation, schema, cancellation)
}

fn normalize_connect_schema(
    session: &DuckDbConnectSession,
    relation: &ConnectRelation,
    actual: StructType,
    cancellation: &CancellationToken,
) -> Result<StructType, ExecutionError> {
    match &relation.kind {
        ConnectRelationKind::Range { .. } => {
            if actual.logical_fields().len() != 1 {
                return Err(failure(
                    "CONNECT_SCHEMA_MISMATCH",
                    "DuckDB range output must have one field",
                ));
            }
            let mut field = actual.logical_fields()[0].clone();
            if !same_logical_type(&field.data_type, &DataType::Long)? {
                return Err(failure(
                    "CONNECT_SCHEMA_MISMATCH",
                    "DuckDB range output must be a 64-bit integer",
                ));
            }
            field.name = "id".into();
            field.data_type = DataType::Long;
            field.nullable = false;
            Ok(StructType::from_logical_fields(vec![field]))
        }
        ConnectRelationKind::Alias { input, .. }
        | ConnectRelationKind::Filter { input, .. }
        | ConnectRelationKind::Limit { input, .. } => {
            let input = input.as_deref().ok_or_else(|| {
                failure(
                    "CONNECT_RELATION_INPUT_REQUIRED",
                    "Connect wrapper relation requires an input",
                )
            })?;
            let expected = describe_connect_plan(
                session,
                &ConnectExecutionPlan::Root(input.clone()),
                cancellation,
            )?;
            validate_logical_types(&actual, &expected)?;
            Ok(expected)
        }
        ConnectRelationKind::Join { .. } => {
            let expected = join_output_schema(session, relation, cancellation)?;
            validate_logical_types(&actual, &expected)?;
            Ok(expected)
        }
        ConnectRelationKind::Project {
            input: Some(input),
            expressions,
        } => project_output_schema(session, input, expressions, actual, cancellation),
        ConnectRelationKind::Aggregate {
            input: Some(input),
            grouping_expressions,
            aggregate_expressions,
            ..
        } => aggregate_output_schema(
            session,
            input,
            grouping_expressions,
            aggregate_expressions,
            actual,
            cancellation,
        ),
        ConnectRelationKind::Sql { query, .. } => {
            normalize_sql_output_schema(session, query, actual, cancellation)
        }
        ConnectRelationKind::Project { input: None, .. }
        | ConnectRelationKind::Aggregate { input: None, .. } => Err(failure(
            "CONNECT_RELATION_INPUT_REQUIRED",
            "Connect projection or aggregate requires an input",
        )),
        _ => Ok(actual),
    }
}

fn project_output_schema(
    session: &DuckDbConnectSession,
    input: &ConnectRelation,
    expressions: &[ConnectExpression],
    actual: StructType,
    cancellation: &CancellationToken,
) -> Result<StructType, ExecutionError> {
    if actual.logical_fields().len() != expressions.len() {
        return Err(failure(
            "CONNECT_SCHEMA_MISMATCH",
            "DuckDB projection width does not match Spark Connect expressions",
        ));
    }
    let source_schema = describe_connect_plan(
        session,
        &ConnectExecutionPlan::Root(input.clone()),
        cancellation,
    )?;
    if !relation_contains_join(input) {
        for expression in expressions {
            if let Some((_, identifier)) = projection_attribute(expression) {
                let name = identifier.rsplit('.').next().unwrap_or(&identifier);
                if source_schema
                    .logical_fields()
                    .iter()
                    .filter(|field| field.name.eq_ignore_ascii_case(name))
                    .take(2)
                    .count()
                    > 1
                {
                    return Err(failure(
                        "CONNECT_ATTRIBUTE_AMBIGUOUS",
                        "Connect column reference is ambiguous",
                    ));
                }
            }
        }
    }
    let source_fields = if let Some(join) = transparent_join(input) {
        Some(join_source_field_map(
            session,
            join,
            cancellation,
            &source_schema,
        )?)
    } else {
        None
    };
    let mut fields = actual.logical_fields().to_vec();
    for (field, expression) in fields.iter_mut().zip(expressions) {
        if let Some(name) = projection_output_name(expression, &field.name) {
            field.name = name;
        }
        if let Some(source) =
            source_field_for_expression(expression, &source_schema, source_fields.as_ref())
        {
            if !same_logical_type(&field.data_type, &source.data_type)? {
                return Err(failure(
                    "CONNECT_SCHEMA_MISMATCH",
                    "DuckDB projected attribute type does not match its Spark Connect input",
                ));
            }
            field.data_type = source.data_type.clone();
            field.nullable = source.nullable;
            field.metadata = source.metadata.clone();
        } else if let Some(nullable) =
            expression_nullability(expression, &source_schema, source_fields.as_ref())
        {
            field.nullable = nullable;
        }
        if let Some(metadata) = expression_alias_metadata(expression)? {
            field.metadata = metadata;
        }
    }
    Ok(StructType::from_logical_fields(fields))
}

fn aggregate_output_schema(
    session: &DuckDbConnectSession,
    input: &ConnectRelation,
    grouping_expressions: &[ConnectExpression],
    aggregate_expressions: &[ConnectExpression],
    actual: StructType,
    cancellation: &CancellationToken,
) -> Result<StructType, ExecutionError> {
    if actual.logical_fields().len() != grouping_expressions.len() + aggregate_expressions.len() {
        return Err(failure(
            "CONNECT_SCHEMA_MISMATCH",
            "DuckDB aggregate output width does not match Spark Connect expressions",
        ));
    }
    let source_schema = describe_connect_plan(
        session,
        &ConnectExecutionPlan::Root(input.clone()),
        cancellation,
    )?;
    let source_fields = if let Some(join) = transparent_join(input) {
        Some(join_source_field_map(
            session,
            join,
            cancellation,
            &source_schema,
        )?)
    } else {
        None
    };
    let mut fields = actual.logical_fields().to_vec();
    for (index, expression) in grouping_expressions.iter().enumerate() {
        let field = &mut fields[index];
        if let Some(name) = projection_output_name(expression, &field.name) {
            field.name = name;
        }
        if let Some(source) =
            source_field_for_expression(expression, &source_schema, source_fields.as_ref())
        {
            field.data_type = source.data_type.clone();
            field.nullable = source.nullable;
            field.metadata = source.metadata.clone();
        }
        if let Some(metadata) = expression_alias_metadata(expression)? {
            field.metadata = metadata;
        }
    }
    for (offset, expression) in aggregate_expressions.iter().enumerate() {
        let index = grouping_expressions.len() + offset;
        let field = &mut fields[index];
        let (aggregate, explicit_alias) = aggregate_function(expression)?;
        match aggregate {
            "count" => {
                field.data_type = DataType::Long;
                field.nullable = false;
            }
            "sum" => {
                let (_, argument) = aggregate_expression(expression).ok_or_else(|| {
                    failure("CONNECT_AGGREGATE_UNSUPPORTED", "sum requires one argument")
                })?;
                let source =
                    source_field_for_expression(argument, &source_schema, source_fields.as_ref())
                        .ok_or_else(|| {
                        failure(
                            "CONNECT_AGGREGATE_UNSUPPORTED",
                            "sum requires a resolvable input column",
                        )
                    })?;
                field.data_type = spark_sum_type(&source.data_type)?.0;
                field.nullable = true;
            }
            _ => {
                return Err(failure(
                    "CONNECT_AGGREGATE_UNSUPPORTED",
                    "DuckDB execution enables count and sum aggregates only",
                ));
            }
        }
        field.name = aggregate_output_name(expression, &field.name);
        if let Some(metadata) = expression_alias_metadata(expression)? {
            field.metadata = metadata;
        } else if aggregate == "sum" && !explicit_alias {
            field.metadata.insert(
                "__autoGeneratedAlias".into(),
                serde_json::Value::String("true".into()),
            );
        }
    }
    Ok(StructType::from_logical_fields(fields))
}

fn aggregate_expression(expression: &ConnectExpression) -> Option<(&str, &ConnectExpression)> {
    let expression = match expression {
        ConnectExpression::Alias {
            expression: Some(expression),
            ..
        } => expression.as_ref(),
        expression => expression,
    };
    let ConnectExpression::Function {
        name, arguments, ..
    } = expression
    else {
        return None;
    };
    let argument = arguments.first()?;
    Some((name.as_str(), argument))
}

fn aggregate_function(expression: &ConnectExpression) -> Result<(&str, bool), ExecutionError> {
    let explicit_alias = matches!(expression, ConnectExpression::Alias { .. });
    let _ = expression_alias_metadata(expression)?;
    let expression = match expression {
        ConnectExpression::Alias {
            expression: Some(expression),
            names,
            ..
        } if names.len() == 1 => expression.as_ref(),
        ConnectExpression::Alias { .. } => {
            return Err(failure(
                "CONNECT_ALIAS_UNSUPPORTED",
                "aggregate aliases require one name",
            ));
        }
        expression => expression,
    };
    let ConnectExpression::Function { name, .. } = expression else {
        return Err(failure(
            "CONNECT_AGGREGATE_UNSUPPORTED",
            "aggregate output must be count or sum",
        ));
    };
    let name = name.to_ascii_lowercase();
    if !matches!(name.as_str(), "count" | "sum") {
        return Err(failure(
            "CONNECT_AGGREGATE_UNSUPPORTED",
            "aggregate output must be count or sum",
        ));
    }
    Ok((if name == "sum" { "sum" } else { "count" }, explicit_alias))
}

fn aggregate_alias_name(expression: &ConnectExpression) -> Option<&str> {
    match expression {
        ConnectExpression::Alias { names, .. } if names.len() == 1 => {
            names.first().map(String::as_str)
        }
        _ => None,
    }
}

fn aggregate_output_name(expression: &ConnectExpression, fallback: &str) -> String {
    if let Some(name) = aggregate_alias_name(expression) {
        return name.to_string();
    }
    let Some((function, argument)) = aggregate_expression(expression) else {
        return fallback.to_string();
    };
    let argument_name = projection_attribute(argument)
        .map(|(_, name)| name)
        .or_else(|| match argument {
            ConnectExpression::Literal(ConnectLiteral::Integer(value)) => Some(value.to_string()),
            ConnectExpression::Literal(ConnectLiteral::Long(value)) => Some(value.to_string()),
            _ => None,
        });
    let argument_name = argument_name.or_else(|| {
        if function.eq_ignore_ascii_case("count") {
            Some("1".into())
        } else {
            None
        }
    });
    argument_name.map_or_else(
        || fallback.to_string(),
        |argument| format!("{}({argument})", function.to_ascii_lowercase()),
    )
}

fn spark_sum_type(data_type: &DataType) -> Result<(DataType, String), ExecutionError> {
    match data_type {
        DataType::Byte | DataType::Short | DataType::Integer | DataType::Long => {
            Ok((DataType::Long, "BIGINT".into()))
        }
        DataType::Float | DataType::Double => Ok((DataType::Double, "DOUBLE".into())),
        DataType::Decimal(precision, scale) => {
            let precision = precision.saturating_add(10).min(38);
            Ok((
                DataType::Decimal(precision, *scale),
                format!("DECIMAL({precision},{scale})"),
            ))
        }
        _ => Err(failure(
            "CONNECT_AGGREGATE_TYPE_UNSUPPORTED",
            "sum supports signed numeric columns only",
        )),
    }
}

fn projection_output_name(expression: &ConnectExpression, fallback: &str) -> Option<String> {
    match expression {
        ConnectExpression::Alias { names, .. } if names.len() == 1 => names.first().cloned(),
        ConnectExpression::Attribute { identifier, .. } => Some(
            identifier
                .rsplit('.')
                .next()
                .unwrap_or(identifier)
                .trim_matches('`')
                .trim_matches('"')
                .to_string(),
        ),
        ConnectExpression::ExpressionString(expression) => expression_string_plan(expression)
            .and_then(|(_, alias)| alias)
            .or_else(|| Some(fallback.into())),
        _ => Some(fallback.into()),
    }
}

fn source_field_for_expression(
    expression: &ConnectExpression,
    source_schema: &StructType,
    plan_fields: Option<&HashMap<i64, Vec<LogicalField>>>,
) -> Option<LogicalField> {
    if let Some((Some(plan_id), identifier)) = projection_attribute(expression)
        && let Some(fields) = plan_fields.and_then(|fields| fields.get(&plan_id))
    {
        let mut matches = fields
            .iter()
            .filter(|field| field.name.eq_ignore_ascii_case(&identifier));
        let field = matches.next()?;
        return matches.next().is_none().then(|| field.clone());
    }
    let (_, identifier) = projection_attribute(expression)?;
    let mut matches = source_schema
        .logical_fields()
        .iter()
        .filter(|field| field.name.eq_ignore_ascii_case(&identifier));
    let field = matches.next()?;
    matches.next().is_none().then(|| field.clone())
}

fn expression_nullability(
    expression: &ConnectExpression,
    source_schema: &StructType,
    plan_fields: Option<&HashMap<i64, Vec<LogicalField>>>,
) -> Option<bool> {
    match expression {
        ConnectExpression::Alias {
            expression: Some(expression),
            ..
        } => expression_nullability(expression, source_schema, plan_fields),
        ConnectExpression::Attribute { .. } => {
            source_field_for_expression(expression, source_schema, plan_fields)
                .map(|field| field.nullable)
        }
        ConnectExpression::Literal(ConnectLiteral::Null) => Some(true),
        ConnectExpression::Literal(_) => Some(false),
        ConnectExpression::Function {
            name, arguments, ..
        } => {
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "isnull" | "isnotnull" | "count"
            ) {
                return Some(false);
            }
            if matches!(name.as_str(), "/" | "%") {
                // Spark marks division and remainder nullable because their denominator may be
                // zero, even when the current input schema is non-nullable.
                return Some(true);
            }
            let nullabilities = arguments
                .iter()
                .map(|argument| expression_nullability(argument, source_schema, plan_fields))
                .collect::<Option<Vec<_>>>()?;
            Some(nullabilities.into_iter().any(|nullable| nullable))
        }
        ConnectExpression::ExpressionString(expression) => {
            let (expression, _) = expression_string_plan(expression)?;
            sql_expression_nullability(&expression, source_schema)
        }
        _ => None,
    }
}

fn sql_expression_nullability(
    expression: &SqlExpression,
    source_schema: &StructType,
) -> Option<bool> {
    match &expression.shape {
        ExpressionShape::Identifier(parts) => {
            let name = parts.last()?;
            let mut matches = source_schema
                .logical_fields()
                .iter()
                .filter(|field| field.name.eq_ignore_ascii_case(name));
            let field = matches.next()?;
            matches.next().is_none().then_some(field.nullable)
        }
        ExpressionShape::Literal => Some(expression.sql.eq_ignore_ascii_case("NULL")),
        ExpressionShape::IsNull { .. } => Some(false),
        ExpressionShape::Unary { expression, .. } => {
            sql_expression_nullability(expression, source_schema)
        }
        ExpressionShape::Binary {
            operator,
            left,
            right,
        } => {
            if matches!(operator.as_str(), "/" | "%") {
                Some(true)
            } else {
                Some(
                    sql_expression_nullability(left, source_schema)?
                        || sql_expression_nullability(right, source_schema)?,
                )
            }
        }
        ExpressionShape::Function { name, .. } if name.eq_ignore_ascii_case("count") => Some(false),
        // SUM can return NULL for empty input even when its argument is non-nullable.
        ExpressionShape::Function { name, .. } if name.eq_ignore_ascii_case("sum") => Some(true),
        ExpressionShape::Function { arguments, .. } => Some(
            arguments
                .iter()
                .map(|argument| sql_expression_nullability(argument, source_schema))
                .collect::<Option<Vec<_>>>()?
                .into_iter()
                .any(|nullable| nullable),
        ),
        ExpressionShape::Composite => None,
    }
}

fn normalize_sql_output_schema(
    session: &DuckDbConnectSession,
    source_sql: &str,
    actual: StructType,
    cancellation: &CancellationToken,
) -> Result<StructType, ExecutionError> {
    let statement = robin_sparkless_sql::parse_statement(source_sql)
        .map_err(|error| failure("CONNECT_SQL_INVALID", &error.to_string()))?;
    let StatementPlan::Query(query) = robin_sparkless_sql::lower_statement(&statement)
        .map_err(|error| failure("CONNECT_SQL_UNSUPPORTED", &error.to_string()))?
    else {
        return Err(failure(
            "CONNECT_SQL_UNSUPPORTED",
            "SQL relations require one read-only SELECT",
        ));
    };
    let RelationalNode::Select(select) = query.body else {
        return Err(failure(
            "CONNECT_SQL_UNSUPPORTED",
            "SQL relations require one SELECT body",
        ));
    };
    if select.projection.len() != actual.logical_fields().len()
        || select.projection.iter().any(|projection| {
            matches!(
                projection,
                Projection::Wildcard | Projection::QualifiedWildcard(_)
            )
        })
    {
        // Wildcards inherit their physical names and nullability from DuckDB's source schema.
        return Ok(actual);
    }

    let source_schema = if let Some(from) = select.from.first() {
        let mut renderer = PlanRenderer {
            session,
            cancellation: cancellation.clone(),
            local_inputs: Vec::new(),
            local_input_schemas: Vec::new(),
        };
        let from = renderer.sql_from(from)?;
        session
            .owner
            .describe_query(&format!("SELECT * FROM {from}"), cancellation)
            .map_err(engine_failure)?
    } else {
        StructType::from_logical_fields(Vec::new())
    };
    let mut fields = actual.logical_fields().to_vec();
    for (field, projection) in fields.iter_mut().zip(select.projection) {
        let Projection::Expression { expression, alias } = projection else {
            continue;
        };
        if let Some(alias) = alias {
            field.name = alias;
        } else if let ExpressionShape::Identifier(parts) = &expression.shape
            && let Some(name) = parts.last()
        {
            // Spark preserves the requested identifier spelling in SQL result schemas even
            // though catalog lookup itself is case-insensitive.
            field.name = name.clone();
        }
        if let Some(nullable) = sql_expression_nullability(&expression, &source_schema) {
            field.nullable = nullable;
        }
        if let Some(data_type) = sql_expression_type(&expression, Some(&source_schema))? {
            field.data_type = data_type;
        }
    }
    Ok(StructType::from_logical_fields(fields))
}

fn expression_string_plan(expression: &str) -> Option<(SqlExpression, Option<String>)> {
    let statement = robin_sparkless_sql::parse_statement(&format!("SELECT {expression}")).ok()?;
    let StatementPlan::Query(query) = robin_sparkless_sql::lower_statement(&statement).ok()? else {
        return None;
    };
    let RelationalNode::Select(select) = query.body else {
        return None;
    };
    if select.projection.len() != 1 {
        return None;
    }
    match select.projection.into_iter().next()? {
        Projection::Expression { expression, alias } => Some((expression, alias)),
        Projection::Wildcard | Projection::QualifiedWildcard(_) => None,
    }
}

fn expression_alias_metadata(
    expression: &ConnectExpression,
) -> Result<Option<std::collections::BTreeMap<String, serde_json::Value>>, ExecutionError> {
    let ConnectExpression::Alias { metadata, .. } = expression else {
        return Ok(None);
    };
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let value = serde_json::from_str::<serde_json::Value>(metadata)
        .map_err(|error| failure("CONNECT_ALIAS_METADATA_INVALID", &error.to_string()))?;
    let Some(object) = value.as_object() else {
        return Err(failure(
            "CONNECT_ALIAS_METADATA_INVALID",
            "Connect alias metadata must be a JSON object",
        ));
    };
    Ok(Some(
        object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    ))
}

fn join_output_schema(
    session: &DuckDbConnectSession,
    relation: &ConnectRelation,
    cancellation: &CancellationToken,
) -> Result<StructType, ExecutionError> {
    let ConnectRelationKind::Join {
        left,
        right,
        using_columns,
        ..
    } = &relation.kind
    else {
        return Err(failure(
            "CONNECT_JOIN_UNSUPPORTED",
            "expected a Join relation",
        ));
    };
    let left = left
        .as_deref()
        .ok_or_else(|| failure("CONNECT_JOIN_LEFT_REQUIRED", "Join left input is required"))?;
    let right = right.as_deref().ok_or_else(|| {
        failure(
            "CONNECT_JOIN_RIGHT_REQUIRED",
            "Join right input is required",
        )
    })?;
    let left_schema = describe_connect_plan(
        session,
        &ConnectExecutionPlan::Root(left.clone()),
        cancellation,
    )?;
    let right_schema = describe_connect_plan(
        session,
        &ConnectExecutionPlan::Root(right.clone()),
        cancellation,
    )?;
    let using = using_columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut fields = left_schema.logical_fields().to_vec();
    if !using.is_empty() {
        for column in &using {
            let left_matches = left_schema
                .logical_fields()
                .iter()
                .filter(|field| field.name.eq_ignore_ascii_case(column))
                .collect::<Vec<_>>();
            let right_matches = right_schema
                .logical_fields()
                .iter()
                .filter(|field| field.name.eq_ignore_ascii_case(column))
                .collect::<Vec<_>>();
            if left_matches.len() != 1 || right_matches.len() != 1 {
                return Err(failure(
                    "CONNECT_JOIN_KEY_AMBIGUOUS",
                    "USING join keys must resolve to one field on each input",
                ));
            }
            if !same_logical_type(&left_matches[0].data_type, &right_matches[0].data_type)? {
                return Err(failure(
                    "CONNECT_JOIN_TYPE_UNSUPPORTED",
                    "USING join keys must have matching logical types",
                ));
            }
            if let Some(field) = fields
                .iter_mut()
                .find(|field| field.name.eq_ignore_ascii_case(column))
            {
                field.nullable = left_matches[0].nullable || right_matches[0].nullable;
            }
        }
    }
    fields.extend(
        right_schema
            .logical_fields()
            .iter()
            .filter(|field| !using.contains(&field.name.to_ascii_lowercase()))
            .cloned(),
    );
    Ok(StructType::from_logical_fields(fields))
}

fn join_source_field_map(
    session: &DuckDbConnectSession,
    relation: &ConnectRelation,
    cancellation: &CancellationToken,
    join_schema: &StructType,
) -> Result<HashMap<i64, Vec<robin_sparkless_core::LogicalField>>, ExecutionError> {
    let ConnectRelationKind::Join { left, right, .. } = &relation.kind else {
        return Err(failure(
            "CONNECT_JOIN_UNSUPPORTED",
            "expected a Join relation",
        ));
    };
    let mut schemas = HashMap::new();
    for input in [left.as_deref(), right.as_deref()] {
        let input = input
            .ok_or_else(|| failure("CONNECT_JOIN_INPUT_REQUIRED", "Join input is required"))?;
        let schema = describe_connect_plan(
            session,
            &ConnectExecutionPlan::Root(input.clone()),
            cancellation,
        )?;
        let mut ids = HashSet::new();
        collect_plan_ids(input, &mut ids, 1)?;
        for id in ids {
            schemas.insert(id, schema.logical_fields().to_vec());
        }
    }
    if let Some(plan_id) = relation.plan_id {
        schemas.insert(plan_id, join_schema.logical_fields().to_vec());
    }
    Ok(schemas)
}

fn transparent_join(relation: &ConnectRelation) -> Option<&ConnectRelation> {
    match &relation.kind {
        ConnectRelationKind::Join { .. } => Some(relation),
        ConnectRelationKind::Alias { input, .. }
        | ConnectRelationKind::Filter { input, .. }
        | ConnectRelationKind::Limit { input, .. } => input.as_deref().and_then(transparent_join),
        _ => None,
    }
}

fn projection_attribute(expression: &ConnectExpression) -> Option<(Option<i64>, String)> {
    match expression {
        ConnectExpression::Alias {
            expression: Some(expression),
            names,
            ..
        } if names.len() == 1 => projection_attribute(expression),
        ConnectExpression::Attribute {
            identifier,
            plan_id,
            is_metadata_column: false,
        } => Some((
            *plan_id,
            identifier
                .rsplit('.')
                .next()
                .unwrap_or(identifier)
                .trim_matches('`')
                .trim_matches('"')
                .to_string(),
        )),
        ConnectExpression::ExpressionString(expression) => {
            let (expression, _) = expression_string_plan(expression)?;
            let ExpressionShape::Identifier(parts) = expression.shape else {
                return None;
            };
            Some((None, parts.last()?.clone()))
        }
        _ => None,
    }
}

fn validate_logical_types(
    actual: &StructType,
    expected: &StructType,
) -> Result<(), ExecutionError> {
    if actual.logical_fields().len() != expected.logical_fields().len() {
        return Err(failure(
            "CONNECT_SCHEMA_MISMATCH",
            "DuckDB join output width does not match Spark Connect inputs",
        ));
    }
    for (actual, expected) in actual
        .logical_fields()
        .iter()
        .zip(expected.logical_fields())
    {
        if !same_logical_type(&actual.data_type, &expected.data_type)? {
            return Err(failure(
                "CONNECT_SCHEMA_MISMATCH",
                "DuckDB join output types do not match Spark Connect inputs",
            ));
        }
    }
    Ok(())
}

fn same_logical_type(
    left: &robin_sparkless_core::DataType,
    right: &robin_sparkless_core::DataType,
) -> Result<bool, ExecutionError> {
    let left = serde_json::to_value(left)
        .map_err(|error| failure("CONNECT_SCHEMA_INVALID", &error.to_string()))?;
    let right = serde_json::to_value(right)
        .map_err(|error| failure("CONNECT_SCHEMA_INVALID", &error.to_string()))?;
    Ok(left == right)
}

pub(super) fn analysis_response(
    session_id: &str,
    server_side_session_id: &str,
    engine_session: &sparkless_server_core::EngineSessionHandle,
    analyze: Option<crate::connect_proto::analyze_plan_request::Analyze>,
    cancellation: &CancellationToken,
) -> Result<crate::connect_proto::AnalyzePlanResponse, tonic::Status> {
    use crate::connect_proto as proto;
    use proto::analyze_plan_request::Analyze;
    use proto::analyze_plan_response::Result as AnalyzeResult;
    let session = engine_session
        .downcast_ref::<DuckDbConnectSession>("duckdb")
        .ok_or_else(|| {
            tonic::Status::failed_precondition("DuckDB analysis received a foreign session handle")
        })?;
    let analyze = analyze
        .ok_or_else(|| tonic::Status::invalid_argument("AnalyzePlanRequest.analyze is required"))?;
    let result = match analyze {
        Analyze::Schema(request) => {
            let plan = request.plan.as_ref().ok_or_else(|| {
                tonic::Status::invalid_argument("AnalyzePlan.Schema.plan is required")
            })?;
            let schema = describe_plan(session, plan, cancellation)?;
            AnalyzeResult::Schema(proto::analyze_plan_response::Schema {
                schema: Some(crate::connect::plan::schema_to_proto(&schema)?),
            })
        }
        Analyze::DdlParse(request) => {
            let schema = crate::connect::plan::parse_ddl_schema(&request.ddl_string)?;
            AnalyzeResult::DdlParse(proto::analyze_plan_response::DdlParse {
                parsed: Some(crate::connect::plan::schema_to_proto(&schema)?),
            })
        }
        Analyze::SparkVersion(_) => {
            AnalyzeResult::SparkVersion(proto::analyze_plan_response::SparkVersion {
                version: "4.0.5".into(),
            })
        }
        Analyze::IsLocal(request) => {
            let plan = request.plan.as_ref().ok_or_else(|| {
                tonic::Status::invalid_argument("AnalyzePlan.IsLocal.plan is required")
            })?;
            let _schema = describe_plan(session, plan, cancellation)?;
            AnalyzeResult::IsLocal(proto::analyze_plan_response::IsLocal { is_local: true })
        }
        Analyze::IsStreaming(request) => {
            let plan = request.plan.as_ref().ok_or_else(|| {
                tonic::Status::invalid_argument("AnalyzePlan.IsStreaming.plan is required")
            })?;
            let _schema = describe_plan(session, plan, cancellation)?;
            AnalyzeResult::IsStreaming(proto::analyze_plan_response::IsStreaming {
                is_streaming: false,
            })
        }
        Analyze::InputFiles(request) => {
            let plan = request.plan.as_ref().ok_or_else(|| {
                tonic::Status::invalid_argument("AnalyzePlan.InputFiles.plan is required")
            })?;
            let _schema = describe_plan(session, plan, cancellation)?;
            AnalyzeResult::InputFiles(proto::analyze_plan_response::InputFiles {
                files: Vec::new(),
            })
        }
        _ => {
            return Err(tonic::Status::unimplemented(
                "requested analysis operation is outside the DuckDB Connect subset",
            ));
        }
    };
    Ok(proto::AnalyzePlanResponse {
        session_id: session_id.to_string(),
        server_side_session_id: server_side_session_id.to_string(),
        result: Some(result),
    })
}

fn describe_plan(
    session: &DuckDbConnectSession,
    plan: &crate::connect_proto::Plan,
    cancellation: &CancellationToken,
) -> Result<StructType, tonic::Status> {
    let plan = super::normalize::normalize_plan(plan)?;
    describe_connect_plan(session, &plan, cancellation).map_err(|error| {
        tonic::Status::failed_precondition(format!("{}: {}", error.code, error.message))
    })
}

struct PreparedConnectQuery {
    sql: String,
    local_inputs: Vec<Vec<u8>>,
    expected_input_schemas: Vec<Option<StructType>>,
    expected_schema: StructType,
}

fn stream_query(
    session: &DuckDbConnectSession,
    query: PreparedConnectQuery,
    max_batch_rows: usize,
    max_response_bytes: usize,
    cancellation: CancellationToken,
) -> Result<ConnectExecutionResult, ExecutionError> {
    let max_rows = max_batch_rows.min(DUCKDB_EXECUTION_MAX_BATCH_ROWS);
    let max_bytes = max_response_bytes
        .saturating_sub(1024)
        .min(DUCKDB_EXECUTION_MAX_BATCH_BYTES);
    if max_rows == 0 || max_bytes == 0 {
        return Err(failure(
            "CONNECT_ARROW_LIMIT",
            "Connect response limits leave no room for an Arrow batch",
        ));
    }
    let (sender, receiver) = mpsc::sync_channel(1);
    let owner = session.owner.clone();
    let worker_sql = query.sql;
    let local_inputs = query.local_inputs;
    let expected_input_schemas = query.expected_input_schemas;
    let expected_schema = query.expected_schema;
    let worker_token = cancellation.clone();
    let worker_output_schema = expected_schema.clone();
    thread::spawn(move || {
        let result = owner.stream_query_with_arrow_ipc(
            ArrowIpcQueryOptions {
                sql: &worker_sql,
                inputs: &local_inputs,
                expected_input_schemas: &expected_input_schemas,
                expected_output_schema: Some(&worker_output_schema),
                max_input_bytes: MAX_CONNECT_LOCAL_IPC_BYTES,
                max_input_rows: MAX_CONNECT_LOCAL_ROWS,
                max_input_decoded_bytes: MAX_CONNECT_LOCAL_DECODED_BYTES,
                token: &worker_token,
                max_batch_rows: max_rows,
                max_batch_bytes: max_bytes,
            },
            |schema| send_event(&sender, DuckDbEvent::Schema(schema)),
            |batch| send_event(&sender, DuckDbEvent::Batch(batch)),
        );
        if let Err(error) = result {
            let _ = sender.send(DuckDbEvent::Error(error));
        } else {
            let _ = sender.send(DuckDbEvent::Finished);
        }
    });
    let schema = match receiver.recv() {
        Ok(DuckDbEvent::Schema(_)) => expected_schema,
        Ok(DuckDbEvent::Error(error)) => return Err(engine_failure(error)),
        _ => {
            return Err(failure(
                "CONNECT_ARROW_STREAM_FAILED",
                "DuckDB did not return an output schema",
            ));
        }
    };
    let batch_token = cancellation.clone();
    let batches = ArrowIpcBatchStream::new(
        std::iter::from_fn(move || {
            loop {
                match receiver.recv() {
                    Ok(DuckDbEvent::Batch(batch)) => return Some(Ok(batch)),
                    Ok(DuckDbEvent::Error(error)) => {
                        return Some(Err(EngineError::invalid_input(error.to_string())));
                    }
                    Ok(DuckDbEvent::Finished) | Err(_) => return None,
                    Ok(DuckDbEvent::Schema(_)) => continue,
                }
            }
        })
        .inspect(move |_| {
            if batch_token.is_cancelled() {
                // Cancellation is checked again by DuckDB before it yields its next batch.
            }
        }),
    );
    Ok(ConnectExecutionResult {
        schema: Some(schema),
        batches,
        sql_command_result: None,
    })
}

enum DuckDbEvent {
    Schema(StructType),
    Batch(ArrowIpcStream),
    Error(EngineError),
    Finished,
}

fn send_event(sender: &SyncSender<DuckDbEvent>, event: DuckDbEvent) -> Result<(), EngineError> {
    sender
        .send(event)
        .map_err(|_| EngineError::internal("Connect result receiver was dropped".into()))
}

struct PlanRenderer<'a> {
    session: &'a DuckDbConnectSession,
    cancellation: CancellationToken,
    local_inputs: Vec<Vec<u8>>,
    local_input_schemas: Vec<Option<StructType>>,
}

impl<'a> PlanRenderer<'a> {
    fn new(
        session: &'a DuckDbConnectSession,
        root: &ConnectRelation,
        cancellation: &CancellationToken,
    ) -> Result<Self, ExecutionError> {
        let mut plan_ids = HashSet::new();
        collect_plan_ids(root, &mut plan_ids, 1)?;
        Ok(Self {
            session,
            cancellation: cancellation.clone(),
            local_inputs: Vec::new(),
            local_input_schemas: Vec::new(),
        })
    }

    fn relation(
        &mut self,
        relation: &ConnectRelation,
        depth: usize,
    ) -> Result<String, ExecutionError> {
        if depth > MAX_RELATION_DEPTH {
            return Err(failure(
                "CONNECT_PLAN_DEPTH",
                "Connect relation depth exceeds its limit",
            ));
        }
        let rendered = match &relation.kind {
            ConnectRelationKind::Range {
                start,
                end,
                step,
                partitions,
            } => {
                if *step == 0 || partitions.is_some_and(|partitions| partitions <= 0) {
                    return Err(failure(
                        "CONNECT_RANGE_INVALID",
                        "Range step must be nonzero and partition count must be positive",
                    ));
                }
                let start = start.unwrap_or(0);
                let distance = if *step > 0 {
                    (i128::from(*end) - i128::from(start)).max(0)
                } else {
                    (i128::from(start) - i128::from(*end)).max(0)
                } as u128;
                let rows = distance.div_ceil(u128::from(step.unsigned_abs()));
                if rows > MAX_RANGE_ROWS {
                    return Err(failure(
                        "CONNECT_RANGE_LIMIT",
                        "Connect range exceeds the one-million-row input limit",
                    ));
                }
                Ok(format!(
                    "(SELECT CAST(range AS BIGINT) AS \"id\" FROM range({start}, {end}, {step}))"
                ))
            }
            ConnectRelationKind::Local { data, schema } => {
                if let Some(data) = data.as_ref().filter(|data| !data.is_empty()) {
                    let input_schema = schema
                        .as_deref()
                        .map(crate::connect::plan::parse_local_schema)
                        .transpose()
                        .map_err(|status| {
                            failure("CONNECT_LOCAL_SCHEMA_INVALID", status.message())
                        })?;
                    self.local_inputs.push(data.clone());
                    self.local_input_schemas.push(input_schema);
                    Ok("(SELECT * FROM arrow(?))".to_string())
                } else if let Some(schema) = schema {
                    let schema =
                        crate::connect::plan::parse_local_schema(schema).map_err(|status| {
                            failure("CONNECT_LOCAL_SCHEMA_INVALID", status.message())
                        })?;
                    empty_relation(&schema)
                } else {
                    Err(failure(
                        "CONNECT_LOCAL_RELATION_INVALID",
                        "LocalRelation requires Arrow data or a schema",
                    ))
                }
            }
            ConnectRelationKind::CachedLocal => Err(failure(
                "CONNECT_LOCAL_CACHE_UNSUPPORTED",
                "CachedLocalRelation is outside the enabled DuckDB execution subset",
            )),
            ConnectRelationKind::Read {
                is_streaming,
                named_table,
            } => {
                if *is_streaming {
                    return Err(failure(
                        "CONNECT_STREAMING_UNSUPPORTED",
                        "streaming table reads are not enabled",
                    ));
                }
                let table = named_table.as_ref().ok_or_else(|| {
                    failure(
                        "CONNECT_TABLE_REQUIRED",
                        "DuckDB Connect reads require a named table",
                    )
                })?;
                if !table.options.is_empty() {
                    return Err(failure(
                        "CONNECT_TABLE_OPTIONS_UNSUPPORTED",
                        "named table options are not enabled",
                    ));
                }
                self.session
                    .owner
                    .table_reference(&table.identifier)
                    .map(|reference| reference.to_string())
                    .map_err(engine_failure)
            }
            ConnectRelationKind::Alias {
                input,
                alias,
                qualifier,
            } => {
                let input = input.as_deref().ok_or_else(|| {
                    failure(
                        "CONNECT_ALIAS_INPUT_REQUIRED",
                        "SubqueryAlias input is required",
                    )
                })?;
                let alias = if !alias.is_empty() {
                    alias.as_str()
                } else {
                    qualifier.last().map(String::as_str).unwrap_or("")
                };
                validate_identifier(alias)?;
                let inner = self.relation(input, depth + 1)?;
                Ok(format!("({inner}) AS {}", quote_identifier(alias)))
            }
            ConnectRelationKind::Filter { input, condition } => {
                let input_relation = input.as_deref().ok_or_else(|| {
                    failure("CONNECT_FILTER_INPUT_REQUIRED", "Filter input is required")
                })?;
                let (input, expression_aliases) =
                    if matches!(&input_relation.kind, ConnectRelationKind::Join { .. }) {
                        self.join_from(input_relation, depth + 1)?
                    } else {
                        (
                            self.relation(input_relation, depth + 1)?,
                            expression_aliases_for_relation(input_relation)?,
                        )
                    };
                let condition = condition.as_ref().ok_or_else(|| {
                    failure(
                        "CONNECT_FILTER_CONDITION_REQUIRED",
                        "Filter condition is required",
                    )
                })?;
                let input_schema = describe_connect_plan(
                    self.session,
                    &ConnectExecutionPlan::Root(input_relation.clone()),
                    &self.cancellation,
                )?;
                let condition = match condition {
                    ConnectExpression::ExpressionString(expression) => {
                        render_connect_expression_string_with_schema(
                            expression,
                            false,
                            Some(&input_schema),
                        )?
                        .0
                    }
                    _ => {
                        validate_connect_string_numeric_comparisons(condition, &input_schema)?;
                        render_connect_expression(condition, &expression_aliases)?
                    }
                };
                Ok(format!("(SELECT * FROM {input} WHERE {condition})"))
            }
            ConnectRelationKind::Project { input, expressions } => {
                if expressions.is_empty() {
                    return Err(failure(
                        "CONNECT_PROJECT_EMPTY",
                        "Project requires at least one expression",
                    ));
                }
                let input_relation = input.as_deref().ok_or_else(|| {
                    failure(
                        "CONNECT_PROJECT_INPUT_REQUIRED",
                        "Project input is required",
                    )
                })?;
                let (input, expression_aliases, filter) = match &input_relation.kind {
                    ConnectRelationKind::Join { .. } => {
                        let (input, aliases) = self.join_from(input_relation, depth + 1)?;
                        (input, aliases, None)
                    }
                    ConnectRelationKind::Filter {
                        input: Some(join),
                        condition: Some(condition),
                    } if matches!(&join.kind, ConnectRelationKind::Join { .. }) => {
                        let (input, aliases) = self.join_from(join, depth + 2)?;
                        let input_schema = describe_connect_plan(
                            self.session,
                            &ConnectExecutionPlan::Root(join.as_ref().clone()),
                            &self.cancellation,
                        )?;
                        let condition = match condition {
                            ConnectExpression::ExpressionString(expression) => {
                                render_connect_expression_string_with_schema(
                                    expression,
                                    false,
                                    Some(&input_schema),
                                )?
                                .0
                            }
                            _ => {
                                validate_connect_string_numeric_comparisons(
                                    condition,
                                    &input_schema,
                                )?;
                                render_connect_expression(condition, &aliases)?
                            }
                        };
                        (input, aliases, Some(condition))
                    }
                    _ => (
                        self.relation(input_relation, depth + 1)?,
                        expression_aliases_for_relation(input_relation)?,
                        None,
                    ),
                };
                let projection = expressions
                    .iter()
                    .map(|expression| render_connect_projection(expression, &expression_aliases))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                Ok(match filter {
                    Some(filter) => {
                        format!("(SELECT {projection} FROM {input} WHERE {filter})")
                    }
                    None => format!("(SELECT {projection} FROM {input})"),
                })
            }
            ConnectRelationKind::Limit { input, limit } => {
                if *limit < 0 || *limit as u128 > MAX_RANGE_ROWS {
                    return Err(failure(
                        "CONNECT_LIMIT_INVALID",
                        "Connect limit must be between zero and one million",
                    ));
                }
                let input = self.relation(
                    input.as_deref().ok_or_else(|| {
                        failure("CONNECT_LIMIT_INPUT_REQUIRED", "Limit input is required")
                    })?,
                    depth + 1,
                )?;
                Ok(format!("(SELECT * FROM {input} LIMIT {limit})"))
            }
            ConnectRelationKind::Join { .. } => self
                .join_from(relation, depth)
                .map(|(from, _)| format!("(SELECT * FROM {from})")),
            ConnectRelationKind::Aggregate {
                input,
                group_type,
                grouping_expressions,
                aggregate_expressions,
                has_pivot,
                has_grouping_sets,
            } => {
                if *group_type != ConnectGroupType::GroupBy
                    || *has_pivot
                    || *has_grouping_sets
                    || aggregate_expressions.is_empty()
                {
                    return Err(failure(
                        "CONNECT_AGGREGATE_UNSUPPORTED",
                        "DuckDB execution enables grouped count and sum only",
                    ));
                }
                if aggregate_expressions
                    .iter()
                    .any(|expression| !is_connect_aggregate(expression))
                {
                    return Err(failure(
                        "CONNECT_AGGREGATE_UNSUPPORTED",
                        "DuckDB execution enables count and sum aggregates only",
                    ));
                }
                let input_relation = input.as_deref().ok_or_else(|| {
                    failure(
                        "CONNECT_AGGREGATE_INPUT_REQUIRED",
                        "Aggregate input is required",
                    )
                })?;
                let expression_aliases = expression_aliases_for_relation(input_relation)?;
                let input_schema = describe_connect_plan(
                    self.session,
                    &ConnectExecutionPlan::Root(input_relation.clone()),
                    &self.cancellation,
                )?;
                let input = self.relation(input_relation, depth + 1)?;
                let aggregate_sql = aggregate_expressions
                    .iter()
                    .map(|expression| {
                        render_connect_aggregate_projection(
                            expression,
                            &expression_aliases,
                            &input_schema,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let groups = grouping_expressions
                    .iter()
                    .map(|expression| render_connect_expression(expression, &expression_aliases))
                    .collect::<Result<Vec<_>, _>>()?;
                let grouping_projection = grouping_expressions
                    .iter()
                    .map(|expression| render_connect_projection(expression, &expression_aliases))
                    .collect::<Result<Vec<_>, _>>()?;
                let projection = grouping_projection
                    .into_iter()
                    .chain(aggregate_sql)
                    .collect::<Vec<_>>()
                    .join(", ");
                if groups.is_empty() {
                    Ok(format!("(SELECT {projection} FROM {input})"))
                } else {
                    Ok(format!(
                        "(SELECT {projection} FROM {input} GROUP BY {})",
                        groups.join(", ")
                    ))
                }
            }
            ConnectRelationKind::Sql {
                query,
                has_arguments,
            } => {
                if *has_arguments {
                    return Err(failure(
                        "CONNECT_SQL_PARAMETERS_UNSUPPORTED",
                        "parameterized SQL relations are not enabled",
                    ));
                }
                let statement = robin_sparkless_sql::parse_statement(query)
                    .map_err(|error| failure("CONNECT_SQL_INVALID", &error.to_string()))?;
                let StatementPlan::Query(query_plan) =
                    robin_sparkless_sql::lower_statement(&statement)
                        .map_err(|error| failure("CONNECT_SQL_UNSUPPORTED", &error.to_string()))?
                else {
                    return Err(failure(
                        "CONNECT_SQL_EFFECT_UNSUPPORTED",
                        "SQL relations must contain a read-only SELECT",
                    ));
                };
                self.sql_query(&query_plan)
            }
            ConnectRelationKind::Unsupported(kind) => Err(failure(
                "CONNECT_RELATION_UNSUPPORTED",
                &format!("Connect relation {kind} is not enabled"),
            )),
            ConnectRelationKind::Missing => Err(failure(
                "CONNECT_RELATION_MISSING",
                "Connect relation type is missing",
            )),
        }?;
        if let Some(plan_id) = relation.plan_id {
            Ok(format!(
                "(SELECT * FROM {rendered}) AS {}",
                quote_identifier(&plan_alias(plan_id))
            ))
        } else {
            Ok(rendered)
        }
    }

    fn join_from(
        &mut self,
        relation: &ConnectRelation,
        depth: usize,
    ) -> Result<(String, HashMap<i64, i64>), ExecutionError> {
        let ConnectRelationKind::Join {
            left,
            right,
            condition,
            join_type,
            using_columns,
            ..
        } = &relation.kind
        else {
            return Err(failure(
                "CONNECT_JOIN_UNSUPPORTED",
                "expected a Join relation",
            ));
        };
        if !matches!(
            join_type,
            ConnectJoinType::Inner | ConnectJoinType::Unspecified
        ) || (condition.is_some() && !using_columns.is_empty())
            || (condition.is_none() && using_columns.is_empty())
        {
            return Err(failure(
                "CONNECT_JOIN_UNSUPPORTED",
                "DuckDB execution enables inner equijoins and USING joins only",
            ));
        }
        let left_relation = left
            .as_deref()
            .ok_or_else(|| failure("CONNECT_JOIN_LEFT_REQUIRED", "Join left input is required"))?;
        let right_relation = right.as_deref().ok_or_else(|| {
            failure(
                "CONNECT_JOIN_RIGHT_REQUIRED",
                "Join right input is required",
            )
        })?;
        let expression_aliases = expression_aliases_for_join(left_relation, right_relation)?;
        let left = self.relation(left_relation, depth + 1)?;
        let right = self.relation(right_relation, depth + 1)?;
        let sql = if !using_columns.is_empty() {
            let columns = using_columns
                .iter()
                .map(|column| validate_identifier(column).map(quote_identifier))
                .collect::<Result<Vec<_>, _>>()?;
            format!("{left} INNER JOIN {right} USING ({})", columns.join(", "))
        } else {
            let condition = condition.as_ref().ok_or_else(|| {
                failure(
                    "CONNECT_JOIN_CONDITION_REQUIRED",
                    "Join condition is required",
                )
            })?;
            if !is_connect_equality(condition) {
                return Err(failure(
                    "CONNECT_JOIN_UNSUPPORTED",
                    "DuckDB execution enables equijoin predicates only",
                ));
            }
            format!(
                "{left} INNER JOIN {right} ON {}",
                render_connect_expression(condition, &expression_aliases)?
            )
        };
        Ok((sql, expression_aliases))
    }

    fn sql_query(&mut self, query: &SqlQueryPlan) -> Result<String, ExecutionError> {
        validate_sql_query_shape(&query.source_sql)?;
        if query.recursive
            || !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.offset.is_some()
        {
            return Err(failure(
                "CONNECT_SQL_UNSUPPORTED",
                "recursive CTEs, CTEs, ordering, and offsets are not enabled",
            ));
        }
        let RelationalNode::Select(select) = &query.body else {
            return Err(failure(
                "CONNECT_SQL_UNSUPPORTED",
                "SQL relations require one SELECT body",
            ));
        };
        let mut sql = self.sql_select(select)?;
        if let Some(limit) = query.limit {
            if limit as u128 > MAX_RANGE_ROWS {
                return Err(failure(
                    "CONNECT_SQL_LIMIT",
                    "SQL limit exceeds one million rows",
                ));
            }
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        Ok(format!("({sql})"))
    }

    fn sql_select(&mut self, select: &SelectPlan) -> Result<String, ExecutionError> {
        if select.distinct.is_some()
            || select.projection.is_empty()
            || select.from.len() > 1
            || select.having.is_some()
        {
            return Err(failure(
                "CONNECT_SQL_UNSUPPORTED",
                "SQL supports non-distinct single-source SELECTs without HAVING",
            ));
        }
        let from = select
            .from
            .first()
            .map(|from| self.sql_from(from))
            .transpose()?;
        let source_schema = if let Some(from) = from.as_deref() {
            Some(
                self.session
                    .owner
                    .describe_query(&format!("SELECT * FROM {from}"), &self.cancellation)
                    .map_err(engine_failure)?,
            )
        } else {
            Some(StructType::from_logical_fields(Vec::new()))
        };
        let projection = select
            .projection
            .iter()
            .map(|projection| self.sql_projection(projection, source_schema.as_ref()))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        let mut sql = format!("SELECT {projection}");
        if let Some(from) = from {
            sql.push_str(" FROM ");
            sql.push_str(&from);
        }
        if let Some(filter) = &select.filter {
            sql.push_str(" WHERE ");
            sql.push_str(&render_sql_expression_with_schema(
                filter,
                source_schema.as_ref(),
            )?);
        }
        match &select.group_by {
            GroupByPlan::Expressions {
                expressions,
                modifiers,
            } if modifiers.is_empty() && !expressions.is_empty() => {
                sql.push_str(" GROUP BY ");
                sql.push_str(
                    &expressions
                        .iter()
                        .map(|expression| {
                            render_sql_expression_with_schema(expression, source_schema.as_ref())
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .join(", "),
                );
            }
            GroupByPlan::Expressions {
                expressions,
                modifiers,
            } if expressions.is_empty() && modifiers.is_empty() => {}
            _ => {
                return Err(failure(
                    "CONNECT_SQL_UNSUPPORTED",
                    "SQL GROUP BY modifiers are not enabled",
                ));
            }
        }
        Ok(sql)
    }

    fn sql_projection(
        &mut self,
        projection: &Projection,
        source_schema: Option<&StructType>,
    ) -> Result<String, ExecutionError> {
        match projection {
            Projection::Wildcard => Ok("*".into()),
            Projection::QualifiedWildcard(qualifier) => {
                let qualifier = qualifier.strip_suffix(".*").ok_or_else(|| {
                    failure(
                        "CONNECT_SQL_WILDCARD_UNSUPPORTED",
                        "qualified wildcards must name a table or relation",
                    )
                })?;
                Ok(format!(
                    "{}.*",
                    quote_identifier(validate_identifier(qualifier)?)
                ))
            }
            Projection::Expression { expression, alias } => {
                let expression = if let ExpressionShape::Function {
                    name, arguments, ..
                } = &expression.shape
                    && name.eq_ignore_ascii_case("sum")
                {
                    let argument = arguments.first().ok_or_else(|| {
                        failure("CONNECT_SQL_FUNCTION_ARITY", "SUM requires one argument")
                    })?;
                    if arguments.len() != 1 {
                        return Err(failure(
                            "CONNECT_SQL_FUNCTION_ARITY",
                            "SUM requires one argument",
                        ));
                    }
                    let ExpressionShape::Identifier(parts) = &argument.shape else {
                        return Err(failure(
                            "CONNECT_SQL_AGGREGATE_TYPE_UNSUPPORTED",
                            "SQL SUM requires a supported numeric input column",
                        ));
                    };
                    let source_name = parts.last().ok_or_else(|| {
                        failure(
                            "CONNECT_SQL_AGGREGATE_TYPE_UNSUPPORTED",
                            "SQL SUM requires a supported numeric input column",
                        )
                    })?;
                    let source_schema = source_schema.ok_or_else(|| {
                        failure(
                            "CONNECT_SQL_AGGREGATE_TYPE_UNSUPPORTED",
                            "SQL SUM requires a supported numeric input column",
                        )
                    })?;
                    let mut matches = source_schema
                        .logical_fields()
                        .iter()
                        .filter(|field| field.name.eq_ignore_ascii_case(source_name));
                    let source = matches.next().ok_or_else(|| {
                        failure(
                            "CONNECT_SQL_AGGREGATE_TYPE_UNSUPPORTED",
                            "SQL SUM input column cannot be resolved",
                        )
                    })?;
                    if matches.next().is_some() {
                        return Err(failure(
                            "CONNECT_SQL_AGGREGATE_TYPE_UNSUPPORTED",
                            "SQL SUM input column is ambiguous",
                        ));
                    }
                    let (_, sql_type) = spark_sum_type(&source.data_type)?;
                    format!(
                        "CAST(SUM({}) AS {sql_type})",
                        render_sql_expression_with_schema(argument, Some(source_schema))?
                    )
                } else {
                    render_sql_expression_with_schema(expression, source_schema)?
                };
                Ok(match alias {
                    Some(alias) => format!(
                        "{expression} AS {}",
                        quote_identifier(validate_identifier(alias)?)
                    ),
                    None => expression,
                })
            }
        }
    }

    fn sql_from(&mut self, from: &FromPlan) -> Result<String, ExecutionError> {
        let mut sql = self.sql_relation(&from.relation)?;
        for join in &from.joins {
            if join.global || !matches!(join.operator, JoinKind::Inner | JoinKind::Cross) {
                return Err(failure(
                    "CONNECT_SQL_JOIN_UNSUPPORTED",
                    "SQL supports inner equijoins and cross joins only",
                ));
            }
            let right = self.sql_relation(&join.relation)?;
            sql.push_str(if join.operator == JoinKind::Cross {
                " CROSS JOIN "
            } else {
                " INNER JOIN "
            });
            sql.push_str(&right);
            match &join.constraint {
                JoinConstraintPlan::On(condition)
                    if join.operator == JoinKind::Inner && is_sql_equality(condition) =>
                {
                    sql.push_str(" ON ");
                    sql.push_str(&render_sql_expression(condition)?);
                }
                JoinConstraintPlan::Using(columns)
                    if join.operator == JoinKind::Inner && !columns.is_empty() =>
                {
                    sql.push_str(" USING (");
                    sql.push_str(
                        &columns
                            .iter()
                            .map(|column| validate_identifier(column).map(quote_identifier))
                            .collect::<Result<Vec<_>, _>>()?
                            .join(", "),
                    );
                    sql.push(')');
                }
                JoinConstraintPlan::None if join.operator == JoinKind::Cross => {}
                _ => {
                    return Err(failure(
                        "CONNECT_SQL_JOIN_UNSUPPORTED",
                        "SQL join condition is outside the equijoin subset",
                    ));
                }
            }
        }
        Ok(sql)
    }

    fn sql_relation(&mut self, relation: &RelationPlan) -> Result<String, ExecutionError> {
        match relation {
            RelationPlan::Table { name, alias, .. } => {
                let table = self
                    .session
                    .owner
                    .table_reference(name)
                    .map_err(engine_failure)?;
                Ok(match alias {
                    Some(alias) => format!(
                        "{table} AS {}",
                        quote_identifier(validate_identifier(alias)?)
                    ),
                    None => table,
                })
            }
            RelationPlan::Derived { query, alias, .. } => {
                let alias = alias.as_deref().ok_or_else(|| {
                    failure(
                        "CONNECT_SQL_ALIAS_REQUIRED",
                        "derived SQL relations require an alias",
                    )
                })?;
                let query = self.sql_query(query)?;
                Ok(format!(
                    "{query} AS {}",
                    quote_identifier(validate_identifier(alias)?)
                ))
            }
            RelationPlan::Opaque { kind, .. } => Err(failure(
                "CONNECT_SQL_TABLE_SOURCE_UNSUPPORTED",
                &format!("SQL table source {kind} is not enabled"),
            )),
        }
    }
}

fn validate_sql_query_shape(sql: &str) -> Result<(), ExecutionError> {
    use robin_sparkless_sql::ast::Statement;
    let statement = robin_sparkless_sql::parse_statement(sql)
        .map_err(|error| failure("CONNECT_SQL_INVALID", &error.to_string()))?;
    let robin_sparkless_sql::SparkStatement::Sqlparser(statement) = statement else {
        return Err(failure(
            "CONNECT_SQL_UNSUPPORTED",
            "SQL relations require one parsed SELECT statement",
        ));
    };
    let Statement::Query(query) = statement.as_ref() else {
        return Err(failure(
            "CONNECT_SQL_UNSUPPORTED",
            "SQL relations require one parsed SELECT statement",
        ));
    };
    validate_sql_query_ast(query.as_ref())
}

fn validate_sql_query_ast(query: &robin_sparkless_sql::ast::Query) -> Result<(), ExecutionError> {
    use robin_sparkless_sql::ast::{Expr, LimitClause, SelectFlavor, SelectItem, SetExpr, Value};
    let supported_limit = match &query.limit_clause {
        None => true,
        Some(LimitClause::LimitOffset {
            limit: Some(Expr::Value(value)),
            offset: None,
            limit_by,
        }) => {
            limit_by.is_empty()
                && matches!(
                    &value.value,
                    Value::Number(number, _) if number.parse::<i64>().is_ok_and(|number| number >= 0)
                )
        }
        Some(LimitClause::LimitOffset { .. } | LimitClause::OffsetCommaLimit { .. }) => false,
    };
    if query.with.is_some()
        || query.order_by.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || !supported_limit
    {
        return Err(failure(
            "CONNECT_SQL_UNSUPPORTED",
            "CTEs, ordering, OFFSET, comma-form limits, LIMIT BY, FETCH, locking, query settings, formats, and pipes are not enabled",
        ));
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(failure(
            "CONNECT_SQL_UNSUPPORTED",
            "SQL relations require a single SELECT body",
        ));
    };
    if select.optimizer_hint.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.top_before_distinct
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.window_before_qualify
        || select.value_table_mode.is_some()
        || select.flavor != SelectFlavor::Standard
    {
        return Err(failure(
            "CONNECT_SQL_UNSUPPORTED",
            "SELECT hints, modifiers, windows, nonstandard clauses, and INTO are not enabled",
        ));
    }
    if select.projection.iter().any(|item| match item {
        SelectItem::Wildcard(options) | SelectItem::QualifiedWildcard(_, options) => {
            options.opt_ilike.is_some()
                || options.opt_exclude.is_some()
                || options.opt_except.is_some()
                || options.opt_replace.is_some()
                || options.opt_rename.is_some()
        }
        SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => false,
    }) {
        return Err(failure(
            "CONNECT_SQL_UNSUPPORTED",
            "wildcard modifiers are not enabled",
        ));
    }
    for from in &select.from {
        validate_sql_table_factor(&from.relation)?;
        for join in &from.joins {
            validate_sql_table_factor(&join.relation)?;
        }
    }
    Ok(())
}

fn validate_sql_table_factor(
    factor: &robin_sparkless_sql::ast::TableFactor,
) -> Result<(), ExecutionError> {
    use robin_sparkless_sql::ast::TableFactor;
    match factor {
        TableFactor::Table {
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } => {
            if args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
                || alias
                    .as_ref()
                    .is_some_and(|alias| !alias.columns.is_empty())
            {
                return Err(failure(
                    "CONNECT_SQL_TABLE_SOURCE_UNSUPPORTED",
                    "table functions, table hints, time travel, sampling, partitions, and alias column lists are not enabled",
                ));
            }
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if *lateral
                || sample.is_some()
                || alias
                    .as_ref()
                    .is_some_and(|alias| !alias.columns.is_empty())
            {
                return Err(failure(
                    "CONNECT_SQL_TABLE_SOURCE_UNSUPPORTED",
                    "lateral, sampled, and column-renamed derived tables are not enabled",
                ));
            }
            validate_sql_query_ast(subquery)?;
        }
        _ => {
            return Err(failure(
                "CONNECT_SQL_TABLE_SOURCE_UNSUPPORTED",
                "only named tables and non-lateral derived tables are enabled",
            ));
        }
    }
    Ok(())
}

fn execute_command(
    session: &DuckDbConnectSession,
    command: &ConnectCommand,
    operation_identity: &ConnectOperationIdentity,
    cancellation: &CancellationToken,
) -> Result<(), ExecutionError> {
    let ConnectCommand::Sql { sql } = command else {
        let ConnectCommand::Unsupported(message) = command else {
            unreachable!()
        };
        return Err(failure("CONNECT_COMMAND_UNSUPPORTED", message));
    };
    let statement = robin_sparkless_sql::parse_statement(sql)
        .map_err(|error| failure("CONNECT_COMMAND_INVALID", &error.to_string()))?;
    let StatementPlan::Command(command) = robin_sparkless_sql::lower_statement(&statement)
        .map_err(|error| failure("CONNECT_COMMAND_UNSUPPORTED", &error.to_string()))?
    else {
        return Err(failure(
            "CONNECT_COMMAND_UNSUPPORTED",
            "SQL commands must be CREATE TABLE or INSERT INTO VALUES",
        ));
    };
    let sql = match statement {
        robin_sparkless_sql::SparkStatement::Sqlparser(mut statement) => match statement.as_mut() {
            robin_sparkless_sql::ast::Statement::CreateTable(create) => {
                if command.kind != robin_sparkless_sql::CommandKind::CreateTable
                    || !simple_create_table(create)
                {
                    return Err(failure(
                        "CONNECT_CREATE_TABLE_UNSUPPORTED",
                        "CREATE TABLE accepts only typed columns and no external, temporary, or CTAS options",
                    ));
                }
                create.name = execution_table_name(&session.owner, &create.name.to_string())?;
                normalize_create_table_types(create);
                statement.to_string()
            }
            robin_sparkless_sql::ast::Statement::Insert(insert) => {
                if command.kind != robin_sparkless_sql::CommandKind::Insert
                    || !simple_insert(insert)
                {
                    return Err(failure(
                        "CONNECT_INSERT_UNSUPPORTED",
                        "INSERT accepts literal VALUES only",
                    ));
                }
                let robin_sparkless_sql::ast::TableObject::TableName(name) = &mut insert.table
                else {
                    return Err(failure(
                        "CONNECT_INSERT_UNSUPPORTED",
                        "INSERT table functions are not enabled",
                    ));
                };
                let table = execution_table_name(&session.owner, &name.to_string())?;
                let table_reference = table.to_string();
                let target_schema = session
                    .owner
                    .describe_query(
                        &format!("SELECT * FROM {table_reference} LIMIT 0"),
                        cancellation,
                    )
                    .map_err(engine_failure)?;
                let Some(query) = insert.source.as_deref() else {
                    return Err(failure(
                        "CONNECT_INSERT_UNSUPPORTED",
                        "INSERT requires literal VALUES rows",
                    ));
                };
                let robin_sparkless_sql::ast::SetExpr::Values(values) = query.body.as_ref() else {
                    return Err(failure(
                        "CONNECT_INSERT_UNSUPPORTED",
                        "INSERT requires literal VALUES rows",
                    ));
                };
                let target_fields = target_schema.logical_fields();
                let insert_columns = if insert.columns.is_empty() {
                    target_fields
                        .iter()
                        .enumerate()
                        .map(|(index, _)| index)
                        .collect::<Vec<_>>()
                } else {
                    let mut indices = Vec::with_capacity(insert.columns.len());
                    for column in &insert.columns {
                        let mut matching = target_fields
                            .iter()
                            .enumerate()
                            .filter(|(_, field)| field.name.eq_ignore_ascii_case(&column.value));
                        let Some((index, _)) = matching.next() else {
                            return Err(failure(
                                "CONNECT_INSERT_COLUMN_UNKNOWN",
                                "INSERT column does not exist in the target table",
                            ));
                        };
                        if matching.next().is_some() || indices.contains(&index) {
                            return Err(failure(
                                "CONNECT_INSERT_COLUMN_AMBIGUOUS",
                                "INSERT column must resolve to one unique target column",
                            ));
                        }
                        indices.push(index);
                    }
                    indices
                };
                let mut rows = Vec::with_capacity(values.rows.len());
                for row in &values.rows {
                    if row.len() != insert_columns.len() {
                        return Err(failure(
                            "CONNECT_INSERT_COLUMN_COUNT",
                            "INSERT VALUES column count must match its target column list",
                        ));
                    }
                    let mut target_values = vec!["NULL".to_string(); target_fields.len()];
                    for (value, target_index) in row.iter().zip(&insert_columns) {
                        target_values[*target_index] =
                            render_insert_value(value, &target_fields[*target_index].data_type)?;
                    }
                    rows.push(format!("({})", target_values.join(", ")));
                }
                format!("INSERT INTO {table_reference} VALUES {}", rows.join(", "))
            }
            _ => {
                return Err(failure(
                    "CONNECT_COMMAND_UNSUPPORTED",
                    "only CREATE TABLE and INSERT INTO VALUES are enabled",
                ));
            }
        },
        _ => {
            return Err(failure(
                "CONNECT_COMMAND_UNSUPPORTED",
                "only CREATE TABLE and INSERT INTO VALUES are enabled",
            ));
        }
    };
    session
        .owner
        .execute_transaction(
            &sql,
            &operation_identity.key,
            &operation_identity.request_fingerprint,
            cancellation,
        )
        .map(|_| ())
        .map_err(engine_failure)
}

fn empty_sql_command_result() -> ConnectRelation {
    ConnectRelation {
        plan_id: None,
        kind: ConnectRelationKind::Range {
            start: None,
            end: 0,
            step: 1,
            partitions: None,
        },
    }
}

fn simple_create_table(create: &robin_sparkless_sql::ast::CreateTable) -> bool {
    !create.or_replace
        && !create.temporary
        && !create.external
        && !create.dynamic
        && create.global.is_none()
        && !create.if_not_exists
        && !create.transient
        && !create.volatile
        && !create.iceberg
        && !create.columns.is_empty()
        && create.columns.iter().all(|column| {
            column.options.is_empty() && supported_execution_column_type(&column.data_type)
        })
        && create.constraints.is_empty()
        && matches!(
            create.hive_distribution,
            robin_sparkless_sql::ast::HiveDistributionStyle::NONE
        )
        && create.hive_formats.is_none()
        && matches!(
            create.table_options,
            robin_sparkless_sql::ast::CreateTableOptions::None
        )
        && create.file_format.is_none()
        && create.location.is_none()
        && create.query.is_none()
        && !create.without_rowid
        && create.like.is_none()
        && create.clone.is_none()
        && create.version.is_none()
        && create.comment.is_none()
        && create.on_commit.is_none()
        && create.on_cluster.is_none()
        && create.primary_key.is_none()
        && create.order_by.is_none()
        && create.partition_by.is_none()
        && create.cluster_by.is_none()
        && create.clustered_by.is_none()
        && create.inherits.is_none()
        && create.partition_of.is_none()
        && create.for_values.is_none()
        && !create.strict
        && !create.copy_grants
        && create.enable_schema_evolution.is_none()
        && create.change_tracking.is_none()
        && create.data_retention_time_in_days.is_none()
        && create.max_data_extension_time_in_days.is_none()
        && create.default_ddl_collation.is_none()
        && create.with_aggregation_policy.is_none()
        && create.with_row_access_policy.is_none()
        && create.with_tags.is_none()
        && create.external_volume.is_none()
        && create.base_location.is_none()
        && create.catalog.is_none()
        && create.catalog_sync.is_none()
        && create.storage_serialization_policy.is_none()
        && create.target_lag.is_none()
        && create.warehouse.is_none()
        && create.refresh_mode.is_none()
        && create.initialize.is_none()
        && !create.require_user
}

fn supported_execution_column_type(data_type: &robin_sparkless_sql::ast::DataType) -> bool {
    let compact = data_type
        .to_string()
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>()
        .to_ascii_uppercase();
    if decimal_type_supported(&compact) {
        return true;
    }
    if matches!(
        compact.as_str(),
        "TIMESTAMPTZ" | "TIMESTAMPNTZ" | "TIMESTAMP_NTZ" | "TIMESTAMP_LTZ"
    ) {
        return true;
    }
    if let Some(remainder) = compact.strip_prefix("TIMESTAMP") {
        let remainder = remainder
            .strip_suffix("WITHTIMEZONE")
            .or_else(|| remainder.strip_suffix("WITHOUTTIMEZONE"))
            .unwrap_or(remainder);
        return remainder.is_empty()
            || (remainder.starts_with('(')
                && remainder.ends_with(')')
                && remainder[1..remainder.len() - 1]
                    .parse::<u8>()
                    .is_ok_and(|precision| precision <= 6));
    }
    let base = compact.split('(').next().unwrap_or(&compact);
    matches!(
        base,
        "BOOLEAN"
            | "BOOL"
            | "TINYINT"
            | "INT1"
            | "BYTE"
            | "SMALLINT"
            | "INT2"
            | "SHORT"
            | "INTEGER"
            | "INT"
            | "INT4"
            | "BIGINT"
            | "INT8"
            | "LONG"
            | "FLOAT"
            | "REAL"
            | "DOUBLE"
            | "DOUBLEPRECISION"
            | "VARCHAR"
            | "CHARACTERVARYING"
            | "STRING"
            | "TEXT"
            | "DATE"
            | "BLOB"
            | "BINARY"
            | "VARBINARY"
    ) && if compact.contains('(') {
        let Some((_, length)) = compact.split_once('(') else {
            return false;
        };
        length
            .strip_suffix(')')
            .and_then(|length| length.parse::<u64>().ok())
            .is_some_and(|length| length > 0)
            && matches!(base, "VARCHAR" | "CHARACTERVARYING" | "VARBINARY")
    } else {
        true
    }
}

fn normalize_create_table_types(create: &mut robin_sparkless_sql::ast::CreateTable) {
    use robin_sparkless_sql::ast::{DataType, TimezoneInfo};
    for column in &mut create.columns {
        if let DataType::Timestamp(precision, TimezoneInfo::None) = &column.data_type {
            column.data_type = DataType::Timestamp(*precision, TimezoneInfo::WithTimeZone);
        }
    }
}

fn decimal_type_supported(data_type: &str) -> bool {
    let Some(precision_and_scale) = ["DECIMAL", "NUMERIC"]
        .iter()
        .find_map(|base| data_type.strip_prefix(base))
    else {
        return false;
    };
    if precision_and_scale.is_empty() {
        return false;
    }
    let Some(parameters) = precision_and_scale
        .strip_prefix('(')
        .and_then(|parameters| parameters.strip_suffix(')'))
    else {
        return false;
    };
    let mut values = parameters.split(',');
    let Some(precision) = values.next().and_then(|value| value.parse::<u8>().ok()) else {
        return false;
    };
    let Some(scale) = values.next().and_then(|scale| scale.parse::<u8>().ok()) else {
        return false;
    };
    values.next().is_none() && precision > 0 && precision <= 38 && scale <= precision
}

fn simple_insert(insert: &robin_sparkless_sql::ast::Insert) -> bool {
    insert.optimizer_hint.is_none()
        && insert.or.is_none()
        && !insert.ignore
        && insert.into
        && insert.table_alias.is_none()
        && !insert.overwrite
        && !insert.has_table_keyword
        && insert.source.as_ref().is_some_and(|query| {
            matches!(
                query.body.as_ref(),
                robin_sparkless_sql::ast::SetExpr::Values(values)
                    if !values.rows.is_empty()
                        && values.rows.iter().all(|row| {
                            !row.is_empty() && row.iter().all(simple_insert_value)
                        })
            ) && query.with.is_none()
                && query.order_by.is_none()
                && query.limit_clause.is_none()
                && query.fetch.is_none()
                && query.locks.is_empty()
                && query.for_clause.is_none()
                && query.settings.is_none()
                && query.format_clause.is_none()
                && query.pipe_operators.is_empty()
        })
        && insert.assignments.is_empty()
        && insert.partitioned.is_none()
        && insert.after_columns.is_empty()
        && insert.on.is_none()
        && insert.returning.is_none()
        && !insert.replace_into
        && insert.priority.is_none()
        && insert.insert_alias.is_none()
        && insert.settings.is_none()
        && insert.format_clause.is_none()
}

fn simple_insert_value(expression: &robin_sparkless_sql::ast::Expr) -> bool {
    use robin_sparkless_sql::ast::{CastKind, Expr, UnaryOperator, Value};
    match expression {
        Expr::Value(value) => matches!(
            &value.value,
            Value::Number(..) | Value::SingleQuotedString(_) | Value::Boolean(_) | Value::Null
        ),
        Expr::Cast {
            kind: CastKind::Cast,
            expr,
            data_type,
            array: false,
            format: None,
        } => {
            let target_type = data_type
                .to_string()
                .chars()
                .filter(|character| !character.is_ascii_whitespace())
                .collect::<String>()
                .to_ascii_uppercase();
            let supported_target = matches!(
                target_type.as_str(),
                "TIMESTAMP"
                    | "TIMESTAMP(0)"
                    | "TIMESTAMP(1)"
                    | "TIMESTAMP(2)"
                    | "TIMESTAMP(3)"
                    | "TIMESTAMP(4)"
                    | "TIMESTAMP(5)"
                    | "TIMESTAMP(6)"
                    | "TIMESTAMPWITHTIMEZONE"
                    | "TIMESTAMPWITHOUTTIMEZONE"
                    | "TIMESTAMPTZ"
                    | "TIMESTAMPNTZ"
                    | "TIMESTAMP_NTZ"
                    | "TIMESTAMP_LTZ"
            ) || decimal_type_supported(&target_type);
            let string_source = matches!(
                expr.as_ref(),
                Expr::Value(value) if matches!(&value.value, Value::SingleQuotedString(_))
            );
            supported_target && string_source
        }
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => matches!(
            expr.as_ref(),
            Expr::Value(value) if matches!(&value.value, Value::Number(..))
        ),
        _ => false,
    }
}

fn render_insert_value(
    expression: &robin_sparkless_sql::ast::Expr,
    target_type: &DataType,
) -> Result<String, ExecutionError> {
    use robin_sparkless_sql::ast::{Expr, Value};

    if matches!(expression, Expr::Value(value) if matches!(&value.value, Value::Null)) {
        return Ok("NULL".into());
    }
    if let Expr::Cast {
        expr,
        data_type,
        kind: robin_sparkless_sql::ast::CastKind::Cast,
        array: false,
        format: None,
    } = expression
    {
        let Expr::Value(value) = expr.as_ref() else {
            return Err(insert_type_mismatch());
        };
        if !matches!(&value.value, Value::SingleQuotedString(_)) {
            return Err(insert_type_mismatch());
        }
        let source_type = data_type
            .to_string()
            .chars()
            .filter(|character| !character.is_ascii_whitespace())
            .collect::<String>()
            .to_ascii_uppercase();
        return match target_type {
            DataType::Timestamp
                if matches!(
                    source_type.as_str(),
                    "TIMESTAMP"
                        | "TIMESTAMP(0)"
                        | "TIMESTAMP(1)"
                        | "TIMESTAMP(2)"
                        | "TIMESTAMP(3)"
                        | "TIMESTAMP(4)"
                        | "TIMESTAMP(5)"
                        | "TIMESTAMP(6)"
                        | "TIMESTAMPWITHTIMEZONE"
                        | "TIMESTAMPTZ"
                        | "TIMESTAMP_LTZ"
                ) =>
            {
                Ok(format!("CAST({expr} AS TIMESTAMP WITH TIME ZONE)"))
            }
            DataType::TimestampNtz
                if matches!(
                    source_type.as_str(),
                    "TIMESTAMPWITHOUTTIMEZONE" | "TIMESTAMPNTZ" | "TIMESTAMP_NTZ"
                ) =>
            {
                Ok(format!("CAST({expr} AS TIMESTAMP WITHOUT TIME ZONE)"))
            }
            DataType::Decimal(precision, scale)
                if source_type == format!("DECIMAL({precision},{scale})")
                    || source_type == format!("NUMERIC({precision},{scale})") =>
            {
                Ok(format!("CAST({expr} AS DECIMAL({precision},{scale}))"))
            }
            _ => Err(insert_type_mismatch()),
        };
    }

    if let Expr::Value(value) = expression {
        match (&value.value, target_type) {
            (Value::SingleQuotedString(_), DataType::String) => {
                return Ok(expression.to_string());
            }
            (Value::SingleQuotedString(_), DataType::Date) => {
                return Ok(format!("CAST({expression} AS DATE)"));
            }
            (Value::Boolean(_), DataType::Boolean) => return Ok(expression.to_string()),
            (Value::Number(..), _) => {}
            _ => return Err(insert_type_mismatch()),
        }
    }

    let Some(source_type) = numeric_insert_literal_type(expression) else {
        return Err(insert_type_mismatch());
    };
    let Some(target_sql_type) = insert_target_sql_type(target_type) else {
        return Err(insert_type_mismatch());
    };
    let source_is_integral = integral_bits(&source_type).is_some();
    let source_is_decimal = matches!(&source_type, DataType::Decimal(..));
    let compatible = match target_type {
        DataType::Byte | DataType::Short | DataType::Integer | DataType::Long => {
            source_is_integral || source_is_decimal
        }
        DataType::Float => {
            source_is_integral || source_is_decimal || matches!(&source_type, DataType::Float)
        }
        DataType::Double => {
            source_is_integral
                || source_is_decimal
                || matches!(&source_type, DataType::Float | DataType::Double)
        }
        DataType::Decimal(..) => source_is_integral || source_is_decimal,
        DataType::String => {
            source_is_integral
                || source_is_decimal
                || matches!(&source_type, DataType::Float | DataType::Double)
        }
        _ => false,
    };
    if !compatible {
        return Err(insert_type_mismatch());
    }

    let value = if matches!(
        target_type,
        DataType::Byte | DataType::Short | DataType::Integer | DataType::Long
    ) && source_is_decimal
    {
        format!("TRUNC({expression})")
    } else {
        expression.to_string()
    };
    Ok(format!("CAST({value} AS {target_sql_type})"))
}

fn numeric_insert_literal_type(expression: &robin_sparkless_sql::ast::Expr) -> Option<DataType> {
    use robin_sparkless_sql::ast::{Expr, UnaryOperator, Value};
    match expression {
        Expr::Value(value) if matches!(&value.value, Value::Number(..)) => {
            sql_literal_type(&expression.to_string())
        }
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } if matches!(expr.as_ref(), Expr::Value(value) if matches!(&value.value, Value::Number(..))) => {
            sql_literal_type(&expression.to_string())
        }
        _ => None,
    }
}

fn insert_target_sql_type(data_type: &DataType) -> Option<String> {
    Some(match data_type {
        DataType::Boolean => "BOOLEAN".into(),
        DataType::Byte => "TINYINT".into(),
        DataType::Short => "SMALLINT".into(),
        DataType::Integer => "INTEGER".into(),
        DataType::Long => "BIGINT".into(),
        DataType::Float => "FLOAT".into(),
        DataType::Double => "DOUBLE".into(),
        DataType::Decimal(precision, scale) => format!("DECIMAL({precision},{scale})"),
        DataType::String => "VARCHAR".into(),
        DataType::Date => "DATE".into(),
        DataType::Timestamp => "TIMESTAMP WITH TIME ZONE".into(),
        DataType::TimestampNtz => "TIMESTAMP WITHOUT TIME ZONE".into(),
        DataType::Binary => "BLOB".into(),
        _ => return None,
    })
}

fn insert_type_mismatch() -> ExecutionError {
    failure(
        "CONNECT_INSERT_TYPE_MISMATCH",
        "INSERT literal cannot be safely assigned to the target column type",
    )
}

fn execution_table_name(
    owner: &robin_sparkless_duckdb::DuckDbExecutionOwner,
    identifier: &str,
) -> Result<robin_sparkless_sql::ast::ObjectName, ExecutionError> {
    let parts = identifier
        .split('.')
        .map(|part| part.trim().trim_matches('`').trim_matches('"'))
        .collect::<Vec<_>>();
    if !(1..=2).contains(&parts.len())
        || parts.iter().any(|part| validate_identifier(part).is_err())
    {
        return Err(failure(
            "CONNECT_TABLE_IDENTIFIER_INVALID",
            "table identifiers must have one or two simple parts",
        ));
    }
    let (schema, table) = match parts.as_slice() {
        [table] => ("main", *table),
        [schema, table] => (*schema, *table),
        _ => unreachable!(),
    };
    let normalized = format!("{schema}.{table}");
    owner.table_reference(&normalized).map_err(engine_failure)?;
    let mut names = Vec::new();
    if owner.is_shared_owner() {
        names.push(robin_sparkless_sql::ast::Ident::with_quote(
            '"',
            "__sparkless_execution",
        ));
    }
    names.push(robin_sparkless_sql::ast::Ident::with_quote('"', schema));
    names.push(robin_sparkless_sql::ast::Ident::with_quote('"', table));
    Ok(names.into())
}

fn empty_relation(schema: &StructType) -> Result<String, ExecutionError> {
    let mut fields = Vec::new();
    for field in schema.logical_fields() {
        let data_type = match field.data_type {
            DataType::Boolean => "BOOLEAN",
            DataType::Byte => "TINYINT",
            DataType::Short => "SMALLINT",
            DataType::Integer => "INTEGER",
            DataType::Long => "BIGINT",
            DataType::Float => "FLOAT",
            DataType::Double => "DOUBLE",
            DataType::String => "VARCHAR",
            DataType::Date => "DATE",
            DataType::Timestamp | DataType::TimestampNtz => "TIMESTAMP",
            DataType::Binary => "BLOB",
            DataType::Decimal(precision, scale)
                if precision > 0 && precision <= 38 && scale <= precision =>
            {
                fields.push(format!(
                    "CAST(NULL AS DECIMAL({precision}, {scale})) AS {}",
                    quote_identifier(validate_identifier(&field.name)?)
                ));
                continue;
            }
            _ => {
                return Err(failure(
                    "CONNECT_LOCAL_TYPE_UNSUPPORTED",
                    "LocalRelation schema contains an unsupported DuckDB scalar type",
                ));
            }
        };
        fields.push(format!(
            "CAST(NULL AS {data_type}) AS {}",
            quote_identifier(validate_identifier(&field.name)?)
        ));
    }
    if fields.is_empty() {
        return Err(failure(
            "CONNECT_LOCAL_SCHEMA_INVALID",
            "LocalRelation schema must contain at least one field",
        ));
    }
    Ok(format!("(SELECT {} WHERE FALSE)", fields.join(", ")))
}

fn render_connect_projection(
    expression: &ConnectExpression,
    plan_aliases: &HashMap<i64, i64>,
) -> Result<String, ExecutionError> {
    if let ConnectExpression::ExpressionString(expression) = expression {
        let (expression, alias) = render_connect_expression_string(expression, true)?;
        return Ok(match alias {
            Some(alias) => format!(
                "{expression} AS {}",
                quote_identifier(validate_identifier(&alias)?)
            ),
            None => expression,
        });
    }
    if let ConnectExpression::Alias {
        expression: alias_expression,
        names,
        ..
    } = expression
    {
        if names.len() != 1 {
            return Err(failure(
                "CONNECT_ALIAS_UNSUPPORTED",
                "projection aliases require one output name",
            ));
        }
        let _ = expression_alias_metadata(expression)?;
        let name = names.first().ok_or_else(|| {
            failure(
                "CONNECT_ALIAS_UNSUPPORTED",
                "projection alias name is required",
            )
        })?;
        let expression = render_connect_expression(
            alias_expression.as_deref().ok_or_else(|| {
                failure(
                    "CONNECT_ALIAS_UNSUPPORTED",
                    "projection alias expression is required",
                )
            })?,
            plan_aliases,
        )?;
        return Ok(format!(
            "{expression} AS {}",
            quote_identifier(validate_identifier(name)?)
        ));
    }
    render_connect_expression(expression, plan_aliases)
}

fn render_connect_aggregate_projection(
    expression: &ConnectExpression,
    plan_aliases: &HashMap<i64, i64>,
    input_schema: &StructType,
) -> Result<String, ExecutionError> {
    let _ = expression_alias_metadata(expression)?;
    let (aggregate, alias) = match expression {
        ConnectExpression::Alias {
            expression: Some(inner),
            names,
            ..
        } if names.len() == 1 => (inner.as_ref(), names.first().map(String::as_str)),
        ConnectExpression::Alias { .. } => {
            return Err(failure(
                "CONNECT_ALIAS_UNSUPPORTED",
                "aggregate aliases require one name and supported metadata",
            ));
        }
        expression => (expression, None),
    };
    let rendered = match aggregate {
        ConnectExpression::Function {
            name, arguments, ..
        } if name.eq_ignore_ascii_case("sum") => {
            let argument = arguments
                .first()
                .ok_or_else(|| failure("CONNECT_AGGREGATE_ARITY", "sum requires one argument"))?;
            if arguments.len() != 1 {
                return Err(failure(
                    "CONNECT_AGGREGATE_ARITY",
                    "sum requires one argument",
                ));
            }
            let source =
                source_field_for_expression(argument, input_schema, None).ok_or_else(|| {
                    failure(
                        "CONNECT_AGGREGATE_UNSUPPORTED",
                        "sum requires a resolvable input column",
                    )
                })?;
            let (_, sql_type) = spark_sum_type(&source.data_type)?;
            let argument = render_connect_expression(argument, plan_aliases)?;
            format!("CAST(SUM({argument}) AS {sql_type})")
        }
        ConnectExpression::Function { name, .. } if name.eq_ignore_ascii_case("count") => {
            render_connect_expression(aggregate, plan_aliases)?
        }
        _ => {
            return Err(failure(
                "CONNECT_AGGREGATE_UNSUPPORTED",
                "DuckDB execution enables count and sum aggregates only",
            ));
        }
    };
    Ok(match alias {
        Some(alias) => format!(
            "{rendered} AS {}",
            quote_identifier(validate_identifier(alias)?)
        ),
        None => rendered,
    })
}

fn render_connect_expression_string(
    expression: &str,
    allow_alias: bool,
) -> Result<(String, Option<String>), ExecutionError> {
    render_connect_expression_string_with_schema(expression, allow_alias, None)
}

fn render_connect_expression_string_with_schema(
    expression: &str,
    allow_alias: bool,
    source_schema: Option<&StructType>,
) -> Result<(String, Option<String>), ExecutionError> {
    const MAX_EXPRESSION_BYTES: usize = 4096;
    if expression.len() > MAX_EXPRESSION_BYTES {
        return Err(failure(
            "CONNECT_EXPRESSION_LIMIT",
            "Connect expression exceeds 4096 bytes",
        ));
    }
    let sql = format!("SELECT {expression}");
    let statement = robin_sparkless_sql::parse_statement(&sql)
        .map_err(|error| failure("CONNECT_EXPRESSION_INVALID", &error.to_string()))?;
    let StatementPlan::Query(query) = robin_sparkless_sql::lower_statement(&statement)
        .map_err(|error| failure("CONNECT_EXPRESSION_UNSUPPORTED", &error.to_string()))?
    else {
        return Err(failure(
            "CONNECT_EXPRESSION_UNSUPPORTED",
            "Connect expression must contain one scalar expression",
        ));
    };
    let RelationalNode::Select(select) = &query.body else {
        return Err(failure(
            "CONNECT_EXPRESSION_UNSUPPORTED",
            "Connect expression must contain one scalar expression",
        ));
    };
    let no_grouping = matches!(
        &select.group_by,
        GroupByPlan::Expressions {
            expressions,
            modifiers
        } if expressions.is_empty() && modifiers.is_empty()
    );
    if query.recursive
        || !query.ctes.is_empty()
        || !query.order_by.is_empty()
        || query.limit.is_some()
        || query.offset.is_some()
        || select.distinct.is_some()
        || !select.from.is_empty()
        || select.filter.is_some()
        || !no_grouping
        || select.having.is_some()
        || select.projection.len() != 1
    {
        return Err(failure(
            "CONNECT_EXPRESSION_UNSUPPORTED",
            "Connect expression must contain exactly one scalar expression without query clauses",
        ));
    }
    let Projection::Expression { expression, alias } = &select.projection[0] else {
        return Err(failure(
            "CONNECT_EXPRESSION_UNSUPPORTED",
            "wildcard expressions are not enabled in selectExpr",
        ));
    };
    if alias.is_some() && !allow_alias {
        return Err(failure(
            "CONNECT_EXPRESSION_UNSUPPORTED",
            "aliases are only enabled in projection expressions",
        ));
    }
    Ok((
        render_sql_expression_with_schema(expression, source_schema)?,
        alias.as_ref().map(|alias| alias.to_string()),
    ))
}

fn validate_connect_string_numeric_comparisons(
    expression: &ConnectExpression,
    source_schema: &StructType,
) -> Result<(), ExecutionError> {
    let ConnectExpression::Function {
        name, arguments, ..
    } = expression
    else {
        return Ok(());
    };
    let operator = name.to_ascii_lowercase();
    if matches!(
        operator.as_str(),
        "=" | "==" | "!=" | "<>" | ">" | ">=" | "<" | "<="
    ) && arguments.len() == 2
    {
        validate_connect_attribute_string_comparison(&arguments[0], &arguments[1], source_schema)?;
        validate_connect_attribute_string_comparison(&arguments[1], &arguments[0], source_schema)?;
    }
    for argument in arguments {
        validate_connect_string_numeric_comparisons(argument, source_schema)?;
    }
    Ok(())
}

fn validate_connect_attribute_string_comparison(
    attribute: &ConnectExpression,
    literal: &ConnectExpression,
    source_schema: &StructType,
) -> Result<(), ExecutionError> {
    let ConnectExpression::Attribute { identifier, .. } = attribute else {
        return Ok(());
    };
    let ConnectExpression::Literal(ConnectLiteral::String(value)) = literal else {
        return Ok(());
    };
    let name = identifier.rsplit('.').next().unwrap_or(identifier);
    let mut matches = source_schema
        .logical_fields()
        .iter()
        .filter(|field| field.name.eq_ignore_ascii_case(name));
    let Some(field) = matches.next() else {
        return Ok(());
    };
    if matches.next().is_some() || numeric_sql_type(&field.data_type).is_none() {
        return Ok(());
    }
    let valid = match &field.data_type {
        DataType::Byte => value.parse::<i8>().is_ok(),
        DataType::Short => value.parse::<i16>().is_ok(),
        DataType::Integer => value.parse::<i32>().is_ok(),
        DataType::Long => value.parse::<i64>().is_ok(),
        DataType::Float => value.parse::<f32>().is_ok_and(f32::is_finite),
        DataType::Double => value.parse::<f64>().is_ok_and(f64::is_finite),
        DataType::Decimal(..) => value.parse::<f64>().is_ok_and(f64::is_finite),
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(failure(
            "CONNECT_EXPRESSION_TYPE_MISMATCH",
            "string literal cannot be cast to the numeric type required by the comparison",
        ))
    }
}

fn render_connect_expression(
    expression: &ConnectExpression,
    plan_aliases: &HashMap<i64, i64>,
) -> Result<String, ExecutionError> {
    match expression {
        ConnectExpression::Literal(literal) => render_literal(literal),
        ConnectExpression::Attribute {
            identifier,
            plan_id,
            is_metadata_column,
        } => {
            if *is_metadata_column {
                return Err(failure(
                    "CONNECT_ATTRIBUTE_UNSUPPORTED",
                    "metadata columns are outside the DuckDB identifier subset",
                ));
            }
            if let Some(plan_id) = plan_id {
                let Some(alias_plan_id) = plan_aliases.get(plan_id) else {
                    return Err(failure(
                        "CONNECT_ATTRIBUTE_UNSUPPORTED",
                        "attribute cannot be resolved against an exposed relation in this plan",
                    ));
                };
                let parts = identifier.split('.').collect::<Vec<_>>();
                if parts.is_empty() || parts.len() > 3 {
                    return Err(failure(
                        "CONNECT_IDENTIFIER_INVALID",
                        "column identifiers must have one to three parts",
                    ));
                }
                let name = validate_identifier(parts.last().expect("nonempty identifier"))?;
                Ok(format!(
                    "{}.{}",
                    quote_identifier(&plan_alias(*alias_plan_id)),
                    quote_identifier(name)
                ))
            } else {
                render_identifier_path(identifier)
            }
        }
        ConnectExpression::Function {
            name,
            arguments,
            is_distinct,
            is_user_defined,
            is_internal,
        } => {
            if *is_distinct || *is_user_defined || *is_internal {
                return Err(failure(
                    "CONNECT_FUNCTION_UNSUPPORTED",
                    "distinct, internal, and user-defined functions are not enabled",
                ));
            }
            if let Some(operator) = binary_operator(name) {
                if arguments.len() != 2 {
                    return Err(failure(
                        "CONNECT_FUNCTION_ARITY",
                        "binary Connect functions require two arguments",
                    ));
                }
                return Ok(format!(
                    "({} {operator} {})",
                    render_connect_expression(&arguments[0], plan_aliases)?,
                    render_connect_expression(&arguments[1], plan_aliases)?
                ));
            }
            let lower = name.to_ascii_lowercase();
            if matches!(lower.as_str(), "isnull" | "isnotnull" | "not") {
                if arguments.len() != 1 {
                    return Err(failure(
                        "CONNECT_FUNCTION_ARITY",
                        "unary Connect functions require one argument",
                    ));
                }
                let argument = render_connect_expression(&arguments[0], plan_aliases)?;
                return Ok(match lower.as_str() {
                    "isnull" => format!("({argument} IS NULL)"),
                    "isnotnull" => format!("({argument} IS NOT NULL)"),
                    _ => format!("(NOT {argument})"),
                });
            }
            if !matches!(lower.as_str(), "count" | "sum" | "abs") {
                return Err(failure(
                    "CONNECT_FUNCTION_UNSUPPORTED",
                    &format!("Connect function {name:?} is not enabled"),
                ));
            }
            if lower == "count" && arguments.is_empty() {
                return Ok("COUNT(*)".into());
            }
            let arguments = arguments
                .iter()
                .map(|argument| render_connect_expression(argument, plan_aliases))
                .collect::<Result<Vec<_>, _>>()?;
            if arguments.len() != 1 {
                return Err(failure(
                    "CONNECT_FUNCTION_ARITY",
                    "enabled scalar and aggregate functions require one argument",
                ));
            }
            Ok(format!("{}({})", lower.to_ascii_uppercase(), arguments[0]))
        }
        ConnectExpression::ExpressionString(expression) => {
            let (expression, _) = render_connect_expression_string(expression, false)?;
            Ok(expression)
        }
        ConnectExpression::Star => Ok("*".into()),
        ConnectExpression::Alias { .. } => Err(failure(
            "CONNECT_ALIAS_UNSUPPORTED",
            "nested aliases are not enabled",
        )),
        ConnectExpression::Unsupported(message) => {
            Err(failure("CONNECT_EXPRESSION_UNSUPPORTED", message))
        }
        ConnectExpression::Missing => Err(failure(
            "CONNECT_EXPRESSION_MISSING",
            "Connect expression is missing",
        )),
    }
}

fn render_literal(literal: &ConnectLiteral) -> Result<String, ExecutionError> {
    Ok(match literal {
        ConnectLiteral::Integer(value) => value.to_string(),
        ConnectLiteral::Long(value) => value.to_string(),
        ConnectLiteral::Boolean(value) => value.to_string(),
        ConnectLiteral::Double(value) if value.is_finite() => {
            format!("CAST({value} AS DOUBLE)")
        }
        ConnectLiteral::Double(_) => {
            return Err(failure(
                "CONNECT_LITERAL_INVALID",
                "non-finite floating literals are not enabled",
            ));
        }
        ConnectLiteral::String(value) => format!("'{}'", value.replace('\'', "''")),
        ConnectLiteral::Null => "NULL".into(),
        ConnectLiteral::Unsupported(message) => {
            return Err(failure("CONNECT_LITERAL_UNSUPPORTED", message));
        }
        ConnectLiteral::Missing => {
            return Err(failure(
                "CONNECT_LITERAL_MISSING",
                "Connect literal type is missing",
            ));
        }
    })
}

fn render_sql_expression(expression: &SqlExpression) -> Result<String, ExecutionError> {
    render_sql_expression_with_schema(expression, None)
}

fn render_sql_expression_with_schema(
    expression: &SqlExpression,
    source_schema: Option<&StructType>,
) -> Result<String, ExecutionError> {
    match &expression.shape {
        ExpressionShape::Identifier(parts) => {
            if parts.is_empty() || parts.len() > 3 {
                return Err(failure(
                    "CONNECT_SQL_IDENTIFIER_INVALID",
                    "SQL identifiers must have one to three parts",
                ));
            }
            parts
                .iter()
                .map(|part| validate_identifier(part).map(quote_identifier))
                .collect::<Result<Vec<_>, _>>()
                .map(|parts| parts.join("."))
        }
        ExpressionShape::Literal => Ok(expression.sql.clone()),
        ExpressionShape::Unary {
            operator,
            expression,
        } => {
            if !matches!(operator.as_str(), "+" | "-" | "NOT") {
                return Err(failure(
                    "CONNECT_SQL_EXPRESSION_UNSUPPORTED",
                    "SQL unary operator is not enabled",
                ));
            }
            Ok(format!(
                "({operator} {})",
                render_sql_expression_with_schema(expression, source_schema)?
            ))
        }
        ExpressionShape::IsNull {
            expression,
            negated,
        } => Ok(format!(
            "({} IS {}NULL)",
            render_sql_expression_with_schema(expression, source_schema)?,
            if *negated { "NOT " } else { "" }
        )),
        ExpressionShape::Binary {
            operator,
            left,
            right,
        } => {
            if !matches!(
                operator.as_str(),
                "+" | "-"
                    | "*"
                    | "/"
                    | "%"
                    | "="
                    | "!="
                    | "<>"
                    | ">"
                    | ">="
                    | "<"
                    | "<="
                    | "AND"
                    | "OR"
            ) {
                return Err(failure(
                    "CONNECT_SQL_EXPRESSION_UNSUPPORTED",
                    "SQL binary operator is not enabled",
                ));
            }
            let mut left_sql = render_sql_expression_with_schema(left, source_schema)?;
            let mut right_sql = render_sql_expression_with_schema(right, source_schema)?;
            let left_type = sql_expression_type(left, source_schema)?;
            let right_type = sql_expression_type(right, source_schema)?;
            if matches!(
                operator.as_str(),
                "=" | "!=" | "<>" | ">" | ">=" | "<" | "<="
            ) {
                if let (Some(DataType::String), Some(right_type)) =
                    (left_type.as_ref(), right_type.as_ref())
                    && numeric_sql_type(right_type).is_some()
                {
                    left_sql = cast_sql_numeric_string(left, right_type)?;
                } else if let (Some(left_type), Some(DataType::String)) =
                    (left_type.as_ref(), right_type.as_ref())
                    && numeric_sql_type(left_type).is_some()
                {
                    right_sql = cast_sql_numeric_string(right, left_type)?;
                }
            }
            let binary = format!("({left_sql} {operator} {right_sql})");
            if matches!(operator.as_str(), "+" | "-" | "*" | "/" | "%")
                && let Some(result_type) = sql_expression_type(expression, source_schema)?
                && let Some(sql_type) = numeric_sql_type(&result_type)
            {
                Ok(format!("CAST({binary} AS {sql_type})"))
            } else {
                Ok(binary)
            }
        }
        ExpressionShape::Function {
            name,
            arguments,
            distinct,
            has_star_argument,
            has_unsupported_modifiers,
        } => {
            let name = name.to_ascii_lowercase();
            if *distinct
                || *has_unsupported_modifiers
                || !matches!(name.as_str(), "count" | "sum" | "abs")
            {
                return Err(failure(
                    "CONNECT_SQL_FUNCTION_UNSUPPORTED",
                    "SQL function is not enabled",
                ));
            }
            if *has_star_argument && name == "count" && arguments.is_empty() {
                return Ok("COUNT(*)".into());
            }
            let args = arguments
                .iter()
                .map(|argument| render_sql_expression_with_schema(argument, source_schema))
                .collect::<Result<Vec<_>, _>>()?;
            if args.len() != 1 {
                return Err(failure(
                    "CONNECT_SQL_FUNCTION_ARITY",
                    "enabled SQL functions require one argument",
                ));
            }
            Ok(format!("{}({})", name.to_ascii_uppercase(), args[0]))
        }
        ExpressionShape::Composite => Err(failure(
            "CONNECT_SQL_EXPRESSION_UNSUPPORTED",
            "composite SQL expressions are outside the enabled subset",
        )),
    }
}

fn sql_expression_type(
    expression: &SqlExpression,
    source_schema: Option<&StructType>,
) -> Result<Option<DataType>, ExecutionError> {
    let data_type = match &expression.shape {
        ExpressionShape::Identifier(parts) => {
            let Some(name) = parts.last() else {
                return Ok(None);
            };
            let Some(source_schema) = source_schema else {
                return Ok(None);
            };
            let mut matches = source_schema
                .logical_fields()
                .iter()
                .filter(|field| field.name.eq_ignore_ascii_case(name));
            let Some(field) = matches.next() else {
                return Ok(None);
            };
            if matches.next().is_some() {
                return Ok(None);
            }
            Some(field.data_type.clone())
        }
        ExpressionShape::Literal => sql_literal_type(&expression.sql),
        ExpressionShape::Unary {
            operator,
            expression,
        } => {
            if operator.eq_ignore_ascii_case("NOT") {
                Some(DataType::Boolean)
            } else {
                sql_expression_type(expression, source_schema)?
            }
        }
        ExpressionShape::IsNull { .. } => Some(DataType::Boolean),
        ExpressionShape::Binary {
            operator,
            left,
            right,
        } => {
            if matches!(
                operator.as_str(),
                "AND" | "OR" | "=" | "!=" | "<>" | ">" | ">=" | "<" | "<="
            ) {
                Some(DataType::Boolean)
            } else {
                let left = sql_expression_type(left, source_schema)?;
                let right = sql_expression_type(right, source_schema)?;
                match (left.as_ref(), right.as_ref()) {
                    (Some(left), Some(right)) => sql_numeric_binary_type(operator, left, right)?,
                    _ => None,
                }
            }
        }
        ExpressionShape::Function {
            name, arguments, ..
        } => {
            let name = name.to_ascii_lowercase();
            if name == "count" {
                Some(DataType::Long)
            } else if arguments.len() == 1 && name == "abs" {
                sql_expression_type(&arguments[0], source_schema)?
            } else if arguments.len() == 1 && name == "sum" {
                match sql_expression_type(&arguments[0], source_schema)? {
                    Some(input_type) => Some(spark_sum_type(&input_type)?.0),
                    None => None,
                }
            } else {
                None
            }
        }
        ExpressionShape::Composite => None,
    };
    Ok(data_type)
}

fn sql_literal_type(sql: &str) -> Option<DataType> {
    let value = sql.trim();
    if value.eq_ignore_ascii_case("TRUE") || value.eq_ignore_ascii_case("FALSE") {
        return Some(DataType::Boolean);
    }
    if value.eq_ignore_ascii_case("NULL") {
        return None;
    }
    if value.starts_with('\'') && value.ends_with('\'') {
        return Some(DataType::String);
    }
    if value.contains(['e', 'E']) {
        return value.parse::<f64>().ok().map(|_| DataType::Double);
    }
    if let Some((whole, fraction)) = value.split_once('.') {
        let precision = whole
            .trim_start_matches(['+', '-'])
            .len()
            .saturating_add(fraction.len())
            .max(1);
        let Ok(precision) = u8::try_from(precision) else {
            return Some(DataType::Double);
        };
        if precision > 38 {
            return Some(DataType::Double);
        }
        return Some(DataType::Decimal(precision, fraction.len() as u8));
    }
    if value.parse::<i32>().is_ok() {
        Some(DataType::Integer)
    } else if value.parse::<i64>().is_ok() {
        Some(DataType::Long)
    } else {
        let precision = value.trim_start_matches(['+', '-']).len();
        u8::try_from(precision)
            .ok()
            .filter(|precision| *precision <= 38)
            .map(|precision| DataType::Decimal(precision.max(1), 0))
            .or(Some(DataType::Double))
    }
}

fn sql_numeric_binary_type(
    operator: &str,
    left: &DataType,
    right: &DataType,
) -> Result<Option<DataType>, ExecutionError> {
    use DataType::{Byte, Decimal, Double, Float, Integer, Long, Short};
    if matches!(left, Double) || matches!(right, Double) {
        return Ok(Some(Double));
    }
    if matches!(left, Float) || matches!(right, Float) {
        return Ok(Some(Float));
    }
    if let (Some(left), Some(right)) = (integral_bits(left), integral_bits(right)) {
        let data_type = if operator == "/" {
            Double
        } else if left.max(right) >= 64 {
            Long
        } else {
            Integer
        };
        return Ok(Some(data_type));
    }
    let left_decimal = decimal_shape(left);
    let right_decimal = decimal_shape(right);
    if let (Some((left_precision, left_scale)), Some((right_precision, right_scale))) =
        (left_decimal, right_decimal)
    {
        let (precision, scale) = match operator {
            "+" | "-" => (
                left_precision
                    .saturating_sub(left_scale)
                    .max(right_precision.saturating_sub(right_scale))
                    .saturating_add(left_scale.max(right_scale))
                    .saturating_add(1),
                left_scale.max(right_scale),
            ),
            "*" => (
                left_precision
                    .saturating_add(right_precision)
                    .saturating_add(1),
                left_scale.saturating_add(right_scale),
            ),
            "/" => {
                let scale = 6u8.max(left_scale.saturating_add(right_precision).saturating_add(1));
                (
                    left_precision
                        .saturating_sub(left_scale)
                        .saturating_add(right_scale)
                        .saturating_add(scale),
                    scale,
                )
            }
            "%" => (
                left_precision
                    .saturating_sub(left_scale)
                    .min(right_precision.saturating_sub(right_scale))
                    .saturating_add(left_scale.max(right_scale)),
                left_scale.max(right_scale),
            ),
            _ => return Ok(None),
        };
        let (precision, scale) = adjust_decimal_precision(precision, scale);
        return Ok(Some(Decimal(precision, scale)));
    }
    if matches!(left, Byte | Short | Integer | Long)
        || matches!(right, Byte | Short | Integer | Long)
    {
        return Ok(None);
    }
    Ok(None)
}

fn decimal_shape(data_type: &DataType) -> Option<(u8, u8)> {
    match data_type {
        DataType::Decimal(precision, scale) => Some((*precision, *scale)),
        DataType::Byte => Some((3, 0)),
        DataType::Short => Some((5, 0)),
        DataType::Integer => Some((10, 0)),
        DataType::Long => Some((20, 0)),
        _ => None,
    }
}

fn integral_bits(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Byte => Some(8),
        DataType::Short => Some(16),
        DataType::Integer => Some(32),
        DataType::Long => Some(64),
        _ => None,
    }
}

fn adjust_decimal_precision(precision: u8, scale: u8) -> (u8, u8) {
    if precision <= 38 {
        return (precision.max(1), scale.min(precision));
    }
    let integral_digits = precision.saturating_sub(scale);
    let adjusted_scale = scale.min(6).max(38u8.saturating_sub(integral_digits));
    (
        integral_digits.saturating_add(adjusted_scale).clamp(1, 38),
        adjusted_scale,
    )
}

fn numeric_sql_type(data_type: &DataType) -> Option<String> {
    Some(match data_type {
        DataType::Byte => "TINYINT".into(),
        DataType::Short => "SMALLINT".into(),
        DataType::Integer => "INTEGER".into(),
        DataType::Long => "BIGINT".into(),
        DataType::Float => "FLOAT".into(),
        DataType::Double => "DOUBLE".into(),
        DataType::Decimal(precision, scale) => format!("DECIMAL({precision},{scale})"),
        _ => return None,
    })
}

fn cast_sql_numeric_string(
    expression: &SqlExpression,
    target_type: &DataType,
) -> Result<String, ExecutionError> {
    let ExpressionShape::Literal = &expression.shape else {
        return Err(failure(
            "CONNECT_SQL_TYPE_UNSUPPORTED",
            "implicit string-to-number conversion is enabled for string literals only",
        ));
    };
    let raw = expression
        .sql
        .trim()
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .map(|value| value.replace("''", "'"))
        .ok_or_else(|| {
            failure(
                "CONNECT_SQL_TYPE_UNSUPPORTED",
                "implicit string-to-number conversion is enabled for string literals only",
            )
        })?;
    // Spark's ANSI string precedence promotes integral comparisons to BIGINT and
    // decimal/float comparisons to DOUBLE. Casting to the narrower operand type
    // can round a string before comparison and change the result.
    let comparison_type = match target_type {
        DataType::Byte | DataType::Short | DataType::Integer | DataType::Long => DataType::Long,
        DataType::Float | DataType::Double | DataType::Decimal(_, _) => DataType::Double,
        _ => unreachable!("string coercion is called only for numeric operands"),
    };
    let valid = match comparison_type {
        DataType::Long => raw.parse::<i64>().is_ok(),
        DataType::Double => raw.parse::<f64>().is_ok(),
        _ => unreachable!("comparison type is chosen from numeric operands"),
    };
    if !valid {
        return Err(failure(
            "CONNECT_SQL_TYPE_MISMATCH",
            "string literal cannot be cast to the numeric type required by the expression",
        ));
    }
    let target = numeric_sql_type(&comparison_type).ok_or_else(|| {
        failure(
            "CONNECT_SQL_TYPE_UNSUPPORTED",
            "numeric cast target is unsupported",
        )
    })?;
    Ok(format!("CAST({} AS {target})", expression.sql))
}

fn binary_operator(name: &str) -> Option<&'static str> {
    match name.to_ascii_lowercase().as_str() {
        "+" => Some("+"),
        "-" => Some("-"),
        "*" => Some("*"),
        "/" => Some("/"),
        "%" => Some("%"),
        "=" | "==" => Some("="),
        "!=" | "<>" => Some("<>"),
        ">" => Some(">"),
        ">=" => Some(">="),
        "<" => Some("<"),
        "<=" => Some("<="),
        "and" => Some("AND"),
        "or" => Some("OR"),
        _ => None,
    }
}

fn collect_plan_ids(
    relation: &ConnectRelation,
    plan_ids: &mut HashSet<i64>,
    depth: usize,
) -> Result<(), ExecutionError> {
    if depth > MAX_RELATION_DEPTH {
        return Err(failure(
            "CONNECT_PLAN_DEPTH",
            "Connect relation depth exceeds its limit",
        ));
    }
    if let Some(plan_id) = relation.plan_id
        && !plan_ids.insert(plan_id)
    {
        return Err(failure(
            "CONNECT_PLAN_ID_DUPLICATE",
            "Connect relation plan IDs must be unique within a request",
        ));
    }
    let mut visit = |child: Option<&Box<ConnectRelation>>| {
        child
            .map(|child| collect_plan_ids(child, plan_ids, depth + 1))
            .transpose()
            .map(|_| ())
    };
    match &relation.kind {
        ConnectRelationKind::Alias { input, .. }
        | ConnectRelationKind::Filter { input, .. }
        | ConnectRelationKind::Project { input, .. }
        | ConnectRelationKind::Limit { input, .. }
        | ConnectRelationKind::Aggregate { input, .. } => visit(input.as_ref())?,
        ConnectRelationKind::Join { left, right, .. } => {
            visit(left.as_ref())?;
            visit(right.as_ref())?;
        }
        ConnectRelationKind::Range { .. }
        | ConnectRelationKind::Local { .. }
        | ConnectRelationKind::CachedLocal
        | ConnectRelationKind::Read { .. }
        | ConnectRelationKind::Sql { .. }
        | ConnectRelationKind::Unsupported(_)
        | ConnectRelationKind::Missing => {}
    }
    Ok(())
}

fn expression_aliases_for_relation(
    relation: &ConnectRelation,
) -> Result<HashMap<i64, i64>, ExecutionError> {
    let Some(exposed_plan_id) = relation.plan_id else {
        return Ok(HashMap::new());
    };
    let mut relation_plan_ids = HashSet::new();
    collect_plan_ids(relation, &mut relation_plan_ids, 1)?;
    let mut aliases = HashMap::new();
    if relation_contains_join(relation) {
        aliases.insert(exposed_plan_id, exposed_plan_id);
    } else {
        for plan_id in relation_plan_ids {
            aliases.insert(plan_id, exposed_plan_id);
        }
    }
    Ok(aliases)
}

fn expression_aliases_for_join(
    left: &ConnectRelation,
    right: &ConnectRelation,
) -> Result<HashMap<i64, i64>, ExecutionError> {
    let mut aliases = expression_aliases_for_relation(left)?;
    aliases.extend(expression_aliases_for_relation(right)?);
    Ok(aliases)
}

fn relation_contains_join(relation: &ConnectRelation) -> bool {
    match &relation.kind {
        ConnectRelationKind::Join { .. } => true,
        ConnectRelationKind::Alias { input, .. }
        | ConnectRelationKind::Filter { input, .. }
        | ConnectRelationKind::Project { input, .. }
        | ConnectRelationKind::Limit { input, .. }
        | ConnectRelationKind::Aggregate { input, .. } => {
            input.as_deref().is_some_and(relation_contains_join)
        }
        ConnectRelationKind::Range { .. }
        | ConnectRelationKind::Local { .. }
        | ConnectRelationKind::CachedLocal
        | ConnectRelationKind::Read { .. }
        | ConnectRelationKind::Sql { .. }
        | ConnectRelationKind::Unsupported(_)
        | ConnectRelationKind::Missing => false,
    }
}

fn plan_alias(plan_id: i64) -> String {
    format!("__sparkless_plan_{plan_id}")
}

fn is_connect_equality(expression: &ConnectExpression) -> bool {
    matches!(expression, ConnectExpression::Function { name, arguments, .. } if (name == "=" || name == "==") && arguments.len() == 2 && arguments.iter().all(|argument| matches!(argument, ConnectExpression::Attribute { .. })))
}

fn is_connect_aggregate(expression: &ConnectExpression) -> bool {
    match expression {
        ConnectExpression::Alias {
            expression: Some(expression),
            names,
            metadata,
        } => {
            names.len() == 1
                && metadata.as_deref().is_none_or(|metadata| metadata == "{}")
                && is_connect_aggregate(expression)
        }
        ConnectExpression::Function {
            name,
            arguments,
            is_distinct,
            is_user_defined,
            is_internal,
        } => {
            let name = name.to_ascii_lowercase();
            !is_distinct
                && !is_user_defined
                && !is_internal
                && match name.as_str() {
                    "count" => arguments.is_empty() || arguments.len() == 1,
                    "sum" => {
                        arguments.len() == 1
                            && !matches!(arguments.first(), Some(ConnectExpression::Star))
                    }
                    _ => false,
                }
        }
        _ => false,
    }
}

fn is_sql_equality(expression: &SqlExpression) -> bool {
    matches!(&expression.shape, ExpressionShape::Binary { operator, left, right } if operator == "=" && matches!(left.shape, ExpressionShape::Identifier(_)) && matches!(right.shape, ExpressionShape::Identifier(_)))
}

fn validate_identifier(identifier: &str) -> Result<&str, ExecutionError> {
    let mut characters = identifier.chars();
    if !characters
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
        || identifier.len() > 128
    {
        return Err(failure(
            "CONNECT_IDENTIFIER_INVALID",
            "identifiers must be simple ASCII names of at most 128 bytes",
        ));
    }
    Ok(identifier)
}

fn render_identifier_path(identifier: &str) -> Result<String, ExecutionError> {
    let parts = identifier.split('.').collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 3 {
        return Err(failure(
            "CONNECT_IDENTIFIER_INVALID",
            "column identifiers must have one to three parts",
        ));
    }
    parts
        .iter()
        .map(|part| validate_identifier(part).map(quote_identifier))
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("."))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn engine_failure(error: EngineError) -> ExecutionError {
    let message = error.to_string();
    if message.contains("outcome is unknown") {
        failure("CONNECT_COMMIT_OUTCOME_UNKNOWN", &message)
    } else {
        failure("DUCKDB_CONNECT_EXECUTION_FAILED", &message)
    }
}

fn failure(code: &'static str, message: &str) -> ExecutionError {
    ExecutionError {
        code,
        message: message.to_string(),
        details: None,
    }
}

#[cfg(test)]
mod tests {
    use super::validate_sql_query_shape;
    use super::{
        DuckDbConnectExecution, DuckDbConnectSession, cast_sql_numeric_string,
        render_connect_expression_string, simple_insert, sql_numeric_binary_type,
    };
    use robin_sparkless_core::DataType;
    use robin_sparkless_core::engine::CancellationToken;
    use robin_sparkless_duckdb::{
        DUCKDB_EXECUTION_MAX_BATCH_BYTES, DUCKDB_EXECUTION_MAX_BATCH_ROWS, DuckDbExecutionOwner,
        DuckDbExecutionOwnerConfig,
    };
    use robin_sparkless_sql::{ExpressionShape, SqlExpression};
    use sparkless_server_core::{
        ConnectCommand, ConnectExecutionPlan, ConnectExecutionPort, ConnectExecutionRequest,
        ConnectExpression, ConnectOperationIdentity, ConnectRelation, ConnectRelationKind,
        EngineSessionHandle,
    };
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_DATABASE_ID: AtomicU64 = AtomicU64::new(0);

    struct TempDatabase(PathBuf);

    impl TempDatabase {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock must be after Unix epoch")
                .as_nanos();
            Self(std::env::temp_dir().join(format!(
                "sparkless-connect-duckdb-{}-{unique}-{}.duckdb",
                std::process::id(),
                NEXT_DATABASE_ID.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }

    impl Drop for TempDatabase {
        fn drop(&mut self) {
            let wal_path = PathBuf::from(format!("{}.wal", self.0.display()));
            let _ = std::fs::remove_file(&self.0);
            let _ = std::fs::remove_file(wal_path);
        }
    }

    fn execution_request(
        plan: ConnectExecutionPlan,
        key: &str,
        fingerprint: u8,
    ) -> ConnectExecutionRequest {
        ConnectExecutionRequest {
            plan,
            operation_identity: ConnectOperationIdentity {
                key: key.to_string(),
                request_fingerprint: [fingerprint; 32],
            },
            max_batch_rows: DUCKDB_EXECUTION_MAX_BATCH_ROWS,
            max_response_bytes: DUCKDB_EXECUTION_MAX_BATCH_BYTES,
            cancellation: CancellationToken::new(),
        }
    }

    #[test]
    fn sql_shape_accepts_the_declared_subset_and_nested_derived_queries() {
        for sql in [
            "SELECT * FROM items",
            // sqlparser normalizes LIMIT ALL to no limit, which has identical semantics.
            "SELECT * FROM items LIMIT ALL",
            "SELECT t.* FROM items AS t LIMIT 10",
            "SELECT id FROM (SELECT id FROM items) AS nested",
        ] {
            assert!(
                validate_sql_query_shape(sql).is_ok(),
                "expected supported SQL shape: {sql}"
            );
        }
    }

    #[test]
    fn sql_shape_rejects_clauses_the_lowered_plan_cannot_preserve() {
        for sql in [
            "WITH source AS (SELECT 1 AS id) SELECT id FROM source",
            "SELECT * FROM items ORDER BY id",
            "SELECT * FROM items LIMIT 10 OFFSET 1",
            "SELECT * FROM items LIMIT 1, 10",
            "SELECT * EXCLUDE (id) FROM items",
            "SELECT * FROM items AS t (renamed)",
        ] {
            assert!(
                validate_sql_query_shape(sql).is_err(),
                "expected unsupported SQL shape to be rejected: {sql}"
            );
        }
    }

    #[test]
    fn sql_arithmetic_preserves_spark_integral_result_types() {
        assert!(matches!(
            sql_numeric_binary_type("+", &DataType::Integer, &DataType::Integer).unwrap(),
            Some(DataType::Integer)
        ));
        assert!(matches!(
            sql_numeric_binary_type("/", &DataType::Integer, &DataType::Integer).unwrap(),
            Some(DataType::Double)
        ));
        assert!(matches!(
            sql_numeric_binary_type("+", &DataType::Long, &DataType::Integer).unwrap(),
            Some(DataType::Long)
        ));
    }

    #[test]
    fn sql_string_comparisons_use_spark_numeric_precedence() {
        let literal = SqlExpression {
            shape: ExpressionShape::Literal,
            sql: "'1.24'".into(),
            debug: "'1.24'".into(),
        };
        assert_eq!(
            cast_sql_numeric_string(&literal, &DataType::Decimal(2, 1)).unwrap(),
            "CAST('1.24' AS DOUBLE)"
        );

        let large_integer = SqlExpression {
            shape: ExpressionShape::Literal,
            sql: "'2147483648'".into(),
            debug: "'2147483648'".into(),
        };
        assert_eq!(
            cast_sql_numeric_string(&large_integer, &DataType::Integer).unwrap(),
            "CAST('2147483648' AS BIGINT)"
        );
    }

    #[test]
    fn select_expr_string_supports_checked_scalar_expressions_and_aliases() {
        let rendered = render_connect_expression_string("id + 1 AS value", true).unwrap();
        assert_eq!(rendered.0, "(\"id\" + 1)");
        assert_eq!(rendered.1.as_deref(), Some("value"));

        let nullable_filter = render_connect_expression_string("id IS NOT NULL", false).unwrap();
        assert_eq!(nullable_filter.0, "(\"id\" IS NOT NULL)");
        assert_eq!(nullable_filter.1, None);

        for expression in [
            "id + 1 FROM items",
            "id, other",
            "id AS value; DROP TABLE items",
        ] {
            assert!(
                render_connect_expression_string(expression, true).is_err(),
                "expected unsupported expression to be rejected: {expression}"
            );
        }
    }

    #[test]
    fn connect_project_expression_string_executes_in_duckdb() {
        let database = TempDatabase::new();
        let owner = DuckDbExecutionOwner::open(DuckDbExecutionOwnerConfig::Local {
            database_path: database.0.clone(),
        })
        .expect("local DuckDB execution owner should open");
        let session = EngineSessionHandle::new(
            "duckdb",
            DuckDbConnectSession {
                owner: Arc::new(owner),
            },
        );
        let input = ConnectRelation {
            plan_id: None,
            kind: ConnectRelationKind::Range {
                start: None,
                end: 3,
                step: 1,
                partitions: None,
            },
        };
        let relation = ConnectRelation {
            plan_id: None,
            kind: ConnectRelationKind::Project {
                input: Some(Box::new(input)),
                expressions: vec![ConnectExpression::ExpressionString(
                    "id + 1 AS value".into(),
                )],
            },
        };
        let result = DuckDbConnectExecution
            .execute_connect_plan(
                &session,
                execution_request(ConnectExecutionPlan::Root(relation), "select-expr", 11),
            )
            .expect("supported selectExpr should execute");
        let schema = result.schema.expect("projection should return a schema");
        assert_eq!(
            schema
                .logical_fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["value"]
        );
        let rows = result
            .batches
            .map(|batch| batch.expect("Arrow batch should be valid").row_count)
            .sum::<u64>();
        assert_eq!(rows, 3);
    }

    #[test]
    fn connect_commands_and_sql_queries_execute_through_the_duckdb_owner() {
        let database = TempDatabase::new();
        let owner = DuckDbExecutionOwner::open(DuckDbExecutionOwnerConfig::Local {
            database_path: database.0.clone(),
        })
        .expect("local DuckDB execution owner should open");
        let session = EngineSessionHandle::new(
            "duckdb",
            DuckDbConnectSession {
                owner: Arc::new(owner),
            },
        );
        let execution = DuckDbConnectExecution;

        for (sql, key, fingerprint) in [
            ("CREATE TABLE items (id BIGINT)", "create-items", 1),
            ("INSERT INTO items VALUES (1), (2)", "insert-items", 2),
        ] {
            let result = execution
                .execute_connect_plan(
                    &session,
                    execution_request(
                        ConnectExecutionPlan::Command(ConnectCommand::Sql {
                            sql: sql.to_string(),
                        }),
                        key,
                        fingerprint,
                    ),
                )
                .expect("supported Connect write should execute");
            assert!(result.schema.is_none());
            assert_eq!(result.batches.count(), 0);
        }

        let relation = ConnectRelation {
            plan_id: Some(1),
            kind: ConnectRelationKind::Sql {
                query:
                    "SELECT i.id, ABS(i.id) AS magnitude FROM items AS i WHERE i.id >= 1 LIMIT 10"
                        .into(),
                has_arguments: false,
            },
        };
        let result = execution
            .execute_connect_plan(
                &session,
                execution_request(ConnectExecutionPlan::Root(relation), "read-items", 3),
            )
            .expect("supported SQL relation should execute in DuckDB");
        let schema = result.schema.expect("read should return an output schema");
        assert_eq!(
            schema
                .logical_fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "magnitude"]
        );
        let rows = result
            .batches
            .map(|batch| batch.expect("Arrow batch should be valid").row_count)
            .sum::<u64>();
        assert_eq!(rows, 2);
    }

    #[test]
    fn insert_values_allow_explicit_typed_casts() {
        for (sql, expected) in [
            (
                "INSERT INTO items VALUES (CAST('2026-10-30 12:34:56.123456' AS TIMESTAMP))",
                true,
            ),
            (
                "INSERT INTO items VALUES (CAST('0.999999' AS DECIMAL(18, 6)))",
                true,
            ),
            (
                "INSERT INTO items VALUES (TRY_CAST('2026-10-30 12:34:56' AS TIMESTAMP))",
                false,
            ),
            (
                "INSERT INTO items VALUES (CAST('2026-10-30' AS DATE))",
                false,
            ),
            (
                "INSERT INTO items VALUES (CAST('1' AS DECIMAL(39, 0)))",
                false,
            ),
        ] {
            let robin_sparkless_sql::SparkStatement::Sqlparser(statement) =
                robin_sparkless_sql::parse_statement(sql).expect("SQL should parse")
            else {
                panic!("INSERT should use the SQL parser");
            };
            let robin_sparkless_sql::ast::Statement::Insert(insert) = statement.as_ref() else {
                panic!("statement should be INSERT");
            };
            assert_eq!(simple_insert(insert), expected, "{sql}");
        }
    }
}
