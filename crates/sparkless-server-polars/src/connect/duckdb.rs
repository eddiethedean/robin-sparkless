//! DuckDB implementation of the frozen Spark Connect relation and command subset.

use super::DuckDbConnectSession;
use crate::connect::{
    MAX_CONNECT_LOCAL_DECODED_BYTES, MAX_CONNECT_LOCAL_IPC_BYTES, MAX_CONNECT_LOCAL_ROWS,
};
use robin_sparkless_core::engine::{ArrowIpcBatchStream, ArrowIpcStream, CancellationToken};
use robin_sparkless_core::{DataType, EngineError, StructType};
use robin_sparkless_duckdb::{DUCKDB_EXECUTION_MAX_BATCH_BYTES, DUCKDB_EXECUTION_MAX_BATCH_ROWS};
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
                })
            }
            ConnectExecutionPlan::Root(relation) => {
                let mut renderer = PlanRenderer::new(session, relation)?;
                let sql = format!("SELECT * FROM {}", renderer.relation(relation, 1)?);
                stream_query(
                    session,
                    sql,
                    renderer.local_inputs,
                    renderer.local_input_schemas,
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
    let mut renderer = PlanRenderer::new(session, relation)?;
    let sql = format!("SELECT * FROM {}", renderer.relation(relation, 1)?);
    if !renderer.local_inputs.is_empty() {
        // LocalRelation schema is encoded in its Arrow stream, so the same bounded owner path
        // validates it and obtains the output schema without collecting result rows.
        let (sender, receiver) = mpsc::sync_channel(1);
        let owner = session.owner.clone();
        let inputs = renderer.local_inputs;
        let expected_input_schemas = renderer.local_input_schemas;
        let cancellation = cancellation.clone();
        thread::spawn(move || {
            if let Err(error) = owner.stream_query_with_arrow_ipc(
                &sql,
                &inputs,
                &expected_input_schemas,
                MAX_CONNECT_LOCAL_IPC_BYTES,
                MAX_CONNECT_LOCAL_ROWS,
                MAX_CONNECT_LOCAL_DECODED_BYTES,
                &cancellation,
                DUCKDB_EXECUTION_MAX_BATCH_ROWS,
                DUCKDB_EXECUTION_MAX_BATCH_BYTES,
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
        return match receiver.recv() {
            Ok(DuckDbEvent::Schema(schema)) => Ok(schema),
            Ok(DuckDbEvent::Error(error)) => Err(engine_failure(error)),
            _ => Err(failure(
                "CONNECT_ANALYSIS_FAILED",
                "DuckDB did not return an output schema",
            )),
        };
    }
    session
        .owner
        .describe_query(&sql, cancellation)
        .map_err(engine_failure)
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

fn stream_query(
    session: &DuckDbConnectSession,
    sql: String,
    local_inputs: Vec<Vec<u8>>,
    expected_input_schemas: Vec<Option<StructType>>,
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
    let worker_sql = sql;
    let worker_token = cancellation.clone();
    thread::spawn(move || {
        let result = owner.stream_query_with_arrow_ipc(
            &worker_sql,
            &local_inputs,
            &expected_input_schemas,
            MAX_CONNECT_LOCAL_IPC_BYTES,
            MAX_CONNECT_LOCAL_ROWS,
            MAX_CONNECT_LOCAL_DECODED_BYTES,
            &worker_token,
            max_rows,
            max_bytes,
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
        Ok(DuckDbEvent::Schema(schema)) => schema,
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
    local_inputs: Vec<Vec<u8>>,
    local_input_schemas: Vec<Option<StructType>>,
}

impl<'a> PlanRenderer<'a> {
    fn new(
        session: &'a DuckDbConnectSession,
        root: &ConnectRelation,
    ) -> Result<Self, ExecutionError> {
        let mut plan_ids = HashSet::new();
        collect_plan_ids(root, &mut plan_ids, 1)?;
        Ok(Self {
            session,
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
                    .map(|reference| format!("{reference}"))
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
                let expression_aliases = expression_aliases_for_relation(input_relation)?;
                let input = self.relation(input_relation, depth + 1)?;
                let condition = render_connect_expression(
                    condition.as_ref().ok_or_else(|| {
                        failure(
                            "CONNECT_FILTER_CONDITION_REQUIRED",
                            "Filter condition is required",
                        )
                    })?,
                    &expression_aliases,
                )?;
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
                let expression_aliases = expression_aliases_for_relation(input_relation)?;
                let input = self.relation(input_relation, depth + 1)?;
                let projection = expressions
                    .iter()
                    .map(|expression| render_connect_projection(expression, &expression_aliases))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                Ok(format!("(SELECT {projection} FROM {input})"))
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
            ConnectRelationKind::Join {
                left,
                right,
                condition,
                join_type,
                using_columns,
                has_join_data_type: _,
            } => {
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
                let left_relation = left.as_deref().ok_or_else(|| {
                    failure("CONNECT_JOIN_LEFT_REQUIRED", "Join left input is required")
                })?;
                let right_relation = right.as_deref().ok_or_else(|| {
                    failure(
                        "CONNECT_JOIN_RIGHT_REQUIRED",
                        "Join right input is required",
                    )
                })?;
                let expression_aliases =
                    expression_aliases_for_join(left_relation, right_relation)?;
                let left = self.relation(left_relation, depth + 1)?;
                let right = self.relation(right_relation, depth + 1)?;
                if !using_columns.is_empty() {
                    let columns = using_columns
                        .iter()
                        .map(|column| validate_identifier(column).map(quote_identifier))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(format!(
                        "(SELECT * FROM {left} INNER JOIN {right} USING ({}))",
                        columns.join(", ")
                    ))
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
                    Ok(format!(
                        "(SELECT * FROM {left} INNER JOIN {right} ON {})",
                        render_connect_expression(condition, &expression_aliases)?
                    ))
                }
            }
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
                let input = self.relation(input_relation, depth + 1)?;
                let aggregates = aggregate_expressions
                    .iter()
                    .map(|expression| render_connect_projection(expression, &expression_aliases))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                let groups = grouping_expressions
                    .iter()
                    .map(|expression| render_connect_expression(expression, &expression_aliases))
                    .collect::<Result<Vec<_>, _>>()?;
                if groups.is_empty() {
                    Ok(format!("(SELECT {aggregates} FROM {input})"))
                } else {
                    Ok(format!(
                        "(SELECT {aggregates} FROM {input} GROUP BY {})",
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
        let projection = select
            .projection
            .iter()
            .map(|projection| self.sql_projection(projection))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        let mut sql = format!("SELECT {projection}");
        if let Some(from) = select.from.first() {
            sql.push_str(" FROM ");
            sql.push_str(&self.sql_from(from)?);
        }
        if let Some(filter) = &select.filter {
            sql.push_str(" WHERE ");
            sql.push_str(&render_sql_expression(filter)?);
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
                        .map(render_sql_expression)
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

    fn sql_projection(&mut self, projection: &Projection) -> Result<String, ExecutionError> {
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
                let expression = render_sql_expression(expression)?;
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
    use robin_sparkless_sql::ast::{LimitClause, SelectFlavor, SelectItem, SetExpr};
    let supported_limit = match &query.limit_clause {
        None => true,
        Some(LimitClause::LimitOffset {
            limit: Some(_),
            offset: None,
            limit_by,
        }) => limit_by.is_empty(),
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
            "CTEs, ordering, OFFSET, LIMIT ALL, comma-form limits, LIMIT BY, FETCH, locking, query settings, formats, and pipes are not enabled",
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
                *name = execution_table_name(&session.owner, &name.to_string())?;
                statement.to_string()
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
    use robin_sparkless_sql::ast::{Expr, UnaryOperator, Value};
    match expression {
        Expr::Value(value) => matches!(
            &value.value,
            Value::Number(..) | Value::SingleQuotedString(_) | Value::Boolean(_) | Value::Null
        ),
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
    if let ConnectExpression::Alias {
        expression,
        names,
        metadata,
    } = expression
    {
        if names.len() != 1 || metadata.as_deref().is_some_and(|metadata| metadata != "{}") {
            return Err(failure(
                "CONNECT_ALIAS_UNSUPPORTED",
                "projection aliases require one output name and empty metadata",
            ));
        }
        let name = names.first().ok_or_else(|| {
            failure(
                "CONNECT_ALIAS_UNSUPPORTED",
                "projection alias name is required",
            )
        })?;
        let expression = render_connect_expression(
            expression.as_deref().ok_or_else(|| {
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
            let (column, value) = crate::connect::plan::parse_filter_string(expression)
                .map_err(|status| failure("CONNECT_FILTER_UNSUPPORTED", status.message()))?;
            Ok(format!(
                "{} > {value}",
                quote_identifier(validate_identifier(&column)?)
            ))
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
        ConnectLiteral::Double(value) if value.is_finite() => value.to_string(),
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
                render_sql_expression(expression)?
            ))
        }
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
            Ok(format!(
                "({} {operator} {})",
                render_sql_expression(left)?,
                render_sql_expression(right)?
            ))
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
                .map(render_sql_expression)
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
    if let Some(plan_id) = relation.plan_id {
        if !plan_ids.insert(plan_id) {
            return Err(failure(
                "CONNECT_PLAN_ID_DUPLICATE",
                "Connect relation plan IDs must be unique within a request",
            ));
        }
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
                    "count" => {
                        arguments.is_empty()
                            || (arguments.len() == 1
                                && !matches!(arguments.first(), Some(ConnectExpression::Star)))
                    }
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
