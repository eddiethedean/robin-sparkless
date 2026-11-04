//! Checked translation for the Spark Connect relation and expression subset extended in 4.19.

use crate::connect_proto as proto;
use proto::data_type::{self, Kind as DataTypeKind};
use proto::expression::{ExprType, literal::LiteralType};
use proto::plan::OpType as PlanOp;
use proto::relation::RelType;
use robin_sparkless::{
    DataType, EngineError, ExprIr, LiteralValue, LogicalField, SparkSession, StructType,
};
use robin_sparkless_core::engine::{CancellationToken, CatalogReadSnapshot};
use spark_sql_parser::ast::{BinaryOperator, Expr as SqlExpr, Value};
use sparkless_server_core::ConnectExecutionPlan;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tonic::{Code, Status};

type OutputColumnNames = HashMap<String, String>;
type JoinedOutputColumnNames = (OutputColumnNames, OutputColumnNames);

const MAX_PLAN_DEPTH: usize = 64;
const MAX_FILTER_EXPRESSION_BYTES: usize = 4096;

pub(super) struct TranslatedPlan {
    pub dataframe: robin_sparkless::DataFrame,
    pub schema: StructType,
    /// Source for the one supported global count aggregate. It is evaluated only after the
    /// execution request has been accepted, never while answering schema analysis.
    pub deferred_count_input: Option<robin_sparkless::DataFrame>,
}

struct TranslatedRelation {
    dataframe: robin_sparkless::DataFrame,
    schema: StructType,
    plan_ids: HashSet<i64>,
    plan_columns: HashMap<i64, HashMap<String, String>>,
    qualifier_columns: HashMap<String, HashMap<String, Vec<String>>>,
    deferred_count_input: Option<robin_sparkless::DataFrame>,
    /// None for managed/local inputs; otherwise the owner selected by table resolution.
    source_owner: Option<String>,
}

#[derive(Clone, Copy)]
enum ExpressionUse {
    Filter,
    Project,
    Aggregate,
}

#[cfg(test)]
pub(super) fn translate_plan(
    session: &SparkSession,
    plan: &proto::Plan,
) -> Result<TranslatedPlan, Status> {
    translate_plan_with_context(session, plan, TranslationContext::analysis())
}

pub(super) fn translate_plan_for_analysis(
    session: &SparkSession,
    plan: &proto::Plan,
    token: &CancellationToken,
) -> Result<TranslatedPlan, Status> {
    translate_plan_with_context(
        session,
        plan,
        TranslationContext {
            source_snapshot: None,
            token: token.clone(),
            materialize_sources: false,
            decoded_local_relations: Arc::new(Mutex::new(HashMap::new())),
        },
    )
}

pub(super) fn translate_plan_for_execution(
    session: &SparkSession,
    plan: &ConnectExecutionPlan,
    token: &CancellationToken,
) -> Result<TranslatedPlan, Status> {
    let plan = super::normalize::materialize_plan(plan);
    translate_proto_plan_for_execution(session, &plan, token)
}

fn translate_proto_plan_for_execution(
    session: &SparkSession,
    plan: &proto::Plan,
    token: &CancellationToken,
) -> Result<TranslatedPlan, Status> {
    let source_snapshot = session
        .begin_external_catalog_read_snapshot(token)
        .map_err(engine_failure)?;
    let decoded_local_relations = Arc::new(Mutex::new(HashMap::new()));
    // Validate the complete plan against metadata before scanning rows, including source
    // ownership across nested joins. Run this even for metadata-only external catalogs: their
    // lack of a reader must not bypass plan validation or let an earlier source scan happen.
    translate_plan_with_context(
        session,
        plan,
        TranslationContext {
            source_snapshot: source_snapshot.clone(),
            token: token.clone(),
            materialize_sources: false,
            decoded_local_relations: decoded_local_relations.clone(),
        },
    )?;
    translate_plan_with_context(
        session,
        plan,
        TranslationContext {
            source_snapshot,
            token: token.clone(),
            materialize_sources: true,
            decoded_local_relations,
        },
    )
}

#[derive(Clone)]
struct TranslationContext {
    source_snapshot: Option<Arc<dyn CatalogReadSnapshot>>,
    token: CancellationToken,
    materialize_sources: bool,
    decoded_local_relations: Arc<Mutex<HashMap<usize, (robin_sparkless::DataFrame, StructType)>>>,
}

impl TranslationContext {
    #[cfg(test)]
    fn analysis() -> Self {
        Self {
            source_snapshot: None,
            token: CancellationToken::new(),
            materialize_sources: false,
            decoded_local_relations: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

fn translate_plan_with_context(
    session: &SparkSession,
    plan: &proto::Plan,
    context: TranslationContext,
) -> Result<TranslatedPlan, Status> {
    let relation = match plan.op_type.as_ref() {
        Some(PlanOp::Root(relation)) => {
            translate_relation_with_context(session, relation, 1, &context)?
        }
        Some(PlanOp::Command(_)) => {
            return Err(unsupported(
                "Connect commands and SQL execution are outside the 4.19 subset",
            ));
        }
        None => return Err(Status::invalid_argument("Plan.op_type is required")),
    };
    Ok(TranslatedPlan {
        dataframe: relation.dataframe,
        schema: relation.schema,
        deferred_count_input: relation.deferred_count_input,
    })
}

#[cfg(test)]
fn translate_relation(
    session: &SparkSession,
    relation: &proto::Relation,
    depth: usize,
) -> Result<TranslatedRelation, Status> {
    translate_relation_with_context(session, relation, depth, &TranslationContext::analysis())
}

fn translate_relation_with_context(
    session: &SparkSession,
    relation: &proto::Relation,
    depth: usize,
    context: &TranslationContext,
) -> Result<TranslatedRelation, Status> {
    if depth > MAX_PLAN_DEPTH {
        return Err(Status::resource_exhausted(format!(
            "Connect relation depth exceeds {MAX_PLAN_DEPTH}"
        )));
    }
    let common = relation.common.as_ref();
    let mut deferred_count_input = None;
    let mut plan_columns = HashMap::new();
    let mut qualifier_columns = HashMap::new();
    let mut source_owner = None;
    let (dataframe, schema, mut plan_ids) = match relation.rel_type.as_ref() {
        Some(RelType::Range(range)) => {
            if let Some(partitions) = range.num_partitions
                && partitions <= 0
            {
                return Err(Status::invalid_argument(
                    "Range.num_partitions must be greater than zero",
                ));
            }
            if range.step == 0 {
                return Err(Status::invalid_argument("Range.step must not be zero"));
            }
            let dataframe = session
                .range(range.start.unwrap_or(0), range.end, range.step)
                .map_err(|error| {
                    let message = error.to_string();
                    if message.contains("exceeding limit") {
                        Status::resource_exhausted(format!("Connect range is too large: {message}"))
                    } else {
                        Status::invalid_argument(format!("invalid Connect range: {message}"))
                    }
                })?;
            let schema = dataframe.schema_engine().map_err(engine_failure)?;
            (dataframe, schema, HashSet::new())
        }
        Some(RelType::LocalRelation(local)) => {
            // Both passes traverse this same protobuf plan, so the relation address is a stable
            // cache key for the decoded inline frame during this request.
            let relation_key = std::ptr::from_ref(relation) as usize;
            let cached = context
                .decoded_local_relations
                .lock()
                .map_err(|_| Status::internal("Connect local relation cache is unavailable"))?
                .get(&relation_key)
                .cloned();
            if let Some((dataframe, schema)) = cached {
                (dataframe, schema, HashSet::new())
            } else {
                if local
                    .data
                    .as_ref()
                    .is_some_and(|data| data.len() > super::MAX_CONNECT_LOCAL_IPC_BYTES)
                {
                    return Err(Status::resource_exhausted(format!(
                        "Connect local Arrow IPC data exceeds {} bytes",
                        super::MAX_CONNECT_LOCAL_IPC_BYTES
                    )));
                }
                let schema = local
                    .schema
                    .as_deref()
                    .map(parse_local_schema)
                    .transpose()?;
                if local.data.is_none() && schema.is_none() {
                    return Err(Status::invalid_argument(
                        "Connect LocalRelation requires data or a schema",
                    ));
                }
                if let Some(schema) = schema.as_ref() {
                    validate_connect_scalar_schema(schema)?;
                }
                let (dataframe, schema) = session
                    .create_dataframe_from_arrow_ipc(
                        local.data.as_deref(),
                        schema,
                        super::MAX_CONNECT_LOCAL_IPC_BYTES,
                        super::MAX_CONNECT_LOCAL_ROWS,
                        super::MAX_CONNECT_LOCAL_DECODED_BYTES,
                        super::MAX_CONNECT_LOCAL_SCALAR_BYTES,
                    )
                    .map_err(local_relation_failure)?;
                validate_connect_scalar_schema(&schema)?;
                context
                    .decoded_local_relations
                    .lock()
                    .map_err(|_| Status::internal("Connect local relation cache is unavailable"))?
                    .insert(relation_key, (dataframe.clone(), schema.clone()));
                (dataframe, schema, HashSet::new())
            }
        }
        Some(RelType::CachedLocalRelation(_)) => {
            return Err(Status::unimplemented(
                "CachedLocalRelation requires the unsupported AddArtifacts transfer path",
            ));
        }
        Some(RelType::Read(read)) => {
            if read.is_streaming {
                return Err(unsupported(
                    "streaming table reads are outside the 4.19 source subset",
                ));
            }
            let Some(proto::read::ReadType::NamedTable(named_table)) = read.read_type.as_ref()
            else {
                return Err(unsupported(
                    "only configured named-table reads are supported; file data sources are disabled",
                ));
            };
            if !named_table.options.is_empty() {
                return Err(unsupported(
                    "named-table options are not enabled for configured source catalogs",
                ));
            }
            if !is_qualified_identifier(&named_table.unparsed_identifier) {
                return Err(unsupported(
                    "named-table reads require a simple qualified table identifier",
                ));
            }
            let resolved = session.table_for_connect(
                &named_table.unparsed_identifier,
                context.source_snapshot.clone(),
                context.token.clone(),
                context.materialize_sources,
            );
            let dataframe = match resolved {
                Ok((dataframe, owner)) => {
                    source_owner = owner;
                    dataframe
                }
                Err(_) if context.token.is_cancelled() => {
                    return Err(Status::cancelled("Connect source read was cancelled"));
                }
                Err(error) => {
                    return Err(engine_failure(EngineError::invalid_input(
                        error.to_string(),
                    )));
                }
            };
            let schema = dataframe.schema_engine().map_err(engine_failure)?;
            (dataframe, schema, HashSet::new())
        }
        Some(RelType::SubqueryAlias(alias)) => {
            if !is_simple_identifier(&alias.alias)
                || alias.qualifier.len() > 1
                || alias
                    .qualifier
                    .iter()
                    .any(|part| !is_simple_identifier(part))
            {
                return Err(unsupported("subquery aliases require a simple identifier"));
            }
            let input = translate_child(
                session,
                alias.input.as_deref(),
                "SubqueryAlias.input",
                depth,
                context,
            )?;
            source_owner = input.source_owner.clone();
            if input.deferred_count_input.is_some() {
                return Err(unsupported(
                    "aliases over a global aggregate are unsupported",
                ));
            }
            plan_columns = input.plan_columns;
            qualifier_columns = input.qualifier_columns;
            let mut alias_bindings = HashMap::new();
            for (name, physical) in relation_column_bindings(&input.dataframe, &input.schema)? {
                alias_bindings.insert(name.to_ascii_lowercase(), vec![physical]);
            }
            qualifier_columns.insert(alias.alias.to_ascii_lowercase(), alias_bindings);
            (
                input.dataframe.alias(&alias.alias),
                input.schema,
                input.plan_ids,
            )
        }
        Some(RelType::Filter(filter)) => {
            let input = translate_child(
                session,
                filter.input.as_deref(),
                "Filter.input",
                depth,
                context,
            )?;
            source_owner = input.source_owner.clone();
            if input.deferred_count_input.is_some() {
                return Err(unsupported(
                    "filters over a global aggregate are outside the 4.19 subset",
                ));
            }
            plan_columns = input.plan_columns.clone();
            qualifier_columns = input.qualifier_columns.clone();
            let condition = filter
                .condition
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("Filter.condition is required"))?;
            let expr = match condition.expr_type.as_ref() {
                Some(ExprType::ExpressionString(expression)) => {
                    let (name, value) = parse_filter_string(&expression.expression)?;
                    ExprIr::Gt(
                        Box::new(ExprIr::Column(resolve_public_column(&input, &name)?)),
                        Box::new(ExprIr::Lit(LiteralValue::I64(value))),
                    )
                }
                other => translate_expression(other, ExpressionUse::Filter, &input)?,
            };
            if !matches!(expr, ExprIr::Ge(_, _) | ExprIr::Gt(_, _)) {
                return Err(unsupported(
                    "filter conditions require a captured comparison expression",
                ));
            }
            validate_relation_comparison_types(&input, &expr)?;
            let schema = input.schema.clone();
            let dataframe = input
                .dataframe
                .filter_expr_ir(&expr)
                .map_err(engine_failure)?;
            (dataframe, schema, input.plan_ids)
        }
        Some(RelType::Project(project)) => {
            let input = translate_child(
                session,
                project.input.as_deref(),
                "Project.input",
                depth,
                context,
            )?;
            source_owner = input.source_owner.clone();
            if input.deferred_count_input.is_some() {
                return Err(unsupported(
                    "projections over a global aggregate are outside the 4.19 subset",
                ));
            }
            if project.expressions.is_empty() {
                return Err(Status::invalid_argument(
                    "Project.expressions must contain at least one expression",
                ));
            }
            if project.expressions.len() > super::MAX_CONNECT_SCHEMA_FIELDS {
                return Err(schema_width_error());
            }
            let expressions = project
                .expressions
                .iter()
                .map(|expression| {
                    translate_expression(
                        expression.expr_type.as_ref(),
                        ExpressionUse::Project,
                        &input,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            for expression in &expressions {
                validate_relation_comparison_types(&input, expression)?;
            }
            let dataframe = input
                .dataframe
                .select_expr_ir(&expressions)
                .map_err(engine_failure)?;
            let schema = projected_schema(&project.expressions, &input.schema, &dataframe)?;
            let output_columns = dataframe.columns().map_err(|error| {
                Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
            })?;
            plan_columns = project_plan_columns(&input, &expressions, &output_columns);
            qualifier_columns = project_qualifier_columns(&input, &expressions, &output_columns);
            (dataframe, schema, input.plan_ids)
        }
        Some(RelType::Limit(limit)) => {
            let input = translate_child(
                session,
                limit.input.as_deref(),
                "Limit.input",
                depth,
                context,
            )?;
            source_owner = input.source_owner.clone();
            if input.deferred_count_input.is_some() {
                return Err(unsupported(
                    "limits over a global aggregate are outside the 4.19 subset",
                ));
            }
            if limit.limit < 0 {
                return Err(Status::invalid_argument("Limit.limit must be nonnegative"));
            }
            plan_columns = input.plan_columns.clone();
            qualifier_columns = input.qualifier_columns.clone();
            let limit_value = usize::try_from(limit.limit)
                .map_err(|_| Status::invalid_argument("Limit.limit is out of range"))?;
            let dataframe = input.dataframe.limit(limit_value).map_err(|error| {
                Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
            })?;
            (dataframe, input.schema, input.plan_ids)
        }
        Some(RelType::Join(join)) => {
            if join.join_data_type.is_some() {
                return Err(unsupported(
                    "JoinWith struct output is outside the 4.19 subset",
                ));
            }
            let left = translate_child(session, join.left.as_deref(), "Join.left", depth, context)?;
            let right =
                translate_child(session, join.right.as_deref(), "Join.right", depth, context)?;
            if left.source_owner != right.source_owner {
                return Err(unsupported(
                    "DuckDB joins require both inputs to use the same source owner; joins with managed/local inputs or another owner are unsupported",
                ));
            }
            source_owner = left.source_owner.clone();
            if left.deferred_count_input.is_some() || right.deferred_count_input.is_some() {
                return Err(unsupported("joins over a global aggregate are unsupported"));
            }
            let join_type = proto::join::JoinType::try_from(join.join_type)
                .map_err(|_| Status::invalid_argument("Join.join_type is unknown"))?;
            let mut coalesced_right_keys = HashMap::new();
            let mut right_input_renames = HashMap::new();
            let how = match join_type {
                proto::join::JoinType::Inner => Some(robin_sparkless::JoinType::Inner),
                proto::join::JoinType::LeftOuter => Some(robin_sparkless::JoinType::Left),
                proto::join::JoinType::RightOuter => Some(robin_sparkless::JoinType::Right),
                proto::join::JoinType::FullOuter => Some(robin_sparkless::JoinType::Outer),
                proto::join::JoinType::LeftAnti => Some(robin_sparkless::JoinType::LeftAnti),
                proto::join::JoinType::LeftSemi => Some(robin_sparkless::JoinType::LeftSemi),
                proto::join::JoinType::Cross => None,
                proto::join::JoinType::Unspecified => {
                    return Err(Status::invalid_argument("Join.join_type is required"));
                }
            };
            let dataframe = if !join.using_columns.is_empty() {
                if join.join_condition.is_some() || how.is_none() {
                    return Err(Status::invalid_argument(
                        "Join.using_columns cannot be combined with a condition or CROSS join",
                    ));
                }
                let left_keys = join
                    .using_columns
                    .iter()
                    .map(|name| resolve_join_key(&left, name))
                    .collect::<Result<Vec<_>, _>>()?;
                let right_keys = join
                    .using_columns
                    .iter()
                    .map(|name| resolve_join_key(&right, name))
                    .collect::<Result<Vec<_>, _>>()?;
                validate_connect_join_key_types(&left, &right, &left_keys, &right_keys)?;
                coalesced_right_keys
                    .extend(right_keys.iter().cloned().zip(left_keys.iter().cloned()));
                let right_frame = if left_keys == right_keys {
                    right.dataframe.clone()
                } else {
                    let (frame, renames) =
                        align_using_join_keys(&left, &right, &coalesced_right_keys)?;
                    right_input_renames = renames;
                    frame
                };
                let left_refs = left_keys.iter().map(String::as_str).collect();
                let right_refs = right_keys
                    .iter()
                    .map(|name| right_input_renames.get(name).unwrap_or(name).as_str())
                    .collect();
                let joined = left
                    .dataframe
                    .join_with_keys_using(&right_frame, left_refs, right_refs, how.unwrap())
                    .map_err(|error| {
                        Status::failed_precondition(format!(
                            "CONNECT_PLAN_ANALYSIS_FAILED: {error}"
                        ))
                    })?;
                let output = joined.columns_engine().map_err(engine_failure)?;
                // Spark USING puts the requested keys before the remaining fields.
                let ordered = left_keys
                    .iter()
                    .map(String::as_str)
                    .chain(
                        output
                            .iter()
                            .filter(|name| !left_keys.contains(name))
                            .map(String::as_str),
                    )
                    .collect::<Vec<_>>();
                if ordered
                    .iter()
                    .copied()
                    .eq(output.iter().map(String::as_str))
                {
                    joined
                } else {
                    let projection = ordered
                        .into_iter()
                        .map(|name| ExprIr::Column(name.to_owned()))
                        .collect::<Vec<_>>();
                    joined.select_expr_ir(&projection).map_err(engine_failure)?
                }
            } else if how.is_none() {
                if join.join_condition.is_some() {
                    return Err(unsupported(
                        "conditioned CROSS joins are outside the 4.19 subset",
                    ));
                }
                left.dataframe
                    .cross_join(&right.dataframe)
                    .map_err(|error| {
                        Status::failed_precondition(format!(
                            "CONNECT_PLAN_ANALYSIS_FAILED: {error}"
                        ))
                    })?
            } else {
                let condition = join.join_condition.as_ref().ok_or_else(|| {
                    Status::invalid_argument("Join requires a condition or using_columns")
                })?;
                let keys = extract_connect_join_keys(
                    condition,
                    &left.plan_ids,
                    &right.plan_ids,
                    &left,
                    &right,
                )?;
                let (left_keys, right_keys): (Vec<_>, Vec<_>) = keys.into_iter().unzip();
                validate_connect_join_key_types(&left, &right, &left_keys, &right_keys)?;
                let left_refs = left_keys.iter().map(String::as_str).collect();
                let right_refs = right_keys.iter().map(String::as_str).collect();
                left.dataframe
                    .join_with_keys(&right.dataframe, left_refs, right_refs, how.unwrap(), true)
                    .map_err(|error| {
                        Status::failed_precondition(format!(
                            "CONNECT_PLAN_ANALYSIS_FAILED: {error}"
                        ))
                    })?
            };
            let (left_output_columns, right_output_columns) = joined_output_columns(
                &left,
                &right,
                &dataframe,
                &coalesced_right_keys,
                &right_input_renames,
            )?;
            let schema = joined_logical_schema(
                &left,
                &right,
                &dataframe,
                join_type,
                &left_output_columns,
                &right_output_columns,
            )?;
            plan_columns =
                joined_plan_columns(&left, &right, &left_output_columns, &right_output_columns);
            qualifier_columns = joined_qualifier_columns(
                &left,
                &right,
                &left_output_columns,
                &right_output_columns,
            );
            let mut ids = left.plan_ids;
            for id in right.plan_ids {
                if !ids.insert(id) {
                    return Err(Status::invalid_argument(format!(
                        "duplicate Connect plan_id {id} across join inputs"
                    )));
                }
            }
            (dataframe, schema, ids)
        }
        Some(RelType::Aggregate(aggregate)) => {
            let input = translate_child(
                session,
                aggregate.input.as_deref(),
                "Aggregate.input",
                depth,
                context,
            )?;
            source_owner = input.source_owner.clone();
            let group_type = proto::aggregate::GroupType::try_from(aggregate.group_type)
                .map_err(|_| Status::invalid_argument("Aggregate.group_type is unknown"))?;
            if group_type != proto::aggregate::GroupType::Groupby
                || aggregate.pivot.is_some()
                || !aggregate.grouping_sets.is_empty()
            {
                return Err(unsupported(
                    "only GROUP_TYPE_GROUPBY without pivot or grouping sets is supported",
                ));
            }
            if input.deferred_count_input.is_some() {
                return Err(unsupported(
                    "nested global aggregates are outside the 4.19 subset",
                ));
            }
            if aggregate.aggregate_expressions.is_empty()
                || aggregate.aggregate_expressions.len() > super::MAX_CONNECT_SCHEMA_FIELDS
            {
                return Err(unsupported(
                    "aggregate expression count is outside the supported schema width",
                ));
            }
            let expressions = aggregate
                .aggregate_expressions
                .iter()
                .map(|expression| {
                    translate_expression(
                        expression.expr_type.as_ref(),
                        ExpressionUse::Aggregate,
                        &input,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            validate_connect_aggregate_types(&input, &expressions)?;
            if aggregate.grouping_expressions.is_empty() {
                let [expression] = expressions.as_slice() else {
                    return Err(unsupported(
                        "one global count(1) or count(column) is supported without grouping expressions",
                    ));
                };
                let global_count_rows = is_global_count_rows(expression);
                if !global_count_rows && !is_global_count_column(expression) {
                    return Err(unsupported(
                        "global aggregation supports count(1) or count(column) only",
                    ));
                }
                let dataframe = input
                    .dataframe
                    .select_expr_ir(&expressions)
                    .map_err(engine_failure)?;
                if global_count_rows {
                    deferred_count_input = Some(input.dataframe);
                }
                let schema = StructType::from_logical_fields(vec![LogicalField::new(
                    aggregate_output_name(&aggregate.aggregate_expressions[0], &input.schema)?,
                    DataType::Long,
                    false,
                )]);
                (dataframe, schema, input.plan_ids)
            } else {
                let grouping_names = aggregate
                    .grouping_expressions
                    .iter()
                    .map(|expression| {
                        let ExprIr::Column(name) = translate_expression(
                            expression.expr_type.as_ref(),
                            ExpressionUse::Project,
                            &input,
                        )?
                        else {
                            return Err(unsupported(
                                "grouping expressions must be simple column references",
                            ));
                        };
                        Ok(name)
                    })
                    .collect::<Result<Vec<_>, Status>>()?;
                let group_refs = grouping_names.iter().map(String::as_str).collect();
                let grouped = input
                    .dataframe
                    .group_by_engine(group_refs)
                    .map_err(engine_failure)?;
                let dataframe = grouped.agg_expr_ir(&expressions).map_err(engine_failure)?;
                let grouping_ir = grouping_names
                    .iter()
                    .cloned()
                    .map(ExprIr::Column)
                    .collect::<Vec<_>>();
                let output_columns = dataframe.columns().map_err(|error| {
                    Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
                })?;
                qualifier_columns =
                    project_qualifier_columns(&input, &grouping_ir, &output_columns);
                let native_schema = dataframe.schema_engine().map_err(engine_failure)?;
                let mut output_names = aggregate
                    .grouping_expressions
                    .iter()
                    .map(|expression| public_expression_name(expression, &input.schema))
                    .collect::<Result<Vec<_>, _>>()?;
                output_names.extend(
                    aggregate
                        .aggregate_expressions
                        .iter()
                        .map(|expression| aggregate_output_name(expression, &input.schema))
                        .collect::<Result<Vec<_>, _>>()?,
                );
                if native_schema.logical_fields().len() != output_names.len() {
                    return Err(Status::failed_precondition(
                        "Connect grouped aggregate schema width does not match its expressions",
                    ));
                }
                let mut fields = native_schema.logical_fields().to_vec();
                for (index, name) in output_names
                    .iter()
                    .take(aggregate.grouping_expressions.len())
                    .enumerate()
                {
                    let source = logical_field_for_physical(&input, &grouping_names[index])?;
                    fields[index].name = name.clone();
                    fields[index].nullable = source.nullable;
                    fields[index].metadata = source.metadata.clone();
                }
                for (offset, expression) in aggregate.aggregate_expressions.iter().enumerate() {
                    let index = aggregate.grouping_expressions.len() + offset;
                    fields[index].name = output_names[index].clone();
                    fields[index].metadata.clear();
                    match aggregate_function_name(expression)? {
                        "count" => {
                            fields[index].data_type = DataType::Long;
                            fields[index].nullable = false;
                        }
                        "sum" => {
                            fields[index].data_type = DataType::Long;
                            fields[index].nullable = true;
                        }
                        name => {
                            return Err(unsupported(format!(
                                "aggregate function {name:?} is not enabled"
                            )));
                        }
                    }
                }
                let schema = StructType::from_logical_fields(fields);
                (dataframe, schema, input.plan_ids)
            }
        }
        Some(_) => {
            return Err(unsupported(
                "relation type is not enabled in the 4.19 subset",
            ));
        }
        None => return Err(Status::invalid_argument("Relation.rel_type is required")),
    };

    if let Some(plan_id) = common.and_then(|common| common.plan_id)
        && !plan_ids.insert(plan_id)
    {
        return Err(Status::invalid_argument(format!(
            "duplicate Connect plan_id {plan_id}"
        )));
    }
    if let Some(plan_id) = common.and_then(|common| common.plan_id) {
        plan_columns.insert(plan_id, relation_column_bindings(&dataframe, &schema)?);
    }
    validate_schema_width(&schema)?;
    Ok(TranslatedRelation {
        dataframe,
        schema,
        plan_ids,
        plan_columns,
        qualifier_columns,
        deferred_count_input,
        source_owner,
    })
}

fn projected_schema(
    expressions: &[proto::Expression],
    input_schema: &StructType,
    dataframe: &robin_sparkless::DataFrame,
) -> Result<StructType, Status> {
    let native_schema = dataframe.schema_engine().map_err(engine_failure)?;
    if native_schema.logical_fields().len() != expressions.len() {
        return Err(Status::failed_precondition(
            "Connect projection schema width does not match its expressions",
        ));
    }
    let fields = expressions
        .iter()
        .zip(native_schema.logical_fields())
        .map(|(expression, native_field)| {
            let mut field = native_field.clone();
            field.name = public_expression_name(expression, input_schema)?;
            Ok(field)
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(StructType::from_logical_fields(fields))
}

fn public_expression_name(
    expression: &proto::Expression,
    input_schema: &StructType,
) -> Result<String, Status> {
    match expression.expr_type.as_ref() {
        Some(ExprType::Alias(alias)) => {
            if alias.name.len() != 1 {
                return Err(unsupported("projection aliases require one name"));
            }
            Ok(alias.name[0].clone())
        }
        Some(ExprType::UnresolvedAttribute(attribute)) => {
            let name = attribute
                .unparsed_identifier
                .rsplit('.')
                .next()
                .expect("non-empty validated identifier");
            if !is_simple_join_identifier(&attribute.unparsed_identifier) {
                return Err(unsupported("projection columns must be simple identifiers"));
            }
            let mut fields = input_schema
                .logical_fields()
                .iter()
                .filter(|candidate| candidate.name.eq_ignore_ascii_case(name));
            if fields.next().is_none() {
                return Err(Status::failed_precondition(format!(
                    "Connect projection column {:?} disappeared from its input schema",
                    name
                )));
            };
            if fields.next().is_some() {
                return Err(Status::invalid_argument(format!(
                    "CONNECT_PLAN_ANALYSIS_FAILED: ambiguous column {:?}",
                    name
                )));
            }
            Ok(name.to_string())
        }
        Some(ExprType::Literal(literal)) => match literal.literal_type.as_ref() {
            Some(LiteralType::Integer(value)) => Ok(value.to_string()),
            Some(LiteralType::Long(value)) => Ok(value.to_string()),
            _ => Err(unsupported(
                "only integer literals are supported in Connect expressions",
            )),
        },
        Some(ExprType::UnresolvedFunction(function)) => {
            let [left, right] = function.arguments.as_slice() else {
                return Err(unsupported(
                    "Connect projection function has unsupported arity",
                ));
            };
            let left = public_expression_name(left, input_schema)?;
            let right = public_expression_name(right, input_schema)?;
            match function.function_name.as_str() {
                "+" => Ok(format!("({left} + {right})")),
                ">=" => Ok(format!("({left} >= {right})")),
                _ => Err(unsupported(format!(
                    "Connect projection function {:?} is unsupported",
                    function.function_name
                ))),
            }
        }
        _ => Err(unsupported(
            "Connect projection expression name is unsupported",
        )),
    }
}

fn resolve_public_column(input: &TranslatedRelation, name: &str) -> Result<String, Status> {
    let mut matches = input
        .schema
        .logical_fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name.eq_ignore_ascii_case(name));
    let Some((index, _)) = matches.next() else {
        return Err(Status::invalid_argument(format!(
            "CONNECT_PLAN_ANALYSIS_FAILED: unresolved column {name:?}"
        )));
    };
    if matches.next().is_some() {
        return Err(Status::invalid_argument(format!(
            "CONNECT_PLAN_ANALYSIS_FAILED: ambiguous column {name:?}"
        )));
    }
    input
        .dataframe
        .columns()
        .map_err(|error| {
            Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
        })?
        .get(index)
        .cloned()
        .ok_or_else(|| {
            Status::failed_precondition(
                "Connect logical schema does not match its physical dataframe",
            )
        })
}

fn relation_column_bindings(
    dataframe: &robin_sparkless::DataFrame,
    schema: &StructType,
) -> Result<HashMap<String, String>, Status> {
    let columns = dataframe.columns().map_err(|error| {
        Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
    })?;
    if columns.len() != schema.logical_fields().len() {
        return Err(Status::failed_precondition(
            "Connect relation schema width does not match its dataframe columns",
        ));
    }
    Ok(schema
        .logical_fields()
        .iter()
        .zip(columns)
        .map(|(field, physical_name)| (field.name.clone(), physical_name))
        .collect())
}

fn logical_field_for_physical<'a>(
    input: &'a TranslatedRelation,
    name: &str,
) -> Result<&'a LogicalField, Status> {
    let columns = input.dataframe.columns().map_err(|error| {
        Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
    })?;
    let matches = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.eq_ignore_ascii_case(name))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => input.schema.logical_fields().get(*index).ok_or_else(|| {
            Status::failed_precondition(
                "Connect logical schema does not match its physical dataframe",
            )
        }),
        [] => Err(Status::invalid_argument(format!(
            "CONNECT_PLAN_ANALYSIS_FAILED: unresolved column {name:?}"
        ))),
        _ => Err(Status::invalid_argument(format!(
            "CONNECT_PLAN_ANALYSIS_FAILED: ambiguous column {name:?}"
        ))),
    }
}

/// Align the right input by its resolved USING keys, which can occupy different
/// physical columns on the two inputs. Retain original names for lineage.
fn align_using_join_keys(
    left: &TranslatedRelation,
    right: &TranslatedRelation,
    right_to_left_keys: &OutputColumnNames,
) -> Result<(robin_sparkless::DataFrame, OutputColumnNames), Status> {
    let right_names = right.dataframe.columns_engine().map_err(engine_failure)?;
    let mut reserved = left
        .dataframe
        .columns_engine()
        .map_err(engine_failure)?
        .into_iter()
        .chain(right_names.iter().cloned())
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let targets = right_to_left_keys
        .values()
        .map(|key| key.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut renames = HashMap::new();
    let mut projection = Vec::with_capacity(right_names.len());
    for name in right_names {
        let target = if let Some(left_key) = right_to_left_keys.get(&name) {
            left_key.clone()
        } else if targets.contains(&name.to_ascii_lowercase()) {
            // A right non-key may already occupy a physical name needed by a key.
            let base = format!("{name}_right");
            let mut target = base.clone();
            for suffix in 1.. {
                if reserved.insert(target.to_ascii_lowercase()) {
                    break;
                }
                target = format!("{base}_{suffix}");
            }
            target
        } else {
            name.clone()
        };
        let column = ExprIr::Column(name.clone());
        if name == target {
            projection.push(column);
        } else {
            projection.push(robin_sparkless::alias(column, &target));
            renames.insert(name, target);
        }
    }
    let frame = right
        .dataframe
        .select_expr_ir(&projection)
        .map_err(engine_failure)?;
    Ok((frame, renames))
}

fn joined_output_columns(
    left: &TranslatedRelation,
    right: &TranslatedRelation,
    output: &robin_sparkless::DataFrame,
    coalesced_right_keys: &HashMap<String, String>,
    right_input_renames: &OutputColumnNames,
) -> Result<JoinedOutputColumnNames, Status> {
    let left_names = left.dataframe.columns().map_err(|error| {
        Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
    })?;
    let output_names = output.columns().map_err(|error| {
        Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
    })?;
    let mut left_map = HashMap::new();
    let mut used = HashSet::new();
    for name in &left_names {
        if let Some(output_name) = output_names
            .iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(name))
        {
            left_map.insert(name.clone(), output_name.clone());
            used.insert(name.to_ascii_lowercase());
        }
    }
    let right_names = right.dataframe.columns().map_err(|error| {
        Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
    })?;
    let left_set = left_names
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let untouched_right_names = right_names
        .iter()
        .filter(|name| !coalesced_right_keys.contains_key(*name))
        .map(|name| right_input_renames.get(name).unwrap_or(name))
        .filter(|name| !left_set.contains(&name.to_ascii_lowercase()))
        .filter(|name| {
            output_names
                .iter()
                .any(|output| output.eq_ignore_ascii_case(name))
        })
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut right_map = HashMap::new();
    for original in right_names {
        if let Some(left_key) = coalesced_right_keys.get(&original) {
            let output_name = left_map.get(left_key).unwrap_or(left_key);
            right_map.insert(original, output_name.clone());
            continue;
        }
        let name = right_input_renames.get(&original).unwrap_or(&original);
        let candidates = if used.contains(&name.to_ascii_lowercase()) {
            std::iter::once(format!("{name}_right"))
                .chain((1..=output_names.len()).map(|index| format!("{name}_right_{index}")))
                .collect::<Vec<_>>()
        } else {
            vec![name.clone()]
        };
        if let Some(output_name) = candidates.iter().find_map(|candidate| {
            output_names.iter().find(|output_name| {
                output_name.eq_ignore_ascii_case(candidate)
                    && !used.contains(&output_name.to_ascii_lowercase())
                    && (candidate.eq_ignore_ascii_case(name)
                        || !untouched_right_names.contains(&output_name.to_ascii_lowercase()))
            })
        }) {
            used.insert(output_name.to_ascii_lowercase());
            right_map.insert(original, output_name.clone());
        }
    }
    Ok((left_map, right_map))
}

fn joined_logical_schema(
    left: &TranslatedRelation,
    right: &TranslatedRelation,
    output: &robin_sparkless::DataFrame,
    join_type: proto::join::JoinType,
    left_columns: &HashMap<String, String>,
    right_columns: &HashMap<String, String>,
) -> Result<StructType, Status> {
    let output_columns = output.columns().map_err(|error| {
        Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
    })?;
    let mut used_names = HashSet::new();
    let mut fields = Vec::with_capacity(output_columns.len());
    for output_column in output_columns {
        let left_source = left_columns
            .iter()
            .find(|(_, joined)| joined.eq_ignore_ascii_case(&output_column))
            .map(|(source, _)| (left, source.as_str(), false));
        let source = left_source.or_else(|| {
            right_columns
                .iter()
                .find(|(_, joined)| joined.eq_ignore_ascii_case(&output_column))
                .map(|(source, _)| (right, source.as_str(), true))
        });
        let Some((relation, source_column, is_right)) = source else {
            return Err(Status::failed_precondition(format!(
                "Connect join output column {output_column:?} has no input lineage"
            )));
        };
        let mut field = logical_field_for_physical(relation, source_column)?.clone();
        if is_right {
            let base = field.name.clone();
            if used_names
                .iter()
                .any(|name: &String| name.eq_ignore_ascii_case(&base))
            {
                // The engine chooses one suffix for every overlapping right column. Preserve
                // that exact physical output name in the logical schema as well.
                field.name = output_column.clone();
            }
            if matches!(
                join_type,
                proto::join::JoinType::LeftOuter | proto::join::JoinType::FullOuter
            ) {
                field.nullable = true;
            }
        } else if matches!(
            join_type,
            proto::join::JoinType::RightOuter | proto::join::JoinType::FullOuter
        ) {
            field.nullable = true;
        }
        used_names.insert(field.name.to_ascii_lowercase());
        fields.push(field);
    }
    Ok(StructType::from_logical_fields(fields))
}

fn joined_plan_columns(
    left: &TranslatedRelation,
    right: &TranslatedRelation,
    left_columns: &HashMap<String, String>,
    right_columns: &HashMap<String, String>,
) -> HashMap<i64, HashMap<String, String>> {
    let mut result = HashMap::new();
    for (relation, output_columns) in [(left, left_columns), (right, right_columns)] {
        for (plan_id, bindings) in &relation.plan_columns {
            let remapped = bindings
                .iter()
                .filter_map(|(source, current)| {
                    output_columns
                        .get(current)
                        .map(|output| (source.clone(), output.clone()))
                })
                .collect();
            result.insert(*plan_id, remapped);
        }
    }
    result
}

fn joined_qualifier_columns(
    left: &TranslatedRelation,
    right: &TranslatedRelation,
    left_columns: &HashMap<String, String>,
    right_columns: &HashMap<String, String>,
) -> HashMap<String, HashMap<String, Vec<String>>> {
    let mut result = HashMap::new();
    for (relation, output_columns) in [(left, left_columns), (right, right_columns)] {
        for (qualifier, columns) in &relation.qualifier_columns {
            let output = result.entry(qualifier.clone()).or_insert_with(HashMap::new);
            for (name, current_names) in columns {
                for current in current_names {
                    if let Some(joined_name) = output_columns.get(current) {
                        let names: &mut Vec<String> = output.entry(name.clone()).or_default();
                        if !names
                            .iter()
                            .any(|value| value.eq_ignore_ascii_case(joined_name))
                        {
                            names.push(joined_name.clone());
                        }
                    }
                }
            }
        }
    }
    result
}

fn project_plan_columns(
    input: &TranslatedRelation,
    expressions: &[ExprIr],
    output_columns: &[String],
) -> HashMap<i64, HashMap<String, String>> {
    let mut result = HashMap::new();
    for (plan_id, bindings) in &input.plan_columns {
        let remapped = bindings
            .iter()
            .filter_map(|(source, current)| {
                expressions
                    .iter()
                    .zip(output_columns)
                    .find(|(expression, _)| expression_references_column(expression, current))
                    .map(|(_, output)| (source.clone(), output.clone()))
            })
            .collect();
        result.insert(*plan_id, remapped);
    }
    result
}

fn project_qualifier_columns(
    input: &TranslatedRelation,
    expressions: &[ExprIr],
    output_columns: &[String],
) -> HashMap<String, HashMap<String, Vec<String>>> {
    let mut result = HashMap::new();
    for (qualifier, columns) in &input.qualifier_columns {
        let mut remapped = HashMap::new();
        for (name, current_names) in columns {
            let projected = current_names
                .iter()
                .flat_map(|current| {
                    expressions
                        .iter()
                        .zip(output_columns)
                        .filter(|(expression, _)| expression_references_column(expression, current))
                        .map(|(_, output)| output.clone())
                })
                .collect::<Vec<_>>();
            if !projected.is_empty() {
                remapped.insert(name.clone(), projected);
            }
        }
        result.insert(qualifier.clone(), remapped);
    }
    result
}

fn expression_references_column(expression: &ExprIr, column: &str) -> bool {
    match expression {
        ExprIr::Column(name) => name.eq_ignore_ascii_case(column),
        ExprIr::Call { name, args } if name == "alias" => {
            matches!(args.as_slice(), [value, ExprIr::Lit(LiteralValue::Str(_))]
                if expression_references_column(value, column))
        }
        _ => false,
    }
}

fn resolve_join_key(input: &TranslatedRelation, name: &str) -> Result<String, Status> {
    let parts = name.split('.').collect::<Vec<_>>();
    let column = match parts.as_slice() {
        [column] if is_simple_identifier(column) => *column,
        [qualifier, column] if is_simple_identifier(qualifier) && is_simple_identifier(column) => {
            let matches = input
                .qualifier_columns
                .get(&qualifier.to_ascii_lowercase())
                .and_then(|columns| columns.get(&column.to_ascii_lowercase()))
                .ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "CONNECT_PLAN_ANALYSIS_FAILED: unresolved join key {name:?}"
                    ))
                })?;
            return match matches.as_slice() {
                [resolved] => Ok(resolved.clone()),
                [] => Err(Status::invalid_argument(format!(
                    "CONNECT_PLAN_ANALYSIS_FAILED: unresolved join key {name:?}"
                ))),
                _ => Err(Status::invalid_argument(format!(
                    "CONNECT_PLAN_ANALYSIS_FAILED: ambiguous join key {name:?}"
                ))),
            };
        }
        _ => {
            return Err(unsupported(
                "join keys must be simple or alias-qualified identifiers",
            ));
        }
    };
    resolve_public_column(input, column)
}

fn validate_connect_join_key_types(
    left: &TranslatedRelation,
    right: &TranslatedRelation,
    left_keys: &[String],
    right_keys: &[String],
) -> Result<(), Status> {
    if left_keys.is_empty() || left_keys.len() != right_keys.len() {
        return Err(unsupported("joins require matching non-empty key lists"));
    }
    for (left_key, right_key) in left_keys.iter().zip(right_keys) {
        let left_type = logical_field_for_physical(left, left_key)?
            .data_type
            .clone();
        let right_type = logical_field_for_physical(right, right_key)?
            .data_type
            .clone();
        if !matches!(
            (left_type.clone(), right_type.clone()),
            (DataType::Integer, DataType::Integer) | (DataType::Long, DataType::Long)
        ) {
            return Err(unsupported(format!(
                "4.19 Connect equi-join keys require matching INT or BIGINT types; found {left_type:?} and {right_type:?}"
            )));
        }
    }
    Ok(())
}

fn is_simple_join_identifier(identifier: &str) -> bool {
    let mut parts = identifier.split('.');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(column), None, None) => is_simple_identifier(column),
        (Some(qualifier), Some(column), None) => {
            is_simple_identifier(qualifier) && is_simple_identifier(column)
        }
        _ => false,
    }
}

fn connect_join_attribute(
    expression: &proto::Expression,
) -> Result<&proto::expression::UnresolvedAttribute, Status> {
    match expression.expr_type.as_ref() {
        Some(ExprType::UnresolvedAttribute(attribute))
            if !attribute.is_metadata_column.unwrap_or(false)
                && is_simple_join_identifier(&attribute.unparsed_identifier) =>
        {
            Ok(attribute)
        }
        _ => Err(unsupported(
            "join conditions support equality between simple column references",
        )),
    }
}

fn extract_connect_join_keys(
    expression: &proto::Expression,
    left_plan_ids: &HashSet<i64>,
    right_plan_ids: &HashSet<i64>,
    left: &TranslatedRelation,
    right: &TranslatedRelation,
) -> Result<Vec<(String, String)>, Status> {
    fn visit(
        expression: &proto::Expression,
        left_plan_ids: &HashSet<i64>,
        right_plan_ids: &HashSet<i64>,
        left: &TranslatedRelation,
        right: &TranslatedRelation,
        keys: &mut Vec<(String, String)>,
    ) -> Result<(), Status> {
        match expression.expr_type.as_ref() {
            Some(ExprType::UnresolvedFunction(function))
                if function.function_name.eq_ignore_ascii_case("and")
                    && function.arguments.len() == 2 =>
            {
                visit(
                    &function.arguments[0],
                    left_plan_ids,
                    right_plan_ids,
                    left,
                    right,
                    keys,
                )?;
                visit(
                    &function.arguments[1],
                    left_plan_ids,
                    right_plan_ids,
                    left,
                    right,
                    keys,
                )
            }
            Some(ExprType::UnresolvedFunction(function))
                if matches!(function.function_name.as_str(), "=" | "==")
                    && function.arguments.len() == 2 =>
            {
                if function.is_distinct
                    || function.is_user_defined_function
                    || function.is_internal.unwrap_or(false)
                {
                    return Err(unsupported("join condition functions must be built-in"));
                }
                let first = connect_join_attribute(&function.arguments[0])?;
                let second = connect_join_attribute(&function.arguments[1])?;
                let first_id = first.plan_id.ok_or_else(|| {
                    unsupported("join condition columns must identify their input plan")
                })?;
                let second_id = second.plan_id.ok_or_else(|| {
                    unsupported("join condition columns must identify their input plan")
                })?;
                let first_is_left = left_plan_ids.contains(&first_id);
                let first_is_right = right_plan_ids.contains(&first_id);
                let second_is_left = left_plan_ids.contains(&second_id);
                let second_is_right = right_plan_ids.contains(&second_id);
                let pair = if first_is_left && !first_is_right && second_is_right && !second_is_left
                {
                    (
                        resolve_join_key(left, &first.unparsed_identifier)?,
                        resolve_join_key(right, &second.unparsed_identifier)?,
                    )
                } else if first_is_right && !first_is_left && second_is_left && !second_is_right {
                    (
                        resolve_join_key(left, &second.unparsed_identifier)?,
                        resolve_join_key(right, &first.unparsed_identifier)?,
                    )
                } else {
                    return Err(unsupported(
                        "join equality must reference one column from each input",
                    ));
                };
                keys.push(pair);
                Ok(())
            }
            _ => Err(unsupported(
                "join conditions support equi-joins combined with AND only",
            )),
        }
    }

    let mut keys = Vec::new();
    visit(
        expression,
        left_plan_ids,
        right_plan_ids,
        left,
        right,
        &mut keys,
    )?;
    if keys.is_empty() {
        return Err(unsupported("join condition contains no equality keys"));
    }
    Ok(keys)
}

fn aggregate_output_name(
    expression: &proto::Expression,
    input_schema: &StructType,
) -> Result<String, Status> {
    match expression.expr_type.as_ref() {
        Some(ExprType::Alias(alias)) => {
            if alias.name.len() != 1
                || alias.name[0].is_empty()
                || alias
                    .metadata
                    .as_deref()
                    .is_some_and(|metadata| metadata != "{}")
            {
                return Err(unsupported(
                    "aggregate aliases require one name and empty metadata",
                ));
            }
            Ok(alias.name[0].clone())
        }
        Some(ExprType::UnresolvedFunction(function)) => {
            let [argument] = function.arguments.as_slice() else {
                return Err(unsupported("aggregate functions require one argument"));
            };
            let argument_name = match argument.expr_type.as_ref() {
                Some(ExprType::Literal(literal))
                    if matches!(
                        literal.literal_type.as_ref(),
                        Some(LiteralType::Integer(1) | LiteralType::Long(1))
                    ) =>
                {
                    "1".to_string()
                }
                Some(ExprType::UnresolvedAttribute(attribute)) => {
                    let _ = public_expression_name(argument, input_schema)?;
                    attribute.unparsed_identifier.clone()
                }
                _ => {
                    return Err(unsupported(
                        "aggregate arguments must be a column or literal 1",
                    ));
                }
            };
            match function.function_name.as_str() {
                "count" => Ok(format!("count({argument_name})")),
                "sum" if argument_name != "1" => Ok(format!("sum({argument_name})")),
                "sum" => Err(unsupported("sum does not accept a literal argument")),
                _ => Err(unsupported(format!(
                    "aggregate output name for {:?} is unsupported",
                    function.function_name
                ))),
            }
        }
        _ => Err(unsupported("aggregate output expression is unsupported")),
    }
}

fn aggregate_function_name(expression: &proto::Expression) -> Result<&str, Status> {
    match expression.expr_type.as_ref() {
        Some(ExprType::Alias(alias)) => {
            let expression = alias
                .expr
                .as_deref()
                .ok_or_else(|| Status::invalid_argument("Alias.expr is required"))?;
            aggregate_function_name(expression)
        }
        Some(ExprType::UnresolvedFunction(function)) => Ok(function.function_name.as_str()),
        _ => Err(unsupported(
            "aggregate expression must be a function or alias",
        )),
    }
}

fn validate_connect_aggregate_types(
    input: &TranslatedRelation,
    expressions: &[ExprIr],
) -> Result<(), Status> {
    for expression in expressions {
        let expression = match expression {
            ExprIr::Call { name, args } if name == "alias" => {
                let [value, ExprIr::Lit(LiteralValue::Str(_))] = args.as_slice() else {
                    return Err(unsupported("malformed aggregate alias expression"));
                };
                value
            }
            expression => expression,
        };
        if let ExprIr::Call { name, args } = expression
            && name == "sum"
        {
            let [ExprIr::Column(column)] = args.as_slice() else {
                return Err(unsupported("sum requires one column reference"));
            };
            let field = logical_field_for_physical(input, column)?;
            if !matches!(&field.data_type, DataType::Integer | DataType::Long) {
                return Err(unsupported(format!(
                    "4.19 Connect sum supports INT and BIGINT columns; found {:?}",
                    field.data_type
                )));
            }
        }
    }
    Ok(())
}

fn is_global_count_rows(expression: &ExprIr) -> bool {
    match expression {
        ExprIr::Call { name, args } if name == "count_rows" => args.is_empty(),
        ExprIr::Call { name, args } if name == "alias" => {
            matches!(args.as_slice(), [inner, ExprIr::Lit(LiteralValue::Str(_))]
                if is_global_count_rows(inner))
        }
        _ => false,
    }
}

fn is_global_count_column(expression: &ExprIr) -> bool {
    match expression {
        ExprIr::Call { name, args } if name == "count" => {
            matches!(args.as_slice(), [ExprIr::Column(_)])
        }
        ExprIr::Call { name, args } if name == "alias" => {
            matches!(args.as_slice(), [inner, ExprIr::Lit(LiteralValue::Str(_))]
                if is_global_count_column(inner))
        }
        _ => false,
    }
}

#[cfg(test)]
fn validate_comparison_types(input_schema: &StructType, expression: &ExprIr) -> Result<(), Status> {
    validate_comparison_types_with(expression, &|name| field_type_by_name(input_schema, name))
}

fn validate_relation_comparison_types(
    input: &TranslatedRelation,
    expression: &ExprIr,
) -> Result<(), Status> {
    validate_comparison_types_with(expression, &|name| {
        logical_field_for_physical(input, name).map(|field| field.data_type.clone())
    })
}

#[cfg(test)]
fn field_type_by_name(input_schema: &StructType, name: &str) -> Result<DataType, Status> {
    let matches = input_schema
        .logical_fields()
        .iter()
        .filter(|field| field.name.eq_ignore_ascii_case(name))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [field] => Ok(field.data_type.clone()),
        [] => Err(Status::invalid_argument(format!(
            "CONNECT_PLAN_ANALYSIS_FAILED: unresolved column {name:?}"
        ))),
        _ => Err(Status::invalid_argument(format!(
            "CONNECT_PLAN_ANALYSIS_FAILED: ambiguous column {name:?}"
        ))),
    }
}

fn validate_comparison_types_with(
    expression: &ExprIr,
    field_type: &dyn Fn(&str) -> Result<DataType, Status>,
) -> Result<(), Status> {
    fn expression_type(
        expression: &ExprIr,
        field_type: &dyn Fn(&str) -> Result<DataType, Status>,
    ) -> Result<DataType, Status> {
        match expression {
            ExprIr::Column(name) => field_type(name),
            ExprIr::Lit(LiteralValue::I32(_)) => Ok(DataType::Integer),
            ExprIr::Lit(LiteralValue::I64(_)) => Ok(DataType::Long),
            ExprIr::Lit(LiteralValue::F64(_)) => Ok(DataType::Double),
            ExprIr::Lit(LiteralValue::Str(_)) => Ok(DataType::String),
            ExprIr::Lit(LiteralValue::Bool(_)) => Ok(DataType::Boolean),
            ExprIr::Lit(LiteralValue::Null) => Err(unsupported(
                "untyped null comparison operands are outside the 4.19 subset",
            )),
            ExprIr::Add(left, right) => {
                let left = expression_type(left, field_type)?;
                let right = expression_type(right, field_type)?;
                if !is_connect_numeric_type(&left) || !is_connect_numeric_type(&right) {
                    return Err(Status::invalid_argument(format!(
                        "CONNECT_PLAN_ANALYSIS_FAILED: arithmetic requires numeric operands, found {left:?} and {right:?}"
                    )));
                }
                Ok(robin_sparkless_plan::common_numeric_type(&left, &right))
            }
            ExprIr::Ge(left, right) | ExprIr::Gt(left, right) => {
                let left_type = expression_type(left, field_type)?;
                let right_type = expression_type(right, field_type)?;
                if compatible_comparison_types(&left_type, &right_type, left, right) {
                    Ok(DataType::Boolean)
                } else {
                    Err(Status::invalid_argument(format!(
                        "CONNECT_PLAN_ANALYSIS_FAILED: comparison requires compatible operand types, found {left_type:?} and {right_type:?}"
                    )))
                }
            }
            ExprIr::Call { name, args } if name == "alias" => {
                let [value, ExprIr::Lit(LiteralValue::Str(_))] = args.as_slice() else {
                    return Err(unsupported("malformed projection alias expression"));
                };
                expression_type(value, field_type)
            }
            _ => Err(unsupported(
                "comparison operand type cannot be analyzed in the 4.19 subset",
            )),
        }
    }

    expression_type(expression, field_type).map(|_| ())
}

fn compatible_comparison_types(
    left_type: &DataType,
    right_type: &DataType,
    left: &ExprIr,
    right: &ExprIr,
) -> bool {
    let numeric_pair = is_connect_numeric_type(left_type) && is_connect_numeric_type(right_type);
    let string_numeric_literal = (matches!(left_type, DataType::String)
        && is_connect_numeric_type(right_type)
        && matches!(left, ExprIr::Column(_))
        && matches!(
            right,
            ExprIr::Lit(LiteralValue::I32(_) | LiteralValue::I64(_))
        ))
        || (matches!(right_type, DataType::String)
            && is_connect_numeric_type(left_type)
            && matches!(right, ExprIr::Column(_))
            && matches!(
                left,
                ExprIr::Lit(LiteralValue::I32(_) | LiteralValue::I64(_))
            ));
    numeric_pair || string_numeric_literal || same_orderable_connect_type(left_type, right_type)
}

fn is_connect_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Byte
            | DataType::Short
            | DataType::Integer
            | DataType::Long
            | DataType::Float
            | DataType::Double
            | DataType::Decimal(..)
    )
}

fn same_orderable_connect_type(left: &DataType, right: &DataType) -> bool {
    matches!(
        (left, right),
        (DataType::String, DataType::String)
            | (DataType::Boolean, DataType::Boolean)
            | (DataType::Date, DataType::Date)
            | (DataType::Timestamp, DataType::Timestamp)
            | (DataType::TimestampNtz, DataType::TimestampNtz)
            | (DataType::Decimal(..), DataType::Decimal(..))
    )
}

fn schema_width_error() -> Status {
    Status::resource_exhausted(format!(
        "Connect schema exceeds {} fields",
        super::MAX_CONNECT_SCHEMA_FIELDS
    ))
}

fn validate_schema_width(schema: &StructType) -> Result<(), Status> {
    let mut field_count = schema.logical_fields().len();
    if field_count > super::MAX_CONNECT_SCHEMA_FIELDS {
        return Err(schema_width_error());
    }
    for field in schema.logical_fields() {
        count_schema_fields(&field.data_type, &mut field_count)?;
    }
    Ok(())
}

fn translate_child(
    session: &SparkSession,
    input: Option<&proto::Relation>,
    name: &'static str,
    depth: usize,
    context: &TranslationContext,
) -> Result<TranslatedRelation, Status> {
    let input = input.ok_or_else(|| Status::invalid_argument(format!("{name} is required")))?;
    translate_relation_with_context(session, input, depth + 1, context)
}

fn translate_expression(
    expr_type: Option<&ExprType>,
    usage: ExpressionUse,
    input: &TranslatedRelation,
) -> Result<ExprIr, Status> {
    match expr_type {
        Some(ExprType::Literal(literal)) => match literal.literal_type.as_ref() {
            Some(LiteralType::Integer(value)) => Ok(ExprIr::Lit(LiteralValue::I32(*value))),
            Some(LiteralType::Long(value)) => Ok(ExprIr::Lit(LiteralValue::I64(*value))),
            Some(_) => Err(unsupported(
                "only integer literals are supported in Connect expressions",
            )),
            None => Err(Status::invalid_argument("Literal.literal_type is required")),
        },
        Some(ExprType::UnresolvedAttribute(attribute)) => {
            if attribute.unparsed_identifier.is_empty()
                || !is_simple_join_identifier(&attribute.unparsed_identifier)
                || attribute.is_metadata_column.unwrap_or(false)
            {
                return Err(unsupported(
                    "only simple non-metadata column references are supported",
                ));
            }
            validate_expression_plan_id(attribute.plan_id, &input.plan_ids)?;
            let mut identifier_parts = attribute.unparsed_identifier.split('.');
            let first = identifier_parts.next().expect("identifier is non-empty");
            let second = identifier_parts.next();
            let (qualifier, identifier) = match second {
                Some(column) => (Some(first), column),
                None => (None, first),
            };
            let plan_column = attribute
                .plan_id
                .and_then(|plan_id| input.plan_columns.get(&plan_id))
                .and_then(|columns| {
                    columns
                        .iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case(identifier))
                        .map(|(_, output_name)| output_name.clone())
                });
            let qualified_column = qualifier
                .and_then(|qualifier| {
                    input
                        .qualifier_columns
                        .get(&qualifier.to_ascii_lowercase())
                        .and_then(|columns| columns.get(&identifier.to_ascii_lowercase()))
                })
                .map(|names| names.as_slice());
            let resolved = if let Some(name) = plan_column {
                name
            } else if let Some(names) = qualified_column {
                match names {
                    [name] => name.clone(),
                    [] => {
                        return Err(Status::invalid_argument(format!(
                            "CONNECT_PLAN_ANALYSIS_FAILED: unresolved qualified column {:?}",
                            attribute.unparsed_identifier
                        )));
                    }
                    _ => {
                        return Err(Status::invalid_argument(format!(
                            "CONNECT_PLAN_ANALYSIS_FAILED: ambiguous qualified column {:?}",
                            attribute.unparsed_identifier
                        )));
                    }
                }
            } else if qualifier.is_some() {
                return Err(Status::invalid_argument(format!(
                    "CONNECT_PLAN_ANALYSIS_FAILED: unresolved qualifier in {:?}",
                    attribute.unparsed_identifier
                )));
            } else {
                resolve_public_column(input, identifier)?
            };
            Ok(ExprIr::Column(resolved))
        }
        Some(ExprType::UnresolvedFunction(function)) => {
            if function.is_distinct
                || function.is_user_defined_function
                || function.is_internal.unwrap_or(false)
            {
                return Err(unsupported(
                    "distinct, internal, and user-defined Connect functions are unsupported",
                ));
            }
            match (
                usage,
                function.function_name.as_str(),
                function.arguments.as_slice(),
            ) {
                (ExpressionUse::Project, "+", [left, right]) => {
                    let left = translate_expression(left.expr_type.as_ref(), usage, input)?;
                    let right = translate_expression(right.expr_type.as_ref(), usage, input)?;
                    Ok(ExprIr::Add(Box::new(left), Box::new(right)))
                }
                (ExpressionUse::Project | ExpressionUse::Filter, ">=", [left, right]) => {
                    let left = translate_expression(left.expr_type.as_ref(), usage, input)?;
                    let right = translate_expression(right.expr_type.as_ref(), usage, input)?;
                    Ok(ExprIr::Ge(Box::new(left), Box::new(right)))
                }
                (ExpressionUse::Aggregate, "count", [argument]) => {
                    let argument = translate_expression(argument.expr_type.as_ref(), usage, input)?;
                    match argument {
                        ExprIr::Lit(LiteralValue::I32(1) | LiteralValue::I64(1)) => {
                            let row_count = ExprIr::Call {
                                name: "count_rows".to_string(),
                                args: Vec::new(),
                            };
                            Ok(robin_sparkless::alias(row_count, "count(1)"))
                        }
                        column @ ExprIr::Column(_) => Ok(ExprIr::Call {
                            name: "count".to_string(),
                            args: vec![column],
                        }),
                        _ => Err(unsupported(
                            "count supports a column or the literal 1 in the 4.19 subset",
                        )),
                    }
                }
                (ExpressionUse::Aggregate, "sum", [argument]) => {
                    let argument = translate_expression(argument.expr_type.as_ref(), usage, input)?;
                    match argument {
                        column @ ExprIr::Column(_) => Ok(ExprIr::Call {
                            name: "sum".to_string(),
                            args: vec![column],
                        }),
                        _ => Err(unsupported("sum requires a simple column reference")),
                    }
                }
                _ => Err(unsupported(format!(
                    "Connect function {:?} with this arity or context is unsupported",
                    function.function_name
                ))),
            }
        }
        Some(ExprType::Alias(alias))
            if matches!(usage, ExpressionUse::Project | ExpressionUse::Aggregate) =>
        {
            if alias.name.len() != 1
                || alias.name[0].is_empty()
                || alias
                    .metadata
                    .as_deref()
                    .is_some_and(|metadata| metadata != "{}")
            {
                return Err(unsupported(
                    "projection aliases require one name and empty metadata",
                ));
            }
            let expression = alias
                .expr
                .as_deref()
                .ok_or_else(|| Status::invalid_argument("Alias.expr is required"))?;
            let expression = translate_expression(expression.expr_type.as_ref(), usage, input)?;
            Ok(robin_sparkless::alias(expression, &alias.name[0]))
        }
        Some(ExprType::ExpressionString(_)) => Err(unsupported(
            "expression_string is supported only as a checked Filter condition",
        )),
        Some(_) => Err(unsupported(
            "Connect expression type is not enabled in the 4.19 subset",
        )),
        None => Err(Status::invalid_argument("Expression.expr_type is required")),
    }
}

fn validate_expression_plan_id(
    plan_id: Option<i64>,
    input_plan_ids: &HashSet<i64>,
) -> Result<(), Status> {
    if let Some(plan_id) = plan_id
        && !input_plan_ids.contains(&plan_id)
    {
        return Err(Status::invalid_argument(format!(
            "expression references unknown input plan_id {plan_id}"
        )));
    }
    Ok(())
}

fn parse_filter_string(expression: &str) -> Result<(String, i64), Status> {
    if expression.len() > MAX_FILTER_EXPRESSION_BYTES {
        return Err(Status::resource_exhausted(format!(
            "Connect filter expression exceeds {MAX_FILTER_EXPRESSION_BYTES} bytes"
        )));
    }
    let expression = robin_sparkless_sql::parse_expression(expression).map_err(|error| {
        Status::invalid_argument(format!("invalid Connect filter expression: {error}"))
    })?;
    let SqlExpr::BinaryOp { left, op, right } = expression else {
        return Err(unsupported(
            "filter strings currently support a simple column > integer predicate",
        ));
    };
    if op != BinaryOperator::Gt {
        return Err(unsupported(
            "filter strings currently support the > comparison only",
        ));
    }
    let SqlExpr::Identifier(column) = *left else {
        return Err(unsupported(
            "filter strings currently require a simple column on the left",
        ));
    };
    if !is_simple_identifier(&column.value) {
        return Err(unsupported(
            "filter strings currently require a simple column identifier",
        ));
    }
    let SqlExpr::Value(value) = *right else {
        return Err(unsupported(
            "filter strings currently require an integer literal on the right",
        ));
    };
    let Value::Number(value, _) = value.value else {
        return Err(unsupported(
            "filter strings currently require an integer literal on the right",
        ));
    };
    if value.contains(['.', 'e', 'E']) {
        return Err(unsupported(
            "filter strings currently require an integer literal on the right",
        ));
    }
    let value = value.parse::<i64>().map_err(|_| {
        Status::invalid_argument("Connect filter integer literal is out of signed 64-bit range")
    })?;
    Ok((column.value, value))
}

fn is_simple_identifier(identifier: &str) -> bool {
    let mut bytes = identifier.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

pub(super) fn schema_to_proto(schema: &StructType) -> Result<proto::DataType, Status> {
    schema.validate_metadata().map_err(|error| {
        Status::invalid_argument(format!("invalid Connect schema metadata: {error}"))
    })?;
    validate_connect_scalar_schema(schema)?;
    validate_schema_width(schema)?;
    Ok(proto::DataType {
        kind: Some(DataTypeKind::Struct(data_type::Struct {
            fields: schema
                .logical_fields()
                .iter()
                .map(logical_field_to_proto)
                .collect::<Result<Vec<_>, _>>()?,
            type_variation_reference: 0,
        })),
    })
}

fn validate_connect_scalar_schema(schema: &StructType) -> Result<(), Status> {
    for field in schema.logical_fields() {
        let supported = matches!(
            &field.data_type,
            DataType::Boolean
                | DataType::Short
                | DataType::Integer
                | DataType::Long
                | DataType::Double
                | DataType::String
                | DataType::Date
                | DataType::Timestamp
        ) || matches!(&field.data_type, DataType::Decimal(p, s) if (1..=38).contains(p) && s <= p);
        if !supported {
            return Err(unsupported(format!(
                "Connect schema type {:?} is not enabled in the scalar Arrow subset",
                field.data_type
            )));
        }
        if field.name.is_empty() || field.name.len() > 128 {
            return Err(Status::invalid_argument(
                "Connect schema field names must contain 1-128 bytes",
            ));
        }
    }
    validate_schema_width(schema)
}

fn parse_local_schema(input: &str) -> Result<StructType, Status> {
    const MAX_JSON_SCHEMA_BYTES: usize = 256 * 1024;
    if input.len() > MAX_JSON_SCHEMA_BYTES {
        return Err(Status::resource_exhausted(format!(
            "Connect JSON schema exceeds {MAX_JSON_SCHEMA_BYTES} bytes"
        )));
    }
    let schema = if input.trim_start().starts_with('{') {
        validate_json_schema_depth(input)?;
        let value: serde_json::Value = serde_json::from_str(input).map_err(|error| {
            Status::invalid_argument(format!("invalid Connect JSON schema: {error}"))
        })?;
        let object = value
            .as_object()
            .ok_or_else(|| Status::invalid_argument("Connect JSON schema must be an object"))?;
        if object.get("type").and_then(serde_json::Value::as_str) != Some("struct") {
            return Err(unsupported(
                "Connect JSON schema must have top-level type 'struct'",
            ));
        }
        let fields = object
            .get("fields")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| Status::invalid_argument("Connect JSON schema fields are required"))?;
        if fields.is_empty() || fields.len() > super::MAX_CONNECT_SCHEMA_FIELDS {
            return Err(Status::resource_exhausted(format!(
                "Connect JSON schema must contain 1-{} fields",
                super::MAX_CONNECT_SCHEMA_FIELDS
            )));
        }
        let fields =
            fields
                .iter()
                .map(|field| {
                    let field = field.as_object().ok_or_else(|| {
                        Status::invalid_argument("Connect JSON schema fields must be objects")
                    })?;
                    let name = field
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            Status::invalid_argument("Connect JSON field name is required")
                        })?;
                    if name.is_empty() || name.len() > 128 {
                        return Err(Status::invalid_argument(
                            "Connect schema field names must contain 1-128 bytes",
                        ));
                    }
                    let type_name = field
                        .get("type")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| unsupported("nested Connect JSON types are not enabled"))?;
                    let data_type = parse_ddl_type(type_name)?;
                    let nullable = field
                        .get("nullable")
                        .and_then(serde_json::Value::as_bool)
                        .ok_or_else(|| {
                            Status::invalid_argument("Connect JSON nullable must be boolean")
                        })?;
                    let metadata = match field.get("metadata") {
                        None => BTreeMap::new(),
                        Some(serde_json::Value::Object(metadata)) => metadata
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect(),
                        _ => {
                            return Err(Status::invalid_argument(
                                "Connect JSON field metadata must be an object",
                            ));
                        }
                    };
                    Ok(LogicalField::new(name.to_string(), data_type, nullable)
                        .with_metadata(metadata))
                })
                .collect::<Result<Vec<_>, Status>>()?;
        StructType::from_logical_fields(fields)
    } else {
        parse_ddl_schema(input)?
    };
    schema.validate_metadata().map_err(|error| {
        Status::invalid_argument(format!("invalid Connect schema metadata: {error}"))
    })?;
    validate_connect_scalar_schema(&schema)?;
    Ok(schema)
}

fn validate_json_schema_depth(json: &str) -> Result<(), Status> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for byte in json.bytes() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > MAX_PLAN_DEPTH {
                    return Err(Status::resource_exhausted(format!(
                        "Connect JSON schema nesting exceeds {MAX_PLAN_DEPTH}"
                    )));
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}

fn local_relation_failure(error: EngineError) -> Status {
    let message = error.to_string();
    if message.contains("not enabled") || message.contains("unsupported") {
        Status::unimplemented(format!("Connect local relation is unsupported: {message}"))
    } else if message.contains("exceeds") {
        Status::resource_exhausted(message)
    } else {
        Status::invalid_argument(format!("invalid Connect local relation: {message}"))
    }
}

fn count_schema_fields(data_type: &DataType, count: &mut usize) -> Result<(), Status> {
    let fields = match data_type {
        DataType::Struct(fields) => Some(
            fields
                .iter()
                .cloned()
                .map(LogicalField::from)
                .collect::<Vec<_>>(),
        ),
        DataType::LogicalStruct(fields) => Some(fields.clone()),
        _ => None,
    };
    if let Some(fields) = fields {
        *count = count.saturating_add(fields.len());
        if *count > super::MAX_CONNECT_SCHEMA_FIELDS {
            return Err(Status::resource_exhausted(format!(
                "Connect schema exceeds {} fields",
                super::MAX_CONNECT_SCHEMA_FIELDS
            )));
        }
        for field in fields {
            count_schema_fields(&field.data_type, count)?;
        }
    }
    Ok(())
}

pub(super) fn parse_ddl_schema(ddl: &str) -> Result<StructType, Status> {
    if ddl.len() > 16 * 1024 {
        return Err(Status::resource_exhausted(
            "Connect DDL schema exceeds 16384 bytes",
        ));
    }
    let definitions = split_ddl_fields(ddl)?;
    if definitions.is_empty() || definitions.len() > super::MAX_CONNECT_SCHEMA_FIELDS {
        return Err(Status::invalid_argument(format!(
            "Connect DDL schema must contain 1-{} fields",
            super::MAX_CONNECT_SCHEMA_FIELDS
        )));
    }
    let fields = definitions
        .into_iter()
        .map(parse_ddl_field)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(StructType::from_logical_fields(fields))
}

fn split_ddl_fields(ddl: &str) -> Result<Vec<&str>, Status> {
    let mut definitions = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    let mut quote = false;
    for (index, character) in ddl.char_indices() {
        match character {
            '`' => quote = !quote,
            '(' if !quote => depth = depth.saturating_add(1),
            ')' if !quote => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    Status::invalid_argument("unbalanced parentheses in Connect DDL schema")
                })?;
            }
            ',' if !quote && depth == 0 => {
                definitions.push(ddl[start..index].trim());
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    if quote || depth != 0 {
        return Err(Status::invalid_argument(
            "unbalanced quoting or parentheses in Connect DDL schema",
        ));
    }
    definitions.push(ddl[start..].trim());
    if definitions.iter().any(|field| field.is_empty()) {
        return Err(Status::invalid_argument(
            "Connect DDL schema contains an empty field definition",
        ));
    }
    Ok(definitions)
}

fn parse_ddl_field(definition: &str) -> Result<LogicalField, Status> {
    let (name, type_and_nullability) = split_ddl_name(definition)?;
    if name.is_empty() || name.len() > 128 {
        return Err(Status::invalid_argument(
            "Connect DDL field names must contain 1-128 bytes",
        ));
    }
    if !definition.trim_start().starts_with('`') && !is_simple_identifier(&name) {
        return Err(Status::invalid_argument(
            "unquoted Connect DDL field names must be simple identifiers",
        ));
    }
    let (type_name, nullable) = strip_nullability(type_and_nullability);
    let data_type = parse_ddl_type(type_name.trim())?;
    Ok(LogicalField::new(name, data_type, nullable))
}

fn split_ddl_name(definition: &str) -> Result<(String, &str), Status> {
    let definition = definition.trim();
    if let Some(rest) = definition.strip_prefix('`') {
        let end = rest.find('`').ok_or_else(|| {
            Status::invalid_argument("unterminated quoted field name in Connect DDL schema")
        })?;
        let name = rest[..end].to_string();
        let tail = rest[end + 1..].trim_start();
        if tail.is_empty() {
            return Err(Status::invalid_argument(
                "Connect DDL field type is required",
            ));
        }
        return Ok((name, tail));
    }
    let end = definition
        .find(char::is_whitespace)
        .ok_or_else(|| Status::invalid_argument("Connect DDL field type is required"))?;
    Ok((
        definition[..end].to_string(),
        definition[end..].trim_start(),
    ))
}

fn strip_nullability(type_and_nullability: &str) -> (&str, bool) {
    let upper = type_and_nullability.to_ascii_uppercase();
    if let Some(index) = upper.rfind(" NOT NULL")
        && upper[index..].trim() == "NOT NULL"
    {
        return (type_and_nullability[..index].trim_end(), false);
    }
    if let Some(index) = upper.rfind(" NULL")
        && upper[index..].trim() == "NULL"
    {
        return (type_and_nullability[..index].trim_end(), true);
    }
    (type_and_nullability, true)
}

fn parse_ddl_type(type_name: &str) -> Result<DataType, Status> {
    let upper = type_name.to_ascii_uppercase();
    let scalar = match upper.as_str() {
        "BOOLEAN" | "BOOL" => Some(DataType::Boolean),
        "TINYINT" | "BYTE" => Some(DataType::Byte),
        "SMALLINT" | "SHORT" => Some(DataType::Short),
        "INT" | "INTEGER" => Some(DataType::Integer),
        "BIGINT" | "LONG" => Some(DataType::Long),
        "FLOAT" | "REAL" => Some(DataType::Float),
        "DOUBLE" => Some(DataType::Double),
        "STRING" => Some(DataType::String),
        "DATE" => Some(DataType::Date),
        "TIMESTAMP" | "TIMESTAMP_LTZ" => Some(DataType::Timestamp),
        "TIMESTAMP_NTZ" => Some(DataType::TimestampNtz),
        _ => None,
    };
    if let Some(scalar) = scalar {
        return Ok(scalar);
    }
    if upper.starts_with("DECIMAL(") && upper.ends_with(')') {
        let body = &upper[8..upper.len() - 1];
        let (precision, scale) = body.split_once(',').ok_or_else(|| {
            Status::invalid_argument("DECIMAL requires precision and scale in Connect DDL")
        })?;
        if scale.contains(',') {
            return Err(Status::invalid_argument(
                "DECIMAL accepts only precision and scale in Connect DDL",
            ));
        }
        let precision = precision
            .trim()
            .parse::<u8>()
            .map_err(|_| Status::invalid_argument("DECIMAL precision must be between 1 and 38"))?;
        let scale = scale
            .trim()
            .parse::<u8>()
            .map_err(|_| Status::invalid_argument("DECIMAL scale is invalid"))?;
        if precision == 0 || precision > 38 || scale > precision {
            return Err(Status::invalid_argument(
                "DECIMAL precision must be 1-38 and scale cannot exceed precision",
            ));
        }
        return Ok(DataType::Decimal(precision, scale));
    }
    Err(unsupported(format!(
        "Connect DDL type {type_name:?} is not enabled in the scalar schema subset"
    )))
}

fn logical_field_to_proto(field: &LogicalField) -> Result<data_type::StructField, Status> {
    let metadata = if field.metadata.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&field.metadata).map_err(|error| {
            Status::invalid_argument(format!("invalid Connect field metadata: {error}"))
        })?)
    };
    Ok(data_type::StructField {
        name: field.name.clone(),
        data_type: Some(data_type_to_proto(&field.data_type)?),
        nullable: field.nullable,
        metadata,
    })
}

fn data_type_to_proto(data_type: &DataType) -> Result<proto::DataType, Status> {
    let kind = match data_type {
        DataType::Boolean => DataTypeKind::Boolean(data_type::Boolean {
            type_variation_reference: 0,
        }),
        DataType::Byte => DataTypeKind::Byte(data_type::Byte {
            type_variation_reference: 0,
        }),
        DataType::Short => DataTypeKind::Short(data_type::Short {
            type_variation_reference: 0,
        }),
        DataType::Integer => DataTypeKind::Integer(data_type::Integer {
            type_variation_reference: 0,
        }),
        DataType::Long => DataTypeKind::Long(data_type::Long {
            type_variation_reference: 0,
        }),
        DataType::Float => DataTypeKind::Float(data_type::Float {
            type_variation_reference: 0,
        }),
        DataType::Double => DataTypeKind::Double(data_type::Double {
            type_variation_reference: 0,
        }),
        DataType::String => DataTypeKind::String(data_type::String {
            type_variation_reference: 0,
            collation: String::new(),
        }),
        DataType::Date => DataTypeKind::Date(data_type::Date {
            type_variation_reference: 0,
        }),
        DataType::Timestamp => DataTypeKind::Timestamp(data_type::Timestamp {
            type_variation_reference: 0,
        }),
        DataType::TimestampNtz => DataTypeKind::TimestampNtz(data_type::TimestampNtz {
            type_variation_reference: 0,
        }),
        DataType::Decimal(precision, scale) => DataTypeKind::Decimal(data_type::Decimal {
            scale: Some(i32::from(*scale)),
            precision: Some(i32::from(*precision)),
            type_variation_reference: 0,
        }),
        DataType::Struct(fields) => DataTypeKind::Struct(data_type::Struct {
            fields: fields
                .iter()
                .map(|field| logical_field_to_proto(&LogicalField::from(field.clone())))
                .collect::<Result<Vec<_>, _>>()?,
            type_variation_reference: 0,
        }),
        DataType::LogicalStruct(fields) => DataTypeKind::Struct(data_type::Struct {
            fields: fields
                .iter()
                .map(logical_field_to_proto)
                .collect::<Result<Vec<_>, _>>()?,
            type_variation_reference: 0,
        }),
        _ => {
            return Err(unsupported(format!(
                "Connect schema type {data_type:?} is not enabled in the 4.19 schema subset"
            )));
        }
    };
    Ok(proto::DataType { kind: Some(kind) })
}

fn engine_failure(error: EngineError) -> Status {
    let code = match error.category() {
        robin_sparkless_core::ErrorCategory::Cancelled => Code::Cancelled,
        robin_sparkless_core::ErrorCategory::InvalidInput
        | robin_sparkless_core::ErrorCategory::Capability => Code::InvalidArgument,
        robin_sparkless_core::ErrorCategory::NotFound => Code::NotFound,
        robin_sparkless_core::ErrorCategory::Conflict => Code::Aborted,
        robin_sparkless_core::ErrorCategory::Provider | robin_sparkless_core::ErrorCategory::Io => {
            Code::Unavailable
        }
        _ => Code::FailedPrecondition,
    };
    Status::new(
        code,
        format!(
            "SPARKLESS_ERROR[{}]: CONNECT_PLAN_ANALYSIS_FAILED: {error}",
            error.code()
        ),
    )
}

fn unsupported(message: impl Into<String>) -> Status {
    Status::unimplemented(format!("UNSUPPORTED_CONNECT_PLAN: {}", message.into()))
}

fn is_qualified_identifier(identifier: &str) -> bool {
    !identifier.is_empty()
        && identifier.split('.').all(|part| {
            !part.is_empty()
                && part.chars().enumerate().all(|(index, ch)| {
                    ch == '_' || ch.is_ascii_alphanumeric() && (index > 0 || !ch.is_ascii_digit())
                })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use proto::expression;

    fn session() -> SparkSession {
        SparkSession::builder()
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.caseSensitive", "false")
            .get_or_create()
    }

    fn connect_expression(expr_type: ExprType) -> proto::Expression {
        proto::Expression {
            common: None,
            expr_type: Some(expr_type),
        }
    }

    fn connect_attribute(name: &str, plan_id: Option<i64>) -> proto::Expression {
        connect_expression(ExprType::UnresolvedAttribute(
            expression::UnresolvedAttribute {
                unparsed_identifier: name.into(),
                plan_id,
                is_metadata_column: Some(false),
            },
        ))
    }

    #[allow(deprecated)]
    fn named_table_relation(name: &str, plan_id: Option<i64>) -> proto::Relation {
        proto::Relation {
            common: plan_id.map(|plan_id| proto::RelationCommon {
                source_info: String::new(),
                plan_id: Some(plan_id),
                origin: None,
            }),
            rel_type: Some(RelType::Read(proto::Read {
                read_type: Some(proto::read::ReadType::NamedTable(proto::read::NamedTable {
                    unparsed_identifier: name.into(),
                    options: Default::default(),
                })),
                is_streaming: false,
            })),
        }
    }

    #[allow(deprecated)]
    fn alias_relation(input: proto::Relation, alias: &str, plan_id: i64) -> proto::Relation {
        proto::Relation {
            common: Some(proto::RelationCommon {
                source_info: String::new(),
                plan_id: Some(plan_id),
                origin: None,
            }),
            rel_type: Some(RelType::SubqueryAlias(Box::new(proto::SubqueryAlias {
                input: Some(Box::new(input)),
                alias: alias.into(),
                qualifier: Vec::new(),
            }))),
        }
    }

    fn local_relation(schema: &str, plan_id: i64) -> proto::Relation {
        proto::Relation {
            common: Some(proto::RelationCommon {
                plan_id: Some(plan_id),
                ..Default::default()
            }),
            rel_type: Some(RelType::LocalRelation(proto::LocalRelation {
                data: None,
                schema: Some(schema.to_string()),
            })),
        }
    }

    #[test]
    fn inline_relation_resolves_only_public_names_and_keeps_bound_columns() {
        let session = session();
        let filter = proto::Relation {
            rel_type: Some(RelType::Filter(Box::new(proto::Filter {
                input: Some(Box::new(local_relation("id INT", 101))),
                condition: Some(connect_expression(ExprType::ExpressionString(
                    expression::ExpressionString {
                        expression: "__sparkless_connect_local_0 > 0".into(),
                    },
                ))),
            }))),
            ..Default::default()
        };
        let error = translate_relation(&session, &filter, 1).err().unwrap();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(error.message().contains("unresolved column"));

        let projection = proto::Relation {
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(local_relation(
                    "id INT, __sparkless_connect_local_0 STRING",
                    101,
                ))),
                expressions: vec![
                    connect_attribute("id", Some(101)),
                    connect_attribute("__sparkless_connect_local_0", None),
                ],
            }))),
            ..Default::default()
        };
        let translated = translate_relation(&session, &projection, 1).unwrap();
        let fields = translated.schema.logical_fields();
        assert!(matches!(fields[0].data_type, DataType::Integer));
        assert!(matches!(fields[1].data_type, DataType::String));
    }

    #[test]
    fn aliased_inline_relations_keep_bound_columns_through_join_projection() {
        let session = session();
        let condition = connect_expression(ExprType::UnresolvedFunction(
            expression::UnresolvedFunction {
                function_name: "==".into(),
                arguments: vec![
                    connect_attribute("l.id", Some(111)),
                    connect_attribute("r.id", Some(222)),
                ],
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: Some(false),
            },
        ));
        let join = proto::Relation {
            common: None,
            rel_type: Some(RelType::Join(Box::new(proto::Join {
                left: Some(Box::new(alias_relation(
                    local_relation("id INT, amount INT", 101),
                    "l",
                    111,
                ))),
                right: Some(Box::new(alias_relation(
                    local_relation("id INT, label STRING", 202),
                    "r",
                    222,
                ))),
                join_condition: Some(condition),
                join_type: proto::join::JoinType::Inner as i32,
                using_columns: Vec::new(),
                join_data_type: None,
            }))),
        };
        let projection = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(join)),
                expressions: vec![
                    connect_expression(ExprType::Alias(Box::new(expression::Alias {
                        expr: Some(Box::new(connect_attribute("l.id", Some(111)))),
                        name: vec!["key".into()],
                        metadata: Some("{}".into()),
                    }))),
                    connect_expression(ExprType::Alias(Box::new(expression::Alias {
                        expr: Some(Box::new(connect_attribute("r.label", Some(222)))),
                        name: vec!["label".into()],
                        metadata: Some("{}".into()),
                    }))),
                ],
            }))),
        };

        let translated = translate_relation(&session, &projection, 1).unwrap();
        assert_eq!(
            translated
                .schema
                .logical_fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["key", "label"]
        );
        assert!(translated.dataframe.collect_rows().unwrap().is_empty());
    }

    #[test]
    fn configured_named_table_relations_resolve_registered_tables_and_reject_options() {
        let session = session();
        let table = session.range(0, 3, 1).unwrap();
        session.register_table("source_table", table);
        let relation = proto::Relation {
            common: None,
            rel_type: Some(RelType::Read(proto::Read {
                read_type: Some(proto::read::ReadType::NamedTable(proto::read::NamedTable {
                    unparsed_identifier: "source_table".into(),
                    options: Default::default(),
                })),
                is_streaming: false,
            })),
        };
        let translated = translate_relation(&session, &relation, 1).unwrap();
        assert_eq!(translated.schema.logical_fields()[0].name, "id");

        let mut with_options = relation.clone();
        let Some(RelType::Read(read)) = with_options.rel_type.as_mut() else {
            unreachable!();
        };
        let Some(proto::read::ReadType::NamedTable(named)) = read.read_type.as_mut() else {
            unreachable!();
        };
        named.options.insert("path".into(), "untrusted".into());
        let error = match translate_relation(&session, &with_options, 1) {
            Err(error) => error,
            Ok(_) => panic!("named table with source options must be rejected"),
        };
        assert_eq!(error.code(), Code::Unimplemented);
    }

    #[test]
    fn named_table_identifiers_reject_unquoted_sql_fragments() {
        assert!(is_qualified_identifier("catalog.schema.table_2"));
        assert!(!is_qualified_identifier("catalog..table"));
        assert!(!is_qualified_identifier("table; DROP TABLE x"));
        assert!(!is_qualified_identifier("2starts_with_digit"));
    }

    #[test]
    fn using_join_key_case_preserves_right_values_through_connect() {
        let session = session();
        session.register_table(
            "left_items",
            session
                .create_dataframe(
                    vec![(1, 10, "left".into()), (2, 20, "left-only".into())],
                    vec!["id", "value", "label"],
                )
                .unwrap(),
        );
        session.register_table(
            "right_items",
            session
                .create_dataframe(
                    vec![(1, 100, "right".into()), (3, 300, "right-only".into())],
                    vec!["ID", "value", "tag"],
                )
                .unwrap(),
        );
        for join_type in [
            proto::join::JoinType::Inner,
            proto::join::JoinType::LeftOuter,
            proto::join::JoinType::RightOuter,
        ] {
            let relation = proto::Relation {
                common: None,
                rel_type: Some(RelType::Join(Box::new(proto::Join {
                    left: Some(Box::new(named_table_relation("left_items", None))),
                    right: Some(Box::new(named_table_relation("right_items", None))),
                    join_condition: None,
                    using_columns: vec!["id".into()],
                    join_type: join_type as i32,
                    join_data_type: None,
                }))),
            };
            let translated = translate_relation(&session, &relation, 1).unwrap();
            assert_eq!(translated.schema.logical_fields().len(), 5);
            let rows = translated.dataframe.collect_rows().unwrap();
            assert_eq!(
                rows.len(),
                if join_type == proto::join::JoinType::Inner {
                    1
                } else {
                    2
                }
            );
            for row in rows {
                let id = row["id"].as_i64().unwrap();
                assert_eq!(
                    row["value_right"],
                    match id {
                        1 => serde_json::json!(100),
                        2 => serde_json::Value::Null,
                        3 => serde_json::json!(300),
                        other => panic!("unexpected join key {other}"),
                    },
                    "{join_type:?}"
                );
            }
        }
    }

    #[test]
    fn using_join_merges_reordered_inline_keys_and_preserves_right_bindings() {
        use serde_json::json;

        let session = session();
        let inline = |fields: Vec<(String, String)>, rows, id| {
            let frame = session
                .create_dataframe_from_rows(rows, fields, false, false)
                .unwrap();
            let data = frame
                .to_arrow_ipc_batches_with_cancellation(10, 65536, &CancellationToken::new())
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .bytes;
            proto::Relation {
                common: Some(proto::RelationCommon {
                    plan_id: Some(id),
                    ..Default::default()
                }),
                rel_type: Some(RelType::LocalRelation(proto::LocalRelation {
                    data: Some(data),
                    schema: None,
                })),
            }
        };
        let left = inline(
            vec![("amount".into(), "int".into()), ("id".into(), "int".into())],
            vec![vec![json!(10), json!(1)], vec![json!(20), json!(2)]],
            101,
        );
        let right = inline(
            vec![("id".into(), "int".into()), ("score".into(), "int".into())],
            vec![vec![json!(1), json!(100)], vec![json!(3), json!(300)]],
            202,
        );
        for (join_type, expected) in [
            (
                proto::join::JoinType::Inner,
                vec![vec![json!(1), json!(10), json!(100)]],
            ),
            (
                proto::join::JoinType::FullOuter,
                vec![
                    vec![json!(1), json!(10), json!(100)],
                    vec![json!(2), json!(20), json!(null)],
                    vec![json!(3), json!(null), json!(300)],
                ],
            ),
        ] {
            let join = proto::Relation {
                common: None,
                rel_type: Some(RelType::Join(Box::new(proto::Join {
                    left: Some(Box::new(left.clone())),
                    right: Some(Box::new(right.clone())),
                    using_columns: vec!["id".into()],
                    join_type: join_type as i32,
                    ..Default::default()
                }))),
            };
            let translated = translate_relation(&session, &join, 1).unwrap();
            let names = translated
                .schema
                .logical_fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>();
            assert_eq!(names, ["id", "amount", "score"]);
            let physical = translated.dataframe.columns_engine().unwrap();
            let mut rows = translated
                .dataframe
                .collect_rows()
                .unwrap()
                .into_iter()
                .map(|row| {
                    physical
                        .iter()
                        .map(|name| row[name].clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(|row| row[0].as_i64().unwrap());
            assert_eq!(rows, expected, "{join_type:?}");

            let projection = proto::Relation {
                common: None,
                rel_type: Some(RelType::Project(Box::new(proto::Project {
                    input: Some(Box::new(join)),
                    expressions: vec![connect_attribute("score", Some(202))],
                }))),
            };
            let bound = translate_relation(&session, &projection, 1).unwrap();
            let name = &bound.dataframe.columns_engine().unwrap()[0];
            let rows = bound.dataframe.collect_rows().unwrap();
            assert_eq!(rows.len(), expected.len());
            assert!(rows.iter().any(|row| row[name] == json!(100)));
        }

        let swapped = inline(
            vec![("id".into(), "int".into()), ("amount".into(), "int".into())],
            vec![vec![json!(1), json!(10)], vec![json!(1), json!(20)]],
            303,
        );
        let join = proto::Relation {
            common: None,
            rel_type: Some(RelType::Join(Box::new(proto::Join {
                left: Some(Box::new(left)),
                right: Some(Box::new(swapped)),
                using_columns: vec!["id".into(), "amount".into()],
                join_type: proto::join::JoinType::Inner as i32,
                ..Default::default()
            }))),
        };
        let translated = translate_relation(&session, &join, 1).unwrap();
        let names = translated
            .schema
            .logical_fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["id", "amount"]);
        let physical = translated.dataframe.columns_engine().unwrap();
        let rows = translated.dataframe.collect_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][&physical[0]], json!(1));
        assert_eq!(rows[0][&physical[1]], json!(10));
    }

    #[test]
    fn duckdb_join_ownership_survives_wrappers_and_rejects_mixed_sources_before_scan() {
        use robin_sparkless_core::engine::{
            CatalogCapabilities, CatalogOwnership, CatalogResolver, ExternalCatalog,
            ExternalTableDescriptor, ExternalTableFormat,
        };

        struct MetadataOnlyCatalog;
        impl CatalogResolver for MetadataOnlyCatalog {
            fn resolve_ownership(&self, name: &str) -> Result<CatalogOwnership, EngineError> {
                let owner = match name {
                    "source.items" | "source.second" => "owner-one",
                    "other.items" => "owner-two",
                    _ => return Ok(CatalogOwnership::Local),
                };
                Ok(CatalogOwnership::external(
                    owner,
                    CatalogCapabilities::READ_ONLY,
                ))
            }

            fn resolve_table(
                &self,
                name: &str,
            ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
                let owner = match name {
                    "source.items" | "source.second" => "owner-one",
                    "other.items" => "owner-two",
                    _ => return Ok(None),
                };
                Ok(Some(ExternalTableDescriptor {
                    table_id: name.into(),
                    canonical_location: owner.into(),
                    schema: StructType::from_logical_fields(vec![LogicalField::new(
                        "id".into(),
                        DataType::Long,
                        false,
                    )]),
                    version: None,
                    provider: "test-duckdb".into(),
                    permissions_context: None,
                    format: ExternalTableFormat::DuckDb,
                }))
            }
        }
        let session = session();
        let resolver: Arc<dyn CatalogResolver> = Arc::new(MetadataOnlyCatalog);
        session
            .set_external_catalog(Some(Arc::new(ExternalCatalog::new(resolver, None, None))))
            .unwrap();
        session.register_table("managed", session.range(0, 3, 1).unwrap());

        let wrapped = proto::Relation {
            common: None,
            rel_type: Some(RelType::Limit(Box::new(proto::Limit {
                input: Some(Box::new(proto::Relation {
                    common: None,
                    rel_type: Some(RelType::Project(Box::new(proto::Project {
                        input: Some(Box::new(alias_relation(
                            named_table_relation("source.items", None),
                            "s",
                            101,
                        ))),
                        expressions: vec![connect_attribute("id", None)],
                    }))),
                })),
                limit: 1,
            }))),
        };
        let join = |right: &str| proto::Plan {
            op_type: Some(PlanOp::Root(proto::Relation {
                common: None,
                rel_type: Some(RelType::Join(Box::new(proto::Join {
                    left: Some(Box::new(wrapped.clone())),
                    right: Some(Box::new(named_table_relation(right, None))),
                    join_condition: None,
                    using_columns: vec!["id".into()],
                    join_type: proto::join::JoinType::Inner as i32,
                    join_data_type: None,
                }))),
            })),
        };
        // Schema-only analysis needs no source row reader, and permits one owner.
        assert!(translate_plan(&session, &join("source.second")).is_ok());
        for right in ["managed", "other.items"] {
            let plan = join(right);
            let analysis_error = translate_plan(&session, &plan).err().unwrap();
            assert_eq!(analysis_error.code(), Code::Unimplemented);
            let execution_error =
                translate_proto_plan_for_execution(&session, &plan, &CancellationToken::new())
                    .err()
                    .unwrap();
            assert_eq!(execution_error.code(), Code::Unimplemented);
            assert!(execution_error.message().contains("same source owner"));
        }

        // A local table that shadows an external name must keep its local identity.
        session.register_table("source.items", session.range(0, 3, 1).unwrap());
        assert!(
            translate_proto_plan_for_execution(
                &session,
                &join("managed"),
                &CancellationToken::new(),
            )
            .is_ok()
        );
    }

    #[test]
    fn configured_named_table_join_translates_and_collects_typed_rows() {
        let session = session();
        let left = session
            .create_dataframe(
                vec![
                    (1, 10, "left".to_string()),
                    (2, 20, "unmatched".to_string()),
                ],
                vec!["id", "score", "label"],
            )
            .unwrap();
        let right = session
            .create_dataframe(
                vec![(1, 100, "right".to_string())],
                vec!["other_id", "amount", "label"],
            )
            .unwrap();
        session.register_table("left_source", left);
        session.register_table("right_source", right);
        let condition = connect_expression(ExprType::UnresolvedFunction(
            expression::UnresolvedFunction {
                function_name: "==".into(),
                arguments: vec![
                    connect_attribute("l.id", Some(111)),
                    connect_attribute("r.other_id", Some(222)),
                ],
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: Some(false),
            },
        ));
        let relation = proto::Relation {
            common: None,
            rel_type: Some(RelType::Join(Box::new(proto::Join {
                left: Some(Box::new(alias_relation(
                    named_table_relation("left_source", Some(101)),
                    "l",
                    111,
                ))),
                right: Some(Box::new(alias_relation(
                    named_table_relation("right_source", Some(202)),
                    "r",
                    222,
                ))),
                join_condition: Some(condition),
                join_type: proto::join::JoinType::Inner as i32,
                using_columns: Vec::new(),
                join_data_type: None,
            }))),
        };

        let translated = translate_relation(&session, &relation, 1).unwrap();
        assert_eq!(translated.schema.logical_fields().len(), 6);
        let rows = translated.dataframe.collect_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], serde_json::json!(1));
        assert_eq!(rows[0]["other_id"], serde_json::json!(1));
        assert_eq!(rows[0]["amount"], serde_json::json!(100));

        let right_label = connect_expression(ExprType::UnresolvedAttribute(
            expression::UnresolvedAttribute {
                unparsed_identifier: "r.label".into(),
                plan_id: Some(222),
                is_metadata_column: Some(false),
            },
        ));
        let projection = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(relation)),
                expressions: vec![connect_expression(ExprType::Alias(Box::new(
                    expression::Alias {
                        expr: Some(Box::new(right_label)),
                        name: vec!["right_label".into()],
                        metadata: Some("{}".into()),
                    },
                )))],
            }))),
        };
        let projected = translate_relation(&session, &projection, 1).unwrap();
        assert_eq!(projected.schema.logical_fields()[0].name, "right_label");
        let rows = projected.dataframe.collect_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["right_label"], serde_json::json!("right"));
    }

    #[test]
    fn grouped_connect_count_and_sum_preserve_schema_and_values() {
        let session = session();
        let table = session
            .create_dataframe(
                vec![
                    (1, 10, "a".to_string()),
                    (1, 20, "b".to_string()),
                    (2, 30, "c".to_string()),
                ],
                vec!["bucket", "amount", "label"],
            )
            .unwrap();
        session.register_table("aggregate_source", table);
        let sum = connect_expression(ExprType::UnresolvedFunction(
            expression::UnresolvedFunction {
                function_name: "sum".into(),
                arguments: vec![connect_attribute("amount", None)],
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: Some(false),
            },
        ));
        let sum = connect_expression(ExprType::Alias(Box::new(expression::Alias {
            expr: Some(Box::new(sum)),
            name: vec!["total".into()],
            metadata: Some("{}".into()),
        })));
        let count = connect_expression(ExprType::UnresolvedFunction(
            expression::UnresolvedFunction {
                function_name: "count".into(),
                arguments: vec![connect_expression(ExprType::Literal(expression::Literal {
                    literal_type: Some(LiteralType::Integer(1)),
                }))],
                is_distinct: false,
                is_user_defined_function: false,
                is_internal: Some(false),
            },
        ));
        let relation = proto::Relation {
            common: None,
            rel_type: Some(RelType::Aggregate(Box::new(proto::Aggregate {
                input: Some(Box::new(named_table_relation("aggregate_source", None))),
                group_type: proto::aggregate::GroupType::Groupby as i32,
                grouping_expressions: vec![connect_attribute("bucket", None)],
                aggregate_expressions: vec![sum, count],
                pivot: None,
                grouping_sets: Vec::new(),
            }))),
        };

        let translated = translate_relation(&session, &relation, 1).unwrap();
        let names = translated
            .schema
            .logical_fields()
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["bucket", "total", "count(1)"]);
        let fields = translated.schema.logical_fields();
        assert!(matches!(fields[1].data_type, DataType::Long));
        assert!(fields[1].nullable);
        assert!(matches!(fields[2].data_type, DataType::Long));
        assert!(!fields[2].nullable);
        let rows = translated.dataframe.collect_rows().unwrap();
        assert_eq!(rows.len(), 2);
        let one = rows
            .iter()
            .find(|row| row["bucket"] == serde_json::json!(1))
            .unwrap();
        assert_eq!(one["total"], serde_json::json!(30));
        assert_eq!(one["count(1)"], serde_json::json!(2));
    }

    fn decode_plan(bytes: &[u8]) -> proto::ExecutePlanRequest {
        proto::ExecutePlanRequest::decode(bytes).expect("captured request decodes")
    }

    #[test]
    fn captured_public_plans_translate_to_native_schemas_without_collecting_rows() {
        let session = session();
        let filter_count = decode_plan(include_bytes!(
            "../../../../tests/connect/fixtures/pyspark-client-4.0.5/execute-range-filter-count.pb"
        ));
        let filtered_count = translate_plan(&session, filter_count.plan.as_ref().unwrap()).unwrap();
        let count_schema = filtered_count.dataframe.schema_engine().unwrap();
        assert_eq!(count_schema.logical_fields()[0].name, "count(1)");
        assert!(matches!(
            count_schema.logical_fields()[0].data_type,
            DataType::Long
        ));

        let projection = decode_plan(include_bytes!(
            "../../../../tests/connect/fixtures/pyspark-client-4.0.5/execute-column-expressions.pb"
        ));
        let projected = translate_plan(&session, projection.plan.as_ref().unwrap()).unwrap();
        let schema = projected.dataframe.schema_engine().unwrap();
        assert_eq!(schema.logical_fields()[0].name, "next");
        assert!(matches!(
            schema.logical_fields()[0].data_type,
            DataType::Long
        ));

        let limit = decode_plan(include_bytes!(
            "../../../../tests/connect/fixtures/pyspark-client-4.0.5/execute-range-select-limit.pb"
        ));
        let limited = translate_plan(&session, limit.plan.as_ref().unwrap()).unwrap();
        assert_eq!(
            limited.dataframe.schema_engine().unwrap().logical_fields()[0].name,
            "id"
        );
    }

    #[test]
    fn global_count_schema_is_available_for_an_empty_range_without_row_execution() {
        let session = session();
        let count_plan = proto::Plan {
            op_type: Some(PlanOp::Root(proto::Relation {
                common: None,
                rel_type: Some(RelType::Aggregate(Box::new(proto::Aggregate {
                    input: Some(Box::new(proto::Relation {
                        common: None,
                        rel_type: Some(RelType::Range(proto::Range {
                            start: Some(4),
                            end: 4,
                            step: 1,
                            num_partitions: Some(1),
                        })),
                    })),
                    group_type: proto::aggregate::GroupType::Groupby as i32,
                    grouping_expressions: Vec::new(),
                    aggregate_expressions: vec![proto::Expression {
                        common: None,
                        expr_type: Some(ExprType::UnresolvedFunction(
                            expression::UnresolvedFunction {
                                function_name: "count".into(),
                                arguments: vec![proto::Expression {
                                    common: None,
                                    expr_type: Some(ExprType::Literal(expression::Literal {
                                        literal_type: Some(LiteralType::Integer(1)),
                                    })),
                                }],
                                is_distinct: false,
                                is_user_defined_function: false,
                                is_internal: None,
                            },
                        )),
                    }],
                    pivot: None,
                    grouping_sets: Vec::new(),
                }))),
            })),
        };
        let translated = translate_plan(&session, &count_plan).unwrap();
        let schema = translated.dataframe.schema_engine().unwrap();
        assert_eq!(schema.logical_fields()[0].name, "count(1)");
        assert!(matches!(
            schema.logical_fields()[0].data_type,
            DataType::Long
        ));
        let rows = translated.dataframe.collect_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["count(1)"], serde_json::json!(0));
    }

    #[test]
    fn global_count_returns_one_row_for_nonempty_input() {
        let request = decode_plan(include_bytes!(
            "../../../../tests/connect/fixtures/pyspark-client-4.0.5/execute-range-filter-count.pb"
        ));
        let translated = translate_plan(&session(), request.plan.as_ref().unwrap()).unwrap();
        let rows = translated.dataframe.collect_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["count(1)"], serde_json::json!(4));
    }

    #[test]
    fn duplicate_public_projection_labels_resolve_as_logical_fields() {
        let alias = |name: &str| proto::Expression {
            common: None,
            expr_type: Some(ExprType::Alias(Box::new(expression::Alias {
                expr: Some(Box::new(proto::Expression {
                    common: None,
                    expr_type: Some(ExprType::UnresolvedAttribute(
                        expression::UnresolvedAttribute {
                            unparsed_identifier: "id".into(),
                            ..Default::default()
                        },
                    )),
                })),
                name: vec![name.into()],
                metadata: None,
            }))),
        };
        let duplicate = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(proto::Relation {
                    common: None,
                    rel_type: Some(RelType::Range(proto::Range {
                        end: 10,
                        step: 1,
                        ..Default::default()
                    })),
                })),
                expressions: vec![alias("x"), alias("x")],
            }))),
        };
        let outer = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(duplicate.clone())),
                expressions: vec![proto::Expression {
                    common: None,
                    expr_type: Some(ExprType::UnresolvedAttribute(
                        expression::UnresolvedAttribute {
                            unparsed_identifier: "x_1".into(),
                            ..Default::default()
                        },
                    )),
                }],
            }))),
        };
        assert_eq!(
            translate_plan(
                &session(),
                &proto::Plan {
                    op_type: Some(PlanOp::Root(outer)),
                }
            )
            .err()
            .unwrap()
            .code(),
            Code::InvalidArgument
        );
        let ambiguous_filter = proto::Relation {
            common: None,
            rel_type: Some(RelType::Filter(Box::new(proto::Filter {
                input: Some(Box::new(duplicate)),
                condition: Some(proto::Expression {
                    common: None,
                    expr_type: Some(ExprType::ExpressionString(expression::ExpressionString {
                        expression: "x > 5".into(),
                    })),
                }),
            }))),
        };
        assert_eq!(
            translate_plan(
                &session(),
                &proto::Plan {
                    op_type: Some(PlanOp::Root(ambiguous_filter)),
                }
            )
            .err()
            .unwrap()
            .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn plain_duplicate_columns_keep_public_labels_and_reject_ambiguous_references() {
        let column = |name: &str| proto::Expression {
            common: None,
            expr_type: Some(ExprType::UnresolvedAttribute(
                expression::UnresolvedAttribute {
                    unparsed_identifier: name.into(),
                    ..Default::default()
                },
            )),
        };
        let duplicate = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(proto::Relation {
                    common: None,
                    rel_type: Some(RelType::Range(proto::Range {
                        end: 10,
                        step: 1,
                        ..Default::default()
                    })),
                })),
                expressions: vec![column("id"), column("id")],
            }))),
        };
        let translated = translate_plan(
            &session(),
            &proto::Plan {
                op_type: Some(PlanOp::Root(duplicate.clone())),
            },
        )
        .unwrap();
        assert_eq!(
            translated
                .schema
                .logical_fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "id"]
        );

        let leaked_suffix = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(duplicate.clone())),
                expressions: vec![column("id_1")],
            }))),
        };
        assert_eq!(
            translate_plan(
                &session(),
                &proto::Plan {
                    op_type: Some(PlanOp::Root(leaked_suffix)),
                }
            )
            .err()
            .unwrap()
            .code(),
            Code::InvalidArgument
        );

        let ambiguous_filter = proto::Relation {
            common: None,
            rel_type: Some(RelType::Filter(Box::new(proto::Filter {
                input: Some(Box::new(duplicate)),
                condition: Some(proto::Expression {
                    common: None,
                    expr_type: Some(ExprType::ExpressionString(expression::ExpressionString {
                        expression: "id > 5".into(),
                    })),
                }),
            }))),
        };
        assert_eq!(
            translate_plan(
                &session(),
                &proto::Plan {
                    op_type: Some(PlanOp::Root(ambiguous_filter)),
                }
            )
            .err()
            .unwrap()
            .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn projection_aliases_may_match_generated_duplicate_suffixes() {
        let alias = |name: &str| proto::Expression {
            common: None,
            expr_type: Some(ExprType::Alias(Box::new(expression::Alias {
                expr: Some(Box::new(proto::Expression {
                    common: None,
                    expr_type: Some(ExprType::UnresolvedAttribute(
                        expression::UnresolvedAttribute {
                            unparsed_identifier: "id".into(),
                            ..Default::default()
                        },
                    )),
                })),
                name: vec![name.into()],
                metadata: None,
            }))),
        };
        let relation = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(proto::Relation {
                    common: None,
                    rel_type: Some(RelType::Range(proto::Range {
                        end: 10,
                        step: 1,
                        ..Default::default()
                    })),
                })),
                expressions: vec![alias("x"), alias("x"), alias("x_1")],
            }))),
        };
        let translated = translate_plan(
            &session(),
            &proto::Plan {
                op_type: Some(PlanOp::Root(relation)),
            },
        )
        .unwrap();
        assert_eq!(
            translated
                .schema
                .logical_fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["x", "x", "x_1"]
        );
    }

    #[test]
    fn case_insensitive_physical_suffix_does_not_shadow_an_explicit_alias() {
        let attribute = |name: &str| proto::Expression {
            common: None,
            expr_type: Some(ExprType::UnresolvedAttribute(
                expression::UnresolvedAttribute {
                    unparsed_identifier: name.into(),
                    ..Default::default()
                },
            )),
        };
        let alias = |expr: proto::Expression, name: &str| proto::Expression {
            common: None,
            expr_type: Some(ExprType::Alias(Box::new(expression::Alias {
                expr: Some(Box::new(expr)),
                name: vec![name.into()],
                metadata: None,
            }))),
        };
        let add = proto::Expression {
            common: None,
            expr_type: Some(ExprType::UnresolvedFunction(
                expression::UnresolvedFunction {
                    function_name: "+".into(),
                    arguments: vec![attribute("id"), attribute("id")],
                    is_distinct: false,
                    is_user_defined_function: false,
                    is_internal: None,
                },
            )),
        };
        let source = proto::Relation {
            common: None,
            rel_type: Some(RelType::Range(proto::Range {
                end: 10,
                step: 1,
                ..Default::default()
            })),
        };
        let duplicate = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(source)),
                expressions: vec![
                    alias(attribute("id"), "x"),
                    alias(attribute("id"), "x"),
                    alias(add, "X_1"),
                ],
            }))),
        };
        let outer = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(duplicate)),
                expressions: vec![attribute("X_1")],
            }))),
        };
        let translated = translate_plan(
            &session(),
            &proto::Plan {
                op_type: Some(PlanOp::Root(outer)),
            },
        )
        .unwrap();
        let rows = translated.dataframe.collect_rows().unwrap();
        assert_eq!(rows.len(), 10);
        assert_eq!(translated.schema.logical_fields()[0].name, "X_1");
        assert_eq!(rows[0].values().next(), Some(&serde_json::json!(0)));
        assert_eq!(rows[9].values().next(), Some(&serde_json::json!(18)));
    }

    #[test]
    fn scalar_projection_alias_matching_input_column_keeps_scalar_values() {
        let relation = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(proto::Relation {
                    common: None,
                    rel_type: Some(RelType::Range(proto::Range {
                        end: 10,
                        step: 1,
                        ..Default::default()
                    })),
                })),
                expressions: vec![proto::Expression {
                    common: None,
                    expr_type: Some(ExprType::Alias(Box::new(expression::Alias {
                        expr: Some(Box::new(proto::Expression {
                            common: None,
                            expr_type: Some(ExprType::Literal(expression::Literal {
                                literal_type: Some(LiteralType::Integer(1)),
                            })),
                        })),
                        name: vec!["id".into()],
                        metadata: None,
                    }))),
                }],
            }))),
        };
        let translated = translate_plan(
            &session(),
            &proto::Plan {
                op_type: Some(PlanOp::Root(relation)),
            },
        )
        .unwrap();
        let rows = translated.dataframe.collect_rows().unwrap();
        assert_eq!(rows.len(), 10);
        assert!(rows.iter().all(|row| row["id"] == serde_json::json!(1)));
    }

    #[test]
    fn repeated_unaliased_expressions_keep_spark_public_names() {
        let attribute = || proto::Expression {
            common: None,
            expr_type: Some(ExprType::UnresolvedAttribute(
                expression::UnresolvedAttribute {
                    unparsed_identifier: "id".into(),
                    ..Default::default()
                },
            )),
        };
        let add = || proto::Expression {
            common: None,
            expr_type: Some(ExprType::UnresolvedFunction(
                expression::UnresolvedFunction {
                    function_name: "+".into(),
                    arguments: vec![
                        attribute(),
                        proto::Expression {
                            common: None,
                            expr_type: Some(ExprType::Literal(expression::Literal {
                                literal_type: Some(LiteralType::Integer(1)),
                            })),
                        },
                    ],
                    is_distinct: false,
                    is_user_defined_function: false,
                    is_internal: None,
                },
            )),
        };
        let literal = || proto::Expression {
            common: None,
            expr_type: Some(ExprType::Literal(expression::Literal {
                literal_type: Some(LiteralType::Integer(1)),
            })),
        };
        let project = |expressions| proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(proto::Relation {
                    common: None,
                    rel_type: Some(RelType::Range(proto::Range {
                        end: 3,
                        step: 1,
                        ..Default::default()
                    })),
                })),
                expressions,
            }))),
        };
        let arithmetic = translate_plan(
            &session(),
            &proto::Plan {
                op_type: Some(PlanOp::Root(project(vec![add(), add()]))),
            },
        )
        .unwrap();
        assert_eq!(
            arithmetic
                .schema
                .logical_fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["(id + 1)", "(id + 1)"]
        );
        let literals = translate_plan(
            &session(),
            &proto::Plan {
                op_type: Some(PlanOp::Root(project(vec![literal(), literal()]))),
            },
        )
        .unwrap();
        assert_eq!(
            literals
                .schema
                .logical_fields()
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["1", "1"]
        );
    }

    #[test]
    fn relation_width_limit_applies_to_intermediate_projections() {
        let source = proto::Relation {
            common: None,
            rel_type: Some(RelType::Range(proto::Range {
                end: 1,
                step: 1,
                ..Default::default()
            })),
        };
        let expressions = (0..=super::super::MAX_CONNECT_SCHEMA_FIELDS)
            .map(|index| proto::Expression {
                common: None,
                expr_type: Some(ExprType::Alias(Box::new(expression::Alias {
                    expr: Some(Box::new(proto::Expression {
                        common: None,
                        expr_type: Some(ExprType::UnresolvedAttribute(
                            expression::UnresolvedAttribute {
                                unparsed_identifier: "id".into(),
                                ..Default::default()
                            },
                        )),
                    })),
                    name: vec![format!("c{index}")],
                    metadata: None,
                }))),
            })
            .collect();
        let wide = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(source)),
                expressions,
            }))),
        };
        assert_eq!(
            translate_plan(
                &session(),
                &proto::Plan {
                    op_type: Some(PlanOp::Root(wide)),
                }
            )
            .err()
            .unwrap()
            .code(),
            Code::ResourceExhausted
        );
    }

    #[test]
    fn unsupported_expressions_and_invalid_plan_ids_fail_closed() {
        let bad_filter = proto::Plan {
            op_type: Some(PlanOp::Root(proto::Relation {
                common: None,
                rel_type: Some(RelType::Filter(Box::new(proto::Filter {
                    input: Some(Box::new(proto::Relation {
                        common: Some(proto::RelationCommon {
                            plan_id: Some(1),
                            ..Default::default()
                        }),
                        rel_type: Some(RelType::Range(proto::Range {
                            start: Some(0),
                            end: 10,
                            step: 1,
                            num_partitions: None,
                        })),
                    })),
                    condition: Some(proto::Expression {
                        common: None,
                        expr_type: Some(ExprType::UnresolvedAttribute(
                            expression::UnresolvedAttribute {
                                unparsed_identifier: "id".into(),
                                plan_id: Some(999),
                                is_metadata_column: Some(false),
                            },
                        )),
                    }),
                }))),
            })),
        };
        let error = translate_plan(&session(), &bad_filter).err().unwrap();
        assert_eq!(error.code(), Code::InvalidArgument);

        let unsupported_root = proto::Plan {
            op_type: Some(PlanOp::Root(proto::Relation {
                common: None,
                rel_type: Some(RelType::Sql(proto::Sql {
                    query: "SELECT 1".into(),
                    ..Default::default()
                })),
            })),
        };
        let error = translate_plan(&session(), &unsupported_root).err().unwrap();
        assert_eq!(error.code(), Code::Unimplemented);
        assert!(error.message().contains("UNSUPPORTED_CONNECT_PLAN"));
    }

    #[test]
    fn typed_execution_normalization_preserves_missing_and_unsupported_statuses() {
        let range = proto::Relation {
            common: None,
            rel_type: Some(RelType::Range(proto::Range {
                start: Some(0),
                end: 1,
                step: 1,
                num_partitions: None,
            })),
        };
        let plans = [
            proto::Plan {
                op_type: Some(PlanOp::Root(proto::Relation::default())),
            },
            proto::Plan {
                op_type: Some(PlanOp::Root(proto::Relation {
                    common: None,
                    rel_type: Some(RelType::Project(Box::new(proto::Project {
                        input: Some(Box::new(range.clone())),
                        expressions: vec![connect_expression(ExprType::Literal(
                            expression::Literal {
                                literal_type: Some(LiteralType::String("unsupported".into())),
                            },
                        ))],
                    }))),
                })),
            },
            proto::Plan {
                op_type: Some(PlanOp::Root(proto::Relation {
                    common: None,
                    rel_type: Some(RelType::Project(Box::new(proto::Project {
                        input: Some(Box::new(range.clone())),
                        expressions: vec![connect_expression(ExprType::UnresolvedStar(
                            expression::UnresolvedStar::default(),
                        ))],
                    }))),
                })),
            },
            proto::Plan {
                op_type: Some(PlanOp::Root(proto::Relation {
                    common: None,
                    rel_type: Some(RelType::Project(Box::new(proto::Project {
                        input: Some(Box::new(range)),
                        expressions: vec![proto::Expression::default()],
                    }))),
                })),
            },
        ];

        for plan in plans {
            let expected = translate_plan(&session(), &plan).err().unwrap().code();
            let normalized = super::super::normalize::normalize_plan(&plan).unwrap();
            let actual =
                translate_plan_for_execution(&session(), &normalized, &CancellationToken::new())
                    .err()
                    .unwrap()
                    .code();
            assert_eq!(
                actual, expected,
                "plan normalization changed its status code"
            );
        }
    }

    #[test]
    fn ddl_schema_parser_preserves_scalar_types_and_nullability() {
        let schema =
            parse_ddl_schema("id BIGINT NOT NULL, label STRING, amount DECIMAL(12, 2)").unwrap();
        let fields = schema.logical_fields();
        assert_eq!(fields.len(), 3);
        assert!(!fields[0].nullable);
        assert!(fields[1].nullable);
        assert!(matches!(fields[2].data_type, DataType::Decimal(12, 2)));
        assert!(matches!(
            schema_to_proto(&schema).unwrap().kind,
            Some(DataTypeKind::Struct(_))
        ));
        assert_eq!(
            parse_ddl_schema("items ARRAY<INT>").unwrap_err().code(),
            Code::Unimplemented
        );
    }

    #[test]
    fn filter_string_parser_accepts_only_the_frozen_predicate_shape() {
        assert_eq!(parse_filter_string("id > 5").unwrap(), ("id".into(), 5));
        for expression in [
            "id >= 5",
            "id > 5, id < 10",
            "id > 5 WHERE id < 10",
            "other.id > 5",
            "id > 5; SELECT 1",
            "cast(id as int) > 5",
        ] {
            assert!(
                parse_filter_string(expression).is_err(),
                "unexpectedly accepted {expression:?}"
            );
        }
    }

    #[test]
    fn comparison_analysis_checks_aliases_and_preserves_string_numeric_coercion() {
        let schema = StructType::from_logical_fields(vec![
            LogicalField::new("flag".into(), DataType::Boolean, true),
            LogicalField::new("text".into(), DataType::String, true),
        ]);
        let invalid = ExprIr::Ge(
            Box::new(ExprIr::Column("flag".into())),
            Box::new(ExprIr::Lit(LiteralValue::I32(1))),
        );
        let aliased = robin_sparkless::alias(invalid, "result");
        assert_eq!(
            validate_comparison_types(&schema, &aliased)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );

        let nested_invalid = ExprIr::Ge(
            Box::new(aliased),
            Box::new(ExprIr::Lit(LiteralValue::I32(0))),
        );
        assert_eq!(
            validate_comparison_types(&schema, &nested_invalid)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );

        let numeric_string = ExprIr::Ge(
            Box::new(ExprIr::Column("text".into())),
            Box::new(ExprIr::Lit(LiteralValue::I32(1))),
        );
        assert!(validate_comparison_types(&schema, &numeric_string).is_ok());
    }

    #[test]
    fn public_projection_name_keeps_the_requested_column_case() {
        let expression = proto::Expression {
            common: None,
            expr_type: Some(ExprType::UnresolvedAttribute(
                expression::UnresolvedAttribute {
                    unparsed_identifier: "ID".into(),
                    ..Default::default()
                },
            )),
        };
        let schema = StructType::from_logical_fields(vec![LogicalField::new(
            "id".into(),
            DataType::Long,
            false,
        )]);
        assert_eq!(public_expression_name(&expression, &schema).unwrap(), "ID");
    }

    #[test]
    fn json_local_sql_and_connect_match_values_and_output_schema_for_shared_subset() {
        use std::collections::HashMap;

        let session = SparkSession::builder()
            .app_name("phase3_frontend_equivalence")
            .config("spark.sql.caseSensitive", "false")
            .get_or_create();
        let rows = vec![
            vec![serde_json::json!(1), serde_json::json!("one")],
            vec![serde_json::json!(2), serde_json::json!("two")],
            vec![serde_json::json!(3), serde_json::json!("three")],
        ];
        let input_schema = vec![
            ("id".to_string(), "long".to_string()),
            ("label".to_string(), "string".to_string()),
        ];
        let source = session
            .create_dataframe_from_rows(rows.clone(), input_schema.clone(), false, false)
            .unwrap();
        session.register_table("phase3_frontend_source", source);

        let json_result = robin_sparkless::execute_plan(
            &session,
            rows.clone(),
            input_schema.clone(),
            &[
                serde_json::json!({"op": "filter", "payload": {"op": "gt", "left": {"col": "id"}, "right": {"lit": 1}}}),
                serde_json::json!({"op": "select", "payload": ["id", "label"]}),
                serde_json::json!({"op": "limit", "payload": {"n": 2}}),
            ],
        )
        .unwrap();
        let sql_result = session
            .sql("SELECT id, label FROM phase3_frontend_source WHERE id > 1 LIMIT 2")
            .unwrap();

        let condition =
            connect_expression(ExprType::ExpressionString(expression::ExpressionString {
                expression: "id > 1".into(),
            }));
        let connect_source = proto::Relation {
            common: None,
            rel_type: Some(RelType::Filter(Box::new(proto::Filter {
                input: Some(Box::new(named_table_relation(
                    "phase3_frontend_source",
                    None,
                ))),
                condition: Some(condition),
            }))),
        };
        let connect_projection = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(connect_source)),
                expressions: vec![
                    connect_attribute("id", None),
                    connect_attribute("label", None),
                ],
            }))),
        };
        let connect_plan = proto::Plan {
            op_type: Some(PlanOp::Root(proto::Relation {
                common: None,
                rel_type: Some(RelType::Limit(Box::new(proto::Limit {
                    input: Some(Box::new(connect_projection)),
                    limit: 2,
                }))),
            })),
        };
        let connect_result = translate_plan(&session, &connect_plan).unwrap();

        let schema_signature = |schema: &StructType| {
            schema
                .logical_fields()
                .iter()
                .map(|field| {
                    (
                        field.name.clone(),
                        format!("{:?}", field.data_type),
                        field.nullable,
                    )
                })
                .collect::<Vec<_>>()
        };
        let rows_by_id = |mut rows: Vec<HashMap<String, serde_json::Value>>| {
            rows.sort_by_key(|row| row.get("id").and_then(serde_json::Value::as_i64));
            rows
        };

        let json_schema = json_result.schema_engine().unwrap();
        let sql_schema = sql_result.schema_engine().unwrap();
        let connect_schema = connect_result.dataframe.schema_engine().unwrap();
        assert_eq!(
            schema_signature(&json_schema),
            schema_signature(&sql_schema)
        );
        assert_eq!(
            schema_signature(&json_schema),
            schema_signature(&connect_schema)
        );

        let json_rows = rows_by_id(json_result.collect_as_json_rows().unwrap());
        let sql_rows = rows_by_id(sql_result.collect_as_json_rows().unwrap());
        let connect_rows = rows_by_id(connect_result.dataframe.collect_as_json_rows().unwrap());
        assert_eq!(json_rows, sql_rows);
        assert_eq!(json_rows, connect_rows);

        let json_result = robin_sparkless::execute_plan(
            &session,
            rows.clone(),
            input_schema,
            &[serde_json::json!({"op": "select", "payload": ["missing"]})],
        );
        let json_error = match json_result {
            Ok(frame) => match frame.collect_as_json_rows() {
                Ok(_) => panic!("JSON plan must reject a missing column"),
                Err(error) => error.to_string(),
            },
            Err(error) => error.to_string(),
        };
        let sql_result = session.sql("SELECT missing FROM phase3_frontend_source");
        let sql_error = match sql_result {
            Ok(frame) => match frame.collect_as_json_rows() {
                Ok(_) => panic!("local SQL must reject a missing column"),
                Err(error) => error.to_string(),
            },
            Err(error) => error.to_string(),
        };
        let missing_connect_column = proto::Relation {
            common: None,
            rel_type: Some(RelType::Project(Box::new(proto::Project {
                input: Some(Box::new(named_table_relation(
                    "phase3_frontend_source",
                    None,
                ))),
                expressions: vec![connect_attribute("missing", None)],
            }))),
        };
        let connect_error = match translate_plan(
            &session,
            &proto::Plan {
                op_type: Some(PlanOp::Root(missing_connect_column)),
            },
        ) {
            Ok(_) => panic!("Connect plan must reject a missing column"),
            Err(error) => error,
        };
        assert_eq!(connect_error.code(), Code::InvalidArgument);
        for error in [json_error, sql_error, connect_error.message().to_string()] {
            assert!(
                error.to_ascii_lowercase().contains("missing"),
                "frontend error should identify the same missing column: {error}"
            );
        }
    }

    #[test]
    fn malformed_connect_plan_is_rejected_before_external_source_scan() {
        use robin_sparkless_core::engine::{
            ArrowIpcStream, CatalogCapabilities, CatalogOwnership, CatalogReadSnapshot,
            CatalogReader, CatalogResolver, ExternalCatalog, ExternalTableDescriptor,
            ExternalTableFormat,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone)]
        struct CountingReader {
            scans: Arc<AtomicUsize>,
        }

        impl CatalogReader for CountingReader {
            fn begin_snapshot(
                &self,
                token: &CancellationToken,
            ) -> Result<Arc<dyn CatalogReadSnapshot>, EngineError> {
                token.check_cancelled()?;
                Ok(Arc::new(CountingSnapshot {
                    scans: self.scans.clone(),
                }))
            }
        }

        struct CountingSnapshot {
            scans: Arc<AtomicUsize>,
        }

        impl CountingSnapshot {
            fn descriptor() -> ExternalTableDescriptor {
                ExternalTableDescriptor {
                    table_id: "source.items".into(),
                    canonical_location: "test://source/items".into(),
                    schema: StructType::from_logical_fields(vec![LogicalField::new(
                        "id".into(),
                        DataType::Long,
                        false,
                    )]),
                    version: None,
                    provider: "phase3-test".into(),
                    permissions_context: None,
                    format: ExternalTableFormat::DuckDb,
                }
            }
        }

        impl CatalogReadSnapshot for CountingSnapshot {
            fn resolve_ownership(&self, identifier: &str) -> Result<CatalogOwnership, EngineError> {
                if identifier == "source.items" {
                    Ok(CatalogOwnership::external(
                        "source-owner",
                        CatalogCapabilities::READ_ONLY,
                    ))
                } else {
                    Ok(CatalogOwnership::Local)
                }
            }

            fn resolve_table(
                &self,
                identifier: &str,
            ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
                Ok((identifier == "source.items").then(Self::descriptor))
            }

            fn scan_table(
                &self,
                _table: &ExternalTableDescriptor,
                _token: &CancellationToken,
                _consume: &mut dyn FnMut(ArrowIpcStream) -> Result<(), EngineError>,
            ) -> Result<(), EngineError> {
                self.scans.fetch_add(1, Ordering::SeqCst);
                Err(EngineError::invalid_input(
                    "malformed Connect plan reached its source scan".into(),
                ))
            }
        }

        struct CountingCatalog;

        impl CatalogResolver for CountingCatalog {
            fn resolve_ownership(&self, identifier: &str) -> Result<CatalogOwnership, EngineError> {
                if identifier == "source.items" {
                    Ok(CatalogOwnership::external(
                        "source-owner",
                        CatalogCapabilities::READ_ONLY,
                    ))
                } else {
                    Ok(CatalogOwnership::Local)
                }
            }

            fn resolve_table(
                &self,
                identifier: &str,
            ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
                Ok((identifier == "source.items").then(CountingSnapshot::descriptor))
            }
        }

        let session = session();
        let scans = Arc::new(AtomicUsize::new(0));
        let resolver: Arc<dyn CatalogResolver> = Arc::new(CountingCatalog);
        let reader: Arc<dyn CatalogReader> = Arc::new(CountingReader {
            scans: scans.clone(),
        });
        session
            .set_external_catalog(Some(Arc::new(ExternalCatalog::new(
                resolver,
                Some(reader),
                None,
            ))))
            .unwrap();

        let malformed_right = proto::Relation {
            common: None,
            rel_type: Some(RelType::Filter(Box::new(proto::Filter {
                input: Some(Box::new(proto::Relation {
                    common: None,
                    rel_type: Some(RelType::Range(proto::Range {
                        end: 1,
                        step: 1,
                        ..Default::default()
                    })),
                })),
                condition: None,
            }))),
        };
        let plan = proto::Plan {
            op_type: Some(PlanOp::Root(proto::Relation {
                common: None,
                rel_type: Some(RelType::Join(Box::new(proto::Join {
                    left: Some(Box::new(named_table_relation("source.items", None))),
                    right: Some(Box::new(malformed_right)),
                    join_condition: None,
                    using_columns: vec!["id".into()],
                    join_type: proto::join::JoinType::Inner as i32,
                    join_data_type: None,
                }))),
            })),
        };

        let result = translate_proto_plan_for_execution(&session, &plan, &CancellationToken::new());
        let error = match result {
            Ok(_) => panic!("missing Filter.condition must invalidate the complete plan"),
            Err(error) => error,
        };
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(scans.load(Ordering::SeqCst), 0);
    }
}
