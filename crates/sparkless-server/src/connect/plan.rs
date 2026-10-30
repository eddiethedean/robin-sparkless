//! Checked translation for the Spark Connect relation and expression subset frozen in 4.18-01.

use crate::connect_proto as proto;
use proto::data_type::{self, Kind as DataTypeKind};
use proto::expression::{ExprType, literal::LiteralType};
use proto::plan::OpType as PlanOp;
use proto::relation::RelType;
use robin_sparkless::{
    DataType, EngineError, ExprIr, LiteralValue, LogicalField, SparkSession, StructType,
};
use spark_sql_parser::ast::{BinaryOperator, Expr as SqlExpr, Value};
use std::collections::{BTreeMap, HashSet};
use tonic::{Code, Status};

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
    deferred_count_input: Option<robin_sparkless::DataFrame>,
}

#[derive(Clone, Copy)]
enum ExpressionUse {
    Filter,
    Project,
    Aggregate,
}

pub(super) fn translate_plan(
    session: &SparkSession,
    plan: &proto::Plan,
) -> Result<TranslatedPlan, Status> {
    let relation = match plan.op_type.as_ref() {
        Some(PlanOp::Root(relation)) => translate_relation(session, relation, 1)?,
        Some(PlanOp::Command(_)) => {
            return Err(unsupported(
                "Connect commands and SQL execution are outside the 4.18 subset",
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

fn translate_relation(
    session: &SparkSession,
    relation: &proto::Relation,
    depth: usize,
) -> Result<TranslatedRelation, Status> {
    if depth > MAX_PLAN_DEPTH {
        return Err(Status::resource_exhausted(format!(
            "Connect relation depth exceeds {MAX_PLAN_DEPTH}"
        )));
    }
    let common = relation.common.as_ref();
    let mut deferred_count_input = None;
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
            (dataframe, schema, HashSet::new())
        }
        Some(RelType::CachedLocalRelation(_)) => {
            return Err(Status::unimplemented(
                "CachedLocalRelation requires the unsupported AddArtifacts transfer path",
            ));
        }
        Some(RelType::Filter(filter)) => {
            let input = translate_child(session, filter.input.as_deref(), "Filter.input", depth)?;
            if input.deferred_count_input.is_some() {
                return Err(unsupported(
                    "filters over a global aggregate are outside the 4.18 subset",
                ));
            }
            let condition = filter
                .condition
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("Filter.condition is required"))?;
            let expr = match condition.expr_type.as_ref() {
                Some(ExprType::ExpressionString(expression)) => {
                    parse_filter_string(&expression.expression)?
                }
                other => translate_expression(other, &input.plan_ids, ExpressionUse::Filter)?,
            };
            if !matches!(expr, ExprIr::Ge(_, _) | ExprIr::Gt(_, _)) {
                return Err(unsupported(
                    "filter conditions require a captured comparison expression",
                ));
            }
            validate_comparison_types(&input.schema, &expr)?;
            let expr = resolve_logical_columns(&input, expr)?;
            let schema = input.schema.clone();
            let dataframe = input
                .dataframe
                .filter_expr_ir(&expr)
                .map_err(engine_failure)?;
            (dataframe, schema, input.plan_ids)
        }
        Some(RelType::Project(project)) => {
            let input = translate_child(session, project.input.as_deref(), "Project.input", depth)?;
            if input.deferred_count_input.is_some() {
                return Err(unsupported(
                    "projections over a global aggregate are outside the 4.18 subset",
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
                        &input.plan_ids,
                        ExpressionUse::Project,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let expressions = expressions
                .into_iter()
                .map(|expression| {
                    validate_comparison_types(&input.schema, &expression)?;
                    resolve_logical_columns(&input, expression)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let dataframe = input
                .dataframe
                .select_expr_ir(&expressions)
                .map_err(engine_failure)?;
            let schema = projected_schema(&project.expressions, &input.schema, &dataframe)?;
            (dataframe, schema, input.plan_ids)
        }
        Some(RelType::Limit(limit)) => {
            let input = translate_child(session, limit.input.as_deref(), "Limit.input", depth)?;
            if input.deferred_count_input.is_some() {
                return Err(unsupported(
                    "limits over a global aggregate are outside the 4.18 subset",
                ));
            }
            if limit.limit < 0 {
                return Err(Status::invalid_argument("Limit.limit must be nonnegative"));
            }
            let limit_value = usize::try_from(limit.limit)
                .map_err(|_| Status::invalid_argument("Limit.limit is out of range"))?;
            let dataframe = input.dataframe.limit(limit_value).map_err(|error| {
                Status::failed_precondition(format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
            })?;
            (dataframe, input.schema, input.plan_ids)
        }
        Some(RelType::Aggregate(aggregate)) => {
            let input = translate_child(
                session,
                aggregate.input.as_deref(),
                "Aggregate.input",
                depth,
            )?;
            let group_type = proto::aggregate::GroupType::try_from(aggregate.group_type)
                .map_err(|_| Status::invalid_argument("Aggregate.group_type is unknown"))?;
            if group_type != proto::aggregate::GroupType::Groupby
                || !aggregate.grouping_expressions.is_empty()
                || aggregate.pivot.is_some()
                || !aggregate.grouping_sets.is_empty()
            {
                return Err(unsupported(
                    "only a global GROUP_TYPE_GROUPBY count(1) aggregate is supported",
                ));
            }
            if input.deferred_count_input.is_some() {
                return Err(unsupported(
                    "nested global aggregates are outside the 4.18 subset",
                ));
            }
            if aggregate.aggregate_expressions.len() != 1 {
                return Err(unsupported(
                    "only one global count(1) aggregate expression is supported",
                ));
            }
            let expression = &aggregate.aggregate_expressions[0];
            let expr = translate_expression(
                expression.expr_type.as_ref(),
                &input.plan_ids,
                ExpressionUse::Aggregate,
            )?;
            let dataframe = input
                .dataframe
                .select_expr_ir(&[expr])
                .map_err(engine_failure)?;
            deferred_count_input = Some(input.dataframe);
            let schema = StructType::from_logical_fields(vec![LogicalField::new(
                "count(1)".to_string(),
                DataType::Long,
                false,
            )]);
            (dataframe, schema, input.plan_ids)
        }
        Some(_) => {
            return Err(unsupported(
                "relation type is not enabled in the 4.18 subset",
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
    validate_schema_width(&schema)?;
    Ok(TranslatedRelation {
        dataframe,
        schema,
        plan_ids,
        deferred_count_input,
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
            let mut fields = input_schema.logical_fields().iter().filter(|candidate| {
                candidate
                    .name
                    .eq_ignore_ascii_case(&attribute.unparsed_identifier)
            });
            if fields.next().is_none() {
                return Err(Status::failed_precondition(format!(
                    "Connect projection column {:?} disappeared from its input schema",
                    attribute.unparsed_identifier
                )));
            };
            if fields.next().is_some() {
                return Err(Status::invalid_argument(format!(
                    "CONNECT_PLAN_ANALYSIS_FAILED: ambiguous column {:?}",
                    attribute.unparsed_identifier
                )));
            }
            Ok(attribute.unparsed_identifier.clone())
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

fn resolve_logical_columns(
    input: &TranslatedRelation,
    expression: ExprIr,
) -> Result<ExprIr, Status> {
    fn resolve(name: &str, input: &TranslatedRelation) -> Result<String, Status> {
        let matches = input
            .schema
            .logical_fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name.eq_ignore_ascii_case(name))
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let index = match matches.as_slice() {
            [index] => *index,
            [] => {
                return Err(Status::invalid_argument(format!(
                    "CONNECT_PLAN_ANALYSIS_FAILED: unresolved column {name:?}"
                )));
            }
            _ => {
                return Err(Status::invalid_argument(format!(
                    "CONNECT_PLAN_ANALYSIS_FAILED: ambiguous column {name:?}"
                )));
            }
        };
        input
            .dataframe
            .schema_engine()
            .map_err(engine_failure)?
            .logical_fields()
            .get(index)
            .map(|field| field.name.clone())
            .ok_or_else(|| {
                Status::failed_precondition(
                    "Connect logical schema does not match its physical dataframe",
                )
            })
    }
    fn visit(expression: ExprIr, input: &TranslatedRelation) -> Result<ExprIr, Status> {
        Ok(match expression {
            ExprIr::Column(name) => ExprIr::Column(resolve(&name, input)?),
            ExprIr::Gt(left, right) => ExprIr::Gt(
                Box::new(visit(*left, input)?),
                Box::new(visit(*right, input)?),
            ),
            ExprIr::Ge(left, right) => ExprIr::Ge(
                Box::new(visit(*left, input)?),
                Box::new(visit(*right, input)?),
            ),
            ExprIr::Add(left, right) => ExprIr::Add(
                Box::new(visit(*left, input)?),
                Box::new(visit(*right, input)?),
            ),
            ExprIr::Call { name, args } => ExprIr::Call {
                name,
                args: args
                    .into_iter()
                    .map(|argument| visit(argument, input))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            other => other,
        })
    }
    visit(expression, input)
}

fn validate_comparison_types(input_schema: &StructType, expression: &ExprIr) -> Result<(), Status> {
    fn expression_type(input_schema: &StructType, expression: &ExprIr) -> Result<DataType, Status> {
        match expression {
            ExprIr::Column(name) => {
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
            ExprIr::Lit(LiteralValue::I32(_)) => Ok(DataType::Integer),
            ExprIr::Lit(LiteralValue::I64(_)) => Ok(DataType::Long),
            ExprIr::Lit(LiteralValue::F64(_)) => Ok(DataType::Double),
            ExprIr::Lit(LiteralValue::Str(_)) => Ok(DataType::String),
            ExprIr::Lit(LiteralValue::Bool(_)) => Ok(DataType::Boolean),
            ExprIr::Lit(LiteralValue::Null) => Err(unsupported(
                "untyped null comparison operands are outside the 4.18 subset",
            )),
            ExprIr::Add(left, right) => {
                let left = expression_type(input_schema, left)?;
                let right = expression_type(input_schema, right)?;
                if !is_connect_numeric_type(&left) || !is_connect_numeric_type(&right) {
                    return Err(Status::invalid_argument(format!(
                        "CONNECT_PLAN_ANALYSIS_FAILED: arithmetic requires numeric operands, found {left:?} and {right:?}"
                    )));
                }
                Ok(connect_common_numeric_type(&left, &right))
            }
            ExprIr::Ge(left, right) | ExprIr::Gt(left, right) => {
                let left_type = expression_type(input_schema, left)?;
                let right_type = expression_type(input_schema, right)?;
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
                expression_type(input_schema, value)
            }
            _ => Err(unsupported(
                "comparison operand type cannot be analyzed in the 4.18 subset",
            )),
        }
    }

    expression_type(input_schema, expression).map(|_| ())
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

fn connect_common_numeric_type(left: &DataType, right: &DataType) -> DataType {
    match (left, right) {
        (DataType::Double, _) | (_, DataType::Double) => DataType::Double,
        (DataType::Float, _) | (_, DataType::Float) => DataType::Float,
        (DataType::Long, _) | (_, DataType::Long) => DataType::Long,
        (DataType::Integer, _) | (_, DataType::Integer) => DataType::Integer,
        (DataType::Short, _) | (_, DataType::Short) => DataType::Short,
        (DataType::Byte, DataType::Byte) => DataType::Byte,
        (DataType::Decimal(precision, scale), _) | (_, DataType::Decimal(precision, scale)) => {
            DataType::Decimal(*precision, *scale)
        }
        _ => left.clone(),
    }
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
) -> Result<TranslatedRelation, Status> {
    let input = input.ok_or_else(|| Status::invalid_argument(format!("{name} is required")))?;
    translate_relation(session, input, depth + 1)
}

fn translate_expression(
    expr_type: Option<&ExprType>,
    input_plan_ids: &HashSet<i64>,
    usage: ExpressionUse,
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
                || !is_simple_identifier(&attribute.unparsed_identifier)
                || attribute.is_metadata_column.unwrap_or(false)
            {
                return Err(unsupported(
                    "only simple non-metadata column references are supported",
                ));
            }
            validate_expression_plan_id(attribute.plan_id, input_plan_ids)?;
            Ok(ExprIr::Column(attribute.unparsed_identifier.clone()))
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
                    let left =
                        translate_expression(left.expr_type.as_ref(), input_plan_ids, usage)?;
                    let right =
                        translate_expression(right.expr_type.as_ref(), input_plan_ids, usage)?;
                    Ok(ExprIr::Add(Box::new(left), Box::new(right)))
                }
                (ExpressionUse::Project | ExpressionUse::Filter, ">=", [left, right]) => {
                    let left =
                        translate_expression(left.expr_type.as_ref(), input_plan_ids, usage)?;
                    let right =
                        translate_expression(right.expr_type.as_ref(), input_plan_ids, usage)?;
                    Ok(ExprIr::Ge(Box::new(left), Box::new(right)))
                }
                (ExpressionUse::Aggregate, "count", [argument]) => {
                    let argument =
                        translate_expression(argument.expr_type.as_ref(), input_plan_ids, usage)?;
                    if !matches!(
                        argument,
                        ExprIr::Lit(LiteralValue::I32(1) | LiteralValue::I64(1))
                    ) {
                        return Err(unsupported("only global count(1) is supported"));
                    }
                    let row_count = ExprIr::Call {
                        name: "count_rows".to_string(),
                        args: Vec::new(),
                    };
                    Ok(robin_sparkless::alias(row_count, "count(1)"))
                }
                _ => Err(unsupported(format!(
                    "Connect function {:?} with this arity or context is unsupported",
                    function.function_name
                ))),
            }
        }
        Some(ExprType::Alias(alias)) if matches!(usage, ExpressionUse::Project) => {
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
            let expression = translate_expression(
                expression.expr_type.as_ref(),
                input_plan_ids,
                ExpressionUse::Project,
            )?;
            Ok(robin_sparkless::alias(expression, &alias.name[0]))
        }
        Some(ExprType::ExpressionString(_)) => Err(unsupported(
            "expression_string is supported only as a checked Filter condition",
        )),
        Some(_) => Err(unsupported(
            "Connect expression type is not enabled in the 4.18 subset",
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

fn parse_filter_string(expression: &str) -> Result<ExprIr, Status> {
    if expression.len() > MAX_FILTER_EXPRESSION_BYTES {
        return Err(Status::resource_exhausted(format!(
            "Connect filter expression exceeds {MAX_FILTER_EXPRESSION_BYTES} bytes"
        )));
    }
    let expression = spark_sql_parser::parse_single_expression(expression).map_err(|error| {
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
    Ok(ExprIr::Gt(
        Box::new(ExprIr::Column(column.value)),
        Box::new(ExprIr::Lit(LiteralValue::I64(value))),
    ))
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
                "Connect schema type {data_type:?} is not enabled in the 4.18 schema subset"
            )));
        }
    };
    Ok(proto::DataType { kind: Some(kind) })
}

fn engine_failure(error: EngineError) -> Status {
    let code = match &error {
        EngineError::User(_) => Code::InvalidArgument,
        EngineError::NotFound(_) => Code::NotFound,
        _ => Code::FailedPrecondition,
    };
    Status::new(code, format!("CONNECT_PLAN_ANALYSIS_FAILED: {error}"))
}

fn unsupported(message: impl Into<String>) -> Status {
    Status::unimplemented(format!("UNSUPPORTED_CONNECT_PLAN: {}", message.into()))
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
        assert!(matches!(
            parse_filter_string("id > 5").unwrap(),
            ExprIr::Gt(_, _)
        ));
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
}
