//! Plan interpreter: execute a serialized logical plan (list of ops) using the existing DataFrame API.
//!
//! See [LOGICAL_PLAN_FORMAT.md](../../../docs/LOGICAL_PLAN_FORMAT.md) for the plan and expression schema.

mod expr;
mod logical;

use crate::dataframe::joins::{
    check_cross_join_budget, expr_contains_only_join_key_equalities,
    try_extract_join_eq_columns_all,
};
use crate::dataframe::{DataFrame, JoinType, disambiguate_agg_output_names};
use crate::functions::{asc_nulls_first, asc_nulls_last, col, desc_nulls_first, desc_nulls_last};
use crate::plan::expr::{expr_from_value, try_column_from_udf_value};
use crate::session::{SparkSession, set_thread_udf_session};
pub use expr::PlanExprError;
pub use logical::LogicalPlan;
use polars::prelude::Expr;
use polars::prelude::PolarsError;
use robin_sparkless_plan::{
    BinaryInput, GroupInput, LogicalPlan as PortablePlan, OperationKind, PlanOperation, PlanPayload,
};
use serde_json::Value;
use std::collections::HashSet;

/// Execute a logical plan: build initial DataFrame from (data, schema), then apply each op in sequence.
///
/// - `data`: rows as `Vec<Vec<Value>>` (each inner vec is one row; order matches schema).
/// - `schema`: list of (column_name, dtype_string) e.g. `[("id", "bigint"), ("name", "string")]`.
/// - `plan`: list of `{"op": "...", "payload": ...}` objects.
///
/// Returns the final DataFrame after applying all operations.
pub fn execute_plan(
    session: &SparkSession,
    data: Vec<Vec<Value>>,
    schema: Vec<(String, String)>,
    plan: &[Value],
) -> Result<DataFrame, PlanError> {
    let plan = PortablePlan::decode(plan).map_err(|error| {
        if error.code() == "UNSUPPORTED_PLAN_OPERATION" {
            PlanError::UnsupportedOp(error.user_message().to_string())
        } else {
            PlanError::InvalidPlan(error.user_message().to_string())
        }
    })?;
    execute_typed_plan(session, data, schema, plan)
}

/// Execute a decoded portable plan after validating every operation at the input boundary.
pub fn execute_typed_plan(
    session: &SparkSession,
    data: Vec<Vec<Value>>,
    schema: Vec<(String, String)>,
    plan: PortablePlan,
) -> Result<DataFrame, PlanError> {
    set_thread_udf_session(session.clone());
    let mut df = session
        .create_dataframe_from_rows(data, schema, false, false)
        .map_err(PlanError::Session)?
        .with_case_insensitive_column_resolution();

    for operation in plan.operations {
        df = apply_op(session, df, operation)?;
    }

    Ok(df)
}

/// Errors from plan execution.
#[derive(Debug)]
pub enum PlanError {
    Session(PolarsError),
    Expr(PlanExprError),
    InvalidPlan(String),
    UnsupportedOp(String),
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::Session(e) => write!(f, "session/df: {e}"),
            PlanError::Expr(e) => write!(f, "expression: {e}"),
            PlanError::InvalidPlan(s) => write!(f, "invalid plan: {s}"),
            PlanError::UnsupportedOp(s) => write!(f, "unsupported op: {s}"),
        }
    }
}

impl std::error::Error for PlanError {}

/// Parse one orderBy column element into zero or more (column_name, ascending) pairs.
/// Accepts: "col", "col DESC", "col ASC", "['a','b']" (Python repr), {"col":"x"}, {"name":"x"}.
fn parse_order_by_element(v: &Value) -> Option<Vec<(String, bool)>> {
    if let Some(s) = v.as_str() {
        let s = s.trim();
        if s.eq_ignore_ascii_case("desc") || s.eq_ignore_ascii_case("asc") {
            return None;
        }
        if s.to_uppercase().ends_with(" DESC") {
            let name = s[..s.len().saturating_sub(5)].trim().to_string();
            return if name.is_empty() {
                None
            } else {
                Some(vec![(name, false)])
            };
        }
        if s.to_uppercase().ends_with(" ASC") {
            let name = s[..s.len().saturating_sub(4)].trim().to_string();
            return if name.is_empty() {
                None
            } else {
                Some(vec![(name, true)])
            };
        }
        if s.starts_with('[') && s.ends_with(']') {
            let inner = s[1..s.len() - 1].trim();
            if inner.is_empty() {
                return Some(vec![]);
            }
            let names: Vec<(String, bool)> = inner
                .split(',')
                .map(|p| {
                    (
                        p.trim().trim_matches('\'').trim_matches('"').to_string(),
                        true,
                    )
                })
                .filter(|(n, _)| !n.is_empty())
                .collect();
            return Some(names);
        }
        return Some(vec![(s.to_string(), true)]);
    }
    let obj = v.as_object()?;
    let name = obj
        .get("col")
        .or_else(|| obj.get("name"))
        .and_then(Value::as_str)
        .map(|s| s.to_string())?;
    Some(vec![(name, true)])
}

/// Extract column name from expression if it is a simple column reference {"col": "name"}.
fn expr_to_col_name(v: &Value) -> Option<String> {
    let obj = v.as_object()?;
    obj.get("col")
        .or_else(|| obj.get("column"))
        .and_then(Value::as_str)
        .map(|s| s.to_string())
}

/// Join condition: column-name keys or a full expression tree (array_contains, compound AND, etc.).
enum JoinOnSpec {
    Keys(Vec<String>),
    Condition(Expr),
}

fn is_simple_eq_same_col(v: &Value) -> bool {
    let Some(obj) = v.as_object() else {
        return false;
    };
    let op = obj
        .get("op")
        .or_else(|| obj.get("operator"))
        .and_then(Value::as_str);
    if !op.map(|o| o == "eq" || o == "==").unwrap_or(false) {
        return false;
    }
    matches!(
        (
            obj.get("left").and_then(expr_to_col_name),
            obj.get("right").and_then(expr_to_col_name),
        ),
        (Some(l), Some(r)) if l == r
    )
}

fn join_on_needs_expression(on: &Value) -> bool {
    if let Some(obj) = on.as_object() {
        if obj.get("fn").is_some() {
            return true;
        }
        if obj.get("col").or_else(|| obj.get("column")).is_some() && obj.len() <= 2 {
            return false;
        }
        if obj.get("op").is_some() && !is_simple_eq_same_col(on) {
            return true;
        }
    }
    if let Some(arr) = on.as_array() {
        return arr.iter().any(|v| {
            !(v.as_str().is_some() || expr_to_col_name(v).is_some() || is_simple_eq_same_col(v))
        });
    }
    false
}

fn parse_join_on_spec(on: &Value, df: &DataFrame) -> Result<JoinOnSpec, PlanError> {
    if join_on_needs_expression(on) {
        let expr = if let Some(arr) = on.as_array() {
            let mut combined: Option<Expr> = None;
            for v in arr {
                let e = expr_from_value(v).map_err(|e| PlanError::InvalidPlan(e.to_string()))?;
                combined = Some(match combined {
                    None => e,
                    Some(prev) => prev.and(e),
                });
            }
            combined.ok_or_else(|| {
                PlanError::InvalidPlan("join 'on' expression array is empty".into())
            })?
        } else {
            expr_from_value(on).map_err(|e| PlanError::InvalidPlan(e.to_string()))?
        };
        return Ok(JoinOnSpec::Condition(expr));
    }
    parse_join_on(on, df).map(JoinOnSpec::Keys)
}

/// Parse join "on" into list of column names. Accepts:
/// - string -> [s]; array of strings -> those; array of {"col": "x"} -> ["x"];
/// - array of {"op": "eq", "left": {"col": "a"}, "right": {"col": "a"}} -> ["a"] (Sparkless v4 format, #552).
///   #704, #698: Reject expression-like strings (e.g. array_contains(...)) with clear error.
fn parse_join_on(on: &Value, df: &DataFrame) -> Result<Vec<String>, PlanError> {
    if let Some(s) = on.as_str() {
        if s.contains('(') {
            return Err(PlanError::InvalidPlan(
                "join on expression (e.g. array_contains(...) or column expr) is not supported; use column names only".into(),
            ));
        }
        let resolved = df.resolve_column_name(s).map_err(PlanError::Session)?;
        return Ok(vec![resolved]);
    }
    let arr = on.as_array().ok_or_else(|| {
        PlanError::InvalidPlan(
            "join 'on' must be string, array of strings, or array of column refs / eq expressions"
                .into(),
        )
    })?;
    let mut keys = Vec::with_capacity(arr.len());
    for v in arr {
        if let Some(s) = v.as_str() {
            let resolved = df.resolve_column_name(s).map_err(PlanError::Session)?;
            keys.push(resolved);
            continue;
        }
        if let Some(obj) = v.as_object() {
            // {"col": "x"} -> single key for both sides
            if let Some(name) = expr_to_col_name(v) {
                let resolved = df.resolve_column_name(&name).map_err(PlanError::Session)?;
                keys.push(resolved);
                continue;
            }
            // {"op": "eq"|"==", "left": {"col": "a"}, "right": {"col": "a"}} (Sparkless v4)
            let op = obj
                .get("op")
                .or_else(|| obj.get("operator"))
                .and_then(Value::as_str);
            if op.map(|o| o == "eq" || o == "==").unwrap_or(false) {
                let left = obj.get("left").and_then(expr_to_col_name);
                let right = obj.get("right").and_then(expr_to_col_name);
                if let (Some(l), Some(r)) = (left, right) {
                    if l == r {
                        let resolved = df.resolve_column_name(&l).map_err(PlanError::Session)?;
                        keys.push(resolved);
                        continue;
                    }
                }
            }
        }
        return Err(PlanError::InvalidPlan(
            "join 'on' element must be string, {\"col\": \"name\"}, or {\"op\": \"eq\", \"left\": {\"col\": \"x\"}, \"right\": {\"col\": \"x\"}}".into(),
        ));
    }
    Ok(keys)
}

/// Convert other_data to rows. Accepts arrays [[v,v],[v,v]] or dicts [{"col":v},...] (Sparkless may send dict rows).
fn other_data_to_rows(other_data: &[Value], schema_names: &[String]) -> Vec<Vec<Value>> {
    other_data
        .iter()
        .filter_map(|v| {
            if let Some(arr) = v.as_array() {
                return Some(arr.clone());
            }
            if let Some(obj) = v.as_object() {
                let row: Vec<Value> = schema_names
                    .iter()
                    .map(|n| obj.get(n).cloned().unwrap_or(Value::Null))
                    .collect();
                return Some(row);
            }
            None
        })
        .collect()
}

/// Parse (name, type) from schema field object. Supports {"name","type"} and {"fieldName","dataType"} (Sparkless).
fn schema_field_to_pair(v: &Value) -> Option<(String, String)> {
    let obj = v.as_object()?;
    let name = obj
        .get("name")
        .or_else(|| obj.get("fieldName"))
        .and_then(Value::as_str)?
        .to_string();
    let ty = obj
        .get("type")
        .or_else(|| obj.get("dataType"))
        .and_then(Value::as_str)
        .or_else(|| {
            // dataType may be nested: {"type":"string"} or {"typeName":"string"}
            obj.get("dataType")?
                .get("typeName")
                .or_else(|| obj.get("dataType")?.get("type"))
                .and_then(Value::as_str)
        })?
        .to_string();
    Some((name, ty))
}

fn apply_op(
    session: &SparkSession,
    df: DataFrame,
    operation: PlanOperation,
) -> Result<DataFrame, PlanError> {
    match (operation.kind(), operation.payload()) {
        (OperationKind::Stop, PlanPayload::Empty) => {
            session.stop();
            Ok(df)
        }
        (OperationKind::Filter, PlanPayload::Expression(expression)) => {
            let expression = expression.to_json_value();
            let expression = expr_from_value(&expression).map_err(PlanError::Expr)?;
            df.filter(expression).map_err(PlanError::Session)
        }
        (OperationKind::Select, PlanPayload::Select(items)) => {
            let mut expressions = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                if let Some(object) = item.as_object() {
                    if let Some(value) = object.get("expr") {
                        let name = object.get("name").and_then(Value::as_str).unwrap_or("_c");
                        let expression = expr_from_value(value).map_err(PlanError::Expr)?;
                        let expression = df
                            .resolve_expr_column_names(expression)
                            .map_err(PlanError::Session)?;
                        expressions.push(expression.alias(name));
                        continue;
                    }
                    if object.contains_key("fn")
                        || object.contains_key("op")
                        || object.contains_key("udf")
                    {
                        let expression = expr_from_value(item).map_err(PlanError::Expr)?;
                        let expression = df
                            .resolve_expr_column_names(expression)
                            .map_err(PlanError::Session)?;
                        let alias = object
                            .get("fn")
                            .and_then(Value::as_str)
                            .filter(|name| *name == "alias")
                            .and_then(|_| object.get("args").and_then(Value::as_array))
                            .and_then(|arguments| arguments.last())
                            .and_then(Value::as_str)
                            .map(String::from)
                            .unwrap_or_else(|| format!("_c{index}"));
                        expressions.push(expression.alias(alias));
                        continue;
                    }
                }

                let name = item
                    .as_str()
                    .map(String::from)
                    .or_else(|| expr_to_col_name(item))
                    .or_else(|| item.get("name").and_then(Value::as_str).map(String::from))
                    .ok_or_else(|| {
                        PlanError::InvalidPlan(
                            "validated select item has no column or expression".into(),
                        )
                    })?;
                let trimmed = name.trim();
                let (expression_text, alias_override) = if let Some(index) = trimmed.rfind(" as ") {
                    (trimmed[..index].trim(), Some(trimmed[index + 4..].trim()))
                } else {
                    (trimmed, None)
                };
                if let Some(expression) =
                    crate::plan::expr::try_parse_concat_expr_from_string(expression_text)
                {
                    let expression = df
                        .resolve_expr_column_names(expression)
                        .map_err(PlanError::Session)?;
                    expressions.push(expression.alias(alias_override.unwrap_or(trimmed)));
                } else {
                    let expression = polars::prelude::col::<&str>(expression_text);
                    let expression = df
                        .resolve_expr_column_names(expression)
                        .map_err(PlanError::Session)?;
                    expressions.push(expression.alias(name.as_str()));
                }
            }
            df.select_exprs(expressions).map_err(PlanError::Session)
        }
        (OperationKind::Limit, PlanPayload::Limit { n }) => {
            let n = usize::try_from(*n).map_err(|_| {
                PlanError::InvalidPlan("validated limit exceeds the platform limit".into())
            })?;
            df.limit(n).map_err(PlanError::Session)
        }
        (OperationKind::Offset, PlanPayload::Offset { n }) => {
            let n = usize::try_from(*n).map_err(|_| {
                PlanError::InvalidPlan("validated offset exceeds the platform limit".into())
            })?;
            df.offset(n).map_err(PlanError::Session)
        }
        (
            OperationKind::OrderBy,
            PlanPayload::OrderBy {
                columns,
                nulls_last,
            },
        ) => {
            let mut pairs = Vec::new();
            for column in columns {
                if let Some(parsed) = parse_order_by_element(column) {
                    pairs.extend(parsed);
                }
            }
            if pairs.is_empty() {
                return Err(PlanError::InvalidPlan(
                    "orderBy columns could not be resolved to sortable names".into(),
                ));
            }
            let names = pairs
                .iter()
                .map(|(name, _)| df.resolve_column_name(name))
                .collect::<Result<Vec<_>, _>>()
                .map_err(PlanError::Session)?;
            let ascending = pairs
                .iter()
                .map(|(_, ascending)| *ascending)
                .collect::<Vec<_>>();
            if let Some(nulls_last) = nulls_last {
                let nulls_last = nulls_last
                    .iter()
                    .filter_map(Value::as_bool)
                    .collect::<Vec<_>>();
                let orders = names
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        let asc = ascending.get(index).copied().unwrap_or(true);
                        let last = nulls_last.get(index).copied().unwrap_or(asc);
                        let column = col(name.as_str());
                        match (asc, last) {
                            (true, true) => asc_nulls_last(&column),
                            (true, false) => asc_nulls_first(&column),
                            (false, true) => desc_nulls_last(&column),
                            (false, false) => desc_nulls_first(&column),
                        }
                    })
                    .collect::<Vec<_>>();
                df.order_by_exprs(orders).map_err(PlanError::Session)
            } else {
                let refs = names.iter().map(String::as_str).collect::<Vec<_>>();
                df.order_by(refs, ascending).map_err(PlanError::Session)
            }
        }
        (OperationKind::Distinct, PlanPayload::Empty) => {
            df.distinct(None).map_err(PlanError::Session)
        }
        (OperationKind::Drop, PlanPayload::Drop { columns }) => {
            let names = columns
                .iter()
                .filter_map(|value| {
                    value
                        .as_str()
                        .map(String::from)
                        .or_else(|| expr_to_col_name(value))
                })
                .map(|name| df.resolve_column_name(&name))
                .collect::<Result<Vec<_>, _>>()
                .map_err(PlanError::Session)?;
            let refs = names.iter().map(String::as_str).collect::<Vec<_>>();
            df.drop(refs).map_err(PlanError::Session)
        }
        (OperationKind::WithColumnRenamed, PlanPayload::Rename { old, new }) => {
            let old = df.resolve_column_name(old).map_err(PlanError::Session)?;
            df.with_column_renamed(&old, new)
                .map_err(PlanError::Session)
        }
        (OperationKind::WithColumn, PlanPayload::WithColumn { name, expression }) => {
            let expression = expression.to_json_value();
            if let Some(column) = try_column_from_udf_value(&expression) {
                let column = column.map_err(PlanError::Expr)?;
                df.with_column(name, &column).map_err(PlanError::Session)
            } else {
                let expression = expr_from_value(&expression).map_err(PlanError::Expr)?;
                df.with_column_expr(name, expression)
                    .map_err(PlanError::Session)
            }
        }
        (OperationKind::GroupBy, PlanPayload::GroupBy { keys, aggregations }) => {
            let keys = grouping_columns(keys, &df)?;
            let refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
            let grouped = df.group_by(refs).map_err(PlanError::Session)?;
            let aggregations = parse_aggs(aggregations, &df)?;
            grouped
                .agg(disambiguate_agg_output_names(aggregations))
                .map_err(PlanError::Session)
        }
        (OperationKind::Join, PlanPayload::Join(input)) => handle_join_op(session, df, input),
        (OperationKind::Union, PlanPayload::Union(input)) => handle_union_op(session, df, input),
        (OperationKind::UnionByName, PlanPayload::UnionByName(input)) => {
            handle_union_by_name_op(session, df, input)
        }
        (OperationKind::CrossJoin, PlanPayload::CrossJoin(input)) => {
            handle_cross_join_op(session, df, input)
        }
        (OperationKind::Rollup, PlanPayload::Rollup(input)) => {
            handle_cube_rollup_op(df, input, false)
        }
        (OperationKind::Cube, PlanPayload::Cube(input)) => handle_cube_rollup_op(df, input, true),
        _ => unreachable!("portable plan decoder guarantees operation/payload pairing"),
    }
}

fn grouping_columns(values: &[Value], df: &DataFrame) -> Result<Vec<String>, PlanError> {
    values
        .iter()
        .filter_map(|value| {
            value
                .as_str()
                .map(String::from)
                .or_else(|| {
                    value
                        .get("col")
                        .and_then(Value::as_str)
                        .or_else(|| value.get("name").and_then(Value::as_str))
                        .map(String::from)
                })
                .or_else(|| {
                    value
                        .get("expr")
                        .and_then(|expression| expr_from_value(expression).ok())
                        .and_then(|expression| {
                            polars_plan::utils::expr_output_name(&expression)
                                .ok()
                                .map(|name| name.as_str().to_string())
                        })
                })
        })
        .map(|name| df.resolve_column_name(&name).map_err(PlanError::Session))
        .collect()
}
fn binary_input_frame(session: &SparkSession, input: &BinaryInput) -> Result<DataFrame, PlanError> {
    let schema = input
        .schema
        .iter()
        .map(schema_field_to_pair)
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            PlanError::InvalidPlan("validated binary input has an invalid schema field".into())
        })?;
    let names = schema
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let rows = other_data_to_rows(&input.data, &names);
    session
        .create_dataframe_from_rows(rows, schema, false, false)
        .map_err(PlanError::Session)
}

fn handle_join_op(
    session: &SparkSession,
    df: DataFrame,
    input: &BinaryInput,
) -> Result<DataFrame, PlanError> {
    let on = input
        .on
        .as_ref()
        .ok_or_else(|| PlanError::InvalidPlan("validated join input has no join key".into()))?;
    let how = input.how.as_deref().unwrap_or("inner");
    let mut other_df = binary_input_frame(session, input)?;

    let join_type = match how {
        "left" => JoinType::Left,
        "right" => JoinType::Right,
        "outer" | "full" | "full_outer" | "fullouter" => JoinType::Outer,
        "left_semi" | "leftsemi" | "semi" => JoinType::LeftSemi,
        "left_anti" | "leftanti" | "anti" => JoinType::LeftAnti,
        "inner" => JoinType::Inner,
        other => {
            return Err(PlanError::UnsupportedOp(format!(
                "Unsupported join type: {other}. Supported: inner, left, right, outer, semi, anti."
            )));
        }
    };

    match parse_join_on_spec(on, &df)? {
        JoinOnSpec::Keys(on_keys_left) => {
            let on_keys_right = parse_join_on(on, &other_df)?;
            for (index, left_name) in on_keys_left.iter().enumerate() {
                if let Some(right_name) = on_keys_right.get(index)
                    && left_name != right_name
                {
                    other_df = other_df
                        .with_column_renamed(right_name, left_name)
                        .map_err(PlanError::Session)?;
                }
            }
            let left_refs = on_keys_left.iter().map(String::as_str).collect::<Vec<_>>();
            let right_refs = on_keys_left.iter().map(String::as_str).collect::<Vec<_>>();
            df.join_with_keys(&other_df, left_refs, right_refs, join_type, false)
                .map_err(PlanError::Session)
        }
        JoinOnSpec::Condition(expression) => {
            execute_plan_join_on_expression(df, other_df, expression, join_type)
        }
    }
}
/// Join using a boolean expression (e.g. array_contains). Uses key-based join when the
/// expression is only column equalities; otherwise cross join + filter.
fn execute_plan_join_on_expression(
    df: DataFrame,
    other_df: DataFrame,
    expr: Expr,
    join_type: JoinType,
) -> Result<DataFrame, PlanError> {
    let left_df = df.clone();
    let pairs = try_extract_join_eq_columns_all(&expr);
    if !pairs.is_empty() {
        let left_cols: HashSet<String> = left_df
            .columns()
            .map_err(PlanError::Session)?
            .into_iter()
            .collect();
        let right_cols: HashSet<String> = other_df
            .columns()
            .map_err(PlanError::Session)?
            .into_iter()
            .collect();
        let mut left_refs: Vec<String> = Vec::with_capacity(pairs.len());
        let mut right_refs: Vec<String> = Vec::with_capacity(pairs.len());
        for (key_a, key_b) in &pairs {
            let left_key = left_cols
                .iter()
                .find(|c| c.as_str() == key_a.as_str() || c.eq_ignore_ascii_case(key_a))
                .cloned();
            let right_key = right_cols
                .iter()
                .find(|c| c.as_str() == key_b.as_str() || c.eq_ignore_ascii_case(key_b))
                .cloned();
            if let (Some(lk), Some(rk)) = (left_key, right_key) {
                left_refs.push(lk);
                right_refs.push(rk);
            }
        }
        if left_refs.len() == pairs.len() {
            let only_key_equalities = expr_contains_only_join_key_equalities(&expr);
            let left_refs_str: Vec<&str> = left_refs.iter().map(|s| s.as_str()).collect();
            let right_refs_str: Vec<&str> = right_refs.iter().map(|s| s.as_str()).collect();
            let mut joined = left_df
                .join_with_keys(
                    &other_df,
                    left_refs_str.clone(),
                    right_refs_str,
                    join_type,
                    only_key_equalities,
                )
                .map_err(PlanError::Session)?;
            if !only_key_equalities {
                let filter_expr = joined
                    .resolve_expr_column_names(expr)
                    .map_err(PlanError::Session)?;
                joined = joined.filter(filter_expr).map_err(PlanError::Session)?;
            }
            return Ok(joined);
        }
    }
    check_cross_join_budget(&df, &other_df).map_err(PlanError::Session)?;
    let crossed = df.cross_join(&other_df).map_err(PlanError::Session)?;
    let filter_expr = crossed
        .resolve_expr_column_names(expr)
        .map_err(PlanError::Session)?;
    crossed.filter(filter_expr).map_err(PlanError::Session)
}

fn handle_union_op(
    session: &SparkSession,
    df: DataFrame,
    input: &BinaryInput,
) -> Result<DataFrame, PlanError> {
    let other_df = binary_input_frame(session, input)?;
    df.union(&other_df).map_err(PlanError::Session)
}

fn handle_union_by_name_op(
    session: &SparkSession,
    df: DataFrame,
    input: &BinaryInput,
) -> Result<DataFrame, PlanError> {
    let other_df = binary_input_frame(session, input)?;
    df.union_by_name(&other_df, true)
        .map_err(PlanError::Session)
}

/// Cube or rollup: compile validated grouping keys and aggregations for Polars.
fn handle_cube_rollup_op(
    df: DataFrame,
    input: &GroupInput,
    is_cube: bool,
) -> Result<DataFrame, PlanError> {
    let columns = grouping_columns(&input.keys, &df)?;
    let refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let grouped = if is_cube {
        df.cube(refs).map_err(PlanError::Session)?
    } else {
        df.rollup(refs).map_err(PlanError::Session)?
    };
    let aggregations = parse_aggs(&input.aggregations, &df)?;
    grouped
        .agg(disambiguate_agg_output_names(aggregations))
        .map_err(PlanError::Session)
}

fn handle_cross_join_op(
    session: &SparkSession,
    df: DataFrame,
    input: &BinaryInput,
) -> Result<DataFrame, PlanError> {
    let other_df = binary_input_frame(session, input)?;
    df.cross_join(&other_df).map_err(PlanError::Session)
}
fn parse_aggs(aggs: &[Value], df: &DataFrame) -> Result<Vec<polars::prelude::Expr>, PlanError> {
    use crate::Column;
    use crate::functions::{avg, count, first as rs_first, max, min, sum as rs_sum};
    use polars::prelude::len;
    use std::collections::HashMap;

    let mut out = Vec::with_capacity(aggs.len());
    let mut alias_count: HashMap<String, u32> = HashMap::new();
    for a in aggs {
        let obj = a
            .as_object()
            .ok_or_else(|| PlanError::InvalidPlan("each agg must be an object".into()))?;
        // Full expression tree (e.g. cast+alias from Sparkless); issue #1255 / #332.
        if let Some(expr_val) = obj.get("expr") {
            let expr = expr_from_value(expr_val).map_err(PlanError::Expr)?;
            let resolved = df
                .resolve_expr_column_names(expr)
                .map_err(PlanError::Session)?;
            out.push(resolved);
            continue;
        }
        // Sparkless may send "func" instead of "agg" (e.g. groupby_first_last; fixes #828–#838).
        let agg = obj
            .get("agg")
            .or_else(|| obj.get("func"))
            .and_then(Value::as_str)
            .ok_or_else(|| PlanError::InvalidPlan("agg must have 'agg' or 'func' string".into()))?;

        if agg == "python_grouped_udf" {
            return Err(PlanError::InvalidPlan(
                "python_grouped_udf aggregations are not supported in execute_plan; use built-in aggregations or apply grouped Python UDFs via the Python DataFrame API".into(),
            ));
        }

        let col_name = obj.get("column").and_then(Value::as_str);
        let c = match col_name {
            Some(name) => {
                let resolved = df.resolve_column_name(name).map_err(PlanError::Session)?;
                Column::new(resolved)
            }
            None => {
                if agg == "count" {
                    Column::new("".to_string()) // count() without column
                } else {
                    return Err(PlanError::InvalidPlan(format!(
                        "agg '{agg}' requires 'column'"
                    )));
                }
            }
        };
        // count() without column = row count per group (PySpark count(*)); use len() (#825, #824). Cast to Int64 for LongType (#734).
        let col_expr = match agg {
            "count" if col_name.map(|s| s.is_empty()).unwrap_or(true) => Column::from_expr(
                len().cast(polars::prelude::DataType::Int64),
                Some("count".to_string()),
            ),
            "count" => count(&c),
            "sum" => {
                // Integer columns: keep Int64 (PySpark sum of int -> long). Else cast to Float64 (string/numeric parity #393).
                let keep_int = col_name
                    .and_then(|n| df.get_column_data_type(n))
                    .map(|dt| {
                        matches!(
                            dt,
                            crate::schema::DataType::Short
                                | crate::schema::DataType::Long
                                | crate::schema::DataType::Integer
                        )
                    })
                    .unwrap_or(false);
                if keep_int {
                    let name = c.name().to_string();
                    Column::from_expr(c.into_expr().sum(), Some(format!("sum({})", name)))
                } else {
                    rs_sum(&c)
                }
            }
            "avg" => avg(&c),
            "min" => min(&c),
            "max" => max(&c),
            "first" => {
                let ignorenulls = obj
                    .get("ignorenulls")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                rs_first(&c, ignorenulls)
            }
            "last" => Column::from_expr(c.into_expr().last(), None),
            _ => return Err(PlanError::InvalidPlan(format!("unsupported agg: {agg}"))),
        };
        let mut expr = col_expr.into_expr();
        // Optional cast (e.g. {"agg": "sum", "column": "Score", "alias": "TotalScore", "cast": "int"}); #1255, #332.
        if let Some(cast_type) = obj
            .get("cast")
            .or_else(|| obj.get("cast_type"))
            .and_then(Value::as_str)
        {
            let dtype =
                crate::functions::parse_type_name(cast_type).map_err(PlanError::InvalidPlan)?;
            expr = expr.strict_cast(dtype);
        }
        // #672, #791: PySpark-style result column names (e.g. avg(Value)) when plan does not set alias.
        // #777: Deduplicate aliases so multiple count() etc. get count, count_1, count_2, ...
        // Accept "name" as fallback for "alias" (Sparkless may send either).
        let base_alias = obj
            .get("alias")
            .or_else(|| obj.get("name"))
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| match (agg, col_name) {
                ("count", None) => "count".to_string(),
                (a, Some(col)) => format!("{}({})", a, col),
                (a, None) => format!("{}({})", a, ""),
            });
        let count = alias_count.entry(base_alias.clone()).or_insert(0);
        *count += 1;
        let alias = if *count == 1 {
            base_alias
        } else {
            format!("{}_{}", base_alias, *count - 1)
        };
        expr = expr.alias(&alias);
        out.push(expr);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn malformed_plan_is_rejected_before_initial_dataframe_validation() {
        let session = crate::session::SparkSession::builder()
            .app_name("plan_decode_before_dataframe")
            .get_or_create();
        let result = execute_plan(
            &session,
            vec![vec![json!(1), json!(2)]],
            vec![("id".to_string(), "bigint".to_string())],
            &[json!({"op": "limit", "payload": {"n": -1}})],
        );
        let error = match result {
            Ok(_) => panic!("plan decoding must fail before dataframe construction"),
            Err(error) => error,
        };

        assert!(matches!(error, PlanError::InvalidPlan(_)));
        assert!(error.to_string().contains("nonnegative integer"));
    }

    /// #672: groupBy + agg without alias produces PySpark-style column name (e.g. avg(Value)).
    #[test]
    fn test_groupby_agg_column_name_avg_value() {
        let session = crate::session::SparkSession::builder()
            .app_name("plan_agg_alias")
            .get_or_create();
        let data = vec![
            vec![json!("Alice"), json!(5.0)],
            vec![json!("Alice"), json!(6.0)],
            vec![json!("Bob"), json!(5.0)],
        ];
        let schema = vec![
            ("Name".to_string(), "string".to_string()),
            ("Value".to_string(), "double".to_string()),
        ];
        let plan = vec![json!({
            "op": "groupBy",
            "payload": {
                "group_by": ["Name"],
                "aggs": [{"agg": "avg", "column": "Value"}]
            }
        })];
        let df = execute_plan(&session, data, schema, &plan).unwrap();
        let out = df.collect_inner().unwrap();
        let names = out.get_column_names();
        assert!(
            names.iter().any(|s| s.as_str() == "avg(Value)"),
            "expected column 'avg(Value)' in {:?}",
            names
        );
    }

    #[test]
    fn test_cross_join_plan() {
        let session = crate::session::SparkSession::builder()
            .app_name("plan_cross_join")
            .get_or_create();
        let data = vec![vec![json!(1)], vec![json!(2)]];
        let schema = vec![("a".to_string(), "bigint".to_string())];
        let plan = vec![json!({
            "op": "crossJoin",
            "payload": {
                "other_data": [[3], [4]],
                "other_schema": [{"name": "b", "type": "bigint"}]
            }
        })];
        let df = execute_plan(&session, data, schema, &plan).unwrap();
        let out = df.collect_inner().unwrap();
        assert_eq!(out.height(), 4, "cross join 2x2 = 4 rows");
        assert_eq!(out.get_column_names(), &["a", "b"]);
    }

    /// Join with expression object in "on" (e.g. array_contains) uses cross join + filter.
    #[test]
    fn test_join_on_expression_object() {
        let session = crate::session::SparkSession::builder()
            .app_name("plan_join_on_expr")
            .get_or_create();
        let data = vec![
            vec![json!(1), json!(json!(["a", "b"]))],
            vec![json!(2), json!(json!(["c"]))],
        ];
        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("tags".to_string(), "array<string>".to_string()),
        ];
        let plan = vec![json!({
            "op": "join",
            "payload": {
                "on": {
                    "fn": "array_contains",
                    "args": [
                        {"col": "tags"},
                        {"col": "tag"}
                    ]
                },
                "how": "inner",
                "other_data": [["a"], ["y"]],
                "other_schema": [{"name": "tag", "type": "string"}]
            }
        })];
        let df = execute_plan(&session, data, schema, &plan).expect("expression join");
        let out = df.collect_inner().expect("collect");
        assert!(
            out.height() >= 1,
            "array_contains join should match at least one row"
        );
    }

    /// Legacy string expression in "on" still returns a clear error (use JSON expr objects).
    #[test]
    fn test_join_on_expression_string_rejected() {
        let session = crate::session::SparkSession::builder()
            .app_name("plan_join_on_expr_str")
            .get_or_create();
        let data = vec![vec![json!(1), json!("a")]];
        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("x".to_string(), "string".to_string()),
        ];
        let plan = vec![json!({
            "op": "join",
            "payload": {
                "on": "array_contains(col, x)",
                "how": "inner",
                "other_data": [[1, "b"]],
                "other_schema": [{"name": "id", "type": "bigint"}, {"name": "x", "type": "string"}]
            }
        })];
        let msg = match execute_plan(&session, data, schema, &plan) {
            Ok(_) => panic!("string expression join should fail"),
            Err(e) => e.to_string(),
        };
        assert!(
            msg.contains("join on expression") || msg.contains("use column names only"),
            "error should explain string expression join: {}",
            msg
        );
    }

    #[test]
    fn test_order_by_col_desc_and_list_format() {
        let session = crate::session::SparkSession::builder()
            .app_name("plan_orderby_desc")
            .get_or_create();
        let data = vec![
            vec![json!(1), json!("z")],
            vec![json!(2), json!("a")],
            vec![json!(3), json!("m")],
        ];
        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let plan = vec![json!({
            "op": "orderBy",
            "payload": { "columns": ["name DESC"] }
        })];
        let df = execute_plan(&session, data.clone(), schema.clone(), &plan).unwrap();
        assert_eq!(df.count().unwrap(), 3);
        let rows = df.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("name").and_then(|v| v.as_str()), Some("z"));
        let plan2 = vec![json!({
            "op": "orderBy",
            "payload": { "columns": ["['id','name']"] }
        })];
        let df2 = execute_plan(&session, data, schema, &plan2).unwrap();
        assert_eq!(df2.collect_inner().unwrap().height(), 3);
    }

    /// PR-1/#860,#874,#876: sin, cos, tan are supported in plan (expr_from_value); regression test.
    #[test]
    fn test_plan_select_sin_cos_tan() {
        let session = crate::session::SparkSession::builder()
            .app_name("plan_trig")
            .get_or_create();
        let pi_2 = std::f64::consts::FRAC_PI_2;
        let data = vec![vec![json!(0.0)], vec![json!(pi_2)]];
        let schema = vec![("x".to_string(), "double".to_string())];
        let plan = vec![json!({
            "op": "select",
            "payload": [
                {"name": "x", "expr": {"col": "x"}},
                {"name": "s", "expr": {"fn": "sin", "args": [{"col": "x"}]}},
                {"name": "c", "expr": {"fn": "cos", "args": [{"col": "x"}]}},
                {"name": "t", "expr": {"fn": "tan", "args": [{"col": "x"}]}}
            ]
        })];
        let df = execute_plan(&session, data, schema, &plan).unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let out = df.collect_inner().unwrap();
        assert_eq!(out.height(), 2);
        // sin(0)=0, cos(0)=1, tan(0)=0; sin(pi/2)=1, cos(pi/2)~0
        let s_col = out.column("s").unwrap();
        let c_col = out.column("c").unwrap();
        assert_eq!(s_col.f64().unwrap().get(0), Some(0.0));
        assert!((c_col.f64().unwrap().get(0).unwrap() - 1.0).abs() < 1e-10);
        assert!((s_col.f64().unwrap().get(1).unwrap() - 1.0).abs() < 1e-10);
    }

    /// PR-5/#873,#884: select and drop accept column refs as {"col": "name"} (Sparkless PyColumn).
    #[test]
    fn test_plan_select_drop_column_ref_objects() {
        let session = crate::session::SparkSession::builder()
            .app_name("plan_select_col_ref")
            .get_or_create();
        let data = vec![
            vec![json!(1), json!("x"), json!(10)],
            vec![json!(2), json!("y"), json!(20)],
        ];
        let schema = vec![
            ("a".to_string(), "bigint".to_string()),
            ("b".to_string(), "string".to_string()),
            ("c".to_string(), "bigint".to_string()),
        ];
        // Select using [{"col": "a"}, {"col": "b"}] instead of ["a", "b"]
        let plan_select = vec![json!({
            "op": "select",
            "payload": [{"col": "a"}, {"col": "b"}]
        })];
        let df = execute_plan(&session, data.clone(), schema.clone(), &plan_select).unwrap();
        let out = df.collect_inner().unwrap();
        assert_eq!(out.get_column_names(), &["a", "b"]);
        assert_eq!(out.height(), 2);
        // Drop using [{"col": "b"}] to leave a, c
        let plan_drop = vec![
            json!({"op": "select", "payload": [{"col": "a"}, {"col": "b"}, {"col": "c"}]}),
            json!({"op": "drop", "payload": {"columns": [{"col": "b"}]}}),
        ];
        let df2 = execute_plan(&session, data, schema, &plan_drop).unwrap();
        let out2 = df2.collect_inner().unwrap();
        assert_eq!(out2.get_column_names(), &["a", "c"]);
    }

    /// Batch 6 / #714: create_dataframe_from_rows with null in rows then plan filter/select.
    #[test]
    fn test_plan_create_dataframe_from_rows_with_nulls() {
        let session = crate::session::SparkSession::builder()
            .app_name("plan_nulls")
            .get_or_create();
        let data = vec![
            vec![json!(1), json!("a"), serde_json::Value::Null],
            vec![json!(2), serde_json::Value::Null, json!(20)],
        ];
        let schema = vec![
            ("id".to_string(), "bigint".to_string()),
            ("name".to_string(), "string".to_string()),
            ("value".to_string(), "bigint".to_string()),
        ];
        let plan = vec![
            json!({"op": "filter", "payload": {"op": "gt", "left": {"col": "id"}, "right": {"lit": 0}}}),
            json!({"op": "select", "payload": ["id", "name", "value"]}),
        ];
        let df = execute_plan(&session, data, schema, &plan).unwrap();
        assert_eq!(df.count().unwrap(), 2);
        let rows = df.collect_as_json_rows().unwrap();
        assert_eq!(rows[0].get("value"), Some(&serde_json::Value::Null));
        assert_eq!(rows[1].get("name"), Some(&serde_json::Value::Null));
    }
}
