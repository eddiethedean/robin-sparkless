//! Plan interpreter: execute a serialized logical plan (list of ops) using the existing DataFrame API.
//!
//! See [LOGICAL_PLAN_FORMAT.md](../../../docs/LOGICAL_PLAN_FORMAT.md) for the plan and expression schema.

mod expr;

use crate::dataframe::{DataFrame, JoinType, disambiguate_agg_output_names};
use crate::functions::{
    SortOrder, asc_nulls_first, asc_nulls_last, col, desc_nulls_first, desc_nulls_last,
};
use crate::plan::expr::{expr_from_value, try_column_from_udf_value};
use crate::session::{SparkSession, set_thread_udf_session};
pub use expr::PlanExprError;
use polars::prelude::PolarsError;
use serde_json::Value;

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
    set_thread_udf_session(session.clone());
    let mut df = session
        .create_dataframe_from_rows(data, schema, false, false)
        .map_err(PlanError::Session)?
        .with_case_insensitive_column_resolution();

    for op_value in plan {
        let op_obj = op_value
            .as_object()
            .ok_or_else(|| PlanError::InvalidPlan("each plan step must be a JSON object".into()))?;
        let op_name = op_obj
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| PlanError::InvalidPlan("each plan step must have 'op' string".into()))?;
        let mut payload = op_obj.get("payload").cloned().unwrap_or(Value::Null);
        // Sparkless may put other_data/other_schema at op level (sibling to payload) or use camelCase.
        // Merge into payload so apply_op finds them (issue #510).
        if matches!(op_name, "join" | "union" | "unionByName" | "crossJoin") {
            payload = merge_other_into_payload(payload, op_obj);
        }

        df = apply_op(session, df, op_name, payload)?;
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

/// Merge other_data/other_schema from op into payload if missing. Supports snake_case and camelCase.
fn merge_other_into_payload(payload: Value, op: &serde_json::Map<String, Value>) -> Value {
    fn get(obj: &serde_json::Map<String, Value>, snake: &str, camel: &str) -> Option<Value> {
        obj.get(snake).or_else(|| obj.get(camel)).cloned()
    }
    let mut p = match payload {
        Value::Object(m) => m,
        _ => return payload,
    };
    if p.get("other_data").or_else(|| p.get("otherData")).is_none() {
        if let Some(v) = get(op, "other_data", "otherData") {
            p.insert("other_data".into(), v);
        }
    }
    if p.get("other_schema")
        .or_else(|| p.get("otherSchema"))
        .is_none()
    {
        if let Some(v) = get(op, "other_schema", "otherSchema") {
            p.insert("other_schema".into(), v);
        }
    }
    if p.get("on").is_none() {
        if let Some(v) = get(op, "on", "on") {
            p.insert("on".into(), v);
        }
    }
    Value::Object(p)
}

/// Get other_data from payload (snake_case or camelCase).
fn get_other_data(payload: &Value) -> Option<&Vec<Value>> {
    payload
        .get("other_data")
        .or_else(|| payload.get("otherData"))
        .and_then(Value::as_array)
}

/// Get other_schema from payload (snake_case or camelCase).
fn get_other_schema(payload: &Value) -> Option<&Vec<Value>> {
    payload
        .get("other_schema")
        .or_else(|| payload.get("otherSchema"))
        .and_then(Value::as_array)
}

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
            obj.get("dataType")?.get("typeName").and_then(Value::as_str)
        })?
        .to_string();
    Some((name, ty))
}

fn apply_op(
    session: &SparkSession,
    df: DataFrame,
    op_name: &str,
    payload: Value,
) -> Result<DataFrame, PlanError> {
    match op_name {
        "stop" => {
            let _ = payload;
            session.stop();
            Ok(df)
        }
        "filter" => {
            let expr = expr_from_value(&payload).map_err(PlanError::Expr)?;
            df.filter(expr).map_err(PlanError::Session)
        }
        "select" => {
            // Accept payload as array or as object with "columns" (Sparkless: {"columns": [...]}).
            // #689, #688: Accept mixed Column/string: each item may be string, {col/column/name}, or {name, expr}.
            let arr = payload
                .as_array()
                .or_else(|| payload.get("columns").and_then(Value::as_array));
            if let Some(arr) = arr {
                if arr.is_empty() {
                    return Err(PlanError::InvalidPlan(
                        "select payload must be non-empty array".into(),
                    ));
                }
                let mut exprs = Vec::with_capacity(arr.len());
                for (idx, v) in arr.iter().enumerate() {
                    if let Some(obj) = v.as_object() {
                        if let Some(expr_val) = obj.get("expr") {
                            // Column-like expression: {name: "<alias>", expr: <expr>}
                            let name = obj
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("_c"); // default alias if Sparkless omits
                            let expr = expr_from_value(expr_val).map_err(PlanError::Expr)?;
                            let resolved = df
                                .resolve_expr_column_names(expr)
                                .map_err(PlanError::Session)?;
                            exprs.push(resolved.alias(name));
                            continue;
                        }
                        // #970: accept bare expression object (e.g. {"fn": "alias", "args": [...]} or {"op": "add", ...})
                        if obj.contains_key("fn") || obj.contains_key("op") {
                            if let Ok(expr) = expr_from_value(v) {
                                let resolved = df
                                    .resolve_expr_column_names(expr)
                                    .map_err(PlanError::Session)?;
                                let alias = obj
                                    .get("fn")
                                    .and_then(Value::as_str)
                                    .filter(|s| *s == "alias")
                                    .and_then(|_| obj.get("args").and_then(Value::as_array))
                                    .and_then(|a| a.last())
                                    .and_then(Value::as_str)
                                    .map(String::from)
                                    .unwrap_or_else(|| format!("_c{idx}"));
                                exprs.push(resolved.alias(alias));
                                continue;
                            }
                        }
                    }
                    // Column name: string or {col/column/name}
                    let name_str: String = if let Some(s) = v.as_str() {
                        s.to_string()
                    } else if let Some(obj) = v.as_object() {
                        expr_to_col_name(v)
                            .or_else(|| obj.get("name").and_then(Value::as_str).map(String::from))
                            .ok_or_else(|| {
                                PlanError::InvalidPlan(
                                    "select item must be string, {col/column/name}, or {name, expr}".into(),
                                )
                            })?
                    } else {
                        return Err(PlanError::InvalidPlan(
                            "select payload must be list of column name strings or {name, expr} or {col/column/name} objects".into(),
                        ));
                    };
                    // #794: "concat(a, b) as full_name" -> parse concat expr if present
                    let s = name_str.trim();
                    let (expr_str, alias_override) = if let Some(ix) = s.rfind(" as ") {
                        (s[..ix].trim(), Some(s[ix + 4..].trim())) // 4 = len(" as ")
                    } else {
                        (s, None)
                    };
                    if let Some(expr) =
                        crate::plan::expr::try_parse_concat_expr_from_string(expr_str)
                    {
                        let resolved = df
                            .resolve_expr_column_names(expr)
                            .map_err(PlanError::Session)?;
                        let alias = alias_override.unwrap_or(s);
                        exprs.push(resolved.alias(alias));
                    } else {
                        let resolved = df
                            .resolve_column_name(expr_str)
                            .map_err(PlanError::Session)?;
                        exprs.push(polars::prelude::col(resolved));
                    }
                }
                df.select_exprs(exprs).map_err(PlanError::Session)
            } else {
                Err(PlanError::InvalidPlan(
                    "select payload must be array of column names or {name, expr} objects, or object with 'columns' array".into(),
                ))
            }
        }
        "limit" => {
            let n = payload.get("n").and_then(Value::as_u64).ok_or_else(|| {
                PlanError::InvalidPlan("limit payload must have 'n' number".into())
            })?;
            df.limit(n as usize).map_err(PlanError::Session)
        }
        "offset" => {
            let n = payload.get("n").and_then(Value::as_u64).unwrap_or(0);
            df.offset(n as usize).map_err(PlanError::Session)
        }
        "orderBy" => {
            let columns = payload
                .get("columns")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    PlanError::InvalidPlan("orderBy payload must have 'columns' array".into())
                })?;
            // Each element: string ("col", "col DESC", "col ASC", "['a','b']"), or object {"col":"name"} / {"name":"x"} (PR-D).
            let mut pairs: Vec<(String, bool)> = Vec::new();
            for v in columns.iter() {
                if let Some(parsed) = parse_order_by_element(v) {
                    pairs.extend(parsed);
                }
            }
            if pairs.is_empty() {
                return Err(PlanError::InvalidPlan(
                    "orderBy columns could not be parsed (expect column names, 'col ASC'/'col DESC', or ['a','b'])".into(),
                ));
            }
            let col_names: Vec<String> = pairs
                .iter()
                .map(|(s, _)| df.resolve_column_name(s.as_str()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(PlanError::Session)?;
            let ascending: Vec<bool> = pairs.iter().map(|(_, asc)| *asc).collect();
            let nulls_last = payload
                .get("nulls_last")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_bool()).collect::<Vec<_>>());
            if let Some(nl) = nulls_last {
                let mut sort_orders: Vec<SortOrder> = Vec::with_capacity(col_names.len());
                for (i, name) in col_names.iter().enumerate() {
                    let asc = ascending.get(i).copied().unwrap_or(true);
                    let nlast = nl.get(i).copied().unwrap_or(asc);
                    let column = col(name.as_str());
                    let so = if asc {
                        if nlast {
                            asc_nulls_last(&column)
                        } else {
                            asc_nulls_first(&column)
                        }
                    } else if nlast {
                        desc_nulls_last(&column)
                    } else {
                        desc_nulls_first(&column)
                    };
                    sort_orders.push(so);
                }
                df.order_by_exprs(sort_orders).map_err(PlanError::Session)
            } else {
                let refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
                df.order_by(refs, ascending).map_err(PlanError::Session)
            }
        }
        "distinct" => df.distinct(None).map_err(PlanError::Session),
        "drop" => {
            let columns = payload
                .get("columns")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    PlanError::InvalidPlan("drop payload must have 'columns' array".into())
                })?;
            let names: Vec<String> = columns
                .iter()
                .filter_map(|v| {
                    v.as_str().map(String::from).or_else(|| expr_to_col_name(v))
                })
                .map(|s| df.resolve_column_name(s.as_str()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(PlanError::Session)?;
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            df.drop(refs).map_err(PlanError::Session)
        }
        "withColumnRenamed" => {
            let old_name = payload.get("old").and_then(Value::as_str).ok_or_else(|| {
                PlanError::InvalidPlan("withColumnRenamed must have 'old'".into())
            })?;
            let new_name = payload.get("new").and_then(Value::as_str).ok_or_else(|| {
                PlanError::InvalidPlan("withColumnRenamed must have 'new'".into())
            })?;
            let resolved_old = df
                .resolve_column_name(old_name)
                .map_err(PlanError::Session)?;
            df.with_column_renamed(&resolved_old, new_name)
                .map_err(PlanError::Session)
        }
        "withColumn" => {
            // #767, #766: column name from "name" or "alias" so cast column appears in schema.
            let name = payload
                .get("name")
                .or_else(|| payload.get("alias"))
                .and_then(Value::as_str)
                .ok_or_else(|| PlanError::InvalidPlan("withColumn must have 'name' or 'alias'".into()))?;
            let expr_val = payload
                .get("expr")
                .ok_or_else(|| PlanError::InvalidPlan("withColumn must have 'expr'".into()))?;
            if let Some(res) = try_column_from_udf_value(expr_val) {
                let col = res.map_err(PlanError::Expr)?;
                df.with_column(name, &col).map_err(PlanError::Session)
            } else {
                let expr = expr_from_value(expr_val).map_err(PlanError::Expr)?;
                df.with_column_expr(name, expr).map_err(PlanError::Session)
            }
        }
        "groupBy" => {
            let group_by = payload
                .get("group_by")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    PlanError::InvalidPlan("groupBy must have 'group_by' array".into())
                })?;
            // Each element: string, {"col"/"name": "x"}, or {"expr": <expr>} (#800: Column/expr as group key).
            let cols: Vec<String> = group_by
                .iter()
                .filter_map(|v| {
                    v.as_str()
                        .map(|s| s.to_string())
                        .or_else(|| {
                            v.get("col")
                                .and_then(Value::as_str)
                                .map(|s| s.to_string())
                                .or_else(|| {
                                    v.get("name").and_then(Value::as_str).map(|s| s.to_string())
                                })
                        })
                        .or_else(|| {
                            // Expression form: resolve to output column name for group key.
                            v.get("expr")
                                .and_then(|e| expr_from_value(e).ok())
                                .and_then(|expr| {
                                    polars_plan::utils::expr_output_name(&expr)
                                        .ok()
                                        .map(|s| s.as_str().to_string())
                                })
                        })
                })
                .map(|s| df.resolve_column_name(s.as_str()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(PlanError::Session)?;
            let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            let grouped = df.group_by(refs).map_err(PlanError::Session)?;
            // Sparkless may send "aggregations" instead of "aggs" (fixes #828–#838).
            let aggs = payload
                .get("aggs")
                .or_else(|| payload.get("aggregations"))
                .and_then(Value::as_array);
            match aggs {
                Some(aggs_arr) => {
                    let agg_exprs = parse_aggs(aggs_arr, &df)?;
                    let disambiguated = disambiguate_agg_output_names(agg_exprs);
                    grouped.agg(disambiguated).map_err(PlanError::Session)
                }
                None => Err(PlanError::InvalidPlan(
                    "groupBy payload must include 'aggs' array (e.g. [{\"agg\": \"sum\", \"column\": \"b\"}])".into(),
                )),
            }
        }
        "join" => {
            let other_data = get_other_data(&payload)
                .ok_or_else(|| PlanError::InvalidPlan("join must have 'other_data'".into()))?;
            let other_schema = get_other_schema(&payload)
                .ok_or_else(|| PlanError::InvalidPlan("join must have 'other_schema'".into()))?;
            let on = payload.get("on").ok_or_else(|| {
                PlanError::InvalidPlan("join must have 'on' array or string".into())
            })?;
            let how = payload
                .get("how")
                .and_then(Value::as_str)
                .unwrap_or("inner");

            let schema_vec: Vec<(String, String)> = other_schema
                .iter()
                .filter_map(schema_field_to_pair)
                .collect();
            let schema_names: Vec<String> = schema_vec.iter().map(|(n, _)| n.clone()).collect();
            let rows = other_data_to_rows(other_data, &schema_names);
            let other_df = session
                .create_dataframe_from_rows(rows, schema_vec, false, false)
                .map_err(PlanError::Session)?;

            let on_keys_left = parse_join_on(on, &df)?;
            // Align right join key column names to left's (e.g. left "Dept_Id" vs right "dept_id" -> rename right to "Dept_Id") (#552).
            let mut other_df = other_df;
            let on_keys_right = parse_join_on(on, &other_df)?;
            for (i, left_name) in on_keys_left.iter().enumerate() {
                if let Some(right_name) = on_keys_right.get(i) {
                    if left_name != right_name {
                        other_df = other_df
                            .with_column_renamed(right_name, left_name)
                            .map_err(PlanError::Session)?;
                    }
                }
            }
            let on_refs: Vec<&str> = on_keys_left.iter().map(|s| s.as_str()).collect();
            let join_type = match how {
                "left" => JoinType::Left,
                "right" => JoinType::Right,
                "outer" => JoinType::Outer,
                "left_semi" | "leftsemi" | "semi" => JoinType::LeftSemi,
                "left_anti" | "leftanti" | "anti" => JoinType::LeftAnti,
                _ => JoinType::Inner,
            };
            df.join(&other_df, on_refs, join_type)
                .map_err(PlanError::Session)
        }
        "union" => {
            let other_data = get_other_data(&payload)
                .ok_or_else(|| PlanError::InvalidPlan("union must have 'other_data'".into()))?;
            let other_schema = get_other_schema(&payload)
                .ok_or_else(|| PlanError::InvalidPlan("union must have 'other_schema'".into()))?;
            let schema_vec: Vec<(String, String)> = other_schema
                .iter()
                .filter_map(schema_field_to_pair)
                .collect();
            let schema_names: Vec<String> = schema_vec.iter().map(|(n, _)| n.clone()).collect();
            let rows = other_data_to_rows(other_data, &schema_names);
            let other_df = session
                .create_dataframe_from_rows(rows, schema_vec, false, false)
                .map_err(PlanError::Session)?;
            df.union(&other_df).map_err(PlanError::Session)
        }
        "unionByName" => {
            let other_data = get_other_data(&payload).ok_or_else(|| {
                PlanError::InvalidPlan("unionByName must have 'other_data'".into())
            })?;
            let other_schema = get_other_schema(&payload).ok_or_else(|| {
                PlanError::InvalidPlan("unionByName must have 'other_schema'".into())
            })?;
            let schema_vec: Vec<(String, String)> = other_schema
                .iter()
                .filter_map(schema_field_to_pair)
                .collect();
            let schema_names: Vec<String> = schema_vec.iter().map(|(n, _)| n.clone()).collect();
            let rows = other_data_to_rows(other_data, &schema_names);
            let other_df = session
                .create_dataframe_from_rows(rows, schema_vec, false, false)
                .map_err(PlanError::Session)?;
            df.union_by_name(&other_df, true)
                .map_err(PlanError::Session)
        }
        "crossJoin" | "cross_join" => {
            let other_data = get_other_data(&payload)
                .ok_or_else(|| PlanError::InvalidPlan("crossJoin must have 'other_data'".into()))?;
            let other_schema = get_other_schema(&payload).ok_or_else(|| {
                PlanError::InvalidPlan("crossJoin must have 'other_schema'".into())
            })?;
            let schema_vec: Vec<(String, String)> = other_schema
                .iter()
                .filter_map(schema_field_to_pair)
                .collect();
            let schema_names: Vec<String> = schema_vec.iter().map(|(n, _)| n.clone()).collect();
            let rows = other_data_to_rows(other_data, &schema_names);
            let other_df = session
                .create_dataframe_from_rows(rows, schema_vec, false, false)
                .map_err(PlanError::Session)?;
            df.cross_join(&other_df).map_err(PlanError::Session)
        }
        "rollup" => Err(PlanError::UnsupportedOp(
            "Plan op 'rollup' is not yet supported. Use groupBy for now. See docs for supported operations.".into(),
        )),
        "cube" => Err(PlanError::UnsupportedOp(
            "Plan op 'cube' is not yet supported. Use groupBy for now. See docs for supported operations.".into(),
        )),
        _ => Err(PlanError::UnsupportedOp(format!(
            "Plan op '{op_name}' is not supported. See docs for supported operations (e.g. select, filter, groupBy, join, orderBy, limit)."
        ))),
    }
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
        // Sparkless may send "func" instead of "agg" (e.g. groupby_first_last; fixes #828–#838).
        let agg = obj
            .get("agg")
            .or_else(|| obj.get("func"))
            .and_then(Value::as_str)
            .ok_or_else(|| PlanError::InvalidPlan("agg must have 'agg' or 'func' string".into()))?;

        if agg == "python_grouped_udf" {
            // Grouped Python UDF aggregations are not expressible as pure Expr; the plan
            // interpreter currently supports only Rust/built-in aggregations at this level.
            return Err(PlanError::InvalidPlan(
                "python_grouped_udf aggregations are not yet supported in execute_plan; use built-in aggregations in plans for now".into(),
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
            "sum" => rs_sum(&c),
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

    /// #704, #698: Join with expression in "on" (e.g. array_contains) returns clear error.
    #[test]
    fn test_join_on_expression_returns_clear_error() {
        let session = crate::session::SparkSession::builder()
            .app_name("plan_join_on_expr")
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
        let result = execute_plan(&session, data, schema, &plan);
        let err = match result {
            Ok(_) => panic!("join on expression should fail"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("join on expression") || msg.contains("use column names only"),
            "error should explain join-on expression not supported: {}",
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
