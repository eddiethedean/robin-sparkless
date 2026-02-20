//! Plan interpreter: execute a serialized logical plan (list of ops) using the existing DataFrame API.
//!
//! See [LOGICAL_PLAN_FORMAT.md](../../../docs/LOGICAL_PLAN_FORMAT.md) for the plan and expression schema.

mod expr;

use crate::dataframe::{DataFrame, JoinType};
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
        .create_dataframe_from_rows(data, schema)
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
        if matches!(op_name, "join" | "union" | "unionByName") {
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
fn parse_join_on(on: &Value, df: &DataFrame) -> Result<Vec<String>, PlanError> {
    if let Some(s) = on.as_str() {
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
            // Accept payload as array or as object with "columns" (Sparkless: {"columns": [...]})
            let arr = payload
                .as_array()
                .or_else(|| payload.get("columns").and_then(Value::as_array));
            if let Some(arr) = arr {
                if arr.is_empty() {
                    return Err(PlanError::InvalidPlan(
                        "select payload must be non-empty array".into(),
                    ));
                }
                let first = &arr[0];
                let is_expr_list = first.is_object() && first.get("expr").is_some();
                if is_expr_list {
                    // Select with computed columns: [{"name": "<alias>", "expr": <expr>}, ...]
                    let mut exprs = Vec::with_capacity(arr.len());
                    for v in arr {
                        let obj = v.as_object().ok_or_else(|| {
                            PlanError::InvalidPlan(
                                "select payload with expressions must be array of {name, expr} objects".into(),
                            )
                        })?;
                        let name = obj.get("name").and_then(Value::as_str).ok_or_else(|| {
                            PlanError::InvalidPlan("select item must have 'name' string".into())
                        })?;
                        let expr_val = obj.get("expr").ok_or_else(|| {
                            PlanError::InvalidPlan("select item must have 'expr'".into())
                        })?;
                        let expr = expr_from_value(expr_val).map_err(PlanError::Expr)?;
                        let resolved = df
                            .resolve_expr_column_names(expr)
                            .map_err(PlanError::Session)?;
                        exprs.push(resolved.alias(name));
                    }
                    df.select_exprs(exprs).map_err(PlanError::Session)
                } else {
                    // Column names: strings or {type, name} / {name} (Sparkless column refs)
                    let strings: Vec<String> = arr
                        .iter()
                        .map(|v| {
                            if let Some(s) = v.as_str() {
                                Ok(s.to_string())
                            } else if let Some(obj) = v.as_object() {
                                obj.get("name")
                                    .and_then(Value::as_str)
                                    .map(|s| s.to_string())
                                    .ok_or_else(|| {
                                        PlanError::InvalidPlan(
                                            "select column item must have 'name' string".into(),
                                        )
                                    })
                            } else {
                                Err(PlanError::InvalidPlan(
                                    "select payload must be list of column name strings or {name, expr} or {type, name} objects".into(),
                                ))
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let has_concat = strings.iter().any(|s| {
                        crate::plan::expr::try_parse_concat_expr_from_string(s.as_str()).is_some()
                    });
                    if !has_concat {
                        let names: Vec<String> = strings
                            .iter()
                            .map(|s| df.resolve_column_name(s.as_str()))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(PlanError::Session)?;
                        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                        return df.select(refs).map_err(PlanError::Session);
                    }
                    let mut exprs = Vec::with_capacity(strings.len());
                    for s in &strings {
                        if let Some(expr) =
                            crate::plan::expr::try_parse_concat_expr_from_string(s.as_str())
                        {
                            let resolved = df
                                .resolve_expr_column_names(expr)
                                .map_err(PlanError::Session)?;
                            exprs.push(resolved.alias(s));
                        } else {
                            let resolved = df
                                .resolve_column_name(s.as_str())
                                .map_err(PlanError::Session)?;
                            exprs.push(polars::prelude::col(resolved));
                        }
                    }
                    df.select_exprs(exprs).map_err(PlanError::Session)
                }
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
            let col_names: Vec<String> = columns
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| df.resolve_column_name(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(PlanError::Session)?;
            let ascending = payload
                .get("ascending")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(|v| v.as_bool()).collect::<Vec<_>>())
                .unwrap_or_else(|| vec![true; col_names.len()]);
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
                .filter_map(|v| v.as_str())
                .map(|s| df.resolve_column_name(s))
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
            let name = payload
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| PlanError::InvalidPlan("withColumn must have 'name'".into()))?;
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
            let cols: Vec<String> = group_by
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| df.resolve_column_name(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(PlanError::Session)?;
            let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            let grouped = df.group_by(refs).map_err(PlanError::Session)?;
            let aggs = payload.get("aggs").and_then(Value::as_array);
            match aggs {
                Some(aggs_arr) => {
                    let agg_exprs = parse_aggs(aggs_arr, &df)?;
                    grouped.agg(agg_exprs).map_err(PlanError::Session)
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
                .create_dataframe_from_rows(rows, schema_vec)
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
                "left_semi" | "semi" => JoinType::LeftSemi,
                "left_anti" | "anti" => JoinType::LeftAnti,
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
                .create_dataframe_from_rows(rows, schema_vec)
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
                .create_dataframe_from_rows(rows, schema_vec)
                .map_err(PlanError::Session)?;
            df.union_by_name(&other_df, true)
                .map_err(PlanError::Session)
        }
        _ => Err(PlanError::UnsupportedOp(op_name.to_string())),
    }
}

fn parse_aggs(aggs: &[Value], df: &DataFrame) -> Result<Vec<polars::prelude::Expr>, PlanError> {
    use crate::Column;
    use crate::functions::{avg, count, max, min, sum as rs_sum};

    let mut out = Vec::with_capacity(aggs.len());
    for a in aggs {
        let obj = a
            .as_object()
            .ok_or_else(|| PlanError::InvalidPlan("each agg must be an object".into()))?;
        let agg = obj
            .get("agg")
            .and_then(Value::as_str)
            .ok_or_else(|| PlanError::InvalidPlan("agg must have 'agg' string".into()))?;

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
        let col_expr = match agg {
            "count" => count(&c),
            "sum" => rs_sum(&c),
            "avg" => avg(&c),
            "min" => min(&c),
            "max" => max(&c),
            "first" => Column::from_expr(c.into_expr().first(), None),
            "last" => Column::from_expr(c.into_expr().last(), None),
            _ => return Err(PlanError::InvalidPlan(format!("unsupported agg: {agg}"))),
        };
        let mut expr = col_expr.into_expr();
        if let Some(alias) = obj.get("alias").and_then(Value::as_str) {
            expr = expr.alias(alias);
        }
        out.push(expr);
    }
    Ok(out)
}
