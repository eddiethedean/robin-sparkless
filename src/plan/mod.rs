//! Plan interpreter: execute a serialized logical plan (list of ops) using the existing DataFrame API.
//!
//! See [LOGICAL_PLAN_FORMAT.md](../../../docs/LOGICAL_PLAN_FORMAT.md) for the plan and expression schema.

mod expr;

use crate::dataframe::{DataFrame, JoinType};
use crate::plan::expr::{expr_from_value, try_column_from_udf_value};
use crate::session::{set_thread_udf_session, SparkSession};
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
        .map_err(PlanError::Session)?;

    for op_value in plan {
        let op_obj = op_value
            .as_object()
            .ok_or_else(|| PlanError::InvalidPlan("each plan step must be a JSON object".into()))?;
        let op_name = op_obj
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| PlanError::InvalidPlan("each plan step must have 'op' string".into()))?;
        let payload = op_obj.get("payload").cloned().unwrap_or(Value::Null);

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

fn apply_op(
    session: &SparkSession,
    df: DataFrame,
    op_name: &str,
    payload: Value,
) -> Result<DataFrame, PlanError> {
    match op_name {
        "filter" => {
            let expr = expr_from_value(&payload).map_err(PlanError::Expr)?;
            df.filter(expr).map_err(PlanError::Session)
        }
        "select" => {
            if let Some(arr) = payload.as_array() {
                if arr.is_empty() {
                    return Err(PlanError::InvalidPlan(
                        "select payload must be non-empty array".into(),
                    ));
                }
                let first = &arr[0];
                if first.is_object() {
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
                    // List of column name strings; any may be concat/concat_ws expression strings
                    let strings: Vec<&str> = arr
                        .iter()
                        .map(|v| {
                            v.as_str().ok_or_else(|| {
                                PlanError::InvalidPlan(
                                    "select payload must be list of column name strings or list of {name, expr} objects".into(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let has_concat = strings
                        .iter()
                        .any(|s| crate::plan::expr::try_parse_concat_expr_from_string(s).is_some());
                    if !has_concat {
                        let names: Vec<String> = strings
                            .iter()
                            .map(|s| df.resolve_column_name(s))
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(PlanError::Session)?;
                        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                        return df.select(refs).map_err(PlanError::Session);
                    }
                    let mut exprs = Vec::with_capacity(strings.len());
                    for s in strings {
                        if let Some(expr) = crate::plan::expr::try_parse_concat_expr_from_string(s)
                        {
                            let resolved = df
                                .resolve_expr_column_names(expr)
                                .map_err(PlanError::Session)?;
                            exprs.push(resolved.alias(s));
                        } else {
                            let resolved = df.resolve_column_name(s).map_err(PlanError::Session)?;
                            exprs.push(polars::prelude::col(resolved));
                        }
                    }
                    df.select_exprs(exprs).map_err(PlanError::Session)
                }
            } else {
                Err(PlanError::InvalidPlan(
                    "select payload must be array of column names or {name, expr} objects".into(),
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
            let refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
            df.order_by(refs, ascending).map_err(PlanError::Session)
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
            let other_data = payload
                .get("other_data")
                .and_then(Value::as_array)
                .ok_or_else(|| PlanError::InvalidPlan("join must have 'other_data'".into()))?;
            let other_schema = payload
                .get("other_schema")
                .and_then(Value::as_array)
                .ok_or_else(|| PlanError::InvalidPlan("join must have 'other_schema'".into()))?;
            let on = payload
                .get("on")
                .and_then(Value::as_array)
                .ok_or_else(|| PlanError::InvalidPlan("join must have 'on' array".into()))?;
            let how = payload
                .get("how")
                .and_then(Value::as_str)
                .unwrap_or("inner");

            let schema_vec: Vec<(String, String)> = other_schema
                .iter()
                .filter_map(|v| {
                    let obj = v.as_object()?;
                    let name = obj.get("name")?.as_str()?.to_string();
                    let ty = obj.get("type")?.as_str()?.to_string();
                    Some((name, ty))
                })
                .collect();
            let rows: Vec<Vec<Value>> = other_data
                .iter()
                .filter_map(|v| v.as_array().cloned())
                .collect();
            let other_df = session
                .create_dataframe_from_rows(rows, schema_vec)
                .map_err(PlanError::Session)?;

            let on_keys: Vec<String> = on
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| df.resolve_column_name(s))
                .collect::<Result<Vec<_>, _>>()
                .map_err(PlanError::Session)?;
            let on_refs: Vec<&str> = on_keys.iter().map(|s| s.as_str()).collect();
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
            let other_data = payload
                .get("other_data")
                .and_then(Value::as_array)
                .ok_or_else(|| PlanError::InvalidPlan("union must have 'other_data'".into()))?;
            let other_schema = payload
                .get("other_schema")
                .and_then(Value::as_array)
                .ok_or_else(|| PlanError::InvalidPlan("union must have 'other_schema'".into()))?;
            let schema_vec: Vec<(String, String)> = other_schema
                .iter()
                .filter_map(|v| {
                    let obj = v.as_object()?;
                    let name = obj.get("name")?.as_str()?.to_string();
                    let ty = obj.get("type")?.as_str()?.to_string();
                    Some((name, ty))
                })
                .collect();
            let rows: Vec<Vec<Value>> = other_data
                .iter()
                .filter_map(|v| v.as_array().cloned())
                .collect();
            let other_df = session
                .create_dataframe_from_rows(rows, schema_vec)
                .map_err(PlanError::Session)?;
            df.union(&other_df).map_err(PlanError::Session)
        }
        "unionByName" => {
            let other_data = payload
                .get("other_data")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    PlanError::InvalidPlan("unionByName must have 'other_data'".into())
                })?;
            let other_schema = payload
                .get("other_schema")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    PlanError::InvalidPlan("unionByName must have 'other_schema'".into())
                })?;
            let schema_vec: Vec<(String, String)> = other_schema
                .iter()
                .filter_map(|v| {
                    let obj = v.as_object()?;
                    let name = obj.get("name")?.as_str()?.to_string();
                    let ty = obj.get("type")?.as_str()?.to_string();
                    Some((name, ty))
                })
                .collect();
            let rows: Vec<Vec<Value>> = other_data
                .iter()
                .filter_map(|v| v.as_array().cloned())
                .collect();
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
    use crate::functions::{avg, count, max, min, sum as rs_sum};
    use crate::Column;

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
