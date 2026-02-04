//! Plan interpreter: execute a serialized logical plan (list of ops) using the existing DataFrame API.
//!
//! See [LOGICAL_PLAN_FORMAT.md](../../../docs/LOGICAL_PLAN_FORMAT.md) for the plan and expression schema.

mod expr;

use crate::dataframe::{DataFrame, JoinType};
use crate::plan::expr::expr_from_value;

pub use expr::PlanExprError;
use crate::session::SparkSession;
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
            PlanError::Session(e) => write!(f, "session/df: {}", e),
            PlanError::Expr(e) => write!(f, "expression: {}", e),
            PlanError::InvalidPlan(s) => write!(f, "invalid plan: {}", s),
            PlanError::UnsupportedOp(s) => write!(f, "unsupported op: {}", s),
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
                let mut names = Vec::with_capacity(arr.len());
                for v in arr {
                    let name = v
                        .as_str()
                        .ok_or_else(|| PlanError::InvalidPlan("select payload must be list of column name strings".into()))?;
                    names.push(name.to_string());
                }
                let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                df.select(refs).map_err(PlanError::Session)
            } else {
                Err(PlanError::InvalidPlan(
                    "select payload must be array of column names".into(),
                ))
            }
        }
        "limit" => {
            let n = payload
                .get("n")
                .and_then(Value::as_u64)
                .ok_or_else(|| PlanError::InvalidPlan("limit payload must have 'n' number".into()))?;
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
                .ok_or_else(|| PlanError::InvalidPlan("orderBy payload must have 'columns' array".into()))?;
            let col_names: Vec<String> = columns
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            let ascending = payload
                .get("ascending")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_bool())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| vec![true; col_names.len()]);
            let refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
            df.order_by(refs, ascending).map_err(PlanError::Session)
        }
        "distinct" => df.distinct(None).map_err(PlanError::Session),
        "drop" => {
            let columns = payload
                .get("columns")
                .and_then(Value::as_array)
                .ok_or_else(|| PlanError::InvalidPlan("drop payload must have 'columns' array".into()))?;
            let names: Vec<&str> = columns
                .iter()
                .filter_map(|v| v.as_str())
                .collect();
            df.drop(names).map_err(PlanError::Session)
        }
        "withColumnRenamed" => {
            let old_name = payload
                .get("old")
                .and_then(Value::as_str)
                .ok_or_else(|| PlanError::InvalidPlan("withColumnRenamed must have 'old'".into()))?;
            let new_name = payload
                .get("new")
                .and_then(Value::as_str)
                .ok_or_else(|| PlanError::InvalidPlan("withColumnRenamed must have 'new'".into()))?;
            df.with_column_renamed(old_name, new_name)
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
            let expr = expr_from_value(expr_val).map_err(PlanError::Expr)?;
            df.with_column_expr(name, expr).map_err(PlanError::Session)
        }
        "groupBy" => {
            let group_by = payload
                .get("group_by")
                .and_then(Value::as_array)
                .ok_or_else(|| PlanError::InvalidPlan("groupBy must have 'group_by' array".into()))?;
            let cols: Vec<String> = group_by
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            let grouped = df.group_by(refs).map_err(PlanError::Session)?;
            // Next op in plan should be agg; we don't peek ahead here, so we return grouped as a DataFrame.
            // Actually GroupedData is different from DataFrame - we need to handle agg in the same op or next.
            // Plan format says "groupBy" then "agg" as separate ops. So we need to either combine groupBy+agg in one step or have state. For simplicity we'll require agg to follow and handle "agg" by taking the previous result. That would require state in the loop. Simpler: have a single "groupBy" payload that can include "aggs", and if present we do group_by + agg in one go. Let me re-read the plan.
            // Plan says: groupBy payload {"group_by": ["a"]}, then next op is agg with {"aggs": [...]}. So we need to carry GroupedData. That would require execute_plan to hold either DataFrame or GroupedData. So we have two options: (1) combine groupBy+agg into one logical step in the interpreter (e.g. if we see groupBy we look for agg payload in the same or next op), or (2) have an intermediate enum. The plan format says "The next operation in the plan should be agg". So we need to peek at the next op when we see groupBy, or we add a compound op. Easiest: when we see "groupBy", we require the next op to be "agg" and we'll process them together. So in apply_op for groupBy we can't return a DataFrame. Alternative: have "groupBy" in the plan include optional "aggs" in payload; if "aggs" is present we do group_by + agg and return DataFrame. So we extend the format slightly: groupBy payload can be {"group_by": ["a"], "aggs": [{"agg": "sum", "column": "b"}]}. Then we don't need two ops. Let me implement that: if groupBy has aggs, do both and return df; otherwise return an error (unsupported: groupBy without aggs in same payload). So we'll require aggs inside groupBy for now.
            let aggs = payload.get("aggs").and_then(Value::as_array);
            match aggs {
                Some(aggs_arr) => {
                    let agg_exprs = parse_aggs(aggs_arr)?;
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
                .filter_map(|v| v.as_array().map(|a| a.clone()))
                .collect();
            let other_df = session
                .create_dataframe_from_rows(rows, schema_vec)
                .map_err(PlanError::Session)?;

            let on_keys: Vec<&str> = on.iter().filter_map(|v| v.as_str()).collect();
            let join_type = match how {
                "left" => JoinType::Left,
                "right" => JoinType::Right,
                "outer" => JoinType::Outer,
                _ => JoinType::Inner,
            };
            df.join(&other_df, on_keys, join_type)
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
                .filter_map(|v| v.as_array().map(|a| a.clone()))
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
                .ok_or_else(|| PlanError::InvalidPlan("unionByName must have 'other_data'".into()))?;
            let other_schema = payload
                .get("other_schema")
                .and_then(Value::as_array)
                .ok_or_else(|| PlanError::InvalidPlan("unionByName must have 'other_schema'".into()))?;
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
                .filter_map(|v| v.as_array().map(|a| a.clone()))
                .collect();
            let other_df = session
                .create_dataframe_from_rows(rows, schema_vec)
                .map_err(PlanError::Session)?;
            df.union_by_name(&other_df)
                .map_err(PlanError::Session)
        }
        _ => Err(PlanError::UnsupportedOp(op_name.to_string())),
    }
}

fn parse_aggs(aggs: &[Value]) -> Result<Vec<polars::prelude::Expr>, PlanError> {
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
        let col_name = obj.get("column").and_then(Value::as_str);
        let c = match col_name {
            Some(name) => Column::new(name.to_string()),
            None => {
                if agg == "count" {
                    Column::new("".to_string()) // count() without column
                } else {
                    return Err(PlanError::InvalidPlan(
                        format!("agg '{}' requires 'column'", agg).into(),
                    ));
                }
            }
        };
        let col_expr = match agg {
            "count" => count(&c),
            "sum" => rs_sum(&c),
            "avg" => avg(&c),
            "min" => min(&c),
            "max" => max(&c),
            _ => return Err(PlanError::InvalidPlan(format!("unsupported agg: {}", agg).into())),
        };
        out.push(col_expr.into_expr());
    }
    Ok(out)
}
