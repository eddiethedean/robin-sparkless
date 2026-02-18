//! Expression interpreter: turn serialized expression trees (JSON/serde) into Polars Expr.
//! Used by the plan interpreter for filter, select, and withColumn payloads.

use polars::prelude::{col, lit, DataType, Expr, Series};
use serde_json::Value;
use std::error::Error;
use std::fmt;

/// Error from parsing or interpreting a plan expression.
#[derive(Debug)]
pub struct PlanExprError(String);

impl fmt::Display for PlanExprError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for PlanExprError {}

/// Convert a serialized expression tree (JSON Value) into a Polars Expr.
/// Supports: col, lit, comparison ops (eq, ne, gt, ge, lt, le), logical (and, or), not, and a subset of functions.
pub fn expr_from_value(v: &Value) -> Result<Expr, PlanExprError> {
    let obj = v
        .as_object()
        .ok_or_else(|| PlanExprError("expression must be a JSON object".to_string()))?;

    // Column reference: {"col": "name"}
    if let Some(name) = obj.get("col").and_then(Value::as_str) {
        return Ok(col(name));
    }

    // Literal: {"lit": <value>}
    if let Some(lit_val) = obj.get("lit") {
        return lit_from_value(lit_val);
    }

    // Binary op: {"op": "gt"|"eq"|..., "left": <expr>, "right": <expr>}
    if let Some(op) = obj.get("op").and_then(Value::as_str) {
        match op {
            "eq" | "ne" | "gt" | "ge" | "lt" | "le" => {
                let left_v = obj
                    .get("left")
                    .ok_or_else(|| PlanExprError(format!("op '{op}' requires 'left'")))?;
                let right_v = obj
                    .get("right")
                    .ok_or_else(|| PlanExprError(format!("op '{op}' requires 'right'")))?;
                let l = expr_from_value(left_v)?;
                let r = expr_from_value(right_v)?;

                // Best-effort type hints: literals we can infer directly; columns are left
                // without types here and will be handled by the DataFrame-level rewriter.
                use polars::prelude::LiteralValue;
                let infer_lit_type = |e: &Expr| -> Option<DataType> {
                    if let Expr::Literal(lv) = e {
                        Some(match lv {
                            LiteralValue::Int32(_) => DataType::Int32,
                            LiteralValue::Int64(_) => DataType::Int64,
                            LiteralValue::UInt32(_) => DataType::UInt32,
                            LiteralValue::UInt64(_) => DataType::UInt64,
                            LiteralValue::Float32(_) => DataType::Float32,
                            LiteralValue::Float64(_) => DataType::Float64,
                            LiteralValue::Boolean(_) => DataType::Boolean,
                            LiteralValue::String(_) => DataType::String,
                            _ => return None,
                        })
                    } else {
                        None
                    }
                };

                let l_ty = infer_lit_type(&l);
                let r_ty = infer_lit_type(&r);

                let expr = match (l_ty, r_ty) {
                    (Some(lt), Some(rt)) => {
                        use crate::type_coercion::{coerce_for_pyspark_comparison, CompareOp};
                        let op_enum = match op {
                            "eq" => CompareOp::Eq,
                            "ne" => CompareOp::NotEq,
                            "gt" => CompareOp::Gt,
                            "ge" => CompareOp::GtEq,
                            "lt" => CompareOp::Lt,
                            "le" => CompareOp::LtEq,
                            _ => unreachable!(),
                        };
                        let (lc, rc) =
                            coerce_for_pyspark_comparison(l.clone(), r.clone(), &lt, &rt, &op_enum)
                                .map_err(|e| PlanExprError(e.to_string()))?;
                        match op {
                            "eq" => lc.eq(rc),
                            "ne" => lc.neq(rc),
                            "gt" => lc.gt(rc),
                            "ge" => lc.gt_eq(rc),
                            "lt" => lc.lt(rc),
                            "le" => lc.lt_eq(rc),
                            _ => unreachable!(),
                        }
                    }
                    _ => match op {
                        "eq" => l.eq(r),
                        "ne" => l.neq(r),
                        "gt" => l.gt(r),
                        "ge" => l.gt_eq(r),
                        "lt" => l.lt(r),
                        "le" => l.lt_eq(r),
                        _ => unreachable!(),
                    },
                };

                return Ok(expr);
            }
            "eq_null_safe" => {
                let left = obj.get("left").ok_or_else(|| {
                    PlanExprError("op 'eq_null_safe' requires 'left'".to_string())
                })?;
                let right = obj.get("right").ok_or_else(|| {
                    PlanExprError("op 'eq_null_safe' requires 'right'".to_string())
                })?;
                let l = expr_from_value(left)?;
                let r = expr_from_value(right)?;
                return eq_null_safe_expr(l, r);
            }
            "and" => {
                let left = obj
                    .get("left")
                    .ok_or_else(|| PlanExprError("op 'and' requires 'left'".to_string()))?;
                let right = obj
                    .get("right")
                    .ok_or_else(|| PlanExprError("op 'and' requires 'right'".to_string()))?;
                return Ok(expr_from_value(left)?.and(expr_from_value(right)?));
            }
            "or" => {
                let left = obj
                    .get("left")
                    .ok_or_else(|| PlanExprError("op 'or' requires 'left'".to_string()))?;
                let right = obj
                    .get("right")
                    .ok_or_else(|| PlanExprError("op 'or' requires 'right'".to_string()))?;
                return Ok(expr_from_value(left)?.or(expr_from_value(right)?));
            }
            "not" => {
                let arg = obj
                    .get("arg")
                    .ok_or_else(|| PlanExprError("op 'not' requires 'arg'".to_string()))?;
                return Ok(expr_from_value(arg)?.not());
            }
            "between" => {
                let left_v = obj
                    .get("left")
                    .ok_or_else(|| PlanExprError("op 'between' requires 'left'".to_string()))?;
                let lower_v = obj
                    .get("lower")
                    .ok_or_else(|| PlanExprError("op 'between' requires 'lower'".to_string()))?;
                let upper_v = obj
                    .get("upper")
                    .ok_or_else(|| PlanExprError("op 'between' requires 'upper'".to_string()))?;
                let left = expr_from_value(left_v)?;
                let lower = expr_from_value(lower_v)?;
                let upper = expr_from_value(upper_v)?;
                return Ok(left.clone().gt_eq(lower).and(left.lt_eq(upper)));
            }
            "**" | "pow" => {
                let left_v = obj
                    .get("left")
                    .ok_or_else(|| PlanExprError(format!("op '{op}' requires 'left'")))?;
                let right_v = obj
                    .get("right")
                    .ok_or_else(|| PlanExprError(format!("op '{op}' requires 'right'")))?;
                let l = expr_from_value(left_v)?;
                let r = expr_from_value(right_v)?;
                let left_col = expr_to_column(l);
                let right_col = expr_to_column(r);
                return Ok(left_col.pow_with(&right_col).into_expr());
            }
            "isin" => {
                // {"op": "isin", "left": <col_expr>, "right": <list_expr>} or
                // {"op": "isin", "left": <col_expr>, "values": [<lit>, ...]}
                // Empty list -> lit(false) for all rows (issue #518)
                let left_v = obj
                    .get("left")
                    .ok_or_else(|| PlanExprError("op 'isin' requires 'left'".to_string()))?;
                let left_expr = expr_from_value(left_v)?;
                let values_opt =
                    if let Some(values_arr) = obj.get("values").and_then(Value::as_array) {
                        try_values_for_isin(values_arr)?
                    } else if let Some(right_v) = obj.get("right") {
                        try_values_from_plan_value(right_v)?
                    } else {
                        return Err(PlanExprError(
                            "op 'isin' requires 'right' or 'values'".to_string(),
                        ));
                    };
                return Ok(match values_opt {
                    None => lit(false),
                    Some(values_expr) => left_expr.is_in(values_expr),
                });
            }
            "getItem" => {
                // {"op": "getItem", "left": <col_expr>, "right": <index_lit>} - PySpark 0-based
                let left_v = obj
                    .get("left")
                    .ok_or_else(|| PlanExprError("op 'getItem' requires 'left'".to_string()))?;
                let right_v = obj
                    .get("right")
                    .ok_or_else(|| PlanExprError("op 'getItem' requires 'right'".to_string()))?;
                let col_expr = expr_from_value(left_v)?;
                let idx = lit_as_i64(right_v)?;
                let col_c = expr_to_column(col_expr);
                return Ok(col_c.get_item(idx).into_expr());
            }
            "startswith" => {
                // {"op": "startswith", "left": col, "right": {"lit": "prefix"}}
                let left_v = obj
                    .get("left")
                    .ok_or_else(|| PlanExprError("op 'startswith' requires 'left'".to_string()))?;
                let right_v = obj
                    .get("right")
                    .ok_or_else(|| PlanExprError("op 'startswith' requires 'right'".to_string()))?;
                let col_expr = expr_from_value(left_v)?;
                let prefix = lit_as_string(right_v)?;
                let col_c = expr_to_column(col_expr);
                return Ok(crate::functions::startswith(&col_c, &prefix).into_expr());
            }
            "is_null" => {
                let arg = obj
                    .get("arg")
                    .ok_or_else(|| PlanExprError("op 'is_null' requires 'arg'".to_string()))?;
                return Ok(expr_from_value(arg)?.is_null());
            }
            "is_not_null" => {
                let arg = obj
                    .get("arg")
                    .ok_or_else(|| PlanExprError("op 'is_not_null' requires 'arg'".to_string()))?;
                return Ok(expr_from_value(arg)?.is_not_null());
            }
            "regexp_extract" => {
                // {"op": "regexp_extract", "left": col, "pattern": {"lit": "..."}, "group": {"lit": 0}}
                // or {"op": "regexp_extract", "args": [col, pattern, group]}
                if let Some(args) = obj.get("args").and_then(Value::as_array) {
                    require_args_min("regexp_extract", args, 3)?;
                    let col_expr = expr_from_value(&args[0])?;
                    let pattern = lit_as_string(&args[1])?;
                    let group_idx = lit_as_usize(&args[2])?;
                    let col_c = expr_to_column(col_expr);
                    return Ok(
                        crate::functions::regexp_extract(&col_c, &pattern, group_idx).into_expr(),
                    );
                }
                let left_v = obj.get("left").ok_or_else(|| {
                    PlanExprError("op 'regexp_extract' requires 'left'".to_string())
                })?;
                let col_expr = expr_from_value(left_v)?;
                let pattern_v =
                    obj.get("pattern")
                        .or_else(|| obj.get("right"))
                        .ok_or_else(|| {
                            PlanExprError(
                                "op 'regexp_extract' requires 'pattern' or 'right'".to_string(),
                            )
                        })?;
                let pattern = lit_as_string(pattern_v)?;
                let group_idx = obj
                    .get("group")
                    .and_then(|v| lit_as_usize(v).ok())
                    .unwrap_or(0);
                let col_c = expr_to_column(col_expr);
                return Ok(
                    crate::functions::regexp_extract(&col_c, &pattern, group_idx).into_expr(),
                );
            }
            "regexp_replace" => {
                // {"op": "regexp_replace", "left": col, "pattern": {"lit": "..."}, "replacement": {"lit": "..."}}
                // or {"op": "regexp_replace", "args": [col, pattern, replacement]} (issue #528)
                if let Some(args) = obj.get("args").and_then(Value::as_array) {
                    require_args_min("regexp_replace", args, 3)?;
                    let col_expr = expr_from_value(&args[0])?;
                    let pattern = lit_as_string(&args[1])?;
                    let replacement = lit_as_string(&args[2])?;
                    let col_c = expr_to_column(col_expr);
                    return Ok(
                        crate::functions::regexp_replace(&col_c, &pattern, &replacement)
                            .into_expr(),
                    );
                }
                let left_v = obj.get("left").ok_or_else(|| {
                    PlanExprError("op 'regexp_replace' requires 'left'".to_string())
                })?;
                let col_expr = expr_from_value(left_v)?;
                let pattern =
                    lit_as_string(obj.get("pattern").or_else(|| obj.get("right")).ok_or_else(
                        || {
                            PlanExprError(
                                "op 'regexp_replace' requires 'pattern' or 'right'".to_string(),
                            )
                        },
                    )?)?;
                let replacement = lit_as_string(obj.get("replacement").ok_or_else(|| {
                    PlanExprError("op 'regexp_replace' requires 'replacement'".to_string())
                })?)?;
                let col_c = expr_to_column(col_expr);
                return Ok(
                    crate::functions::regexp_replace(&col_c, &pattern, &replacement).into_expr(),
                );
            }
            "create_map" | "createMap" => {
                // {"op": "create_map"|"createMap", "args": [key1, val1, key2, val2, ...]} (issue #542)
                let args_arr = obj.get("args").and_then(Value::as_array).ok_or_else(|| {
                    PlanExprError("op 'create_map'/'createMap' requires 'args' array".to_string())
                })?;
                let exprs: Result<Vec<Expr>, _> = args_arr.iter().map(expr_from_value).collect();
                let cols: Vec<crate::Column> = exprs?.into_iter().map(expr_to_column).collect();
                let refs: Vec<&crate::Column> = cols.iter().collect();
                return Ok(crate::functions::create_map(&refs)
                    .map_err(|e| PlanExprError(e.to_string()))?
                    .into_expr());
            }
            "add" | "+" => {
                // {"op": "add", "left": <expr>, "right": <expr>} - e.g. row_number().over(w) + 10
                let left_v = obj
                    .get("left")
                    .ok_or_else(|| PlanExprError("op 'add' requires 'left'".to_string()))?;
                let right_v = obj
                    .get("right")
                    .ok_or_else(|| PlanExprError("op 'add' requires 'right'".to_string()))?;
                let l = expr_from_value(left_v)?;
                let r = expr_from_value(right_v)?;
                let a = expr_to_column(l);
                let b = expr_to_column(r);
                return Ok(a.add_pyspark(&b).into_expr());
            }
            "udf" => {
                // {"op": "udf", "udf"|"name": "udf_name", "args": [<expr>, ...]} (issue #545)
                let udf_name = obj
                    .get("udf")
                    .or_else(|| obj.get("name"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        PlanExprError("op 'udf' requires 'udf' or 'name'".to_string())
                    })?;
                let args = obj
                    .get("args")
                    .and_then(Value::as_array)
                    .ok_or_else(|| PlanExprError("op 'udf' requires 'args' array".to_string()))?;
                let col = column_from_udf_call(udf_name, args)?;
                if col.udf_call.is_some() {
                    return Err(PlanExprError(
                        "Python/Vectorized UDFs are only supported in withColumn/select, not in filter/plan expressions"
                            .into(),
                    ));
                }
                return Ok(col.expr().clone());
            }
            _ => {
                return Err(PlanExprError(format!("unsupported expression op: {op}")));
            }
        }
    }

    // UDF call: {"udf": "name", "args": [<expr>, ...]} - returns Expr for Rust UDF only (Python UDF needs Column path)
    if let Some(udf_name) = obj.get("udf").and_then(Value::as_str) {
        let args = obj
            .get("args")
            .and_then(Value::as_array)
            .ok_or_else(|| PlanExprError("udf requires 'args' array".to_string()))?;
        let col = column_from_udf_call(udf_name, args)?;
        if col.udf_call.is_some() {
            return Err(PlanExprError(
                "Python/Vectorized UDFs are only supported in withColumn/select, not in filter/plan expressions"
                    .into(),
            ));
        }
        return Ok(col.expr().clone());
    }

    // Function call: {"fn"|"function": "upper"|...|"row_number", "args": [<expr>, ...], optional "window": {...}}
    // Sparkless may send "function" instead of "fn" (issue #517). Window fns allow empty args.
    let fn_name = obj
        .get("fn")
        .or_else(|| obj.get("function"))
        .and_then(Value::as_str);
    if let Some(fn_name) = fn_name {
        if let Some(window_val) = obj.get("window") {
            return expr_from_window_fn(
                fn_name,
                window_val,
                obj.get("args").and_then(Value::as_array),
            );
        }
        let args = obj
            .get("args")
            .and_then(Value::as_array)
            .ok_or_else(|| PlanExprError(format!("fn '{fn_name}' requires 'args' array")))?;
        return expr_from_fn(fn_name, args);
    }

    // type: "window" - Sparkless: {"type": "window", "fn"|"function": "row_number", "window": {...}}
    if let Some(typ) = obj.get("type").and_then(Value::as_str) {
        if typ == "window" {
            let fn_name = obj
                .get("fn")
                .or_else(|| obj.get("function"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    PlanExprError("type window requires 'fn' or 'function'".to_string())
                })?;
            let window_val = obj
                .get("window")
                .ok_or_else(|| PlanExprError("type window requires 'window'".to_string()))?;
            let args = obj.get("args").and_then(Value::as_array);
            return expr_from_window_fn(fn_name, window_val, args);
        }
    }

    Err(PlanExprError(
        "expression must have 'col', 'lit', 'op', 'fn', or 'type'".to_string(),
    ))
}

/// Extract column name from window spec item: "col" or {"col": "name"} or {"col": "name", "asc": true} (issue #517).
fn window_col_from_value(x: &Value) -> Option<String> {
    if let Some(s) = x.as_str() {
        return Some(s.to_string());
    }
    if let Some(obj) = x.as_object() {
        if let Some(name) = obj.get("col").and_then(Value::as_str) {
            return Some(name.to_string());
        }
    }
    None
}

/// Parse window spec object into (order_col_names, partition_by_col_names).
/// order_by / partition_by can be arrays of "col" or {"col": "name", "asc": true}.
/// Does not apply fallback; callers that need order column use order_cols.or(part_cols) (issue #517).
fn parse_window_spec(v: &Value) -> Result<(Vec<String>, Vec<String>), PlanExprError> {
    let obj = v
        .as_object()
        .ok_or_else(|| PlanExprError("window spec must be object".to_string()))?;
    let order_arr = obj.get("order_by").and_then(Value::as_array);
    let part_arr = obj.get("partition_by").and_then(Value::as_array);
    let order_cols: Vec<String> = order_arr
        .map(|a| a.iter().filter_map(window_col_from_value).collect())
        .unwrap_or_default();
    let part_cols: Vec<String> = part_arr
        .map(|a| a.iter().filter_map(window_col_from_value).collect())
        .unwrap_or_default();
    Ok((order_cols, part_cols))
}

/// Order column names for window: prefer order_by, fall back to partition_by (issue #517).
fn window_order_cols(order_cols: &[String], part_cols: &[String]) -> Vec<String> {
    if order_cols.is_empty() {
        part_cols.to_vec()
    } else {
        order_cols.to_vec()
    }
}

/// Build window expression for row_number (issue #517, #520).
fn expr_from_row_number_window(v: &Value) -> Result<Expr, PlanExprError> {
    let (order_cols, part_cols) = parse_window_spec(v)?;
    let part_refs: Vec<&str> = part_cols.iter().map(|s| s.as_str()).collect();
    let effective_order = window_order_cols(&order_cols, &part_cols);
    let order_col = if effective_order.is_empty() {
        crate::Column::from_expr(lit(1i32), None)
    } else {
        crate::Column::new(effective_order[0].clone())
    };
    let rn = order_col.row_number(false).over(&part_refs);
    Ok(rn.into_expr())
}

/// Dispatch window fn by name: row_number, rank, dense_rank, percent_rank, ntile, lag, lead, sum, avg (issues #517, #521).
fn expr_from_window_fn(
    fn_name: &str,
    window_val: &Value,
    args: Option<&Vec<Value>>,
) -> Result<Expr, PlanExprError> {
    use crate::Column;
    let (order_cols, part_cols) = parse_window_spec(window_val)?;
    let part_refs: Vec<&str> = part_cols.iter().map(|s| s.as_str()).collect();
    let effective_order = window_order_cols(&order_cols, &part_cols);
    let empty: &[Value] = &[];
    let args: &[Value] = args.map_or(empty, |v| v);
    let order_col = if effective_order.is_empty() {
        Column::from_expr(lit(1i32), None)
    } else {
        Column::new(effective_order[0].clone())
    };

    match fn_name {
        "row_number" => expr_from_row_number_window(window_val),
        "rank" => {
            let c = order_col.rank(false).over(&part_refs);
            Ok(c.into_expr())
        }
        "dense_rank" => {
            let c = order_col.dense_rank(false).over(&part_refs);
            Ok(c.into_expr())
        }
        "percent_rank" => {
            let c = order_col.percent_rank(&part_refs, false);
            Ok(c.into_expr())
        }
        "ntile" => {
            let n = args
                .first()
                .and_then(|v| v.get("lit").and_then(Value::as_i64))
                .or_else(|| args.first().and_then(Value::as_i64))
                .ok_or_else(|| PlanExprError("ntile window requires n (number of buckets)".to_string()))? as u32;
            let c = order_col.ntile(n.max(1), &part_refs, false);
            Ok(c.into_expr())
        }
        "lag" => {
            let n = args
                .get(1)
                .and_then(|v| v.get("lit").and_then(Value::as_i64))
                .or_else(|| args.get(1).and_then(Value::as_i64))
                .unwrap_or(1);
            let col_expr = expr_to_column(expr_from_value(
                args.first().ok_or_else(|| PlanExprError("lag window requires column arg".to_string()))?,
            )?);
            let c = col_expr.lag(n).over(&part_refs);
            Ok(c.into_expr())
        }
        "lead" => {
            let n = args
                .get(1)
                .and_then(|v| v.get("lit").and_then(Value::as_i64))
                .or_else(|| args.get(1).and_then(Value::as_i64))
                .unwrap_or(1);
            let col_expr = expr_to_column(expr_from_value(
                args.first().ok_or_else(|| PlanExprError("lead window requires column arg".to_string()))?,
            )?);
            let c = col_expr.lead(n).over(&part_refs);
            Ok(c.into_expr())
        }
        "sum" => {
            let col_expr = expr_to_column(expr_from_value(
                args.first().ok_or_else(|| PlanExprError("sum window requires column arg".to_string()))?,
            )?);
            let sum_expr = col_expr.expr().clone().sum();
            let partition_exprs: Vec<Expr> = part_refs.iter().map(|s| col(*s)).collect();
            Ok(sum_expr.over(partition_exprs))
        }
        "avg" | "mean" => {
            let col_expr = expr_to_column(expr_from_value(
                args.first().ok_or_else(|| PlanExprError("avg window requires column arg".to_string()))?,
            )?);
            let mean_expr = col_expr.expr().clone().mean();
            let partition_exprs: Vec<Expr> = part_refs.iter().map(|s| col(*s)).collect();
            Ok(mean_expr.over(partition_exprs))
        }
        _ => Err(PlanExprError(format!(
            "unsupported window fn '{fn_name}' (supported: row_number, rank, dense_rank, percent_rank, ntile, lag, lead, sum, avg)"
        ))),
    }
}

fn lit_from_value(v: &Value) -> Result<Expr, PlanExprError> {
    use polars::prelude::LiteralValue;
    if v.is_null() {
        return Ok(Expr::Literal(LiteralValue::Null));
    }
    if let Some(n) = v.as_i64() {
        return Ok(lit(n));
    }
    if let Some(n) = v.as_f64() {
        return Ok(lit(n));
    }
    if let Some(b) = v.as_bool() {
        return Ok(lit(b));
    }
    if let Some(s) = v.as_str() {
        return Ok(lit(s));
    }
    Err(PlanExprError("unsupported literal type".to_string()))
}

// --- Literal extraction from {"lit": value} (for function args) ---

fn lit_as_string(v: &Value) -> Result<String, PlanExprError> {
    let lit_val = v
        .get("lit")
        .ok_or_else(|| PlanExprError("expected literal".to_string()))?;
    if lit_val.is_null() {
        return Err(PlanExprError("literal string cannot be null".to_string()));
    }
    if let Some(s) = lit_val.as_str() {
        return Ok(s.to_string());
    }
    if let Some(n) = lit_val.as_i64() {
        return Ok(n.to_string());
    }
    if let Some(n) = lit_val.as_f64() {
        return Ok(n.to_string());
    }
    if let Some(b) = lit_val.as_bool() {
        return Ok(b.to_string());
    }
    Err(PlanExprError(
        "literal must be string, number, or bool".to_string(),
    ))
}

fn lit_as_i64(v: &Value) -> Result<i64, PlanExprError> {
    let lit_val = v
        .get("lit")
        .ok_or_else(|| PlanExprError("expected literal".to_string()))?;
    lit_val
        .as_i64()
        .ok_or_else(|| PlanExprError("literal must be integer".to_string()))
}

fn lit_as_i32(v: &Value) -> Result<i32, PlanExprError> {
    let n = lit_as_i64(v)?;
    n.try_into()
        .map_err(|_| PlanExprError("literal out of i32 range".to_string()))
}

fn lit_as_u32(v: &Value) -> Result<u32, PlanExprError> {
    let lit_val = v
        .get("lit")
        .ok_or_else(|| PlanExprError("expected literal".to_string()))?;
    if let Some(n) = lit_val.as_u64() {
        return n
            .try_into()
            .map_err(|_| PlanExprError("literal out of u32 range".to_string()));
    }
    if let Some(n) = lit_val.as_i64() {
        return (n as u64)
            .try_into()
            .map_err(|_| PlanExprError("literal out of u32 range".to_string()));
    }
    Err(PlanExprError("literal must be number".to_string()))
}

fn lit_as_f64(v: &Value) -> Result<f64, PlanExprError> {
    let lit_val = v
        .get("lit")
        .ok_or_else(|| PlanExprError("expected literal".to_string()))?;
    if let Some(n) = lit_val.as_f64() {
        return Ok(n);
    }
    if let Some(n) = lit_val.as_i64() {
        return Ok(n as f64);
    }
    Err(PlanExprError("literal must be number".to_string()))
}

#[allow(dead_code)]
fn lit_as_bool(v: &Value) -> Result<bool, PlanExprError> {
    let lit_val = v
        .get("lit")
        .ok_or_else(|| PlanExprError("expected literal".to_string()))?;
    lit_val
        .as_bool()
        .ok_or_else(|| PlanExprError("literal must be boolean".to_string()))
}

fn lit_as_usize(v: &Value) -> Result<usize, PlanExprError> {
    let n = lit_as_i64(v)?;
    if n < 0 {
        return Err(PlanExprError(
            "literal must be non-negative for usize".to_string(),
        ));
    }
    n.try_into()
        .map_err(|_| PlanExprError("literal out of usize range".to_string()))
}

/// Extract values for isin from JSON array. Each element: {"lit": v} or plain v. Returns Expr for is_in.
/// When arr is empty or parses to no values (e.g. all nulls), returns Ok(None) — caller uses lit(false)
/// so that col.isin([]) is false for all rows (PySpark parity; issue #518).
/// Mixed types / string vs numeric (issue #525): when list has strings we use string series; when list
/// has only numbers we use string series too so that string column isin(1, 2) works (PySpark casts literals to column type).
fn try_values_for_isin(arr: &[Value]) -> Result<Option<Expr>, PlanExprError> {
    if arr.is_empty() {
        return Ok(None);
    }
    let mut str_vals: Vec<String> = Vec::new();
    for v in arr {
        let lit_val = if let Some(obj) = v.as_object() {
            obj.get("lit").unwrap_or(v)
        } else {
            v
        };
        if lit_val.is_null() {
            continue;
        }
        if let Some(s) = lit_val.as_str() {
            str_vals.push(s.to_string());
        } else if let Some(n) = lit_val.as_i64() {
            str_vals.push(n.to_string());
        } else if let Some(n) = lit_val.as_f64() {
            str_vals.push(n.to_string());
        }
    }
    if str_vals.is_empty() {
        return Ok(None);
    }
    let s: Series = Series::from_iter(str_vals.iter().map(|x| x.as_str()));
    Ok(Some(lit(s)))
}

/// Extract values for isin from plan value. Returns None when empty (caller uses lit(false)).
fn try_values_from_plan_value(v: &Value) -> Result<Option<Expr>, PlanExprError> {
    if let Some(lit_val) = v.get("lit") {
        if let Some(arr) = lit_val.as_array() {
            return try_values_for_isin(arr);
        }
        // Single literal - wrap in array
        #[allow(clippy::cloned_ref_to_slice_refs)]
        return try_values_for_isin(&[v.clone()]);
    }
    if let Some(arr) = v.as_array() {
        return try_values_for_isin(arr);
    }
    Err(PlanExprError(
        "isin right/values must be array or {lit: [...]}".to_string(),
    ))
}

/// Optional string literal: if `args[i]` is missing or null, return None; else require {"lit": "..."}.
fn arg_lit_opt_str(args: &[Value], i: usize) -> Result<Option<String>, PlanExprError> {
    let v = match args.get(i) {
        Some(x) => x,
        None => return Ok(None),
    };
    if v.is_null() {
        return Ok(None);
    }
    if let Some(obj) = v.as_object() {
        if obj.get("lit").is_some() {
            return Ok(Some(lit_as_string(v)?));
        }
    }
    Ok(None)
}

fn arg_expr(args: &[Value], i: usize) -> Result<Expr, PlanExprError> {
    let v = args
        .get(i)
        .ok_or_else(|| PlanExprError(format!("fn requires argument at index {i}")))?;
    expr_from_value(v)
}

fn arg_lit_str(args: &[Value], i: usize) -> Result<String, PlanExprError> {
    let v = args
        .get(i)
        .ok_or_else(|| PlanExprError(format!("fn requires string literal at index {i}")))?;
    lit_as_string(v)
}

fn arg_lit_i64(args: &[Value], i: usize) -> Result<i64, PlanExprError> {
    let v = args
        .get(i)
        .ok_or_else(|| PlanExprError(format!("fn requires integer literal at index {i}")))?;
    lit_as_i64(v)
}

fn arg_lit_i32(args: &[Value], i: usize) -> Result<i32, PlanExprError> {
    let v = args
        .get(i)
        .ok_or_else(|| PlanExprError(format!("fn requires integer literal at index {i}")))?;
    lit_as_i32(v)
}

fn arg_lit_u32(args: &[Value], i: usize) -> Result<u32, PlanExprError> {
    let v = args
        .get(i)
        .ok_or_else(|| PlanExprError(format!("fn requires non-negative integer at index {i}")))?;
    lit_as_u32(v)
}

fn arg_lit_f64(args: &[Value], i: usize) -> Result<f64, PlanExprError> {
    let v = args
        .get(i)
        .ok_or_else(|| PlanExprError(format!("fn requires number literal at index {i}")))?;
    lit_as_f64(v)
}

fn arg_lit_usize(args: &[Value], i: usize) -> Result<usize, PlanExprError> {
    let v = args
        .get(i)
        .ok_or_else(|| PlanExprError(format!("fn requires non-negative integer at index {i}")))?;
    lit_as_usize(v)
}

/// Get optional i64 from `args[i]` if present and a literal.
fn opt_lit_i64(args: &[Value], i: usize) -> Option<i64> {
    let v = args.get(i)?;
    v.get("lit").and_then(Value::as_i64)
}

/// Get optional u64 from args (e.g. for rand(seed)).
#[allow(dead_code)]
fn opt_lit_u64(args: &[Value], i: usize) -> Option<u64> {
    let v = args.get(i)?;
    if let Some(n) = v.get("lit").and_then(Value::as_i64) {
        if n >= 0 {
            return Some(n as u64);
        }
        return Some((-n) as u64); // allow negative seed as unsigned
    }
    v.get("lit").and_then(Value::as_u64)
}

fn expr_to_column(expr: Expr) -> crate::Column {
    crate::Column::from_expr(expr, None)
}

/// Null-safe equality: (a <=> b) is true when both null, or both non-null and equal.
/// Applies PySpark-style type coercion (e.g. string vs int) so eq_null_safe matches PySpark (issue #266).
fn eq_null_safe_expr(left: Expr, right: Expr) -> Result<Expr, PlanExprError> {
    use polars::prelude::*;
    let (left_c, right_c) = crate::type_coercion::coerce_for_pyspark_eq_null_safe(left, right)
        .map_err(|e| PlanExprError(e.to_string()))?;
    let left_null = left_c.clone().is_null();
    let right_null = right_c.clone().is_null();
    let both_null = left_null.clone().and(right_null.clone());
    let both_non_null = left_null.not().and(right_null.not());
    let eq_result = left_c.eq(right_c);
    Ok(when(both_null)
        .then(lit(true))
        .when(both_non_null)
        .then(eq_result)
        .otherwise(lit(false)))
}

/// Find index of the closing paren matching the open paren at start.
fn matching_paren(s: &str, start: usize) -> Option<usize> {
    if s.as_bytes().get(start) != Some(&b'(') {
        return None;
    }
    let mut depth = 1u32;
    for (i, b) in s.bytes().enumerate().skip(start + 1) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Parse a single part (column name or literal) for concat/concat_ws.
fn concat_part_to_expr(part: &str) -> Expr {
    let part = part.trim();
    if part.is_empty() {
        return lit("");
    }
    if (part.starts_with('"') && part.ends_with('"'))
        || (part.starts_with('\'') && part.ends_with('\''))
    {
        let inner = part[1..part.len() - 1].trim_matches(['\'', '"']);
        return lit(inner);
    }
    col(part)
}

/// Try to parse a select-item string as concat(...) or concat_ws(...) expression.
/// Used when Sparkless sends e.g. "concat(first_name, , last_name)" as a column name;
/// we treat it as an expression and evaluate it. Returns None if s doesn't look like concat/concat_ws.
pub fn try_parse_concat_expr_from_string(s: &str) -> Option<Expr> {
    use polars::prelude::concat_str;
    let s = s.trim();
    // concat(...)
    if s.starts_with("concat(") {
        let close = matching_paren(s, 6)?; // 6 = len("concat")
        if close != s.len() - 1 {
            return None;
        }
        let inner = s[7..close].trim();
        let parts: Vec<&str> = inner.split(',').map(str::trim).collect();
        if parts.is_empty() {
            return None;
        }
        let exprs: Vec<Expr> = parts.iter().map(|p| concat_part_to_expr(p)).collect();
        return Some(concat_str(&exprs, "", false));
    }
    // concat_ws(sep, ...)
    if s.starts_with("concat_ws(") {
        let close = matching_paren(s, 10)?; // 10 = len("concat_ws")
        if close != s.len() - 1 {
            return None;
        }
        let inner = s[10..close].trim();
        let parts: Vec<&str> = inner.split(',').map(str::trim).collect();
        if parts.len() < 2 {
            return None;
        }
        let sep = parts[0].trim_matches(['\'', '"']);
        let exprs: Vec<Expr> = parts
            .iter()
            .skip(1)
            .map(|p| concat_part_to_expr(p))
            .collect();
        if exprs.is_empty() {
            return None;
        }
        return Some(concat_str(&exprs, sep, false));
    }
    None
}

/// Build a Column from a UDF call. Used by expr_from_value and apply_op (withColumn).
/// Returns Column; caller checks udf_call for Python UDF (needs with_column, not with_column_expr).
pub fn column_from_udf_call(
    udf_name: &str,
    args: &[Value],
) -> Result<crate::Column, PlanExprError> {
    use crate::Column;
    let cols: Vec<Column> = args
        .iter()
        .map(|v| expr_from_value(v).map(expr_to_column))
        .collect::<Result<Vec<_>, _>>()?;
    crate::functions::call_udf(udf_name, &cols).map_err(|e| PlanExprError(e.to_string()))
}

/// Try to parse a UDF expression and build Column. Supports {"udf": "name", "args": [...]}
/// and {"fn": "call_udf", "args": [{"lit": "name"}, ...]}. Returns None if not a UDF expression.
pub fn try_column_from_udf_value(v: &Value) -> Option<Result<crate::Column, PlanExprError>> {
    let obj = v.as_object()?;
    let (udf_name, args) = if let Some(name) = obj.get("udf").and_then(Value::as_str) {
        let args = obj.get("args")?.as_array()?;
        (name.to_string(), args)
    } else if obj.get("fn").and_then(Value::as_str) == Some("call_udf") {
        let args = obj.get("args")?.as_array()?;
        if args.is_empty() {
            return Some(Err(PlanExprError(
                "call_udf requires at least name and one arg".into(),
            )));
        }
        let name = match lit_as_string(&args[0]) {
            Ok(n) => n,
            Err(e) => return Some(Err(e)),
        };
        let rest: &[Value] = &args[1..];
        return Some(column_from_udf_call(&name, rest));
    } else {
        return None;
    };
    Some(column_from_udf_call(&udf_name, args))
}

fn expr_from_fn(name: &str, args: &[Value]) -> Result<Expr, PlanExprError> {
    #[allow(unused_imports)]
    use crate::functions::{
        add_months, array_agg, array_append, array_compact, array_contains, array_distinct,
        array_except, array_insert, array_intersect, array_join, array_prepend, array_remove,
        array_slice, array_sort, array_sum, array_union, arrays_overlap, arrays_zip, ascii,
        assert_true, atan2, base64, bin, bit_and, bit_count, bit_get, bit_length, bit_or, bit_xor,
        bitwise_not, bround, btrim, cast, cbrt, ceiling, char as rs_char, chr, coalesce, concat,
        concat_ws, contains, conv, cos, cosh, cot, crc32, csc, curdate, current_catalog,
        current_database, current_date, current_schema, current_timestamp, current_timezone,
        current_user, date_add, date_diff, date_format, date_from_unix_date, date_part, date_sub,
        date_trunc, dateadd, datediff, datepart, day, dayname, dayofmonth, dayofweek, dayofyear,
        days, decode, degrees, e, element_at, elt, encode, endswith, equal_null, exp,
        explode_outer, extract, factorial, find_in_set, floor, format_number, format_string,
        from_unixtime, from_utc_timestamp, get, get_json_object, getbit, greatest, hash, hex, hour,
        hypot, ilike, initcap, input_file_name, instr, isnan, last_day, lcase, least, left, length,
        like, lit_str, ln, localtimestamp, locate, log, log10, log1p, log2, lower, lpad, make_date,
        make_interval, make_timestamp, make_timestamp_ntz, mask, md5, minute,
        monotonically_increasing_id, month, months_between, nanvl, negate, negative, next_day, now,
        nullif, nvl, nvl2, octet_length, overlay, parse_url, pi, pmod, positive, pow, power,
        quarter, radians, raise_error, rand, randn, regexp, regexp_count, regexp_extract,
        regexp_extract_all, regexp_instr, regexp_like, regexp_replace, regexp_substr, repeat,
        replace, reverse, right, rint, rlike, round, rpad, sec, second, sha1, sha2, shift_left,
        shift_right, signum, sin, sinh, size, soundex, spark_partition_id, split, split_part, sqrt,
        startswith, str_to_map, struct_, substr, substring, substring_index, tan, tanh,
        timestamp_micros, timestamp_millis, timestamp_seconds, timestampadd, timestampdiff,
        to_binary, to_char, to_date, to_degrees, to_radians, to_timestamp, to_unix_timestamp,
        to_utc_timestamp, to_varchar, translate, trim, trunc, try_add, try_cast, try_divide,
        try_element_at, try_multiply, try_subtract, try_to_binary, try_to_number, try_to_timestamp,
        typeof_, ucase, unbase64, unhex, unix_date, unix_micros, unix_millis, unix_seconds,
        unix_timestamp, unix_timestamp_now, upper, url_decode, url_encode, user, version, weekday,
        weekofyear, when_then_otherwise_null, width_bucket, xxhash64, year,
    };
    use crate::Column;

    match name {
        "call_udf" => {
            if args.is_empty() {
                return Err(PlanExprError(
                    "call_udf requires at least name and one arg".into(),
                ));
            }
            let udf_name = lit_as_string(&args[0])?;
            let col = column_from_udf_call(&udf_name, &args[1..])?;
            if col.udf_call.is_some() {
                return Err(PlanExprError(
                    "Python/Vectorized UDFs are only supported in withColumn/select, not in filter/plan expressions"
                        .into(),
                ));
            }
            Ok(col.expr().clone())
        }
        "upper" => {
            require_args(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            Ok(upper(&c).into_expr())
        }
        "lower" => {
            require_args(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            Ok(lower(&c).into_expr())
        }
        "coalesce" => {
            if args.is_empty() {
                return Err(PlanExprError(format!(
                    "fn '{name}' requires at least one argument"
                )));
            }
            let exprs: Result<Vec<Expr>, _> = args.iter().map(expr_from_value).collect();
            let exprs = exprs?;
            Ok(polars::prelude::coalesce(&exprs))
        }
        "when" => {
            if args.len() != 2 {
                return Err(PlanExprError(format!(
                    "fn '{name}' two-arg form requires [condition, then_expr]"
                )));
            }
            let cond = expr_to_column(arg_expr(args, 0)?);
            let then_val = expr_to_column(arg_expr(args, 1)?);
            Ok(when_then_otherwise_null(&cond, &then_val).into_expr())
        }
        // --- String ---
        "length" | "char_length" | "character_length" => {
            require_args(name, args, 1)?;
            Ok(length(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "trim" => {
            require_args(name, args, 1)?;
            Ok(trim(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "ltrim" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::ltrim(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "rtrim" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::rtrim(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "btrim" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let trim_str: Option<String> = arg_lit_opt_str(args, 1)?;
            Ok(btrim(&c, trim_str.as_deref()).into_expr())
        }
        "substring" | "substr" => {
            require_args_min(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let start = arg_lit_i64(args, 1)?;
            let len = opt_lit_i64(args, 2);
            Ok(substring(&c, start, len).into_expr())
        }
        "concat" => {
            if args.is_empty() {
                return Err(PlanExprError(format!(
                    "fn '{name}' requires at least one argument"
                )));
            }
            let exprs: Result<Vec<Expr>, _> = args.iter().map(expr_from_value).collect();
            let cols: Vec<Column> = exprs?.into_iter().map(expr_to_column).collect();
            let refs: Vec<&Column> = cols.iter().collect();
            Ok(concat(&refs).into_expr())
        }
        "concat_ws" => {
            require_args_min(name, args, 2)?;
            let sep = arg_lit_str(args, 0)?;
            let exprs: Result<Vec<Expr>, _> = args.iter().skip(1).map(expr_from_value).collect();
            let cols: Vec<Column> = exprs?.into_iter().map(expr_to_column).collect();
            let refs: Vec<&Column> = cols.iter().collect();
            Ok(concat_ws(&sep, &refs).into_expr())
        }
        "initcap" => {
            require_args(name, args, 1)?;
            Ok(initcap(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "repeat" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let n = arg_lit_i32(args, 1)?;
            Ok(repeat(&c, n).into_expr())
        }
        "reverse" => {
            require_args(name, args, 1)?;
            Ok(reverse(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "instr" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let substr = arg_lit_str(args, 1)?;
            Ok(instr(&c, &substr).into_expr())
        }
        "position" => {
            require_args_min(name, args, 2)?;
            let substr = arg_lit_str(args, 0)?;
            let c = expr_to_column(arg_expr(args, 1)?);
            let pos = opt_lit_i64(args, 2).unwrap_or(1);
            Ok(locate(&substr, &c, pos).into_expr())
        }
        "locate" => {
            require_args_min(name, args, 2)?;
            let substr = arg_lit_str(args, 0)?;
            let c = expr_to_column(arg_expr(args, 1)?);
            let pos = opt_lit_i64(args, 2).unwrap_or(1);
            Ok(locate(&substr, &c, pos).into_expr())
        }
        "ascii" => {
            require_args(name, args, 1)?;
            Ok(ascii(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "format_number" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let decimals = arg_lit_u32(args, 1)?;
            Ok(format_number(&c, decimals).into_expr())
        }
        "overlay" => {
            require_args_min(name, args, 4)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let replace_str = arg_lit_str(args, 1)?;
            let pos = arg_lit_i64(args, 2)?;
            let len = arg_lit_i64(args, 3)?;
            Ok(overlay(&c, &replace_str, pos, len).into_expr())
        }
        "char" => {
            require_args(name, args, 1)?;
            Ok(rs_char(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "chr" => {
            require_args(name, args, 1)?;
            Ok(chr(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "base64" => {
            require_args(name, args, 1)?;
            Ok(base64(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "unbase64" => {
            require_args(name, args, 1)?;
            Ok(unbase64(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "sha1" => {
            require_args(name, args, 1)?;
            Ok(sha1(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "sha2" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let bits = arg_lit_i32(args, 1)?;
            Ok(sha2(&c, bits).into_expr())
        }
        "md5" => {
            require_args(name, args, 1)?;
            Ok(md5(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "lpad" => {
            require_args(name, args, 3)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let len = arg_lit_i32(args, 1)?;
            let pad = arg_lit_str(args, 2)?;
            Ok(lpad(&c, len, &pad).into_expr())
        }
        "rpad" => {
            require_args(name, args, 3)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let len = arg_lit_i32(args, 1)?;
            let pad = arg_lit_str(args, 2)?;
            Ok(rpad(&c, len, &pad).into_expr())
        }
        "translate" => {
            require_args(name, args, 3)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let from_str = arg_lit_str(args, 1)?;
            let to_str = arg_lit_str(args, 2)?;
            Ok(translate(&c, &from_str, &to_str).into_expr())
        }
        "substring_index" => {
            require_args(name, args, 3)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let delim = arg_lit_str(args, 1)?;
            let count = arg_lit_i64(args, 2)?;
            Ok(substring_index(&c, &delim, count).into_expr())
        }
        "left" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let n = arg_lit_i64(args, 1)?;
            Ok(left(&c, n).into_expr())
        }
        "right" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let n = arg_lit_i64(args, 1)?;
            Ok(right(&c, n).into_expr())
        }
        "replace" => {
            require_args(name, args, 3)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let search = arg_lit_str(args, 1)?;
            let replacement = arg_lit_str(args, 2)?;
            Ok(replace(&c, &search, &replacement).into_expr())
        }
        "startswith" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let prefix = arg_lit_str(args, 1)?;
            Ok(startswith(&c, &prefix).into_expr())
        }
        "endswith" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let suffix = arg_lit_str(args, 1)?;
            Ok(endswith(&c, &suffix).into_expr())
        }
        "contains" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let substring = arg_lit_str(args, 1)?;
            Ok(contains(&c, &substring).into_expr())
        }
        "like" => {
            require_args_min(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pattern = arg_lit_str(args, 1)?;
            let escape = arg_lit_opt_str(args, 2)?.and_then(|s| s.chars().next());
            Ok(like(&c, &pattern, escape).into_expr())
        }
        "ilike" => {
            require_args_min(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pattern = arg_lit_str(args, 1)?;
            let escape = arg_lit_opt_str(args, 2)?.and_then(|s| s.chars().next());
            Ok(ilike(&c, &pattern, escape).into_expr())
        }
        "rlike" | "regexp" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pattern = arg_lit_str(args, 1)?;
            Ok(rlike(&c, &pattern).into_expr())
        }
        "soundex" => {
            require_args(name, args, 1)?;
            Ok(soundex(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "levenshtein" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(crate::functions::levenshtein(&a, &b).into_expr())
        }
        "crc32" => {
            require_args(name, args, 1)?;
            Ok(crc32(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "xxhash64" => {
            require_args(name, args, 1)?;
            Ok(xxhash64(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "regexp_extract" => {
            // Plan execution requires literal pattern and group index (issue #523).
            require_args(name, args, 3)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pattern = arg_lit_str(args, 1).map_err(|_| {
                PlanExprError(
                    "regexp_extract in plan requires literal pattern at arg 2 (column refs not supported)".into(),
                )
            })?;
            let group_index = arg_lit_usize(args, 2).map_err(|_| {
                PlanExprError(
                    "regexp_extract in plan requires literal group index at arg 3 (column refs not supported)".into(),
                )
            })?;
            Ok(regexp_extract(&c, &pattern, group_index).into_expr())
        }
        "regexp_replace" => {
            require_args(name, args, 3)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pattern = arg_lit_str(args, 1)?;
            let replacement = arg_lit_str(args, 2)?;
            Ok(regexp_replace(&c, &pattern, &replacement).into_expr())
        }
        "regexp_extract_all" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pattern = arg_lit_str(args, 1)?;
            Ok(regexp_extract_all(&c, &pattern).into_expr())
        }
        "regexp_like" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pattern = arg_lit_str(args, 1)?;
            Ok(regexp_like(&c, &pattern).into_expr())
        }
        "regexp_count" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pattern = arg_lit_str(args, 1)?;
            Ok(regexp_count(&c, &pattern).into_expr())
        }
        "regexp_substr" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pattern = arg_lit_str(args, 1)?;
            Ok(regexp_substr(&c, &pattern).into_expr())
        }
        "regexp_instr" => {
            require_args_min(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pattern = arg_lit_str(args, 1)?;
            let group_idx = args.get(2).and_then(|v| lit_as_usize(v).ok());
            Ok(regexp_instr(&c, &pattern, group_idx).into_expr())
        }
        "split" => {
            require_args_min(name, args, 2)?;
            if args.len() > 3 {
                return Err(PlanExprError("split takes at most 3 arguments".to_string()));
            }
            let c = expr_to_column(arg_expr(args, 0)?);
            let delimiter = arg_lit_str(args, 1)?;
            let limit = args
                .get(2)
                .and_then(|v| lit_as_i64(v).ok())
                .map(|n| n as i32);
            Ok(split(&c, &delimiter, limit).into_expr())
        }
        "split_part" => {
            require_args(name, args, 3)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let delimiter = arg_lit_str(args, 1)?;
            let part_num = arg_lit_i64(args, 2)?;
            Ok(split_part(&c, &delimiter, part_num).into_expr())
        }
        "find_in_set" => {
            require_args(name, args, 2)?;
            let str_col = expr_to_column(arg_expr(args, 0)?);
            let set_col = expr_to_column(arg_expr(args, 1)?);
            Ok(find_in_set(&str_col, &set_col).into_expr())
        }
        "format_string" | "printf" => {
            require_args_min(name, args, 2)?;
            let format_str = arg_lit_str(args, 0)?;
            let exprs: Result<Vec<Expr>, _> = args.iter().skip(1).map(expr_from_value).collect();
            let cols: Vec<Column> = exprs?.into_iter().map(expr_to_column).collect();
            let refs: Vec<&Column> = cols.iter().collect();
            Ok(format_string(&format_str, &refs).into_expr())
        }
        "lcase" => {
            require_args(name, args, 1)?;
            Ok(lcase(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "ucase" => {
            require_args(name, args, 1)?;
            Ok(ucase(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "mask" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let u = args
                .get(1)
                .and_then(|v| lit_as_string(v).ok())
                .and_then(|s| s.chars().next());
            let l = args
                .get(2)
                .and_then(|v| lit_as_string(v).ok())
                .and_then(|s| s.chars().next());
            let d = args
                .get(3)
                .and_then(|v| lit_as_string(v).ok())
                .and_then(|s| s.chars().next());
            let o = args
                .get(4)
                .and_then(|v| lit_as_string(v).ok())
                .and_then(|s| s.chars().next());
            Ok(mask(&c, u, l, d, o).into_expr())
        }
        "str_to_map" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let pair_delim: Option<String> = arg_lit_opt_str(args, 1)?;
            let key_value_delim: Option<String> = arg_lit_opt_str(args, 2)?;
            Ok(str_to_map(&c, pair_delim.as_deref(), key_value_delim.as_deref()).into_expr())
        }
        "isin" => {
            // {"fn": "isin", "args": [col_expr, lit1, lit2, ...]} - col.isin(1, 3)
            // Empty list or [col, {"lit": null}] -> lit(false) (issue #518)
            require_args_min(name, args, 1)?;
            let col_expr = arg_expr(args, 0)?;
            let values_opt = try_values_for_isin(&args[1..])?;
            Ok(match values_opt {
                None => lit(false),
                Some(values_expr) => col_expr.is_in(values_expr),
            })
        }
        _ => expr_from_fn_rest(name, args),
    }
}

fn expr_from_fn_rest(name: &str, args: &[Value]) -> Result<Expr, PlanExprError> {
    #[allow(unused_imports)]
    use crate::functions::{
        abs, acos, add_months, array, array_agg, array_append, array_compact, array_contains,
        array_distinct, array_except, array_insert, array_intersect, array_join, array_max,
        array_min, array_prepend, array_remove, array_size, array_slice, array_sort, array_sum,
        array_union, arrays_overlap, arrays_zip, asin, atan, atan2, bround, cast, cbrt, ceiling,
        cos, cosh, cot, create_map, csc, curdate, current_catalog, current_database, current_date,
        current_schema, current_timestamp, current_timezone, current_user, date_add, date_diff,
        date_format, date_from_unix_date, date_part, date_sub, date_trunc, dateadd, datediff,
        datepart, day, dayname, dayofmonth, dayofweek, dayofyear, days, decode, degrees, e,
        element_at, encode, equal_null, exp, explode, explode_outer, expm1, extract, factorial,
        floor, from_unixtime, from_utc_timestamp, get, get_json_object, greatest, grouping,
        grouping_id, hash, hour, hours, hypot, input_file_name, last_day, least, localtimestamp,
        log, log10, log1p, log2, make_date, make_interval, make_timestamp, make_timestamp_ntz,
        map_keys, map_values, minute, minutes, monotonically_increasing_id, month, months,
        months_between, negate, next_day, now, nullif, nvl, nvl2, parse_url, pi, pmod, positive,
        pow, quarter, radians, rint, round, sec, second, shift_left, shift_right, signum, sin,
        sinh, size, spark_partition_id, sqrt, tan, tanh, timestamp_micros, timestamp_millis,
        timestamp_seconds, timestampadd, timestampdiff, to_binary, to_char, to_date, to_degrees,
        to_number, to_radians, to_timestamp, to_unix_timestamp, to_utc_timestamp, to_varchar,
        trunc, try_add, try_cast, try_divide, try_element_at, try_multiply, try_subtract,
        try_to_number, try_to_timestamp, typeof_, unix_date, unix_micros, unix_millis,
        unix_seconds, unix_timestamp, unix_timestamp_now, user, weekday, weekofyear, width_bucket,
        year, years,
    };
    use crate::Column;

    // --- Math / numeric ---
    match name {
        "abs" => {
            require_args(name, args, 1)?;
            Ok(abs(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "ceil" | "ceiling" => {
            require_args(name, args, 1)?;
            Ok(ceiling(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "floor" => {
            require_args(name, args, 1)?;
            Ok(floor(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "round" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let decimals = opt_lit_i64(args, 1).map(|n| n as u32).unwrap_or(0);
            Ok(round(&c, decimals).into_expr())
        }
        "bround" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let scale = opt_lit_i64(args, 1).unwrap_or(0) as i32;
            Ok(bround(&c, scale).into_expr())
        }
        "negate" | "negative" => {
            require_args(name, args, 1)?;
            Ok(negate(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "positive" => {
            require_args(name, args, 1)?;
            Ok(positive(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "sqrt" => {
            require_args(name, args, 1)?;
            Ok(sqrt(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "pow" | "power" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let exp_val = arg_lit_i64(args, 1)?;
            Ok(pow(&c, exp_val).into_expr())
        }
        "exp" => {
            require_args(name, args, 1)?;
            Ok(exp(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "log" | "ln" => {
            if args.len() == 1 {
                Ok(log(&expr_to_column(arg_expr(args, 0)?)).into_expr())
            } else if args.len() == 2 {
                let col_expr = expr_to_column(arg_expr(args, 0)?);
                let base = match &args[1] {
                    Value::Number(n) => n
                        .as_f64()
                        .ok_or_else(|| PlanExprError("log base must be a number".to_string()))?,
                    _ => return Err(PlanExprError("log base must be a number".to_string())),
                };
                Ok(crate::functions::log_with_base(&col_expr, base).into_expr())
            } else {
                Err(PlanExprError(format!(
                    "fn '{name}' requires 1 or 2 arguments"
                )))
            }
        }
        "sin" => {
            require_args(name, args, 1)?;
            Ok(sin(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "cos" => {
            require_args(name, args, 1)?;
            Ok(cos(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "tan" => {
            require_args(name, args, 1)?;
            Ok(tan(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "asin" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::asin(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "acos" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::acos(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "atan" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::atan(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "atan2" => {
            require_args(name, args, 2)?;
            let y = expr_to_column(arg_expr(args, 0)?);
            let x = expr_to_column(arg_expr(args, 1)?);
            Ok(atan2(&y, &x).into_expr())
        }
        "degrees" => {
            require_args(name, args, 1)?;
            Ok(degrees(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "radians" => {
            require_args(name, args, 1)?;
            Ok(radians(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "signum" | "sign" => {
            require_args(name, args, 1)?;
            Ok(signum(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "cot" => {
            require_args(name, args, 1)?;
            Ok(cot(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "csc" => {
            require_args(name, args, 1)?;
            Ok(csc(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "sec" => {
            require_args(name, args, 1)?;
            Ok(sec(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "e" => {
            if !args.is_empty() {
                return Err(PlanExprError("fn 'e' takes no arguments".to_string()));
            }
            Ok(e().into_expr())
        }
        "pi" => {
            if !args.is_empty() {
                return Err(PlanExprError("fn 'pi' takes no arguments".to_string()));
            }
            Ok(pi().into_expr())
        }
        "pmod" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(pmod(&a, &b).into_expr())
        }
        "factorial" => {
            require_args(name, args, 1)?;
            Ok(factorial(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "hypot" => {
            require_args(name, args, 2)?;
            let x = expr_to_column(arg_expr(args, 0)?);
            let y = expr_to_column(arg_expr(args, 1)?);
            Ok(hypot(&x, &y).into_expr())
        }
        "cosh" => {
            require_args(name, args, 1)?;
            Ok(cosh(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "sinh" => {
            require_args(name, args, 1)?;
            Ok(sinh(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "tanh" => {
            require_args(name, args, 1)?;
            Ok(tanh(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "cbrt" => {
            require_args(name, args, 1)?;
            Ok(cbrt(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "expm1" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::expm1(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "log1p" => {
            require_args(name, args, 1)?;
            Ok(log1p(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "log10" => {
            require_args(name, args, 1)?;
            Ok(log10(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "log2" => {
            require_args(name, args, 1)?;
            Ok(log2(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "rint" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::rint(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "to_degrees" => {
            require_args(name, args, 1)?;
            Ok(to_degrees(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "to_radians" => {
            require_args(name, args, 1)?;
            Ok(to_radians(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        // --- Type / conditional ---
        "cast" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let type_name = arg_lit_str(args, 1)?;
            Ok(cast(&c, &type_name).map_err(PlanExprError)?.into_expr())
        }
        "try_cast" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let type_name = arg_lit_str(args, 1)?;
            Ok(try_cast(&c, &type_name).map_err(PlanExprError)?.into_expr())
        }
        "nvl" | "ifnull" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(nvl(&a, &b).into_expr())
        }
        "nullif" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(nullif(&a, &b).into_expr())
        }
        "greatest" => {
            require_args_min(name, args, 1)?;
            let exprs: Result<Vec<Expr>, _> = args.iter().map(expr_from_value).collect();
            let cols: Vec<Column> = exprs?.into_iter().map(expr_to_column).collect();
            let refs: Vec<&Column> = cols.iter().collect();
            Ok(greatest(&refs).map_err(PlanExprError)?.into_expr())
        }
        "least" => {
            require_args_min(name, args, 1)?;
            let exprs: Result<Vec<Expr>, _> = args.iter().map(expr_from_value).collect();
            let cols: Vec<Column> = exprs?.into_iter().map(expr_to_column).collect();
            let refs: Vec<&Column> = cols.iter().collect();
            Ok(least(&refs).map_err(PlanExprError)?.into_expr())
        }
        "typeof" => {
            require_args(name, args, 1)?;
            Ok(typeof_(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "try_divide" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(try_divide(&a, &b).into_expr())
        }
        // PySpark-style arithmetic with string/numeric coercion (issue #201)
        "add" | "+" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(a.add_pyspark(&b).into_expr())
        }
        "subtract" | "-" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(a.subtract_pyspark(&b).into_expr())
        }
        "multiply" | "*" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(a.multiply_pyspark(&b).into_expr())
        }
        "divide" | "/" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(a.divide_pyspark(&b).into_expr())
        }
        "mod" | "remainder" | "%" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(a.mod_pyspark(&b).into_expr())
        }
        "try_add" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(try_add(&a, &b).into_expr())
        }
        "try_subtract" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(try_subtract(&a, &b).into_expr())
        }
        "try_multiply" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(try_multiply(&a, &b).into_expr())
        }
        "width_bucket" => {
            require_args(name, args, 4)?;
            let val = expr_to_column(arg_expr(args, 0)?);
            let min_val = arg_lit_f64(args, 1)?;
            let max_val = arg_lit_f64(args, 2)?;
            let num_bucket = arg_lit_i64(args, 3)?;
            if num_bucket <= 0 {
                return Err(PlanExprError(
                    "width_bucket: num_bucket must be positive".into(),
                ));
            }
            Ok(width_bucket(&val, min_val, max_val, num_bucket).into_expr())
        }
        "equal_null" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(equal_null(&a, &b).into_expr())
        }
        // --- Datetime ---
        "year" => {
            require_args(name, args, 1)?;
            Ok(year(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "month" => {
            require_args(name, args, 1)?;
            Ok(month(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "day" | "dayofmonth" => {
            require_args(name, args, 1)?;
            Ok(day(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "hour" => {
            require_args(name, args, 1)?;
            Ok(hour(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "minute" => {
            require_args(name, args, 1)?;
            Ok(minute(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "second" => {
            require_args(name, args, 1)?;
            Ok(second(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "quarter" => {
            require_args(name, args, 1)?;
            Ok(quarter(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "weekofyear" => {
            require_args(name, args, 1)?;
            Ok(weekofyear(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "dayofweek" => {
            require_args(name, args, 1)?;
            Ok(dayofweek(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "dayofyear" => {
            require_args(name, args, 1)?;
            Ok(dayofyear(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "to_date" => {
            require_args_min(name, args, 1)?;
            if args.len() > 2 {
                return Err(PlanExprError(format!(
                    "fn '{name}' takes at most 2 argument(s)"
                )));
            }
            let col = expr_to_column(arg_expr(args, 0)?);
            let format_str = if args.len() == 2 {
                Some(arg_lit_str(args, 1)?)
            } else {
                None
            };
            to_date(&col, format_str.as_deref())
                .map_err(PlanExprError)
                .map(|c| c.into_expr())
        }
        "date_format" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let format = arg_lit_str(args, 1)?;
            Ok(date_format(&c, &format).into_expr())
        }
        "date_add" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let n = arg_lit_i32(args, 1)?;
            Ok(date_add(&c, n).into_expr())
        }
        "date_sub" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let n = arg_lit_i32(args, 1)?;
            Ok(date_sub(&c, n).into_expr())
        }
        "datediff" | "date_diff" => {
            require_args(name, args, 2)?;
            let end = expr_to_column(arg_expr(args, 0)?);
            let start = expr_to_column(arg_expr(args, 1)?);
            Ok(datediff(&end, &start).into_expr())
        }
        "last_day" => {
            require_args(name, args, 1)?;
            Ok(last_day(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "trunc" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let format = arg_lit_str(args, 1)?;
            Ok(trunc(&c, &format).into_expr())
        }
        "date_trunc" => {
            require_args(name, args, 2)?;
            let format = arg_lit_str(args, 0)?;
            let c = expr_to_column(arg_expr(args, 1)?);
            Ok(date_trunc(&format, &c).into_expr())
        }
        "add_months" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let n = arg_lit_i32(args, 1)?;
            Ok(add_months(&c, n).into_expr())
        }
        "months_between" => {
            require_args_min(name, args, 2)?;
            let end = expr_to_column(arg_expr(args, 0)?);
            let start = expr_to_column(arg_expr(args, 1)?);
            let round_off = args
                .get(2)
                .and_then(|v| v.get("lit").and_then(Value::as_bool))
                .unwrap_or(true);
            Ok(months_between(&end, &start, round_off).into_expr())
        }
        "next_day" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let day_of_week = arg_lit_str(args, 1)?;
            Ok(next_day(&c, &day_of_week).into_expr())
        }
        "unix_timestamp" => {
            if args.is_empty() {
                Ok(unix_timestamp_now().into_expr())
            } else {
                require_args_min(name, args, 1)?;
                let c = expr_to_column(arg_expr(args, 0)?);
                let format: Option<String> = arg_lit_opt_str(args, 1)?;
                Ok(unix_timestamp(&c, format.as_deref()).into_expr())
            }
        }
        "from_unixtime" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let format: Option<String> = arg_lit_opt_str(args, 1)?;
            Ok(from_unixtime(&c, format.as_deref()).into_expr())
        }
        "to_unix_timestamp" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let format: Option<String> = arg_lit_opt_str(args, 1)?;
            Ok(to_unix_timestamp(&c, format.as_deref()).into_expr())
        }
        "make_date" => {
            require_args(name, args, 3)?;
            let y = expr_to_column(arg_expr(args, 0)?);
            let m = expr_to_column(arg_expr(args, 1)?);
            let d = expr_to_column(arg_expr(args, 2)?);
            Ok(make_date(&y, &m, &d).into_expr())
        }
        "make_timestamp" => {
            require_args_min(name, args, 6)?;
            let y = expr_to_column(arg_expr(args, 0)?);
            let mo = expr_to_column(arg_expr(args, 1)?);
            let d = expr_to_column(arg_expr(args, 2)?);
            let h = expr_to_column(arg_expr(args, 3)?);
            let mi = expr_to_column(arg_expr(args, 4)?);
            let s = expr_to_column(arg_expr(args, 5)?);
            let tz: Option<String> = arg_lit_opt_str(args, 6)?;
            Ok(make_timestamp(&y, &mo, &d, &h, &mi, &s, tz.as_deref()).into_expr())
        }
        "make_timestamp_ntz" => {
            require_args(name, args, 6)?;
            let y = expr_to_column(arg_expr(args, 0)?);
            let mo = expr_to_column(arg_expr(args, 1)?);
            let d = expr_to_column(arg_expr(args, 2)?);
            let h = expr_to_column(arg_expr(args, 3)?);
            let mi = expr_to_column(arg_expr(args, 4)?);
            let s = expr_to_column(arg_expr(args, 5)?);
            Ok(make_timestamp_ntz(&y, &mo, &d, &h, &mi, &s).into_expr())
        }
        "timestampadd" => {
            require_args(name, args, 3)?;
            let unit = arg_lit_str(args, 0)?;
            let amount = expr_to_column(arg_expr(args, 1)?);
            let ts = expr_to_column(arg_expr(args, 2)?);
            Ok(timestampadd(&unit, &amount, &ts).into_expr())
        }
        "timestampdiff" => {
            require_args(name, args, 3)?;
            let unit = arg_lit_str(args, 0)?;
            let start = expr_to_column(arg_expr(args, 1)?);
            let end = expr_to_column(arg_expr(args, 2)?);
            Ok(timestampdiff(&unit, &start, &end).into_expr())
        }
        "days" => {
            require_args(name, args, 1)?;
            let n = arg_lit_i64(args, 0)?;
            Ok(days(n).into_expr())
        }
        "hours" => {
            require_args(name, args, 1)?;
            let n = arg_lit_i64(args, 0)?;
            Ok(hours(n).into_expr())
        }
        "minutes" => {
            require_args(name, args, 1)?;
            let n = arg_lit_i64(args, 0)?;
            Ok(minutes(n).into_expr())
        }
        "months" => {
            require_args(name, args, 1)?;
            let n = arg_lit_i64(args, 0)?;
            Ok(months(n).into_expr())
        }
        "years" => {
            require_args(name, args, 1)?;
            let n = arg_lit_i64(args, 0)?;
            Ok(years(n).into_expr())
        }
        "from_utc_timestamp" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let tz = arg_lit_str(args, 1)?;
            Ok(from_utc_timestamp(&c, &tz).into_expr())
        }
        "to_utc_timestamp" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let tz = arg_lit_str(args, 1)?;
            Ok(to_utc_timestamp(&c, &tz).into_expr())
        }
        "convert_timezone" => {
            require_args(name, args, 3)?;
            let source_tz = arg_lit_str(args, 0)?;
            let target_tz = arg_lit_str(args, 1)?;
            let c = expr_to_column(arg_expr(args, 2)?);
            Ok(crate::functions::convert_timezone(&source_tz, &target_tz, &c).into_expr())
        }
        "current_date" | "curdate" => {
            if !args.is_empty() {
                return Err(PlanExprError(format!("fn '{name}' takes no arguments")));
            }
            Ok(current_date().into_expr())
        }
        "current_timestamp" | "now" => {
            if !args.is_empty() {
                return Err(PlanExprError(format!("fn '{name}' takes no arguments")));
            }
            Ok(current_timestamp().into_expr())
        }
        "localtimestamp" => {
            if !args.is_empty() {
                return Err(PlanExprError(
                    "fn 'localtimestamp' takes no arguments".to_string(),
                ));
            }
            Ok(localtimestamp().into_expr())
        }
        "extract" | "date_part" | "datepart" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let field = arg_lit_str(args, 1)?;
            Ok(extract(&c, &field).into_expr())
        }
        "dateadd" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let n = arg_lit_i32(args, 1)?;
            Ok(dateadd(&c, n).into_expr())
        }
        "unix_micros" | "unix_millis" | "unix_seconds" => {
            require_args(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let out = match name {
                "unix_micros" => unix_micros(&c),
                "unix_millis" => unix_millis(&c),
                _ => unix_seconds(&c),
            };
            Ok(out.into_expr())
        }
        "dayname" => {
            require_args(name, args, 1)?;
            Ok(dayname(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "weekday" => {
            require_args(name, args, 1)?;
            Ok(weekday(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "timestamp_seconds" | "timestamp_millis" | "timestamp_micros" => {
            require_args(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let out = match name {
                "timestamp_seconds" => timestamp_seconds(&c),
                "timestamp_millis" => timestamp_millis(&c),
                _ => timestamp_micros(&c),
            };
            Ok(out.into_expr())
        }
        "unix_date" => {
            require_args(name, args, 1)?;
            Ok(unix_date(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "date_from_unix_date" => {
            require_args(name, args, 1)?;
            Ok(date_from_unix_date(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "to_char" | "to_varchar" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let format: Option<String> = arg_lit_opt_str(args, 1)?;
            Ok(to_char(&c, format.as_deref())
                .map_err(PlanExprError)?
                .into_expr())
        }
        "to_timestamp" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let format: Option<String> = arg_lit_opt_str(args, 1)?;
            Ok(to_timestamp(&c, format.as_deref())
                .map_err(PlanExprError)?
                .into_expr())
        }
        "try_to_timestamp" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let format: Option<String> = arg_lit_opt_str(args, 1)?;
            Ok(try_to_timestamp(&c, format.as_deref())
                .map_err(PlanExprError)?
                .into_expr())
        }
        "to_number" | "try_to_number" => {
            require_args_min(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let format: Option<String> = arg_lit_opt_str(args, 1)?;
            let out = if name == "to_number" {
                to_number(&c, format.as_deref()).map_err(PlanExprError)?
            } else {
                try_to_number(&c, format.as_deref()).map_err(PlanExprError)?
            };
            Ok(out.into_expr())
        }
        "current_timezone" => {
            if !args.is_empty() {
                return Err(PlanExprError(
                    "fn 'current_timezone' takes no arguments".to_string(),
                ));
            }
            Ok(current_timezone().into_expr())
        }
        // --- Zero-arg JVM/runtime stubs ---
        "spark_partition_id"
        | "input_file_name"
        | "monotonically_increasing_id"
        | "current_catalog"
        | "current_database"
        | "current_schema"
        | "current_user"
        | "user" => {
            if !args.is_empty() {
                return Err(PlanExprError(format!("fn '{name}' takes no arguments")));
            }
            let out = match name {
                "spark_partition_id" => spark_partition_id(),
                "input_file_name" => input_file_name(),
                "monotonically_increasing_id" => monotonically_increasing_id(),
                "current_catalog" => current_catalog(),
                "current_database" => current_database(),
                "current_schema" => current_schema(),
                "current_user" => current_user(),
                "user" => user(),
                _ => current_catalog(), // unreachable
            };
            Ok(out.into_expr())
        }
        "hash" => {
            require_args_min(name, args, 1)?;
            let exprs: Result<Vec<Expr>, _> = args.iter().map(expr_from_value).collect();
            let cols: Vec<Column> = exprs?.into_iter().map(expr_to_column).collect();
            let refs: Vec<&Column> = cols.iter().collect();
            Ok(crate::functions::hash(&refs).into_expr())
        }
        "shift_left" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let n = arg_lit_i32(args, 1)?;
            Ok(shift_left(&c, n).into_expr())
        }
        "shift_right" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let n = arg_lit_i32(args, 1)?;
            Ok(shift_right(&c, n).into_expr())
        }
        "version" => {
            if !args.is_empty() {
                return Err(PlanExprError("fn 'version' takes no arguments".to_string()));
            }
            Ok(crate::functions::version().into_expr())
        }
        // --- Array / list ---
        "array" => {
            let exprs: Result<Vec<Expr>, _> = args.iter().map(expr_from_value).collect();
            let cols: Vec<Column> = exprs?.into_iter().map(expr_to_column).collect();
            let refs: Vec<&Column> = cols.iter().collect();
            Ok(array(&refs)
                .map_err(|e| PlanExprError(e.to_string()))?
                .into_expr())
        }
        "array_max" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::array_max(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "array_min" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::array_min(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "array_size" | "size" | "cardinality" => {
            require_args(name, args, 1)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            Ok(array_size(&c).into_expr())
        }
        "element_at" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let idx = arg_lit_i64(args, 1)?;
            Ok(element_at(&c, idx).into_expr())
        }
        "try_element_at" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let idx = arg_lit_i64(args, 1)?;
            Ok(try_element_at(&c, idx).into_expr())
        }
        "array_contains" => {
            require_args(name, args, 2)?;
            let arr = expr_to_column(arg_expr(args, 0)?);
            let val = expr_to_column(arg_expr(args, 1)?);
            Ok(array_contains(&arr, &val).into_expr())
        }
        "array_join" => {
            require_args(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let sep = arg_lit_str(args, 1)?;
            Ok(array_join(&c, &sep).into_expr())
        }
        "array_sort" => {
            require_args(name, args, 1)?;
            Ok(array_sort(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "array_distinct" => {
            require_args(name, args, 1)?;
            Ok(array_distinct(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "array_slice" => {
            require_args_min(name, args, 2)?;
            let c = expr_to_column(arg_expr(args, 0)?);
            let start = arg_lit_i64(args, 1)?;
            let length = opt_lit_i64(args, 2);
            Ok(array_slice(&c, start, length).into_expr())
        }
        "array_compact" => {
            require_args(name, args, 1)?;
            Ok(array_compact(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "array_remove" => {
            require_args(name, args, 2)?;
            let arr = expr_to_column(arg_expr(args, 0)?);
            let val = expr_to_column(arg_expr(args, 1)?);
            Ok(array_remove(&arr, &val).into_expr())
        }
        "explode" => {
            require_args(name, args, 1)?;
            Ok(explode(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "explode_outer" => {
            require_args(name, args, 1)?;
            Ok(explode_outer(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "inline" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::inline(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "inline_outer" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::inline_outer(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "sequence" => {
            require_args_min(name, args, 2)?;
            let start = expr_to_column(arg_expr(args, 0)?);
            let stop = expr_to_column(arg_expr(args, 1)?);
            let step = if args.len() > 2 {
                Some(expr_to_column(arg_expr(args, 2)?))
            } else {
                None
            };
            Ok(crate::functions::sequence(&start, &stop, step.as_ref()).into_expr())
        }
        "shuffle" => {
            require_args(name, args, 1)?;
            Ok(crate::functions::shuffle(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "array_position" => {
            require_args(name, args, 2)?;
            let arr = expr_to_column(arg_expr(args, 0)?);
            let val = expr_to_column(arg_expr(args, 1)?);
            Ok(crate::functions::array_position(&arr, &val).into_expr())
        }
        "array_append" => {
            require_args(name, args, 2)?;
            let arr = expr_to_column(arg_expr(args, 0)?);
            let elem = expr_to_column(arg_expr(args, 1)?);
            Ok(array_append(&arr, &elem).into_expr())
        }
        "array_prepend" => {
            require_args(name, args, 2)?;
            let arr = expr_to_column(arg_expr(args, 0)?);
            let elem = expr_to_column(arg_expr(args, 1)?);
            Ok(array_prepend(&arr, &elem).into_expr())
        }
        "array_insert" => {
            require_args(name, args, 3)?;
            let arr = expr_to_column(arg_expr(args, 0)?);
            let pos = expr_to_column(arg_expr(args, 1)?);
            let elem = expr_to_column(arg_expr(args, 2)?);
            Ok(array_insert(&arr, &pos, &elem).into_expr())
        }
        "array_except" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(array_except(&a, &b).into_expr())
        }
        "array_intersect" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(array_intersect(&a, &b).into_expr())
        }
        "array_union" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(array_union(&a, &b).into_expr())
        }
        "arrays_overlap" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(arrays_overlap(&a, &b).into_expr())
        }
        "arrays_zip" => {
            require_args(name, args, 2)?;
            let a = expr_to_column(arg_expr(args, 0)?);
            let b = expr_to_column(arg_expr(args, 1)?);
            Ok(arrays_zip(&a, &b).into_expr())
        }
        "array_agg" => {
            require_args(name, args, 1)?;
            Ok(array_agg(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "array_sum" => {
            require_args(name, args, 1)?;
            Ok(array_sum(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        // --- Map / struct ---
        "create_map" | "createMap" => {
            // PySpark F.create_map(key1, val1, ...) or empty map {} per row (#512, #542).
            let exprs: Result<Vec<Expr>, _> = args.iter().map(expr_from_value).collect();
            let cols: Vec<Column> = exprs?.into_iter().map(expr_to_column).collect();
            let refs: Vec<&Column> = cols.iter().collect();
            Ok(create_map(&refs)
                .map_err(|e| PlanExprError(e.to_string()))?
                .into_expr())
        }
        "map_keys" => {
            require_args(name, args, 1)?;
            Ok(map_keys(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "map_values" => {
            require_args(name, args, 1)?;
            Ok(map_values(&expr_to_column(arg_expr(args, 0)?)).into_expr())
        }
        "get" => {
            require_args(name, args, 2)?;
            let map_col = expr_to_column(arg_expr(args, 0)?);
            let key = expr_to_column(arg_expr(args, 1)?);
            Ok(get(&map_col, &key).into_expr())
        }
        "get_field" | "getField" => {
            // Struct field by name (PySpark Column.getField). Used e.g. to assert withField result (issue #541).
            require_args(name, args, 2)?;
            let struct_col = expr_to_column(arg_expr(args, 0)?);
            let field_name = lit_as_string(&args[1])?;
            Ok(struct_col.get_field(&field_name).into_expr())
        }
        "get_item" => {
            // Array: get_item(col, 0); map: get_item(col, "key") (issue #522)
            require_args(name, args, 2)?;
            let col_c = expr_to_column(arg_expr(args, 0)?);
            let second = &args[1];
            if let Some(idx) = second.get("lit").and_then(|v| v.as_i64()) {
                Ok(col_c.get_item(idx).into_expr())
            } else {
                let key = expr_to_column(arg_expr(args, 1)?);
                Ok(get(&col_c, &key).into_expr())
            }
        }
        "struct" => {
            // struct(col1, col2, ...) (issue #527)
            require_args_min(name, args, 1)?;
            let cols: Vec<crate::Column> = (0..args.len())
                .map(|i| arg_expr(args, i).map(expr_to_column))
                .collect::<Result<Vec<_>, _>>()?;
            let refs: Vec<&crate::Column> = cols.iter().collect();
            Ok(crate::functions::struct_(&refs).into_expr())
        }
        "named_struct" => {
            // named_struct("name1", col1, "name2", col2, ...) (issue #527)
            require_args_min(name, args, 2)?;
            if !args.len().is_multiple_of(2) {
                return Err(PlanExprError(
                    "named_struct requires even number of args (name, value pairs)".into(),
                ));
            }
            let mut names: Vec<String> = Vec::new();
            let mut cols: Vec<crate::Column> = Vec::new();
            for i in (0..args.len()).step_by(2) {
                names.push(lit_as_string(&args[i])?);
                cols.push(expr_to_column(arg_expr(args, i + 1)?));
            }
            let refs: Vec<(&str, &crate::Column)> =
                names.iter().map(|s| s.as_str()).zip(cols.iter()).collect();
            Ok(crate::functions::named_struct(&refs).into_expr())
        }
        "with_field" | "withField" => {
            // withField(struct_col, field_name, value) - PySpark struct field add/replace (issue #541)
            require_args(name, args, 3)?;
            let struct_col = expr_to_column(arg_expr(args, 0)?);
            let field_name = lit_as_string(&args[1])?;
            let value_col = expr_to_column(arg_expr(args, 2)?);
            let out = struct_col
                .try_with_field(&field_name, &value_col)
                .map_err(|e| PlanExprError(format!("with_field: {e}")))?;
            Ok(out.into_expr())
        }
        "nvl2" => {
            require_args(name, args, 3)?;
            let col1 = expr_to_column(arg_expr(args, 0)?);
            let col2 = expr_to_column(arg_expr(args, 1)?);
            let col3 = expr_to_column(arg_expr(args, 2)?);
            Ok(nvl2(&col1, &col2, &col3).into_expr())
        }
        _ => Err(PlanExprError(format!("unsupported function: {name}"))),
    }
}

fn require_args(name: &str, args: &[Value], n: usize) -> Result<(), PlanExprError> {
    if args.len() != n {
        return Err(PlanExprError(format!(
            "fn '{name}' requires exactly {n} argument(s)"
        )));
    }
    Ok(())
}

fn require_args_min(name: &str, args: &[Value], n: usize) -> Result<(), PlanExprError> {
    if args.len() < n {
        return Err(PlanExprError(format!(
            "fn '{name}' requires at least {n} argument(s)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_col() {
        let v = json!({"col": "age"});
        let _e = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_lit_i64() {
        let v = json!({"lit": 30});
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_gt() {
        let v = json!({
            "op": "gt",
            "left": {"col": "age"},
            "right": {"lit": 30}
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_and() {
        let v = json!({
            "op": "and",
            "left": {"op": "gt", "left": {"col": "a"}, "right": {"lit": 1}},
            "right": {"op": "lt", "left": {"col": "b"}, "right": {"lit": 10}}
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_upper() {
        let v = json!({"fn": "upper", "args": [{"col": "name"}]});
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_length() {
        let v = json!({"fn": "length", "args": [{"col": "name"}]});
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_substring() {
        let v = json!({
            "fn": "substring",
            "args": [{"col": "s"}, {"lit": 1}, {"lit": 3}]
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_year() {
        let v = json!({"fn": "year", "args": [{"col": "ts"}]});
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_cast() {
        let v = json!({
            "fn": "cast",
            "args": [{"col": "x"}, {"lit": "string"}]
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_isin_op() {
        let v = json!({
            "op": "isin",
            "left": {"col": "id"},
            "right": {"lit": [1, 3]}
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_isin_fn() {
        let v = json!({
            "fn": "isin",
            "args": [{"col": "id"}, {"lit": 1}, {"lit": 3}]
        });
        let _ = expr_from_value(&v).unwrap();
    }

    /// col.isin([]) returns false for all rows (issue #518).
    #[test]
    fn test_isin_op_empty() {
        let v = json!({
            "op": "isin",
            "left": {"col": "id"},
            "right": {"lit": []}
        });
        let expr = expr_from_value(&v).unwrap();
        // Should be lit(false), not is_in with empty series
        assert!(matches!(expr, Expr::Literal(_)));
    }

    /// col.isin() with no values or col.isin(null) -> false (issue #518).
    #[test]
    fn test_isin_fn_empty() {
        let v = json!({
            "fn": "isin",
            "args": [{"col": "id"}]
        });
        let expr = expr_from_value(&v).unwrap();
        assert!(matches!(expr, Expr::Literal(_)));
        let v2 = json!({
            "fn": "isin",
            "args": [{"col": "id"}, {"lit": null}]
        });
        let expr2 = expr_from_value(&v2).unwrap();
        assert!(matches!(expr2, Expr::Literal(_)));
    }

    #[test]
    fn test_struct_named_struct_fn() {
        let v = json!({"fn": "struct", "args": [{"col": "a"}, {"col": "b"}]});
        let _ = expr_from_value(&v).unwrap();
        let v2 = json!({
            "fn": "named_struct",
            "args": [{"lit": "x"}, {"col": "a"}, {"lit": "y"}, {"col": "b"}]
        });
        let _ = expr_from_value(&v2).unwrap();
    }

    #[test]
    fn test_get_item_fn() {
        let v = json!({"fn": "get_item", "args": [{"col": "arr"}, {"lit": 0}]});
        let _ = expr_from_value(&v).unwrap();
        let v2 = json!({"fn": "get_item", "args": [{"col": "m"}, {"lit": "key"}]});
        let _ = expr_from_value(&v2).unwrap();
    }

    #[test]
    fn test_get_item_op() {
        let v = json!({"op": "getItem", "left": {"col": "arr"}, "right": {"lit": 1}});
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_startswith_op() {
        let v = json!({
            "op": "startswith",
            "left": {"col": "name"},
            "right": {"lit": "A"}
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_is_null_op() {
        let v = json!({"op": "is_null", "arg": {"col": "x"}});
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_is_not_null_op() {
        let v = json!({"op": "is_not_null", "arg": {"col": "x"}});
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_regexp_extract_op() {
        let v = json!({
            "op": "regexp_extract",
            "left": {"col": "s"},
            "pattern": {"lit": r"(\w+)"},
            "group": {"lit": 1}
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_regexp_replace_op() {
        let v = json!({
            "op": "regexp_replace",
            "left": {"col": "str"},
            "pattern": {"lit": r"\d"},
            "replacement": {"lit": "X"}
        });
        let _ = expr_from_value(&v).unwrap();
        let v2 = json!({
            "op": "regexp_replace",
            "args": [{"col": "str"}, {"lit": r"\d"}, {"lit": "X"}]
        });
        let _ = expr_from_value(&v2).unwrap();
    }

    #[test]
    fn test_create_map_op() {
        let v = json!({
            "op": "create_map",
            "args": [{"lit": "k"}, {"col": "a"}]
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_create_map_fn_empty() {
        // PySpark F.create_map() with no args: empty map {} per row (#512).
        let v = json!({"fn": "create_map", "args": []});
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_type_window_row_number_order_by() {
        let v = json!({
            "type": "window",
            "fn": "row_number",
            "window": {"order_by": ["val"]}
        });
        let _ = expr_from_value(&v).unwrap();
    }

    /// Sparkless format: fn + args + window; order_by as [{"col": "salary", "asc": true}] (issue #517).
    #[test]
    fn test_window_row_number_sparkless_format() {
        let v = json!({
            "fn": "row_number",
            "args": [],
            "window": {
                "partition_by": ["dept"],
                "order_by": [{"col": "salary", "asc": true}]
            }
        });
        let _ = expr_from_value(&v).unwrap();
    }

    /// row_number() with empty partition_by and order_by (single partition; issue #520).
    #[test]
    fn test_row_number_window_empty() {
        let v = json!({
            "type": "window",
            "fn": "row_number",
            "window": {}
        });
        let _ = expr_from_value(&v).unwrap();
        let v2 = json!({
            "fn": "row_number",
            "args": [],
            "window": {"partition_by": [], "order_by": []}
        });
        let _ = expr_from_value(&v2).unwrap();
    }

    /// Sparkless may send "function" instead of "fn" (issue #517).
    #[test]
    fn test_type_window_function_key() {
        let v = json!({
            "type": "window",
            "function": "row_number",
            "window": {"partition_by": ["dept"]}
        });
        let _ = expr_from_value(&v).unwrap();
    }

    /// rank, dense_rank in plan execution (issue #521).
    #[test]
    fn test_window_rank_dense_rank() {
        let v = json!({
            "fn": "rank",
            "args": [],
            "window": {"partition_by": ["dept"], "order_by": ["salary"]}
        });
        let _ = expr_from_value(&v).unwrap();
        let v2 = json!({
            "type": "window",
            "fn": "dense_rank",
            "window": {"order_by": ["val"]}
        });
        let _ = expr_from_value(&v2).unwrap();
    }

    #[test]
    fn test_window_plus_literal_op() {
        // (row_number().over(w) + 10) - add op with window as left
        let v = json!({
            "op": "add",
            "left": {"type": "window", "fn": "row_number", "window": {"order_by": ["val"]}},
            "right": {"lit": 10}
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_when_two_arg() {
        let v = json!({
            "fn": "when",
            "args": [
                {"op": "gt", "left": {"col": "a"}, "right": {"lit": 0}},
                {"lit": "positive"}
            ]
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_concat() {
        let v = json!({
            "fn": "concat",
            "args": [{"col": "first"}, {"lit": " "}, {"col": "last"}]
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_greatest() {
        let v = json!({
            "fn": "greatest",
            "args": [{"col": "a"}, {"col": "b"}, {"lit": 0}]
        });
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_array_size() {
        let v = json!({"fn": "array_size", "args": [{"col": "arr"}]});
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_element_at() {
        let v = json!({"fn": "element_at", "args": [{"col": "arr"}, {"lit": 1}]});
        let _ = expr_from_value(&v).unwrap();
    }

    #[test]
    fn test_coalesce() {
        let v = json!({
            "fn": "coalesce",
            "args": [{"col": "a"}, {"col": "b"}, {"lit": 0}]
        });
        let _ = expr_from_value(&v).unwrap();
    }
}
