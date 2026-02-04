//! Expression interpreter: turn serialized expression trees (JSON/serde) into Polars Expr.
//! Used by the plan interpreter for filter, select, and withColumn payloads.

use polars::prelude::{col, lit, Expr};
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
                let left = obj
                    .get("left")
                    .ok_or_else(|| PlanExprError(format!("op '{}' requires 'left'", op)))?;
                let right = obj
                    .get("right")
                    .ok_or_else(|| PlanExprError(format!("op '{}' requires 'right'", op)))?;
                let l = expr_from_value(left)?;
                let r = expr_from_value(right)?;
                return Ok(match op {
                    "eq" => l.eq(r),
                    "ne" => l.neq(r),
                    "gt" => l.gt(r),
                    "ge" => l.gt_eq(r),
                    "lt" => l.lt(r),
                    "le" => l.lt_eq(r),
                    _ => unreachable!(),
                });
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
                return Ok(eq_null_safe_expr(l, r));
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
            _ => {
                return Err(PlanExprError(format!("unsupported expression op: {}", op)));
            }
        }
    }

    // Function call: {"fn": "upper"|"lower"|..., "args": [<expr>, ...]}
    if let Some(fn_name) = obj.get("fn").and_then(Value::as_str) {
        let args = obj
            .get("args")
            .and_then(Value::as_array)
            .ok_or_else(|| PlanExprError(format!("fn '{}' requires 'args' array", fn_name)))?;
        return expr_from_fn(fn_name, args);
    }

    Err(PlanExprError(
        "expression must have 'col', 'lit', 'op', or 'fn'".to_string(),
    ))
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

/// Null-safe equality: (a <=> b) is true when both null, or both non-null and equal.
fn eq_null_safe_expr(left: Expr, right: Expr) -> Expr {
    use polars::prelude::*;
    let left_null = left.clone().is_null();
    let right_null = right.clone().is_null();
    let both_null = left_null.clone().and(right_null.clone());
    let both_non_null = left_null.not().and(right_null.not());
    let eq_result = left.eq(right);
    when(both_null)
        .then(lit(true))
        .when(both_non_null)
        .then(eq_result)
        .otherwise(lit(false))
}

fn expr_from_fn(name: &str, args: &[Value]) -> Result<Expr, PlanExprError> {
    match name {
        "upper" => {
            if args.len() != 1 {
                return Err(PlanExprError("upper() requires exactly one argument".to_string()));
            }
            let e = expr_from_value(&args[0])?;
            Ok(e.str().to_uppercase())
        }
        "lower" => {
            if args.len() != 1 {
                return Err(PlanExprError("lower() requires exactly one argument".to_string()));
            }
            let e = expr_from_value(&args[0])?;
            Ok(e.str().to_lowercase())
        }
        "coalesce" => {
            if args.is_empty() {
                return Err(PlanExprError("coalesce() requires at least one argument".to_string()));
            }
            let exprs: Result<Vec<Expr>, _> = args.iter().map(expr_from_value).collect();
            let exprs = exprs?;
            Ok(polars::prelude::coalesce(&exprs))
        }
        _ => Err(PlanExprError(format!("unsupported function: {}", name))),
    }
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
}
