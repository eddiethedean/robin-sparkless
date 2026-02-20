//! Convert core ExprIr to Polars Expr. Used when the root API passes ExprIr into the backend.

use polars::prelude::{Expr, col, lit, when};
use robin_sparkless_core::{EngineError, ExprIr, LiteralValue};

/// Convert engine-agnostic ExprIr into a Polars Expr.
pub fn expr_ir_to_expr(ir: &ExprIr) -> Result<Expr, EngineError> {
    match ir {
        ExprIr::Column(name) => Ok(col(name)),
        ExprIr::Lit(lv) => lit_from_core(lv).map_err(EngineError::Internal),

        ExprIr::Eq(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l.eq(r))
        }
        ExprIr::Ne(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l.neq(r))
        }
        ExprIr::Gt(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l.gt(r))
        }
        ExprIr::Ge(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l.gt_eq(r))
        }
        ExprIr::Lt(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l.lt(r))
        }
        ExprIr::Le(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l.lt_eq(r))
        }
        ExprIr::EqNullSafe(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            let (left_c, right_c) = crate::type_coercion::coerce_for_pyspark_eq_null_safe(l, r)
                .map_err(|e| EngineError::Internal(e.to_string()))?;
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

        ExprIr::And(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l.and(r))
        }
        ExprIr::Or(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l.or(r))
        }
        ExprIr::Not(a) => {
            let x = expr_ir_to_expr(a)?;
            Ok(x.not())
        }

        ExprIr::Add(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l + r)
        }
        ExprIr::Sub(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l - r)
        }
        ExprIr::Mul(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l * r)
        }
        ExprIr::Div(a, b) => {
            let l = expr_ir_to_expr(a)?;
            let r = expr_ir_to_expr(b)?;
            Ok(l / r)
        }

        ExprIr::Between { left, lower, upper } => {
            let l = expr_ir_to_expr(left)?;
            let lo = expr_ir_to_expr(lower)?;
            let hi = expr_ir_to_expr(upper)?;
            Ok(l.clone().gt_eq(lo).and(l.lt_eq(hi)))
        }
        ExprIr::IsIn(left, right) => {
            let l = expr_ir_to_expr(left)?;
            let r = expr_ir_to_expr(right)?;
            Ok(l.is_in(r, false))
        }

        ExprIr::IsNull(a) => {
            let x = expr_ir_to_expr(a)?;
            Ok(x.is_null())
        }
        ExprIr::IsNotNull(a) => {
            let x = expr_ir_to_expr(a)?;
            Ok(x.is_not_null())
        }

        ExprIr::When {
            condition,
            then_expr,
            otherwise,
        } => {
            let cond = expr_ir_to_expr(condition)?;
            let then_e = expr_ir_to_expr(then_expr)?;
            let else_e = expr_ir_to_expr(otherwise)?;
            Ok(polars::prelude::when(cond).then(then_e).otherwise(else_e))
        }

        ExprIr::Call { name, args } => call_to_expr(name, args),
    }
}

fn lit_from_core(lv: &LiteralValue) -> Result<Expr, String> {
    Ok(match lv {
        LiteralValue::I64(n) => lit(*n),
        LiteralValue::I32(n) => lit(*n),
        LiteralValue::F64(n) => lit(*n),
        LiteralValue::Str(s) => lit(s.as_str()),
        LiteralValue::Bool(b) => lit(*b),
        LiteralValue::Null => lit(polars::prelude::NULL),
    })
}

fn call_to_expr(name: &str, args: &[ExprIr]) -> Result<Expr, EngineError> {
    match (name, args) {
        ("sum", [a]) => Ok(expr_ir_to_expr(a)?.sum()),
        ("count", [a]) => Ok(expr_ir_to_expr(a)?.count()),
        ("min", [a]) => Ok(expr_ir_to_expr(a)?.min()),
        ("max", [a]) => Ok(expr_ir_to_expr(a)?.max()),
        ("mean" | "avg", [a]) => Ok(expr_ir_to_expr(a)?.mean()),
        ("first", [a]) => Ok(expr_ir_to_expr(a)?.first()),
        ("last", [a]) => Ok(expr_ir_to_expr(a)?.last()),
        ("alias", [a, _b]) => {
            let e = expr_ir_to_expr(a)?;
            let name = match &args[1] {
                ExprIr::Lit(LiteralValue::Str(s)) => s.as_str(),
                _ => {
                    return Err(EngineError::User(
                        "alias second arg must be string literal".into(),
                    ));
                }
            };
            Ok(e.alias(name))
        }
        _ => Err(EngineError::Internal(format!(
            "ExprIr::Call '{name}' not yet implemented in expr_ir_to_expr"
        ))),
    }
}
