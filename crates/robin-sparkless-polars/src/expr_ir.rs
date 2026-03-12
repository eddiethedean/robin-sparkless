//! Convert core ExprIr to Polars Expr. Used when the root API passes ExprIr into the backend.

use polars::prelude::{DataType, Expr, QuantileMethod, coalesce, col, lit, when};
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
            // #977: Polars .and() requires Boolean; coerce so string/i64 etc. never passed.
            let l = crate::functions::expr_coerce_to_boolean(expr_ir_to_expr(a)?);
            let r = crate::functions::expr_coerce_to_boolean(expr_ir_to_expr(b)?);
            Ok(l.and(r))
        }
        ExprIr::Or(a, b) => {
            // #977: Polars .or() requires Boolean; coerce so string/i64 etc. never passed.
            let l = crate::functions::expr_coerce_to_boolean(expr_ir_to_expr(a)?);
            let r = crate::functions::expr_coerce_to_boolean(expr_ir_to_expr(b)?);
            Ok(l.or(r))
        }
        ExprIr::Not(a) => {
            // #977: Polars .not() requires Boolean; coerce so string/i64 etc. never passed.
            let x = crate::functions::expr_coerce_to_boolean(expr_ir_to_expr(a)?);
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
            // #975: Polars when() requires Boolean condition; coerce so i64/string/etc. never passed as condition.
            let cond = crate::functions::expr_coerce_to_boolean(expr_ir_to_expr(condition)?);
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

fn lit_to_i64(ir: &ExprIr) -> Result<i64, EngineError> {
    match ir {
        ExprIr::Lit(LiteralValue::I64(n)) => Ok(*n),
        ExprIr::Lit(LiteralValue::F64(n)) => Ok(*n as i64),
        ExprIr::Lit(LiteralValue::I32(n)) => Ok(*n as i64),
        _ => Err(EngineError::User(
            "expected integer literal (i64, i32, or f64)".into(),
        )),
    }
}

fn lit_to_f64(ir: &ExprIr) -> Result<f64, EngineError> {
    match ir {
        ExprIr::Lit(LiteralValue::F64(n)) => Ok(*n),
        ExprIr::Lit(LiteralValue::I64(n)) => Ok(*n as f64),
        ExprIr::Lit(LiteralValue::I32(n)) => Ok(*n as f64),
        _ => Err(EngineError::User(
            "expected numeric literal (f64, i64, or i32)".into(),
        )),
    }
}

fn call_to_expr(name: &str, args: &[ExprIr]) -> Result<Expr, EngineError> {
    if let Some(expr) = agg_call_to_expr(name, args)? {
        return Ok(expr);
    }
    if let Some(expr) = cast_call_to_expr(name, args)? {
        return Ok(expr);
    }
    if let Some(expr) = string_call_to_expr(name, args)? {
        return Ok(expr);
    }
    if let Some(expr) = coalesce_call_to_expr(name, args)? {
        return Ok(expr);
    }

    Err(EngineError::Internal(format!(
        "ExprIr::Call '{name}' not yet implemented in expr_ir_to_expr"
    )))
}

/// Aggregation-style calls (sum, count, avg, percentiles, etc.).
fn agg_call_to_expr(name: &str, args: &[ExprIr]) -> Result<Option<Expr>, EngineError> {
    let out = match (name, args) {
        ("sum", [a]) => Some(expr_ir_to_expr(a)?.sum()),
        ("count", [a]) => Some(expr_ir_to_expr(a)?.count()),
        ("min", [a]) => Some(expr_ir_to_expr(a)?.min()),
        ("max", [a]) => Some(expr_ir_to_expr(a)?.max()),
        ("mean" | "avg", [a]) => Some(expr_ir_to_expr(a)?.mean()),
        ("first", [a]) => Some(expr_ir_to_expr(a)?.first()),
        ("last", [a]) => Some(expr_ir_to_expr(a)?.last()),
        ("try_sum", [a]) => Some(expr_ir_to_expr(a)?.sum()),
        ("try_avg", [a]) => Some(expr_ir_to_expr(a)?.mean()),
        ("stddev" | "std" | "stddev_samp", [a]) => Some(expr_ir_to_expr(a)?.std(1)),
        ("stddev_pop", [a]) => Some(expr_ir_to_expr(a)?.std(0)),
        ("variance" | "var_samp", [a]) => Some(expr_ir_to_expr(a)?.var(1)),
        ("var_pop", [a]) => Some(expr_ir_to_expr(a)?.var(0)),
        ("count_distinct" | "approx_count_distinct", [a]) => {
            Some(expr_ir_to_expr(a)?.n_unique().cast(DataType::Int64))
        }
        ("collect_list", [a]) => Some(expr_ir_to_expr(a)?.implode()),
        ("collect_set", [a]) => Some(expr_ir_to_expr(a)?.unique().implode()),
        ("bool_and" | "every", [a]) => {
            // #980: Polars .all() requires Boolean; coerce so string/i64 etc. never passed.
            Some(
                crate::functions::expr_coerce_to_boolean(expr_ir_to_expr(a)?).all(true),
            )
        }
        ("median", [a]) => {
            Some(expr_ir_to_expr(a)?.quantile(lit(0.5), QuantileMethod::Linear))
        }
        ("count_if", [a]) => {
            // #981: count_if counts where condition is true; Polars cast(Int64) on Boolean; coerce condition to Boolean.
            Some(
                crate::functions::expr_coerce_to_boolean(expr_ir_to_expr(a)?)
                    .cast(DataType::Int64)
                    .sum(),
            )
        }
        ("mode", [a]) => {
            let e = expr_ir_to_expr(a)?;
            let col = crate::column::Column::from_expr(e, None);
            Some(col.mode().into_expr())
        }
        ("kurtosis", [a]) => Some(
            expr_ir_to_expr(a)?
                .cast(DataType::Float64)
                .kurtosis(true, true),
        ),
        ("skewness", [a]) => {
            Some(expr_ir_to_expr(a)?.cast(DataType::Float64).skew(true))
        }
        ("approx_percentile" | "percentile_approx", [a, b]) => {
            let e = expr_ir_to_expr(a)?;
            let p = lit_to_f64(b)?;
            Some(e.quantile(lit(p), QuantileMethod::Linear))
        }
        _ => None,
    };
    Ok(out)
}

/// Alias and cast-style calls.
fn cast_call_to_expr(name: &str, args: &[ExprIr]) -> Result<Option<Expr>, EngineError> {
    let out = match (name, args) {
        ("alias", [a, _b]) => {
            let e = expr_ir_to_expr(a)?;
            let alias = match &args[1] {
                ExprIr::Lit(LiteralValue::Str(s)) => s.as_str(),
                _ => {
                    return Err(EngineError::User(
                        "alias second arg must be string literal".into(),
                    ));
                }
            };
            Some(e.alias(alias))
        }
        ("cast", [a, _b]) => {
            let type_name = match &args[1] {
                ExprIr::Lit(LiteralValue::Str(s)) => s.as_str(),
                _ => {
                    return Err(EngineError::User(
                        "cast second arg must be string literal (type name)".into(),
                    ));
                }
            };
            let e = expr_ir_to_expr(a)?;
            let col = crate::column::Column::from_expr(e, None);
            let out = crate::functions::cast(&col, type_name).map_err(EngineError::User)?;
            Some(out.into_expr())
        }
        ("try_cast", [a, _b]) => {
            let type_name = match &args[1] {
                ExprIr::Lit(LiteralValue::Str(s)) => s.as_str(),
                _ => {
                    return Err(EngineError::User(
                        "try_cast second arg must be string literal (type name)".into(),
                    ));
                }
            };
            let e = expr_ir_to_expr(a)?;
            let col = crate::column::Column::from_expr(e, None);
            let out =
                crate::functions::try_cast(&col, type_name).map_err(EngineError::User)?;
            Some(out.into_expr())
        }
        _ => None,
    };
    Ok(out)
}

/// String and substring-style calls.
fn string_call_to_expr(name: &str, args: &[ExprIr]) -> Result<Option<Expr>, EngineError> {
    let out = match (name, args) {
        ("upper", [a]) => Some(expr_ir_to_expr(a)?.str().to_uppercase()),
        ("lower", [a]) => Some(expr_ir_to_expr(a)?.str().to_lowercase()),
        ("length", [a]) => Some(expr_ir_to_expr(a)?.str().len_chars()),
        ("trim", [a]) => Some(expr_ir_to_expr(a)?.str().strip_chars(lit(" \t\n\r"))),
        ("ltrim", [a]) => Some(
            expr_ir_to_expr(a)?
                .str()
                .strip_chars_start(lit(" \t\n\r")),
        ),
        ("rtrim", [a]) => Some(
            expr_ir_to_expr(a)?
                .str()
                .strip_chars_end(lit(" \t\n\r")),
        ),
        ("substring" | "substr", args) if args.len() >= 2 => {
            let col_expr = expr_ir_to_expr(&args[0])?;
            let start = lit_to_i64(&args[1])?;
            let length = args.get(2).map(lit_to_i64).transpose()?;
            let col = crate::column::Column::from_expr(col_expr, None);
            Some(col.substr(start, length).into_expr())
        }
        _ => None,
    };
    Ok(out)
}

/// Coalesce / nvl-style calls.
fn coalesce_call_to_expr(name: &str, args: &[ExprIr]) -> Result<Option<Expr>, EngineError> {
    let out = match (name, args) {
        ("coalesce" | "nvl", args) if !args.is_empty() => {
            let exprs: Result<Vec<Expr>, _> = args.iter().map(expr_ir_to_expr).collect();
            Some(coalesce(&exprs?))
        }
        _ => None,
    };
    Ok(out)
}
