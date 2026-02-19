//! Translate sqlparser AST to DataFrame operations.
//! Resolves unknown functions as UDFs from the session registry.

use std::collections::HashMap;

use crate::column::Column;
use crate::dataframe::{join, DataFrame, JoinType};
use crate::functions;
use crate::session::{set_thread_udf_session, SparkSession};
use polars::prelude::{col, lit, DataFrame as PlDataFrame, Expr, PolarsError};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, GroupByExpr,
    JoinConstraint, JoinOperator, ObjectType, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, Value,
};

use super::parser;

/// Parse a single SQL expression string and convert to Polars Expr using the given DataFrame for column resolution.
/// Used by selectExpr/expr() for PySpark parity. Parses "SELECT expr FROM __t" and returns the first select item's Expr.
pub fn expr_string_to_polars(
    expr_str: &str,
    session: &SparkSession,
    df: &DataFrame,
) -> Result<Expr, PolarsError> {
    let query = format!("SELECT {} FROM __selectexpr_t", expr_str);
    let stmt = parser::parse_sql(&query)?;
    let query_ast = match &stmt {
        Statement::Query(q) => q.as_ref(),
        _ => {
            return Err(PolarsError::InvalidOperation(
                "expr_string_to_polars: expected SELECT statement".into(),
            ));
        }
    };
    let body = match query_ast.body.as_ref() {
        SetExpr::Select(s) => s.as_ref(),
        _ => {
            return Err(PolarsError::InvalidOperation(
                "expr_string_to_polars: expected SELECT".into(),
            ));
        }
    };
    let first = body.projection.first().ok_or_else(|| {
        PolarsError::InvalidOperation("expr_string_to_polars: empty SELECT list".into())
    })?;
    set_thread_udf_session(session.clone());
    let (sql_expr, alias) = match first {
        SelectItem::UnnamedExpr(e) => ((*e).clone(), None),
        SelectItem::ExprWithAlias { expr, alias: a } => ((*expr).clone(), Some(a.value.as_str())),
        _ => {
            return Err(PolarsError::InvalidOperation(
                format!("expr_string_to_polars: unsupported select item {:?}", first).into(),
            ));
        }
    };
    let expr = sql_expr_to_polars(&sql_expr, session, Some(df), None)?;
    Ok(match alias {
        Some(a) => expr.alias(a),
        None => expr,
    })
}

/// Translate a parsed Statement (Query or DDL) into a DataFrame using the session catalog.
/// CREATE SCHEMA / CREATE DATABASE return empty DataFrame. DROP TABLE / DROP VIEW remove from session catalog.
pub fn translate(
    session: &SparkSession,
    stmt: &Statement,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    set_thread_udf_session(session.clone());
    match stmt {
        Statement::Query(q) => translate_query(session, q.as_ref()),
        Statement::CreateSchema { schema_name, .. } => {
            let name = schema_name.to_string();
            session.register_database(&name);
            Ok(DataFrame::from_polars_with_options(
                PlDataFrame::empty(),
                session.is_case_sensitive(),
            ))
        }
        Statement::CreateDatabase { db_name, .. } => {
            let name = db_name.to_string();
            session.register_database(&name);
            Ok(DataFrame::from_polars_with_options(
                PlDataFrame::empty(),
                session.is_case_sensitive(),
            ))
        }
        Statement::Drop {
            object_type: ObjectType::Table | ObjectType::View,
            names,
            ..
        } => {
            for obj_name in names {
                let name = obj_name.to_string();
                if name.starts_with("global_temp.") {
                    if let Some(suffix) = name.strip_prefix("global_temp.") {
                        session.drop_global_temp_view(suffix);
                    }
                }
                session.drop_temp_view(&name);
                session.drop_table(&name);
            }
            Ok(DataFrame::from_polars_with_options(
                PlDataFrame::empty(),
                session.is_case_sensitive(),
            ))
        }
        Statement::Drop {
            object_type: ObjectType::Schema,
            names,
            ..
        } => {
            for obj_name in names {
                session.drop_database(&obj_name.to_string());
            }
            Ok(DataFrame::from_polars_with_options(
                PlDataFrame::empty(),
                session.is_case_sensitive(),
            ))
        }
        _ => Err(PolarsError::InvalidOperation(
            "SQL: only SELECT, CREATE SCHEMA/DATABASE, and DROP TABLE/VIEW/SCHEMA are supported."
                .into(),
        )),
    }
}

fn translate_query(
    session: &SparkSession,
    query: &Query,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let body = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        _ => {
            return Err(PolarsError::InvalidOperation(
                "SQL: only SELECT (no UNION/EXCEPT/INTERSECT) is supported.".into(),
            ));
        }
    };
    let mut df = translate_select_from(session, body)?;
    if let Some(selection) = &body.selection {
        let expr = sql_expr_to_polars(selection, session, Some(&df), None)?;
        df = df.filter(expr)?;
    }
    let group_exprs: &[SqlExpr] = match &body.group_by {
        GroupByExpr::Expressions(exprs) => exprs.as_slice(),
        GroupByExpr::All => {
            return Err(PolarsError::InvalidOperation(
                "SQL: GROUP BY ALL is not supported. Use explicit GROUP BY columns.".into(),
            ));
        }
    };
    let has_group_by = !group_exprs.is_empty();
    let mut having_agg_map: HashMap<(String, String), String> = HashMap::new();
    if has_group_by {
        // Support GROUP BY column name or expression, e.g. GROUP BY age or GROUP BY (age > 30) (issue #588).
        let pairs: Vec<(Expr, String)> = group_exprs
            .iter()
            .enumerate()
            .map(|(i, e)| {
                Ok(match e {
                    SqlExpr::Identifier(ident) => {
                        let name = ident.value.as_str();
                        let resolved = df.resolve_column_name(name)?;
                        (col(resolved.as_str()), resolved)
                    }
                    SqlExpr::CompoundIdentifier(parts) => {
                        let name = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                        let resolved = df.resolve_column_name(name)?;
                        (col(resolved.as_str()), resolved)
                    }
                    _ => {
                        let expr = sql_expr_to_polars(e, session, Some(&df), None)?;
                        let name = format!("group_{}", i);
                        (expr.alias(&name), name)
                    }
                })
            })
            .collect::<Result<Vec<_>, PolarsError>>()?;
        let (group_exprs_polars, group_cols): (Vec<Expr>, Vec<String>) = pairs.into_iter().unzip();
        let grouped = df.group_by_exprs(group_exprs_polars, group_cols.clone())?;
        let mut agg_exprs = projection_to_agg_exprs(&body.projection, &group_cols, &df)?;
        if let Some(having_expr) = &body.having {
            let having_list = extract_having_agg_calls(having_expr);
            for (func, alias) in &having_list {
                push_agg_function(
                    &func.name,
                    &func.args,
                    &df,
                    Some(alias.as_str()),
                    &mut agg_exprs,
                )?;
            }
            having_agg_map = having_list
                .into_iter()
                .filter_map(|(f, alias)| agg_function_key(&f).map(|k| (k, alias)))
                .collect();
        }
        if agg_exprs.is_empty() {
            df = grouped.count()?;
        } else {
            df = grouped.agg(agg_exprs)?;
        }
    } else if projection_is_scalar_aggregate(&body.projection) {
        // SELECT AVG(salary) FROM t (no GROUP BY) — scalar aggregation (issue #587).
        let agg_exprs = projection_to_agg_exprs(&body.projection, &[], &df)?;
        let pl_df = df.lazy_frame().select(agg_exprs).collect()?;
        df = DataFrame::from_polars_with_options(pl_df, df.case_sensitive);
    } else {
        df = apply_projection(&df, &body.projection, session)?;
    }
    if let Some(having_expr) = &body.having {
        let having_polars = sql_expr_to_polars(
            having_expr,
            session,
            Some(&df),
            Some(&having_agg_map).filter(|m| !m.is_empty()),
        )?;
        df = df.filter(having_polars)?;
    }
    if !query.order_by.is_empty() {
        let pairs: Vec<(String, bool)> = query
            .order_by
            .iter()
            .map(|o| {
                let col_name = sql_expr_to_col_name(&o.expr)?;
                let resolved = df.resolve_column_name(&col_name)?;
                let ascending = o.asc.unwrap_or(true);
                Ok((resolved, ascending))
            })
            .collect::<Result<Vec<_>, PolarsError>>()?;
        let (cols, asc): (Vec<String>, Vec<bool>) = pairs.into_iter().unzip();
        let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
        df = df.order_by(col_refs, asc)?;
    }
    if let Some(limit_expr) = &query.limit {
        let n = sql_limit_to_usize(limit_expr)?;
        df = df.limit(n)?;
    }
    Ok(df)
}

fn translate_select_from(
    session: &SparkSession,
    select: &Select,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    if select.from.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "SQL: FROM clause is required. Register a table with create_or_replace_temp_view."
                .into(),
        ));
    }
    let first_tj = &select.from[0];
    let mut df = resolve_table_factor(session, &first_tj.relation)?;
    for join_spec in &first_tj.joins {
        let right_df = resolve_table_factor(session, &join_spec.relation)?;
        let join_type = match &join_spec.join_operator {
            JoinOperator::Inner(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) => JoinType::Left,
            JoinOperator::RightOuter(_) => JoinType::Right,
            JoinOperator::FullOuter(_) => JoinType::Outer,
            _ => {
                return Err(PolarsError::InvalidOperation(
                    "SQL: only INNER, LEFT, RIGHT, FULL JOIN are supported.".into(),
                ));
            }
        };
        let on_cols = join_condition_to_on_columns(&join_spec.join_operator)?;
        let on_refs: Vec<&str> = on_cols.iter().map(|s| s.as_str()).collect();
        df = join(
            &df,
            &right_df,
            on_refs,
            join_type,
            session.is_case_sensitive(),
        )?;
    }
    Ok(df)
}

fn resolve_table_factor(
    session: &SparkSession,
    factor: &TableFactor,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    match factor {
        TableFactor::Table { name, .. } => {
            // Build full name for global_temp.xyz (sqlparser: [Ident("global_temp"), Ident("people")])
            let table_name = if name.0.len() >= 2 {
                let parts: Vec<&str> = name.0.iter().map(|i| i.value.as_str()).collect();
                parts.join(".")
            } else {
                name.0
                    .last()
                    .map(|i| i.value.as_str())
                    .unwrap_or("")
                    .to_string()
            };
            session.table(&table_name)
        }
        _ => Err(PolarsError::InvalidOperation(
            "SQL: only plain table names are supported in FROM (no subqueries, derived tables). Register with create_or_replace_temp_view.".into(),
        )),
    }
}

fn join_condition_to_on_columns(join_op: &JoinOperator) -> Result<Vec<String>, PolarsError> {
    let constraint = match join_op {
        JoinOperator::Inner(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c) => c,
        _ => {
            return Err(PolarsError::InvalidOperation(
                "SQL: only INNER/LEFT/RIGHT/FULL JOIN with ON are supported.".into(),
            ));
        }
    };
    match constraint {
        JoinConstraint::On(expr) => match expr {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let l = sql_expr_to_col_name(left.as_ref())?;
                let r = sql_expr_to_col_name(right.as_ref())?;
                if l != r {
                    return Err(PolarsError::InvalidOperation(
                            "SQL: JOIN ON must use same column name on both sides (e.g. a.id = b.id where both become 'id').".into(),
                        ));
                }
                Ok(vec![l])
            }
            _ => Err(PolarsError::InvalidOperation(
                "SQL: JOIN ON must be a single equality (col = col).".into(),
            )),
        },
        _ => Err(PolarsError::InvalidOperation(
            "SQL: JOIN must use ON (equality); NATURAL/USING not supported.".into(),
        )),
    }
}

fn sql_expr_to_polars(
    expr: &SqlExpr,
    session: &SparkSession,
    df: Option<&DataFrame>,
    having_agg_map: Option<&HashMap<(String, String), String>>,
) -> Result<Expr, PolarsError> {
    match expr {
        SqlExpr::Identifier(ident) => {
            let name = ident.value.as_str();
            let resolved = df
                .map(|d| d.resolve_column_name(name))
                .transpose()?
                .unwrap_or_else(|| name.to_string());
            Ok(col(resolved.as_str()))
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let name = parts
                .last()
                .map(|i| i.value.as_str())
                .unwrap_or("");
            let resolved = df
                .map(|d| d.resolve_column_name(name))
                .transpose()?
                .unwrap_or_else(|| name.to_string());
            Ok(col(resolved.as_str()))
        }
        SqlExpr::Value(Value::Number(s, _)) => {
            if s.contains('.') {
                let v: f64 = s.parse().map_err(|_| {
                    PolarsError::InvalidOperation(format!("SQL: invalid number literal '{}'", s).into())
                })?;
                Ok(lit(v))
            } else {
                let v: i64 = s.parse().map_err(|_| {
                    PolarsError::InvalidOperation(format!("SQL: invalid integer literal '{}'", s).into())
                })?;
                Ok(lit(v))
            }
        }
        SqlExpr::Value(Value::SingleQuotedString(s)) => Ok(lit(s.as_str())),
        SqlExpr::Value(Value::Boolean(b)) => Ok(lit(*b)),
        SqlExpr::Value(Value::Null) => Ok(lit(polars::prelude::LiteralValue::Null)),
        SqlExpr::BinaryOp { left, op, right } => {
            let l = sql_expr_to_polars(left, session, df, having_agg_map)?;
            let r = sql_expr_to_polars(right, session, df, having_agg_map)?;
            match op {
                BinaryOperator::Eq => Ok(l.eq(r)),
                BinaryOperator::NotEq => Ok(l.eq(r).not()),
                BinaryOperator::Gt => Ok(l.gt(r)),
                BinaryOperator::GtEq => Ok(l.gt_eq(r)),
                BinaryOperator::Lt => Ok(l.lt(r)),
                BinaryOperator::LtEq => Ok(l.lt_eq(r)),
                BinaryOperator::And => Ok(l.and(r)),
                BinaryOperator::Or => Ok(l.or(r)),
                _ => Err(PolarsError::InvalidOperation(
                    format!("SQL: unsupported operator in WHERE: {:?}. Use =, <>, <, <=, >, >=, AND, OR.", op).into(),
                )),
            }
        }
        SqlExpr::IsNull(expr) => Ok(sql_expr_to_polars(expr, session, df, having_agg_map)?.is_null()),
        SqlExpr::IsNotNull(expr) => Ok(sql_expr_to_polars(expr, session, df, having_agg_map)?.is_not_null()),
        SqlExpr::UnaryOp { op, expr } => {
            let e = sql_expr_to_polars(expr, session, df, having_agg_map)?;
            match op {
                sqlparser::ast::UnaryOperator::Not => Ok(e.not()),
                _ => Err(PolarsError::InvalidOperation(
                    format!("SQL: unsupported unary operator in WHERE: {:?}", op).into(),
                )),
            }
        }
        SqlExpr::Function(func) => {
            if let Some(map) = having_agg_map {
                if let Some(key) = agg_function_key(func) {
                    if let Some(col_name) = map.get(&key) {
                        return Ok(col(col_name.as_str()));
                    }
                }
            }
            sql_function_to_expr(func, session, df)
        }
        SqlExpr::Like {
            negated,
            expr: left,
            pattern,
            escape_char,
        } => {
            let col_expr = sql_expr_to_polars(left.as_ref(), session, df, having_agg_map)?;
            let pattern_str = sql_expr_to_string_literal(pattern.as_ref())?;
            let col_col = crate::column::Column::from_expr(col_expr, None);
            let like_expr = col_col.like(&pattern_str, *escape_char).into_expr();
            Ok(if *negated {
                like_expr.not()
            } else {
                like_expr
            })
        }
        SqlExpr::InList {
            expr: left,
            list,
            negated,
        } => {
            let col_expr = sql_expr_to_polars(left.as_ref(), session, df, having_agg_map)?;
            if list.is_empty() {
                return Ok(lit(false));
            }
            let series = sql_in_list_to_series(list)?;
            let in_expr = col_expr.is_in(lit(series));
            Ok(if *negated {
                in_expr.not()
            } else {
                in_expr
            })
        }
        _ => Err(PolarsError::InvalidOperation(
            format!("SQL: unsupported expression in WHERE: {:?}. Use column, literal, =, <, >, AND, OR, IS NULL, LIKE, IN.", expr).into(),
        )),
    }
}

/// Convert SQL function call to Polars Expr. Supports built-ins (UPPER, LOWER, etc.) and UDFs.
/// For Python UDF in WHERE/HAVING we cannot return a lazy Expr; returns error (Python UDF in
/// predicates requires eager materialization - deferred).
fn sql_function_to_expr(
    func: &Function,
    session: &SparkSession,
    df: Option<&DataFrame>,
) -> Result<Expr, PolarsError> {
    let func_name = func.name.0.last().map(|i| i.value.as_str()).unwrap_or("");
    let args = sql_function_args_to_columns(func, session, df)?;

    let case_sensitive = session.is_case_sensitive();

    // Built-in scalar functions (single-column arg)
    if let Some(col) = args.first() {
        let builtin_expr = match func_name.to_uppercase().as_str() {
            "UPPER" | "UCASE" if args.len() == 1 => Some(functions::upper(col).expr().clone()),
            "LOWER" | "LCASE" if args.len() == 1 => Some(functions::lower(col).expr().clone()),
            _ => None,
        };
        if let Some(e) = builtin_expr {
            return Ok(e);
        }
    }

    // UDF lookup
    if session.udf_registry.has_udf(func_name, case_sensitive) {
        let col = functions::call_udf(func_name, &args)?;
        if col.udf_call.is_some() {
            return Err(PolarsError::InvalidOperation(
                "SQL: Python UDF in WHERE/HAVING not yet supported. Use in SELECT.".into(),
            ));
        }
        return Ok(col.expr().clone());
    }

    Err(PolarsError::InvalidOperation(
        format!("SQL: unknown function '{}'. Register with spark.udf.register() or use built-ins: UPPER, LOWER.", func_name).into(),
    ))
}

fn sql_function_args_to_columns(
    func: &Function,
    session: &SparkSession,
    df: Option<&DataFrame>,
) -> Result<Vec<Column>, PolarsError> {
    let mut cols = Vec::new();
    for arg in &func.args {
        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
            let e = sql_expr_to_polars(expr, session, df, None)?;
            cols.push(Column::from_expr(e, None));
        } else {
            return Err(PolarsError::InvalidOperation(
                "SQL: only positional function arguments supported.".into(),
            ));
        }
    }
    Ok(cols)
}

fn sql_expr_to_col_name(expr: &SqlExpr) -> Result<String, PolarsError> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(ident.value.clone()),
        SqlExpr::CompoundIdentifier(parts) => parts
            .last()
            .map(|i| i.value.clone())
            .ok_or_else(|| PolarsError::InvalidOperation("SQL: empty compound identifier.".into())),
        _ => Err(PolarsError::InvalidOperation(
            format!("SQL: expected column name, got {:?}", expr).into(),
        )),
    }
}

/// Extract a string literal from a SQL expression (for LIKE pattern). Issue #590.
fn sql_expr_to_string_literal(expr: &SqlExpr) -> Result<String, PolarsError> {
    match expr {
        SqlExpr::Value(Value::SingleQuotedString(s)) => Ok(s.clone()),
        _ => Err(PolarsError::InvalidOperation(
            format!("SQL: LIKE pattern must be a string literal, got {:?}", expr).into(),
        )),
    }
}

/// Build a Polars Series from SQL IN list literals (for WHERE col IN (1,2,3)). Issue #590.
fn sql_in_list_to_series(list: &[SqlExpr]) -> Result<polars::prelude::Series, PolarsError> {
    use polars::prelude::Series;
    let mut str_vals: Vec<String> = Vec::new();
    let mut int_vals: Vec<i64> = Vec::new();
    let mut float_vals: Vec<f64> = Vec::new();
    let mut has_string = false;
    let mut has_float = false;
    for e in list {
        match e {
            SqlExpr::Value(Value::SingleQuotedString(s)) => {
                str_vals.push(s.clone());
                has_string = true;
            }
            SqlExpr::Value(Value::Number(n, _)) => {
                str_vals.push(n.clone());
                if n.contains('.') {
                    let v: f64 = n.parse().map_err(|_| {
                        PolarsError::InvalidOperation(
                            format!("SQL: invalid number in IN list '{}'", n).into(),
                        )
                    })?;
                    float_vals.push(v);
                    has_float = true;
                } else {
                    let v: i64 = n.parse().map_err(|_| {
                        PolarsError::InvalidOperation(
                            format!("SQL: invalid integer in IN list '{}'", n).into(),
                        )
                    })?;
                    int_vals.push(v);
                }
            }
            SqlExpr::Value(Value::Boolean(b)) => {
                str_vals.push(b.to_string());
                has_string = true;
            }
            SqlExpr::Value(Value::Null) => {}
            _ => {
                return Err(PolarsError::InvalidOperation(
                    format!("SQL: IN list supports only literals, got {:?}", e).into(),
                ));
            }
        }
    }
    let series = if has_string {
        Series::from_iter(str_vals.iter().map(|s| s.as_str()))
    } else if !has_float && int_vals.len() == str_vals.len() {
        Series::from_iter(int_vals)
    } else if float_vals.len() == str_vals.len() {
        Series::from_iter(float_vals)
    } else {
        Series::from_iter(str_vals.iter().map(|s| s.as_str()))
    };
    Ok(series)
}

/// Projection item: either a plain Expr (built-in, Rust UDF, identifier) or Python UDF Column.
enum ProjItem {
    Expr(Expr, String),
    PythonUdf(Column, String),
}

fn apply_projection(
    df: &crate::dataframe::DataFrame,
    projection: &[SelectItem],
    session: &SparkSession,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    // Wildcard: expand to all columns
    for item in projection {
        if matches!(item, SelectItem::Wildcard(_)) {
            let column_names = df.columns()?;
            let all_col_names: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
            return df.select(all_col_names);
        }
    }

    let mut items = Vec::new();
    for item in projection {
        let proj = match item {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => {
                let name = ident.value.as_str();
                let resolved = df.resolve_column_name(name)?;
                ProjItem::Expr(col(resolved.as_str()), name.to_string())
            }
            SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts)) => {
                let name = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                let resolved = df.resolve_column_name(name)?;
                ProjItem::Expr(col(resolved.as_str()), name.to_string())
            }
            SelectItem::UnnamedExpr(SqlExpr::Function(func)) => {
                projection_function_to_item(func, session, Some(df))?
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let alias_str = alias.value.clone();
                match expr {
                    SqlExpr::Identifier(ident) => {
                        let name = ident.value.as_str();
                        let resolved = df.resolve_column_name(name)?;
                        ProjItem::Expr(col(resolved.as_str()), alias_str)
                    }
                    SqlExpr::CompoundIdentifier(parts) => {
                        let name = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                        let resolved = df.resolve_column_name(name)?;
                        ProjItem::Expr(col(resolved.as_str()), alias_str)
                    }
                    SqlExpr::Function(func) => {
                        let mut item = projection_function_to_item(func, session, Some(df))?;
                        // Override alias with AS alias
                        item = match item {
                            ProjItem::Expr(e, _) => ProjItem::Expr(e, alias_str),
                            ProjItem::PythonUdf(c, _) => ProjItem::PythonUdf(c, alias_str),
                        };
                        item
                    }
                    _ => {
                        return Err(PolarsError::InvalidOperation(
                            format!("SQL: unsupported expression with alias: {:?}", expr).into(),
                        ));
                    }
                }
            }
            _ => {
                return Err(PolarsError::InvalidOperation(
                    format!(
                        "SQL: SELECT supports column names, *, and function calls. Got {:?}",
                        item
                    )
                    .into(),
                ));
            }
        };
        items.push(proj);
    }

    if items.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "SQL: SELECT must list at least one column or *.".into(),
        ));
    }

    // Check if any Python UDF (requires with_column path)
    let has_python_udf = items.iter().any(|i| matches!(i, ProjItem::PythonUdf(_, _)));

    let mut df = df.clone();

    if has_python_udf {
        // Add Python UDF columns first, then select all in order
        for item in &items {
            if let ProjItem::PythonUdf(ref col, ref alias) = item {
                df = df.with_column(alias, col)?;
            }
        }
        let exprs: Vec<Expr> = items
            .iter()
            .map(|i| match i {
                ProjItem::Expr(e, alias) => e.clone().alias(alias),
                ProjItem::PythonUdf(_, alias) => col(alias.as_str()).alias(alias),
            })
            .collect();
        df.select_exprs(exprs)
    } else {
        // All exprs: use select_with_exprs
        let exprs: Vec<Expr> = items
            .iter()
            .map(|i| match i {
                ProjItem::Expr(e, alias) => e.clone().alias(alias),
                ProjItem::PythonUdf(_, _) => unreachable!(),
            })
            .collect();
        df.select_exprs(exprs)
    }
}

fn sql_function_alias(func: &Function) -> String {
    let func_name = func.name.0.last().map(|i| i.value.as_str()).unwrap_or("");
    let arg_parts: Vec<String> = func
        .args
        .iter()
        .filter_map(|a| {
            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Identifier(ident))) = a {
                Some(ident.value.to_string())
            } else if let FunctionArg::Unnamed(FunctionArgExpr::Expr(
                SqlExpr::CompoundIdentifier(parts),
            )) = a
            {
                parts.last().map(|i| i.value.to_string())
            } else {
                Some("_".to_string())
            }
        })
        .collect();
    if arg_parts.is_empty() {
        format!("{}()", func_name)
    } else {
        format!("{}({})", func_name, arg_parts.join(", "))
    }
}

fn projection_function_to_item(
    func: &Function,
    session: &SparkSession,
    df: Option<&DataFrame>,
) -> Result<ProjItem, PolarsError> {
    let func_name = func.name.0.last().map(|i| i.value.as_str()).unwrap_or("");
    let args = sql_function_args_to_columns(func, session, df)?;
    let case_sensitive = session.is_case_sensitive();
    let alias = sql_function_alias(func);

    // Built-ins
    if let Some(col) = args.first() {
        let builtin = match func_name.to_uppercase().as_str() {
            "UPPER" | "UCASE" if args.len() == 1 => {
                Some(functions::upper(col).expr().clone().alias(&alias))
            }
            "LOWER" | "LCASE" if args.len() == 1 => {
                Some(functions::lower(col).expr().clone().alias(&alias))
            }
            _ => None,
        };
        if let Some(e) = builtin {
            return Ok(ProjItem::Expr(e, alias));
        }
    }

    // UDF lookup
    if session.udf_registry.has_udf(func_name, case_sensitive) {
        let col = functions::call_udf(func_name, &args)?;
        if col.udf_call.is_some() {
            return Ok(ProjItem::PythonUdf(col, alias));
        }
        return Ok(ProjItem::Expr(col.expr().clone().alias(&alias), alias));
    }

    Err(PolarsError::InvalidOperation(
        format!(
            "SQL: unknown function '{}'. Register with spark.udf.register() or use built-ins: UPPER, LOWER.",
            func_name
        )
        .into(),
    ))
}

/// Push one aggregate expression from a SQL function. `alias_override`: when Some (e.g. AS cnt), use it; when None use default (count, sum(col), etc).
fn push_agg_function(
    name: &sqlparser::ast::ObjectName,
    args: &[sqlparser::ast::FunctionArg],
    df: &DataFrame,
    alias_override: Option<&str>,
    agg: &mut Vec<Expr>,
) -> Result<(), PolarsError> {
    use polars::prelude::len;

    let func_name = name.0.last().map(|i| i.value.as_str()).unwrap_or("");
    let (expr, default_alias) = match func_name.to_uppercase().as_str() {
        "COUNT" => {
            let e = if args.is_empty() {
                len()
            } else {
                match &args[0] {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Wildcard),
                    ) => len(),
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
                    ) => {
                        let resolved = df.resolve_column_name(ident.value.as_str())?;
                        col(resolved.as_str()).count()
                    }
                    _ => {
                        return Err(PolarsError::InvalidOperation(
                            "SQL: COUNT(*) or COUNT(column) only.".into(),
                        ));
                    }
                }
            };
            (e, "count".to_string())
        }
        "SUM" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
            )) = args.first()
            {
                let resolved = df.resolve_column_name(ident.value.as_str())?;
                (
                    col(resolved.as_str()).sum(),
                    format!("sum({})", ident.value),
                )
            } else {
                return Err(PolarsError::InvalidOperation(
                    "SQL: SUM(column) only.".into(),
                ));
            }
        }
        "AVG" | "MEAN" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
            )) = args.first()
            {
                let resolved = df.resolve_column_name(ident.value.as_str())?;
                (
                    col(resolved.as_str()).mean(),
                    format!("avg({})", ident.value),
                )
            } else {
                return Err(PolarsError::InvalidOperation(
                    "SQL: AVG(column) only.".into(),
                ));
            }
        }
        "MIN" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
            )) = args.first()
            {
                let resolved = df.resolve_column_name(ident.value.as_str())?;
                (
                    col(resolved.as_str()).min(),
                    format!("min({})", ident.value),
                )
            } else {
                return Err(PolarsError::InvalidOperation(
                    "SQL: MIN(column) only.".into(),
                ));
            }
        }
        "MAX" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
            )) = args.first()
            {
                let resolved = df.resolve_column_name(ident.value.as_str())?;
                (
                    col(resolved.as_str()).max(),
                    format!("max({})", ident.value),
                )
            } else {
                return Err(PolarsError::InvalidOperation(
                    "SQL: MAX(column) only.".into(),
                ));
            }
        }
        _ => {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "SQL: unsupported aggregate in SELECT: {}. Use COUNT, SUM, AVG, MIN, MAX.",
                    func_name
                )
                .into(),
            ));
        }
    };
    let name = alias_override.unwrap_or_else(|| default_alias.as_str());
    agg.push(expr.alias(name));
    Ok(())
}

/// True if the projection contains only aggregate function calls (COUNT, SUM, AVG, MIN, MAX).
/// Used for scalar aggregation: SELECT AVG(salary) FROM t (issue #587).
fn projection_is_scalar_aggregate(projection: &[SelectItem]) -> bool {
    use sqlparser::ast::SelectItem;
    if projection.is_empty() {
        return false;
    }
    for item in projection {
        let is_agg = match item {
            SelectItem::UnnamedExpr(SqlExpr::Function(f)) => is_agg_function_name(f),
            SelectItem::ExprWithAlias {
                expr: SqlExpr::Function(f),
                ..
            } => is_agg_function_name(f),
            _ => false,
        };
        if !is_agg {
            return false;
        }
    }
    true
}

fn is_agg_function_name(func: &Function) -> bool {
    let name = func.name.0.last().map(|i| i.value.as_str()).unwrap_or("");
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MEAN" | "MIN" | "MAX"
    )
}

/// Key for deduplicating aggregate function calls in HAVING (issue #589).
fn agg_function_key(func: &Function) -> Option<(String, String)> {
    let name = func.name.0.last().map(|i| i.value.as_str()).unwrap_or("");
    if !matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MEAN" | "MIN" | "MAX"
    ) {
        return None;
    }
    let arg_desc = match func.args.first() {
        None => "*".to_string(),
        Some(sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
            SqlExpr::Identifier(ident),
        ))) => ident.value.to_string(),
        Some(sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
            SqlExpr::Wildcard,
        ))) => "*".to_string(),
        _ => return None,
    };
    Some((name.to_uppercase(), arg_desc))
}

/// Collect unique aggregate function calls from a HAVING expression and assign __having_0, __having_1, ...
fn extract_having_agg_calls(expr: &SqlExpr) -> Vec<(Function, String)> {
    let mut seen: HashMap<(String, String), String> = HashMap::new();
    let mut list: Vec<(Function, String)> = Vec::new();
    fn walk(
        e: &SqlExpr,
        seen: &mut HashMap<(String, String), String>,
        list: &mut Vec<(Function, String)>,
    ) {
        match e {
            SqlExpr::Function(f) => {
                if let Some(key) = agg_function_key(f) {
                    if !seen.contains_key(&key) {
                        let alias = format!("__having_{}", list.len());
                        seen.insert(key.clone(), alias.clone());
                        list.push((f.clone(), alias));
                    }
                    return;
                }
            }
            _ => {}
        }
        match e {
            SqlExpr::BinaryOp { left, right, .. } => {
                walk(left.as_ref(), seen, list);
                walk(right.as_ref(), seen, list);
            }
            SqlExpr::UnaryOp { expr: inner, .. } => walk(inner.as_ref(), seen, list),
            SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => walk(inner.as_ref(), seen, list),
            SqlExpr::Function(f) => {
                for arg in &f.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(a)) = arg {
                        walk(a, seen, list);
                    }
                }
            }
            _ => {}
        }
    }
    walk(expr, &mut seen, &mut list);
    list
}

fn projection_to_agg_exprs(
    projection: &[SelectItem],
    group_cols: &[String],
    df: &DataFrame,
) -> Result<Vec<Expr>, PolarsError> {
    let mut agg = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => {
                let resolved = df.resolve_column_name(ident.value.as_str())?;
                if !group_cols.iter().any(|c| c == &resolved) {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "SQL: non-aggregated column '{}' must appear in GROUP BY.",
                            ident.value
                        )
                        .into(),
                    ));
                }
            }
            SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts)) => {
                let name = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                let resolved = df.resolve_column_name(name)?;
                if !group_cols.iter().any(|c| c == &resolved) {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "SQL: non-aggregated column '{}' must appear in GROUP BY.",
                            name
                        )
                        .into(),
                    ));
                }
            }
            SelectItem::UnnamedExpr(SqlExpr::Function(Function { name, args, .. })) => {
                push_agg_function(name, args, df, None, &mut agg)?;
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let alias_str = alias.value.as_str();
                match expr {
                    SqlExpr::Identifier(ident) => {
                        let resolved = df.resolve_column_name(ident.value.as_str())?;
                        if !group_cols.iter().any(|c| c == &resolved) {
                            return Err(PolarsError::InvalidOperation(
                                format!(
                                    "SQL: non-aggregated column '{}' must appear in GROUP BY.",
                                    ident.value
                                )
                                .into(),
                            ));
                        }
                        // Group column with alias (e.g. grp AS g): validation only; result keeps group column name from frame.
                    }
                    SqlExpr::CompoundIdentifier(parts) => {
                        let name = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                        let resolved = df.resolve_column_name(name)?;
                        if !group_cols.iter().any(|c| c == &resolved) {
                            return Err(PolarsError::InvalidOperation(
                                format!(
                                    "SQL: non-aggregated column '{}' must appear in GROUP BY.",
                                    name
                                )
                                .into(),
                            ));
                        }
                    }
                    SqlExpr::Function(Function { name, args, .. }) => {
                        push_agg_function(&name, args.as_slice(), df, Some(alias_str), &mut agg)?;
                    }
                    _ => {
                        return Err(PolarsError::InvalidOperation(
                            format!(
                                "SQL: unsupported aliased SELECT item in aggregation: {:?}",
                                expr
                            )
                            .into(),
                        ));
                    }
                }
            }
            SelectItem::Wildcard(_) => {
                return Err(PolarsError::InvalidOperation(
                    "SQL: SELECT * with GROUP BY is not supported; list columns and aggregates explicitly.".into(),
                ));
            }
            _ => {
                return Err(PolarsError::InvalidOperation(
                    format!("SQL: unsupported SELECT item in aggregation: {:?}", item).into(),
                ));
            }
        }
    }
    Ok(agg)
}

fn sql_limit_to_usize(expr: &SqlExpr) -> Result<usize, PolarsError> {
    match expr {
        SqlExpr::Value(Value::Number(s, _)) => {
            let n: i64 = s.parse().map_err(|_| {
                PolarsError::InvalidOperation(
                    format!("SQL: LIMIT must be a positive integer, got '{}'", s).into(),
                )
            })?;
            if n < 0 {
                return Err(PolarsError::InvalidOperation(
                    "SQL: LIMIT must be non-negative.".into(),
                ));
            }
            Ok(n as usize)
        }
        _ => Err(PolarsError::InvalidOperation(
            "SQL: LIMIT must be a literal integer.".into(),
        )),
    }
}
