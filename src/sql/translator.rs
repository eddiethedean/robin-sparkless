//! Translate sqlparser AST to DataFrame operations.

use crate::dataframe::{join, JoinType};
use crate::session::SparkSession;
use polars::prelude::{col, lit, Expr, PolarsError};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, GroupByExpr, JoinConstraint, JoinOperator, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, Value,
};

/// Translate a parsed Statement (must be Query) into a DataFrame using the session catalog.
pub fn translate(
    session: &SparkSession,
    stmt: &Statement,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let query = match stmt {
        Statement::Query(q) => q.as_ref(),
        _ => {
            return Err(PolarsError::InvalidOperation(
                "SQL: only SELECT queries are supported.".into(),
            ));
        }
    };
    translate_query(session, query)
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
        let expr = sql_expr_to_polars(selection)?;
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
    if has_group_by {
        let group_cols: Vec<String> = group_exprs
            .iter()
            .map(|e| sql_expr_to_col_name(e))
            .collect::<Result<Vec<_>, _>>()?;
        let group_refs: Vec<&str> = group_cols.iter().map(|s| s.as_str()).collect();
        let grouped = df.group_by(group_refs)?;
        let agg_exprs = projection_to_agg_exprs(&body.projection, &group_cols)?;
        if agg_exprs.is_empty() {
            df = grouped.count()?;
        } else {
            df = grouped.agg(agg_exprs)?;
        }
    } else {
        df = apply_projection(&df, &body.projection)?;
    }
    if let Some(having_expr) = &body.having {
        let having_polars = sql_expr_to_polars(having_expr)?;
        df = df.filter(having_polars)?;
    }
    if !query.order_by.is_empty() {
        let pairs: Vec<(String, bool)> = query
            .order_by
            .iter()
            .map(|o| {
                let col_name = sql_expr_to_col_name(&o.expr)?;
                let ascending = o.asc.unwrap_or(true);
                Ok((col_name, ascending))
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
            let table_name = name
                .0
                .last()
                .map(|i| i.value.as_str())
                .unwrap_or("");
            session.table(table_name)
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

fn sql_expr_to_polars(expr: &SqlExpr) -> Result<Expr, PolarsError> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(col(ident.value.as_str())),
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
            let l = sql_expr_to_polars(left)?;
            let r = sql_expr_to_polars(right)?;
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
        SqlExpr::IsNull(expr) => Ok(sql_expr_to_polars(expr)?.is_null()),
        SqlExpr::IsNotNull(expr) => Ok(sql_expr_to_polars(expr)?.is_not_null()),
        SqlExpr::UnaryOp { op, expr } => {
            let e = sql_expr_to_polars(expr)?;
            match op {
                sqlparser::ast::UnaryOperator::Not => Ok(e.not()),
                _ => Err(PolarsError::InvalidOperation(
                    format!("SQL: unsupported unary operator in WHERE: {:?}", op).into(),
                )),
            }
        }
        _ => Err(PolarsError::InvalidOperation(
            format!("SQL: unsupported expression in WHERE: {:?}. Use column, literal, =, <, >, AND, OR, IS NULL.", expr).into(),
        )),
    }
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

fn apply_projection(
    df: &crate::dataframe::DataFrame,
    projection: &[SelectItem],
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let mut cols = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => {
                cols.push(ident.value.as_str());
            }
            SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts)) => {
                let name = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                cols.push(name);
            }
            SelectItem::Wildcard(_) => {
                let column_names = df.columns()?;
                let all_col_names: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
                return df.select(all_col_names);
            }
            _ => {
                return Err(PolarsError::InvalidOperation(
                    format!(
                        "SQL: SELECT only supports column names or * for now, got {:?}",
                        item
                    )
                    .into(),
                ));
            }
        }
    }
    if cols.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "SQL: SELECT must list at least one column or *.".into(),
        ));
    }
    df.select(cols)
}

fn projection_to_agg_exprs(
    projection: &[SelectItem],
    group_cols: &[String],
) -> Result<Vec<Expr>, PolarsError> {
    use polars::prelude::len;
    use sqlparser::ast::Function;

    let mut agg = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => {
                if !group_cols.iter().any(|c| c == &ident.value) {
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
                if !group_cols.iter().any(|c| c == name) {
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
                let func_name = name.0.last().map(|i| i.value.as_str()).unwrap_or("");
                match func_name.to_uppercase().as_str() {
                    "COUNT" => {
                        let expr = if args.is_empty() {
                            len().alias("count")
                        } else {
                            match &args[0] {
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Wildcard),
                                ) => len().alias("count"),
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(
                                        ident,
                                    )),
                                ) => col(ident.value.as_str()).count().alias("count"),
                                _ => {
                                    return Err(PolarsError::InvalidOperation(
                                        "SQL: COUNT(*) or COUNT(column) only.".into(),
                                    ));
                                }
                            }
                        };
                        agg.push(expr);
                    }
                    "SUM" => {
                        if let Some(sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
                        )) = args.first()
                        {
                            agg.push(
                                col(ident.value.as_str())
                                    .sum()
                                    .alias(format!("sum({})", ident.value)),
                            );
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
                            agg.push(
                                col(ident.value.as_str())
                                    .mean()
                                    .alias(format!("avg({})", ident.value)),
                            );
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
                            agg.push(
                                col(ident.value.as_str())
                                    .min()
                                    .alias(format!("min({})", ident.value)),
                            );
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
                            agg.push(
                                col(ident.value.as_str())
                                    .max()
                                    .alias(format!("max({})", ident.value)),
                            );
                        } else {
                            return Err(PolarsError::InvalidOperation(
                                "SQL: MAX(column) only.".into(),
                            ));
                        }
                    }
                    _ => {
                        return Err(PolarsError::InvalidOperation(
                            format!("SQL: unsupported aggregate in SELECT: {}. Use COUNT, SUM, AVG, MIN, MAX.", func_name).into(),
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
