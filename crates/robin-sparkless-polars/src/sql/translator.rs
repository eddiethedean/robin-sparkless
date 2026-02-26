//! Translate sqlparser AST to DataFrame operations.
//! Resolves unknown functions as UDFs from the session registry.

use std::collections::HashMap;

use crate::column::Column;
use crate::dataframe::{DataFrame, JoinType, disambiguate_agg_output_names, join};
use crate::functions;
use crate::schema::{DataType as CoreDataType, StructType};
use crate::session::{SparkSession, set_thread_udf_session};
use polars::prelude::{DataFrame as PlDataFrame, Expr, PolarsError, col, lit, when};
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr as SqlExpr, FromTable, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, ObjectType,
    OrderByKind, Query, Select, SelectItem, SetExpr, SetOperator, Statement, TableFactor, Value,
    ValueWithSpan,
};

/// Parsed SQL number literal: integer or float.
enum SqlNumberVal {
    Int(i64),
    Float(f64),
}

fn parse_sql_number_val(s: &str, context: &str) -> Result<SqlNumberVal, PolarsError> {
    if s.contains('.') {
        let v: f64 = s.parse().map_err(|_| {
            PolarsError::InvalidOperation(format!("SQL: invalid number {} '{}'", context, s).into())
        })?;
        Ok(SqlNumberVal::Float(v))
    } else {
        let v: i64 = s.parse().map_err(|_| {
            PolarsError::InvalidOperation(
                format!("SQL: invalid integer {} '{}'", context, s).into(),
            )
        })?;
        Ok(SqlNumberVal::Int(v))
    }
}

fn parse_sql_number_expr(s: &str) -> Result<Expr, PolarsError> {
    match parse_sql_number_val(s, "literal")? {
        SqlNumberVal::Int(i) => Ok(lit(i)),
        SqlNumberVal::Float(f) => Ok(lit(f)),
    }
}

/// Return a slice of positional function arguments for List variant; empty otherwise.
fn function_args_slice(args: &FunctionArguments) -> &[FunctionArg] {
    match args {
        FunctionArguments::List(list) => &list.args,
        _ => &[],
    }
}

/// Parse a single SQL expression string and convert to Polars Expr using the given DataFrame for column resolution.
/// Used by selectExpr/expr() for PySpark parity. Parses "SELECT expr FROM __t" and returns the first select item's Expr.
pub fn expr_string_to_polars(
    expr_str: &str,
    session: &SparkSession,
    df: &DataFrame,
) -> Result<Expr, PolarsError> {
    let query = format!("SELECT {} FROM __selectexpr_t", expr_str);
    let stmt = spark_sql_parser::parse_sql(&query)
        .map_err(|e| PolarsError::InvalidOperation(e.to_string().into()))?;
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
        Statement::CreateTable(create_table) => {
            let table_name = create_table.name.to_string();
            if session.table_exists(&table_name) {
                if create_table.if_not_exists {
                    return Ok(DataFrame::from_polars_with_options(
                        PlDataFrame::empty(),
                        session.is_case_sensitive(),
                    ));
                }
                return Err(PolarsError::InvalidOperation(
                    format!("SQL: table already exists: {table_name}").into(),
                ));
            }

            // CREATE TABLE ... AS SELECT: run query and register result (Phase 7 / BUG-011).
            if let Some(ref q) = create_table.query {
                let df = translate_query(session, q)?;
                session.register_table(&table_name, df);
                return Ok(DataFrame::from_polars_with_options(
                    PlDataFrame::empty(),
                    session.is_case_sensitive(),
                ));
            }

            fn dtype_to_schema_str(dt: &sqlparser::ast::DataType) -> String {
                let s = dt.to_string().to_lowercase();
                if s.starts_with("int") || s.starts_with("integer") || s == "int4" {
                    "int".to_string()
                } else if s.starts_with("bigint") || s == "int8" || s == "long" {
                    "long".to_string()
                } else if s.starts_with("double") {
                    "double".to_string()
                } else if s.starts_with("float") {
                    "float".to_string()
                } else if s.starts_with("bool") {
                    "boolean".to_string()
                } else if s.starts_with("date") {
                    "date".to_string()
                } else if s.starts_with("timestamp") {
                    "timestamp".to_string()
                } else {
                    // STRING, VARCHAR, CHAR, etc.
                    "string".to_string()
                }
            }

            let schema: Vec<(String, String)> = create_table
                .columns
                .iter()
                .map(|c| (c.name.value.clone(), dtype_to_schema_str(&c.data_type)))
                .collect();
            let df = session.create_dataframe_from_rows(
                Vec::<Vec<JsonValue>>::new(),
                schema,
                false,
                false,
            )?;
            session.register_table(&table_name, df);
            Ok(DataFrame::from_polars_with_options(
                PlDataFrame::empty(),
                session.is_case_sensitive(),
            ))
        }
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
        Statement::Drop { names, .. } => {
            // Any other DROP (e.g. MaterializedView): treat as table/view drop for parity (#745).
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
        Statement::Update(u) => translate_update(session, u),
        Statement::Delete(d) => translate_delete(session, d),
        Statement::ExplainTable { table_name, .. } => translate_describe_table(session, table_name),
        _ => Err(PolarsError::InvalidOperation(
            "SQL: only SELECT, CREATE SCHEMA/DATABASE, DROP TABLE/VIEW/SCHEMA, and DESCRIBE are supported."
                .into(),
        )),
    }
}

/// Convert robin_sparkless_core DataType to schema type string (DESCRIBE / PySpark simpleString).
fn core_data_type_to_str(dt: &CoreDataType) -> String {
    match dt {
        CoreDataType::String => "string".to_string(),
        CoreDataType::Integer => "int".to_string(),
        CoreDataType::Long => "long".to_string(),
        CoreDataType::Double => "double".to_string(),
        CoreDataType::Boolean => "boolean".to_string(),
        CoreDataType::Date => "date".to_string(),
        CoreDataType::Timestamp => "timestamp".to_string(),
        CoreDataType::Binary => "binary".to_string(),
        CoreDataType::Array(inner) => format!("array<{}>", core_data_type_to_str(inner)),
        CoreDataType::Map(k, v) => {
            format!(
                "map<{},{}>",
                core_data_type_to_str(k),
                core_data_type_to_str(v)
            )
        }
        CoreDataType::Struct(fields) => {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| format!("{}:{}", f.name, core_data_type_to_str(&f.data_type)))
                .collect();
            format!("struct<{}>", parts.join(","))
        }
    }
}

/// DESCRIBE table_name: return a DataFrame with col_name, data_type (PySpark DESCRIBE parity).
fn translate_describe_table(
    session: &SparkSession,
    table_name: &sqlparser::ast::ObjectName,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let name = table_name_from_object_name(table_name);
    let df = session.table(&name)?;
    let schema: StructType = df.schema()?;
    let rows: Vec<Vec<JsonValue>> = schema
        .fields()
        .iter()
        .map(|f| {
            vec![
                JsonValue::String(f.name.clone()),
                JsonValue::String(core_data_type_to_str(&f.data_type)),
            ]
        })
        .collect();
    let out_schema = vec![
        ("col_name".to_string(), "string".to_string()),
        ("data_type".to_string(), "string".to_string()),
    ];
    session.create_dataframe_from_rows(rows, out_schema, false, false)
}

/// Translate a SetExpr (SELECT, Query, or SetOperation) to a DataFrame.
/// PR-B/#774: UNION [ALL] supported; EXCEPT/INTERSECT return a clear error.
fn translate_set_expr(
    session: &SparkSession,
    body: &SetExpr,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    match body {
        SetExpr::Select(select) => translate_select_body(session, select.as_ref()),
        SetExpr::Query(q) => translate_query(session, q.as_ref()),
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } => {
            match op {
                SetOperator::Union => {}
                SetOperator::Except | SetOperator::Intersect | SetOperator::Minus => {
                    return Err(PolarsError::InvalidOperation(
                        "SQL: EXCEPT, INTERSECT, and MINUS are not yet supported. Use UNION or UNION ALL."
                            .into(),
                    ));
                }
            }
            let left_df = translate_set_expr(session, left.as_ref())?;
            let right_df = translate_set_expr(session, right.as_ref())?;
            let mut df = left_df.union(&right_df)?;
            // DISTINCT (default for UNION) => drop duplicates; ALL => keep all rows.
            let is_distinct = matches!(set_quantifier, sqlparser::ast::SetQuantifier::Distinct);
            if is_distinct {
                df = df.distinct(None)?;
            }
            Ok(df)
        }
        _ => Err(PolarsError::InvalidOperation(
            "SQL: only SELECT and UNION are supported (no VALUES, INSERT, etc.).".into(),
        )),
    }
}

/// Translate UPDATE table SET col = expr [WHERE condition]. Modifies the table in the session catalog.
fn translate_update(
    session: &SparkSession,
    update: &sqlparser::ast::Update,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    if !update.table.joins.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "SQL: UPDATE with JOIN is not supported. Use a single table.".into(),
        ));
    }
    let table_name = table_name_from_factor(&update.table.relation)?;
    let mut df = session.table(&table_name)?;

    let where_expr = match &update.selection {
        Some(sel) => sql_expr_to_polars(sel, session, Some(&df), None)?,
        None => lit(true),
    };

    for assign in &update.assignments {
        let (col_name, value_expr) = match &assign.target {
            AssignmentTarget::ColumnName(name) => {
                let cn = table_name_from_object_name(name);
                let value = sql_expr_to_polars(&assign.value, session, Some(&df), None)?;
                (cn, value)
            }
            AssignmentTarget::Tuple(_) => {
                return Err(PolarsError::InvalidOperation(
                    "SQL: UPDATE with tuple assignment is not supported.".into(),
                ));
            }
        };
        let resolved = df.resolve_column_name(&col_name)?;
        let new_expr = when(where_expr.clone())
            .then(value_expr)
            .otherwise(col(resolved.as_str()));
        df = df.with_column_expr(&resolved, new_expr)?;
    }

    session.create_or_replace_temp_view(&table_name, df.clone());
    session.register_table(&table_name, df);
    Ok(DataFrame::from_polars_with_options(
        PlDataFrame::empty(),
        session.is_case_sensitive(),
    ))
}

/// Translate DELETE FROM table [WHERE condition]. Removes matching rows from the table in the session catalog.
fn translate_delete(
    session: &SparkSession,
    delete: &sqlparser::ast::Delete,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let first = tables.first().ok_or_else(|| {
        PolarsError::InvalidOperation("SQL: DELETE FROM requires a table.".into())
    })?;
    if !first.joins.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "SQL: DELETE with JOIN is not supported. Use a single table.".into(),
        ));
    }
    let table_name = table_name_from_factor(&first.relation)?;
    let df = session.table(&table_name)?;

    let keep_condition = match &delete.selection {
        Some(sel) => {
            let pred = sql_expr_to_polars(sel, session, Some(&df), None)?;
            pred.not()
        }
        None => lit(false),
    };
    let new_df = df.filter(keep_condition)?;

    session.create_or_replace_temp_view(&table_name, new_df.clone());
    session.register_table(&table_name, new_df);
    Ok(DataFrame::from_polars_with_options(
        PlDataFrame::empty(),
        session.is_case_sensitive(),
    ))
}

/// Translate a single Select (FROM, WHERE, GROUP BY, projection, HAVING) to a DataFrame.
fn translate_select_body(
    session: &SparkSession,
    body: &Select,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let mut df = translate_select_from(session, body)?;
    if let Some(selection) = &body.selection {
        let expr = sql_expr_to_polars(selection, session, Some(&df), None)?;
        df = df.filter(expr)?;
    }
    let group_exprs: &[SqlExpr] = match &body.group_by {
        GroupByExpr::Expressions(exprs, _) => exprs.as_slice(),
        GroupByExpr::All(_) => {
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
                    function_args_slice(&func.args),
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
        let agg_exprs = disambiguate_agg_output_names(agg_exprs);
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
    Ok(df)
}

fn translate_query(
    session: &SparkSession,
    query: &Query,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let mut df = translate_set_expr(session, query.body.as_ref())?;
    if let Some(order_by) = &query.order_by {
        if let OrderByKind::Expressions(exprs) = &order_by.kind {
            if !exprs.is_empty() {
                let pairs: Vec<(String, bool)> = exprs
                    .iter()
                    .map(|o| {
                        let col_name = sql_expr_to_col_name(&o.expr)?;
                        let resolved = df.resolve_column_name(&col_name)?;
                        let ascending = o.options.asc.unwrap_or(true);
                        Ok((resolved, ascending))
                    })
                    .collect::<Result<Vec<_>, PolarsError>>()?;
                let (cols, asc): (Vec<String>, Vec<bool>) = pairs.into_iter().unzip();
                let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
                df = df.order_by(col_refs, asc)?;
            }
        }
    }
    let limit_expr = query.fetch.as_ref().and_then(|f| f.quantity.as_ref());
    if let Some(limit_expr) = limit_expr {
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
        match &join_spec.join_operator {
            JoinOperator::CrossJoin(_) => {
                df = df.cross_join(&right_df).map_err(|e| {
                    PolarsError::InvalidOperation(format!("SQL: CROSS JOIN failed: {e}").into())
                })?;
            }
            _ => {
                let join_type = match &join_spec.join_operator {
                    JoinOperator::Join(_) | JoinOperator::Inner(_) => JoinType::Inner,
                    JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => JoinType::Left,
                    JoinOperator::Right(_) | JoinOperator::RightOuter(_) => JoinType::Right,
                    JoinOperator::FullOuter(_) => JoinType::Outer,
                    JoinOperator::LeftSemi(_) => JoinType::LeftSemi,
                    JoinOperator::LeftAnti(_) => JoinType::LeftAnti,
                    JoinOperator::Semi(_) => JoinType::LeftSemi,
                    JoinOperator::Anti(_) => JoinType::LeftAnti,
                    _ => {
                        return Err(PolarsError::InvalidOperation(
                            "SQL: only INNER, LEFT, RIGHT, FULL, LEFT SEMI, LEFT ANTI, CROSS JOIN are supported.".into(),
                        ));
                    }
                };
                let (left_on, right_on) = join_condition_to_on_columns(&join_spec.join_operator)?;
                let left_refs: Vec<&str> = left_on.iter().map(|s| s.as_str()).collect();
                let right_refs: Vec<&str> = right_on.iter().map(|s| s.as_str()).collect();
                df = join(
                    &df,
                    &right_df,
                    left_refs,
                    right_refs,
                    join_type,
                    session.is_case_sensitive(),
                )?;
            }
        }
    }
    Ok(df)
}

fn table_name_from_object_name(name: &sqlparser::ast::ObjectName) -> String {
    if name.0.len() >= 2 {
        let parts: Vec<String> = name
            .0
            .iter()
            .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
            .collect();
        parts.join(".")
    } else {
        name.0
            .last()
            .and_then(|p| p.as_ident())
            .map(|i| i.value.clone())
            .unwrap_or_default()
    }
}

fn table_name_from_factor(factor: &TableFactor) -> Result<String, PolarsError> {
    match factor {
        TableFactor::Table { name, .. } => Ok(table_name_from_object_name(name)),
        _ => Err(PolarsError::InvalidOperation(
            "SQL: only plain table names are supported for UPDATE/DELETE.".into(),
        )),
    }
}

fn resolve_table_factor(
    session: &SparkSession,
    factor: &TableFactor,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    match factor {
        TableFactor::Table { name, .. } => {
            let table_name = table_name_from_object_name(name);
            session.table(&table_name)
        }
        _ => Err(PolarsError::InvalidOperation(
            "SQL: only plain table names are supported in FROM (no subqueries, derived tables). Register with create_or_replace_temp_view.".into(),
        )),
    }
}

/// Returns (left_column_names, right_column_names) for JOIN ON. Same length; may differ (e.g. a.id = b.other_id) (#743).
fn join_condition_to_on_columns(
    join_op: &JoinOperator,
) -> Result<(Vec<String>, Vec<String>), PolarsError> {
    let constraint = match join_op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::Semi(c)
        | JoinOperator::Anti(c) => c,
        JoinOperator::CrossJoin(_) => return Ok((vec![], vec![])),
        _ => {
            return Err(PolarsError::InvalidOperation(
                "SQL: only INNER/LEFT/RIGHT/FULL/LEFT SEMI/LEFT ANTI/CROSS JOIN with ON are supported.".into(),
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
                Ok((vec![l], vec![r]))
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
            let name = parts.last().map(|i| i.value.as_str()).unwrap_or("");
            let resolved = df
                .map(|d| d.resolve_column_name(name))
                .transpose()?
                .unwrap_or_else(|| name.to_string());
            Ok(col(resolved.as_str()))
        }
        SqlExpr::Value(ValueWithSpan { value: Value::Number(s, _), .. }) => {
            parse_sql_number_expr(s)
        }
        SqlExpr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => Ok(lit(s.as_str())),
        SqlExpr::Value(ValueWithSpan { value: Value::Boolean(b), .. }) => Ok(lit(*b)),
        SqlExpr::Value(ValueWithSpan { value: Value::Null, .. }) => Ok(lit(polars::prelude::NULL)),
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
        SqlExpr::Nested(inner) => sql_expr_to_polars(inner, session, df, having_agg_map),
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
            any: _,
        } => {
            let col_expr = sql_expr_to_polars(left.as_ref(), session, df, having_agg_map)?;
            let pattern_str = sql_expr_to_string_literal(pattern.as_ref())?;
            let col_col = crate::column::Column::from_expr(col_expr, None);
            let escape: Option<char> = escape_char.as_ref().and_then(|v| match v {
                Value::SingleQuotedString(s) => s.chars().next(),
                _ => None,
            });
            let like_expr = col_col.like(&pattern_str, escape).into_expr();
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
            let in_expr = col_expr.is_in(lit(series), false);
            Ok(if *negated {
                in_expr.not()
            } else {
                in_expr
            })
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            // Simple CASE: CASE x WHEN a THEN b -> when(x.eq(a)).then(b). Searched CASE: CASE WHEN p THEN b -> when(p).then(b).
            // Build from the end so we stay in Expr type: else_e = else; for (cond, res) in reverse, else_e = when(cond).then(res).otherwise(else_e).
            let else_e = match else_result {
                Some(e) => sql_expr_to_polars(e.as_ref(), session, df, having_agg_map)?,
                None => lit(polars::prelude::NULL),
            };
            let mut expr = else_e;
            for cw in conditions.iter().rev() {
                let cond = if let Some(op) = &operand {
                    sql_expr_to_polars(op.as_ref(), session, df, having_agg_map)?
                        .eq(sql_expr_to_polars(&cw.condition, session, df, having_agg_map)?)
                } else {
                    sql_expr_to_polars(&cw.condition, session, df, having_agg_map)?
                };
                let res = sql_expr_to_polars(&cw.result, session, df, having_agg_map)?;
                expr = when(cond).then(res).otherwise(expr);
            }
            Ok(expr)
        }
        SqlExpr::InSubquery { .. } => Err(PolarsError::InvalidOperation(
            "SQL: subquery in WHERE (e.g. col IN (SELECT ...)) is not yet supported.".into(),
        )),
        SqlExpr::Exists { .. } => Err(PolarsError::InvalidOperation(
            "SQL: subquery in WHERE (EXISTS (SELECT ...)) is not yet supported.".into(),
        )),
        SqlExpr::Subquery(_) => Err(PolarsError::InvalidOperation(
            "SQL: subquery in WHERE is not yet supported.".into(),
        )),
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
    let func_name = func
        .name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .unwrap_or("");
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
    if session.udf_registry.has_udf(func_name, case_sensitive)? {
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
    for arg in function_args_slice(&func.args) {
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
        SqlExpr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) => Ok(s.clone()),
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
            SqlExpr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(s),
                ..
            }) => {
                str_vals.push(s.clone());
                has_string = true;
            }
            SqlExpr::Value(ValueWithSpan {
                value: Value::Number(n, _),
                ..
            }) => {
                str_vals.push(n.clone());
                match parse_sql_number_val(n, "IN list")? {
                    SqlNumberVal::Int(v) => int_vals.push(v),
                    SqlNumberVal::Float(v) => {
                        float_vals.push(v);
                        has_float = true;
                    }
                }
            }
            SqlExpr::Value(ValueWithSpan {
                value: Value::Boolean(b),
                ..
            }) => {
                str_vals.push(b.to_string());
                has_string = true;
            }
            SqlExpr::Value(ValueWithSpan {
                value: Value::Null, ..
            }) => {}
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

impl ProjItem {
    fn alias(&self) -> &str {
        match self {
            ProjItem::Expr(_, a) => a.as_str(),
            ProjItem::PythonUdf(_, a) => a.as_str(),
        }
    }
}

/// Disambiguate duplicate output aliases (e.g. multiple aggs named "count" -> count, count_1, count_2).
fn disambiguate_aliases(aliases: &[String]) -> Vec<String> {
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut out = Vec::with_capacity(aliases.len());
    for alias in aliases {
        let count = seen.entry(alias.clone()).or_insert(0);
        let name = if *count == 0 {
            alias.clone()
        } else {
            format!("{}_{}", alias, count)
        };
        *count += 1;
        out.push(name);
    }
    out
}

fn apply_projection(
    df: &crate::dataframe::DataFrame,
    projection: &[SelectItem],
    session: &SparkSession,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    // Wildcard: expand to all columns (disambiguate if JOIN produced duplicate names)
    for item in projection {
        if matches!(item, SelectItem::Wildcard(_)) {
            let column_names: Vec<String> = df.columns()?.into_iter().collect();
            let unique_aliases = disambiguate_aliases(&column_names);
            if unique_aliases == column_names {
                let refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
                return df.select(refs);
            }
            let exprs: Vec<Expr> = column_names
                .iter()
                .zip(unique_aliases.iter())
                .map(|(c, alias)| col(c.as_str()).alias(alias))
                .collect();
            return df.select_exprs(exprs);
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
                    SqlExpr::Case { .. } => {
                        // CASE ... END AS alias (#776)
                        let e = sql_expr_to_polars(expr, session, Some(df), None)?;
                        ProjItem::Expr(e, alias_str)
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

    let aliases: Vec<String> = items.iter().map(|i| i.alias().to_string()).collect();
    let final_aliases = disambiguate_aliases(&aliases);

    // Check if any Python UDF (requires with_column path)
    let has_python_udf = items.iter().any(|i| matches!(i, ProjItem::PythonUdf(_, _)));

    let mut df = df.clone();

    if has_python_udf {
        // Add Python UDF columns first (with disambiguated names), then select all in order
        for (i, item) in items.iter().enumerate() {
            if let ProjItem::PythonUdf(col, _) = item {
                df = df.with_column(&final_aliases[i], col)?;
            }
        }
        let exprs: Vec<Expr> = items
            .iter()
            .enumerate()
            .map(|(i, item)| match item {
                ProjItem::Expr(e, _) => e.clone().alias(&final_aliases[i]),
                ProjItem::PythonUdf(_, _) => {
                    col(final_aliases[i].as_str()).alias(&final_aliases[i])
                }
            })
            .collect();
        df.select_exprs(exprs)
    } else {
        // All exprs: use select_with_exprs (disambiguated aliases avoid duplicate column names)
        let exprs: Vec<Expr> = items
            .iter()
            .enumerate()
            .map(|(i, item)| match item {
                ProjItem::Expr(e, _) => e.clone().alias(&final_aliases[i]),
                ProjItem::PythonUdf(_, _) => unreachable!(),
            })
            .collect();
        df.select_exprs(exprs)
    }
}

fn sql_function_alias(func: &Function) -> String {
    let func_name = func
        .name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .unwrap_or("");
    let arg_parts: Vec<String> = function_args_slice(&func.args)
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
    let func_name = func
        .name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .unwrap_or("");
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
    if session.udf_registry.has_udf(func_name, case_sensitive)? {
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

    let func_name = name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .unwrap_or("");
    let (expr, default_alias) = match func_name.to_uppercase().as_str() {
        "COUNT" => {
            let e = if args.is_empty() {
                len()
            } else if args.len() == 1 {
                use sqlparser::ast::FunctionArgExpr;
                match &args[0] {
                    sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => len(),
                    sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                        let expr = match e {
                            SqlExpr::Nested(inner) => inner.as_ref(),
                            other => other,
                        };
                        match expr {
                            SqlExpr::Wildcard(_) => len(),
                            SqlExpr::Identifier(ident) => {
                                let resolved = df.resolve_column_name(ident.value.as_str())?;
                                col(resolved.as_str()).count()
                            }
                            _ => len(), // COUNT(1) etc.
                        }
                    }
                    _ => {
                        return Err(PolarsError::InvalidOperation(
                            "SQL: COUNT(*) or COUNT(column) only.".into(),
                        ));
                    }
                }
            } else {
                return Err(PolarsError::InvalidOperation(
                    "SQL: COUNT takes at most one argument.".into(),
                ));
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
    let name = alias_override.unwrap_or(default_alias.as_str());
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
    let name = func
        .name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .unwrap_or("");
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MEAN" | "MIN" | "MAX"
    )
}

/// Key for deduplicating aggregate function calls in HAVING (issue #589).
fn agg_function_key(func: &Function) -> Option<(String, String)> {
    let name = func
        .name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .unwrap_or("");
    if !matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MEAN" | "MIN" | "MAX"
    ) {
        return None;
    }
    let arg_desc = match function_args_slice(&func.args).first() {
        None => "*".to_string(),
        Some(sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
            SqlExpr::Identifier(ident),
        ))) => ident.value.to_string(),
        Some(sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
            SqlExpr::Wildcard(_),
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
        if let SqlExpr::Function(f) = e {
            if let Some(key) = agg_function_key(f) {
                if !seen.contains_key(&key) {
                    let alias = format!("__having_{}", list.len());
                    seen.insert(key.clone(), alias.clone());
                    list.push((f.clone(), alias));
                }
                return;
            }
        }
        match e {
            SqlExpr::BinaryOp { left, right, .. } => {
                walk(left.as_ref(), seen, list);
                walk(right.as_ref(), seen, list);
            }
            SqlExpr::UnaryOp { expr: inner, .. } => walk(inner.as_ref(), seen, list),
            SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => walk(inner.as_ref(), seen, list),
            SqlExpr::Function(f) => {
                for arg in function_args_slice(&f.args) {
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
                push_agg_function(name, function_args_slice(args), df, None, &mut agg)?;
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
                        push_agg_function(
                            name,
                            function_args_slice(args),
                            df,
                            Some(alias_str),
                            &mut agg,
                        )?;
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
        SqlExpr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        }) => {
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
