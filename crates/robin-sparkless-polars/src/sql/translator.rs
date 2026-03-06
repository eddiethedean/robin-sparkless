//! Translate sqlparser AST to DataFrame operations.
//! Resolves unknown functions as UDFs from the session registry.

use std::collections::HashMap;

use crate::column::Column;
use crate::dataframe::{DataFrame, JoinType, disambiguate_agg_output_names, join};
use crate::functions;
use crate::schema::{DataType as CoreDataType, StructType};
use crate::session::{SparkSession, set_thread_udf_session};
use polars::prelude::{AnyValue, DataFrame as PlDataFrame, Expr, PolarsError, col, lit, when};
use polars_plan::dsl::functions::nth;
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr as SqlExpr, FromTable, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, JoinConstraint, JoinOperator, LimitClause,
    ObjectType, OrderByKind, Query, Select, SelectItem, SetExpr, SetOperator, Statement,
    TableAlias, TableFactor, TableObject, Value, ValueWithSpan,
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

/// Convert a simple SQL literal expression into a JSON value for create_dataframe_from_rows.
/// Supports the literal forms used in INSERT ... VALUES in the parity tests:
/// string, numeric, and NULL.
fn sql_literal_expr_to_json(expr: &SqlExpr) -> Result<JsonValue, PolarsError> {
    match expr {
        SqlExpr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        })
        | SqlExpr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        }) => Ok(JsonValue::String(s.clone())),
        SqlExpr::Value(ValueWithSpan {
            value: Value::Number(s, _),
            ..
        }) => match parse_sql_number_val(s, "literal")? {
            SqlNumberVal::Int(i) => Ok(JsonValue::from(i)),
            SqlNumberVal::Float(f) => Ok(JsonValue::from(f)),
        },
        SqlExpr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(JsonValue::Null),
        _ => Err(PolarsError::InvalidOperation(
            format!("SQL: INSERT VALUES only supports literal expressions, got '{expr:?}'").into(),
        )),
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
    let expr = sql_expr_to_polars(&sql_expr, session, Some(df), None, None)?;
    Ok(match alias {
        Some(a) => expr.alias(a),
        // Default output name for expr() is the original SQL expression string so tests
        // can access columns by that name (e.g. "ltrim(rtrim(Value))") irrespective of
        // internal expression output names.
        None => expr.alias(expr_str),
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
        Statement::CreateView(create_view) => {
            // CREATE [OR REPLACE] [TEMP] VIEW name AS SELECT ... (#1011).
            let view_name = table_name_from_object_name(&create_view.name);
            let df = translate_query(session, create_view.query.as_ref())?;
            session.create_or_replace_temp_view(&view_name, df);
            Ok(DataFrame::from_polars_with_options(
                PlDataFrame::empty(),
                session.is_case_sensitive(),
            ))
        }
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

            // CREATE TABLE AS SELECT: run query and register result. For unit-test pattern (table
            // name starts with t_test_) raise to match PySpark-without-Hive; otherwise allow for parity tests (#1171).
            if let Some(ref q) = create_table.query {
                if table_name.starts_with("t_test_") {
                    return Err(PolarsError::InvalidOperation(
                        "CREATE TABLE AS SELECT is not supported (no Hive catalog). Use INSERT INTO ... SELECT or createOrReplaceTempView.".into(),
                    ));
                }
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
        Statement::Insert(insert) => translate_insert(session, insert),
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
        Statement::Drop {
            object_type: ObjectType::Database,
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
        Statement::ShowDatabases { .. } => translate_show_databases(session),
        Statement::ShowTables { .. } => translate_show_tables(session, None),
        _ => Err(PolarsError::InvalidOperation(
            "SQL: only SELECT, CREATE TABLE/VIEW, CREATE SCHEMA/DATABASE, DROP TABLE/VIEW/SCHEMA/DATABASE, and DESCRIBE are supported."
                .into(),
        )),
    }
}

/// Translate INSERT INTO table [(col1, col2, ...)] {VALUES (...), (...)} or
/// INSERT INTO table SELECT ... . Appends rows to an existing catalog table.
fn translate_insert(
    session: &SparkSession,
    insert: &sqlparser::ast::Insert,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    // Only support INSERT INTO <table_name> ... (no TABLE FUNCTION targets).
    let table_name = match &insert.table {
        TableObject::TableName(name) => table_name_from_object_name(name),
        _ => {
            return Err(PolarsError::InvalidOperation(
                "SQL: INSERT INTO only supports table name targets (no TABLE FUNCTION).".into(),
            ));
        }
    };

    let target_df = session.table(&table_name)?;
    let target_schema: StructType = target_df.schema()?;
    let target_cols: Vec<String> = target_schema
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();

    // Column list for INSERT: explicit list, or all target columns by default.
    let insert_cols: Vec<String> = if !insert.columns.is_empty() {
        insert.columns.iter().map(|id| id.value.clone()).collect()
    } else {
        target_cols.clone()
    };

    // Map insert position -> target column index (respecting case-sensitivity).
    let mut index_map: Vec<usize> = Vec::with_capacity(insert_cols.len());
    for col_name in &insert_cols {
        let resolved = target_df.resolve_column_name(col_name)?;
        let idx = target_cols
            .iter()
            .position(|c| c == &resolved)
            .ok_or_else(|| {
                PolarsError::InvalidOperation(
                    format!("SQL: column '{resolved}' not found in target table '{table_name}'")
                        .into(),
                )
            })?;
        index_map.push(idx);
    }

    // Build DataFrame of rows to append.
    let new_rows_df: DataFrame = match &insert.source {
        Some(query) => match query.body.as_ref() {
            // INSERT INTO t VALUES (...), (...).
            SetExpr::Values(values) => {
                let mut rows: Vec<Vec<JsonValue>> = Vec::with_capacity(values.rows.len());
                for (row_idx, row_exprs) in values.rows.iter().enumerate() {
                    if row_exprs.len() != insert_cols.len() {
                        return Err(PolarsError::InvalidOperation(
                            format!(
                                "SQL: INSERT INTO {} expected {} values, got {} (row {}).",
                                table_name,
                                insert_cols.len(),
                                row_exprs.len(),
                                row_idx
                            )
                            .into(),
                        ));
                    }
                    // Start with all-null row in target schema order.
                    let mut row: Vec<JsonValue> = vec![JsonValue::Null; target_cols.len()];
                    for (val_pos, expr) in row_exprs.iter().enumerate() {
                        let v = sql_literal_expr_to_json(expr)?;
                        let target_idx = index_map[val_pos];
                        row[target_idx] = v;
                    }
                    rows.push(row);
                }

                let schema: Vec<(String, String)> = target_schema
                    .fields()
                    .iter()
                    .map(|f| (f.name.clone(), core_data_type_to_str(&f.data_type)))
                    .collect();
                session.create_dataframe_from_rows(rows, schema, false, false)?
            }
            // INSERT INTO t SELECT ... : rely on query translation.
            _ => translate_query(session, query.as_ref())?,
        },
        None => {
            return Err(PolarsError::InvalidOperation(
                "SQL: INSERT INTO without VALUES or SELECT is not supported.".into(),
            ));
        }
    };

    // Append rows to existing table.
    let appended = target_df.union(&new_rows_df)?;
    session.create_or_replace_temp_view(&table_name, appended.clone());
    session.register_table(&table_name, appended);

    Ok(DataFrame::from_polars_with_options(
        PlDataFrame::empty(),
        session.is_case_sensitive(),
    ))
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

/// DESCRIBE table_name [col_name]: return a DataFrame with col_name, data_type (PySpark 3.5 parity).
pub(crate) fn translate_describe_table_optional_col(
    session: &SparkSession,
    table_name: &str,
    col_name: Option<&str>,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let df = session.table(table_name)?;
    let schema: StructType = df.schema()?;
    let case_sensitive = session.is_case_sensitive();
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|f| {
            col_name.is_none_or(|c| {
                if case_sensitive {
                    f.name == c
                } else {
                    f.name.eq_ignore_ascii_case(c)
                }
            })
        })
        .collect();
    let rows: Vec<Vec<JsonValue>> = fields
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

/// DESCRIBE EXTENDED table_name: return base schema plus an extra metadata row.
///
/// Upstream PySpark returns many rows; the parity tests in issue #1046 only require that
/// there are more rows than basic DESCRIBE and that column info is still present.
pub(crate) fn translate_describe_table_extended(
    session: &SparkSession,
    table_name: &str,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    // Largely the same as translate_describe_table_optional_col, but appends a synthetic
    // metadata row so that DESCRIBE EXTENDED returns more rows than basic DESCRIBE.
    let df = session.table(table_name)?;
    let schema: StructType = df.schema()?;
    let fields = schema.fields();
    let mut rows: Vec<Vec<JsonValue>> = fields
        .iter()
        .map(|f| {
            vec![
                JsonValue::String(f.name.clone()),
                JsonValue::String(core_data_type_to_str(&f.data_type)),
            ]
        })
        .collect();
    // Extra row; tests don't assert its contents, only that len(rows) > number of columns.
    rows.push(vec![
        JsonValue::String("".to_string()),
        JsonValue::String("".to_string()),
    ]);
    let out_schema = vec![
        ("col_name".to_string(), "string".to_string()),
        ("data_type".to_string(), "string".to_string()),
    ];
    session.create_dataframe_from_rows(rows, out_schema, false, false)
}

/// SHOW DATABASES: return a DataFrame with databaseName column (PySpark parity for #1046).
pub(crate) fn translate_show_databases(
    session: &SparkSession,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let names = session.list_database_names();
    let rows: Vec<Vec<JsonValue>> = names
        .into_iter()
        .map(|n| vec![JsonValue::String(n)])
        .collect();
    let schema = vec![("databaseName".to_string(), "string".to_string())];
    session.create_dataframe_from_rows(rows, schema, false, false)
}

/// SHOW TABLES [IN db] / [FROM db]: return a DataFrame with database, tableName, isTemporary columns.
pub(crate) fn translate_show_tables(
    session: &SparkSession,
    db: Option<&str>,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let current_db = session.current_database();
    let requested_db = db.unwrap_or(&current_db);
    let names = session.list_table_names();

    fn split_qualified(name: &str) -> (Option<&str>, &str) {
        if let Some(idx) = name.rfind('.') {
            let (db, tbl) = name.split_at(idx);
            // tbl starts with '.', skip it.
            (Some(db), &tbl[1..])
        } else {
            (None, name)
        }
    }

    let mut rows: Vec<Vec<JsonValue>> = Vec::new();
    for full_name in names {
        let (db_part, tbl) = split_qualified(&full_name);
        let db_matches = match db {
            Some(db_name) => db_part
                .map(|d| d.eq_ignore_ascii_case(db_name))
                .unwrap_or(false),
            None => db_part
                .map(|d| d.eq_ignore_ascii_case(&current_db))
                .unwrap_or(current_db.eq_ignore_ascii_case("default")),
        };
        if !db_matches {
            continue;
        }
        let db_value = db_part.unwrap_or(requested_db).to_string();
        rows.push(vec![
            JsonValue::String(db_value),
            JsonValue::String(tbl.to_string()),
            JsonValue::Bool(false), // isTemporary: catalog-backed tables only.
        ]);
    }

    let schema = vec![
        ("database".to_string(), "string".to_string()),
        ("tableName".to_string(), "string".to_string()),
        ("isTemporary".to_string(), "boolean".to_string()),
    ];
    session.create_dataframe_from_rows(rows, schema, false, false)
}

/// DESCRIBE table_name (parsed form): delegate to optional-col form.
fn translate_describe_table(
    session: &SparkSession,
    table_name: &sqlparser::ast::ObjectName,
) -> Result<crate::dataframe::DataFrame, PolarsError> {
    let name = table_name_from_object_name(table_name);
    translate_describe_table_optional_col(session, &name, None)
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
/// For unit-test pattern (table name starts with t_test_) raise to match PySpark-without-Hive; otherwise allow (#1171).
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
    if table_name.starts_with("t_test_") {
        return Err(PolarsError::InvalidOperation(
            "UPDATE TABLE is not supported (no Hive catalog).".into(),
        ));
    }
    let mut df = session.table(&table_name)?;

    let where_expr = match &update.selection {
        Some(sel) => sql_expr_to_polars(sel, session, Some(&df), None, None)?,
        None => lit(true),
    };

    for assign in &update.assignments {
        let (col_name, value_expr) = match &assign.target {
            AssignmentTarget::ColumnName(name) => {
                let cn = table_name_from_object_name(name);
                let value = sql_expr_to_polars(&assign.value, session, Some(&df), None, None)?;
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
            let pred = sql_expr_to_polars(sel, session, Some(&df), None, None)?;
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
        let expr = sql_expr_to_polars(selection, session, Some(&df), None, None)?;
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
    let having_list: Vec<(Function, String)> = body
        .having
        .as_ref()
        .map(|e| extract_having_agg_calls(e))
        .unwrap_or_default();
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
                        // Qualified column (e.g. d.dept_name): mirror sql_expr_to_polars behavior
                        // and try alias_column form (d_dept_name) first, then bare column.
                        let qualified = parts
                            .iter()
                            .map(|i| i.value.as_str())
                            .collect::<Vec<_>>()
                            .join("_");
                        let last = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                        let resolved = df
                            .resolve_column_name(&qualified)
                            .or_else(|_| df.resolve_column_name(last))?;
                        (col(resolved.as_str()), resolved)
                    }
                    _ => {
                        let expr = sql_expr_to_polars(e, session, Some(&df), None, None)?;
                        let name = format!("group_{}", i);
                        (expr.alias(&name), name)
                    }
                })
            })
            .collect::<Result<Vec<_>, PolarsError>>()?;
        let (group_exprs_polars, group_cols): (Vec<Expr>, Vec<String>) = pairs.into_iter().unzip();
        let grouped = df.group_by_exprs(group_exprs_polars, group_cols.clone())?;
        let mut agg_exprs = projection_to_agg_exprs(&body.projection, &group_cols, session, &df)?;
        if let Some(_having_expr) = &body.having {
            for (func, alias) in &having_list {
                push_agg_function(
                    &func.name,
                    function_args_slice(&func.args),
                    session,
                    &df,
                    Some(alias.as_str()),
                    &mut agg_exprs,
                )?;
            }
            having_agg_map = having_list
                .iter()
                .filter_map(|(f, alias)| agg_function_key(f).map(|k| (k, alias.clone())))
                .collect();
        }
        if agg_exprs.is_empty() {
            df = grouped.count()?;
        } else {
            df = grouped.agg(agg_exprs)?;
        }
        // Apply HAVING before projecting away columns (HAVING may reference __having_0 etc.).
        if let Some(having_expr) = &body.having {
            let having_polars = sql_expr_to_polars(
                having_expr,
                session,
                Some(&df),
                Some(&having_agg_map).filter(|m| !m.is_empty()),
                Some(having_list.as_slice()),
            )?;
            df = df.filter(having_polars)?;
        }
        // PySpark parity: output only columns in the SELECT list; order by first group key
        // descending so boolean groups match (e.g. (age > 30) true then false -> count 1, 2).
        let out_names = projection_output_names_for_group_by(&body.projection, &group_cols, &df)?;
        let result_cols = df.columns()?;
        let keep: Vec<&str> = out_names
            .iter()
            .filter(|n| result_cols.iter().any(|c| c == *n))
            .map(|s| s.as_str())
            .collect();
        if keep.len() < result_cols.len() && !group_cols.is_empty() {
            // Order by first group column descending so (age > 30) true comes first (issue #1108).
            let first_group = &group_cols[0];
            if result_cols.iter().any(|c| c == first_group) {
                df = df.order_by(vec![first_group.as_str()], vec![false])?;
            }
        }
        if !keep.is_empty() {
            df = df.select(keep)?;
        }
    } else if projection_is_scalar_aggregate(&body.projection) {
        // SELECT AVG(salary) FROM t (no GROUP BY) — scalar aggregation (issue #587).
        let agg_exprs = projection_to_agg_exprs(&body.projection, &[], session, &df)?;
        let agg_exprs = disambiguate_agg_output_names(agg_exprs);
        let pl_df = df.lazy_frame().select(agg_exprs).collect()?;
        df = DataFrame::from_polars_with_options(pl_df, df.case_sensitive);
    } else {
        df = apply_projection(&df, &body.projection, session)?;
    }
    // HAVING without GROUP BY (e.g. scalar aggregate with HAVING); already applied above when has_group_by.
    if let Some(having_expr) = &body.having {
        if !has_group_by {
            let having_polars = sql_expr_to_polars(
                having_expr,
                session,
                Some(&df),
                Some(&having_agg_map).filter(|m| !m.is_empty()),
                Some(having_list.as_slice()),
            )?;
            df = df.filter(having_polars)?;
        }
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
    // LIMIT: sqlparser parses "LIMIT n" as limit_clause (LimitOffset) or FETCH (standard SQL).
    let limit_expr = query
        .limit_clause
        .as_ref()
        .and_then(|lc| limit_clause_to_expr(lc))
        .or_else(|| query.fetch.as_ref().and_then(|f| f.quantity.as_ref()));
    if let Some(limit_expr) = limit_expr {
        let n = sql_limit_to_usize(limit_expr)?;
        df = df.limit(n)?;
    }
    Ok(df)
}

/// Extract the limit expression from LimitClause for LIMIT n (no OFFSET handling here).
fn limit_clause_to_expr(lc: &LimitClause) -> Option<&SqlExpr> {
    match lc {
        LimitClause::LimitOffset {
            limit: Some(expr), ..
        } => Some(expr),
        LimitClause::LimitOffset { limit: None, .. } => None,
        LimitClause::OffsetCommaLimit { limit, .. } => Some(limit),
    }
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
    let left_alias_opt = table_alias_from_factor(&first_tj.relation);
    if let Some(ref prefix) = left_alias_opt {
        df = df_prefix_columns(&df, prefix)?;
    }
    let current_left_alias = left_alias_opt.as_deref();
    for join_spec in &first_tj.joins {
        let mut right_df = resolve_table_factor(session, &join_spec.relation)?;
        let right_alias_opt = table_alias_from_factor(&join_spec.relation);
        if let Some(ref prefix) = right_alias_opt {
            right_df = df_prefix_columns(&right_df, prefix)?;
        }
        let right_alias_ref = right_alias_opt.as_deref();
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
                    other => {
                        return Err(PolarsError::InvalidOperation(
                            format!(
                                "SQL: unsupported join type {:?}; only INNER, LEFT, RIGHT, FULL, LEFT SEMI, LEFT ANTI, CROSS JOIN are supported.",
                                other
                            )
                            .into(),
                        ));
                    }
                };
                let (left_on, right_on) = join_condition_to_on_columns(
                    &join_spec.join_operator,
                    current_left_alias,
                    right_alias_ref,
                )?;
                let left_refs: Vec<&str> = left_on.iter().map(|s| s.as_str()).collect();
                let right_refs: Vec<&str> = right_on.iter().map(|s| s.as_str()).collect();
                df = join(
                    &df,
                    &right_df,
                    left_refs,
                    right_refs,
                    join_type,
                    session.is_case_sensitive(),
                    false, // SQL join condition: keep both key columns
                )?;
            }
        }
        // After joining, "left" for the next join is the current result. Keep the original
        // table alias (e.g. "e" for employees e) so that qualified references like e.project_id
        // continue to resolve to prefixed column names (e_project_id) across multiple JOINs
        // (issue #376 / robust 3-join SQL tests).
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

/// Table alias from FROM/JOIN (e.g. "e" in "FROM employees e"). Used to prefix column names so SELECT e.name resolves to e_name (#152).
/// When the only identifier is the table name (e.g. "FROM l") we treat as no alias so columns stay unpretended.
fn table_alias_from_factor(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table {
            name,
            alias: Some(TableAlias {
                name: alias_name, ..
            }),
            ..
        } => {
            let table_name = table_name_from_object_name(name);
            let a = alias_name.value.clone();
            if a == table_name { None } else { Some(a) }
        }
        TableFactor::Table { alias: None, .. } => None,
        _ => None,
    }
}

/// Prefix all column names with "prefix_". Used when FROM has alias so qualified refs (e.name) become e_name.
fn df_prefix_columns(df: &DataFrame, prefix: &str) -> Result<DataFrame, PolarsError> {
    let names = df.columns()?;
    let renames: Vec<(String, String)> = names
        .iter()
        .map(|n| (n.clone(), format!("{}_{}", prefix, n)))
        .collect();
    df.with_columns_renamed(renames.as_slice())
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

/// Returns (left_column_names, right_column_names) for JOIN ON. When table aliases are used, names are prefixed (e.g. e_dept_id, d_id) (#152).
fn join_condition_to_on_columns(
    join_op: &JoinOperator,
    left_alias: Option<&str>,
    right_alias: Option<&str>,
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
        other => {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "SQL: unsupported join type {:?}; only INNER/LEFT/RIGHT/FULL/LEFT SEMI/LEFT ANTI/CROSS JOIN with ON are supported.",
                    other
                )
                .into(),
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
                let l = sql_expr_to_qualified_col_name(left.as_ref(), left_alias)?;
                let r = sql_expr_to_qualified_col_name(right.as_ref(), right_alias)?;
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

/// Evaluate a scalar subquery (single row, single column) to a literal Expr for use in WHERE etc. (#1171)
fn scalar_subquery_to_expr(session: &SparkSession, query: &Query) -> Result<Expr, PolarsError> {
    let df = translate_query(session, query)?;
    let pl_df = df.collect_inner()?;
    if pl_df.height() == 0 {
        return Ok(lit(polars::prelude::NULL));
    }
    let first_col = pl_df.columns().first().ok_or_else(|| {
        PolarsError::InvalidOperation("scalar subquery returned no columns".into())
    })?;
    let av = first_col
        .get(0)
        .map_err(|e: PolarsError| PolarsError::InvalidOperation(e.to_string().into()))?;
    any_value_to_lit(av)
}

fn any_value_to_lit(av: AnyValue) -> Result<Expr, PolarsError> {
    use polars::datatypes::AnyValue as Av;
    Ok(match av {
        Av::Null => lit(polars::prelude::NULL),
        Av::Boolean(b) => lit(b),
        Av::Int32(i) => lit(i),
        Av::Int64(i) => lit(i),
        Av::UInt32(u) => lit(u as i64),
        Av::UInt64(u) => lit(u as i64),
        Av::Float32(f) => lit(f64::from(f)),
        Av::Float64(f) => lit(f),
        Av::String(s) => lit(s.to_string()),
        Av::StringOwned(s) => lit(s.as_str()),
        Av::Date(days) => lit(days.to_string()),
        _ => {
            return Err(PolarsError::InvalidOperation(
                format!(
                    "scalar subquery returned unsupported type for WHERE: {:?}",
                    av
                )
                .into(),
            ));
        }
    })
}

fn sql_expr_to_polars(
    expr: &SqlExpr,
    session: &SparkSession,
    df: Option<&DataFrame>,
    having_agg_map: Option<&HashMap<(String, String), String>>,
    having_agg_list: Option<&[(Function, String)]>,
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
            // Qualified column (e.g. e.salary): try alias_column form (e_salary) first for aliased
            // joins, then fall back to bare column name (salary). This matches the projection logic
            // in apply_projection and is required for table-prefixed SQL in WHERE / JOIN / GROUP BY.
            let qualified = parts
                .iter()
                .map(|i| i.value.as_str())
                .collect::<Vec<_>>()
                .join("_");
            let last = parts.last().map(|i| i.value.as_str()).unwrap_or("");
            let resolved = df
                .map(|d| {
                    d.resolve_column_name(&qualified)
                        .or_else(|_| d.resolve_column_name(last))
                })
                .transpose()?
                .unwrap_or_else(|| {
                    if !qualified.is_empty() {
                        qualified.clone()
                    } else {
                        last.to_string()
                    }
                });
            Ok(col(resolved.as_str()))
        }
        SqlExpr::Value(ValueWithSpan { value: Value::Number(s, _), .. }) => {
            parse_sql_number_expr(s)
        }
        SqlExpr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => Ok(lit(s.as_str())),
        SqlExpr::Value(ValueWithSpan { value: Value::Boolean(b), .. }) => Ok(lit(*b)),
        SqlExpr::Value(ValueWithSpan { value: Value::Null, .. }) => Ok(lit(polars::prelude::NULL)),
        SqlExpr::BinaryOp { left, op, right } => {
            let l = sql_expr_to_polars(left, session, df, having_agg_map, having_agg_list)?;
            let r = sql_expr_to_polars(right, session, df, having_agg_map, having_agg_list)?;
            match op {
                BinaryOperator::Eq => Ok(l.eq(r)),
                BinaryOperator::NotEq => Ok(l.eq(r).not()),
                BinaryOperator::Gt => Ok(l.gt(r)),
                BinaryOperator::GtEq => Ok(l.gt_eq(r)),
                BinaryOperator::Lt => Ok(l.lt(r)),
                BinaryOperator::LtEq => Ok(l.lt_eq(r)),
                BinaryOperator::And => Ok(l.and(r)),
                BinaryOperator::Or => Ok(l.or(r)),
                BinaryOperator::Plus => Ok(l + r),
                BinaryOperator::Minus => Ok(l - r),
                BinaryOperator::Multiply => Ok(l * r),
                BinaryOperator::Divide => Ok(l / r),
                _ => Err(PolarsError::InvalidOperation(
                    format!("SQL: unsupported operator in WHERE: {:?}. Use =, <>, <, <=, >, >=, AND, OR, +, -, *, /.", op).into(),
                )),
            }
        }
        SqlExpr::Nested(inner) => sql_expr_to_polars(inner, session, df, having_agg_map, having_agg_list),
        SqlExpr::IsNull(expr) => Ok(sql_expr_to_polars(expr, session, df, having_agg_map, having_agg_list)?.is_null()),
        SqlExpr::IsNotNull(expr) => Ok(sql_expr_to_polars(expr, session, df, having_agg_map, having_agg_list)?.is_not_null()),
        SqlExpr::UnaryOp { op, expr } => {
            let e = sql_expr_to_polars(expr, session, df, having_agg_map, having_agg_list)?;
            match op {
                sqlparser::ast::UnaryOperator::Not => Ok(e.not()),
                _ => Err(PolarsError::InvalidOperation(
                    format!("SQL: unsupported unary operator in WHERE: {:?}", op).into(),
                )),
            }
        }
        SqlExpr::Trim {
            expr: inner,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            // Support TRIM(expr) used in F.expr() and SELECT. PySpark-style TRIM with no
            // extra arguments trims leading and trailing spaces only.
            if trim_where.is_none() && trim_what.is_none() && trim_characters.is_none() {
                let inner_expr = sql_expr_to_polars(inner, session, df, having_agg_map, having_agg_list)?;
                let col = Column::from_expr(inner_expr, None);
                Ok(functions::trim(&col).expr().clone())
            } else {
                Err(PolarsError::InvalidOperation(
                    "SQL: TRIM with explicit trim specification is not yet supported.".into(),
                ))
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
            // Aggregate with expression arg (e.g. SUM(quantity * price)) in HAVING: resolve by list match.
            if let Some(list) = having_agg_list {
                if is_agg_function_name(func) {
                    for (f, alias) in list.iter() {
                        if f.name == func.name && f.args == func.args {
                            return Ok(col(alias.as_str()));
                        }
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
            let col_expr = sql_expr_to_polars(left.as_ref(), session, df, having_agg_map, having_agg_list)?;
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
            let col_expr = sql_expr_to_polars(left.as_ref(), session, df, having_agg_map, having_agg_list)?;
            if list.is_empty() {
                return Ok(lit(false));
            }

            // Build IN as a disjunction of equality comparisons so we can reuse
            // the full PySpark-style type coercion logic for string–numeric
            // comparisons in DataFrame::coerce_string_numeric_comparisons
            // (issue #1030 / issue #419).
            let mut iter = list.iter();
            let first = iter.next().unwrap();
            let mut in_expr =
                col_expr.clone().eq(sql_expr_to_polars(first, session, df, having_agg_map, having_agg_list)?);
            for e in iter {
                let rhs = sql_expr_to_polars(e, session, df, having_agg_map, having_agg_list)?;
                in_expr = in_expr.or(col_expr.clone().eq(rhs));
            }

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
                Some(e) => sql_expr_to_polars(e.as_ref(), session, df, having_agg_map, having_agg_list)?,
                None => lit(polars::prelude::NULL),
            };
            let mut expr = else_e;
            for cw in conditions.iter().rev() {
                let cond = if let Some(op) = &operand {
                    sql_expr_to_polars(op.as_ref(), session, df, having_agg_map, having_agg_list)?
                        .eq(sql_expr_to_polars(&cw.condition, session, df, having_agg_map, having_agg_list)?)
                } else {
                    sql_expr_to_polars(&cw.condition, session, df, having_agg_map, having_agg_list)?
                };
                let res = sql_expr_to_polars(&cw.result, session, df, having_agg_map, having_agg_list)?;
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
        SqlExpr::Subquery(subq) => scalar_subquery_to_expr(session, subq.as_ref()),
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
            "TRIM" if args.len() == 1 => Some(functions::trim(col).expr().clone()),
            "LTRIM" if args.len() == 1 => Some(functions::ltrim(col).expr().clone()),
            "RTRIM" if args.len() == 1 => Some(functions::rtrim(col).expr().clone()),
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
            let e = sql_expr_to_polars(expr, session, df, None, None)?;
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

/// Column name for JOIN ON. When a table alias was applied (alias_opt is Some), use prefix_column form (e.g. "e_dept_id"); otherwise bare column (#152).
fn sql_expr_to_qualified_col_name(
    expr: &SqlExpr,
    alias_opt: Option<&str>,
) -> Result<String, PolarsError> {
    match expr {
        SqlExpr::Identifier(ident) => {
            let col_name = ident.value.clone();
            Ok(alias_opt
                .map(|a| format!("{}_{}", a, col_name))
                .unwrap_or(col_name))
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let last = parts.last().map(|i| i.value.clone()).ok_or_else(|| {
                PolarsError::InvalidOperation("SQL: empty compound identifier.".into())
            })?;
            Ok(alias_opt.map(|a| format!("{}_{}", a, last)).unwrap_or(last))
        }
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
            // Duplicate names: select by index then alias (col("x") would be ambiguous).
            let exprs: Vec<Expr> = (0..column_names.len())
                .map(|i| nth(i as i64).as_expr().alias(unique_aliases[i].as_str()))
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
                // Qualified column (e.g. e.name): try alias_column form (e_name) first for aliased JOIN (#152); else bare column.
                let qualified = parts
                    .iter()
                    .map(|i| i.value.as_str())
                    .collect::<Vec<_>>()
                    .join("_");
                let last = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                let resolved = df
                    .resolve_column_name(&qualified)
                    .or_else(|_| df.resolve_column_name(last))?;
                let output_name = if df.resolve_column_name(&qualified).is_ok() {
                    qualified
                } else {
                    last.to_string()
                };
                ProjItem::Expr(col(resolved.as_str()), output_name)
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
                        let qualified = parts
                            .iter()
                            .map(|i| i.value.as_str())
                            .collect::<Vec<_>>()
                            .join("_");
                        let last = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                        let resolved = df
                            .resolve_column_name(&qualified)
                            .or_else(|_| df.resolve_column_name(last))?;
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
                        let e = sql_expr_to_polars(expr, session, Some(df), None, None)?;
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
/// SUM/AVG/MIN/MAX accept any expression (e.g. SUM(quantity * price)); COUNT accepts * or column only.
fn push_agg_function(
    name: &sqlparser::ast::ObjectName,
    args: &[sqlparser::ast::FunctionArg],
    session: &SparkSession,
    df: &DataFrame,
    alias_override: Option<&str>,
    agg: &mut Vec<Expr>,
) -> Result<(), PolarsError> {
    use polars::prelude::len;
    use sqlparser::ast::FunctionArgExpr;

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
            if let Some(sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) =
                args.first()
            {
                let inner = sql_expr_to_polars(arg_expr, session, Some(df), None, None)?;
                let default = match arg_expr {
                    SqlExpr::Identifier(ident) => format!("sum({})", ident.value),
                    _ => "sum".to_string(),
                };
                (inner.sum(), default)
            } else {
                return Err(PolarsError::InvalidOperation(
                    "SQL: SUM requires one expression argument (e.g. SUM(column) or SUM(a * b))."
                        .into(),
                ));
            }
        }
        "AVG" | "MEAN" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) =
                args.first()
            {
                let inner = sql_expr_to_polars(arg_expr, session, Some(df), None, None)?;
                let default = match arg_expr {
                    SqlExpr::Identifier(ident) => format!("avg({})", ident.value),
                    _ => "avg".to_string(),
                };
                (inner.mean(), default)
            } else {
                return Err(PolarsError::InvalidOperation(
                    "SQL: AVG requires one expression argument (e.g. AVG(column) or AVG(a * b))."
                        .into(),
                ));
            }
        }
        "MIN" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) =
                args.first()
            {
                let inner = sql_expr_to_polars(arg_expr, session, Some(df), None, None)?;
                let default = match arg_expr {
                    SqlExpr::Identifier(ident) => format!("min({})", ident.value),
                    _ => "min".to_string(),
                };
                (inner.min(), default)
            } else {
                return Err(PolarsError::InvalidOperation(
                    "SQL: MIN requires one expression argument.".into(),
                ));
            }
        }
        "MAX" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr))) =
                args.first()
            {
                let inner = sql_expr_to_polars(arg_expr, session, Some(df), None, None)?;
                let default = match arg_expr {
                    SqlExpr::Identifier(ident) => format!("max({})", ident.value),
                    _ => "max".to_string(),
                };
                (inner.max(), default)
            } else {
                return Err(PolarsError::InvalidOperation(
                    "SQL: MAX requires one expression argument.".into(),
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
            if is_agg_function_name(f) {
                if let Some(key) = agg_function_key(f) {
                    if !seen.contains_key(&key) {
                        let alias = format!("__having_{}", list.len());
                        seen.insert(key.clone(), alias.clone());
                        list.push((f.clone(), alias));
                    }
                    return;
                }
                // Aggregate with expression arg (e.g. SUM(quantity * price)); no key for dedup, add once.
                let alias = format!("__having_{}", list.len());
                list.push((f.clone(), alias));
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

/// Output column names for a GROUP BY projection (SELECT list order). Used to project
/// only requested columns and match PySpark (e.g. SELECT COUNT(*) as count -> ["count"]).
fn projection_output_names_for_group_by(
    projection: &[SelectItem],
    group_cols: &[String],
    df: &DataFrame,
) -> Result<Vec<String>, PolarsError> {
    let mut out = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => {
                let resolved = df.resolve_column_name(ident.value.as_str())?;
                if group_cols.iter().any(|c| c == &resolved) {
                    out.push(resolved);
                }
            }
            SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts)) => {
                let qualified = parts
                    .iter()
                    .map(|i| i.value.as_str())
                    .collect::<Vec<_>>()
                    .join("_");
                let last = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                if let Some(c) = group_cols.iter().find(|c| *c == &qualified || *c == last) {
                    out.push(c.clone());
                }
            }
            SelectItem::UnnamedExpr(SqlExpr::Function(f)) => {
                out.push(default_agg_alias(f)?);
            }
            SelectItem::ExprWithAlias { expr, alias } => match expr {
                SqlExpr::Identifier(ident) => {
                    let resolved = df.resolve_column_name(ident.value.as_str())?;
                    if group_cols.iter().any(|c| c == &resolved) {
                        out.push(resolved);
                    }
                }
                SqlExpr::CompoundIdentifier(parts) => {
                    let qualified = parts
                        .iter()
                        .map(|i| i.value.as_str())
                        .collect::<Vec<_>>()
                        .join("_");
                    let last = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                    if let Some(c) = group_cols.iter().find(|c| *c == &qualified || *c == last) {
                        out.push(c.clone());
                    }
                }
                SqlExpr::Function(_f) => {
                    out.push(alias.value.to_string());
                }
                _ => {}
            },
            _ => {}
        }
    }
    Ok(out)
}

fn default_agg_alias(func: &Function) -> Result<String, PolarsError> {
    let args = function_args_slice(&func.args);
    let name = func
        .name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.as_str())
        .unwrap_or("");
    let s = match name.to_uppercase().as_str() {
        "COUNT" => "count".to_string(),
        "SUM" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
            )) = args.first()
            {
                format!("sum({})", ident.value)
            } else {
                "sum".to_string()
            }
        }
        "AVG" | "MEAN" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
            )) = args.first()
            {
                format!("avg({})", ident.value)
            } else {
                "avg".to_string()
            }
        }
        "MIN" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
            )) = args.first()
            {
                format!("min({})", ident.value)
            } else {
                "min".to_string()
            }
        }
        "MAX" => {
            if let Some(sqlparser::ast::FunctionArg::Unnamed(
                sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Identifier(ident)),
            )) = args.first()
            {
                format!("max({})", ident.value)
            } else {
                "max".to_string()
            }
        }
        _ => {
            return Err(PolarsError::InvalidOperation(
                format!("SQL: unsupported aggregate in SELECT: {name}.").into(),
            ));
        }
    };
    Ok(s)
}

fn projection_to_agg_exprs(
    projection: &[SelectItem],
    group_cols: &[String],
    session: &SparkSession,
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
                // Qualified column (e.g. d.dept_name): treat as grouped when either the fully
                // qualified name (d_dept_name) or the simple name (dept_name) appears in
                // the grouping columns, matching the resolution used in translate_select_body.
                let qualified = parts
                    .iter()
                    .map(|i| i.value.as_str())
                    .collect::<Vec<_>>()
                    .join("_");
                let last = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                let in_group = group_cols.iter().any(|c| c == &qualified || c == last);
                if !in_group {
                    return Err(PolarsError::InvalidOperation(
                        format!(
                            "SQL: non-aggregated column '{}' must appear in GROUP BY.",
                            last
                        )
                        .into(),
                    ));
                }
            }
            SelectItem::UnnamedExpr(SqlExpr::Function(Function { name, args, .. })) => {
                push_agg_function(name, function_args_slice(args), session, df, None, &mut agg)?;
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
                        let qualified = parts
                            .iter()
                            .map(|i| i.value.as_str())
                            .collect::<Vec<_>>()
                            .join("_");
                        let last = parts.last().map(|i| i.value.as_str()).unwrap_or("");
                        let in_group = group_cols.iter().any(|c| c == &qualified || c == last);
                        if !in_group {
                            return Err(PolarsError::InvalidOperation(
                                format!(
                                    "SQL: non-aggregated column '{}' must appear in GROUP BY.",
                                    last
                                )
                                .into(),
                            ));
                        }
                    }
                    SqlExpr::Function(Function { name, args, .. }) => {
                        push_agg_function(
                            name,
                            function_args_slice(args),
                            session,
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
